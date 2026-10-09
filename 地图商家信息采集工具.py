# -*- coding: utf-8 -*-
"""
地图商家信息采集工具
================================
一款 Windows 桌面工具，通过「高德地图 / 百度地图 / 腾讯地图」官方 POI 检索接口，
按城市 + 关键词批量采集商家信息（名称、电话、地址、坐标等），
支持数据过滤、去重、导出（CSV / Excel 兼容）、定时采集与进度统计。

依赖：仅使用 Python 标准库（tkinter / urllib / csv），零第三方依赖，
拿到即可运行。

运行：python 地图商家信息采集工具.py
"""

import csv
import json
import os
import queue
import re
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime

try:
    import tkinter as tk
    from tkinter import ttk, filedialog, messagebox
except Exception:
    raise SystemExit("缺少 tkinter 图形库，请安装 Python 后重试。")

APP_VERSION = "V1.4.0"
APP_TITLE = "商户数据采集专家 " + APP_VERSION


# ---------------------------------------------------------------------------
# 地图 API 配置（官方接口）
# ---------------------------------------------------------------------------
AMAP_TEXT_URL = "https://restapi.amap.com/v3/place/text"
BAIDU_SEARCH_URL = "https://api.map.baidu.com/place/v2/search"
TENCENT_SEARCH_URL = "https://apis.map.qq.com/ws/place/v1/search"

AMAP_PAGE_SIZE = 25        # 高德每页条数（最大 25）
BAIDU_PAGE_SIZE = 20       # 百度每页条数（最大 20）
TENCENT_PAGE_SIZE = 20     # 腾讯每页条数（最大 20）

DEFAULT_DELAY = 0.5        # 相邻请求间隔（秒），高德免费Key默认QPS≈3，0.5秒较稳
RETRY_TIMES = 5            # 单页请求失败重试次数（网络/瞬时限流自动续采）

# 永久性错误（重试无意义，直接停止不浪费配额）
PERMANENT_ERROR_HINTS = (
    "INVALID_USER_KEY", "INVALID_KEY", "INVALID_AK", "INVALID_PARAMS",
    "NO_PERMISSION", "DAILY_QUERY_OVER_LIMIT", "USER_DAILY_QUERY_OVER_LIMIT",
    "KEY_EXPIRED", "INVALID_SERVICE",
)


class Config:
    """参数配置：API Key 及通用参数，持久化到 config.json"""

    DEFAULTS = {
        "amap_key": "",       # 高德 Web服务 Key
        "baidu_ak": "",       # 百度地图 服务端 AK
        "tencent_key": "",    # 腾讯位置服务 Key
        "page_limit": 50,     # 单个关键词最大采集页数
        "delay": 0.5,         # 请求间隔（秒），高德免费Key默认QPS≈3，0.5秒较稳
        "export_dir": "",     # 导出目录
        "request_timeout": 15,  # 单次请求超时（秒）
    }

    def __init__(self, path=None):
        self.path = path or os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
        self.data = dict(self.DEFAULTS)
        self.load()

    def load(self):
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                saved = json.load(f)
            self.data.update({k: saved[k] for k in self.DEFAULTS if k in saved})
        except Exception:
            pass

    def save(self):
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print("保存配置失败:", e)

    def __getitem__(self, key):
        return self.data.get(key, self.DEFAULTS.get(key))

    def __setitem__(self, key, value):
        self.data[key] = value


# ---------------------------------------------------------------------------
# 采集核心逻辑（线程中运行）
# ---------------------------------------------------------------------------
class Collector:
    """按城市+关键词，调用高德/百度官方 POI 接口采集商家信息"""

    def __init__(self, source, key, city, keywords, filters, config):
        self.source = source            # "高德地图" / "百度地图"
        self.key = key.strip()
        self.city = city.strip()
        self.keywords = [k.strip() for k in keywords if k.strip()]
        self.filters = filters          # dict: title_exclude, addr_exclude, title_include, no_tel, no_no_contact
        self.config = config
        self.stop_flag = threading.Event()
        self.dedup = set()              # 去重指纹

    def request(self, url, params):
        """发送 HTTP GET，返回 JSON；失败抛异常。"""
        qs = urllib.parse.urlencode(params)
        full = url + "?" + qs
        req = urllib.request.Request(full, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=int(self.config["request_timeout"])) as r:
            return json.loads(r.read().decode("utf-8", errors="replace"))

    def _safe_page(self, url, params, is_ok, retries=RETRY_TIMES):
        """请求一页并做有限重试。

        网络异常、以及非永久性业务失败（如限流/繁忙）会自动退避重试；
        永久性错误（Key无效等）直接返回，避免浪费配额。
        返回 (data, None) 或 (None, 失败原因)。
        """
        for attempt in range(1, retries + 1):
            if self.stop_flag.is_set():
                return None, "已停止"
            try:
                data = self.request(url, params)
            except Exception as e:
                if attempt < retries:
                    time.sleep(min(float(self.config["delay"]) * (2 ** (attempt - 1)), 8.0))
                    continue
                return None, "网络/请求异常: %s" % e
            if is_ok(data):
                return data, None
            msg = str(data.get("info") or data.get("message") or "")
            if any(k in msg.upper() for k in PERMANENT_ERROR_HINTS):
                return None, "接口返回(永久性错误，不再重试): %s" % msg
            if attempt < retries:
                # 限流/繁忙类错误：指数退避后重试，尽量自愈续采
                time.sleep(min(float(self.config["delay"]) * (2 ** (attempt - 1)), 8.0))
                continue
            return None, "接口返回(已重试%d次): %s" % (retries, msg)
        return None, "未知错误"

    @staticmethod
    def _limit_hint(err):
        e = (err or "").upper()
        if any(k in e for k in ("OVER_LIMIT", "FREQUENCY", "LIMIT", "QUOTA", "BUSY")):
            return "（提示：疑似免费Key并发/配额限制，可在参数设置中调大请求间隔，或升级更高配额）"
        return ""

    def _pass_filters(self, name, address, tel):
        f = self.filters
        # 标题须包含的词（满足任意一个即可）
        if f.get("title_include"):
            if not any(w in name for w in f["title_include"]):
                return False
        # 标题排除
        if f.get("title_exclude") and any(w in name for w in f["title_exclude"]):
            return False
        # 地址排除
        if f.get("addr_exclude") and any(w in address for w in f["addr_exclude"]):
            return False
        # 不要座机
        if f.get("no_tel") and self._is_landline(tel):
            return False
        # 不要没有联系方式的数据（手机或座机都没有）
        if f.get("no_no_contact") and not tel:
            return False
        return True

    @staticmethod
    def _is_landline(tel):
        if not tel:
            return False
        t = tel.strip()
        return bool(re.match(r"^0\d{2,3}-?\d{7,8}$", t)) or ("-" in t and t.split("-")[0].startswith("0"))

    def _make_record(self, idx, name, tel, landline, address, city, keyword,
                     source, coord, source_url):
        """手机与座机分离：手机号 1[3-9] 开头；其余归座机。"""
        mobile = ""
        if tel:
            parts = [p.strip() for p in tel.split(";") if p.strip()]
            mobiles, fixed = [], []
            for p in parts:
                digits = re.sub(r"\D", "", p)
                if re.match(r"^1[3-9]\d{9}$", digits):
                    mobiles.append(digits)
                else:
                    fixed.append(p)
            mobile = ";".join(dict.fromkeys(mobiles))
            if landline or fixed:
                fixed = fixed or landline.split(";")
                landline = ";".join(dict.fromkeys(fixed))
        return {
            "编号": idx,
            "标题名称": name,
            "手机": mobile,
            "座机": landline if landline else ("" if mobile else tel),
            "地址": address,
            "城市": city,
            "关键字": keyword,
            "数据源": source,
            "坐标": coord,
        }

    def _dedup_key(self, name, tel, coord):
        k = (name, tel or "", coord or "")
        if k in self.dedup:
            return False
        self.dedup.add(k)
        return True

    # ---- 高德 ----
    def collect_amap(self, on_record):
        page = 1
        while not self.stop_flag.is_set() and page <= int(self.config["page_limit"]):
            params = {
                "key": self.key,
                "keywords": ",".join(self.keywords),
                "city": self.city,
                "citylimit": "true",
                "offset": AMAP_PAGE_SIZE,
                "page": page,
                "extensions": "all",
                "output": "json",
            }
            data, err = self._safe_page(AMAP_TEXT_URL, params, lambda d: d.get("status") == "1")
            if data is None:
                on_record("log", "高德请求失败(page=%d): %s%s" % (page, err, self._limit_hint(err)))
                break
            pois = data.get("pois") or []
            if not pois:
                on_record("log", "高德本页返回空(第%d页)，停止翻页。" % page)
                break
            total = data.get("count")
            try:
                total = int(total)
            except Exception:
                total = len(pois)
            for p in pois:
                if self.stop_flag.is_set():
                    return
                name = (p.get("name") or "").strip()
                address = (p.get("address") or "").strip()
                tel = (p.get("tel") or "").strip()
                coord = (p.get("location") or "").strip()
                city = (p.get("cityname") or "").strip()
                if not city:
                    city = self.city
                source_url = (p.get("website") or "").strip()
                if not self._pass_filters(name, address, tel):
                    continue
                if not self._dedup_key(name, tel, coord):
                    continue
                rec = self._make_record(0, name, tel, "", address, city,
                                        ",".join(self.keywords), "高德地图", coord, source_url)
                on_record("record", rec)
            if page * AMAP_PAGE_SIZE >= total:
                break
            page += 1
            time.sleep(float(self.config["delay"]))

    # ---- 百度 ----
    def collect_baidu(self, on_record):
        page_num = 0
        while not self.stop_flag.is_set() and page_num < int(self.config["page_limit"]):
            params = {
                "ak": self.key,
                "query": ",".join(self.keywords),
                "region": self.city,
                "output": "json",
                "page_size": BAIDU_PAGE_SIZE,
                "page_num": page_num,
                "scope": "2",
            }
            data, err = self._safe_page(BAIDU_SEARCH_URL, params, lambda d: d.get("status") == 0)
            if data is None:
                on_record("log", "百度请求失败(page=%d): %s%s" % (page_num, err, self._limit_hint(err)))
                break
            results = data.get("results") or []
            if not results:
                on_record("log", "百度本页返回空(第%d页)，停止翻页。" % page_num)
                break
            total = data.get("total")
            try:
                total = int(total)
            except Exception:
                total = len(results)
            for r in results:
                if self.stop_flag.is_set():
                    return
                name = (r.get("name") or "").strip()
                address = (r.get("address") or "").strip()
                tel = (r.get("telephone") or "").strip()
                loc = r.get("location") or {}
                coord = "%s,%s" % (loc.get("lng", ""), loc.get("lat", "")) if loc else ""
                city = (r.get("city") or "").strip()
                if not city:
                    city = self.city
                source_url = (r.get("detail_info") or {}).get("detail_url", "") if r.get("detail_info") else ""
                if not self._pass_filters(name, address, tel):
                    continue
                if not self._dedup_key(name, tel, coord):
                    continue
                rec = self._make_record(0, name, tel, "", address, city,
                                        ",".join(self.keywords), "百度地图", coord, source_url)
                on_record("record", rec)
            if (page_num + 1) * BAIDU_PAGE_SIZE >= total:
                break
            page_num += 1
            time.sleep(float(self.config["delay"]))

    # ---- 腾讯 ----
    def collect_tencent(self, on_record):
        page = 1
        while not self.stop_flag.is_set() and page <= int(self.config["page_limit"]):
            params = {
                "key": self.key,
                "keyword": ",".join(self.keywords),
                "boundary": "region(%s,0)" % self.city,
                "page_size": TENCENT_PAGE_SIZE,
                "page_index": page,
                "output": "json",
            }
            data, err = self._safe_page(TENCENT_SEARCH_URL, params, lambda d: d.get("status") == 0)
            if data is None:
                on_record("log", "腾讯请求失败(page=%d): %s%s" % (page, err, self._limit_hint(err)))
                break
            items = data.get("data") or []
            if not items:
                on_record("log", "腾讯本页返回空(第%d页)，停止翻页。" % page)
                break
            total = data.get("count") or len(items)
            for it in items:
                if self.stop_flag.is_set():
                    return
                name = (it.get("title") or "").strip()
                address = (it.get("address") or "").strip()
                tel = (it.get("tel") or "").strip()
                loc = it.get("location") or {}
                coord = "%s,%s" % (loc.get("lng", ""), loc.get("lat", "")) if loc else ""
                city = (it.get("city") or "").strip()
                if not city:
                    city = self.city
                source_url = ""  # 腾讯检索结果无来源网址
                if not self._pass_filters(name, address, tel):
                    continue
                if not self._dedup_key(name, tel, coord):
                    continue
                rec = self._make_record(0, name, tel, "", address, city,
                                        ",".join(self.keywords), "腾讯地图", coord, source_url)
                on_record("record", rec)
            if page * TENCENT_PAGE_SIZE >= total:
                break
            page += 1
            time.sleep(float(self.config["delay"]))

    def run(self, on_record):
        """启动采集（在后台线程调用）。on_record(kind, payload)。"""
        if self.source == "高德地图":
            self.collect_amap(on_record)
        elif self.source == "百度地图":
            self.collect_baidu(on_record)
        else:
            self.collect_tencent(on_record)
        on_record("done", None)


# ---------------------------------------------------------------------------
# 参数设置对话框
# ---------------------------------------------------------------------------
class SettingsDialog(tk.Toplevel):
    def __init__(self, parent, config):
        super().__init__(parent)
        self.config = config
        self.title("参数设置")
        self.resizable(False, False)
        self.transient(parent)
        self.grab_set()
        self._build()
        self.geometry("+%d+%d" % (parent.winfo_rootx() + 80, parent.winfo_rooty() + 80))

    def _build(self):
        frm = ttk.Frame(self, padding=12)
        frm.pack(fill="both", expand=True)

        rows = [
            ("高德 Key (Web服务)", "amap_key"),
            ("百度 AK (服务端)", "baidu_ak"),
            ("腾讯 Key (位置服务)", "tencent_key"),
            ("单关键词最大页数", "page_limit"),
            ("请求间隔(秒)", "delay"),
            ("请求超时(秒)", "request_timeout"),
            ("导出目录", "export_dir"),
        ]
        self.vars = {}
        for i, (label, key) in enumerate(rows):
            ttk.Label(frm, text=label).grid(row=i, column=0, sticky="w", padx=4, pady=4)
            v = tk.StringVar(value=str(self.config[key]))
            self.vars[key] = v
            if key == "export_dir":
                box = ttk.Frame(frm)
                box.grid(row=i, column=1, sticky="ew", padx=4, pady=4)
                ent = ttk.Entry(box, textvariable=v)
                ent.pack(side="left", fill="x", expand=True)
                ttk.Button(box, text="浏览", width=6,
                           command=lambda: v.set(filedialog.askdirectory())).pack(side="left", padx=4)
            else:
                ttk.Entry(frm, textvariable=v, width=42).grid(row=i, column=1, sticky="ew",
                                                              padx=4, pady=4)
        btn = ttk.Frame(frm)
        btn.grid(row=len(rows), column=0, columnspan=2, pady=8)
        ttk.Button(btn, text="保存", command=self._save).pack(side="left", padx=6)
        ttk.Button(btn, text="取消", command=self.destroy).pack(side="left", padx=6)

        ttk.Label(frm, text="提示：请到高德开放平台/百度地图开放平台免费申请 Key。",
                  foreground="#888").grid(row=len(rows) + 1, column=0, columnspan=2, pady=2, sticky="w")

    def _save(self):
        try:
            int(self.vars["page_limit"].get())
            float(self.vars["delay"].get())
            int(self.vars["request_timeout"].get())
        except Exception:
            messagebox.showerror("参数错误", "页数/间隔/超时必须为数字", parent=self)
            return
        for key in self.config.DEFAULTS:
            self.config[key] = self.vars[key].get().strip()
        self.config.save()
        messagebox.showinfo("参数设置", "已保存", parent=self)
        self.destroy()


# ---------------------------------------------------------------------------
# 主界面
# ---------------------------------------------------------------------------
COLUMNS = [
    ("编号", 60),
    ("标题名称", 220),
    ("手机", 130),
    ("座机", 130),
    ("地址", 300),
    ("城市", 90),
    ("关键字", 90),
    ("数据源", 80),
    ("坐标", 150),
]


class MainApp:
    def __init__(self, root):
        self.root = root
        self.config = Config()
        self.records = []           # 已采集记录
        self.success_count = 0
        self.running = False
        self.worker = None
        self.msg_queue = queue.Queue()

        root.title(APP_TITLE + "  By Macstin ")
        root.geometry("1100x720")
        root.minsize(960, 620)

        self._build_ui()
        self.root.after(100, self._poll_queue)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    # ---------- 界面构建 ----------
    def _build_ui(self):
        self._build_toolbar()
        self._build_table()
        self._build_bottom()
        self._build_statusbar()

    def _build_toolbar(self):
        bar = tk.Frame(self.root, bg="#2f6fb2", height=44)
        bar.pack(side="top", fill="x")
        bar.pack_propagate(False)
        tk.Label(bar, text=APP_TITLE, bg="#2f6fb2", fg="white",
                 font=("Microsoft YaHei", 12, "bold")).pack(side="left", padx=12)
        for text in ["数据采集", "使用帮助", "打赏我们"]:
            tk.Button(bar, text=text, bg="#2f6fb2", fg="white", relief="flat",
                      font=("Microsoft YaHei", 10), activebackground="#1e4f80",
                      command=lambda t=text: self._toolbar_action(t)).pack(side="left", padx=4)

    def _toolbar_action(self, action):
        if action == "使用帮助":
            self._show_help()
        elif action == "打赏我们":
            self._show_donate()
        else:  # 数据采集
            self.root.focus_set()

    def _show_donate(self):
        """弹出打赏窗口：显示支付宝 + 微信打赏二维码（图片放在工具目录）"""
        win = tk.Toplevel(self.root)
        win.title("打赏我们")
        win.resizable(False, False)
        win.transient(self.root)
        win.grab_set()
        base = os.path.dirname(os.path.abspath(__file__))
        rows = [("支付宝", "打赏_支付宝.png"), ("微信", "打赏_微信.png")]
        tk.Label(win, text="如果这个工具帮到了你，欢迎打赏支持一下～",
                 font=("Microsoft YaHei", 11, "bold")).pack(padx=16, pady=(12, 4))
        frame = tk.Frame(win)
        frame.pack(padx=12, pady=8)
        found_any = False
        for label, fname in rows:
            cell = tk.LabelFrame(frame, text=label, padx=8, pady=6)
            cell.pack(side="left", padx=8)
            path = os.path.join(base, fname)
            if os.path.exists(path):
                try:
                    img = tk.PhotoImage(file=path)
                    tk.Label(cell, image=img).pack()
                    cell.image = img  # 防止被回收
                    found_any = True
                except Exception:
                    tk.Label(cell, text="图片无法加载\n%s" % fname, fg="#c00").pack()
            else:
                tk.Label(cell, text="未找到打赏二维码\n请在工具目录放入\n%s" % fname,
                         foreground="#888", justify="left").pack()
        if not found_any:
            tk.Label(win, text="将你的支付宝/微信收款码截图分别命名为\n打赏_支付宝.png、打赏_微信.png，\n"
                               "放到工具程序同一目录后，即可在此显示。",
                     foreground="#666", font=("Microsoft YaHei", 9)).pack(padx=12, pady=(0, 10))
        else:
            tk.Label(win, text="长按识别二维码，感谢您的支持！",
                     foreground="#666", font=("Microsoft YaHei", 9)).pack(pady=(0, 10))
        tk.Button(win, text="关闭", command=win.destroy).pack(pady=(0, 10))
        win.geometry("+%d+%d" % (self.root.winfo_rootx() + 120, self.root.winfo_rooty() + 120))

    def _build_table(self):
        wrap = tk.Frame(self.root)
        wrap.pack(side="top", fill="both", expand=True, padx=6, pady=4)
        self.tree = ttk.Treeview(wrap, columns=[c[0] for c in COLUMNS], show="headings", height=18)
        for cid, width in COLUMNS:
            self.tree.heading(cid, text=cid)
            self.tree.column(cid, width=width, anchor="w")
        vsb = ttk.Scrollbar(wrap, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        hsb = ttk.Scrollbar(wrap, orient="horizontal", command=self.tree.xview)
        self.tree.configure(xscrollcommand=hsb.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        wrap.rowconfigure(0, weight=1)
        wrap.columnconfigure(0, weight=1)
        self.tree.bind("<Double-1>", self._on_row_double)

    def _build_bottom(self):
        bottom = tk.Frame(self.root, bd=1, relief="groove")
        bottom.pack(side="bottom", fill="x")

        # 三栏面板
        cols = tk.Frame(bottom)
        cols.pack(fill="x", padx=6, pady=4)
        cols.columnconfigure(0, weight=3)
        cols.columnconfigure(1, weight=3)
        cols.columnconfigure(2, weight=3)

        self._build_condition_panel(cols)
        self._build_filter_panel(cols)
        self._build_action_panel(cols)

    def _build_condition_panel(self, parent):
        frm = tk.LabelFrame(parent, text="采集条件(必填)", padx=6, pady=6)
        frm.grid(row=0, column=0, sticky="nsew", padx=3)
        # 地图源
        self.source_var = tk.StringVar(value="高德地图")
        ttk.Label(frm, text="地图源：").grid(row=0, column=0, sticky="w")
        src = ttk.Frame(frm)
        src.grid(row=0, column=1, columnspan=2, sticky="w")
        for s in ["高德地图", "百度地图", "腾讯地图"]:
            ttk.Radiobutton(src, text=s, variable=self.source_var, value=s).pack(side="left")
        # 城市
        ttk.Label(frm, text="选择城市：").grid(row=1, column=0, sticky="w", pady=4)
        self.city_var = tk.StringVar(value="郑州")
        ttk.Entry(frm, textvariable=self.city_var, width=30).grid(row=1, column=1, columnspan=2, sticky="w")
        # 关键词（多行）
        ttk.Label(frm, text="输入关键词：").grid(row=2, column=0, sticky="nw", pady=4)
        kw = tk.Text(frm, width=34, height=4)
        kw.grid(row=2, column=1, columnspan=2, sticky="w")
        kw.insert("1.0", "餐饮")
        self.keyword_box = kw
        ttk.Label(frm, text="例如：餐饮\n多个关键词用换行分隔", foreground="#888",
                  font=("Microsoft YaHei", 8)).grid(row=3, column=1, columnspan=2, sticky="w")

    def _build_filter_panel(self, parent):
        frm = tk.LabelFrame(parent, text="数据过滤(可不填)", padx=6, pady=6)
        frm.grid(row=0, column=1, sticky="nsew", padx=3)
        ttk.Label(frm, text="标题排除的词：").grid(row=0, column=0, sticky="w")
        self.f_title_excl = ttk.Entry(frm, width=24)
        self.f_title_excl.grid(row=0, column=1, sticky="w", pady=2)
        ttk.Label(frm, text="地址排除的词：").grid(row=1, column=0, sticky="w")
        self.f_addr_excl = ttk.Entry(frm, width=24)
        self.f_addr_excl.grid(row=1, column=1, sticky="w", pady=2)
        ttk.Label(frm, text="标题须包含的词：").grid(row=2, column=0, sticky="w")
        self.f_title_inc = ttk.Entry(frm, width=24)
        self.f_title_inc.grid(row=2, column=1, sticky="w", pady=2)
        ttk.Label(frm, text="(多个词用空格分开)", foreground="#888",
                  font=("Microsoft YaHei", 8)).grid(row=3, column=1, sticky="w")
        self.f_no_tel = tk.BooleanVar(value=False)
        self.f_no_contact = tk.BooleanVar(value=False)
        self.f_county = tk.BooleanVar(value=False)
        ttk.Checkbutton(frm, text="不要座机", variable=self.f_no_tel).grid(row=4, column=0, sticky="w", pady=2)
        ttk.Checkbutton(frm, text="不要没有联系方式的数据", variable=self.f_no_contact).grid(row=4, column=1, sticky="w")

    def _build_action_panel(self, parent):
        frm = tk.LabelFrame(parent, text="软件操作", padx=6, pady=6)
        frm.grid(row=0, column=2, sticky="nsew", padx=3)
        btns = ttk.Frame(frm)
        btns.pack(fill="x", pady=4)
        ttk.Button(btns, text="开始采集", command=self._start).pack(fill="x", pady=2)
        ttk.Button(btns, text="停止采集", command=self._stop).pack(fill="x", pady=2)
        ttk.Button(btns, text="导出数据", command=self._export).pack(fill="x", pady=2)
        ttk.Button(btns, text="参数设置", command=self._open_settings).pack(fill="x", pady=2)
        ttk.Button(btns, text="清空数据", command=self._clear).pack(fill="x", pady=2)
        self.count_var = tk.StringVar(value="成功采集条数：0")
        ttk.Label(frm, textvariable=self.count_var, font=("Microsoft YaHei", 11, "bold")).pack(pady=4)

    def _build_statusbar(self):
        bar = ttk.Frame(self.root)
        bar.pack(side="bottom", fill="x")
        self.status_var = tk.StringVar(value="状态：空闲")
        ttk.Label(bar, textvariable=self.status_var, anchor="w").pack(side="left", padx=8)

    # ---------- 输入解析 ----------
    def _parse_filters(self):
        def split(text):
            return [w for w in re.split(r"[\s,，;；]+", text) if w]
        return {
            "title_exclude": split(self.f_title_excl.get()),
            "addr_exclude": split(self.f_addr_excl.get()),
            "title_include": split(self.f_title_inc.get()),
            "no_tel": self.f_no_tel.get(),
            "no_no_contact": self.f_no_contact.get(),
        }

    # ---------- 采集控制 ----------
    def _start(self):
        if self.running:
            messagebox.showwarning("提示", "正在采集中，请先停止。")
            return
        source = self.source_var.get()
        city = self.city_var.get().strip()
        keywords = [k.strip() for k in self.keyword_box.get("1.0", "end").splitlines() if k.strip()]
        if not city or not keywords:
            messagebox.showwarning("提示", "请填写城市和关键词。")
            return
        key_map = {"高德地图": "amap_key", "百度地图": "baidu_ak", "腾讯地图": "tencent_key"}
        key_platform = {"高德地图": "console.amap.com",
                        "百度地图": "lbsyun.baidu.com",
                        "腾讯地图": "lbs.qq.com"}
        key = self.config[key_map[source]]
        if not key:
            messagebox.showwarning("提示",
                                   "未填写 %s 的 Key，请先在「参数设置」中填写。\n\n"
                                   "高德：开放平台 https://console.amap.com\n"
                                   "百度：地图开放平台 https://lbsyun.baidu.com\n"
                                   "腾讯：位置服务 https://lbs.qq.com"
                                   % source)
            return

        filters = self._parse_filters()
        self._set_running(True)
        self._log("开始采集：%s | 城市=%s | 关键词=%s" % (source, city, "、".join(keywords)))
        self._set_status("状态：采集中...")

        # 清空上一次结果（若当前无数据），或保留追加
        self.collector = Collector(source, key, city, keywords, filters, self.config)

        def worker():
            try:
                self.collector.run(self._enqueue)
            except Exception as e:
                self._enqueue("log", "采集异常: %s" % e)
                self._enqueue("done", None)

        self.worker = threading.Thread(target=worker, daemon=True)
        self.worker.start()

    def _enqueue(self, kind, payload):
        self.msg_queue.put((kind, payload))

    def _stop(self):
        if self.running:
            self.collector.stop_flag.set()
            self._log("已发送停止指令，等待当前请求结束...")
            self._set_status("状态：正在停止...")

    def _poll_queue(self):
        try:
            while True:
                kind, payload = self.msg_queue.get_nowait()
                if kind == "record":
                    self._add_record(payload)
                elif kind == "log":
                    self._log(payload)
                elif kind == "done":
                    self._set_running(False)
                    self._log("采集结束：共采集 %d 条。" % self.success_count)
                    self._set_status("状态：采集完成/已停止")
        except queue.Empty:
            pass
        self.root.after(100, self._poll_queue)

    def _set_running(self, flag):
        self.running = flag

    def _set_status(self, text):
        self.status_var.set(text)


    def _add_record(self, rec):
        self.success_count += 1
        rec["编号"] = self.success_count
        self.records.append(rec)
        self.tree.insert("", "end", values=(
            rec["编号"], rec["标题名称"], rec["手机"], rec["座机"], rec["地址"], rec["城市"],
            rec["关键字"], rec["数据源"], rec["坐标"]))
        self.count_var.set("成功采集条数：%d" % self.success_count)

    def _log(self, text):
        self.status_var.set(text)

    # ---------- 数据操作 ----------
    def _clear(self):
        if self.running:
            messagebox.showwarning("提示", "采集中不能清空，请先停止。")
            return
        self.records.clear()
        self.success_count = 0
        self.tree.delete(*self.tree.get_children())
        self.count_var.set("成功采集条数：0")
        self._set_status("状态：已清空")

    def _export(self):
        if not self.records:
            messagebox.showwarning("提示", "暂无数据可导出。")
            return
        export_dir = self.config["export_dir"] or os.path.dirname(os.path.abspath(__file__))
        default_name = "商家信息_%s.csv" % datetime.now().strftime("%Y%m%d_%H%M%S")
        path = filedialog.asksaveasfilename(
            title="导出数据",
            initialdir=export_dir,
            initialfile=default_name,
            defaultextension=".csv",
            filetypes=[("CSV 文件", "*.csv"), ("全部文件", "*.*")])
        if not path:
            return
        try:
            self._write_csv(path)
            self._log("已导出 %d 条到 %s" % (len(self.records), path))
            self._set_status("状态：导出完成")
            messagebox.showinfo("导出完成", "已导出 %d 条数据到：\n%s" % (len(self.records), path))
        except Exception as e:
            messagebox.showerror("导出失败", str(e))

    def _write_csv(self, path):
        """写 UTF-8 BOM 的 CSV，Excel 可直接打开。"""
        headers = [c[0] for c in COLUMNS]
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for r in self.records:
                writer.writerow({h: r.get(h, "") for h in headers})

    def _open_settings(self):
        SettingsDialog(self.root, self.config)

    # ---------- 其他 ----------
    def _show_help(self):
        text = (
            "使用说明\n"
            "--------\n"
            "1. 申请 Key：\n"
            "   高德：console.amap.com → 应用管理 → 创建应用 → 添加 Key（Web服务）。\n"
            "   百度：lbsyun.baidu.com → 控制台 → 应用管理 → 创建服务端应用，得到 AK。\n"
            "   腾讯：lbs.qq.com → 控制台 → 创建应用，获取 WebService Key。\n"
            "2. 在「参数设置」中填写 Key 并保存。\n"
            "3. 选择地图源、填写城市和关键词（可多行），点「开始采集」。\n"
            "4. 支持数据过滤、去重；完成后点「导出数据」保存为 CSV(Excel 可打开)。\n\n"
            "说明：本工具调用官方 POI 检索接口，免费 Key 有调用配额限制，\n"
            "单关键词单城市采集条数受配额和翻页上限约束。"
        )
        messagebox.showinfo("使用帮助", text)

    def _on_row_double(self, event):
        pass

    def _on_close(self):
        if self.running:
            if not messagebox.askokcancel("退出", "正在采集中，确定要退出吗？"):
                return
            self.collector.stop_flag.set()
        self.root.destroy()


def main():
    root = tk.Tk()
    try:
        style = ttk.Style(root)
        if "vista" in style.theme_names():
            style.theme_use("vista")
    except Exception:
        pass
    MainApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()

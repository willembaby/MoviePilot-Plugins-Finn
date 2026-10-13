# -*- coding: utf-8 -*-
"""
站点已删除种子清理插件 (SiteDeletedTorrentCleaner)

功能：
- 扫描 MoviePilot 已配置下载器 + 自定义下载器中的全部种子
- 依据 tracker 状态（报错消息）判定"站点已删除"的死种（如 torrent not found / unregistered / not registered 等）
- 展示死种列表：单条勾选 / 组内全选 / 一键全选，支持 删除单个 / 删除选中 / 删除全部
- 删除行为：仅删任务，或 删任务+数据文件（带辅种保护——若数据文件仍被其他种子做种则不删数据文件）
- 定时扫描（可配自动删除）+ 页面手动触发，飞书 Webhook 或系统消息通知
"""

from datetime import datetime, timedelta
from typing import Any, List, Dict, Tuple, Optional
from urllib.parse import urlparse

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.helper.downloader import DownloaderHelper
from app.log import logger
from app.plugins import _PluginBase

import requests


class SiteDeletedTorrentCleaner(_PluginBase):
    """
    站点已删除种子清理插件
    """

    plugin_name = "站点已删除种子清理"
    plugin_desc = ("扫描 MoviePilot 与自定义下载器中的死种（依据 tracker 状态判定为站点已删除），"
                   "提供勾选/全选/删除，支持辅种保护删除数据文件，定时与手动触发，飞书或系统通知。")
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/torrent.png"
    plugin_version = "1.3"
    plugin_author = "Finn"
    author_url = "https://github.com"
    plugin_config_prefix = "sitedeletedtorrentcleaner_"
    plugin_order = 33
    auth_level = 1

    # 配置（私有）
    _enabled = False
    _onlyonce = False
    _cron = "0 3 * * *"
    _delete_file = False          # 是否允许删除数据文件（有辅种保护）
    _auto_delete = False          # 定时扫描后是否自动删除
    _notify = True
    _webhook = ""
    _notify_title_prefix = "HA通知"
    _keywords = ("unregistered\nnot found\nnot registered\nnot authorized\n"
                 "unauthorized\nnot exist\n不存在\n已删除\ndeleted\nmissing")
    _custom_downloaders = ""      # 自定义下载器，每行：类型|地址|端口|用户名|密码
    _scheduler = None
    _sel = []                     # 当前勾选的条目 id
    _site_map_cache = None        # 站点域名→站点名 缓存

    # ==================== 生命周期 ====================

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled", False)
            self._onlyonce = config.get("onlyonce", False) or False
            self._cron = config.get("cron") or "0 3 * * *"
            self._delete_file = bool(config.get("delete_file", False))
            self._auto_delete = bool(config.get("auto_delete", False))
            self._notify = bool(config.get("notify", True))
            self._webhook = config.get("webhook", "")
            self._notify_title_prefix = config.get("notify_title_prefix") or "HA通知"
            self._keywords = config.get("keywords") or self._keywords
            self._custom_downloaders = config.get("custom_downloaders") or ""
        self._sel = []

        self.__stop_service()

        if self._enabled:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            try:
                self._scheduler.add_job(
                    func=self._schedule_scan,
                    trigger=CronTrigger.from_crontab(self._cron),
                    name="站点已删除种子清理",
                )
                logger.info(f"站点已删除种子清理定时任务注册成功：cron={self._cron}")
            except Exception as e:
                logger.error(f"站点已删除种子清理定时任务注册失败：{e}")

            if self._onlyonce:
                logger.info("站点已删除种子清理服务启动，立即运行一次")
                self._scheduler.add_job(
                    func=self._schedule_scan,
                    trigger="date",
                    run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=5),
                    name="站点已删除种子清理-立即运行",
                )
                self._onlyonce = False
                self.__update_config()

            if self._scheduler.get_jobs():
                self._scheduler.start()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {"path": "/scan", "endpoint": self.scan_api, "methods": ["GET"], "summary": "扫描全部下载器的死种"},
            {"path": "/toggle", "endpoint": self.toggle_api, "methods": ["GET"], "summary": "切换死种勾选状态"},
            {"path": "/toggle_all", "endpoint": self.toggle_all_api, "methods": ["GET"], "summary": "组内/全部勾选"},
            {"path": "/delete", "endpoint": self.delete_api, "methods": ["GET"], "summary": "删除单个死种"},
            {"path": "/delete_selected", "endpoint": self.delete_selected_api, "methods": ["GET"], "summary": "批量删除勾选死种"},
            {"path": "/delete_all", "endpoint": self.delete_all_api, "methods": ["GET"], "summary": "删除全部/本组死种"},
            {"path": "/clear", "endpoint": self.clear_api, "methods": ["GET"], "summary": "清空扫描结果"},
        ]

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        插件配置页面
        """
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                             'content': [{'component': 'VSwitch', 'props': {'model': 'enabled', 'label': '启用插件'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                             'content': [{'component': 'VSwitch', 'props': {'model': 'notify', 'label': '发送通知'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                             'content': [{'component': 'VSwitch', 'props': {'model': 'onlyonce', 'label': '立即运行一次'}}]},
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6},
                             'content': [{'component': 'VSwitch', 'props': {'model': 'delete_file', 'label': '允许删除数据文件（带辅种保护）'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6},
                             'content': [{'component': 'VSwitch', 'props': {'model': 'auto_delete', 'label': '定时扫描后自动删除'}}]},
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 8},
                             'content': [{'component': 'VTextField', 'props': {'model': 'cron', 'label': '执行周期(Cron)', 'placeholder': '0 3 * * *（每天3点）'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                             'content': [{'component': 'VTextField', 'props': {'model': 'notify_title_prefix', 'label': '通知前缀', 'placeholder': 'HA通知'}}]},
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 12},
                             'content': [{'component': 'VTextField', 'props': {'model': 'webhook', 'label': '飞书 Webhook（留空走系统消息）', 'placeholder': 'https://open.feishu.cn/open-apis/bot/v2/hook/xxx'}}]},
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 12},
                             'content': [{'component': 'VTextarea', 'props': {'model': 'custom_downloaders', 'label': '自定义下载器（每行：类型|地址|端口|用户名|密码，类型 qbittorrent/transmission）', 'rows': 2, 'placeholder': 'qbittorrent|192.168.10.11|8080|admin|pass\ntransmission|192.168.10.20|9091|user|pass'}}]},
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 12},
                             'content': [{'component': 'VTextarea', 'props': {'model': 'keywords', 'label': 'tracker 判定关键词（每行一个，命中即视为站点已删除）', 'rows': 5}}]},
                        ]
                    },
                ]
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "cron": "0 3 * * *",
            "delete_file": False,
            "auto_delete": False,
            "notify": True,
            "webhook": "",
            "notify_title_prefix": "HA通知",
            "keywords": "unregistered\nnot found\nnot registered\nnot authorized\nunauthorized\nnot exist\n不存在\n已删除\ndeleted\nmissing",
            "custom_downloaders": ""
        }

    def get_page(self) -> Optional[List[dict]]:
        if not self._enabled:
            return [{'component': 'VAlert', 'props': {'type': 'warning', 'text': '插件未启用，请先到「设置」页启用', 'variant': 'tonal'}}]

        saved = self.get_data("result") or {}
        items = saved.get("items") or []
        scan_time = saved.get("time", "")
        selected = self._sel
        selected_set = set(selected)

        page = []
        page.append({
            'component': 'VAlert',
            'props': {'type': 'info', 'variant': 'tonal', 'density': 'compact', 'class': 'mb-2',
                      'text': f"扫描时间：{scan_time or '尚未扫描'}   死种 {len(items)} 项   已勾选 {len(selected)} 项"}
        })

        page.append({
            'component': 'VRow',
            'props': {'class': 'mb-1'},
            'content': [
                {'component': 'VCol', 'props': {'cols': 12, 'md': 3},
                 'content': [self._btn("扫描死种", 'plugin/SiteDeletedTorrentCleaner/scan', {}, 'primary', 'elevated')]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 3},
                 'content': [self._btn(f"删除选中（{len(selected)}）", 'plugin/SiteDeletedTorrentCleaner/delete_selected',
                                       {'confirm': 'yes'}, 'error', 'elevated')]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 3},
                 'content': [self._btn("删除全部死种", 'plugin/SiteDeletedTorrentCleaner/delete_all',
                                       {'confirm': 'yes'}, 'error', 'text')]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 3},
                 'content': [self._btn("清空结果", 'plugin/SiteDeletedTorrentCleaner/clear',
                                       {'confirm': 'yes'}, 'secondary', 'text')]},
            ]
        })

        page.append({
            'component': 'div',
            'props': {'class': 'text-caption text-grey mb-2'},
            'text': "依据 tracker 报错判定站点已删除；勾选后点「删除选中」。允许删数据文件时带辅种保护：若数据文件仍被其他种子做种则不删数据文件。"
        })

        if not items:
            page.append({'component': 'div', 'text': '暂无结果，请点击「扫描死种」', 'props': {'class': 'text-center text-grey mt-6'}})
        else:
            groups = {}
            for it in items:
                d = it.get("downloader") or "未知下载器"
                groups.setdefault(d, []).append(it)
            for d in sorted(groups.keys()):
                g_items = groups[d]
                sel_in_group = sum(1 for it in g_items if it.get("id") in selected_set)
                all_selected = len(g_items) > 0 and sel_in_group == len(g_items)
                page.append({
                    'component': 'VRow',
                    'props': {'class': 'ma-0 mt-3 mb-1 py-2 px-2 d-flex align-center',
                              'style': 'background: rgba(2,136,209,0.10); border-radius: 8px;'},
                    'content': [
                        {'component': 'VCol', 'props': {'cols': 12, 'md': 1},
                         'content': [{'component': 'VCheckboxBtn',
                                       'props': {'model-value': all_selected, 'density': 'compact'},
                                       'events': {'click': {'api': 'plugin/SiteDeletedTorrentCleaner/toggle_all',
                                                           'method': 'get',
                                                           'params': {'dl': d, 'apikey': settings.API_TOKEN}}}}]},
                        {'component': 'VCol', 'props': {'cols': 12, 'md': 7},
                         'content': [{'component': 'div', 'props': {'class': 'text-subtitle-2 font-weight-bold text-primary'}, 'text': f"⬇ {d}"},
                                      {'component': 'div', 'props': {'class': 'text-caption text-grey'}, 'text': f"{len(g_items)} 项 · 已勾选 {sel_in_group}"}]},
                        {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                         'content': [self._btn("删除本组全部", 'plugin/SiteDeletedTorrentCleaner/delete_all',
                                               {'dl': d, 'confirm': 'yes'}, 'error', 'tonal')]},
                    ]
                })
                for it in sorted(g_items, key=lambda x: x.get("name", "")):
                    idx = it.get("id", "")
                    checked = idx in selected_set
                    row_style = 'background: rgba(76,175,80,0.12); border-radius: 6px;' if checked else ''
                    page.append({
                        'component': 'VRow',
                        'props': {'class': 'ma-0 py-1 px-2 d-flex align-center', 'style': row_style},
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 1},
                             'content': [{'component': 'VCheckboxBtn', 'props': {'model-value': checked, 'density': 'compact'},
                                           'events': {'click': {'api': 'plugin/SiteDeletedTorrentCleaner/toggle',
                                                               'method': 'get',
                                                               'params': {'id': idx, 'apikey': settings.API_TOKEN}}}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6},
                             'content': [
                                 {'component': 'div', 'props': {'class': 'text-body-2'}, 'text': it.get("name", "")},
                                 {'component': 'div', 'props': {'class': 'text-caption text-grey'},
                                  'text': f"{self._fmt_size(it.get('size', 0))} · 进度 {self._fmt_progress(it.get('progress', 0))}"},
                             ]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 2},
                             'content': [
                                 {'component': 'div', 'props': {'class': 'text-caption text-grey'}, 'text': '站点'},
                                 {'component': 'div', 'props': {'class': 'text-body-2 text-primary font-weight-medium'},
                                  'text': it.get("site") or '—'},
                             ]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 3},
                             'content': [
                                 {'component': 'div', 'props': {'class': 'text-caption text-error text-wrap'},
                                  'text': it.get("tracker_status", "") or ""},
                             ]},
                        ]
                    })
        return page

    def stop_service(self):
        self.__stop_service()

    def __stop_service(self):
        if self._scheduler:
            self._scheduler.remove_all_jobs()
            if self._scheduler.running:
                try:
                    self._scheduler.shutdown()
                except Exception as e:
                    logger.warning(f"站点已删除种子清理：停止调度器异常：{e}")
            self._scheduler = None

    def __update_config(self):
        self.update_config({
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "delete_file": self._delete_file,
            "auto_delete": self._auto_delete,
            "notify": self._notify,
            "webhook": self._webhook,
            "notify_title_prefix": self._notify_title_prefix,
            "keywords": self._keywords,
            "custom_downloaders": self._custom_downloaders,
        })

    # ==================== 工具 ====================

    @staticmethod
    def _btn(text: str, api: str, payload: dict, color: str, variant: str = "tonal") -> Dict:
        params = dict(payload)
        params['apikey'] = settings.API_TOKEN
        return {
            "component": "VBtn",
            "props": {"color": color, "variant": variant, "size": "small", "block": True},
            "text": text,
            "events": {"click": {"api": api, "method": "get", "params": params}},
        }

    @staticmethod
    def _fmt_size(size) -> str:
        try:
            size = float(size or 0)
            if size >= 1024 ** 3:
                return f"{size / 1024 ** 3:.2f} GB"
            if size >= 1024 ** 2:
                return f"{size / 1024 ** 2:.1f} MB"
            if size >= 1024:
                return f"{size / 1024:.0f} KB"
            return f"{int(size)} B"
        except Exception:
            return "0 B"

    @staticmethod
    def _fmt_progress(p) -> str:
        try:
            return f"{float(p or 0) * 100:.0f}%"
        except Exception:
            return "0%"

    @staticmethod
    def _host_of(url: str) -> str:
        if not url:
            return ""
        try:
            return urlparse(url).hostname or ""
        except Exception:
            return ""

    def _site_name_map(self) -> Dict[str, str]:
        """站点主域名 -> 站点名（来自 MoviePilot 站点管理，带缓存）"""
        if self._site_map_cache is not None:
            return self._site_map_cache
        m = {}
        try:
            from app.db.site_oper import SiteOper
            for site in (SiteOper().list() or []):
                name = str(getattr(site, "name", "") or "").strip()
                domain = str(getattr(site, "domain", "") or "").strip().lower()
                if name and domain:
                    m[domain] = name
        except Exception as e:
            logger.warning(f"站点已删除种子清理：读取站点列表失败：{e}")
        self._site_map_cache = m
        return m

    def _match_site_name(self, tracker_url: str) -> str:
        """tracker URL -> 站点名（匹配站点主域名；匹配不到返回 hostname）"""
        if not tracker_url:
            return ""
        host = self._host_of(tracker_url)
        if not host:
            return ""
        m = self._site_name_map()
        if not m:
            return host
        low = host.lower()
        if low in m:
            return m[low]
        for d, n in m.items():
            if low.endswith("." + d):
                return n
        return host

    def _keyword_list(self) -> List[str]:
        return [k.strip() for k in (self._keywords or "").splitlines() if k.strip()]

    def _is_dead(self, text: str) -> bool:
        if not text:
            return False
        low = text.lower()
        for kw in self._keyword_list():
            if kw and kw.lower() in low:
                return True
        return False

    # ==================== 下载器遍历与种子信息 ====================

    def _parse_custom(self) -> List[Tuple[str, str, str, str, str]]:
        out = []
        for line in (self._custom_downloaders or "").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split("|")]
            if len(parts) < 3:
                continue
            out.append((parts[0].lower(), parts[1], parts[2],
                        parts[3] if len(parts) > 3 else "",
                        parts[4] if len(parts) > 4 else ""))
        return out

    @staticmethod
    def _build_custom(dtype: str, host: str, port: str, user: str, pwd: str):
        try:
            if "qb" in dtype:
                from app.modules.qbittorrent.qbittorrent import Qbittorrent
                return Qbittorrent(host=host, port=int(port) if port else None,
                                   username=user or None, password=pwd or None)
            if "transmission" in dtype or dtype == "tr":
                from app.modules.transmission.transmission import Transmission
                return Transmission(host=host, port=int(port) if port else None,
                                    username=user or None, password=pwd or None)
        except Exception as e:
            logger.error(f"站点已删除种子清理：构建自定义下载器失败 {dtype} {host}:{port}：{e}")
        return None

    def _iter_downloaders(self):
        try:
            services = DownloaderHelper().get_services() or {}
            for name, info in services.items():
                inst = getattr(info, "instance", None)
                dtype = str(getattr(info, "type", "") or "").lower()
                if not inst or not dtype:
                    continue
                yield name, dtype, inst
        except Exception as e:
            logger.error(f"站点已删除种子清理：获取 MoviePilot 下载器失败：{e}")
        for idx, cfg in enumerate(self._parse_custom()):
            dtype, host, port, user, pwd = cfg
            inst = self._build_custom(dtype, host, port, user, pwd)
            if inst:
                yield f"custom:{idx}", dtype, inst

    def _torrent_info(self, dtype: str, t: Any) -> Optional[Dict]:
        """从下载器种子提取统一信息字典"""
        dtype = dtype or ""
        if "qb" in dtype or "rtorrent" in dtype:
            try:
                tget = t.get if hasattr(t, "get") else (lambda k, d=None: getattr(t, k, d))
                t_hash = str(tget("hash", "") or "")
                if not t_hash:
                    return None
                t_name = str(tget("name", "") or "")
                tracker = str(tget("tracker", "") or "")
                return {
                    "hash": t_hash,
                    "name": t_name,
                    "size": tget("size", 0) or 0,
                    "progress": tget("progress", 0) or 0,
                    "tracker": tracker,
                    "site": self._host_of(tracker),
                    "data_id": str(tget("content_path", "") or "") \
                        or (str(tget("save_path", "") or "") + "/" + t_name),
                }
            except Exception:
                return None
        if "transmission" in dtype:
            try:
                t_hash = str(getattr(t, "hashString", "") or getattr(t, "hash", "") or "")
                if not t_hash:
                    return None
                t_name = str(getattr(t, "name", "") or "")
                tlist = getattr(t, "tracker_list", None) or getattr(t, "trackerList", None) or []
                tracker = tlist[0] if isinstance(tlist, (list, tuple)) and tlist else ""
                return {
                    "hash": t_hash,
                    "name": t_name,
                    "size": int(getattr(t, "sizeTotal", 0) or 0),
                    "progress": float(getattr(t, "percentDone", 0) or 0),
                    "tracker": tracker,
                    "site": self._host_of(tracker),
                    "data_id": str(getattr(t, "downloadDir", "") or "") + "/" + t_name,
                }
            except Exception:
                return None
        return None

    def _tracker_status(self, dtype: str, inst: Any, hash_string: str, t: Any) -> str:
        """获取种子 tracker 报错/状态消息"""
        dtype = dtype or ""
        if "qb" in dtype:
            try:
                qbc = getattr(inst, "qbc", None)
                if not qbc:
                    return ""
                trs = qbc.torrents_trackers(torrent_hash=hash_string) or []
                msgs = []
                for x in trs:
                    if isinstance(x, dict) and x.get("msg"):
                        msgs.append(str(x["msg"]))
                    elif hasattr(x, "msg") and getattr(x, "msg"):
                        msgs.append(str(getattr(x, "msg")))
                return " | ".join(msgs)
            except Exception as e:
                logger.warning(f"站点已删除种子清理：获取 qBittorrent tracker 状态失败：{e}")
                return ""
        if "transmission" in dtype:
            try:
                stats = getattr(t, "trackerStats", None) or []
                results = []
                for ts in stats:
                    r = ts.get("lastAnnounceResult") if isinstance(ts, dict) \
                        else getattr(ts, "lastAnnounceResult", None)
                    if r:
                        results.append(str(r))
                return " | ".join(results)
            except Exception as e:
                logger.warning(f"站点已删除种子清理：获取 transmission tracker 状态失败：{e}")
                return ""
        return ""

    def _qb_tracker_info(self, inst: Any, hash_string: str) -> Tuple[str, str]:
        """
        qBittorrent：一次 RPC 同时拿 tracker 报错消息与真实 announce URL。
        磁力链接种子的 torrents 字段 tracker 为空，必须从 trackers 列表取；
        过滤 DHT/PEX/LSD 伪 tracker（** [DHT] ** 等）。
        返回 (报错消息拼接, 真实 announce URL)
        """
        try:
            qbc = getattr(inst, "qbc", None)
            if not qbc:
                return "", ""
            trs = qbc.torrents_trackers(torrent_hash=hash_string) or []
            msgs = []
            real_url = ""
            for x in trs:
                if not isinstance(x, dict):
                    continue
                url = str(x.get("url") or "")
                if url.startswith("**"):
                    continue
                msg = str(x.get("msg") or "")
                if msg:
                    msgs.append(msg)
                if url and not real_url:
                    real_url = url
            return " | ".join(msgs), real_url
        except Exception as e:
            logger.warning(f"站点已删除种子清理：获取 qBittorrent tracker 信息失败：{e}")
            return "", ""

    def _tr_tracker_map(self, inst: Any) -> Dict[str, Dict[str, str]]:
        """
        Transmission 专用：transmission-rpc 库取不到 trackerStats，
        改用直接 RPC 一次性获取全部种子的 tracker 报错信息与 announce 地址。
        返回 {hash: {"status": "lastAnnounceResult 拼接", "tracker": "首个 announce URL"}}
        """
        host = getattr(inst, "_host", None)
        if not host:
            return {}
        proto = getattr(inst, "_protocol", "http") or "http"
        port = getattr(inst, "_port", None)
        user = getattr(inst, "_username", "") or ""
        pwd = getattr(inst, "_password", "") or ""
        url = f"{proto}://{host}:{port}/transmission/rpc" if port \
            else f"{proto}://{host}/transmission/rpc"
        out = {}
        try:
            sess = requests.Session()
            r1 = sess.post(url, data={}, auth=(user, pwd), timeout=20)
            sid = r1.headers.get("X-Transmission-Session-Id")
            if not sid:
                return out
            r = sess.post(url, headers={"X-Transmission-Session-Id": sid}, auth=(user, pwd),
                          json={"method": "torrent-get",
                                "arguments": {"fields": ["hashString", "name", "trackerStats", "trackers"]}},
                          timeout=40)
            for t in r.json().get("arguments", {}).get("torrents", []):
                h = t.get("hashString")
                if not h:
                    continue
                msgs = [str(s.get("lastAnnounceResult") or "")
                        for s in (t.get("trackerStats") or []) if isinstance(s, dict)]
                announces = [str(s.get("announce") or "")
                             for s in (t.get("trackers") or []) if isinstance(s, dict)]
                out[h] = {
                    "status": " | ".join(x for x in msgs if x),
                    "tracker": announces[0] if announces else "",
                }
        except Exception as e:
            logger.warning(f"站点已删除种子清理：TR RPC 获取 tracker 状态失败：{e}")
        return out

    # ==================== 扫描 ====================

    def _scan_all(self) -> Tuple[List[Dict], Dict[str, Dict[str, str]]]:
        """
        扫描全部下载器，返回 (死种条目列表, {下载器: {hash: data_id}})
        """
        dead_items = []
        all_data = {}
        for name, dtype, inst in self._iter_downloaders():
            try:
                torrents, err = inst.get_torrents()
            except Exception as e:
                logger.error(f"站点已删除种子清理：获取下载器 {name} 种子失败：{e}")
                continue
            if err or not torrents:
                logger.info(f"站点已删除种子清理：{name} 无种子或获取异常")
                continue
            logger.info(f"站点已删除种子清理：{name} 共 {len(torrents)} 个种子待检查")
            all_data.setdefault(name, {})
            # Transmission：transmission-rpc 取不到 trackerStats，直接 RPC 批量获取
            tr_map = self._tr_tracker_map(inst) if "transmission" in dtype else None
            for t in torrents:
                info = self._torrent_info(dtype, t)
                if not info or not info.get("hash"):
                    continue
                all_data[name][info["hash"]] = info.get("data_id", "")
                if tr_map is not None:
                    # TR：status 与 announce URL 均来自直接 RPC
                    tri = tr_map.get(info["hash"]) or {}
                    ts = tri.get("status", "") or ""
                    tracker_url = tri.get("tracker", "") or ""
                else:
                    # qB：一次 RPC 拿报错 + 真实 announce URL（磁力种子字段为空）
                    ts, qb_url = self._qb_tracker_info(inst, info["hash"])
                    tracker_url = qb_url or info.get("tracker", "") or ""
                if not ts or not self._is_dead(ts):
                    continue
                site_name = self._match_site_name(tracker_url) or ""
                dead_items.append({
                    "id": f"{name}|{info['hash']}",
                    "downloader": name,
                    "downloader_type": dtype,
                    "hash": info["hash"],
                    "name": info["name"],
                    "size": info["size"],
                    "progress": info["progress"],
                    "site": site_name,
                    "tracker": tracker_url,
                    "tracker_status": ts,
                    "data_id": info["data_id"],
                })
        return dead_items, all_data

    def _schedule_scan(self):
        """定时任务入口"""
        dead_items, _ = self._scan_all()
        if not self._auto_delete:
            self.save_data("result", {"time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                      "items": dead_items, "total": len(dead_items)})
            logger.info(f"站点已删除种子清理：定时扫描完成，发现 {len(dead_items)} 个死种")
            if self._notify and dead_items:
                self._send_notification(f"{self._notify_title_prefix}: 站点已删除种子清理-扫描发现 {len(dead_items)} 个死种\n"
                                        + "\n".join(f"  ▶ {x.get('name')}" for x in dead_items[:20]))
            return
        if dead_items:
            ok, _ = self._delete_entries(dead_items)
            logger.info(f"站点已删除种子清理：自动删除完成，成功 {ok}/{len(dead_items)}")
            if self._notify:
                self._send_notification(f"{self._notify_title_prefix}: 站点已删除种子清理-自动删除 {ok}/{len(dead_items)} 个死种")
        else:
            logger.info("站点已删除种子清理：定时扫描完成，未发现死种")

    def scan_api(self) -> Dict[str, Any]:
        """手动扫描"""
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        logger.info("站点已删除种子清理：开始扫描下载器死种...")
        dead_items, _ = self._scan_all()
        self.save_data("result", {"time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                  "items": dead_items, "total": len(dead_items)})
        self._sel = []
        logger.info(f"站点已删除种子清理：扫描完成，共 {len(dead_items)} 个死种")
        if self._notify and dead_items:
            self._send_notification(f"{self._notify_title_prefix}: 站点已删除种子清理-扫描完成\n"
                                    f"发现 {len(dead_items)} 个死种，请到插件页面查看/删除")
        return {"success": True, "total": len(dead_items), "items": dead_items}

    # ==================== 勾选 ====================

    def toggle_api(self, id: str) -> Dict[str, Any]:
        if not id:
            return {"success": False, "message": "未指定条目"}
        selected = self._sel
        if id in selected:
            selected.remove(id)
            msg = "已取消勾选"
        else:
            selected.append(id)
            msg = "已勾选"
        self._sel = selected
        return {"success": True, "message": msg, "selected_count": len(selected)}

    def toggle_all_api(self, dl: str = "") -> Dict[str, Any]:
        """组内/全部一键勾选（dl 为空则全部）"""
        saved = self.get_data("result") or {}
        items = saved.get("items") or []
        if dl:
            ids = [it.get("id") for it in items if it.get("downloader") == dl]
        else:
            ids = [it.get("id") for it in items]
        if not ids:
            return {"success": False, "message": "当前无结果"}
        selected = self._sel
        id_set = set(ids)
        if all(i in selected for i in ids):
            selected = [p for p in selected if p not in id_set]
            msg = f"已取消勾选 {len(ids)} 项"
        else:
            for i in ids:
                if i not in selected:
                    selected.append(i)
            msg = f"已勾选 {len(ids)} 项"
        self._sel = selected
        return {"success": True, "message": msg, "selected_count": len(selected)}

    # ==================== 删除（带辅种保护） ====================

    def _get_downloader(self, dl: str):
        if dl.startswith("custom:"):
            try:
                idx = int(dl.split(":")[1])
            except Exception:
                return None, ""
            custom = self._parse_custom()
            if idx >= len(custom):
                return None, ""
            dtype, host, port, user, pwd = custom[idx]
            return self._build_custom(dtype, host, port, user, pwd), dtype
        try:
            services = DownloaderHelper().get_services() or {}
            info = services.get(dl)
            if info and getattr(info, "instance", None):
                return info.instance, str(getattr(info, "type", "") or "").lower()
        except Exception as e:
            logger.error(f"站点已删除种子清理：获取下载器 {dl} 失败：{e}")
        return None, ""

    def _collect_data_ids(self, dtype: str, inst) -> Dict[str, str]:
        """收集下载器内全部种子的数据标识（用于辅种判断）"""
        out = {}
        try:
            torrents, err = inst.get_torrents()
            if err or not torrents:
                return out
            for t in torrents:
                info = self._torrent_info(dtype, t)
                if info and info.get("hash"):
                    out[info["hash"]] = info.get("data_id", "")
        except Exception:
            pass
        return out

    def _delete_entries(self, entries: List[Dict]) -> Tuple[int, List[str]]:
        """
        按下载器分组删除，带辅种保护。
        返回 (成功数, 保留文件提示列表)
        """
        by_dl = {}
        for e in entries:
            dl = e.get("downloader")
            if dl:
                by_dl.setdefault(dl, []).append(e)
        ok = 0
        kept = []
        for dl, items in by_dl.items():
            inst, dtype = self._get_downloader(dl)
            if not inst:
                logger.error(f"站点已删除种子清理：下载器 {dl} 不可用，跳过")
                continue
            all_ids = self._collect_data_ids(dtype, inst)
            groups = {False: [], True: []}
            for it in items:
                h = it.get("hash")
                if not h:
                    continue
                data_id = it.get("data_id", "")
                # 是否被其他种子辅种引用（数据根相同）
                referenced = any(
                    oh != h and od and od == data_id
                    for oh, od in all_ids.items()
                )
                if referenced:
                    # 数据文件仍被其他种子做种 → 保留文件，仅删任务
                    kept.append(it.get("name"))
                    groups[False].append(h)
                else:
                    # 无辅种 → 是否删数据文件由配置决定
                    groups[bool(self._delete_file)].append(h)
            for delete_file, hashes in groups.items():
                if not hashes:
                    continue
                try:
                    if inst.delete_torrents(delete_file=delete_file, ids=hashes):
                        ok += len(hashes)
                    else:
                        logger.error(f"站点已删除种子清理：{dl} 删除失败（delete_file={delete_file}）")
                except Exception as e:
                    logger.error(f"站点已删除种子清理：{dl} 删除种子异常：{e}")
        return ok, kept

    def delete_api(self, id: str, confirm: str = "") -> Dict[str, Any]:
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        if confirm != "yes":
            return {"success": False, "message": "请确认参数 confirm=yes"}
        saved = self.get_data("result") or {}
        items = saved.get("items") or []
        target = next((it for it in items if it.get("id") == id), None)
        if not target:
            return {"success": False, "message": "条目不存在"}
        ok, _ = self._delete_entries([target])
        return {"success": ok > 0, "message": f"已删除 {ok} 个" if ok else "删除失败"}

    def delete_selected_api(self, confirm: str = "") -> Dict[str, Any]:
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        if confirm != "yes":
            return {"success": False, "message": "请确认参数 confirm=yes"}
        selected = self._sel
        if not selected:
            return {"success": False, "message": "未勾选任何死种"}
        saved = self.get_data("result") or {}
        items = saved.get("items") or []
        targets = [it for it in items if it.get("id") in selected]
        if not targets:
            return {"success": False, "message": "勾选的条目不在结果中，请重新扫描"}
        ok, _ = self._delete_entries(targets)
        self._sel = []
        saved["items"] = [it for it in items if it.get("id") not in selected]
        saved["total"] = len(saved["items"])
        self.save_data("result", saved)
        logger.info(f"站点已删除种子清理：批量删除勾选完成 {ok}/{len(targets)}")
        if self._notify and ok:
            self._send_notification(f"{self._notify_title_prefix}: 站点已删除种子清理-清理勾选 {ok}/{len(targets)} 个死种")
        return {"success": True, "ok": ok, "total": len(targets)}

    def delete_all_api(self, dl: str = "", confirm: str = "") -> Dict[str, Any]:
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        if confirm != "yes":
            return {"success": False, "message": "请确认参数 confirm=yes"}
        saved = self.get_data("result") or {}
        items = saved.get("items") or []
        targets = items if not dl else [it for it in items if it.get("downloader") == dl]
        if not targets:
            return {"success": False, "message": "当前无结果"}
        ok, _ = self._delete_entries(targets)
        del_ids = {it.get("id") for it in targets}
        saved["items"] = [it for it in saved.get("items", []) if it.get("id") not in del_ids]
        saved["total"] = len(saved["items"])
        self.save_data("result", saved)
        self._sel = [s for s in self._sel if s not in del_ids]
        logger.info(f"站点已删除种子清理：删除完成 成功 {ok}/{len(targets)}")
        if self._notify and ok:
            self._send_notification(f"{self._notify_title_prefix}: 站点已删除种子清理-删除 {ok}/{len(targets)} 个死种")
        return {"success": True, "ok": ok, "total": len(targets)}

    def clear_api(self, confirm: str = "") -> Dict[str, Any]:
        if confirm != "yes":
            return {"success": False, "message": "请确认参数 confirm=yes"}
        self.save_data("result", {"time": "", "items": [], "total": 0})
        self._sel = []
        return {"success": True}

    # ==================== 通知 ====================

    def _send_notification(self, text: str):
        if self._webhook:
            try:
                resp = requests.post(self._webhook,
                                     json={"msg_type": "text", "content": {"text": text}}, timeout=15)
                if resp.status_code == 200 and resp.json().get("code") == 0:
                    logger.info("站点已删除种子清理飞书通知发送成功")
                    return
                else:
                    logger.error(f"站点已删除种子清理飞书通知发送失败：{resp.text[:200]}")
            except Exception as e:
                logger.error(f"站点已删除种子清理飞书通知发送异常：{e}")
        self.post_message(title="站点已删除种子清理", text=text)
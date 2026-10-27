# -*- coding: utf-8 -*-
"""
订阅剧集自动清理插件 (SubscribeEpisodeCleaner)

功能：
- 针对 MoviePilot「指定的多个订阅电视剧」（可多选）定期（可设 Cron）清理已下载的剧集
- 只保留最新 N 集（按集号最大），更早的剧集连同下载器数据一起删除（删数据文件 + 退出做种）
- 删除的剧集号写入订阅 note（已下载集 JSON 列表），MoviePilot 搜索/刷新时将其视为
  「已下载过」而从缺失集中剔除，不会通过订阅再次重新下载
- 删除时同步清理媒体库硬链接文件与转移记录（带共享引用/链接数双重保护，复用跨站清理模式）
- 删除前回收站备份（支持误删恢复）、含未完成种子跳过、按下载器分批删除
- 定时自动执行 + 页面手动触发，飞书 Webhook 或系统消息通知
"""

import json
import os
import re
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.helper.downloader import DownloaderHelper
from app.log import logger
from app.plugins import _PluginBase

# qBittorrent 视为"未完成/进行中"的状态
_QB_ACTIVE_STATES = {
    "downloading", "stalledDL", "metaDL", "checkingDL", "forcedDL",
    "allocating", "checkingResumeData", "moving", "forcedMetaDL",
}


def norm_text(s) -> str:
    """归一化用于名称匹配：去掉标点/空白，保留中文与字母数字"""
    if s is None:
        return ""
    return re.sub(r"[\W_]+", "", str(s)).lower()


def parse_episodes(name):
    """
    解析剧集标题中的季与集号。
    返回 (seasons: set[int], episodes: list[int] 升序)。
    episodes 为空表示无法解析出明确集号（整季/整包种子），调用方应保守保留。
    支持形态：S01E05 / E05 / 第05集 / 第05话 / S01E01-E05 / 第01-10集
    """
    n = str(name or "")
    seasons = set()
    for m in re.finditer(r"第\s*(\d{1,2})\s*季", n):
        seasons.add(int(m.group(1)))
    for m in re.finditer(r"(?i)(?:^|\D)s(\d{1,2})(?:\D|$)", n):
        seasons.add(int(m.group(1)))
    eps = set()
    # 单集 S01E05 / E05（限制 E 后 1~3 位数字，避免误匹配年份/码率）
    for m in re.finditer(r"(?i)(?:s\d{1,2})?[e_\s]\s*(\d{1,3})\b", n):
        eps.add(int(m.group(1)))
    # 中文 第X集/话
    for m in re.finditer(r"第\s*(\d{1,3})\s*[集话]", n):
        eps.add(int(m.group(1)))
    # 范围 第X-Y集
    for m in re.finditer(r"第\s*(\d{1,3})\s*-\s*(\d{1,3})\s*[集话]", n):
        lo, hi = int(m.group(1)), int(m.group(2))
        eps.update(range(lo, min(hi + 1, lo + 200)))
    # 范围 S01E01-E05 / S01E01-05
    for m in re.finditer(r"(?i)s(\d{1,2})e(\d{1,3})\s*-\s*e?(\d{1,3})", n):
        lo, hi = int(m.group(2)), int(m.group(3))
        if lo and hi:
            eps.update(range(lo, min(hi + 1, lo + 30)))
    return seasons, sorted(eps)


def compute_keep(downloaded_eps: List[int], keep: int) -> Tuple[int, List[int], List[int]]:
    """
    计算保留窗口。
    downloaded_eps: 已下载的去重集号（升序）
    keep: 保留最新几集
    返回 (threshold, keep_list, drop_list)
    - threshold: 保留窗口最小集号（>=threshold 视为最新保留）
    - keep_list: 实际保留的集号
    - drop_list: 需从 1..max 中剔除（不再保留、不再重下）的集号
    """
    distinct = sorted(set(int(x) for x in (downloaded_eps or [])))
    if not distinct:
        return 0, [], []
    max_ep = max(distinct)
    keep_n = int(keep or 0)
    if keep_n <= 0:
        # 不保留任何集：全部剔除
        return 1, [], list(range(1, max_ep + 1))
    keep_list = distinct[-keep_n:]
    threshold = keep_list[0]
    drop_list = [e for e in range(1, max_ep + 1) if e < threshold]
    return threshold, keep_list, drop_list


class SubscribeEpisodeCleaner(_PluginBase):
    """
    订阅剧集自动清理
    """

    plugin_name = "订阅剧集自动清理"
    plugin_desc = ("指定 MoviePilot 多个电视剧订阅（多选），按设定的 Cron 定期清理各订阅已下载剧集、只保留最新 N 集（各订阅独立计算）；"
                   "删除时退出做种并清理数据文件，同步清理媒体库硬链接与转移记录；"
                   "删除的集号写入各订阅 note（已下载集），MoviePilot 搜索/刷新将其视为已下载，"
                   "不会通过订阅再次重新下载；含未完成保护、回收站备份恢复、通知。")
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/tv.png"
    plugin_version = "1.1"
    plugin_author = "Finn"
    author_url = "https://github.com"
    plugin_config_prefix = "subscribeepisodecleaner_"
    plugin_order = 37
    auth_level = 1

    # 配置（私有）
    _enabled = False
    _onlyonce = False
    _cron = "0 3 * * *"
    _subscribe_ids = []         # 指定订阅 ID 列表（多选）
    _keep_episodes = 5           # 保留最新几集
    _season = ""                 # 覆盖季（留空用订阅自带季）
    _match_keywords = ""         # 额外匹配词（每行一个，种子名命中任一即归属该订阅）
    _delete_file = True          # 删除数据文件 + 退出做种（本插件核心行为）
    _clean_media_links = True    # 同步清理媒体库硬链接与转移记录
    _recycle_enabled = True      # 删除前备份种子到回收站
    _skip_active = True          # 含未完成（下载/校验中）的种子跳过
    _auto = True                 # 定时执行后自动清理（否则仅分析通知）
    _notify = True
    _webhook = ""
    _notify_title_prefix = "HA通知"
    _scheduler = None
    _custom_downloaders = ""

    # ==================== 生命周期 ====================

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled", False)
            self._onlyonce = config.get("onlyonce", False) or False
            self._cron = config.get("cron") or "0 3 * * *"
            # 订阅多选（兼容旧版单选字符串自动迁移为列表）
            raw_ids = config.get("subscribe_ids")
            if raw_ids is None:
                old = str(config.get("subscribe_id") or "").strip()
                raw_ids = [old] if old else []
            if isinstance(raw_ids, str):
                raw_ids = [raw_ids] if raw_ids.strip() else []
            self._subscribe_ids = [str(x).strip() for x in (raw_ids or []) if str(x).strip()]
            try:
                self._keep_episodes = max(int(config.get("keep_episodes") or 5), 1)
            except Exception:
                self._keep_episodes = 5
            self._season = str(config.get("season") or "").strip()
            self._match_keywords = config.get("match_keywords", "") or ""
            self._delete_file = bool(config.get("delete_file", True))
            self._clean_media_links = bool(config.get("clean_media_links", True))
            self._recycle_enabled = bool(config.get("recycle_enabled", True))
            self._skip_active = bool(config.get("skip_active", True))
            self._auto = bool(config.get("auto", True))
            self._notify = bool(config.get("notify", True))
            self._webhook = config.get("webhook", "")
            self._notify_title_prefix = config.get("notify_title_prefix") or "HA通知"
            self._custom_downloaders = config.get("custom_downloaders") or ""

        self.__stop_service()

        if self._enabled:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            try:
                self._scheduler.add_job(
                    func=self._schedule_run,
                    trigger=CronTrigger.from_crontab(self._cron),
                    name="订阅剧集自动清理",
                )
                logger.info(f"订阅剧集自动清理定时任务注册成功：cron={self._cron}")
            except Exception as e:
                logger.error(f"订阅剧集自动清理定时任务注册失败：{e}")

            if self._onlyonce:
                logger.info("订阅剧集自动清理服务启动，立即运行一次")
                self._scheduler.add_job(
                    func=self._schedule_run,
                    trigger="date",
                    run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=5),
                    name="订阅剧集自动清理-立即运行",
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
            {"path": "/scan", "endpoint": self.scan_api, "methods": ["GET"], "summary": "扫描分析应清理的旧集"},
            {"path": "/run", "endpoint": self.run_api, "methods": ["GET"], "summary": "执行清理（删数据+退出做种+写note防重下）"},
            {"path": "/clear", "endpoint": self.clear_api, "methods": ["GET"], "summary": "清空分析结果"},
        ]

    # ==================== 表单 ====================

    def _subscribe_list(self) -> List[Dict[str, Any]]:
        """读 MoviePilot 全部电视剧订阅，供下拉选择"""
        try:
            from app.db.subscribe_oper import SubscribeOper
            items = []
            for s in (SubscribeOper().list() or []):
                # 兼容不同版本 type 字段：tv / 电视剧 / T
                t = str(getattr(s, "type", "") or "").lower()
                if "剧" not in t and t not in ("tv", "电视剧", "t"):
                    continue
                name = str(getattr(s, "name", "") or "")
                year = str(getattr(s, "year", "") or "")
                season = str(getattr(s, "season", "") or "")
                lack = getattr(s, "lack_episode", 0) or 0
                sid = getattr(s, "id", None)
                label = f"{name}（{year}）第{season}季 · 缺失{lack}集 · ID{sid}"
                items.append({"title": label, "value": str(sid)})
            return items
        except Exception as e:
            logger.warning(f"订阅剧集自动清理：读取订阅列表失败：{e}")
            return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        sub_items = self._subscribe_list()
        return [
            {
                'component': 'VForm',
                'content': [
                    {'component': 'VRow', 'content': [
                        {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                         'content': [{'component': 'VSwitch', 'props': {'model': 'enabled', 'label': '启用插件'}}]},
                        {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                         'content': [{'component': 'VSwitch', 'props': {'model': 'notify', 'label': '发送通知'}}]},
                        {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                         'content': [{'component': 'VSwitch', 'props': {'model': 'onlyonce', 'label': '立即运行一次'}}]},
                    ]},
                    {'component': 'VRow', 'content': [
                        {'component': 'VCol', 'props': {'cols': 12, 'md': 6},
                         'content': [{'component': 'VSwitch', 'props': {'model': 'auto', 'label': '定时自动清理（不勾选则仅分析通知，需手动执行）'}}]},
                        {'component': 'VCol', 'props': {'cols': 12, 'md': 6},
                         'content': [{'component': 'VSwitch', 'props': {'model': 'delete_file', 'label': '删除数据文件并退出做种（本插件核心行为）'}}]},
                    ]},
                    {'component': 'VRow', 'content': [
                        {'component': 'VCol', 'props': {'cols': 12, 'md': 6},
                         'content': [{'component': 'VSwitch', 'props': {'model': 'clean_media_links', 'label': '同步清理媒体库硬链接与转移记录'}}]},
                        {'component': 'VCol', 'props': {'cols': 12, 'md': 6},
                         'content': [{'component': 'VSwitch', 'props': {'model': 'recycle_enabled', 'label': '删除前备份种子到回收站（支持误删恢复）'}}]},
                    ]},
                    {'component': 'VRow', 'content': [
                        {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                         'content': [{'component': 'VSelect', 'props': {'model': 'subscribe_ids', 'label': '指定订阅（电视剧，可多选）', 'items': sub_items, 'multiple': True, 'chips': True, 'clearable': True}}]},
                        {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                         'content': [{'component': 'VTextField', 'props': {'model': 'keep_episodes', 'label': '保留最新几集', 'placeholder': '5'}}]},
                        {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                         'content': [{'component': 'VTextField', 'props': {'model': 'season', 'label': '季（留空用订阅自带季，如 1）', 'placeholder': ''}}]},
                    ]},
                    {'component': 'VRow', 'content': [
                        {'component': 'VCol', 'props': {'cols': 12, 'md': 6},
                         'content': [{'component': 'VTextField', 'props': {'model': 'cron', 'label': '执行周期(Cron)', 'placeholder': '0 3 * * *（每天3点）'}}]},
                        {'component': 'VCol', 'props': {'cols': 12, 'md': 6},
                         'content': [{'component': 'VTextField', 'props': {'model': 'notify_title_prefix', 'label': '通知前缀', 'placeholder': 'HA通知'}}]},
                    ]},
                    {'component': 'VRow', 'content': [
                        {'component': 'VCol', 'props': {'cols': 12, 'md': 12},
                         'content': [{'component': 'VTextField', 'props': {'model': 'webhook', 'label': '飞书 Webhook（留空走系统消息）', 'placeholder': 'https://open.feishu.cn/open-apis/bot/v2/hook/xxx'}}]},
                    ]},
                    {'component': 'VRow', 'content': [
                        {'component': 'VCol', 'props': {'cols': 12, 'md': 12},
                         'content': [{'component': 'VTextField', 'props': {'model': 'match_keywords', 'label': '额外匹配词（每行一个；订阅名匹配不到的发布名可在此补充，任一命中即视为该订阅资源）', 'rows': 2}}]},
                    ]},
                    {'component': 'VRow', 'content': [
                        {'component': 'VCol', 'props': {'cols': 12, 'md': 12},
                         'content': [{'component': 'VTextarea', 'props': {'model': 'custom_downloaders', 'label': '自定义下载器（每行：类型|地址|端口|用户名|密码，类型 qbittorrent/transmission；留空则仅用 MoviePilot 已配置下载器）', 'rows': 2}}]},
                    ]},
                ]
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "cron": "0 3 * * *",
            "subscribe_ids": [],
            "keep_episodes": 5,
            "season": "",
            "match_keywords": "",
            "delete_file": True,
            "clean_media_links": True,
            "recycle_enabled": True,
            "skip_active": True,
            "auto": True,
            "notify": True,
            "webhook": "",
            "notify_title_prefix": "HA通知",
            "custom_downloaders": "",
        }

    # ==================== 页面 ====================

    def get_page(self) -> Optional[List[dict]]:
        if not self._enabled:
            return [{'component': 'VAlert', 'props': {'type': 'warning', 'text': '插件未启用，请先到「设置」页启用', 'variant': 'tonal'}}]

        saved = self.get_data("result") or {}
        subs_results = saved.get("subs") or []
        scan_time = saved.get("time", "")
        candidates = saved.get("candidates") or []

        page = []
        page.append({
            'component': 'VAlert',
            'props': {'type': 'info', 'variant': 'tonal', 'density': 'compact', 'class': 'mb-2',
                      'text': (f"已选 {len(self._subscribe_ids)} 个订阅 · 分析时间：{scan_time or '尚未分析'}")}
        })
        page.append({
            'component': 'VRow',
            'props': {'class': 'mb-2'},
            'content': [
                {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                 'content': [self._btn("扫描分析", 'plugin/SubscribeEpisodeCleaner/scan', {}, 'primary', 'elevated')]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                 'content': [self._btn(f"执行清理({len(candidates)})", 'plugin/SubscribeEpisodeCleaner/run',
                                       {'confirm': 'yes'}, 'error', 'elevated')]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                 'content': [self._btn("清空结果", 'plugin/SubscribeEpisodeCleaner/clear',
                                       {'confirm': 'yes'}, 'secondary', 'text')]},
            ]
        })

        if not subs_results:
            page.append({'component': 'VAlert',
                         'props': {'type': 'info', 'variant': 'tonal',
                                   'text': '尚未分析，点击「扫描分析」查看各订阅待清理的旧集。'}})
            return page

        for r in subs_results:
            sub = r.get("sub") or {}
            cands = r.get("candidates") or []
            keep_eps = r.get("keep_eps") or []
            drop_eps = r.get("drop_eps") or []
            title = f"《{sub.get('name', '-')}》（{sub.get('year', '-')}）第{sub.get('season', '-')}季"
            if cands:
                page.append({
                    'component': 'VAlert',
                    'props': {'type': 'warning', 'variant': 'tonal', 'density': 'compact', 'class': 'mb-1',
                              'text': (f"{title}：待清理 {len(cands)} 个种子，释放约 {self._fmt_size(r.get('free_size', 0))}；"
                                       f"保留最新 {self._keep_episodes} 集（{self._eps_text(keep_eps) or '无'}），"
                                       f"剔除集 {self._eps_text(drop_eps) or '无'} 将写入订阅不再重下。")}
                })
                for e in cands[:100]:
                    page.append({
                        'component': 'VRow',
                        'props': {'class': 'pa-0 ma-0 ml-2', 'align': 'center', 'dense': True},
                        'content': [
                            {'component': 'VCol', 'props': {'cols': True},
                             'content': [{'component': 'div', 'props': {'class': 'text-caption'},
                                          'text': f"[{e.get('downloader', '')}] {str(e.get('name', ''))[:70]}"
                                                  f"（集 {self._eps_text(e.get('eps', []))}）"}]},
                            {'component': 'VCol', 'props': {'cols': 'auto'},
                             'content': [{'component': 'VChip', 'props': {'size': 'x-small', 'color': 'orange', 'variant': 'tonal'},
                                          'text': self._fmt_size(e.get('size', 0))}]},
                        ]
                    })
            else:
                page.append({'component': 'VAlert',
                             'props': {'type': 'success', 'variant': 'tonal', 'density': 'compact', 'class': 'mb-1',
                                       'text': (f"{title}：无待清理旧集。保留最新 {self._keep_episodes} 集："
                                                f"{self._eps_text(keep_eps) or '暂无'}"
                                                + (f"；已剔除不重下集：{self._eps_text(drop_eps)}" if drop_eps else ""))}})

        return page

    @staticmethod
    def _eps_text(eps: List[int]) -> str:
        if not eps:
            return ""
        s = sorted(set(int(x) for x in eps))
        parts = []
        start = prev = s[0]
        for v in s[1:]:
            if v == prev + 1:
                prev = v
            else:
                parts.append(f"{start}-{prev}" if start != prev else f"{start}")
                start = prev = v
        parts.append(f"{start}-{prev}" if start != prev else f"{start}")
        return ",".join(parts)

    @staticmethod
    def _fmt_size(size) -> str:
        try:
            size = float(size or 0)
            if size >= 1024 ** 4:
                return f"{size / 1024 ** 4:.2f} TB"
            if size >= 1024 ** 3:
                return f"{size / 1024 ** 3:.2f} GB"
            if size >= 1024 ** 2:
                return f"{size / 1024 ** 2:.1f} MB"
            return f"{size / 1024:.0f} KB"
        except Exception:
            return "0 KB"

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

    # ==================== 下载器遍历 ====================

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
            logger.error(f"订阅剧集自动清理：构建自定义下载器失败 {dtype} {host}:{port}：{e}")
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
            logger.error(f"订阅剧集自动清理：获取 MoviePilot 下载器失败：{e}")
        for idx, cfg in enumerate(self._parse_custom()):
            dtype, host, port, user, pwd = cfg
            inst = self._build_custom(dtype, host, port, user, pwd)
            if inst:
                yield f"custom:{idx}", dtype, inst

    def _get_downloader(self, dl: str):
        if dl and dl.startswith("custom:"):
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
            logger.error(f"订阅剧集自动清理：获取下载器 {dl} 失败：{e}")
        return None, ""

    def _tr_fetch_all(self, inst: Any, dl_name: str) -> Dict[str, dict]:
        out = {}
        host = getattr(inst, "_host", None)
        if not host:
            return out
        proto = getattr(inst, "_protocol", "http") or "http"
        port = getattr(inst, "_port", None)
        user = getattr(inst, "_username", "") or ""
        pwd = getattr(inst, "_password", "") or ""
        url = f"{proto}://{host}:{port}/transmission/rpc" if port \
            else f"{proto}://{host}/transmission/rpc"
        try:
            sess = requests.Session()
            r1 = sess.post(url, data={}, auth=(user, pwd), timeout=20)
            sid = r1.headers.get("X-Transmission-Session-Id")
            if not sid:
                logger.error("订阅剧集自动清理：TR 会话握手失败（未返回 Session-Id）")
                return out
            r = sess.post(url, headers={"X-Transmission-Session-Id": sid}, auth=(user, pwd),
                          json={"method": "torrent-get",
                                "arguments": {"fields": [
                                    "hashString", "name", "totalSize", "percentDone", "status",
                                    "downloadDir", "contentPath", "magnetLink"]}},
                          timeout=120)
            for t in r.json().get("arguments", {}).get("torrents", []):
                h = t.get("hashString")
                if not h:
                    continue
                done = float(t.get("percentDone") or 0) >= 1
                active = int(t.get("status", 0) or 0) in (1, 2, 3, 4) or not done
                content = str(t.get("contentPath") or "") \
                    or (str(t.get("downloadDir") or "").rstrip("/") + "/" + str(t.get("name") or ""))
                out[h] = {
                    "dl": dl_name, "dl_type": "transmission", "hash": h,
                    "name": str(t.get("name") or ""),
                    "size": int(t.get("totalSize") or 0),
                    "active": active,
                    "magnet": str(t.get("magnetLink") or ""),
                    "save_path": str(t.get("downloadDir") or ""),
                    "data_id": content,
                }
        except Exception as e:
            logger.error(f"订阅剧集自动清理：TR RPC 拉取种子失败：{e}")
        return out

    def _qb_fetch_all(self, inst: Any, dl_name: str) -> Dict[str, dict]:
        out = {}
        qbc = getattr(inst, "qbc", None)
        if not qbc:
            return out
        try:
            infos = list(qbc.torrents_info() or [])
        except Exception as e:
            logger.error(f"订阅剧集自动清理：QB 拉取种子列表失败：{e}")
            return out
        for x in infos:
            h = str(x.get("hash") or "")
            if not h:
                continue
            state = str(x.get("state") or "")
            active = float(x.get("progress") or 0) < 1 or state.lower() in _QB_ACTIVE_STATES
            out[h] = {
                "dl": dl_name, "dl_type": "qbittorrent", "hash": h,
                "name": str(x.get("name") or ""),
                "size": int(x.get("size") or 0),
                "active": active,
                "magnet": str(x.get("magnet_uri") or ""),
                "save_path": str(x.get("save_path") or ""),
                "data_id": str(x.get("content_path") or "")
                or (str(x.get("save_path") or "").rstrip("/") + "/" + str(x.get("name") or "")),
            }
        return out

    # ==================== 归属与匹配 ====================

    def _load_subscribe(self, sid) -> Optional[Dict[str, Any]]:
        try:
            from app.db.subscribe_oper import SubscribeOper
        except Exception as e:
            logger.error(f"订阅剧集自动清理：加载订阅模块失败：{e}")
            return None
        try:
            if not str(sid).isdigit():
                return None
            s = SubscribeOper().get(int(sid))
            if not s:
                return None
            return {
                "id": getattr(s, "id", None),
                "name": str(getattr(s, "name", "") or ""),
                "year": str(getattr(s, "year", "") or ""),
                "season": str(getattr(s, "season", "") or ""),
                "total_episode": getattr(s, "total_episode", 0) or 0,
                "lack_episode": getattr(s, "lack_episode", 0) or 0,
                "note": getattr(s, "note", "") or "",
                "keyword": str(getattr(s, "keyword", "") or ""),
                "type": str(getattr(s, "type", "") or ""),
            }
        except Exception as e:
            logger.error(f"订阅剧集自动清理：读取订阅 {sid} 失败：{e}")
            return None

    def _match_terms(self, sub: Dict[str, Any]) -> List[str]:
        """用于匹配种子标题的归一化词：订阅名 / 订阅 keyword / 自定义匹配词"""
        terms = []
        if sub:
            if sub.get("name"):
                terms.append(norm_text(sub["name"]))
            if sub.get("keyword"):
                terms.append(norm_text(sub["keyword"]))
        for kw in (self._match_keywords or "").splitlines():
            k = kw.strip()
            if k:
                terms.append(norm_text(k))
        return [t for t in terms if t]

    def _is_matched(self, torrent_name: str, terms: List[str], sub: Dict[str, Any]) -> bool:
        if not terms:
            return False
        t = norm_text(torrent_name)
        if not any(tm and tm in t for tm in terms):
            return False
        # 季过滤（可选）：配置季或订阅季
        season = self._season or (sub or {}).get("season", "")
        try:
            s = int(season)
        except Exception:
            return True
        if s <= 0:
            return True  # 未指定有效季（如 0），不按季过滤
        t_seasons, _ = parse_episodes(torrent_name)
        if t_seasons and s not in t_seasons:
            return False
        return True

    # ==================== 扫描分析 ====================

    def _scan_all(self) -> Dict[str, dict]:
        """扫描全部下载器种子，返回 {key: it}"""
        items: Dict[str, dict] = {}
        for name, dtype, inst in self._iter_downloaders():
            try:
                if "transmission" in dtype:
                    d = self._tr_fetch_all(inst, name)
                elif "qb" in dtype:
                    d = self._qb_fetch_all(inst, name)
                else:
                    d = {}
                for h, it in d.items():
                    it["key"] = f"{it['dl']}|{h}"
                    items[it["key"]] = it
            except Exception as e:
                logger.error(f"订阅剧集自动清理：下载器 {name} 处理异常：{e}")
        return items

    def _analyze(self, sub: Dict[str, Any], items: Dict[str, dict]) -> Dict[str, Any]:
        """分析订阅剧集，返回应清理的种子与保留/剔除信息"""
        terms = self._match_terms(sub)
        matched = []
        for key, it in items.items():
            if not self._is_matched(it.get("name", ""), terms, sub):
                continue
            seasons, eps = parse_episodes(it.get("name", ""))
            it["_seasons"] = seasons
            it["_eps"] = eps
            it["key"] = key
            matched.append(it)
        downloaded_eps = sorted({int(e) for it in matched for e in (it.get("_eps") or [])})
        threshold, keep_list, drop_list = compute_keep(downloaded_eps, self._keep_episodes)

        candidates = []
        for it in matched:
            eps = it.get("_eps") or []
            if not eps:
                continue  # 整包/无法解析集号的种子（可能含保留集）一律保留，不删
            if max(eps) >= threshold:
                continue  # 覆盖到保留窗口，保留
            if self._skip_active and it.get("active"):
                continue  # 未完成跳过
            candidates.append({
                "id": it["key"], "downloader": it.get("dl", ""), "dl_type": it.get("dl_type", ""),
                "hash": it.get("hash", ""), "name": it.get("name", ""),
                "eps": eps, "max_ep": max(eps), "size": int(it.get("size") or 0),
                "save_path": it.get("save_path", ""), "magnet": it.get("magnet", ""),
                "data_id": it.get("data_id", ""), "active": bool(it.get("active")),
            })
        candidates.sort(key=lambda c: (c["max_ep"], c["name"]))
        free_size = sum(int(c["size"] or 0) for c in candidates)
        return {
            "sub": {"id": sub.get("id"), "name": sub.get("name"), "year": sub.get("year"),
                    "season": sub.get("season")},
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_torrents": len(items),
            "matched_torrents": len(matched),
            "downloaded_eps": downloaded_eps,
            "threshold": threshold,
            "keep_eps": keep_list,
            "drop_eps": drop_list,
            "candidates": candidates,
            "free_size": free_size,
        }

    def _analyze_all(self, subs: List[Dict[str, Any]], items: Dict[str, dict]) -> Dict[str, Any]:
        """对多个订阅逐个分析并汇总（保留窗口按各订阅独立计算），候选种子合并去重。"""
        results = []
        for sub in subs:
            try:
                results.append(self._analyze(sub, items))
            except Exception as e:
                logger.error(f"订阅剧集自动清理：分析订阅《{sub.get('name')}》失败：{e}")
        candidates = []
        seen = set()
        for r in results:
            for c in (r.get("candidates") or []):
                if c.get("id") not in seen:
                    seen.add(c.get("id"))
                    candidates.append(c)
        return {
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_torrents": len(items),
            "subs": results,
            "candidates": candidates,
            "free_size": sum(int(c.get("size") or 0) for c in candidates),
        }

    def _schedule_run(self):
        subs = []
        for sid in self._subscribe_ids:
            s = self._load_subscribe(sid)
            if s:
                subs.append(s)
            else:
                logger.warning(f"订阅剧集自动清理：订阅不存在（ID={sid}）")
        if not subs:
            logger.warning("订阅剧集自动清理：未选择有效订阅，请检查配置")
            if self._notify:
                self._send_notification(f"{self._notify_title_prefix}: 订阅剧集自动清理-未选择有效订阅，请检查配置")
            return
        items = self._scan_all()
        agg = self._analyze_all(subs, items)
        self.save_data("result", agg)
        logger.info(f"订阅剧集自动清理：分析完成，共 {len(subs)} 个订阅，种子 {agg['total_torrents']}，"
                    f"待清理 {len(agg['candidates'])}（释放 {self._fmt_size(agg['free_size'])}）")
        if self._auto and agg.get("candidates"):
            self._execute(agg["candidates"])
        elif self._notify and agg.get("candidates"):
            lines = []
            for r in (agg.get("subs") or []):
                if r.get("candidates"):
                    lines.append(f"▶ 《{r['sub'].get('name')}》待清理 {len(r['candidates'])} 个旧集"
                                 f"（保留最新 {self._keep_episodes} 集：{self._eps_text(r.get('keep_eps'))}）")
            self._send_notification(
                f"{self._notify_title_prefix}: 订阅剧集自动清理-发现 {len(agg['candidates'])} 个旧集待清理"
                f"（{self._fmt_size(agg['free_size'])}）\n" + "\n".join(lines) + "\n请到插件页面执行清理")

    def scan_api(self) -> Dict[str, Any]:
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        if not self._subscribe_ids:
            return {"success": False, "message": "未选择订阅"}
        subs = []
        for sid in self._subscribe_ids:
            s = self._load_subscribe(sid)
            if s:
                subs.append(s)
        if not subs:
            return {"success": False, "message": "所选订阅均不存在"}
        items = self._scan_all()
        agg = self._analyze_all(subs, items)
        self.save_data("result", agg)
        logger.info(f"订阅剧集自动清理：扫描完成，共 {len(subs)} 个订阅，待清理 {len(agg['candidates'])} 个种子")
        return {"success": True, "subs": len(subs),
                "candidates": len(agg["candidates"]),
                "free_size": agg["free_size"]}

    # ==================== 执行清理 ====================

    def run_api(self, confirm: str = "") -> Dict[str, Any]:
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        if confirm != "yes":
            return {"success": False, "message": "请确认参数 confirm=yes"}
        saved = self.get_data("result") or {}
        candidates = saved.get("candidates") or []
        if not candidates:
            return {"success": False, "message": "无待清理结果，请先扫描分析"}
        ok, kept, t_stats = self._execute(candidates)
        return {"success": True, "ok": ok, "total": len(candidates),
                "kept": len(kept), "transfer": t_stats}

    def _execute(self, candidates: List[Dict]) -> Tuple[int, List[str], Dict[str, int]]:
        """执行删除：按下载器分批删数据+退出做种，备份回收站，清理媒体库硬链接与转移记录；
        更新各订阅 note 防重下。返回 (成功数, 仅删任务提示, 整理联动统计)"""
        # 多订阅可能命中同一种子，按种子 ID 去重
        uniq: Dict[str, Dict] = {}
        for c in candidates:
            key = c.get("id") or f"{c.get('downloader')}|{c.get('hash')}"
            if key not in uniq:
                uniq[key] = c
        candidates = list(uniq.values())
        ok = 0
        kept: List[str] = []
        del_data_hashes: List[str] = []
        by_dl: Dict[str, List[Dict]] = {}
        for c in candidates:
            dl = c.get("downloader")
            if dl:
                by_dl.setdefault(dl, []).append(c)
        for dl, items in by_dl.items():
            inst, dtype = self._get_downloader(dl)
            if not inst:
                logger.error(f"订阅剧集自动清理：下载器 {dl} 不可用，跳过")
                continue
            try:
                if "transmission" in (dtype or ""):
                    all_ids = {h: it.get("data_id") or "" for h, it in self._tr_fetch_all(inst, dl).items()}
                elif "qb" in (dtype or ""):
                    all_ids = {h: it.get("data_id") or "" for h, it in self._qb_fetch_all(inst, dl).items()}
                else:
                    all_ids = {}
            except Exception as e:
                logger.error(f"订阅剧集自动清理：获取 {dl} 全量种子失败，跳过该下载器：{e}")
                continue
            del_hashes = {c.get("hash") for c in items}
            do_hashes = []
            for c in items:
                h = (c.get("hash") or "").split("@")[0]
                if h not in all_ids:
                    logger.warning(f"订阅剧集自动清理：种子 {h} 已不在下载器 {dl}，跳过")
                    continue
                data_id = all_ids.get(h) or ""
                referenced = False
                if self._delete_file:
                    for oh, od in all_ids.items():
                        oh_plain = oh.split("@")[0]
                        if oh_plain == h or oh_plain in del_hashes:
                            continue
                        if od and data_id and od == data_id:
                            referenced = True
                            break
                if referenced:
                    kept.append(c.get("name") or h)
                    continue  # 数据仍被其他种子引用：保留，不删
                do_hashes.append(h)
            if not do_hashes:
                continue
            self._backup_entries(dl, dtype, inst, items, set(do_hashes))
            try:
                if inst.delete_torrents(delete_file=self._delete_file, ids=do_hashes):
                    ok += len(do_hashes)
                    if self._delete_file:
                        del_data_hashes.extend(do_hashes)
                else:
                    logger.error(f"订阅剧集自动清理：{dl} 删除失败")
            except Exception as e:
                logger.error(f"订阅剧集自动清理：{dl} 删除种子异常：{e}")

        # 更新结果缓存
        try:
            saved = self.get_data("result") or {}
            del_ids = {c.get("id") for c in candidates}
            saved["candidates"] = [c for c in saved.get("candidates", []) if c.get("id") not in del_ids]
            for r in (saved.get("subs") or []):
                r["candidates"] = [c for c in (r.get("candidates") or []) if c.get("id") not in del_ids]
            self.save_data("result", saved)
        except Exception:
            pass
        # 整理联动
        t_stats = self._clean_transfer_links(del_data_hashes) if del_data_hashes else {}
        if t_stats:
            logger.info(f"订阅剧集自动清理：媒体库联动完成 转移记录 {t_stats.get('records', 0)} 条 / "
                        f"媒体文件 {t_stats.get('files', 0)} 个 / 跳过 {t_stats.get('skipped', 0)} 个")
        # 更新订阅 note：把这些剔除的集号标记为"已下载过"，防止重下
        self._mark_episodes_done()
        return ok, kept, t_stats

    def _mark_episodes_done(self):
        """把每个订阅本次剔除的集号写入各自订阅 note（JSON 列表），MP 将其视为已下载不再补下。"""
        saved = self.get_data("result") or {}
        subs_results = saved.get("subs") or []
        if not subs_results:
            return
        try:
            from app.db.subscribe_oper import SubscribeOper
        except Exception as e:
            logger.error(f"订阅剧集自动清理：无法访问订阅库：{e}")
            return
        oper = SubscribeOper()
        for r in subs_results:
            sub_info = r.get("sub") or {}
            drop_eps = r.get("drop_eps") or []
            sid = sub_info.get("id")
            if not sid or not drop_eps:
                continue
            s = self._load_subscribe(sid)
            if not s:
                continue
            note = []
            raw = s.get("note") or ""
            if raw:
                try:
                    parsed = json.loads(raw)
                    if isinstance(parsed, list):
                        note = [int(x) for x in parsed if str(x).isdigit()]
                    elif isinstance(parsed, int):
                        note = [parsed]
                except Exception:
                    note = []
            merged = sorted(set(note) | set(int(x) for x in drop_eps))
            try:
                oper.update(sid, {"note": json.dumps(merged)})
                logger.info(f"订阅剧集自动清理：《{s.get('name')}》已把剔除集号 {self._eps_text(drop_eps)} "
                            f"写入已下载 note，后续不再通过订阅重新下载")
            except Exception as e:
                logger.error(f"订阅剧集自动清理：更新订阅 {sid} note 失败：{e}")

    # ==================== 媒体库硬链接联动 ====================

    def _clean_transfer_links(self, del_hashes: List[str]) -> Dict[str, int]:
        """对已删数据的种子，同步清理媒体库硬链接与转移记录（download_hash 精确匹配 + 双重保护）。"""
        stats = {"records": 0, "files": 0, "skipped": 0}
        if not self._clean_media_links or not del_hashes:
            return stats
        try:
            from app.db import SessionFactory
            from app.db.models.transferhistory import TransferHistory
            from sqlalchemy import select
        except Exception as e:
            logger.warning(f"订阅剧集自动清理：媒体库联动不可用（数据库模块导入失败）：{e}")
            return stats
        try:
            with SessionFactory() as session:
                all_rows = list(session.execute(select(TransferHistory)).scalars().all())
                by_hash: Dict[str, List[Any]] = {}
                dest_refs: Dict[str, set] = {}
                for r in all_rows:
                    dh = (getattr(r, "download_hash", "") or "").lower()
                    if dh:
                        by_hash.setdefault(dh, []).append(r)
                    dest = getattr(r, "dest", "") or ""
                    if dest and dh:
                        dest_refs.setdefault(dest, set()).add(dh)
                del_set = {h.split("@")[0].lower() for h in del_hashes}
                touched: Dict[int, Any] = {}
                for h in del_set:
                    for r in by_hash.get(h, []):
                        touched[r.id] = r
                if not touched:
                    logger.info("订阅剧集自动清理：媒体库联动未命中任何转移记录")
                    return stats
                for rid, r in touched.items():
                    dest = getattr(r, "dest", "") or ""
                    dest_storage = str(getattr(r, "dest_storage", "") or "").lower()
                    can_del_file = False
                    if dest and dest_storage in ("", "local"):
                        other_refs = dest_refs.get(dest, set()) - del_set
                        if not other_refs:
                            try:
                                st = os.stat(dest)
                                can_del_file = st.st_nlink <= 1
                            except FileNotFoundError:
                                pass
                            except Exception:
                                can_del_file = False
                        if can_del_file:
                            try:
                                os.remove(dest)
                                stats["files"] += 1
                                self._prune_empty_dirs(os.path.dirname(dest))
                            except Exception as e:
                                logger.warning(f"订阅剧集自动清理：删除媒体文件失败 {dest}：{e}")
                        elif os.path.exists(dest):
                            stats["skipped"] += 1
                    try:
                        session.delete(r)
                        stats["records"] += 1
                    except Exception as e:
                        logger.warning(f"订阅剧集自动清理：删除转移记录 #{rid} 失败：{e}")
                session.commit()
        except Exception as e:
            logger.error(f"订阅剧集自动清理：媒体库联动执行异常：{e}")
        return stats

    @staticmethod
    def _prune_empty_dirs(path: str, max_up: int = 4):
        p = str(path or "").rstrip("/")
        for _ in range(max_up):
            if not p or p == "/" or len(p) < 6:
                return
            try:
                if os.path.isdir(p) and not os.listdir(p):
                    os.rmdir(p)
                else:
                    return
            except Exception:
                return
            p = os.path.dirname(p)

    # ==================== 回收站备份 ====================

    @staticmethod
    def _recycle_dir() -> str:
        try:
            base = settings.PLUGIN_DATA_PATH
        except Exception:
            base = "/config/plugins"
        d = os.path.join(str(base), "subscribe_episode_recycle")
        os.makedirs(d, exist_ok=True)
        return d

    @staticmethod
    def _safe_name(s: str) -> str:
        return "".join(c if c.isalnum() or c in "-_." else "_" for c in str(s or ""))

    def _backup_entries(self, dl: str, dtype: str, inst: Any, items: List[Dict], hash_set: set):
        if not self._recycle_enabled:
            return
        try:
            rec = self.get_data("recycle") or {}
        except Exception:
            rec = {}
        dirty = False
        for it in items:
            h_plain = (it.get("hash") or "").split("@")[0]
            if not h_plain or h_plain not in hash_set:
                continue
            key = it.get("id") or f"{dl}|{h_plain}"
            if key in rec:
                continue
            bk_type, bk_file, bk_magnet = "", "", ""
            if "qb" in (dtype or ""):
                try:
                    qbc = getattr(inst, "qbc", None)
                    if qbc:
                        resp = qbc.torrents_export(torrent_hash=h_plain)
                        content = resp if isinstance(resp, bytes) else getattr(resp, "content", None)
                        if content and content[:1] == b"d":
                            fn = f"{self._safe_name(dl)}_{h_plain}.torrent"
                            with open(os.path.join(self._recycle_dir(), fn), "wb") as f:
                                f.write(content)
                            bk_type, bk_file = "torrent", fn
                except Exception as e:
                    logger.warning(f"订阅剧集自动清理：导出种子文件失败（{str(it.get('name', ''))[:30]}）：{e}")
            if not bk_type and it.get("magnet"):
                bk_type, bk_magnet = "magnet", str(it["magnet"])
            if not bk_type:
                logger.warning(f"订阅剧集自动清理：种子无可用备份材料（{str(it.get('name', ''))[:40]}）")
                continue
            rec[key] = {
                "downloader": dl, "dl_type": dtype or "",
                "hash": h_plain, "name": it.get("name", ""),
                "size": it.get("size", 0) or 0,
                "save_path": it.get("save_path", ""),
                "deleted_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "deleted_file": bool(self._delete_file),
                "bk_type": bk_type, "bk_file": bk_file, "bk_magnet": bk_magnet,
            }
            dirty = True
        if dirty:
            self.save_data("recycle", rec)

    def clear_api(self, confirm: str = "") -> Dict[str, Any]:
        if confirm != "yes":
            return {"success": False, "message": "请确认参数 confirm=yes"}
        self.save_data("result", {})
        return {"success": True}

    # ==================== 通知与停止 ====================

    def stop_service(self):
        self.__stop_service()

    def __stop_service(self):
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            print(str(e))

    def __update_config(self):
        self.update_config({
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "subscribe_ids": self._subscribe_ids,
            "keep_episodes": self._keep_episodes,
            "season": self._season,
            "match_keywords": self._match_keywords,
            "delete_file": self._delete_file,
            "clean_media_links": self._clean_media_links,
            "recycle_enabled": self._recycle_enabled,
            "skip_active": self._skip_active,
            "auto": self._auto,
            "notify": self._notify,
            "webhook": self._webhook,
            "notify_title_prefix": self._notify_title_prefix,
            "custom_downloaders": self._custom_downloaders,
        })

    def _send_notification(self, text: str):
        if self._webhook:
            try:
                resp = requests.post(self._webhook,
                                     json={"msg_type": "text", "content": {"text": text}}, timeout=15)
                if resp.status_code == 200 and resp.json().get("code") == 0:
                    logger.info("订阅剧集自动清理飞书通知发送成功")
                    return
                else:
                    logger.error(f"订阅剧集自动清理飞书通知发送失败：{resp.text[:200]}")
            except Exception as e:
                logger.error(f"订阅剧集自动清理飞书通知发送异常：{e}")
        self.post_message(title="订阅剧集自动清理", text=text)
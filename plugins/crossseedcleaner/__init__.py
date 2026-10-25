# -*- coding: utf-8 -*-
"""
跨站做种清理插件 (CrossSeedCleaner)

功能：
- 扫描 MoviePilot 已配置下载器 + 自定义下载器（qBittorrent / Transmission）中的全部种子
- 按「同一内容」聚合（种子名称或数据路径分组），统计该内容在哪些站点做种
- 判定"单站独苗"：某内容只有 1 个站点做种 → 删除种子（可选删除数据文件）
- 多站做种的内容自动保留（跨站保流）
- 保护规则：未完成种子跳过、保护站点白名单、保护关键词、累计上传量阈值
- 删除前自动备份种子到回收站（qB 导出 .torrent / TR 记录磁力链），支持一键恢复
- 数据文件辅种保护：数据仍被本次删除范围之外的种子引用时自动降级为仅删任务
- 定时扫描（可配自动清理）+ 页面手动触发，飞书 Webhook 或系统消息通知
"""

import hashlib
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs

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


def host_of(url: str) -> str:
    """URL -> 主机名"""
    try:
        if not url:
            return ""
        if "://" not in url:
            url = "http://" + str(url)
        return (urlparse(url).hostname or "").lower()
    except Exception:
        return ""


def site_key(url_or_host: str) -> str:
    """tracker URL / 主机名 -> 站点主域 key（取最后两段，跨 announce 前缀去重）"""
    if not url_or_host:
        return ""
    s = str(url_or_host).strip().lower()
    h = host_of(s) if "://" in s else s.split("/")[0]
    h = h.split(":")[0]
    parts = [p for p in h.split(".") if p]
    if len(parts) >= 2:
        return parts[-2] + "." + parts[-1]
    return h


def analyze_items(items: dict, group_by: str = "name") -> dict:
    """
    核心分组分析（纯函数，便于测试）。
    items: {hash: {"name","size","sites"(list),"data_id",...}}
    返回 {"groups": {gkey: {"entries": [...], "sites": set(), "name", "path"}},
          "keep": [gkey...], "single": [gkey...]}
    """
    groups: Dict[str, dict] = {}
    for h, it in (items or {}).items():
        key = ""
        if group_by == "path":
            key = str(it.get("data_id") or "")
        if not key:
            key = str(it.get("name") or "")
        g = groups.setdefault(key, {"entries": [], "sites": set(), "name": it.get("name", ""),
                                    "path": it.get("data_id", "")})
        g["entries"].append(dict(it, _key=h))
        for s in (it.get("sites") or []):
            if s:
                g["sites"].add(s)
    keep, single = [], []
    for gk, g in groups.items():
        if len(g["sites"]) >= 2:
            keep.append(gk)
        else:
            single.append(gk)
    return {"groups": groups, "keep": keep, "single": single}


class CrossSeedCleaner(_PluginBase):
    """
    跨站做种清理插件
    """

    plugin_name = "跨站做种清理"
    plugin_desc = ("按内容聚合下载器种子（qBittorrent/Transmission），统计站点做种数："
                   "仅 1 个站点做种的独苗内容清理（可选删数据），多站做种自动保留（跨站保流）；"
                   "支持未完成保护/站点白名单/关键词/上传量阈值保护、回收站备份恢复、定时与手动触发。")
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/torrent.png"
    plugin_version = "1.0"
    plugin_author = "Finn"
    author_url = "https://github.com"
    plugin_config_prefix = "crossseedcleaner_"
    plugin_order = 36
    auth_level = 1

    # 配置（私有）
    _enabled = False
    _onlyonce = False
    _cron = "0 4 * * *"
    _delete_file = False          # 是否删除数据文件（否则仅删任务）
    _auto_delete = False          # 定时扫描后是否自动清理
    _notify = True
    _webhook = ""
    _notify_title_prefix = "HA通知"
    _group_by = "name"            # 聚合方式：name=种子名称 / path=数据路径
    _skip_active = True           # 含未完成种子的独苗组跳过（不删）
    _protected_sites = ""         # 保护站点（逗号分隔站点名/域名，独苗站点命中则保留）
    _protected_keywords = ""      # 保护关键词（每行一个，种子名命中则整组保留）
    _protect_uploaded_gb = 0      # 累计上传量保护阈值 GB（0=不启用）
    _recycle_enabled = True       # 删除前备份种子（支持误删恢复）
    _custom_downloaders = ""      # 自定义下载器，每行：类型|地址|端口|用户名|密码
    _scheduler = None
    _sel: List[str] = []
    _site_map_cache: Optional[Dict[str, str]] = None

    # 内置常见 PT 站点 tracker 主域 -> 站点名（实测映射，MP 站点表优先生效）
    BUILTIN_SITE_MAP = {
        "springsunday.net": "SpringSunday",
        "m-team.cc": "馒头M-Team",
        "pterclub.net": "飘雪PTerClub",
        "btschool.club": "BT学校",
        "zmpt.cc": "猪猪ZMPT",
        "hdfans.org": "HDFans",
        "soulvoice.club": "SoulVoice",
        "audiences.me": "Audiences观众",
        "cinefiles.info": "Cinefiles",
        "hdarea.club": "HDArea高清地带",
        "hdtime.org": "HDTime高清时光",
        "pthome.org": "PTHome",
        "carpt.net": "CarPT",
        "totheglory.im": "ToTheGlory",
        "hddolby.com": "HDDolby",
        "ubits.club": "uBits",
        "hdhome.org": "HDHome",
        "pandapt.net": "熊猫PandaPT",
        "cyanbug.net": "青虫CyanBug",
        "rousi.pro": "肉丝Rousi",
        "open.cd": "皇后OpenCD",
        "keepfrds.com": "FRDS",
        "zhuque.in": "朱雀Zhuque",
        "hhanclub.net": "HHanClub",
        "1ptba.com": "1PTBA",
        "ptsbao.club": "PTSbao",
        "hdsky.me": "HD影Sky",
        "0ff.cc": "OFF",
        "discfan.xyz": "碟粉DiscFan",
        "52pt.site": "52PT",
        "xingtan.one": "星坛XingTan",
        "haidan.video": "海豚Haidan",
        "htpt.cc": "HTTPT",
        "gamegamept.com": "GameGame",
        "greatposterwall.com": "GreatPosterWall",
        "eastgame.org": "EastGame",
        "nicept.net": "NicePT",
        "pttime.org": "PTTime",
    }

    # ==================== 生命周期 ====================

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled", False)
            self._onlyonce = config.get("onlyonce", False) or False
            self._cron = config.get("cron") or "0 4 * * *"
            self._delete_file = bool(config.get("delete_file", False))
            self._auto_delete = bool(config.get("auto_delete", False))
            self._notify = bool(config.get("notify", True))
            self._webhook = config.get("webhook", "")
            self._notify_title_prefix = config.get("notify_title_prefix") or "HA通知"
            self._group_by = (config.get("group_by") or "name").strip() or "name"
            if self._group_by not in ("name", "path"):
                self._group_by = "name"
            self._skip_active = bool(config.get("skip_active", True))
            self._protected_sites = config.get("protected_sites", "") or ""
            self._protected_keywords = config.get("protected_keywords", "") or ""
            try:
                self._protect_uploaded_gb = float(config.get("protect_uploaded_gb") or 0)
            except Exception:
                self._protect_uploaded_gb = 0
            self._recycle_enabled = bool(config.get("recycle_enabled", True))
            self._custom_downloaders = config.get("custom_downloaders") or ""
        self._sel = []

        self.__stop_service()

        if self._enabled:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            try:
                self._scheduler.add_job(
                    func=self._schedule_scan,
                    trigger=CronTrigger.from_crontab(self._cron),
                    name="跨站做种清理",
                )
                logger.info(f"跨站做种清理定时任务注册成功：cron={self._cron}")
            except Exception as e:
                logger.error(f"跨站做种清理定时任务注册失败：{e}")

            if self._onlyonce:
                logger.info("跨站做种清理服务启动，立即运行一次")
                self._scheduler.add_job(
                    func=self._schedule_scan,
                    trigger="date",
                    run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=5),
                    name="跨站做种清理-立即运行",
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
            {"path": "/scan", "endpoint": self.scan_api, "methods": ["GET"], "summary": "扫描分析独苗种子"},
            {"path": "/toggle", "endpoint": self.toggle_api, "methods": ["GET"], "summary": "切换条目勾选"},
            {"path": "/toggle_group", "endpoint": self.toggle_group_api, "methods": ["GET"], "summary": "按组勾选/取消"},
            {"path": "/toggle_all", "endpoint": self.toggle_all_api, "methods": ["GET"], "summary": "一键勾选全部"},
            {"path": "/delete", "endpoint": self.delete_api, "methods": ["GET"], "summary": "删除单条"},
            {"path": "/delete_selected", "endpoint": self.delete_selected_api, "methods": ["GET"], "summary": "删除勾选条目"},
            {"path": "/delete_all", "endpoint": self.delete_all_api, "methods": ["GET"], "summary": "删除全部独苗"},
            {"path": "/clear", "endpoint": self.clear_api, "methods": ["GET"], "summary": "清空扫描结果"},
            {"path": "/restore", "endpoint": self.restore_api, "methods": ["GET"], "summary": "从回收站恢复误删种子"},
            {"path": "/recycle_delete", "endpoint": self.recycle_delete_api, "methods": ["GET"], "summary": "清除回收站单条备份"},
            {"path": "/recycle_clear", "endpoint": self.recycle_clear_api, "methods": ["GET"], "summary": "清空回收站"},
        ]

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
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
                             'content': [{'component': 'VSwitch', 'props': {'model': 'delete_file', 'label': '允许删除数据文件（独苗清理释放空间的核心）'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6},
                             'content': [{'component': 'VSwitch', 'props': {'model': 'auto_delete', 'label': '定时扫描后自动清理（不勾选则仅分析通知）'}}]},
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6},
                             'content': [{'component': 'VSwitch', 'props': {'model': 'recycle_enabled', 'label': '删除前备份种子到回收站（支持误删恢复）'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6},
                             'content': [{'component': 'VSwitch', 'props': {'model': 'skip_active', 'label': '含未完成种子的组跳过（下载/校验中不删）'}}]},
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                             'content': [{'component': 'VSelect', 'props': {'model': 'group_by', 'label': '内容聚合方式', 'items': [
                                 {'title': '按种子名称（推荐）', 'value': 'name'},
                                 {'title': '按数据路径（跨下载器挂载一致时更严格）', 'value': 'path'},
                             ]}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                             'content': [{'component': 'VTextField', 'props': {'model': 'cron', 'label': '执行周期(Cron)', 'placeholder': '0 4 * * *（每天4点）'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                             'content': [{'component': 'VTextField', 'props': {'model': 'protect_uploaded_gb', 'label': '累计上传保护阈值GB(0=关)', 'placeholder': '0'}}]},
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6},
                             'content': [{'component': 'VTextField', 'props': {'model': 'protected_sites', 'label': '保护站点（逗号分隔，独苗站点命中不删）', 'placeholder': '馒头M-Team,HD影Sky'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6},
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
                             'content': [{'component': 'VTextarea', 'props': {'model': 'custom_downloaders', 'label': '自定义下载器（每行：类型|地址|端口|用户名|密码，类型 qbittorrent/transmission；留空则仅用 MoviePilot 已配置下载器）', 'rows': 2, 'placeholder': 'qbittorrent|192.168.10.11|8080|admin|pass\ntransmission|192.168.10.11|9091|admin|pass'}}]},
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 12},
                             'content': [{'component': 'VTextarea', 'props': {'model': 'protected_keywords', 'label': '保护关键词（每行一个，种子名命中则整组保留）', 'rows': 3}}]},
                        ]
                    },
                ]
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "cron": "0 4 * * *",
            "delete_file": False,
            "auto_delete": False,
            "notify": True,
            "webhook": "",
            "notify_title_prefix": "HA通知",
            "group_by": "name",
            "skip_active": True,
            "protected_sites": "",
            "protected_keywords": "",
            "protect_uploaded_gb": 0,
            "recycle_enabled": True,
            "custom_downloaders": "",
        }

    # ==================== 页面 ====================

    def get_page(self) -> Optional[List[dict]]:
        if not self._enabled:
            return [{'component': 'VAlert', 'props': {'type': 'warning', 'text': '插件未启用，请先到「设置」页启用', 'variant': 'tonal'}}]

        saved = self.get_data("result") or {}
        groups = saved.get("groups") or []
        scan_time = saved.get("time", "")
        stat = saved.get("stat") or {}
        selected = set(self._sel)

        page = []
        page.append({
            'component': 'VAlert',
            'props': {'type': 'info', 'variant': 'tonal', 'density': 'compact', 'class': 'mb-2',
                      'text': (f"分析时间：{scan_time or '尚未扫描'}   种子总数 {stat.get('total', 0)}   "
                               f"多站保留 {stat.get('keep_groups', 0)} 组   独苗待清理 {len(groups)} 组"
                               f"({stat.get('del_seeds', 0)} 种子/{self._fmt_size(stat.get('del_size', 0))})   "
                               f"已勾选 {len(selected)} 个")}
        })

        page.append({
            'component': 'VRow',
            'props': {'class': 'mb-1'},
            'content': [
                {'component': 'VCol', 'props': {'cols': 12, 'md': 3},
                 'content': [self._btn("扫描分析", 'plugin/CrossSeedCleaner/scan', {}, 'primary', 'elevated')]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 3},
                 'content': [self._btn(f"删除选中({len(selected)})", 'plugin/CrossSeedCleaner/delete_selected',
                                       {'confirm': 'yes'}, 'error', 'elevated')]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 3},
                 'content': [self._btn("删除全部独苗", 'plugin/CrossSeedCleaner/delete_all',
                                       {'confirm': 'yes'}, 'error', 'text')]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 3},
                 'content': [self._btn("清空结果", 'plugin/CrossSeedCleaner/clear',
                                       {'confirm': 'yes'}, 'secondary', 'text')]},
            ]
        })

        if not groups:
            page.append({'component': 'VAlert',
                         'props': {'type': 'success', 'variant': 'tonal',
                                   'text': '当前无独苗种子：全部内容均有 2 个以上站点做种，或已全部处理/保护命中'}})
        else:
            for g in groups:
                entry_ids = [e.get("id") for e in g.get("entries", [])]
                all_sel = all(e in selected for e in entry_ids)
                page.append({
                    'component': 'VCard',
                    'props': {'variant': 'outlined', 'class': 'mb-1', 'density': 'compact'},
                    'content': [
                        {
                            'component': 'VRow',
                            'props': {'class': 'pa-1 ma-0', 'align': 'center'},
                            'content': [
                                {'component': 'VCol', 'props': {'cols': 'auto'},
                                 'content': [{'component': 'VCheckboxBtn', 'props': {'model-value': all_sel, 'density': 'compact'},
                                              'events': {'click': {'api': 'plugin/CrossSeedCleaner/toggle_group',
                                                                   'method': 'get',
                                                                   'params': {'apikey': settings.API_TOKEN,
                                                                              'gid': g.get('gid', '')}}}}]},
                                {'component': 'VCol', 'props': {'cols': True},
                                 'content': [{'component': 'div', 'props': {'class': 'text-subtitle-2'},
                                              'text': str(g.get("name", ""))[:80]}]},
                                {'component': 'VCol', 'props': {'cols': 'auto'},
                                 'content': [{'component': 'VChip', 'props': {'size': 'x-small', 'color': 'info', 'variant': 'tonal', 'class': 'mr-1'},
                                              'text': str(g.get("site_name", "") or "未知站点")}]},
                                {'component': 'VCol', 'props': {'cols': 'auto'},
                                 'content': [{'component': 'VChip', 'props': {'size': 'x-small', 'color': 'orange', 'variant': 'tonal', 'class': 'mr-1'},
                                              'text': f"{len(g.get('entries', []))}种子"}]},
                                {'component': 'VCol', 'props': {'cols': 'auto'},
                                 'content': [{'component': 'VChip', 'props': {'size': 'x-small', 'color': 'deep-purple', 'variant': 'tonal'},
                                              'text': self._fmt_size(g.get("size", 0))}]},
                            ]
                        },
                        *[
                            {
                                'component': 'VRow',
                                'props': {'class': 'pa-0 ma-0 ml-4', 'align': 'center', 'dense': True,
                                          'style': (('background: rgba(76,175,80,0.12); border-radius: 6px;')
                                                    if (e.get("id", "") in selected) else '')},
                                'content': [
                                    {'component': 'VCol', 'props': {'cols': 'auto'},
                                     'content': [{'component': 'VCheckboxBtn', 'props': {'model-value': (e.get("id", "") in selected), 'density': 'compact'},
                                                  'events': {'click': {'api': 'plugin/CrossSeedCleaner/toggle',
                                                                       'method': 'get',
                                                                       'params': {'apikey': settings.API_TOKEN,
                                                                                  'id': e.get("id", "")}}}}]},
                                    {'component': 'VCol', 'props': {'cols': True},
                                     'content': [{'component': 'div', 'props': {'class': 'text-caption'},
                                                  'text': f"[{e.get('downloader', '')}] {str(e.get('name', ''))[:75]}"}]},
                                    {'component': 'VCol', 'props': {'cols': 'auto'},
                                     'content': [{'component': 'div', 'props': {'class': 'text-caption'},
                                                  'text': (f"已传 {self._fmt_size(e.get('uploaded', 0))}"
                                                           if e.get('uploaded') else '')}]},
                                ]
                            } for e in g.get("entries", [])
                        ],
                    ]
                })

        # 回收站
        try:
            rec = self.get_data("recycle") or {}
        except Exception:
            rec = {}
        recycle_items = list(rec.items())
        page.append({'component': 'div', 'props': {'class': 'text-subtitle-1 mt-3 mb-1'},
                     'text': f"删除回收站（{len(recycle_items)} 条，支持误删恢复）"})
        for rid, it in recycle_items[:50]:
            page.append({
                'component': 'VRow',
                'props': {'class': 'pa-0 ma-0', 'align': 'center', 'dense': True},
                'content': [
                    {'component': 'VCol', 'props': {'cols': True},
                     'content': [{'component': 'div', 'props': {'class': 'text-caption'},
                                  'text': f"[{it.get('deleted_at', '')}] {str(it.get('name', ''))[:60]} "
                                          f"({'已删数据' if it.get('deleted_file') else '仅删任务'} / {it.get('site', '')})"}]},
                    {'component': 'VCol', 'props': {'cols': 'auto'},
                     'content': [self._btn("恢复", 'plugin/CrossSeedCleaner/restore',
                                           {'id': rid}, 'primary', 'text')]},
                    {'component': 'VCol', 'props': {'cols': 'auto'},
                     'content': [self._btn("清除", 'plugin/CrossSeedCleaner/recycle_delete',
                                           {'id': rid}, 'error', 'text')]},
                ]
            })
        if recycle_items:
            page.append(self._btn("清空回收站", 'plugin/CrossSeedCleaner/recycle_clear',
                                  {'confirm': 'yes'}, 'secondary', 'text'))
        else:
            page.append({'component': 'div', 'props': {'class': 'text-caption text-disabled'}, 'text': '回收站为空'})

        return page

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
            if size >= 1024 ** 4:
                return f"{size / 1024 ** 4:.2f} TB"
            if size >= 1024 ** 3:
                return f"{size / 1024 ** 3:.2f} GB"
            if size >= 1024 ** 2:
                return f"{size / 1024 ** 2:.1f} MB"
            return f"{size / 1024:.0f} KB"
        except Exception:
            return "0 KB"

    def site_display(self, key: str) -> str:
        """站点主域 key -> 展示名"""
        if not key:
            return ""
        m = self._site_name_map()
        if key in m:
            return m[key]
        for d, n in m.items():
            if key == d or key.endswith("." + d):
                return n
        if key in self.BUILTIN_SITE_MAP:
            return self.BUILTIN_SITE_MAP[key]
        return key

    def _site_name_map(self) -> Dict[str, str]:
        """MoviePilot 站点表域名->站点名（缓存）"""
        if self._site_map_cache is not None:
            return self._site_map_cache
        m = {}
        try:
            from app.db.site_oper import SiteOper
            for site in (SiteOper().list() or []):
                name = str(getattr(site, "name", "") or "").strip()
                domain = str(getattr(site, "domain", "") or "").strip().lower()
                if name and domain:
                    h = host_of(domain) if "://" in domain else domain.split("/")[0]
                    if h:
                        m[h] = name
        except Exception as e:
            logger.warning(f"跨站做种清理：读取站点列表失败：{e}")
        self._site_map_cache = m
        return m

    def _protected_site_list(self) -> List[str]:
        return [s.strip().lower() for s in (self._protected_sites or "").replace("，", ",").split(",") if s.strip()]

    def _keyword_list(self) -> List[str]:
        return [k.strip() for k in (self._protected_keywords or "").splitlines() if k.strip()]

    def _site_protected(self, site_keys: set) -> bool:
        if not site_keys:
            return False
        plist = self._protected_site_list()
        if not plist:
            return False
        for k in site_keys:
            disp = self.site_display(k)
            disp_low = (disp or "").lower()
            for p in plist:
                if p == k or p == k.split(".")[0] or (disp_low and disp_low == p) \
                        or (disp_low and p in disp_low):
                    return True
        return False

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
            logger.error(f"跨站做种清理：构建自定义下载器失败 {dtype} {host}:{port}：{e}")
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
            logger.error(f"跨站做种清理：获取 MoviePilot 下载器失败：{e}")
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
            logger.error(f"跨站做种清理：获取下载器 {dl} 失败：{e}")
        return None, ""

    # ==================== 种子抓取（TR 直连 RPC / QB 并发） ====================

    def _tr_fetch_all(self, inst: Any, dl_name: str) -> Dict[str, dict]:
        """Transmission：直接 RPC 一次获取全部种子（全部 trackers / 大小 / 上传量 / 路径）"""
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
                logger.error("跨站做种清理：TR 会话握手失败（未返回 Session-Id）")
                return out
            r = sess.post(url, headers={"X-Transmission-Session-Id": sid}, auth=(user, pwd),
                          json={"method": "torrent-get",
                                "arguments": {"fields": [
                                    "hashString", "name", "totalSize", "percentDone", "status",
                                    "downloadDir", "contentPath", "uploadedEver",
                                    "trackers", "magnetLink"]}},
                          timeout=120)
            for t in r.json().get("arguments", {}).get("torrents", []):
                h = t.get("hashString")
                if not h:
                    continue
                sites = sorted({site_key(tk.get("announce", "")) for tk in (t.get("trackers") or [])
                                if isinstance(tk, dict)})
                sites = [s for s in sites if s]
                done = float(t.get("percentDone") or 0) >= 1
                active = int(t.get("status", 0) or 0) in (1, 2, 3, 4) or not done
                content = str(t.get("contentPath") or "") \
                    or (str(t.get("downloadDir") or "").rstrip("/") + "/" + str(t.get("name") or ""))
                out[h] = {
                    "dl": dl_name, "dl_type": "transmission", "hash": h,
                    "name": str(t.get("name") or ""),
                    "size": int(t.get("totalSize") or 0),
                    "uploaded": int(t.get("uploadedEver") or 0),
                    "sites": sites,
                    "active": active,
                    "magnet": str(t.get("magnetLink") or ""),
                    "save_path": str(t.get("downloadDir") or ""),
                    "data_id": content,
                }
        except Exception as e:
            logger.error(f"跨站做种清理：TR RPC 拉取种子失败：{e}")
        return out

    def _qb_fetch_all(self, inst: Any, dl_name: str) -> Dict[str, dict]:
        """qBittorrent：torrents_info + 并发取 trackers（过滤 DHT/PEX/LSD 伪 tracker，带重试回退）"""
        out = {}
        qbc = getattr(inst, "qbc", None)
        if not qbc:
            return out
        try:
            infos = list(qbc.torrents_info() or [])
        except Exception as e:
            logger.error(f"跨站做种清理：QB 拉取种子列表失败：{e}")
            return out

        def _trackers(h: str) -> List[str]:
            for attempt in range(2):
                try:
                    trs = qbc.torrents_trackers(torrent_hash=h) or []
                    return [str(x.get("url") or "") for x in trs
                            if isinstance(x, dict) and str(x.get("url") or "").startswith(("http", "udp"))]
                except Exception:
                    if attempt == 0:
                        time.sleep(0.2)
            return []

        hashes = [str(x.get("hash") or "") for x in infos]
        with ThreadPoolExecutor(max_workers=4) as ex:
            results = list(ex.map(_trackers, hashes))

        for x, urls in zip(infos, results):
            h = str(x.get("hash") or "")
            if not h:
                continue
            if not urls:
                t_field = str(x.get("tracker") or "")
                if t_field.startswith(("http", "udp")):
                    urls = [t_field]
            if not urls:
                mu = str(x.get("magnet_uri") or "")
                try:
                    qs = parse_qs(urlparse(mu).query).get("tr") or []
                    urls = [str(q) for q in qs]
                except Exception:
                    urls = []
            sites = sorted({site_key(u) for u in urls if u})
            sites = [s for s in sites if s]
            state = str(x.get("state") or "")
            active = float(x.get("progress") or 0) < 1 or state.lower() in _QB_ACTIVE_STATES
            out[h] = {
                "dl": dl_name, "dl_type": "qbittorrent", "hash": h,
                "name": str(x.get("name") or ""),
                "size": int(x.get("size") or 0),
                "uploaded": int(x.get("total_uploaded") or x.get("uploaded") or 0),
                "sites": sites,
                "active": active,
                "magnet": str(x.get("magnet_uri") or ""),
                "save_path": str(x.get("save_path") or ""),
                "data_id": str(x.get("content_path") or "")
                or (str(x.get("save_path") or "").rstrip("/") + "/" + str(x.get("name") or "")),
            }
        return out

    # ==================== 扫描分析 ====================

    def _scan_all(self) -> Tuple[Dict[str, dict], Dict]:
        """扫描全部下载器并做独苗分析，返回 (全量种子, 分析结果)"""
        items: Dict[str, dict] = {}
        for name, dtype, inst in self._iter_downloaders():
            try:
                if "transmission" in dtype:
                    d = self._tr_fetch_all(inst, name)
                elif "qb" in dtype:
                    d = self._qb_fetch_all(inst, name)
                else:
                    d = {}
                logger.info(f"跨站做种清理：下载器 {name}（{dtype}）共抓取 {len(d)} 个种子")
                for h, it in d.items():
                    if h in items:
                        k = f"{h}@{name}"
                        items[k] = dict(it, hash=k)
                    else:
                        items[h] = it
            except Exception as e:
                logger.error(f"跨站做种清理：下载器 {name} 处理异常：{e}")

        analyzed = analyze_items(items, self._group_by)
        groups = analyzed["groups"]

        del_groups: List[dict] = []
        protected_n = 0
        for gk in analyzed["single"]:
            g = groups[gk]
            entries = g["entries"]
            site_keys = set(g["sites"])
            gname = g.get("name") or ""
            if self._skip_active and any(e.get("active") for e in entries):
                protected_n += 1
                continue
            if self._site_protected(site_keys):
                protected_n += 1
                continue
            kws = self._keyword_list()
            if kws and any(k.lower() in gname.lower() for k in kws):
                protected_n += 1
                continue
            if self._protect_uploaded_gb > 0:
                total_up = sum(int(e.get("uploaded") or 0) for e in entries)
                if total_up >= self._protect_uploaded_gb * 1024 ** 3:
                    protected_n += 1
                    continue
            first_site = (list(site_keys) or [""])[0]
            del_groups.append({
                "gid": hashlib.sha1(gk.encode("utf-8", "ignore")).hexdigest()[:12],
                "name": gname,
                "site": first_site,
                "site_name": self.site_display(first_site) if first_site else "未知站点",
                "size": max([int(e.get("size") or 0) for e in entries] or [0]),
                "path": g.get("path", ""),
                "entries": [{"id": f"{e['dl']}|{e['hash']}",
                             "downloader": e["dl"], "dl_type": e.get("dl_type", ""),
                             "hash": e["hash"], "name": e.get("name", ""),
                             "size": int(e.get("size") or 0),
                             "uploaded": int(e.get("uploaded") or 0),
                             "magnet": e.get("magnet", ""),
                             "save_path": e.get("save_path", ""),
                             "data_id": e.get("data_id", "")} for e in entries],
            })

        del_seeds = sum(len(g["entries"]) for g in del_groups)
        del_size = sum(int(g["size"] or 0) for g in del_groups)
        site_stat: Dict[str, int] = {}
        for g in del_groups:
            site_stat[g["site_name"]] = site_stat.get(g["site_name"], 0) + len(g["entries"])
        result = {
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "groups": del_groups,
            "stat": {
                "total": len(items),
                "keep_groups": len(analyzed["keep"]),
                "del_groups": len(del_groups),
                "del_seeds": del_seeds,
                "del_size": del_size,
                "protected": protected_n,
                "site_stat": site_stat,
            },
        }
        return items, result

    def _schedule_scan(self):
        items, result = self._scan_all()
        self.save_data("result", result)
        stat = result.get("stat") or {}
        logger.info(f"跨站做种清理：分析完成 种子{stat.get('total', 0)} 多站保留组{stat.get('keep_groups', 0)} "
                    f"独苗组{stat.get('del_groups', 0)}（涉及{stat.get('del_seeds', 0)}种子）保护命中{stat.get('protected', 0)}")
        if self._auto_delete and result.get("groups"):
            entries = [e for g in result["groups"] for e in g["entries"]]
            ok, kept = self._delete_entries(entries)
            logger.info(f"跨站做种清理：自动清理完成 成功 {ok}/{len(entries)}，数据保留 {len(kept)} 条")
            if self._notify:
                self._send_notification(
                    f"{self._notify_title_prefix}: 跨站做种清理-自动清理 {ok}/{len(entries)} 个种子"
                    f"（释放约 {self._fmt_size(stat.get('del_size', 0))}）")
        elif self._notify and result.get("groups"):
            site_stat = stat.get("site_stat") or {}
            site_line = " / ".join(f"{k}:{v}" for k, v in
                                   sorted(site_stat.items(), key=lambda x: -x[1])[:8])
            self._send_notification(
                f"{self._notify_title_prefix}: 跨站做种清理-发现 {stat.get('del_groups', 0)} 组独苗"
                f"（{stat.get('del_seeds', 0)} 个种子 / {self._fmt_size(stat.get('del_size', 0))}）\n"
                f"多站保留 {stat.get('keep_groups', 0)} 组，保护命中 {stat.get('protected', 0)} 组\n"
                f"站点分布：{site_line or '-'}\n请到插件页面查看并删除")

    def scan_api(self) -> Dict[str, Any]:
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        logger.info("跨站做种清理：开始扫描分析...")
        items, result = self._scan_all()
        self.save_data("result", result)
        self._sel = []
        stat = result.get("stat") or {}
        logger.info(f"跨站做种清理：扫描完成 {stat}")
        if self._notify and result.get("groups"):
            self._send_notification(f"{self._notify_title_prefix}: 跨站做种清理-扫描完成，"
                                    f"独苗 {stat.get('del_groups', 0)} 组 {stat.get('del_seeds', 0)} 个种子，请到插件页面查看")
        return {"success": True, "stat": stat}

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

    def toggle_group_api(self, gid: str) -> Dict[str, Any]:
        if not gid:
            return {"success": False, "message": "未指定组"}
        saved = self.get_data("result") or {}
        groups = saved.get("groups") or []
        g = next((x for x in groups if x.get("gid") == gid), None)
        if not g:
            return {"success": False, "message": "组不存在，请重新扫描"}
        ids = [e.get("id") for e in g.get("entries", [])]
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

    def toggle_all_api(self) -> Dict[str, Any]:
        saved = self.get_data("result") or {}
        groups = saved.get("groups") or []
        ids = [e.get("id") for g in groups for e in g.get("entries", [])]
        if not ids:
            return {"success": False, "message": "当前无结果"}
        selected = self._sel
        id_set = set(ids)
        if all(i in selected for i in ids):
            selected = [p for p in selected if p not in id_set]
            msg = f"已取消勾选全部 {len(ids)} 项"
        else:
            selected = list(dict.fromkeys(ids))
            msg = f"已勾选全部 {len(ids)} 项"
        self._sel = selected
        return {"success": True, "message": msg, "selected_count": len(selected)}

    # ==================== 删除（带组外辅种保护 + 回收站备份） ====================

    def _delete_entries(self, entries: List[Dict]) -> Tuple[int, List[str]]:
        """
        按下载器分组删除；删除数据文件前检查数据是否被「本次删除范围之外」的种子引用，
        被引用则该种子降级为仅删任务。返回 (成功数, 数据保留提示列表)
        """
        by_dl: Dict[str, List[Dict]] = {}
        for e in entries:
            dl = e.get("downloader")
            if dl:
                by_dl.setdefault(dl, []).append(e)
        ok = 0
        kept: List[str] = []
        del_ids = {e.get("id") for e in entries}
        for dl, items in by_dl.items():
            inst, dtype = self._get_downloader(dl)
            if not inst:
                logger.error(f"跨站做种清理：下载器 {dl} 不可用，跳过")
                continue
            # 重新拉取全量种子（组外引用判断 + 存在性校验）
            try:
                if "transmission" in (dtype or ""):
                    all_ids = {h: it.get("data_id") or "" for h, it in self._tr_fetch_all(inst, dl).items()}
                elif "qb" in (dtype or ""):
                    all_ids = {h: it.get("data_id") or "" for h, it in self._qb_fetch_all(inst, dl).items()}
                else:
                    all_ids = {}
            except Exception as e:
                logger.error(f"跨站做种清理：获取 {dl} 全量种子失败，跳过该下载器：{e}")
                continue
            # 本下载器中本次要删的 hash 集合（用于排除组内互引）
            del_hashes = {(e.get("hash") or "").split("@")[0] for e in entries if e.get("downloader") == dl}
            groups: Dict[bool, List[str]] = {False: [], True: []}
            for it in items:
                h = it.get("hash") or ""
                h_plain = h.split("@")[0]
                if h_plain not in all_ids:
                    logger.warning(f"跨站做种清理：种子 {h_plain} 已不在下载器 {dl}，跳过")
                    continue
                data_id = all_ids.get(h_plain) or ""
                referenced = False
                if self._delete_file:
                    for oh, od in all_ids.items():
                        oh_plain = oh.split("@")[0]
                        if oh_plain == h_plain or oh_plain in del_hashes:
                            continue
                        if od and data_id and od == data_id:
                            referenced = True
                            break
                if referenced:
                    kept.append(it.get("name") or h_plain)
                    groups[False].append(h_plain)
                else:
                    groups[bool(self._delete_file)].append(h_plain)
            for delete_file, hashes in groups.items():
                if not hashes:
                    continue
                self._backup_entries(dl, dtype, inst, items, delete_file, set(hashes))
                try:
                    if inst.delete_torrents(delete_file=delete_file, ids=hashes):
                        ok += len(hashes)
                    else:
                        logger.error(f"跨站做种清理：{dl} 删除失败（delete_file={delete_file}）")
                except Exception as e:
                    logger.error(f"跨站做种清理：{dl} 删除种子异常：{e}")
        # 更新结果缓存
        try:
            saved = self.get_data("result") or {}
            groups = saved.get("groups") or []
            for g in groups:
                g["entries"] = [e for e in g.get("entries", []) if e.get("id") not in del_ids]
            saved["groups"] = [g for g in groups if g.get("entries")]
            stat = saved.get("stat") or {}
            stat["del_seeds"] = sum(len(g["entries"]) for g in saved["groups"])
            stat["del_groups"] = len(saved["groups"])
            saved["stat"] = stat
            self.save_data("result", saved)
        except Exception as e:
            logger.warning(f"跨站做种清理：更新结果缓存失败：{e}")
        self._sel = [s for s in self._sel if s not in del_ids]
        return ok, kept

    # ==================== 回收站（误删恢复） ====================

    @staticmethod
    def _recycle_dir() -> str:
        import os
        try:
            base = settings.PLUGIN_DATA_PATH
        except Exception:
            base = "/config/plugins"
        d = os.path.join(str(base), "crossseed_recycle")
        os.makedirs(d, exist_ok=True)
        return d

    @staticmethod
    def _safe_name(s: str) -> str:
        return "".join(c if c.isalnum() or c in "-_." else "_" for c in str(s or ""))

    def _backup_entries(self, dl: str, dtype: str, inst: Any, items: List[Dict],
                        delete_file: bool, hash_set: set):
        if not self._recycle_enabled:
            return
        import os
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
                    logger.warning(f"跨站做种清理：导出种子文件失败（{str(it.get('name', ''))[:30]}）：{e}")
            if not bk_type and it.get("magnet"):
                bk_type, bk_magnet = "magnet", str(it["magnet"])
            if not bk_type:
                logger.warning(f"跨站做种清理：该种子无可用备份材料（{str(it.get('name', ''))[:40]}）")
                continue
            rec[key] = {
                "downloader": dl,
                "dl_type": dtype or "",
                "hash": h_plain,
                "name": it.get("name", ""),
                "site": it.get("site", ""),
                "size": it.get("size", 0) or 0,
                "uploaded": it.get("uploaded", 0) or 0,
                "save_path": it.get("save_path", ""),
                "deleted_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "deleted_file": bool(delete_file),
                "bk_type": bk_type,
                "bk_file": bk_file,
                "bk_magnet": bk_magnet,
            }
            dirty = True
        if dirty:
            self.save_data("recycle", rec)

    def restore_api(self, id: str = "") -> Dict[str, Any]:
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        if not id:
            return {"success": False, "message": "未指定条目"}
        try:
            rec = self.get_data("recycle") or {}
        except Exception:
            rec = {}
        item = rec.get(id)
        if not item:
            return {"success": False, "message": "回收站中无此记录"}
        inst, dtype = self._get_downloader(item.get("downloader") or "")
        if not inst:
            return {"success": False, "message": f"原下载器 {item.get('downloader')} 不可用"}
        import os
        content = None
        if item.get("bk_type") == "torrent" and item.get("bk_file"):
            path = os.path.join(self._recycle_dir(), item["bk_file"])
            if not os.path.isfile(path):
                return {"success": False, "message": "备份种子文件缺失"}
            try:
                with open(path, "rb") as f:
                    content = f.read()
            except Exception as e:
                return {"success": False, "message": f"读取备份文件失败：{e}"}
        elif item.get("bk_magnet"):
            content = str(item["bk_magnet"])
        if not content:
            return {"success": False, "message": "无可用备份材料"}
        try:
            if "transmission" in (dtype or ""):
                t = inst.add_torrent(content=content, is_paused=True,
                                     download_dir=item.get("save_path") or None)
                ok = t is not None
            else:
                ok, _ = inst.add_torrent(content=content, is_paused=True,
                                         download_dir=item.get("save_path") or None)
        except Exception as e:
            return {"success": False, "message": f"添加种子失败：{e}"}
        if not ok:
            return {"success": False, "message": "添加种子失败（下载器返回失败）"}
        h = item.get("hash") or ""
        if h:
            try:
                inst.recheck_torrents(ids=[h])
            except Exception:
                pass
            try:
                inst.start_torrents(ids=[h])
            except Exception:
                pass
        rec.pop(id, None)
        self.save_data("recycle", rec)
        if item.get("bk_file"):
            try:
                os.remove(os.path.join(self._recycle_dir(), item["bk_file"]))
            except Exception:
                pass
        logger.info(f"跨站做种清理：已恢复种子 {str(item.get('name', ''))[:40]}")
        return {"success": True, "message": "恢复成功：已重新添加并校验数据"
                + ("（原数据已删，将重新下载）" if item.get("deleted_file") else "")}

    def recycle_delete_api(self, id: str = "") -> Dict[str, Any]:
        if not id:
            return {"success": False, "message": "未指定条目"}
        try:
            rec = self.get_data("recycle") or {}
        except Exception:
            rec = {}
        item = rec.pop(id, None)
        if not item:
            return {"success": False, "message": "回收站中无此记录"}
        self.save_data("recycle", rec)
        if item.get("bk_file"):
            try:
                import os
                os.remove(os.path.join(self._recycle_dir(), item["bk_file"]))
            except Exception:
                pass
        return {"success": True, "message": "已清除该备份"}

    def recycle_clear_api(self, confirm: str = "") -> Dict[str, Any]:
        if confirm != "yes":
            return {"success": False, "message": "请确认参数 confirm=yes"}
        self.save_data("recycle", {})
        try:
            import os
            d = self._recycle_dir()
            for fn in os.listdir(d):
                if fn.endswith(".torrent"):
                    try:
                        os.remove(os.path.join(d, fn))
                    except Exception:
                        pass
        except Exception:
            pass
        return {"success": True, "message": "回收站已清空"}

    # ==================== 删除 API ====================

    def delete_api(self, id: str, confirm: str = "") -> Dict[str, Any]:
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        if confirm != "yes":
            return {"success": False, "message": "请确认参数 confirm=yes"}
        saved = self.get_data("result") or {}
        groups = saved.get("groups") or []
        target = None
        for g in groups:
            for e in g.get("entries", []):
                if e.get("id") == id:
                    target = e
                    break
            if target:
                break
        if not target:
            return {"success": False, "message": "条目不存在，请重新扫描"}
        ok, _ = self._delete_entries([target])
        return {"success": ok > 0, "message": f"已删除 {ok} 个" if ok else "删除失败"}

    def delete_selected_api(self, confirm: str = "") -> Dict[str, Any]:
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        if confirm != "yes":
            return {"success": False, "message": "请确认参数 confirm=yes"}
        selected = self._sel
        if not selected:
            return {"success": False, "message": "未勾选任何条目"}
        saved = self.get_data("result") or {}
        groups = saved.get("groups") or []
        targets = [e for g in groups for e in g.get("entries", []) if e.get("id") in selected]
        if not targets:
            return {"success": False, "message": "勾选条目不在结果中，请重新扫描"}
        ok, kept = self._delete_entries(targets)
        logger.info(f"跨站做种清理：删除勾选完成 {ok}/{len(targets)}")
        if self._notify and ok:
            self._send_notification(f"{self._notify_title_prefix}: 跨站做种清理-删除勾选 {ok}/{len(targets)} 个种子"
                                    + (f"，{len(kept)} 条数据因组外辅种引用改为仅删任务" if kept else ""))
        return {"success": True, "ok": ok, "total": len(targets)}

    def delete_all_api(self, confirm: str = "") -> Dict[str, Any]:
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        if confirm != "yes":
            return {"success": False, "message": "请确认参数 confirm=yes"}
        saved = self.get_data("result") or {}
        groups = saved.get("groups") or []
        targets = [e for g in groups for e in g.get("entries", [])]
        if not targets:
            return {"success": False, "message": "当前无结果"}
        ok, kept = self._delete_entries(targets)
        logger.info(f"跨站做种清理：全部删除完成 {ok}/{len(targets)}")
        if self._notify and ok:
            self._send_notification(f"{self._notify_title_prefix}: 跨站做种清理-删除全部独苗 {ok}/{len(targets)} 个种子"
                                    + (f"，{len(kept)} 条数据因组外辅种引用改为仅删任务" if kept else ""))
        return {"success": True, "ok": ok, "total": len(targets)}

    def clear_api(self, confirm: str = "") -> Dict[str, Any]:
        if confirm != "yes":
            return {"success": False, "message": "请确认参数 confirm=yes"}
        self.save_data("result", {"time": "", "groups": [], "stat": {}})
        self._sel = []
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
            "delete_file": self._delete_file,
            "auto_delete": self._auto_delete,
            "notify": self._notify,
            "webhook": self._webhook,
            "notify_title_prefix": self._notify_title_prefix,
            "group_by": self._group_by,
            "skip_active": self._skip_active,
            "protected_sites": self._protected_sites,
            "protected_keywords": self._protected_keywords,
            "protect_uploaded_gb": self._protect_uploaded_gb,
            "recycle_enabled": self._recycle_enabled,
            "custom_downloaders": self._custom_downloaders,
        })

    def _send_notification(self, text: str):
        if self._webhook:
            try:
                resp = requests.post(self._webhook,
                                     json={"msg_type": "text", "content": {"text": text}}, timeout=15)
                if resp.status_code == 200 and resp.json().get("code") == 0:
                    logger.info("跨站做种清理飞书通知发送成功")
                    return
                else:
                    logger.error(f"跨站做种清理飞书通知发送失败：{resp.text[:200]}")
            except Exception as e:
                logger.error(f"跨站做种清理飞书通知发送异常：{e}")
        self.post_message(title="跨站做种清理", text=text)
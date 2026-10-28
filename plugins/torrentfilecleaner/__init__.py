import os
from datetime import datetime, timedelta
from typing import Any, List, Dict, Tuple, Optional

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.helper.downloader import DownloaderHelper
from app.log import logger
from app.plugins import _PluginBase

import requests


class TorrentFileCleaner(_PluginBase):
    """
    种子文件清理插件（v1.4：扫描结果页面展示，手动勾选删除/可选自动删除）

    解决问题：下载器中存在数据文件已被删除（手工清理或迁移）的种子任务，
    这些"空壳"种子占用下载器资源、影响做种统计。

    工作原理：
    定时/手动扫描下载器全部种子，逐一检查其数据文件/目录是否仍存在于下载目录中
    （MoviePilot 容器与下载器需共享下载目录挂载或配置路径映射），
    不存在的为"无数据文件种子"候选；默认展示在插件详情页供人工确认删除
    （仅删任务不删文件），可开启「定时任务自动删除」回退为自动化（v1.4）。

    支持的下载器：
    - Transmission：download_dir + name 检查，兼容 .part 后缀，删除用 id
    - qBittorrent：content_path（优先）/ save_path + name 检查，兼容 .!qB 后缀，删除用 hash

    v1.2 按下载器粒度控制：
    - 每个下载器独立"是否执行清理"开关（配置页动态列出全部下载器）
    - 每个下载器独立路径映射（格式：/downloads=/media，分号分隔多对）
    - 插件级映射未配置时自动继承 MP 下载器设置中的系统级 path_mapping

    安全设计：
    1. 路径映射：容器挂载点不一致时通过前缀映射转换（插件级 > MP系统级）
    2. 全量不可达保护：某下载器全部种子（>=3）路径均不存在时判定挂载问题，跳过并通知
    3. 仅删任务不删文件：delete_torrents(delete_file=False)
    """

    # 插件元数据
    plugin_name = "种子文件清理"
    plugin_desc = ("定期扫描 Transmission / qBittorrent 种子，检测数据文件已被删除的种子任务"
                   "并自动清理（仅删任务不删文件）。每个下载器可独立开关与配置容器路径映射，"
                   "支持全量不可达保护，飞书通知。")
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/clean.png"
    plugin_version = "1.4.0"
    plugin_author = "Finn"
    author_url = "https://github.com"
    plugin_config_prefix = "torrentfilecleaner_"
    plugin_order = 32
    auth_level = 1

    # 私有属性（配置）
    _enabled = False
    _onlyonce = False
    _cron = "0 */6 * * *"
    _notify = True
    _webhook = ""
    _notify_title_prefix = "HA通知"
    _dl_enable: Dict[str, bool] = {}    # {下载器服务名: 是否执行清理}
    _dl_maps_raw: Dict[str, str] = {}   # {下载器服务名: 映射原始字符串}
    _auto_delete = False                # 定时扫描后自动删除（默认 False：只展示到页面，人工确认删）（v1.4）
    _scheduler = None
    _sel: List[str] = []                # 当前勾选的条目 id（内存，避免每次点击写库闪烁）（v1.4）

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled", False)
            self._onlyonce = config.get("onlyonce", False) or False
            self._cron = config.get("cron") or "0 */6 * * *"
            self._notify = bool(config.get("notify", True))
            self._webhook = config.get("webhook", "")
            self._notify_title_prefix = config.get("notify_title_prefix") or "HA通知"
            self._auto_delete = bool(config.get("auto_delete", False))  # v1.4
            # 解析按下载器的开关与映射（key: enable_<name> / map_<name>）
            self._dl_enable = {}
            self._dl_maps_raw = {}
            for key, val in config.items():
                if key.startswith("enable_"):
                    self._dl_enable[key[7:]] = bool(val)
                elif key.startswith("map_"):
                    self._dl_maps_raw[key[4:]] = str(val or "")
            enabled_dl = [k for k, v in self._dl_enable.items() if v]
            disabled_dl = [k for k, v in self._dl_enable.items() if not v]
            logger.info(f"种子文件清理：下载器开关已加载 - 启用={enabled_dl or '(未配置,默认全部)'}"
                        f" 禁用={disabled_dl}")

            self._sel = []  # v1.4：页面加载时重置勾选

        # 停止现有任务
        self.__stop_service()

        if self._enabled:
            # 注册定时任务
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            try:
                self._scheduler.add_job(
                    func=self._scan_clean,
                    trigger=CronTrigger.from_crontab(self._cron),
                    name="种子文件清理",
                )
                logger.info(f"种子文件清理定时任务注册成功：cron={self._cron}")
            except Exception as e:
                logger.error(f"种子文件清理定时任务注册失败：{e}")

            # 立即运行一次
            if self._onlyonce:
                logger.info("种子文件清理服务启动，立即运行一次")
                self._scheduler.add_job(
                    func=self._scan_clean,
                    trigger="date",
                    run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=5),
                    name="种子文件清理-立即运行",
                )
                # 关闭一次性开关
                self._onlyonce = False
                self.__update_config()

            # 启动调度器（否则 job 挂起不执行）
            if self._scheduler.get_jobs():
                self._scheduler.start()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {"path": "/scan", "endpoint": self.scan_api, "methods": ["GET"], "summary": "扫描无数据文件种子（只分析不删）"},
            {"path": "/toggle", "endpoint": self.toggle_api, "methods": ["GET"], "summary": "切换条目勾选"},
            {"path": "/toggle_dl", "endpoint": self.toggle_dl_api, "methods": ["GET"], "summary": "按下载器组全选/取消"},
            {"path": "/delete", "endpoint": self.delete_api, "methods": ["GET"], "summary": "删除单个种子任务"},
            {"path": "/delete_selected", "endpoint": self.delete_selected_api, "methods": ["GET"], "summary": "删除勾选条目"},
            {"path": "/delete_all", "endpoint": self.delete_all_api, "methods": ["GET"], "summary": "删除全部/本组种子任务"},
            {"path": "/clear", "endpoint": self.clear_api, "methods": ["GET"], "summary": "清空扫描结果"},
        ]

    @staticmethod
    def __get_downloader_configs() -> Dict[str, Any]:
        """
        获取已配置的下载器（仅读取配置，不实际连接）
        """
        try:
            return DownloaderHelper().get_configs()
        except Exception as e:
            logger.warning(f"种子文件清理：获取下载器配置失败：{e}")
            return {}

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        插件配置页面：
        - 基础区：每行3列等宽
        - 下载器区：动态生成，每个下载器一行（独立开关 + 独立路径映射）
        - Webhook 独占整行
        """
        dl_configs = self.__get_downloader_configs()

        # 动态生成每个下载器的配置行
        dl_rows = []
        for name, conf in dl_configs.items():
            dl_type = getattr(conf, "type", "") or ""
            type_label = "TR" if "transmission" in dl_type else ("qB" if "qbittorrent" in dl_type else dl_type)
            dl_rows.append({
                'component': 'VRow',
                'content': [
                    {
                        'component': 'VCol',
                        'props': {'cols': 12, 'md': 4},
                        'content': [
                            {
                                'component': 'VSwitch',
                                'props': {'model': f'enable_{name}', 'label': f'清理 {name}（{type_label}）'}
                            }
                        ]
                    },
                    {
                        'component': 'VCol',
                        'props': {'cols': 12, 'md': 8},
                        'content': [
                            {
                                'component': 'VTextField',
                                'props': {
                                    'model': f'map_{name}',
                                    'label': f'{name} 路径映射（选填）',
                                    'placeholder': '下载器路径=MP容器路径，如：/downloads=/media（多对分号分隔）'
                                }
                            }
                        ]
                    },
                ]
            })

        # 默认数据（含每个下载器的开关与映射 key）
        defaults = {
            "enabled": False,
            "onlyonce": False,
            "cron": "0 */6 * * *",
            "notify": True,
            "webhook": "",
            "notify_title_prefix": "HA通知",
            "auto_delete": False,
        }
        for name in dl_configs:
            defaults[f"enable_{name}"] = True   # 默认启用（与历史版本行为一致）
            defaults[f"map_{name}"] = ""

        return [
            {
                'component': 'VForm',
                'content': [
                    # Row 1: enabled / notify / onlyonce
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {'model': 'enabled', 'label': '启用插件'}
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {'model': 'notify', 'label': '发送通知'}
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {'model': 'onlyonce', 'label': '立即运行一次'}
                                    }
                                ]
                            },
                        ]
                    },
                    # Row 2: cron / notify_title_prefix
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 8},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'cron',
                                            'label': '执行周期(Cron)',
                                            'placeholder': '0 */6 * * *（每6小时）'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'notify_title_prefix',
                                            'label': '通知前缀',
                                            'placeholder': 'HA通知'
                                        }
                                    }
                                ]
                            },
                        ]
                    },
                    # Row 2.5: 删除模式（v1.4）
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 12},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'auto_delete',
                                            'label': '定时扫描后自动删除（关闭则只展示到插件详情页，人工勾选删除，默认）'
                                        }
                                    }
                                ]
                            },
                        ]
                    },
                    # 下载器分区标题
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 12},
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '下载器执行范围：每个下载器独立开关；路径映射用于容器挂载点不一致的场景（未配置时自动继承 MoviePilot 下载器设置中的路径映射）'
                                        }
                                    }
                                ]
                            },
                        ]
                    },
                    # 动态下载器行
                    *dl_rows,
                    # Webhook 独占整行
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 12},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'webhook',
                                            'label': '飞书 Webhook（留空走系统消息）',
                                            'placeholder': 'https://open.feishu.cn/open-apis/bot/v2/hook/xxx'
                                        }
                                    }
                                ]
                            },
                        ]
                    },
                ]
            }
        ], defaults

    def get_page(self) -> Optional[List[dict]]:
        """插件详情页（v1.4）：本轮扫描结果按下载器分组展示，勾选/单删/批量删"""
        if not self._enabled:
            return [{'component': 'VAlert',
                     'props': {'type': 'warning', 'text': '插件未启用，请先到「设置」页启用', 'variant': 'tonal'}}]

        saved = self.get_data("result") or {}
        items = saved.get("items") or []
        stats = saved.get("stats") or []
        skipped = saved.get("skipped") or []
        scan_time = saved.get("time", "")
        selected = self._sel
        selected_set = set(selected)
        mode_label = "定时扫描后自动删除" if self._auto_delete else "手动确认删除（扫描只展示不删）"

        page = []
        page.append({
            'component': 'VAlert',
            'props': {'type': 'info', 'variant': 'tonal', 'density': 'compact', 'class': 'mb-2',
                      'text': f"扫描时间：{scan_time or '尚未扫描'}   待清理 {len(items)} 项   "
                              f"已勾选 {len(selected_set)} 项   模式：{mode_label}"}
        })

        page.append({
            'component': 'VRow',
            'props': {'class': 'mb-1'},
            'content': [
                {'component': 'VCol', 'props': {'cols': 12, 'md': 3},
                 'content': [self._btn("扫描", 'plugin/TorrentFileCleaner/scan', {}, 'primary', 'elevated')]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 3},
                 'content': [self._btn(f"删除选中（{len(selected_set)}）", 'plugin/TorrentFileCleaner/delete_selected',
                                       {'confirm': 'yes'}, 'error', 'elevated')]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 3},
                 'content': [self._btn("删除全部", 'plugin/TorrentFileCleaner/delete_all',
                                       {'confirm': 'yes'}, 'error', 'text')]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 3},
                 'content': [self._btn("清空结果", 'plugin/TorrentFileCleaner/clear',
                                       {'confirm': 'yes'}, 'secondary', 'text')]},
            ]
        })

        page.append({
            'component': 'div',
            'props': {'class': 'text-caption text-grey mb-2'},
            'text': "候选 = 数据文件已不存在的种子任务（TR 检查 .part、qB 检查 .!qB）；删除仅移除任务不删数据文件；某下载器全部路径不可达时整组跳过（见下方警示）。"
        })

        for sk in skipped:
            page.append({'component': 'VAlert', 'props': {'type': 'warning', 'variant': 'tonal',
                                                          'density': 'compact', 'class': 'mb-1', 'text': sk}})

        if not items:
            page.append({'component': 'div', 'text': '暂无结果，请点击「扫描」分析无数据文件种子',
                         'props': {'class': 'text-center text-grey mt-6'}})
        else:
            # 按下载器分组
            groups = {}
            for it in items:
                groups.setdefault(it.get("downloader") or "未知下载器", []).append(it)
            for dl in sorted(groups.keys()):
                g_items = groups[dl]
                dl_type = g_items[0].get("dl_type", "?") if g_items else "?"
                sel_in_group = sum(1 for it in g_items if it.get("id") in selected_set)
                all_selected = sel_in_group == len(g_items)
                page.append({
                    'component': 'VRow',
                    'props': {'class': 'ma-0 mt-3 mb-1 py-2 px-2 d-flex align-center',
                              'style': 'background: rgba(2,136,209,0.10); border-radius: 8px;'},
                    'content': [
                        {'component': 'VCol', 'props': {'cols': 12, 'md': 1},
                         'content': [{'component': 'VCheckboxBtn',
                                      'props': {'model-value': all_selected, 'density': 'compact'},
                                      'events': {'click': {'api': 'plugin/TorrentFileCleaner/toggle_dl',
                                                          'method': 'get',
                                                          'params': {'dl': dl, 'apikey': settings.API_TOKEN}}}}]},
                        {'component': 'VCol', 'props': {'cols': 12, 'md': 7},
                         'content': [
                             {'component': 'div', 'props': {'class': 'text-subtitle-2 font-weight-bold text-primary'},
                              'text': f"⬇ {dl}（{dl_type}）"},
                             {'component': 'div', 'props': {'class': 'text-caption text-grey'},
                              'text': f"{len(g_items)} 项 · 已勾选 {sel_in_group}"}]},
                        {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                         'content': [self._btn("删除本组全部", 'plugin/TorrentFileCleaner/delete_all',
                                               {'dl': dl, 'confirm': 'yes'}, 'error', 'tonal')]},
                    ]
                })
                for it in sorted(g_items, key=lambda x: x.get("name", "")):
                    idx = it.get("id", "")
                    checked = idx in selected_set
                    row_style = 'background: rgba(76,175,80,0.12); border-radius: 6px;' if checked else ''
                    h = str(it.get("hash", ""))
                    sub = it.get("path", "") or "—"
                    if it.get("size"):
                        sub += f" · {self._fmt_size(it.get('size'))}"
                    if h:
                        sub += f" · hash {h[:12]}"
                    page.append({
                        'component': 'VRow',
                        'props': {'class': 'ma-0 py-1 px-2 d-flex align-center', 'style': row_style},
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 1},
                             'content': [{'component': 'VCheckboxBtn', 'props': {'model-value': checked, 'density': 'compact'},
                                          'events': {'click': {'api': 'plugin/TorrentFileCleaner/toggle',
                                                              'method': 'get',
                                                              'params': {'id': idx, 'apikey': settings.API_TOKEN}}}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 7},
                             'content': [
                                 {'component': 'div', 'props': {'class': 'text-body-2'},
                                  'text': it.get("name", "")},
                                 {'component': 'div', 'props': {'class': 'text-caption text-grey text-truncate'},
                                  'props_style': '', 'text': sub}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 2},
                             'content': [
                                 {'component': 'div', 'props': {'class': 'text-caption text-grey'}, 'text': "保存目录"},
                                 {'component': 'div', 'props': {'class': 'text-caption text-grey-darken-1 text-truncate'},
                                  'text': it.get("save_dir", "") or "—"}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 2},
                             'content': [self._btn("删除", 'plugin/TorrentFileCleaner/delete',
                                                   {'id': idx, 'confirm': 'yes'}, 'error', 'text')]},
                        ]
                    })
        if stats:
            stat_lines = []
            for s in stats:
                stat_lines.append(f"{s.get('downloader')}({s.get('dl_type')}): {s.get('scanned', 0)} 个种子"
                                  + (f"，发现 {s.get('found', 0)} 个" if s.get('found') else "，正常"))
            page.append({'component': 'div', 'props': {'class': 'text-caption text-grey mt-4 mb-2'},
                         'text': "各下载器明细：" + " / ".join(stat_lines)})
        page.append({'component': 'div', 'props': {'class': 'text-caption text-grey mt-2'},
                     'text': "删除立即生效：仅从下载器移除任务，不会删除磁盘上的任何数据文件。"})
        return page

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

    def stop_service(self):
        """
        停止插件服务（由插件管理器调用）
        """
        self.__stop_service()

    def __stop_service(self):
        if self._scheduler:
            self._scheduler.remove_all_jobs()
            if self._scheduler.running:
                try:
                    self._scheduler.shutdown()
                except Exception as e:
                    logger.warning(f"种子文件清理：停止调度器异常：{e}")
            self._scheduler = None

    def __update_config(self):
        cfg = {
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "notify": self._notify,
            "webhook": self._webhook,
            "notify_title_prefix": self._notify_title_prefix,
            "auto_delete": self._auto_delete,
        }
        for name, val in self._dl_enable.items():
            cfg[f"enable_{name}"] = val
        for name, val in self._dl_maps_raw.items():
            cfg[f"map_{name}"] = val
        self.update_config(cfg)

    # ==================== 路径工具 ====================

    @staticmethod
    def __parse_maps(raw: str) -> List[Tuple[str, str]]:
        """
        解析映射字符串：格式 "old=new;old2=new2"
        """
        maps = []
        for pair in (raw or "").split(";"):
            if "=" in pair:
                old, new = pair.split("=", 1)
                old, new = old.strip(), new.strip()
                if old and new:
                    maps.append((old, new))
        return maps

    @staticmethod
    def __apply_path_maps(path: str, maps: List[Tuple[str, str]]) -> str:
        """
        应用路径映射：前缀匹配替换
        """
        for old, new in maps:
            if path.startswith(old):
                return new + path[len(old):]
        return path

    def __get_downloader_maps(self, name: str, info) -> List[Tuple[str, str]]:
        """
        获取下载器的有效路径映射：插件级 > MP 系统级(path_mapping)
        """
        # 1) 插件级映射
        maps = self.__parse_maps(self._dl_maps_raw.get(name, ""))
        if maps:
            return maps
        # 2) 继承 MP 下载器设置中的系统级 path_mapping
        try:
            conf = getattr(info, "config", None)
            sys_maps = getattr(conf, "path_mapping", None) or []
            result = []
            for m in sys_maps:
                if isinstance(m, (list, tuple)) and len(m) == 2:
                    old, new = str(m[0]), str(m[1])
                    if old and new:
                        result.append((old, new))
            if result:
                logger.info(f"种子文件清理：[{name}] 使用 MoviePilot 系统级路径映射 {result}")
            return result
        except Exception as e:
            logger.warning(f"种子文件清理：读取 {name} 系统级路径映射失败：{e}")
            return []

    # ==================== 核心扫描清理逻辑 ====================

    def _scan_clean(self):
        """
        定时任务入口（v1.4 重构）：扫描收集 → 结果落库展示。
        - 手动模式（默认）：只展示候选与统计，发发现通知，不删除
        - 开启 auto_delete：直接删除并回执（v1.3 前的自动行为）
        """
        try:
            items, stats, skipped = self._scan_collect()
            if items is None:
                if self._notify:
                    self._send_notification(
                        f"{self._notify_title_prefix}: 种子文件清理-未找到已连接的下载器，跳过本轮扫描")
                return
            self._sel = []
            self._save_result(items, stats, skipped)
            auto_ok = None
            if self._auto_delete and items:
                auto_ok, deleted_ids = self._delete_entries(items)
                rest = [it for it in items if it.get("id") not in set(deleted_ids)]
                self._save_result(rest, stats, skipped, keep_time=True)
                logger.info(f"种子文件清理：本轮自动清理 {auto_ok}/{len(items)} 个无数据种子任务")
            if self._notify:
                if auto_ok is not None:
                    deleted_set = set(deleted_ids)
                    shown = [it for it in items if it.get("id") in deleted_set]
                    self._send_notification(self._scan_notify_text(shown, stats, skipped, auto_ok=auto_ok))
                else:
                    self._send_notification(self._scan_notify_text(items, stats, skipped))
        except Exception as e:
            logger.error(f"种子文件清理：扫描清理失败：{e}")
            if self._notify:
                self._send_notification(
                    f"{self._notify_title_prefix}: 种子文件清理-扫描失败\n- 错误信息：{e}")

    def _save_result(self, items: List[Dict], stats: List[Dict], skipped: List[str],
                     keep_time: bool = False):
        """保存本轮扫描结果（v1.4）；keep_time=True 保留上次扫描时间戳"""
        saved = self.get_data("result") or {}
        self.save_data("result", {
            "time": saved.get("time") if keep_time else datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "items": items, "total": len(items),
            "stats": stats, "skipped": skipped})

    def _scan_collect(self):
        """扫描全部下载器，收集无数据文件种子候选（v1.4 起不直接删除，展示到详情页）。
        返回 (items, stats, skipped)；items=None 表示无已连接下载器。"""
        items, stats, skipped = [], [], []
        services = DownloaderHelper().get_services() or {}
        if not services:
            logger.warning("种子文件清理：未找到已连接的下载器")
            return None, stats, skipped
        for name, info in services.items():
            inst = getattr(info, "instance", None)
            if not inst:
                continue
            mod = type(inst).__module__
            dl_type = "TR" if "transmission" in mod else ("qB" if "qbittorrent" in mod else "?")
            # 用户开关过滤（未配置的下载器默认执行）
            if name in self._dl_enable and not self._dl_enable[name]:
                logger.info(f"种子文件清理：下载器 {name} 已被用户禁用，跳过")
                stats.append({"downloader": name, "dl_type": dl_type, "scanned": 0, "found": 0, "disabled": True})
                continue
            maps = self.__get_downloader_maps(name, info)
            if "transmission" in mod:
                result = self.__scan_transmission(name, inst, maps)
            elif "qbittorrent" in mod:
                result = self.__scan_qbittorrent(name, inst, maps)
            else:
                continue
            if result is None:
                stats.append({"downloader": name, "dl_type": dl_type, "scanned": 0, "found": 0, "failed": True})
                continue
            sub_items, skipped_reason, scanned = result
            st = {"downloader": name, "dl_type": dl_type, "scanned": scanned, "found": len(sub_items)}
            if skipped_reason:
                skipped.append(f"[{name}({dl_type})] {skipped_reason}")
                st["skipped"] = True
            items.extend(sub_items)
            stats.append(st)
        return items, stats, skipped

    def _scan_notify_text(self, items: List[Dict], stats: List[Dict], skipped: List[str],
                          auto_ok: Optional[int] = None) -> str:
        """构建扫描回执文本（v1.4：手动发现提示 / 自动清理明细，每轮必发）"""
        lines = []
        if auto_ok is not None:
            lines.append(f"{self._notify_title_prefix}: 种子文件清理-已自动清理 {auto_ok} 个无数据种子任务")
            lines.append("- 以下种子的数据文件已不存在，已从下载器移除任务（未删除文件）：")
            lines.extend(f"  ▶ 《{it.get('name')}》 [{it.get('downloader')}/{it.get('dl_type')}]" for it in items)
        elif items:
            lines.append(f"{self._notify_title_prefix}: 种子文件清理-发现 {len(items)} 个无数据文件种子")
            lines.append("- 数据文件已不存在的种子已展示在插件详情页，请到页面确认删除（仅删任务不删文件）：")
            lines.extend(f"  ▶ 《{it.get('name')}》 [{it.get('downloader')}/{it.get('dl_type')}]" for it in items[:20])
            if len(items) > 20:
                lines.append(f"  ▶ ...等共 {len(items)} 个，详见插件详情页")
        else:
            total_scanned = sum(s.get("scanned", 0) for s in stats)
            lines.append(f"{self._notify_title_prefix}: 种子文件清理-本轮扫描完成（未发现无数据文件种子）")
            lines.append(f"- 共扫描 {len(stats)} 个下载器 {total_scanned} 个种子")
        if skipped:
            lines.append("")
            lines.append("⚠ 以下下载器疑似路径不可达已跳过（请检查容器挂载或配置该下载器路径映射）：")
            lines.extend(f"  ▶ {x}" for x in skipped)
        if stats:
            lines.append("")
            lines.append("- 各下载器明细：")
            for s in stats:
                if s.get("disabled"):
                    line = f"{s['downloader']}({s['dl_type']}): 已禁用，跳过"
                elif s.get("failed"):
                    line = f"{s['downloader']}({s['dl_type']}): 连接失败，未扫描"
                elif s.get("skipped"):
                    line = f"{s['downloader']}({s['dl_type']}): {s['scanned']} 个种子，路径不可达已跳过"
                elif s.get("found"):
                    line = f"{s['downloader']}({s['dl_type']}): {s['scanned']} 个种子，发现 {s['found']} 个"
                else:
                    line = f"{s['downloader']}({s['dl_type']}): {s['scanned']} 个种子，正常"
                lines.append(f"  ▶ {line}")
        return "\n".join(lines)

    def __scan_transmission(self, name: str, inst, maps: List[Tuple[str, str]]):
        """收集 TR 中数据文件不存在的种子（v1.4：只收集不删）。
        返回 (items, 跳过原因, 扫描数)；获取种子失败返回 None
        TR 删除用 sha1 hash（hash_string/hashString），数字 id 会被 RPC 拒收（v1.3.1）"""
        torrents, err = inst.get_torrents()
        if err:
            logger.error(f"种子文件清理：获取 {name}(TR) 种子列表失败，跳过")
            return None
        logger.info(f"种子文件清理：开始扫描 {name}(TR)，共 {len(torrents)} 个种子")
        items = []
        missing = 0
        sample_dir = ""
        for t in torrents:
            download_dir = str(t.download_dir or "")
            torrent_name = str(t.name or "")
            if not download_dir or not torrent_name:
                continue
            if not sample_dir:
                sample_dir = download_dir
            full_path = self.__apply_path_maps(os.path.join(download_dir, torrent_name), maps)
            part_path = full_path + ".part"
            if os.path.exists(full_path) or os.path.exists(part_path):
                continue
            missing += 1
            h = self.__tr_hash(t)
            try:
                size = int(getattr(t, "size", 0) or 0)
            except Exception:
                size = 0
            logger.info(f"种子文件清理：[{name}] 检测到无数据文件种子 "
                        f"- 《{torrent_name}》(path={full_path}, hash={h[:12]})")
            items.append({"id": f"{name}|{h}", "downloader": name, "dl_type": "TR",
                          "name": torrent_name, "path": full_path, "hash": h,
                          "size": size, "save_dir": download_dir})
        # 全量不可达保护：全部（>=3）路径不存在 = 疑似挂载问题而非数据丢失
        if missing >= 3 and missing == len(torrents):
            logger.warning(f"种子文件清理：{name}(TR) 全部 {missing} 个种子路径均不可达，"
                           f"疑似容器挂载不一致，跳过防止误删")
            return [], (f"全部 {missing} 个种子路径不可达（如 {sample_dir}），"
                        f"请检查容器挂载或配置该下载器路径映射"), len(torrents)
        return items, None, len(torrents)

    @staticmethod
    def __tr_hash(t) -> str:
        """v1.3.1：取 TR 种子的 sha1 hash（兼容新老字段名 hash_string / hashString），无则回退数字 id"""
        h = getattr(t, "hash_string", None) or getattr(t, "hashString", None)
        return str(h or getattr(t, "id", ""))

    def __scan_qbittorrent(self, name: str, inst, maps: List[Tuple[str, str]]):
        """收集 qB 中数据文件不存在的种子（v1.4：只收集不删）。
        返回 (items, 跳过原因, 扫描数)；获取种子失败返回 None
        qB 候选路径：content_path（优先）/ save_path+name，兼容 .!qB 后缀"""
        torrents, err = inst.get_torrents()
        if err:
            logger.error(f"种子文件清理：获取 {name}(qB) 种子列表失败，跳过")
            return None
        logger.info(f"种子文件清理：开始扫描 {name}(qB)，共 {len(torrents)} 个种子")
        items = []
        missing = 0
        sample_dir = ""
        for t in torrents:
            d = t if isinstance(t, dict) else {}
            torrent_name = str(d.get("name") or "")
            torrent_hash = str(d.get("hash") or "")
            if not torrent_name or not torrent_hash:
                continue
            content_path = str(d.get("content_path") or "")
            save_path = str(d.get("save_path") or "")
            if not sample_dir and save_path:
                sample_dir = save_path
            candidates = []
            if content_path:
                candidates.append(self.__apply_path_maps(content_path, maps))
            if save_path and torrent_name:
                candidates.append(self.__apply_path_maps(os.path.join(save_path, torrent_name), maps))
            all_candidates = []
            for p in candidates:
                all_candidates.append(p)
                all_candidates.append(p + ".!qB")
            if any(os.path.exists(p) for p in all_candidates):
                continue
            missing += 1
            try:
                size = int(d.get("size") or 0)
            except Exception:
                size = 0
            logger.info(f"种子文件清理：[{name}] 检测到无数据文件种子 "
                        f"- 《{torrent_name}》(path={candidates[0] if candidates else '?'}, hash={torrent_hash[:12]})")
            items.append({"id": f"{name}|{torrent_hash}", "downloader": name, "dl_type": "qB",
                          "name": torrent_name, "path": candidates[0] if candidates else "",
                          "hash": torrent_hash, "size": size, "save_dir": save_path})
        # 全量不可达保护
        if missing >= 3 and missing == len(torrents):
            logger.warning(f"种子文件清理：{name}(qB) 全部 {missing} 个种子路径均不可达，"
                           f"疑似容器挂载不一致，跳过防止误删")
            map_hint = sample_dir or "qB 保存目录"
            return [], (f"全部 {missing} 个种子路径不可达（如 {map_hint}），"
                        f"请配置该下载器路径映射（如 {map_hint}=MP容器对应路径）"), len(torrents)
        return items, None, len(torrents)

    # ==================== 删除入口（v1.4） ====================

    def _delete_entries(self, entries: List[Dict]) -> Tuple[int, List[str]]:
        """按下载器分组删除种子任务（仅删任务不删文件；TR/qB 均传 hash）。
        返回 (成功条数, 已删除的条目 id 列表)"""
        by_dl = {}
        for it in entries:
            if it.get("downloader") and it.get("hash"):
                by_dl.setdefault(it["downloader"], []).append(it)
        ok, deleted = 0, []
        services = DownloaderHelper().get_services() or {}
        for dl, group in by_dl.items():
            info = services.get(dl)
            inst = getattr(info, "instance", None) if info else None
            if not inst:
                logger.error(f"种子文件清理：下载器 {dl} 不可用，跳过 {len(group)} 条删除")
                continue
            hashes = [it["hash"] for it in group]
            try:
                success = inst.delete_torrents(delete_file=False, ids=hashes)
            except Exception as e:
                logger.error(f"种子文件清理：从 {dl} 删除种子任务异常：{e}")
                continue
            if success:
                ok += len(hashes)
                deleted.extend(it["id"] for it in group)
                logger.info(f"种子文件清理：已从 {dl} 清理 {len(hashes)} 个无数据种子任务")
            else:
                logger.error(f"种子文件清理：从 {dl} 删除种子任务失败")
        return ok, deleted

    def _remove_from_result(self, ids: List[str]):
        """从结果中移除已删除条目并同步勾选集合"""
        id_set = set(ids)
        saved = self.get_data("result") or {}
        saved["items"] = [it for it in saved.get("items", []) if it.get("id") not in id_set]
        saved["total"] = len(saved["items"])
        # 同步统计中的 found 数
        for s in saved.get("stats", []):
            if isinstance(s, dict):
                s["found"] = sum(1 for it in saved["items"] if it.get("downloader") == s.get("downloader"))
        self.save_data("result", saved)
        self._sel = [x for x in self._sel if x not in id_set]

    def scan_api(self) -> Dict[str, Any]:
        """手动扫描（v1.4：只分析不删除，结果展示到详情页）"""
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        items, stats, skipped = self._scan_collect()
        if items is None:
            return {"success": False, "message": "未找到已连接的下载器"}
        self._sel = []
        self._save_result(items, stats, skipped)
        logger.info(f"种子文件清理：扫描完成，发现 {len(items)} 个无数据文件种子")
        if self._notify:
            self._send_notification(self._scan_notify_text(items, stats, skipped))
        return {"success": True, "total": len(items), "items": items,
                "message": f"扫描完成，发现 {len(items)} 个无数据文件种子，请到插件详情页确认删除"}

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

    def toggle_dl_api(self, dl: str) -> Dict[str, Any]:
        """按下载器组全选/取消"""
        if not dl:
            return {"success": False, "message": "未指定下载器"}
        saved = self.get_data("result") or {}
        ids = [it.get("id") for it in saved.get("items", []) if it.get("downloader") == dl]
        if not ids:
            return {"success": False, "message": "该下载器无条目"}
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

    def delete_api(self, id: str, confirm: str = "") -> Dict[str, Any]:
        """删除单条（仅删任务不删文件）"""
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        if confirm != "yes":
            return {"success": False, "message": "请确认参数 confirm=yes"}
        if not id:
            return {"success": False, "message": "未指定条目"}
        saved = self.get_data("result") or {}
        target = next((it for it in saved.get("items", []) if it.get("id") == id), None)
        if not target:
            return {"success": False, "message": "条目不存在，请重新扫描"}
        ok, deleted = self._delete_entries([target])
        if deleted:
            self._remove_from_result(deleted)
            if self._notify:
                self._send_notification(
                    f"{self._notify_title_prefix}: 种子文件清理-已删除 1 个种子任务\n"
                    f"  ▶ 《{target.get('name')}》 [{target.get('downloader')}/{target.get('dl_type')}]（未删文件）")
            return {"success": True, "message": "已删除"}
        return {"success": False, "message": "删除失败，请查看日志"}

    def delete_selected_api(self, confirm: str = "") -> Dict[str, Any]:
        """批量删除勾选条目"""
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        if confirm != "yes":
            return {"success": False, "message": "请确认参数 confirm=yes"}
        selected = self._sel
        if not selected:
            return {"success": False, "message": "未勾选任何条目"}
        saved = self.get_data("result") or {}
        targets = [it for it in saved.get("items", []) if it.get("id") in set(selected)]
        if not targets:
            return {"success": False, "message": "勾选的条目不在结果中，请重新扫描"}
        ok, deleted = self._delete_entries(targets)
        if deleted:
            self._remove_from_result(deleted)
            logger.info(f"种子文件清理：批量删除勾选完成 {ok}/{len(targets)}")
            if self._notify:
                self._send_notification(
                    f"{self._notify_title_prefix}: 种子文件清理-已删除 {len(deleted)} 个种子任务（未删文件），"
                    f"请到插件详情页查看剩余")
            return {"success": True, "ok": len(deleted), "message": f"已删除 {len(deleted)} 个"}
        return {"success": False, "message": "删除失败，请查看日志"}

    def delete_all_api(self, dl: str = "", confirm: str = "") -> Dict[str, Any]:
        """删除全部/本下载器组条目"""
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        if confirm != "yes":
            return {"success": False, "message": "请确认参数 confirm=yes"}
        saved = self.get_data("result") or {}
        if dl:
            targets = [it for it in saved.get("items", []) if it.get("downloader") == dl]
        else:
            targets = saved.get("items", [])
        if not targets:
            return {"success": False, "message": "当前无结果"}
        ok, deleted = self._delete_entries(targets)
        if deleted:
            self._remove_from_result(deleted)
            logger.info(f"种子文件清理：删除完成 成功 {ok}/{len(targets)}")
            if self._notify:
                self._send_notification(
                    f"{self._notify_title_prefix}: 种子文件清理-已删除 {len(deleted)} 个种子任务（未删文件）")
            return {"success": True, "ok": len(deleted), "total": len(targets), "message": f"已删除 {len(deleted)} 个"}
        return {"success": False, "message": "删除失败，请查看日志"}

    def clear_api(self, confirm: str = "") -> Dict[str, Any]:
        if confirm != "yes":
            return {"success": False, "message": "请确认参数 confirm=yes"}
        self.save_data("result", {"time": "", "items": [], "total": 0, "stats": [], "skipped": []})
        self._sel = []
        return {"success": True}

    # ==================== 通知 ====================

    def _send_notification(self, text: str):
        """
        发送通知：优先飞书 Webhook，否则走系统消息
        """
        if self._webhook:
            try:
                payload = {
                    "msg_type": "text",
                    "content": {"text": text}
                }
                resp = requests.post(self._webhook, json=payload, timeout=15)
                if resp.status_code == 200 and resp.json().get("code") == 0:
                    logger.info("种子文件清理飞书通知发送成功")
                    return
                else:
                    logger.error(f"种子文件清理飞书通知发送失败：{resp.text[:200]}")
            except Exception as e:
                logger.error(f"种子文件清理飞书通知发送异常：{e}")
        # 走系统消息通道
        self.post_message(title="种子文件清理", text=text)

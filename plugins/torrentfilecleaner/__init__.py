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
    种子文件清理插件（v1.2：按下载器独立开关与路径映射）

    解决问题：下载器中存在数据文件已被删除（手工清理或迁移）的种子任务，
    这些"空壳"种子占用下载器资源、影响做种统计。

    工作原理：
    定时扫描下载器全部种子，逐一检查其数据文件/目录是否仍存在于下载目录中
    （MoviePilot 容器与下载器需共享下载目录挂载或配置路径映射），
    不存在的视为"无数据文件种子"，自动删除种子任务（仅删任务，不删文件）。

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
    plugin_version = "1.3"
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
    _scheduler = None

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled", False)
            self._onlyonce = config.get("onlyonce", False) or False
            self._cron = config.get("cron") or "0 */6 * * *"
            self._notify = bool(config.get("notify", True))
            self._webhook = config.get("webhook", "")
            self._notify_title_prefix = config.get("notify_title_prefix") or "HA通知"
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
        pass

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
        pass

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
        扫描全部下载器种子，检查数据文件是否存在，清理无数据文件的种子任务。
        按下载器配置过滤执行范围，支持 TR/qB，带全量不可达保护。
        每轮无论是否有清理结果均发送通知（含各下载器扫描统计）。
        """
        try:
            services = DownloaderHelper().get_services()
            if not services:
                logger.warning("种子文件清理：未找到已连接的下载器，跳过本轮扫描")
                if self._notify:
                    self._send_notification(
                        f"{self._notify_title_prefix}: 种子文件清理-未找到已连接的下载器，跳过本轮扫描")
                return

            total_removed = 0
            total_scanned = 0
            removed_detail = []
            skipped_downloaders = []
            scan_stats = []

            for name, info in services.items():
                inst = info.instance
                mod = type(inst).__module__
                dl_type = "TR" if "transmission" in mod else ("qB" if "qbittorrent" in mod else "?")

                # 用户开关过滤（未配置的下载器默认执行）
                if name in self._dl_enable and not self._dl_enable[name]:
                    logger.info(f"种子文件清理：下载器 {name} 已被用户禁用，跳过")
                    scan_stats.append(f"{name}({dl_type}): 已禁用，跳过")
                    continue

                # 获取该下载器的有效路径映射
                maps = self.__get_downloader_maps(name, info)

                if "transmission" in mod:
                    result = self.__clean_transmission(name, inst, maps)
                elif "qbittorrent" in mod:
                    result = self.__clean_qbittorrent(name, inst, maps)
                else:
                    continue

                if not result:
                    scan_stats.append(f"{name}({dl_type}): 连接失败，未扫描")
                    continue
                removed, detail, skipped_reason, scanned = result
                total_scanned += scanned
                if skipped_reason:
                    skipped_downloaders.append(f"[{name}] {skipped_reason}")
                    scan_stats.append(f"{name}({dl_type}): {scanned} 个种子，路径不可达已跳过")
                elif removed > 0:
                    total_removed += removed
                    removed_detail.extend(detail)
                    scan_stats.append(f"{name}({dl_type}): {scanned} 个种子，清理 {removed} 个")
                else:
                    scan_stats.append(f"{name}({dl_type}): {scanned} 个种子，正常")

            # 通知：无论是否有清理都发送（保证每轮有执行回执）
            lines = []
            if total_removed > 0:
                logger.info(f"种子文件清理：本轮共清理 {total_removed} 个无数据种子任务")
                lines.append(f"{self._notify_title_prefix}: 种子文件清理-已清理 {total_removed} 个无数据种子任务")
                lines.append("- 以下种子的数据文件已不存在，已从下载器移除任务（未删除文件）：")
                lines.extend(f"  ▶ {x}" for x in removed_detail)
            else:
                logger.info("种子文件清理：本轮扫描完成，未发现无数据文件的种子")
                lines.append(f"{self._notify_title_prefix}: 种子文件清理-本轮扫描完成（未发现无数据文件种子）")
                lines.append(f"- 共扫描 {len(scan_stats)} 个下载器 {total_scanned} 个种子，全部正常")
            if skipped_downloaders:
                logger.warning(f"种子文件清理：以下下载器被跳过：{skipped_downloaders}")
                lines.append("")
                lines.append("⚠ 以下下载器疑似路径不可达已跳过清理（请检查容器挂载或配置路径映射）：")
                lines.extend(f"  ▶ {x}" for x in skipped_downloaders)
            if scan_stats:
                lines.append("")
                lines.append("- 各下载器明细：")
                lines.extend(f"  ▶ {x}" for x in scan_stats)
            if self._notify:
                self._send_notification("\n".join(lines))

        except Exception as e:
            logger.error(f"种子文件清理：扫描清理失败：{e}")
            if self._notify:
                self._send_notification(
                    f"{self._notify_title_prefix}: 种子文件清理-扫描失败\n"
                    f"- 错误信息：{e}"
                )

    def __clean_transmission(self, name: str, inst,
                             maps: List[Tuple[str, str]]) -> Tuple[int, List[str], Optional[str]]:
        """
        清理 Transmission 下载器，返回 (删除数, 明细, 跳过原因, 扫描种子数)
        """
        torrents, err = inst.get_torrents()
        if err:
            logger.error(f"种子文件清理：获取 {name}(TR) 种子列表失败，跳过")
            return 0, [], None, 0

        logger.info(f"种子文件清理：开始扫描 {name}(TR)，共 {len(torrents)} 个种子")

        remove_ids, removed_detail = [], []
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
            logger.info(f"种子文件清理：[{name}] 检测到无数据文件种子 "
                        f"- 《{torrent_name}》(path={full_path}, id={t.id})")
            remove_ids.append(str(t.id))
            removed_detail.append(f"《{torrent_name}》 [{name}/TR 目录: {download_dir}]")

        # 全量不可达保护：全部（>=3）路径不存在 = 疑似挂载问题而非数据丢失
        if missing >= 3 and missing == len(torrents):
            logger.warning(f"种子文件清理：{name}(TR) 全部 {missing} 个种子路径均不可达，"
                           f"疑似容器挂载不一致，跳过清理防止误删")
            return 0, [], (f"TR 全部 {missing} 个种子路径不可达（如 {sample_dir}），"
                           f"请检查容器挂载或配置该下载器路径映射"), len(torrents)

        if remove_ids:
            success = inst.delete_torrents(delete_file=False, ids=remove_ids)
            if success:
                logger.info(f"种子文件清理：已从 {name}(TR) 清理 {len(remove_ids)} 个无数据种子任务")
                return len(remove_ids), removed_detail, None, len(torrents)
            else:
                logger.error(f"种子文件清理：从 {name}(TR) 删除种子任务失败")
        return 0, [], None, len(torrents)

    def __clean_qbittorrent(self, name: str, inst,
                            maps: List[Tuple[str, str]]) -> Tuple[int, List[str], Optional[str]]:
        """
        清理 qBittorrent 下载器，返回 (删除数, 明细, 跳过原因, 扫描种子数)
        """
        torrents, err = inst.get_torrents()
        if err:
            logger.error(f"种子文件清理：获取 {name}(qB) 种子列表失败，跳过")
            return 0, [], None, 0

        logger.info(f"种子文件清理：开始扫描 {name}(qB)，共 {len(torrents)} 个种子")

        remove_hashes, removed_detail = [], []
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

            # 候选路径：content_path（完整路径）优先，其次 save_path + name
            candidates = []
            if content_path:
                candidates.append(self.__apply_path_maps(content_path, maps))
            if save_path and torrent_name:
                candidates.append(self.__apply_path_maps(os.path.join(save_path, torrent_name), maps))
            # 兼容 qB 未完成文件后缀 .!qB
            all_candidates = []
            for p in candidates:
                all_candidates.append(p)
                all_candidates.append(p + ".!qB")

            if any(os.path.exists(p) for p in all_candidates):
                continue

            missing += 1
            logger.info(f"种子文件清理：[{name}] 检测到无数据文件种子 "
                        f"- 《{torrent_name}》(path={candidates[0] if candidates else '?'}, hash={torrent_hash[:12]})")
            remove_hashes.append(torrent_hash)
            removed_detail.append(f"《{torrent_name}》 [{name}/qB 目录: {save_path}]")

        # 全量不可达保护
        if missing >= 3 and missing == len(torrents):
            logger.warning(f"种子文件清理：{name}(qB) 全部 {missing} 个种子路径均不可达，"
                           f"疑似容器挂载不一致，跳过清理防止误删")
            map_hint = sample_dir if sample_dir else "qB保存目录"
            return 0, [], (f"qB 全部 {missing} 个种子路径不可达（如 {map_hint}），"
                           f"请配置该下载器路径映射（如 {map_hint}=MP容器对应路径）"), len(torrents)

        if remove_hashes:
            success = inst.delete_torrents(delete_file=False, ids=remove_hashes)
            if success:
                logger.info(f"种子文件清理：已从 {name}(qB) 清理 {len(remove_hashes)} 个无数据种子任务")
                return len(remove_hashes), removed_detail, None, len(torrents)
            else:
                logger.error(f"种子文件清理：从 {name}(qB) 删除种子任务失败")
        return 0, [], None, len(torrents)

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

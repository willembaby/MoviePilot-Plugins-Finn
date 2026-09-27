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
    种子文件清理插件

    解决问题：Transmission 中存在数据文件已被删除（手工清理或迁移）的种子任务，
    这些"空壳"种子占用下载器资源、影响做种统计。

    工作原理：
    定时扫描 Transmission 全部种子，逐一检查其数据文件/目录是否仍存在于下载目录中
    （共享挂载路径，MoviePilot 容器可直接访问 Transmission 的下载路径），
    不存在的视为"无数据文件种子"，自动删除种子任务（仅删任务，不删文件——文件已不存在）。

    安全设计：
    - 同时兼容 .part 后缀（Transmission 未完成下载时启用了 rename-partial-files 的场景）
    - 仅处理 Transmission 类型的下载器，不影响 qBittorrent
    - 只删除任务记录，不触碰任何磁盘文件
    """

    # 插件元数据
    plugin_name = "种子文件清理"
    plugin_desc = ("定期扫描 Transmission 种子，检测数据文件已被删除的种子任务并自动清理"
                   "（仅删任务不删文件），支持飞书 Webhook 通知。")
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/clean.png"
    plugin_version = "1.0"
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
    _scheduler = None

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled", False)
            self._onlyonce = config.get("onlyonce", False) or False
            self._cron = config.get("cron") or "0 */6 * * *"
            self._notify = bool(config.get("notify", True))
            self._webhook = config.get("webhook", "")
            self._notify_title_prefix = config.get("notify_title_prefix") or "HA通知"

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

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        插件配置页面：每行3列等宽布局，Webhook 独占整行
        """
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
                    # Row 3: Webhook 独占整行
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
        ], {
            "enabled": False,
            "onlyonce": False,
            "cron": "0 */6 * * *",
            "notify": True,
            "webhook": "",
            "notify_title_prefix": "HA通知"
        }

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
        self.update_config({
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "notify": self._notify,
            "webhook": self._webhook,
            "notify_title_prefix": self._notify_title_prefix,
        })

    # ==================== 核心扫描清理逻辑 ====================

    def _scan_clean(self):
        """
        扫描 Transmission 全部种子，检查数据文件是否存在，清理无数据文件的种子任务。
        """
        try:
            services = DownloaderHelper().get_services()
            if not services:
                logger.warning("种子文件清理：未找到已连接的下载器，跳过本轮扫描")
                return

            total_removed = 0
            removed_detail = []

            for name, info in services.items():
                inst = info.instance
                if "transmission" not in type(inst).__module__:
                    continue  # 仅处理 Transmission 类型下载器

                logger.info(f"种子文件清理：开始扫描下载器 {name}")

                torrents, err = inst.get_torrents()
                if err:
                    logger.error(f"种子文件清理：获取 {name} 种子列表失败，跳过")
                    continue

                logger.info(f"种子文件清理：{name} 共 {len(torrents)} 个种子待检查")

                remove_ids = []
                for t in torrents:
                    download_dir = str(t.download_dir or "")
                    torrent_name = str(t.name or "")
                    if not download_dir or not torrent_name:
                        continue

                    full_path = os.path.join(download_dir, torrent_name)
                    part_path = full_path + ".part"

                    # 文件/目录存在（含 .part 后缀兼容）→ 跳过
                    if os.path.exists(full_path) or os.path.exists(part_path):
                        continue

                    # 数据文件不存在 → 标记清理
                    logger.info(f"种子文件清理：检测到无数据文件种子 "
                                f"- 《{torrent_name}》(path={full_path}, id={t.id})")
                    remove_ids.append(str(t.id))
                    removed_detail.append(f"《{torrent_name}》 [目录: {download_dir}]")

                if remove_ids:
                    success = inst.delete_torrents(delete_file=False, ids=remove_ids)
                    if success:
                        total_removed += len(remove_ids)
                        logger.info(f"种子文件清理：已从 {name} 清理 {len(remove_ids)} 个无数据种子任务")
                    else:
                        logger.error(f"种子文件清理：从 {name} 删除种子任务失败")

            # 发送通知
            if total_removed > 0:
                logger.info(f"种子文件清理：本轮共清理 {total_removed} 个无数据种子任务")
                if self._notify:
                    self._send_notification(
                        f"{self._notify_title_prefix}: 种子文件清理-已清理 {total_removed} 个无数据种子任务\n"
                        f"- 以下种子的数据文件已不存在，已从 Transmission 移除任务（未删除文件）：\n"
                        + "\n".join(f"  ▶ {x}" for x in removed_detail)
                    )
            else:
                logger.info("种子文件清理：本轮扫描完成，未发现无数据文件的种子")

        except Exception as e:
            logger.error(f"种子文件清理：扫描清理失败：{e}")
            if self._notify:
                self._send_notification(
                    f"{self._notify_title_prefix}: 种子文件清理-扫描失败\n"
                    f"- 错误信息：{e}"
                )

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

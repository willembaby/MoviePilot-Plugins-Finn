
from datetime import datetime, timedelta
from typing import Any, List, Dict, Tuple, Optional

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.chain.subscribe import SubscribeChain
from app.chain.tmdb import TmdbChain
from app.core.config import settings
from app.db.subscribe_oper import SubscribeOper
from app.log import logger
from app.plugins import _PluginBase

import requests


class SubscribeCalendarMonitor(_PluginBase):
    """
    订阅日历监控插件
    每日定时检查"当日播出"的订阅剧集是否已下载入库，并发送通知
    """

    # 插件元数据
    plugin_name = "订阅日历监控"
    plugin_desc = "监控订阅剧集播出情况，检查当日播出剧集是否已下载入库，并发送通知。"
    plugin_icon = "https://raw.githubusercontent.com/thsrite/MoviePilot-Plugins/main/icons/subscribe_reminder.png"
    plugin_version = "1.2"
    plugin_author = "Finn"
    author_url = "https://github.com"
    plugin_config_prefix = "subscribecalendarmonitor_"
    plugin_order = 30
    auth_level = 1

    # 私有属性
    _enabled = False
    _onlyonce = False
    _cron = "0 19 * * *"
    _notify_only_missing = True
    _webhook = ""
    _notify_title_prefix = "HA通知"
    _scheduler = None

    # 依赖句柄
    _subscribe_oper = None
    _subscribe_chain = None
    _tmdb_chain = None

    def init_plugin(self, config: dict = None):
        # 依赖初始化
        self._subscribe_oper = SubscribeOper()
        self._subscribe_chain = SubscribeChain()
        self._tmdb_chain = TmdbChain()

        if config:
            self._enabled = config.get("enabled", False)
            self._onlyonce = config.get("onlyonce", False) or False
            self._cron = config.get("cron") or "0 19 * * *"
            self._notify_only_missing = config.get("notify_only_missing", True)
            self._webhook = config.get("webhook", "")
            self._notify_title_prefix = config.get("notify_title_prefix") or "HA通知"

        # 停止现有任务
        self.__stop_service()

        # 立即运行一次
        if self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            logger.info("订阅日历监控服务启动，立即运行一次")
            self._scheduler.add_job(
                func=self.run_check,
                trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                name="订阅日历监控",
            )
            # 启动调度器（否则 job 挂起不执行）
            self._scheduler.start()
            # 关闭一次性开关
            self._onlyonce = False
            self.__update_config()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        插件配置页面：开关独立一行，Webhook 独占整行，避免文字换行
        """
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'notify_only_missing',
                                            'label': '仅有缺失时通知',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'onlyonce',
                                            'label': '立即运行一次',
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'cron',
                                            'label': '检查时间（Cron）',
                                            'placeholder': '0 19 * * *'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'notify_title_prefix',
                                            'label': '通知前缀（飞书关键词）',
                                            'placeholder': 'HA通知'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'webhook',
                                            'label': '飞书Webhook（留空走系统消息）',
                                            'placeholder': 'https://open.feishu.cn/open-apis/bot/v2/hook/xxx'
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": self._enabled,
            "onlyonce": False,
            "cron": self._cron,
            "notify_only_missing": self._notify_only_missing,
            "webhook": self._webhook,
            "notify_title_prefix": self._notify_title_prefix
        }

    def get_page(self) -> List[dict]:
        pass

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件定时服务
        """
        if self._enabled and self._cron:
            try:
                return [{
                    "id": "SubscribeCalendarMonitor",
                    "name": "订阅日历监控服务",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.run_check,
                    "kwargs": {}
                }]
            except Exception as e:
                logger.error(f"订阅日历监控插件定时服务注册失败：{e}")
        return []

    def stop_service(self):
        self.__stop_service()

    def __stop_service(self):
        """
        停止插件定时服务
        """
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    try:
                        self._scheduler.shutdown()
                    except Exception:
                        pass
                self._scheduler = None
        except Exception as e:
            logger.error(f"停止订阅日历监控服务失败：{e}")

    def __update_config(self):
        """
        保存插件配置
        """
        self.update_config({
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "notify_only_missing": self._notify_only_missing,
            "webhook": self._webhook,
            "notify_title_prefix": self._notify_title_prefix
        })

    def run_check(self):
        """
        执行检查：当日播出的订阅剧集是否已入库
        """
        logger.info("开始执行订阅日历监控检查...")
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            result = self._check_today_episodes(today)
            self._send_report(result, today)
            logger.info(f"订阅日历监控检查完成：{result}")
        except Exception as e:
            logger.error(f"订阅日历监控检查失败：{e}")
            self._send_notification(f"{self._notify_title_prefix}: MoviePilot订阅日历监控检查失败：{e}")

    def _check_today_episodes(self, today: str) -> dict:
        """
        检查当日播出的订阅剧集状态
        使用 SubscribeOper.list() 获取订阅列表，TmdbChain.tmdb_episodes() 查询播出日期，
        SubscribeChain.subscribe_files_info() 判断每集是否已下载/入库
        """
        result = {"today_eps": [], "tmdb_fail": [], "total_subs": 0, "active_tv_subs": 0}

        subscribes = self._subscribe_oper.list(state="R")
        if not subscribes:
            return result
        result["total_subs"] = len(subscribes)

        tv_subs = [s for s in subscribes if s.type == "电视剧"]
        result["active_tv_subs"] = len(tv_subs)

        for sub in tv_subs:
            if not sub.tmdbid or sub.season is None:
                continue

            # 查询本季播出日期（TmdbChain 走模块代理与缓存，稳定可靠）
            today_ep_nums = []
            try:
                episodes = self._tmdb_chain.tmdb_episodes(
                    tmdbid=sub.tmdbid, season=sub.season, episode_group=sub.episode_group
                )
                if episodes:
                    for ep in episodes:
                        if ep.episode_number and ep.air_date and ep.air_date == today:
                            today_ep_nums.append(ep.episode_number)
                if not today_ep_nums:
                    continue
            except Exception as e:
                logger.warn(f"查询 {sub.name} TMDB播出信息失败：{e}")
                result["tmdb_fail"].append({"name": sub.name, "id": sub.id})
                continue

            # 检查这些集的下载/入库状态（与 API subscribe/files 同源逻辑）
            ep_statuses = []
            try:
                sub_info = self._subscribe_chain.subscribe_files_info(sub)
                episodes_info = sub_info.episodes if sub_info else {}
                for num in sorted(today_ep_nums):
                    ep_data = episodes_info.get(num)
                    downloaded = None
                    if ep_data is not None:
                        downloaded = bool(ep_data.download or ep_data.library)
                    ep_statuses.append({"ep": num, "downloaded": downloaded})
            except Exception as e:
                logger.warn(f"查询 {sub.name} 集文件状态失败：{e}")
                for num in sorted(today_ep_nums):
                    ep_statuses.append({"ep": num, "downloaded": None})

            result["today_eps"].append({
                "id": sub.id, "name": sub.name, "season": sub.season,
                "eps": ep_statuses
            })

        return result

    def _send_report(self, result: dict, today: str):
        """
        生成并发送报告
        """
        today_eps = result.get("today_eps", [])
        tmdb_fail = result.get("tmdb_fail", [])
        lines = []
        lines.append(f"{self._notify_title_prefix}: MoviePilot 日历监控日报 {today}")
        lines.append("")

        if not today_eps:
            lines.append("📅 今日无订阅剧集播出计划")
            if tmdb_fail:
                lines.append(f"⚠️ {len(tmdb_fail)} 个订阅播出信息查询失败：{'、'.join(s['name'] for s in tmdb_fail)}")
            lines.append("✅ 无需处理")
            msg = "\n".join(lines)
            # 无播出计划时按配置决定是否发
            if self._notify_only_missing:
                return
            self._send_notification(msg)
            return

        ok_subs = []
        missing_subs = []
        for s in today_eps:
            missing = [e["ep"] for e in s["eps"] if e.get("downloaded") is False]
            unknown = [e["ep"] for e in s["eps"] if e.get("downloaded") is None]
            if missing or unknown:
                missing_subs.append((s, missing, unknown))
            else:
                ok_subs.append(s)

        total_eps = sum(len(s["eps"]) for s in today_eps)
        lines.append(f"📅 今日有 {len(today_eps)} 个订阅共 {total_eps} 集播出计划")

        if missing_subs:
            lines.append(f"⚠️ {len(missing_subs)} 个订阅今日剧集未入库：")
            for s, missing, unknown in missing_subs:
                season_str = f"S{s['season']}" if s.get("season") is not None else ""
                if missing:
                    missing_str = ",".join(f"E{e}" for e in sorted(missing))
                    lines.append(f"  ▶ {s['name']} {season_str}{missing_str} 未入库")
                if unknown:
                    unk_str = ",".join(f"E{e}" for e in sorted(unknown))
                    lines.append(f"  ▶ {s['name']} {season_str}{unk_str} 状态查询失败")
        else:
            lines.append("✅ 今日所有播出剧集均已下载入库")

        if ok_subs and missing_subs:
            ok_names = "、".join(s["name"] for s in ok_subs)
            lines.append(f"✅ 已入库：{ok_names}")

        if tmdb_fail:
            lines.append(f"⚠️ 播出信息查询失败：{'、'.join(s['name'] for s in tmdb_fail)}")

        lines.append("")
        lines.append(f"⏰ 检查时间：{today} {datetime.now().strftime('%H:%M')}")

        msg = "\n".join(lines)
        self._send_notification(msg)

    def _send_notification(self, text: str):
        """
        发送通知：优先飞书 webhook，否则走系统消息
        """
        if self._webhook:
            try:
                payload = {
                    "msg_type": "text",
                    "content": {"text": text}
                }
                resp = requests.post(
                    self._webhook,
                    json=payload,
                    timeout=15,
                )
                if resp.status_code == 200 and resp.json().get("code") == 0:
                    logger.info("订阅日历监控飞书通知发送成功")
                    return
                else:
                    logger.error(f"订阅日历监控飞书通知发送失败：{resp.text[:200]}")
            except Exception as e:
                logger.error(f"订阅日历监控飞书通知发送异常：{e}")
        # 走系统消息通道
        self.post_message(title="订阅日历监控", text=text)

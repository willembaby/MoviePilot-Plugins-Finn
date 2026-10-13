import re
from datetime import datetime, timedelta
from typing import Any, List, Dict, Tuple, Optional

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.chain.subscribe import SubscribeChain
from app.core.config import settings
from app.core.event import eventmanager
from app.db import SessionFactory
from app.db.models.subscribehistory import SubscribeHistory
from app.db.subscribe_oper import SubscribeOper
from app.log import logger
from app.modules.douban.apiv2 import DoubanApi
from app.plugins import _PluginBase
from app.schemas.types import ChainEventType, MediaType

import json
import requests
import subprocess

# 豆瓣释放后跳过校验的天数（防止"超时放行后又被挂起"的循环）
RELEASED_KEEP_DAYS = 7
# Bangumi API
BANGUMI_UA = "MoviePilot/SubscribeCompletionCrossCheck (https://github.com/jxxghp)"
# 无总集数时的扩展缓冲集数默认值
DEFAULT_BUFFER_EPISODES = 12


class SubscribeCompletionCrossCheck(_PluginBase):
    """
    订阅完结交叉校验插件

    解决问题：TMDB 数据滞后（总集数标注不全）导致订阅在剧集尚未完结时被提前判定完结。

    双防线设计：
    1. 实时守卫（SubscribeCompletionCheck 链式事件）：
       订阅被判定完结、写库归档之前，交叉校验国内连载状态：
       - 豆瓣：episodes_count 总集数 + episodes_info 平台连载状态
         （"更新至X集"由豆瓣聚合自优酷/腾讯视频/爱奇艺等国内播放平台）
       - Bangumi：total_episodes 总集数（对动漫收录较好，搜索索引可能滞后，
         搜不到时自动跳过）
       任一源显示剧集未完结即否决本次完结，并自动扩展订阅总集数。
    2. 每日兜底扫描：
       扫描最近 N 天已完结的电视剧订阅历史，发现未完结的自动重建订阅。

    全源不可用时的处理：
    - 所有启用源均接口异常时，挂起完结（暂缓归档），每日重试；
    - 连续 N 天（pending_days）仍不可用则释放放行并通知（7天内不再重复校验）。
    """

    # 插件元数据
    plugin_name = "订阅完结交叉校验"
    plugin_desc = ("订阅判定完结时到豆瓣（聚合优酷/腾讯/爱奇艺等平台连载状态）与Bangumi交叉校验，"
                   "防止TMDB数据滞后导致订阅提前完结；国内剧以豆瓣为准（豆瓣完结而TMDB集数虚高时自动收缩订阅促完结），"
                   "国外剧以TMDB为准；源不可用时挂起重试，每日兜底扫描误完结并自动重建订阅。")
    plugin_icon = "https://raw.githubusercontent.com/thsrite/MoviePilot-Plugins/main/icons/subscribe_reminder.png"
    plugin_version = "1.6"
    plugin_author = "Finn"
    author_url = "https://github.com"
    plugin_config_prefix = "subscribecompletioncrosscheck_"
    plugin_order = 31
    auth_level = 1

    # 私有属性（配置）
    _enabled = False
    _onlyonce = False
    _cron = "0 9 * * *"
    _history_days = 14
    _history_count = 50
    _pending_days = 2
    _enable_bangumi = True
    _buffer_episodes = DEFAULT_BUFFER_EPISODES
    _webhook = ""
    _notify_title_prefix = "HA通知"
    _scheduler = None

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled", False)
            self._onlyonce = config.get("onlyonce", False) or False
            self._cron = config.get("cron") or "0 9 * * *"
            self._history_days = int(config.get("history_days") or 14)
            self._history_count = int(config.get("history_count") or 50)
            self._pending_days = int(config.get("pending_days") or 2)
            self._enable_bangumi = bool(config.get("enable_bangumi", True))
            self._buffer_episodes = int(config.get("buffer_episodes") or DEFAULT_BUFFER_EPISODES)
            self._webhook = config.get("webhook", "")
            self._notify_title_prefix = config.get("notify_title_prefix") or "HA通知"

        # 停止现有任务
        self.__stop_service()

        # 立即运行一次兜底扫描
        if self._enabled and self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            logger.info("订阅完结交叉校验服务启动，立即运行一次兜底扫描")
            self._scheduler.add_job(
                func=self.run_daily_check,
                trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=5),
                name="订阅完结交叉校验",
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
        插件配置页面：每行3列等宽布局，长链接独占整行，避免开关文字换行
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
                                            'model': 'enable_bangumi',
                                            'label': '启用Bangumi校验',
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
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'cron',
                                            'label': '兜底扫描时间（Cron）',
                                            'placeholder': '0 9 * * *'
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
                                            'model': 'pending_days',
                                            'label': '源不可用挂起天数',
                                            'placeholder': '2'
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
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'history_days',
                                            'label': '兜底扫描范围（天）',
                                            'placeholder': '14'
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
                                            'model': 'history_count',
                                            'label': '兜底扫描条数',
                                            'placeholder': '50'
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
                                            'model': 'buffer_episodes',
                                            'label': '无总集数时缓冲（集）',
                                            'placeholder': '12'
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
            "history_days": self._history_days,
            "history_count": self._history_count,
            "pending_days": self._pending_days,
            "enable_bangumi": self._enable_bangumi,
            "buffer_episodes": self._buffer_episodes,
            "webhook": self._webhook,
            "notify_title_prefix": self._notify_title_prefix
        }

    def get_page(self) -> List[dict]:
        pass

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件定时服务（每日兜底扫描）
        """
        if self._enabled and self._cron:
            try:
                return [{
                    "id": "SubscribeCompletionCrossCheck",
                    "name": "订阅完结交叉校验兜底扫描",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.run_daily_check,
                    "kwargs": {}
                }]
            except Exception as e:
                logger.error(f"订阅完结交叉校验插件定时服务注册失败：{e}")
        return []

    def stop_service(self):
        self.__stop_service()

    def __stop_service(self):
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
            logger.error(f"停止订阅完结交叉校验服务失败：{e}")

    def __update_config(self):
        self.update_config({
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "history_days": self._history_days,
            "history_count": self._history_count,
            "pending_days": self._pending_days,
            "enable_bangumi": self._enable_bangumi,
            "buffer_episodes": self._buffer_episodes,
            "webhook": self._webhook,
            "notify_title_prefix": self._notify_title_prefix
        })

    # ==================== 记录读写（挂起/释放状态） ====================

    def _load_records(self) -> List[dict]:
        """读取挂起/释放记录列表"""
        try:
            records = self.get_data("completion_records") or []
            if isinstance(records, list):
                return records
        except Exception:
            pass
        return []

    def _save_records(self, records: List[dict]):
        """保存挂起/释放记录列表"""
        try:
            self.save_data("completion_records", records)
        except Exception as e:
            logger.error(f"完结交叉校验：保存挂起记录失败：{e}")

    @staticmethod
    def _fmt(dt: datetime) -> str:
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _parse(s: str) -> Optional[datetime]:
        try:
            return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    @staticmethod
    def _find_record(records: List[dict], sid: int) -> Optional[dict]:
        for r in records:
            if r.get("sid") == sid:
                return r
        return None

    # ==================== 实时守卫：完结判定事件 ====================

    @eventmanager.register(ChainEventType.SubscribeCompletionCheck)
    def on_completion_check(self, event):
        """
        订阅完成判定（链式事件，可否决）：
        在订阅被自动判定完成、写库归档之前触发。
        交叉校验豆瓣（平台连载状态+总集数）与Bangumi（总集数），
        任一源显示未完结则否决完成并扩展订阅。
        """
        if not self._enabled:
            return
        data = event.event_data
        if data is None:
            return
        subscribe = data.subscribe
        if not subscribe:
            return
        # 只校验电视剧订阅
        if subscribe.type != MediaType.TV.value:
            return
        current_total = subscribe.total_episode or 0
        if current_total <= 0:
            return

        name = subscribe.name
        year = subscribe.year
        sid = subscribe.id
        records = self._load_records()
        rec = self._find_record(records, sid)

        # 已释放（源长期不可用超时放行）且未过期：直接放行，不再校验
        if rec and rec.get("state") == "released":
            expire = self._parse(rec.get("expire"))
            if expire and datetime.now() < expire:
                logger.info(f"完结交叉校验：《{name}》处于源不可用释放期（至 {rec.get('expire')}），直接放行")
                return
            # 释放期已过：移除记录，重新完整校验
            records.remove(rec)
            self._save_records(records)
            rec = None

        logger.info(f"完结交叉校验：开始校验《{name}》（当前总集数 {current_total}）")

        try:
            douban_id = getattr(subscribe, "doubanid", None)
            bangumi_id = getattr(subscribe, "bangumiid", None)
            mediainfo = getattr(data, "mediainfo", None)
            if not douban_id and mediainfo:
                douban_id = getattr(mediainfo, "douban_id", None)
            if not bangumi_id and mediainfo:
                bangumi_id = getattr(mediainfo, "bangumi_id", None)

            decision, target, evidence, source_status = self._cross_check(
                name, year, current_total, douban_id, bangumi_id)

            if decision == "all_error":
                # 所有源接口异常：挂起
                self._handle_sources_down(sid, name, records)
                data.cancel = True
                data.source = "SubscribeCompletionCrossCheck"
                data.reason = f"校验源暂不可用（{source_status}），已暂缓完结判定（{self._pending_days}天后自动释放）"
                logger.info(f"完结交叉校验：《{name}》校验源异常（{source_status}），已挂起暂缓完结判定")
                return

            # 清理挂起/释放记录（正常判定完成）
            if rec:
                records.remove(rec)
                self._save_records(records)

            if decision == "not_finished":
                reason = f"{evidence}，剧集未完结"
                data.cancel = True
                data.source = "SubscribeCompletionCrossCheck"
                data.reason = reason
                logger.info(f"完结交叉校验：《{name}》{reason}，已否决完结")
                fixed = self._extend_subscribe(sid, current_total, target)
                fix_msg = f"已自动扩展订阅集数至 {target} 集" if fixed else "订阅集数扩展失败，请手动检查"
                self._send_notification(
                    f"{self._notify_title_prefix}: MoviePilot订阅完结校验-《{name}》未完结\n"
                    f"- 订阅总集数(TMDB)：{current_total}\n"
                    f"- 校验证据：{evidence}\n"
                    f"- 处理：已否决本次自动完结，{fix_msg}，继续监控后续更新"
                )
            else:
                # finished / no_data：放行
                logger.info(f"完结交叉校验：《{name}》{source_status}，确认完结，放行")
        except Exception as e:
            # 校验异常时放行，避免阻塞正常完结
            logger.error(f"完结交叉校验：《{name}》校验异常（放行）：{e}")

    def _handle_sources_down(self, sid: int, name: str, records: List[dict]):
        """
        校验源不可用时挂起订阅：记录待复查（首次挂起发一次通知，不重复刷屏）
        """
        now = datetime.now()
        rec = self._find_record(records, sid)
        if rec and rec.get("state") == "pending":
            return
        expire = now + timedelta(days=self._pending_days)
        records.append({
            "sid": sid,
            "name": name,
            "state": "pending",
            "first": self._fmt(now),
            "expire": self._fmt(expire),
        })
        self._save_records(records)
        self._send_notification(
            f"{self._notify_title_prefix}: MoviePilot订阅完结校验-《{name}》校验源暂不可用\n"
            f"- 订阅《{name}》被判定完结，但豆瓣/Bangumi校验源查询失败，无法确认是否真完结\n"
            f"- 处理：已暂缓本次完结判定，将自动重试；"
            f"{self._pending_days} 天后仍不可用将自动放行并通知"
        )

    # ==================== 每日兜底扫描 ====================

    def run_daily_check(self):
        """
        每日兜底扫描：
        1. 重试挂起项（校验源恢复后按正常逻辑判定，超时则释放）
        2. 扫描最近 N 天已完结的电视剧订阅历史，发现未完结的，重建订阅
        """
        logger.info("开始订阅完结交叉校验兜底扫描...")
        try:
            self._retry_pending()
            self._scan_active()
            self._scan_history()
        except Exception as e:
            logger.error(f"完结交叉校验兜底扫描失败：{e}")
            self._send_notification(f"{self._notify_title_prefix}: 订阅完结交叉校验兜底扫描失败：{e}")

    def _retry_pending(self):
        """重试挂起项（校验源恢复后正常判定；超时释放）"""
        records = self._load_records()
        if not records:
            return
        now = datetime.now()
        new_records = []
        oper = SubscribeOper()
        for rec in records:
            if rec.get("state") != "pending":
                # released 记录：过期则清理
                expire = self._parse(rec.get("expire"))
                if expire and now >= expire:
                    continue
                new_records.append(rec)
                continue
            sid = rec.get("sid")
            subscribe = oper.get(sid)
            if not subscribe:
                continue
            douban_id = getattr(subscribe, "doubanid", None)
            bangumi_id = getattr(subscribe, "bangumiid", None)
            current_total = subscribe.total_episode or 0
            decision, target, evidence, source_status = self._cross_check(
                subscribe.name, subscribe.year, current_total, douban_id, bangumi_id)
            if decision == "not_finished":
                self._extend_subscribe(sid, current_total, target)
                self._send_notification(
                    f"{self._notify_title_prefix}: MoviePilot订阅完结校验-《{subscribe.name}》校验源已恢复\n"
                    f"- 校验证据：{evidence}，确认未完结\n"
                    f"- 已自动扩展订阅集数至 {target} 集并继续监控"
                )
                continue
            if decision == "all_error":
                # 仍不可用
                expire = self._parse(rec.get("expire"))
                if expire and now >= expire:
                    # 超时释放：放行完结 + 通知
                    rec["state"] = "released"
                    rec["expire"] = self._fmt(now + timedelta(days=RELEASED_KEEP_DAYS))
                    new_records.append(rec)
                    self._send_notification(
                        f"{self._notify_title_prefix}: MoviePilot订阅完结校验-《{subscribe.name}》校验源持续不可用\n"
                        f"- 豆瓣/Bangumi 已连续不可用超过 {self._pending_days} 天，无法确认完结状态\n"
                        f"- 处理：已自动放行完结归档（若该剧实际未完结，将不会被继续监控，请注意）\n"
                        f"- 建议：手动到豆瓣/播放平台确认该剧状态"
                    )
                    continue
                new_records.append(rec)
                continue
            # finished / no_data：正常判定完成，移除记录
            logger.info(f"完结交叉校验：挂起项《{subscribe.name}》校验恢复并确认完结，移除挂起")
        self._save_records(new_records)

    def _scan_active(self):
        """
        扫描活跃 TV 订阅（v1.4：国内剧以豆瓣为准）：
        豆瓣显示已完结（非"更新至"连载状态）且总集数少于订阅集数（TMDB 集数虚高）时，
        收缩订阅总集数，使缺失集数按豆瓣口径重算，待集齐后由主程序自然判定完结归档。
        国外剧（豆瓣无条目或非中国大陆出品）以 TMDB 为准，不做收缩。
        """
        try:
            oper = SubscribeOper()
            subs = [s for s in (oper.list(state="R") or []) if s.type == MediaType.TV.value]
            if not subs:
                return
            api = DoubanApi()
            shrunk, skipped = [], 0
            for s in subs:
                current_total = s.total_episode or 0
                lack = s.lack_episode or 0
                if current_total <= 0 or lack <= 0:
                    continue  # 缺集为0的由主程序正常完结，无需收缩
                try:
                    sid = str(s.doubanid) if s.doubanid else self._search_douban_subject(api, s.name, s.year)
                    if not sid:
                        skipped += 1  # 豆瓣无条目：按国外/冷门剧处理，以TMDB为准
                        continue
                    detail = api.tv_detail(sid)
                    if not isinstance(detail, dict):
                        continue
                    # 年份复核，防同名误匹配
                    dy, sy = str(detail.get("year") or ""), str(s.year or "")
                    if dy and sy and dy != sy:
                        continue
                    # 国内剧判断
                    countries = detail.get("countries") or []
                    if isinstance(countries, str):
                        countries = [countries]
                    if not any("中国" in str(c) for c in countries):
                        skipped += 1  # 国外剧：以TMDB为准
                        continue
                    # 连载中（"更新至X集"）不收缩
                    info = str(detail.get("episodes_info") or "")
                    if "更新至" in info:
                        continue
                    eps = detail.get("episodes_count")
                    if not eps or int(eps) >= current_total:
                        continue  # 豆瓣集数不比订阅少：无需收缩
                    # 条目口径校验：豆瓣集数远小于订阅集数时，判定为分季/拆分条目误匹配
                    # （如年番搜到第一季条目），跳过收缩，避免连载剧集被误完结
                    if int(eps) < int(current_total * 0.4):
                        logger.info(f"完结交叉校验：《{s.name}》豆瓣条目集数 {eps} 与订阅总集数 "
                                    f"{current_total} 差距过大（疑似分季条目），跳过收缩")
                        continue
                    # 国内剧 + 豆瓣已完结 + TMDB 集数虚高 → 收缩
                    completed = max(current_total - lack, 0)
                    new_total = max(int(eps), completed)
                    if new_total >= current_total:
                        continue
                    if self._shrink_subscribe(s, new_total):
                        shrunk.append(f"《{s.name}》{current_total}→{new_total}集（豆瓣已完结，共{eps}集）")
                except Exception as e:
                    logger.warn(f"完结交叉校验：活跃订阅《{s.name}》收缩检查异常：{e}")
            if shrunk:
                self._send_notification(
                    f"{self._notify_title_prefix}: MoviePilot订阅完结校验-发现TMDB集数虚高的已完结剧集\n"
                    f"- 以下国内剧豆瓣显示已完结，订阅总集数已按豆瓣口径收缩，"
                    f"缺失集数补齐后将自动完结归档：\n"
                    + "\n".join(f"  ▶ {x}" for x in shrunk)
                )
            logger.info(f"完结交叉校验：活跃订阅收缩扫描完成，收缩 {len(shrunk)} 部，"
                        f"国外剧/无条目跳过 {skipped} 部")
        except Exception as e:
            logger.error(f"完结交叉校验：活跃订阅收缩扫描失败：{e}")

    def _shrink_subscribe(self, subscribe, new_total: int) -> bool:
        """
        收缩订阅总集数（国内剧豆瓣已完结而 TMDB 集数虚高时）：
        total_episode 调整为 new_total，lack_episode 按已完成集数重算。
        缺失归零后由主程序在下次订阅检查时自然判定完结归档。
        """
        try:
            oper = SubscribeOper()
            old_total = subscribe.total_episode or 0
            lack = subscribe.lack_episode or 0
            completed = max(old_total - lack, 0)
            new_lack = max(new_total - completed, 0)
            oper.update(subscribe.id, {
                "total_episode": new_total,
                "lack_episode": new_lack,
                "manual_total_episode": 1,
            })
            logger.info(f"完结交叉校验：订阅 {subscribe.id}《{subscribe.name}》总集数收缩 "
                        f"{old_total} → {new_total}（已完成 {completed}，缺失 {new_lack}），"
                        f"待主程序下次检查时判定完结")
            return True
        except Exception as e:
            logger.error(f"完结交叉校验：收缩订阅 {subscribe.id} 失败：{e}")
            return False

    def _scan_history(self):
        """扫描最近 N 天已完结的电视剧订阅历史，发现误完结自动重建"""
        try:
            cutoff = datetime.now() - timedelta(days=self._history_days)
            with SessionFactory() as db:
                histories = SubscribeHistory.list_by_type(
                    db, MediaType.TV.value, page=1, count=self._history_count
                ) or []
            logger.info(f"完结交叉校验兜底：取到 {len(histories)} 条电视剧完结历史，"
                        f"筛选最近 {self._history_days} 天内的记录")

            checked, skipped_active, fixed_subs, failed = 0, 0, [], []
            oper = SubscribeOper()
            for h in histories:
                try:
                    hdate = datetime.strptime(h.date, "%Y-%m-%d %H:%M:%S")
                except Exception:
                    continue
                if hdate < cutoff:
                    continue
                if h.tmdbid and oper.list_by_tmdbid(h.tmdbid, h.season):
                    skipped_active += 1
                    continue
                checked += 1
                current_total = h.total_episode or 0
                decision, target, evidence, source_status = self._cross_check(
                    h.name, h.year, current_total, h.doubanid, h.bangumiid)
                if decision != "not_finished":
                    continue
                sid, msg = self._rebuild_subscribe(h, current_total, target)
                if sid:
                    fixed_subs.append(f"《{h.name}》{current_total}→{target}集({evidence})")
                else:
                    failed.append(f"《{h.name}》：{msg}")

            lines = [f"{self._notify_title_prefix}: MoviePilot订阅完结交叉校验日报",
                     f"- 扫描范围：最近 {self._history_days} 天完结历史 {checked} 条"
                     f"（{skipped_active} 条已有活跃订阅跳过）"]
            if fixed_subs:
                lines.append(f"- 发现 {len(fixed_subs)} 部误完结，已自动重建订阅：")
                lines.extend(f"  ▶ {s}" for s in fixed_subs)
            if failed:
                lines.append(f"- {len(failed)} 部校验发现未完结但重建失败：")
                lines.extend(f"  ▶ {s}" for s in failed)
            if not fixed_subs and not failed:
                lines.append("- 未发现误完结订阅")
            msg = "\n".join(lines)
            self._send_notification(msg)
            logger.info(f"完结交叉校验兜底扫描完成：checked={checked}, fixed={len(fixed_subs)}, failed={len(failed)}")
        except Exception as e:
            logger.error(f"完结交叉校验兜底扫描失败：{e}")

    # ==================== 交叉校验核心 ====================

    def _cross_check(self, name: str, year: Optional[str], current_total: int,
                     douban_id: Optional[str] = None,
                     bangumi_id: Optional[int] = None) -> Tuple[str, Optional[int], str, str]:
        """
        多源交叉校验剧集是否完结。
        :return: (decision, target_total, evidence, source_status)
                 decision: not_finished / finished / all_error / no_data
                 target_total: 未完结时的扩展目标总集数（not_finished 时有效）
                 evidence: 证据描述
                 source_status: 各源状态描述
        """
        evidence = []
        errors = []
        target = None
        aired_eps = None

        # ---- 豆瓣源：总集数 + 平台连载状态 ----
        d_total, d_info, d_matched, d_status = self._get_douban_info(name, year, douban_id)
        if d_status == "ok":
            if d_total and d_total > current_total:
                evidence.append(f"豆瓣总集数 {d_total} 集 > 当前 {current_total} 集")
                target = d_total
            # 解析平台连载状态（豆瓣聚合自优酷/腾讯/爱奇艺等）
            # "更新至X集"仅当 X > 当前总集数时构成证据（平台进度已超过TMDB宣称总数）
            m = re.search(r"更新至\s*(\d+)\s*集", d_info or "")
            if m:
                aired_eps = int(m.group(1))
                if aired_eps > current_total:
                    evidence.append(f"平台连载状态\"{d_info}\"已超过当前 {current_total} 集")
                    if not target:
                        target = aired_eps + self._buffer_episodes
            elif d_info and ("全集" in d_info):
                logger.info(f"完结交叉校验：《{name}》豆瓣显示\"{d_info}\"（已完结信号）")
        elif d_status == "error":
            errors.append("豆瓣")
        # notfound：豆瓣无条目，不算错误

        # ---- Bangumi 源：总集数 ----
        if self._enable_bangumi:
            b_total, b_matched, b_status = self._get_bangumi_info(name, year, bangumi_id)
            if b_status == "ok":
                # 阈值2集：Bangumi与订阅总集数存在口径差异时避免误判与死挂
                if b_total and b_total > current_total + 2:
                    evidence.append(f"Bangumi总集数 {b_total} 集 > 当前 {current_total} 集")
                    if not target:
                        target = b_total
            elif b_status == "error":
                errors.append("Bangumi")
            # notfound：Bangumi 无条目（搜索索引滞后常见），跳过

        # ---- 判定 ----
        source_status = (f"豆瓣:{d_status}, "
                         f"Bangumi:{'disabled' if not self._enable_bangumi else b_status}")

        if evidence:
            # 未完结：确定扩展目标
            if not target:
                base = aired_eps if aired_eps else current_total
                target = max(base + self._buffer_episodes, current_total + 1)
                evidence.append(f"无确切总集数，扩展至 {target} 集（含 {self._buffer_episodes} 集缓冲）")
            return "not_finished", target, "；".join(evidence), source_status

        if errors:
            # 有未完结证据缺失但部分源异常：只要有任一源可用即按已可用源判定
            if d_status == "error" and (not self._enable_bangumi or b_status == "error"):
                return "all_error", None, "", f"接口异常（{'、'.join(errors)}）"
            # 部分源可用且未发现未完结证据：视为完结（可用的源已给出判断）

        if d_status == "ok" or (self._enable_bangumi and b_status == "ok"):
            return "finished", None, "", source_status
        # 两边都没有条目（豆瓣 notfound 且 bangumi notfound/未启用）
        return "no_data", None, "", source_status

    # ==================== 豆瓣校验 ====================

    def _get_douban_info(self, name: str, year: Optional[str],
                         douban_id: Optional[str] = None) -> Tuple[Optional[int], Optional[str], Optional[str], str]:
        """
        查询豆瓣的总集数与平台连载状态。
        :return: (总集数 or None, episodes_info 如"更新至11集", 豆瓣条目ID, 状态)
                 status: ok / notfound / error
        """
        try:
            api = DoubanApi()
        except Exception as e:
            logger.warn(f"完结交叉校验：《{name}》豆瓣API初始化失败：{e}")
            return None, None, None, "error"
        try:
            sid = str(douban_id) if douban_id else None
            if not sid:
                sid = self._search_douban_subject(api, name, year)
            if not sid:
                return None, None, None, "notfound"
            detail = api.tv_detail(sid)
            if not isinstance(detail, dict):
                return None, None, sid, "notfound"
            # 年份复核（有年份信息且明显不符时放弃，避免同名误匹配）
            detail_year = str(detail.get("year") or "")
            sub_year = str(year or "")
            if detail_year and sub_year and detail_year != sub_year:
                logger.info(f"完结交叉校验：《{name}》豆瓣条目 {sid} 年份 {detail_year} "
                            f"与订阅 {sub_year} 不符，放弃该结果")
                return None, None, sid, "notfound"
            episodes = detail.get("episodes_count")
            info = detail.get("episodes_info") or ""
            # vendors 里的各平台状态（取第一个非空的，与总 info 一致时无增益）
            if not info and isinstance(detail.get("vendors"), list):
                for v in detail.get("vendors"):
                    if isinstance(v, dict) and v.get("episodes_info"):
                        info = v.get("episodes_info")
                        break
            if episodes or info:
                return int(episodes) if episodes else None, info, sid, "ok"
            return None, None, sid, "notfound"
        except Exception as e:
            logger.warn(f"完结交叉校验：《{name}》豆瓣查询异常（标记不可用）：{e}")
            return None, None, None, "error"

    @staticmethod
    def _search_douban_subject(api: DoubanApi, name: str, year: Optional[str]) -> Optional[str]:
        """
        在豆瓣搜索剧集条目，返回 subject_id。
        tv_search 部分新版条目搜不到（如 2026 版《将夜》），无结果时用聚合搜索
        search() 兜底：聚合结果混合电视剧/图书/豆列条目，仅取 target_type == "tv"
        且 id 有效的条目参与匹配，避免匹配到书籍或垃圾数据。
        """
        sub_year = str(year or "")

        def _pick(items: List[dict]) -> Optional[str]:
            best_partial = None
            for it in items:
                if not isinstance(it, dict):
                    continue
                target = it.get("target") or it
                if not isinstance(target, dict):
                    continue
                title = str(target.get("title") or "")
                sid = target.get("id")
                item_year = str(target.get("year") or "")
                if not title or not sid:
                    continue
                if title == name:
                    if not sub_year or not item_year or item_year == sub_year:
                        return str(sid)
                    continue
                if name in title and best_partial is None:
                    if not sub_year or not item_year or item_year == sub_year:
                        best_partial = str(sid)
            return best_partial

        # 1) 电视剧搜索（常规路径）
        try:
            res = api.tv_search(name)
        except Exception as e:
            logger.warn(f"完结交叉校验：豆瓣搜索《{name}》异常（标记不可用）：{e}")
            return None
        if isinstance(res, dict):
            sid = _pick(res.get("items") or [])
            if sid:
                return sid
        # 2) 聚合搜索兜底（tv_search 搜不到时）
        try:
            res = api.search(name)
            if isinstance(res, dict):
                items = [it for it in (res.get("items") or [])
                         if isinstance(it, dict) and it.get("target_type") == "tv"
                         and isinstance(it.get("target"), dict)
                         and (it.get("target") or {}).get("id")]
                if items:
                    logger.info(f"完结交叉校验：豆瓣tv搜索无《{name}》结果，"
                                f"聚合搜索兜底命中 {len(items)} 个剧集条目")
                sid = _pick(items)
                if sid:
                    return sid
        except Exception as e:
            logger.warn(f"完结交叉校验：豆瓣聚合搜索《{name}》异常（忽略兜底）：{e}")
        return None

    # ==================== Bangumi 校验 ====================

    @staticmethod
    def _bgm_curl_json(url: str, post_json: Optional[dict] = None) -> Optional[dict]:
        """
        通过 curl 子进程请求 Bangumi API（容器内 requests 直连易被 TLS 阻断，curl 实测可用）
        """
        try:
            cmd = ["curl", "-s", "--max-time", "20", "-H", f"User-Agent: {BANGUMI_UA}"]
            if post_json is not None:
                cmd += ["-X", "POST", "-H", "Content-Type: application/json",
                        "-d", json.dumps(post_json, ensure_ascii=False)]
            cmd.append(url)
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if r.stdout and r.stdout.strip():
                return json.loads(r.stdout)
        except Exception:
            return None
        return None

    def _get_bangumi_info(self, name: str, year: Optional[str],
                          bangumi_id: Optional[int] = None) -> Tuple[Optional[int], Optional[int], str]:
        """
        查询 Bangumi 总集数。
        :return: (总集数 or None, 条目ID, 状态) 状态: ok / notfound / error
        """
        try:
            bid = int(bangumi_id) if bangumi_id else None
            if not bid:
                bid = self._search_bangumi_subject(name, year)
            if not bid:
                return None, None, "notfound"
            detail = self._bgm_curl_json(f"https://api.bgm.tv/v0/subjects/{bid}")
            if not isinstance(detail, dict) or not detail.get("name_cn"):
                return None, bid, "notfound"
            # 年份复核
            bdate = str(detail.get("date") or "")[:4]
            sub_year = str(year or "")
            if bdate and sub_year and bdate != sub_year:
                logger.info(f"完结交叉校验：《{name}》Bangumi条目 {bid} 年份 {bdate} "
                            f"与订阅 {sub_year} 不符，放弃该结果")
                return None, bid, "notfound"
            total = detail.get("total_episodes") or detail.get("eps")
            if total:
                return int(total), bid, "ok"
            return None, bid, "notfound"
        except Exception as e:
            logger.warn(f"完结交叉校验：《{name}》Bangumi查询异常（标记不可用）：{e}")
            return None, None, "error"

    def _search_bangumi_subject(self, name: str, year: Optional[str]) -> Optional[int]:
        """
        在 Bangumi 搜索条目（v0 接口对新番索引可能滞后，搜不到返回 None）
        """
        res = self._bgm_curl_json("https://api.bgm.tv/v0/search/subjects", {
            "keyword": name, "filter": {"type": [2, 6]}, "limit": 10
        })
        if not isinstance(res, dict):
            return None
        sub_year = str(year or "")
        best = None
        for it in res.get("data") or []:
            if not isinstance(it, dict):
                continue
            title = str(it.get("name_cn") or it.get("name") or "")
            iid = it.get("id")
            idate = str(it.get("date") or "")[:4]
            if not title or not iid:
                continue
            if title == name or name in title or title in name:
                if not sub_year or not idate or idate == sub_year:
                    if title == name:
                        return int(iid)
                    if best is None:
                        best = int(iid)
        return best

    # ==================== 订阅修正与重建 ====================

    def _extend_subscribe(self, subscribe_id: int, old_total: int, new_total: int) -> bool:
        """
        扩展订阅总集数（否决完结时调用）：
        total_episode 提升至 new_total，lack_episode 按已完成集数重算，
        并标记手动总集数，防止后续被 TMDB 刷新覆盖。
        """
        try:
            oper = SubscribeOper()
            subscribe = oper.get(subscribe_id)
            if not subscribe:
                logger.error(f"完结交叉校验：订阅 {subscribe_id} 不存在，无法扩展")
                return False
            lack = subscribe.lack_episode or 0
            completed = max(old_total - lack, 0)
            new_lack = max(new_total - completed, 0)
            oper.update(subscribe_id, {
                "total_episode": new_total,
                "lack_episode": new_lack,
                "manual_total_episode": 1,
            })
            logger.info(f"完结交叉校验：订阅 {subscribe_id}《{subscribe.name}》总集数 "
                        f"{old_total} → {new_total}（已完成 {completed}，缺失 {new_lack}）")
            return True
        except Exception as e:
            logger.error(f"完结交叉校验：扩展订阅 {subscribe_id} 失败：{e}")
            return False

    def _rebuild_subscribe(self, hist, old_total: int, new_total: int) -> Tuple[Optional[int], str]:
        """
        重建已完结订阅（兜底扫描发现误完结时调用）：
        通过 SubscribeChain.add 重建订阅，总集数使用校验数据，从已完成的下一集开始。
        """
        try:
            chain = SubscribeChain()
            sid, msg = chain.add(
                title=hist.name,
                year=hist.year,
                mtype=MediaType.TV,
                tmdbid=hist.tmdbid,
                season=hist.season,
                username=hist.username,
                total_episode=new_total,
                start_episode=old_total + 1,
                lack_episode=max(new_total - old_total, 0),
                exist_ok=True,
                message=False,
            )
            if sid:
                SubscribeOper().update(sid, {"manual_total_episode": 1})
                logger.info(f"完结交叉校验：已重建订阅《{hist.name}》（#{sid}），"
                            f"总集数 {old_total} → {new_total}")
                return sid, "重建成功"
            logger.warn(f"完结交叉校验：重建订阅《{hist.name}》失败：{msg}")
            return None, msg or "重建失败"
        except Exception as e:
            logger.error(f"完结交叉校验：重建订阅《{hist.name}》异常：{e}")
            return None, str(e)

    # ==================== 通知 ====================

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
                resp = requests.post(self._webhook, json=payload, timeout=15)
                if resp.status_code == 200 and resp.json().get("code") == 0:
                    logger.info("订阅完结交叉校验飞书通知发送成功")
                    return
                else:
                    logger.error(f"订阅完结交叉校验飞书通知发送失败：{resp.text[:200]}")
            except Exception as e:
                logger.error(f"订阅完结交叉校验飞书通知发送异常：{e}")
        # 走系统消息通道
        self.post_message(title="订阅完结交叉校验", text=text)
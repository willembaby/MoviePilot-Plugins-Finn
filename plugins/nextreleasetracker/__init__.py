from __future__ import annotations

from datetime import datetime, timedelta
import json
import math
import re
from threading import Lock
import time
from typing import Any, Dict, List, Optional, Tuple

import pytz
from apscheduler.triggers.cron import CronTrigger
from fastapi import Body

try:
    from app.chain.mediaserver import MediaServerChain
except ImportError:
    from app.chain import MediaServerChain
from app.chain.subscribe import SubscribeChain
from app.chain.tmdb import TmdbChain
from app.core.config import settings
from app.core.context import MediaInfo
from app.core.event import Event, eventmanager
from app.db.subscribe_oper import SubscribeOper
from app.db.transferhistory_oper import TransferHistoryOper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType, MediaType, NotificationType

from .api_routes import NextReleaseTrackerApiMixin
from .event_handlers import NextReleaseTrackerEventMixin
from .logic import (
    coerce_int,
    normalize_tmdb_id_list,
    parse_season_token,
    season_fully_exists,
    select_ready_collection_movies,
    select_ready_tv_seasons,
)
from .scan_workflow import NextReleaseTrackerScanMixin
from .state import TrackerStateStore


class ScanBudgetExhausted(RuntimeError):
    pass


class NextReleaseTracker(
    NextReleaseTrackerApiMixin,
    NextReleaseTrackerEventMixin,
    NextReleaseTrackerScanMixin,
    _PluginBase,
):
    plugin_name = "追剧助手"
    plugin_desc = (
        "只追你明确加入名单的剧集和电影；按设定周期均摊检查量，发现新一季或同系列下一部后提醒一次。"
    )
    plugin_icon = "nextreleasetracker.png"
    plugin_version = "1.1.25"
    plugin_author = "morningstar-ski"
    author_url = "https://github.com/morningstar-ski"
    plugin_config_prefix = "nextreleasetracker_"
    plugin_order = 30
    auth_level = 1
    diagnostic_tv_tmdb_id = 9900001
    diagnostic_tv_title = "NRT Diagnostic Series"
    FORM_CANDIDATE_LIMIT = 120
    FORM_CANDIDATE_PAGE_SIZE = 15
    SELECTED_TRACK_PAGE_SIZE = 10
    DEFAULT_CRON = "0 3 * * 1"
    MAX_TMDB_CALLS_PER_MINUTE_CAP = 5
    SCAN_TICK_CRON = "* * * * *"
    CONFIG_FIELDS = (
        "enabled",
        "notify",
        "enable_tv",
        "enable_movie",
        "backfill_on_enable",
        "onlyonce",
        "cron",
        "grace_days",
        "history_days",
        "log_retention",
        "max_tmdb_calls_per_minute",
        "tracked_tv_ids",
        "tracked_movie_ids",
        "manual_tv_seasons",
        "manual_movie_mappings",
    )

    _enabled = False
    _notify = True
    _enable_tv = True
    _enable_movie = False
    _backfill_on_enable = False
    _onlyonce = False
    _cron = DEFAULT_CRON
    _grace_days = 3
    _history_days = 365
    _log_retention = 200
    _max_tmdb_calls_per_minute = MAX_TMDB_CALLS_PER_MINUTE_CAP
    _selected_tv_ids: List[int] = []
    _selected_movie_ids: List[int] = []
    _manual_tv_seasons: Dict[int, int] = {}

    _state_store: Optional[TrackerStateStore] = None
    _tmdb_chain: Optional[TmdbChain] = None
    _subscribe_chain: Optional[SubscribeChain] = None
    _media_server_chain: Optional[MediaServerChain] = None
    _scan_lock: Lock = Lock()

    def init_plugin(self, config: dict = None):
        config = config or {}
        normalized_config = self._normalize_config(config)
        self._enabled = normalized_config["enabled"]
        self._notify = normalized_config["notify"]
        self._enable_tv = normalized_config["enable_tv"]
        self._enable_movie = normalized_config["enable_movie"]
        self._backfill_on_enable = normalized_config["backfill_on_enable"]
        self._onlyonce = normalized_config["onlyonce"]
        self._cron = normalized_config["cron"]
        self._grace_days = normalized_config["grace_days"]
        self._history_days = normalized_config["history_days"]
        self._log_retention = normalized_config["log_retention"]
        self._max_tmdb_calls_per_minute = normalized_config["max_tmdb_calls_per_minute"]
        self._selected_tv_ids = []
        self._selected_movie_ids = []

        self._state_store = TrackerStateStore(
            self.get_data,
            self.save_data,
            self.del_data,
            log_limit=self._log_retention,
        )
        self._tmdb_chain = TmdbChain()
        self._subscribe_chain = SubscribeChain()
        self._media_server_chain = MediaServerChain()
        self._restore_selected_track_state(config, normalized_config)
        self._manual_tv_seasons = self._parse_manual_tv_season_text(
            normalized_config.get("manual_tv_seasons")
        )
        if self._selected_movie_ids and "enable_movie" not in config:
            self._enable_movie = True
            normalized_config["enable_movie"] = True
        if "manual_movie_mappings" in config:
            self._sync_manual_mappings_from_text(normalized_config["manual_movie_mappings"])
        else:
            normalized_config["manual_movie_mappings"] = self._serialize_manual_mapping_text(
                self._state_store.get_manual_mappings()
            )
        normalized_config["tracked_tv_ids"] = self._serialize_track_selection(self._selected_tv_ids)
        normalized_config["tracked_movie_ids"] = self._serialize_track_selection(self._selected_movie_ids)
        self._sync_selected_track_state()
        self._bootstrap_selected_tracks_from_local_catalog()
        if not config or self._config_needs_cleanup(config, normalized_config):
            self.update_config(self._current_config_snapshot())

        if self._backfill_on_enable and self._enabled and not self._state_store.has_tracks():
            runtime = self._state_store.get_runtime_state()
            if not runtime.get("bootstrap_history_imported"):
                summary = self._import_transfer_history(days=self._history_days, reason="bootstrap")
                self._state_store.update_runtime(
                    {
                        "bootstrap_history_imported": True,
                        "bootstrap_history_summary": summary,
                    }
                )

    def get_state(self) -> bool:
        return bool(self._enabled or self._onlyonce)

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return NextReleaseTrackerApiMixin.get_api(self)
    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        state = self._ensure_state_store().snapshot()
        tv_tracks = state.get(TrackerStateStore.KEY_TRACKED_TV, {})
        movie_tracks = state.get(TrackerStateStore.KEY_TRACKED_MOVIE, {})
        tv_candidates, _ = self._sorted_form_candidates(MediaType.TV.value)
        movie_candidates, _ = self._sorted_form_candidates(MediaType.MOVIE.value)
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VAlert",
                        "props": {
                            "type": "info",
                            "variant": "tonal",
                            "text": (
                                "这里就是你的追更名单设置。"
                                "把想继续追的剧或电影加到下面后，"
                                "插件会帮你留意后面的新一季或下一部；提醒一次后，这条追踪就会自动结束。"
                            ),
                        },
                    },
                    self._form_section_card(
                        title="基础开关与定时任务",
                        subtitle="先打开插件，再设置希望多久完成一轮检查。插件会按这个周期把名单均摊到每分钟执行，不会在某一个时刻一次性扫完整个名单。",
                        content=[
                            {
                                "component": "VRow",
                                "content": [
                                    self._col(3, self._switch("enabled", "启用插件")),
                                    self._col(3, self._switch("notify", "发送通知")),
                                    self._col(3, self._switch("enable_tv", "启用剧集追踪")),
                                    self._col(3, self._switch("enable_movie", "启用电影追踪")),
                                ],
                            },
                            {
                                "component": "VRow",
                                "content": [
                                    self._col(3, self._switch("onlyonce", "立即执行一次")),
                                    self._col(3, self._textfield("grace_days", "提前天数", "默认 3")),
                                    self._col(3, self._textfield("cron", "均摊周期 Cron", "0 3 * * 1")),
                                    self._col(3, self._textfield("history_days", "历史回填天数", "默认 365")),
                                ],
                            },
                            {
                                "component": "VAlert",
                                "props": {
                                    "type": "info",
                                    "variant": "tonal",
                                    "text": (
                                        "例如填 `0 3 * * 1` 代表希望一周内扫完一轮；"
                                        "插件内部会按分钟均摊推进，并把 TMDB 调用限制在每分钟最多 5 次。"
                                    ),
                                },
                            },
                        ],
                    ),
                    self._form_selection_editor(
                        title="剧集追更名单",
                        subtitle="添加想继续追的剧。插件会结合本地记录判断你已追到哪一季，后续只关注更新；可直接搜索剧名加入。",
                        media_label="剧集",
                        model_key="tracked_tv_ids",
                        add_model="tv_candidate_id",
                        remove_model="tv_remove_id",
                        search_model="tv_candidate_search_text",
                        page_model="tv_candidate_page",
                        placeholder="60625",
                        saved_table=self._selection_current_table_live(
                            media_label="剧集",
                            model_key="tracked_tv_ids",
                            page_model="tv_selected_page",
                            selected_ids=self._selected_tv_ids,
                            track_lookup=tv_tracks,
                            is_movie=False,
                            candidates=tv_candidates,
                            manual_tv_season_model="manual_tv_seasons",
                        ),
                        candidates=tv_candidates,
                        add_button_expr=self._selection_add_tv_expr(
                            model_key="tracked_tv_ids",
                            add_model="tv_candidate_id",
                            season_model="tv_manual_season",
                            manual_season_model="manual_tv_seasons",
                            notice_model="tv_pending_notice",
                        ),
                        add_aux_model="tv_manual_season",
                        add_aux_label="已追到第几季",
                        add_aux_placeholder="例如 2 或 S02",
                        extra_content=[
                            self._selection_notice_field(
                                model="tv_pending_notice",
                                label="待保存操作",
                            )
                        ],
                        post_table_content=[
                            {
                                "component": "VAlert",
                                "props": {
                                    "type": "info",
                                    "variant": "tonal",
                                    "text": "如果某部剧没有本地历史或订阅线索，可以在下面按 TMDB 编号补“已追到第几季”。支持填 2 或 S02；点“更新季数”后再保存，或者直接填完点保存，都能生效。",
                                },
                            },
                            {
                                "component": "VRow",
                                "content": [
                                    self._col(
                                        5,
                                        {
                                            "component": "VTextField",
                                            "props": {
                                                "model": "tv_season_edit_tmdb_id",
                                                "label": "剧集编号（已加入）",
                                                "placeholder": "247718",
                                                "clearable": True,
                                            },
                                        },
                                    ),
                                    self._col(
                                        3,
                                        {
                                            "component": "VTextField",
                                            "props": {
                                                "model": "tv_season_edit_value",
                                                "label": "已追到第几季",
                                                "placeholder": "2 或 S02",
                                                "clearable": True,
                                            },
                                        },
                                    ),
                                    self._col(
                                        2,
                                        self._form_action_button(
                                            "更新季数",
                                            "primary",
                                            "mdi-pencil",
                                            self._selection_update_tv_season_expr(
                                                model_key="tracked_tv_ids",
                                                tmdb_model="tv_season_edit_tmdb_id",
                                                season_model="tv_season_edit_value",
                                                manual_season_model="manual_tv_seasons",
                                                notice_model="tv_pending_notice",
                                            ),
                                            "mt-md-6",
                                        ),
                                    ),
                                    self._col(
                                        2,
                                        self._form_action_button(
                                            "清除季数",
                                            "warning",
                                            "mdi-eraser",
                                            self._selection_clear_tv_season_expr(
                                                tmdb_model="tv_season_edit_tmdb_id",
                                                season_model="tv_season_edit_value",
                                                manual_season_model="manual_tv_seasons",
                                                notice_model="tv_pending_notice",
                                            ),
                                            "mt-md-6",
                                        ),
                                    ),
                                ],
                            },
                        ],
                    ),
                    self._form_selection_editor(
                        title="电影追更名单",
                        subtitle="在这里添加你想继续追的电影。加入后，插件会自动帮你关注这个系列后面的新片。可以直接搜电影名后加入。",
                        media_label="电影",
                        model_key="tracked_movie_ids",
                        add_model="movie_candidate_id",
                        remove_model="movie_remove_id",
                        search_model="movie_candidate_search_text",
                        page_model="movie_candidate_page",
                        placeholder="550",
                        saved_table=self._selection_current_table_live(
                            media_label="电影",
                            model_key="tracked_movie_ids",
                            page_model="movie_selected_page",
                            selected_ids=self._selected_movie_ids,
                            track_lookup=movie_tracks,
                            is_movie=True,
                            candidates=movie_candidates,
                        ),
                        candidates=movie_candidates,
                    ),
                    self._form_section_card(
                        title="高级设置（一般不用动）",
                        subtitle="只有在你想补扫旧记录、调低分钟调用上限，或者某个电影系列没自动认出来时，才需要来这里设置。",
                        content=[
                            {
                                "component": "VRow",
                                "content": [
                                    self._col(6, self._switch("backfill_on_enable", "首次启用时回填历史")),
                                    self._col(3, self._textfield("log_retention", "日志保留条数", "默认 200")),
                                    self._col(3, self._textfield("max_tmdb_calls_per_minute", "每分钟 TMDB 上限", "默认 5，最高 5")),
                                ],
                            },
                            {
                                "component": "VAlert",
                                "props": {
                                    "type": "warning",
                                    "variant": "tonal",
                                    "text": (
                                        "只有加入追更名单的内容，插件才会继续帮你关注。"
                                        "电影自动找续作时只认 TMDB collection；没有 collection 的电影，必须手动补关联。"
                                    ),
                                },
                            },
                        ],
                    ),
                    self._form_section_card(
                        title="电影手动关联（高级）",
                        subtitle="如果 TMDB 没给某部电影挂 collection，或者你想指定更明确的续作链，可以在这里手动告诉插件后面还要继续关注哪些电影。",
                        content=[
                            {
                                "component": "VAlert",
                                "props": {
                                    "type": "info",
                                    "variant": "tonal",
                                    "text": (
                                        "每行一条，左边填你加入名单的源电影 TMDB 编号，右边填想继续关注的目标电影 TMDB 编号。"
                                        "空行会自动忽略，保存后生效。"
                                    ),
                                },
                            },
                            self._textarea(
                                "manual_movie_mappings",
                                "每行一条电影关联",
                                "603=604,605 # 示例：黑客帝国(603) 后面继续关注 重装上阵(604)、矩阵革命(605)",
                            ),
                            {
                                "component": "VAlert",
                                "props": {
                                    "type": "warning",
                                    "variant": "tonal",
                                    "text": (
                                        "格式示例：603=604,605。这里的数字都是 TMDB 编号；例如黑客帝国(603) -> 黑客帝国2：重装上阵(604)、黑客帝国3：矩阵革命(605)。"
                                        "这只对电影追踪生效，电视剧不用填。"
                                    ),
                                },
                            },
                        ],
                    ),
                ],
            }
        ], {
            "enabled": False,
            "notify": True,
            "enable_tv": True,
            "enable_movie": False,
            "backfill_on_enable": False,
            "onlyonce": False,
            "cron": "0 3 * * 1",
            "grace_days": 3,
            "history_days": 365,
            "log_retention": 200,
            "max_tmdb_calls_per_minute": 5,
            "tracked_tv_ids": "",
            "tracked_movie_ids": "",
            "manual_tv_seasons": "",
            "manual_movie_mappings": "",
            "tv_candidate_search_text": "",
            "movie_candidate_search_text": "",
            "tv_candidate_page": 1,
            "movie_candidate_page": 1,
            "tv_selected_page": 1,
            "movie_selected_page": 1,
            "tv_candidate_id": "",
            "tv_manual_season": "",
            "tv_pending_notice": "",
            "tv_season_edit_tmdb_id": "",
            "tv_season_edit_value": "",
            "tv_remove_id": "",
            "movie_candidate_id": "",
            "movie_remove_id": "",
        }

    def get_page(self) -> List[dict]:
        state = self._ensure_state_store().snapshot()
        tv_tracks = state.get(TrackerStateStore.KEY_TRACKED_TV, {})
        movie_tracks = state.get(TrackerStateStore.KEY_TRACKED_MOVIE, {})
        mappings = state.get(TrackerStateStore.KEY_MANUAL_MAPPINGS, {})
        logs = list(reversed(state.get(TrackerStateStore.KEY_ACTION_LOG, [])))[:20]
        runtime = state.get(TrackerStateStore.KEY_RUNTIME_STATE, {})
        scan_plan = runtime.get("scan_plan") or {}
        cycle_stats = runtime.get("current_cycle_stats") or {}
        tmdb_rate_limit = runtime.get("tmdb_rate_limit") or {}
        last_scan = runtime.get("last_scan_summary") or {}
        last_history = runtime.get("last_history_import") or {}
        last_diagnostic = runtime.get("last_diagnostic") or {}
        scan_finished_at = runtime.get("last_scan_finished_at") or "未执行"
        scan_scope = runtime.get("last_scan_scope") or "all"
        scan_reason = runtime.get("last_scan_reason") or "-"
        selected_tv_count = len(self._selected_tv_ids)
        selected_movie_count = len(self._selected_movie_ids)
        history_summary = "未执行"
        if last_history:
            history_summary = (
                f"最近一次回填：{last_history.get('records', 0)} 条记录"
                f"（TV {last_history.get('tv_imported', 0)} / 电影 {last_history.get('movie_imported', 0)} / "
                f"错误 {last_history.get('errors', 0)}）"
            )

        tv_pending = sum(len(track.get("pending_seasons") or []) for track in tv_tracks.values())
        movie_pending = sum(len(track.get("pending_tmdb_ids") or []) for track in movie_tracks.values())
        selected_total = selected_tv_count + selected_movie_count
        active_total = len(tv_tracks) + len(movie_tracks)
        pending_total = tv_pending + movie_pending
        plugin_mode = "TV + 电影" if self._enable_tv and self._enable_movie else "TV-only" if self._enable_tv else "电影-only" if self._enable_movie else "未启用追踪"
        diagnostic_rows = []
        if last_diagnostic:
            diagnostic_rows = [
                ["最近校验时间", last_diagnostic.get("checked_at") or "-"],
                ["事件类型", last_diagnostic.get("event") or "-"],
                ["样本剧集", last_diagnostic.get("title") or "-"],
                ["TMDB / 季", f"{last_diagnostic.get('tmdb_id') or '-'} / S{int(last_diagnostic.get('season') or 0):02d}"],
                ["事件总线命中", "是" if last_diagnostic.get("track_detected") else "否"],
                ["自动清理", "已执行" if last_diagnostic.get("cleanup_performed") else ("已跳过" if last_diagnostic.get("cleanup") else "保留样本")],
            ]
        selected_tv_rows = self._selected_tv_rows(tv_tracks)
        selected_movie_rows = self._selected_movie_rows(movie_tracks)
        recent_signal_rows = [
            [
                log_entry.get("time") or "-",
                log_entry.get("action") or "-",
                self._log_message(log_entry),
            ]
            for log_entry in logs
            if (log_entry.get("action") or "") in {"tv_detected", "movie_detected", "track_remove"}
        ][:8]

        content: List[dict] = [
            self._page_style(),
            {
                "component": "div",
                "props": {"class": "nrt-page"},
                "content": [
                    self._page_toolbar(),
                    {
                        "component": "VAlert",
                        "props": {
                            "type": "info",
                            "variant": "tonal",
                            "text": "这里用来看你已经加入的内容、当前追踪状态，以及手动重扫。想新增或修改追更名单，请回到配置页保存。",
                        },
                    },
                    {
                        "component": "VAlert",
                        "props": {
                            "type": "warning",
                            "variant": "tonal",
                            "text": "只有加入追更名单的内容，插件才会继续帮你关注。电影默认只按 TMDB collection 自动找下一部；TMDB 没有 collection 的电影，需要去配置页手动补关联。",
                        },
                    },
                    {
                        "component": "VAlert",
                        "props": {
                            "type": "success" if self._enabled else "warning",
                            "variant": "tonal",
                            "text": (
                                f"当前模式：{plugin_mode} | 均摊周期：{self._cron or '未配置'} | "
                                f"分钟上限：{self._max_tmdb_calls_per_minute} 次 TMDB 调用 | "
                                f"通知：{'开启' if self._notify else '关闭'} | "
                                f"已加入名单：{selected_total} 项"
                            ) if self._enabled else (
                                "插件当前未启用。你现在还能手动重扫或清理列表，但自动盯更新暂时不会运行。"
                            ),
                        },
                    },
                    {
                        "component": "div",
                        "props": {"class": "nrt-summary"},
                        "content": [
                            self._stat_card("已加入名单", selected_total, f"剧集 {selected_tv_count} / 电影 {selected_movie_count}"),
                            self._stat_card("待处理候选", pending_total, f"新季 {tv_pending} / 续作 {movie_pending}"),
                            self._stat_card("最近扫描通知", last_scan.get("notifications_sent", 0), f"本轮结束 {last_scan.get('tracks_completed', 0)} 条"),
                        ],
                    },
                    self._section_card(
                        "运行概览",
                        [
                            self._status_chips(
                                [
                                    self._chip("插件已启用" if self._enabled else "插件未启用", "success" if self._enabled else "warning", "mdi-power"),
                                    self._chip(f"模式 {plugin_mode}", "primary", "mdi-view-dashboard"),
                                    self._chip(f"最近扫描 {scan_scope}", "info", "mdi-radar"),
                                    self._chip(f"分钟已用 {coerce_int(tmdb_rate_limit.get('used'), 0) or 0}/{self._max_tmdb_calls_per_minute}", "warning", "mdi-speedometer"),
                                    self._chip(f"错误 {last_scan.get('errors', 0)}", "error" if last_scan.get("errors", 0) else "success", "mdi-alert-circle-outline"),
                                ]
                            ),
                            self._simple_table(
                                ["项目", "值"],
                                [
                                    ["最近扫描时间", scan_finished_at],
                                    ["扫描来源", scan_reason],
                                    ["当前周期", self._cron or "-"],
                                    ["当前周期剩余", f"{len(scan_plan.get('pending_task_ids') or [])} / {len(scan_plan.get('task_ids') or []) or 0}"],
                                    ["本周期 tick 次数", coerce_int(cycle_stats.get("tick_count"), 0) or 0],
                                    ["本周期空跑次数", coerce_int(cycle_stats.get("idle_tick_count"), 0) or 0],
                                    ["本周期活跃次数", coerce_int(cycle_stats.get("active_tick_count"), 0) or 0],
                                    ["本周期处理任务数", coerce_int(cycle_stats.get("processed_task_count"), 0) or 0],
                                    ["本周期锁冲突跳过", coerce_int(cycle_stats.get("locked_skip_count"), 0) or 0],
                                    ["最近活跃 tick", cycle_stats.get("last_active_tick_at") or "-"],
                                    ["最近锁冲突", cycle_stats.get("last_locked_skip_at") or "-"],
                                    ["本分钟已用", f"{coerce_int(tmdb_rate_limit.get('used'), 0) or 0} / {self._max_tmdb_calls_per_minute}"],
                                    ["发送通知", last_scan.get("notifications_sent", 0)],
                                    ["结束追踪", last_scan.get("tracks_completed", 0)],
                                    ["更新追踪状态", last_scan.get("tracks_updated", 0)],
                                    ["分钟预算耗尽", "是" if last_scan.get("budget_exhausted") else "否"],
                                    ["命中已存在订阅", last_scan.get("existing_subscriptions", 0)],
                                    ["命中媒体库", last_scan.get("existing_in_library", 0)],
                                    ["历史回填摘要", history_summary],
                                ],
                            ),
                        ],
                    ),
                    self._section_card(
                        "已加入的剧集",
                        [
                            self._simple_table(
                                ["剧名", "TMDB", "状态", "当前进度", "操作"],
                                selected_tv_rows,
                                empty_text="你还没把任何剧加入追更名单。去配置页搜索后加入，这里就会显示。",
                            ),
                        ],
                    ),
                    self._section_card(
                        "已加入的电影",
                        [
                            self._simple_table(
                                ["标题", "TMDB", "状态", "当前进度", "操作"],
                                selected_movie_rows,
                                empty_text="你还没把任何电影加入追更名单。去配置页搜索后加入，这里就会显示。",
                            )
                        ],
                    ),
                    self._section_card(
                        "最近命中与结束",
                        [
                            self._simple_table(
                                ["时间", "动作", "消息"],
                                recent_signal_rows,
                                empty_text="最近还没有发现新季/续作，或没有手动移除操作。",
                            )
                        ],
                    ),
                    self._section_card(
                        "运行态诊断",
                        [
                            {
                                "component": "VAlert",
                                "props": {
                                    "type": (
                                        "info"
                                        if not last_diagnostic
                                        else ("success" if last_diagnostic.get("success") else "warning")
                                    ),
                                    "variant": "tonal",
                                    "text": (
                                        "通过诊断按钮可在运行进程内发送合成 TV 完结事件，默认写入后立即清理，仅用于校验事件总线、状态落库和页面反馈。"
                                        if not last_diagnostic
                                        else (
                                            f"最近一次诊断：{last_diagnostic.get('event') or '-'} / "
                                            f"{last_diagnostic.get('title') or '-'} / "
                                            f"{'通过' if last_diagnostic.get('success') else '未通过'}"
                                        )
                                    ),
                                },
                            },
                            self._simple_table(
                                ["项目", "值"],
                                diagnostic_rows,
                                empty_text="尚未执行运行态诊断。",
                            ),
                        ],
                    ),
                    self._section_card(
                        "手工映射",
                        [
                            self._simple_table(
                                ["源 TMDB", "目标 TMDB 列表", "备注", "更新时间", "操作"],
                                [
                                    [
                                        mapping.get("source_tmdb_id"),
                                        self._join_values(mapping.get("target_tmdb_ids")),
                                        mapping.get("note") or "-",
                                        mapping.get("updated_at") or "-",
                                        self._page_manual_map_remove_button(coerce_int(mapping.get("source_tmdb_id"))),
                                    ]
                                    for mapping in sorted(
                                        mappings.values(),
                                        key=lambda item: coerce_int(item.get("source_tmdb_id"), 0) or 0,
                                    )
                                ],
                                empty_text="暂无手工映射。",
                            )
                        ],
                    ),
                    self._section_card(
                        "最近动作日志",
                        [
                            self._simple_table(
                                ["时间", "级别", "动作", "消息"],
                                [
                                    [
                                        log_entry.get("time") or "-",
                                        self._chip_cell(
                                            str(log_entry.get("level") or "info").upper(),
                                            self._level_color(log_entry.get("level")),
                                            self._level_icon(log_entry.get("level")),
                                        ),
                                        log_entry.get("action") or "-",
                                        self._log_message(log_entry),
                                    ]
                                    for log_entry in logs
                                ],
                                empty_text="暂无动作日志，执行一次手动重扫后会在这里显示。",
                            )
                        ],
                    ),
                ],
            },
        ]
        return content

    def get_service(self) -> List[Dict[str, Any]]:
        services: List[Dict[str, Any]] = []
        if self._enabled and (self._enable_tv or self._enable_movie) and self._cron:
            services.append(
                {
                    "id": "scan_tick",
                    "name": "Next release tracker paced scan tick",
                    "trigger": CronTrigger.from_crontab(self.SCAN_TICK_CRON),
                    "func": self.service_scan,
                    "func_kwargs": {"scope": "all", "reason": "cron_tick", "notify": None},
                    "kwargs": {},
                }
            )
        if self._onlyonce:
            services.append(
                {
                    "id": "once",
                    "name": "Next release tracker run once",
                    "trigger": "date",
                    "func": self.run_once_scan,
                    "func_kwargs": {},
                    "kwargs": {
                        "run_date": datetime.now(pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                    },
                }
            )
        return services

    @eventmanager.register(EventType.SubscribeComplete)
    def on_subscribe_complete(self, event: Event):
        return NextReleaseTrackerEventMixin.on_subscribe_complete(self, event)

    @eventmanager.register(EventType.TransferComplete)
    def on_transfer_complete(self, event: Event):
        return NextReleaseTrackerEventMixin.on_transfer_complete(self, event)

    def service_scan(self, scope: str = "all", reason: str = "cron_tick", notify: Optional[bool] = None):
        return NextReleaseTrackerScanMixin.service_scan(self, scope=scope, reason=reason, notify=notify)

    def run_once_scan(self):
        return NextReleaseTrackerScanMixin.run_once_scan(self)

    def stop_service(self):
        return NextReleaseTrackerScanMixin.stop_service(self)

    def api_tracks(self) -> Dict[str, Any]:
        return NextReleaseTrackerApiMixin.api_tracks(self)

    def api_automation_health(self) -> Dict[str, Any]:
        return NextReleaseTrackerApiMixin.api_automation_health(self)

    def api_rescan(self, payload: Optional[dict] = Body(default=None)) -> Dict[str, Any]:
        return NextReleaseTrackerApiMixin.api_rescan(self, payload)

    def api_track_add(self, payload: Optional[dict] = Body(default=None)) -> Dict[str, Any]:
        return NextReleaseTrackerApiMixin.api_track_add(self, payload)

    def api_track_remove(self, payload: Optional[dict] = Body(default=None)) -> Dict[str, Any]:
        return NextReleaseTrackerApiMixin.api_track_remove(self, payload)

    def api_import_transfer_history(self, payload: Optional[dict] = Body(default=None)) -> Dict[str, Any]:
        return NextReleaseTrackerApiMixin.api_import_transfer_history(self, payload)

    def api_diagnostic_event(self, payload: Optional[dict] = Body(default=None)) -> Dict[str, Any]:
        return NextReleaseTrackerApiMixin.api_diagnostic_event(self, payload)

    def api_manual_map_add(self, payload: Optional[dict] = Body(default=None)) -> Dict[str, Any]:
        return NextReleaseTrackerApiMixin.api_manual_map_add(self, payload)

    def api_manual_map_remove(self, payload: Optional[dict] = Body(default=None)) -> Dict[str, Any]:
        return NextReleaseTrackerApiMixin.api_manual_map_remove(self, payload)

    def _run_rescan(self, *, scope: str, reason: str, notify: Optional[bool]) -> Dict[str, Any]:
        return NextReleaseTrackerScanMixin._run_rescan(self, scope=scope, reason=reason, notify=notify)

    def _process_scan_task(
        self,
        *,
        task: Dict[str, Any],
        summary: Dict[str, Any],
        notify_flag: bool,
        scan_cache: Dict[str, Dict[Any, Any]],
        tmdb_budget: Dict[str, Any],
    ) -> None:
        return NextReleaseTrackerScanMixin._process_scan_task(
            self,
            task=task,
            summary=summary,
            notify_flag=notify_flag,
            scan_cache=scan_cache,
            tmdb_budget=tmdb_budget,
        )

    def _process_tv_scan_task(
        self,
        *,
        task: Dict[str, Any],
        summary: Dict[str, Any],
        notify_flag: bool,
        scan_cache: Dict[str, Dict[Any, Any]],
        tmdb_budget: Dict[str, Any],
    ) -> None:
        return NextReleaseTrackerScanMixin._process_tv_scan_task(
            self,
            task=task,
            summary=summary,
            notify_flag=notify_flag,
            scan_cache=scan_cache,
            tmdb_budget=tmdb_budget,
        )

    def _process_movie_scan_task(
        self,
        *,
        task: Dict[str, Any],
        summary: Dict[str, Any],
        notify_flag: bool,
        scan_cache: Dict[str, Dict[Any, Any]],
        tmdb_budget: Dict[str, Any],
    ) -> None:
        return NextReleaseTrackerScanMixin._process_movie_scan_task(
            self,
            task=task,
            summary=summary,
            notify_flag=notify_flag,
            scan_cache=scan_cache,
            tmdb_budget=tmdb_budget,
        )

    def _resolve_scan_task_plan(self, *, scope: str, reason: str) -> Dict[str, Any]:
        return NextReleaseTrackerScanMixin._resolve_scan_task_plan(self, scope=scope, reason=reason)

    def _build_scheduled_task_plan(self, *, scope: str, task_map: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        return NextReleaseTrackerScanMixin._build_scheduled_task_plan(self, scope=scope, task_map=task_map)

    def _build_scan_tasks(self, scope: str) -> List[Dict[str, Any]]:
        return NextReleaseTrackerScanMixin._build_scan_tasks(self, scope)

    @staticmethod
    def _interleave_scan_groups(groups: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        return NextReleaseTrackerScanMixin._interleave_scan_groups(groups)

    def _commit_scan_task_progress(self, task_plan: Dict[str, Any], processed_task_ids: List[str]) -> None:
        return NextReleaseTrackerScanMixin._commit_scan_task_progress(self, task_plan, processed_task_ids)

    def _remaining_scan_tasks(self, task_plan: Dict[str, Any], processed_task_ids: List[str]) -> int:
        return NextReleaseTrackerScanMixin._remaining_scan_tasks(self, task_plan, processed_task_ids)

    def _create_tmdb_budget(self) -> Dict[str, Any]:
        return NextReleaseTrackerScanMixin._create_tmdb_budget(self)

    def _consume_tmdb_budget(self, tmdb_budget: Dict[str, Any]) -> None:
        return NextReleaseTrackerScanMixin._consume_tmdb_budget(self, tmdb_budget)

    def _load_tv_seasons(
        self,
        *,
        tmdb_id: int,
        scan_cache: Dict[str, Dict[Any, Any]],
        tmdb_budget: Dict[str, Any],
    ) -> List[Any]:
        return NextReleaseTrackerScanMixin._load_tv_seasons(
            self,
            tmdb_id=tmdb_id,
            scan_cache=scan_cache,
            tmdb_budget=tmdb_budget,
        )

    def _load_movie_collection(
        self,
        *,
        collection_id: int,
        scan_cache: Dict[str, Dict[Any, Any]],
        tmdb_budget: Dict[str, Any],
    ) -> List[Any]:
        return NextReleaseTrackerScanMixin._load_movie_collection(
            self,
            collection_id=collection_id,
            scan_cache=scan_cache,
            tmdb_budget=tmdb_budget,
        )

    def _import_transfer_history(self, *, days: int, reason: str) -> Dict[str, Any]:
        cutoff = datetime.now() - timedelta(days=max(days, 0))
        histories = TransferHistoryOper().list_by_date(cutoff.strftime("%Y-%m-%d %H:%M:%S")) or []
        selected_tv = set(self._selected_tv_ids)
        selected_movie = set(self._selected_movie_ids)
        summary = {
            "days": days,
            "reason": reason,
            "records": 0,
            "tv_imported": 0,
            "movie_imported": 0,
            "errors": 0,
        }

        for history in histories:
            if not getattr(history, "status", False):
                continue
            tmdb_id = coerce_int(getattr(history, "tmdbid", None))
            media_type = str(getattr(history, "type", "") or "")
            if not tmdb_id:
                continue

            try:
                if media_type == MediaType.TV.value and self._enable_tv:
                    if tmdb_id not in selected_tv:
                        continue
                    season = parse_season_token(getattr(history, "seasons", None)) or 1
                    self._ensure_state_store().acknowledge_tv_completion(
                        tmdb_id=tmdb_id,
                        title=str(getattr(history, "title", "") or f"TMDB-{tmdb_id}"),
                        year=self._as_str(getattr(history, "year", None)),
                        season=season,
                        source="history_import",
                        last_transfer_history_id=coerce_int(getattr(history, "id", None)),
                    )
                    summary["records"] += 1
                    summary["tv_imported"] += 1
                    continue

                if media_type == MediaType.MOVIE.value and self._enable_movie:
                    if tmdb_id not in selected_movie:
                        continue
                    media = self._recognize_media(tmdb_id=tmdb_id, mtype=MediaType.MOVIE)
                    title = self._as_str(getattr(history, "title", None)) or self._media_value(media, "title") or f"TMDB-{tmdb_id}"
                    year = self._as_str(getattr(history, "year", None)) or self._as_str(self._media_value(media, "year"))
                    collection_id = coerce_int(self._media_value(media, "collection_id"))
                    track = self._ensure_state_store().upsert_movie_track(
                        anchor_tmdb_id=tmdb_id,
                        title=title,
                        year=year,
                        source="history_import",
                        collection_id=collection_id,
                        known_tmdb_ids=[tmdb_id],
                        last_transfer_history_id=coerce_int(getattr(history, "id", None)),
                    )
                    self._ensure_state_store().acknowledge_movie_completion(
                        track_key=track["track_key"],
                        tmdb_id=tmdb_id,
                        title=title,
                        year=year,
                        collection_id=collection_id,
                        source="history_import",
                    )
                    summary["records"] += 1
                    summary["movie_imported"] += 1
            except Exception as exc:
                logger.exception("[NextReleaseTracker] history import failed")
                self._log("error", "history_import", f"Failed to import transfer history: {exc}", {"tmdb_id": tmdb_id})
                summary["errors"] += 1

        self._ensure_state_store().update_runtime({"last_history_import": summary})
        self._log("info", "history_import", f"Transfer history import completed: tv={summary['tv_imported']}, movie={summary['movie_imported']}, errors={summary['errors']}", {"days": days, "reason": reason})
        return summary

    def _track_movie_event(
        self,
        *,
        tmdb_id: int,
        title: str,
        year: Optional[str],
        collection_id: Optional[int],
        source: str,
        last_transfer_history_id: Optional[int] = None,
    ) -> dict:
        return NextReleaseTrackerEventMixin._track_movie_event(
            self,
            tmdb_id=tmdb_id,
            title=title,
            year=year,
            collection_id=collection_id,
            source=source,
            last_transfer_history_id=last_transfer_history_id,
        )

    def _notify_summary(self, summary: Dict[str, Any]) -> None:
        return NextReleaseTrackerScanMixin._notify_summary(self, summary)
    @staticmethod
    def _coerce_bool(value: Any, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value).strip().lower()
        if not text:
            return default
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off"}:
            return False
        return default

    def _normalize_cron_expr(self, value: Any) -> str:
        default = self.DEFAULT_CRON
        text = str(value or default).strip() or default
        try:
            CronTrigger.from_crontab(text)
        except Exception:
            logger.warning(f"[NextReleaseTracker] invalid cron expression ignored: {text}")
            return default
        return text

    def _normalize_tmdb_call_limit(self, value: Any) -> int:
        return self._normalized_tmdb_call_limit(value)

    @classmethod
    def _normalized_tmdb_call_limit(cls, value: Any) -> int:
        raw = coerce_int(value, cls.MAX_TMDB_CALLS_PER_MINUTE_CAP) or cls.MAX_TMDB_CALLS_PER_MINUTE_CAP
        return min(max(raw, 1), cls.MAX_TMDB_CALLS_PER_MINUTE_CAP)

    @staticmethod
    def _cron_step_value(field: str) -> Optional[int]:
        text = str(field or "").strip()
        match = re.match(r"^\*/(\d+)$", text)
        if not match:
            return None
        step = coerce_int(match.group(1))
        return step if step and step > 0 else None

    @classmethod
    def _estimate_cron_period_minutes(cls, expr: str) -> int:
        fields = str(expr or cls.DEFAULT_CRON).split()
        if len(fields) != 5:
            return 7 * 24 * 60
        minute, hour, day, month, weekday = fields
        if fields == ["*", "*", "*", "*", "*"]:
            return 1
        if cls._cron_step_value(minute) and hour == "*" and day == "*" and month == "*" and weekday == "*":
            return cls._cron_step_value(minute) or 1
        if minute != "*" and hour == "*" and day == "*" and month == "*" and weekday == "*":
            return 60
        if minute != "*" and cls._cron_step_value(hour) and day == "*" and month == "*" and weekday == "*":
            return (cls._cron_step_value(hour) or 1) * 60
        if minute != "*" and hour != "*" and cls._cron_step_value(day) and month == "*" and weekday == "*":
            return (cls._cron_step_value(day) or 1) * 24 * 60
        if minute != "*" and hour != "*" and day == "*" and month == "*" and weekday == "*":
            return 24 * 60
        if minute != "*" and hour != "*" and day == "*" and month == "*" and weekday != "*":
            return 7 * 24 * 60
        if minute != "*" and hour != "*" and day != "*" and month == "*" and weekday == "*":
            return 31 * 24 * 60
        if minute != "*" and hour != "*" and day != "*" and month != "*" and weekday == "*":
            return 366 * 24 * 60
        return 7 * 24 * 60

    def _normalize_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        tracked_tv_ids = self._parse_track_selection(config.get("tracked_tv_ids"))
        manual_tv_seasons = self._parse_manual_tv_season_text(config.get("manual_tv_seasons"))

        transient_add_tmdb_id = coerce_int(config.get("tv_candidate_id"))
        transient_add_season = parse_season_token(config.get("tv_manual_season"))
        if transient_add_tmdb_id and transient_add_season and transient_add_season > 0:
            tracked_tv_ids = [item for item in tracked_tv_ids if item != int(transient_add_tmdb_id)]
            tracked_tv_ids.insert(0, int(transient_add_tmdb_id))
            manual_tv_seasons[int(transient_add_tmdb_id)] = int(transient_add_season)

        transient_edit_tmdb_id = coerce_int(config.get("tv_season_edit_tmdb_id"))
        transient_edit_season = parse_season_token(config.get("tv_season_edit_value"))
        if transient_edit_tmdb_id and transient_edit_season and transient_edit_season > 0:
            tracked_tv_ids = [item for item in tracked_tv_ids if item != int(transient_edit_tmdb_id)]
            tracked_tv_ids.insert(0, int(transient_edit_tmdb_id))
            manual_tv_seasons[int(transient_edit_tmdb_id)] = int(transient_edit_season)

        tracked_tv_id_set = set(tracked_tv_ids)
        manual_tv_seasons = {
            tmdb_id: season
            for tmdb_id, season in manual_tv_seasons.items()
            if tmdb_id in tracked_tv_id_set and season > 0
        }

        return {
            "enabled": self._coerce_bool(config.get("enabled"), False),
            "notify": self._coerce_bool(config.get("notify"), True),
            "enable_tv": self._coerce_bool(config.get("enable_tv"), True),
            "enable_movie": self._coerce_bool(config.get("enable_movie"), False),
            "backfill_on_enable": self._coerce_bool(config.get("backfill_on_enable"), False),
            "onlyonce": self._coerce_bool(config.get("onlyonce"), False),
            "cron": self._normalize_cron_expr(config.get("cron")),
            "grace_days": max(coerce_int(config.get("grace_days"), 3) or 3, 0),
            "history_days": max(coerce_int(config.get("history_days"), 365) or 365, 0),
            "log_retention": max(coerce_int(config.get("log_retention"), 200) or 200, 20),
            "max_tmdb_calls_per_minute": self._normalize_tmdb_call_limit(config.get("max_tmdb_calls_per_minute")),
            "tracked_tv_ids": self._serialize_track_selection(tracked_tv_ids),
            "tracked_movie_ids": self._serialize_track_selection(self._parse_track_selection(config.get("tracked_movie_ids"))),
            "manual_tv_seasons": self._serialize_manual_tv_season_text(manual_tv_seasons),
            "manual_movie_mappings": self._serialize_manual_mapping_text(
                self._parse_manual_mapping_text(config.get("manual_movie_mappings"))
            ),
        }

    def _current_config_snapshot(self) -> Dict[str, Any]:
        current_mappings = self._state_store.get_manual_mappings() if self._state_store else {}
        manual_tv_seasons = {
            tmdb_id: season
            for tmdb_id, season in self._manual_tv_seasons.items()
            if tmdb_id in set(self._selected_tv_ids) and season > 0
        }
        return {
            "enabled": self._enabled,
            "notify": self._notify,
            "enable_tv": self._enable_tv,
            "enable_movie": self._enable_movie,
            "backfill_on_enable": self._backfill_on_enable,
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "grace_days": self._grace_days,
            "history_days": self._history_days,
            "log_retention": self._log_retention,
            "max_tmdb_calls_per_minute": self._max_tmdb_calls_per_minute,
            "tracked_tv_ids": self._serialize_track_selection(self._selected_tv_ids),
            "tracked_movie_ids": self._serialize_track_selection(self._selected_movie_ids),
            "manual_tv_seasons": self._serialize_manual_tv_season_text(manual_tv_seasons),
            "manual_movie_mappings": self._serialize_manual_mapping_text(current_mappings),
        }

    def _config_needs_cleanup(self, config: Dict[str, Any], normalized_config: Dict[str, Any]) -> bool:
        if any(key not in self.CONFIG_FIELDS for key in config.keys()):
            return True
        return any(config.get(key) != value for key, value in normalized_config.items())

    def _save_current_config(self) -> None:
        self._persist_selected_track_state()
        self.update_config(self._current_config_snapshot())

    def _restore_selected_track_state(self, config: Dict[str, Any], normalized_config: Dict[str, Any]) -> None:
        store = self._ensure_state_store()
        config_has_tv_selection_key = "tracked_tv_ids" in config
        config_has_movie_selection_key = "tracked_movie_ids" in config
        stored_tv_ids = store.get_selected_tv_ids() if store.has_selected_track_ids() else []
        stored_movie_ids = store.get_selected_movie_ids() if store.has_selected_track_ids() else []
        recovered_tv_ids, recovered_movie_ids = self._recover_selected_track_ids_from_tracks()
        config_tv_ids = self._parse_track_selection(normalized_config["tracked_tv_ids"])
        config_movie_ids = self._parse_track_selection(normalized_config["tracked_movie_ids"])
        self._selected_tv_ids = (
            config_tv_ids if config_has_tv_selection_key else stored_tv_ids or recovered_tv_ids
        )
        self._selected_movie_ids = (
            config_movie_ids if config_has_movie_selection_key else stored_movie_ids or recovered_movie_ids
        )
        self._persist_selected_track_state()

    def _recover_selected_track_ids_from_tracks(self) -> Tuple[List[int], List[int]]:
        store = self._ensure_state_store()
        return (
            normalize_tmdb_id_list(
                coerce_int(track.get("tmdb_id"))
                for track in store.get_tv_tracks().values()
            ),
            normalize_tmdb_id_list(
                coerce_int(track.get("anchor_tmdb_id"))
                for track in store.get_movie_tracks().values()
            ),
        )

    def _persist_selected_track_state(self) -> None:
        store = self._ensure_state_store()
        self._selected_tv_ids = store.set_selected_tv_ids(self._selected_tv_ids)
        self._selected_movie_ids = store.set_selected_movie_ids(self._selected_movie_ids)

    def _sync_manual_mappings_from_text(self, value: Any) -> None:
        store = self._ensure_state_store()
        parsed_mappings = self._parse_manual_mapping_text(value)
        existing_mappings = store.get_manual_mappings()

        existing_source_ids = {
            coerce_int(mapping.get("source_tmdb_id"), 0) or coerce_int(source_key, 0) or 0
            for source_key, mapping in existing_mappings.items()
        }
        for source_tmdb_id in sorted(source_id for source_id in existing_source_ids if source_id and source_id not in parsed_mappings):
            store.remove_manual_mapping(source_tmdb_id)

        for source_tmdb_id, mapping in parsed_mappings.items():
            current = store.get_manual_mapping(source_tmdb_id) or {}
            current_targets = self._parse_track_selection(current.get("target_tmdb_ids"))
            current_note = self._normalize_manual_mapping_note(current.get("note"))
            if current_targets == mapping["target_tmdb_ids"] and current_note == mapping["note"]:
                continue
            store.set_manual_mapping(
                source_tmdb_id=source_tmdb_id,
                target_tmdb_ids=mapping["target_tmdb_ids"],
                note=mapping["note"],
            )

    @staticmethod
    def _normalize_manual_mapping_note(value: Any) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()

    @classmethod
    def _parse_manual_mapping_text(cls, value: Any) -> Dict[int, Dict[str, Any]]:
        mappings: Dict[int, Dict[str, Any]] = {}
        for raw_line in str(value or "").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue

            note = ""
            if "#" in line:
                line, note = line.split("#", 1)
            match = re.match(r"^\s*(\d+)\s*(?:=|=>|->|:|：)\s*(.+?)\s*$", line.strip())
            if not match:
                continue

            source_tmdb_id = coerce_int(match.group(1))
            target_tmdb_ids = cls._parse_track_selection(match.group(2))
            if not source_tmdb_id or not target_tmdb_ids:
                continue

            mappings[int(source_tmdb_id)] = {
                "source_tmdb_id": int(source_tmdb_id),
                "target_tmdb_ids": target_tmdb_ids,
                "note": cls._normalize_manual_mapping_note(note),
            }
        return mappings

    @classmethod
    def _serialize_manual_mapping_text(cls, mappings: Dict[Any, dict]) -> str:
        lines: List[str] = []
        for mapping in sorted(
            mappings.values(),
            key=lambda item: coerce_int(item.get("source_tmdb_id"), 0) or 0,
        ):
            source_tmdb_id = coerce_int(mapping.get("source_tmdb_id"))
            target_tmdb_ids = cls._parse_track_selection(mapping.get("target_tmdb_ids"))
            if not source_tmdb_id or not target_tmdb_ids:
                continue

            line = f"{source_tmdb_id}={','.join(str(item) for item in target_tmdb_ids)}"
            note = cls._normalize_manual_mapping_note(mapping.get("note"))
            if note:
                line = f"{line} # {note}"
            lines.append(line)
        return "\n".join(lines)

    def _sync_selected_track_state(self) -> None:
        store = self._ensure_state_store()
        selected_tv = set(self._selected_tv_ids)
        for track in list(store.get_tv_tracks().values()):
            tmdb_id = coerce_int(track.get("tmdb_id"))
            if tmdb_id and tmdb_id not in selected_tv:
                store.remove_tv_track(tmdb_id)

        selected_movie = set(self._selected_movie_ids)
        for track_key, track in list(store.get_movie_tracks().items()):
            anchor_tmdb_id = coerce_int(track.get("anchor_tmdb_id"))
            if anchor_tmdb_id and anchor_tmdb_id not in selected_movie:
                store.remove_movie_track(track_key=track_key)

    def _is_selected_track(self, media_type: str, tmdb_id: int) -> bool:
        if media_type == MediaType.TV.value:
            return int(tmdb_id) in set(self._selected_tv_ids)
        if media_type == MediaType.MOVIE.value:
            return int(tmdb_id) in set(self._selected_movie_ids)
        return False

    def _select_track_id(self, media_type: str, tmdb_id: int) -> bool:
        if media_type == MediaType.TV.value:
            if tmdb_id in self._selected_tv_ids:
                return False
            self._selected_tv_ids = normalize_tmdb_id_list([*self._selected_tv_ids, tmdb_id])
            self._save_current_config()
            return True
        if media_type == MediaType.MOVIE.value:
            if tmdb_id in self._selected_movie_ids:
                return False
            self._selected_movie_ids = normalize_tmdb_id_list([*self._selected_movie_ids, tmdb_id])
            self._save_current_config()
            return True
        return False

    def _deselect_track_id(self, media_type: str, tmdb_id: Optional[int]) -> bool:
        if not tmdb_id:
            return False
        if media_type == MediaType.TV.value and tmdb_id in self._selected_tv_ids:
            self._selected_tv_ids = [candidate for candidate in self._selected_tv_ids if candidate != int(tmdb_id)]
            self._save_current_config()
            return True
        if media_type == MediaType.MOVIE.value and tmdb_id in self._selected_movie_ids:
            self._selected_movie_ids = [candidate for candidate in self._selected_movie_ids if candidate != int(tmdb_id)]
            self._save_current_config()
            return True
        return False

    def _bootstrap_selected_tracks_from_local_catalog(self) -> None:
        if not (self._selected_tv_ids or self._selected_movie_ids):
            return
        store = self._ensure_state_store()
        tv_lookup, movie_lookup = self._local_candidate_lookup()

        if self._enable_tv:
            for tmdb_id in self._selected_tv_ids:
                candidate = tv_lookup.get(int(tmdb_id))
                existing_track = store.get_tv_tracks().get(str(tmdb_id)) or {}
                existing_latest_season = coerce_int(existing_track.get("latest_season"), 0) or 0
                manual_season = coerce_int(self._manual_tv_seasons.get(int(tmdb_id)), 0) or 0
                baseline_season = coerce_int((candidate or {}).get("baseline_season"), 0) or 0
                season = max(manual_season, baseline_season)
                if season <= 0 or existing_latest_season >= season:
                    continue
                store.acknowledge_tv_completion(
                    tmdb_id=tmdb_id,
                    title=str(
                        existing_track.get("title")
                        or (candidate or {}).get("title")
                        or f"TMDB-{tmdb_id}"
                    ),
                    year=self._as_str(existing_track.get("year") or (candidate or {}).get("year")),
                    season=season,
                    source="selection_bootstrap",
                )

        if self._enable_movie:
            for tmdb_id in self._selected_movie_ids:
                existing_movie_tracks = store.get_movie_tracks()
                if any(coerce_int(track.get("anchor_tmdb_id")) == int(tmdb_id) for track in existing_movie_tracks.values()):
                    continue
                candidate = movie_lookup.get(int(tmdb_id))
                media = self._recognize_media(tmdb_id=tmdb_id, mtype=MediaType.MOVIE)
                title = (
                    self._as_str((candidate or {}).get("title"))
                    or self._as_str(self._media_value(media, "title"))
                    or f"TMDB-{tmdb_id}"
                )
                year = self._as_str((candidate or {}).get("year")) or self._as_str(self._media_value(media, "year"))
                collection_id = coerce_int(self._media_value(media, "collection_id"))
                track = store.upsert_movie_track(
                    anchor_tmdb_id=tmdb_id,
                    title=title,
                    year=year,
                    source="selection_bootstrap",
                    collection_id=collection_id,
                    known_tmdb_ids=[tmdb_id],
                )
                store.acknowledge_movie_completion(
                    track_key=track["track_key"],
                    tmdb_id=tmdb_id,
                    title=title,
                    year=year,
                    collection_id=collection_id,
                    source="selection_bootstrap",
                )

    def _complete_tv_tracking(self, tmdb_id: int) -> None:
        self._ensure_state_store().remove_tv_track(tmdb_id)
        self._deselect_track_id(MediaType.TV.value, tmdb_id)

    def _complete_movie_tracking(self, *, track_key: str, anchor_tmdb_id: int) -> None:
        self._ensure_state_store().remove_movie_track(track_key=track_key)
        self._deselect_track_id(MediaType.MOVIE.value, anchor_tmdb_id)

    def _notify_tv_release(
        self,
        *,
        title: str,
        year: Optional[str],
        tmdb_id: int,
        seasons: List[int],
        status: str,
        auto_subscribed: bool = False,
    ) -> None:
        season_text = ", ".join(f"S{int(season):02d}" for season in normalize_tmdb_id_list(seasons))
        label = f"{title} ({year})" if year else title
        text = (
            f"{label} | TMDB {tmdb_id} | 新季 {season_text} | "
            f"{self._tv_release_status_text(status, auto_subscribed=auto_subscribed)} | 已结束本条追踪"
        )
        self.post_message(mtype=NotificationType.Plugin, title="追剧助手", text=text)

    def _notify_movie_release(
        self,
        *,
        title: str,
        anchor_tmdb_id: int,
        collection_id: Optional[int],
        candidates: List[Any],
        status: str,
        auto_subscribed: bool = False,
    ) -> None:
        candidate_text = ", ".join(
            f"{candidate.title}({candidate.year or '-'})"
            for candidate in candidates
        )
        collection_text = f" | Collection {collection_id}" if collection_id else ""
        text = (
            f"{title} | 锚点 TMDB {anchor_tmdb_id}{collection_text} | 续作 {candidate_text} | "
            f"{self._movie_release_status_text(status, auto_subscribed=auto_subscribed)} | 已结束本条追踪"
        )
        self.post_message(mtype=NotificationType.Plugin, title="追剧助手", text=text)

    @staticmethod
    def _merge_release_status(current: Optional[str], candidate: str) -> str:
        priority = {"library": 0, "subscription": 1, "available": 2}
        if current is None:
            return candidate
        return candidate if priority.get(candidate, -1) >= priority.get(current, -1) else current

    @staticmethod
    def _release_status_text(status: str) -> str:
        if status == "library":
            return "已在媒体库中发现"
        if status == "subscription":
            return "已存在订阅"
        return "发现可处理的新条目"

    @classmethod
    def _tv_release_status_text(cls, status: str, auto_subscribed: bool = False) -> str:
        if status == "library":
            return "发现追剧新季已在媒体库"
        if status == "subscription":
            return "发现追剧新季已在订阅中"
        if auto_subscribed:
            return "发现追剧新季，已自动添加订阅"
        return "发现追剧新季已上线"

    @classmethod
    def _movie_release_status_text(cls, status: str, auto_subscribed: bool = False) -> str:
        if status == "library":
            return "发现续作已在媒体库"
        if status == "subscription":
            return "发现续作已在订阅中"
        if auto_subscribed:
            return "发现续作，已自动添加订阅"
        return "发现续作已上映"

    def _auto_subscribe(
        self,
        *,
        title: str,
        year: Optional[str],
        mtype: MediaType,
        tmdb_id: int,
        season: Optional[int] = None,
    ) -> bool:
        chain = self._subscribe_chain or SubscribeChain()
        subscribe_id = None
        message = ""
        try:
            subscribe_id, message = chain.add(
                title=title,
                year=year or "",
                mtype=mtype,
                tmdbid=tmdb_id,
                season=season,
                exist_ok=False,
                message=False,
                source="NextReleaseTracker",
                username="NextReleaseTracker",
            )
        except Exception as exc:
            self._log(
                "error",
                "auto_subscribe",
                f"Automatic subscribe failed: {title} - {exc}",
                {"tmdb_id": tmdb_id, "season": season, "type": mtype.value},
            )
            return False

        if subscribe_id:
            self._log(
                "success",
                "auto_subscribe",
                f"Automatic subscribe created: {title}",
                {"tmdb_id": tmdb_id, "season": season, "type": mtype.value, "subscribe_id": subscribe_id},
            )
            return True

        self._log(
            "warning",
            "auto_subscribe",
            f"Automatic subscribe not created: {title} - {message or 'unknown result'}",
            {"tmdb_id": tmdb_id, "season": season, "type": mtype.value},
        )
        return False

    @staticmethod
    def _parse_track_selection(value: Any) -> List[int]:
        if value is None or value == "":
            return []
        if isinstance(value, (list, tuple, set)):
            return normalize_tmdb_id_list(list(value))
        parts = re.split(r"[\s,;|]+", str(value).strip())
        return normalize_tmdb_id_list(parts)

    @staticmethod
    def _serialize_track_selection(values: List[int]) -> str:
        return "\n".join(str(value) for value in normalize_tmdb_id_list(values))

    @staticmethod
    def _parse_manual_tv_season_text(value: Any) -> Dict[int, int]:
        mappings: Dict[int, int] = {}
        for raw_line in str(value or "").splitlines():
            line = raw_line.split("#", 1)[0].strip()
            if not line or "=" not in line:
                continue
            tmdb_text, season_text = line.split("=", 1)
            tmdb_id = coerce_int(tmdb_text)
            season = parse_season_token(season_text)
            if not tmdb_id or not season or season <= 0:
                continue
            mappings[int(tmdb_id)] = int(season)
        return mappings

    @staticmethod
    def _serialize_manual_tv_season_text(mappings: Dict[int, int]) -> str:
        rows = []
        for tmdb_id in sorted(mappings):
            season = coerce_int(mappings.get(tmdb_id), 0) or 0
            if tmdb_id and season > 0:
                rows.append(f"{int(tmdb_id)}={int(season)}")
        return "\n".join(rows)

    @staticmethod
    def _form_section_card(
        title: str,
        subtitle: str,
        content: List[dict],
        show_expr: Optional[str] = None,
        body_component: str = "VCardText",
        body_props: Optional[Dict[str, Any]] = None,
    ) -> dict:
        card_body = {
            "component": body_component,
            "content": content,
        }
        if body_props:
            card_body["props"] = dict(body_props)
        props: Dict[str, Any] = {"variant": "outlined", "class": "mb-4"}
        if show_expr:
            props["show"] = show_expr
        return {
            "component": "VCard",
            "props": props,
            "content": [
                {
                    "component": "VCardItem",
                    "content": [
                        {"component": "VCardTitle", "text": title},
                        {"component": "VCardSubtitle", "text": subtitle},
                    ],
                },
                card_body,
            ],
        }

    @staticmethod
    def _form_action_button(text: str, color: str, icon: str, on_click: str, button_class: str = "") -> dict:
        props: Dict[str, Any] = {
            "color": color,
            "variant": "tonal",
            "prepend-icon": icon,
            "block": True,
            "onClick": on_click,
        }
        if button_class:
            props["class"] = button_class
        return {"component": "VBtn", "props": props, "text": text}

    def _form_selection_editor(
        self,
        *,
        title: str,
        subtitle: str,
        media_label: str,
        model_key: str,
        add_model: str,
        remove_model: str,
        search_model: str,
        page_model: str,
        placeholder: str,
        saved_table: dict,
        candidates: List[Dict[str, Any]],
        show_expr: Optional[str] = None,
        add_button_expr: Optional[str] = None,
        add_aux_model: Optional[str] = None,
        add_aux_label: Optional[str] = None,
        add_aux_placeholder: str = "",
        extra_content: Optional[List[dict]] = None,
        post_table_content: Optional[List[dict]] = None,
    ) -> dict:
        candidate_content: List[dict] = [
            {
                "component": "VTextField",
                "props": {
                    "model": search_model,
                    "label": f"搜索{media_label}候选列表",
                    "placeholder": "可以输入片名、年份或 TMDB 编号",
                    "clearable": True,
                    "prepend-inner-icon": "mdi-magnify",
                    "hide-details": "auto",
                },
            },
            self._candidate_selection_table(
                media_label=media_label,
                model_key=model_key,
                search_model=search_model,
                page_model=page_model,
                candidates=candidates,
            ),
        ]
        add_row_content: List[dict] = [
            self._col(
                8,
                {
                    "component": "VTextField",
                    "props": {
                        "model": add_model,
                        "label": f"手动输入{media_label}编号（高级）",
                        "placeholder": placeholder,
                        "clearable": True,
                    },
                },
            ),
            self._col(
                4,
                self._form_action_button(
                    f"手动加入{media_label}名单",
                    "primary",
                    "mdi-plus",
                    add_button_expr or self._selection_add_expr(model_key, add_model),
                    "mt-md-6",
                ),
            ),
        ]
        if add_aux_model and add_aux_label:
            add_row_content = [
                self._col(
                    5,
                    {
                        "component": "VTextField",
                        "props": {
                            "model": add_model,
                            "label": f"手动输入{media_label}编号（高级）",
                            "placeholder": placeholder,
                            "clearable": True,
                        },
                    },
                ),
                self._col(
                    3,
                    {
                        "component": "VTextField",
                        "props": {
                            "model": add_aux_model,
                            "label": add_aux_label,
                            "placeholder": add_aux_placeholder,
                            "clearable": True,
                        },
                    },
                ),
                self._col(
                    4,
                    self._form_action_button(
                        f"手动加入{media_label}名单",
                        "primary",
                        "mdi-plus",
                        add_button_expr or self._selection_add_expr(model_key, add_model),
                        "mt-md-6",
                    ),
                ),
            ]
        return self._form_section_card(
            title=title,
            subtitle=subtitle,
            show_expr=show_expr,
            content=[
                *candidate_content,
                {
                    "component": "VRow",
                    "content": add_row_content,
                },
                *(extra_content or []),
                {
                    "component": "VRow",
                    "content": [
                        self._col(
                            6,
                            {
                                "component": "VTextField",
                                "props": {
                                    "model": remove_model,
                                    "label": f"移除{media_label}编号（高级）",
                                    "placeholder": placeholder,
                                    "clearable": True,
                                },
                            },
                        ),
                        self._col(
                            3,
                            self._form_action_button(
                                "移除这个编号",
                                "warning",
                                "mdi-minus",
                                self._selection_remove_expr(
                                    model_key,
                                    remove_model,
                                    manual_season_model="manual_tv_seasons" if model_key == "tracked_tv_ids" else None,
                                    notice_model="tv_pending_notice" if model_key == "tracked_tv_ids" else None,
                                ),
                                "mt-md-6",
                            ),
                        ),
                        self._col(
                            3,
                            self._form_action_button(
                                "清空白名单",
                                "error",
                                "mdi-delete-outline",
                                self._selection_clear_expr(
                                    model_key,
                                    add_model,
                                    remove_model,
                                    manual_season_model="manual_tv_seasons" if model_key == "tracked_tv_ids" else None,
                                    notice_model="tv_pending_notice" if model_key == "tracked_tv_ids" else None,
                                ),
                                "mt-md-6",
                            ),
                        ),
                    ],
                },
                saved_table,
                *(post_table_content or []),
                {
                    "component": "VAlert",
                    "props": {
                        "type": "warning",
                        "variant": "tonal",
                        "text": "这里的点选和按钮只是先改当前表单；记得点“保存”，名单才会真正更新。",
                    },
                },
            ],
        )

    def _selection_snapshot_alert(
        self,
        media_label: str,
        selected_ids: List[int],
        track_lookup: Dict[str, Any],
        is_movie: bool,
    ) -> dict:
        preview_lines: List[str] = []
        for tmdb_id in selected_ids[:5]:
            track = track_lookup.get(str(tmdb_id)) or {}
            if is_movie:
                if track:
                    pending_count = len(track.get("pending_tmdb_ids") or [])
                    pending_text = f"，待留意 {pending_count} 部" if pending_count else ""
                    preview_lines.append(
                        f"{track.get('title') or f'编号 {tmdb_id}'}（已找到系列{pending_text}）"
                    )
                else:
                    preview_lines.append(f"编号 {tmdb_id}（已加入，等待第一次命中）")
            else:
                if track:
                    latest_season = coerce_int(track.get("latest_season"), 0) or 0
                    season_text = f"S{latest_season:02d}" if latest_season else "-"
                    pending_count = len(track.get("pending_seasons") or [])
                    pending_text = f"，待留意 {pending_count} 季" if pending_count else ""
                    preview_lines.append(
                        f"{track.get('title') or f'编号 {tmdb_id}'}（当前到 {season_text}{pending_text}）"
                    )
                else:
                    preview_lines.append(f"编号 {tmdb_id}（已加入，等待确认已追到哪一季）")

        if not selected_ids:
            text = f"当前还没加入任何{media_label}。把想追的内容加到这里后，插件才会开始帮你留意更新。"
            alert_type = "info"
        else:
            extra = f"；另有 {len(selected_ids) - len(preview_lines)} 项" if len(selected_ids) > len(preview_lines) else ""
            text = f"当前已加入 {len(selected_ids)} 个{media_label}：{'；'.join(preview_lines)}{extra}"
            alert_type = "success"
        return {"component": "VAlert", "props": {"type": alert_type, "variant": "tonal", "text": text}}

    @classmethod
    def _manual_tv_season_map_expr(cls, model_key: str) -> str:
        return (
            "(() => { "
            f"const lines = String(model.{model_key} || '').split(/\\r?\\n+/); "
            "const mapping = {}; "
            "lines.forEach((rawLine) => { "
            "const line = String(rawLine || '').split('#')[0].trim(); "
            "if (!line || !line.includes('=')) { return; } "
            "const [tmdbText, seasonText] = line.split('=', 2); "
            "const tmdbId = Number(String(tmdbText || '').trim()); "
            "const match = String(seasonText || '').trim().match(/^S?(\\d+)$/i); "
            "const season = match ? Number(match[1]) : NaN; "
            "if (Number.isInteger(tmdbId) && tmdbId > 0 && Number.isInteger(season) && season > 0) { "
            "mapping[tmdbId] = season; "
            "} "
            "}); "
            "return mapping; "
            "})()"
        )

    @staticmethod
    def _season_number_from_model_expr(model_name: str) -> str:
        return (
            "(() => { "
            f"const raw = String(model.{model_name} || '').trim(); "
            "if (!raw) { return null; } "
            "const match = raw.match(/^S?(\\d+)$/i); "
            "if (!match) { return null; } "
            "const season = Number(match[1]); "
            "return Number.isInteger(season) && season > 0 ? season : null; "
            "})()"
        )

    def _selection_current_table(
        self,
        media_label: str,
        selected_ids: List[int],
        track_lookup: Dict[str, Any],
        is_movie: bool,
    ) -> dict:
        headers = ["名称", "TMDB", "当前状态"]
        rows: List[List[Any]] = []

        if is_movie:
            movie_tracks = list(track_lookup.values())
            for tmdb_id in selected_ids:
                track = next(
                    (
                        item
                        for item in movie_tracks
                        if coerce_int(item.get("anchor_tmdb_id"), 0) == int(tmdb_id)
                    ),
                    {},
                )
                pending_count = len(normalize_tmdb_id_list((track or {}).get("pending_tmdb_ids")))
                known_count = len(normalize_tmdb_id_list((track or {}).get("known_tmdb_ids")))
                if pending_count:
                    status = f"已识别 {pending_count} 个待关注续作"
                elif track and known_count:
                    status = f"已建立系列追踪（已知 {known_count} 部）"
                else:
                    status = "已加入，保存后建立追踪锚点"
                rows.append(
                    [
                        (track or {}).get("title") or f"TMDB-{tmdb_id}",
                        tmdb_id,
                        status,
                    ]
                )
        else:
            for tmdb_id in selected_ids:
                track = track_lookup.get(str(tmdb_id)) or {}
                latest_season = coerce_int(track.get("latest_season"), 0) or 0
                pending_count = len(normalize_tmdb_id_list(track.get("pending_seasons")))
                if latest_season:
                    status = f"已追到 S{latest_season:02d}"
                    if pending_count:
                        status = f"{status} / 待关注 {pending_count} 季"
                elif coerce_int(self._manual_tv_seasons.get(int(tmdb_id)), 0):
                    status = (
                        f"已填 S{coerce_int(self._manual_tv_seasons.get(int(tmdb_id)), 0):02d}"
                        " / 等待保存生效"
                    )
                else:
                    status = "已加入，请填写已追到第几季后保存"
                rows.append(
                    [
                        track.get("title") or f"TMDB-{tmdb_id}",
                        tmdb_id,
                        status,
                    ]
                )

        return {
            "component": "div",
            "content": [
                {
                    "component": "div",
                    "props": {"class": "text-subtitle-2 mt-3 mb-2"},
                    "text": f"当前已加入的{media_label}",
                },
                self._simple_table(headers, rows, empty_text=f"当前还没有加入任何{media_label}。"),
            ],
        }

    def _selection_current_table_live(
        self,
        *,
        media_label: str,
        model_key: str,
        page_model: str,
        selected_ids: List[int],
        track_lookup: Dict[str, Any],
        is_movie: bool,
        candidates: List[Dict[str, Any]],
        manual_tv_season_model: Optional[str] = None,
    ) -> dict:
        headers = ["名称", "TMDB编号", "状态", "操作"] if is_movie else ["名称", "TMDB编号", "已追季数", "状态", "操作"]
        candidate_lookup = {
            coerce_int(candidate.get("tmdb_id"), 0) or 0: candidate
            for candidate in candidates
            if coerce_int(candidate.get("tmdb_id"), 0)
        }
        ordered_ids: List[int] = []
        for tmdb_id in [*selected_ids, *candidate_lookup.keys()]:
            normalized = coerce_int(tmdb_id, 0) or 0
            if normalized and normalized not in ordered_ids:
                ordered_ids.append(normalized)

        body_rows: List[dict] = []
        for tmdb_id in ordered_ids:
            candidate = candidate_lookup.get(tmdb_id) or {}
            title = candidate.get("title") or f"TMDB-{tmdb_id}"

            if is_movie:
                movie_track = next(
                    (
                        item
                        for item in track_lookup.values()
                        if coerce_int(item.get("anchor_tmdb_id"), 0) == int(tmdb_id)
                    ),
                    {},
                )
                title = movie_track.get("title") or title
                pending_count = len(normalize_tmdb_id_list((movie_track or {}).get("pending_tmdb_ids")))
                known_count = len(normalize_tmdb_id_list((movie_track or {}).get("known_tmdb_ids")))
                if pending_count:
                    status = f"已识别 {pending_count} 个待关注续作"
                elif movie_track and known_count:
                    status = f"已建立系列追踪（已知 {known_count} 部）"
                else:
                    status = "已加入，保存后建立追踪锚点"
            else:
                tv_track = track_lookup.get(str(tmdb_id)) or {}
                title = tv_track.get("title") or title
                latest_season = coerce_int(tv_track.get("latest_season"), 0) or 0
                pending_count = len(normalize_tmdb_id_list(tv_track.get("pending_seasons")))
                baseline_season = coerce_int(candidate.get("baseline_season"), 0) or 0
                display_season = latest_season or baseline_season
                if latest_season:
                    status = f"已追到 S{latest_season:02d}"
                    if pending_count:
                        status = f"{status} / 待关注 {pending_count} 季"
                else:
                    status = "请设置已追季数"

            row_content = [
                self._table_cell(title),
                self._table_cell(tmdb_id),
            ]
            if is_movie:
                row_content.append(self._table_cell(status))
            elif latest_season:
                row_content.extend(
                    [
                        self._table_cell(f"S{display_season:02d}" if display_season > 0 else "-"),
                        self._table_cell(status),
                    ]
                )
            else:
                pending_expr = self._selection_manual_tv_season_exists_expr(
                    manual_tv_season_model or "manual_tv_seasons",
                    tmdb_id,
                )
                transient_expr = self._selection_transient_selected_expr(
                    model_key=model_key,
                    tmdb_id=tmdb_id,
                    selected_ids=selected_ids,
                )
                empty_expr = self._selection_manual_tv_season_missing_expr(
                    manual_tv_season_model or "manual_tv_seasons",
                    tmdb_id,
                )
                stable_missing_expr = (
                    "{{ (() => { "
                    f"return ({empty_expr[3:-3]}) && !({transient_expr[3:-3]}); "
                    "})() }}"
                )
                pending_or_transient_expr = (
                    "{{ (() => { "
                    f"return ({pending_expr[3:-3]}) || ({transient_expr[3:-3]}); "
                    "})() }}"
                )
                row_content.extend(
                    [
                        {
                            "component": "td",
                            "props": {"show": pending_or_transient_expr},
                            "text": "待保存",
                        },
                        {
                            "component": "td",
                            "props": {"show": stable_missing_expr},
                            "text": f"S{display_season:02d}" if display_season > 0 else "-",
                        },
                        {
                            "component": "td",
                            "props": {"show": pending_or_transient_expr},
                            "text": "待保存，保存后生效",
                        },
                        {
                            "component": "td",
                            "props": {"show": stable_missing_expr},
                            "text": status,
                        },
                    ]
                )
            row_content.append(
                self._table_cell(
                    self._selection_remove_fixed_button(
                        model_key=model_key,
                        tmdb_id=tmdb_id,
                        page_model=page_model,
                        manual_season_model=manual_tv_season_model if not is_movie else None,
                        notice_model="tv_pending_notice" if model_key == "tracked_tv_ids" else None,
                    )
                )
            )

            body_rows.append(
                {
                    "component": "tr",
                    "props": {
                        "show": self._selection_paged_contains_expr(
                            model_key=model_key,
                            page_model=page_model,
                            tmdb_id=tmdb_id,
                            page_size=self.SELECTED_TRACK_PAGE_SIZE,
                        )
                    },
                    "content": row_content,
                }
            )

        return {
            "component": "div",
            "content": [
                {
                    "component": "div",
                    "props": {"class": "text-subtitle-2 mt-3 mb-2"},
                    "text": f"当前已加入的{media_label}",
                },
                {
                    "component": "div",
                    "props": {"show": self._selection_empty_expr(model_key)},
                    "content": [
                        {
                            "component": "div",
                            "props": {"class": "nrt-empty"},
                            "text": f"当前还没有加入任何{media_label}。",
                        }
                    ],
                },
                {
                    "component": "VTable",
                    "props": {
                        "hover": True,
                        "density": "compact",
                        "show": self._selection_has_any_expr(model_key),
                    },
                    "content": [
                        {
                            "component": "thead",
                            "content": [
                                {
                                    "component": "tr",
                                    "content": [
                                        {"component": "th", "props": {"class": "text-start"}, "text": header}
                                        for header in headers
                                    ],
                                }
                            ],
                        },
                        {"component": "tbody", "content": body_rows},
                    ],
                },
                {
                    "component": "div",
                    "props": {"class": "d-flex justify-end mt-3"},
                    "content": [
                        {
                            "component": "div",
                            "props": {
                                "show": self._selection_pagination_needed_expr(
                                    model_key=model_key,
                                    page_size=self.SELECTED_TRACK_PAGE_SIZE,
                                )
                            },
                            "content": [
                                {
                                    "component": "VPagination",
                                    "props": {
                                        "model": page_model,
                                        "length": self._selection_page_length_expr(
                                            model_key=model_key,
                                            page_size=self.SELECTED_TRACK_PAGE_SIZE,
                                        ),
                                        "total-visible": 5,
                                        "density": "comfortable",
                                    },
                                }
                            ],
                        }
                    ],
                },
            ],
        }

    @staticmethod
    def _selection_model_parse_js(model_key: str) -> str:
        return (
            f"Array.from(new Set(String(model.{model_key} || '')"
            ".split(/[\\s,;|]+/)"
            ".map((item) => Number(String(item).trim()))"
            ".filter((item) => Number.isInteger(item) && item > 0)))"
        )

    @staticmethod
    def _js_value(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False)

    @classmethod
    def _selection_has_any_expr(cls, model_key: str) -> str:
        parsed = cls._selection_model_parse_js(model_key)
        return "{{ (() => { " f"const ids = {parsed}; " "return ids.length > 0; " "})() }}"

    @classmethod
    def _selection_empty_expr(cls, model_key: str) -> str:
        parsed = cls._selection_model_parse_js(model_key)
        return "{{ (() => { " f"const ids = {parsed}; " "return ids.length === 0; " "})() }}"

    @classmethod
    def _model_has_text_expr(cls, model_name: str) -> str:
        return (
            "{{ (() => { "
            f"return String(model.{model_name} || '').trim().length > 0; "
            "})() }}"
        )

    @classmethod
    def _selection_contains_expr(cls, model_key: str, tmdb_id: int) -> str:
        parsed = cls._selection_model_parse_js(model_key)
        return "{{ (() => { " f"const ids = {parsed}; " f"return ids.includes({int(tmdb_id)}); " "})() }}"

    @classmethod
    def _selection_transient_selected_expr(
        cls,
        *,
        model_key: str,
        tmdb_id: int,
        selected_ids: List[int],
    ) -> str:
        parsed = cls._selection_model_parse_js(model_key)
        selected_ids_js = cls._js_value([int(item) for item in selected_ids])
        return (
            "{{ (() => { "
            f"const ids = {parsed}; "
            f"const savedIds = {selected_ids_js}; "
            f"return ids.includes({int(tmdb_id)}) && !savedIds.includes({int(tmdb_id)}); "
            "})() }}"
        )

    @classmethod
    def _selection_paged_contains_expr(
        cls,
        *,
        model_key: str,
        page_model: str,
        tmdb_id: int,
        page_size: int,
    ) -> str:
        parsed = cls._selection_model_parse_js(model_key)
        return (
            "{{ (() => { "
            f"const ids = {parsed}; "
            f"const pageSize = {int(page_size)}; "
            f"const rawPage = Number(model.{page_model} || 1); "
            "const totalPages = Math.max(1, Math.ceil(ids.length / pageSize)); "
            "const page = Math.min(Math.max(Number.isFinite(rawPage) ? Math.trunc(rawPage) : 1, 1), totalPages); "
            "const start = (page - 1) * pageSize; "
            "const end = start + pageSize; "
            f"return ids.slice(start, end).includes({int(tmdb_id)}); "
            "})() }}"
        )

    @classmethod
    def _selection_page_length_expr(cls, *, model_key: str, page_size: int) -> str:
        parsed = cls._selection_model_parse_js(model_key)
        return (
            "{{ (() => { "
            f"const ids = {parsed}; "
            f"return Math.max(1, Math.ceil(ids.length / {int(page_size)})); "
            "})() }}"
        )

    @classmethod
    def _selection_pagination_needed_expr(cls, *, model_key: str, page_size: int) -> str:
        parsed = cls._selection_model_parse_js(model_key)
        return (
            "{{ (() => { "
            f"const ids = {parsed}; "
            f"return ids.length > {int(page_size)}; "
            "})() }}"
        )

    @classmethod
    def _selection_manual_tv_season_exists_expr(cls, manual_tv_season_model: str, tmdb_id: int) -> str:
        manual_seasons = cls._manual_tv_season_map_expr(manual_tv_season_model)
        return (
            "{{ (() => { "
            f"const mappings = {manual_seasons}; "
            f"return Number(mappings[{int(tmdb_id)}] || 0) > 0; "
            "})() }}"
        )

    @classmethod
    def _selection_manual_tv_season_missing_expr(cls, manual_tv_season_model: str, tmdb_id: int) -> str:
        manual_seasons = cls._manual_tv_season_map_expr(manual_tv_season_model)
        return (
            "{{ (() => { "
            f"const mappings = {manual_seasons}; "
            f"return Number(mappings[{int(tmdb_id)}] || 0) <= 0; "
            "})() }}"
        )

    @classmethod
    def _selection_manual_tv_season_value_expr(cls, manual_tv_season_model: str, tmdb_id: int) -> str:
        manual_seasons = cls._manual_tv_season_map_expr(manual_tv_season_model)
        return (
            "{{ (() => { "
            f"const mappings = {manual_seasons}; "
            f"const season = Number(mappings[{int(tmdb_id)}] || 0); "
            "if (!Number.isInteger(season) || season <= 0) { return '-'; } "
            "return `S${String(season).padStart(2, '0')}`; "
            "})() }}"
        )

    @classmethod
    def _selection_add_expr(
        cls,
        model_key: str,
        add_model: str,
        notice_model: Optional[str] = None,
    ) -> str:
        parsed = cls._selection_model_parse_js(model_key)
        notice_stmt = (
            f"model.{notice_model} = `待保存新增：TMDB-${{candidate}}；当前表单共 ${{ids.length}} 项`; "
            if notice_model
            else ""
        )
        return (
            "(event) => { "
            f"const candidate = Number(String(model.{add_model} || '').trim()); "
            "if (!Number.isInteger(candidate) || candidate <= 0) { return; } "
            f"const ids = {parsed}.filter((item) => item !== candidate); "
            "ids.unshift(candidate); "
            f"model.{model_key} = ids.join('\\n'); "
            f"{notice_stmt}"
            f"model.{add_model} = ''; "
            "}"
        )

    @classmethod
    def _selection_add_tv_expr(
        cls,
        *,
        model_key: str,
        add_model: str,
        season_model: str,
        manual_season_model: str,
        notice_model: Optional[str] = None,
    ) -> str:
        parsed = cls._selection_model_parse_js(model_key)
        manual_seasons = cls._manual_tv_season_map_expr(manual_season_model)
        season_expr = cls._season_number_from_model_expr(season_model)
        notice_stmt = (
            f"model.{notice_model} = `待保存新增：TMDB-${{candidate}} / 已追到 S${{String(season).padStart(2, '0')}}；当前表单共 ${{ids.length}} 项`; "
            if notice_model
            else ""
        )
        return (
            "(event) => { "
            f"const candidate = Number(String(model.{add_model} || '').trim()); "
            f"const season = {season_expr}; "
            "if (!Number.isInteger(candidate) || candidate <= 0) { return; } "
            "if (!Number.isInteger(season) || season <= 0) { return; } "
            f"const ids = {parsed}.filter((item) => item !== candidate); "
            "ids.unshift(candidate); "
            f"const mappings = {manual_seasons}; "
            "mappings[candidate] = season; "
            "const lines = Object.keys(mappings) "
            ".map((key) => Number(key)) "
            ".filter((key) => Number.isInteger(key) && key > 0 && Number(mappings[key]) > 0) "
            ".sort((left, right) => left - right) "
            ".map((key) => `${key}=${Number(mappings[key])}`); "
            f"model.{model_key} = ids.join('\\n'); "
            f"model.{manual_season_model} = lines.join('\\n'); "
            f"{notice_stmt}"
            f"model.{add_model} = ''; "
            f"model.{season_model} = ''; "
            "}"
        )

    @classmethod
    def _selection_update_tv_season_expr(
        cls,
        *,
        model_key: str,
        tmdb_model: str,
        season_model: str,
        manual_season_model: str,
        notice_model: Optional[str] = None,
    ) -> str:
        parsed = cls._selection_model_parse_js(model_key)
        manual_seasons = cls._manual_tv_season_map_expr(manual_season_model)
        season_expr = cls._season_number_from_model_expr(season_model)
        notice_stmt = (
            f"model.{notice_model} = `待保存修改：TMDB-${{tmdbId}} / 已追到 S${{String(season).padStart(2, '0')}}`; "
            if notice_model
            else ""
        )
        return (
            "(event) => { "
            f"const tmdbId = Number(String(model.{tmdb_model} || '').trim()); "
            f"const season = {season_expr}; "
            "if (!Number.isInteger(tmdbId) || tmdbId <= 0) { return; } "
            "if (!Number.isInteger(season) || season <= 0) { return; } "
            f"const ids = {parsed}; "
            "if (!ids.includes(tmdbId)) { return; } "
            f"const mappings = {manual_seasons}; "
            "mappings[tmdbId] = season; "
            "const lines = Object.keys(mappings) "
            ".map((key) => Number(key)) "
            ".filter((key) => Number.isInteger(key) && key > 0 && Number(mappings[key]) > 0) "
            ".sort((left, right) => left - right) "
            ".map((key) => `${key}=${Number(mappings[key])}`); "
            f"model.{manual_season_model} = lines.join('\\n'); "
            f"{notice_stmt}"
            f"model.{season_model} = ''; "
            "}"
        )

    @classmethod
    def _selection_clear_tv_season_expr(
        cls,
        *,
        tmdb_model: str,
        season_model: str,
        manual_season_model: str,
        notice_model: Optional[str] = None,
    ) -> str:
        manual_seasons = cls._manual_tv_season_map_expr(manual_season_model)
        notice_stmt = (
            f"model.{notice_model} = `待保存修改：TMDB-${{tmdbId}} / 已清除季数`; "
            if notice_model
            else ""
        )
        return (
            "(event) => { "
            f"const tmdbId = Number(String(model.{tmdb_model} || '').trim()); "
            "if (!Number.isInteger(tmdbId) || tmdbId <= 0) { return; } "
            f"const mappings = {manual_seasons}; "
            "delete mappings[tmdbId]; "
            "const lines = Object.keys(mappings) "
            ".map((key) => Number(key)) "
            ".filter((key) => Number.isInteger(key) && key > 0 && Number(mappings[key]) > 0) "
            ".sort((left, right) => left - right) "
            ".map((key) => `${key}=${Number(mappings[key])}`); "
            f"model.{manual_season_model} = lines.join('\\n'); "
            f"{notice_stmt}"
            f"model.{season_model} = ''; "
            "}"
        )

    @classmethod
    def _selection_add_fixed_expr(
        cls,
        model_key: str,
        tmdb_id: int,
        notice_model: Optional[str] = None,
    ) -> str:
        parsed = cls._selection_model_parse_js(model_key)
        notice_stmt = (
            f"model.{notice_model} = `待保存新增：TMDB-{int(tmdb_id)}；当前表单共 ${{ids.length}} 项`; "
            if notice_model
            else ""
        )
        return (
            "(event) => { "
            f"const ids = {parsed}.filter((item) => item !== {int(tmdb_id)}); "
            f"ids.unshift({int(tmdb_id)}); "
            f"model.{model_key} = ids.join('\\n'); "
            f"{notice_stmt}"
            "}"
        )

    @classmethod
    def _selection_remove_fixed_expr(
        cls,
        *,
        model_key: str,
        tmdb_id: int,
        page_model: Optional[str] = None,
        manual_season_model: Optional[str] = None,
        notice_model: Optional[str] = None,
    ) -> str:
        parsed = cls._selection_model_parse_js(model_key)
        notice_stmt = (
            f"model.{notice_model} = `待保存移除：TMDB-{int(tmdb_id)}；当前表单剩余 ${{ids.length}} 项`; "
            if notice_model
            else ""
        )
        manual_stmt = ""
        if manual_season_model:
            manual_seasons = cls._manual_tv_season_map_expr(manual_season_model)
            manual_stmt = (
                f"const mappings = {manual_seasons}; "
                f"delete mappings[{int(tmdb_id)}]; "
                "const lines = Object.keys(mappings) "
                ".map((key) => Number(key)) "
                ".filter((key) => Number.isInteger(key) && key > 0 && Number(mappings[key]) > 0) "
                ".sort((left, right) => left - right) "
                ".map((key) => `${key}=${Number(mappings[key])}`); "
                f"model.{manual_season_model} = lines.join('\\n'); "
            )
        page_stmt = f"model.{page_model} = 1; " if page_model else ""
        return (
            "(event) => { "
            f"const ids = {parsed}.filter((item) => item !== {int(tmdb_id)}); "
            f"model.{model_key} = ids.join('\\n'); "
            f"{manual_stmt}"
            f"{page_stmt}"
            f"{notice_stmt}"
            "}"
        )

    @classmethod
    def _selection_remove_expr(
        cls,
        model_key: str,
        remove_model: str,
        manual_season_model: Optional[str] = None,
        notice_model: Optional[str] = None,
    ) -> str:
        parsed = cls._selection_model_parse_js(model_key)
        notice_stmt = (
            f"model.{notice_model} = `待保存移除：TMDB-${{candidate}}；当前表单剩余 ${{ids.length}} 项`; "
            if notice_model
            else ""
        )
        manual_stmt = ""
        if manual_season_model:
            manual_seasons = cls._manual_tv_season_map_expr(manual_season_model)
            manual_stmt = (
                f"const mappings = {manual_seasons}; "
                "delete mappings[candidate]; "
                "const lines = Object.keys(mappings) "
                ".map((key) => Number(key)) "
                ".filter((key) => Number.isInteger(key) && key > 0 && Number(mappings[key]) > 0) "
                ".sort((left, right) => left - right) "
                ".map((key) => `${key}=${Number(mappings[key])}`); "
                f"model.{manual_season_model} = lines.join('\\n'); "
            )
        return (
            "(event) => { "
            f"const candidate = Number(String(model.{remove_model} || '').trim()); "
            "if (!Number.isInteger(candidate) || candidate <= 0) { return; } "
            f"const ids = {parsed}.filter((item) => item !== candidate); "
            f"model.{model_key} = ids.join('\\n'); "
            f"{manual_stmt}"
            f"{notice_stmt}"
            f"model.{remove_model} = ''; "
            "}"
        )

    @classmethod
    def _selection_remove_fixed_button(
        cls,
        *,
        model_key: str,
        tmdb_id: int,
        page_model: Optional[str] = None,
        manual_season_model: Optional[str] = None,
        notice_model: Optional[str] = None,
    ) -> dict:
        return cls._form_action_button(
            "移出白名单",
            "warning",
            "mdi-minus",
            cls._selection_remove_fixed_expr(
                model_key=model_key,
                tmdb_id=tmdb_id,
                page_model=page_model,
                manual_season_model=manual_season_model,
                notice_model=notice_model,
            ),
        )

    @staticmethod
    def _selection_clear_expr(
        model_key: str,
        add_model: str,
        remove_model: str,
        manual_season_model: Optional[str] = None,
        notice_model: Optional[str] = None,
    ) -> str:
        notice_stmt = f"model.{notice_model} = '待保存：已清空名单'; " if notice_model else ""
        manual_stmt = f"model.{manual_season_model} = ''; " if manual_season_model else ""
        return (
            "(event) => { "
            f"model.{model_key} = ''; "
            f"{manual_stmt}"
            f"{notice_stmt}"
            f"model.{add_model} = ''; "
            f"model.{remove_model} = ''; "
            "}"
        )

    def _selected_tv_rows(self, tv_tracks: Dict[str, Any]) -> List[List[Any]]:
        rows: List[List[Any]] = []
        for tmdb_id in self._selected_tv_ids:
            track = tv_tracks.get(str(tmdb_id)) or {}
            latest_season = coerce_int(track.get("latest_season"), 0) or 0
            latest_text = f"S{latest_season:02d}" if latest_season else "-"
            pending_seasons = normalize_tmdb_id_list(track.get("pending_seasons")) if track else []
            if track:
                status = "追踪中"
                progress = f"当前到 {latest_text}"
                if pending_seasons:
                    status = "已发现待处理新季"
                    progress = f"{progress} / 待留意 {self._join_values(pending_seasons)}"
                else:
                    progress = f"{progress} / 已建立真实追踪"
            else:
                status = "未建立追踪"
                progress = (
                    f"已填 S{coerce_int(self._manual_tv_seasons.get(int(tmdb_id)), 0):02d} / 等待保存生效"
                    if coerce_int(self._manual_tv_seasons.get(int(tmdb_id)), 0)
                    else "已加入名单，尚未建立真实追踪"
                )
            rows.append(
                [
                    track.get("title") or f"TMDB-{tmdb_id}",
                    tmdb_id,
                    status,
                    progress,
                    self._page_track_remove_button(MediaType.TV.value, tmdb_id),
                ]
            )
        return rows

    def _selected_movie_rows(self, movie_tracks: Dict[str, Any]) -> List[List[Any]]:
        rows: List[List[Any]] = []
        for tmdb_id in self._selected_movie_ids:
            track = movie_tracks.get(f"movie:{tmdb_id}") or {}
            if not track:
                for candidate in movie_tracks.values():
                    if coerce_int(candidate.get("anchor_tmdb_id")) == tmdb_id:
                        track = candidate
                        break
            pending_count = len(track.get("pending_tmdb_ids") or []) if track else 0
            known_count = len(track.get("known_tmdb_ids") or []) if track else 0
            if track:
                status = "追踪中"
                progress = f"已建立系列追踪 / 已知 {known_count} 部"
                if pending_count:
                    status = "已发现待处理续作"
                    progress = f"{progress} / 待留意 {pending_count} 部"
            else:
                status = "未建立追踪"
                progress = "已加入名单，尚未建立真实追踪"
            rows.append(
                [
                    track.get("title") or f"TMDB-{tmdb_id}",
                    tmdb_id,
                    status,
                    progress,
                    self._page_track_remove_button(
                        MediaType.MOVIE.value,
                        tmdb_id,
                        coerce_int(track.get("collection_id")) if track else None,
                    ),
                ]
            )
        return rows

    def _page_track_remove_button(
        self,
        media_type: str,
        tmdb_id: Optional[int],
        collection_id: Optional[int] = None,
    ) -> Any:
        if not tmdb_id and not collection_id:
            return "-"
        plugin_api = f"plugin/{self.__class__.__name__}"
        payload: Dict[str, Any] = {"media_type": media_type}
        if tmdb_id:
            payload["tmdb_id"] = tmdb_id
        if collection_id:
            payload["collection_id"] = collection_id
        return self._action_button(
            text="移出白名单",
            api=f"{plugin_api}/track/remove?apikey={settings.API_TOKEN}",
            payload=payload,
            color="error",
        )

    def _page_manual_map_remove_button(self, source_tmdb_id: Optional[int]) -> Any:
        if not source_tmdb_id:
            return "-"
        plugin_api = f"plugin/{self.__class__.__name__}"
        return self._action_button(
            text="删除映射",
            api=f"{plugin_api}/manual_map/remove?apikey={settings.API_TOKEN}",
            payload={"source_tmdb_id": source_tmdb_id},
            color="warning",
        )

    def _subscribe_exists(self, *, tmdb_id: int, season: Optional[int] = None) -> bool:
        return SubscribeOper().exists(tmdbid=tmdb_id, season=season)

    def _recognize_media(
        self,
        *,
        tmdb_id: int,
        mtype: MediaType,
        scan_cache: Optional[Dict[str, Dict[Any, Any]]] = None,
        tmdb_budget: Optional[Dict[str, Any]] = None,
    ) -> Optional[MediaInfo]:
        cache_key = f"{mtype.value}:{tmdb_id}"
        if scan_cache is not None:
            media_cache = scan_cache.setdefault("media", {})
            if cache_key in media_cache:
                return media_cache[cache_key]
        try:
            if tmdb_budget is not None:
                self._consume_tmdb_budget(tmdb_budget)
            media = self.chain.recognize_media(tmdbid=tmdb_id, mtype=mtype, cache=True)
            if scan_cache is not None:
                scan_cache.setdefault("media", {})[cache_key] = media
            return media
        except Exception as exc:
            logger.warning(f"[NextReleaseTracker] media recognition failed for {tmdb_id}: {exc}")
            return None

    def _ensure_state_store(self) -> TrackerStateStore:
        if not self._state_store:
            self._state_store = TrackerStateStore(
                self.get_data,
                self.save_data,
                self.del_data,
                log_limit=self._log_retention,
            )
        return self._state_store

    def _log(self, level: str, action: str, message: str, context: Optional[dict] = None) -> None:
        self._ensure_state_store().append_action(
            level=level,
            action=action,
            message=message,
            context=context or {},
        )
        if level == "error":
            logger.error(f"[NextReleaseTracker] {action}: {message}")
        elif level == "warning":
            logger.warning(f"[NextReleaseTracker] {action}: {message}")
        else:
            logger.info(f"[NextReleaseTracker] {action}: {message}")

    @staticmethod
    def _ok(message: str, data: Any = None) -> Dict[str, Any]:
        return {"success": True, "message": message, "data": data}

    @staticmethod
    def _error(message: str) -> Dict[str, Any]:
        return {"success": False, "message": message}

    @staticmethod
    def _media_value(media: Any, key: str) -> Any:
        if media is None:
            return None
        if isinstance(media, dict):
            return media.get(key)
        return getattr(media, key, None)

    @staticmethod
    def _media_type_name(media: Any) -> str:
        media_type = NextReleaseTracker._media_value(media, "type")
        try:
            if isinstance(media_type, MediaType):
                return media_type.value
        except TypeError:
            pass
        return str(media_type or "")

    @staticmethod
    def _normalize_media_type(value: Any) -> str:
        text = str(value or "").strip().lower()
        if text == "tv":
            return MediaType.TV.value
        if text == "movie":
            return MediaType.MOVIE.value
        return str(value or "").strip()

    @staticmethod
    def _as_str(value: Any) -> Optional[str]:
        if value is None or value == "":
            return None
        return str(value)

    @staticmethod
    def _now() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _now_dt(self) -> datetime:
        return self._parse_runtime_datetime(self._now()) or datetime.now()

    @staticmethod
    def _format_runtime_datetime(value: datetime) -> str:
        return value.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _parse_runtime_datetime(value: Any) -> Optional[datetime]:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None

    def _build_diagnostic_event_payload(
        self,
        *,
        event_name: str,
        tmdb_id: int,
        title: str,
        year: Optional[str],
        season: int,
        transfer_history_id: Optional[int],
    ) -> Dict[str, Any]:
        return NextReleaseTrackerEventMixin._build_diagnostic_event_payload(
            self,
            event_name=event_name,
            tmdb_id=tmdb_id,
            title=title,
            year=year,
            season=season,
            transfer_history_id=transfer_history_id,
        )

    @staticmethod
    def _dispatch_diagnostic_event(event_name: str, event_data: Dict[str, Any]) -> None:
        return NextReleaseTrackerEventMixin._dispatch_diagnostic_event(event_name, event_data)
    def _sorted_form_candidates(self, media_type: str) -> Tuple[List[Dict[str, Any]], int]:
        tv_lookup, movie_lookup = self._local_candidate_lookup()
        lookup = tv_lookup if media_type == MediaType.TV.value else movie_lookup
        lookup = self._filter_live_form_candidates(lookup, media_type)
        rows = sorted(
            lookup.values(),
            key=lambda item: (
                str(item.get("last_seen_at") or ""),
                coerce_int(item.get("latest_season"), 0) or 0,
                coerce_int(item.get("tmdb_id"), 0) or 0,
            ),
            reverse=True,
        )
        limited = rows[: self.FORM_CANDIDATE_LIMIT]
        return limited, max(len(rows) - len(limited), 0)

    def _filter_live_form_candidates(
        self,
        lookup: Dict[int, Dict[str, Any]],
        media_type: str,
    ) -> Dict[int, Dict[str, Any]]:
        if not lookup:
            return lookup
        mtype = MediaType.TV if media_type == MediaType.TV.value else MediaType.MOVIE
        live_lookup: Dict[int, Dict[str, Any]] = {}
        for tmdb_id, candidate in lookup.items():
            media = self._recognize_media(tmdb_id=tmdb_id, mtype=mtype)
            if not media:
                live_lookup[tmdb_id] = candidate
                continue
            try:
                exists_info = (self._media_server_chain or MediaServerChain()).media_exists(media)
            except Exception as exc:
                logger.warning(f"[NextReleaseTracker] live candidate check failed for {tmdb_id}: {exc}")
                live_lookup[tmdb_id] = candidate
                continue
            if exists_info:
                live_lookup[tmdb_id] = candidate
        return live_lookup

    def _local_candidate_lookup(self) -> Tuple[Dict[int, Dict[str, Any]], Dict[int, Dict[str, Any]]]:
        tv_lookup: Dict[int, Dict[str, Any]] = {}
        movie_lookup: Dict[int, Dict[str, Any]] = {}

        cutoff = datetime.now() - timedelta(days=max(self._history_days, 0))
        histories: List[Any] = []
        subscriptions: List[Any] = []

        try:
            histories = TransferHistoryOper().list_by_date(cutoff.strftime("%Y-%m-%d %H:%M:%S")) or []
        except Exception as exc:
            logger.warning(f"[NextReleaseTracker] candidate history load failed: {exc}")
        try:
            subscriptions = SubscribeOper().list() or []
        except Exception as exc:
            logger.warning(f"[NextReleaseTracker] candidate subscribe load failed: {exc}")

        for history in histories:
            if not getattr(history, "status", False):
                continue
            tmdb_id = coerce_int(getattr(history, "tmdbid", None))
            media_type = self._normalize_media_type(getattr(history, "type", None))
            if not tmdb_id or media_type not in {MediaType.TV.value, MediaType.MOVIE.value}:
                continue
            self._merge_candidate_lookup(
                tv_lookup if media_type == MediaType.TV.value else movie_lookup,
                tmdb_id=tmdb_id,
                title=self._as_str(getattr(history, "title", None)) or f"TMDB-{tmdb_id}",
                year=self._as_str(getattr(history, "year", None)),
                last_seen_at=self._as_str(getattr(history, "date", None)),
                source_label="最近入库",
                latest_season=parse_season_token(getattr(history, "seasons", None)) if media_type == MediaType.TV.value else None,
                baseline_season=parse_season_token(getattr(history, "seasons", None)) if media_type == MediaType.TV.value else None,
            )

        for subscription in subscriptions:
            tmdb_id = coerce_int(getattr(subscription, "tmdbid", None))
            media_type = self._normalize_media_type(getattr(subscription, "type", None))
            if not tmdb_id or media_type not in {MediaType.TV.value, MediaType.MOVIE.value}:
                continue
            self._merge_candidate_lookup(
                tv_lookup if media_type == MediaType.TV.value else movie_lookup,
                tmdb_id=tmdb_id,
                title=self._as_str(getattr(subscription, "name", None)) or f"TMDB-{tmdb_id}",
                year=self._as_str(getattr(subscription, "year", None)),
                last_seen_at=self._as_str(getattr(subscription, "last_update", None))
                or self._as_str(getattr(subscription, "date", None)),
                source_label="最近订阅",
                latest_season=coerce_int(getattr(subscription, "season", None)) if media_type == MediaType.TV.value else None,
                baseline_season=coerce_int(getattr(subscription, "season", None)) if media_type == MediaType.TV.value else None,
            )

        return tv_lookup, movie_lookup

    def _tmdb_discover_candidates(self, media_type: str) -> List[Any]:
        chain = self._tmdb_chain or TmdbChain()
        discover = getattr(chain, "tmdb_discover", None)
        if not callable(discover):
            return []

        items: List[Any] = []
        page_limit = 2
        for page in range(1, page_limit + 1):
            try:
                result = discover(
                    mtype=MediaType.TV if media_type == MediaType.TV.value else MediaType.MOVIE,
                    sort_by="popularity.desc",
                    with_genres="",
                    with_original_language="zh|en|ja|ko" if media_type == MediaType.TV.value else "",
                    with_keywords="",
                    with_watch_providers="",
                    vote_average=0.0,
                    vote_count=0,
                    release_date="",
                    page=page,
                ) or []
                items.extend(result)
            except Exception as exc:
                logger.warning(f"[NextReleaseTracker] TMDB discover candidates failed for {media_type}: {exc}")
                break
        return items

    @staticmethod
    def _merge_candidate_lookup(
        lookup: Dict[int, Dict[str, Any]],
        *,
        tmdb_id: int,
        title: str,
        year: Optional[str],
        last_seen_at: Optional[str],
        source_label: str,
        latest_season: Optional[int] = None,
        baseline_season: Optional[int] = None,
    ) -> None:
        entry = lookup.get(int(tmdb_id)) or {
            "tmdb_id": int(tmdb_id),
            "title": title or f"TMDB-{tmdb_id}",
            "year": year,
            "last_seen_at": last_seen_at,
            "latest_season": latest_season,
            "baseline_season": baseline_season,
            "sources": [],
        }
        if title and (not entry.get("title") or str(entry.get("title", "")).startswith("TMDB-")):
            entry["title"] = title
        if year and not entry.get("year"):
            entry["year"] = year
        current_seen = str(entry.get("last_seen_at") or "")
        candidate_seen = str(last_seen_at or "")
        if candidate_seen and candidate_seen >= current_seen:
            entry["last_seen_at"] = candidate_seen
            if title:
                entry["title"] = title
            if year:
                entry["year"] = year
        if latest_season is not None:
            entry["latest_season"] = max(
                coerce_int(entry.get("latest_season"), 0) or 0,
                coerce_int(latest_season, 0) or 0,
            ) or None
        if baseline_season is not None:
            entry["baseline_season"] = max(
                coerce_int(entry.get("baseline_season"), 0) or 0,
                coerce_int(baseline_season, 0) or 0,
            ) or None
        if source_label not in entry["sources"]:
            entry["sources"].append(source_label)
        entry["search_text"] = " ".join(
            part.lower()
            for part in [
                str(entry.get("title") or ""),
                str(entry.get("year") or ""),
                str(entry.get("tmdb_id") or ""),
            ]
            if part
        )
        lookup[int(tmdb_id)] = entry

    def _candidate_selection_table(
        self,
        *,
        media_label: str,
        model_key: str,
        search_model: str,
        page_model: str,
        candidates: List[Dict[str, Any]],
    ) -> dict:
        headers = ["片名", "年份", "TMDB", "最近时间", "来源", "线索", "操作"]
        table_content: List[dict]
        if not candidates:
            table_content = [
                {
                    "component": "div",
                    "props": {"class": "nrt-empty"},
                    "text": f"暂无最近出现过的{media_label}候选；可以直接手动输入 TMDB 编号加入。",
                }
            ]
            return {
                "component": "div",
                "content": [
                    {
                        "component": "div",
                        "props": {"class": "text-subtitle-2 mt-3 mb-2"},
                        "text": f"{media_label}候选名单",
                    },
                    *table_content,
                ],
            }

        is_tv = media_label == "剧集"
        search_texts = [str(candidate.get("search_text") or "") for candidate in candidates]

        body_rows = []
        for row_index, candidate in enumerate(candidates):
            tmdb_id = coerce_int(candidate.get("tmdb_id"), 0) or 0
            latest_season = coerce_int(candidate.get("latest_season"), 0) or 0
            baseline_season = coerce_int(candidate.get("baseline_season"), 0) or 0
            if is_tv:
                if baseline_season and latest_season > baseline_season:
                    clue_text = f"本地到 S{baseline_season:02d} / 已发现到 S{latest_season:02d}"
                elif baseline_season:
                    clue_text = f"本地到 S{baseline_season:02d}"
                elif latest_season:
                    clue_text = f"已发现到 S{latest_season:02d} / 待确认起点"
                else:
                    clue_text = "待确认起点"
            else:
                clue_text = "已找到系列线索"
            body_rows.append(
                {
                    "component": "tr",
                    "props": {
                        "show": self._candidate_row_show_expr(
                            search_model,
                            page_model,
                            search_texts,
                            row_index,
                        )
                    },
                    "content": [
                        self._table_cell(candidate.get("title") or f"TMDB-{tmdb_id}"),
                        self._table_cell(candidate.get("year") or "-"),
                        self._table_cell(tmdb_id),
                        self._table_cell(candidate.get("last_seen_at") or "-"),
                        self._table_cell(" / ".join(candidate.get("sources") or []) or "-"),
                        self._table_cell(clue_text),
                        self._table_cell(
                            {
                                "component": "VBtn",
                                "props": {
                                    "color": "primary",
                                    "variant": "tonal",
                                    "size": "small",
                                    "prepend-icon": "mdi-plus",
                                    "disabled": self._selection_contains_expr(model_key, tmdb_id),
                                    "onClick": self._selection_add_fixed_expr(
                                        model_key,
                                        tmdb_id,
                                        notice_model="tv_pending_notice" if model_key == "tracked_tv_ids" else None,
                                    ),
                                },
                                "text": "加入白名单",
                            }
                        ),
                    ],
                }
            )

        return {
            "component": "div",
            "content": [
                {
                    "component": "div",
                    "props": {"class": "text-subtitle-2 mt-3 mb-2"},
                    "text": f"{media_label}候选名单",
                },
                {
                    "component": "VTable",
                    "props": {"hover": True, "density": "compact"},
                    "content": [
                        {
                            "component": "thead",
                            "content": [
                                {
                                    "component": "tr",
                                    "content": [
                                        {"component": "th", "props": {"class": "text-start"}, "text": header}
                                        for header in headers
                                    ],
                                }
                            ],
                        },
                        {"component": "tbody", "content": body_rows},
                    ],
                },
                {
                    "component": "div",
                    "props": {"class": "d-flex justify-end mt-3"},
                    "content": [
                        {
                            "component": "div",
                            "props": {"show": self._candidate_pagination_needed_expr(search_model, search_texts)},
                            "content": [
                                {
                                    "component": "VPagination",
                                    "props": {
                                        "model": page_model,
                                        "length": self._candidate_page_length_expr(search_model, search_texts),
                                        "total-visible": 5,
                                        "density": "comfortable",
                                    },
                                }
                            ],
                        },
                    ],
                },
            ],
        }

    @classmethod
    def _candidate_filtered_indexes_expr(cls, search_model: str, search_texts: List[str]) -> str:
        candidate_texts = cls._js_value([str(search_text or "").lower() for search_text in search_texts])
        return (
            "(() => { "
            f"const rows = {candidate_texts}; "
            f"const query = String(model.{search_model} || '').trim().toLowerCase(); "
            "const matched = []; "
            "rows.forEach((haystack, index) => { "
            "if (!query || String(haystack || '').includes(query)) { matched.push(index); } "
            "}); "
            "return matched; "
            "})()"
        )

    @classmethod
    def _candidate_row_show_expr(
        cls,
        search_model: str,
        page_model: str,
        search_texts: List[str],
        row_index: int,
    ) -> str:
        filtered_indexes = cls._candidate_filtered_indexes_expr(search_model, search_texts)
        return (
            "{{ (() => { "
            f"const matched = {filtered_indexes}; "
            f"const pageSize = {cls.FORM_CANDIDATE_PAGE_SIZE}; "
            "const totalPages = Math.max(1, Math.ceil(matched.length / pageSize)); "
            f"const rawPage = Number(model.{page_model} || 1); "
            "const page = Math.min(Math.max(Number.isFinite(rawPage) ? Math.trunc(rawPage) : 1, 1), totalPages); "
            "const start = (page - 1) * pageSize; "
            "const end = start + pageSize; "
            f"return matched.slice(start, end).includes({int(row_index)}); "
            "})() }}"
        )

    @classmethod
    def _candidate_page_length_expr(cls, search_model: str, search_texts: List[str]) -> str:
        filtered_indexes = cls._candidate_filtered_indexes_expr(search_model, search_texts)
        return (
            "{{ (() => { "
            f"const matched = {filtered_indexes}; "
            f"return Math.max(1, Math.ceil(matched.length / {cls.FORM_CANDIDATE_PAGE_SIZE})); "
            "})() }}"
        )

    @classmethod
    def _candidate_pagination_needed_expr(cls, search_model: str, search_texts: List[str]) -> str:
        filtered_indexes = cls._candidate_filtered_indexes_expr(search_model, search_texts)
        return (
            "{{ (() => { "
            f"const matched = {filtered_indexes}; "
            f"return matched.length > {cls.FORM_CANDIDATE_PAGE_SIZE}; "
            "})() }}"
        )

    @staticmethod
    def _col(md: int, content: dict) -> dict:
        return {
            "component": "VCol",
            "props": {"cols": 12, "md": md},
            "content": [content],
        }

    @staticmethod
    def _switch(model: str, label: str) -> dict:
        return {
            "component": "VSwitch",
            "props": {"model": model, "label": label},
        }

    @staticmethod
    def _textfield(model: str, label: str, placeholder: str = "") -> dict:
        return {
            "component": "VTextField",
            "props": {
                "model": model,
                "label": label,
                "placeholder": placeholder,
            },
        }

    @staticmethod
    def _selection_notice_field(model: str, label: str) -> dict:
        return {
            "component": "VTextField",
            "props": {
                "model": model,
                "label": label,
                "readonly": True,
                "hide-details": "auto",
                "show": NextReleaseTracker._model_has_text_expr(model),
            },
        }

    @staticmethod
    def _textarea(model: str, label: str, placeholder: str = "") -> dict:
        return {
            "component": "VTextarea",
            "props": {
                "model": model,
                "label": label,
                "placeholder": placeholder,
                "rows": 4,
                "auto-grow": True,
            },
        }

    @staticmethod
    def _stat_card(title: str, value: Any, subtitle: str) -> dict:
        return {
            "component": "div",
            "props": {"class": "nrt-stat"},
            "content": [
                {"component": "div", "props": {"class": "nrt-stat__title"}, "text": str(title)},
                {"component": "div", "props": {"class": "nrt-stat__value"}, "text": str(value)},
                {"component": "div", "props": {"class": "nrt-stat__subtitle"}, "text": str(subtitle)},
            ],
        }

    def _page_toolbar(self) -> dict:
        plugin_api = f"plugin/{self.__class__.__name__}"
        buttons = [
            self._action_button(
                text="扫描全部",
                api=f"{plugin_api}/rescan?apikey={settings.API_TOKEN}",
                payload={"scope": "all", "notify": True},
                color="primary",
            ),
            self._action_button(
                text="仅扫剧集",
                api=f"{plugin_api}/rescan?apikey={settings.API_TOKEN}",
                payload={"scope": "tv", "notify": True},
                color="info",
            ),
            self._action_button(
                text="诊断订阅事件",
                api=f"{plugin_api}/diagnostic/event?apikey={settings.API_TOKEN}",
                payload={
                    "event": "subscribe_complete",
                    "media_type": "tv",
                    "tmdb_id": self.diagnostic_tv_tmdb_id,
                    "title": self.diagnostic_tv_title,
                    "season": 1,
                    "cleanup": True,
                },
                color="success",
            ),
            self._action_button(
                text="诊断整理事件",
                api=f"{plugin_api}/diagnostic/event?apikey={settings.API_TOKEN}",
                payload={
                    "event": "transfer_complete",
                    "media_type": "tv",
                    "tmdb_id": self.diagnostic_tv_tmdb_id,
                    "title": self.diagnostic_tv_title,
                    "season": 1,
                    "cleanup": True,
                },
                color="secondary",
            ),
        ]
        if self._enable_movie:
            buttons.append(
                self._action_button(
                    text="仅扫电影",
                    api=f"{plugin_api}/rescan?apikey={settings.API_TOKEN}",
                    payload={"scope": "movie", "notify": True},
                    color="secondary",
                )
            )
        buttons.append(
            self._action_button(
                text="回填最近 1 天",
                api=f"{plugin_api}/import/transfer_history?apikey={settings.API_TOKEN}",
                payload={"days": 1},
                color="warning",
            )
        )
        return {
            "component": "div",
            "props": {"class": "nrt-toolbar"},
            "content": [
                {
                    "component": "div",
                    "content": [
                        {"component": "div", "props": {"class": "nrt-toolbar__title"}, "text": "追剧助手"},
                        {
                            "component": "div",
                            "props": {"class": "nrt-toolbar__subtitle"},
                            "text": "这里可以查看追更名单、当前状态和重扫结果；新增或修改名单请回到配置页。内置诊断只会清理自己的测试样本，不会动你的真实订阅。",
                        },
                    ],
                },
                {"component": "div", "props": {"class": "nrt-toolbar__actions"}, "content": buttons},
            ],
        }

    @staticmethod
    def _page_style() -> dict:
        return {
            "component": "style",
            "text": """
            .nrt-page { display: flex; flex-direction: column; gap: 12px; }
            .nrt-toolbar { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; flex-wrap: wrap; }
            .nrt-toolbar__title { font-size: 1.05rem; font-weight: 700; color: rgba(var(--v-theme-on-surface), 1); }
            .nrt-toolbar__subtitle { margin-top: 4px; max-width: 780px; color: rgba(var(--v-theme-on-surface), .68); font-size: .86rem; line-height: 1.5; }
            .nrt-toolbar__actions { display: flex; gap: 8px; flex-wrap: wrap; }
            .nrt-summary { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 8px; }
            .nrt-stat { min-width: 0; border: 1px solid rgba(var(--v-theme-on-surface), .10); border-radius: 8px; background: rgba(var(--v-theme-surface), 1); padding: 12px 14px; }
            .nrt-stat__title { color: rgba(var(--v-theme-on-surface), .64); font-size: .78rem; font-weight: 600; }
            .nrt-stat__value { margin-top: 6px; font-size: 1.4rem; font-weight: 700; color: rgba(var(--v-theme-on-surface), 1); }
            .nrt-stat__subtitle { margin-top: 4px; font-size: .78rem; color: rgba(var(--v-theme-on-surface), .60); }
            .nrt-section { border-radius: 8px; }
            .nrt-section__title { font-size: .95rem; font-weight: 700; }
            .nrt-chip-row { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 12px; }
            .nrt-empty { padding: 6px 0; color: rgba(var(--v-theme-on-surface), .64); }
            @media (max-width: 960px) { .nrt-summary { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
            @media (max-width: 600px) { .nrt-summary { grid-template-columns: repeat(1, minmax(0, 1fr)); } }
            """,
        }

    @staticmethod
    def _section_card(title: str, content: List[dict]) -> dict:
        return {
            "component": "VCard",
            "props": {"variant": "outlined", "class": "nrt-section"},
            "content": [
                {
                    "component": "VCardItem",
                    "props": {"class": "pb-0"},
                    "content": [
                        {"component": "VCardTitle", "props": {"class": "nrt-section__title"}, "text": title},
                    ],
                },
                {"component": "VCardText", "content": content},
            ],
        }

    @staticmethod
    def _status_chips(chips: List[dict]) -> dict:
        return {"component": "div", "props": {"class": "nrt-chip-row"}, "content": chips}

    @staticmethod
    def _chip(text: str, color: str, icon: str) -> dict:
        return {
            "component": "VChip",
            "props": {"color": color, "size": "small", "variant": "tonal"},
            "content": [
                {"component": "VIcon", "props": {"size": "small", "start": True}, "text": icon},
                {"component": "span", "text": text},
            ],
        }

    @staticmethod
    def _chip_cell(text: str, color: str, icon: str) -> dict:
        return {"component": "div", "content": [NextReleaseTracker._chip(text, color, icon)]}

    @staticmethod
    def _simple_table(headers: List[str], rows: List[List[Any]], empty_text: str = "暂无数据。") -> dict:
        if not rows:
            return {"component": "div", "props": {"class": "nrt-empty"}, "text": empty_text}
        header_row = {
            "component": "tr",
            "content": [{"component": "th", "props": {"class": "text-start"}, "text": header} for header in headers],
        }
        body_rows = []
        for row in rows:
            body_rows.append(
                {
                    "component": "tr",
                    "content": [NextReleaseTracker._table_cell(cell) for cell in row],
                }
            )
        return {
            "component": "VTable",
            "props": {"hover": True, "density": "compact"},
            "content": [
                {"component": "thead", "content": [header_row]},
                {"component": "tbody", "content": body_rows},
            ],
        }

    @staticmethod
    def _table_cell(value: Any) -> dict:
        if isinstance(value, dict):
            return {"component": "td", "content": [value]}
        return {"component": "td", "text": "-" if value is None or value == "" else str(value)}

    @staticmethod
    def _action_button(text: str, api: str, payload: dict, color: str) -> dict:
        return {
            "component": "VBtn",
            "props": {"color": color, "variant": "tonal", "size": "small"},
            "text": text,
            "events": {
                "click": {
                    "api": api,
                    "method": "post",
                    "params": payload,
                }
            },
        }

    @staticmethod
    def _join_values(values: Any) -> str:
        if not values:
            return "-"
        if not isinstance(values, list):
            return str(values)
        normalized = [str(value) for value in values if value is not None and value != ""]
        return ", ".join(normalized) if normalized else "-"

    @staticmethod
    def _log_message(log_entry: dict) -> str:
        context = log_entry.get("context") or {}
        context_text = ", ".join(
            f"{key}={value}"
            for key, value in context.items()
            if value is not None and value != ""
        )
        message = str(log_entry.get("message") or "-")
        return f"{message} [{context_text}]" if context_text else message

    @staticmethod
    def _level_color(level: Any) -> str:
        text = str(level or "info").lower()
        if text == "error":
            return "error"
        if text == "warning":
            return "warning"
        if text == "success":
            return "success"
        return "info"

    @staticmethod
    def _level_icon(level: Any) -> str:
        text = str(level or "info").lower()
        if text == "error":
            return "mdi-close-circle-outline"
        if text == "warning":
            return "mdi-alert-outline"
        if text == "success":
            return "mdi-check-circle-outline"
        return "mdi-information-outline"


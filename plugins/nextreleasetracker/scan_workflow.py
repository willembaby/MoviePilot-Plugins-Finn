from __future__ import annotations

from datetime import timedelta
import math
from typing import Any, Dict, List, Optional

try:
    from app.chain.mediaserver import MediaServerChain
except ImportError:
    from app.chain import MediaServerChain
from app.chain.tmdb import TmdbChain
from app.log import logger
from app.schemas.types import MediaType, NotificationType

from .logic import (
    coerce_int,
    season_fully_exists,
    select_ready_collection_movies,
    select_ready_tv_seasons,
)


class ScanBudgetExhausted(RuntimeError):
    pass


class NextReleaseTrackerScanMixin:
    def service_scan(self, scope: str = "all", reason: str = "cron_tick", notify: Optional[bool] = None):
        return self._run_rescan(scope=scope, reason=reason, notify=notify)

    def run_once_scan(self):
        try:
            return self._run_rescan(scope="all", reason="once", notify=self._notify)
        finally:
            if self._onlyonce:
                self._onlyonce = False
                self._save_current_config()

    def stop_service(self):
        pass

    def _run_rescan(self, *, scope: str, reason: str, notify: Optional[bool]) -> Dict[str, Any]:
        scope = scope if scope in {"all", "tv", "movie"} else "all"
        if not self._scan_lock.acquire(blocking=False):
            self._record_cycle_stat(
                locked_skip_increment=1,
                last_locked_skip_at=self._now(),
            )
            return {
                "success": False,
                "message": "scan is already running",
                "scope": scope,
                "reason": reason,
            }

        notify_flag = self._notify if notify is None else bool(notify)
        tmdb_budget = self._create_tmdb_budget()
        scan_cache: Dict[str, Dict[Any, Any]] = {
            "media": {},
            "tv_seasons": {},
            "movie_collection": {},
        }
        task_plan = self._resolve_scan_task_plan(scope=scope, reason=reason)
        summary: Dict[str, Any] = {
            "success": True,
            "scope": scope,
            "reason": reason,
            "started_at": self._now(),
            "period_minutes": task_plan["period_minutes"],
            "planned_tracks": len(task_plan["task_ids"]),
            "remaining_tracks": task_plan["remaining_before"],
            "scanned_tv": 0,
            "scanned_movie": 0,
            "tv_candidates": 0,
            "movie_candidates": 0,
            "notifications_sent": 0,
            "tracks_completed": 0,
            "tracks_updated": 0,
            "existing_in_library": 0,
            "existing_subscriptions": 0,
            "errors": 0,
            "budget_exhausted": False,
            "tmdb_calls_limit": tmdb_budget["limit"],
            "tmdb_calls_used": 0,
        }

        self._ensure_state_store().update_runtime(
            {
                "last_scan_started_at": summary["started_at"],
                "last_scan_scope": scope,
                "last_scan_reason": reason,
            }
        )
        self._record_cycle_stat(tick_increment=1)

        processed_task_ids: List[str] = []
        try:
            for task_id in task_plan["task_ids"]:
                task = task_plan["task_map"].get(task_id)
                if not task:
                    processed_task_ids.append(task_id)
                    continue
                try:
                    self._process_scan_task(
                        task=task,
                        summary=summary,
                        notify_flag=notify_flag,
                        scan_cache=scan_cache,
                        tmdb_budget=tmdb_budget,
                    )
                    processed_task_ids.append(task_id)
                except ScanBudgetExhausted:
                    summary["budget_exhausted"] = True
                    summary["message"] = (
                        f"TMDB minute budget reached: {tmdb_budget['limit']} calls. "
                        "Remaining tasks will continue next minute."
                    )
                    self._log(
                        "warning",
                        "scan_budget",
                        summary["message"],
                        {"scope": scope, "reason": reason},
                    )
                    break
                except Exception as exc:
                    logger.exception("[NextReleaseTracker] scan task failed")
                    summary["errors"] += 1
                    processed_task_ids.append(task_id)
                    self._log(
                        "error",
                        "scan_track",
                        f"Scan task failed: {task_id} - {exc}",
                        {"scope": scope, "reason": reason, "task_id": task_id},
                    )
        except Exception as exc:
            logger.exception("[NextReleaseTracker] scan failed")
            summary["success"] = False
            summary["errors"] += 1
            summary["message"] = f"scan failed: {exc}"
            self._log("error", "scan", summary["message"])
        finally:
            self._commit_scan_task_progress(task_plan, processed_task_ids)
            summary["tmdb_calls_used"] = tmdb_budget["used"] - tmdb_budget["used_before"]
            summary["remaining_tracks"] = self._remaining_scan_tasks(task_plan, processed_task_ids)
            summary["finished_at"] = self._now()
            if reason == "cron_tick" and not summary["planned_tracks"] and not summary["errors"] and not summary["budget_exhausted"]:
                self._record_cycle_stat(idle_tick_increment=1)
                self._log(
                    "info",
                    "cron_idle",
                    "Cron tick reached with no due tasks in the current cycle.",
                    {
                        "scope": scope,
                        "reason": reason,
                        "remaining_tracks": summary["remaining_tracks"],
                        "period_minutes": summary["period_minutes"],
                    },
                )
            runtime_patch = {}
            if (
                reason != "cron_tick"
                or summary["planned_tracks"]
                or summary["errors"]
                or summary["budget_exhausted"]
                or task_plan.get("cycle_reset")
                or not summary["planned_tracks"]
            ):
                runtime_patch["last_scan_finished_at"] = summary["finished_at"]
                runtime_patch["last_scan_summary"] = summary
            if runtime_patch:
                self._ensure_state_store().update_runtime(runtime_patch)
            self._scan_lock.release()

        if notify_flag and summary["errors"]:
            self._notify_summary(summary)
        return summary

    def _process_scan_task(
        self,
        *,
        task: Dict[str, Any],
        summary: Dict[str, Any],
        notify_flag: bool,
        scan_cache: Dict[str, Dict[Any, Any]],
        tmdb_budget: Dict[str, Any],
    ) -> None:
        if task.get("kind") == "tv":
            self._process_tv_scan_task(
                task=task,
                summary=summary,
                notify_flag=notify_flag,
                scan_cache=scan_cache,
                tmdb_budget=tmdb_budget,
            )
            return
        if task.get("kind") == "movie":
            self._process_movie_scan_task(
                task=task,
                summary=summary,
                notify_flag=notify_flag,
                scan_cache=scan_cache,
                tmdb_budget=tmdb_budget,
            )
            return
        raise ValueError(f"Unknown scan task kind: {task.get('kind')}")

    def _process_tv_scan_task(
        self,
        *,
        task: Dict[str, Any],
        summary: Dict[str, Any],
        notify_flag: bool,
        scan_cache: Dict[str, Dict[Any, Any]],
        tmdb_budget: Dict[str, Any],
    ) -> None:
        track = self._ensure_state_store().get_tv_tracks().get(task.get("track_key"))
        tmdb_id = coerce_int((track or {}).get("tmdb_id"))
        if not track or not tmdb_id:
            return

        seasons = self._load_tv_seasons(tmdb_id=tmdb_id, scan_cache=scan_cache, tmdb_budget=tmdb_budget)
        summary["scanned_tv"] += 1
        candidates = select_ready_tv_seasons(
            latest_season=coerce_int(track.get("latest_season"), 0) or 0,
            pending_seasons=track.get("pending_seasons"),
            seasons=seasons,
            grace_days=self._grace_days,
        )
        summary["tv_candidates"] += len(candidates)
        if not candidates:
            return

        media = self._recognize_media(
            tmdb_id=tmdb_id,
            mtype=MediaType.TV,
            scan_cache=scan_cache,
            tmdb_budget=tmdb_budget,
        )
        exists_info = (self._media_server_chain or MediaServerChain()).media_exists(media) if media else None
        title = track.get("title") or self._media_value(media, "title") or f"TMDB-{tmdb_id}"
        year = self._as_str(track.get("year") or self._media_value(media, "year"))
        current_latest_season = coerce_int(track.get("latest_season"), 0) or 0
        detected_seasons = [candidate.season_number for candidate in candidates]
        resolved_seasons: List[int] = []
        available_seasons: List[int] = []
        status: Optional[str] = None

        for candidate in candidates:
            if season_fully_exists(exists_info, candidate.season_number, candidate.episode_count):
                self._log(
                    "info",
                    "tv_scan",
                    f"Season already in library: {title} season {candidate.season_number}",
                    {"tmdb_id": tmdb_id},
                )
                summary["existing_in_library"] += 1
                status = self._merge_release_status(status, "library")
                resolved_seasons.append(candidate.season_number)
                continue

            if self._subscribe_exists(tmdb_id=tmdb_id, season=candidate.season_number):
                self._log(
                    "info",
                    "tv_scan",
                    f"Season already subscribed: {title} season {candidate.season_number}",
                    {"tmdb_id": tmdb_id},
                )
                summary["existing_subscriptions"] += 1
                status = self._merge_release_status(status, "subscription")
                resolved_seasons.append(candidate.season_number)
                continue

            self._log(
                "success",
                "tv_detected",
                f"Detected next TV season: {title} season {candidate.season_number}",
                {"tmdb_id": tmdb_id},
            )
            status = self._merge_release_status(status, "available")
            available_seasons.append(candidate.season_number)

        if not detected_seasons:
            return
        if notify_flag and available_seasons:
            target_season = min(available_seasons)
            self._log(
                "info",
                "auto_subscribe_decision",
                f"Selected season {target_season} for automatic subscribe.",
                {
                    "media_type": MediaType.TV.value,
                    "tmdb_id": tmdb_id,
                    "candidates": available_seasons,
                    "selected": target_season,
                    "reason": "earliest_available_season",
                },
            )
            if self._auto_subscribe(
                title=title,
                year=year,
                mtype=MediaType.TV,
                tmdb_id=tmdb_id,
                season=target_season,
            ):
                self._notify_tv_release(
                    title=title,
                    year=year,
                    tmdb_id=tmdb_id,
                    seasons=[target_season],
                    status="available",
                    auto_subscribed=True,
                )
                summary["notifications_sent"] += 1
                self._record_cycle_stat(
                    active_tick_increment=1,
                    processed_task_increment=1,
                    last_active_tick_at=self._now(),
                )
                self._complete_tv_tracking(tmdb_id)
                summary["tracks_completed"] += 1
                return
            for season in available_seasons:
                self._ensure_state_store().mark_tv_pending(tmdb_id, season)
            summary["tracks_updated"] += 1
            self._record_cycle_stat(
                active_tick_increment=1,
                processed_task_increment=1,
                last_active_tick_at=self._now(),
            )
            return
        if notify_flag:
            self._complete_tv_tracking(tmdb_id)
            summary["tracks_completed"] += 1
            self._record_cycle_stat(
                active_tick_increment=1,
                processed_task_increment=1,
                last_active_tick_at=self._now(),
            )
            return

        if available_seasons:
            first_available_season = min(available_seasons)
            resolved_before_pending = [
                season
                for season in resolved_seasons
                if season < first_available_season
            ]
            completed_latest_season = max(resolved_before_pending, default=current_latest_season)
            if completed_latest_season > current_latest_season:
                self._ensure_state_store().acknowledge_tv_completion(
                    tmdb_id=tmdb_id,
                    title=title,
                    year=year,
                    season=completed_latest_season,
                    source="scan",
                )
            for season in available_seasons:
                self._ensure_state_store().mark_tv_pending(tmdb_id, season)
            summary["tracks_updated"] += 1
            self._record_cycle_stat(
                active_tick_increment=1,
                processed_task_increment=1,
                last_active_tick_at=self._now(),
            )
            return

        self._complete_tv_tracking(tmdb_id)
        summary["tracks_completed"] += 1
        self._record_cycle_stat(
            active_tick_increment=1,
            processed_task_increment=1,
            last_active_tick_at=self._now(),
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
        track_key = str(task.get("track_key") or "")
        track = self._ensure_state_store().get_movie_tracks().get(track_key)
        if not track:
            return

        item_map: Dict[int, Any] = {}
        collection_id = coerce_int(track.get("collection_id"))
        if collection_id:
            for item in self._load_movie_collection(
                collection_id=collection_id,
                scan_cache=scan_cache,
                tmdb_budget=tmdb_budget,
            ):
                item_tmdb_id = coerce_int(self._media_value(item, "tmdb_id"))
                if item_tmdb_id:
                    item_map[item_tmdb_id] = item

        manual_mapping = self._ensure_state_store().get_manual_mapping(
            coerce_int(track.get("anchor_tmdb_id"), 0) or 0
        )
        for target_tmdb_id in (manual_mapping or {}).get("target_tmdb_ids", []):
            target_tmdb_id = coerce_int(target_tmdb_id)
            if not target_tmdb_id or target_tmdb_id in item_map:
                continue
            media = self._recognize_media(
                tmdb_id=target_tmdb_id,
                mtype=MediaType.MOVIE,
                scan_cache=scan_cache,
                tmdb_budget=tmdb_budget,
            )
            if media:
                item_map[target_tmdb_id] = media

        summary["scanned_movie"] += 1
        candidates = select_ready_collection_movies(
            known_tmdb_ids=track.get("known_tmdb_ids"),
            pending_tmdb_ids=track.get("pending_tmdb_ids"),
            items=item_map.values(),
            grace_days=self._grace_days,
        )
        summary["movie_candidates"] += len(candidates)
        if not candidates:
            return

        detected_candidates = []
        resolved_candidates = []
        available_candidates = []
        status: Optional[str] = None
        for candidate in candidates:
            media = item_map.get(candidate.tmdb_id)
            title = candidate.title or track.get("title") or f"TMDB-{candidate.tmdb_id}"
            exists_info = (self._media_server_chain or MediaServerChain()).media_exists(media) if media else None
            detected_candidates.append(candidate)

            if exists_info:
                self._log("info", "movie_scan", f"Movie already in library: {title}", {"tmdb_id": candidate.tmdb_id})
                summary["existing_in_library"] += 1
                status = self._merge_release_status(status, "library")
                resolved_candidates.append(candidate)
                continue

            if self._subscribe_exists(tmdb_id=candidate.tmdb_id):
                self._log(
                    "info",
                    "movie_scan",
                    f"Movie already subscribed: {title}",
                    {"tmdb_id": candidate.tmdb_id},
                )
                summary["existing_subscriptions"] += 1
                status = self._merge_release_status(status, "subscription")
                resolved_candidates.append(candidate)
                continue

            self._log(
                "success",
                "movie_detected",
                f"Detected next movie release: {title}",
                {"tmdb_id": candidate.tmdb_id},
            )
            status = self._merge_release_status(status, "available")
            available_candidates.append(candidate)

        if not detected_candidates:
            return
        if notify_flag and available_candidates:
            target_candidate = available_candidates[0]
            self._log(
                "info",
                "auto_subscribe_decision",
                f"Selected movie {target_candidate.tmdb_id} for automatic subscribe.",
                {
                    "media_type": MediaType.MOVIE.value,
                    "anchor_tmdb_id": coerce_int(track.get("anchor_tmdb_id"), 0) or 0,
                    "candidates": [candidate.tmdb_id for candidate in available_candidates],
                    "selected": target_candidate.tmdb_id,
                    "reason": "first_available_candidate",
                },
            )
            if self._auto_subscribe(
                title=target_candidate.title or track.get("title") or f"TMDB-{target_candidate.tmdb_id}",
                year=target_candidate.year,
                mtype=MediaType.MOVIE,
                tmdb_id=target_candidate.tmdb_id,
            ):
                self._notify_movie_release(
                    title=track.get("title") or f"TMDB-{track.get('anchor_tmdb_id')}",
                    anchor_tmdb_id=coerce_int(track.get("anchor_tmdb_id"), 0) or 0,
                    collection_id=collection_id,
                    candidates=[target_candidate],
                    status="available",
                    auto_subscribed=True,
                )
                summary["notifications_sent"] += 1
                self._record_cycle_stat(
                    active_tick_increment=1,
                    processed_task_increment=1,
                    last_active_tick_at=self._now(),
                )
                self._complete_movie_tracking(
                    track_key=track_key,
                    anchor_tmdb_id=coerce_int(track.get("anchor_tmdb_id"), 0) or 0,
                )
                summary["tracks_completed"] += 1
                return
            for candidate in available_candidates:
                self._ensure_state_store().mark_movie_pending(track_key, candidate.tmdb_id)
            summary["tracks_updated"] += 1
            self._record_cycle_stat(
                active_tick_increment=1,
                processed_task_increment=1,
                last_active_tick_at=self._now(),
            )
            return
        if notify_flag:
            self._complete_movie_tracking(
                track_key=track_key,
                anchor_tmdb_id=coerce_int(track.get("anchor_tmdb_id"), 0) or 0,
            )
            summary["tracks_completed"] += 1
            self._record_cycle_stat(
                active_tick_increment=1,
                processed_task_increment=1,
                last_active_tick_at=self._now(),
            )
            return

        if available_candidates:
            current_track_key = track_key
            for candidate in resolved_candidates:
                current_track = self._ensure_state_store().acknowledge_movie_completion(
                    track_key=current_track_key,
                    tmdb_id=candidate.tmdb_id,
                    title=candidate.title,
                    year=candidate.year,
                    source="scan",
                    collection_id=candidate.collection_id or collection_id,
                )
                current_track_key = current_track["track_key"]
            for candidate in available_candidates:
                self._ensure_state_store().mark_movie_pending(current_track_key, candidate.tmdb_id)
            summary["tracks_updated"] += 1
            self._record_cycle_stat(
                active_tick_increment=1,
                processed_task_increment=1,
                last_active_tick_at=self._now(),
            )
            return

        self._complete_movie_tracking(
            track_key=track_key,
            anchor_tmdb_id=coerce_int(track.get("anchor_tmdb_id"), 0) or 0,
        )
        summary["tracks_completed"] += 1
        self._record_cycle_stat(
            active_tick_increment=1,
            processed_task_increment=1,
            last_active_tick_at=self._now(),
        )

    def _resolve_scan_task_plan(self, *, scope: str, reason: str) -> Dict[str, Any]:
        task_list = self._build_scan_tasks(scope)
        task_map = {task["task_id"]: task for task in task_list}
        if reason == "cron_tick":
            return self._build_scheduled_task_plan(scope=scope, task_map=task_map)
        task_ids = list(task_map.keys())
        return {
            "scheduled": False,
            "period_minutes": 0,
            "task_ids": task_ids,
            "task_map": task_map,
            "remaining_before": len(task_ids),
        }

    def _build_scheduled_task_plan(self, *, scope: str, task_map: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        store = self._ensure_state_store()
        runtime = store.get_runtime_state()
        current_task_ids = list(task_map.keys())
        period_minutes = self._estimate_cron_period_minutes(self._cron)
        cycle_started_at = self._parse_runtime_datetime((runtime.get("scan_plan") or {}).get("cycle_started_at"))
        cycle_config_matches = (
            isinstance(runtime.get("scan_plan"), dict)
            and (runtime.get("scan_plan") or {}).get("cron") == self._cron
            and (runtime.get("scan_plan") or {}).get("scope") == scope
            and coerce_int((runtime.get("scan_plan") or {}).get("period_minutes"), 0) == period_minutes
        )
        now = self._now_dt()
        reset_cycle = (
            not cycle_config_matches
            or not cycle_started_at
            or now >= cycle_started_at + timedelta(minutes=period_minutes)
        )

        if reset_cycle:
            pending_task_ids = list(current_task_ids)
            store.clear_action_log()
            plan_state = {
                "cron": self._cron,
                "scope": scope,
                "period_minutes": period_minutes,
                "cycle_started_at": self._format_runtime_datetime(now),
                "task_ids": list(current_task_ids),
                "pending_task_ids": list(pending_task_ids),
            }
            store.update_runtime(
                {
                    "current_cycle_stats": {
                        "cycle_started_at": plan_state["cycle_started_at"],
                        "tick_count": 0,
                        "idle_tick_count": 0,
                        "active_tick_count": 0,
                        "processed_task_count": 0,
                        "locked_skip_count": 0,
                        "last_active_tick_at": None,
                        "last_locked_skip_at": None,
                    }
                }
            )
        else:
            stored_plan = runtime.get("scan_plan") or {}
            previous_task_ids = set(stored_plan.get("task_ids") or [])
            pending_task_ids = [
                task_id
                for task_id in (stored_plan.get("pending_task_ids") or [])
                if task_id in task_map
            ]
            pending_seen = set(pending_task_ids)
            pending_task_ids.extend(
                task_id
                for task_id in current_task_ids
                if task_id not in previous_task_ids and task_id not in pending_seen
            )
            plan_state = {
                "cron": self._cron,
                "scope": scope,
                "period_minutes": period_minutes,
                "cycle_started_at": stored_plan.get("cycle_started_at") or self._format_runtime_datetime(now),
                "task_ids": list(current_task_ids),
                "pending_task_ids": list(pending_task_ids),
            }

        started_at = self._parse_runtime_datetime(plan_state.get("cycle_started_at")) or now
        store.prune_action_log_before(plan_state.get("cycle_started_at"))
        total_tasks = len(current_task_ids)
        processed_tasks = max(total_tasks - len(plan_state["pending_task_ids"]), 0)
        elapsed_minutes = max(int((now - started_at).total_seconds() // 60), 0)
        target_completed = min(
            total_tasks,
            math.ceil(((elapsed_minutes + 1) * total_tasks) / period_minutes) if total_tasks else 0,
        )
        due_count = max(target_completed - processed_tasks, 0)
        due_task_ids = list(plan_state["pending_task_ids"][:due_count])

        store.update_runtime({"scan_plan": plan_state})
        return {
            "scheduled": True,
            "period_minutes": period_minutes,
            "task_ids": due_task_ids,
            "task_map": task_map,
            "remaining_before": len(plan_state["pending_task_ids"]),
            "plan_state": plan_state,
            "cycle_reset": reset_cycle,
        }

    def _record_cycle_stat(
        self,
        *,
        tick_increment: int = 0,
        idle_tick_increment: int = 0,
        active_tick_increment: int = 0,
        processed_task_increment: int = 0,
        locked_skip_increment: int = 0,
        last_active_tick_at: Optional[str] = None,
        last_locked_skip_at: Optional[str] = None,
    ) -> None:
        store = self._ensure_state_store()
        runtime = store.get_runtime_state()
        stats = dict(runtime.get("current_cycle_stats") or {})
        if not stats:
            stats = {
                "cycle_started_at": self._now(),
                "tick_count": 0,
                "idle_tick_count": 0,
                "active_tick_count": 0,
                "processed_task_count": 0,
                "locked_skip_count": 0,
                "last_active_tick_at": None,
                "last_locked_skip_at": None,
            }
        stats["tick_count"] = max(coerce_int(stats.get("tick_count"), 0) or 0, 0) + tick_increment
        stats["idle_tick_count"] = max(coerce_int(stats.get("idle_tick_count"), 0) or 0, 0) + idle_tick_increment
        stats["active_tick_count"] = max(coerce_int(stats.get("active_tick_count"), 0) or 0, 0) + active_tick_increment
        stats["processed_task_count"] = max(coerce_int(stats.get("processed_task_count"), 0) or 0, 0) + processed_task_increment
        stats["locked_skip_count"] = max(coerce_int(stats.get("locked_skip_count"), 0) or 0, 0) + locked_skip_increment
        if last_active_tick_at:
            stats["last_active_tick_at"] = last_active_tick_at
        if last_locked_skip_at:
            stats["last_locked_skip_at"] = last_locked_skip_at
        store.update_runtime({"current_cycle_stats": stats})

    def _build_scan_tasks(self, scope: str) -> List[Dict[str, Any]]:
        groups: List[List[Dict[str, Any]]] = []
        if scope in {"all", "tv"} and self._enable_tv:
            tv_tracks = self._ensure_state_store().get_tv_tracks()
            groups.append(
                [
                    {
                        "task_id": f"tv:{track_key}",
                        "kind": "tv",
                        "track_key": track_key,
                    }
                    for track_key in sorted(tv_tracks.keys(), key=lambda value: coerce_int(value, 0) or 0)
                ]
            )
        if scope in {"all", "movie"} and self._enable_movie:
            movie_tracks = self._ensure_state_store().get_movie_tracks()
            groups.append(
                [
                    {
                        "task_id": f"movie:{track_key}",
                        "kind": "movie",
                        "track_key": track_key,
                    }
                    for track_key in sorted(
                        movie_tracks.keys(),
                        key=lambda key: (
                            coerce_int((movie_tracks.get(key) or {}).get("anchor_tmdb_id"), 0) or 0,
                            key,
                        ),
                    )
                ]
            )
        return self._interleave_scan_groups(groups)

    @staticmethod
    def _interleave_scan_groups(groups: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        normalized = [group for group in groups if group]
        if not normalized:
            return []
        if len(normalized) == 1:
            return list(normalized[0])
        merged: List[Dict[str, Any]] = []
        max_len = max(len(group) for group in normalized)
        for index in range(max_len):
            for group in normalized:
                if index < len(group):
                    merged.append(group[index])
        return merged

    def _commit_scan_task_progress(self, task_plan: Dict[str, Any], processed_task_ids: List[str]) -> None:
        if not task_plan.get("scheduled") or not processed_task_ids:
            return
        processed_set = set(processed_task_ids)
        plan_state = dict(task_plan.get("plan_state") or {})
        plan_state["pending_task_ids"] = [
            task_id
            for task_id in (plan_state.get("pending_task_ids") or [])
            if task_id not in processed_set
        ]
        self._ensure_state_store().update_runtime({"scan_plan": plan_state})
        task_plan["plan_state"] = plan_state

    def _remaining_scan_tasks(self, task_plan: Dict[str, Any], processed_task_ids: List[str]) -> int:
        if task_plan.get("scheduled"):
            return len((task_plan.get("plan_state") or {}).get("pending_task_ids") or [])
        return max(task_plan.get("remaining_before", 0) - len(processed_task_ids), 0)

    def _create_tmdb_budget(self) -> Dict[str, Any]:
        store = self._ensure_state_store()
        runtime = store.get_runtime_state()
        limit = self._normalized_tmdb_call_limit(self._max_tmdb_calls_per_minute)
        minute_key = self._now_dt().strftime("%Y-%m-%d %H:%M")
        state = runtime.get("tmdb_rate_limit") or {}
        used = coerce_int(state.get("used"), 0) or 0
        if state.get("minute") != minute_key:
            used = 0
        store.update_runtime(
            {
                "tmdb_rate_limit": {
                    "minute": minute_key,
                    "used": used,
                    "limit": limit,
                }
            }
        )
        return {
            "limit": limit,
            "minute": minute_key,
            "used_before": used,
            "used": used,
        }

    def _consume_tmdb_budget(self, tmdb_budget: Dict[str, Any]) -> None:
        if tmdb_budget["used"] >= tmdb_budget["limit"]:
            raise ScanBudgetExhausted(f"tmdb minute budget exhausted: {tmdb_budget['minute']}")
        tmdb_budget["used"] += 1
        self._ensure_state_store().update_runtime(
            {
                "tmdb_rate_limit": {
                    "minute": tmdb_budget["minute"],
                    "used": tmdb_budget["used"],
                    "limit": tmdb_budget["limit"],
                }
            }
        )

    def _load_tv_seasons(
        self,
        *,
        tmdb_id: int,
        scan_cache: Dict[str, Dict[Any, Any]],
        tmdb_budget: Dict[str, Any],
    ) -> List[Any]:
        cache = scan_cache.setdefault("tv_seasons", {})
        if tmdb_id not in cache:
            self._consume_tmdb_budget(tmdb_budget)
            cache[tmdb_id] = (self._tmdb_chain or TmdbChain()).tmdb_seasons(tmdb_id) or []
        return list(cache.get(tmdb_id) or [])

    def _load_movie_collection(
        self,
        *,
        collection_id: int,
        scan_cache: Dict[str, Dict[Any, Any]],
        tmdb_budget: Dict[str, Any],
    ) -> List[Any]:
        cache = scan_cache.setdefault("movie_collection", {})
        if collection_id not in cache:
            self._consume_tmdb_budget(tmdb_budget)
            cache[collection_id] = (self._tmdb_chain or TmdbChain()).tmdb_collection(collection_id) or []
        return list(cache.get(collection_id) or [])

    def _notify_summary(self, summary: Dict[str, Any]) -> None:
        text = (
            f"scope={summary.get('scope')} | "
            f"notifications={summary.get('notifications_sent', 0)} | "
            f"completed={summary.get('tracks_completed', 0)} | "
            f"updated={summary.get('tracks_updated', 0)} | "
            f"in_library={summary.get('existing_in_library', 0)} | "
            f"existing_subscriptions={summary.get('existing_subscriptions', 0)} | "
            f"errors={summary.get('errors', 0)}"
        )
        self.post_message(mtype=NotificationType.Plugin, title="追剧助手", text=text)

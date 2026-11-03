from __future__ import annotations

from datetime import datetime
import time
from typing import Any, Dict, List, Optional

from fastapi import Body

from app.schemas.types import MediaType

from .logic import coerce_int, normalize_tmdb_id_list


class NextReleaseTrackerApiMixin:
    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/tracks",
                "endpoint": self.api_tracks,
                "methods": ["GET"],
                "summary": "Get tracker state",
            },
            {
                "path": "/automation/health",
                "endpoint": self.api_automation_health,
                "methods": ["GET"],
                "summary": "Get automation health summary",
            },
            {
                "path": "/rescan",
                "endpoint": self.api_rescan,
                "methods": ["POST"],
                "summary": "Run tracker scan immediately",
            },
            {
                "path": "/track/add",
                "endpoint": self.api_track_add,
                "methods": ["POST"],
                "summary": "Add a tracker entry manually",
            },
            {
                "path": "/track/remove",
                "endpoint": self.api_track_remove,
                "methods": ["POST"],
                "summary": "Remove a tracker entry",
            },
            {
                "path": "/import/transfer_history",
                "endpoint": self.api_import_transfer_history,
                "methods": ["POST"],
                "summary": "Backfill from transfer history",
            },
            {
                "path": "/diagnostic/event",
                "endpoint": self.api_diagnostic_event,
                "methods": ["POST"],
                "summary": "Dispatch a synthetic runtime diagnostic event",
            },
            {
                "path": "/manual_map/add",
                "endpoint": self.api_manual_map_add,
                "methods": ["POST"],
                "summary": "Add movie manual mapping",
            },
            {
                "path": "/manual_map/remove",
                "endpoint": self.api_manual_map_remove,
                "methods": ["POST"],
                "summary": "Remove movie manual mapping",
            },
        ]

    def api_tracks(self) -> Dict[str, Any]:
        return self._ok("tracker state loaded", self._ensure_state_store().snapshot())

    def api_automation_health(self) -> Dict[str, Any]:
        runtime = self._ensure_state_store().get_runtime_state()
        scan_plan = runtime.get("scan_plan") or {}
        cycle_stats = runtime.get("current_cycle_stats") or {}
        last_scan = runtime.get("last_scan_summary") or {}
        rate_limit = runtime.get("tmdb_rate_limit") or {}

        planned_task_ids = scan_plan.get("task_ids") or []
        pending_task_ids = scan_plan.get("pending_task_ids") or []
        health = {
            "scheduler": {
                "minute_tick_cron": getattr(self, "SCAN_TICK_CRON", "* * * * *"),
                "configured_cycle_cron": getattr(self, "_cron", None),
                "plugin_enabled": bool(getattr(self, "_enabled", False)),
                "tv_enabled": bool(getattr(self, "_enable_tv", False)),
                "movie_enabled": bool(getattr(self, "_enable_movie", False)),
                "notify_enabled": bool(getattr(self, "_notify", False)),
                "run_once_pending": bool(getattr(self, "_onlyonce", False)),
            },
            "runtime": {
                "last_scan_started_at": runtime.get("last_scan_started_at"),
                "last_scan_finished_at": runtime.get("last_scan_finished_at"),
                "last_scan_scope": runtime.get("last_scan_scope"),
                "last_scan_reason": runtime.get("last_scan_reason"),
                "last_scan_success": last_scan.get("success"),
                "last_scan_errors": last_scan.get("errors"),
                "last_scan_planned_tracks": last_scan.get("planned_tracks"),
                "last_scan_remaining_tracks": last_scan.get("remaining_tracks"),
                "last_scan_budget_exhausted": last_scan.get("budget_exhausted"),
            },
            "cycle": {
                "cycle_started_at": cycle_stats.get("cycle_started_at"),
                "tick_count": cycle_stats.get("tick_count"),
                "idle_tick_count": cycle_stats.get("idle_tick_count"),
                "active_tick_count": cycle_stats.get("active_tick_count"),
                "processed_task_count": cycle_stats.get("processed_task_count"),
                "locked_skip_count": cycle_stats.get("locked_skip_count"),
                "last_active_tick_at": cycle_stats.get("last_active_tick_at"),
                "last_locked_skip_at": cycle_stats.get("last_locked_skip_at"),
            },
            "plan": {
                "scope": scan_plan.get("scope"),
                "cycle_started_at": scan_plan.get("cycle_started_at"),
                "period_minutes": scan_plan.get("period_minutes"),
                "planned_task_count": len(planned_task_ids),
                "pending_task_count": len(pending_task_ids),
                "next_due_task_id": pending_task_ids[0] if pending_task_ids else None,
            },
            "tmdb_rate_limit": {
                "limit": rate_limit.get("limit", getattr(self, "_max_tmdb_calls_per_minute", None)),
                "used": rate_limit.get("used"),
                "window_started_at": rate_limit.get("window_started_at"),
            },
        }
        return self._ok("automation health loaded", health)

    def api_rescan(self, payload: Optional[dict] = Body(default=None)) -> Dict[str, Any]:
        payload = payload or {}
        scope = str(payload.get("scope") or "all").lower()
        notify = payload.get("notify")
        summary = self._run_rescan(scope=scope, reason="api", notify=notify)
        return self._ok("scan completed" if summary.get("success") else "scan failed", summary)

    def api_track_add(self, payload: Optional[dict] = Body(default=None)) -> Dict[str, Any]:
        payload = payload or {}
        tmdb_id = coerce_int(payload.get("tmdb_id"))
        media_type = self._normalize_media_type(payload.get("media_type") or payload.get("type"))
        if not tmdb_id or media_type not in {MediaType.TV.value, MediaType.MOVIE.value}:
            return self._error("tmdb_id and media_type(tv/movie) are required")

        if media_type == MediaType.TV.value:
            season = coerce_int(payload.get("season"))
            if not season:
                return self._error("season is required for TV manual tracking")
            self._select_track_id(media_type, tmdb_id)
            media = self._recognize_media(tmdb_id=tmdb_id, mtype=MediaType.TV)
            title = str(payload.get("title") or self._media_value(media, "title") or f"TMDB-{tmdb_id}")
            year = self._as_str(payload.get("year") or self._media_value(media, "year"))
            track = self._ensure_state_store().acknowledge_tv_completion(
                tmdb_id=tmdb_id,
                title=title,
                year=year,
                season=season,
                source="manual",
                note=self._as_str(payload.get("note")),
            )
            self._log("success", "track_add", f"Added TV track manually: {title} season {season}", {"tmdb_id": tmdb_id})
            return self._ok("tv track added", track)

        self._select_track_id(media_type, tmdb_id)
        media = self._recognize_media(tmdb_id=tmdb_id, mtype=MediaType.MOVIE)
        title = str(payload.get("title") or self._media_value(media, "title") or f"TMDB-{tmdb_id}")
        year = self._as_str(payload.get("year") or self._media_value(media, "year"))
        collection_id = coerce_int(payload.get("collection_id") or self._media_value(media, "collection_id"))
        track = self._ensure_state_store().upsert_movie_track(
            anchor_tmdb_id=tmdb_id,
            title=title,
            year=year,
            source="manual",
            collection_id=collection_id,
            known_tmdb_ids=[tmdb_id],
            note=self._as_str(payload.get("note")),
        )
        track = self._ensure_state_store().acknowledge_movie_completion(
            track_key=track["track_key"],
            tmdb_id=tmdb_id,
            title=title,
            year=year,
            collection_id=collection_id,
            source="manual",
        )
        self._log("success", "track_add", f"Added movie track manually: {title}", {"tmdb_id": tmdb_id, "collection_id": collection_id})
        return self._ok("movie track added", track)

    def api_track_remove(self, payload: Optional[dict] = Body(default=None)) -> Dict[str, Any]:
        payload = payload or {}
        media_type = self._normalize_media_type(payload.get("media_type") or payload.get("type"))
        tmdb_id = coerce_int(payload.get("tmdb_id"))
        collection_id = coerce_int(payload.get("collection_id"))

        if media_type == MediaType.TV.value and tmdb_id:
            selection_removed = self._deselect_track_id(media_type, tmdb_id)
            removed = self._ensure_state_store().remove_tv_track(tmdb_id)
            if not removed and not selection_removed:
                return self._error("tv track not found")
            title = (removed or {}).get("title") or f"TMDB-{tmdb_id}"
            self._log("info", "track_remove", f"Removed TV tracking: {title}", {"tmdb_id": tmdb_id})
            return self._ok("tv tracking removed", {"track": removed, "selection_removed": selection_removed})

        if media_type == MediaType.MOVIE.value and (tmdb_id or collection_id):
            selection_removed = self._deselect_track_id(media_type, tmdb_id) if tmdb_id else False
            removed = self._ensure_state_store().remove_movie_track(anchor_tmdb_id=tmdb_id, collection_id=collection_id)
            removed_anchor_tmdb_id = coerce_int((removed or {}).get("anchor_tmdb_id"))
            if removed_anchor_tmdb_id:
                selection_removed = self._deselect_track_id(media_type, removed_anchor_tmdb_id) or selection_removed
            mapping_source_tmdb_id = tmdb_id or removed_anchor_tmdb_id
            if mapping_source_tmdb_id:
                self._ensure_state_store().remove_manual_mapping(mapping_source_tmdb_id)
                self._save_current_config()
            if not removed and not selection_removed:
                return self._error("movie track not found")
            title = (removed or {}).get("title") or f"TMDB-{tmdb_id or collection_id}"
            self._log("info", "track_remove", f"Removed movie tracking: {title}", {"tmdb_id": tmdb_id, "collection_id": collection_id})
            return self._ok("movie tracking removed", {"track": removed, "selection_removed": selection_removed})

        return self._error("media_type and tmdb_id/collection_id are required")

    def api_import_transfer_history(self, payload: Optional[dict] = Body(default=None)) -> Dict[str, Any]:
        payload = payload or {}
        days = max(coerce_int(payload.get("days"), self._history_days) or self._history_days, 0)
        summary = self._import_transfer_history(days=days, reason="api")
        return self._ok("transfer history imported", summary)

    def api_diagnostic_event(self, payload: Optional[dict] = Body(default=None)) -> Dict[str, Any]:
        payload = payload or {}
        if not self._enabled:
            return self._error("plugin must be enabled before runtime diagnostics can run")

        media_type = self._normalize_media_type(payload.get("media_type") or "tv")
        if media_type != MediaType.TV.value:
            return self._error("runtime diagnostics currently support TV only")

        event_name = str(payload.get("event") or payload.get("event_type") or "transfer_complete").strip().lower()
        if event_name not in {"subscribe_complete", "transfer_complete"}:
            return self._error("event must be subscribe_complete or transfer_complete")

        tmdb_id = coerce_int(payload.get("tmdb_id"), self.diagnostic_tv_tmdb_id) or self.diagnostic_tv_tmdb_id
        season = max(coerce_int(payload.get("season"), 1) or 1, 1)
        title = str(payload.get("title") or self.diagnostic_tv_title)
        year = self._as_str(payload.get("year") or str(datetime.now().year))
        cleanup = bool(payload.get("cleanup", True))
        transfer_history_id = coerce_int(payload.get("transfer_history_id"), 9900001)

        store = self._ensure_state_store()
        store.remove_tv_track(tmdb_id)
        temporary_selection = not self._is_selected_track(MediaType.TV.value, tmdb_id)
        original_selected_tv_ids = list(self._selected_tv_ids)
        if temporary_selection:
            self._selected_tv_ids = normalize_tmdb_id_list([*self._selected_tv_ids, tmdb_id])

        try:
            event_data = self._build_diagnostic_event_payload(
                event_name=event_name,
                tmdb_id=tmdb_id,
                title=title,
                year=year,
                season=season,
                transfer_history_id=transfer_history_id,
            )
            self._dispatch_diagnostic_event(event_name=event_name, event_data=event_data)

            track = None
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                track = store.get_tv_tracks().get(str(tmdb_id))
                if track and int(track.get("latest_season") or 0) >= season:
                    break
                time.sleep(0.05)
            success = bool(track) and int(track.get("latest_season") or 0) >= season
            cleanup_performed = False
            if cleanup and track:
                cleanup_performed = bool(store.remove_tv_track(tmdb_id))
        finally:
            if temporary_selection:
                self._selected_tv_ids = original_selected_tv_ids
                if not cleanup:
                    store.remove_tv_track(tmdb_id)

        result = {
            "success": success,
            "event": event_name,
            "media_type": media_type,
            "tmdb_id": tmdb_id,
            "title": title,
            "year": year,
            "season": season,
            "cleanup": cleanup,
            "cleanup_performed": cleanup_performed,
            "track_detected": bool(track),
            "latest_season": track.get("latest_season") if track else None,
            "checked_at": self._now(),
        }
        if event_name == "transfer_complete":
            result["transfer_history_id"] = transfer_history_id

        store.update_runtime({"last_diagnostic": result})
        context = {
            "tmdb_id": tmdb_id,
            "season": season,
            "cleanup": cleanup,
            "track_detected": bool(track),
            "diagnostic": True,
        }
        if success:
            self._log(
                "success",
                f"diagnostic_{event_name}",
                f"Runtime diagnostic verified: {title} season {season}",
                context,
            )
            return self._ok("runtime diagnostic verified", result)

        self._log(
            "warning",
            f"diagnostic_{event_name}",
            f"Runtime diagnostic did not create the expected track: {title} season {season}",
            context,
        )
        return {
            "success": False,
            "message": "runtime diagnostic did not create the expected track",
            "data": result,
        }

    def api_manual_map_add(self, payload: Optional[dict] = Body(default=None)) -> Dict[str, Any]:
        payload = payload or {}
        source_tmdb_id = coerce_int(payload.get("source_tmdb_id"))
        target_tmdb_ids = payload.get("target_tmdb_ids") or []
        if not source_tmdb_id or not isinstance(target_tmdb_ids, list):
            return self._error("source_tmdb_id and list target_tmdb_ids are required")
        mapping = self._ensure_state_store().set_manual_mapping(
            source_tmdb_id=source_tmdb_id,
            target_tmdb_ids=target_tmdb_ids,
            note=self._as_str(payload.get("note")) or "",
        )
        self._save_current_config()
        self._log("info", "manual_map_add", "movie manual mapping updated", {"source_tmdb_id": source_tmdb_id})
        return self._ok("manual mapping saved", mapping)

    def api_manual_map_remove(self, payload: Optional[dict] = Body(default=None)) -> Dict[str, Any]:
        payload = payload or {}
        source_tmdb_id = coerce_int(payload.get("source_tmdb_id"))
        if not source_tmdb_id:
            return self._error("source_tmdb_id is required")
        mapping = self._ensure_state_store().remove_manual_mapping(source_tmdb_id)
        if not mapping:
            return self._error("manual mapping not found")
        self._save_current_config()
        self._log("info", "manual_map_remove", "movie manual mapping removed", {"source_tmdb_id": source_tmdb_id})
        return self._ok("manual mapping removed", mapping)

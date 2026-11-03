from __future__ import annotations

from typing import Any, Dict, Optional

from app.core.event import Event, eventmanager
from app.schemas.types import EventType, MediaType

from .logic import coerce_int, parse_season_token


class NextReleaseTrackerEventMixin:
    # V3 适配：事件注册移至插件主类（_PluginBase 子类）的同名装饰器方法，
    # Mixin 内不再注册——V3 事件绑定 resolver 无法解析 Mixin 声明的处理器。
    def on_subscribe_complete(self, event: Event):
        if not self._enabled:
            return

        event_data = event.event_data or {}
        subscribe_info = event_data.get("subscribe_info") or {}
        mediainfo = event_data.get("mediainfo") or {}
        tmdb_id = coerce_int(self._media_value(mediainfo, "tmdb_id"))
        if not tmdb_id:
            return

        media_type = self._media_type_name(mediainfo)
        if media_type == MediaType.TV.value and self._enable_tv:
            if not self._is_selected_track(media_type, tmdb_id):
                return
            season = coerce_int(subscribe_info.get("season") or self._media_value(mediainfo, "season"), 1) or 1
            track = self._ensure_state_store().acknowledge_tv_completion(
                tmdb_id=tmdb_id,
                title=str(self._media_value(mediainfo, "title") or f"TMDB-{tmdb_id}"),
                year=self._as_str(self._media_value(mediainfo, "year")),
                season=season,
                source="subscribe_complete",
            )
            self._log("success", "subscribe_complete", f"Recorded TV completion: {track['title']} season {season}", {"tmdb_id": tmdb_id})
            return

        if media_type == MediaType.MOVIE.value and self._enable_movie:
            if not self._is_selected_track(media_type, tmdb_id):
                return
            self._track_movie_event(
                tmdb_id=tmdb_id,
                title=str(self._media_value(mediainfo, "title") or f"TMDB-{tmdb_id}"),
                year=self._as_str(self._media_value(mediainfo, "year")),
                collection_id=coerce_int(self._media_value(mediainfo, "collection_id")),
                source="subscribe_complete",
            )

    def on_transfer_complete(self, event: Event):
        if not self._enabled:
            return

        event_data = event.event_data or {}
        mediainfo = event_data.get("mediainfo")
        if not mediainfo:
            return

        tmdb_id = coerce_int(self._media_value(mediainfo, "tmdb_id"))
        if not tmdb_id:
            return

        media_type = self._media_type_name(mediainfo)
        history_id = coerce_int(event_data.get("transfer_history_id"))
        if media_type == MediaType.TV.value and self._enable_tv:
            if not self._is_selected_track(media_type, tmdb_id):
                return
            meta = event_data.get("meta")
            season = parse_season_token(meta) or coerce_int(self._media_value(mediainfo, "season"), 1) or 1
            track = self._ensure_state_store().acknowledge_tv_completion(
                tmdb_id=tmdb_id,
                title=str(self._media_value(mediainfo, "title") or f"TMDB-{tmdb_id}"),
                year=self._as_str(self._media_value(mediainfo, "year")),
                season=season,
                source="transfer_complete",
                last_transfer_history_id=history_id,
            )
            self._log("success", "transfer_complete", f"Recorded TV transfer: {track['title']} season {season}", {"tmdb_id": tmdb_id})
            return

        if media_type == MediaType.MOVIE.value and self._enable_movie:
            if not self._is_selected_track(media_type, tmdb_id):
                return
            self._track_movie_event(
                tmdb_id=tmdb_id,
                title=str(self._media_value(mediainfo, "title") or f"TMDB-{tmdb_id}"),
                year=self._as_str(self._media_value(mediainfo, "year")),
                collection_id=coerce_int(self._media_value(mediainfo, "collection_id")),
                source="transfer_complete",
                last_transfer_history_id=history_id,
            )

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
        track = self._ensure_state_store().upsert_movie_track(
            anchor_tmdb_id=tmdb_id,
            title=title,
            year=year,
            source=source,
            collection_id=collection_id,
            known_tmdb_ids=[tmdb_id],
            last_transfer_history_id=last_transfer_history_id,
        )
        track = self._ensure_state_store().acknowledge_movie_completion(
            track_key=track["track_key"],
            tmdb_id=tmdb_id,
            title=title,
            year=year,
            collection_id=collection_id,
            source=source,
        )
        self._log("success", source, f"Recorded movie anchor: {title}", {"tmdb_id": tmdb_id, "collection_id": collection_id})
        return track

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
        mediainfo = {
            "tmdb_id": tmdb_id,
            "type": MediaType.TV.value,
            "title": title,
            "year": year,
            "season": season,
        }
        if event_name == "subscribe_complete":
            return {
                "subscribe_id": 0,
                "subscribe_info": {
                    "season": season,
                    "name": title,
                    "type": MediaType.TV.value,
                },
                "mediainfo": mediainfo,
            }
        return {
            "meta": f"S{season:02d}",
            "mediainfo": mediainfo,
            "transfer_history_id": transfer_history_id,
            "transferinfo": {"success": True, "synthetic": True},
            "fileitem": {"path": f"/diagnostics/{tmdb_id}/S{season:02d}.mkv"},
        }

    @staticmethod
    def _dispatch_diagnostic_event(event_name: str, event_data: Dict[str, Any]) -> None:
        if event_name == "subscribe_complete":
            eventmanager.send_event(EventType.SubscribeComplete, event_data)
            return
        eventmanager.send_event(EventType.TransferComplete, event_data)

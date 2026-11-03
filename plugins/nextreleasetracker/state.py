from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from threading import RLock
from typing import Any, Callable, Dict, Optional

try:
    from .logic import build_movie_track_key, build_tv_track_key, merge_int_lists, normalize_tmdb_id_list
except ImportError:
    from logic import build_movie_track_key, build_tv_track_key, merge_int_lists, normalize_tmdb_id_list


class TrackerStateStore:
    KEY_SELECTED_TV_IDS = "selected_tv_ids"
    KEY_SELECTED_MOVIE_IDS = "selected_movie_ids"
    KEY_TRACKED_TV = "tracked_tv"
    KEY_TRACKED_MOVIE = "tracked_movie"
    KEY_ACTION_LOG = "action_log"
    KEY_RUNTIME_STATE = "runtime_state"
    KEY_MANUAL_MAPPINGS = "manual_mappings"

    def __init__(
        self,
        load_fn: Callable[[str], Any],
        save_fn: Callable[[str, Any], None],
        delete_fn: Optional[Callable[[str], Any]] = None,
        *,
        now_fn: Optional[Callable[[], str]] = None,
        log_limit: int = 200,
    ) -> None:
        self._load_fn = load_fn
        self._save_fn = save_fn
        self._delete_fn = delete_fn
        self._now_fn = now_fn or (lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self._log_limit = max(int(log_limit or 200), 1)
        self._lock = RLock()

    def _now(self) -> str:
        return self._now_fn()

    def _load(self, key: str, default: Any) -> Any:
        value = self._load_fn(key)
        if value is None:
            return deepcopy(default)
        return deepcopy(value)

    def _save(self, key: str, value: Any) -> Any:
        self._save_fn(key, deepcopy(value))
        return value

    def _exists(self, key: str) -> bool:
        return self._load_fn(key) is not None

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                self.KEY_SELECTED_TV_IDS: self.get_selected_tv_ids(),
                self.KEY_SELECTED_MOVIE_IDS: self.get_selected_movie_ids(),
                self.KEY_TRACKED_TV: self.get_tv_tracks(),
                self.KEY_TRACKED_MOVIE: self.get_movie_tracks(),
                self.KEY_ACTION_LOG: self.get_action_log(),
                self.KEY_RUNTIME_STATE: self.get_runtime_state(),
                self.KEY_MANUAL_MAPPINGS: self.get_manual_mappings(),
            }

    def has_selected_track_ids(self) -> bool:
        return self._exists(self.KEY_SELECTED_TV_IDS) or self._exists(self.KEY_SELECTED_MOVIE_IDS)

    def get_selected_tv_ids(self) -> list[int]:
        return normalize_tmdb_id_list(self._load(self.KEY_SELECTED_TV_IDS, []))

    def get_selected_movie_ids(self) -> list[int]:
        return normalize_tmdb_id_list(self._load(self.KEY_SELECTED_MOVIE_IDS, []))

    def set_selected_tv_ids(self, tmdb_ids: Optional[list[int]]) -> list[int]:
        with self._lock:
            normalized = normalize_tmdb_id_list(tmdb_ids)
            self._save(self.KEY_SELECTED_TV_IDS, normalized)
            return list(normalized)

    def set_selected_movie_ids(self, tmdb_ids: Optional[list[int]]) -> list[int]:
        with self._lock:
            normalized = normalize_tmdb_id_list(tmdb_ids)
            self._save(self.KEY_SELECTED_MOVIE_IDS, normalized)
            return list(normalized)

    def has_tracks(self) -> bool:
        return bool(self.get_tv_tracks() or self.get_movie_tracks())

    def get_tv_tracks(self) -> Dict[str, dict]:
        return self._load(self.KEY_TRACKED_TV, {})

    def get_movie_tracks(self) -> Dict[str, dict]:
        return self._load(self.KEY_TRACKED_MOVIE, {})

    def get_action_log(self) -> list[dict]:
        return self._load(self.KEY_ACTION_LOG, [])

    def clear_action_log(self) -> None:
        with self._lock:
            self._save(self.KEY_ACTION_LOG, [])

    def prune_action_log_before(self, started_at: Optional[str]) -> None:
        if not started_at:
            return
        try:
            threshold = datetime.strptime(str(started_at), "%Y-%m-%d %H:%M:%S")
        except (TypeError, ValueError):
            return
        with self._lock:
            logs = self.get_action_log()
            filtered = []
            for entry in logs:
                try:
                    entry_time = datetime.strptime(str(entry.get("time") or ""), "%Y-%m-%d %H:%M:%S")
                except (TypeError, ValueError):
                    continue
                if entry_time >= threshold:
                    filtered.append(entry)
            if len(filtered) != len(logs):
                self._save(self.KEY_ACTION_LOG, filtered[-self._log_limit :])

    def get_runtime_state(self) -> dict:
        return self._load(self.KEY_RUNTIME_STATE, {})

    def get_manual_mappings(self) -> Dict[str, dict]:
        return self._load(self.KEY_MANUAL_MAPPINGS, {})

    def upsert_tv_track(
        self,
        *,
        tmdb_id: int,
        title: str,
        year: Optional[str],
        latest_season: int,
        source: str,
        last_transfer_history_id: Optional[int] = None,
        note: Optional[str] = None,
    ) -> dict:
        key = build_tv_track_key(tmdb_id)
        with self._lock:
            tracks = self.get_tv_tracks()
            now = self._now()
            current = tracks.get(key) or {
                "tmdb_id": int(tmdb_id),
                "title": title,
                "year": year,
                "latest_season": int(latest_season),
                "pending_seasons": [],
                "source": source,
                "added_at": now,
            }
            current["title"] = title or current.get("title") or f"TMDB-{tmdb_id}"
            current["year"] = year or current.get("year")
            current["latest_season"] = max(int(latest_season), int(current.get("latest_season") or 0))
            current["pending_seasons"] = [
                season
                for season in normalize_tmdb_id_list(current.get("pending_seasons"))
                if season > current["latest_season"]
            ]
            current["source"] = source or current.get("source")
            current["updated_at"] = now
            if last_transfer_history_id:
                current["last_transfer_history_id"] = int(last_transfer_history_id)
            if note:
                current["note"] = note
            tracks[key] = current
            self._save(self.KEY_TRACKED_TV, tracks)
            return deepcopy(current)

    def mark_tv_pending(self, tmdb_id: int, season: int) -> dict:
        key = build_tv_track_key(tmdb_id)
        with self._lock:
            tracks = self.get_tv_tracks()
            current = tracks.get(key) or {
                "tmdb_id": int(tmdb_id),
                "title": f"TMDB-{tmdb_id}",
                "year": None,
                "latest_season": 0,
                "pending_seasons": [],
                "source": "scan",
                "added_at": self._now(),
            }
            current["pending_seasons"] = merge_int_lists(current.get("pending_seasons"), [season])
            current["updated_at"] = self._now()
            tracks[key] = current
            self._save(self.KEY_TRACKED_TV, tracks)
            return deepcopy(current)

    def acknowledge_tv_completion(
        self,
        *,
        tmdb_id: int,
        title: str,
        year: Optional[str],
        season: int,
        source: str,
        last_transfer_history_id: Optional[int] = None,
        note: Optional[str] = None,
    ) -> dict:
        current = self.upsert_tv_track(
            tmdb_id=tmdb_id,
            title=title,
            year=year,
            latest_season=season,
            source=source,
            last_transfer_history_id=last_transfer_history_id,
            note=note,
        )
        current["pending_seasons"] = [
            pending
            for pending in normalize_tmdb_id_list(current.get("pending_seasons"))
            if pending > int(current.get("latest_season") or 0)
        ]
        tracks = self.get_tv_tracks()
        tracks[build_tv_track_key(tmdb_id)] = current
        self._save(self.KEY_TRACKED_TV, tracks)
        return deepcopy(current)

    def remove_tv_track(self, tmdb_id: int) -> Optional[dict]:
        key = build_tv_track_key(tmdb_id)
        with self._lock:
            tracks = self.get_tv_tracks()
            removed = tracks.pop(key, None)
            self._save(self.KEY_TRACKED_TV, tracks)
            return deepcopy(removed)

    def find_movie_track_key(
        self,
        *,
        anchor_tmdb_id: Optional[int] = None,
        collection_id: Optional[int] = None,
    ) -> Optional[str]:
        tracks = self.get_movie_tracks()
        if collection_id is not None:
            expected = build_movie_track_key(collection_id, anchor_tmdb_id or collection_id)
            if expected in tracks:
                return expected
        if anchor_tmdb_id is None:
            return None
        for track_key, track in tracks.items():
            if int(track.get("anchor_tmdb_id") or 0) == int(anchor_tmdb_id):
                return track_key
        return None

    def upsert_movie_track(
        self,
        *,
        anchor_tmdb_id: int,
        title: str,
        year: Optional[str],
        source: str,
        collection_id: Optional[int] = None,
        known_tmdb_ids: Optional[list[int]] = None,
        last_transfer_history_id: Optional[int] = None,
        note: Optional[str] = None,
    ) -> dict:
        with self._lock:
            tracks = self.get_movie_tracks()
            target_key = build_movie_track_key(collection_id, anchor_tmdb_id)
            existing_key = self.find_movie_track_key(anchor_tmdb_id=anchor_tmdb_id, collection_id=collection_id)
            current = tracks.pop(existing_key, None) if existing_key else None
            now = self._now()
            if not current:
                current = {
                    "track_key": target_key,
                    "anchor_tmdb_id": int(anchor_tmdb_id),
                    "title": title,
                    "year": year,
                    "collection_id": collection_id,
                    "known_tmdb_ids": [],
                    "pending_tmdb_ids": [],
                    "source": source,
                    "added_at": now,
                }
            current["track_key"] = target_key
            current["anchor_tmdb_id"] = int(anchor_tmdb_id)
            current["title"] = title or current.get("title") or f"TMDB-{anchor_tmdb_id}"
            current["year"] = year or current.get("year")
            current["collection_id"] = int(collection_id) if collection_id else None
            current["known_tmdb_ids"] = merge_int_lists(
                current.get("known_tmdb_ids"),
                [anchor_tmdb_id],
                known_tmdb_ids,
            )
            current["pending_tmdb_ids"] = [
                tmdb_id
                for tmdb_id in normalize_tmdb_id_list(current.get("pending_tmdb_ids"))
                if tmdb_id not in set(current["known_tmdb_ids"])
            ]
            current["source"] = source or current.get("source")
            current["updated_at"] = now
            if last_transfer_history_id:
                current["last_transfer_history_id"] = int(last_transfer_history_id)
            if note:
                current["note"] = note
            tracks[target_key] = current
            self._save(self.KEY_TRACKED_MOVIE, tracks)
            return deepcopy(current)

    def mark_movie_pending(self, track_key: str, tmdb_id: int) -> dict:
        with self._lock:
            tracks = self.get_movie_tracks()
            current = tracks.get(track_key)
            if not current:
                raise KeyError(f"Movie track not found: {track_key}")
            current["pending_tmdb_ids"] = merge_int_lists(current.get("pending_tmdb_ids"), [tmdb_id])
            current["pending_tmdb_ids"] = [
                candidate
                for candidate in current["pending_tmdb_ids"]
                if candidate not in set(normalize_tmdb_id_list(current.get("known_tmdb_ids")))
            ]
            current["updated_at"] = self._now()
            tracks[track_key] = current
            self._save(self.KEY_TRACKED_MOVIE, tracks)
            return deepcopy(current)

    def acknowledge_movie_completion(
        self,
        *,
        track_key: str,
        tmdb_id: int,
        title: Optional[str],
        year: Optional[str],
        source: str,
        collection_id: Optional[int] = None,
        note: Optional[str] = None,
    ) -> dict:
        with self._lock:
            tracks = self.get_movie_tracks()
            current = tracks.get(track_key)
            if not current:
                current = self.upsert_movie_track(
                    anchor_tmdb_id=tmdb_id,
                    title=title or f"TMDB-{tmdb_id}",
                    year=year,
                    source=source,
                    collection_id=collection_id,
                    known_tmdb_ids=[tmdb_id],
                    note=note,
                )
                track_key = current["track_key"]
                tracks = self.get_movie_tracks()
                current = tracks.get(track_key)
            current["known_tmdb_ids"] = merge_int_lists(current.get("known_tmdb_ids"), [tmdb_id])
            current["pending_tmdb_ids"] = [
                candidate
                for candidate in normalize_tmdb_id_list(current.get("pending_tmdb_ids"))
                if candidate != int(tmdb_id)
            ]
            if title:
                current["title"] = title
            if year:
                current["year"] = year
            if collection_id and not current.get("collection_id"):
                current["collection_id"] = int(collection_id)
            current["updated_at"] = self._now()
            current["source"] = source or current.get("source")
            if note:
                current["note"] = note

            new_key = build_movie_track_key(current.get("collection_id"), current["anchor_tmdb_id"])
            if new_key != track_key:
                tracks.pop(track_key, None)
                current["track_key"] = new_key
                tracks[new_key] = current
            else:
                tracks[track_key] = current
            self._save(self.KEY_TRACKED_MOVIE, tracks)
            return deepcopy(current)

    def remove_movie_track(
        self,
        *,
        track_key: Optional[str] = None,
        anchor_tmdb_id: Optional[int] = None,
        collection_id: Optional[int] = None,
    ) -> Optional[dict]:
        with self._lock:
            key = track_key or self.find_movie_track_key(anchor_tmdb_id=anchor_tmdb_id, collection_id=collection_id)
            if not key:
                return None
            tracks = self.get_movie_tracks()
            removed = tracks.pop(key, None)
            self._save(self.KEY_TRACKED_MOVIE, tracks)
            return deepcopy(removed)

    def set_manual_mapping(self, *, source_tmdb_id: int, target_tmdb_ids: list[int], note: str = "") -> dict:
        with self._lock:
            mappings = self.get_manual_mappings()
            key = str(int(source_tmdb_id))
            mappings[key] = {
                "source_tmdb_id": int(source_tmdb_id),
                "target_tmdb_ids": normalize_tmdb_id_list(target_tmdb_ids),
                "note": note or "",
                "updated_at": self._now(),
            }
            self._save(self.KEY_MANUAL_MAPPINGS, mappings)
            return deepcopy(mappings[key])

    def get_manual_mapping(self, source_tmdb_id: int) -> Optional[dict]:
        mappings = self.get_manual_mappings()
        return deepcopy(mappings.get(str(int(source_tmdb_id))))

    def remove_manual_mapping(self, source_tmdb_id: int) -> Optional[dict]:
        with self._lock:
            mappings = self.get_manual_mappings()
            removed = mappings.pop(str(int(source_tmdb_id)), None)
            self._save(self.KEY_MANUAL_MAPPINGS, mappings)
            return deepcopy(removed)

    def append_action(self, *, level: str, action: str, message: str, context: Optional[dict] = None) -> dict:
        with self._lock:
            logs = self.get_action_log()
            entry = {
                "time": self._now(),
                "level": str(level or "info"),
                "action": str(action or "event"),
                "message": str(message or ""),
                "context": deepcopy(context or {}),
            }
            logs.append(entry)
            logs = logs[-self._log_limit :]
            self._save(self.KEY_ACTION_LOG, logs)
            return deepcopy(entry)

    def update_runtime(self, patch: dict) -> dict:
        with self._lock:
            runtime = self.get_runtime_state()
            runtime.update(deepcopy(patch or {}))
            runtime["updated_at"] = self._now()
            self._save(self.KEY_RUNTIME_STATE, runtime)
            return deepcopy(runtime)

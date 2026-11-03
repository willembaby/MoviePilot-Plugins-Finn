from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import re
from typing import Any, Iterable, Optional


_INTEGER_RE = re.compile(r"(\d+)")


@dataclass(frozen=True)
class TVSeasonCandidate:
    season_number: int
    air_date: str
    episode_count: int
    name: Optional[str] = None


@dataclass(frozen=True)
class MovieCandidate:
    tmdb_id: int
    title: str
    year: Optional[str]
    release_date: str
    collection_id: Optional[int] = None


def get_value(item: Any, key: str, default: Any = None) -> Any:
    if item is None:
        return default
    if isinstance(item, dict):
        return item.get(key, default)
    return getattr(item, key, default)


def coerce_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return default
    if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
        return int(text)
    match = _INTEGER_RE.search(text)
    if match:
        return int(match.group(1))
    return default


def parse_date(value: Any) -> Optional[date]:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def parse_season_token(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    begin_season = getattr(value, "begin_season", None)
    if begin_season is not None:
        return coerce_int(begin_season)
    season_number = getattr(value, "season_number", None)
    if season_number is not None:
        return coerce_int(season_number)
    return coerce_int(value)


def build_tv_track_key(tmdb_id: int) -> str:
    return str(int(tmdb_id))


def build_movie_track_key(collection_id: Optional[int], anchor_tmdb_id: int) -> str:
    if collection_id:
        return f"collection:{int(collection_id)}"
    return f"movie:{int(anchor_tmdb_id)}"


def normalize_tmdb_id_list(values: Optional[Iterable[Any]]) -> list[int]:
    normalized: list[int] = []
    seen: set[int] = set()
    for value in values or []:
        candidate = coerce_int(value)
        if candidate is None or candidate in seen:
            continue
        seen.add(candidate)
        normalized.append(candidate)
    return normalized


def merge_int_lists(*values: Optional[Iterable[Any]]) -> list[int]:
    merged: list[int] = []
    seen: set[int] = set()
    for items in values:
        for item in normalize_tmdb_id_list(items):
            if item in seen:
                continue
            seen.add(item)
            merged.append(item)
    return merged


def season_fully_exists(existing_info: Any, season_number: int, expected_episode_count: int) -> bool:
    seasons = get_value(existing_info, "seasons", {}) or {}
    existing_episodes = seasons.get(season_number) or seasons.get(str(season_number)) or []
    if not existing_episodes:
        return False
    if expected_episode_count <= 0:
        return True
    return len(existing_episodes) >= expected_episode_count


def select_ready_tv_seasons(
    latest_season: int,
    pending_seasons: Optional[Iterable[Any]],
    seasons: Optional[Iterable[Any]],
    *,
    today: Optional[date] = None,
    grace_days: int = 3,
) -> list[TVSeasonCandidate]:
    ready: list[TVSeasonCandidate] = []
    ready_until = (today or date.today()) + timedelta(days=max(grace_days, 0))
    pending = set(normalize_tmdb_id_list(pending_seasons))

    for season in seasons or []:
        season_number = coerce_int(get_value(season, "season_number"))
        episode_count = coerce_int(get_value(season, "episode_count"), 0) or 0
        air_date = parse_date(get_value(season, "air_date"))
        if season_number is None or season_number <= latest_season or season_number == 0:
            continue
        if season_number in pending:
            continue
        if episode_count <= 0:
            continue
        if not air_date or air_date > ready_until:
            continue
        ready.append(
            TVSeasonCandidate(
                season_number=season_number,
                air_date=air_date.isoformat(),
                episode_count=episode_count,
                name=get_value(season, "name"),
            )
        )

    ready.sort(key=lambda item: item.season_number)
    return ready


def select_ready_collection_movies(
    known_tmdb_ids: Optional[Iterable[Any]],
    pending_tmdb_ids: Optional[Iterable[Any]],
    items: Optional[Iterable[Any]],
    *,
    today: Optional[date] = None,
    grace_days: int = 3,
) -> list[MovieCandidate]:
    ready: list[MovieCandidate] = []
    ready_until = (today or date.today()) + timedelta(days=max(grace_days, 0))
    blocked = set(merge_int_lists(known_tmdb_ids, pending_tmdb_ids))
    seen_candidates: set[int] = set()

    for item in items or []:
        tmdb_id = coerce_int(get_value(item, "tmdb_id"))
        release_date = parse_date(get_value(item, "release_date"))
        if tmdb_id is None or tmdb_id in blocked or tmdb_id in seen_candidates:
            continue
        if not release_date or release_date > ready_until:
            continue
        seen_candidates.add(tmdb_id)
        title = str(get_value(item, "title") or f"TMDB-{tmdb_id}")
        year = str(get_value(item, "year") or "") or release_date.isoformat()[:4]
        ready.append(
            MovieCandidate(
                tmdb_id=tmdb_id,
                title=title,
                year=year,
                release_date=release_date.isoformat(),
                collection_id=coerce_int(get_value(item, "collection_id")),
            )
        )

    ready.sort(key=lambda entry: (entry.release_date, entry.tmdb_id))
    return ready

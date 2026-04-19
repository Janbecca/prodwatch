from __future__ import annotations

import json
from datetime import UTC, datetime
from hashlib import sha1
from typing import Any, Mapping, Optional


def _utc_now_ts() -> str:
    return datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%S")


def _coerce_str(value: Any, *, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, str):
        return value
    return str(value)


def _coerce_int(value: Any, *, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    try:
        return int(float(value))
    except Exception:
        return default


def _sha1_hex(text: str) -> str:
    return sha1(text.encode("utf-8")).hexdigest()


def make_dedup_key_from_post_url(post_url: str) -> str:
    """
    ProdWatch dedup rule (current): `dedup_key = sha1_hex(post_url)`.

    Notes:
    - `insert_posts()` relies on `dedup_key` for INSERT OR IGNORE behavior.
    - We intentionally do NOT incorporate other fields (e.g. note_id) to match existing logic.
    """
    return _sha1_hex(_coerce_str(post_url, default=""))


def ms_epoch_to_utc_datetime_str(value: Any, *, default: Optional[str] = None) -> str:
    """
    Convert a millisecond epoch timestamp into ProdWatch's UTC DATETIME string: 'YYYY-MM-DD HH:MM:SS'.

    Rules:
    - If value is empty/invalid, fall back to `default` or current UTC time.
    """
    if value is None:
        return default or _utc_now_ts()

    if isinstance(value, str):
        s = value.strip()
        if s == "":
            return default or _utc_now_ts()
        value = s

    try:
        ms = int(float(value))
    except Exception:
        return default or _utc_now_ts()

    # Treat as milliseconds epoch.
    dt = datetime.fromtimestamp(ms / 1000.0, tz=UTC)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def map_xhs_item_to_post_candidate(
    item: Mapping[str, Any],
    *,
    project_id: int,
    crawl_job_id: int,
    platform_id: int,
    brand_id: Optional[int],
    crawled_at: Optional[str] = None,
    created_at: Optional[str] = None,
    raw_payload_json: Optional[str] = None,
) -> "PostCandidate":
    """
    Map a MediaCrawler XHS search result item into ProdWatch's PostCandidate.

    Expected XHS item fields (as provided):
    - note_id, title, desc, nickname, time (ms epoch)
    - liked_count, comment_count, share_count, note_url
    - source_keyword
    - plus the original raw JSON (optional; can be passed via `raw_payload_json`)
    """
    # Local import to avoid importing the huge pipeline module at import-time.
    from backend.pipeline_main import PostCandidate

    external_post_id = _coerce_str(item.get("note_id"), default="")
    post_url = _coerce_str(item.get("note_url"), default="")
    author_name = _coerce_str(item.get("nickname"), default="")

    desc = _coerce_str(item.get("desc"), default="")
    title = _coerce_str(item.get("title"), default="").strip()
    if title == "":
        # Title fallback: first 60 chars of desc (may be empty).
        title = desc[:60]

    content = desc  # desc is allowed to be empty string

    publish_time = ms_epoch_to_utc_datetime_str(item.get("time"))
    crawled_at_ts = crawled_at or _utc_now_ts()
    created_at_ts = created_at or _utc_now_ts()

    # Count coercion rules:
    # - empty string => 0
    # - missing => 0
    like_count = _coerce_int(item.get("liked_count"), default=0)
    comment_count = _coerce_int(item.get("comment_count"), default=0)
    share_count = _coerce_int(item.get("share_count"), default=0)
    view_count = 0

    if raw_payload_json is not None:
        raw_payload = raw_payload_json
    else:
        # Best-effort fallback when the caller didn't provide the original raw JSON string.
        raw_payload = json.dumps(item, ensure_ascii=False, separators=(",", ":"))

    dedup_key = make_dedup_key_from_post_url(post_url)

    return PostCandidate(
        project_id=project_id,
        crawl_job_id=crawl_job_id,
        platform_id=platform_id,
        brand_id=brand_id,
        external_post_id=external_post_id,
        author_name=author_name,
        title=title,
        content=content,
        post_url=post_url,
        publish_time=publish_time,
        crawled_at=crawled_at_ts,
        like_count=like_count,
        comment_count=comment_count,
        share_count=share_count,
        view_count=view_count,
        raw_payload=raw_payload,
        dedup_key=dedup_key,
        created_at=created_at_ts,
    )


def xhs_item_to_post_candidate(
    item: Mapping[str, Any],
    *,
    project_id: int,
    crawl_job_id: int,
    platform_id: int,
    brand_id: Optional[int],
    crawled_at: Optional[str] = None,
    created_at: Optional[str] = None,
    raw_payload_json: Optional[str] = None,
) -> "PostCandidate":
    """
    Backwards-compatible alias.

    Prefer `map_xhs_item_to_post_candidate(...)` for new code.
    """
    return map_xhs_item_to_post_candidate(
        item,
        project_id=project_id,
        crawl_job_id=crawl_job_id,
        platform_id=platform_id,
        brand_id=brand_id,
        crawled_at=crawled_at,
        created_at=created_at,
        raw_payload_json=raw_payload_json,
    )

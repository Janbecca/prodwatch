from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any, Optional


def _now_ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def ensure_crawl_job_progress_tables(con: sqlite3.Connection) -> None:
    """
    Best-effort schema creation for crawl_job progress tracking.

    Why:
    - Frontend needs real-time, stage-accurate progress for manual refresh.
    - Avoids adding a migration step for the demo SQLite DB.
    """
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS crawl_job_progress (
          crawl_job_id INTEGER PRIMARY KEY,
          stage TEXT,
          stage_started_at DATETIME,
          stage_updated_at DATETIME,
          message TEXT,
          meta_json TEXT
        );
        """
    )


def upsert_crawl_job_progress(
    con: sqlite3.Connection,
    *,
    crawl_job_id: int,
    stage: str,
    message: Optional[str] = None,
    meta: Optional[dict[str, Any]] = None,
    stage_started_at: Optional[str] = None,
) -> None:
    """
    Upsert a single-row progress state for a crawl_job.

    Contract:
    - `stage_started_at` is only set on first insert or when explicitly provided.
    - `stage_updated_at` always refreshes on update.
    """
    ensure_crawl_job_progress_tables(con)
    ts = _now_ts()
    st = str(stage or "").strip()
    msg = None if message is None else str(message)[:500]
    meta_json = None
    if meta is not None:
        try:
            meta_json = json.dumps(meta, ensure_ascii=False)
        except Exception:
            meta_json = None

    # Keep stage_started_at stable unless caller explicitly provides a value.
    row = con.execute(
        "SELECT stage_started_at FROM crawl_job_progress WHERE crawl_job_id=? LIMIT 1;",
        (int(crawl_job_id),),
    ).fetchone()
    started_at = stage_started_at or (row["stage_started_at"] if row is not None else None) or ts

    con.execute(
        """
        INSERT INTO crawl_job_progress(
          crawl_job_id, stage, stage_started_at, stage_updated_at, message, meta_json
        )
        VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(crawl_job_id) DO UPDATE SET
          stage=excluded.stage,
          stage_started_at=excluded.stage_started_at,
          stage_updated_at=excluded.stage_updated_at,
          message=excluded.message,
          meta_json=excluded.meta_json;
        """,
        (int(crawl_job_id), st, str(started_at), ts, msg, meta_json),
    )


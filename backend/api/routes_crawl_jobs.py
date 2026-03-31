from __future__ import annotations

import sqlite3
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.api.db import get_db


router = APIRouter(prefix="/api/crawl_jobs", tags=["crawl_jobs"])


def _has_column(db: sqlite3.Connection, table: str, col: str) -> bool:
    try:
        cols = {r[1] for r in db.execute(f"PRAGMA table_info({table});").fetchall()}
        return col in cols
    except sqlite3.Error:
        return False


def _trigger_type(job_type: Optional[str], trigger_source: Optional[str], schedule_type: Optional[str]) -> str:
    ts = str(trigger_source or "").strip().lower()
    st = str(schedule_type or "").strip().lower()
    jt = str(job_type or "").strip().lower()
    if ts in {"scheduled", "scheduler"} or st in {"daily", "cron", "interval"} or jt in {"daily"}:
        return "scheduled"
    return "manual"


@router.get("/status")
def crawl_job_status(
    crawl_job_id: int = Query(..., ge=1),
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    has_finished_at = _has_column(db, "crawl_job", "finished_at")
    finished_col = "finished_at" if has_finished_at else "ended_at"
    row = db.execute(
        f"""
        SELECT
          id,
          project_id,
          status,
          started_at,
          ended_at,
          {finished_col} AS finished_at,
          error_message,
          job_type,
          trigger_source,
          schedule_type,
          created_by
        FROM crawl_job
        WHERE id=?
        LIMIT 1;
        """,
        (int(crawl_job_id),),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="crawl_job not found")

    trigger_type = _trigger_type(row["job_type"], row["trigger_source"], row["schedule_type"])
    return {
        "ok": True,
        "item": {
            "id": int(row["id"]),
            "project_id": int(row["project_id"]),
            "status": row["status"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "error_message": row["error_message"],
            "trigger_type": trigger_type,
        },
    }


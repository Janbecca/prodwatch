from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.api.db import get_db

from backend.services.refresh_service import get_refresh_service


router = APIRouter(prefix="/api/projects", tags=["projects"])


class ManualRefreshPayload(BaseModel):
    stat_date: Optional[str] = Field(
        default=None, description="YYYY-MM-DD (default: today UTC)"
    )
    posts_per_target: int = Field(default=3, ge=1, le=50)
    created_by: str = Field(default="user")


@router.post("/{project_id}/refresh")
def manual_refresh_project(
    project_id: int, payload: ManualRefreshPayload, db: sqlite3.Connection = Depends(get_db)
) -> dict[str, Any]:
    """
    Trigger a manual refresh (crawl + analysis + aggregation) for a project.
    Runs synchronously in-process (suitable for demo/dev).
    """
    try:
        svc = get_refresh_service()
        with db:
            r = svc.refresh_project_sync(
                con=db,
                project_id=int(project_id),
                stat_date=payload.stat_date,
                posts_per_target=int(payload.posts_per_target),
                trigger="manual",
                created_by=str(payload.created_by or "user"),
            )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"manual refresh failed: {e}")

    if r.skipped:
        raise HTTPException(status_code=409, detail=f"refresh skipped: {r.reason}")
    if r.error_message:
        raise HTTPException(status_code=500, detail=f"manual refresh failed: {r.error_message}")

    return {
        "project_id": int(project_id),
        "crawl_job_id": int(r.crawl_job_id or 0),
        "stat_date": str(r.stat_date),
        "triggered_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    }

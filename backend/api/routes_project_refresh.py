# 作用：后端 API：项目刷新相关路由与接口实现。

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.api.db import DEFAULT_DB_PATH, get_db, resolve_db_path
from backend.llm.config_store import get_llm_config_store
from backend.llm.provider_factory import get_provider_factory
from backend.llm.prompts.store import get_prompt_store

from backend.services.refresh_service import get_refresh_service


router = APIRouter(prefix="/api/projects", tags=["projects"])


class ManualRefreshPayload(BaseModel):
    stat_date: Optional[str] = Field(
        default=None, description="YYYY-MM-DD (default: today UTC)"
    )
    posts_per_target: int = Field(default=3, ge=1, le=50)
    created_by: str = Field(default="user")


@router.get("/{project_id}/refresh/status")
def project_refresh_status(project_id: int, db: sqlite3.Connection = Depends(get_db)) -> dict[str, Any]:
    """
    Refresh status endpoint for the frontend.

    Why:
    - /refresh returns 409 when a refresh is already running (by design).
    - Browsers may log non-2xx requests with an "initiator" stack trace in the console.
      This endpoint lets the UI check status first and avoid triggering a 409 in normal cases.
    """
    svc = get_refresh_service()
    # First check in-process lock: avoids a short race window where a refresh is running but
    # crawl_job hasn't been created yet (DB check would return not running).
    if svc.is_running_in_memory(int(project_id)):
        return {"ok": True, "project_id": int(project_id), "running": True, "reason": "in_memory_lock"}

    with db:
        job_id = svc.get_recent_running_job_id(db, int(project_id))
        if job_id is None:
            return {"ok": True, "project_id": int(project_id), "running": False}
        row = db.execute("SELECT id, started_at FROM crawl_job WHERE id=? LIMIT 1;", (int(job_id),)).fetchone()
    return {
        "ok": True,
        "project_id": int(project_id),
        "running": True,
        "reason": "db_running",
        "crawl_job_id": int(job_id),
        "started_at": (row["started_at"] if row is not None else None),
    }


@router.post("/{project_id}/refresh", status_code=202)
def manual_refresh_project(
    project_id: int, payload: ManualRefreshPayload, db: sqlite3.Connection = Depends(get_db)
) -> dict[str, Any]:
    """
    Trigger a manual refresh (crawl + analysis + aggregation) for a project.
    Starts a background refresh job and returns immediately.
    """
    svc = get_refresh_service()

    # Provide a deterministic "plan" for post generation at trigger time.
    # Avoid depending on the request DB connection here to reduce "database is locked" races
    # with the just-started background refresh worker.
    post_generation_plan: dict[str, Any] | None = None
    try:
        cfg = get_llm_config_store().get("crawler_generation", con=None)
        prompt_version = get_prompt_store().get("crawler_generation").version
        provider = str(getattr(cfg, "provider", "") or "").strip().lower() or "deepseek"
        if get_provider_factory().get(provider) is None:
            provider = (get_provider_factory().list_provider_names() or ["deepseek"])[0]
        model = str(getattr(cfg, "model", "") or "").strip()
        post_generation_plan = {
            "generated_by": "llm",
            "provider": provider,
            "model": model,
            "prompt_version": str(prompt_version or "").strip(),
        }
    except Exception:
        # Best-effort fallback: still return a stable shape for the frontend logger.
        post_generation_plan = {"generated_by": "llm", "provider": "unknown", "model": "", "prompt_version": ""}

    try:
        db_path = resolve_db_path(DEFAULT_DB_PATH)
        r = svc.refresh_project_async(
            db_path=str(db_path),
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
        # Keep the API contract (409) but make the error message actionable for the frontend.
        # Do NOT change request/response schema on success; only improve the conflict detail.
        if r.reason == "db_running" and r.crawl_job_id:
            raise HTTPException(
                status_code=409,
                detail=f"刷新被跳过：该项目已有刷新任务在运行中（任务编号={int(r.crawl_job_id)}），请稍后重试。",
            )
        if r.reason == "in_memory_lock":
            if r.crawl_job_id:
                raise HTTPException(
                    status_code=409,
                    detail=f"刷新被跳过：该项目正在刷新中（任务编号={int(r.crawl_job_id)}），请稍后重试。",
                )
            raise HTTPException(status_code=409, detail="刷新被跳过：该项目正在刷新中，请稍后重试。")
        raise HTTPException(status_code=409, detail=f"刷新被跳过：{r.reason or '未知原因'}")
    if r.error_message:
        raise HTTPException(status_code=500, detail=f"manual refresh failed: {r.error_message}")

    return {
        "project_id": int(project_id),
        "crawl_job_id": int(r.crawl_job_id or 0),
        "stat_date": str(r.stat_date),
        "triggered_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "accepted": True,
        "running": True,
        "post_generation_plan": post_generation_plan,
    }

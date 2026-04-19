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
    crawl_source: Optional[str] = Field(default=None, description="mock_llm|media_crawler (default: mock_llm)")


class SimulatePayload(BaseModel):
    stat_date: Optional[str] = Field(default=None, description="YYYY-MM-DD (default: today UTC)")
    posts_per_target: int = Field(default=3, ge=1, le=50)
    created_by: str = Field(default="user")
    crawl_source: Optional[str] = Field(default=None, description="mock_llm|media_crawler (default: mock_llm)")


class AnalyzePayload(BaseModel):
    source_crawl_job_id: Optional[int] = Field(default=None, ge=1, description="Analyze posts from this crawl_job_id")
    created_by: str = Field(default="user")


def _post_generation_plan_best_effort() -> dict[str, Any]:
    """
    Best-effort plan snapshot for the frontend logger.

    Keeps the response schema stable even if LLM config is missing or invalid.
    """
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
        post_generation_plan = {"generated_by": "llm", "provider": "unknown", "model": "", "prompt_version": ""}
    return post_generation_plan


def _resolve_latest_post_batch_job_id(db: sqlite3.Connection, project_id: int) -> Optional[int]:
    row = db.execute(
        """
        SELECT MAX(crawl_job_id) AS id
        FROM post_raw
        WHERE project_id=? AND crawl_job_id IS NOT NULL;
        """,
        (int(project_id),),
    ).fetchone()
    if row is None:
        return None
    v = row["id"]
    return int(v) if v is not None else None


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

    post_generation_plan = _post_generation_plan_best_effort()

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
            crawl_source=payload.crawl_source,
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


@router.post("/{project_id}/simulate", status_code=202)
def simulate_posts(
    project_id: int, payload: SimulatePayload, db: sqlite3.Connection = Depends(get_db)
) -> dict[str, Any]:
    """
    Simulate/generate posts only (no analysis).
    """
    svc = get_refresh_service()
    post_generation_plan = _post_generation_plan_best_effort()

    try:
        db_path = resolve_db_path(DEFAULT_DB_PATH)
        r = svc.simulate_project_async(
            db_path=str(db_path),
            con=db,
            project_id=int(project_id),
            stat_date=payload.stat_date,
            posts_per_target=int(payload.posts_per_target),
            trigger="manual",
            created_by=str(payload.created_by or "user"),
            crawl_source=payload.crawl_source,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"simulate failed: {e}")

    if r.skipped:
        raise HTTPException(status_code=409, detail=f"simulate skipped: {r.reason or 'unknown'}")
    if r.error_message:
        raise HTTPException(status_code=500, detail=f"simulate failed: {r.error_message}")

    return {
        "project_id": int(project_id),
        "crawl_job_id": int(r.crawl_job_id or 0),
        "stat_date": str(r.stat_date),
        "triggered_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "accepted": True,
        "running": True,
        "job_kind": "simulate",
        "post_generation_plan": post_generation_plan,
    }


@router.post("/{project_id}/analyze", status_code=202)
def analyze_posts(
    project_id: int, payload: AnalyzePayload, db: sqlite3.Connection = Depends(get_db)
) -> dict[str, Any]:
    """
    Analyze posts from an existing simulated batch (or the latest available batch).
    """
    svc = get_refresh_service()

    src = int(payload.source_crawl_job_id) if payload.source_crawl_job_id else None
    if src is None:
        src = _resolve_latest_post_batch_job_id(db, int(project_id))
    if src is None:
        raise HTTPException(status_code=400, detail="no post batch found to analyze")

    # Ensure the source batch belongs to this project.
    row = db.execute(
        "SELECT 1 FROM post_raw WHERE project_id=? AND crawl_job_id=? LIMIT 1;",
        (int(project_id), int(src)),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=400, detail="source_crawl_job_id not found for this project")

    try:
        db_path = resolve_db_path(DEFAULT_DB_PATH)
        r = svc.analyze_project_async(
            db_path=str(db_path),
            con=db,
            project_id=int(project_id),
            source_crawl_job_id=int(src),
            trigger="manual",
            created_by=str(payload.created_by or "user"),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"analyze failed: {e}")

    if r.skipped:
        raise HTTPException(status_code=409, detail=f"analyze skipped: {r.reason or 'unknown'}")
    if r.error_message:
        raise HTTPException(status_code=500, detail=f"analyze failed: {r.error_message}")

    return {
        "project_id": int(project_id),
        "crawl_job_id": int(r.crawl_job_id or 0),
        "triggered_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "accepted": True,
        "running": True,
        "job_kind": "analyze",
        "source_crawl_job_id": int(src),
    }

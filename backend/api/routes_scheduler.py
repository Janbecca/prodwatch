# 作用：后端 API：调度器相关路由与接口实现。

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel, Field

from backend.services.daily_refresh_scheduler import get_daily_scheduler


router = APIRouter(prefix="/api/scheduler", tags=["scheduler"])


@router.get("/status")
def scheduler_status() -> dict:
    """
    Scheduler status for local debugging.
    """
    s = get_daily_scheduler()
    return {"ok": True, "scheduler": s.status()}


class RunDailyOncePayload(BaseModel):
    stat_date: str | None = Field(default=None, description="YYYY-MM-DD (default: today local)")


@router.post("/run_daily_once")
def run_daily_once(payload: RunDailyOncePayload) -> dict:
    """
    Manually trigger the daily scheduled refresh logic once.
    Useful for local simulation without waiting for the configured hh:mm.
    """
    s = get_daily_scheduler()
    return s.run_daily_once(stat_date=payload.stat_date)


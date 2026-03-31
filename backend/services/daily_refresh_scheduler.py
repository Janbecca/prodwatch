from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional

from backend.api.db import DEFAULT_DB_PATH, connect, resolve_db_path
from backend.services.refresh_service import RefreshResult, get_refresh_service


log = logging.getLogger("prodwatch.scheduler")


@dataclass(frozen=True)
class SchedulerConfig:
    enabled: bool
    hour: int
    minute: int
    posts_per_target: int
    created_by: str


def load_scheduler_config() -> SchedulerConfig:
    enabled = os.environ.get("PRODWATCH_SCHEDULER_ENABLED", "1").strip() not in {"0", "false", "False"}
    hour = int(os.environ.get("PRODWATCH_DAILY_REFRESH_HOUR", "2"))
    minute = int(os.environ.get("PRODWATCH_DAILY_REFRESH_MINUTE", "0"))
    posts_per_target = int(os.environ.get("PRODWATCH_DAILY_POSTS_PER_TARGET", "3"))
    created_by = os.environ.get("PRODWATCH_SCHEDULER_CREATED_BY", "scheduler").strip() or "scheduler"
    # clamp
    hour = max(0, min(23, hour))
    minute = max(0, min(59, minute))
    posts_per_target = max(1, min(50, posts_per_target))
    return SchedulerConfig(
        enabled=bool(enabled),
        hour=hour,
        minute=minute,
        posts_per_target=posts_per_target,
        created_by=created_by,
    )


class DailyRefreshScheduler:
    """
    Lightweight in-process daily scheduler (no external deps).

    - Checks local time periodically.
    - Runs once per day when now >= configured hh:mm.
    - Uses RefreshService so manual & scheduled share the same pipeline chain.
    """

    def __init__(self, cfg: SchedulerConfig):
        self.cfg = cfg
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._last_run_date: Optional[str] = None  # YYYY-MM-DD (local)
        self._last_run_at: Optional[str] = None
        self._last_results: Optional[list[dict[str, Any]]] = None

    def start(self) -> None:
        if not self.cfg.enabled:
            log.info("DailyRefreshScheduler disabled by PRODWATCH_SCHEDULER_ENABLED=0")
            return
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._loop, name="prodwatch-daily-scheduler", daemon=True)
        self._thread.start()
        log.info("DailyRefreshScheduler started: %02d:%02d", self.cfg.hour, self.cfg.minute)

    def stop(self) -> None:
        self._stop.set()
        t = self._thread
        if t and t.is_alive():
            t.join(timeout=5)
        log.info("DailyRefreshScheduler stopped")

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "enabled": self.cfg.enabled,
                "hour": self.cfg.hour,
                "minute": self.cfg.minute,
                "posts_per_target": self.cfg.posts_per_target,
                "last_run_date": self._last_run_date,
                "last_run_at": self._last_run_at,
                "next_run_at": self._next_run_at_local(),
            }

    def run_daily_once(self, *, stat_date: Optional[str] = None) -> dict[str, Any]:
        # This method is safe to call from an API endpoint for local simulation.
        results = self._run_daily(stat_date=stat_date)
        return {"ok": True, "mode": "daily", "results": results}

    def _next_run_at_local(self) -> str:
        now = datetime.now()
        target = now.replace(hour=self.cfg.hour, minute=self.cfg.minute, second=0, microsecond=0)
        if target <= now:
            target = target + timedelta(days=1)
        return target.strftime("%Y-%m-%d %H:%M:%S")

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                now = datetime.now()
                today = now.strftime("%Y-%m-%d")
                target = now.replace(hour=self.cfg.hour, minute=self.cfg.minute, second=0, microsecond=0)
                should_run = now >= target
                with self._lock:
                    already_ran_today = self._last_run_date == today
                if should_run and not already_ran_today:
                    self._run_daily(stat_date=today)
                # polling interval
                self._stop.wait(5.0)
            except Exception:
                log.exception("DailyRefreshScheduler loop error")
                self._stop.wait(10.0)

    def _run_daily(self, *, stat_date: Optional[str]) -> list[dict[str, Any]]:
        started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        results: list[dict[str, Any]] = []
        try:
            db_path = resolve_db_path(DEFAULT_DB_PATH)
            con = connect(db_path)
            try:
                svc = get_refresh_service()
                project_ids = svc.list_daily_projects(con)
                for pid in project_ids:
                    with con:
                        r: RefreshResult = svc.refresh_project_sync(
                            con=con,
                            project_id=int(pid),
                            stat_date=stat_date,
                            posts_per_target=int(self.cfg.posts_per_target),
                            trigger="scheduled",
                            created_by=self.cfg.created_by,
                        )
                    results.append(
                        {
                            "project_id": r.project_id,
                            "skipped": bool(r.skipped),
                            "reason": r.reason,
                            "crawl_job_id": r.crawl_job_id,
                            "stat_date": r.stat_date,
                            "error_message": r.error_message,
                        }
                    )
            finally:
                con.close()

            with self._lock:
                self._last_run_date = (stat_date or datetime.now().strftime("%Y-%m-%d"))
                self._last_run_at = started_at
                self._last_results = results
            log.info("Daily refresh done projects=%s", len(results))
            return results
        except Exception:
            log.exception("Daily refresh failed")
            with self._lock:
                self._last_run_at = started_at
                self._last_results = results
            return results


_scheduler: Optional[DailyRefreshScheduler] = None


def get_daily_scheduler() -> DailyRefreshScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = DailyRefreshScheduler(load_scheduler_config())
    return _scheduler


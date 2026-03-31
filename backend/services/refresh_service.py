from __future__ import annotations

import logging
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional

from backend.pipeline_main import parse_stat_date, run_pipeline_with_trigger


log = logging.getLogger("prodwatch.refresh")


@dataclass(frozen=True)
class RefreshResult:
    project_id: int
    skipped: bool
    reason: Optional[str] = None
    crawl_job_id: Optional[int] = None
    stat_date: Optional[str] = None
    error_message: Optional[str] = None


class RefreshService:
    """
    Refresh service used by both manual API and scheduled jobs.

    Design goals:
    - Keep the refresh chain reusable across triggers (manual/scheduled).
    - Provide in-memory de-duplication for a single process.
    - Add a DB guard to avoid overlapping runs when a job is already running.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._running_projects: set[int] = set()

    def list_daily_projects(self, con: sqlite3.Connection) -> list[int]:
        rows = con.execute(
            """
            SELECT id
            FROM project
            WHERE is_active=1
              AND refresh_mode='daily'
              AND deleted_at IS NULL
            ORDER BY id;
            """
        ).fetchall()
        return [int(r["id"]) for r in rows]

    def _db_recent_running_job_id(
        self, con: sqlite3.Connection, project_id: int, *, within_minutes: int = 180
    ) -> Optional[int]:
        row = con.execute(
            """
            SELECT id, started_at
            FROM crawl_job
            WHERE project_id=?
              AND status='running'
            ORDER BY id DESC
            LIMIT 1;
            """,
            (int(project_id),),
        ).fetchone()
        if row is None:
            return None

        started_at = row["started_at"]
        if not started_at:
            return int(row["id"])
        try:
            dt = datetime.strptime(str(started_at)[:19], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return int(row["id"])
        if dt >= (datetime.utcnow() - timedelta(minutes=int(within_minutes))):
            return int(row["id"])
        return None

    def refresh_project_sync(
        self,
        *,
        con: sqlite3.Connection,
        project_id: int,
        stat_date: Optional[str],
        posts_per_target: int,
        trigger: str,  # manual|scheduled
        created_by: str,
    ) -> RefreshResult:
        # Normalize stat_date to pipeline format.
        stat_date_norm = parse_stat_date(stat_date)
        pid = int(project_id)

        with self._lock:
            if pid in self._running_projects:
                return RefreshResult(project_id=pid, skipped=True, reason="in_memory_lock", stat_date=stat_date_norm)
            self._running_projects.add(pid)

        try:
            running_job_id = self._db_recent_running_job_id(con, pid)
            if running_job_id is not None:
                return RefreshResult(
                    project_id=pid,
                    skipped=True,
                    reason="db_running",
                    crawl_job_id=int(running_job_id),
                    stat_date=stat_date_norm,
                )

            if trigger == "scheduled":
                crawl_job_id = run_pipeline_with_trigger(
                    con=con,
                    project_id=pid,
                    stat_date=str(stat_date_norm),
                    posts_per_target=int(posts_per_target),
                    job_type="daily",
                    trigger_source="scheduled",
                    schedule_type="daily",
                    schedule_expr=None,
                    created_by=str(created_by),
                )
            else:
                crawl_job_id = run_pipeline_with_trigger(
                    con=con,
                    project_id=pid,
                    stat_date=str(stat_date_norm),
                    posts_per_target=int(posts_per_target),
                    job_type="manual",
                    trigger_source="manual",
                    schedule_type="manual",
                    schedule_expr=None,
                    created_by=str(created_by),
                )

            return RefreshResult(
                project_id=pid,
                skipped=False,
                crawl_job_id=int(crawl_job_id),
                stat_date=stat_date_norm,
            )
        except Exception as e:
            msg = str(e)
            log.exception("refresh_project_sync failed project_id=%s trigger=%s", pid, trigger)
            return RefreshResult(
                project_id=pid,
                skipped=False,
                stat_date=stat_date_norm,
                error_message=msg,
            )
        finally:
            with self._lock:
                self._running_projects.discard(pid)


_default_service: Optional[RefreshService] = None


def get_refresh_service() -> RefreshService:
    global _default_service
    if _default_service is None:
        _default_service = RefreshService()
    return _default_service

# 作用：后端服务层：数据刷新相关业务逻辑封装。

from __future__ import annotations

import logging
import os
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

    def is_running_in_memory(self, project_id: int) -> bool:
        """
        In-process guard state.

        Used by status endpoints to reduce user-visible 409s caused by race windows:
        - A refresh can be running but hasn't created a crawl_job row yet.
        - DB-only checks would return "not running", then the frontend POSTs /refresh and hits 409.
        """
        pid = int(project_id)
        with self._lock:
            return pid in self._running_projects

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
        """
        DB guardrail: avoid overlapping refresh runs across processes.

        Note:
        - In dev/demo, the backend may crash mid-refresh, leaving crawl_job.status='running' behind.
          That stale record would block subsequent manual refresh calls and surface as HTTP 409.
        - To improve recoverability without changing any API schemas, we automatically mark
          "stale running" jobs as failed when they are older than a small TTL.
        """
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

        # Stale-running TTL (seconds). Keep small by default; manual refresh runs are synchronous.
        # Override via env when deploying in environments where refresh may legitimately run longer.
        try:
            stale_s = int(str(os.environ.get("PRODWATCH_REFRESH_STALE_RUNNING_AFTER_S") or "60").strip())
        except Exception:
            stale_s = 60

        job_id = int(row["id"])
        started_at = row["started_at"]
        if not started_at:
            self._mark_job_failed_best_effort(
                con, job_id, "刷新任务状态异常：running 但 started_at 为空，已自动标记为失败（可重试）"
            )
            return None
        try:
            dt = datetime.strptime(str(started_at)[:19], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            self._mark_job_failed_best_effort(
                con, job_id, "刷新任务状态异常：started_at 无法解析，已自动标记为失败（可重试）"
            )
            return None

        # If a running job is too old, consider it stale (likely crash) and auto-clear it.
        if stale_s > 0 and dt < (datetime.utcnow() - timedelta(seconds=int(stale_s))):
            self._mark_job_failed_best_effort(
                con, job_id, f"刷新任务超时（运行中超过 {int(stale_s)} 秒），已自动标记为失败（可重试）"
            )
            return None
        if dt >= (datetime.utcnow() - timedelta(minutes=int(within_minutes))):
            return job_id
        return None

    def get_recent_running_job_id(self, con: sqlite3.Connection, project_id: int) -> Optional[int]:
        """
        Public wrapper for checking whether a project has a "running" crawl_job.

        Used by:
        - Manual refresh endpoint (to return 409)
        - Refresh status endpoint (to avoid triggering a 409 from the frontend)

        Note: This method includes stale-running auto-recovery (see _db_recent_running_job_id).
        """
        return self._db_recent_running_job_id(con, int(project_id))

    def _mark_job_failed_best_effort(self, con: sqlite3.Connection, crawl_job_id: int, error_message: str) -> None:
        """
        Best-effort helper used by the refresh DB guard.

        Keep compatible with older DBs that don't have `finished_at`.
        """
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        msg = (error_message or "")[:500]
        try:
            con.execute(
                "UPDATE crawl_job SET status=?, ended_at=?, finished_at=?, error_message=? WHERE id=?;",
                ("failed", ts, ts, msg, int(crawl_job_id)),
            )
        except sqlite3.OperationalError as e:
            s = str(e).lower()
            if ("no such column" not in s) and ("has no column named" not in s):
                raise
            con.execute(
                "UPDATE crawl_job SET status=?, ended_at=?, error_message=? WHERE id=?;",
                ("failed", ts, msg, int(crawl_job_id)),
            )

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
                # Another refresh is executing in this process.
                # Best-effort: attach the currently running crawl_job_id (if already created)
                # so the API can return an actionable 409 message for the frontend.
                running_job_id = self._db_recent_running_job_id(con, pid)
                return RefreshResult(
                    project_id=pid,
                    skipped=True,
                    reason="in_memory_lock",
                    crawl_job_id=int(running_job_id) if running_job_id is not None else None,
                    stat_date=stat_date_norm,
                )
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

            # Guardrail: a "success" crawl_job with 0 post_raw rows is confusing to the user.
            # This most commonly happens when the generated external_post_id/post_url is stable
            # across refresh runs (INSERT OR IGNORE de-dupes everything), or when the project has
            # no effective crawl targets (no platforms/brands/keywords configured).
            try:
                post_cnt = int(
                    con.execute("SELECT COUNT(*) AS cnt FROM post_raw WHERE crawl_job_id=?;", (int(crawl_job_id),))
                    .fetchone()["cnt"]
                )
            except Exception:
                post_cnt = -1

            if post_cnt == 0:
                msg = (
                    "刷新未生成任何帖子：本次 crawl_job 写入的 post_raw=0。"
                    "可能原因：生成结果被去重（external_post_id/post_url 复用）、或项目配置为空。"
                    "请检查项目配置，或查看 crawl_job/error_message 以定位。"
                )
                try:
                    self._mark_job_failed_best_effort(con, int(crawl_job_id), msg)
                except Exception:
                    pass
                return RefreshResult(
                    project_id=pid,
                    skipped=False,
                    crawl_job_id=int(crawl_job_id),
                    stat_date=stat_date_norm,
                    error_message=msg,
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

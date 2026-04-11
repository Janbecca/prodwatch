# 作用：后端服务层：数据刷新相关业务逻辑封装。

from __future__ import annotations

import logging
import os
import sqlite3
import threading
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional

from backend.pipeline_main import (
    create_crawl_job,
    mark_job_running,
    parse_stat_date,
    run_pipeline_existing_job,
)
from backend.api.db import connect


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
        self._running_started_at: dict[int, datetime] = {}

    def _in_memory_stale_after_s(self) -> int:
        """
        In-memory lock stale TTL (seconds).

        Why:
        - The refresh chain is synchronous and should finish quickly in demo/dev.
        - If the process crashes mid-refresh, the in-memory set is cleared on restart.
        - If a refresh blocks/hangs (e.g. long external call), the in-memory lock would otherwise
          keep the UI in a permanent "refreshing" state. We auto-clear after a TTL to recover.
        """
        try:
            return int(str(os.environ.get("PRODWATCH_REFRESH_STALE_IN_MEMORY_AFTER_S") or "300").strip())
        except Exception:
            return 300

    def is_running_in_memory(self, project_id: int) -> bool:
        """
        In-process guard state.

        Used by status endpoints to reduce user-visible 409s caused by race windows:
        - A refresh can be running but hasn't created a crawl_job row yet.
        - DB-only checks would return "not running", then the frontend POSTs /refresh and hits 409.
        """
        pid = int(project_id)
        with self._lock:
            if pid not in self._running_projects:
                return False
            # Best-effort stale recovery for in-memory lock.
            ttl_s = self._in_memory_stale_after_s()
            if ttl_s > 0:
                started = self._running_started_at.get(pid)
                if started and started < (datetime.now() - timedelta(seconds=int(ttl_s))):
                    self._running_projects.discard(pid)
                    self._running_started_at.pop(pid, None)
                    return False
            return True

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

        # Stale-running TTL (seconds).
        # Manual refresh now runs async and may take a while (LLM calls), so keep the default generous.
        # Override via env when deploying in environments where refresh may legitimately run longer.
        try:
            stale_s = int(str(os.environ.get("PRODWATCH_REFRESH_STALE_RUNNING_AFTER_S") or "1800").strip())
        except Exception:
            stale_s = 1800

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
        # `started_at` is typically written using sqlite datetime('now','localtime').
        # Compare with local time here to avoid timezone skew.
        if stale_s > 0 and dt < (datetime.now() - timedelta(seconds=int(stale_s))):
            self._mark_job_failed_best_effort(
                con, job_id, f"刷新任务超时（运行中超过 {int(stale_s)} 秒），已自动标记为失败（可重试）"
            )
            return None
        if dt >= (datetime.now() - timedelta(minutes=int(within_minutes))):
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
            self._running_started_at[pid] = datetime.now()

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
                self._running_started_at.pop(pid, None)

    def refresh_project_async(
        self,
        *,
        db_path: str,
        con: sqlite3.Connection,
        project_id: int,
        stat_date: Optional[str],
        posts_per_target: int,
        trigger: str,  # manual|scheduled
        created_by: str,
    ) -> RefreshResult:
        """
        Fire-and-forget refresh runner.

        Why:
        - Manual refresh can be slow (LLM calls). Keeping it synchronous makes the browser request hang,
          and can also hold SQLite transactions longer than necessary.
        - The frontend already polls /refresh/status; this method makes manual refresh responsive.
        """
        stat_date_norm = parse_stat_date(stat_date)
        pid = int(project_id)

        with self._lock:
            if pid in self._running_projects:
                running_job_id = self._db_recent_running_job_id(con, pid)
                return RefreshResult(
                    project_id=pid,
                    skipped=True,
                    reason="in_memory_lock",
                    crawl_job_id=int(running_job_id) if running_job_id is not None else None,
                    stat_date=stat_date_norm,
                )
            self._running_projects.add(pid)
            self._running_started_at[pid] = datetime.now()

        # DB guard (release in-memory lock on early return)
        running_job_id = self._db_recent_running_job_id(con, pid)
        if running_job_id is not None:
            with self._lock:
                self._running_projects.discard(pid)
                self._running_started_at.pop(pid, None)
            return RefreshResult(
                project_id=pid,
                skipped=True,
                reason="db_running",
                crawl_job_id=int(running_job_id),
                stat_date=stat_date_norm,
            )

        # Create a crawl_job row up-front so we always have an id (even if the worker fails early).
        job_type = "daily" if trigger == "scheduled" else "manual"
        trigger_source = "scheduled" if trigger == "scheduled" else "manual"
        schedule_type = "daily" if trigger == "scheduled" else "manual"
        with con:
            crawl_job_id = create_crawl_job(
                con,
                int(pid),
                job_type=job_type,
                trigger_source=trigger_source,
                schedule_type=schedule_type,
                schedule_expr=None,
                created_by=str(created_by or "user"),
            )
            # Mark running immediately to avoid a race where the UI polls /refresh/status before the
            # worker thread executes `mark_job_running`, which would otherwise appear as "not running".
            # This also helps attribute early worker failures to a visible crawl_job row.
            mark_job_running(con, int(crawl_job_id))

        def _con_db_path(c: sqlite3.Connection) -> Optional[str]:
            try:
                rows = c.execute("PRAGMA database_list;").fetchall()
                for r in rows:
                    # columns: seq, name, file
                    if len(r) >= 3 and str(r[1]) == "main":
                        v = str(r[2] or "").strip()
                        return v or None
            except Exception:
                return None
            return None

        # Prefer the exact DB file backing the request connection. Using a mismatched relative path
        # can cause the background worker to connect to a different SQLite file and silently do no work.
        worker_db_path = _con_db_path(con) or str(db_path)

        def _worker() -> None:
            con2: Optional[sqlite3.Connection] = None
            try:
                con2 = connect(str(worker_db_path))
                with con2:
                    run_pipeline_existing_job(
                        con=con2,
                        crawl_job_id=int(crawl_job_id),
                        project_id=pid,
                        stat_date=str(stat_date_norm),
                        posts_per_target=int(posts_per_target),
                    )
            except Exception as e:
                log.exception("refresh_project_async worker failed project_id=%s trigger=%s", pid, trigger)
                # Best-effort: persist error on the crawl_job row so the UI/DB can diagnose it.
                try:
                    if con2 is None:
                        con2 = connect(str(worker_db_path))
                    with con2:
                        self._mark_job_failed_best_effort(
                            con2,
                            int(crawl_job_id),
                            f"{type(e).__name__}: {e}",
                        )
                except Exception:
                    pass
            finally:
                if con2 is not None:
                    with suppress(Exception):
                        con2.close()
                with self._lock:
                    self._running_projects.discard(pid)
                    self._running_started_at.pop(pid, None)

        t = threading.Thread(target=_worker, name=f"prodwatch-refresh-{pid}", daemon=True)
        t.start()

        return RefreshResult(project_id=pid, skipped=False, stat_date=stat_date_norm, crawl_job_id=int(crawl_job_id))


_default_service: Optional[RefreshService] = None


def get_refresh_service() -> RefreshService:
    global _default_service
    if _default_service is None:
        _default_service = RefreshService()
    return _default_service

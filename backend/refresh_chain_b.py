from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional

ROOT = str(Path(__file__).resolve().parents[1])
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from backend.pipeline_main import (
    DB_DEFAULT_PATH,
    bootstrap_if_empty,
    connect,
    parse_stat_date,
    resolve_db_path,
    run_pipeline_with_trigger,
)


def date_only(ts: Optional[str]) -> Optional[str]:
    if not ts:
        return None
    try:
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
    except ValueError:
        try:
            return datetime.strptime(ts, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            return None


def get_project_last_refresh_date(con: sqlite3.Connection, project_id: int) -> Optional[str]:
    row = con.execute("SELECT last_refresh_at FROM project WHERE id=?;", (project_id,)).fetchone()
    if not row:
        return None
    return date_only(row["last_refresh_at"])


def project_has_job_today(con: sqlite3.Connection, project_id: int, stat_date: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM crawl_job
        WHERE project_id=?
          AND date(started_at)=?
          AND status IN ('running','success')
        LIMIT 1;
        """,
        (project_id, stat_date),
    ).fetchone()
    return row is not None


def list_daily_projects(con: sqlite3.Connection) -> list[int]:
    rows = con.execute(
        """
        SELECT id
        FROM project
        WHERE is_active=1 AND refresh_mode='daily' AND deleted_at IS NULL
        ORDER BY id;
        """
    ).fetchall()
    return [int(r["id"]) for r in rows]


def auto_refresh_daily(
    con: sqlite3.Connection,
    stat_date: str,
    posts_per_target: int,
    created_by: str,
) -> list[dict]:
    results: list[dict] = []
    for project_id in list_daily_projects(con):
        last_date = get_project_last_refresh_date(con, project_id)
        if last_date == stat_date:
            results.append(
                {"project_id": project_id, "skipped": True, "reason": "already_refreshed", "stat_date": stat_date}
            )
            continue
        if project_has_job_today(con, project_id, stat_date):
            results.append(
                {"project_id": project_id, "skipped": True, "reason": "job_exists_today", "stat_date": stat_date}
            )
            continue

        crawl_job_id = run_pipeline_with_trigger(
            con=con,
            project_id=project_id,
            stat_date=stat_date,
            posts_per_target=posts_per_target,
            job_type="daily",
            trigger_source="scheduler",
            schedule_type="daily",
            schedule_expr=None,
            created_by=created_by,
        )
        results.append(
            {"project_id": project_id, "skipped": False, "crawl_job_id": crawl_job_id, "stat_date": stat_date}
        )
    return results


def manual_refresh(
    con: sqlite3.Connection,
    project_id: int,
    stat_date: str,
    posts_per_target: int,
    created_by: str,
) -> dict:
    crawl_job_id = run_pipeline_with_trigger(
        con=con,
        project_id=project_id,
        stat_date=stat_date,
        posts_per_target=posts_per_target,
        job_type="manual",
        trigger_source="user",
        schedule_type="manual",
        schedule_expr=None,
        created_by=created_by,
    )
    return {"project_id": project_id, "crawl_job_id": crawl_job_id, "stat_date": stat_date}


def main() -> None:
    parser = argparse.ArgumentParser(description="主链路B：自动/手动刷新链路（SQLite）")
    parser.add_argument("--db", default=DB_DEFAULT_PATH)
    parser.add_argument("--mode", choices=["daily", "manual"], required=True)
    parser.add_argument("--project-id", type=int, default=None)
    parser.add_argument("--stat-date", default=None, help="YYYY-MM-DD (default: today UTC)")
    parser.add_argument("--posts-per-target", type=int, default=3)
    parser.add_argument("--created-by", default="system")
    args = parser.parse_args()

    stat_date = parse_stat_date(args.stat_date)
    db_path = resolve_db_path(str(args.db))
    con = connect(db_path)
    try:
        with con:
            _ = bootstrap_if_empty(con)

        if args.mode == "daily":
            with con:
                result = auto_refresh_daily(
                    con=con,
                    stat_date=stat_date,
                    posts_per_target=int(args.posts_per_target),
                    created_by=str(args.created_by),
                )
            print(
                json.dumps(
                    {"ok": True, "db": db_path, "mode": "daily", "stat_date": stat_date, "results": result},
                    ensure_ascii=False,
                )
            )
            return

        if args.mode == "manual":
            if args.project_id is None:
                raise SystemExit("--project-id is required for --mode manual")
            with con:
                result = manual_refresh(
                    con=con,
                    project_id=int(args.project_id),
                    stat_date=stat_date,
                    posts_per_target=int(args.posts_per_target),
                    created_by=str(args.created_by),
                )
            print(
                json.dumps(
                    {"ok": True, "db": db_path, "mode": "manual", "stat_date": stat_date, **result},
                    ensure_ascii=False,
                )
            )
            return
    finally:
        con.close()


if __name__ == "__main__":
    main()

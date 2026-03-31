from __future__ import annotations

"""
Minimal backend self-test (no frontend required).

Covers the main happy-path chains:
1) Create project
2) Configure brand/platform/keywords (via update before activation)
3) Activate project
4) Trigger manual refresh (standard pipeline)
5) Verify post_raw inserted
6) Verify analysis tables inserted
7) Verify daily_* aggregated tables inserted
8) Create report
9) Verify report content_markdown + report_evidence generated

Notes (mock limitations):
- Post analysis is currently rule/mock based (see backend/services/analyzer_service.py).
- Report content generation is currently mock/template based (see backend/services/report_generation_service.py).
"""

import argparse
import os
import shutil
import sqlite3
import sys
from dataclasses import asdict
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from backend.api.db import connect
from backend.api.routes_project_mutations import (  # noqa: E402
    ActivationPayload,
    KeywordItem,
    ProjectPayload,
    create_project,
    set_project_activation,
    update_project,
)
from backend.api.routes_project_refresh import ManualRefreshPayload, manual_refresh_project  # noqa: E402
from backend.api.routes_reports import CreateReportRequest, create_report  # noqa: E402


def ensure_column(con: sqlite3.Connection, table: str, col: str, ddl: str) -> None:
    cols = {r[1] for r in con.execute(f"PRAGMA table_info({table});").fetchall()}
    if col in cols:
        return
    con.execute(ddl)


def ensure_index(con: sqlite3.Connection, ddl: str) -> None:
    con.execute(ddl)


def prepare_test_db(template_path: Path, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(str(template_path), str(out_path))


def apply_minimal_migrations(con: sqlite3.Connection) -> None:
    # crawl_job.finished_at (optional, but helpful for status & future async)
    ensure_column(con, "crawl_job", "finished_at", "ALTER TABLE crawl_job ADD COLUMN finished_at DATETIME;")

    # crawl_job_target traceability fields
    ensure_column(con, "crawl_job_target", "project_id", "ALTER TABLE crawl_job_target ADD COLUMN project_id INTEGER;")
    ensure_column(con, "crawl_job_target", "status", "ALTER TABLE crawl_job_target ADD COLUMN status TEXT;")
    ensure_column(con, "crawl_job_target", "created_at", "ALTER TABLE crawl_job_target ADD COLUMN created_at DATETIME;")
    ensure_index(
        con,
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_crawl_job_target_combo ON crawl_job_target(crawl_job_id, platform_id, brand_id, keyword);",
    )

    # report task/error fields (optional)
    ensure_column(con, "report", "error_message", "ALTER TABLE report ADD COLUMN error_message TEXT;")
    ensure_column(con, "report", "trigger_type", "ALTER TABLE report ADD COLUMN trigger_type TEXT;")
    ensure_column(con, "report", "started_at", "ALTER TABLE report ADD COLUMN started_at DATETIME;")
    ensure_column(con, "report", "finished_at", "ALTER TABLE report ADD COLUMN finished_at DATETIME;")


def pick_ids(con: sqlite3.Connection) -> tuple[list[int], list[int]]:
    brand_ids = [int(r[0]) for r in con.execute("SELECT id FROM brand ORDER BY id LIMIT 2;").fetchall()]
    platform_ids = [int(r[0]) for r in con.execute("SELECT id FROM platform WHERE is_enabled=1 ORDER BY id LIMIT 2;").fetchall()]
    if not platform_ids:
        platform_ids = [int(r[0]) for r in con.execute("SELECT id FROM platform ORDER BY id LIMIT 2;").fetchall()]
    if not brand_ids or not platform_ids:
        raise RuntimeError("seed DB missing brand/platform rows")
    return brand_ids, platform_ids


def count(con: sqlite3.Connection, sql: str, params: tuple = ()) -> int:
    return int(con.execute(sql, params).fetchone()[0] or 0)


def main() -> int:
    parser = argparse.ArgumentParser(description="ProdWatch minimal backend self-test")
    parser.add_argument("--template-db", default=str(REPO_ROOT / "backend/database/database..sqlite"))
    parser.add_argument("--out-db", default=str(REPO_ROOT / "backend/database/selftest.sqlite"))
    parser.add_argument("--stat-date", default="2026-03-31", help="YYYY-MM-DD")
    parser.add_argument("--posts-per-target", type=int, default=2)
    args = parser.parse_args()

    template_db = Path(args.template_db)
    out_db = Path(args.out_db)
    if not template_db.exists():
        raise SystemExit(f"template db not found: {template_db}")

    prepare_test_db(template_db, out_db)

    con = connect(str(out_db))
    try:
        with con:
            apply_minimal_migrations(con)

        brand_ids, platform_ids = pick_ids(con)

        # Step 1+2: create project with scope config
        payload = ProjectPayload(
            name="SelfTest Project",
            product_category="selftest",
            description="created by minimal_backend_selftest.py",
            our_brand_id=brand_ids[0],
            status=None,
            is_active=0,
            refresh_mode="manual",
            refresh_cron=None,
            brand_ids=brand_ids,
            platform_ids=platform_ids,
            keywords=[
                KeywordItem(keyword="battery", keyword_type="feature", weight=10, is_enabled=1),
                KeywordItem(keyword="demo", keyword_type="topic", weight=5, is_enabled=1),
            ],
        )
        with con:
            created = create_project(payload, db=con)
        project_id = int(created["project_id"])

        # Step 2 (explicit): update config before activation (idempotent)
        payload2 = ProjectPayload(
            name="SelfTest Project",
            product_category="selftest",
            description="updated by minimal_backend_selftest.py",
            our_brand_id=brand_ids[0],
            status="inactive",
            is_active=0,
            refresh_mode="manual",
            refresh_cron=None,
            brand_ids=brand_ids,
            platform_ids=platform_ids,
            keywords=[
                KeywordItem(keyword="battery", keyword_type="feature", weight=10, is_enabled=1),
                KeywordItem(keyword="demo", keyword_type="topic", weight=6, is_enabled=1),
                KeywordItem(keyword="lag", keyword_type="feature", weight=3, is_enabled=1),
            ],
        )
        with con:
            _ = update_project(project_id, payload2, db=con)

        # Step 3: activate project
        with con:
            _ = set_project_activation(project_id, ActivationPayload(is_active=1, status="active"), db=con)

        # Step 4: manual refresh
        before_posts = count(con, "SELECT COUNT(*) FROM post_raw WHERE project_id=?;", (project_id,))
        with con:
            refresh_res = manual_refresh_project(
                project_id,
                ManualRefreshPayload(stat_date=args.stat_date, posts_per_target=int(args.posts_per_target), created_by="selftest"),
                db=con,
            )
        crawl_job_id = int(refresh_res["crawl_job_id"])
        after_posts = count(con, "SELECT COUNT(*) FROM post_raw WHERE project_id=?;", (project_id,))
        new_posts = after_posts - before_posts
        if new_posts <= 0:
            raise AssertionError("post_raw not increased after refresh")

        # Step 5: verify post_raw by crawl_job_id
        job_posts = count(con, "SELECT COUNT(*) FROM post_raw WHERE crawl_job_id=?;", (crawl_job_id,))
        if job_posts <= 0:
            raise AssertionError("no post_raw rows for crawl_job_id")

        # Step 6: verify analysis results for job posts
        params = (crawl_job_id,)
        clean_cnt = count(
            con,
            "SELECT COUNT(*) FROM post_clean_result WHERE post_id IN (SELECT id FROM post_raw WHERE crawl_job_id=?);",
            params,
        )
        sent_cnt = count(
            con,
            "SELECT COUNT(*) FROM post_sentiment_result WHERE post_id IN (SELECT id FROM post_raw WHERE crawl_job_id=?);",
            params,
        )
        kw_cnt = count(
            con,
            "SELECT COUNT(*) FROM post_keyword_result WHERE post_id IN (SELECT id FROM post_raw WHERE crawl_job_id=?);",
            params,
        )
        feat_cnt = count(
            con,
            "SELECT COUNT(*) FROM post_feature_result WHERE post_id IN (SELECT id FROM post_raw WHERE crawl_job_id=?);",
            params,
        )
        spam_cnt = count(
            con,
            "SELECT COUNT(*) FROM post_spam_result WHERE post_id IN (SELECT id FROM post_raw WHERE crawl_job_id=?);",
            params,
        )
        if min(clean_cnt, sent_cnt, kw_cnt, feat_cnt, spam_cnt) <= 0:
            raise AssertionError("analysis tables missing rows for job posts")

        # Step 7: verify aggregated daily tables
        daily_metric_cnt = count(
            con,
            "SELECT COUNT(*) FROM daily_metric WHERE project_id=? AND stat_date=?;",
            (project_id, args.stat_date),
        )
        daily_kw_cnt = count(
            con,
            "SELECT COUNT(*) FROM daily_keyword_metric WHERE project_id=? AND stat_date=?;",
            (project_id, args.stat_date),
        )
        daily_feat_cnt = count(
            con,
            "SELECT COUNT(*) FROM daily_feature_metric WHERE project_id=? AND stat_date=?;",
            (project_id, args.stat_date),
        )
        if min(daily_metric_cnt, daily_kw_cnt, daily_feat_cnt) <= 0:
            raise AssertionError("daily_* tables missing rows")

        # Step 8: create report (will generate immediately, mock template)
        with con:
            report_res = create_report(
                CreateReportRequest(
                    project_id=project_id,
                    title="SelfTest Report",
                    report_type="daily",
                    data_start_date=args.stat_date,
                    data_end_date=args.stat_date,
                    platform_ids=None,
                    brand_ids=None,
                    keywords=None,
                    include_sentiment=True,
                    include_trend=True,
                    include_topics=True,
                    include_feature_analysis=True,
                    include_spam=True,
                    include_competitor_compare=True,
                    include_strategy=True,
                ),
                db=con,
            )
        report_id = int(report_res["report_id"])

        # Step 9: verify report content and evidence
        rep = con.execute(
            "SELECT id,status,summary,content_markdown FROM report WHERE id=?;",
            (report_id,),
        ).fetchone()
        if rep is None:
            raise AssertionError("report row missing")
        if str(rep["status"]) not in {"success", "done"}:
            raise AssertionError(f"report not success: status={rep['status']}")
        if not (rep["content_markdown"] and str(rep["content_markdown"]).strip()):
            raise AssertionError("report.content_markdown empty")

        ev_cnt = count(con, "SELECT COUNT(*) FROM report_evidence WHERE report_id=?;", (report_id,))
        if ev_cnt <= 0:
            raise AssertionError("report_evidence empty")
        missing_post = count(
            con,
            """
            SELECT COUNT(*)
            FROM report_evidence re
            LEFT JOIN post_raw pr ON pr.id = re.post_id
            WHERE re.report_id=? AND pr.id IS NULL;
            """,
            (report_id,),
        )
        if missing_post != 0:
            raise AssertionError("report_evidence contains non-existent post_id")

        # Summary output (easy to grep in CI / manual run)
        print(
            {
                "ok": True,
                "db": str(out_db),
                "project_id": project_id,
                "crawl_job_id": crawl_job_id,
                "counts": {
                    "post_raw_new": int(new_posts),
                    "post_raw_for_job": int(job_posts),
                    "clean": int(clean_cnt),
                    "sentiment": int(sent_cnt),
                    "keyword": int(kw_cnt),
                    "feature": int(feat_cnt),
                    "spam": int(spam_cnt),
                    "daily_metric": int(daily_metric_cnt),
                    "daily_keyword_metric": int(daily_kw_cnt),
                    "daily_feature_metric": int(daily_feat_cnt),
                    "report_id": int(report_id),
                    "report_evidence": int(ev_cnt),
                },
                "notes": {
                    "analysis": "mock/rule-based",
                    "report_generation": "mock/template-based",
                },
            }
        )
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())


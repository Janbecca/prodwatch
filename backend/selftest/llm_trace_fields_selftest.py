from __future__ import annotations

"""
Self-test: verify analysis/report tables record provider/model/prompt_version after migrations.

This test:
- Copies seed DB
- Applies minimal ALTER TABLE statements (best-effort) for trace fields
- Runs one refresh (so analysis tables get rows)
- Creates one report (so report gets trace fields)
- Asserts the new columns are populated (provider_name/prompt_version at least)
"""

import shutil
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from backend.api.db import connect  # noqa: E402
from backend.services.refresh_service import get_refresh_service  # noqa: E402
from backend.api.routes_reports import CreateReportRequest, create_report  # noqa: E402


def ensure_column(con, table: str, col: str, ddl: str) -> None:
    cols = {r[1] for r in con.execute(f"PRAGMA table_info({table});").fetchall()}
    if col in cols:
        return
    con.execute(ddl)


def main() -> int:
    template = REPO_ROOT / "backend/database/database..sqlite"
    out = REPO_ROOT / "backend/database/llm_trace_fields_selftest.sqlite"
    shutil.copyfile(str(template), str(out))

    con = connect(str(out))
    try:
        # Apply minimal schema updates for this test DB copy.
        for t in ["post_clean_result", "post_sentiment_result", "post_keyword_result", "post_feature_result", "post_spam_result"]:
            ensure_column(con, t, "provider_name", f"ALTER TABLE {t} ADD COLUMN provider_name TEXT;")
            ensure_column(con, t, "model_name", f"ALTER TABLE {t} ADD COLUMN model_name TEXT;")
            ensure_column(con, t, "prompt_version", f"ALTER TABLE {t} ADD COLUMN prompt_version TEXT;")
            ensure_column(con, t, "generated_at", f"ALTER TABLE {t} ADD COLUMN generated_at DATETIME;")
            ensure_column(con, t, "raw_response", f"ALTER TABLE {t} ADD COLUMN raw_response TEXT;")
            ensure_column(con, t, "error_message", f"ALTER TABLE {t} ADD COLUMN error_message TEXT;")

        ensure_column(con, "report", "provider_name", "ALTER TABLE report ADD COLUMN provider_name TEXT;")
        ensure_column(con, "report", "model_name", "ALTER TABLE report ADD COLUMN model_name TEXT;")
        ensure_column(con, "report", "prompt_version", "ALTER TABLE report ADD COLUMN prompt_version TEXT;")
        ensure_column(con, "report", "generated_at", "ALTER TABLE report ADD COLUMN generated_at DATETIME;")
        ensure_column(con, "report", "raw_response", "ALTER TABLE report ADD COLUMN raw_response TEXT;")
        ensure_column(con, "report", "error_message", "ALTER TABLE report ADD COLUMN error_message TEXT;")
        con.commit()

        pid = int(con.execute("SELECT id FROM project WHERE deleted_at IS NULL ORDER BY id LIMIT 1;").fetchone()[0])
        con.execute("UPDATE project SET is_active=1, refresh_mode='daily' WHERE id=?;", (pid,))
        con.commit()

        svc = get_refresh_service()
        with con:
            rr = svc.refresh_project_sync(con=con, project_id=pid, stat_date="2026-03-31", posts_per_target=1, trigger="manual", created_by="selftest")
        if rr.error_message:
            raise AssertionError(rr.error_message)
        job_id = int(rr.crawl_job_id or 0)

        # pick one post from this job
        post_id = int(con.execute("SELECT id FROM post_raw WHERE crawl_job_id=? ORDER BY id LIMIT 1;", (job_id,)).fetchone()[0])

        def must_have(table: str) -> dict:
            r = con.execute(
                f"SELECT provider_name, model_name, prompt_version, generated_at, error_message FROM {table} WHERE post_id=? LIMIT 1;",
                (post_id,),
            ).fetchone()
            if not r:
                raise AssertionError(f"missing row in {table}")
            if not (r["provider_name"] or ""):
                raise AssertionError(f"{table}.provider_name empty")
            if not (r["prompt_version"] or "") and table != "post_clean_result":
                raise AssertionError(f"{table}.prompt_version empty")
            return dict(r)

        out_tables = {
            "clean": must_have("post_clean_result"),
            "sentiment": must_have("post_sentiment_result"),
            "spam": must_have("post_spam_result"),
            "keyword": must_have("post_keyword_result"),
            "feature": must_have("post_feature_result"),
        }

        with con:
            rep = create_report(
                CreateReportRequest(
                    project_id=pid,
                    title="LLM trace selftest",
                    report_type="daily",
                    data_start_date="2026-03-31",
                    data_end_date="2026-03-31",
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
        rid = int(rep["report_id"])
        rr2 = con.execute(
            "SELECT status, provider_name, model_name, prompt_version, generated_at, length(raw_response) AS raw_len FROM report WHERE id=?;",
            (rid,),
        ).fetchone()
        if not rr2 or str(rr2["status"]) not in {"success", "done"}:
            raise AssertionError("report not success")
        if not rr2["provider_name"] or not rr2["prompt_version"]:
            raise AssertionError("report trace fields missing")

        print({"ok": True, "db": str(out), "post_id": post_id, "analysis": out_tables, "report": dict(rr2)})
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())


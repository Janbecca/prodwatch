from __future__ import annotations

"""
Self-test: prompt template loading + prompt_version recording in llm_call_log.

This test:
- Creates a DB copy
- Creates llm_call_log table
- Runs one refresh (triggers crawler_generation + sentiment/keyword/feature/spam tasks)
- Creates one report (triggers report_generation task)
- Asserts llm_call_log has prompt_version recorded for each task_type
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


def main() -> int:
    template = REPO_ROOT / "backend/database/database..sqlite"
    out = REPO_ROOT / "backend/database/prompt_version_selftest.sqlite"
    shutil.copyfile(str(template), str(out))

    con = connect(str(out))
    try:
        # create llm_call_log if not migrated
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS llm_call_log (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              task_type TEXT NOT NULL,
              provider TEXT NOT NULL,
              model TEXT,
              prompt_version TEXT,
              ok INTEGER NOT NULL,
              error_message TEXT,
              request_json TEXT,
              response_json TEXT,
              created_at DATETIME
            );
            """
        )
        con.commit()

        pid = int(con.execute("SELECT id FROM project WHERE deleted_at IS NULL ORDER BY id LIMIT 1;").fetchone()[0])
        con.execute("UPDATE project SET is_active=1, refresh_mode='daily' WHERE id=?;", (pid,))
        con.commit()

        svc = get_refresh_service()
        with con:
            rr = svc.refresh_project_sync(
                con=con,
                project_id=pid,
                stat_date="2026-03-31",
                posts_per_target=1,
                trigger="scheduled",
                created_by="scheduler",
            )
        if rr.error_message:
            raise AssertionError(rr.error_message)

        with con:
            rep = create_report(
                CreateReportRequest(
                    project_id=pid,
                    title="Prompt version selftest",
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

        rows = con.execute(
            """
            SELECT task_type, COUNT(*) AS cnt, MIN(prompt_version) AS min_v, MAX(prompt_version) AS max_v
            FROM llm_call_log
            GROUP BY task_type
            ORDER BY task_type;
            """
        ).fetchall()
        summary = {str(r["task_type"]): {"cnt": int(r["cnt"]), "min_v": r["min_v"], "max_v": r["max_v"]} for r in rows}

        required = {
            "crawler_generation",
            "sentiment_analysis",
            "keyword_extraction",
            "feature_extraction",
            "spam_detection",
            "report_generation",
        }
        missing = [t for t in sorted(required) if t not in summary]
        if missing:
            raise AssertionError(f"missing llm_call_log task_types: {missing}. got={list(summary.keys())}")
        for t in required:
            if not summary[t]["min_v"]:
                raise AssertionError(f"missing prompt_version for {t}")

        print({"ok": True, "db": str(out), "llm_call_log": summary})
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())


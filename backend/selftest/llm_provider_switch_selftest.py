from __future__ import annotations

"""
Self-test: verify per-task provider switching + fallback.

This does not call real DeepSeek/Qwen APIs (providers are stubs). Instead it verifies:
- Router chooses configured provider per task_type
- When provider fails, it falls back to MockProvider
- Business logic (refresh + report generation) still runs and writes to DB

Usage:
  .\\.venv\\Scripts\\python.exe backend/selftest/llm_provider_switch_selftest.py
"""

import os
import shutil
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from backend.api.db import connect  # noqa: E402
from backend.llm.config_store import get_llm_config_store  # noqa: E402
from backend.llm.types import LLMTaskConfig  # noqa: E402
from backend.services.refresh_service import get_refresh_service  # noqa: E402
from backend.api.routes_reports import CreateReportRequest, create_report  # noqa: E402


def main() -> int:
    template = REPO_ROOT / "backend/database/database..sqlite"
    out = REPO_ROOT / "backend/database/llm_switch_selftest.sqlite"
    shutil.copyfile(str(template), str(out))

    con = connect(str(out))
    try:
        # Create config table if you didn't run migrations in this DB.
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS llm_task_config (
              task_type TEXT PRIMARY KEY,
              provider TEXT NOT NULL,
              model TEXT,
              fallback_provider TEXT NOT NULL DEFAULT 'mock',
              fallback_model TEXT,
              updated_at DATETIME
            );
            """
        )
        # Force some tasks to use deepseek/qwen stubs -> should fallback to mock.
        store = get_llm_config_store()
        store.upsert(con, LLMTaskConfig(task_type="sentiment_analysis", provider="deepseek", model="deepseek-chat", fallback_provider="mock"))
        store.upsert(con, LLMTaskConfig(task_type="report_generation", provider="qwen", model="qwen-max", fallback_provider="mock"))
        store.upsert(con, LLMTaskConfig(task_type="crawler_generation", provider="deepseek", model="deepseek-chat", fallback_provider="mock"))
        con.commit()

        # Ensure at least one daily active project and run one refresh to exercise crawler+analysis tasks.
        pid = int(con.execute("SELECT id FROM project WHERE deleted_at IS NULL ORDER BY id LIMIT 1;").fetchone()[0])
        con.execute("UPDATE project SET is_active=1, refresh_mode='daily' WHERE id=?;", (pid,))
        con.commit()

        svc = get_refresh_service()
        with con:
            rr = svc.refresh_project_sync(con=con, project_id=pid, stat_date="2026-03-31", posts_per_target=1, trigger="scheduled", created_by="scheduler")
        if rr.error_message:
            raise AssertionError(rr.error_message)

        # Create a report (will generate via router -> qwen stub -> fallback to mock).
        with con:
            rep = create_report(
                CreateReportRequest(
                    project_id=pid,
                    title="LLM switch selftest",
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
        row = con.execute("SELECT status, content_markdown FROM report WHERE id=?;", (rid,)).fetchone()
        if not row or str(row["status"]) not in {"success", "done"}:
            raise AssertionError("report not success")
        md = str(row["content_markdown"] or "")
        if "<!-- generator: mock/mock-v1 -->" not in md:
            raise AssertionError("expected mock generator marker in markdown (fallback not applied?)")

        print({"ok": True, "db": str(out), "project_id": pid, "crawl_job_id": rr.crawl_job_id, "report_id": rid})
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())


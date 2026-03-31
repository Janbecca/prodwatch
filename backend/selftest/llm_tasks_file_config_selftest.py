from __future__ import annotations

"""
自测：通过 llm_tasks 配置文件验证每任务切换。

此测试不调用真实的 Qwen/DeepSeek API（提供者为存根）。它验证：
- 路由器读取 PRODWATCH_LLM_CONFIG_PATH（YAML-lite）
- 对于每个 task_type，第一次尝试使用配置的提供者（ok 可能为 0）
- 当存根失败时，路由器回退到 MockProvider 并且链仍然成功

用法：
  $env:PRODWATCH_LLM_CONFIG_PATH='backend/llm/tasks_config.example.yml'
  ..venvScriptspython.exe backend/selftest/llm_tasks_file_config_selftest.py
"""

import os
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
    cfg_path = os.environ.get("PRODWATCH_LLM_CONFIG_PATH")
    if not cfg_path:
        os.environ["PRODWATCH_LLM_CONFIG_PATH"] = "backend/llm/tasks_config.example.yml"

    template = REPO_ROOT / "backend/database/database..sqlite"
    out = REPO_ROOT / "backend/database/llm_tasks_file_config_selftest.sqlite"
    shutil.copyfile(str(template), str(out))

    con = connect(str(out))
    try:
        # create llm_call_log for assertions
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
            rr = svc.refresh_project_sync(con=con, project_id=pid, stat_date="2026-03-31", posts_per_target=1, trigger="manual", created_by="selftest")
        if rr.error_message:
            raise AssertionError(rr.error_message)

        with con:
            rep = create_report(
                CreateReportRequest(
                    project_id=pid,
                    title="llm_tasks file config selftest",
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

        # For stub providers (qwen/deepseek) we expect at least one failed attempt logged before fallback.
        # For mock tasks, ok should be 1 with provider=mock.
        rows = con.execute(
            """
            SELECT task_type,
                   SUM(CASE WHEN ok=1 THEN 1 ELSE 0 END) AS ok_cnt,
                   SUM(CASE WHEN ok=0 THEN 1 ELSE 0 END) AS fail_cnt,
                   GROUP_CONCAT(DISTINCT provider) AS providers
            FROM llm_call_log
            GROUP BY task_type;
            """
        ).fetchall()
        summary = {str(r["task_type"]): dict(r) for r in rows}

        # Must have at least these tasks invoked
        required = {"crawler_generation", "sentiment_analysis", "keyword_extraction", "feature_extraction", "spam_detection", "report_generation"}
        missing = [t for t in sorted(required) if t not in summary]
        if missing:
            raise AssertionError(f"missing tasks in llm_call_log: {missing}")

        # crawler_generation configured as mock -> should succeed with provider=mock
        if int(summary["crawler_generation"]["ok_cnt"] or 0) <= 0 or "mock" not in str(summary["crawler_generation"]["providers"] or ""):
            raise AssertionError("crawler_generation did not use mock successfully")

        # report_generation configured as deepseek -> stub fails then fallback mock should succeed
        if int(summary["report_generation"]["fail_cnt"] or 0) <= 0:
            raise AssertionError("expected report_generation failure attempts (deepseek stub)")
        if int(summary["report_generation"]["ok_cnt"] or 0) <= 0:
            raise AssertionError("expected report_generation fallback success")

        print({"ok": True, "db": str(out), "llm_call_log_summary": {k: {"providers": v["providers"], "ok": int(v["ok_cnt"]), "fail": int(v["fail_cnt"])} for k, v in summary.items()}})
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())


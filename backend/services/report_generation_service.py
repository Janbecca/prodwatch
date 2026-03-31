from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any, Optional

from backend.report_chain_e import (
    fetch_agg_overview,
    fetch_candidate_posts,
    fetch_competitor_compare,
    fetch_sentiment_trend,
    fetch_top_keywords,
    fetch_top_negative_features,
    llm_mock_generate_markdown,
    read_report,
    read_report_config,
    select_and_write_evidence,
    set_report_status,
    update_report_content,
)


@dataclass(frozen=True)
class ReportGenerationInput:
    """
    Structured input for the report generator.

    Keep this stable so future LLM-based generators can be swapped in without changing the pipeline.
    """

    report: sqlite3.Row
    overview: dict[str, Any]
    trend: list[dict[str, Any]]
    top_keywords: list[dict[str, Any]]
    top_features: list[dict[str, Any]]
    competitor: list[dict[str, Any]]
    posts: dict[str, list[sqlite3.Row]]


class ReportGenerationService:
    """
    Synchronous report generation service (demo/dev).

    - Uses aggregated tables + joined post details as inputs.
    - Generates structured markdown via mock generator (rule/template).
    - Selects evidence strictly from existing DB posts (report_evidence.post_id FK).
    """

    def __init__(self, *, generator_name: str = "mock-v1"):
        self.generator_name = generator_name

    def generate_sync(self, con: sqlite3.Connection, report_id: int, *, force: bool = False) -> dict[str, Any]:
        row = con.execute(
            "SELECT id, status FROM report WHERE id=? LIMIT 1;",
            (int(report_id),),
        ).fetchone()
        if row is None:
            raise ValueError("report not found")

        status = str(row["status"] or "")
        if status == "running":
            raise RuntimeError("report is running")
        if status in {"success", "done"} and not force:
            raise RuntimeError("report already success")

        self._mark_running(con, int(report_id))

        report = read_report(con, int(report_id))
        cfg = read_report_config(con, int(report_id))

        project_id = int(report["project_id"])
        start = str(report["data_start_date"])
        end = str(report["data_end_date"])

        overview = fetch_agg_overview(con, project_id, start, end, cfg)
        trend = fetch_sentiment_trend(con, project_id, start, end, cfg) if cfg.include_trend else []
        top_keywords = fetch_top_keywords(con, project_id, start, end, cfg) if cfg.include_topics else []
        top_features = (
            fetch_top_negative_features(con, project_id, start, end, cfg) if cfg.include_feature_analysis else []
        )
        competitor = (
            fetch_competitor_compare(con, project_id, start, end, cfg) if cfg.include_competitor_compare else []
        )
        posts = fetch_candidate_posts(con, project_id, start, end, cfg, limit_each=6)

        gen_input = ReportGenerationInput(
            report=report,
            overview=overview,
            trend=trend,
            top_keywords=top_keywords,
            top_features=top_features,
            competitor=competitor,
            posts=posts,
        )

        summary, content_md = self._generate_markdown(gen_input)

        update_report_content(con, int(report_id), summary, content_md)
        # Evidence must come from real posts (post_raw) selected from `posts` query results.
        select_and_write_evidence(con, int(report_id), posts)
        self._mark_finished(con, int(report_id), status="success", error_message=None)
        return {"report_id": int(report_id), "status": "success"}

    def mark_failed(self, con: sqlite3.Connection, report_id: int, error_message: str) -> None:
        self._mark_finished(con, int(report_id), status="failed", error_message=error_message)

    def _mark_running(self, con: sqlite3.Connection, report_id: int) -> None:
        # Clear previous failure message and set running + started_at when possible.
        try:
            con.execute(
                """
                UPDATE report
                SET status='running',
                    started_at=COALESCE(started_at, datetime('now','localtime')),
                    trigger_type=COALESCE(trigger_type, 'manual'),
                    error_message=NULL,
                    updated_at=datetime('now','localtime')
                WHERE id=?;
                """,
                (int(report_id),),
            )
            return
        except sqlite3.OperationalError as e:
            # Backward DB without these columns.
            msg = str(e).lower()
            if "no such column" not in msg and "has no column named" not in msg:
                raise
        set_report_status(con, int(report_id), "running")
        # Best-effort: clear error_message if present.
        try:
            con.execute("UPDATE report SET error_message=NULL WHERE id=?;", (int(report_id),))
        except sqlite3.OperationalError:
            pass

    def _mark_finished(self, con: sqlite3.Connection, report_id: int, *, status: str, error_message: str | None) -> None:
        msg = (str(error_message or "")[:1000]) if error_message else None
        try:
            con.execute(
                """
                UPDATE report
                SET status=?,
                    finished_at=datetime('now','localtime'),
                    error_message=?,
                    updated_at=datetime('now','localtime')
                WHERE id=?;
                """,
                (str(status), msg, int(report_id)),
            )
            return
        except sqlite3.OperationalError as e:
            m = str(e).lower()
            if "no such column" not in m and "has no column named" not in m:
                raise
        # Fallback: only status + updated_at.
        set_report_status(con, int(report_id), str(status))
        try:
            con.execute(
                "UPDATE report SET error_message=?, updated_at=datetime('now','localtime') WHERE id=?;",
                (msg, int(report_id)),
            )
        except sqlite3.OperationalError:
            pass

    def _generate_markdown(self, input_: ReportGenerationInput) -> tuple[str, str]:
        # Reuse existing mock generator implementation.
        return llm_mock_generate_markdown(
            report=input_.report,
            overview=input_.overview,
            trend=input_.trend,
            top_keywords=input_.top_keywords,
            top_features=input_.top_features,
            competitor=input_.competitor,
            posts=input_.posts,
        )


_default_service: Optional[ReportGenerationService] = None


def get_report_generation_service() -> ReportGenerationService:
    global _default_service
    if _default_service is None:
        _default_service = ReportGenerationService()
    return _default_service

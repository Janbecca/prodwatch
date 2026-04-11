# 作用：后端服务层：报告生成相关业务逻辑封装。

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from typing import Any, Optional

from backend.llm.router import get_llm_router
from backend.report_chain_e import (
    fetch_agg_overview,
    fetch_candidate_posts,
    fetch_competitor_compare,
    fetch_sentiment_trend,
    fetch_top_topics,
    fetch_top_negative_features,
    llm_mock_generate_markdown,
    read_report,
    read_report_config,
    select_and_write_evidence,
    set_report_status,
    update_report_content,
)

log = logging.getLogger("prodwatch.report_generation")


@dataclass(frozen=True)
class ReportGenerationInput:
    """
    Structured input for the report generator.

    Keep this stable so future LLM-based generators can be swapped in without changing the pipeline.
    """

    report: sqlite3.Row
    overview: dict[str, Any]
    trend: list[dict[str, Any]]
    top_topics: list[dict[str, Any]]
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
        top_topics = fetch_top_topics(con, project_id, start, end, cfg) if cfg.include_topics else []
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
            top_topics=top_topics,
            top_features=top_features,
            competitor=competitor,
            posts=posts,
        )

        # 1) Generate stable skeleton via existing template (conservative, deterministic).
        summary, content_md = self._generate_markdown(gen_input)

        # 2) Generate incremental LLM blocks (summary/strategy add-ons) and merge into skeleton.
        try:
            blocks = self._generate_llm_blocks(con, gen_input, cfg)
            summary, content_md = self._merge_llm_blocks(summary, content_md, blocks)
        except Exception:
            # Never break report generation chain due to LLM issues.
            blocks = None
        if isinstance(blocks, dict) and any(str(blocks.get(k) or "").strip() for k in ["executive_summary_md", "strategy_suggestions_md"]):
            log.info("report_generation ai_blocks_inserted report_id=%s", int(report_id))

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
            top_topics=input_.top_topics,
            top_features=input_.top_features,
            competitor=input_.competitor,
            posts=input_.posts,
        )

    def _generate_llm_blocks(self, con: sqlite3.Connection, input_: ReportGenerationInput, cfg: Any) -> dict[str, Any]:
        """
        Conservative LLM usage:
        - Only generate incremental blocks (executive summary add-ons + strategy add-ons)
        - Do not let LLM control the full markdown structure
        """
        report = input_.report
        payload = {
            "report": {
                "id": int(report["id"]),
                "project_id": int(report["project_id"]),
                "title": str(report["title"] or ""),
                "report_type": str(report["report_type"] or ""),
                "data_start_date": str(report["data_start_date"] or ""),
                "data_end_date": str(report["data_end_date"] or ""),
            },
            "config": {
                "include_sentiment": int(getattr(cfg, "include_sentiment", 0)),
                "include_trend": int(getattr(cfg, "include_trend", 0)),
                "include_topics": int(getattr(cfg, "include_topics", 0)),
                "include_feature_analysis": int(getattr(cfg, "include_feature_analysis", 0)),
                "include_spam": int(getattr(cfg, "include_spam", 0)),
                "include_competitor_compare": int(getattr(cfg, "include_competitor_compare", 0)),
                "include_strategy": int(getattr(cfg, "include_strategy", 0)),
            },
            "overview": input_.overview or {},
            "trend": (input_.trend or [])[:14],
            "top_topics": (input_.top_topics or [])[:12],
            "top_features": (input_.top_features or [])[:12],
            "competitor": (input_.competitor or [])[:12],
            "post_excerpts": self._post_excerpts(input_.posts or {}, limit_each=3, max_chars=220),
        }

        res = get_llm_router().run(task_type="report_generation", input=payload, con=con)
        log.info(
            "report_generation llm_blocks task_type=report_generation provider=%s model=%s ok=%s",
            getattr(res, "provider", None),
            getattr(res, "model", None),
            bool(getattr(res, "ok", False)),
        )
        out = res.output if isinstance(res.output, dict) else {}
        if not out:
            return {}
        return out

    def _post_excerpts(self, posts: dict[str, list[sqlite3.Row]], *, limit_each: int, max_chars: int) -> dict[str, list[dict[str, Any]]]:
        out: dict[str, list[dict[str, Any]]] = {}
        for k, rows in (posts or {}).items():
            items: list[dict[str, Any]] = []
            for r in (rows or [])[: int(limit_each)]:
                try:
                    content = str(r["content"] or "").strip().replace("\n", " ")
                except Exception:
                    content = ""
                if len(content) > int(max_chars):
                    content = content[: int(max_chars)] + "..."
                items.append(
                    {
                        "post_id": int(r["post_id"]) if "post_id" in r.keys() else None,
                        "platform_id": int(r["platform_id"]) if "platform_id" in r.keys() and r["platform_id"] is not None else None,
                        "sentiment": str(r["sentiment"] or "") if "sentiment" in r.keys() else "",
                        "like_count": int(r["like_count"] or 0) if "like_count" in r.keys() else 0,
                        "title": str(r["title"] or "") if "title" in r.keys() else "",
                        "content_excerpt": content,
                    }
                )
            out[str(k)] = items
        return out

    def _merge_llm_blocks(self, summary: str, content_md: str, blocks: dict[str, Any]) -> tuple[str, str]:
        if not isinstance(blocks, dict):
            return summary, content_md

        new_summary = self._to_text(blocks.get("summary")).strip()
        exec_md = self._normalize_md_bullets(blocks.get("executive_summary_md")).strip()
        strat_md = self._normalize_md_bullets(blocks.get("strategy_suggestions_md")).strip()

        merged_summary = new_summary or summary
        merged_md = str(content_md or "")

        if exec_md:
            merged_md = self._insert_under_heading(merged_md, "## 执行摘要", "### AI 补充", exec_md)
        if strat_md:
            merged_md = self._insert_under_heading(merged_md, "## 策略建议", "### AI 补充", strat_md)

        return merged_summary, merged_md

    def _to_text(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, (str, int, float, bool)):
            return str(value)
        # Avoid dumping large structures into report body.
        try:
            import json

            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)

    def _normalize_md_bullets(self, value: Any) -> str:
        """
        Normalize LLM block values into a markdown bullet-list string.

        Why:
        - Some providers/models may return arrays for bullet blocks.
        - Converting arrays with `str()` would render as Python/JSON list (e.g. "['- ...']"),
          which looks broken in the report.
        """
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        if isinstance(value, list):
            lines: list[str] = []
            for it in value:
                s = self._to_text(it).strip()
                if not s:
                    continue
                # Ensure each line starts with "- "
                if s.startswith("- "):
                    lines.append(s)
                elif s.startswith("-"):
                    lines.append("- " + s.lstrip("-").strip())
                else:
                    lines.append("- " + s)
            return "\n".join(lines)
        # Fallback: stringify unknown types, but try to keep it single-line.
        return self._to_text(value)

    def _insert_under_heading(self, md: str, heading: str, subheading: str, block_md: str) -> str:
        """
        Insert a markdown block right after a known heading (best-effort).
        If heading is not found, append to the end.
        """
        text = str(md or "")
        h = str(heading)
        idx = text.find(h)
        block = "\n".join(["", subheading, block_md, ""])
        if idx < 0:
            return text.rstrip() + block + "\n"
        # insert after heading line
        end = text.find("\n", idx)
        if end < 0:
            return text.rstrip() + block + "\n"
        return text[: end + 1] + block + text[end + 1 :]


_default_service: Optional[ReportGenerationService] = None


def get_report_generation_service() -> ReportGenerationService:
    global _default_service
    if _default_service is None:
        _default_service = ReportGenerationService()
    return _default_service

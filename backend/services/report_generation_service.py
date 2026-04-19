# 作用：后端服务层：报告生成相关业务逻辑封装。

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from typing import Any, Optional
import re
import json

from backend.llm.router import get_llm_router
from backend.report_chain_e import (
    fetch_evidence_key_feedback_posts,
    fetch_evidence_overview_by_brand,
    fetch_evidence_risk_keywords,
    fetch_evidence_sentiment_trend_daily_by_brand,
    fetch_evidence_topic_monitor_stacked,
    materialize_report_dataset_evidence,
    read_report,
    read_report_config,
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
    overview_items: list[dict[str, Any]]
    trend: dict[str, Any]
    topics: dict[str, Any]
    risk_keywords: list[dict[str, Any]]
    key_user_feedback: list[dict[str, Any]]
    post_excerpts: dict[str, list[dict[str, Any]]]


class ReportGenerationService:
    """
    Synchronous report generation service (demo/dev).

    - Uses aggregated tables + joined post details as inputs.
    - Generates structured markdown via mock generator (rule/template).
    - Selects evidence strictly from existing DB posts (report_evidence.post_id FK).
    """

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

        # 0) Materialize dataset evidence first: all matching posts must be stored as evidence.
        materialize_report_dataset_evidence(con, int(report_id))

        # 1) Compute all boards strictly from `report_evidence` dataset (no dashboard aggregates).
        overview_items = fetch_evidence_overview_by_brand(con, int(report_id))
        overview = self._overview_rollup(overview_items)
        trend = (
            fetch_evidence_sentiment_trend_daily_by_brand(con, int(report_id), top_n=4)
            if int(getattr(cfg, "include_trend", 0))
            else {"dates": [], "series": []}
        )
        topics = (
            fetch_evidence_topic_monitor_stacked(con, int(report_id), top_n=15)
            if int(getattr(cfg, "include_topics", 0))
            else {"dates": [], "series": []}
        )
        risk_keywords = (
            fetch_evidence_risk_keywords(con, int(report_id), top_n=20)
            if int(getattr(cfg, "include_feature_analysis", 0))
            else []
        )
        key_user_feedback = (
            fetch_evidence_key_feedback_posts(con, int(report_id), limit=20)
            if int(getattr(cfg, "include_spam", 0))
            else []
        )
        post_excerpts = self._post_excerpts_for_llm(con, int(report_id), max_chars=220)

        gen_input = ReportGenerationInput(
            report=report,
            overview=overview,
            overview_items=overview_items,
            trend=trend,
            topics=topics,
            risk_keywords=risk_keywords,
            key_user_feedback=key_user_feedback,
            post_excerpts=post_excerpts,
        )

        # 2) Generate stable skeleton (structure only; charts/boards are rendered by frontend).
        summary, content_md = self._generate_skeleton_markdown(gen_input, cfg)

        # 3) Generate LLM text blocks (must be real; surface errors rather than silently outputting empty summaries).
        blocks = self._generate_llm_blocks(con, gen_input, cfg)
        summary, content_md = self._merge_llm_blocks(summary, content_md, blocks, cfg)
        if isinstance(blocks, dict):
            has_exec = bool(str(blocks.get("executive_summary_md") or "").strip())
            has_strat = bool(str(blocks.get("strategy_suggestions_md") or "").strip()) and bool(getattr(cfg, "include_strategy", 0))
            if has_exec or has_strat:
                log.info("report_generation ai_blocks_inserted report_id=%s", int(report_id))

        update_report_content(con, int(report_id), summary, content_md)
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

    def _generate_skeleton_markdown(self, input_: ReportGenerationInput, cfg: Any) -> tuple[str, str]:
        """
        Minimal, stable markdown skeleton.

        Notes:
        - Executive summary: only LLM text (no metric bullets).
        - All boards/charts are rendered by frontend from evidence-based endpoints.
        """
        title = str(input_.report["title"] or "").strip() or "Report"
        parts: list[str] = [f"# {title}", ""]

        parts.append("## 执行摘要")
        parts.append("")

        if int(getattr(cfg, "include_trend", 0)):
            parts.extend(["## 舆情趋势", ""])
        if int(getattr(cfg, "include_feature_analysis", 0)):
            parts.extend(["## 风险点", ""])
        if int(getattr(cfg, "include_spam", 0)):
            parts.extend(["## 关键用户反馈", ""])
        if int(getattr(cfg, "include_competitor_compare", 0)):
            parts.extend(["## 竞品对比", ""])
        if int(getattr(cfg, "include_topics", 0)):
            parts.extend(["## 热点话题", ""])

        if int(getattr(cfg, "include_strategy", 0)):
            parts.extend(["## 策略建议", ""])

        return "", "\n".join(parts).rstrip() + "\n"

    def _overview_rollup(self, items: list[dict[str, Any]]) -> dict[str, Any]:
        pos = 0
        neu = 0
        neg = 0
        total = 0
        for it in items or []:
            total += int(it.get("total_post_count") or 0)
            pos += int(it.get("positive_count") or 0)
            neu += int(it.get("neutral_count") or 0)
            neg += int(it.get("negative_count") or 0)
        return {
            "total_post_count": total,
            "valid_post_count": total,
            "positive_count": pos,
            "neutral_count": neu,
            "negative_count": neg,
            "negative_rate": (neg / total) if total else 0.0,
        }

    def _generate_llm_blocks(self, con: sqlite3.Connection, input_: ReportGenerationInput, cfg: Any) -> dict[str, Any]:
        """
        Conservative LLM usage:
        - Only generate incremental blocks (executive summary add-ons + strategy add-ons)
        - Do not let LLM control the full markdown structure
        """
        report = input_.report

        include_sentiment = int(getattr(cfg, "include_sentiment", 0))
        include_trend = int(getattr(cfg, "include_trend", 0))
        include_topics = int(getattr(cfg, "include_topics", 0))
        include_feature_analysis = int(getattr(cfg, "include_feature_analysis", 0))
        include_spam = int(getattr(cfg, "include_spam", 0))
        include_competitor_compare = int(getattr(cfg, "include_competitor_compare", 0))
        include_strategy = int(getattr(cfg, "include_strategy", 0))
        has_keywords = bool(getattr(cfg, "keywords", None))

        overview_in = input_.overview or {}
        overview: dict[str, Any] = {}
        # Safe baseline fields (counts only, no module-specific metrics).
        for k in ["total_post_count", "valid_post_count"]:
            if k in overview_in:
                overview[k] = overview_in.get(k)
        if include_sentiment:
            for k in [
                "positive_count",
                "neutral_count",
                "negative_count",
                "negative_rate",
                "weighted_avg_sentiment_score",
            ]:
                if k in overview_in:
                    overview[k] = overview_in.get(k)
        if include_spam:
            for k in ["spam_post_count", "spam_rate"]:
                if k in overview_in:
                    overview[k] = overview_in.get(k)
        # There is no include_keywords flag; treat it as enabled only when keywords are explicitly set.
        if has_keywords:
            if "keyword_hit_count" in overview_in:
                overview["keyword_hit_count"] = overview_in.get("keyword_hit_count")

        post_excerpts: dict[str, list[dict[str, Any]]] = {}
        if include_strategy:
            post_excerpts = input_.post_excerpts or {}
            if not include_sentiment:
                for items in post_excerpts.values():
                    for it in items:
                        it.pop("sentiment", None)

        def _slice_board(board: dict[str, Any], *, max_days: int) -> dict[str, Any]:
            """
            Reduce board payload size for LLM.

            Frontend renders full boards; LLM only needs a short window for summaries.
            """
            b = board if isinstance(board, dict) else {}
            dates = b.get("dates") if isinstance(b.get("dates"), list) else []
            series = b.get("series") if isinstance(b.get("series"), list) else []
            if not dates or max_days <= 0:
                return {"dates": [], "series": []}
            tail = dates[-int(max_days) :]
            n = len(tail)
            new_series = []
            for s in series:
                if not isinstance(s, dict):
                    continue
                ns = dict(s)
                for k in ["total_post_count", "positive_count", "negative_count", "data"]:
                    if isinstance(ns.get(k), list) and len(ns.get(k)) >= n:
                        ns[k] = ns.get(k)[-n:]
                new_series.append(ns)
            return {"dates": tail, "series": new_series}

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
                "include_sentiment": include_sentiment,
                "include_trend": include_trend,
                "include_topics": include_topics,
                "include_feature_analysis": include_feature_analysis,
                "include_spam": include_spam,
                "include_competitor_compare": include_competitor_compare,
                "include_strategy": include_strategy,
            },
            "overview": overview,
            "trend": (_slice_board(input_.trend, max_days=14) if (include_sentiment and include_trend) else {"dates": [], "series": []}),
            "top_topics": (_slice_board(input_.topics, max_days=14) if include_topics else {"dates": [], "series": []}),
            "top_features": ((input_.risk_keywords or [])[:20] if include_feature_analysis else []),
            "competitor": ((input_.overview_items or [])[:20] if (include_sentiment and include_competitor_compare) else []),
            "key_user_feedback": ((input_.key_user_feedback or [])[:20] if include_spam else []),
            "post_excerpts": post_excerpts,
        }

        router = get_llm_router()
        res = router.run(task_type="report_generation", input=payload, con=con)
        log.info(
            "report_generation llm_blocks task_type=report_generation provider=%s model=%s ok=%s",
            getattr(res, "provider", None),
            getattr(res, "model", None),
            bool(getattr(res, "ok", False)),
        )
        out = res.output if isinstance(res.output, dict) else {}
        if bool(getattr(res, "ok", False)) and out:
            return out

        err = str(getattr(res, "error", None) or "LLM output empty")
        # Retry once with a smaller payload when timeout happens.
        if "timeout" in err.lower() or "readtimeout" in err.lower():
            log.warning("report_generation llm_timeout_retry report_id=%s err=%s", int(report["id"]), err)
            payload2 = dict(payload)
            payload2["trend"] = {"dates": [], "series": []}
            payload2["top_topics"] = {"dates": [], "series": []}
            payload2["key_user_feedback"] = []
            # Keep only the most representative excerpts.
            pe = payload2.get("post_excerpts") if isinstance(payload2.get("post_excerpts"), dict) else {}
            if isinstance(pe, dict):
                payload2["post_excerpts"] = {
                    "negative": (pe.get("negative") or [])[:2],
                    "popular": (pe.get("popular") or [])[:2],
                }
            res2 = router.run(task_type="report_generation", input=payload2, con=con)
            out2 = res2.output if isinstance(res2.output, dict) else {}
            if bool(getattr(res2, "ok", False)) and out2:
                return out2
            err2 = str(getattr(res2, "error", None) or "LLM output empty")
            raise RuntimeError(err2)

        raise RuntimeError(err)

    def _post_excerpts_for_llm(self, con: sqlite3.Connection, report_id: int, *, max_chars: int) -> dict[str, list[dict[str, Any]]]:
        """
        Representative post excerpts pulled from the report evidence dataset.

        Used as LLM grounding for executive summary / risk / strategy suggestions.
        """
        out: dict[str, list[dict[str, Any]]] = {"negative": [], "popular": []}

        for kind in ["negative", "popular"]:
            if kind == "negative":
                rows = con.execute(
                    """
                    SELECT
                      pr.id AS post_id,
                      pr.platform_id,
                      pr.title,
                      pr.content,
                      pr.like_count,
                      ps.sentiment,
                      ps.sentiment_score
                    FROM report_evidence re
                    JOIN post_raw pr ON pr.id=re.post_id
                    LEFT JOIN post_sentiment_result ps ON ps.post_id=pr.id
                    WHERE re.report_id=? AND re.section_name=?
                      AND COALESCE(ps.sentiment,'')='negative'
                    ORDER BY COALESCE(ps.sentiment_score, 0.0) ASC, COALESCE(pr.like_count,0) DESC, pr.id DESC
                    LIMIT 5;
                    """,
                    (int(report_id), "dataset"),
                ).fetchall()
            else:
                rows = con.execute(
                    """
                    SELECT
                      pr.id AS post_id,
                      pr.platform_id,
                      pr.title,
                      pr.content,
                      pr.like_count,
                      ps.sentiment
                    FROM report_evidence re
                    JOIN post_raw pr ON pr.id=re.post_id
                    LEFT JOIN post_sentiment_result ps ON ps.post_id=pr.id
                    WHERE re.report_id=? AND re.section_name=?
                    ORDER BY (COALESCE(pr.like_count,0) + COALESCE(pr.comment_count,0) * 2 + COALESCE(pr.share_count,0) * 3) DESC, pr.id DESC
                    LIMIT 5;
                    """,
                    (int(report_id), "dataset"),
                ).fetchall()

            items: list[dict[str, Any]] = []
            for r in rows or []:
                content = str(r["content"] or "").strip().replace("\n", " ")
                if len(content) > int(max_chars):
                    content = content[: int(max_chars)] + "..."
                items.append(
                    {
                        "post_id": int(r["post_id"]),
                        "platform_id": int(r["platform_id"]) if r["platform_id"] is not None else None,
                        "sentiment": str(r["sentiment"] or ""),
                        "like_count": int(r["like_count"] or 0),
                        "title": str(r["title"] or ""),
                        "content_excerpt": content,
                    }
                )
            out[kind] = items

        return out

    def _merge_llm_blocks(self, summary: str, content_md: str, blocks: dict[str, Any], cfg: Any) -> tuple[str, str]:
        if not isinstance(blocks, dict):
            return summary, content_md

        new_summary = self._to_text(blocks.get("summary")).strip()
        exec_md = self._normalize_md_bullets(blocks.get("executive_summary_md")).strip()
        trend_md = self._normalize_md_bullets(blocks.get("trend_summary_md")).strip()
        risk_md = self._normalize_md_bullets(blocks.get("risk_points_md")).strip()
        feedback_md = self._normalize_md_bullets(blocks.get("key_user_feedback_md")).strip()
        competitor_md = self._normalize_md_bullets(blocks.get("competitor_compare_md")).strip()
        topics_md = self._normalize_md_bullets(blocks.get("hot_topics_md")).strip()
        strat_md = self._normalize_md_bullets(blocks.get("strategy_suggestions_md")).strip()

        include_trend = int(getattr(cfg, "include_trend", 0))
        include_risk = int(getattr(cfg, "include_feature_analysis", 0))
        include_feedback = int(getattr(cfg, "include_spam", 0))
        include_competitor = int(getattr(cfg, "include_competitor_compare", 0))
        include_topics = int(getattr(cfg, "include_topics", 0))
        include_strategy = int(getattr(cfg, "include_strategy", 0))

        def _ensure_block(enabled: int, md: str, placeholder: str) -> str:
            if int(enabled) and not str(md or "").strip():
                return f"- {placeholder}"
            return md

        # Avoid empty regions in frontend when LLM returns an empty string unexpectedly.
        exec_md = _ensure_block(1, exec_md, "暂无可生成的执行摘要（请检查数据范围或稍后重试）")
        trend_md = _ensure_block(include_trend, trend_md, "暂无可生成的趋势摘要（请检查数据范围或稍后重试）")
        risk_md = _ensure_block(include_risk, risk_md, "暂无可生成的风险点摘要（请检查负面帖子与关键词数据）")
        feedback_md = _ensure_block(include_feedback, feedback_md, "暂无可生成的关键用户反馈摘要（请检查证据帖子是否为空）")
        competitor_md = _ensure_block(include_competitor, competitor_md, "暂无可生成的竞品对比摘要（请检查品牌选择与数据范围）")
        topics_md = _ensure_block(include_topics, topics_md, "暂无可生成的热点话题摘要（请检查话题/关键词结果是否为空）")
        strat_md = _ensure_block(include_strategy, strat_md, "暂无可生成的策略建议（请检查大模型配置或稍后重试）")

        merged_summary = new_summary or summary
        merged_md = str(content_md or "")

        if exec_md:
            merged_md = self._replace_section_body(merged_md, "## 执行摘要", exec_md)
        if trend_md and include_trend:
            merged_md = self._replace_section_body(merged_md, "## 舆情趋势", trend_md)
        if risk_md and include_risk:
            merged_md = self._replace_section_body(merged_md, "## 风险点", risk_md)
        if feedback_md and include_feedback:
            merged_md = self._replace_section_body(merged_md, "## 关键用户反馈", feedback_md)
        if competitor_md and include_competitor:
            merged_md = self._replace_section_body(merged_md, "## 竞品对比", competitor_md)
        if topics_md and include_topics:
            merged_md = self._replace_section_body(merged_md, "## 热点话题", topics_md)
        if strat_md and include_strategy:
            merged_md = self._replace_section_body(merged_md, "## 策略建议", strat_md)

        merged_md = self._upsert_ai_blocks_marker(merged_md, blocks)
        return merged_summary, merged_md

    def _upsert_ai_blocks_marker(self, md: str, blocks: dict[str, Any]) -> str:
        """
        Embed AI blocks as a hidden HTML comment in markdown for frontend rendering.
        Avoids DB schema changes while keeping report view structured.
        """
        text = str(md or "")
        if not isinstance(blocks, dict):
            return text

        payload = {
            "summary": self._to_text(blocks.get("summary")).strip(),
            "executive_summary_md": self._normalize_md_bullets(blocks.get("executive_summary_md")).strip(),
            "trend_summary_md": self._normalize_md_bullets(blocks.get("trend_summary_md")).strip(),
            "risk_points_md": self._normalize_md_bullets(blocks.get("risk_points_md")).strip(),
            "key_user_feedback_md": self._normalize_md_bullets(blocks.get("key_user_feedback_md")).strip(),
            "competitor_compare_md": self._normalize_md_bullets(blocks.get("competitor_compare_md")).strip(),
            "hot_topics_md": self._normalize_md_bullets(blocks.get("hot_topics_md")).strip(),
            "strategy_suggestions_md": self._normalize_md_bullets(blocks.get("strategy_suggestions_md")).strip(),
        }
        payload = {k: v for k, v in payload.items() if str(v or "").strip() != ""}
        if not payload:
            return text

        try:
            j = json.dumps(payload, ensure_ascii=False)
        except Exception:
            return text

        marker = f"<!--PRODWATCH_AI_BLOCKS\n{j}\n-->"
        text = re.sub(r"<!--PRODWATCH_AI_BLOCKS\\s*\\{.*?\\}\\s*-->", "", text, flags=re.S)
        return text.rstrip() + "\n\n" + marker + "\n"

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
        lines = [""]
        if str(subheading or "").strip():
            lines.append(str(subheading).strip())
        lines.append(str(block_md or "").rstrip())
        lines.append("")
        block = "\n".join(lines)
        if idx < 0:
            return text.rstrip() + block + "\n"
        # insert after heading line
        end = text.find("\n", idx)
        if end < 0:
            return text.rstrip() + block + "\n"
        return text[: end + 1] + block + text[end + 1 :]

    def _replace_section_body(self, md: str, heading: str, body_md: str) -> str:
        """
        Replace the section body (between heading and next '## ') with body_md.
        Best-effort: if heading not found, append heading + body to the end.
        """
        text = str(md or "")
        h = str(heading).strip()
        body = str(body_md or "").strip()
        if not body:
            return text

        # Match a level-2 heading line exactly, then lazily capture until next level-2 heading or EOF.
        pattern = re.compile(rf"(^\s*{re.escape(h)}\s*$)(.*?)(?=^\s*##\s+|\Z)", re.M | re.S)
        m = pattern.search(text)
        replacement = f"\n{body}\n\n"
        if not m:
            sep = "\n\n" if text.strip() else ""
            return text.rstrip() + sep + h + replacement
        # Keep the heading line; replace only the body part.
        start_body, end_body = m.span(2)
        return text[:start_body] + replacement + text[end_body:]


_default_service: Optional[ReportGenerationService] = None


def get_report_generation_service() -> ReportGenerationService:
    global _default_service
    if _default_service is None:
        _default_service = ReportGenerationService()
    return _default_service

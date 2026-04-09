# 作用：后端链路：报告生成链路编排（链路 E）。

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional


def now_ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def parse_date(value: str) -> str:
    datetime.strptime(value, "%Y-%m-%d")
    return value


def connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON;")
    con.execute("PRAGMA journal_mode = WAL;")
    con.execute("PRAGMA synchronous = NORMAL;")
    return con


def resolve_db_path(db_path: str) -> str:
    if not os.path.exists(db_path):
        folder = os.path.dirname(db_path) or "."
        base = os.path.basename(db_path)
        if base == "database.sqlite":
            alt = os.path.join(folder, "database..sqlite")
            if os.path.exists(alt):
                return alt
        raise FileNotFoundError(db_path)

    def has_project_table(path: str) -> bool:
        try:
            ro = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            try:
                row = ro.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='project' LIMIT 1;"
                ).fetchone()
                return row is not None
            finally:
                ro.close()
        except sqlite3.Error:
            return False

    if has_project_table(db_path):
        return db_path

    folder = os.path.dirname(db_path) or "."
    base = os.path.basename(db_path)
    candidates: list[str] = []
    if base == "database.sqlite":
        candidates.append(os.path.join(folder, "database..sqlite"))
    candidates.append(os.path.join(folder, "database.sqlite"))
    candidates.append(os.path.join(folder, "database..sqlite"))
    for c in candidates:
        if c != db_path and os.path.exists(c) and has_project_table(c):
            return c

    return db_path


def parse_int_list(value: Optional[str]) -> Optional[list[int]]:
    if value is None:
        return None
    value = value.strip()
    if value == "":
        return []
    out: list[int] = []
    for part in value.split(","):
        part = part.strip()
        if part:
            out.append(int(part))
    return out


def parse_str_list(value: Optional[str]) -> Optional[list[str]]:
    if value is None:
        return None
    value = value.strip()
    if value == "":
        return []
    out: list[str] = []
    for part in value.split(","):
        part = part.strip()
        if part:
            out.append(part)
    return out


def json_text(value: Optional[list[Any]]) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False)


@dataclass(frozen=True)
class ReportCreateRequest:
    project_id: int
    title: str
    report_type: str
    data_start_date: str
    data_end_date: str
    created_by: str


@dataclass(frozen=True)
class ReportConfigInput:
    platform_ids: Optional[list[int]]
    brand_ids: Optional[list[int]]
    keywords: Optional[list[str]]
    include_sentiment: int
    include_trend: int
    include_topics: int
    include_feature_analysis: int
    include_spam: int
    include_competitor_compare: int
    include_strategy: int


@dataclass(frozen=True)
class ReportConfigResolved:
    report_id: int
    platform_ids: Optional[list[int]]
    brand_ids: Optional[list[int]]
    keywords: Optional[list[str]]
    include_sentiment: int
    include_trend: int
    include_topics: int
    include_feature_analysis: int
    include_spam: int
    include_competitor_compare: int
    include_strategy: int


def get_or_create_report(con: sqlite3.Connection, req: ReportCreateRequest) -> int:
    row = con.execute(
        """
        SELECT id
        FROM report
        WHERE project_id=?
          AND report_type=?
          AND data_start_date=?
          AND data_end_date=?
          AND title=?
        ORDER BY id DESC
        LIMIT 1;
        """,
        (req.project_id, req.report_type, req.data_start_date, req.data_end_date, req.title),
    ).fetchone()
    if row:
        return int(row["id"])

    ts = now_ts()
    con.execute(
        """
        INSERT INTO report(
          project_id, title, report_type, data_start_date, data_end_date,
          status, summary, content_markdown, created_by, created_at, updated_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (
            req.project_id,
            req.title,
            req.report_type,
            req.data_start_date,
            req.data_end_date,
            "pending",
            None,
            None,
            req.created_by,
            ts,
            ts,
        ),
    )
    return int(con.execute("SELECT last_insert_rowid();").fetchone()[0])


def upsert_report_config(con: sqlite3.Connection, report_id: int, cfg: ReportConfigInput) -> None:
    row = con.execute("SELECT id FROM report_config WHERE report_id=? LIMIT 1;", (report_id,)).fetchone()
    if row:
        con.execute(
            """
            UPDATE report_config
            SET platform_ids=?,
                brand_ids=?,
                keywords=?,
                include_sentiment=?,
                include_trend=?,
                include_topics=?,
                include_feature_analysis=?,
                include_spam=?,
                include_competitor_compare=?,
                include_strategy=?
            WHERE report_id=?;
            """,
            (
                json_text(cfg.platform_ids),
                json_text(cfg.brand_ids),
                json_text(cfg.keywords),
                int(cfg.include_sentiment),
                int(cfg.include_trend),
                int(cfg.include_topics),
                int(cfg.include_feature_analysis),
                int(cfg.include_spam),
                int(cfg.include_competitor_compare),
                int(cfg.include_strategy),
                report_id,
            ),
        )
        return

    con.execute(
        """
        INSERT INTO report_config(
          report_id, platform_ids, brand_ids, keywords,
          include_sentiment, include_trend, include_topics,
          include_feature_analysis, include_spam, include_competitor_compare, include_strategy
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (
            report_id,
            json_text(cfg.platform_ids),
            json_text(cfg.brand_ids),
            json_text(cfg.keywords),
            int(cfg.include_sentiment),
            int(cfg.include_trend),
            int(cfg.include_topics),
            int(cfg.include_feature_analysis),
            int(cfg.include_spam),
            int(cfg.include_competitor_compare),
            int(cfg.include_strategy),
        ),
    )


def read_report(con: sqlite3.Connection, report_id: int) -> sqlite3.Row:
    row = con.execute("SELECT * FROM report WHERE id=?;", (report_id,)).fetchone()
    if not row:
        raise ValueError(f"report_id not found: {report_id}")
    return row


def read_report_config(con: sqlite3.Connection, report_id: int) -> ReportConfigResolved:
    row = con.execute("SELECT * FROM report_config WHERE report_id=?;", (report_id,)).fetchone()
    if not row:
        raise ValueError(f"report_config not found for report_id: {report_id}")

    def _json_or_csv_list(raw: Any, *, kind: str) -> Optional[list[Any]]:
        """
        Backward/compat parser for `report_config` fields.

        This project currently has two possible encodings in `report_config`:
        - JSON text (e.g. "[1,2]" / "["a","b"]") written by this chain module
        - CSV text  (e.g. "1,2" / "a,b") written by API endpoints
        """
        if raw is None:
            return None
        s = str(raw).strip()
        if s == "":
            return None

        # 1) JSON list / scalar
        try:
            j = json.loads(s)
            if j is None:
                return None
            if isinstance(j, list):
                return j
            # tolerate scalar -> singleton list
            return [j]
        except json.JSONDecodeError:
            pass

        # 2) CSV fallback
        parts = [p.strip() for p in s.split(",") if p.strip() != ""]
        if not parts:
            return None
        if kind == "int":
            out: list[int] = []
            for p in parts:
                try:
                    n = int(p)
                except ValueError:
                    continue
                if n > 0:
                    out.append(n)
            return out or None
        out_s: list[str] = []
        for p in parts:
            if p:
                out_s.append(p)
        return out_s or None

    return ReportConfigResolved(
        report_id=report_id,
        platform_ids=_json_or_csv_list(row["platform_ids"], kind="int"),
        brand_ids=_json_or_csv_list(row["brand_ids"], kind="int"),
        keywords=_json_or_csv_list(row["keywords"], kind="str"),
        include_sentiment=int(row["include_sentiment"] or 0),
        include_trend=int(row["include_trend"] or 0),
        include_topics=int(row["include_topics"] or 0),
        include_feature_analysis=int(row["include_feature_analysis"] or 0),
        include_spam=int(row["include_spam"] or 0),
        include_competitor_compare=int(row["include_competitor_compare"] or 0),
        include_strategy=int(row["include_strategy"] or 0),
    )


def _in_filter(field: str, values: Optional[list[Any]]) -> tuple[str, list[Any]]:
    if values is None:
        return "", []
    if len(values) == 0:
        return " AND 1=0", []
    placeholders = ",".join(["?"] * len(values))
    return f" AND {field} IN ({placeholders})", list(values)


def fetch_agg_overview(con: sqlite3.Connection, project_id: int, start: str, end: str, cfg: ReportConfigResolved) -> dict:
    plat_sql, plat_params = _in_filter("platform_id", cfg.platform_ids)
    brand_sql, brand_params = _in_filter("brand_id", cfg.brand_ids)
    row = con.execute(
        f"""
        SELECT
          SUM(COALESCE(total_post_count,0)) AS total_post_count,
          SUM(COALESCE(valid_post_count,0)) AS valid_post_count,
          SUM(COALESCE(spam_post_count,0)) AS spam_post_count,
          SUM(COALESCE(positive_count,0)) AS positive_count,
          SUM(COALESCE(neutral_count,0))  AS neutral_count,
          SUM(COALESCE(negative_count,0)) AS negative_count,
          SUM(COALESCE(keyword_count,0)) AS keyword_count,
          CASE
            WHEN SUM(COALESCE(total_post_count,0))=0 THEN 0.0
            ELSE SUM(COALESCE(avg_sentiment_score,0.0) * COALESCE(total_post_count,0))
                 / SUM(COALESCE(total_post_count,0))
          END AS weighted_avg_sentiment_score
        FROM daily_metric
        WHERE project_id=?
          AND stat_date BETWEEN ? AND ?
          {plat_sql}
          {brand_sql};
        """,
        (project_id, start, end, *plat_params, *brand_params),
    ).fetchone()
    if not row:
        return {"total_post_count": 0}
    total = int(row["total_post_count"] or 0)
    spam = int(row["spam_post_count"] or 0)
    neg = int(row["negative_count"] or 0)
    return {
        "total_post_count": total,
        "valid_post_count": int(row["valid_post_count"] or 0),
        "spam_post_count": spam,
        "spam_rate": (spam / total) if total else 0.0,
        "positive_count": int(row["positive_count"] or 0),
        "neutral_count": int(row["neutral_count"] or 0),
        "negative_count": neg,
        "negative_rate": (neg / total) if total else 0.0,
        "keyword_hit_count": int(row["keyword_count"] or 0),
        "weighted_avg_sentiment_score": float(row["weighted_avg_sentiment_score"] or 0.0),
    }


def fetch_sentiment_trend(con: sqlite3.Connection, project_id: int, start: str, end: str, cfg: ReportConfigResolved) -> list[dict]:
    plat_sql, plat_params = _in_filter("platform_id", cfg.platform_ids)
    brand_sql, brand_params = _in_filter("brand_id", cfg.brand_ids)
    rows = con.execute(
        f"""
        SELECT
          stat_date,
          SUM(COALESCE(total_post_count,0)) AS total_post_count,
          SUM(COALESCE(positive_count,0)) AS positive_count,
          SUM(COALESCE(neutral_count,0))  AS neutral_count,
          SUM(COALESCE(negative_count,0)) AS negative_count,
          CASE
            WHEN SUM(COALESCE(total_post_count,0))=0 THEN 0.0
            ELSE SUM(COALESCE(avg_sentiment_score,0.0) * COALESCE(total_post_count,0))
                 / SUM(COALESCE(total_post_count,0))
          END AS weighted_avg_sentiment_score
        FROM daily_metric
        WHERE project_id=?
          AND stat_date BETWEEN ? AND ?
          {plat_sql}
          {brand_sql}
        GROUP BY stat_date
        ORDER BY stat_date;
        """,
        (project_id, start, end, *plat_params, *brand_params),
    ).fetchall()
    return [
        {
            "stat_date": r["stat_date"],
            "total_post_count": int(r["total_post_count"] or 0),
            "positive_count": int(r["positive_count"] or 0),
            "neutral_count": int(r["neutral_count"] or 0),
            "negative_count": int(r["negative_count"] or 0),
            "weighted_avg_sentiment_score": float(r["weighted_avg_sentiment_score"] or 0.0),
        }
        for r in rows
    ]


def fetch_top_keywords(con: sqlite3.Connection, project_id: int, start: str, end: str, cfg: ReportConfigResolved, top_n: int = 15) -> list[dict]:
    plat_sql, plat_params = _in_filter("platform_id", cfg.platform_ids)
    brand_sql, brand_params = _in_filter("brand_id", cfg.brand_ids)
    rows = con.execute(
        f"""
        SELECT keyword, SUM(COALESCE(hit_count,0)) AS hit_count
        FROM daily_keyword_metric
        WHERE project_id=?
          AND stat_date BETWEEN ? AND ?
          {plat_sql}
          {brand_sql}
        GROUP BY keyword
        ORDER BY hit_count DESC, keyword ASC
        LIMIT ?;
        """,
        (project_id, start, end, *plat_params, *brand_params, int(top_n)),
    ).fetchall()
    return [{"keyword": r["keyword"], "hit_count": int(r["hit_count"] or 0)} for r in rows]


def fetch_top_negative_features(con: sqlite3.Connection, project_id: int, start: str, end: str, cfg: ReportConfigResolved, top_n: int = 10) -> list[dict]:
    brand_sql, brand_params = _in_filter("brand_id", cfg.brand_ids)
    rows = con.execute(
        f"""
        SELECT
          feature_name,
          SUM(COALESCE(mention_count,0)) AS mention_count,
          SUM(COALESCE(negative_count,0)) AS negative_count,
          SUM(COALESCE(positive_count,0)) AS positive_count,
          SUM(COALESCE(neutral_count,0)) AS neutral_count
        FROM daily_feature_metric
        WHERE project_id=?
          AND stat_date BETWEEN ? AND ?
          {brand_sql}
        GROUP BY feature_name
        ORDER BY negative_count DESC, mention_count DESC, feature_name ASC
        LIMIT ?;
        """,
        (project_id, start, end, *brand_params, int(top_n)),
    ).fetchall()
    return [
        {
            "feature_name": r["feature_name"],
            "mention_count": int(r["mention_count"] or 0),
            "negative_count": int(r["negative_count"] or 0),
            "positive_count": int(r["positive_count"] or 0),
            "neutral_count": int(r["neutral_count"] or 0),
        }
        for r in rows
    ]


def fetch_competitor_compare(con: sqlite3.Connection, project_id: int, start: str, end: str, cfg: ReportConfigResolved) -> list[dict]:
    proj = con.execute("SELECT our_brand_id FROM project WHERE id=?;", (project_id,)).fetchone()
    our_brand_id = int(proj["our_brand_id"]) if proj and proj["our_brand_id"] is not None else None
    plat_sql, plat_params = _in_filter("platform_id", cfg.platform_ids)

    brand_ids: list[int]
    if cfg.brand_ids is not None:
        brand_ids = [int(x) for x in cfg.brand_ids]
    else:
        rows = con.execute(
            "SELECT brand_id FROM project_brand WHERE project_id=? ORDER BY brand_id;",
            (project_id,),
        ).fetchall()
        brand_ids = [int(r["brand_id"]) for r in rows]

    results: list[dict] = []
    for bid in brand_ids:
        row = con.execute(
            f"""
            SELECT
              SUM(COALESCE(total_post_count,0)) AS total_post_count,
              SUM(COALESCE(negative_count,0)) AS negative_count,
              CASE
                WHEN SUM(COALESCE(total_post_count,0))=0 THEN 0.0
                ELSE SUM(COALESCE(avg_sentiment_score,0.0) * COALESCE(total_post_count,0))
                     / SUM(COALESCE(total_post_count,0))
              END AS weighted_avg_sentiment_score
            FROM daily_metric
            WHERE project_id=?
              AND brand_id=?
              AND stat_date BETWEEN ? AND ?
              {plat_sql};
            """,
            (project_id, bid, start, end, *plat_params),
        ).fetchone()
        name_row = con.execute("SELECT name FROM brand WHERE id=?;", (bid,)).fetchone()
        brand_name = name_row["name"] if name_row else str(bid)
        total = int(row["total_post_count"] or 0) if row else 0
        neg = int(row["negative_count"] or 0) if row else 0
        results.append(
            {
                "brand_id": bid,
                "brand_name": brand_name,
                "is_our_brand": (our_brand_id == bid) if our_brand_id is not None else False,
                "total_post_count": total,
                "negative_rate": (neg / total) if total else 0.0,
                "weighted_avg_sentiment_score": float(row["weighted_avg_sentiment_score"] or 0.0) if row else 0.0,
            }
        )

    results.sort(key=lambda x: (0 if x["is_our_brand"] else 1, -x["total_post_count"], x["brand_name"]))
    return results


def fetch_candidate_posts(
    con: sqlite3.Connection,
    project_id: int,
    start: str,
    end: str,
    cfg: ReportConfigResolved,
    limit_each: int = 5,
) -> dict[str, list[sqlite3.Row]]:
    plat_sql, plat_params = _in_filter("pr.platform_id", cfg.platform_ids)
    brand_sql, brand_params = _in_filter("pr.brand_id", cfg.brand_ids)

    base_where = f"""
      pr.project_id=?
      AND date(COALESCE(pr.publish_time, pr.crawled_at)) BETWEEN ? AND ?
      {plat_sql}
      {brand_sql}
    """
    base_params = [project_id, start, end, *plat_params, *brand_params]

    negative = con.execute(
        f"""
        SELECT
          pr.id AS post_id,
          pr.platform_id,
          pr.brand_id,
          pr.title,
          pr.content,
          pr.post_url,
          pr.publish_time,
          pr.like_count,
          pr.comment_count,
          pr.share_count,
          ps.sentiment,
          ps.sentiment_score,
          sp.spam_label,
          sp.spam_score
        FROM post_raw pr
        LEFT JOIN post_clean_result pc ON pc.post_id=pr.id
        LEFT JOIN post_sentiment_result ps ON ps.post_id=pr.id
        LEFT JOIN post_spam_result sp ON sp.post_id=pr.id
        WHERE {base_where}
          AND COALESCE(pc.is_valid, 1)=1
        ORDER BY COALESCE(ps.sentiment_score, 0.0) ASC, COALESCE(pr.like_count,0) DESC, pr.id DESC
        LIMIT ?;
        """,
        (*base_params, int(limit_each)),
    ).fetchall()

    spam = con.execute(
        f"""
        SELECT
          pr.id AS post_id,
          pr.platform_id,
          pr.brand_id,
          pr.title,
          pr.content,
          pr.post_url,
          pr.publish_time,
          pr.like_count,
          pr.comment_count,
          pr.share_count,
          ps.sentiment,
          ps.sentiment_score,
          sp.spam_label,
          sp.spam_score
        FROM post_raw pr
        LEFT JOIN post_clean_result pc ON pc.post_id=pr.id
        LEFT JOIN post_sentiment_result ps ON ps.post_id=pr.id
        LEFT JOIN post_spam_result sp ON sp.post_id=pr.id
        WHERE {base_where}
          AND COALESCE(pc.is_valid, 1)=1
          AND sp.spam_label='spam'
        ORDER BY COALESCE(sp.spam_score, 0.0) DESC, COALESCE(pr.like_count,0) DESC, pr.id DESC
        LIMIT ?;
        """,
        (*base_params, int(limit_each)),
    ).fetchall()

    popular = con.execute(
        f"""
        SELECT
          pr.id AS post_id,
          pr.platform_id,
          pr.brand_id,
          pr.title,
          pr.content,
          pr.post_url,
          pr.publish_time,
          pr.like_count,
          pr.comment_count,
          pr.share_count,
          ps.sentiment,
          ps.sentiment_score,
          sp.spam_label,
          sp.spam_score
        FROM post_raw pr
        LEFT JOIN post_clean_result pc ON pc.post_id=pr.id
        LEFT JOIN post_sentiment_result ps ON ps.post_id=pr.id
        LEFT JOIN post_spam_result sp ON sp.post_id=pr.id
        WHERE {base_where}
          AND COALESCE(pc.is_valid, 1)=1
        ORDER BY COALESCE(pr.like_count,0) DESC, COALESCE(pr.comment_count,0) DESC, pr.id DESC
        LIMIT ?;
        """,
        (*base_params, int(limit_each)),
    ).fetchall()

    return {"negative": negative, "spam": spam, "popular": popular}


def llm_mock_generate_markdown(
    report: sqlite3.Row,
    overview: dict,
    trend: list[dict],
    top_keywords: list[dict],
    top_features: list[dict],
    competitor: list[dict],
    posts: dict[str, list[sqlite3.Row]],
) -> tuple[str, str]:
    title = report["title"] or "Report"
    start = report["data_start_date"]
    end = report["data_end_date"]

    def pct(x: float) -> str:
        return f"{x*100:.1f}%"

    exec_lines = [
        f"- Range: {start} ~ {end}",
        f"- Total posts: {overview.get('total_post_count', 0)}",
        f"- Negative rate: {pct(float(overview.get('negative_rate', 0.0)))}",
        f"- Spam rate: {pct(float(overview.get('spam_rate', 0.0)))}",
        f"- Avg sentiment score: {float(overview.get('weighted_avg_sentiment_score', 0.0)):.3f}",
    ]

    trend_lines = []
    for d in trend:
        trend_lines.append(
            f"- {d['stat_date']}: total={d['total_post_count']}, pos={d['positive_count']}, neu={d['neutral_count']}, neg={d['negative_count']}, score={d['weighted_avg_sentiment_score']:.3f}"
        )

    keyword_lines = []
    for k in top_keywords[:10]:
        keyword_lines.append(f"- {k['keyword']}: {k['hit_count']}")

    risk_lines = []
    if top_features:
        risk_lines.append("Top negative features:")
        for f in top_features[:5]:
            risk_lines.append(f"- {f['feature_name']}: neg={f['negative_count']}, mentions={f['mention_count']}")
    if top_keywords:
        risk_lines.append("Top keywords:")
        for k in top_keywords[:5]:
            risk_lines.append(f"- {k['keyword']}: hits={k['hit_count']}")

    feedback_lines = []
    for r in posts.get("popular", [])[:5]:
        content = (r["content"] or "").strip().replace("\n", " ")
        feedback_lines.append(
            f"- (post_id={r['post_id']}) likes={r['like_count'] or 0} sentiment={r['sentiment']}: {content[:120]}"
        )

    competitor_lines = []
    for c in competitor:
        competitor_lines.append(
            f"- {c['brand_name']}: total={c['total_post_count']} neg_rate={pct(float(c['negative_rate']))} score={float(c['weighted_avg_sentiment_score']):.3f}"
        )

    strategy_lines = [
        "- Prioritize fixes for top negative features (above).",
        "- Improve response playbook for high-risk keywords and complaints.",
        "- Track daily sentiment score and negative rate; trigger alerts on spikes.",
    ]

    md = "\n".join(
        [
            f"# {title}",
            "",
            "## Executive Summary",
            *exec_lines,
            "",
            "## Public Opinion Trend",
            *(trend_lines if trend_lines else ["- (no aggregated data)"]),
            "",
            "## Risk Points",
            *(risk_lines if risk_lines else ["- (no risk data)"]),
            "",
            "## Key User Feedback",
            *(feedback_lines if feedback_lines else ["- (no posts)"]),
            "",
            "## Competitor Compare",
            *(competitor_lines if competitor_lines else ["- (no competitor data)"]),
            "",
            "## Strategy Suggestions",
            *strategy_lines,
            "",
            "## Keyword Monitor",
            *(keyword_lines if keyword_lines else ["- (no keywords)"]),
            "",
        ]
    )

    summary = (
        f"total={overview.get('total_post_count', 0)}, "
        f"neg_rate={pct(float(overview.get('negative_rate', 0.0)))}, "
        f"spam_rate={pct(float(overview.get('spam_rate', 0.0)))}"
    )
    return summary, md


def update_report_content(con: sqlite3.Connection, report_id: int, summary: str, content_markdown: str) -> None:
    con.execute(
        "UPDATE report SET summary=?, content_markdown=?, updated_at=? WHERE id=?;",
        (summary, content_markdown, now_ts(), report_id),
    )


def set_report_status(con: sqlite3.Connection, report_id: int, status: str) -> None:
    con.execute("UPDATE report SET status=?, updated_at=? WHERE id=?;", (status, now_ts(), report_id))


def insert_report_evidence(
    con: sqlite3.Connection,
    report_id: int,
    post_id: int,
    section_name: str,
    quote_reason: str,
    sentiment: Optional[str],
    spam_label: Optional[str],
) -> None:
    exists = con.execute(
        "SELECT 1 FROM report_evidence WHERE report_id=? AND post_id=? AND section_name=? LIMIT 1;",
        (report_id, post_id, section_name),
    ).fetchone()
    if exists:
        return
    con.execute(
        """
        INSERT INTO report_evidence(
          report_id, post_id, section_name, quote_reason, sentiment, spam_label, created_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?);
        """,
        (report_id, post_id, section_name, quote_reason, sentiment, spam_label, now_ts()),
    )


def select_and_write_evidence(con: sqlite3.Connection, report_id: int, posts: dict[str, list[sqlite3.Row]]) -> None:
    picked: set[int] = set()

    for r in posts.get("negative", [])[:3]:
        pid = int(r["post_id"])
        if pid in picked:
            continue
        picked.add(pid)
        insert_report_evidence(
            con,
            report_id,
            pid,
            "risk_points",
            "top negative sentiment post",
            r["sentiment"],
            r["spam_label"],
        )

    for r in posts.get("spam", [])[:3]:
        pid = int(r["post_id"])
        if pid in picked:
            continue
        picked.add(pid)
        insert_report_evidence(
            con,
            report_id,
            pid,
            "risk_points",
            "spam-labeled post",
            r["sentiment"],
            r["spam_label"],
        )

    for r in posts.get("popular", [])[:3]:
        pid = int(r["post_id"])
        if pid in picked:
            continue
        picked.add(pid)
        insert_report_evidence(
            con,
            report_id,
            pid,
            "key_user_feedback",
            "high engagement post",
            r["sentiment"],
            r["spam_label"],
        )


def generate_report(con: sqlite3.Connection, report_id: int) -> dict[str, Any]:
    report = read_report(con, report_id)
    cfg = read_report_config(con, report_id)

    project_id = int(report["project_id"])
    start = str(report["data_start_date"])
    end = str(report["data_end_date"])

    set_report_status(con, report_id, "running")

    overview = fetch_agg_overview(con, project_id, start, end, cfg)
    trend = fetch_sentiment_trend(con, project_id, start, end, cfg) if cfg.include_trend else []
    top_keywords = fetch_top_keywords(con, project_id, start, end, cfg) if cfg.include_topics else []
    top_features = fetch_top_negative_features(con, project_id, start, end, cfg) if cfg.include_feature_analysis else []
    competitor = fetch_competitor_compare(con, project_id, start, end, cfg) if cfg.include_competitor_compare else []
    posts = fetch_candidate_posts(con, project_id, start, end, cfg, limit_each=6)

    summary, content_md = llm_mock_generate_markdown(
        report=report,
        overview=overview,
        trend=trend,
        top_keywords=top_keywords,
        top_features=top_features,
        competitor=competitor,
        posts=posts,
    )

    update_report_content(con, report_id, summary, content_md)
    select_and_write_evidence(con, report_id, posts)
    set_report_status(con, report_id, "success")
    return {"report_id": report_id, "status": "success"}


def get_report_evidence_details(con: sqlite3.Connection, report_id: int) -> list[dict[str, Any]]:
    rows = con.execute(
        """
        SELECT
          re.id AS evidence_id,
          re.section_name,
          re.quote_reason,
          re.sentiment AS evidence_sentiment,
          re.spam_label AS evidence_spam_label,
          re.created_at AS evidence_created_at,
          pr.id AS post_id,
          pr.platform_id,
          pr.brand_id,
          pr.author_name,
          pr.title,
          pr.content,
          pr.post_url,
          pr.publish_time,
          pr.like_count,
          pr.comment_count,
          pr.share_count,
          ps.sentiment,
          ps.sentiment_score,
          sp.spam_label,
          sp.spam_score
        FROM report_evidence re
        JOIN post_raw pr ON pr.id = re.post_id
        LEFT JOIN post_sentiment_result ps ON ps.post_id = pr.id
        LEFT JOIN post_spam_result sp ON sp.post_id = pr.id
        WHERE re.report_id=?
        ORDER BY re.id;
        """,
        (report_id,),
    ).fetchall()

    return [
        {
            "evidence_id": int(r["evidence_id"]),
            "section_name": r["section_name"],
            "quote_reason": r["quote_reason"],
            "evidence_sentiment": r["evidence_sentiment"],
            "evidence_spam_label": r["evidence_spam_label"],
            "evidence_created_at": r["evidence_created_at"],
            "post": {
                "post_id": int(r["post_id"]),
                "platform_id": r["platform_id"],
                "brand_id": r["brand_id"],
                "author_name": r["author_name"],
                "title": r["title"],
                "content": r["content"],
                "post_url": r["post_url"],
                "publish_time": r["publish_time"],
                "like_count": r["like_count"],
                "comment_count": r["comment_count"],
                "share_count": r["share_count"],
            },
            "analysis": {
                "sentiment": r["sentiment"],
                "sentiment_score": r["sentiment_score"],
                "spam_label": r["spam_label"],
                "spam_score": r["spam_score"],
            },
        }
        for r in rows
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Main chain E: report generation (SQLite)")
    parser.add_argument("--db", required=True)
    parser.add_argument("--action", choices=["create", "generate", "create_and_generate", "evidence"], required=True)

    parser.add_argument("--report-id", type=int, default=None)
    parser.add_argument("--project-id", type=int, default=None)
    parser.add_argument("--title", default=None)
    parser.add_argument("--report-type", default="daily")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--created-by", default="system")

    parser.add_argument("--platform-ids", default=None)
    parser.add_argument("--brand-ids", default=None)
    parser.add_argument("--keywords", default=None)

    parser.add_argument("--include-sentiment", type=int, default=1)
    parser.add_argument("--include-trend", type=int, default=1)
    parser.add_argument("--include-topics", type=int, default=1)
    parser.add_argument("--include-feature-analysis", type=int, default=1)
    parser.add_argument("--include-spam", type=int, default=1)
    parser.add_argument("--include-competitor-compare", type=int, default=1)
    parser.add_argument("--include-strategy", type=int, default=1)

    args = parser.parse_args()

    db_path = resolve_db_path(str(args.db))
    con = connect(db_path)
    try:
        if args.action in ("create", "create_and_generate"):
            if args.project_id is None:
                raise SystemExit("--project-id is required")
            if not args.start_date or not args.end_date:
                raise SystemExit("--start-date and --end-date are required")

            start = parse_date(str(args.start_date))
            end = parse_date(str(args.end_date))
            title = args.title or f"{args.report_type} report {start}~{end}"

            with con:
                report_id = get_or_create_report(
                    con,
                    ReportCreateRequest(
                        project_id=int(args.project_id),
                        title=str(title),
                        report_type=str(args.report_type),
                        data_start_date=start,
                        data_end_date=end,
                        created_by=str(args.created_by),
                    ),
                )
                upsert_report_config(
                    con,
                    report_id,
                    ReportConfigInput(
                        platform_ids=parse_int_list(args.platform_ids),
                        brand_ids=parse_int_list(args.brand_ids),
                        keywords=parse_str_list(args.keywords),
                        include_sentiment=int(args.include_sentiment),
                        include_trend=int(args.include_trend),
                        include_topics=int(args.include_topics),
                        include_feature_analysis=int(args.include_feature_analysis),
                        include_spam=int(args.include_spam),
                        include_competitor_compare=int(args.include_competitor_compare),
                        include_strategy=int(args.include_strategy),
                    ),
                )

            if args.action == "create":
                print(json.dumps({"ok": True, "db": db_path, "report_id": report_id}, ensure_ascii=False))
                return

            with con:
                out = generate_report(con, int(report_id))
            print(json.dumps({"ok": True, "db": db_path, **out}, ensure_ascii=False))
            return

        if args.action == "generate":
            if args.report_id is None:
                raise SystemExit("--report-id is required")
            with con:
                out = generate_report(con, int(args.report_id))
            print(json.dumps({"ok": True, "db": db_path, **out}, ensure_ascii=False))
            return

        if args.action == "evidence":
            if args.report_id is None:
                raise SystemExit("--report-id is required")
            out = get_report_evidence_details(con, int(args.report_id))
            print(
                json.dumps(
                    {"ok": True, "db": db_path, "report_id": int(args.report_id), "evidence": out},
                    ensure_ascii=False,
                )
            )
            return
    finally:
        con.close()


if __name__ == "__main__":
    main()

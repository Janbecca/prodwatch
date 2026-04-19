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


def fetch_top_topics(con: sqlite3.Connection, project_id: int, start: str, end: str, cfg: ReportConfigResolved, top_n: int = 15) -> list[dict]:
    """
    Hot topics for the report (main source: topic_result).

    Primary: daily_topic_metric (derived from topic_result).
    Fallback: aggregate directly from topic_result + post_raw (legacy DB).
    """
    plat_sql, plat_params = _in_filter("platform_id", cfg.platform_ids)
    brand_sql, brand_params = _in_filter("brand_id", cfg.brand_ids)
    try:
        rows = con.execute(
            f"""
            SELECT topic, SUM(COALESCE(hit_count,0)) AS hit_count
            FROM daily_topic_metric
            WHERE project_id=?
              AND stat_date BETWEEN ? AND ?
              {plat_sql}
              {brand_sql}
            GROUP BY topic
            ORDER BY hit_count DESC, topic ASC
            LIMIT ?;
            """,
            (project_id, start, end, *plat_params, *brand_params, int(top_n)),
        ).fetchall()
        return [{"topic": r["topic"], "hit_count": int(r["hit_count"] or 0)} for r in rows]
    except sqlite3.Error as e:
        if "no such table" not in str(e).lower():
            raise

    rows = con.execute(
        f"""
        SELECT tr.topic AS topic, COUNT(*) AS hit_count
        FROM topic_result tr
        JOIN post_raw pr ON pr.id=tr.post_id
        WHERE pr.project_id=?
          AND date(COALESCE(pr.publish_time, pr.crawled_at)) BETWEEN ? AND ?
          {plat_sql.replace('platform_id', 'pr.platform_id')}
          {brand_sql.replace('brand_id', 'pr.brand_id')}
        GROUP BY tr.topic
        ORDER BY hit_count DESC, tr.topic ASC
        LIMIT ?;
        """,
        (project_id, start, end, *plat_params, *brand_params, int(top_n)),
    ).fetchall()
    return [{"topic": r["topic"], "hit_count": int(r["hit_count"] or 0)} for r in rows]


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
    top_topics: list[dict],
    top_features: list[dict],
    competitor: list[dict],
    posts: dict[str, list[sqlite3.Row]],
    cfg: Optional[ReportConfigResolved] = None,
) -> tuple[str, str]:
    title = report["title"] or "Report"
    start = report["data_start_date"]
    end = report["data_end_date"]

    show_sentiment = bool(getattr(cfg, "include_sentiment", 1)) if cfg is not None else True
    show_trend = bool(getattr(cfg, "include_trend", 1)) if cfg is not None else True
    show_topics = bool(getattr(cfg, "include_topics", 1)) if cfg is not None else True
    show_feature = bool(getattr(cfg, "include_feature_analysis", 1)) if cfg is not None else True
    show_spam = bool(getattr(cfg, "include_spam", 1)) if cfg is not None else True
    show_competitor = bool(getattr(cfg, "include_competitor_compare", 1)) if cfg is not None else True
    show_strategy = bool(getattr(cfg, "include_strategy", 1)) if cfg is not None else True
    # Some sections inherently depend on sentiment analysis results.
    show_trend = bool(show_trend and show_sentiment)
    show_competitor = bool(show_competitor and show_sentiment)

    def pct(x: float) -> str:
        return f"{x*100:.1f}%"

    exec_lines: list[str] = []
    exec_lines.append(f"- Range: {start} ~ {end}")
    exec_lines.append(f"- Total posts: {overview.get('total_post_count', 0)}")
    if show_sentiment:
        exec_lines.append(f"- Negative rate: {pct(float(overview.get('negative_rate', 0.0)))}")
        exec_lines.append(f"- Avg sentiment score: {float(overview.get('weighted_avg_sentiment_score', 0.0)):.3f}")
    if show_spam:
        exec_lines.append(f"- Spam rate: {pct(float(overview.get('spam_rate', 0.0)))}")

    trend_lines = []
    for d in trend:
        trend_lines.append(
            f"- {d['stat_date']}: 总量={d['total_post_count']}，正面={d['positive_count']}，中性={d['neutral_count']}，负面={d['negative_count']}，情感得分={d['weighted_avg_sentiment_score']:.3f}"
        )

    topic_lines = []
    for t in top_topics[:10]:
        topic_lines.append(f"- {t['topic']}: {t['hit_count']}")

    risk_lines = []
    if show_feature and top_features:
        risk_lines.append("负面特征（Top）：")
        for f in top_features[:5]:
            risk_lines.append(f"- {f['feature_name']}: 负面={f['negative_count']}，提及={f['mention_count']}")
    if show_topics and top_topics:
        risk_lines.append("热点话题（Top）：")
        for t in top_topics[:5]:
            risk_lines.append(f"- {t['topic']}: 提及={t['hit_count']}")

    feedback_lines = []
    if show_strategy:
        for r in posts.get("popular", [])[:5]:
            content = (r["content"] or "").strip().replace("\n", " ")
            feedback_lines.append(
            f"- (post_id={r['post_id']}) 点赞={r['like_count'] or 0} 情感={r['sentiment']}: {content[:120]}"
            )

    competitor_lines = []
    if show_competitor:
        for c in competitor:
            competitor_lines.append(
            f"- {c['brand_name']}: 总量={c['total_post_count']} 负面占比={pct(float(c['negative_rate']))} 情感得分={float(c['weighted_avg_sentiment_score']):.3f}"
            )

    strategy_lines = [
        "- 优先修复负面提及最多的特征问题，并同步发布进展。",
        "- 针对高风险关键词与投诉类型，完善各平台的响应话术与节奏。",
        "- 持续追踪情感得分与负面占比，出现异常波动时及时告警与复盘。",
    ]

    parts: list[str] = []
    parts.extend([f"# {title}", "", "## 执行摘要", *exec_lines, ""])

    if show_trend:
        parts.extend(["## 舆情趋势", *(trend_lines if trend_lines else ["- （暂无聚合数据）"]), ""])

    show_risk = bool(show_sentiment or show_spam or show_feature or show_topics)
    if show_risk:
        parts.extend(["## 风险点", *(risk_lines if risk_lines else ["- （暂无风险数据）"]), ""])

    if show_strategy:
        parts.extend(["## 关键用户反馈", *(feedback_lines if feedback_lines else ["- （暂无帖子）"]), ""])

    if show_competitor:
        parts.extend(["## 竞品对比", *(competitor_lines if competitor_lines else ["- （暂无竞品数据）"]), ""])

    if show_strategy:
        parts.extend(["## 策略建议", *strategy_lines, ""])

    if show_topics:
        parts.extend(["## 热点话题", *(topic_lines if topic_lines else ["- （暂无话题数据）"]), ""])

    md = "\n".join(parts)

    summary_parts = [f"总帖子数={overview.get('total_post_count', 0)}"]
    if show_sentiment:
        summary_parts.append(f"负面占比={pct(float(overview.get('negative_rate', 0.0)))}")
    if show_spam:
        summary_parts.append(f"垃圾占比={pct(float(overview.get('spam_rate', 0.0)))}")
    summary = "，".join(summary_parts)
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

DATASET_SECTION_NAME = "dataset"

def _ensure_report_evidence_table(con: sqlite3.Connection) -> None:
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS report_evidence (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              report_id INTEGER NOT NULL,
              post_id INTEGER NOT NULL,
              section_name TEXT,
              quote_reason TEXT,
              sentiment TEXT,
              spam_label TEXT,
              created_at DATETIME
            );
            """
        )
        con.execute("CREATE INDEX IF NOT EXISTS idx_report_evidence_report_id ON report_evidence(report_id);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_report_evidence_post_id ON report_evidence(post_id);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_report_evidence_report_section ON report_evidence(report_id, section_name);")
    except Exception:
        return


def _safe_keywords(values: Optional[list[str]]) -> list[str]:
    out: list[str] = []
    seen = set()
    for v in values or []:
        s = str(v or "").strip()
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def materialize_report_dataset_evidence(con: sqlite3.Connection, report_id: int) -> int:
    """
    Materialize the report's dataset into `report_evidence` (1 row per post).

    Requirement:
    - The report dataset is defined by the report's data range + report_config filters
      (platform/brand/keyword).
    - All matching posts must be stored as evidence for this report.
    """
    _ensure_report_evidence_table(con)
    report = read_report(con, int(report_id))
    cfg = read_report_config(con, int(report_id))

    project_id = int(report["project_id"])
    start = str(report["data_start_date"])
    end = str(report["data_end_date"])

    plat_sql, plat_params = _in_filter("pr.platform_id", cfg.platform_ids)
    brand_sql, brand_params = _in_filter("pr.brand_id", cfg.brand_ids)

    keywords = _safe_keywords(getattr(cfg, "keywords", None))
    kw_sql = ""
    kw_params: list[Any] = []
    if keywords:
        placeholders = ",".join(["?"] * len(keywords))
        # Prefer `post_keyword_result` when available (pipeline output).
        kw_sql = f" AND EXISTS (SELECT 1 FROM post_keyword_result pkr WHERE pkr.post_id=pr.id AND pkr.keyword IN ({placeholders}))"
        kw_params = list(keywords)

    ts = now_ts()
    con.execute("DELETE FROM report_evidence WHERE report_id=?;", (int(report_id),))

    sql = f"""
        INSERT INTO report_evidence(
          report_id, post_id, section_name, quote_reason, sentiment, spam_label, created_at
        )
        SELECT
          ? AS report_id,
          pr.id AS post_id,
          ? AS section_name,
          '' AS quote_reason,
          ps.sentiment AS sentiment,
          sp.spam_label AS spam_label,
          ? AS created_at
        FROM post_raw pr
        LEFT JOIN post_clean_result pc ON pc.post_id=pr.id
        LEFT JOIN post_sentiment_result ps ON ps.post_id=pr.id
        LEFT JOIN post_spam_result sp ON sp.post_id=pr.id
        WHERE pr.project_id=?
          AND date(COALESCE(pr.publish_time, pr.crawled_at)) BETWEEN ? AND ?
          {plat_sql}
          {brand_sql}
          {kw_sql}
          AND COALESCE(pc.is_valid, 1)=1;
    """

    params: list[Any] = [
        int(report_id),
        DATASET_SECTION_NAME,
        ts,
        project_id,
        start,
        end,
        *plat_params,
        *brand_params,
        *kw_params,
    ]

    try:
        cur = con.execute(sql, params)
        return int(cur.rowcount or 0)
    except sqlite3.Error as e:
        # If keyword table is missing, fall back to naive LIKE matching on post text.
        msg = str(e).lower()
        if keywords and ("no such table" in msg and "post_keyword_result" in msg):
            like_clauses: list[str] = []
            like_params: list[Any] = []
            for kw in keywords:
                like_clauses.append("(COALESCE(pr.title,'') LIKE ? OR COALESCE(pr.content,'') LIKE ?)")
                like_params.extend([f"%{kw}%", f"%{kw}%"])
            like_sql = " AND (" + " OR ".join(like_clauses) + ")" if like_clauses else ""
            sql2 = sql.replace(kw_sql, like_sql)
            params2 = [
                int(report_id),
                DATASET_SECTION_NAME,
                ts,
                project_id,
                start,
                end,
                *plat_params,
                *brand_params,
                *like_params,
            ]
            cur2 = con.execute(sql2, params2)
            return int(cur2.rowcount or 0)
        raise


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


def _date_axis(start: str, end: str) -> list[str]:
    s = datetime.strptime(str(start), "%Y-%m-%d").date()
    e = datetime.strptime(str(end), "%Y-%m-%d").date()
    out: list[str] = []
    cur = s
    while cur <= e:
        out.append(cur.strftime("%Y-%m-%d"))
        cur = cur.fromordinal(cur.toordinal() + 1)
    return out


def fetch_evidence_overview_by_brand(con: sqlite3.Connection, report_id: int) -> list[dict[str, Any]]:
    rows = con.execute(
        """
        SELECT
          pr.brand_id AS brand_id,
          COUNT(1) AS total_post_count,
          SUM(CASE WHEN COALESCE(sp.spam_label,'')='spam' THEN 1 ELSE 0 END) AS spam_post_count,
          SUM(CASE WHEN COALESCE(ps.sentiment,'')='positive' THEN 1 ELSE 0 END) AS positive_count,
          SUM(CASE WHEN COALESCE(ps.sentiment,'')='neutral'  THEN 1 ELSE 0 END) AS neutral_count,
          SUM(CASE WHEN COALESCE(ps.sentiment,'')='negative' THEN 1 ELSE 0 END) AS negative_count,
          SUM(COALESCE(pr.like_count,0)) AS total_like_count,
          SUM(COALESCE(pr.comment_count,0)) AS total_comment_count,
          SUM(COALESCE(pr.share_count,0)) AS total_share_count,
          AVG(COALESCE(ps.sentiment_score, 0.0)) AS avg_sentiment_score
        FROM report_evidence re
        JOIN post_raw pr ON pr.id=re.post_id
        LEFT JOIN post_sentiment_result ps ON ps.post_id=pr.id
        LEFT JOIN post_spam_result sp ON sp.post_id=pr.id
        WHERE re.report_id=?
          AND re.section_name=?
        GROUP BY pr.brand_id
        ORDER BY total_post_count DESC, pr.brand_id ASC;
        """,
        (int(report_id), DATASET_SECTION_NAME),
    ).fetchall()

    items: list[dict[str, Any]] = []
    for r in rows:
        total = int(r["total_post_count"] or 0)
        spam = int(r["spam_post_count"] or 0)
        items.append(
            {
                "brand_id": r["brand_id"],
                "total_post_count": total,
                "valid_post_count": total,
                "spam_post_count": spam,
                "spam_rate": (spam / total) if total else 0.0,
                "positive_count": int(r["positive_count"] or 0),
                "neutral_count": int(r["neutral_count"] or 0),
                "negative_count": int(r["negative_count"] or 0),
                "weighted_avg_sentiment_score": float(r["avg_sentiment_score"] or 0.0),
                "keyword_hit_count": 0,
                "total_like_count": int(r["total_like_count"] or 0),
                "total_comment_count": int(r["total_comment_count"] or 0),
                "total_share_count": int(r["total_share_count"] or 0),
            }
        )
    return items


def fetch_evidence_sentiment_trend_daily_by_brand(
    con: sqlite3.Connection, report_id: int, *, top_n: int = 4
) -> dict[str, Any]:
    report = read_report(con, int(report_id))
    start = str(report["data_start_date"])
    end = str(report["data_end_date"])
    dates = _date_axis(start, end)
    if not dates:
        return {"dates": [], "series": []}

    top_rows = con.execute(
        """
        SELECT pr.brand_id AS brand_id, COUNT(1) AS total_post_count
        FROM report_evidence re
        JOIN post_raw pr ON pr.id=re.post_id
        WHERE re.report_id=? AND re.section_name=?
        GROUP BY pr.brand_id
        ORDER BY total_post_count DESC, pr.brand_id ASC
        LIMIT ?;
        """,
        (int(report_id), DATASET_SECTION_NAME, int(top_n)),
    ).fetchall()
    brand_ids = [int(r["brand_id"]) for r in top_rows if r["brand_id"] is not None]
    if not brand_ids:
        return {"dates": [], "series": []}

    placeholders = ",".join(["?"] * len(brand_ids))
    rows = con.execute(
        f"""
        SELECT
          date(COALESCE(pr.publish_time, pr.crawled_at)) AS stat_date,
          pr.brand_id AS brand_id,
          COUNT(1) AS total_post_count,
          SUM(CASE WHEN COALESCE(ps.sentiment,'')='positive' THEN 1 ELSE 0 END) AS positive_count,
          SUM(CASE WHEN COALESCE(ps.sentiment,'')='negative' THEN 1 ELSE 0 END) AS negative_count
        FROM report_evidence re
        JOIN post_raw pr ON pr.id=re.post_id
        LEFT JOIN post_sentiment_result ps ON ps.post_id=pr.id
        WHERE re.report_id=? AND re.section_name=?
          AND pr.brand_id IN ({placeholders})
        GROUP BY stat_date, pr.brand_id
        ORDER BY stat_date ASC, pr.brand_id ASC;
        """,
        (int(report_id), DATASET_SECTION_NAME, *brand_ids),
    ).fetchall()

    idx = {d: i for i, d in enumerate(dates)}
    series_map: dict[int, dict[str, list[int]]] = {
        bid: {
            "total_post_count": [0] * len(dates),
            "positive_count": [0] * len(dates),
            "negative_count": [0] * len(dates),
        }
        for bid in brand_ids
    }
    for r in rows:
        d = str(r["stat_date"])
        bid = int(r["brand_id"])
        i = idx.get(d)
        if i is None or bid not in series_map:
            continue
        series_map[bid]["total_post_count"][i] = int(r["total_post_count"] or 0)
        series_map[bid]["positive_count"][i] = int(r["positive_count"] or 0)
        series_map[bid]["negative_count"][i] = int(r["negative_count"] or 0)

    return {"dates": dates, "series": [{"brand_id": bid, **series_map[bid]} for bid in brand_ids]}


def fetch_evidence_topic_monitor_stacked(
    con: sqlite3.Connection, report_id: int, *, top_n: int = 15
) -> dict[str, Any]:
    report = read_report(con, int(report_id))
    start = str(report["data_start_date"])
    end = str(report["data_end_date"])
    dates = _date_axis(start, end)
    if not dates:
        return {"dates": [], "series": []}

    # Prefer topic_result when available.
    try:
        top_rows = con.execute(
            """
            SELECT tr.topic AS topic, COUNT(1) AS hit_count
            FROM report_evidence re
            JOIN topic_result tr ON tr.post_id=re.post_id
            WHERE re.report_id=? AND re.section_name=?
            GROUP BY tr.topic
            ORDER BY hit_count DESC, tr.topic ASC
            LIMIT ?;
            """,
            (int(report_id), DATASET_SECTION_NAME, int(top_n)),
        ).fetchall()
        topics = [str(r["topic"]) for r in top_rows if str(r["topic"] or "").strip() != ""]
        if not topics:
            return {"dates": [], "series": []}

        placeholders = ",".join(["?"] * len(topics))
        rows = con.execute(
            f"""
            SELECT
              date(COALESCE(pr.publish_time, pr.crawled_at)) AS stat_date,
              tr.topic AS topic,
              COUNT(1) AS hit_count
            FROM report_evidence re
            JOIN post_raw pr ON pr.id=re.post_id
            JOIN topic_result tr ON tr.post_id=re.post_id
            WHERE re.report_id=? AND re.section_name=?
              AND tr.topic IN ({placeholders})
            GROUP BY stat_date, tr.topic
            ORDER BY stat_date ASC, tr.topic ASC;
            """,
            (int(report_id), DATASET_SECTION_NAME, *topics),
        ).fetchall()

        idx = {d: i for i, d in enumerate(dates)}
        series_map: dict[str, list[int]] = {tp: [0] * len(dates) for tp in topics}
        for r in rows:
            d = str(r["stat_date"])
            tp = str(r["topic"])
            if tp in series_map and d in idx:
                series_map[tp][idx[d]] = int(r["hit_count"] or 0)
        return {
            "dates": dates,
            "series": [{"keyword": tp, "data": series_map[tp]} for tp in topics],
        }
    except sqlite3.Error:
        pass

    # Fallback to keyword hits when topic tables are unavailable.
    try:
        top_rows = con.execute(
            """
            SELECT pkr.keyword AS keyword, COUNT(DISTINCT re.post_id) AS post_count
            FROM report_evidence re
            JOIN post_keyword_result pkr ON pkr.post_id=re.post_id
            WHERE re.report_id=? AND re.section_name=?
            GROUP BY pkr.keyword
            ORDER BY post_count DESC, pkr.keyword ASC
            LIMIT ?;
            """,
            (int(report_id), DATASET_SECTION_NAME, int(top_n)),
        ).fetchall()
        kws = [str(r["keyword"]) for r in top_rows if str(r["keyword"] or "").strip() != ""]
        if not kws:
            return {"dates": [], "series": []}
        placeholders = ",".join(["?"] * len(kws))
        rows = con.execute(
            f"""
            SELECT
              date(COALESCE(pr.publish_time, pr.crawled_at)) AS stat_date,
              pkr.keyword AS keyword,
              COUNT(DISTINCT re.post_id) AS post_count
            FROM report_evidence re
            JOIN post_raw pr ON pr.id=re.post_id
            JOIN post_keyword_result pkr ON pkr.post_id=re.post_id
            WHERE re.report_id=? AND re.section_name=?
              AND pkr.keyword IN ({placeholders})
            GROUP BY stat_date, pkr.keyword
            ORDER BY stat_date ASC, pkr.keyword ASC;
            """,
            (int(report_id), DATASET_SECTION_NAME, *kws),
        ).fetchall()
        idx = {d: i for i, d in enumerate(dates)}
        series_map: dict[str, list[int]] = {kw: [0] * len(dates) for kw in kws}
        for r in rows:
            d = str(r["stat_date"])
            kw = str(r["keyword"])
            if kw in series_map and d in idx:
                series_map[kw][idx[d]] = int(r["post_count"] or 0)
        return {"dates": dates, "series": [{"keyword": kw, "data": series_map[kw]} for kw in kws]}
    except sqlite3.Error:
        return {"dates": [], "series": []}


def fetch_evidence_risk_keywords(con: sqlite3.Connection, report_id: int, *, top_n: int = 20) -> list[dict[str, Any]]:
    """
    Risk board:
    - keywords mentioned by negative-sentiment posts in this report evidence dataset.
    """
    try:
        rows = con.execute(
            f"""
            SELECT
              pkr.keyword AS keyword,
              COUNT(DISTINCT re.post_id) AS post_count
            FROM report_evidence re
            JOIN post_sentiment_result ps ON ps.post_id=re.post_id
            JOIN post_keyword_result pkr ON pkr.post_id=re.post_id
            WHERE re.report_id=? AND re.section_name=?
              AND COALESCE(ps.sentiment,'')='negative'
            GROUP BY pkr.keyword
            ORDER BY post_count DESC, pkr.keyword ASC
            LIMIT ?;
            """,
            (int(report_id), DATASET_SECTION_NAME, int(top_n)),
        ).fetchall()
        return [{"keyword": r["keyword"], "post_count": int(r["post_count"] or 0)} for r in rows]
    except sqlite3.Error:
        return []


def fetch_evidence_key_feedback_posts(
    con: sqlite3.Connection, report_id: int, *, limit: int = 20, max_chars: int = 220
) -> list[dict[str, Any]]:
    rows = con.execute(
        """
        SELECT
          pr.id AS post_id,
          pr.platform_id,
          pl.name AS platform_name,
          pr.publish_time,
          pr.title,
          pr.content,
          pr.post_url,
          COALESCE(pr.like_count,0) AS like_count,
          COALESCE(pr.comment_count,0) AS comment_count,
          COALESCE(pr.share_count,0) AS share_count,
          ps.sentiment AS sentiment
        FROM report_evidence re
        JOIN post_raw pr ON pr.id=re.post_id
        LEFT JOIN platform pl ON pl.id=pr.platform_id
        LEFT JOIN post_sentiment_result ps ON ps.post_id=pr.id
        WHERE re.report_id=? AND re.section_name=?
        ORDER BY (COALESCE(pr.like_count,0) + COALESCE(pr.comment_count,0) * 2 + COALESCE(pr.share_count,0) * 3) DESC, pr.id DESC
        LIMIT ?;
        """,
        (int(report_id), DATASET_SECTION_NAME, int(limit)),
    ).fetchall()

    items: list[dict[str, Any]] = []
    for r in rows:
        content = str(r["content"] or "").strip().replace("\n", " ")
        if len(content) > int(max_chars):
            content = content[: int(max_chars)] + "..."
        items.append(
            {
                "post_id": int(r["post_id"]),
                "platform_id": int(r["platform_id"]) if r["platform_id"] is not None else None,
                "platform_name": r["platform_name"],
                "publish_time": r["publish_time"],
                "title": r["title"],
                "content_excerpt": content,
                "post_url": r["post_url"],
                "sentiment": r["sentiment"],
                "like_count": int(r["like_count"] or 0),
                "comment_count": int(r["comment_count"] or 0),
                "share_count": int(r["share_count"] or 0),
                "reason": "高互动",
            }
        )
    return items


def generate_report(con: sqlite3.Connection, report_id: int) -> dict[str, Any]:
    report = read_report(con, report_id)
    cfg = read_report_config(con, report_id)

    project_id = int(report["project_id"])
    start = str(report["data_start_date"])
    end = str(report["data_end_date"])

    set_report_status(con, report_id, "running")

    overview = fetch_agg_overview(con, project_id, start, end, cfg)
    trend = fetch_sentiment_trend(con, project_id, start, end, cfg) if cfg.include_trend else []
    top_topics = fetch_top_topics(con, project_id, start, end, cfg) if cfg.include_topics else []
    top_features = fetch_top_negative_features(con, project_id, start, end, cfg) if cfg.include_feature_analysis else []
    competitor = fetch_competitor_compare(con, project_id, start, end, cfg) if cfg.include_competitor_compare else []
    posts = fetch_candidate_posts(con, project_id, start, end, cfg, limit_each=6)

    summary, content_md = llm_mock_generate_markdown(
        report=report,
        overview=overview,
        trend=trend,
        top_topics=top_topics,
        top_features=top_features,
        competitor=competitor,
        posts=posts,
        cfg=cfg,
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

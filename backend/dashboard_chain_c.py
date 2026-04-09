# 作用：后端链路：仪表盘分析链路编排（链路 C）。

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional

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
            con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            try:
                row = con.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='project' LIMIT 1;"
                ).fetchone()
                return row is not None
            finally:
                con.close()
        except sqlite3.Error:
            return False

    if has_project_table(db_path):
        return db_path

    folder = os.path.dirname(db_path) or "."
    base = os.path.basename(db_path)
    candidates = []
    if base == "database.sqlite":
        candidates.append(os.path.join(folder, "database..sqlite"))
    candidates.append(os.path.join(folder, "database.sqlite"))
    candidates.append(os.path.join(folder, "database..sqlite"))
    for c in candidates:
        if c != db_path and os.path.exists(c) and has_project_table(c):
            return c

    return db_path


@dataclass(frozen=True)
class DateRange:
    start_date: str  # YYYY-MM-DD
    end_date: str  # YYYY-MM-DD


def parse_date(value: str) -> str:
    datetime.strptime(value, "%Y-%m-%d")
    return value


def default_date_range(days: int = 7) -> DateRange:
    end = datetime.utcnow().date()
    start = end - timedelta(days=days - 1)
    return DateRange(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))


def list_enabled_projects(con: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = con.execute(
        """
        SELECT id, name, product_category, status, last_refresh_at
        FROM project
        WHERE is_active=1 AND deleted_at IS NULL
        ORDER BY id;
        """
    ).fetchall()
    return [
        {
            "id": int(r["id"]),
            "name": r["name"],
            "product_category": r["product_category"],
            "status": r["status"],
            "last_refresh_at": r["last_refresh_at"],
        }
        for r in rows
    ]


def list_project_platforms(con: sqlite3.Connection, project_id: int) -> list[dict[str, Any]]:
    rows = con.execute(
        """
        SELECT p.id, p.code, p.name, p.is_enabled
        FROM project_platform pp
        JOIN platform p ON p.id = pp.platform_id
        WHERE pp.project_id=?
        ORDER BY p.id;
        """,
        (project_id,),
    ).fetchall()
    return [
        {"id": int(r["id"]), "code": r["code"], "name": r["name"], "is_enabled": int(r["is_enabled"] or 0)}
        for r in rows
    ]


def list_project_brands(con: sqlite3.Connection, project_id: int) -> list[dict[str, Any]]:
    rows = con.execute(
        """
        SELECT b.id, b.name, b.alias, b.category, pb.is_core_brand
        FROM project_brand pb
        JOIN brand b ON b.id = pb.brand_id
        WHERE pb.project_id=?
        ORDER BY COALESCE(pb.is_core_brand, 0) DESC, b.id;
        """,
        (project_id,),
    ).fetchall()
    return [
        {
            "id": int(r["id"]),
            "name": r["name"],
            "alias": r["alias"],
            "category": r["category"],
            "is_core_brand": int(r["is_core_brand"] or 0),
        }
        for r in rows
    ]


def _metric_filters(brand_id: Optional[int], platform_id: Optional[int]) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if brand_id is not None:
        clauses.append("brand_id=?")
        params.append(int(brand_id))
    if platform_id is not None:
        clauses.append("platform_id=?")
        params.append(int(platform_id))
    if not clauses:
        return "", []
    return " AND " + " AND ".join(clauses), params


def fetch_overview(
    con: sqlite3.Connection,
    project_id: int,
    date_range: DateRange,
    brand_id: Optional[int],
    platform_id: Optional[int],
) -> dict[str, Any]:
    where_extra, extra_params = _metric_filters(brand_id, platform_id)
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
          SUM(COALESCE(total_like_count,0)) AS total_like_count,
          SUM(COALESCE(total_comment_count,0)) AS total_comment_count,
          SUM(COALESCE(total_share_count,0)) AS total_share_count,
          CASE
            WHEN SUM(COALESCE(total_post_count,0))=0 THEN 0.0
            ELSE SUM(COALESCE(avg_sentiment_score,0.0) * COALESCE(total_post_count,0))
                 / SUM(COALESCE(total_post_count,0))
          END AS weighted_avg_sentiment_score
        FROM daily_metric
        WHERE project_id=?
          AND stat_date BETWEEN ? AND ?
          {where_extra};
        """,
        (project_id, date_range.start_date, date_range.end_date, *extra_params),
    ).fetchone()
    if not row:
        return {"has_data": False}

    total = int(row["total_post_count"] or 0)
    spam = int(row["spam_post_count"] or 0)
    valid = int(row["valid_post_count"] or 0)
    return {
        "has_data": total > 0,
        "total_post_count": total,
        "valid_post_count": valid,
        "spam_post_count": spam,
        "spam_rate": (spam / total) if total else 0.0,
        "positive_count": int(row["positive_count"] or 0),
        "neutral_count": int(row["neutral_count"] or 0),
        "negative_count": int(row["negative_count"] or 0),
        "weighted_avg_sentiment_score": float(row["weighted_avg_sentiment_score"] or 0.0),
        "keyword_hit_count": int(row["keyword_count"] or 0),
        "total_like_count": int(row["total_like_count"] or 0),
        "total_comment_count": int(row["total_comment_count"] or 0),
        "total_share_count": int(row["total_share_count"] or 0),
    }


def fetch_sentiment_trend(
    con: sqlite3.Connection,
    project_id: int,
    date_range: DateRange,
    brand_id: Optional[int],
    platform_id: Optional[int],
) -> list[dict[str, Any]]:
    where_extra, extra_params = _metric_filters(brand_id, platform_id)
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
          {where_extra}
        GROUP BY stat_date
        ORDER BY stat_date;
        """,
        (project_id, date_range.start_date, date_range.end_date, *extra_params),
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


def fetch_keyword_monitor(
    con: sqlite3.Connection,
    project_id: int,
    date_range: DateRange,
    brand_id: Optional[int],
    platform_id: Optional[int],
    top_n: int = 20,
) -> list[dict[str, Any]]:
    clauses: list[str] = ["project_id=?", "stat_date BETWEEN ? AND ?"]
    params: list[Any] = [project_id, date_range.start_date, date_range.end_date]
    if brand_id is not None:
        clauses.append("brand_id=?")
        params.append(int(brand_id))
    if platform_id is not None:
        clauses.append("platform_id=?")
        params.append(int(platform_id))

    rows = con.execute(
        f"""
        SELECT keyword, SUM(COALESCE(hit_count,0)) AS hit_count
        FROM daily_keyword_metric
        WHERE {" AND ".join(clauses)}
        GROUP BY keyword
        ORDER BY hit_count DESC, keyword ASC
        LIMIT ?;
        """,
        (*params, int(top_n)),
    ).fetchall()
    return [{"keyword": r["keyword"], "hit_count": int(r["hit_count"] or 0)} for r in rows]


def dashboard_load(con: sqlite3.Connection) -> dict[str, Any]:
    return {"projects": list_enabled_projects(con)}


def dashboard_project_options(con: sqlite3.Connection, project_id: int) -> dict[str, Any]:
    return {
        "project_id": project_id,
        "brands": list_project_brands(con, project_id),
        "platforms": list_project_platforms(con, project_id),
    }


def dashboard_query(
    con: sqlite3.Connection,
    project_id: int,
    date_range: DateRange,
    brand_id: Optional[int],
    platform_id: Optional[int],
) -> dict[str, Any]:
    overview = fetch_overview(con, project_id, date_range, brand_id, platform_id)
    trend = fetch_sentiment_trend(con, project_id, date_range, brand_id, platform_id)
    keyword_monitor = fetch_keyword_monitor(con, project_id, date_range, brand_id, platform_id, top_n=20)
    return {
        "project_id": project_id,
        "filters": {
            "start_date": date_range.start_date,
            "end_date": date_range.end_date,
            "brand_id": brand_id,
            "platform_id": platform_id,
        },
        "overview": overview,
        "sentiment_trend": trend,
        "keyword_monitor": keyword_monitor,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="主链路C：仪表盘展示链路（优先读聚合表）")
    parser.add_argument("--db", required=True)
    parser.add_argument("--action", choices=["load", "options", "query"], required=True)
    parser.add_argument("--project-id", type=int, default=None)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--brand-id", type=int, default=None)
    parser.add_argument("--platform-id", type=int, default=None)
    parser.add_argument("--days", type=int, default=7, help="default date range when start/end not provided")
    args = parser.parse_args()

    db_path = resolve_db_path(str(args.db))
    con = connect(db_path)
    try:
        if args.action == "load":
            out = dashboard_load(con)
            print(json.dumps({"ok": True, "db": db_path, **out}, ensure_ascii=False))
            return

        if args.project_id is None:
            raise SystemExit("--project-id is required for action options/query")

        if args.action == "options":
            out = dashboard_project_options(con, int(args.project_id))
            print(json.dumps({"ok": True, "db": db_path, **out}, ensure_ascii=False))
            return

        if args.action == "query":
            if args.start_date and args.end_date:
                dr = DateRange(parse_date(args.start_date), parse_date(args.end_date))
            else:
                dr = default_date_range(days=int(args.days))
            out = dashboard_query(
                con,
                int(args.project_id),
                dr,
                int(args.brand_id) if args.brand_id is not None else None,
                int(args.platform_id) if args.platform_id is not None else None,
            )
            print(json.dumps({"ok": True, "db": db_path, **out}, ensure_ascii=False))
            return
    finally:
        con.close()


if __name__ == "__main__":
    main()

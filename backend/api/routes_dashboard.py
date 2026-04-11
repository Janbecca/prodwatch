# 作用：后端 API：仪表盘相关路由与接口实现。

from __future__ import annotations

from datetime import datetime, timedelta
import sqlite3
import time
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.api.db import get_db
from backend.api.params import DateRange, in_filter, parse_date_range


router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


def _range_where(date_range: DateRange) -> tuple[str, list[Any]]:
    return " AND stat_date BETWEEN ? AND ?", [date_range.start_date, date_range.end_date]


def _sqlite_error(e: Exception) -> HTTPException:
    return HTTPException(status_code=500, detail=f"SQLite error: {e}")


def _is_locked_error(e: sqlite3.Error) -> bool:
    msg = str(e).lower()
    return "database is locked" in msg or "database is busy" in msg or "locked" == msg


def _fetchall_retry(db: sqlite3.Connection, sql: str, params: list[Any] | tuple[Any, ...]):
    delays = [0.05, 0.1, 0.2]
    for i in range(len(delays) + 1):
        try:
            return db.execute(sql, params).fetchall()
        except sqlite3.OperationalError as e:
            if not _is_locked_error(e) or i >= len(delays):
                raise
            time.sleep(delays[i])


@router.get("/overview_by_brand")
def dashboard_overview_by_brand(
    project_id: int = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    platform_ids: Optional[list[int]] = Query(None),
    brand_ids: Optional[list[int]] = Query(None),
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    try:
        dr = parse_date_range(start_date, end_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    plat_sql, plat_params = in_filter("platform_id", platform_ids)
    brand_sql, brand_params = in_filter("brand_id", brand_ids)
    range_sql, range_params = _range_where(dr)
    params = [project_id, *range_params, *plat_params, *brand_params]

    try:
        rows = _fetchall_retry(
            db,
            f"""
            SELECT
              brand_id,
              SUM(COALESCE(total_post_count,0)) AS total_post_count,
              SUM(COALESCE(valid_post_count,0)) AS valid_post_count,
              SUM(COALESCE(spam_post_count,0)) AS spam_post_count,
              SUM(COALESCE(positive_count,0)) AS positive_count,
              SUM(COALESCE(neutral_count,0))  AS neutral_count,
              SUM(COALESCE(negative_count,0)) AS negative_count,
              SUM(COALESCE(total_like_count,0)) AS total_like_count,
              SUM(COALESCE(total_comment_count,0)) AS total_comment_count,
              SUM(COALESCE(total_share_count,0)) AS total_share_count,
              SUM(COALESCE(keyword_count,0)) AS keyword_count,
              CASE
                WHEN SUM(COALESCE(total_post_count,0))=0 THEN 0.0
                ELSE SUM(COALESCE(avg_sentiment_score,0.0) * COALESCE(total_post_count,0))
                     / SUM(COALESCE(total_post_count,0))
              END AS weighted_avg_sentiment_score
            FROM daily_metric
            WHERE project_id=?
              {range_sql}
              {plat_sql}
              {brand_sql}
            GROUP BY brand_id
            ORDER BY total_post_count DESC, brand_id ASC;
            """,
            params,
        )
    except sqlite3.Error as e:
        if _is_locked_error(e):
            raise HTTPException(status_code=503, detail=f"SQLite busy: {e}")
        raise _sqlite_error(e)

    items = []
    for r in rows:
        total = int(r["total_post_count"] or 0)
        spam = int(r["spam_post_count"] or 0)
        items.append(
            {
                "brand_id": r["brand_id"],
                "total_post_count": total,
                "valid_post_count": int(r["valid_post_count"] or 0),
                "spam_post_count": spam,
                "spam_rate": (spam / total) if total else 0.0,
                "positive_count": int(r["positive_count"] or 0),
                "neutral_count": int(r["neutral_count"] or 0),
                "negative_count": int(r["negative_count"] or 0),
                "weighted_avg_sentiment_score": float(r["weighted_avg_sentiment_score"] or 0.0),
                "keyword_hit_count": int(r["keyword_count"] or 0),
                "total_like_count": int(r["total_like_count"] or 0),
                "total_comment_count": int(r["total_comment_count"] or 0),
                "total_share_count": int(r["total_share_count"] or 0),
            }
        )

    return {
        "project_id": project_id,
        "date_range": {"start_date": dr.start_date, "end_date": dr.end_date},
        "filters": {"platform_ids": platform_ids, "brand_ids": brand_ids},
        "items": items,
    }


@router.get("/sentiment_trend_daily")
def dashboard_sentiment_trend_daily(
    project_id: int = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    platform_ids: Optional[list[int]] = Query(None),
    brand_ids: Optional[list[int]] = Query(None),
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    try:
        dr = parse_date_range(start_date, end_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    plat_sql, plat_params = in_filter("platform_id", platform_ids)
    brand_sql, brand_params = in_filter("brand_id", brand_ids)
    range_sql, range_params = _range_where(dr)
    params = [project_id, *range_params, *plat_params, *brand_params]

    try:
        rows = _fetchall_retry(
            db,
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
              {range_sql}
              {plat_sql}
              {brand_sql}
            GROUP BY stat_date
            ORDER BY stat_date ASC;
            """,
            params,
        )
    except sqlite3.Error as e:
        if _is_locked_error(e):
            raise HTTPException(status_code=503, detail=f"SQLite busy: {e}")
        raise _sqlite_error(e)

    items = [
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
    return {
        "project_id": project_id,
        "date_range": {"start_date": dr.start_date, "end_date": dr.end_date},
        "filters": {"platform_ids": platform_ids, "brand_ids": brand_ids},
        "items": items,
    }


@router.get("/sentiment_trend_daily_by_brand")
def dashboard_sentiment_trend_daily_by_brand(
    project_id: int = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    platform_ids: Optional[list[int]] = Query(None),
    brand_ids: Optional[list[int]] = Query(None),
    top_n: int = Query(4, ge=1, le=20),
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    """
    Daily sentiment trend grouped by brand (series dimension = brand).

    Data source: daily_metric (aggregated table).
    Frontend can compute ratios (e.g. positive/negative share) from counts.
    """
    try:
        dr = parse_date_range(start_date, end_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    plat_sql, plat_params = in_filter("platform_id", platform_ids)
    range_sql, range_params = _range_where(dr)

    effective_brand_ids: list[int] = []
    if brand_ids is None:
        # No explicit brand filter -> pick top brands to keep series count bounded.
        try:
            top_rows = _fetchall_retry(
                db,
                f"""
                SELECT brand_id, SUM(COALESCE(total_post_count,0)) AS total_post_count
                FROM daily_metric
                WHERE project_id=?
                  {range_sql}
                  {plat_sql}
                GROUP BY brand_id
                ORDER BY total_post_count DESC, brand_id ASC
                LIMIT ?;
                """,
                (project_id, *range_params, *plat_params, int(top_n)),
            )
        except sqlite3.Error as e:
            if _is_locked_error(e):
                raise HTTPException(status_code=503, detail=f"SQLite busy: {e}")
            raise _sqlite_error(e)
        effective_brand_ids = [int(r["brand_id"]) for r in top_rows if r["brand_id"] is not None]
    elif len(brand_ids) == 0:
        effective_brand_ids = []
    else:
        effective_brand_ids = [int(x) for x in brand_ids]

    if not effective_brand_ids:
        return {
            "project_id": project_id,
            "date_range": {"start_date": dr.start_date, "end_date": dr.end_date},
            "filters": {"platform_ids": platform_ids, "brand_ids": brand_ids},
            "dates": [],
            "series": [],
        }

    brand_sql, brand_params = in_filter("brand_id", effective_brand_ids)
    params = [project_id, *range_params, *plat_params, *brand_params]
    try:
        rows = _fetchall_retry(
            db,
            f"""
            SELECT
              stat_date,
              brand_id,
              SUM(COALESCE(total_post_count,0)) AS total_post_count,
              SUM(COALESCE(positive_count,0)) AS positive_count,
              SUM(COALESCE(negative_count,0)) AS negative_count
            FROM daily_metric
            WHERE project_id=?
              {range_sql}
              {plat_sql}
              {brand_sql}
            GROUP BY stat_date, brand_id
            ORDER BY stat_date ASC, brand_id ASC;
            """,
            params,
        )
    except sqlite3.Error as e:
        if _is_locked_error(e):
            raise HTTPException(status_code=503, detail=f"SQLite busy: {e}")
        raise _sqlite_error(e)

    start_dt = datetime.strptime(dr.start_date, "%Y-%m-%d").date()
    end_dt = datetime.strptime(dr.end_date, "%Y-%m-%d").date()
    dates = []
    cur = start_dt
    while cur <= end_dt:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)

    idx = {d: i for i, d in enumerate(dates)}
    series_map: dict[int, dict[str, list[int]]] = {
        bid: {
            "total_post_count": [0] * len(dates),
            "positive_count": [0] * len(dates),
            "negative_count": [0] * len(dates),
        }
        for bid in effective_brand_ids
    }

    for r in rows:
        d = str(r["stat_date"])
        bid = int(r["brand_id"])
        i = idx.get(d)
        if i is None:
            continue
        if bid not in series_map:
            continue
        series_map[bid]["total_post_count"][i] = int(r["total_post_count"] or 0)
        series_map[bid]["positive_count"][i] = int(r["positive_count"] or 0)
        series_map[bid]["negative_count"][i] = int(r["negative_count"] or 0)

    return {
        "project_id": project_id,
        "date_range": {"start_date": dr.start_date, "end_date": dr.end_date},
        "filters": {"platform_ids": platform_ids, "brand_ids": brand_ids},
        "dates": dates,
        "series": [
            {"brand_id": bid, **series_map[bid]} for bid in effective_brand_ids if bid in series_map
        ],
    }


@router.get("/keyword_monitor_stacked")
def dashboard_keyword_monitor_stacked(
    project_id: int = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    platform_ids: Optional[list[int]] = Query(None),
    brand_ids: Optional[list[int]] = Query(None),
    top_n: int = Query(15, ge=1, le=50),
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    try:
        dr = parse_date_range(start_date, end_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    plat_sql, plat_params = in_filter("platform_id", platform_ids)
    brand_sql, brand_params = in_filter("brand_id", brand_ids)
    params_base = [project_id, dr.start_date, dr.end_date, *plat_params, *brand_params]

    try:
        top_rows = _fetchall_retry(
            db,
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
            (*params_base, int(top_n)),
        )
    except sqlite3.Error as e:
        msg = str(e).lower()
        if ("no such table" in msg and "daily_keyword_metric" in msg) or ("no such column" in msg):
            return {
                "project_id": project_id,
                "date_range": {"start_date": dr.start_date, "end_date": dr.end_date},
                "filters": {"platform_ids": platform_ids, "brand_ids": brand_ids},
                "dates": [],
                "series": [],
            }
        if _is_locked_error(e):
            raise HTTPException(status_code=503, detail=f"SQLite busy: {e}")
        raise _sqlite_error(e)

    keywords = [str(r["keyword"]) for r in top_rows]
    if not keywords:
        return {
            "project_id": project_id,
            "date_range": {"start_date": dr.start_date, "end_date": dr.end_date},
            "filters": {"platform_ids": platform_ids, "brand_ids": brand_ids},
            "dates": [],
            "series": [],
        }

    kw_sql, kw_params = in_filter("keyword", keywords)
    try:
        rows = _fetchall_retry(
            db,
            f"""
            SELECT stat_date, keyword, SUM(COALESCE(hit_count,0)) AS hit_count
            FROM daily_keyword_metric
            WHERE project_id=?
              AND stat_date BETWEEN ? AND ?
              {plat_sql}
              {brand_sql}
              {kw_sql}
            GROUP BY stat_date, keyword
            ORDER BY stat_date ASC, keyword ASC;
            """,
            (*params_base, *kw_params),
        )
    except sqlite3.Error as e:
        msg = str(e).lower()
        if ("no such table" in msg and "daily_keyword_metric" in msg) or ("no such column" in msg):
            return {
                "project_id": project_id,
                "date_range": {"start_date": dr.start_date, "end_date": dr.end_date},
                "filters": {"platform_ids": platform_ids, "brand_ids": brand_ids},
                "dates": [],
                "series": [],
            }
        if _is_locked_error(e):
            raise HTTPException(status_code=503, detail=f"SQLite busy: {e}")
        raise _sqlite_error(e)

    start_dt = datetime.strptime(dr.start_date, "%Y-%m-%d").date()
    end_dt = datetime.strptime(dr.end_date, "%Y-%m-%d").date()
    dates: list[str] = []
    cur = start_dt
    while cur <= end_dt:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)

    idx = {d: i for i, d in enumerate(dates)}
    series_map: dict[str, list[int]] = {kw: [0] * len(dates) for kw in keywords}
    for r in rows:
        d = str(r["stat_date"])
        kw = str(r["keyword"])
        if kw in series_map and d in idx:
            series_map[kw][idx[d]] = int(r["hit_count"] or 0)

    return {
        "project_id": project_id,
        "date_range": {"start_date": dr.start_date, "end_date": dr.end_date},
        "filters": {"platform_ids": platform_ids, "brand_ids": brand_ids},
        "dates": dates,
        "series": [{"keyword": kw, "data": series_map[kw]} for kw in keywords],
    }


@router.get("/topic_monitor_stacked")
def dashboard_topic_monitor_stacked(
    project_id: int = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    platform_ids: Optional[list[int]] = Query(None),
    brand_ids: Optional[list[int]] = Query(None),
    top_n: int = Query(15, ge=1, le=50),
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    """
    Stacked series of hot topics.

    Data source priority:
    1) daily_topic_metric (aggregated, derived from topic_result)
    2) graceful empty when table missing (legacy DB)
    """
    try:
        dr = parse_date_range(start_date, end_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    plat_sql, plat_params = in_filter("platform_id", platform_ids)
    brand_sql, brand_params = in_filter("brand_id", brand_ids)
    params_base = [project_id, dr.start_date, dr.end_date, *plat_params, *brand_params]

    try:
        top_rows = _fetchall_retry(
            db,
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
            (*params_base, int(top_n)),
        )
    except sqlite3.Error as e:
        msg = str(e).lower()
        if ("no such table" in msg and "daily_topic_metric" in msg) or ("no such column" in msg):
            return {
                "project_id": project_id,
                "date_range": {"start_date": dr.start_date, "end_date": dr.end_date},
                "filters": {"platform_ids": platform_ids, "brand_ids": brand_ids},
                "dates": [],
                "series": [],
            }
        if _is_locked_error(e):
            raise HTTPException(status_code=503, detail=f"SQLite busy: {e}")
        raise _sqlite_error(e)

    topics = [str(r["topic"]) for r in top_rows if (r["topic"] or "").strip() != ""]
    if not topics:
        return {
            "project_id": project_id,
            "date_range": {"start_date": dr.start_date, "end_date": dr.end_date},
            "filters": {"platform_ids": platform_ids, "brand_ids": brand_ids},
            "dates": [],
            "series": [],
        }

    t_sql, t_params = in_filter("topic", topics)
    try:
        rows = _fetchall_retry(
            db,
            f"""
            SELECT stat_date, topic, SUM(COALESCE(hit_count,0)) AS hit_count
            FROM daily_topic_metric
            WHERE project_id=?
              AND stat_date BETWEEN ? AND ?
              {plat_sql}
              {brand_sql}
              {t_sql}
            GROUP BY stat_date, topic
            ORDER BY stat_date ASC, topic ASC;
            """,
            (*params_base, *t_params),
        )
    except sqlite3.Error as e:
        msg = str(e).lower()
        if ("no such table" in msg and "daily_topic_metric" in msg) or ("no such column" in msg):
            return {
                "project_id": project_id,
                "date_range": {"start_date": dr.start_date, "end_date": dr.end_date},
                "filters": {"platform_ids": platform_ids, "brand_ids": brand_ids},
                "dates": [],
                "series": [],
            }
        if _is_locked_error(e):
            raise HTTPException(status_code=503, detail=f"SQLite busy: {e}")
        raise _sqlite_error(e)

    start_dt = datetime.strptime(dr.start_date, "%Y-%m-%d").date()
    end_dt = datetime.strptime(dr.end_date, "%Y-%m-%d").date()
    dates: list[str] = []
    cur = start_dt
    while cur <= end_dt:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)

    idx = {d: i for i, d in enumerate(dates)}
    series_map: dict[str, list[int]] = {tp: [0] * len(dates) for tp in topics}
    for r in rows:
        d = str(r["stat_date"])
        tp = str(r["topic"])
        if tp in series_map and d in idx:
            series_map[tp][idx[d]] = int(r["hit_count"] or 0)

    return {
        "project_id": project_id,
        "date_range": {"start_date": dr.start_date, "end_date": dr.end_date},
        "filters": {"platform_ids": platform_ids, "brand_ids": brand_ids},
        "dates": dates,
        "series": [{"topic": tp, "data": series_map[tp]} for tp in topics],
    }


@router.get("/feature_monitor_stacked")
def dashboard_feature_monitor_stacked(
    project_id: int = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    brand_ids: Optional[list[int]] = Query(None),
    top_n: int = Query(15, ge=1, le=50),
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    """
    Stacked bar chart data for feature mentions.
    Data source: daily_feature_metric (aggregated).

    Notes:
    - This table has no platform_id, so platform filtering is not supported here.
    """
    try:
        dr = parse_date_range(start_date, end_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    brand_sql, brand_params = in_filter("brand_id", brand_ids)
    params_base = [project_id, dr.start_date, dr.end_date, *brand_params]

    try:
        top_rows = _fetchall_retry(
            db,
            f"""
            SELECT feature_name, SUM(COALESCE(mention_count,0)) AS mention_count
            FROM daily_feature_metric
            WHERE project_id=?
              AND stat_date BETWEEN ? AND ?
              {brand_sql}
            GROUP BY feature_name
            ORDER BY mention_count DESC, feature_name ASC
            LIMIT ?;
            """,
            (*params_base, int(top_n)),
        )
    except sqlite3.Error as e:
        if _is_locked_error(e):
            raise HTTPException(status_code=503, detail=f"SQLite busy: {e}")
        raise _sqlite_error(e)

    features = [str(r["feature_name"]) for r in top_rows if r["feature_name"] is not None]
    if not features:
        return {
            "project_id": project_id,
            "date_range": {"start_date": dr.start_date, "end_date": dr.end_date},
            "filters": {"brand_ids": brand_ids},
            "dates": [],
            "series": [],
        }

    feat_sql, feat_params = in_filter("feature_name", features)
    try:
        rows = _fetchall_retry(
            db,
            f"""
            SELECT stat_date, feature_name, SUM(COALESCE(mention_count,0)) AS mention_count
            FROM daily_feature_metric
            WHERE project_id=?
              AND stat_date BETWEEN ? AND ?
              {brand_sql}
              {feat_sql}
            GROUP BY stat_date, feature_name
            ORDER BY stat_date ASC, feature_name ASC;
            """,
            (*params_base, *feat_params),
        )
    except sqlite3.Error as e:
        if _is_locked_error(e):
            raise HTTPException(status_code=503, detail=f"SQLite busy: {e}")
        raise _sqlite_error(e)

    start_dt = datetime.strptime(dr.start_date, "%Y-%m-%d").date()
    end_dt = datetime.strptime(dr.end_date, "%Y-%m-%d").date()
    dates: list[str] = []
    cur = start_dt
    while cur <= end_dt:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)

    idx = {d: i for i, d in enumerate(dates)}
    series_map: dict[str, list[int]] = {f: [0] * len(dates) for f in features}
    for r in rows:
        d = str(r["stat_date"])
        f = str(r["feature_name"])
        if f in series_map and d in idx:
            series_map[f][idx[d]] = int(r["mention_count"] or 0)

    return {
        "project_id": project_id,
        "date_range": {"start_date": dr.start_date, "end_date": dr.end_date},
        "filters": {"brand_ids": brand_ids},
        "dates": dates,
        "series": [{"feature": f, "data": series_map[f]} for f in features],
    }

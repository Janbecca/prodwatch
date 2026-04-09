# 作用：后端 API：帖子相关路由与接口实现。

from __future__ import annotations

import sqlite3
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.api.db import get_db
from backend.api.params import DateRange, in_filter, parse_date_range


router = APIRouter(prefix="/api/posts", tags=["posts"])


def _build_post_where(
    *,
    project_id: int,
    date_range: DateRange,
    platform_ids: Optional[list[int]],
    brand_ids: Optional[list[int]],
    sentiments: Optional[list[str]],
    spam_labels: Optional[list[str]],
    is_valid: Optional[bool],
    keywords: Optional[list[str]],
    sentiment_score_min: Optional[float],
    sentiment_score_max: Optional[float],
    like_min: Optional[int],
    like_max: Optional[int],
    comment_min: Optional[int],
    comment_max: Optional[int],
    share_min: Optional[int],
    share_max: Optional[int],
    search: Optional[str],
) -> tuple[str, list[Any]]:
    clauses: list[str] = [
        "pr.project_id=?",
        "date(pr.publish_time) BETWEEN ? AND ?",
    ]
    params: list[Any] = [project_id, date_range.start_date, date_range.end_date]

    plat_sql, plat_params = in_filter("pr.platform_id", platform_ids)
    brand_sql, brand_params = in_filter("pr.brand_id", brand_ids)
    clauses.append(plat_sql[5:] if plat_sql else "")  # strip leading " AND "
    clauses.append(brand_sql[5:] if brand_sql else "")
    params.extend(plat_params)
    params.extend(brand_params)

    sent_sql, sent_params = in_filter("ps.sentiment", sentiments)
    spam_sql, spam_params = in_filter("COALESCE(sp.spam_label,'normal')", spam_labels)
    clauses.append(sent_sql[5:] if sent_sql else "")
    clauses.append(spam_sql[5:] if spam_sql else "")
    params.extend(sent_params)
    params.extend(spam_params)

    if is_valid is not None:
        clauses.append("COALESCE(pc.is_valid, 1)=?")
        params.append(1 if is_valid else 0)

    if sentiment_score_min is not None or sentiment_score_max is not None:
        lo = -1.0 if sentiment_score_min is None else float(sentiment_score_min)
        hi = 1.0 if sentiment_score_max is None else float(sentiment_score_max)
        if lo > hi:
            lo, hi = hi, lo
        clauses.append("(ps.sentiment_score IS NOT NULL AND ps.sentiment_score BETWEEN ? AND ?)")
        params.extend([lo, hi])

    def add_int_range(field_sql: str, vmin: Optional[int], vmax: Optional[int]):
        if vmin is None and vmax is None:
            return
        if vmin is not None:
            clauses.append(f"COALESCE({field_sql},0) >= ?")
            params.append(int(vmin))
        if vmax is not None:
            clauses.append(f"COALESCE({field_sql},0) <= ?")
            params.append(int(vmax))

    add_int_range("pr.like_count", like_min, like_max)
    add_int_range("pr.comment_count", comment_min, comment_max)
    add_int_range("pr.share_count", share_min, share_max)

    if keywords is not None:
        kw_sql, kw_params = in_filter("pkr.keyword", keywords)
        if kw_sql:
            clauses.append(
                f"EXISTS(SELECT 1 FROM post_keyword_result pkr WHERE pkr.post_id=pr.id{kw_sql})"
            )
            params.extend(kw_params)
        else:
            clauses.append("1=0")

    if search is not None and search.strip() != "":
        s = search.strip()
        clauses.append(
            "(pr.title LIKE ? OR pr.content LIKE ?)"
        )
        params.extend([f"%{s}%", f"%{s}%"])

    # Drop empty fragments
    clauses = [c for c in clauses if c]
    where = " WHERE " + " AND ".join(clauses)
    return where, params


def _parse_date_range_or_400(start_date: str, end_date: str) -> DateRange:
    try:
        return parse_date_range(start_date, end_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/overview")
def posts_overview(
    project_id: int = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    platform_ids: Optional[list[int]] = Query(None),
    brand_ids: Optional[list[int]] = Query(None),
    sentiments: Optional[list[str]] = Query(None, description="positive/neutral/negative"),
    spam_labels: Optional[list[str]] = Query(None, description="spam/normal"),
    is_valid: Optional[bool] = Query(None),
    keywords: Optional[list[str]] = Query(None),
    sentiment_score_min: Optional[float] = Query(None),
    sentiment_score_max: Optional[float] = Query(None),
    like_min: Optional[int] = Query(None, ge=0),
    like_max: Optional[int] = Query(None, ge=0),
    comment_min: Optional[int] = Query(None, ge=0),
    comment_max: Optional[int] = Query(None, ge=0),
    share_min: Optional[int] = Query(None, ge=0),
    share_max: Optional[int] = Query(None, ge=0),
    search: Optional[str] = Query(None),
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    dr = _parse_date_range_or_400(start_date, end_date)
    where, params = _build_post_where(
        project_id=project_id,
        date_range=dr,
        platform_ids=platform_ids,
        brand_ids=brand_ids,
        sentiments=sentiments,
        spam_labels=spam_labels,
        is_valid=is_valid,
        keywords=keywords,
        sentiment_score_min=sentiment_score_min,
        sentiment_score_max=sentiment_score_max,
        like_min=like_min,
        like_max=like_max,
        comment_min=comment_min,
        comment_max=comment_max,
        share_min=share_min,
        share_max=share_max,
        search=search,
    )

    row = db.execute(
        f"""
        WITH filtered AS (
          SELECT DISTINCT pr.id AS post_id
          FROM post_raw pr
          LEFT JOIN post_clean_result pc ON pc.post_id=pr.id
          LEFT JOIN post_sentiment_result ps ON ps.post_id=pr.id
          LEFT JOIN post_spam_result sp ON sp.post_id=pr.id
          {where}
        )
        SELECT
          COUNT(*) AS total,
          SUM(CASE WHEN COALESCE(pc.is_valid,1)=1 THEN 1 ELSE 0 END) AS valid_count,
          SUM(CASE WHEN COALESCE(pc.is_valid,1)=0 THEN 1 ELSE 0 END) AS invalid_count,
          SUM(CASE WHEN COALESCE(sp.spam_label,'normal')='spam' THEN 1 ELSE 0 END) AS spam_count,
          SUM(CASE WHEN ps.sentiment='negative' THEN 1 ELSE 0 END) AS negative_count,
          (
            SELECT COUNT(DISTINCT pkr.keyword)
            FROM post_keyword_result pkr
            INNER JOIN filtered f2 ON f2.post_id=pkr.post_id
          ) AS hot_keyword_count
        FROM filtered f
        LEFT JOIN post_clean_result pc ON pc.post_id=f.post_id
        LEFT JOIN post_spam_result sp ON sp.post_id=f.post_id
        LEFT JOIN post_sentiment_result ps ON ps.post_id=f.post_id;
        """,
        params,
    ).fetchone()

    overview = {
        "total": int(row["total"] or 0),
        "valid_count": int(row["valid_count"] or 0),
        "negative_count": int(row["negative_count"] or 0),
        "spam_count": int(row["spam_count"] or 0),
        "hot_keyword_count": int(row["hot_keyword_count"] or 0),
    }

    return {
        "project_id": project_id,
        "date_range": {"start_date": dr.start_date, "end_date": dr.end_date},
        "filters": {
            "platform_ids": platform_ids,
            "brand_ids": brand_ids,
            "sentiments": sentiments,
            "spam_labels": spam_labels,
            "is_valid": is_valid,
            "keywords": keywords,
            "sentiment_score_min": sentiment_score_min,
            "sentiment_score_max": sentiment_score_max,
            "like_min": like_min,
            "like_max": like_max,
            "comment_min": comment_min,
            "comment_max": comment_max,
            "share_min": share_min,
            "share_max": share_max,
            "search": search,
        },
        "overview": overview,
    }


@router.get("/list")
def list_posts(
    project_id: int = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    platform_ids: Optional[list[int]] = Query(None),
    brand_ids: Optional[list[int]] = Query(None),
    sentiments: Optional[list[str]] = Query(None, description="positive/neutral/negative"),
    spam_labels: Optional[list[str]] = Query(None, description="spam/normal"),
    is_valid: Optional[bool] = Query(None),
    keywords: Optional[list[str]] = Query(None),
    sentiment_score_min: Optional[float] = Query(None),
    sentiment_score_max: Optional[float] = Query(None),
    like_min: Optional[int] = Query(None, ge=0),
    like_max: Optional[int] = Query(None, ge=0),
    comment_min: Optional[int] = Query(None, ge=0),
    comment_max: Optional[int] = Query(None, ge=0),
    share_min: Optional[int] = Query(None, ge=0),
    share_max: Optional[int] = Query(None, ge=0),
    search: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    dr = _parse_date_range_or_400(start_date, end_date)
    where, params = _build_post_where(
        project_id=project_id,
        date_range=dr,
        platform_ids=platform_ids,
        brand_ids=brand_ids,
        sentiments=sentiments,
        spam_labels=spam_labels,
        is_valid=is_valid,
        keywords=keywords,
        sentiment_score_min=sentiment_score_min,
        sentiment_score_max=sentiment_score_max,
        like_min=like_min,
        like_max=like_max,
        comment_min=comment_min,
        comment_max=comment_max,
        share_min=share_min,
        share_max=share_max,
        search=search,
    )

    total = db.execute(
        f"""
        SELECT COUNT(DISTINCT pr.id) AS cnt
        FROM post_raw pr
        LEFT JOIN post_clean_result pc ON pc.post_id=pr.id
        LEFT JOIN post_sentiment_result ps ON ps.post_id=pr.id
        LEFT JOIN post_spam_result sp ON sp.post_id=pr.id
        {where};
        """,
        params,
    ).fetchone()["cnt"]

    offset = (page - 1) * page_size
    rows = db.execute(
        f"""
        SELECT
          pr.id,
          pr.project_id,
          pr.crawl_job_id,
          pr.platform_id,
          pr.brand_id,
          pr.external_post_id,
          pr.author_name,
          pr.title,
          pr.content,
          pr.post_url,
          pr.publish_time,
          pr.crawled_at,
          pr.like_count,
          pr.comment_count,
          pr.share_count,
          pr.view_count,
          pc.is_valid,
          pc.invalid_reason,
          pc.clean_text,
          ps.sentiment,
          ps.sentiment_score,
          ps.emotion_intensity,
          sp.spam_label,
          sp.spam_score,
          GROUP_CONCAT(DISTINCT pkr.keyword) AS keywords,
          GROUP_CONCAT(DISTINCT pfr.feature_name) AS features
        FROM post_raw pr
        LEFT JOIN post_clean_result pc ON pc.post_id=pr.id
        LEFT JOIN post_sentiment_result ps ON ps.post_id=pr.id
        LEFT JOIN post_spam_result sp ON sp.post_id=pr.id
        LEFT JOIN post_keyword_result pkr ON pkr.post_id=pr.id
        LEFT JOIN post_feature_result pfr ON pfr.post_id=pr.id
        {where}
        GROUP BY pr.id
        ORDER BY COALESCE(pr.publish_time, pr.crawled_at) DESC, pr.id DESC
        LIMIT ? OFFSET ?;
        """,
        (*params, int(page_size), int(offset)),
    ).fetchall()

    items = []
    for r in rows:
        items.append(
            {
                "id": int(r["id"]),
                "project_id": int(r["project_id"]),
                "crawl_job_id": r["crawl_job_id"],
                "platform_id": r["platform_id"],
                "brand_id": r["brand_id"],
                "external_post_id": r["external_post_id"],
                "author_name": r["author_name"],
                "title": r["title"],
                "content": r["content"],
                "post_url": r["post_url"],
                "publish_time": r["publish_time"],
                "crawled_at": r["crawled_at"],
                "like_count": r["like_count"],
                "comment_count": r["comment_count"],
                "share_count": r["share_count"],
                "view_count": r["view_count"],
                "is_valid": int(r["is_valid"]) if r["is_valid"] is not None else None,
                "invalid_reason": r["invalid_reason"],
                "clean_text": r["clean_text"],
                "sentiment": r["sentiment"],
                "sentiment_score": r["sentiment_score"],
                "emotion_intensity": r["emotion_intensity"],
                "spam_label": r["spam_label"],
                "spam_score": r["spam_score"],
                "keywords": (r["keywords"].split(",") if r["keywords"] else []),
                "features": (r["features"].split(",") if r["features"] else []),
            }
        )

    overview_row = db.execute(
        f"""
        WITH filtered AS (
          SELECT DISTINCT pr.id AS post_id
          FROM post_raw pr
          LEFT JOIN post_clean_result pc ON pc.post_id=pr.id
          LEFT JOIN post_sentiment_result ps ON ps.post_id=pr.id
          LEFT JOIN post_spam_result sp ON sp.post_id=pr.id
          {where}
        )
        SELECT
          COUNT(*) AS total,
          SUM(CASE WHEN COALESCE(pc.is_valid,1)=1 THEN 1 ELSE 0 END) AS valid_count,
          SUM(CASE WHEN COALESCE(pc.is_valid,1)=0 THEN 1 ELSE 0 END) AS invalid_count,
          SUM(CASE WHEN COALESCE(sp.spam_label,'normal')='spam' THEN 1 ELSE 0 END) AS spam_count,
          SUM(CASE WHEN ps.sentiment='positive' THEN 1 ELSE 0 END) AS positive_count,
          SUM(CASE WHEN ps.sentiment='neutral' THEN 1 ELSE 0 END)  AS neutral_count,
          SUM(CASE WHEN ps.sentiment='negative' THEN 1 ELSE 0 END) AS negative_count,
          (
            SELECT COUNT(DISTINCT pkr.keyword)
            FROM post_keyword_result pkr
            INNER JOIN filtered f2 ON f2.post_id=pkr.post_id
          ) AS hot_keyword_count
        FROM filtered f
        LEFT JOIN post_clean_result pc ON pc.post_id=f.post_id
        LEFT JOIN post_spam_result sp ON sp.post_id=f.post_id
        LEFT JOIN post_sentiment_result ps ON ps.post_id=f.post_id;
        """,
        params,
    ).fetchone()

    overview = {
        "total": int(overview_row["total"] or 0),
        "valid_count": int(overview_row["valid_count"] or 0),
        "invalid_count": int(overview_row["invalid_count"] or 0),
        "spam_count": int(overview_row["spam_count"] or 0),
        "positive_count": int(overview_row["positive_count"] or 0),
        "neutral_count": int(overview_row["neutral_count"] or 0),
        "negative_count": int(overview_row["negative_count"] or 0),
        "hot_keyword_count": int(overview_row["hot_keyword_count"] or 0),
    }

    return {
        "project_id": project_id,
        "date_range": {"start_date": dr.start_date, "end_date": dr.end_date},
        "filters": {
            "platform_ids": platform_ids,
            "brand_ids": brand_ids,
            "sentiments": sentiments,
            "spam_labels": spam_labels,
            "is_valid": is_valid,
            "keywords": keywords,
            "sentiment_score_min": sentiment_score_min,
            "sentiment_score_max": sentiment_score_max,
            "like_min": like_min,
            "like_max": like_max,
            "comment_min": comment_min,
            "comment_max": comment_max,
            "share_min": share_min,
            "share_max": share_max,
            "search": search,
        },
        "page": page,
        "page_size": page_size,
        "total": int(total or 0),
        "overview": overview,
        "items": items,
    }


@router.get("/detail")
def post_detail(
    project_id: int = Query(...),
    post_id: int = Query(..., ge=1),
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    """
    Fetch a single post with rich joined info for the detail drawer.
    """
    row = db.execute(
        """
        SELECT
          pr.id,
          pr.project_id,
          pr.crawl_job_id,
          pr.platform_id,
          pr.brand_id,
          pr.external_post_id,
          pr.author_name,
          pr.title,
          pr.content,
          pr.post_url,
          pr.publish_time,
          pr.crawled_at,
          pr.like_count,
          pr.comment_count,
          pr.share_count,
          pr.view_count,
          pr.raw_payload,
          pc.is_valid,
          pc.invalid_reason,
          pc.clean_text,
          ps.sentiment,
          ps.sentiment_score,
          ps.emotion_intensity,
          sp.spam_label,
          sp.spam_score
        FROM post_raw pr
        LEFT JOIN post_clean_result pc ON pc.post_id=pr.id
        LEFT JOIN post_sentiment_result ps ON ps.post_id=pr.id
        LEFT JOIN post_spam_result sp ON sp.post_id=pr.id
        WHERE pr.project_id=? AND pr.id=?
        LIMIT 1;
        """,
        (int(project_id), int(post_id)),
    ).fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="post not found")

    kw_rows = db.execute(
        """
        SELECT keyword
        FROM post_keyword_result
        WHERE post_id=?
        GROUP BY keyword
        ORDER BY keyword ASC;
        """,
        (int(post_id),),
    ).fetchall()
    keywords = [str(r["keyword"]) for r in kw_rows if r["keyword"] is not None]

    feat_rows = db.execute(
        """
        SELECT feature_name, feature_sentiment, confidence
        FROM post_feature_result
        WHERE post_id=?
        ORDER BY confidence DESC, feature_name ASC;
        """,
        (int(post_id),),
    ).fetchall()
    features = [
        {
            "feature_name": r["feature_name"],
            "feature_sentiment": r["feature_sentiment"],
            "confidence": float(r["confidence"] or 0.0),
        }
        for r in feat_rows
        if r["feature_name"] is not None
    ]

    item = {
        "id": int(row["id"]),
        "project_id": int(row["project_id"]),
        "crawl_job_id": row["crawl_job_id"],
        "platform_id": row["platform_id"],
        "brand_id": row["brand_id"],
        "external_post_id": row["external_post_id"],
        "author_name": row["author_name"],
        "title": row["title"],
        "content": row["content"],
        "post_url": row["post_url"],
        "publish_time": row["publish_time"],
        "crawled_at": row["crawled_at"],
        "like_count": row["like_count"],
        "comment_count": row["comment_count"],
        "share_count": row["share_count"],
        "view_count": row["view_count"],
        "raw_payload": row["raw_payload"],
        "is_valid": int(row["is_valid"]) if row["is_valid"] is not None else None,
        "invalid_reason": row["invalid_reason"],
        "clean_text": row["clean_text"],
        "sentiment": row["sentiment"],
        "sentiment_score": row["sentiment_score"],
        "emotion_intensity": row["emotion_intensity"],
        "spam_label": row["spam_label"],
        "spam_score": row["spam_score"],
        "keywords": keywords,
        "features": features,
    }

    return {"project_id": int(project_id), "post_id": int(post_id), "item": item}

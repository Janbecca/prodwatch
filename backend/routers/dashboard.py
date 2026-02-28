from fastapi import APIRouter, Depends
import pandas as pd
from backend.storage.db import get_repo
from .auth import get_current_user

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


def _safe_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 4)


@router.get("/kpis")
def get_kpis(user=Depends(get_current_user)):
    repo = get_repo()
    posts_df = repo.query("post_raw")
    sent_df = repo.query("sentiment_result")

    total_posts = len(posts_df)
    total_sent = len(sent_df)
    pos = int((sent_df["polarity"] == "positive").sum()) if "polarity" in sent_df.columns else 0
    neg = int((sent_df["polarity"] == "negative").sum()) if "polarity" in sent_df.columns else 0
    intensity = 0.0
    if "intensity" in sent_df.columns and total_sent:
        vals = pd.to_numeric(sent_df["intensity"], errors="coerce").dropna()
        intensity = round(float(vals.mean()), 4) if not vals.empty else 0.0

    return {
        "comments": total_posts,
        "positive": _safe_ratio(pos, total_sent),
        "negative": _safe_ratio(neg, total_sent),
        "intensity": intensity,
    }


@router.get("/trends")
def get_trends(user=Depends(get_current_user)):
    repo = get_repo()
    daily_df = repo.query("daily_metric")
    platform_df = repo.query("platform")
    name_map = {}
    if not platform_df.empty and "id" in platform_df.columns and "name" in platform_df.columns:
        for _, row in platform_df.iterrows():
            pid = pd.to_numeric(row.get("id"), errors="coerce")
            if pd.notna(pid):
                name_map[int(pid)] = str(row.get("name"))

    series = []
    if not daily_df.empty and {"platform_id", "metric_date", "total_posts"}.issubset(daily_df.columns):
        daily_df["metric_date"] = pd.to_datetime(daily_df["metric_date"], errors="coerce")
        grouped = daily_df.sort_values("metric_date").groupby("platform_id")
        for pid, g in grouped:
            vals = pd.to_numeric(g["total_posts"], errors="coerce").fillna(0).astype(int).tolist()
            if vals:
                series.append({"name": name_map.get(int(pid), f"platform_{pid}"), "data": vals})
    return {"series": series}


@router.get("/ranking")
def get_ranking(user=Depends(get_current_user)):
    repo = get_repo()
    daily_df = repo.query("daily_metric")
    platform_df = repo.query("platform")
    if daily_df.empty:
        return []

    name_map = {}
    if "id" in platform_df.columns and "name" in platform_df.columns:
        for _, row in platform_df.iterrows():
            pid = pd.to_numeric(row.get("id"), errors="coerce")
            if pd.notna(pid):
                name_map[int(pid)] = str(row.get("name"))

    rows = []
    grouped = daily_df.groupby("platform_id")
    for pid, g in grouped:
        total = pd.to_numeric(g.get("total_posts"), errors="coerce").fillna(0).sum()
        spam = pd.to_numeric(g.get("spam_posts"), errors="coerce").fillna(0).sum()
        score = int(max(total - spam, 0))
        rows.append({"product": name_map.get(int(pid), f"platform_{pid}"), "score": score})

    rows.sort(key=lambda x: x["score"], reverse=True)
    return rows


@router.get("/alerts")
def get_alerts(user=Depends(get_current_user)):
    repo = get_repo()
    daily_df = repo.query("daily_metric")
    platform_df = repo.query("platform")
    if daily_df.empty:
        return []

    name_map = {}
    if "id" in platform_df.columns and "name" in platform_df.columns:
        for _, row in platform_df.iterrows():
            pid = pd.to_numeric(row.get("id"), errors="coerce")
            if pd.notna(pid):
                name_map[int(pid)] = str(row.get("name"))

    alerts = []
    for _, row in daily_df.iterrows():
        total = pd.to_numeric(row.get("total_posts"), errors="coerce")
        neg = pd.to_numeric(row.get("neg_posts"), errors="coerce")
        if pd.isna(total) or pd.isna(neg) or int(total) <= 0:
            continue
        ratio = float(neg) / float(total)
        if ratio >= 0.4:
            pid = int(pd.to_numeric(row.get("platform_id"), errors="coerce"))
            alerts.append(
                {
                    "level": "high" if ratio >= 0.6 else "medium",
                    "product": name_map.get(pid, f"platform_{pid}"),
                    "reason": f"负面占比偏高({ratio:.2%})",
                }
            )
    return alerts

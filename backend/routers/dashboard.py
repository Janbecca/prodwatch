from fastapi import APIRouter, Body, Depends, HTTPException, Query
import pandas as pd
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from backend.storage.db import get_repo
from .auth import get_current_user
from pydantic import BaseModel
from zoneinfo import ZoneInfo

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


class ManualRefreshRequest(BaseModel):
    project_id: Optional[int] = None
    brand_ids: Optional[List[int]] = None
    platform_ids: Optional[List[int]] = None
    max_posts_per_run: int = 30
    sentiment_model: str = "rule-based"
    trigger_type: str = "manual"  # manual | schedule


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


@router.post("/manual_refresh")
def manual_refresh(payload: ManualRefreshRequest = Body(default=ManualRefreshRequest()), user=Depends(get_current_user)):
    """
    Dashboard "manual refresh": generate a small batch of simulated crawler data,
    then the frontend can re-fetch dashboard KPIs/trends immediately.

    Scope:
    - If brand_ids provided: limit to projects linked to those brands.
    - Otherwise: run for all active projects.
    """
    repo = get_repo()

    try:
        from backend.agents.simulator import run_simulated_crawl
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"simulator import failed: {e}") from e

    run_date = datetime.now(ZoneInfo("Asia/Shanghai")).date()
    max_posts_per_run = int(max(1, min(int(payload.max_posts_per_run or 30), 500)))
    sentiment_model = str(payload.sentiment_model or "rule-based")
    trigger_type = str(payload.trigger_type or "manual")

    # When project_id omitted: refresh all enabled projects (is_active=1)
    project_ids: List[int] = []
    if payload.project_id is not None:
        project_ids = [int(payload.project_id)]
    else:
        proj_df = repo.query("monitor_project")
        if proj_df is not None and not proj_df.empty and {"id", "is_active"}.issubset(proj_df.columns):
            tmp = proj_df.copy()
            tmp["id"] = pd.to_numeric(tmp["id"], errors="coerce")
            tmp["is_active"] = pd.to_numeric(tmp["is_active"], errors="coerce").fillna(0).astype(int)
            tmp = tmp.dropna(subset=["id"])
            tmp["id"] = tmp["id"].astype(int)
            project_ids = sorted(tmp[tmp["is_active"] == 1]["id"].tolist())

    if not project_ids:
        raise HTTPException(status_code=422, detail="project_id is required (or enable at least one project)")

    summaries: List[Dict[str, Any]] = []
    for pid in project_ids:
        summaries.append(
            run_simulated_crawl(
                project_id=int(pid),
                run_date=run_date,
                seed=None,
                brand_ids=[int(x) for x in (payload.brand_ids or []) if x is not None] or None,
                platform_ids=[int(x) for x in (payload.platform_ids or []) if x is not None] or None,
                max_posts_per_run=max_posts_per_run,
                sentiment_model=sentiment_model,
                trigger_type=trigger_type,
                crawl_source=("manual" if trigger_type == "manual" else "schedule"),
            )
        )

    return {"items": summaries, "total_runs": sum(int(x.get("pipeline_runs") or 0) for x in summaries)}


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        return int(value)
    except Exception:
        return None


def _parse_days(days: int) -> Tuple[datetime, datetime]:
    days = max(1, min(int(days), 365))
    end = datetime.utcnow()
    start = end - timedelta(days=days - 1)
    start = datetime(start.year, start.month, start.day)
    return start, end


def _parse_range(days: int, start_date: Optional[str], end_date: Optional[str]) -> Tuple[datetime, datetime]:
    """
    Returns (start, end) in UTC.
    - If start_date/end_date provided: use them (end_date is inclusive for the whole day).
    - Otherwise fallback to `days` (last N days).
    """
    if not start_date and not end_date:
        return _parse_days(days)

    now = datetime.utcnow()
    start_dt = pd.to_datetime(start_date, errors="coerce") if start_date else pd.NaT
    end_dt = pd.to_datetime(end_date, errors="coerce") if end_date else pd.NaT

    start: datetime
    end: datetime

    if pd.notna(end_dt):
        end = end_dt.to_pydatetime() if hasattr(end_dt, "to_pydatetime") else now
    else:
        end = now
    end = datetime(end.year, end.month, end.day, 23, 59, 59, 999999)

    if pd.notna(start_dt):
        start = start_dt.to_pydatetime() if hasattr(start_dt, "to_pydatetime") else (end - timedelta(days=days - 1))
    else:
        start = end - timedelta(days=max(1, min(int(days), 365)) - 1)
    start = datetime(start.year, start.month, start.day)

    if start > end:
        start, end = end, start
    return start, end


def _project_name_map(repo) -> Dict[int, str]:
    df = repo.query("monitor_project")
    mapping: Dict[int, str] = {}
    if df.empty or "id" not in df.columns:
        return mapping
    for _, row in df.iterrows():
        pid = pd.to_numeric(row.get("id"), errors="coerce")
        if pd.notna(pid):
            mapping[int(pid)] = str(row.get("name") or f"project_{int(pid)}")
    return mapping


def _project_brand_map(repo) -> Dict[int, Optional[int]]:
    join_df = repo.query("monitor_project_brand")
    mapping: Dict[int, Optional[int]] = {}
    if join_df is not None and not join_df.empty and {"project_id", "brand_id"}.issubset(join_df.columns):
        join_df = join_df.copy()
        join_df["project_id"] = pd.to_numeric(join_df["project_id"], errors="coerce")
        join_df["brand_id"] = pd.to_numeric(join_df["brand_id"], errors="coerce")
        join_df = join_df.dropna(subset=["project_id", "brand_id"])
        join_df["project_id"] = join_df["project_id"].astype(int)
        join_df["brand_id"] = join_df["brand_id"].astype(int)
        for pid, g in join_df.groupby("project_id"):
            bids = sorted({int(x) for x in g["brand_id"].tolist() if x is not None})
            mapping[int(pid)] = bids[0] if bids else None
        return mapping

    df = repo.query("monitor_project")
    if df is None or df.empty or "id" not in df.columns:
        return mapping
    for _, row in df.iterrows():
        pid = pd.to_numeric(row.get("id"), errors="coerce")
        if pd.notna(pid):
            mapping[int(pid)] = _safe_int(row.get("brand_id"))
    return mapping


def _brand_name_map(repo) -> Dict[int, str]:
    df = repo.query("brand")
    mapping: Dict[int, str] = {}
    if df.empty or not {"id", "name"}.issubset(df.columns):
        return mapping
    for _, row in df.iterrows():
        bid = pd.to_numeric(row.get("id"), errors="coerce")
        if pd.notna(bid):
            mapping[int(bid)] = str(row.get("name") or f"brand_{int(bid)}")
    return mapping


def _platform_name_map(repo) -> Dict[int, str]:
    df = repo.query("platform")
    mapping: Dict[int, str] = {}
    if df.empty or not {"id", "name"}.issubset(df.columns):
        return mapping
    for _, row in df.iterrows():
        pid = pd.to_numeric(row.get("id"), errors="coerce")
        if pd.notna(pid):
            mapping[int(pid)] = str(row.get("name") or f"platform_{int(pid)}")
    return mapping


def _projects_for_brands(repo, brand_ids: List[int]) -> Tuple[List[int], Dict[int, int]]:
    """
    Returns (project_ids, project_id -> brand_id map) for the given brand ids.
    """
    brand_set = set(int(x) for x in brand_ids if x is not None)

    join_df = repo.query("monitor_project_brand")
    if join_df is not None and not join_df.empty and {"project_id", "brand_id"}.issubset(join_df.columns):
        join_df = join_df.copy()
        join_df["project_id"] = pd.to_numeric(join_df["project_id"], errors="coerce")
        join_df["brand_id"] = pd.to_numeric(join_df["brand_id"], errors="coerce")
        join_df = join_df.dropna(subset=["project_id", "brand_id"])
        join_df["project_id"] = join_df["project_id"].astype(int)
        join_df["brand_id"] = join_df["brand_id"].astype(int)

        project_ids: List[int] = []
        project_brand: Dict[int, int] = {}
        for _, row in join_df.iterrows():
            pid = int(row["project_id"])
            bid = int(row["brand_id"])
            project_brand[pid] = bid
            if bid in brand_set:
                project_ids.append(pid)
        return sorted(list(set(project_ids))), project_brand

    projects_df = repo.query("monitor_project")
    project_ids: List[int] = []
    project_brand: Dict[int, int] = {}
    if projects_df is None or projects_df.empty or "id" not in projects_df.columns:
        return project_ids, project_brand

    for _, row in projects_df.iterrows():
        pid = _safe_int(row.get("id"))
        bid = _safe_int(row.get("brand_id"))
        if pid is None or bid is None:
            continue
        project_brand[pid] = bid
        if bid in brand_set:
            project_ids.append(pid)
    return project_ids, project_brand


def _coerce_daily_metric_numeric(daily_df: pd.DataFrame) -> pd.DataFrame:
    if daily_df is None or daily_df.empty:
        return daily_df
    numeric_cols = [
        "project_id",
        "platform_id",
        "total_posts",
        "valid_posts",
        "spam_posts",
        "pos_posts",
        "neu_posts",
        "neg_posts",
    ]
    for c in numeric_cols:
        if c in daily_df.columns:
            daily_df[c] = pd.to_numeric(daily_df[c], errors="coerce").fillna(0)
    return daily_df


@router.get("/options")
def get_options(user=Depends(get_current_user)):
    repo = get_repo()
    brand_df = repo.query("brand")
    projects_df = repo.query("monitor_project")
    platforms_df = repo.query("platform")
    project_brand_df = repo.query("monitor_project_brand")
    project_platform_df = repo.query("monitor_project_platform")

    brands: List[Dict[str, Any]] = []
    if not brand_df.empty:
        for _, row in brand_df.iterrows():
            bid = _safe_int(row.get("id"))
            if bid is None:
                continue
            brands.append({"id": bid, "name": str(row.get("name") or f"brand_{bid}")})

    projects: List[Dict[str, Any]] = []
    if not projects_df.empty:
        proj_brand_map: Dict[int, List[int]] = {}
        if project_brand_df is not None and not project_brand_df.empty and {"project_id", "brand_id"}.issubset(project_brand_df.columns):
            tmp = project_brand_df.copy()
            tmp["project_id"] = pd.to_numeric(tmp["project_id"], errors="coerce")
            tmp["brand_id"] = pd.to_numeric(tmp["brand_id"], errors="coerce")
            tmp = tmp.dropna(subset=["project_id", "brand_id"])
            tmp["project_id"] = tmp["project_id"].astype(int)
            tmp["brand_id"] = tmp["brand_id"].astype(int)
            for pid, g in tmp.groupby("project_id"):
                proj_brand_map[int(pid)] = sorted({int(x) for x in g["brand_id"].tolist() if x is not None})

        proj_platform_cfg: Dict[int, List[Dict[str, Any]]] = {}
        enabled_platform_ids: Dict[int, List[int]] = {}
        if project_platform_df is not None and not project_platform_df.empty and {"project_id", "platform_id", "is_enabled"}.issubset(project_platform_df.columns):
            t = project_platform_df.copy()
            for c in ["id", "project_id", "platform_id", "is_enabled", "max_posts_per_run"]:
                if c in t.columns:
                    t[c] = pd.to_numeric(t[c], errors="coerce")
            t = t.dropna(subset=["project_id", "platform_id"])
            t["project_id"] = t["project_id"].astype(int)
            t["platform_id"] = t["platform_id"].astype(int)
            t["is_enabled"] = t["is_enabled"].fillna(0).astype(int)
            for pid, g in t.groupby("project_id"):
                cfgs: List[Dict[str, Any]] = []
                enabled: List[int] = []
                for _, r in g.sort_values(by=["platform_id"]).iterrows():
                    plat_id = int(r.get("platform_id"))
                    is_enabled = int(r.get("is_enabled") or 0) == 1
                    if is_enabled:
                        enabled.append(plat_id)
                    cfgs.append(
                        {
                            "id": _safe_int(r.get("id")),
                            "platform_id": plat_id,
                            "is_enabled": is_enabled,
                            "crawl_mode": str(r.get("crawl_mode") or "schedule"),
                            "cron_expr": str(r.get("cron_expr") or "0 5 * * *"),
                            "timezone": str(r.get("timezone") or "Asia/Shanghai"),
                            "max_posts_per_run": int(pd.to_numeric(r.get("max_posts_per_run"), errors="coerce") or 0) or 20,
                            "sentiment_model": str(r.get("sentiment_model") or "rule-based"),
                        }
                    )
                proj_platform_cfg[int(pid)] = cfgs
                enabled_platform_ids[int(pid)] = sorted({int(x) for x in enabled})

        for _, row in projects_df.iterrows():
            pid = _safe_int(row.get("id"))
            if pid is None:
                continue
            desc = row.get("description")
            if desc is None or (isinstance(desc, float) and pd.isna(desc)):
                desc = None
            legacy_bid = _safe_int(row.get("brand_id"))
            brand_ids = proj_brand_map.get(int(pid), ([] if legacy_bid is None else [legacy_bid]))
            projects.append(
                {
                    "id": pid,
                    "brand_ids": brand_ids,
                    "name": str(row.get("name") or f"project_{pid}"),
                    "product_category": (None if pd.isna(row.get("product_category")) else row.get("product_category")),
                    "description": desc,
                    "is_active": int(pd.to_numeric(row.get("is_active"), errors="coerce") or 0),
                    "enabled_platform_ids": enabled_platform_ids.get(int(pid), []),
                    "platform_configs": proj_platform_cfg.get(int(pid), []),
                }
            )

    platforms: List[Dict[str, Any]] = []
    if not platforms_df.empty:
        for _, row in platforms_df.iterrows():
            pid = _safe_int(row.get("id"))
            if pid is None:
                continue
            platforms.append(
                {
                    "id": pid,
                    "code": str(row.get("code") or ""),
                    "name": str(row.get("name") or f"platform_{pid}"),
                }
            )

    return {"brands": brands, "projects": projects, "platforms": platforms}


@router.get("/overview")
def get_overview(
    brand_ids: Optional[List[int]] = Query(None, description="1-4 brand ids"),
    project_ids: Optional[List[int]] = Query(None, description="(Deprecated) 1-4 project ids"),
    project_id: Optional[int] = Query(None, description="Active project id (optional)."),
    days: int = Query(7, ge=1, le=365),
    start_date: Optional[str] = Query(None, description="Custom date range start (YYYY-MM-DD)."),
    end_date: Optional[str] = Query(None, description="Custom date range end (YYYY-MM-DD)."),
    user=Depends(get_current_user),
):
    """
    Aggregated KPI panel for selected brands.
    Uses `daily_metric` as the primary source of truth.
    """
    # Backward-compatible input: old frontend used `project_ids`.
    if not brand_ids and project_ids:
        proj_brand = _project_brand_map(get_repo())
        derived = []
        for pid in project_ids:
            try:
                bid = proj_brand.get(int(pid))
                if bid is not None:
                    derived.append(int(bid))
            except Exception:
                continue
        brand_ids = derived

    if not brand_ids:
        raise HTTPException(status_code=422, detail="brand_ids is required (or provide deprecated project_ids)")

    brand_ids = [int(x) for x in brand_ids][:4]
    repo = get_repo()
    start, end = _parse_range(days, start_date, end_date)
    project_ids, project_brand = _projects_for_brands(repo, brand_ids)
    if project_id is not None:
        project_ids = [int(project_id)] if int(project_id) in set(project_ids) else []
    if not project_ids:
        return {"range": {"from": start.date().isoformat(), "to": end.date().isoformat()}, "items": []}

    daily_df = repo.query("daily_metric")
    if daily_df.empty:
        return {"range": {"from": start.date().isoformat(), "to": end.date().isoformat()}, "items": []}

    daily_df["metric_date"] = pd.to_datetime(daily_df.get("metric_date"), errors="coerce")
    daily_df = daily_df.dropna(subset=["metric_date"])
    daily_df = daily_df[(daily_df["metric_date"] >= start) & (daily_df["metric_date"] <= end)]
    daily_df = _coerce_daily_metric_numeric(daily_df)
    if "project_id" in daily_df.columns:
        daily_df = daily_df[daily_df["project_id"].astype(int).isin(project_ids)]
    if daily_df.empty:
        return {"range": {"from": start.date().isoformat(), "to": end.date().isoformat()}, "items": []}

    brand_map = _brand_name_map(repo)
    sent_df = repo.query("sentiment_result")
    brand_intensity_map: Dict[int, float] = {}
    if not sent_df.empty and "intensity" in sent_df.columns:
        tmp = sent_df.copy()
        tmp["intensity"] = pd.to_numeric(tmp["intensity"], errors="coerce")
        tmp = tmp.dropna(subset=["intensity"])
        if not tmp.empty and "project_id" in tmp.columns:
            tmp["project_id"] = pd.to_numeric(tmp["project_id"], errors="coerce")
            tmp = tmp.dropna(subset=["project_id"])
            tmp["project_id"] = tmp["project_id"].astype(int)
            tmp = tmp[tmp["project_id"].isin(project_ids)]

        if not tmp.empty and "brand_id" in tmp.columns:
            tmp["brand_id"] = pd.to_numeric(tmp["brand_id"], errors="coerce")
            if tmp["brand_id"].notna().any():
                tmp = tmp.dropna(subset=["brand_id"])
                tmp["brand_id"] = tmp["brand_id"].astype(int)
                for bid, g in tmp.groupby("brand_id"):
                    brand_intensity_map[int(bid)] = round(float(g["intensity"].mean()), 4)
            else:
                # legacy: project -> first brand
                tmp["brand_id"] = tmp["project_id"].apply(lambda x: project_brand.get(int(x), None))
                for bid, g in tmp.dropna(subset=["brand_id"]).groupby("brand_id"):
                    brand_intensity_map[int(bid)] = round(float(g["intensity"].mean()), 4)
        elif not tmp.empty:
            tmp["brand_id"] = tmp["project_id"].apply(lambda x: project_brand.get(int(x), None))
            for bid, g in tmp.dropna(subset=["brand_id"]).groupby("brand_id"):
                brand_intensity_map[int(bid)] = round(float(g["intensity"].mean()), 4)

    # Aggregate per brand
    if "brand_id" in daily_df.columns and daily_df["brand_id"].notna().any():
        daily_df["brand_id_norm"] = pd.to_numeric(daily_df["brand_id"], errors="coerce")
    else:
        daily_df["brand_id_norm"] = daily_df["project_id"].apply(lambda x: project_brand.get(int(x), None))
    items: List[Dict[str, Any]] = []
    for bid, g in daily_df.dropna(subset=["brand_id_norm"]).groupby("brand_id_norm"):
        bid_int = int(pd.to_numeric(bid, errors="coerce") or 0)
        if bid_int <= 0:
            continue
        if bid_int not in brand_ids:
            continue
        total = int(g.get("total_posts", 0).sum())
        valid = int(g.get("valid_posts", 0).sum())
        spam = int(g.get("spam_posts", 0).sum())
        pos = int(g.get("pos_posts", 0).sum())
        neu = int(g.get("neu_posts", 0).sum())
        neg = int(g.get("neg_posts", 0).sum())
        denom = max(pos + neu + neg, 0)
        items.append(
            {
                "brand_id": bid_int,
                "brand_name": brand_map.get(bid_int, f"brand_{bid_int}"),
                "total_posts": total,
                "positive_ratio": _safe_ratio(pos, denom),
                "negative_ratio": _safe_ratio(neg, denom),
                "intensity": brand_intensity_map.get(bid_int, 0.0),
                "spam_ratio": _safe_ratio(spam, max(valid, 0)),
            }
        )
    items.sort(key=lambda x: x.get("total_posts", 0), reverse=True)

    return {"range": {"from": start.date().isoformat(), "to": end.date().isoformat()}, "items": items}


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

    if daily_df.empty or not {"platform_id", "metric_date", "total_posts"}.issubset(daily_df.columns):
        return {"dates": [], "series": []}

    daily_df["metric_date"] = pd.to_datetime(daily_df["metric_date"], errors="coerce")
    daily_df = daily_df.dropna(subset=["metric_date"])
    if daily_df.empty:
        return {"dates": [], "series": []}

    # Build unified date axis
    daily_df["date_str"] = daily_df["metric_date"].dt.strftime("%Y-%m-%d")
    dates = sorted(daily_df["date_str"].dropna().unique().tolist())
    date_idx = {d: i for i, d in enumerate(dates)}

    series = []
    grouped = daily_df.groupby("platform_id")
    for pid, g in grouped:
        data = [0] * len(dates)
        for _, r in g.iterrows():
            ds = r.get("date_str")
            if ds not in date_idx:
                continue
            val = pd.to_numeric(r.get("total_posts"), errors="coerce")
            data[date_idx[ds]] = int(val) if pd.notna(val) else 0
        series.append({"name": name_map.get(int(pid), f"platform_{pid}"), "data": data})
    return {"dates": dates, "series": series}


@router.get("/sentiment_trends")
def get_sentiment_trends(
    brand_ids: Optional[List[int]] = Query(None),
    project_ids: Optional[List[int]] = Query(None, description="(Deprecated) 1-4 project ids"),
    project_id: Optional[int] = Query(None, description="Active project id (optional)."),
    days: int = Query(14, ge=1, le=365),
    start_date: Optional[str] = Query(None, description="Custom date range start (YYYY-MM-DD)."),
    end_date: Optional[str] = Query(None, description="Custom date range end (YYYY-MM-DD)."),
    metric: str = Query("negative_ratio"),
    user=Depends(get_current_user),
):
    """
    Trend lines by brand, aggregated across projects/platforms per day.
    """
    # Backward-compatible input: old frontend used `project_ids`.
    if not brand_ids and project_ids:
        proj_brand = _project_brand_map(get_repo())
        derived = []
        for pid in project_ids:
            try:
                bid = proj_brand.get(int(pid))
                if bid is not None:
                    derived.append(int(bid))
            except Exception:
                continue
        brand_ids = derived

    if not brand_ids:
        raise HTTPException(status_code=422, detail="brand_ids is required (or provide deprecated project_ids)")

    brand_ids = [int(x) for x in brand_ids][:4]
    if metric not in {"negative_ratio", "positive_ratio", "spam_ratio"}:
        metric = "negative_ratio"
    repo = get_repo()
    start, end = _parse_range(days, start_date, end_date)
    project_ids, project_brand = _projects_for_brands(repo, brand_ids)
    if project_id is not None:
        project_ids = [int(project_id)] if int(project_id) in set(project_ids) else []
    if not project_ids:
        return {"dates": [], "series": []}
    daily_df = repo.query("daily_metric")
    if daily_df.empty or not {"project_id", "metric_date"}.issubset(daily_df.columns):
        return {"dates": [], "series": []}

    daily_df["metric_date"] = pd.to_datetime(daily_df.get("metric_date"), errors="coerce")
    daily_df = daily_df.dropna(subset=["metric_date"])
    daily_df = daily_df[(daily_df["metric_date"] >= start) & (daily_df["metric_date"] <= end)]
    daily_df["date_str"] = daily_df["metric_date"].dt.strftime("%Y-%m-%d")
    daily_df = _coerce_daily_metric_numeric(daily_df)
    daily_df = daily_df[daily_df["project_id"].astype(int).isin(project_ids)]
    if daily_df.empty:
        return {"dates": [], "series": []}

    dates = sorted(daily_df["date_str"].dropna().unique().tolist())
    date_idx = {d: i for i, d in enumerate(dates)}
    brand_map = _brand_name_map(repo)

    def row_value(row: pd.Series) -> float:
        def n(key: str) -> float:
            v = pd.to_numeric(row.get(key), errors="coerce")
            return float(v) if pd.notna(v) else 0.0

        total = n("total_posts")
        valid = n("valid_posts")
        spam = n("spam_posts")
        pos = n("pos_posts")
        neu = n("neu_posts")
        neg = n("neg_posts")
        denom = max(pos + neu + neg, 0.0)
        if metric == "negative_ratio":
            return round((neg / denom), 4) if denom else 0.0
        if metric == "positive_ratio":
            return round((pos / denom), 4) if denom else 0.0
        # spam_ratio
        return round((spam / valid), 4) if valid else 0.0

    series = []
    metric_cols = ["total_posts", "valid_posts", "spam_posts", "pos_posts", "neu_posts", "neg_posts"]
    if "brand_id" in daily_df.columns and daily_df["brand_id"].notna().any():
        daily_df["brand_id_norm"] = pd.to_numeric(daily_df["brand_id"], errors="coerce")
    else:
        daily_df["brand_id_norm"] = daily_df["project_id"].apply(lambda x: project_brand.get(int(x), None))
    for bid, g in daily_df.dropna(subset=["brand_id_norm"]).groupby("brand_id_norm"):
        bid_int = int(pd.to_numeric(bid, errors="coerce") or 0)
        if bid_int <= 0:
            continue
        if bid_int not in brand_ids:
            continue
        data = [0.0] * len(dates)
        cols = [c for c in metric_cols if c in g.columns]
        grouped = g.groupby("date_str")[cols].sum().reset_index()
        for _, r in grouped.iterrows():
            ds = r.get("date_str")
            if ds not in date_idx:
                continue
            data[date_idx[ds]] = row_value(r)
        series.append({"brand_id": bid_int, "name": brand_map.get(bid_int, f"brand_{bid_int}"), "data": data})
    return {"dates": dates, "series": series, "metric": metric}


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


@router.get("/competitor_ranking")
def get_competitor_ranking(
    brand_ids: Optional[List[int]] = Query(None),
    days: int = Query(7, ge=1, le=365),
    start_date: Optional[str] = Query(None, description="Custom date range start (YYYY-MM-DD)."),
    end_date: Optional[str] = Query(None, description="Custom date range end (YYYY-MM-DD)."),
    user=Depends(get_current_user),
):
    """
    Ranking by brand. "Heat" is a simple composite score.
    """
    repo = get_repo()
    start, end = _parse_range(days, start_date, end_date)
    daily_df = repo.query("daily_metric")
    if daily_df.empty or "project_id" not in daily_df.columns:
        return []
    daily_df["metric_date"] = pd.to_datetime(daily_df.get("metric_date"), errors="coerce")
    daily_df = daily_df.dropna(subset=["metric_date"])
    daily_df = daily_df[(daily_df["metric_date"] >= start) & (daily_df["metric_date"] <= end)]
    daily_df = _coerce_daily_metric_numeric(daily_df)
    if daily_df.empty:
        return []

    brand_map = _brand_name_map(repo)
    project_brand = _project_brand_map(repo)
    rows: List[Dict[str, Any]] = []
    if "brand_id" in daily_df.columns and daily_df["brand_id"].notna().any():
        daily_df["brand_id_norm"] = pd.to_numeric(daily_df["brand_id"], errors="coerce")
    else:
        daily_df["brand_id_norm"] = daily_df["project_id"].apply(lambda x: project_brand.get(int(x), None))
    if brand_ids:
        allowed = set(int(x) for x in brand_ids[:4])
        daily_df = daily_df[daily_df["brand_id_norm"].isin(list(allowed))]
    for bid, g in daily_df.dropna(subset=["brand_id_norm"]).groupby("brand_id_norm"):
        bid_int = int(pd.to_numeric(bid, errors="coerce") or 0)
        if bid_int <= 0:
            continue
        total = float(pd.to_numeric(g.get("total_posts"), errors="coerce").fillna(0).sum())
        spam = float(pd.to_numeric(g.get("spam_posts"), errors="coerce").fillna(0).sum())
        neg = float(pd.to_numeric(g.get("neg_posts"), errors="coerce").fillna(0).sum())
        pos = float(pd.to_numeric(g.get("pos_posts"), errors="coerce").fillna(0).sum())
        neu = float(pd.to_numeric(g.get("neu_posts"), errors="coerce").fillna(0).sum())
        heat = max(total - 0.5 * spam + 0.2 * abs(neg - pos), 0.0)
        valid = float(pd.to_numeric(g.get("valid_posts"), errors="coerce").fillna(0).sum())
        denom_sent = max(pos + neu + neg, 0.0)
        rows.append(
            {
                "brand_id": bid_int,
                "name": brand_map.get(bid_int, f"brand_{bid_int}"),
                "heat": int(round(heat)),
                "total_posts": int(total),
                "spam_posts": int(spam),
                "negative_ratio": round((neg / denom_sent), 4) if denom_sent else 0.0,
                "spam_ratio": round((spam / valid), 4) if valid else 0.0,
            }
        )
    rows.sort(key=lambda x: x["heat"], reverse=True)
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


@router.get("/sentiment_alerts")
def get_sentiment_alerts(
    brand_ids: Optional[List[int]] = Query(None),
    project_id: Optional[int] = Query(None, description="Active project id (optional)."),
    days: int = Query(14, ge=2, le=365),
    start_date: Optional[str] = Query(None, description="Custom date range start (YYYY-MM-DD)."),
    end_date: Optional[str] = Query(None, description="Custom date range end (YYYY-MM-DD)."),
    min_total: int = Query(10, ge=0, le=100000),
    user=Depends(get_current_user),
):
    """
    Alert on sentiment ratio spikes (positive or negative) between the latest two days.
    """
    repo = get_repo()
    start, end = _parse_range(days, start_date, end_date)
    daily_df = repo.query("daily_metric")
    if daily_df.empty or not {"project_id", "metric_date"}.issubset(daily_df.columns):
        return []

    daily_df["metric_date"] = pd.to_datetime(daily_df.get("metric_date"), errors="coerce")
    daily_df = daily_df.dropna(subset=["metric_date"])
    daily_df = daily_df[(daily_df["metric_date"] >= start) & (daily_df["metric_date"] <= end)]
    daily_df = _coerce_daily_metric_numeric(daily_df)
    if project_id is not None and "project_id" in daily_df.columns:
        daily_df = daily_df[daily_df["project_id"].astype(int) == int(project_id)]
    if daily_df.empty:
        return []

    daily_df["date_str"] = daily_df["metric_date"].dt.strftime("%Y-%m-%d")
    all_dates = sorted(daily_df["date_str"].unique().tolist())
    if len(all_dates) < 2:
        return []
    d2, d1 = all_dates[-2], all_dates[-1]  # previous, latest

    brand_map = _brand_name_map(repo)
    project_brand = _project_brand_map(repo)
    alerts: List[Dict[str, Any]] = []

    if "brand_id" in daily_df.columns and daily_df["brand_id"].notna().any():
        daily_df["brand_id_norm"] = pd.to_numeric(daily_df["brand_id"], errors="coerce")
    else:
        daily_df["brand_id_norm"] = daily_df["project_id"].apply(lambda x: project_brand.get(int(x), None))
    if brand_ids:
        allowed = set(int(x) for x in brand_ids[:4])
        daily_df = daily_df[daily_df["brand_id_norm"].isin(list(allowed))]

    for bid, g in daily_df.dropna(subset=["brand_id_norm"]).groupby("brand_id_norm"):
        bid_int = int(pd.to_numeric(bid, errors="coerce") or 0)
        if bid_int <= 0:
            continue
        metric_cols = ["total_posts", "valid_posts", "spam_posts", "pos_posts", "neu_posts", "neg_posts"]
        cols = [c for c in metric_cols if c in g.columns]
        agg = g.groupby("date_str")[cols].sum()
        if d1 not in agg.index or d2 not in agg.index:
            continue

        def ratios(row: pd.Series) -> Tuple[float, float, int]:
            pos = float(row.get("pos_posts") or 0)
            neu = float(row.get("neu_posts") or 0)
            neg = float(row.get("neg_posts") or 0)
            total = int(row.get("total_posts") or 0)
            denom = max(pos + neu + neg, 0.0)
            pos_r = (pos / denom) if denom else 0.0
            neg_r = (neg / denom) if denom else 0.0
            return pos_r, neg_r, total

        pos2, neg2, tot2 = ratios(agg.loc[d2])
        pos1, neg1, tot1 = ratios(agg.loc[d1])

        # only alert when volume is meaningful
        if tot1 < int(min_total):
            continue

        neg_delta = neg1 - neg2
        pos_delta = pos1 - pos2

        if abs(neg_delta) >= 0.2:
            level = "high" if abs(neg_delta) >= 0.35 else "medium"
            alerts.append(
                {
                    "level": level,
                    "brand_id": bid_int,
                    "product": brand_map.get(bid_int, f"brand_{bid_int}"),
                    "type": "negative_spike" if neg_delta > 0 else "negative_drop",
                    "reason": f"负面占比变化 {neg_delta:+.1%}（{d2}→{d1}）",
                }
            )
        if abs(pos_delta) >= 0.2:
            level = "high" if abs(pos_delta) >= 0.35 else "medium"
            alerts.append(
                {
                    "level": level,
                    "brand_id": bid_int,
                    "product": brand_map.get(bid_int, f"brand_{bid_int}"),
                    "type": "positive_spike" if pos_delta > 0 else "positive_drop",
                    "reason": f"正面占比变化 {pos_delta:+.1%}（{d2}→{d1}）",
                }
            )

    return alerts


@router.get("/keyword_frequencies")
def get_keyword_frequencies(
    brand_ids: Optional[List[int]] = Query(None, description="Filter by brand ids (repeatable)."),
    project_id: Optional[int] = Query(None, description="Active project id (optional)."),
    platform_id: Optional[int] = Query(None, description="Optional platform filter (reserved)."),
    days: int = Query(14, ge=1, le=365),
    start_date: Optional[str] = Query(None, description="Custom date range start (YYYY-MM-DD)."),
    end_date: Optional[str] = Query(None, description="Custom date range end (YYYY-MM-DD)."),
    top_n: int = Query(12, ge=3, le=50),
    user=Depends(get_current_user),
):
    """
    Keyword frequency statistics from `post_raw.keyword_id` (joined to `monitor_keyword`).
    Returns the top keywords overall and a per-brand breakdown for charting.
    """
    repo = get_repo()
    start, end = _parse_range(days, start_date, end_date)

    df = repo.query("post_raw")
    if df is None or df.empty:
        return {"top_keywords": [], "brands": [], "range": {"from": start.date().isoformat(), "to": end.date().isoformat()}}

    df = df.copy()

    # Ensure publish_time is datetime for time filtering.
    if "publish_time" in df.columns:
        df["publish_time"] = pd.to_datetime(df["publish_time"], errors="coerce")
        df = df.dropna(subset=["publish_time"])
        df = df[(df["publish_time"] >= start) & (df["publish_time"] <= end)]
    if project_id is not None and "project_id" in df.columns:
        df["project_id"] = pd.to_numeric(df["project_id"], errors="coerce")
        df = df.dropna(subset=["project_id"])
        df = df[df["project_id"].astype(int) == int(project_id)]
    if platform_id is not None and "platform_id" in df.columns:
        df["platform_id"] = pd.to_numeric(df["platform_id"], errors="coerce")
        df = df.dropna(subset=["platform_id"])
        df = df[df["platform_id"].astype(int) == int(platform_id)]
    if df.empty:
        return {"top_keywords": [], "brands": [], "range": {"from": start.date().isoformat(), "to": end.date().isoformat()}}

    # Normalize keyword_id.
    if "keyword_id" not in df.columns:
        return {"top_keywords": [], "brands": [], "range": {"from": start.date().isoformat(), "to": end.date().isoformat()}}
    df["keyword_id"] = pd.to_numeric(df["keyword_id"], errors="coerce")
    df = df.dropna(subset=["keyword_id"])
    if df.empty:
        return {"top_keywords": [], "brands": [], "range": {"from": start.date().isoformat(), "to": end.date().isoformat()}}
    df["keyword_id"] = df["keyword_id"].astype(int)

    # Normalize brand_id (prefer stored brand_id; fallback to project -> brand mapping for legacy rows).
    if "brand_id" in df.columns:
        df["brand_id"] = pd.to_numeric(df["brand_id"], errors="coerce")
    else:
        df["brand_id"] = None

    if "project_id" in df.columns:
        project_brand = _project_brand_map(repo)
        df["project_id"] = pd.to_numeric(df["project_id"], errors="coerce")
        m = df["brand_id"].isna() & pd.notna(df["project_id"])
        if m.any():
            df.loc[m, "brand_id"] = df.loc[m, "project_id"].astype(int).map(project_brand)

    df["brand_id"] = pd.to_numeric(df["brand_id"], errors="coerce")
    df = df.dropna(subset=["brand_id"])
    if df.empty:
        return {"top_keywords": [], "brands": [], "range": {"from": start.date().isoformat(), "to": end.date().isoformat()}}
    df["brand_id"] = df["brand_id"].astype(int)

    if brand_ids:
        allowed = set(int(x) for x in brand_ids if x is not None)
        df = df[df["brand_id"].isin(list(allowed))]
        if df.empty:
            return {"top_keywords": [], "brands": [], "range": {"from": start.date().isoformat(), "to": end.date().isoformat()}}

    # keyword_id -> keyword
    kw_df = repo.query("monitor_keyword")
    if kw_df is None or kw_df.empty or not {"id", "keyword"}.issubset(kw_df.columns):
        return {"top_keywords": [], "brands": [], "range": {"from": start.date().isoformat(), "to": end.date().isoformat()}}
    kw_df = kw_df.copy()
    kw_df["id"] = pd.to_numeric(kw_df["id"], errors="coerce")
    kw_df = kw_df.dropna(subset=["id"])
    kw_df["id"] = kw_df["id"].astype(int)
    kw_map = dict(zip(kw_df["id"].tolist(), kw_df["keyword"].tolist()))
    df["keyword"] = df["keyword_id"].map(kw_map)
    df = df.dropna(subset=["keyword"])
    if df.empty:
        return {"top_keywords": [], "brands": [], "range": {"from": start.date().isoformat(), "to": end.date().isoformat()}}

    # Overall top keywords.
    overall = df.groupby("keyword").size().sort_values(ascending=False)
    overall = overall.head(int(top_n))
    top_keywords = [{"keyword": str(k), "count": int(v)} for k, v in overall.items()]
    top_list = [str(x["keyword"]) for x in top_keywords]
    top_set = set(top_list)

    # Per-brand counts for top keywords.
    df2 = df[df["keyword"].astype(str).isin(list(top_set))]
    brand_map = _brand_name_map(repo)
    brands_out: List[Dict[str, Any]] = []
    if not df2.empty:
        pivot = df2.groupby(["brand_id", "keyword"]).size().reset_index(name="count")
        for bid, g in pivot.groupby("brand_id"):
            bid_int = int(bid)
            item_map = {str(r["keyword"]): int(r["count"]) for _, r in g.iterrows()}
            brands_out.append(
                {
                    "brand_id": bid_int,
                    "brand_name": brand_map.get(bid_int, f"brand_{bid_int}"),
                    "items": [{"keyword": k, "count": int(item_map.get(k, 0))} for k in top_list],
                }
            )

    brands_out.sort(key=lambda x: x.get("brand_name") or "")
    return {
        "top_keywords": top_keywords,
        "brands": brands_out,
        "range": {"from": start.date().isoformat(), "to": end.date().isoformat()},
    }

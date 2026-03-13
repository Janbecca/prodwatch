from fastapi import APIRouter, Query
from typing import List, Optional, Set

from datetime import datetime, timedelta
import pandas as pd

from backend.storage.db import get_latest_pipeline_run_id, get_repo

router = APIRouter(prefix="/posts", tags=["posts"])


def _projects_for_brands(repo, brand_ids: List[int]) -> Set[int]:
    projects_df = repo.query("monitor_project")
    if projects_df.empty or not {"id", "brand_id"}.issubset(projects_df.columns):
        return set()

    allowed = set(int(x) for x in brand_ids if x is not None)
    out: Set[int] = set()
    for _, row in projects_df.iterrows():
        bid = pd.to_numeric(row.get("brand_id"), errors="coerce")
        pid = pd.to_numeric(row.get("id"), errors="coerce")
        if pd.notna(bid) and pd.notna(pid) and int(bid) in allowed:
            out.add(int(pid))
    return out


@router.get("")
def list_posts(
    platform_id: Optional[int] = Query(None),
    project_id: Optional[int] = Query(None),
    brand_ids: Optional[List[int]] = Query(None, description="Filter by brand ids (repeatable)."),
    days: Optional[int] = Query(None, ge=1, le=365, description="Relative time range: last N days."),
    start_date: Optional[str] = Query(None, description="Custom date range start (YYYY-MM-DD)."),
    end_date: Optional[str] = Query(None, description="Custom date range end (YYYY-MM-DD)."),
    mode: Optional[str] = Query("latest_run"),
):
    repo = get_repo()

    filters = {}
    if platform_id is not None:
        filters["platform_id"] = int(platform_id)

    allowed_projects: Optional[Set[int]] = None
    if brand_ids:
        allowed_projects = _projects_for_brands(repo, [int(x) for x in brand_ids if x is not None])

    if project_id is not None:
        allowed_projects = ({int(project_id)} if allowed_projects is None else (allowed_projects & {int(project_id)}))

    if mode == "latest_run":
        latest_run_id = get_latest_pipeline_run_id(repo)
        if latest_run_id is None:
            return []
        filters["pipeline_run_id"] = latest_run_id

    # Push down an equality filter when possible (Excel repo likely only supports eq filters).
    if allowed_projects is not None:
        if len(allowed_projects) == 0:
            return []
        if len(allowed_projects) == 1:
            filters["project_id"] = next(iter(allowed_projects))
    elif project_id is not None:
        filters["project_id"] = int(project_id)

    df = repo.query("post_raw", filters if filters else None)

    if allowed_projects is not None and not df.empty and "project_id" in df.columns and "project_id" not in filters:
        df["project_id"] = pd.to_numeric(df["project_id"], errors="coerce")
        df = df.dropna(subset=["project_id"])
        df = df[df["project_id"].astype(int).isin(list(allowed_projects))]

    if "id" in df.columns:
        df = df[pd.to_numeric(df["id"], errors="coerce").notna()]
        df["id"] = pd.to_numeric(df["id"], errors="coerce").astype(int)

    if not df.empty:
        if "project_id" in df.columns:
            df["project_id"] = pd.to_numeric(df["project_id"], errors="coerce")
        if "pipeline_run_id" in df.columns:
            df["pipeline_run_id"] = pd.to_numeric(df["pipeline_run_id"], errors="coerce")
        if "platform_id" in df.columns:
            df["platform_id"] = pd.to_numeric(df["platform_id"], errors="coerce")
        if "keyword_id" in df.columns:
            df["keyword_id"] = pd.to_numeric(df["keyword_id"], errors="coerce")

    if "publish_time" in df.columns:
        df["publish_time"] = pd.to_datetime(df["publish_time"], errors="coerce")

        # If time filtering is requested but publish_time is missing/invalid, backfill from pipeline_run timestamps.
        needs_time_filter = bool(start_date or end_date or days is not None)
        if needs_time_filter and df["publish_time"].isna().any() and "pipeline_run_id" in df.columns:
            run_df = repo.query("pipeline_run")
            if not run_df.empty and "id" in run_df.columns:
                for col in ["start_time", "created_at", "end_time"]:
                    if col in run_df.columns:
                        run_df[col] = pd.to_datetime(run_df[col], errors="coerce")
                run_df["id"] = pd.to_numeric(run_df["id"], errors="coerce")
                run_df = run_df.dropna(subset=["id"])
                run_df["id"] = run_df["id"].astype(int)

                def _pick_ts(row: pd.Series):
                    for col in ["start_time", "created_at", "end_time"]:
                        if col in row.index and pd.notna(row.get(col)):
                            return row.get(col)
                    return pd.NaT

                run_df["_ts"] = run_df.apply(_pick_ts, axis=1)
                run_map = dict(zip(run_df["id"].tolist(), run_df["_ts"].tolist()))

                df["pipeline_run_id"] = pd.to_numeric(df["pipeline_run_id"], errors="coerce")
                mask = df["publish_time"].isna() & df["pipeline_run_id"].notna()
                if mask.any():
                    df.loc[mask, "publish_time"] = df.loc[mask, "pipeline_run_id"].astype(int).map(run_map)

            # last resort: avoid dropping all rows when publish_time is empty
            df["publish_time"] = df["publish_time"].fillna(pd.Timestamp(datetime.utcnow()))

        # time filtering (custom range has priority over relative days)
        if start_date or end_date:
            start_dt = pd.to_datetime(start_date, errors="coerce") if start_date else None
            end_dt = pd.to_datetime(end_date, errors="coerce") if end_date else None
            if start_dt is not None and not pd.isna(start_dt):
                df = df[df["publish_time"] >= start_dt]
            if end_dt is not None and not pd.isna(end_dt):
                # include the whole end day if user passed a date without time
                if isinstance(end_dt, pd.Timestamp) and end_dt.hour == 0 and end_dt.minute == 0 and end_dt.second == 0:
                    end_dt = end_dt + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
                df = df[df["publish_time"] <= end_dt]
        elif days is not None:
            start = datetime.utcnow() - timedelta(days=int(days))
            df = df[df["publish_time"] >= start]
        df = df.sort_values(by="publish_time", ascending=False)

    if not df.empty and "publish_time" in df.columns:
        def _iso(x):
            if x is None or (isinstance(x, float) and pd.isna(x)) or pd.isna(x):
                return None
            if isinstance(x, pd.Timestamp):
                return x.to_pydatetime().isoformat()
            if hasattr(x, "isoformat"):
                try:
                    return x.isoformat()
                except Exception:
                    return str(x)
            return str(x)

        df["publish_time"] = df["publish_time"].map(_iso)

    # Enrich rows for the UI: brand / keyword / sentiment scores.
    if not df.empty:
        # project_id -> brand
        if "project_id" in df.columns:
            proj_df = repo.query("monitor_project")
            brand_df = repo.query("brand")

            if not proj_df.empty and {"id", "brand_id"}.issubset(proj_df.columns):
                proj_df = proj_df.copy()
                proj_df["id"] = pd.to_numeric(proj_df["id"], errors="coerce")
                proj_df["brand_id"] = pd.to_numeric(proj_df["brand_id"], errors="coerce")
                proj_df = proj_df.dropna(subset=["id", "brand_id"])
                proj_df["id"] = proj_df["id"].astype(int)
                proj_df["brand_id"] = proj_df["brand_id"].astype(int)
                project_to_brand = dict(zip(proj_df["id"].tolist(), proj_df["brand_id"].tolist()))

                df["brand_id"] = df["project_id"].dropna().astype(int).map(project_to_brand)
            else:
                df["brand_id"] = None

            if not brand_df.empty and {"id", "name"}.issubset(brand_df.columns):
                brand_df = brand_df.copy()
                brand_df["id"] = pd.to_numeric(brand_df["id"], errors="coerce")
                brand_df = brand_df.dropna(subset=["id"])
                brand_df["id"] = brand_df["id"].astype(int)
                brand_id_to_name = dict(zip(brand_df["id"].tolist(), brand_df["name"].tolist()))
                df["brand_name"] = df.get("brand_id").map(brand_id_to_name)
            else:
                df["brand_name"] = None

        # keyword_id -> keyword
        if "keyword_id" in df.columns:
            kw_df = repo.query("monitor_keyword")
            if not kw_df.empty and {"id", "keyword"}.issubset(kw_df.columns):
                kw_df = kw_df.copy()
                kw_df["id"] = pd.to_numeric(kw_df["id"], errors="coerce")
                kw_df = kw_df.dropna(subset=["id"])
                kw_df["id"] = kw_df["id"].astype(int)
                keyword_map = dict(zip(kw_df["id"].tolist(), kw_df["keyword"].tolist()))
                df["keyword"] = None
                mask = pd.notna(df["keyword_id"])
                if mask.any():
                    df.loc[mask, "keyword"] = df.loc[mask, "keyword_id"].astype(int).map(keyword_map)
            else:
                df["keyword"] = None

        # post_raw.id -> post_clean.id -> sentiment_result.*
        raw_ids: List[int] = df["id"].dropna().astype(int).tolist() if "id" in df.columns else []
        if raw_ids:
            clean_df = repo.query("post_clean")
            if not clean_df.empty and {"id", "post_raw_id"}.issubset(clean_df.columns):
                clean_df = clean_df.copy()
                clean_df["id"] = pd.to_numeric(clean_df["id"], errors="coerce")
                clean_df["post_raw_id"] = pd.to_numeric(clean_df["post_raw_id"], errors="coerce")
                clean_df = clean_df.dropna(subset=["id", "post_raw_id"])
                clean_df["id"] = clean_df["id"].astype(int)
                clean_df["post_raw_id"] = clean_df["post_raw_id"].astype(int)
                clean_df = clean_df.sort_values(by="id").drop_duplicates(subset=["post_raw_id"], keep="last")
                raw_to_clean = dict(zip(clean_df["post_raw_id"].tolist(), clean_df["id"].tolist()))
                df["post_clean_id"] = df["id"].map(raw_to_clean)
            else:
                df["post_clean_id"] = None

            sent_df = repo.query("sentiment_result")
            if (
                not sent_df.empty
                and {"post_clean_id", "polarity", "confidence", "intensity", "emotions"}.issubset(sent_df.columns)
            ):
                sent_df = sent_df.copy()
                sent_df["post_clean_id"] = pd.to_numeric(sent_df["post_clean_id"], errors="coerce")
                sent_df = sent_df.dropna(subset=["post_clean_id"])
                sent_df["post_clean_id"] = sent_df["post_clean_id"].astype(int)
                if "id" in sent_df.columns:
                    sent_df["id"] = pd.to_numeric(sent_df["id"], errors="coerce")
                    sent_df = sent_df.sort_values(by="id").drop_duplicates(subset=["post_clean_id"], keep="last")
                sent_map = {
                    int(row["post_clean_id"]): {
                        "polarity": row.get("polarity"),
                        "confidence": row.get("confidence"),
                        "intensity": row.get("intensity"),
                        "emotions": row.get("emotions"),
                    }
                    for _, row in sent_df.iterrows()
                }

                def _sent_val(x, key: str):
                    if x is None or (isinstance(x, float) and pd.isna(x)):
                        return None
                    try:
                        cid = int(x)
                    except Exception:
                        return None
                    return sent_map.get(cid, {}).get(key)

                df["polarity"] = df["post_clean_id"].map(lambda x: _sent_val(x, "polarity"))
                df["confidence"] = df["post_clean_id"].map(lambda x: _sent_val(x, "confidence"))
                df["intensity"] = df["post_clean_id"].map(lambda x: _sent_val(x, "intensity"))
                df["emotions"] = df["post_clean_id"].map(lambda x: _sent_val(x, "emotions"))
            else:
                df["polarity"] = None
                df["confidence"] = None
                df["intensity"] = None
                df["emotions"] = None

    # Starlette JSONResponse uses `allow_nan=False`, so sanitize NaN/Inf.
    if not df.empty:
        df = df.replace([float("inf"), float("-inf")], None)
        df = df.astype(object).where(pd.notnull(df), None)

    return df.to_dict(orient="records")


@router.get("/{post_id}")
def get_post(post_id: int):
    repo = get_repo()
    df = repo.query("post_raw", {"id": post_id})
    if df.empty:
        return {"id": post_id, "detail": "not found"}
    return df.iloc[0].to_dict()

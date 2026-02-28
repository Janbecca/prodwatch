from fastapi import APIRouter, Query
from typing import Optional
import pandas as pd
from backend.storage.db import get_repo, get_latest_pipeline_run_id

router = APIRouter(prefix="/posts", tags=["posts"])


@router.get("")
def list_posts(
    platform_id: Optional[int] = Query(None),
    project_id: Optional[int] = Query(None),
    mode: Optional[str] = Query("latest_run"),
):
    repo = get_repo()
    filters = {}
    if platform_id is not None:
        filters["platform_id"] = platform_id
    if project_id is not None:
        filters["project_id"] = project_id
    if mode == "latest_run":
        latest_run_id = get_latest_pipeline_run_id(repo)
        if latest_run_id is None:
            return []
        filters["pipeline_run_id"] = latest_run_id

    df = repo.query("post_raw", filters if filters else None)
    if "id" in df.columns:
        df = df[pd.to_numeric(df["id"], errors="coerce").notna()]
    if "publish_time" in df.columns:
        df["publish_time"] = pd.to_datetime(df["publish_time"], errors="coerce")
        df = df.sort_values(by="publish_time", ascending=False)
    return df.to_dict(orient="records")


@router.get("/{post_id}")
def get_post(post_id: int):
    repo = get_repo()
    df = repo.query("post_raw", {"id": post_id})
    if df.empty:
        return {"id": post_id, "detail": "not found"}
    return df.iloc[0].to_dict()

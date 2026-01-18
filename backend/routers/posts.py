# backend/routers/posts.py
from fastapi import APIRouter, Query
from typing import Optional
from backend.storage.db import get_repo

router = APIRouter(prefix="/posts", tags=["posts"])


@router.get("")  # 最终暴露为 GET /api/posts
def list_posts(
    platform: Optional[str] = Query(None),
    project_id: Optional[int] = Query(None),
):
    repo = get_repo()
    filters = {}
    if platform is not None:
        filters["platform"] = platform
    if project_id is not None:
        filters["project_id"] = project_id
    df = repo.query("post_raw", filters if filters else None)
    return df.to_dict(orient="records")


@router.get("/{post_id}")
def get_post(post_id: int):
    return {"id": post_id, "title": f"示例贴文#{post_id}", "content": "..."}

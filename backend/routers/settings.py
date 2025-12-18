from fastapi import APIRouter, Depends, HTTPException
from typing import List
from .auth import get_current_user

router = APIRouter(prefix="/settings", tags=["settings"])


@router.get("/datasources")
def list_datasources(user=Depends(get_current_user)):
    return [{"id": "weibo", "freq": "*/10 * * * *"}, {"id": "bilibili", "freq": "*/15 * * * *"}]


@router.post("/datasources")
def save_datasources(sources: List[dict], user=Depends(get_current_user)):
    return {"saved": len(sources)}


@router.get("/users")
def list_users(user=Depends(get_current_user)):
    # admin-only example (omitted check for brevity)
    return []

from fastapi import APIRouter, Depends
from typing import Optional
from .auth import get_current_user

router = APIRouter(prefix="/moderation", tags=["moderation"])


@router.get("/spam/overview")
def spam_overview(user=Depends(get_current_user)):
    return {"spam_ratio": 0.12, "total": 1000, "spam": 120}


@router.get("/spam/list")
def spam_list(product: Optional[str] = None, user=Depends(get_current_user)):
    return [{"id": 101, "product": product or "CamX", "reason": "重复内容/高频账号"}]

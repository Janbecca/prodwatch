from fastapi import APIRouter, Depends, Query
from typing import List, Optional
from .auth import get_current_user

router = APIRouter(prefix="/analysis", tags=["analysis"])


@router.get("/summary")
def analysis_summary(products: List[str] = Query(default=["CamX"]), start: Optional[str] = None, end: Optional[str] = None, user=Depends(get_current_user)):
    return {"products": products, "dimensions": ["画质","价格","售后"], "sentiment": {"positive": 0.6, "neutral": 0.3, "negative": 0.1}}


@router.get("/comments")
def list_comments(product: str, sentiment: Optional[str] = None, start: Optional[str] = None, end: Optional[str] = None, user=Depends(get_current_user)):
    return [{"id": 1, "product": product, "sentiment": sentiment or "positive", "text": "样例评论"}]

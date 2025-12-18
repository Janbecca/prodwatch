from fastapi import APIRouter, Depends
from .auth import get_current_user

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/kpis")
def get_kpis(user=Depends(get_current_user)):
    return {"comments": 12345, "positive": 0.62, "negative": 0.18, "intensity": 0.73}


@router.get("/trends")
def get_trends(user=Depends(get_current_user)):
    return {"series": [{"name": "BrandA", "data": [1,2,3,2,4]}]}


@router.get("/ranking")
def get_ranking(user=Depends(get_current_user)):
    return [{"product": "CamX", "score": 87}, {"product": "CamY", "score": 76}]


@router.get("/alerts")
def get_alerts(user=Depends(get_current_user)):
    return [{"level": "high", "product": "CamX", "reason": "负面情感激增"}]

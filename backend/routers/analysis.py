from fastapi import APIRouter, Query
from typing import List

from backend.agents.pipeline import run_manual_pipeline

router = APIRouter(prefix="/analysis", tags=["analysis"])


@router.post("/run")
def run_analysis(
    project_id: int = Query(...),
    platform: List[str] = Query(...),
    keyword: str = Query(...),
    model: str = Query("rule-based"),
):
    """
    Manual pipeline trigger:
    - crawl (mock) -> clean -> spam -> sentiment -> daily metrics -> report

    Manual fill:
    - Replace the mock crawler with real platform adapters.
    """
    result = run_manual_pipeline(project_id=project_id, platform_codes=platform, keyword=keyword, sentiment_model=model)
    return {
        "run_id": result.pipeline_run_id,
        "crawled_posts": result.crawled_posts,
        "cleaned_posts": result.cleaned_posts,
        "spam_scored": result.spam_scored,
        "sentiment_scored": result.sentiment_scored,
        "daily_metrics": result.daily_metrics,
        "report_id": result.report_id,
    }


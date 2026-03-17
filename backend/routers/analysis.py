from fastapi import APIRouter, Query
from typing import List, Optional

import pandas as pd

from backend.agents.pipeline import run_manual_pipeline
from backend.storage.db import get_latest_pipeline_run_id, get_repo

router = APIRouter(prefix="/analysis", tags=["analysis"])


@router.get("")
def list_analysis_results(
    limit: int = Query(50, ge=1, le=500),
    mode: str = Query("latest_run", description="latest_run or all"),
    pipeline_run_id: Optional[int] = Query(None),
):
    """
    Lightweight analysis results for the UI demo table.

    Returns rows: {content, sentiment, bot_score}.
    """
    repo = get_repo()

    filters = {}
    if mode == "latest_run":
        run_id = pipeline_run_id
        if run_id is None:
            run_id = get_latest_pipeline_run_id(repo)
        if run_id is None:
            return []
        filters["pipeline_run_id"] = int(run_id)

    raw_df = repo.query("post_raw", filters if filters else None)
    if raw_df is None or raw_df.empty:
        return []

    if "id" in raw_df.columns:
        raw_df = raw_df[~pd.isna(pd.to_numeric(raw_df["id"], errors="coerce"))]
        raw_df["id"] = pd.to_numeric(raw_df["id"], errors="coerce").astype(int)

    raw_df = raw_df.head(int(limit))

    clean_df = repo.query("post_clean", filters if filters else None)
    raw_to_clean = {}
    if clean_df is not None and not clean_df.empty and {"id", "post_raw_id"}.issubset(clean_df.columns):
        clean_df = clean_df.copy()
        clean_df["id"] = pd.to_numeric(clean_df["id"], errors="coerce")
        clean_df["post_raw_id"] = pd.to_numeric(clean_df["post_raw_id"], errors="coerce")
        clean_df = clean_df.dropna(subset=["id", "post_raw_id"])
        clean_df["id"] = clean_df["id"].astype(int)
        clean_df["post_raw_id"] = clean_df["post_raw_id"].astype(int)
        if "is_valid" in clean_df.columns:
            clean_df["is_valid"] = pd.to_numeric(clean_df["is_valid"], errors="coerce").fillna(0).astype(int)
            clean_df = clean_df[clean_df["is_valid"] == 1]
        clean_df = clean_df.sort_values(by="id").drop_duplicates(subset=["post_raw_id"], keep="last")
        raw_to_clean = dict(zip(clean_df["post_raw_id"].tolist(), clean_df["id"].tolist()))

    sent_df = repo.query("sentiment_result")
    sent_by_clean = {}
    if sent_df is not None and not sent_df.empty and {"post_clean_id", "polarity"}.issubset(sent_df.columns):
        sent_df = sent_df.copy()
        sent_df["post_clean_id"] = pd.to_numeric(sent_df["post_clean_id"], errors="coerce")
        sent_df = sent_df.dropna(subset=["post_clean_id"])
        sent_df["post_clean_id"] = sent_df["post_clean_id"].astype(int)
        if "id" in sent_df.columns:
            sent_df["id"] = pd.to_numeric(sent_df["id"], errors="coerce")
            sent_df = sent_df.sort_values(by="id").drop_duplicates(subset=["post_clean_id"], keep="last")
        sent_by_clean = dict(zip(sent_df["post_clean_id"].tolist(), sent_df["polarity"].astype(str).tolist()))

    spam_df = repo.query("spam_score")
    spam_by_clean = {}
    if spam_df is not None and not spam_df.empty and "post_clean_id" in spam_df.columns:
        spam_df = spam_df.copy()
        spam_df["post_clean_id"] = pd.to_numeric(spam_df["post_clean_id"], errors="coerce")
        spam_df = spam_df.dropna(subset=["post_clean_id"])
        spam_df["post_clean_id"] = spam_df["post_clean_id"].astype(int)
        if "id" in spam_df.columns:
            spam_df["id"] = pd.to_numeric(spam_df["id"], errors="coerce")
            spam_df = spam_df.sort_values(by="id").drop_duplicates(subset=["post_clean_id"], keep="last")
        score_col = "score_total" if "score_total" in spam_df.columns else None
        if score_col:
            spam_by_clean = dict(zip(spam_df["post_clean_id"].tolist(), pd.to_numeric(spam_df[score_col], errors="coerce").fillna(0.0).tolist()))
        else:
            spam_by_clean = dict(zip(spam_df["post_clean_id"].tolist(), [0.0] * len(spam_df)))

    out = []
    for _, r in raw_df.iterrows():
        rid = int(r.get("id"))
        cid = raw_to_clean.get(rid)
        out.append(
            {
                "content": str(r.get("raw_text") or ""),
                "sentiment": (sent_by_clean.get(int(cid), "neutral") if cid is not None else "neutral"),
                "bot_score": (float(spam_by_clean.get(int(cid), 0.0)) if cid is not None else 0.0),
            }
        )
    return out


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

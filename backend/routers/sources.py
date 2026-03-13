from fastapi import APIRouter, HTTPException
from backend.storage.db import EXCEL_PATH, get_repo
from backend.agents.importer import import_from_excel
from backend.agents.pipeline import process_existing_run

router = APIRouter(prefix="/sources", tags=["sources"])


@router.get("")
def list_sources():
    repo = get_repo()
    df = repo.query("platform")
    if df.empty:
        return []
    cols = [c for c in ["id", "code", "name", "created_at"] if c in df.columns]
    return df[cols].to_dict(orient="records")


@router.get("/{source_id}")
def get_source(source_id: str):
    repo = get_repo()
    df = repo.query("platform")
    if "code" not in df.columns:
        raise HTTPException(status_code=404, detail="source not found")
    target = df[df["code"].astype(str) == source_id]
    if target.empty:
        raise HTTPException(status_code=404, detail="source not found")
    return target.iloc[0].to_dict()


@router.post("/import_excel")
def import_excel():
    repo = get_repo()
    result = import_from_excel(repo, EXCEL_PATH)
    run_id = result.get("pipeline_run_id")
    if run_id:
        post = process_existing_run(int(run_id))
        result["post_process"] = {
            "cleaned_posts": post.cleaned_posts,
            "spam_scored": post.spam_scored,
            "sentiment_scored": post.sentiment_scored,
            "daily_metrics": post.daily_metrics,
            "report_id": post.report_id,
        }
    return result

from fastapi import APIRouter
import pandas as pd
from backend.storage.db import get_repo

router = APIRouter(prefix="/report", tags=["report"])


@router.get("")
def get_report_summary():
    repo = get_repo()
    df = repo.query("report")
    if df.empty:
        return {"summary": "", "generatedAt": None}

    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
        df = df.sort_values(by="created_at", ascending=False)
    row = df.iloc[0]
    return {
        "summary": row.get("summary"),
        "generatedAt": row.get("created_at").isoformat() if pd.notna(row.get("created_at")) else None,
        "title": row.get("title"),
        "reportType": row.get("report_type"),
        "pipelineRunId": row.get("pipeline_run_id"),
    }

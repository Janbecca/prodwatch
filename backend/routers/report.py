from typing import Optional

from fastapi import APIRouter, Query
import pandas as pd
from backend.storage.db import get_repo

router = APIRouter(prefix="/report", tags=["report"])


@router.get("")
def get_report_summary(
    project_id: Optional[int] = Query(None),
    pipeline_run_id: Optional[int] = Query(None),
):
    repo = get_repo()
    df = repo.query("report")
    if df.empty:
        return {"summary": "", "generatedAt": None}

    df = df.copy()
    if pipeline_run_id is not None and "pipeline_run_id" in df.columns:
        df["pipeline_run_id"] = pd.to_numeric(df["pipeline_run_id"], errors="coerce")
        df = df[df["pipeline_run_id"] == int(pipeline_run_id)]

    if project_id is not None and "project_id" in df.columns:
        df["project_id"] = pd.to_numeric(df["project_id"], errors="coerce")
        df = df[df["project_id"] == int(project_id)]

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
        "projectId": row.get("project_id"),
    }

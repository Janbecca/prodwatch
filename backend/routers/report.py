from typing import Optional

from fastapi import APIRouter, Query
import pandas as pd
from backend.storage.db import get_repo

router = APIRouter(prefix="/report", tags=["report"])


def _json_safe(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    # pandas/numpy scalar types
    if hasattr(value, "item") and callable(getattr(value, "item")):
        try:
            value = value.item()
        except Exception:
            pass

    # pandas Timestamp
    if hasattr(value, "to_pydatetime") and callable(getattr(value, "to_pydatetime")):
        try:
            return value.to_pydatetime()
        except Exception:
            pass

    # normalize numeric types (Excel often round-trips ints as floats)
    if isinstance(value, bool):
        return bool(value)
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return int(value) if float(value).is_integer() else float(value)
    return value


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
    created_at = _json_safe(row.get("created_at"))
    return {
        "summary": _json_safe(row.get("summary")),
        "generatedAt": (created_at.isoformat() if hasattr(created_at, "isoformat") else None),
        "title": _json_safe(row.get("title")),
        "reportType": _json_safe(row.get("report_type")),
        "pipelineRunId": _json_safe(row.get("pipeline_run_id")),
        "projectId": _json_safe(row.get("project_id")),
    }

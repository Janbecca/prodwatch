from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse

from backend.storage.db import get_repo
from .auth import get_current_user

router = APIRouter(prefix="/reports", tags=["reports"])


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _none_if_nan(x: Any) -> Any:
    try:
        return None if pd.isna(x) else x
    except Exception:
        return x


def _safe_int(x: Any) -> Optional[int]:
    v = pd.to_numeric(x, errors="coerce")
    if pd.isna(v):
        return None
    return int(v)


def _dt_iso(x: Any) -> Optional[str]:
    x = _none_if_nan(x)
    if x is None:
        return None
    dt = pd.to_datetime(x, errors="coerce")
    if pd.isna(dt):
        return None
    try:
        return dt.to_pydatetime().isoformat()
    except Exception:
        try:
            return dt.isoformat()
        except Exception:
            return str(dt)


def _next_int_id(df: Optional[pd.DataFrame]) -> int:
    if df is None or df.empty or "id" not in df.columns:
        return 1
    ids = pd.to_numeric(df["id"], errors="coerce").dropna()
    if ids.empty:
        return 1
    return int(ids.max()) + 1


def _parse_days(days: int) -> Tuple[datetime, datetime]:
    days = max(1, min(int(days), 365))
    end = datetime.utcnow()
    start = end - timedelta(days=days - 1)
    start = datetime(start.year, start.month, start.day)
    end = datetime(end.year, end.month, end.day, 23, 59, 59, 999999)
    return start, end


def _parse_range(days: Optional[int], start_date: Optional[str], end_date: Optional[str]) -> Tuple[datetime, datetime]:
    if not start_date and not end_date:
        return _parse_days(int(days or 14))

    now = datetime.utcnow()
    start_dt = pd.to_datetime(start_date, errors="coerce") if start_date else pd.NaT
    end_dt = pd.to_datetime(end_date, errors="coerce") if end_date else pd.NaT

    if pd.notna(end_dt):
        end = end_dt.to_pydatetime() if hasattr(end_dt, "to_pydatetime") else now
    else:
        end = now
    end = datetime(end.year, end.month, end.day, 23, 59, 59, 999999)

    if pd.notna(start_dt):
        start = start_dt.to_pydatetime() if hasattr(start_dt, "to_pydatetime") else (end - timedelta(days=int(days or 14) - 1))
    else:
        start = end - timedelta(days=max(1, min(int(days or 14), 365)) - 1)
    start = datetime(start.year, start.month, start.day)

    if start > end:
        start, end = end, start
    return start, end


def _ensure_report_sheets(repo) -> None:
    # report
    df = repo.query("report")
    if df is None or df.empty:
        repo.replace("report", [])
        df = repo.query("report")

    required_report = [
        "id",
        "project_id",
        "title",
        "report_type",
        "status",
        "template_code",
        "created_at",
        "time_start",
        "time_end",
        "range_from",
        "range_to",
        "summary",
        "content_json",
        "chart_json",
        "file_pdf_url",
        "file_docx_url",
        "file_pptx_url",
        "pipeline_run_id",
    ]
    if df is not None:
        df2 = df.copy()
        for c in required_report:
            if c not in df2.columns:
                df2[c] = None
        # backfill time<->range
        m = df2["time_start"].isna() & df2["range_from"].notna()
        if m.any():
            df2.loc[m, "time_start"] = df2.loc[m, "range_from"]
        m = df2["time_end"].isna() & df2["range_to"].notna()
        if m.any():
            df2.loc[m, "time_end"] = df2.loc[m, "range_to"]
        m = df2["range_from"].isna() & df2["time_start"].notna()
        if m.any():
            df2.loc[m, "range_from"] = df2.loc[m, "time_start"]
        m = df2["range_to"].isna() & df2["time_end"].notna()
        if m.any():
            df2.loc[m, "range_to"] = df2.loc[m, "time_end"]
        repo.replace("report", df2.to_dict(orient="records"))

    # report_config
    cfg = repo.query("report_config")
    if cfg is None or cfg.empty:
        repo.replace("report_config", [])
        cfg = repo.query("report_config")
    required_cfg = [
        "id",
        "report_id",
        "project_id",
        "time_start",
        "time_end",
        "platform_ids",
        "brand_ids",
        "keyword_ids",
        "include_sections",
        "config_json",
        "created_at",
    ]
    if cfg is not None:
        cfg2 = cfg.copy()
        for c in required_cfg:
            if c not in cfg2.columns:
                cfg2[c] = None
        repo.replace("report_config", cfg2.to_dict(orient="records"))

    # report_citation
    cit = repo.query("report_citation")
    if cit is None or cit.empty:
        repo.replace("report_citation", [])
        cit = repo.query("report_citation")
    required_cit = [
        "id",
        "report_id",
        "citation_type",
        "section_code",
        "quote_text",
        "sort_order",
        "post_raw_id",
        "reason",
        "section",
        "quote",
        "created_at",
    ]
    if cit is not None:
        cit2 = cit.copy()
        for c in required_cit:
            if c not in cit2.columns:
                cit2[c] = None
        m = cit2["quote_text"].isna() & cit2["quote"].notna()
        if m.any():
            cit2.loc[m, "quote_text"] = cit2.loc[m, "quote"]
        m = cit2["section_code"].isna() & cit2["section"].notna()
        if m.any():
            cit2.loc[m, "section_code"] = cit2.loc[m, "section"]
        repo.replace("report_citation", cit2.to_dict(orient="records"))


def _project_name_map(repo) -> Dict[int, str]:
    df = repo.query("monitor_project")
    out: Dict[int, str] = {}
    if df is None or df.empty or "id" not in df.columns:
        return out
    for _, r in df.iterrows():
        pid = _safe_int(r.get("id"))
        if pid is None:
            continue
        out[int(pid)] = str(r.get("name") or f"project_{pid}")
    return out


def _platform_name_map(repo) -> Dict[int, str]:
    df = repo.query("platform")
    out: Dict[int, str] = {}
    if df is None or df.empty or "id" not in df.columns:
        return out
    for _, r in df.iterrows():
        pid = _safe_int(r.get("id"))
        if pid is None:
            continue
        out[int(pid)] = str(r.get("name") or f"platform_{pid}")
    return out


def _brand_name_map(repo) -> Dict[int, str]:
    df = repo.query("brand")
    out: Dict[int, str] = {}
    if df is None or df.empty or "id" not in df.columns:
        return out
    for _, r in df.iterrows():
        bid = _safe_int(r.get("id"))
        if bid is None:
            continue
        out[int(bid)] = str(r.get("name") or f"brand_{bid}")
    return out


def _include_dict(include_sections: Any) -> Dict[str, bool]:
    defaults = {
        "sentiment_analysis": True,
        "sentiment_trends": True,
        "hot_topics": True,
        "entities": True,
        "spam": True,
        "competitor_compare": True,
        "suggestions": True,
    }
    if include_sections is None:
        return defaults
    if isinstance(include_sections, dict):
        d = {k: bool(v) for k, v in include_sections.items()}
        for k, v in defaults.items():
            d.setdefault(k, v)
        return d
    if isinstance(include_sections, list):
        s = {str(x) for x in include_sections}
        return {k: (k in s) for k in defaults.keys()}
    return defaults


def _int_list(v: Any) -> List[int]:
    if v is None or not isinstance(v, list):
        return []
    out: List[int] = []
    for x in v:
        n = _safe_int(x)
        if n is not None:
            out.append(int(n))
    return sorted({int(x) for x in out})


@router.get("")
def _placeholder(user=Depends(get_current_user)):
    # Will be replaced later in this file by the actual endpoints.
    return []

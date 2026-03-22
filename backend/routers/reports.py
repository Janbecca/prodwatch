from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from fastapi import APIRouter, Body, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

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
        "project_platform_id",
        "platform_id",
        "brand_id",
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
        "trigger_type",
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


class ReportCreateIn(BaseModel):
    projectId: int = Field(..., ge=1)
    title: str = Field(..., min_length=1, max_length=120)
    reportType: str = Field("daily")
    platformIds: List[int] = Field(default_factory=list)
    brandIds: List[int] = Field(default_factory=list)
    keywordIds: List[int] = Field(default_factory=list)
    includeSections: List[str] = Field(default_factory=list)
    exportFormats: List[str] = Field(default_factory=list)
    days: Optional[int] = Field(default=14, ge=1, le=365)
    startDate: Optional[str] = None
    endDate: Optional[str] = None


def _parse_create_range(payload: ReportCreateIn) -> Tuple[datetime, datetime]:
    if payload.startDate and payload.endDate:
        start_dt = pd.to_datetime(payload.startDate, errors="coerce")
        end_dt = pd.to_datetime(payload.endDate, errors="coerce")
        if pd.notna(start_dt) and pd.notna(end_dt):
            start = start_dt.to_pydatetime() if hasattr(start_dt, "to_pydatetime") else datetime.utcnow()
            end = end_dt.to_pydatetime() if hasattr(end_dt, "to_pydatetime") else datetime.utcnow()
            start = datetime(start.year, start.month, start.day)
            end = datetime(end.year, end.month, end.day, 23, 59, 59, 999999)
            if start > end:
                start, end = end, start
            return start, end
    return _parse_days(int(payload.days or 14))


def _json_load(v: Any) -> Any:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, (dict, list)):
        return v
    s = str(v).strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return None


@router.get("")
def list_reports(
    project_id: Optional[int] = Query(None),
    brand_id: Optional[int] = Query(None),
    platform_id: Optional[int] = Query(None),
    report_type: Optional[str] = Query(None),
    time_from: Optional[str] = Query(None),
    time_to: Optional[str] = Query(None),
    created_from: Optional[str] = Query(None),
    created_to: Optional[str] = Query(None),
    title_keyword: Optional[str] = Query(None),
    user=Depends(get_current_user),
):
    repo = get_repo()
    _ensure_report_sheets(repo)

    df = repo.query("report")
    if df is None or df.empty:
        return []

    df = df.copy()
    if "project_id" in df.columns:
        df["project_id"] = pd.to_numeric(df["project_id"], errors="coerce")
    if "platform_id" in df.columns:
        df["platform_id"] = pd.to_numeric(df["platform_id"], errors="coerce")
    if "brand_id" in df.columns:
        df["brand_id"] = pd.to_numeric(df["brand_id"], errors="coerce")
    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    if "time_start" in df.columns:
        df["time_start"] = pd.to_datetime(df["time_start"], errors="coerce")
    if "time_end" in df.columns:
        df["time_end"] = pd.to_datetime(df["time_end"], errors="coerce")
    if "range_from" in df.columns:
        df["range_from"] = pd.to_datetime(df["range_from"], errors="coerce")
    if "range_to" in df.columns:
        df["range_to"] = pd.to_datetime(df["range_to"], errors="coerce")

    if project_id is not None and "project_id" in df.columns:
        df = df[df["project_id"].notna() & (df["project_id"].astype(int) == int(project_id))]
    if brand_id is not None and "brand_id" in df.columns:
        df = df[df["brand_id"].notna() & (df["brand_id"].astype(int) == int(brand_id))]
    if platform_id is not None and "platform_id" in df.columns:
        df = df[df["platform_id"].notna() & (df["platform_id"].astype(int) == int(platform_id))]
    if report_type:
        df = df[df.get("report_type").astype(str).str.lower() == str(report_type).lower()]
    if title_keyword:
        kw = str(title_keyword).strip().lower()
        if kw:
            df = df[df.get("title").astype(str).str.lower().str.contains(kw, na=False)]

    # time range filter prefers time_start/time_end, falls back to range_from/range_to
    tf = pd.to_datetime(time_from, errors="coerce") if time_from else pd.NaT
    tt = pd.to_datetime(time_to, errors="coerce") if time_to else pd.NaT
    if pd.notna(tf) or pd.notna(tt):
        start = (tf.to_pydatetime() if hasattr(tf, "to_pydatetime") else datetime.utcnow()) if pd.notna(tf) else None
        end = (tt.to_pydatetime() if hasattr(tt, "to_pydatetime") else datetime.utcnow()) if pd.notna(tt) else None
        if start is not None:
            df = df[(df["time_start"].notna() & (df["time_start"] >= start)) | (df["range_from"].notna() & (df["range_from"] >= start))]
        if end is not None:
            df = df[(df["time_end"].notna() & (df["time_end"] <= end)) | (df["range_to"].notna() & (df["range_to"] <= end))]

    cf = pd.to_datetime(created_from, errors="coerce") if created_from else pd.NaT
    ct = pd.to_datetime(created_to, errors="coerce") if created_to else pd.NaT
    if pd.notna(cf):
        start = cf.to_pydatetime() if hasattr(cf, "to_pydatetime") else datetime.utcnow()
        df = df[df["created_at"].notna() & (df["created_at"] >= start)]
    if pd.notna(ct):
        end = ct.to_pydatetime() if hasattr(ct, "to_pydatetime") else datetime.utcnow()
        end = datetime(end.year, end.month, end.day, 23, 59, 59, 999999)
        df = df[df["created_at"].notna() & (df["created_at"] <= end)]

    proj_name = _project_name_map(repo)
    out: List[Dict[str, Any]] = []
    if "created_at" in df.columns:
        df = df.sort_values(by="created_at", ascending=False)
    for _, r in df.iterrows():
        rid = _safe_int(r.get("id"))
        if rid is None:
            continue
        pid = _safe_int(r.get("project_id"))
        out.append(
            {
                "id": int(rid),
                "title": str(_none_if_nan(r.get("title")) or ""),
                "reportType": str(_none_if_nan(r.get("report_type")) or ""),
                "projectId": pid,
                "projectName": proj_name.get(int(pid), f"project_{pid}") if pid is not None else None,
                "status": str(_none_if_nan(r.get("status")) or ""),
                "summary": str(_none_if_nan(r.get("summary")) or ""),
                "createdAt": _dt_iso(r.get("created_at")),
                "timeStart": _dt_iso(r.get("time_start")),
                "timeEnd": _dt_iso(r.get("time_end")),
                "rangeFrom": _dt_iso(r.get("range_from")),
                "rangeTo": _dt_iso(r.get("range_to")),
                "pipelineRunId": _safe_int(r.get("pipeline_run_id")),
                "platformId": _safe_int(r.get("platform_id")),
                "brandId": _safe_int(r.get("brand_id")),
                "triggerType": str(_none_if_nan(r.get("trigger_type")) or ""),
            }
        )
    return out


@router.get("/{report_id}")
def get_report(report_id: int, user=Depends(get_current_user)):
    repo = get_repo()
    _ensure_report_sheets(repo)
    df = repo.query("report")
    if df is None or df.empty:
        raise HTTPException(status_code=404, detail="report not found")

    df = df.copy()
    df["id"] = pd.to_numeric(df.get("id"), errors="coerce")
    df = df.dropna(subset=["id"])
    df["id"] = df["id"].astype(int)
    hit = df[df["id"] == int(report_id)]
    if hit.empty:
        raise HTTPException(status_code=404, detail="report not found")
    r = hit.iloc[0].to_dict()

    cfg_df = repo.query("report_config")
    cfg = None
    if cfg_df is not None and not cfg_df.empty and {"report_id"}.issubset(cfg_df.columns):
        tmp = cfg_df.copy()
        tmp["report_id"] = pd.to_numeric(tmp["report_id"], errors="coerce")
        tmp = tmp.dropna(subset=["report_id"])
        tmp["report_id"] = tmp["report_id"].astype(int)
        c = tmp[tmp["report_id"] == int(report_id)]
        if not c.empty:
            cfg = c.sort_values(by=["id"] if "id" in c.columns else None).iloc[-1].to_dict()

    return {
        "id": int(report_id),
        "projectId": _safe_int(r.get("project_id")),
        "platformId": _safe_int(r.get("platform_id")),
        "brandId": _safe_int(r.get("brand_id")),
        "pipelineRunId": _safe_int(r.get("pipeline_run_id")),
        "title": _none_if_nan(r.get("title")),
        "reportType": _none_if_nan(r.get("report_type")),
        "status": _none_if_nan(r.get("status")),
        "summary": _none_if_nan(r.get("summary")),
        "createdAt": _dt_iso(r.get("created_at")),
        "timeStart": _dt_iso(r.get("time_start")),
        "timeEnd": _dt_iso(r.get("time_end")),
        "rangeFrom": _dt_iso(r.get("range_from")),
        "rangeTo": _dt_iso(r.get("range_to")),
        "triggerType": _none_if_nan(r.get("trigger_type")),
        "content": _json_load(r.get("content_json")) or {},
        "config": {
            "projectId": _safe_int((cfg or {}).get("project_id")),
            "title": _none_if_nan((cfg or {}).get("title")),
            "reportType": _none_if_nan((cfg or {}).get("report_type")),
            "startDate": _dt_iso((cfg or {}).get("time_start") or (cfg or {}).get("range_from")),
            "endDate": _dt_iso((cfg or {}).get("time_end") or (cfg or {}).get("range_to")),
            "platformIds": _json_load((cfg or {}).get("platform_ids")) or _json_load((cfg or {}).get("platformIds")) or [],
            "brandIds": _json_load((cfg or {}).get("brand_ids")) or _json_load((cfg or {}).get("brandIds")) or [],
            "keywordIds": _json_load((cfg or {}).get("keyword_ids")) or _json_load((cfg or {}).get("keywordIds")) or [],
            "includeSections": _json_load((cfg or {}).get("include_sections")) or _json_load((cfg or {}).get("includeSections")) or [],
            "exportFormats": _json_load((cfg or {}).get("export_formats")) or _json_load((cfg or {}).get("exportFormats")) or [],
        },
    }


@router.get("/{report_id}/citations")
def list_report_citations(report_id: int, user=Depends(get_current_user)):
    repo = get_repo()
    _ensure_report_sheets(repo)

    cit_df = repo.query("report_citation")
    if cit_df is None or cit_df.empty:
        return []
    tmp = cit_df.copy()
    tmp["report_id"] = pd.to_numeric(tmp.get("report_id"), errors="coerce")
    tmp = tmp.dropna(subset=["report_id"])
    tmp["report_id"] = tmp["report_id"].astype(int)
    tmp = tmp[tmp["report_id"] == int(report_id)]
    if tmp.empty:
        return []

    raw_df = repo.query("post_raw")
    plat_df = repo.query("platform")
    plat_name = _platform_name_map(repo)

    clean_df = repo.query("post_clean")
    sent_df = repo.query("sentiment_result")
    spam_df = repo.query("spam_score")

    raw_map: Dict[int, Dict[str, Any]] = {}
    if raw_df is not None and not raw_df.empty and "id" in raw_df.columns:
        r = raw_df.copy()
        r["id"] = pd.to_numeric(r.get("id"), errors="coerce")
        r = r.dropna(subset=["id"])
        r["id"] = r["id"].astype(int)
        for _, rr in r.iterrows():
            rid = int(rr["id"])
            raw_map[rid] = {
                "platform_id": _safe_int(rr.get("platform_id")),
                "publish_time": _dt_iso(rr.get("publish_time")),
                "raw_text": _none_if_nan(rr.get("raw_text")),
            }

    raw_to_clean: Dict[int, int] = {}
    if clean_df is not None and not clean_df.empty and {"post_raw_id", "id"}.issubset(clean_df.columns):
        c = clean_df.copy()
        c["post_raw_id"] = pd.to_numeric(c.get("post_raw_id"), errors="coerce")
        c["id"] = pd.to_numeric(c.get("id"), errors="coerce")
        c = c.dropna(subset=["post_raw_id", "id"])
        c["post_raw_id"] = c["post_raw_id"].astype(int)
        c["id"] = c["id"].astype(int)
        if "id" in c.columns:
            c = c.sort_values(by="id").drop_duplicates(subset=["post_raw_id"], keep="last")
        raw_to_clean = dict(zip(c["post_raw_id"].tolist(), c["id"].tolist()))

    polarity_by_clean: Dict[int, str] = {}
    if sent_df is not None and not sent_df.empty and {"post_clean_id", "polarity"}.issubset(sent_df.columns):
        s = sent_df.copy()
        s["post_clean_id"] = pd.to_numeric(s.get("post_clean_id"), errors="coerce")
        s = s.dropna(subset=["post_clean_id"])
        s["post_clean_id"] = s["post_clean_id"].astype(int)
        if "id" in s.columns:
            s["id"] = pd.to_numeric(s.get("id"), errors="coerce")
            s = s.sort_values(by="id").drop_duplicates(subset=["post_clean_id"], keep="last")
        polarity_by_clean = {int(rr["post_clean_id"]): str(rr.get("polarity") or "") for _, rr in s.iterrows()}

    spam_by_clean: Dict[int, str] = {}
    if spam_df is not None and not spam_df.empty and {"post_clean_id", "label"}.issubset(spam_df.columns):
        s = spam_df.copy()
        s["post_clean_id"] = pd.to_numeric(s.get("post_clean_id"), errors="coerce")
        s = s.dropna(subset=["post_clean_id"])
        s["post_clean_id"] = s["post_clean_id"].astype(int)
        if "id" in s.columns:
            s["id"] = pd.to_numeric(s.get("id"), errors="coerce")
            s = s.sort_values(by="id").drop_duplicates(subset=["post_clean_id"], keep="last")
        spam_by_clean = {int(rr["post_clean_id"]): str(rr.get("label") or "") for _, rr in s.iterrows()}

    out: List[Dict[str, Any]] = []
    if "sort_order" in tmp.columns:
        tmp["sort_order"] = pd.to_numeric(tmp.get("sort_order"), errors="coerce")
        tmp = tmp.sort_values(by=["sort_order"], ascending=True)

    for _, r in tmp.iterrows():
        cid = _safe_int(r.get("id"))
        post_raw_id = _safe_int(r.get("post_raw_id"))
        raw = raw_map.get(int(post_raw_id), {}) if post_raw_id is not None else {}
        clean_id = raw_to_clean.get(int(post_raw_id)) if post_raw_id is not None else None
        out.append(
            {
                "id": int(cid) if cid is not None else None,
                "quoteText": str(_none_if_nan(r.get("quote_text")) or _none_if_nan(r.get("quote")) or ""),
                "platformName": plat_name.get(int(raw.get("platform_id") or 0), "-") if raw.get("platform_id") else "-",
                "publishTime": raw.get("publish_time"),
                "sentimentLabel": (polarity_by_clean.get(int(clean_id)) if clean_id is not None else None) or "-",
                "spamLabel": (spam_by_clean.get(int(clean_id)) if clean_id is not None else None) or "-",
                "reason": str(_none_if_nan(r.get("reason")) or ""),
            }
        )
    return out


@router.delete("/{report_id}")
def delete_report(report_id: int, user=Depends(get_current_user)):
    repo = get_repo()
    _ensure_report_sheets(repo)

    df = repo.query("report")
    if df is None or df.empty or "id" not in df.columns:
        raise HTTPException(status_code=404, detail="report not found")
    tmp = df.copy()
    tmp["id"] = pd.to_numeric(tmp.get("id"), errors="coerce")
    tmp = tmp.dropna(subset=["id"])
    tmp["id"] = tmp["id"].astype(int)
    if int(report_id) not in set(tmp["id"].tolist()):
        raise HTTPException(status_code=404, detail="report not found")
    tmp = tmp[tmp["id"] != int(report_id)]
    repo.replace("report", tmp.to_dict(orient="records"))

    # cascade: report_config + citations
    cfg = repo.query("report_config")
    if cfg is not None and not cfg.empty and "report_id" in cfg.columns:
        c = cfg.copy()
        c["report_id"] = pd.to_numeric(c.get("report_id"), errors="coerce")
        c = c.dropna(subset=["report_id"])
        c["report_id"] = c["report_id"].astype(int)
        c = c[c["report_id"] != int(report_id)]
        repo.replace("report_config", c.to_dict(orient="records"))

    cit = repo.query("report_citation")
    if cit is not None and not cit.empty and "report_id" in cit.columns:
        c = cit.copy()
        c["report_id"] = pd.to_numeric(c.get("report_id"), errors="coerce")
        c = c.dropna(subset=["report_id"])
        c["report_id"] = c["report_id"].astype(int)
        c = c[c["report_id"] != int(report_id)]
        repo.replace("report_citation", c.to_dict(orient="records"))

    return {"deleted": int(report_id)}


@router.get("/{report_id}/export")
def export_report(report_id: int, format: str = Query("word"), user=Depends(get_current_user)):
    repo = get_repo()
    _ensure_report_sheets(repo)

    df = repo.query("report")
    if df is None or df.empty or "id" not in df.columns:
        raise HTTPException(status_code=404, detail="report not found")
    tmp = df.copy()
    tmp["id"] = pd.to_numeric(tmp.get("id"), errors="coerce")
    tmp = tmp.dropna(subset=["id"])
    tmp["id"] = tmp["id"].astype(int)
    hit = tmp[tmp["id"] == int(report_id)]
    if hit.empty:
        raise HTTPException(status_code=404, detail="report not found")
    r = hit.iloc[0].to_dict()

    fmt = str(format or "word").lower()
    ext = "doc" if fmt == "word" else "pdf" if fmt == "pdf" else "ppt" if fmt == "ppt" else "txt"

    os.makedirs("reports", exist_ok=True)
    fname = f"report_{report_id}.{ext}"
    path = os.path.join("reports", fname)

    content = _json_load(r.get("content_json")) or {}
    summary = str(_none_if_nan(r.get("summary")) or "")
    title = str(_none_if_nan(r.get("title")) or f"report_{report_id}")
    with open(path, "w", encoding="utf-8") as f:
        f.write(title + "\n\n")
        f.write(summary + "\n\n")
        f.write(json.dumps(content, ensure_ascii=False, indent=2))

    return FileResponse(path, filename=fname)


@router.post("")
def create_report(payload: ReportCreateIn = Body(...), user=Depends(get_current_user)):
    repo = get_repo()
    _ensure_report_sheets(repo)

    pid = int(payload.projectId)
    time_start, time_end = _parse_create_range(payload)
    include = _include_dict(payload.includeSections)

    # Filter base posts for citations and detail
    raw_df = repo.query("post_raw")
    if raw_df is None or raw_df.empty:
        raw_df = pd.DataFrame()
    raw = raw_df.copy()
    if not raw.empty and "publish_time" in raw.columns:
        raw["publish_time"] = pd.to_datetime(raw["publish_time"], errors="coerce")
        raw = raw.dropna(subset=["publish_time"])
        raw = raw[(raw["publish_time"] >= time_start) & (raw["publish_time"] <= time_end)]
    if not raw.empty and "project_id" in raw.columns:
        raw["project_id"] = pd.to_numeric(raw["project_id"], errors="coerce")
        raw = raw.dropna(subset=["project_id"])
        raw = raw[raw["project_id"].astype(int) == pid]
    if payload.platformIds and not raw.empty and "platform_id" in raw.columns:
        raw["platform_id"] = pd.to_numeric(raw["platform_id"], errors="coerce")
        raw = raw.dropna(subset=["platform_id"])
        raw = raw[raw["platform_id"].astype(int).isin([int(x) for x in payload.platformIds])]
    if payload.brandIds and not raw.empty and "brand_id" in raw.columns:
        raw["brand_id"] = pd.to_numeric(raw["brand_id"], errors="coerce")
        raw = raw.dropna(subset=["brand_id"])
        raw = raw[raw["brand_id"].astype(int).isin([int(x) for x in payload.brandIds])]
    if payload.keywordIds and not raw.empty and "keyword_id" in raw.columns:
        raw["keyword_id"] = pd.to_numeric(raw["keyword_id"], errors="coerce")
        raw = raw.dropna(subset=["keyword_id"])
        raw = raw[raw["keyword_id"].astype(int).isin([int(x) for x in payload.keywordIds])]

    # Overview + trends from daily_metric (project_id + optional brand/platform filters)
    daily_df = repo.query("daily_metric")
    daily = daily_df.copy() if daily_df is not None else pd.DataFrame()
    if not daily.empty and "metric_date" in daily.columns:
        daily["metric_date"] = pd.to_datetime(daily["metric_date"], errors="coerce")
        daily = daily.dropna(subset=["metric_date"])
        daily = daily[(daily["metric_date"] >= time_start) & (daily["metric_date"] <= time_end)]
    if not daily.empty and "project_id" in daily.columns:
        daily["project_id"] = pd.to_numeric(daily["project_id"], errors="coerce")
        daily = daily.dropna(subset=["project_id"])
        daily = daily[daily["project_id"].astype(int) == pid]
    if payload.platformIds and not daily.empty and "platform_id" in daily.columns:
        daily["platform_id"] = pd.to_numeric(daily["platform_id"], errors="coerce")
        daily = daily.dropna(subset=["platform_id"])
        daily = daily[daily["platform_id"].astype(int).isin([int(x) for x in payload.platformIds])]
    if payload.brandIds and not daily.empty and "brand_id" in daily.columns:
        daily["brand_id"] = pd.to_numeric(daily["brand_id"], errors="coerce")
        daily = daily.dropna(subset=["brand_id"])
        daily = daily[daily["brand_id"].astype(int).isin([int(x) for x in payload.brandIds])]

    def _sum(col: str) -> int:
        if daily.empty or col not in daily.columns:
            return 0
        return int(pd.to_numeric(daily[col], errors="coerce").fillna(0).sum())

    total_posts = int(len(raw)) if not raw.empty else _sum("total_posts")
    valid_posts = _sum("valid_posts")
    spam_posts = _sum("spam_posts")
    pos_posts = _sum("pos_posts")
    neu_posts = _sum("neu_posts")
    neg_posts = _sum("neg_posts")

    trends = {"dates": [], "positive": [], "neutral": [], "negative": []}
    if not daily.empty and "metric_date" in daily.columns:
        tmp = daily.copy()
        tmp["date_str"] = tmp["metric_date"].dt.strftime("%Y-%m-%d")
        agg = tmp.groupby("date_str")[["pos_posts", "neu_posts", "neg_posts"]].sum().reset_index()
        trends["dates"] = agg["date_str"].tolist()
        trends["positive"] = [int(x) for x in agg["pos_posts"].tolist()]
        trends["neutral"] = [int(x) for x in agg["neu_posts"].tolist()]
        trends["negative"] = [int(x) for x in agg["neg_posts"].tolist()]

    # topics/entities (run-scoped not available here; aggregate by filtered raw->clean ids)
    topics: List[Dict[str, Any]] = []
    entities: List[Dict[str, Any]] = []
    if not raw.empty and "id" in raw.columns:
        raw_ids = pd.to_numeric(raw["id"], errors="coerce").dropna().astype(int).tolist()
        clean_df = repo.query("post_clean")
        if clean_df is not None and not clean_df.empty and {"post_raw_id", "id"}.issubset(clean_df.columns):
            c = clean_df.copy()
            c["post_raw_id"] = pd.to_numeric(c.get("post_raw_id"), errors="coerce")
            c["id"] = pd.to_numeric(c.get("id"), errors="coerce")
            c = c.dropna(subset=["post_raw_id", "id"])
            c["post_raw_id"] = c["post_raw_id"].astype(int)
            c["id"] = c["id"].astype(int)
            c = c[c["post_raw_id"].isin(raw_ids)]
            clean_ids = c["id"].tolist()

            if include.get("hot_topics"):
                tdf = repo.query("topic_result")
                if tdf is not None and not tdf.empty and {"post_clean_id", "topic_name"}.issubset(tdf.columns):
                    t = tdf.copy()
                    t["post_clean_id"] = pd.to_numeric(t.get("post_clean_id"), errors="coerce")
                    t = t.dropna(subset=["post_clean_id"])
                    t["post_clean_id"] = t["post_clean_id"].astype(int)
                    t = t[t["post_clean_id"].isin(clean_ids)]
                    if not t.empty:
                        vc = t["topic_name"].astype(str).value_counts().head(10)
                        topics = [{"topic": str(k), "count": int(v)} for k, v in vc.items()]

            if include.get("entities"):
                edf = repo.query("entity_result")
                if edf is not None and not edf.empty and {"post_clean_id", "entity_text"}.issubset(edf.columns):
                    e = edf.copy()
                    e["post_clean_id"] = pd.to_numeric(e.get("post_clean_id"), errors="coerce")
                    e = e.dropna(subset=["post_clean_id"])
                    e["post_clean_id"] = e["post_clean_id"].astype(int)
                    e = e[e["post_clean_id"].isin(clean_ids)]
                    if not e.empty:
                        vc = e["entity_text"].astype(str).value_counts().head(15)
                        entities = [{"entity_text": str(k), "count": int(v)} for k, v in vc.items()]

    content = {
        "range": {"from": time_start.date().isoformat(), "to": time_end.date().isoformat()},
        "executive_summary": {
            "overall_trend": "系统自动聚合生成（报告中心）。",
            "main_risks": "关注负面占比上升与水军比例异常。",
            "key_feedback": "结合热点话题与实体/功能点可提炼更具体建议。",
            "strategic_suggestions": ["持续监控负面集中点，快速闭环产品/售后问题。"],
        },
        "overview": {
            "total_posts": int(total_posts),
            "valid_posts": int(valid_posts),
            "spam_posts": int(spam_posts),
            "positive_posts": int(pos_posts),
            "neutral_posts": int(neu_posts),
            "negative_posts": int(neg_posts),
        },
        "sentiment_trends": trends,
        "hot_topics": topics,
        "entities": entities,
        "spam": {"spam_posts": int(spam_posts)},
        "competitor_compare": [],
        "strategic_suggestions": ["持续监控负面集中点，快速闭环产品/售后问题。"],
    }

    rid = int(datetime.utcnow().timestamp() * 1000)
    now = datetime.utcnow()

    repo.insert(
        "report",
        {
            "id": rid,
            "project_id": pid,
            "project_platform_id": None,
            "platform_id": (int(payload.platformIds[0]) if payload.platformIds else None),
            "brand_id": (int(payload.brandIds[0]) if payload.brandIds else None),
            "pipeline_run_id": None,
            "trigger_type": "custom",
            "title": payload.title,
            "report_type": payload.reportType,
            "status": "generated",
            "created_at": now,
            "time_start": time_start,
            "time_end": time_end,
            "range_from": time_start,
            "range_to": time_end,
            "summary": f"报告范围 {time_start.date().isoformat()} ~ {time_end.date().isoformat()}；总帖子 {int(total_posts)}。",
            "content_json": json.dumps(content, ensure_ascii=False),
        },
    )

    repo.insert(
        "report_config",
        {
            "id": int(datetime.utcnow().timestamp() * 1000) + 1,
            "report_id": rid,
            "project_id": pid,
            "time_start": time_start,
            "time_end": time_end,
            "platform_ids": json.dumps([int(x) for x in payload.platformIds], ensure_ascii=False),
            "brand_ids": json.dumps([int(x) for x in payload.brandIds], ensure_ascii=False),
            "keyword_ids": json.dumps([int(x) for x in payload.keywordIds], ensure_ascii=False),
            "include_sections": json.dumps(payload.includeSections or [], ensure_ascii=False),
            "config_json": json.dumps(payload.model_dump(), ensure_ascii=False),
            "created_at": now,
        },
    )

    # citations (top 10 negative posts)
    if not raw.empty and "id" in raw.columns:
        clean_df = repo.query("post_clean")
        sent_df = repo.query("sentiment_result")
        if clean_df is not None and not clean_df.empty and sent_df is not None and not sent_df.empty:
            c = clean_df.copy()
            c["post_raw_id"] = pd.to_numeric(c.get("post_raw_id"), errors="coerce")
            c["id"] = pd.to_numeric(c.get("id"), errors="coerce")
            c = c.dropna(subset=["post_raw_id", "id"])
            c["post_raw_id"] = c["post_raw_id"].astype(int)
            c["id"] = c["id"].astype(int)
            raw_ids = pd.to_numeric(raw["id"], errors="coerce").dropna().astype(int).tolist()
            c = c[c["post_raw_id"].isin(raw_ids)]

            s = sent_df.copy()
            s["post_clean_id"] = pd.to_numeric(s.get("post_clean_id"), errors="coerce")
            s = s.dropna(subset=["post_clean_id"])
            s["post_clean_id"] = s["post_clean_id"].astype(int)
            if "id" in s.columns:
                s["id"] = pd.to_numeric(s.get("id"), errors="coerce")
                s = s.sort_values(by="id").drop_duplicates(subset=["post_clean_id"], keep="last")
            s["polarity"] = s.get("polarity").astype(str).str.lower()

            m = c.merge(s[["post_clean_id", "polarity"]], left_on="id", right_on="post_clean_id", how="left")
            m = m.merge(raw[["id", "raw_text"]], left_on="post_raw_id", right_on="id", how="left", suffixes=("_c", "_r"))
            m = m[m["polarity"] == "negative"].head(10)
            if not m.empty:
                rows = []
                base = int(datetime.utcnow().timestamp() * 1000) * 1000
                for i, rr in enumerate(m.itertuples(index=False)):
                    rows.append(
                        {
                            "id": base + i,
                            "report_id": rid,
                            "citation_type": "post",
                            "section_code": "sentiment_analysis",
                            "quote_text": str(getattr(rr, "raw_text", "") or "")[:600],
                            "sort_order": i + 1,
                            "post_raw_id": int(getattr(rr, "post_raw_id", 0) or 0),
                            "reason": "负面样本（报告中心）",
                            "created_at": now,
                        }
                    )
                if rows:
                    if hasattr(repo, "insert_many"):
                        repo.insert_many("report_citation", rows)
                    else:
                        for row in rows:
                            repo.insert("report_citation", row)

    return {"id": rid}

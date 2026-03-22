from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.storage.db import get_repo
from .auth import get_current_user

router = APIRouter(prefix="/projects", tags=["projects"])


class ProjectIn(BaseModel):
    id: Optional[int] = None
    name: str = Field(..., min_length=1, max_length=80)
    product_category: Optional[str] = Field(None, max_length=80)
    brand_ids: List[int] = Field(default_factory=list)
    is_active: bool = True


class ProjectActivateIn(BaseModel):
    active: bool


class ProjectPlatformIn(BaseModel):
    platform_id: int
    is_enabled: bool = True
    crawl_mode: str = "schedule"  # schedule | manual
    cron_expr: str = "0 5 * * *"
    timezone: str = "Asia/Shanghai"
    max_posts_per_run: int = 20
    sentiment_model: str = "rule-based"
    query_strategy: Optional[str] = None


class ProjectPlatformsIn(BaseModel):
    platforms: List[ProjectPlatformIn] = Field(default_factory=list)


def _now_ts_ms() -> int:
    return int(datetime.utcnow().timestamp() * 1000)


def _norm_int(x: Any) -> Optional[int]:
    v = pd.to_numeric(x, errors="coerce")
    if pd.isna(v):
        return None
    return int(v)


def _load_projects(repo) -> pd.DataFrame:
    df = repo.query("monitor_project")
    if df is None or df.empty:
        return pd.DataFrame(columns=["id", "name", "product_category", "is_active", "created_at", "updated_at"])
    if "id" in df.columns:
        ids = pd.to_numeric(df["id"], errors="coerce")
        if ids.notna().any():
            df = df[ids.notna()].copy()
            df["id"] = pd.to_numeric(df["id"], errors="coerce").astype(int)
    for col in ["name", "product_category", "is_active", "created_at", "updated_at"]:
        if col not in df.columns:
            df[col] = None
    if "is_active" in df.columns:
        df["is_active"] = pd.to_numeric(df["is_active"], errors="coerce").fillna(0).astype(int)
    return df


def _load_project_brand(repo) -> pd.DataFrame:
    df = repo.query("monitor_project_brand")
    if df is None or df.empty:
        return pd.DataFrame(columns=["id", "project_id", "brand_id", "created_at"])
    for col in ["id", "project_id", "brand_id", "created_at"]:
        if col not in df.columns:
            df[col] = None
    for c in ["id", "project_id", "brand_id"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["project_id", "brand_id"])
    df["project_id"] = df["project_id"].astype(int)
    df["brand_id"] = df["brand_id"].astype(int)
    if df["id"].notna().any():
        df["id"] = df["id"].fillna(0).astype(int)
    else:
        df["id"] = 0
    return df


def _project_brand_ids(repo, project_id: int) -> List[int]:
    join_df = _load_project_brand(repo)
    if join_df is None or join_df.empty:
        return []
    g = join_df[join_df["project_id"] == int(project_id)]
    if g.empty:
        return []
    return sorted({int(x) for x in g["brand_id"].tolist() if x is not None})


def _load_project_platform(repo) -> pd.DataFrame:
    df = repo.query("monitor_project_platform")
    if df is None or df.empty:
        return pd.DataFrame(
            columns=[
                "id",
                "project_id",
                "platform_id",
                "is_enabled",
                "crawl_mode",
                "cron_expr",
                "timezone",
                "sentiment_model",
                "max_posts_per_run",
                "query_strategy",
                "last_run_at",
                "last_success_at",
                "next_run_at",
                "created_at",
                "updated_at",
            ]
        )

    for col in [
        "id",
        "project_id",
        "platform_id",
        "is_enabled",
        "crawl_mode",
        "cron_expr",
        "timezone",
        "sentiment_model",
        "max_posts_per_run",
        "query_strategy",
        "last_run_at",
        "last_success_at",
        "next_run_at",
        "created_at",
        "updated_at",
    ]:
        if col not in df.columns:
            df[col] = None

    for c in ["id", "project_id", "platform_id", "is_enabled", "max_posts_per_run"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["project_id", "platform_id"])
    df["project_id"] = df["project_id"].astype(int)
    df["platform_id"] = df["platform_id"].astype(int)
    df["is_enabled"] = df["is_enabled"].fillna(0).astype(int)
    if df["id"].notna().any():
        df["id"] = df["id"].fillna(0).astype(int)
    else:
        df["id"] = 0
    if df["max_posts_per_run"].notna().any():
        df["max_posts_per_run"] = df["max_posts_per_run"].fillna(0).astype(int)
    else:
        df["max_posts_per_run"] = 0
    return df


def _enabled_platform_ids(repo, project_id: int) -> List[int]:
    df = _load_project_platform(repo)
    if df.empty:
        return []
    tmp = df[(df["project_id"] == int(project_id)) & (df["is_enabled"] == 1)]
    if tmp.empty:
        return []
    return sorted({int(x) for x in tmp["platform_id"].tolist() if x is not None})


def _seed_default_project_platforms(repo, project_id: int) -> int:
    """
    If a project has no platform configs, seed defaults for all platforms.
    Returns number of inserted rows.
    """
    plat_df = repo.query("platform")
    if plat_df is None or plat_df.empty or "id" not in plat_df.columns:
        return 0

    cfg_df = repo.query("datasource_config")
    defaults_by_code: Dict[str, Dict[str, Any]] = {}
    if cfg_df is not None and not cfg_df.empty:
        for _, r in cfg_df.iterrows():
            code = str(r.get("id") or "").strip()
            if not code:
                continue
            defaults_by_code[code] = {
                "default_cron_expr": r.get("default_cron_exper") or r.get("default_cron_expr"),
                "default_max_posts_per_run": r.get("default_max_posts_per_run"),
                "is_global_enabled": r.get("is_global_enabled"),
            }

    now = datetime.utcnow()
    existing = _load_project_platform(repo)
    existing_plats = set(existing[existing["project_id"] == int(project_id)]["platform_id"].tolist()) if not existing.empty else set()

    rows: List[Dict[str, Any]] = []
    for _, r in plat_df.iterrows():
        pid = pd.to_numeric(r.get("id"), errors="coerce")
        if pd.isna(pid):
            continue
        plat_id = int(pid)
        if plat_id in existing_plats:
            continue
        code = str(r.get("code") or "").strip()
        defaults = defaults_by_code.get(code, {})
        cron_expr = str(defaults.get("default_cron_expr") or "0 5 * * *")
        max_posts = pd.to_numeric(defaults.get("default_max_posts_per_run"), errors="coerce")
        max_posts = int(max_posts) if pd.notna(max_posts) and int(max_posts) > 0 else 20

        rows.append(
            {
                "id": _now_ts_ms() + len(rows) + 1,
                "project_id": int(project_id),
                "platform_id": plat_id,
                "is_enabled": 1,
                "crawl_mode": "schedule",
                "cron_expr": cron_expr,
                "timezone": "Asia/Shanghai",
                "sentiment_model": "rule-based",
                "max_posts_per_run": max_posts,
                "query_strategy": None,
                "last_run_at": None,
                "last_success_at": None,
                "next_run_at": None,
                "created_at": now,
                "updated_at": now,
            }
        )

    if not rows:
        return 0
    if hasattr(repo, "insert_many"):
        return int(repo.insert_many("monitor_project_platform", rows))
    for row in rows:
        repo.insert("monitor_project_platform", row)
    return len(rows)


def _next_project_id(repo) -> int:
    """
    Avoid re-using ids after deletion.
    Excel sheets can keep historical rows that still reference old project ids.
    """
    candidate_max = 0
    for sheet in ["monitor_project", "post_raw", "post_clean", "pipeline_run", "daily_metric", "report"]:
        df = repo.query(sheet)
        if df is None or df.empty:
            continue
        if "project_id" in df.columns:
            vals = pd.to_numeric(df["project_id"], errors="coerce").dropna()
            if not vals.empty:
                try:
                    candidate_max = max(candidate_max, int(vals.max()))
                except Exception:
                    pass
        if sheet == "monitor_project" and "id" in df.columns:
            vals = pd.to_numeric(df["id"], errors="coerce").dropna()
            if not vals.empty:
                try:
                    candidate_max = max(candidate_max, int(vals.max()))
                except Exception:
                    pass

    # Also consult a persisted sequence to prevent id reuse after deletions
    # (even when no other sheet keeps historical references).
    meta_df = repo.query("meta")
    seq_next = None
    if meta_df is not None and not meta_df.empty and {"key", "value"}.issubset(meta_df.columns):
        tmp = meta_df.copy()
        tmp["key"] = tmp["key"].astype(str)
        hit = tmp[tmp["key"] == "next_project_id"]
        if not hit.empty:
            v = pd.to_numeric(hit.iloc[0].get("value"), errors="coerce")
            if pd.notna(v):
                try:
                    seq_next = int(v)
                except Exception:
                    seq_next = None

    candidate_next = int(candidate_max) + 1 if candidate_max > 0 else 1
    if seq_next is not None and seq_next > candidate_next:
        return int(seq_next)
    return int(candidate_next)


def _persist_next_project_id(repo, next_project_id: int) -> None:
    now = datetime.utcnow()
    df = repo.query("meta")
    if df is None or df.empty:
        df = pd.DataFrame(columns=["key", "value", "updated_at"])
    if "key" not in df.columns:
        df["key"] = None
    if "value" not in df.columns:
        df["value"] = None
    if "updated_at" not in df.columns:
        df["updated_at"] = None

    df2 = df.copy()
    df2["key"] = df2["key"].astype(str)
    mask = df2["key"] == "next_project_id"
    if mask.any():
        df2.loc[mask, "value"] = int(next_project_id)
        df2.loc[mask, "updated_at"] = now
    else:
        df2 = pd.concat(
            [
                df2,
                pd.DataFrame(
                    [{"key": "next_project_id", "value": int(next_project_id), "updated_at": now}]
                ),
            ],
            ignore_index=True,
        )
    repo.replace("meta", df2.to_dict(orient="records"))


def _cascade_delete_project_data(repo, project_id: int) -> Dict[str, int]:
    """
    Best-effort cascade cleanup for Excel-based storage.
    Removes rows that reference this project to prevent new projects (id reuse) from inheriting old data.
    """
    pid = int(project_id)
    counts: Dict[str, int] = {}

    run_df = repo.query("pipeline_run")
    run_ids: List[int] = []
    if run_df is not None and not run_df.empty and {"id", "project_id"}.issubset(run_df.columns):
        tmp = run_df.copy()
        tmp["project_id"] = pd.to_numeric(tmp["project_id"], errors="coerce")
        tmp["id"] = pd.to_numeric(tmp["id"], errors="coerce")
        tmp = tmp.dropna(subset=["id"])
        if "project_id" in tmp.columns:
            tmp2 = tmp.dropna(subset=["project_id"]).copy()
            tmp2["project_id"] = tmp2["project_id"].astype(int)
            run_ids = pd.to_numeric(tmp2.loc[tmp2["project_id"] == pid, "id"], errors="coerce").dropna().astype(int).tolist()

    def _drop_by_project(sheet: str) -> None:
        df = repo.query(sheet)
        if df is None or df.empty or "project_id" not in df.columns:
            return
        before = int(len(df))
        tmp = df.copy()
        tmp["project_id"] = pd.to_numeric(tmp["project_id"], errors="coerce")
        tmp_keep = tmp[tmp["project_id"].isna() | (tmp["project_id"].astype(int) != pid)]
        repo.replace(sheet, tmp_keep.to_dict(orient="records"))
        counts[sheet] = before - int(len(tmp_keep))

    # main fact tables
    raw_df = repo.query("post_raw")
    removed_raw_ids: List[int] = []
    if raw_df is not None and not raw_df.empty:
        before = int(len(raw_df))
        tmp = raw_df.copy()
        if "project_id" in tmp.columns:
            tmp["project_id"] = pd.to_numeric(tmp["project_id"], errors="coerce")
        if "pipeline_run_id" in tmp.columns:
            tmp["pipeline_run_id"] = pd.to_numeric(tmp["pipeline_run_id"], errors="coerce")
        if "id" in tmp.columns:
            tmp["id"] = pd.to_numeric(tmp["id"], errors="coerce")
        drop_mask = pd.Series([False] * len(tmp), index=tmp.index)
        if "project_id" in tmp.columns:
            drop_mask = drop_mask | (tmp["project_id"].notna() & (tmp["project_id"].astype(int) == pid))
        if run_ids and "pipeline_run_id" in tmp.columns:
            drop_mask = drop_mask | (tmp["pipeline_run_id"].notna() & tmp["pipeline_run_id"].astype(int).isin(run_ids))
        if "id" in tmp.columns:
            removed_raw_ids = pd.to_numeric(tmp.loc[drop_mask, "id"], errors="coerce").dropna().astype(int).tolist()
        tmp_keep = tmp[~drop_mask]
        repo.replace("post_raw", tmp_keep.to_dict(orient="records"))
        counts["post_raw"] = before - int(len(tmp_keep))

    clean_df = repo.query("post_clean")
    removed_clean_ids: List[int] = []
    if clean_df is not None and not clean_df.empty:
        before = int(len(clean_df))
        tmp = clean_df.copy()
        for c in ["project_id", "pipeline_run_id", "id", "post_raw_id"]:
            if c in tmp.columns:
                tmp[c] = pd.to_numeric(tmp[c], errors="coerce")
        drop_mask = pd.Series([False] * len(tmp), index=tmp.index)
        if "project_id" in tmp.columns:
            drop_mask = drop_mask | (tmp["project_id"].notna() & (tmp["project_id"].astype(int) == pid))
        if run_ids and "pipeline_run_id" in tmp.columns:
            drop_mask = drop_mask | (tmp["pipeline_run_id"].notna() & tmp["pipeline_run_id"].astype(int).isin(run_ids))
        if removed_raw_ids and "post_raw_id" in tmp.columns:
            drop_mask = drop_mask | (tmp["post_raw_id"].notna() & tmp["post_raw_id"].astype(int).isin(removed_raw_ids))
        if "id" in tmp.columns:
            removed_clean_ids = pd.to_numeric(tmp.loc[drop_mask, "id"], errors="coerce").dropna().astype(int).tolist()
        tmp_keep = tmp[~drop_mask]
        repo.replace("post_clean", tmp_keep.to_dict(orient="records"))
        counts["post_clean"] = before - int(len(tmp_keep))

    def _drop_by_clean_ids(sheet: str, clean_col: str) -> None:
        df = repo.query(sheet)
        if df is None or df.empty:
            return
        before = int(len(df))
        tmp = df.copy()
        if clean_col in tmp.columns:
            tmp[clean_col] = pd.to_numeric(tmp[clean_col], errors="coerce")
        if "project_id" in tmp.columns:
            tmp["project_id"] = pd.to_numeric(tmp["project_id"], errors="coerce")
        drop_mask = pd.Series([False] * len(tmp), index=tmp.index)
        if "project_id" in tmp.columns:
            drop_mask = drop_mask | (tmp["project_id"].notna() & (tmp["project_id"].astype(int) == pid))
        if removed_clean_ids and clean_col in tmp.columns:
            drop_mask = drop_mask | (tmp[clean_col].notna() & tmp[clean_col].astype(int).isin(removed_clean_ids))
        tmp_keep = tmp[~drop_mask]
        repo.replace(sheet, tmp_keep.to_dict(orient="records"))
        counts[sheet] = before - int(len(tmp_keep))

    _drop_by_clean_ids("spam_score", "post_clean_id")
    _drop_by_clean_ids("sentiment_result", "post_clean_id")
    _drop_by_clean_ids("topic_result", "post_clean_id")
    _drop_by_clean_ids("entity_result", "post_clean_id")

    _drop_by_project("daily_metric")

    # reports & citations
    report_df = repo.query("report")
    removed_report_ids: List[int] = []
    if report_df is not None and not report_df.empty:
        before = int(len(report_df))
        tmp = report_df.copy()
        for c in ["id", "project_id", "pipeline_run_id"]:
            if c in tmp.columns:
                tmp[c] = pd.to_numeric(tmp[c], errors="coerce")
        drop_mask = pd.Series([False] * len(tmp), index=tmp.index)
        if "project_id" in tmp.columns:
            drop_mask = drop_mask | (tmp["project_id"].notna() & (tmp["project_id"].astype(int) == pid))
        if run_ids and "pipeline_run_id" in tmp.columns:
            drop_mask = drop_mask | (tmp["pipeline_run_id"].notna() & tmp["pipeline_run_id"].astype(int).isin(run_ids))
        if "id" in tmp.columns:
            removed_report_ids = pd.to_numeric(tmp.loc[drop_mask, "id"], errors="coerce").dropna().astype(int).tolist()
        tmp_keep = tmp[~drop_mask]
        repo.replace("report", tmp_keep.to_dict(orient="records"))
        counts["report"] = before - int(len(tmp_keep))

    cfg_df = repo.query("report_config")
    if cfg_df is not None and not cfg_df.empty:
        before = int(len(cfg_df))
        tmp = cfg_df.copy()
        for c in ["report_id", "project_id"]:
            if c in tmp.columns:
                tmp[c] = pd.to_numeric(tmp[c], errors="coerce")
        drop_mask = pd.Series([False] * len(tmp), index=tmp.index)
        if "project_id" in tmp.columns:
            drop_mask = drop_mask | (tmp["project_id"].notna() & (tmp["project_id"].astype(int) == pid))
        if removed_report_ids and "report_id" in tmp.columns:
            drop_mask = drop_mask | (tmp["report_id"].notna() & tmp["report_id"].astype(int).isin(removed_report_ids))
        tmp_keep = tmp[~drop_mask]
        repo.replace("report_config", tmp_keep.to_dict(orient="records"))
        counts["report_config"] = before - int(len(tmp_keep))

    cit_df = repo.query("report_citation")
    if cit_df is not None and not cit_df.empty:
        before = int(len(cit_df))
        tmp = cit_df.copy()
        for c in ["report_id", "post_raw_id"]:
            if c in tmp.columns:
                tmp[c] = pd.to_numeric(tmp[c], errors="coerce")
        drop_mask = pd.Series([False] * len(tmp), index=tmp.index)
        if removed_report_ids and "report_id" in tmp.columns:
            drop_mask = drop_mask | (tmp["report_id"].notna() & tmp["report_id"].astype(int).isin(removed_report_ids))
        if removed_raw_ids and "post_raw_id" in tmp.columns:
            drop_mask = drop_mask | (tmp["post_raw_id"].notna() & tmp["post_raw_id"].astype(int).isin(removed_raw_ids))
        tmp_keep = tmp[~drop_mask]
        repo.replace("report_citation", tmp_keep.to_dict(orient="records"))
        counts["report_citation"] = before - int(len(tmp_keep))

    # pipeline runs last (some filtering above used run_ids)
    if run_df is not None and not run_df.empty and run_ids:
        before = int(len(run_df))
        tmp = run_df.copy()
        if "id" not in tmp.columns:
            return counts
        tmp["id"] = pd.to_numeric(tmp.get("id"), errors="coerce")
        tmp_keep = tmp[tmp["id"].isna() | (~tmp["id"].astype(int).isin(run_ids))]
        repo.replace("pipeline_run", tmp_keep.to_dict(orient="records"))
        counts["pipeline_run"] = before - int(len(tmp_keep))

    return counts


@router.get("")
def list_projects(user=Depends(get_current_user)):
    repo = get_repo()
    projects_df = _load_projects(repo)
    join_df = _load_project_brand(repo)
    proj_plat_df = _load_project_platform(repo)

    brand_map: Dict[int, List[int]] = {}
    if not join_df.empty:
        for pid, g in join_df.groupby("project_id"):
            brand_map[int(pid)] = sorted({int(x) for x in g["brand_id"].tolist() if x is not None})

    enabled_plat_map: Dict[int, List[int]] = {}
    plat_count_map: Dict[int, int] = {}
    if proj_plat_df is not None and not proj_plat_df.empty:
        for pid, g in proj_plat_df.groupby("project_id"):
            plat_count_map[int(pid)] = int(len(g))
            enabled_plat_map[int(pid)] = sorted({int(x) for x in g[g["is_enabled"] == 1]["platform_id"].tolist() if x is not None})

    out: List[Dict[str, Any]] = []
    for _, r in projects_df.sort_values(by="id").iterrows():
        pid = _norm_int(r.get("id"))
        if pid is None:
            continue
        out.append(
            {
                "id": int(pid),
                "name": str(r.get("name") or ""),
                "product_category": (None if pd.isna(r.get("product_category")) else r.get("product_category")),
                "brand_ids": brand_map.get(int(pid), []),
                "is_active": bool(int(pd.to_numeric(r.get("is_active"), errors="coerce") or 0)),
                "platform_configs_count": int(plat_count_map.get(int(pid), 0)),
                "enabled_platform_ids": enabled_plat_map.get(int(pid), []),
            }
        )
    return out


@router.post("/{project_id}/activate")
def activate_project(project_id: int, payload: ProjectActivateIn, user=Depends(get_current_user)):
    repo = get_repo()
    projects_df = _load_projects(repo)
    if projects_df.empty or "id" not in projects_df.columns:
        raise HTTPException(status_code=404, detail="project not found")

    if int(project_id) not in set(projects_df["id"].tolist()):
        raise HTTPException(status_code=404, detail="project not found")

    row = projects_df[projects_df["id"] == int(project_id)].iloc[0]
    current_active = bool(int(pd.to_numeric(row.get("is_active"), errors="coerce") or 0))
    if payload.active == current_active:
        return {"id": int(project_id), "is_active": current_active}

    if payload.active:
        # Max 3 enabled projects
        active_count = int((projects_df.get("is_active") == 1).sum()) if "is_active" in projects_df.columns else 0
        if not current_active and active_count >= 3:
            raise HTTPException(status_code=409, detail="at most 3 projects can be enabled")

        # Cannot enable without config: product_category and at least one brand
        product_category = row.get("product_category")
        if product_category is None or (isinstance(product_category, float) and pd.isna(product_category)) or not str(product_category).strip():
            raise HTTPException(status_code=422, detail="product_category is required to enable")
        brand_ids = _project_brand_ids(repo, int(project_id))
        if not brand_ids:
            raise HTTPException(status_code=422, detail="at least one brand is required to enable")

        # Ensure at least one enabled platform config exists; if missing, seed defaults.
        enabled_platforms = _enabled_platform_ids(repo, int(project_id))
        if not enabled_platforms:
            _seed_default_project_platforms(repo, int(project_id))
            enabled_platforms = _enabled_platform_ids(repo, int(project_id))
        if not enabled_platforms:
            raise HTTPException(status_code=422, detail="at least one enabled platform is required to enable")

    # Update is_active
    now = datetime.utcnow()
    updated = projects_df.copy()
    updated.loc[updated["id"] == int(project_id), "is_active"] = 1 if payload.active else 0
    if "updated_at" not in updated.columns:
        updated["updated_at"] = None
    updated.loc[updated["id"] == int(project_id), "updated_at"] = now
    repo.replace("monitor_project", updated.to_dict(orient="records"))
    return {"id": int(project_id), "is_active": payload.active}


@router.post("")
def save_projects(payload: List[ProjectIn], user=Depends(get_current_user)):
    repo = get_repo()
    now = datetime.utcnow()

    existing = _load_projects(repo)
    existing_ids = set(existing["id"].tolist()) if not existing.empty and "id" in existing.columns else set()
    next_id = max(max(existing_ids) + 1 if existing_ids else 1, _next_project_id(repo))

    # Keep other projects not included in the payload (this endpoint behaves like upsert).
    by_id: Dict[int, Dict[str, Any]] = {}
    if not existing.empty:
        for _, r in existing.iterrows():
            pid = _norm_int(r.get("id"))
            if pid is None:
                continue
            by_id[int(pid)] = {
                "id": int(pid),
                "brand_id": (None if pd.isna(r.get("brand_id")) else r.get("brand_id")),
                "name": r.get("name"),
                "description": r.get("description"),
                "is_active": int(pd.to_numeric(r.get("is_active"), errors="coerce") or 0),
                "created_at": r.get("created_at"),
                "updated_at": r.get("updated_at"),
                "product_category": r.get("product_category"),
            }

    join_df = _load_project_brand(repo)
    join_rows_existing = join_df.to_dict(orient="records") if join_df is not None and not join_df.empty else []

    touched_project_ids: set[int] = set()
    join_rows_new: List[Dict[str, Any]] = []

    for item in payload:
        pid = item.id
        if pid is None or pid <= 0:
            pid = next_id
            next_id += 1
        pid = int(pid)
        touched_project_ids.add(pid)

        # Enabled projects cannot be modified (except is_active via /activate).
        prev = by_id.get(pid)
        if prev and int(prev.get("is_active") or 0) == 1:
            # allow no-op save for enabled projects; block any content changes.
            prev_name = str(prev.get("name") or "")
            prev_cat = prev.get("product_category")
            prev_cat_str = "" if prev_cat is None or (isinstance(prev_cat, float) and pd.isna(prev_cat)) else str(prev_cat)
            prev_brand_ids = set(_project_brand_ids(repo, pid))
            new_brand_ids = set(int(x) for x in item.brand_ids if x is not None)
            if prev_name != item.name or prev_cat_str != (item.product_category or "") or prev_brand_ids != new_brand_ids:
                raise HTTPException(status_code=409, detail="enabled project config cannot be modified; disable it first")

        created_at = by_id.get(pid, {}).get("created_at") or now
        by_id[pid] = {
            "id": pid,
            "brand_id": None,  # legacy column unused once join table exists
            "name": item.name,
            "description": None,
            "is_active": 1 if item.is_active else 0,
            "created_at": created_at,
            "updated_at": now,
            "product_category": item.product_category,
        }

        unique_brand_ids = sorted({int(x) for x in item.brand_ids if x is not None})
        for bid in unique_brand_ids:
            join_rows_new.append(
                {
                    "id": _now_ts_ms() + len(join_rows_new) + 1,
                    "project_id": pid,
                    "brand_id": int(bid),
                    "created_at": now,
                }
            )

    # Remove old join rows for touched projects, keep others.
    join_rows_merged = [
        r for r in join_rows_existing if _norm_int(r.get("project_id")) not in touched_project_ids
    ] + join_rows_new

    df_proj = pd.DataFrame(list(by_id.values())).sort_values(by="id")
    df_proj = df_proj.replace([float("inf"), float("-inf")], None)
    df_proj = df_proj.astype(object).where(pd.notnull(df_proj), None)

    # Enforce max 3 enabled projects after save.
    if "is_active" in df_proj.columns:
        active_count = int((pd.to_numeric(df_proj["is_active"], errors="coerce").fillna(0).astype(int) == 1).sum())
        if active_count > 3:
            raise HTTPException(status_code=409, detail="at most 3 projects can be enabled")

    df_join = pd.DataFrame(join_rows_merged, columns=["id", "project_id", "brand_id", "created_at"])
    df_join = df_join.replace([float("inf"), float("-inf")], None)
    df_join = df_join.astype(object).where(pd.notnull(df_join), None)

    repo.replace("monitor_project", df_proj.to_dict(orient="records"))
    repo.replace("monitor_project_brand", df_join.to_dict(orient="records"))
    _persist_next_project_id(repo, next_id)
    return {"saved": len(touched_project_ids)}


@router.delete("/{project_id}")
def delete_project(project_id: int, user=Depends(get_current_user)):
    repo = get_repo()
    projects_df = _load_projects(repo)
    if projects_df.empty:
        raise HTTPException(status_code=404, detail="project not found")

    if int(project_id) not in set(projects_df["id"].tolist()):
        raise HTTPException(status_code=404, detail="project not found")

    row = projects_df[projects_df["id"] == int(project_id)].iloc[0]
    current_active = bool(int(pd.to_numeric(row.get("is_active"), errors="coerce") or 0))
    if current_active:
        raise HTTPException(status_code=409, detail="cannot delete an enabled project; disable it first")

    projects_df = projects_df[projects_df["id"] != int(project_id)]
    repo.replace("monitor_project", projects_df.to_dict(orient="records"))

    join_df = _load_project_brand(repo)
    if not join_df.empty:
        join_df = join_df[join_df["project_id"] != int(project_id)]
        repo.replace("monitor_project_brand", join_df.to_dict(orient="records"))

    plat_df = _load_project_platform(repo)
    if plat_df is not None and not plat_df.empty:
        plat_df = plat_df[plat_df["project_id"] != int(project_id)]
        repo.replace("monitor_project_platform", plat_df.to_dict(orient="records"))

    # Best-effort cascade cleanup: remove all rows referencing this project.
    kw_df = repo.query("monitor_keyword")
    if kw_df is not None and not kw_df.empty and "project_id" in kw_df.columns:
        kw_df = kw_df.copy()
        kw_df["project_id"] = pd.to_numeric(kw_df["project_id"], errors="coerce")
        kw_df = kw_df[kw_df["project_id"].isna() | (kw_df["project_id"].astype(int) != int(project_id))]
        repo.replace("monitor_keyword", kw_df.to_dict(orient="records"))

    cascade_counts = _cascade_delete_project_data(repo, int(project_id))

    return {"deleted": int(project_id), "cascade": cascade_counts}


@router.get("/{project_id}/platforms")
def get_project_platforms(project_id: int, user=Depends(get_current_user)):
    repo = get_repo()
    projects_df = _load_projects(repo)
    if projects_df.empty or int(project_id) not in set(projects_df["id"].tolist()):
        raise HTTPException(status_code=404, detail="project not found")

    plat_df = repo.query("platform")
    plat_meta: Dict[int, Dict[str, Any]] = {}
    if plat_df is not None and not plat_df.empty and {"id", "code", "name"}.issubset(plat_df.columns):
        tmp = plat_df.copy()
        tmp["id"] = pd.to_numeric(tmp["id"], errors="coerce")
        tmp = tmp.dropna(subset=["id"])
        tmp["id"] = tmp["id"].astype(int)
        for _, r in tmp.iterrows():
            plat_meta[int(r["id"])] = {"code": str(r.get("code") or ""), "name": str(r.get("name") or "")}

    df = _load_project_platform(repo)
    df = df[df["project_id"] == int(project_id)] if df is not None and not df.empty else df
    out: List[Dict[str, Any]] = []
    if df is not None and not df.empty:
        for _, r in df.sort_values(by=["platform_id"]).iterrows():
            pid = int(r["platform_id"])
            out.append(
                {
                    "id": int(r.get("id") or 0),
                    "project_id": int(project_id),
                    "platform_id": pid,
                    "platform_code": plat_meta.get(pid, {}).get("code"),
                    "platform_name": plat_meta.get(pid, {}).get("name"),
                    "is_enabled": bool(int(pd.to_numeric(r.get("is_enabled"), errors="coerce") or 0)),
                    "crawl_mode": str(r.get("crawl_mode") or "schedule"),
                    "cron_expr": str(r.get("cron_expr") or "0 5 * * *"),
                    "timezone": str(r.get("timezone") or "Asia/Shanghai"),
                    "max_posts_per_run": int(pd.to_numeric(r.get("max_posts_per_run"), errors="coerce") or 0) or 20,
                    "sentiment_model": str(r.get("sentiment_model") or "rule-based"),
                    "query_strategy": (None if pd.isna(r.get("query_strategy")) else r.get("query_strategy")),
                    "updated_at": r.get("updated_at"),
                }
            )
    return out


@router.put("/{project_id}/platforms")
def save_project_platforms(project_id: int, payload: ProjectPlatformsIn, user=Depends(get_current_user)):
    repo = get_repo()
    projects_df = _load_projects(repo)
    if projects_df.empty or int(project_id) not in set(projects_df["id"].tolist()):
        raise HTTPException(status_code=404, detail="project not found")

    now = datetime.utcnow()
    existing = _load_project_platform(repo)
    existing_project = existing[existing["project_id"] == int(project_id)] if existing is not None and not existing.empty else pd.DataFrame()
    created_at_by_platform: Dict[int, Any] = {}
    id_by_platform: Dict[int, int] = {}
    if existing_project is not None and not existing_project.empty:
        for _, r in existing_project.iterrows():
            pid = int(r["platform_id"])
            created_at_by_platform[pid] = r.get("created_at")
            rid = pd.to_numeric(r.get("id"), errors="coerce")
            if pd.notna(rid):
                id_by_platform[pid] = int(rid)

    rows: List[Dict[str, Any]] = []
    for item in payload.platforms or []:
        plat_id = int(item.platform_id)
        row_id = id_by_platform.get(plat_id) or (_now_ts_ms() + len(rows) + 1)
        rows.append(
            {
                "id": int(row_id),
                "project_id": int(project_id),
                "platform_id": plat_id,
                "is_enabled": 1 if bool(item.is_enabled) else 0,
                "crawl_mode": str(item.crawl_mode or "schedule"),
                "cron_expr": str(item.cron_expr or "0 5 * * *"),
                "timezone": str(item.timezone or "Asia/Shanghai"),
                "sentiment_model": str(item.sentiment_model or "rule-based"),
                "max_posts_per_run": int(max(1, min(int(item.max_posts_per_run or 20), 500))),
                "query_strategy": item.query_strategy,
                "last_run_at": None,
                "last_success_at": None,
                "next_run_at": None,
                "created_at": created_at_by_platform.get(plat_id) or now,
                "updated_at": now,
            }
        )

    # merge: keep other projects' rows; replace current project's rows
    keep_rows: List[Dict[str, Any]] = []
    if existing is not None and not existing.empty:
        keep_rows = [r for r in existing.to_dict(orient="records") if _norm_int(r.get("project_id")) != int(project_id)]
    merged = keep_rows + rows

    repo.replace("monitor_project_platform", merged)
    return {"saved": len(rows)}

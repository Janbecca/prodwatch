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


@router.get("")
def list_projects(user=Depends(get_current_user)):
    repo = get_repo()
    projects_df = _load_projects(repo)
    join_df = _load_project_brand(repo)

    brand_map: Dict[int, List[int]] = {}
    if not join_df.empty:
        for pid, g in join_df.groupby("project_id"):
            brand_map[int(pid)] = sorted({int(x) for x in g["brand_id"].tolist() if x is not None})

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
    next_id = max(existing_ids) + 1 if existing_ids else 1

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
    return {"saved": len(touched_project_ids)}


@router.delete("/{project_id}")
def delete_project(project_id: int, user=Depends(get_current_user)):
    repo = get_repo()
    projects_df = _load_projects(repo)
    if projects_df.empty:
        raise HTTPException(status_code=404, detail="project not found")

    if int(project_id) not in set(projects_df["id"].tolist()):
        raise HTTPException(status_code=404, detail="project not found")

    projects_df = projects_df[projects_df["id"] != int(project_id)]
    repo.replace("monitor_project", projects_df.to_dict(orient="records"))

    join_df = _load_project_brand(repo)
    if not join_df.empty:
        join_df = join_df[join_df["project_id"] != int(project_id)]
        repo.replace("monitor_project_brand", join_df.to_dict(orient="records"))

    return {"deleted": int(project_id)}

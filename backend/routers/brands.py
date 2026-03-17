from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.storage.db import get_repo
from .auth import get_current_user

router = APIRouter(prefix="/brands", tags=["brands"])


class BrandCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=80)
    industry: Optional[str] = Field(None, max_length=80)


def _norm_int(x: Any) -> Optional[int]:
    v = pd.to_numeric(x, errors="coerce")
    if pd.isna(v):
        return None
    return int(v)


@router.get("")
def list_brands(user=Depends(get_current_user)):
    repo = get_repo()
    df = repo.query("brand")
    if df is None or df.empty:
        return []

    if "id" in df.columns:
        ids = pd.to_numeric(df["id"], errors="coerce")
        if ids.notna().any():
            df = df[ids.notna()].copy()
            df["id"] = pd.to_numeric(df["id"], errors="coerce").astype(int)

    out: List[Dict[str, Any]] = []
    for _, r in df.sort_values(by="id").iterrows():
        bid = _norm_int(r.get("id"))
        if bid is None:
            continue
        out.append(
            {
                "id": int(bid),
                "name": str(r.get("name") or ""),
                "industry": (None if pd.isna(r.get("industry")) else r.get("industry")),
            }
        )
    return out


@router.post("")
def create_brand(payload: BrandCreate, user=Depends(get_current_user)):
    repo = get_repo()
    df = repo.query("brand")
    if df is None:
        df = pd.DataFrame()

    if not df.empty and "id" in df.columns:
        ids = pd.to_numeric(df["id"], errors="coerce")
        if ids.notna().any():
            df = df[ids.notna()].copy()
            df["id"] = pd.to_numeric(df["id"], errors="coerce").astype(int)

    # prevent duplicates by name (case-insensitive)
    if not df.empty and "name" in df.columns:
        names = df["name"].astype(str).str.strip().str.lower()
        if (names == payload.name.strip().lower()).any():
            raise HTTPException(status_code=409, detail="brand already exists")

    next_id = int(df["id"].max()) + 1 if (not df.empty and "id" in df.columns) else 1
    now = datetime.utcnow()
    repo.insert(
        "brand",
        {
            "id": next_id,
            "name": payload.name.strip(),
            "industry": payload.industry,
            "created_at": now,
        },
    )
    return {"id": next_id}


@router.delete("/{brand_id}")
def delete_brand(brand_id: int, user=Depends(get_current_user)):
    repo = get_repo()

    join_df = repo.query("monitor_project_brand")
    if join_df is not None and not join_df.empty and "brand_id" in join_df.columns:
        join_df = join_df.copy()
        join_df["brand_id"] = pd.to_numeric(join_df["brand_id"], errors="coerce")
        in_use = join_df["brand_id"].dropna().astype(int).isin([int(brand_id)]).any()
        if in_use:
            raise HTTPException(status_code=409, detail="brand is used by existing projects")

    projects_df = repo.query("monitor_project")
    if projects_df is not None and not projects_df.empty and "brand_id" in projects_df.columns:
        projects_df = projects_df.copy()
        projects_df["brand_id"] = pd.to_numeric(projects_df["brand_id"], errors="coerce")
        in_use = projects_df["brand_id"].dropna().astype(int).isin([int(brand_id)]).any()
        if in_use:
            raise HTTPException(status_code=409, detail="brand is used by existing projects")

    brand_df = repo.query("brand")
    if brand_df is None or brand_df.empty:
        raise HTTPException(status_code=404, detail="brand not found")
    if "id" not in brand_df.columns:
        raise HTTPException(status_code=404, detail="brand not found")

    brand_df = brand_df.copy()
    brand_df["id"] = pd.to_numeric(brand_df["id"], errors="coerce")
    brand_df = brand_df.dropna(subset=["id"])
    brand_df["id"] = brand_df["id"].astype(int)
    if int(brand_id) not in set(brand_df["id"].tolist()):
        raise HTTPException(status_code=404, detail="brand not found")

    brand_df = brand_df[brand_df["id"] != int(brand_id)]
    repo.replace("brand", brand_df.to_dict(orient="records"))

    if join_df is not None and not join_df.empty:
        join_df = join_df.copy()
        join_df["brand_id"] = pd.to_numeric(join_df["brand_id"], errors="coerce")
        join_df = join_df.dropna(subset=["brand_id"])
        join_df["brand_id"] = join_df["brand_id"].astype(int)
        join_df = join_df[join_df["brand_id"] != int(brand_id)]
        repo.replace("monitor_project_brand", join_df.to_dict(orient="records"))

    return {"deleted": int(brand_id)}

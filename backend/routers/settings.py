from datetime import datetime
from fastapi import APIRouter, Depends
from typing import List
import pandas as pd
from .auth import get_current_user
from backend.storage.db import get_repo

router = APIRouter(prefix="/settings", tags=["settings"])


@router.get("/datasources")
def list_datasources(user=Depends(get_current_user)):
    repo = get_repo()
    platform_df = repo.query("platform")
    try:
        cfg_df = repo.query("datasource_config")
    except Exception:
        cfg_df = pd.DataFrame(columns=["id", "freq"])

    cfg_map = {}
    if not cfg_df.empty and {"id", "freq"}.issubset(cfg_df.columns):
        for _, row in cfg_df.iterrows():
            cfg_map[str(row.get("id"))] = row.get("freq")

    rows = []
    for _, row in platform_df.iterrows():
        code = str(row.get("code"))
        rows.append({"id": code, "name": row.get("name"), "freq": cfg_map.get(code)})
    return rows


@router.post("/datasources")
def save_datasources(sources: List[dict], user=Depends(get_current_user)):
    repo = get_repo()
    now = datetime.utcnow()
    rows = []
    for item in sources:
        rows.append(
            {
                "id": str(item.get("id")),
                "freq": item.get("freq"),
                "updated_at": now,
            }
        )
    repo.replace("datasource_config", rows)
    return {"saved": len(rows)}


@router.get("/users")
def list_users(user=Depends(get_current_user)):
    repo = get_repo()
    projects = repo.query("monitor_project")
    if projects.empty:
        return []
    cols = [c for c in ["id", "name", "description", "is_active"] if c in projects.columns]
    return projects[cols].to_dict(orient="records")

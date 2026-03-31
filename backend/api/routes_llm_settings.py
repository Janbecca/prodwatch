from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.api.db import get_db
from backend.llm.config_store import DEFAULT_TASKS, get_llm_config_store
from backend.llm.provider_factory import get_provider_factory
from backend.llm.types import LLMTaskConfig


router = APIRouter(prefix="/api/settings/llm", tags=["settings"])


def _has_table(con: sqlite3.Connection, table: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;",
        (table,),
    ).fetchone()
    return row is not None


def _ensure_llm_task_config_table(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS llm_task_config (
          task_type TEXT PRIMARY KEY,
          provider TEXT NOT NULL,
          model TEXT,
          fallback_provider TEXT NOT NULL DEFAULT 'mock',
          fallback_model TEXT,
          updated_at DATETIME
        );
        """
    )


def _list_known_task_types(con: sqlite3.Connection) -> list[str]:
    """
    Return a stable list of task types for UI configuration.

    Sources:
    - Hardcoded default tasks
    - Prompt template files
    - Existing DB overrides
    """
    tasks = set(str(t) for t in (DEFAULT_TASKS or []))

    # prompt templates
    try:
        base = Path(__file__).resolve().parents[1] / "llm" / "prompts" / "templates"
        # When running from installed package / different CWD, fall back to backend/llm/prompts/templates
        if not base.exists():
            base = Path(__file__).resolve().parents[1] / "llm" / "prompts" / "templates"
    except Exception:
        base = None
    if base and base.exists():
        for p in base.glob("*.json"):
            tasks.add(p.stem)

    # db overrides
    if _has_table(con, "llm_task_config"):
        rows = con.execute("SELECT task_type FROM llm_task_config ORDER BY task_type ASC;").fetchall()
        for r in rows:
            tasks.add(str(r["task_type"]))

    return sorted(tasks)


class LLMTaskConfigDTO(BaseModel):
    task_type: str = Field(..., min_length=1)
    provider: str = Field(..., min_length=1)
    model: Optional[str] = None
    fallback_provider: str = Field(default="mock", min_length=1)
    fallback_model: Optional[str] = None


class UpdateLLMTaskConfigRequest(BaseModel):
    configs: list[LLMTaskConfigDTO]


@router.get("/providers")
def list_providers() -> dict[str, Any]:
    factory = get_provider_factory()
    return {"providers": factory.list_provider_names()}


@router.get("/task-config")
def get_task_configs(db: sqlite3.Connection = Depends(get_db)) -> dict[str, Any]:
    # Read-only endpoint should not create tables implicitly; but if the table exists, include overrides.
    tasks = _list_known_task_types(db)
    store = get_llm_config_store()

    overrides: dict[str, Any] = {}
    if _has_table(db, "llm_task_config"):
        rows = db.execute(
            """
            SELECT task_type, provider, model, fallback_provider, fallback_model, updated_at
            FROM llm_task_config
            ORDER BY task_type ASC;
            """
        ).fetchall()
        for r in rows:
            overrides[str(r["task_type"])] = {
                "provider": r["provider"],
                "model": r["model"],
                "fallback_provider": r["fallback_provider"],
                "fallback_model": r["fallback_model"],
                "updated_at": r["updated_at"],
            }

    out = []
    for t in tasks:
        eff = store.get(t, con=db)
        ov = overrides.get(t)
        out.append(
            {
                "task_type": t,
                "effective": {
                    "provider": eff.provider,
                    "model": eff.model,
                    "fallback_provider": eff.fallback_provider,
                    "fallback_model": eff.fallback_model,
                },
                "override": ov,
            }
        )

    return {"tasks": tasks, "items": out, "providers": get_provider_factory().list_provider_names()}


@router.put("/task-config")
def upsert_task_configs(payload: UpdateLLMTaskConfigRequest, db: sqlite3.Connection = Depends(get_db)) -> dict[str, Any]:
    _ensure_llm_task_config_table(db)
    db.commit()

    allowed = set(get_provider_factory().list_provider_names())
    store = get_llm_config_store()

    for dto in payload.configs:
        provider = str(dto.provider).strip().lower()
        fb_provider = str(dto.fallback_provider).strip().lower()
        if provider not in allowed:
            raise HTTPException(status_code=400, detail=f"unknown provider: {provider}")
        if fb_provider not in allowed:
            raise HTTPException(status_code=400, detail=f"unknown fallback_provider: {fb_provider}")
        cfg = LLMTaskConfig(
            task_type=str(dto.task_type).strip(),
            provider=provider,
            model=(str(dto.model).strip() if dto.model is not None and str(dto.model).strip() != "" else None),
            fallback_provider=fb_provider,
            fallback_model=(
                str(dto.fallback_model).strip()
                if dto.fallback_model is not None and str(dto.fallback_model).strip() != ""
                else None
            ),
        )
        store.upsert(db, cfg)

    db.commit()
    return get_task_configs(db)


@router.delete("/task-config/{task_type}")
def delete_task_config(task_type: str, db: sqlite3.Connection = Depends(get_db)) -> dict[str, Any]:
    if not _has_table(db, "llm_task_config"):
        return get_task_configs(db)
    db.execute("DELETE FROM llm_task_config WHERE task_type=?;", (str(task_type),))
    db.commit()
    return get_task_configs(db)


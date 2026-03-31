from __future__ import annotations

import sqlite3
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.api.db import get_db
from backend.llm.config_store import get_llm_config_store
from backend.llm.provider_factory import get_provider_factory
from backend.llm.types import LLMTaskConfig


router = APIRouter(prefix="/api/llm", tags=["llm"])


TASKS = [
    {"task_type": "crawler_generation", "title": "Crawler Generation"},
    {"task_type": "post_analysis", "title": "Post Analysis"},
    {"task_type": "report_generation", "title": "Report Generation"},
]


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


def _models_by_provider() -> dict[str, list[str]]:
    """
    Model dropdown source for the UI.

    Keep small + conservative. Actual runtime defaults can still be overridden by env vars in providers.
    """
    return {
        "mock": ["mock-v1"],
        "deepseek": ["deepseek-chat", "deepseek-reasoner"],
        # DashScope OpenAI-compat common names
        "qwen": ["qwen-turbo", "qwen-plus", "qwen-max"],
    }


def _cheap_defaults() -> dict[str, dict[str, Any]]:
    """
    Default cost-saving plan.
    - crawler_generation: prefer mock to avoid paid calls for simulated crawling
    - post_analysis/report_generation: use a cheap general chat model + mock fallback
    """
    return {
        "crawler_generation": {
            "provider": "mock",
            "model": "mock-v1",
            "fallback_provider": "mock",
            "fallback_model": "mock-v1",
        },
        "post_analysis": {
            "provider": "deepseek",
            "model": "deepseek-chat",
            "fallback_provider": "mock",
            "fallback_model": "mock-v1",
        },
        "report_generation": {
            "provider": "deepseek",
            "model": "deepseek-chat",
            "fallback_provider": "mock",
            "fallback_model": "mock-v1",
        },
    }


def _effective_configs(con: sqlite3.Connection) -> list[dict[str, Any]]:
    store = get_llm_config_store()
    rows = []
    for t in TASKS:
        task_type = t["task_type"]
        cfg = store.get(task_type, con=con)
        rows.append(
            {
                "task_type": task_type,
                "title": t["title"],
                "config": {
                    "provider": cfg.provider,
                    "model": cfg.model,
                    "fallback_provider": cfg.fallback_provider,
                    "fallback_model": cfg.fallback_model,
                },
            }
        )
    return rows


class LLMTaskConfigDTO(BaseModel):
    task_type: str = Field(..., min_length=1)
    provider: str = Field(..., min_length=1)
    model: Optional[str] = None
    fallback_provider: str = Field(default="mock", min_length=1)
    fallback_model: Optional[str] = None


class PutLLMConfigRequest(BaseModel):
    items: list[LLMTaskConfigDTO] = Field(default_factory=list)


@router.get("/models")
def get_models() -> dict[str, Any]:
    factory = get_provider_factory()
    providers = factory.list_provider_names()
    m = _models_by_provider()
    # Only return models for registered providers to avoid front-end drift.
    models_by_provider = {p: list(m.get(p) or []) for p in providers}
    return {
        "providers": providers,
        "models_by_provider": models_by_provider,
        "cheap_defaults": _cheap_defaults(),
    }


@router.get("/config")
def get_config(db: sqlite3.Connection = Depends(get_db)) -> dict[str, Any]:
    factory = get_provider_factory()
    providers = factory.list_provider_names()
    m = _models_by_provider()
    models_by_provider = {p: list(m.get(p) or []) for p in providers}
    return {
        "notes": [
            "当前配置对所有项目生效",
            "仅影响后续新任务",
        ],
        "tasks": _effective_configs(db),
        "providers": providers,
        "models_by_provider": models_by_provider,
        "cheap_defaults": _cheap_defaults(),
    }


@router.put("/config")
def put_config(payload: PutLLMConfigRequest, db: sqlite3.Connection = Depends(get_db)) -> dict[str, Any]:
    _ensure_llm_task_config_table(db)
    db.commit()

    allowed_tasks = {t["task_type"] for t in TASKS}
    allowed_providers = set(get_provider_factory().list_provider_names())
    models_by_provider = _models_by_provider()
    store = get_llm_config_store()

    for item in payload.items or []:
        task_type = str(item.task_type).strip()
        if task_type not in allowed_tasks:
            raise HTTPException(status_code=400, detail=f"unknown task_type: {task_type}")

        provider = str(item.provider).strip().lower()
        fb_provider = str(item.fallback_provider).strip().lower()
        if provider not in allowed_providers:
            raise HTTPException(status_code=400, detail=f"unknown provider: {provider}")
        if fb_provider not in allowed_providers:
            raise HTTPException(status_code=400, detail=f"unknown fallback_provider: {fb_provider}")

        model = (str(item.model).strip() if item.model is not None else "")
        model = None if model == "" else model
        fb_model = (str(item.fallback_model).strip() if item.fallback_model is not None else "")
        fb_model = None if fb_model == "" else fb_model

        # Optional guard: if the provider has a known model list, validate membership.
        known_models = set(models_by_provider.get(provider) or [])
        if known_models and model is not None and model not in known_models:
            raise HTTPException(status_code=400, detail=f"unknown model for provider={provider}: {model}")
        known_fb_models = set(models_by_provider.get(fb_provider) or [])
        if known_fb_models and fb_model is not None and fb_model not in known_fb_models:
            raise HTTPException(status_code=400, detail=f"unknown fallback_model for provider={fb_provider}: {fb_model}")

        store.upsert(
            db,
            LLMTaskConfig(
                task_type=task_type,
                provider=provider,
                model=model,
                fallback_provider=fb_provider,
                fallback_model=fb_model,
            ),
        )

    db.commit()
    return get_config(db)


# 作用：后端 API：LLM 配置相关路由与接口实现。

from __future__ import annotations

import sqlite3
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.api.db import get_db_relaxed
from backend.llm.config_store import get_llm_config_store
from backend.llm.provider_factory import get_provider_factory
from backend.llm.types import LLMTaskConfig


router = APIRouter(prefix="/api/llm", tags=["llm"])


TASKS = [
    {"task_type": "crawler_generation", "title": "帖子生成（模拟爬虫）"},
    {"task_type": "post_analysis", "title": "帖子分析"},
    {"task_type": "report_generation", "title": "报告生成"},
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
          updated_at DATETIME
        );
        """
    )


# 作用：LLM：模型提供方实现（OpenAI 兼容协议客户端封装）。
def _models_by_provider() -> dict[str, list[str]]:
    """
    用于前端的模型下拉列表数据源。
    实际运行时的默认值仍可通过各提供商（Provider）中的环境变量进行覆盖。
    """
    return {
        "deepseek": ["deepseek-chat", "deepseek-reasoner"],
        "qwen": ["qwen-turbo", "qwen-plus", "qwen-max"],
    }


def _cheap_defaults() -> dict[str, dict[str, Any]]:
    """
    Default (single-model) plan.

    Note: This project does not support fallback_provider/fallback_model.
    """
    return {
        "crawler_generation": {
            "provider": "deepseek",
            "model": "deepseek-chat",
        },
        "post_analysis": {
            "provider": "deepseek",
            "model": "deepseek-chat",
        },
        "report_generation": {
            "provider": "deepseek",
            "model": "deepseek-chat",
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
                },
            }
        )
    return rows


class LLMTaskConfigDTO(BaseModel):
    task_type: str = Field(..., min_length=1)
    provider: str = Field(..., min_length=1)
    model: Optional[str] = None


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
def get_config(db: sqlite3.Connection = Depends(get_db_relaxed)) -> dict[str, Any]:
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
def put_config(payload: PutLLMConfigRequest, db: sqlite3.Connection = Depends(get_db_relaxed)) -> dict[str, Any]:
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
        if provider not in allowed_providers:
            raise HTTPException(status_code=400, detail=f"unknown provider: {provider}")

        model = (str(item.model).strip() if item.model is not None else "")
        model = None if model == "" else model

        # Do not hard-fail on model names: the UI has a conservative dropdown list,
        # but real providers may support more models and users may type custom names.

        store.upsert(
            db,
            LLMTaskConfig(
                task_type=task_type,
                provider=provider,
                model=model,
            ),
        )

    db.commit()
    return get_config(db)


# 作用：LLM：配置读取/存储与运行时开关管理。

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import asdict
from typing import Any, Optional

from backend.llm.types import LLMTaskConfig
from backend.llm.file_task_config import get_llm_config_path, load_llm_tasks_from_file

# 系统支持的任务类型枚举
DEFAULT_TASKS = [
    "crawler_generation",
    "sentiment_analysis",
    "keyword_extraction",
    "feature_extraction",
    "spam_detection",
    "report_generation",
]

# 默认配置：所有任务默认使用 Mock 提供者和模型，确保系统在没有外部配置时仍能正常运行。
def default_config(task_type: str) -> LLMTaskConfig:
    return LLMTaskConfig(task_type=str(task_type), provider="mock", model="mock-v1", fallback_provider="mock", fallback_model="mock-v1")

# 从环境变量读取 JSON 配置，支持覆盖默认配置。环境变量格式示例：
def _env_json_config() -> dict[str, Any]:
    raw = os.environ.get("PRODWATCH_LLM_TASK_CONFIG_JSON") or ""
    raw = raw.strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def _db_has_table(con: sqlite3.Connection, table: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;",
        (table,),
    ).fetchone()
    return row is not None


class LLMConfigStore:
    """
    Per-task LLM config store.

    Resolution order:
    1) DB table `llm_task_config` if it exists and contains a row for task_type
    2) Env var JSON `PRODWATCH_LLM_TASK_CONFIG_JSON`
    3) Hardcoded defaults (Mock)
    """

    def get(self, task_type: str, con: Optional[sqlite3.Connection] = None) -> LLMTaskConfig:
        task = str(task_type)
        # DB override
        if con is not None and _db_has_table(con, "llm_task_config"):
            row = con.execute(
                """
                SELECT task_type, provider, model, fallback_provider, fallback_model
                FROM llm_task_config
                WHERE task_type=?
                LIMIT 1;
                """,
                (task,),
            ).fetchone()
            if row is not None:
                return LLMTaskConfig(
                    task_type=task,
                    provider=str(row["provider"] or "mock"),
                    model=row["model"],
                    fallback_provider=str(row["fallback_provider"] or "mock"),
                    fallback_model=row["fallback_model"],
                )

        # File override (PRODWATCH_LLM_CONFIG_PATH)
        path = get_llm_config_path()
        if path:
            data = load_llm_tasks_from_file(path)
            llm_tasks = data.get("llm_tasks") if isinstance(data, dict) else None
            if isinstance(llm_tasks, dict):
                cfg = llm_tasks.get(task)
                if isinstance(cfg, dict):
                    return LLMTaskConfig(
                        task_type=task,
                        provider=str(cfg.get("provider") or "mock"),
                        model=cfg.get("model"),
                        fallback_provider=str(cfg.get("fallback_provider") or "mock"),
                        fallback_model=cfg.get("fallback_model"),
                    )

        # Env override
        env = _env_json_config()
        cfg = env.get(task)
        if isinstance(cfg, dict):
            return LLMTaskConfig(
                task_type=task,
                provider=str(cfg.get("provider") or "mock"),
                model=cfg.get("model"),
                fallback_provider=str(cfg.get("fallback_provider") or "mock"),
                fallback_model=cfg.get("fallback_model"),
            )

        return default_config(task)
    
    def upsert(self, con: sqlite3.Connection, cfg: LLMTaskConfig) -> None:
        con.execute(
            """
            INSERT INTO llm_task_config(task_type, provider, model, fallback_provider, fallback_model, updated_at)
            VALUES(?, ?, ?, ?, ?, datetime('now','localtime'))
            ON CONFLICT(task_type) DO UPDATE SET
              provider=excluded.provider,
              model=excluded.model,
              fallback_provider=excluded.fallback_provider,
              fallback_model=excluded.fallback_model,
              updated_at=datetime('now','localtime');
            """,
            (
                cfg.task_type,
                cfg.provider,
                cfg.model,
                cfg.fallback_provider,
                cfg.fallback_model,
            ),
        )


_store: Optional[LLMConfigStore] = None


def get_llm_config_store() -> LLMConfigStore:
    global _store
    if _store is None:
        _store = LLMConfigStore()
    return _store

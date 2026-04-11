# 作用：LLM：配置读取/存储与运行时开关管理。

from __future__ import annotations

import json
import os
import sqlite3
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

DEFAULT_PROVIDER = "deepseek"
DEFAULT_MODEL_BY_PROVIDER: dict[str, str] = {
    "deepseek": "deepseek-chat",
    "qwen": "qwen-plus",
}


def _normalize_provider(name: object) -> str:
    """
    Normalize provider names.

    Compatibility: older DBs/configs may still contain "mock". Treat it as unset.
    """
    v = str(name or "").strip().lower()
    if not v or v == "mock":
        return DEFAULT_PROVIDER
    return v


def _default_model(provider: str) -> str | None:
    p = _normalize_provider(provider)
    return DEFAULT_MODEL_BY_PROVIDER.get(p)

def _normalize_model(provider: str, model: object) -> str | None:
    """
    Normalize model names.

    Compatibility: older configs might have leftover mock model names like "mock-v1".
    When provider is real (deepseek/qwen), treat "mock*" as unset so we fall back to the
    provider default model.
    """
    v = str(model or "").strip()
    if not v:
        return None
    if v.lower().startswith("mock"):
        return None
    return v


# 默认配置：所有任务默认使用真实 LLM 提供者，避免 mock 兜底逻辑。
def default_config(task_type: str) -> LLMTaskConfig:
    provider = DEFAULT_PROVIDER
    return LLMTaskConfig(
        task_type=str(task_type),
        provider=provider,
        model=_default_model(provider),
    )

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
                SELECT task_type, provider, model
                FROM llm_task_config
                WHERE task_type=?
                LIMIT 1;
                """,
                (task,),
            ).fetchone()
            if row is not None:
                provider = _normalize_provider(row["provider"])
                return LLMTaskConfig(
                    task_type=task,
                    provider=provider,
                    model=_normalize_model(provider, row["model"]) or _default_model(provider),
                )

        # File override (PRODWATCH_LLM_CONFIG_PATH)
        path = get_llm_config_path()
        if path:
            data = load_llm_tasks_from_file(path)
            llm_tasks = data.get("llm_tasks") if isinstance(data, dict) else None
            if isinstance(llm_tasks, dict):
                cfg = llm_tasks.get(task)
                if isinstance(cfg, dict):
                    provider = _normalize_provider(cfg.get("provider"))
                    return LLMTaskConfig(
                        task_type=task,
                        provider=provider,
                        model=_normalize_model(provider, cfg.get("model")) or _default_model(provider),
                    )

        # Env override
        env = _env_json_config()
        cfg = env.get(task)
        if isinstance(cfg, dict):
            provider = _normalize_provider(cfg.get("provider"))
            return LLMTaskConfig(
                task_type=task,
                provider=provider,
                model=_normalize_model(provider, cfg.get("model")) or _default_model(provider),
            )

        return default_config(task)
    
    def upsert(self, con: sqlite3.Connection, cfg: LLMTaskConfig) -> None:
        con.execute(
            """
            INSERT INTO llm_task_config(task_type, provider, model, updated_at)
            VALUES(?, ?, ?, datetime('now','localtime'))
            ON CONFLICT(task_type) DO UPDATE SET
              provider=excluded.provider,
              model=excluded.model,
              updated_at=datetime('now','localtime');
            """,
            (
                cfg.task_type,
                cfg.provider,
                cfg.model,
            ),
        )


_store: Optional[LLMConfigStore] = None


def get_llm_config_store() -> LLMConfigStore:
    global _store
    if _store is None:
        _store = LLMConfigStore()
    return _store

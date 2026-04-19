# 作用：后端 API：LLM 配置相关路由与接口实现。

from __future__ import annotations

import sqlite3
from pathlib import Path
from datetime import datetime
import json
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.api.db import get_db_relaxed
from backend.llm.config_store import get_llm_config_store
from backend.llm.prompts.store import get_prompt_store
from backend.llm.provider_factory import get_provider_factory
from backend.llm.types import LLMTaskConfig


router = APIRouter(prefix="/api/llm", tags=["llm"])


TASKS = [
    {"task_type": "crawler_generation", "title": "舆情爬取"},
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


def _prompt_template_path(task_type: str) -> Path:
    base = Path(__file__).resolve().parents[1] / "llm" / "prompts" / "templates"
    return base / f"{str(task_type)}.json"


def _iso_local(ts: float) -> str:
    try:
        return datetime.fromtimestamp(float(ts)).astimezone().isoformat(timespec="seconds")
    except Exception:
        return ""


class PromptTemplateDTO(BaseModel):
    task_type: str
    version: str
    template: str
    updated_at: Optional[str] = None


class PutPromptTemplateRequest(BaseModel):
    """
    Only allow editing the "prompt" section. Output/Input JSON sections are kept read-only.

    Backward compatibility:
    - If prompt is not provided, `template` is accepted as the prompt text.
    - `version` is ignored; backend will auto-bump based on current version.
    """

    prompt: Optional[str] = None
    template: Optional[str] = None
    version: Optional[str] = None


_TAG_OUT_START = "[OUTPUT_JSON]"
_TAG_OUT_END = "[/OUTPUT_JSON]"
_TAG_IN_START = "[INPUT_JSON]"
_TAG_IN_END = "[/INPUT_JSON]"
_TAG_PROMPT_START = "[PROMPT]"
_TAG_PROMPT_END = "[/PROMPT]"


def _split_template_sections(template: str) -> tuple[str, str, str]:
    s = str(template or "")
    try:
        o1 = s.index(_TAG_OUT_START) + len(_TAG_OUT_START)
        o2 = s.index(_TAG_OUT_END, o1)
        i1 = s.index(_TAG_IN_START, o2) + len(_TAG_IN_START)
        i2 = s.index(_TAG_IN_END, i1)
        p1 = s.index(_TAG_PROMPT_START, i2) + len(_TAG_PROMPT_START)
        p2 = s.index(_TAG_PROMPT_END, p1)
        out_json = s[o1:o2].strip("\n")
        in_json = s[i1:i2].strip("\n")
        prompt = s[p1:p2].strip("\n")
        return out_json, in_json, prompt
    except Exception:
        # Backward compatible fallback: treat entire template as prompt
        return "", "", s.strip("\n")


def _join_template_sections(*, output_json: str, input_json: str, prompt: str) -> str:
    return (
        f"{_TAG_OUT_START}\n{str(output_json or '').strip()}\n{_TAG_OUT_END}\n\n"
        f"{_TAG_IN_START}\n{str(input_json or '').strip()}\n{_TAG_IN_END}\n\n"
        f"{_TAG_PROMPT_START}\n{str(prompt or '').strip()}\n{_TAG_PROMPT_END}\n"
    )


def _bump_version(v: str) -> str:
    raw = str(v or "").strip()
    if raw == "":
        return "v1"
    # Prefer "vN" format, but be tolerant.
    import re

    m = re.search(r"(.*?)(\d+)(\D*)$", raw)
    if not m:
        # no number -> start at v1
        return "v1"
    prefix, num, suffix = m.group(1), m.group(2), m.group(3)
    try:
        n = int(num)
        return f"{prefix}{n + 1}{suffix}"
    except Exception:
        return "v1"


@router.get("/prompts")
def get_prompts() -> dict[str, Any]:
    """
    Return prompt templates that the backend will actually use (PromptStore).

    Note: updated_at is derived from the on-disk template file mtime (not stored in JSON).
    """
    store = get_prompt_store()

    # Return all existing file templates + known task types.
    base_dir = _prompt_template_path("x").parent
    file_types: list[str] = []
    try:
        if base_dir.exists():
            for p in base_dir.glob("*.json"):
                file_types.append(p.stem)
    except Exception:
        file_types = []

    task_types = []
    seen = set()
    for t in (file_types + [str(t["task_type"]) for t in TASKS]):
        tt = str(t or "").strip()
        if not tt or tt in seen:
            continue
        seen.add(tt)
        task_types.append(tt)

    items: list[dict[str, Any]] = []
    for task_type in task_types:
        pt = store.get(task_type)
        path = _prompt_template_path(task_type)
        updated_at = _iso_local(path.stat().st_mtime) if path.exists() else None
        items.append(
            {
                "task_type": str(pt.task_type),
                "version": str(pt.version),
                "template": str(pt.template),
                "updated_at": updated_at,
            }
        )

    return {"items": items}


@router.put("/prompts/{task_type}")
def put_prompt(task_type: str, payload: PutPromptTemplateRequest) -> dict[str, Any]:
    """
    Update file-based prompt template:
      backend/llm/prompts/templates/<task_type>.json

    JSON file format is stable and must NOT change:
      { "task_type": "...", "version": "...", "template": "..." }
    """
    tt = str(task_type or "").strip()
    # Allow editing:
    # - known LLM tasks (TASKS), and
    # - any existing file-based templates under backend/llm/prompts/templates/*.json
    allowed_tasks = {str(t["task_type"]) for t in TASKS}
    path = _prompt_template_path(tt)
    if tt not in allowed_tasks and (not path.exists()):
        raise HTTPException(status_code=400, detail=f"unknown task_type: {tt}")

    # Only allow editing prompt section. Ignore any incoming version.
    prompt = payload.prompt if payload.prompt is not None else payload.template
    prompt = "" if prompt is None else str(prompt)
    if str(prompt).strip() == "":
        raise HTTPException(status_code=400, detail="prompt must not be empty")

    path.parent.mkdir(parents=True, exist_ok=True)

    # Load current template on disk to keep read-only sections stable.
    # If missing, fall back to PromptStore's current template.
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception:
            existing = {}
        cur_version = str(existing.get("version") or "").strip()
        cur_template = str(existing.get("template") or "")
    else:
        pt0 = get_prompt_store().get(tt)
        cur_version = str(pt0.version or "").strip()
        cur_template = str(pt0.template or "")

    out_json, in_json, _old_prompt = _split_template_sections(cur_template)
    new_template = _join_template_sections(output_json=out_json, input_json=in_json, prompt=prompt)
    new_version = _bump_version(cur_version)

    data = {"task_type": tt, "version": new_version, "template": new_template}
    try:
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"failed to write prompt template: {e}")

    # Ensure runtime router uses the latest version.
    store = get_prompt_store()
    store.invalidate(tt)
    pt = store.get(tt)
    updated_at = _iso_local(path.stat().st_mtime) if path.exists() else None
    return {
        "item": {
            "task_type": str(pt.task_type),
            "version": str(pt.version),
            "template": str(pt.template),
            "updated_at": updated_at,
        }
    }


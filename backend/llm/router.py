# 作用：LLM：统一调用入口，路由到不同模型提供方并记录调用信息。

from __future__ import annotations

import logging
import sqlite3
import json
import os
from typing import Optional

from backend.llm.call_log import log_llm_call
from backend.llm.config_store import get_llm_config_store
from backend.llm.prompts.store import get_prompt_store, render_prompt
from backend.llm.provider_factory import get_provider_factory
from backend.llm.types import LLMTaskRequest, LLMTaskResponse


log = logging.getLogger("prodwatch.llm")


def _has_table(con: sqlite3.Connection, table: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;",
        (table,),
    ).fetchone()
    return row is not None


def _auto_create_tables_enabled() -> bool:
    v = (os.environ.get("PRODWATCH_LLM_AUTO_CREATE_TABLES") or "1").strip().lower()
    return v not in {"0", "false", "no", "off"}


def _ensure_llm_call_log_table(con: sqlite3.Connection) -> None:
    """
    Best-effort: create llm_call_log to support caching/debugging without a separate migration runner.
    """
    if not _auto_create_tables_enabled():
        return
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS llm_call_log (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              task_type TEXT NOT NULL,
              provider TEXT NOT NULL,
              model TEXT,
              prompt_version TEXT,
              ok INTEGER NOT NULL,
              error_message TEXT,
              request_json TEXT,
              response_json TEXT,
              created_at DATETIME
            );
            """
        )
        con.execute("CREATE INDEX IF NOT EXISTS idx_llm_call_log_task_time ON llm_call_log(task_type, created_at);")
    except Exception:
        return


def _prompt_max_chars(task_type: str) -> Optional[int]:
    """
    Cost-first guardrail: truncate prompts to cap tokens.

    Override by env:
      - PRODWATCH_LLM_PROMPT_MAX_CHARS (global)
      - PRODWATCH_LLM_PROMPT_MAX_CHARS_<TASK_TYPE> (task specific, uppercased)

    Set to 0 / empty to disable truncation.
    """
    t = str(task_type or "").strip()
    specific = os.environ.get(f"PRODWATCH_LLM_PROMPT_MAX_CHARS_{t.upper()}")
    raw = specific if specific is not None else os.environ.get("PRODWATCH_LLM_PROMPT_MAX_CHARS")
    if raw is not None and str(raw).strip() != "":
        try:
            n = int(str(raw).strip())
            return None if n <= 0 else n
        except Exception:
            return None

    # default per task (best-effort, safe for all providers)
    if t in {"sentiment_analysis", "spam_detection"}:
        return 2000
    if t in {"keyword_extraction", "feature_extraction"}:
        return 5000
    if t in {"post_analysis"}:
        return 6500
    if t in {"crawler_generation"}:
        return 7000
    if t in {"report_generation"}:
        return 24000
    return 8000


def _truncate_prompt(task_type: str, prompt_text: str) -> str:
    n = _prompt_max_chars(task_type)
    if n is None:
        return prompt_text
    s = str(prompt_text or "")
    if len(s) <= n:
        return s
    return s[:n] + "\n...[TRUNCATED]..."


def _cache_enabled() -> bool:
    v = (os.environ.get("PRODWATCH_LLM_CACHE_ENABLED") or os.environ.get("PRODWATCH_LLM_CACHE") or "1").strip().lower()
    return v not in {"0", "false", "no", "off"}


def _lookup_cached_ok(
    con: Optional[sqlite3.Connection],
    *,
    task_type: str,
    prompt_version: str,
    request_obj: dict,
    limit: int = 50,
) -> Optional[LLMTaskResponse]:
    """
    Cache: reuse the most recent successful identical request (same prompt_version + provider/model).

    Why: saves cost when the same task is re-triggered (e.g. retries, repeated refresh runs, dedup misses).
    """
    if con is None or (not _cache_enabled()):
        return None
    if not _has_table(con, "llm_call_log"):
        return None
    try:
        req_json = json.dumps(request_obj or {}, ensure_ascii=False, default=str)
        rows = con.execute(
            """
            SELECT provider, model, prompt_version, request_json, response_json
            FROM llm_call_log
            WHERE task_type=? AND ok=1 AND prompt_version=?
            ORDER BY id DESC
            LIMIT ?;
            """,
            (str(task_type), str(prompt_version), int(limit)),
        ).fetchall()
        for r in rows:
            if (r["request_json"] or "") != req_json:
                continue
            try:
                payload = json.loads(r["response_json"] or "{}")
            except Exception:
                payload = {}
            out = payload.get("output") if isinstance(payload, dict) else None
            if not isinstance(out, dict):
                out = {}
            return LLMTaskResponse(
                ok=True,
                provider=str(r["provider"] or ""),
                model=r["model"],
                prompt_version=str(r["prompt_version"] or prompt_version),
                output=out,
                error=None,
            )
    except Exception:
        return None
    return None


class LLMRouter:
    """
    Task router: picks provider/model per task_type, with optional LLM fallback.
    """

    def __init__(self):
        self.factory = get_provider_factory()

    def run(
        self,
        *,
        task_type: str,
        input: dict,
        con: Optional[sqlite3.Connection] = None,
        enable_cache: bool = True,
        enable_log: bool = True,
        strict: bool = False,
    ) -> LLMTaskResponse:
        cache_con = con if bool(enable_cache) else None
        log_con = con if bool(enable_log) else None
        if log_con is not None:
            _ensure_llm_call_log_table(log_con)
            # Avoid holding a SQLite transaction open across slow LLM calls.
            try:
                log_con.commit()
            except Exception:
                pass
        cfg = get_llm_config_store().get(task_type, con=con)
        prompt_tpl = get_prompt_store().get(task_type)
        variables = dict(input or {})
        variables["input_json"] = json.dumps(input or {}, ensure_ascii=False, default=str)
        prompt_text = _truncate_prompt(task_type, render_prompt(prompt_tpl.template, variables))

        provider_name = str(cfg.provider or "").strip().lower()
        if strict and provider_name == "mock":
            return LLMTaskResponse(
                ok=False,
                provider=str(cfg.provider),
                model=cfg.model,
                prompt_version=prompt_tpl.version,
                output={},
                error="strict mode: mock provider is not allowed",
            )

        provider = self.factory.get(str(cfg.provider))
        if provider is None:
            return LLMTaskResponse(
                ok=False,
                provider=str(cfg.provider),
                model=cfg.model,
                prompt_version=prompt_tpl.version,
                output={},
                error=f"unknown provider: {cfg.provider}",
            )

        req = LLMTaskRequest(
            task_type=str(task_type),
            input=input or {},
            prompt_text=prompt_text,
            prompt_version=prompt_tpl.version,
            provider=str(cfg.provider),
            model=cfg.model,
        )

        request_obj = {
            "task_type": task_type,
            "input": input,
            "prompt_text": prompt_text,
            "provider": str(cfg.provider),
            "model": cfg.model,
        }
        cached = _lookup_cached_ok(cache_con, task_type=task_type, prompt_version=prompt_tpl.version, request_obj=request_obj)
        if cached is not None:
            log.info(
                "LLM task cache_hit task_type=%s provider=%s model=%s prompt_version=%s",
                task_type,
                cached.provider,
                cached.model,
                cached.prompt_version,
            )
            return cached

        res = provider.run_task(req)
        log.info(
            "LLM task run task_type=%s provider=%s model=%s prompt_version=%s ok=%s",
            task_type,
            res.provider,
            res.model,
            prompt_tpl.version,
            bool(res.ok),
        )
        log_llm_call(
            log_con,
            task_type=task_type,
            provider=res.provider,
            model=res.model,
            prompt_version=prompt_tpl.version,
            ok=bool(res.ok),
            request=request_obj,
            response={"output": res.output, "error": res.error, "provider": res.provider, "model": res.model},
            error=res.error,
        )
        if log_con is not None:
            try:
                log_con.commit()
            except Exception:
                pass
        if res.ok:
            return res
        # No fallback provider/model is supported. Fail fast and surface the error to the caller.
        return res


_router: Optional[LLMRouter] = None


def get_llm_router() -> LLMRouter:
    global _router
    if _router is None:
        _router = LLMRouter()
    return _router

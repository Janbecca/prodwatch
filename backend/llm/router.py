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
    Task router: picks provider/model per task_type, with fallback to mock.
    """

    def __init__(self):
        self.factory = get_provider_factory()

    def run(self, *, task_type: str, input: dict, con: Optional[sqlite3.Connection] = None) -> LLMTaskResponse:
        if con is not None:
            _ensure_llm_call_log_table(con)
        cfg = get_llm_config_store().get(task_type, con=con)
        prompt_tpl = get_prompt_store().get(task_type)
        variables = dict(input or {})
        variables["input_json"] = json.dumps(input or {}, ensure_ascii=False, default=str)
        prompt_text = _truncate_prompt(task_type, render_prompt(prompt_tpl.template, variables))

        provider = self.factory.get(str(cfg.provider))
        if provider is None:
            provider = self.factory.get_or_mock("mock")
            cfg = cfg.__class__(**{**cfg.__dict__, "provider": "mock"})

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
        cached = _lookup_cached_ok(con, task_type=task_type, prompt_version=prompt_tpl.version, request_obj=request_obj)
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
            con,
            task_type=task_type,
            provider=res.provider,
            model=res.model,
            prompt_version=prompt_tpl.version,
            ok=bool(res.ok),
            request=request_obj,
            response={"output": res.output, "error": res.error, "provider": res.provider, "model": res.model},
            error=res.error,
        )
        if res.ok:
            return res

        # fallback
        fb = self.factory.get_or_mock(str(cfg.fallback_provider))
        fb_req = LLMTaskRequest(
            task_type=str(task_type),
            input=input or {},
            prompt_text=prompt_text,
            prompt_version=prompt_tpl.version,
            provider=str(cfg.fallback_provider),
            model=cfg.fallback_model,
        )
        fb_request_obj = {
            "task_type": task_type,
            "input": input,
            "prompt_text": prompt_text,
            "provider": str(cfg.fallback_provider),
            "model": cfg.fallback_model,
            "fallback_from": str(cfg.provider),
        }
        fb_cached = _lookup_cached_ok(con, task_type=task_type, prompt_version=prompt_tpl.version, request_obj=fb_request_obj)
        if fb_cached is not None:
            log.info(
                "LLM task fallback_cache_hit task_type=%s provider=%s model=%s prompt_version=%s",
                task_type,
                fb_cached.provider,
                fb_cached.model,
                fb_cached.prompt_version,
            )
            return fb_cached

        fb_res = fb.run_task(fb_req)
        log.info(
            "LLM task fallback_run task_type=%s provider=%s model=%s prompt_version=%s ok=%s",
            task_type,
            fb_res.provider,
            fb_res.model,
            prompt_tpl.version,
            bool(fb_res.ok),
        )
        log_llm_call(
            con,
            task_type=task_type,
            provider=fb_res.provider,
            model=fb_res.model,
            prompt_version=prompt_tpl.version,
            ok=bool(fb_res.ok),
            request=fb_request_obj,
            response={"output": fb_res.output, "error": fb_res.error, "provider": fb_res.provider, "model": fb_res.model},
            error=fb_res.error,
        )
        if fb_res.ok:
            log.warning(
                "LLM task fallback task_type=%s provider=%s err=%s -> fallback=%s",
                task_type,
                cfg.provider,
                res.error,
                cfg.fallback_provider,
            )
            return fb_res

        # Final safety-net: always try mock so business chains can degrade gracefully even if
        # the configured fallback provider also fails.
        if str(cfg.fallback_provider).strip().lower() != "mock":
            mock = self.factory.get_or_mock("mock")
            mock_req = LLMTaskRequest(
                task_type=str(task_type),
                input=input or {},
                prompt_text=prompt_text,
                prompt_version=prompt_tpl.version,
                provider="mock",
                model="mock-v1",
            )
            mock_request_obj = {
                "task_type": task_type,
                "input": input,
                "prompt_text": prompt_text,
                "provider": "mock",
                "model": "mock-v1",
                "fallback_from": str(cfg.fallback_provider),
            }
            mock_cached = _lookup_cached_ok(
                con, task_type=task_type, prompt_version=prompt_tpl.version, request_obj=mock_request_obj
            )
            if mock_cached is not None:
                log.info(
                    "LLM task final_fallback_cache_hit task_type=%s provider=%s model=%s prompt_version=%s",
                    task_type,
                    mock_cached.provider,
                    mock_cached.model,
                    mock_cached.prompt_version,
                )
                return mock_cached

            mock_res = mock.run_task(mock_req)
            log.info(
                "LLM task final_fallback_run task_type=%s provider=%s model=%s prompt_version=%s ok=%s",
                task_type,
                mock_res.provider,
                mock_res.model,
                prompt_tpl.version,
                bool(mock_res.ok),
            )
            log_llm_call(
                con,
                task_type=task_type,
                provider=mock_res.provider,
                model=mock_res.model,
                prompt_version=prompt_tpl.version,
                ok=bool(mock_res.ok),
                request=mock_request_obj,
                response={"output": mock_res.output, "error": mock_res.error, "provider": mock_res.provider, "model": mock_res.model},
                error=mock_res.error,
            )
            if mock_res.ok:
                log.warning(
                    "LLM task final_fallback task_type=%s provider=%s err=%s fallback=%s fallback_err=%s -> mock",
                    task_type,
                    cfg.provider,
                    res.error,
                    cfg.fallback_provider,
                    fb_res.error,
                )
                return mock_res

        log.error(
            "LLM task failed task_type=%s provider=%s err=%s fallback=%s fallback_err=%s",
            task_type,
            cfg.provider,
            res.error,
            cfg.fallback_provider,
            fb_res.error,
        )
        return fb_res


_router: Optional[LLMRouter] = None


def get_llm_router() -> LLMRouter:
    global _router
    if _router is None:
        _router = LLMRouter()
    return _router

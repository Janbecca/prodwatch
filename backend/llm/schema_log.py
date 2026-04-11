"""
Persist lightweight LLM output schema for debugging in the browser.

Why:
- Frontend cannot see raw LLM responses (LLM runs on backend).
- Full raw responses can be large; we store only a structural "shape".
- Link schemas to crawl_job_id so manual refresh can surface them.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Optional

from backend.llm.types import LLMTaskResponse


def _has_table(con: sqlite3.Connection, table: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;",
        (table,),
    ).fetchone()
    return row is not None


def ensure_llm_task_schema_log_table(con: sqlite3.Connection) -> None:
    """
    Best-effort: create debug schema table.
    """
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS llm_task_schema_log (
          crawl_job_id INTEGER NOT NULL,
          task_type TEXT NOT NULL,
          provider TEXT,
          model TEXT,
          prompt_version TEXT,
          ok INTEGER NOT NULL,
          error_message TEXT,
          schema_json TEXT,
          updated_at DATETIME,
          PRIMARY KEY (crawl_job_id, task_type)
        );
        """
    )


def _type_name(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int) and not isinstance(v, bool):
        return "int"
    if isinstance(v, float):
        return "float"
    if isinstance(v, str):
        return "str"
    if isinstance(v, list):
        return "list"
    if isinstance(v, dict):
        return "dict"
    return type(v).__name__


def _schema(v: Any, *, depth: int = 3, max_keys: int = 24, max_list_samples: int = 3) -> Any:
    """
    Create a small structural summary of JSON-like values.
    """
    if depth <= 0:
        return {"type": _type_name(v)}
    if v is None or isinstance(v, (bool, int, float, str)):
        return {"type": _type_name(v)}
    if isinstance(v, list):
        sample = []
        for it in v[: max(0, int(max_list_samples))]:
            sample.append(_schema(it, depth=depth - 1, max_keys=max_keys, max_list_samples=max_list_samples))
        return {"type": "list", "len": len(v), "sample": sample}
    if isinstance(v, dict):
        keys = list(v.keys())
        keys_sorted = sorted([str(k) for k in keys])[: max(0, int(max_keys))]
        props = {}
        for k in keys_sorted:
            try:
                props[k] = _schema(v.get(k), depth=depth - 1, max_keys=max_keys, max_list_samples=max_list_samples)
            except Exception:
                props[k] = {"type": "unknown"}
        extra = max(0, len(keys) - len(keys_sorted))
        out = {"type": "dict", "keys": keys_sorted, "props": props}
        if extra:
            out["keys_truncated"] = extra
        return out
    return {"type": _type_name(v)}


def log_llm_schema(
    con: Optional[sqlite3.Connection],
    *,
    crawl_job_id: int,
    task_type: str,
    res: LLMTaskResponse,
) -> None:
    """
    Upsert a schema record for (crawl_job_id, task_type).
    """
    if con is None:
        return
    try:
        if not _has_table(con, "llm_task_schema_log"):
            ensure_llm_task_schema_log_table(con)
    except Exception:
        return

    try:
        schema_obj = _schema(res.output, depth=3)
        schema_json = json.dumps(schema_obj, ensure_ascii=False, default=str)
    except Exception:
        schema_json = json.dumps({"type": "unknown"}, ensure_ascii=False)

    try:
        con.execute(
            """
            INSERT INTO llm_task_schema_log(
              crawl_job_id, task_type, provider, model, prompt_version, ok, error_message, schema_json, updated_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'))
            ON CONFLICT(crawl_job_id, task_type) DO UPDATE SET
              provider=excluded.provider,
              model=excluded.model,
              prompt_version=excluded.prompt_version,
              ok=excluded.ok,
              error_message=excluded.error_message,
              schema_json=excluded.schema_json,
              updated_at=datetime('now','localtime');
            """,
            (
                int(crawl_job_id),
                str(task_type),
                str(res.provider or ""),
                (str(res.model) if res.model is not None else None),
                str(res.prompt_version or ""),
                1 if bool(res.ok) else 0,
                (str(res.error)[:2000] if res.error else None),
                schema_json,
            ),
        )
    except Exception:
        return


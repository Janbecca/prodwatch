from __future__ import annotations

import json
import sqlite3
from typing import Any, Optional


def _has_table(con: sqlite3.Connection, table: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;",
        (table,),
    ).fetchone()
    return row is not None


def log_llm_call(
    con: Optional[sqlite3.Connection],
    *,
    task_type: str,
    provider: str,
    model: Optional[str],
    prompt_version: str,
    ok: bool,
    request: dict[str, Any],
    response: dict[str, Any],
    error: Optional[str],
) -> None:
    """
    Best-effort logging to SQLite when llm_call_log table exists.
    """
    if con is None:
        return
    if not _has_table(con, "llm_call_log"):
        return
    try:
        con.execute(
            """
            INSERT INTO llm_call_log(
              task_type, provider, model, prompt_version,
              ok, error_message, request_json, response_json, created_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'));
            """,
            (
                str(task_type),
                str(provider),
                model,
                str(prompt_version),
                1 if ok else 0,
                (str(error)[:2000] if error else None),
                json.dumps(request or {}, ensure_ascii=False, default=str),
                json.dumps(response or {}, ensure_ascii=False, default=str),
            ),
        )
    except Exception:
        # never break business flow
        return


# 作用：后端 API：数据库/存储访问封装与依赖注入。

from __future__ import annotations

import os
import sqlite3
from typing import Iterator

from fastapi import HTTPException


DEFAULT_DB_PATH = os.environ.get("PRODWATCH_DB_PATH", "backend/database/database.sqlite")
EXPECTED_TABLES = {"project", "brand", "platform", "daily_metric", "daily_keyword_metric"}


def resolve_db_path(db_path: str) -> str:
    """
    在不同的工作目录中稳健地解析 sqlite 数据库路径。
    """

    def has_expected_schema(path: str) -> bool:
        try:
            ro = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            try:
                expected = {"project", "brand", "platform", "daily_metric", "daily_keyword_metric"}
                rows = ro.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name IN ("
                    + ",".join(["?"] * len(expected))
                    + ");",
                    tuple(sorted(expected)),
                ).fetchall()
                found = {r[0] for r in rows}
                return expected.issubset(found)
            finally:
                ro.close()
        except sqlite3.Error:
            return False

    tried: list[str] = []

    # Candidate roots: as-is and repo-root-relative (if `db_path` is relative).
    candidates: list[str] = [db_path]
    if not os.path.isabs(db_path):
        try:
            from pathlib import Path

            repo_root = str(Path(__file__).resolve().parents[2])
            candidates.append(os.path.join(repo_root, db_path))
        except Exception:
            pass

    # Expand "database.sqlite" -> also try "database..sqlite" for each candidate dir.
    expanded: list[str] = []
    for c in candidates:
        expanded.append(c)
        base = os.path.basename(c)
        folder = os.path.dirname(c) or "."
        if base == "database.sqlite":
            expanded.append(os.path.join(folder, "database..sqlite"))

    # Return the first existing candidate that matches schema expectation.
    for c in expanded:
        tried.append(c)
        if os.path.exists(c) and has_expected_schema(c):
            return c

    # If nothing matches schema, return the first existing path (still better than creating a new db),
    # else fail loudly with helpful diagnostics.
    for c in expanded:
        if os.path.exists(c):
            return c

    raise FileNotFoundError(f"{db_path} (tried: {tried})")

    # unreachable


def connect(db_path: str) -> sqlite3.Connection:
    # Increase timeout to reduce transient "database is locked" failures when another process is writing.
    # FastAPI may run sync dependencies and handlers in different worker threads. Disable sqlite's
    # same-thread guard so a per-request connection can be safely used within that request lifecycle.
    # Note: this does not eliminate the single-writer nature of SQLite. If a long refresh job is
    # holding a write transaction, other write requests may still time out; callers should handle
    # "locked/busy" gracefully (e.g., retry/backoff or return 503/409 with actionable hints).
    con = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    con.row_factory = sqlite3.Row
    try:
        con.execute("PRAGMA foreign_keys = ON;")
    except sqlite3.Error:
        pass
    try:
        con.execute("PRAGMA busy_timeout = 30000;")
    except sqlite3.Error:
        pass
    try:
        con.execute("PRAGMA journal_mode = WAL;")
    except sqlite3.Error:
        pass
    try:
        con.execute("PRAGMA synchronous = NORMAL;")
    except sqlite3.Error:
        pass
    return con


def get_db() -> Iterator[sqlite3.Connection]:
    try:
        db_path = resolve_db_path(DEFAULT_DB_PATH)
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=f"SQLite DB not found: {e}")

    try:
        con = connect(db_path)
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"SQLite connect failed: {e}")
    try:
        # Schema check with a small retry window to avoid transient writer locks.
        rows = None
        for delay in [0.0, 0.05, 0.1, 0.2]:
            try:
                if delay:
                    import time

                    time.sleep(delay)
                rows = con.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name IN ("
                    + ",".join(["?"] * len(EXPECTED_TABLES))
                    + ");",
                    tuple(sorted(EXPECTED_TABLES)),
                ).fetchall()
                break
            except sqlite3.OperationalError as e:
                msg = str(e).lower()
                if "database is locked" in msg or "database is busy" in msg:
                    continue
                raise
        if rows is None:
            raise HTTPException(status_code=503, detail=f"SQLite busy during schema check. db_path={db_path}")
        found = {r[0] for r in rows}
        missing = sorted(EXPECTED_TABLES - found)
        if missing:
            raise HTTPException(
                status_code=500,
                detail=f"SQLite schema missing tables: {missing}. db_path={db_path}",
            )
    except HTTPException:
        con.close()
        raise
    except sqlite3.OperationalError as e:
        con.close()
        msg = str(e).lower()
        if "database is locked" in msg or "database is busy" in msg:
            raise HTTPException(status_code=503, detail=f"SQLite busy: {e}. db_path={db_path}")
        raise HTTPException(status_code=500, detail=f"SQLite schema check failed: {e}. db_path={db_path}")
    except sqlite3.Error as e:
        con.close()
        raise HTTPException(status_code=500, detail=f"SQLite schema check failed: {e}. db_path={db_path}")
    try:
        yield con
    finally:
        con.close()


def get_db_relaxed() -> Iterator[sqlite3.Connection]:
    """
    Get a SQLite connection without enforcing the full business schema.

    Why: some endpoints (e.g. LLM configuration) should remain usable even when the main
    business tables haven't been migrated/seeded yet.
    """
    try:
        db_path = resolve_db_path(DEFAULT_DB_PATH)
    except FileNotFoundError:
        # Create a new DB at the default path (repo-root-relative if needed).
        if os.path.isabs(DEFAULT_DB_PATH):
            db_path = DEFAULT_DB_PATH
        else:
            try:
                from pathlib import Path

                repo_root = str(Path(__file__).resolve().parents[2])
                db_path = os.path.join(repo_root, DEFAULT_DB_PATH)
            except Exception:
                db_path = DEFAULT_DB_PATH
        folder = os.path.dirname(db_path)
        if folder:
            try:
                os.makedirs(folder, exist_ok=True)
            except Exception:
                pass

    try:
        con = connect(db_path)
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"SQLite connect failed: {e}")
    try:
        yield con
    finally:
        con.close()

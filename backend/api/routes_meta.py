# 作用：后端 API：元信息相关路由与接口实现。

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.api.db import get_db, get_db_relaxed


router = APIRouter(prefix="/api/meta", tags=["meta"])


def now_ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _table_exists(db: sqlite3.Connection, table: str) -> bool:
    try:
        row = db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;", (str(table),)
        ).fetchone()
        return row is not None
    except Exception:
        return False

# 计给定品牌 ID 列表在多个表中的引用次数，返回一个字典
# 键：品牌 ID，值：该品牌在各表中的总引用次数。
# 通过动态构建 SQL 查询来获取每个表中对应品牌 ID 的引用数量，并累加到结果中。
def _brand_ref_counts(db: sqlite3.Connection, brand_ids: list[int]) -> dict[int, int]:
    ids = [int(x) for x in brand_ids if x is not None]
    if not ids:
        return {}

    counts: dict[int, int] = {int(x): 0 for x in ids}
    placeholders = ",".join(["?"] * len(ids))

    def add_counts(table: str, sql: str, params: list[Any]):
        if not _table_exists(db, table):
            return
        try:
            for r in db.execute(sql, tuple(params)).fetchall():
                bid = int(r["brand_id"])
                if bid in counts:
                    counts[bid] += int(r["cnt"] or 0)
        except Exception:
            return

    add_counts(
        "project_brand",
        f"SELECT brand_id, COUNT(1) AS cnt FROM project_brand WHERE brand_id IN ({placeholders}) GROUP BY brand_id;",
        ids,
    )
    add_counts(
        "project",
        f"""
        SELECT our_brand_id AS brand_id, COUNT(1) AS cnt
        FROM project
        WHERE our_brand_id IS NOT NULL AND our_brand_id IN ({placeholders})
        GROUP BY our_brand_id;
        """,
        ids,
    )
    add_counts(
        "post_raw",
        f"SELECT brand_id, COUNT(1) AS cnt FROM post_raw WHERE brand_id IN ({placeholders}) GROUP BY brand_id;",
        ids,
    )
    add_counts(
        "daily_metric",
        f"SELECT brand_id, COUNT(1) AS cnt FROM daily_metric WHERE brand_id IN ({placeholders}) GROUP BY brand_id;",
        ids,
    )
    add_counts(
        "daily_keyword_metric",
        f"SELECT brand_id, COUNT(1) AS cnt FROM daily_keyword_metric WHERE brand_id IN ({placeholders}) GROUP BY brand_id;",
        ids,
    )
    add_counts(
        "crawl_job_target",
        f"SELECT brand_id, COUNT(1) AS cnt FROM crawl_job_target WHERE brand_id IN ({placeholders}) GROUP BY brand_id;",
        ids,
    )

    return counts


def _brand_ref_breakdown(db: sqlite3.Connection, brand_id: int) -> dict[str, int]:
    bid = int(brand_id)
    out: dict[str, int] = {}

    def count_if_exists(table: str, where_sql: str, params: tuple[Any, ...]) -> None:
        if not _table_exists(db, table):
            return
        try:
            row = db.execute(f"SELECT COUNT(1) AS cnt FROM {table} WHERE {where_sql};", params).fetchone()
            out[table] = int(row["cnt"] or 0) if row else 0
        except Exception:
            return

    count_if_exists("project_brand", "brand_id=?", (bid,))
    count_if_exists("project", "our_brand_id=?", (bid,))
    count_if_exists("post_raw", "brand_id=?", (bid,))
    count_if_exists("daily_metric", "brand_id=?", (bid,))
    count_if_exists("daily_keyword_metric", "brand_id=?", (bid,))
    count_if_exists("crawl_job_target", "brand_id=?", (bid,))
    return out


class BrandCreatePayload(BaseModel):
    name: str = Field(min_length=1)
    alias: str | None = None
    category: str | None = None


@router.get("/health")
def healthcheck(db: sqlite3.Connection = Depends(get_db_relaxed)) -> dict[str, Any]:
    try:
        db_row = db.execute("PRAGMA database_list;").fetchone()
        db_path = (db_row[2] if db_row and len(db_row) >= 3 else None)  # type: ignore[index]
    except Exception:
        db_path = None

    try:
        tables = [str(r[0]) for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;").fetchall()]
    except Exception:
        tables = []

    return {"ok": True, "db_path": db_path, "tables": tables}


@router.get("/brands")
def list_brands(db: sqlite3.Connection = Depends(get_db)) -> dict[str, Any]:
    try:
        rows = db.execute(
            """
            SELECT id, name, alias, category, created_at
            FROM brand
            ORDER BY id;
            """
        ).fetchall()
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"list_brands failed: {e}")

    brand_ids = [int(r["id"]) for r in rows]
    ref_counts = _brand_ref_counts(db, brand_ids)
    return {
        "brands": [
            {
                "id": int(r["id"]),
                "name": r["name"],
                "alias": r["alias"],
                "category": r["category"],
                "created_at": r["created_at"],
                "ref_count": int(ref_counts.get(int(r["id"]), 0)),
                "is_deletable": int(ref_counts.get(int(r["id"]), 0)) == 0,
            }
            for r in rows
        ]
    }


@router.post("/brands")
def create_brand(payload: BrandCreatePayload, db: sqlite3.Connection = Depends(get_db)) -> dict[str, Any]:
    name = str(payload.name or "").strip()
    if name == "":
        raise HTTPException(status_code=400, detail="name is required")

    alias = str(payload.alias).strip() if payload.alias is not None and str(payload.alias).strip() != "" else name
    category = (
        str(payload.category).strip()
        if payload.category is not None and str(payload.category).strip() != ""
        else "default"
    )
    ts = now_ts()

    try:
        db.execute("BEGIN;")
        existing = db.execute(
            "SELECT id, name, alias, category, created_at FROM brand WHERE name=? LIMIT 1;", (name,)
        ).fetchone()
        if existing is not None:
            db.commit()
            bid = int(existing["id"])
            refc = int(_brand_ref_counts(db, [bid]).get(bid, 0))
            return {
                "created": False,
                "brand": {
                    "id": bid,
                    "name": existing["name"],
                    "alias": existing["alias"],
                    "category": existing["category"],
                    "created_at": existing["created_at"],
                    "ref_count": refc,
                    "is_deletable": refc == 0,
                },
            }

        db.execute(
            "INSERT INTO brand(name, alias, category, created_at) VALUES(?, ?, ?, ?);",
            (name, alias, category, ts),
        )
        brand_id = int(db.execute("SELECT last_insert_rowid();").fetchone()[0])
        db.commit()
        return {
            "created": True,
            "brand": {
                "id": brand_id,
                "name": name,
                "alias": alias,
                "category": category,
                "created_at": ts,
                "ref_count": 0,
                "is_deletable": True,
            },
        }
    except sqlite3.IntegrityError as e:
        try:
            db.rollback()
        except Exception:
            pass
        row = db.execute(
            "SELECT id, name, alias, category, created_at FROM brand WHERE name=? LIMIT 1;", (name,)
        ).fetchone()
        if row is not None:
            bid = int(row["id"])
            refc = int(_brand_ref_counts(db, [bid]).get(bid, 0))
            return {
                "created": False,
                "brand": {
                    "id": bid,
                    "name": row["name"],
                    "alias": row["alias"],
                    "category": row["category"],
                    "created_at": row["created_at"],
                    "ref_count": refc,
                    "is_deletable": refc == 0,
                },
            }
        raise HTTPException(status_code=409, detail=f"create_brand conflict: {e}")
    except Exception as e:
        try:
            db.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"create_brand failed: {e}")


@router.delete("/brands/{brand_id}")
def delete_brand(brand_id: int, db: sqlite3.Connection = Depends(get_db)) -> dict[str, Any]:
    row = db.execute("SELECT id, name FROM brand WHERE id=? LIMIT 1;", (int(brand_id),)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="brand not found")

    breakdown = _brand_ref_breakdown(db, int(brand_id))
    total_refs = sum(int(v or 0) for v in breakdown.values())
    if total_refs > 0:
        parts = [f"{k}={int(v)}" for k, v in sorted(breakdown.items()) if int(v or 0) > 0]
        detail = "brand is referenced; cannot delete"
        if parts:
            detail = f"{detail}: " + ", ".join(parts)
        raise HTTPException(status_code=409, detail=detail)

    try:
        db.execute("BEGIN;")
        db.execute("DELETE FROM brand WHERE id=?;", (int(brand_id),))
        db.commit()
        return {"deleted": True, "brand_id": int(brand_id)}
    except sqlite3.IntegrityError as e:
        try:
            db.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=409, detail=f"delete_brand blocked: {e}")
    except Exception as e:
        try:
            db.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"delete_brand failed: {e}")


@router.get("/platforms")
def list_platforms(db: sqlite3.Connection = Depends(get_db)) -> dict[str, Any]:
    try:
        rows = db.execute(
            """
            SELECT id, code, name, is_enabled, created_at
            FROM platform
            ORDER BY id;
            """
        ).fetchall()
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"list_platforms failed: {e}")
    return {
        "platforms": [
            {
                "id": int(r["id"]),
                "code": r["code"],
                "name": r["name"],
                "is_enabled": int(r["is_enabled"] or 0),
                "created_at": r["created_at"],
            }
            for r in rows
        ]
    }


# 作用：后端 API：项目相关路由与接口实现。

from __future__ import annotations

import sqlite3
from typing import Any

from fastapi import APIRouter, Depends

from backend.api.db import get_db


router = APIRouter(prefix="/api/projects", tags=["projects"])

@router.get("/list")
def list_projects(db: sqlite3.Connection = Depends(get_db)) -> dict[str, Any]:
    rows = db.execute(
        """
        SELECT id, name, product_category, status, is_active, last_refresh_at
        FROM project
        WHERE deleted_at IS NULL
        ORDER BY id;
        """
    ).fetchall()
    return {
        "projects": [
            {
                "id": int(r["id"]),
                "name": r["name"],
                "product_category": r["product_category"],
                "status": r["status"],
                "is_active": int(r["is_active"] or 0),
                "last_refresh_at": r["last_refresh_at"],
            }
            for r in rows
        ]
    }


@router.get("/enabled")
def list_enabled_projects(db: sqlite3.Connection = Depends(get_db)) -> dict[str, Any]:
    rows = db.execute(
        """
        SELECT id, name, product_category, status, last_refresh_at
        FROM project
        WHERE is_active=1 AND deleted_at IS NULL
        ORDER BY id;
        """
    ).fetchall()
    return {
        "projects": [
            {
                "id": int(r["id"]),
                "name": r["name"],
                "product_category": r["product_category"],
                "status": r["status"],
                "last_refresh_at": r["last_refresh_at"],
            }
            for r in rows
        ]
    }

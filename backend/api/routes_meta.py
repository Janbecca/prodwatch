from __future__ import annotations

import sqlite3
from typing import Any

from fastapi import APIRouter, Depends

from backend.api.db import get_db


router = APIRouter(prefix="/api/meta", tags=["meta"])


@router.get("/brands")
def list_brands(db: sqlite3.Connection = Depends(get_db)) -> dict[str, Any]:
    rows = db.execute(
        """
        SELECT id, name, alias, category, created_at
        FROM brand
        ORDER BY id;
        """
    ).fetchall()
    return {
        "brands": [
            {
                "id": int(r["id"]),
                "name": r["name"],
                "alias": r["alias"],
                "category": r["category"],
                "created_at": r["created_at"],
            }
            for r in rows
        ]
    }


@router.get("/platforms")
def list_platforms(db: sqlite3.Connection = Depends(get_db)) -> dict[str, Any]:
    rows = db.execute(
        """
        SELECT id, code, name, is_enabled, created_at
        FROM platform
        ORDER BY id;
        """
    ).fetchall()
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


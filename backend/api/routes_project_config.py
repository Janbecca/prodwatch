from __future__ import annotations

import sqlite3
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from backend.api.db import get_db


router = APIRouter(prefix="/api/projects", tags=["projects"])


@router.get("/{project_id}/config")
def get_project_config(project_id: int, db: sqlite3.Connection = Depends(get_db)) -> dict[str, Any]:
    project = db.execute(
        """
        SELECT
          p.id,
          p.name,
          p.product_category,
          p.description,
          p.our_brand_id,
          ob.name AS our_brand_name,
          p.status,
          p.is_active,
          p.refresh_mode,
          p.refresh_cron,
          p.last_refresh_at,
          p.created_at,
          p.updated_at
        FROM project p
        LEFT JOIN brand ob ON ob.id = p.our_brand_id
        WHERE p.id=? AND p.deleted_at IS NULL
        LIMIT 1;
        """,
        (project_id,),
    ).fetchone()
    if not project:
        raise HTTPException(status_code=404, detail="project not found")

    brands = db.execute(
        """
        SELECT
          b.id,
          b.name,
          b.alias,
          b.category,
          pb.is_core_brand,
          pb.created_at
        FROM project_brand pb
        JOIN brand b ON b.id = pb.brand_id
        WHERE pb.project_id=?
        ORDER BY COALESCE(pb.is_core_brand, 0) DESC, b.id ASC;
        """,
        (project_id,),
    ).fetchall()

    platforms = db.execute(
        """
        SELECT
          p.id,
          p.code,
          p.name,
          p.is_enabled,
          pp.created_at
        FROM project_platform pp
        JOIN platform p ON p.id = pp.platform_id
        WHERE pp.project_id=?
        ORDER BY p.id ASC;
        """,
        (project_id,),
    ).fetchall()

    keywords = db.execute(
        """
        SELECT
          id,
          keyword,
          keyword_type,
          weight,
          is_enabled,
          created_at
        FROM project_keyword
        WHERE project_id=?
        ORDER BY COALESCE(weight, 0) DESC, id ASC;
        """,
        (project_id,),
    ).fetchall()

    return {
        "project": {
            "id": int(project["id"]),
            "name": project["name"],
            "product_category": project["product_category"],
            "description": project["description"],
            "our_brand_id": project["our_brand_id"],
            "our_brand_name": project["our_brand_name"],
            "is_active": int(project["is_active"] or 0),
            "status": project["status"],
            "refresh_mode": project["refresh_mode"],
            "refresh_cron": project["refresh_cron"],
            "last_refresh_at": project["last_refresh_at"],
            "created_at": project["created_at"],
            "updated_at": project["updated_at"],
        },
        "brands": [
            {
                "id": int(r["id"]),
                "name": r["name"],
                "alias": r["alias"],
                "category": r["category"],
                "is_core_brand": int(r["is_core_brand"] or 0),
                "created_at": r["created_at"],
            }
            for r in brands
        ],
        "platforms": [
            {
                "id": int(r["id"]),
                "code": r["code"],
                "name": r["name"],
                "is_enabled": int(r["is_enabled"] or 0),
                "created_at": r["created_at"],
            }
            for r in platforms
        ],
        "keywords": [
            {
                "id": int(r["id"]),
                "keyword": r["keyword"],
                "keyword_type": r["keyword_type"],
                "weight": r["weight"],
                "is_enabled": int(r["is_enabled"] or 0),
                "created_at": r["created_at"],
            }
            for r in keywords
        ],
    }


from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.api.db import get_db


router = APIRouter(prefix="/api/projects", tags=["projects"])


def now_ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


class KeywordItem(BaseModel):
    keyword: str = Field(min_length=1)
    keyword_type: Optional[str] = None
    weight: Optional[int] = None
    is_enabled: int = 1


class ProjectPayload(BaseModel):
    name: str = Field(min_length=1)
    product_category: Optional[str] = None
    description: Optional[str] = None
    our_brand_id: Optional[int] = None
    status: Optional[str] = None
    # Note: activation should be controlled via /activation endpoint.
    # Keep this field for backward compatibility with existing frontend payloads,
    # but the backend will ignore it for create/update.
    is_active: int = 0
    refresh_mode: Optional[str] = None
    refresh_cron: Optional[str] = None

    brand_ids: list[int] = Field(default_factory=list)
    platform_ids: list[int] = Field(default_factory=list)
    keywords: list[KeywordItem] = Field(default_factory=list)


class ActivationPayload(BaseModel):
    is_active: int
    status: Optional[str] = None


def validate_scope(payload: ProjectPayload) -> None:
    if not payload.name or payload.name.strip() == "":
        raise HTTPException(status_code=400, detail="name is required")
    if not payload.brand_ids:
        raise HTTPException(status_code=400, detail="brand_ids must have at least 1 item")
    if not payload.platform_ids:
        raise HTTPException(status_code=400, detail="platform_ids must have at least 1 item")
    if not payload.keywords:
        raise HTTPException(status_code=400, detail="keywords must have at least 1 item")


def _project_exists(db: sqlite3.Connection, project_id: int) -> sqlite3.Row:
    row = db.execute(
        "SELECT * FROM project WHERE id=? AND deleted_at IS NULL LIMIT 1;", (project_id,)
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="project not found")
    return row


def _normalize_mutation_status(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    v = str(value).strip().lower()
    if v == "":
        return None
    return v


def _begin(db: sqlite3.Connection) -> None:
    # Ensure the whole mutation is atomic even if an exception is raised mid-way.
    db.execute("BEGIN;")


def _rollback_quietly(db: sqlite3.Connection) -> None:
    try:
        db.rollback()
    except Exception:
        pass


@router.post("")
def create_project(payload: ProjectPayload, db: sqlite3.Connection = Depends(get_db)) -> dict[str, Any]:
    validate_scope(payload)
    ts = now_ts()

    # Backend decides default activation/state: newly created projects are inactive.
    status = "inactive"
    is_active = 0

    try:
        _begin(db)
        # name is UNIQUE in schema -> let sqlite enforce; map to 409.
        db.execute(
            """
            INSERT INTO project(
              name, product_category, description, our_brand_id,
              status, is_active, refresh_mode, refresh_cron,
              last_refresh_at, created_at, updated_at, deleted_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                payload.name.strip(),
                payload.product_category,
                payload.description,
                payload.our_brand_id,
                status,
                is_active,
                payload.refresh_mode,
                payload.refresh_cron,
                None,
                ts,
                ts,
                None,
            ),
        )
        project_id = int(db.execute("SELECT last_insert_rowid();").fetchone()[0])

        for bid in payload.brand_ids:
            db.execute(
                """
                INSERT OR IGNORE INTO project_brand(project_id, brand_id, is_core_brand, created_at)
                VALUES(?, ?, ?, ?);
                """,
                (project_id, int(bid), 0, ts),
            )
        for pid in payload.platform_ids:
            db.execute(
                """
                INSERT OR IGNORE INTO project_platform(project_id, platform_id, created_at)
                VALUES(?, ?, ?);
                """,
                (project_id, int(pid), ts),
            )
        for kw in payload.keywords:
            db.execute(
                """
                INSERT INTO project_keyword(project_id, keyword, keyword_type, weight, is_enabled, created_at)
                VALUES(?, ?, ?, ?, ?, ?);
                """,
                (project_id, kw.keyword.strip(), kw.keyword_type, kw.weight, int(kw.is_enabled or 0), ts),
            )

        db.commit()
        return {
            "id": project_id,
            "project_id": project_id,
            "name": payload.name.strip(),
            "is_active": is_active,
            "status": status,
        }
    except sqlite3.IntegrityError as e:
        _rollback_quietly(db)
        raise HTTPException(status_code=409, detail=f"create project failed: {e}")
    except HTTPException:
        _rollback_quietly(db)
        raise
    except Exception as e:
        _rollback_quietly(db)
        raise HTTPException(status_code=500, detail=f"create project failed: {e}")


@router.put("/{project_id}")
def update_project(project_id: int, payload: ProjectPayload, db: sqlite3.Connection = Depends(get_db)) -> dict[str, Any]:
    validate_scope(payload)
    project = _project_exists(db, project_id)
    if int(project["is_active"] or 0) == 1 or str(project["status"] or "") == "active":
        raise HTTPException(
            status_code=409,
            detail="project is active/read-only; deactivate via POST /api/projects/{id}/activation (is_active=0) before editing",
        )

    ts = now_ts()
    next_status = _normalize_mutation_status(payload.status)
    if next_status == "active":
        raise HTTPException(status_code=400, detail="status=active is only allowed via /activation endpoint")
    if next_status is None:
        next_status = str(project["status"] or "inactive")
    try:
        _begin(db)
        db.execute(
            """
            UPDATE project
            SET name=?,
                product_category=?,
                description=?,
                our_brand_id=?,
                status=?,
                refresh_mode=?,
                refresh_cron=?,
                updated_at=?
            WHERE id=?;
            """,
            (
                payload.name.strip(),
                payload.product_category,
                payload.description,
                payload.our_brand_id,
                next_status,
                payload.refresh_mode,
                payload.refresh_cron,
                ts,
                project_id,
            ),
        )
    except sqlite3.IntegrityError as e:
        _rollback_quietly(db)
        raise HTTPException(status_code=409, detail=str(e))
    except HTTPException:
        _rollback_quietly(db)
        raise
    except Exception as e:
        _rollback_quietly(db)
        raise HTTPException(status_code=500, detail=f"update project failed: {e}")

    # Replace scope sets (clear then insert) for clarity and idempotency.
    try:
        db.execute("DELETE FROM project_brand WHERE project_id=?;", (project_id,))
        db.execute("DELETE FROM project_platform WHERE project_id=?;", (project_id,))
        db.execute("DELETE FROM project_keyword WHERE project_id=?;", (project_id,))

        for bid in payload.brand_ids:
            db.execute(
                """
                INSERT OR IGNORE INTO project_brand(project_id, brand_id, is_core_brand, created_at)
                VALUES(?, ?, ?, ?);
                """,
                (project_id, int(bid), 0, ts),
            )
        for pid in payload.platform_ids:
            db.execute(
                """
                INSERT OR IGNORE INTO project_platform(project_id, platform_id, created_at)
                VALUES(?, ?, ?);
                """,
                (project_id, int(pid), ts),
            )
        for kw in payload.keywords:
            db.execute(
                """
                INSERT INTO project_keyword(project_id, keyword, keyword_type, weight, is_enabled, created_at)
                VALUES(?, ?, ?, ?, ?, ?);
                """,
                (project_id, kw.keyword.strip(), kw.keyword_type, kw.weight, int(kw.is_enabled or 0), ts),
            )

        db.commit()
        return {
            "id": int(project_id),
            "project_id": int(project_id),
            "name": payload.name.strip(),
            "is_active": int(project["is_active"] or 0),
            "status": next_status,
        }
    except sqlite3.IntegrityError as e:
        _rollback_quietly(db)
        raise HTTPException(status_code=409, detail=f"update project scope failed: {e}")
    except HTTPException:
        _rollback_quietly(db)
        raise
    except Exception as e:
        _rollback_quietly(db)
        raise HTTPException(status_code=500, detail=f"update project scope failed: {e}")


@router.post("/{project_id}/activation")
def set_project_activation(
    project_id: int, payload: ActivationPayload, db: sqlite3.Connection = Depends(get_db)
) -> dict[str, Any]:
    _ = _project_exists(db, project_id)
    ts = now_ts()
    try:
        _begin(db)
        status = payload.status
        if status is None:
            status = "active" if int(payload.is_active or 0) == 1 else "inactive"
        db.execute(
            "UPDATE project SET is_active=?, status=?, updated_at=? WHERE id=?;",
            (int(payload.is_active or 0), str(status), ts, project_id),
        )
        db.commit()
        return {"project_id": project_id, "is_active": int(payload.is_active or 0), "status": str(status)}
    except HTTPException:
        _rollback_quietly(db)
        raise
    except Exception as e:
        _rollback_quietly(db)
        raise HTTPException(status_code=500, detail=f"set activation failed: {e}")


@router.delete("/{project_id}")
def delete_project(project_id: int, db: sqlite3.Connection = Depends(get_db)) -> dict[str, Any]:
    _ = _project_exists(db, project_id)
    ts = now_ts()
    try:
        _begin(db)
        db.execute(
            "UPDATE project SET deleted_at=?, is_active=0, status=?, updated_at=? WHERE id=?;",
            (ts, "archived", ts, project_id),
        )
        db.commit()
        return {"project_id": project_id, "deleted": True}
    except HTTPException:
        _rollback_quietly(db)
        raise
    except Exception as e:
        _rollback_quietly(db)
        raise HTTPException(status_code=500, detail=f"delete project failed: {e}")

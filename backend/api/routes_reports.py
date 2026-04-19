# 作用：后端 API：报告相关路由与接口实现。

from __future__ import annotations

import logging
import sqlite3
import time
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from backend.api.db import get_db
from backend.api.params import DateRange, parse_date_range
from backend.services.refresh_service import get_refresh_service
from backend.services.report_generation_service import get_report_generation_service
from backend.report_chain_e import (
    fetch_evidence_key_feedback_posts,
    fetch_evidence_overview_by_brand,
    fetch_evidence_risk_keywords,
    fetch_evidence_sentiment_trend_daily_by_brand,
    fetch_evidence_topic_monitor_stacked,
)


router = APIRouter(prefix="/api/reports", tags=["reports"])
log = logging.getLogger("prodwatch.reports")

def _has_column(db: sqlite3.Connection, table: str, col: str) -> bool:
    try:
        cols = {r[1] for r in db.execute(f"PRAGMA table_info({table});").fetchall()}
        return col in cols
    except sqlite3.Error:
        return False


def _report_trigger_type(created_by: Optional[str]) -> str:
    cb = str(created_by or "").strip().lower()
    if cb in {"scheduler", "scheduled"}:
        return "scheduled"
    return "manual"


def _ensure_report_config_table(db: sqlite3.Connection) -> None:
    """
    Best-effort runtime schema alignment for `report_config`.

    This repo is often run without a dedicated migration runner, and older SQLite files may miss
    some include_* columns. We auto-add them to avoid 500s on report creation/generation.
    """
    try:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS report_config (
              report_id INTEGER PRIMARY KEY,
              platform_ids TEXT,
              brand_ids TEXT,
              keywords TEXT,
              include_sentiment INTEGER,
              include_trend INTEGER,
              include_topics INTEGER,
              include_feature_analysis INTEGER,
              include_spam INTEGER,
              include_competitor_compare INTEGER,
              include_strategy INTEGER
            );
            """
        )
    except Exception:
        return

    try:
        cols = {r[1] for r in db.execute("PRAGMA table_info(report_config);").fetchall()}
    except Exception:
        cols = set()

    # Some older DBs may have an auto-increment `id` primary key and no `report_id` column.
    # We always require a `report_id` column to join report_config -> report.
    if "report_id" not in cols:
        try:
            db.execute("ALTER TABLE report_config ADD COLUMN report_id INTEGER;")
        except Exception:
            pass
        try:
            db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_report_config_report_id ON report_config(report_id);")
        except Exception:
            pass

    desired = [
        ("platform_ids", "TEXT"),
        ("brand_ids", "TEXT"),
        ("keywords", "TEXT"),
        ("include_sentiment", "INTEGER"),
        ("include_trend", "INTEGER"),
        ("include_topics", "INTEGER"),
        ("include_feature_analysis", "INTEGER"),
        ("include_spam", "INTEGER"),
        ("include_competitor_compare", "INTEGER"),
        ("include_strategy", "INTEGER"),
    ]
    for name, typ in desired:
        if name in cols:
            continue
        try:
            db.execute(f"ALTER TABLE report_config ADD COLUMN {name} {typ};")
        except Exception:
            continue


def _table_info_safe(db: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    try:
        rows = db.execute(f"PRAGMA table_info({table});").fetchall()
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for r in rows or []:
        try:
            out.append({"name": r[1], "type": r[2]})
        except Exception:
            continue
    return out


@router.get("/status")
def report_status(
    report_id: int = Query(..., ge=1),
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    has_started_at = _has_column(db, "report", "started_at")
    has_finished_at = _has_column(db, "report", "finished_at")
    has_error_message = _has_column(db, "report", "error_message")
    started_col = "started_at" if has_started_at else "created_at"
    finished_col = "finished_at" if has_finished_at else "updated_at"
    err_col = "error_message" if has_error_message else "NULL AS error_message"
    row = db.execute(
        f"""
        SELECT
          id,
          project_id,
          status,
          {started_col} AS started_at,
          {finished_col} AS finished_at,
          {err_col},
          created_by
        FROM report
        WHERE id=?
        LIMIT 1;
        """,
        (int(report_id),),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="report not found")

    return {
        "ok": True,
        "item": {
            "id": int(row["id"]),
            "project_id": int(row["project_id"]),
            "status": row["status"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "error_message": row["error_message"],
            "trigger_type": _report_trigger_type(row["created_by"]),
        },
    }


def _parse_date_range_optional(start_date: Optional[str], end_date: Optional[str]) -> Optional[DateRange]:
    if start_date is None and end_date is None:
        return None
    if not start_date or not end_date:
        raise ValueError("start_date and end_date must be both provided")
    return parse_date_range(start_date, end_date)


@router.get("/list")
def list_reports(
    project_id: Optional[int] = Query(None),
    report_type: Optional[str] = Query(None),
    # created time range (report.created_at)
    start_date: Optional[str] = Query(None, description="Filter by report.created_at date, YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="Filter by report.created_at date, YYYY-MM-DD"),
    # data time range (report.data_start_date/data_end_date)
    data_start_date: Optional[str] = Query(None, description="Filter by report data range, YYYY-MM-DD"),
    data_end_date: Optional[str] = Query(None, description="Filter by report data range, YYYY-MM-DD"),
    search: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    try:
        dr = _parse_date_range_optional(start_date, end_date)
        ddr = _parse_date_range_optional(data_start_date, data_end_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    has_error_message = _has_column(db, "report", "error_message")
    error_select = "error_message" if has_error_message else "NULL AS error_message"

    clauses: list[str] = []
    params: list[Any] = []
    if project_id is not None:
        clauses.append("project_id=?")
        params.append(int(project_id))
    if report_type is not None and report_type.strip() != "":
        clauses.append("report_type=?")
        params.append(report_type.strip())
    if dr is not None:
        clauses.append("date(created_at) BETWEEN ? AND ?")
        params.extend([dr.start_date, dr.end_date])
    if ddr is not None:
        # overlap filter: [data_start,data_end] overlaps [ddr.start,ddr.end]
        clauses.append("NOT (data_end_date < ? OR data_start_date > ?)")
        params.extend([ddr.start_date, ddr.end_date])
    if search is not None and search.strip() != "":
        s = search.strip()
        clauses.append("(title LIKE ? OR summary LIKE ? OR content_markdown LIKE ?)")
        params.extend([f"%{s}%", f"%{s}%", f"%{s}%"])

    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    total = db.execute(f"SELECT COUNT(*) AS cnt FROM report{where};", params).fetchone()["cnt"]

    offset = (page - 1) * page_size
    rows = db.execute(
        f"""
        SELECT
          id,
          project_id,
          title,
          report_type,
          data_start_date,
          data_end_date,
          status,
          summary,
          {error_select},
          created_by,
          created_at,
          updated_at
        FROM report
        {where}
        ORDER BY created_at DESC, id DESC
        LIMIT ? OFFSET ?;
        """,
        (*params, int(page_size), int(offset)),
    ).fetchall()

    items = [
        {
            "id": int(r["id"]),
            "project_id": int(r["project_id"]),
            "title": r["title"],
            "report_type": r["report_type"],
            "data_start_date": r["data_start_date"],
            "data_end_date": r["data_end_date"],
            "status": r["status"],
            "summary": r["summary"],
            "error_message": r["error_message"],
            "created_by": r["created_by"],
            "created_at": r["created_at"],
            "updated_at": r["updated_at"],
        }
        for r in rows
    ]

    return {
        "filters": {
            "project_id": project_id,
            "report_type": report_type,
            "start_date": dr.start_date if dr else None,
            "end_date": dr.end_date if dr else None,
            "data_start_date": ddr.start_date if ddr else None,
            "data_end_date": ddr.end_date if ddr else None,
            "search": search,
        },
        "page": page,
        "page_size": page_size,
        "total": int(total or 0),
        "items": items,
    }


@router.get("/detail")
def report_detail(
    report_id: int = Query(..., ge=1),
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    has_error_message = _has_column(db, "report", "error_message")
    error_select = "error_message" if has_error_message else "NULL AS error_message"
    row = db.execute(
        """
        SELECT
          id,
          project_id,
          title,
          report_type,
          data_start_date,
          data_end_date,
          status,
          summary,
          content_markdown,
          {error_select},
          created_by,
          created_at,
          updated_at
        FROM report
        WHERE id=?
        LIMIT 1;
        """.format(error_select=error_select),
        (int(report_id),),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="report not found")

    cfg = db.execute(
        """
        SELECT
          platform_ids,
          brand_ids,
          keywords,
          include_sentiment,
          include_trend,
          include_topics,
          include_feature_analysis,
          include_spam,
          include_competitor_compare,
          include_strategy
        FROM report_config
        WHERE report_id=?
        LIMIT 1;
        """,
        (int(report_id),),
    ).fetchone()

    item = {
        "id": int(row["id"]),
        "project_id": int(row["project_id"]),
        "title": row["title"],
        "report_type": row["report_type"],
        "data_start_date": row["data_start_date"],
        "data_end_date": row["data_end_date"],
        "status": row["status"],
        "summary": row["summary"],
        "content_markdown": row["content_markdown"],
        "error_message": row["error_message"],
        "created_by": row["created_by"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "config": None,
    }
    if cfg is not None:
        item["config"] = {
            "platform_ids": cfg["platform_ids"],
            "brand_ids": cfg["brand_ids"],
            "keywords": cfg["keywords"],
            "include_sentiment": int(cfg["include_sentiment"] or 0),
            "include_trend": int(cfg["include_trend"] or 0),
            "include_topics": int(cfg["include_topics"] or 0),
            "include_feature_analysis": int(cfg["include_feature_analysis"] or 0),
            "include_spam": int(cfg["include_spam"] or 0),
            "include_competitor_compare": int(cfg["include_competitor_compare"] or 0),
            "include_strategy": int(cfg["include_strategy"] or 0),
        }

    return {"report_id": int(report_id), "item": item}


@router.delete("/delete")
def delete_report(
    report_id: int = Query(..., ge=1),
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    row = db.execute("SELECT id FROM report WHERE id=? LIMIT 1;", (int(report_id),)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="report not found")
    # Best-effort cascade (no FK cascade guaranteed).
    db.execute("DELETE FROM report_evidence WHERE report_id=?;", (int(report_id),))
    db.execute("DELETE FROM report_config WHERE report_id=?;", (int(report_id),))
    db.execute("DELETE FROM report WHERE id=?;", (int(report_id),))
    db.commit()
    return {"ok": True, "report_id": int(report_id)}


class CreateReportRequest(BaseModel):
    project_id: int = Field(..., ge=1)
    title: str = Field(..., min_length=1)
    report_type: str = Field(..., min_length=1)
    data_start_date: str = Field(..., description="YYYY-MM-DD")
    data_end_date: str = Field(..., description="YYYY-MM-DD")
    platform_ids: Optional[list[int]] = None
    brand_ids: Optional[list[int]] = None
    keywords: Optional[list[str]] = None
    include_sentiment: bool = True
    include_trend: bool = True
    include_topics: bool = True
    include_feature_analysis: bool = True
    include_spam: bool = True
    include_competitor_compare: bool = True
    include_strategy: bool = True


def _csv_ints(values: Optional[list[int]]) -> Optional[str]:
    if values is None:
        return None
    xs = []
    seen = set()
    for v in values:
        n = int(v)
        if n in seen:
            continue
        seen.add(n)
        xs.append(str(n))
    return ",".join(xs) if xs else ""


def _csv_strs(values: Optional[list[str]]) -> Optional[str]:
    if values is None:
        return None
    xs = []
    seen = set()
    for v in values:
        s = str(v).strip()
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        xs.append(s)
    return ",".join(xs) if xs else ""


@router.post("/create")
def create_report(req: CreateReportRequest, db: sqlite3.Connection = Depends(get_db)) -> dict[str, Any]:
    t0 = time.time()
    try:
        log.info(
            "report_create start project_id=%s report_type=%s data_range=%s~%s title=%s",
            int(req.project_id),
            str(req.report_type or ""),
            str(req.data_start_date or ""),
            str(req.data_end_date or ""),
            str(req.title or "")[:120],
        )
        log.info(
            "report_create filters project_id=%s platforms=%s brands=%s keywords=%s",
            int(req.project_id),
            (req.platform_ids if req.platform_ids is not None else None),
            (req.brand_ids if req.brand_ids is not None else None),
            (req.keywords if req.keywords is not None else None),
        )
        log.info(
            "report_create modules project_id=%s %s",
            int(req.project_id),
            {
                "include_sentiment": bool(req.include_sentiment),
                "include_trend": bool(req.include_trend),
                "include_topics": bool(req.include_topics),
                "include_feature_analysis": bool(req.include_feature_analysis),
                "include_spam": bool(req.include_spam),
                "include_competitor_compare": bool(req.include_competitor_compare),
                "include_strategy": bool(req.include_strategy),
            },
        )
    except Exception:
        pass
    try:
        dr = parse_date_range(req.data_start_date, req.data_end_date)
    except ValueError as e:
        log.warning("report_create invalid_date_range project_id=%s err=%s", getattr(req, "project_id", None), str(e))
        raise HTTPException(status_code=400, detail=str(e))

    # Avoid creating/generating reports while a refresh is running.
    # Reason: refresh is a long write-heavy operation; SQLite allows only one writer at a time,
    # which would cause report creation to hit "database is locked/busy" and return 503.
    #
    # Important: use RefreshService guard which also performs stale-running auto-recovery,
    # instead of blindly trusting crawl_job.status='running' forever.
    try:
        svc = get_refresh_service()
        if svc.is_running_in_memory(int(req.project_id)):
            raise HTTPException(status_code=409, detail="项目正在刷新中，请稍后再新建报告。")
        with db:
            job_id = svc.get_recent_running_job_id(db, int(req.project_id))
            if job_id is not None:
                started_at = None
                try:
                    row = db.execute("SELECT started_at FROM crawl_job WHERE id=? LIMIT 1;", (int(job_id),)).fetchone()
                    started_at = (row["started_at"] if row is not None else None)
                except Exception:
                    started_at = None
                msg = f"项目正在刷新中（任务编号={int(job_id)}"
                if started_at:
                    msg += f"，开始时间={started_at}"
                msg += "），请稍后再新建报告。"
                raise HTTPException(status_code=409, detail=msg)
    except HTTPException:
        raise
    except Exception:
        # Best-effort only: if the status check fails, continue and let normal create flow decide.
        pass

    title = req.title.strip()
    if title == "":
        raise HTTPException(status_code=400, detail="title must not be empty")

    report_type = req.report_type.strip()
    if report_type == "":
        raise HTTPException(status_code=400, detail="report_type must not be empty")

    # Backward compatible insert: older DBs may not have `error_message` yet.
    try:
        cur = db.execute(
            """
            INSERT INTO report(
              project_id,
              title,
              report_type,
              data_start_date,
              data_end_date,
              status,
              summary,
              content_markdown,
              error_message,
              created_by,
              created_at,
              updated_at
            )
            VALUES(
              ?,
              ?,
              ?,
              ?,
              ?,
              'pending',
              '',
              '',
              NULL,
              'ui',
              datetime('now','localtime'),
              datetime('now','localtime')
            );
            """,
            (int(req.project_id), title, report_type, dr.start_date, dr.end_date),
        )
    except sqlite3.IntegrityError as e:
        # Note: report_id is not available yet (this is the report row insert).
        log.warning("report_create insert_integrity_error project_id=%s err=%s", int(req.project_id), str(e))
        try:
            db.rollback()
        except Exception:
            pass
        msg = str(e).lower()
        if "foreign key constraint failed" in msg:
            raise HTTPException(status_code=400, detail="项目不存在或已被删除，无法创建报告。")
        raise HTTPException(status_code=400, detail=f"创建报告失败：{e}")
    except sqlite3.OperationalError as e:
        # Note: report_id is not available yet (this is the report row insert).
        log.exception("report_create insert_operational_error project_id=%s err=%s", int(req.project_id), str(e))
        msg = str(e).lower()
        if "database is locked" in msg or "database is busy" in msg:
            raise HTTPException(status_code=503, detail="数据库繁忙（写入被锁定），请稍后重试。")
        if ("no such column" not in msg) and ("has no column named" not in msg):
            schema = _table_info_safe(db, "report")
            raise HTTPException(status_code=500, detail={"error": f"create report failed: {e}", "report_schema": schema})
        try:
            cur = db.execute(
                """
                INSERT INTO report(
                  project_id,
                  title,
                  report_type,
                  data_start_date,
                  data_end_date,
                  status,
                  summary,
                  content_markdown,
                  created_by,
                  created_at,
                  updated_at
                )
                VALUES(
                  ?,
                  ?,
                  ?,
                  ?,
                  ?,
                  'pending',
                  '',
                  '',
                  'ui',
                  datetime('now','localtime'),
                  datetime('now','localtime')
                );
                """,
                (int(req.project_id), title, report_type, dr.start_date, dr.end_date),
            )
        except sqlite3.IntegrityError as e2:
            log.warning("report_create insert_integrity_error_legacy project_id=%s err=%s", int(req.project_id), str(e2))
            try:
                db.rollback()
            except Exception:
                pass
            msg2 = str(e2).lower()
            if "foreign key constraint failed" in msg2:
                raise HTTPException(status_code=400, detail="项目不存在或已被删除，无法创建报告。")
            raise HTTPException(status_code=400, detail=f"创建报告失败：{e2}")
        except sqlite3.OperationalError as e2:
            log.exception("report_create insert_operational_error_legacy project_id=%s err=%s", int(req.project_id), str(e2))
            msg2 = str(e2).lower()
            if "database is locked" in msg2 or "database is busy" in msg2:
                raise HTTPException(status_code=503, detail="数据库繁忙（写入被锁定），请稍后重试。")
            raise

    report_id = int(cur.lastrowid)
    log.info(
        "report_create inserted report_id=%s project_id=%s dt_ms=%s",
        report_id,
        int(req.project_id),
        int((time.time() - t0) * 1000),
    )

    try:
        _ensure_report_config_table(db)
        db.execute(
            """
            INSERT INTO report_config(
              report_id,
              platform_ids,
              brand_ids,
              keywords,
              include_sentiment,
              include_trend,
              include_topics,
              include_feature_analysis,
              include_spam,
              include_competitor_compare,
              include_strategy
            )
            VALUES(
              ?,
              ?,
              ?,
              ?,
              ?,
              ?,
              ?,
              ?,
              ?,
              ?,
              ?
            );
            """,
            (
                report_id,
                _csv_ints(req.platform_ids),
                _csv_ints(req.brand_ids),
                _csv_strs(req.keywords),
                1 if req.include_sentiment else 0,
                1 if req.include_trend else 0,
                1 if req.include_topics else 0,
                1 if req.include_feature_analysis else 0,
                1 if req.include_spam else 0,
                1 if req.include_competitor_compare else 0,
                1 if req.include_strategy else 0,
            ),
        )
        db.commit()
        log.info("report_create config_inserted report_id=%s project_id=%s", report_id, int(req.project_id))
    except sqlite3.IntegrityError as e:
        try:
            db.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=400, detail=f"创建报告配置失败：{e}")
    except sqlite3.OperationalError as e:
        try:
            db.rollback()
        except Exception:
            pass
        msg = str(e).lower()
        if "database is locked" in msg or "database is busy" in msg:
            raise HTTPException(status_code=503, detail="数据库繁忙（写入被锁定），请稍后重试。")
        schema = _table_info_safe(db, "report_config")
        raise HTTPException(status_code=500, detail={"error": f"create report failed: {e}", "report_config_schema": schema})

    # Generate immediately (minimal runnable chain). Keep create success even if generation fails,
    # but set report.status=failed and record error_message for observability.
    svc = get_report_generation_service()
    try:
        with db:
            # Evidence is derivable; clear it before generating to keep consistency.
            db.execute("DELETE FROM report_evidence WHERE report_id=?;", (int(report_id),))
            log.info("report_create generate_start report_id=%s", int(report_id))
            result = svc.generate_sync(db, int(report_id), force=True)
            log.info(
                "report_create generate_done report_id=%s status=%s dt_ms=%s",
                int(report_id),
                str((result or {}).get("status") or ""),
                int((time.time() - t0) * 1000),
            )
        return {"ok": True, "report_id": report_id, **(result or {})}
    except Exception as e:
        log.exception("report_create generate_failed report_id=%s err=%s", int(report_id), str(e))
        try:
            with db:
                svc.mark_failed(db, int(report_id), str(e))
        except Exception:
            pass
        return {"ok": True, "report_id": report_id, "status": "failed", "error_message": str(e)}


class GenerateReportRequest(BaseModel):
    """
    Generate report content (markdown + evidence) from aggregated data.

    - Uses server-side generation chain (currently mock-LLM markdown in `backend/report_chain_e.py`).
    - Intended as the minimal runnable report generation link for demo/dev.
    """

    report_id: int = Field(..., ge=1)
    force: bool = Field(default=False, description="Allow re-generating even if the report is already success")


@router.post("/generate")
def generate_report(req: GenerateReportRequest, db: sqlite3.Connection = Depends(get_db)) -> dict[str, Any]:
    row = db.execute(
        "SELECT id, status FROM report WHERE id=? LIMIT 1;",
        (int(req.report_id),),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="report not found")

    status = str(row["status"] or "")
    if status == "running":
        raise HTTPException(status_code=409, detail="report is running")
    if status in {"success", "done"} and not req.force:
        raise HTTPException(status_code=409, detail="report already success; pass force=true to re-generate")

    svc = get_report_generation_service()
    try:
        with db:
            db.execute("DELETE FROM report_evidence WHERE report_id=?;", (int(req.report_id),))
            result = svc.generate_sync(db, int(req.report_id), force=bool(req.force))
        return {"ok": True, **(result or {})}
    except Exception as e:
        try:
            with db:
                svc.mark_failed(db, int(req.report_id), str(e))
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"generate report failed: {e}")


@router.get("/evidence/list")
def list_report_evidence(
    report_id: int = Query(..., ge=1),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    total = db.execute(
        "SELECT COUNT(*) AS cnt FROM report_evidence WHERE report_id=?;",
        (int(report_id),),
    ).fetchone()["cnt"]

    offset = (page - 1) * page_size
    rows = db.execute(
        """
        SELECT
          re.id AS evidence_id,
          re.report_id,
          re.post_id,
          re.section_name,
          re.quote_reason,
          re.sentiment AS evidence_sentiment,
          re.spam_label AS evidence_spam_label,
          re.created_at AS evidence_created_at,
          pr.platform_id,
          pl.name AS platform_name,
          pr.publish_time,
          pr.title,
          pr.content
        FROM report_evidence re
        LEFT JOIN post_raw pr ON pr.id=re.post_id
        LEFT JOIN platform pl ON pl.id=pr.platform_id
        WHERE re.report_id=?
        ORDER BY re.created_at DESC, re.id DESC
        LIMIT ? OFFSET ?;
        """,
        (int(report_id), int(page_size), int(offset)),
    ).fetchall()

    items = []
    for r in rows:
        items.append(
            {
                "evidence_id": int(r["evidence_id"]),
                "report_id": int(r["report_id"]),
                "post_id": int(r["post_id"]) if r["post_id"] is not None else None,
                "section_name": r["section_name"],
                "quote_reason": r["quote_reason"],
                "sentiment": r["evidence_sentiment"],
                "spam_label": r["evidence_spam_label"],
                "created_at": r["evidence_created_at"],
                "platform_id": r["platform_id"],
                "platform_name": r["platform_name"],
                "publish_time": r["publish_time"],
                "title": r["title"],
                "content": r["content"],
            }
        )

    return {
        "report_id": int(report_id),
        "page": page,
        "page_size": page_size,
        "total": int(total or 0),
        "items": items,
    }


@router.get("/boards")
def report_boards(
    report_id: int = Query(..., ge=1),
    top_brand_n: int = Query(4, ge=1, le=20),
    top_topic_n: int = Query(15, ge=1, le=50),
    top_risk_keyword_n: int = Query(20, ge=1, le=50),
    feedback_limit: int = Query(20, ge=1, le=100),
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    """
    Report boards computed strictly from `report_evidence` dataset.

    This ensures:
    - report respects the scope selected at "create report" time;
    - all boards are derived from the report's evidence posts.
    """
    row = db.execute("SELECT id FROM report WHERE id=? LIMIT 1;", (int(report_id),)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="report not found")

    overview_items = fetch_evidence_overview_by_brand(db, int(report_id))
    trend = fetch_evidence_sentiment_trend_daily_by_brand(db, int(report_id), top_n=int(top_brand_n))
    topics = fetch_evidence_topic_monitor_stacked(db, int(report_id), top_n=int(top_topic_n))
    risk_keywords = fetch_evidence_risk_keywords(db, int(report_id), top_n=int(top_risk_keyword_n))
    feedback_posts = fetch_evidence_key_feedback_posts(db, int(report_id), limit=int(feedback_limit))

    return {
        "report_id": int(report_id),
        "overview_by_brand": {"items": overview_items},
        "sentiment_trend_daily_by_brand": {"dates": trend.get("dates") or [], "series": trend.get("series") or []},
        "topic_monitor_stacked": {"dates": topics.get("dates") or [], "series": topics.get("series") or []},
        "risk_keywords": risk_keywords,
        "key_user_feedback": {"items": feedback_posts},
    }

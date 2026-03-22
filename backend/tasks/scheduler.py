from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from zoneinfo import ZoneInfo

from backend.agents.simulator import run_simulated_crawl
from backend.storage.db import get_repo


def _safe_int(v: Any) -> Optional[int]:
    n = pd.to_numeric(v, errors="coerce")
    if pd.isna(n):
        return None
    return int(n)


def _norm_str(v: Any) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return str(v).strip()


def _parse_simple_cron(expr: str) -> Optional[Tuple[int, int]]:
    """
    Minimal cron parser for `minute hour * * *`.
    Returns (hour, minute) or None when unsupported.
    """
    parts = [p for p in str(expr or "").strip().split() if p]
    if len(parts) != 5:
        return None
    minute_s, hour_s, dom, mon, dow = parts
    if dom != "*" or mon != "*" or dow != "*":
        return None
    minute = pd.to_numeric(minute_s, errors="coerce")
    hour = pd.to_numeric(hour_s, errors="coerce")
    if pd.isna(minute) or pd.isna(hour):
        return None
    minute_i = int(minute)
    hour_i = int(hour)
    if not (0 <= minute_i <= 59 and 0 <= hour_i <= 23):
        return None
    return hour_i, minute_i


def _scheduled_dt(now: datetime, *, hour: int, minute: int, tz: ZoneInfo) -> datetime:
    base = now.astimezone(tz)
    return datetime(base.year, base.month, base.day, hour, minute, tzinfo=tz)


def _update_monitor_project_platform(repo: Any, row_id: int, updates: Dict[str, Any]) -> None:
    df = repo.query("monitor_project_platform")
    if df is None or df.empty or "id" not in df.columns:
        return
    tmp = df.copy()
    tmp["id"] = pd.to_numeric(tmp["id"], errors="coerce")
    tmp = tmp.dropna(subset=["id"])
    tmp["id"] = tmp["id"].astype(int)
    if int(row_id) not in set(tmp["id"].tolist()):
        return
    for k, v in updates.items():
        if k not in tmp.columns:
            tmp[k] = None
        tmp.loc[tmp["id"] == int(row_id), k] = v
    repo.replace("monitor_project_platform", tmp.to_dict(orient="records"))


def _iter_due_jobs(now_utc: datetime) -> List[Dict[str, Any]]:
    repo = get_repo()
    df = repo.query("monitor_project_platform")
    if df is None or df.empty:
        return []

    required = {"id", "project_id", "platform_id", "is_enabled"}
    if not required.issubset(df.columns):
        return []

    tmp = df.copy()
    for c in ["id", "project_id", "platform_id", "is_enabled", "max_posts_per_run"]:
        if c in tmp.columns:
            tmp[c] = pd.to_numeric(tmp[c], errors="coerce")
    tmp = tmp.dropna(subset=["id", "project_id", "platform_id"])
    tmp["id"] = tmp["id"].astype(int)
    tmp["project_id"] = tmp["project_id"].astype(int)
    tmp["platform_id"] = tmp["platform_id"].astype(int)
    tmp["is_enabled"] = tmp["is_enabled"].fillna(0).astype(int)

    tmp = tmp[tmp["is_enabled"] == 1]
    if "crawl_mode" in tmp.columns:
        tmp["crawl_mode"] = tmp["crawl_mode"].astype(str).str.lower()
        tmp = tmp[tmp["crawl_mode"].isin(["schedule", "scheduled", "cron"])]

    jobs: List[Dict[str, Any]] = []
    for _, r in tmp.iterrows():
        cron_expr = _norm_str(r.get("cron_expr") or "0 5 * * *")
        hm = _parse_simple_cron(cron_expr)
        if hm is None:
            # unsupported cron, skip
            continue
        hour, minute = hm
        tz_name = _norm_str(r.get("timezone") or "Asia/Shanghai") or "Asia/Shanghai"
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = ZoneInfo("Asia/Shanghai")

        now_local = now_utc.replace(tzinfo=ZoneInfo("UTC")).astimezone(tz)
        sched_today = _scheduled_dt(now_local, hour=hour, minute=minute, tz=tz)

        # last_success_at check (date-level)
        last_success = pd.to_datetime(r.get("last_success_at"), errors="coerce")
        last_success_local = last_success.tz_localize("UTC").tz_convert(tz) if pd.notna(last_success) and getattr(last_success, "tzinfo", None) is None else last_success
        ran_today = False
        if pd.notna(last_success):
            try:
                last_dt = last_success.to_pydatetime() if hasattr(last_success, "to_pydatetime") else None
                if last_dt is not None:
                    ran_today = last_dt.astimezone(tz).date() == now_local.date()
            except Exception:
                ran_today = False

        due = (now_local >= sched_today) and (not ran_today)
        if not due:
            continue

        jobs.append(
            {
                "row_id": int(r.get("id")),
                "project_id": int(r.get("project_id")),
                "platform_id": int(r.get("platform_id")),
                "sentiment_model": _norm_str(r.get("sentiment_model") or "rule-based") or "rule-based",
                "max_posts_per_run": int(pd.to_numeric(r.get("max_posts_per_run"), errors="coerce") or 0) or 20,
                "timezone": tz.key,
                "cron_expr": cron_expr,
            }
        )
    return jobs


def run_scheduler_loop(*, stop_event: threading.Event) -> None:
    """
    Simple in-process scheduler:
    - Every 60s, checks `monitor_project_platform` for due schedule jobs
    - Runs the simulator for (project_id, platform_id) once per day (per timezone+cron)
    """
    while not stop_event.is_set():
        now_utc = datetime.utcnow()
        try:
            jobs = _iter_due_jobs(now_utc)
            for job in jobs:
                row_id = int(job["row_id"])
                _update_monitor_project_platform(get_repo(), row_id, {"last_run_at": now_utc, "updated_at": now_utc})
                run_simulated_crawl(
                    project_id=int(job["project_id"]),
                    run_date=datetime.now(ZoneInfo(job.get("timezone") or "Asia/Shanghai")).date(),
                    seed=None,
                    brand_ids=None,
                    platform_ids=[int(job["platform_id"])],
                    max_posts_per_run=int(job["max_posts_per_run"]),
                    sentiment_model=str(job["sentiment_model"]),
                    trigger_type="schedule",
                    crawl_source="schedule",
                )
                _update_monitor_project_platform(get_repo(), row_id, {"last_success_at": now_utc, "updated_at": now_utc})
        except Exception:
            # best-effort: do not crash the server due to scheduler errors
            pass

        # sleep in short increments so stop_event can interrupt
        for _ in range(60):
            if stop_event.is_set():
                break
            time.sleep(1)


_scheduler_thread: Optional[threading.Thread] = None
_stop_event: Optional[threading.Event] = None


def start_scheduler() -> None:
    global _scheduler_thread, _stop_event
    if _scheduler_thread is not None and _scheduler_thread.is_alive():
        return
    _stop_event = threading.Event()
    _scheduler_thread = threading.Thread(target=run_scheduler_loop, kwargs={"stop_event": _stop_event}, daemon=True)
    _scheduler_thread.start()


def stop_scheduler() -> None:
    global _scheduler_thread, _stop_event
    if _stop_event is not None:
        _stop_event.set()
    _scheduler_thread = None
    _stop_event = None


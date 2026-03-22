from __future__ import annotations

import json
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
from zoneinfo import ZoneInfo

from backend.storage.db import get_repo
from .pipeline import process_existing_run, PipelineResult


def _now_ts_ms() -> int:
    return int(datetime.utcnow().timestamp() * 1000)


def _safe_int(v: Any) -> Optional[int]:
    try:
        n = pd.to_numeric(v, errors="coerce")
        if pd.isna(n):
            return None
        return int(n)
    except Exception:
        return None


def _norm_str(v: Any) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return str(v).strip()


def _truthy(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return bool(v)
    n = pd.to_numeric(v, errors="coerce")
    if pd.notna(n):
        return int(n) == 1
    return str(v).strip().lower() in {"true", "yes", "y", "on", "enabled", "active", "1"}


def _insert_many(repo: Any, sheet: str, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    if hasattr(repo, "insert_many"):
        return int(repo.insert_many(sheet, rows))
    for row in rows:
        repo.insert(sheet, row)
    return len(rows)


@dataclass(frozen=True)
class ProjectPlatformConfig:
    project_platform_id: int
    platform_id: int
    is_enabled: bool
    crawl_mode: str
    cron_expr: str
    timezone: str
    max_posts_per_run: int
    sentiment_model: str


def _load_project_brand_ids(repo: Any, project_id: int) -> List[int]:
    join_df = repo.query("monitor_project_brand")
    if join_df is None or join_df.empty or not {"project_id", "brand_id"}.issubset(join_df.columns):
        return []
    tmp = join_df.copy()
    tmp["project_id"] = pd.to_numeric(tmp["project_id"], errors="coerce")
    tmp["brand_id"] = pd.to_numeric(tmp["brand_id"], errors="coerce")
    tmp = tmp.dropna(subset=["project_id", "brand_id"])
    tmp["project_id"] = tmp["project_id"].astype(int)
    tmp["brand_id"] = tmp["brand_id"].astype(int)
    g = tmp[tmp["project_id"] == int(project_id)]
    if g.empty:
        return []
    return sorted({int(x) for x in g["brand_id"].tolist() if x is not None})


def _load_project_keyword_rows(repo: Any, project_id: int) -> List[Dict[str, Any]]:
    kw_df = repo.query("monitor_keyword")
    if kw_df is None or kw_df.empty or not {"id", "project_id", "keyword"}.issubset(kw_df.columns):
        return []
    tmp = kw_df.copy()
    tmp["project_id"] = pd.to_numeric(tmp["project_id"], errors="coerce")
    tmp["id"] = pd.to_numeric(tmp["id"], errors="coerce")
    tmp = tmp.dropna(subset=["project_id", "id"])
    tmp["project_id"] = tmp["project_id"].astype(int)
    tmp["id"] = tmp["id"].astype(int)
    tmp = tmp[tmp["project_id"] == int(project_id)]
    if "is_active" in tmp.columns:
        tmp["is_active"] = pd.to_numeric(tmp["is_active"], errors="coerce").fillna(0).astype(int)
        tmp = tmp[tmp["is_active"] == 1]
    out: List[Dict[str, Any]] = []
    for _, r in tmp.iterrows():
        kid = _safe_int(r.get("id"))
        kw = _norm_str(r.get("keyword"))
        if kid is not None and kw:
            out.append({"id": int(kid), "keyword": kw})
    return out


def _load_project_platforms(repo: Any, project_id: int, *, only_platform_ids: Optional[Sequence[int]] = None) -> List[ProjectPlatformConfig]:
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
    tmp = tmp[tmp["project_id"] == int(project_id)]
    if only_platform_ids:
        allow = {int(x) for x in only_platform_ids if x is not None}
        tmp = tmp[tmp["platform_id"].isin(list(allow))]

    out: List[ProjectPlatformConfig] = []
    for _, r in tmp.iterrows():
        if int(r.get("is_enabled") or 0) != 1:
            continue
        out.append(
            ProjectPlatformConfig(
                project_platform_id=int(r.get("id")),
                platform_id=int(r.get("platform_id")),
                is_enabled=True,
                crawl_mode=_norm_str(r.get("crawl_mode")) or "schedule",
                cron_expr=_norm_str(r.get("cron_expr")) or "0 5 * * *",
                timezone=_norm_str(r.get("timezone")) or "Asia/Shanghai",
                max_posts_per_run=max(1, min(int(pd.to_numeric(r.get("max_posts_per_run"), errors="coerce") or 0) or 20, 500)),
                sentiment_model=_norm_str(r.get("sentiment_model")) or "rule-based",
            )
        )
    return out


def _brand_name_map(repo: Any) -> Dict[int, str]:
    df = repo.query("brand")
    if df is None or df.empty or not {"id", "name"}.issubset(df.columns):
        return {}
    tmp = df.copy()
    tmp["id"] = pd.to_numeric(tmp["id"], errors="coerce")
    tmp = tmp.dropna(subset=["id"])
    tmp["id"] = tmp["id"].astype(int)
    return {int(r["id"]): _norm_str(r.get("name")) or f"brand_{int(r['id'])}" for _, r in tmp.iterrows()}


def _platform_name_map(repo: Any) -> Dict[int, str]:
    df = repo.query("platform")
    if df is None or df.empty or not {"id", "name"}.issubset(df.columns):
        return {}
    tmp = df.copy()
    tmp["id"] = pd.to_numeric(tmp["id"], errors="coerce")
    tmp = tmp.dropna(subset=["id"])
    tmp["id"] = tmp["id"].astype(int)
    return {int(r["id"]): _norm_str(r.get("name")) or f"platform_{int(r['id'])}" for _, r in tmp.iterrows()}


def _make_text(rng: random.Random, *, platform_name: str, brand_name: str, keyword: str, project_name: str) -> str:
    templates = [
        "【{platform}】最近看到很多人讨论 {brand}：{keyword} 体验怎么样？",
        "【{brand}】关于 {keyword} 的吐槽越来越多了，{platform} 上有人跟我一样吗？",
        "{platform} 上刷到 {brand} 的测评，说 {keyword} 很不错，是真的吗？",
        "这两天 {platform} 上 {brand} 相关内容挺多，关键词：{keyword}。",
        "【竞品对比】{brand} 在 {platform} 的口碑：{keyword} 相关讨论升温。",
        "【用户反馈】{brand} - {keyword}；来自 {platform} 的真实评论（模拟）。",
    ]
    t = templates[rng.randrange(0, len(templates))]
    return t.format(platform=platform_name, brand=brand_name, keyword=keyword, project=project_name)


def run_simulated_crawl(
    *,
    project_id: int,
    run_date: Optional[date] = None,
    seed: Optional[int] = None,
    brand_ids: Optional[List[int]] = None,
    platform_ids: Optional[List[int]] = None,
    max_posts_per_run: Optional[int] = None,
    sentiment_model: str = "rule-based",
    trigger_type: str = "manual",
    crawl_source: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Creates per-platform pipeline_run rows and inserts fake post_raw rows with explicit brand_id/platform_id/project_platform_id.
    Then runs the pipeline for each run_id.
    """
    repo = get_repo()

    pid = int(project_id)
    tz = ZoneInfo("Asia/Shanghai")
    run_date = run_date or datetime.now(tz).date()
    crawl_source = crawl_source or ("schedule" if trigger_type == "schedule" else "manual")

    # Load config
    if brand_ids is None:
        brand_ids = _load_project_brand_ids(repo, pid)
    brand_ids = [int(x) for x in (brand_ids or []) if x is not None]

    keyword_rows = _load_project_keyword_rows(repo, pid)
    if not keyword_rows:
        # best-effort: seed one keyword based on project id (avoid empty generation)
        kw_id = _now_ts_ms() * 1000
        now = datetime.utcnow()
        keyword_rows = [{"id": int(kw_id), "keyword": f"project_{pid}"}]
        _insert_many(
            repo,
            "monitor_keyword",
            [
                {
                    "id": int(kw_id),
                    "project_id": pid,
                    "keyword": f"project_{pid}",
                    "keyword_type": None,
                    "weight": None,
                    "is_active": 1,
                    "created_at": now,
                }
            ],
        )

    platforms_cfg = _load_project_platforms(repo, pid, only_platform_ids=platform_ids)
    if not platforms_cfg:
        # No config rows: fall back to "all platforms enabled"
        plat_df = repo.query("platform")
        if plat_df is not None and not plat_df.empty and "id" in plat_df.columns:
            tmp = plat_df.copy()
            tmp["id"] = pd.to_numeric(tmp["id"], errors="coerce")
            tmp = tmp.dropna(subset=["id"])
            tmp["id"] = tmp["id"].astype(int)
            platforms_cfg = [
                ProjectPlatformConfig(
                    project_platform_id=_now_ts_ms() + i + 1,
                    platform_id=int(r["id"]),
                    is_enabled=True,
                    crawl_mode="schedule",
                    cron_expr="0 5 * * *",
                    timezone="Asia/Shanghai",
                    max_posts_per_run=20,
                    sentiment_model=sentiment_model or "rule-based",
                )
                for i, (_, r) in enumerate(tmp.iterrows())
            ]

    brand_name = _brand_name_map(repo)
    platform_name = _platform_name_map(repo)

    rng = random.Random(seed if seed is not None else (pid * 10_000 + run_date.toordinal()))
    now_utc = datetime.utcnow()

    # existing platform_post_id set (avoid duplicates on rerun)
    raw_df = repo.query("post_raw")
    existing_post_keys: set[str] = set()
    if raw_df is not None and not raw_df.empty and "platform_post_id" in raw_df.columns:
        for v in raw_df["platform_post_id"].tolist():
            s = _norm_str(v)
            if s:
                existing_post_keys.add(s)

    base_run_id = _now_ts_ms() * 1000
    run_seq = 0

    summary: Dict[str, Any] = {
        "project_id": pid,
        "run_date": run_date.isoformat(),
        "trigger_type": trigger_type,
        "pipeline_runs": 0,
        "inserted_posts": 0,
        "processed_runs": 0,
        "skipped_existing_posts": 0,
        "run_ids": [],
    }

    project_df = repo.query("monitor_project", {"id": pid})
    project_name = None
    if project_df is not None and not project_df.empty and "name" in project_df.columns:
        project_name = _norm_str(project_df.iloc[0].get("name"))
    project_name = project_name or f"project_{pid}"

    for cfg in platforms_cfg:
        target_n = int(cfg.max_posts_per_run or 20)
        if max_posts_per_run is not None:
            target_n = int(max(1, min(int(max_posts_per_run), 500)))

        if target_n <= 0:
            continue

        run_id = int(base_run_id + run_seq)
        run_seq += 1

        # snapshots
        keyword_ids_snapshot = [int(r["id"]) for r in keyword_rows if r.get("id") is not None]
        params = {
            "project_id": pid,
            "project_platform_id": int(cfg.project_platform_id),
            "platform_id": int(cfg.platform_id),
            "brand_ids": brand_ids,
            "keyword_ids": keyword_ids_snapshot,
            "run_date": run_date.isoformat(),
            "trigger_type": trigger_type,
        }

        run_no = f"{trigger_type}-{run_date.strftime('%Y%m%d')}-{pid}-{cfg.platform_id}-{run_id}"
        run_row = {
            "id": run_id,
            "project_id": pid,
            "project_platform_id": int(cfg.project_platform_id),
            "platform_id": int(cfg.platform_id),
            "run_no": run_no,
            "trigger_type": trigger_type,
            "status": "running",
            "start_time": now_utc,
            "end_time": None,
            "params": json.dumps(params, ensure_ascii=False),
            "brand_ids_snapshot": json.dumps(brand_ids, ensure_ascii=False),
            "keyword_ids_snapshot": json.dumps(keyword_ids_snapshot, ensure_ascii=False),
            "max_posts_per_run": target_n,
            "sentiment_model": sentiment_model or cfg.sentiment_model or "rule-based",
            "created_at": now_utc,
        }

        # posts
        post_rows: List[Dict[str, Any]] = []
        base_post_id = _now_ts_ms() * 1000 + rng.randint(0, 10_000)
        post_seq = 0
        attempts = 0
        while len(post_rows) < target_n and attempts < target_n * 5:
            attempts += 1
            keyword_row = keyword_rows[rng.randrange(0, len(keyword_rows))]
            kid = int(keyword_row["id"])
            kw = str(keyword_row["keyword"])

            bid = brand_ids[rng.randrange(0, len(brand_ids))] if brand_ids else None
            bname = brand_name.get(int(bid), f"brand_{int(bid)}") if bid is not None else project_name

            plat_name = platform_name.get(int(cfg.platform_id), f"platform_{int(cfg.platform_id)}")
            if str(trigger_type).lower() in {"schedule", "scheduled", "cron"}:
                platform_post_id = f"sim_schedule_{run_date.strftime('%Y%m%d')}_{pid}_{cfg.platform_id}_{attempts}"
            else:
                platform_post_id = f"sim_manual_{run_id}_{pid}_{cfg.platform_id}_{attempts}"
            if platform_post_id in existing_post_keys:
                summary["skipped_existing_posts"] += 1
                continue

            # publish_time within the day (Asia/Shanghai), stored as UTC timestamp-like (naive)
            sec = rng.randint(0, 24 * 3600 - 1)
            publish_local = datetime(run_date.year, run_date.month, run_date.day, tzinfo=tz) + timedelta(seconds=sec)
            publish_time = publish_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)

            text = _make_text(rng, platform_name=plat_name, brand_name=bname, keyword=kw, project_name=project_name)

            like_count = int(max(0, rng.gauss(80, 30)))
            comment_count = int(max(0, rng.gauss(18, 8)))
            share_count = int(max(0, rng.gauss(6, 4)))

            post_rows.append(
                {
                    "id": int(base_post_id + post_seq),
                    "pipeline_run_id": run_id,
                    "project_platform_id": int(cfg.project_platform_id),
                    "project_id": pid,
                    "platform_id": int(cfg.platform_id),
                    "brand_id": int(bid) if bid is not None else None,
                    "keyword_id": int(kid),
                    "content_type": "post",
                    "platform_post_id": platform_post_id,
                    "author_id": None,
                    "publish_time": publish_time,
                    "raw_text": text,
                    "like_count": like_count,
                    "comment_count": comment_count,
                    "share_count": share_count,
                    "crawl_source": crawl_source,
                    "created_at": now_utc,
                }
            )
            existing_post_keys.add(platform_post_id)
            post_seq += 1

        if not post_rows:
            continue

        summary["pipeline_runs"] += 1
        summary["inserted_posts"] += len(post_rows)
        summary["run_ids"].append(run_id)

        _insert_many(repo, "pipeline_run", [run_row])
        _insert_many(repo, "post_raw", post_rows)

        result: Optional[PipelineResult] = process_existing_run(run_id, sentiment_model=sentiment_model or cfg.sentiment_model or "rule-based")
        if result is not None:
            summary["processed_runs"] += 1

    return summary

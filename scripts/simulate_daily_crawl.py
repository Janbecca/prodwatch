from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from zoneinfo import ZoneInfo

import pandas as pd

# Allow running as a script: ensure repo root is on sys.path (so `import backend.*` works).
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.agents.pipeline import process_existing_run
from backend.storage.db import get_repo


def _now_ts_ms() -> int:
    return int(datetime.utcnow().timestamp() * 1000)


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        n = int(v)
        return n
    except Exception:
        return None


def _norm_str(v: Any) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return str(v).strip()


def _truthy_active(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return bool(v)
    n = pd.to_numeric(v, errors="coerce")
    if pd.notna(n):
        return int(n) == 1
    s = str(v).strip().lower()
    return s in {"true", "yes", "y", "on", "enabled", "active"}


def _insert_many(repo: Any, sheet: str, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    if hasattr(repo, "insert_many"):
        return int(repo.insert_many(sheet, rows))
    for row in rows:
        repo.insert(sheet, row)
    return len(rows)


def _parse_run_date(value: Optional[str]) -> date:
    if value:
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except Exception as e:
            raise SystemExit(f"Invalid --date: {value} (expected YYYY-MM-DD): {e}") from e
    return datetime.now(ZoneInfo("Asia/Shanghai")).date()


def _freq_to_posts_per_run(freq: Any, *, default: int = 40) -> int:
    """
    datasource_config.freq -> number of posts per (project, platform) run.
    Supported:
    - int/float/string number: use as posts/day
    - strings: high/low/daily/2h/4h/1h
    """
    if freq is None or (isinstance(freq, float) and pd.isna(freq)):
        return default

    if isinstance(freq, (int, float)) and not pd.isna(freq):
        return int(max(5, min(int(freq), 300)))

    s = str(freq).strip().lower()
    if not s or s in {"off", "disabled", "0", "false", "none", "null"}:
        return 0

    num = pd.to_numeric(s, errors="coerce")
    if pd.notna(num):
        return int(max(5, min(int(num), 300)))

    if "high" in s or "1h" in s or "hourly" in s:
        return 120
    if "2h" in s:
        return 80
    if "4h" in s:
        return 50
    if "low" in s:
        return 20
    if "daily" in s or "day" in s:
        return 30
    return default


@dataclass(frozen=True)
class ProjectConfig:
    project_id: int
    project_name: str
    brand_names: List[str]
    keyword_rows: List[Tuple[int, str]]  # (keyword_id, keyword)


@dataclass(frozen=True)
class PlatformConfig:
    platform_id: int
    platform_code: str
    platform_name: str
    freq: Any


def _load_platforms(repo: Any) -> List[PlatformConfig]:
    plat_df = repo.query("platform")
    if plat_df is None or plat_df.empty:
        return []

    cfg_df = repo.query("datasource_config")
    cfg_map: Dict[str, Any] = {}
    if cfg_df is not None and not cfg_df.empty and {"id", "freq"}.issubset(cfg_df.columns):
        for _, r in cfg_df.iterrows():
            cfg_map[_norm_str(r.get("id"))] = r.get("freq")

    out: List[PlatformConfig] = []
    for _, r in plat_df.iterrows():
        pid = _safe_int(r.get("id"))
        if pid is None:
            continue
        code = _norm_str(r.get("code"))
        name = _norm_str(r.get("name")) or f"platform_{pid}"
        freq = cfg_map.get(code)
        out.append(PlatformConfig(platform_id=pid, platform_code=code or str(pid), platform_name=name, freq=freq))
    return out


def _load_projects(repo: Any) -> List[ProjectConfig]:
    proj_df = repo.query("monitor_project")
    if proj_df is None or proj_df.empty:
        return []

    brand_df = repo.query("brand")
    brand_name: Dict[int, str] = {}
    if brand_df is not None and not brand_df.empty and {"id", "name"}.issubset(brand_df.columns):
        tmp = brand_df.copy()
        tmp["id"] = pd.to_numeric(tmp["id"], errors="coerce")
        tmp = tmp.dropna(subset=["id"])
        tmp["id"] = tmp["id"].astype(int)
        for _, r in tmp.iterrows():
            brand_name[int(r["id"])] = _norm_str(r.get("name")) or f"brand_{int(r['id'])}"

    join_df = repo.query("monitor_project_brand")
    brands_by_project: Dict[int, List[int]] = {}
    if join_df is not None and not join_df.empty and {"project_id", "brand_id"}.issubset(join_df.columns):
        j = join_df.copy()
        j["project_id"] = pd.to_numeric(j["project_id"], errors="coerce")
        j["brand_id"] = pd.to_numeric(j["brand_id"], errors="coerce")
        j = j.dropna(subset=["project_id", "brand_id"])
        j["project_id"] = j["project_id"].astype(int)
        j["brand_id"] = j["brand_id"].astype(int)
        for pid, g in j.groupby("project_id"):
            brands_by_project[int(pid)] = sorted({int(x) for x in g["brand_id"].tolist() if x is not None})

    kw_df = repo.query("monitor_keyword")

    out: List[ProjectConfig] = []
    for _, r in proj_df.iterrows():
        pid = _safe_int(r.get("id"))
        if pid is None:
            continue
        if "is_active" in proj_df.columns and not _truthy_active(r.get("is_active")):
            continue

        pname = _norm_str(r.get("name")) or f"project_{pid}"
        brand_ids = brands_by_project.get(pid)
        if not brand_ids:
            bid = _safe_int(r.get("brand_id"))
            brand_ids = [bid] if bid is not None else []
        brand_names = [brand_name.get(int(b), f"brand_{int(b)}") for b in brand_ids if b is not None]
        brand_names = [n for n in brand_names if n]

        keyword_rows: List[Tuple[int, str]] = []
        if kw_df is not None and not kw_df.empty and {"id", "project_id", "keyword"}.issubset(kw_df.columns):
            tmp = kw_df.copy()
            tmp["project_id"] = pd.to_numeric(tmp["project_id"], errors="coerce")
            tmp["id"] = pd.to_numeric(tmp["id"], errors="coerce")
            tmp = tmp.dropna(subset=["project_id", "id"])
            tmp["project_id"] = tmp["project_id"].astype(int)
            tmp["id"] = tmp["id"].astype(int)
            tmp = tmp[tmp["project_id"] == int(pid)]
            if "is_active" in tmp.columns:
                tmp["is_active"] = pd.to_numeric(tmp["is_active"], errors="coerce").fillna(0).astype(int)
                tmp = tmp[tmp["is_active"] == 1]
            for _, krow in tmp.iterrows():
                kid = _safe_int(krow.get("id"))
                kw = _norm_str(krow.get("keyword"))
                if kid is not None and kw:
                    keyword_rows.append((int(kid), kw))

        out.append(ProjectConfig(project_id=int(pid), project_name=pname, brand_names=brand_names, keyword_rows=keyword_rows))
    return out


def _ensure_project_keywords(repo: Any, projects: List[ProjectConfig]) -> List[ProjectConfig]:
    """
    Ensures each project has at least one keyword row.
    If missing, seed a keyword using brand/project name and write into `monitor_keyword`.
    """
    missing: List[ProjectConfig] = [p for p in projects if not p.keyword_rows]
    if not missing:
        return projects

    now = datetime.utcnow()
    base_id = _now_ts_ms() * 1000
    rows: List[Dict[str, Any]] = []
    seq = 0
    seeded_ids: Dict[int, List[Tuple[int, str]]] = {}
    for p in missing:
        seed_kw = (p.brand_names[0] if p.brand_names else p.project_name).strip()
        if not seed_kw:
            seed_kw = f"project_{p.project_id}"
        kid = base_id + seq
        seq += 1
        rows.append(
            {
                "id": kid,
                "project_id": p.project_id,
                "keyword": seed_kw,
                "keyword_type": None,
                "weight": None,
                "is_active": 1,
                "created_at": now,
            }
        )
        seeded_ids[p.project_id] = [(int(kid), seed_kw)]

    _insert_many(repo, "monitor_keyword", rows)

    out: List[ProjectConfig] = []
    for p in projects:
        if p.project_id in seeded_ids:
            out.append(ProjectConfig(p.project_id, p.project_name, p.brand_names, seeded_ids[p.project_id]))
        else:
            out.append(p)
    return out


def _pick_keyword(rng: random.Random, keywords: Sequence[Tuple[int, str]]) -> Tuple[int, str]:
    return keywords[rng.randrange(0, len(keywords))]


def _make_text(
    rng: random.Random,
    *,
    platform_name: str,
    brand_names: Sequence[str],
    keyword: str,
    project_name: str,
) -> str:
    brand = brand_names[rng.randrange(0, len(brand_names))] if brand_names else project_name

    # Distribution tuned to drive existing rule-based pipelines:
    # - Sentiment word lists in backend/agents/analyzer.py
    # - Spam rules in backend/agents/pipeline.py
    p = rng.random()
    if p < 0.12:
        # spam / promo
        suffix = "！" * rng.randint(3, 8) + ("？" * rng.randint(0, 3))
        link = "https://example.com/promo" if rng.random() < 0.7 else "http://example.com/deal"
        return f"{brand} {keyword} 返现 免费 福利 加微信VX领取 {link}{suffix}"
    if p < 0.37:
        # negative
        bad = rng.choice(["差", "糟", "失望", "垃圾", "故障", "卡顿", "发烫", "退货", "售后差", "不行"])
        return f"在{platform_name}刷到{brand}相关内容：{keyword}真的{bad}，体验一般，建议谨慎。"
    if p < 0.62:
        # positive
        good = rng.choice(["好", "满意", "推荐", "清晰", "稳定", "方便", "实用", "性价比", "喜欢"])
        return f"{brand}这次{keyword}我觉得挺{good}的，在{platform_name}看到不少用户反馈也还不错。"
    # neutral
    return f"{platform_name}今日舆情：围绕{brand}的{keyword}讨论较多，主要集中在功能体验与价格对比。"


def _make_counts(rng: random.Random, *, is_spam: bool) -> Tuple[int, int, int]:
    base_like = rng.randint(0, 80) + (rng.randint(0, 300) if rng.random() < 0.08 else 0)
    base_comment = rng.randint(0, 20) + (rng.randint(0, 80) if rng.random() < 0.06 else 0)
    base_share = rng.randint(0, 10) + (rng.randint(0, 50) if rng.random() < 0.04 else 0)
    if is_spam and rng.random() < 0.3:
        base_like = int(base_like * 1.6)
    return int(base_like), int(base_comment), int(base_share)


def simulate_daily_crawl(
    *,
    run_date: date,
    seed: Optional[int] = None,
    sentiment_model: str = "rule-based",
    max_posts_per_run: Optional[int] = None,
    only_project_ids: Optional[Sequence[int]] = None,
    only_platform_codes: Optional[Sequence[str]] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    repo = get_repo()

    platforms = _load_platforms(repo)
    projects = _load_projects(repo)
    projects = _ensure_project_keywords(repo, projects)

    if only_project_ids:
        allow = {int(x) for x in only_project_ids}
        projects = [p for p in projects if p.project_id in allow]
    if only_platform_codes:
        allow_codes = {str(x).strip().lower() for x in only_platform_codes}
        platforms = [p for p in platforms if p.platform_code.strip().lower() in allow_codes]

    # If datasource_config is present, treat empty/disabled freq as off.
    enabled_platforms: List[PlatformConfig] = []
    for p in platforms:
        n = _freq_to_posts_per_run(p.freq, default=40)
        if n > 0:
            enabled_platforms.append(p)

    date_key = run_date.strftime("%Y%m%d")
    rng = random.Random(seed if seed is not None else int(date_key))

    summary = {
        "run_date": run_date.isoformat(),
        "projects": len(projects),
        "platforms": len(enabled_platforms),
        "pipeline_runs": 0,
        "inserted_posts": 0,
        "skipped_existing_posts": 0,
        "processed_runs": 0,
        "dry_run": dry_run,
    }

    if not projects:
        return summary
    if not enabled_platforms:
        return summary

    # Preload per (project, platform) existing platform_post_id to make reruns idempotent.
    existing_keys: Dict[Tuple[int, int], set[str]] = {}
    for proj in projects:
        for plat in enabled_platforms:
            df = repo.query("post_raw", {"project_id": int(proj.project_id), "platform_id": int(plat.platform_id)})
            ids = set()
            if df is not None and not df.empty and "platform_post_id" in df.columns:
                for v in df["platform_post_id"].tolist():
                    s = _norm_str(v)
                    if s:
                        ids.add(s)
            existing_keys[(proj.project_id, plat.platform_id)] = ids

    now = datetime.utcnow()
    base_run_id = _now_ts_ms() * 1000
    run_seq = 0

    for proj in projects:
        for plat in enabled_platforms:
            target_n = _freq_to_posts_per_run(plat.freq, default=40)
            if max_posts_per_run is not None:
                target_n = int(max(0, min(int(max_posts_per_run), 500)))
            if target_n <= 0:
                continue

            existing = existing_keys.get((proj.project_id, plat.platform_id), set())

            # Build rows deterministically so reruns can skip by platform_post_id.
            post_rows: List[Dict[str, Any]] = []
            base_post_id = _now_ts_ms() * 1000 + rng.randint(0, 10_000)
            post_seq = 0
            attempts = 0
            while len(post_rows) < target_n and attempts < target_n * 3:
                attempts += 1
                idx = attempts
                platform_post_id = f"sim_{date_key}_{proj.project_id}_{plat.platform_code}_{idx}"
                if platform_post_id in existing:
                    summary["skipped_existing_posts"] += 1
                    continue

                keyword_id, keyword = _pick_keyword(rng, proj.keyword_rows)
                text = _make_text(
                    rng,
                    platform_name=plat.platform_name or plat.platform_code,
                    brand_names=proj.brand_names,
                    keyword=keyword,
                    project_name=proj.project_name,
                )
                is_spam = any(k in text for k in ["加微信", "加V", "VX", "福利", "返现", "免费", "http://", "https://"])
                like_count, comment_count, share_count = _make_counts(rng, is_spam=is_spam)

                publish_time = now - timedelta(seconds=rng.randint(0, 24 * 3600 - 1))

                post_rows.append(
                    {
                        "id": base_post_id + post_seq,
                        "pipeline_run_id": None,  # fill after run_id decided
                        "project_id": proj.project_id,
                        "platform_id": plat.platform_id,
                        "keyword_id": int(keyword_id),
                        "content_type": "post",
                        "platform_post_id": platform_post_id,
                        "author_id": None,
                        "publish_time": publish_time,
                        "raw_text": text,
                        "like_count": like_count,
                        "comment_count": comment_count,
                        "share_count": share_count,
                    }
                )
                post_seq += 1
                existing.add(platform_post_id)

            if not post_rows:
                continue

            run_id = base_run_id + run_seq
            run_seq += 1
            for row in post_rows:
                row["pipeline_run_id"] = int(run_id)

            run_no = f"scheduled-{date_key}-{proj.project_id}-{plat.platform_code}-{run_id}"
            run_row = {
                "id": int(run_id),
                "project_id": int(proj.project_id),
                "run_no": run_no,
                "trigger_type": "scheduled_simulation",
                "status": "running",
                "start_time": now,
                "end_time": None,
                "params": None,
                "created_at": now,
            }

            summary["pipeline_runs"] += 1
            summary["inserted_posts"] += len(post_rows)

            if dry_run:
                continue

            _insert_many(repo, "pipeline_run", [run_row])
            _insert_many(repo, "post_raw", post_rows)

            result = process_existing_run(int(run_id), sentiment_model=sentiment_model)
            if result is not None:
                summary["processed_runs"] += 1

    return summary


def _parse_csv_ints(value: Optional[str]) -> Optional[List[int]]:
    if not value:
        return None
    parts = [p.strip() for p in str(value).split(",") if p.strip()]
    out: List[int] = []
    for p in parts:
        n = _safe_int(p)
        if n is not None:
            out.append(int(n))
    return out or None


def _parse_csv_str(value: Optional[str]) -> Optional[List[str]]:
    if not value:
        return None
    parts = [p.strip() for p in str(value).split(",") if p.strip()]
    return parts or None


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Simulate a daily crawler run and write fake sentiment posts into the database.")
    parser.add_argument("--date", default=None, help="Run date in Asia/Shanghai (YYYY-MM-DD). Default: today in Asia/Shanghai.")
    parser.add_argument("--seed", default=None, type=int, help="Random seed override.")
    parser.add_argument("--sentiment-model", default="rule-based", help="Sentiment model passed to pipeline (default: rule-based).")
    parser.add_argument("--max-posts-per-run", default=None, type=int, help="Override posts per (project, platform) run.")
    parser.add_argument("--projects", default=None, help="Limit to project ids, comma separated (e.g. 1,2).")
    parser.add_argument("--platforms", default=None, help="Limit to platform codes, comma separated (e.g. weibo,xhs).")
    parser.add_argument("--dry-run", action="store_true", help="Plan only, do not write data.")
    args = parser.parse_args(list(argv) if argv is not None else None)

    run_date = _parse_run_date(args.date)
    summary = simulate_daily_crawl(
        run_date=run_date,
        seed=args.seed,
        sentiment_model=str(args.sentiment_model),
        max_posts_per_run=args.max_posts_per_run,
        only_project_ids=_parse_csv_ints(args.projects),
        only_platform_codes=_parse_csv_str(args.platforms),
        dry_run=bool(args.dry_run),
    )
    print(pd.Series(summary).to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

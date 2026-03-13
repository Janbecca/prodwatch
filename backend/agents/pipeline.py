from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, date
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from backend.storage.db import get_repo
from .crawler import crawl
from .analyzer import analyze_sentiment


def _now_ts_ms() -> int:
    return int(datetime.utcnow().timestamp() * 1000)


def _normalize_str(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def _safe_int(value: Any) -> Optional[int]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _safe_date(value: Any) -> Optional[date]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        dt = pd.to_datetime(value, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.date()
    except Exception:
        return None


def _sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass
class PipelineResult:
    pipeline_run_id: int
    imported_posts: int = 0
    crawled_posts: int = 0
    cleaned_posts: int = 0
    spam_scored: int = 0
    sentiment_scored: int = 0
    daily_metrics: int = 0
    report_id: Optional[int] = None


def clean_posts_for_run(pipeline_run_id: int) -> int:
    repo = get_repo()
    raw_df = repo.query("post_raw", {"pipeline_run_id": pipeline_run_id})
    if raw_df.empty:
        return 0

    existing = repo.query("post_clean", {"pipeline_run_id": pipeline_run_id})
    existing_by_raw_id = set()
    if not existing.empty and "post_raw_id" in existing.columns:
        existing_by_raw_id = set(pd.to_numeric(existing["post_raw_id"], errors="coerce").dropna().astype(int).tolist())

    rows: List[Dict[str, Any]] = []
    for _, r in raw_df.iterrows():
        raw_id = _safe_int(r.get("id"))
        if raw_id is None or raw_id in existing_by_raw_id:
            continue

        raw_text = _normalize_str(r.get("raw_text"))
        clean_text = raw_text
        is_valid = 1
        invalid_reason = None
        if not clean_text:
            is_valid = 0
            invalid_reason = "empty"
        elif len(clean_text) < 2:
            is_valid = 0
            invalid_reason = "too_short"

        rows.append(
            {
                "id": _now_ts_ms(),
                "post_raw_id": raw_id,
                "pipeline_run_id": pipeline_run_id,
                "project_id": _safe_int(r.get("project_id")),
                "clean_text": clean_text,
                "text_hash": _sha256_hex(clean_text) if clean_text else None,
                "is_valid": is_valid,
                "invalid_reason": invalid_reason,
            }
        )

    if not rows:
        return 0

    # append via per-row insert to keep existing rows
    for row in rows:
        repo.insert("post_clean", row)
    return len(rows)


def score_spam_for_run(pipeline_run_id: int) -> int:
    repo = get_repo()
    clean_df = repo.query("post_clean", {"pipeline_run_id": pipeline_run_id})
    if clean_df.empty:
        return 0

    existing = repo.query("spam_score")
    existing_clean_ids = set()
    if not existing.empty and "post_clean_id" in existing.columns:
        existing_clean_ids = set(pd.to_numeric(existing["post_clean_id"], errors="coerce").dropna().astype(int).tolist())

    rows: List[Dict[str, Any]] = []
    for _, r in clean_df.iterrows():
        clean_id = _safe_int(r.get("id"))
        if clean_id is None or clean_id in existing_clean_ids:
            continue
        if int(pd.to_numeric(r.get("is_valid"), errors="coerce") or 0) != 1:
            continue

        text = _normalize_str(r.get("clean_text"))
        hits: Dict[str, Any] = {}
        score = 0.0

        if len(text) >= 120:
            hits["too_long"] = True
            score += 0.15
        if len(set(text)) <= max(1, len(text) // 6):
            hits["low_char_diversity"] = True
            score += 0.25
        if any(k in text for k in ["加微信", "VX", "福利", "返现", "免费", "http://", "https://"]):
            hits["promo_or_link"] = True
            score += 0.35
        if text.count("！") + text.count("!") >= 5:
            hits["excessive_exclaim"] = True
            score += 0.15

        label = "normal"
        if score >= 0.6:
            label = "spam"
        elif score >= 0.3:
            label = "suspect"

        rows.append(
            {
                "id": _now_ts_ms(),
                "post_clean_id": clean_id,
                "project_id": _safe_int(r.get("project_id")),
                "score_total": round(float(score), 4),
                "label": label,
                "rule_hits": json.dumps(hits, ensure_ascii=False),
            }
        )

    for row in rows:
        repo.insert("spam_score", row)
    return len(rows)


def aggregate_daily_metrics_for_run(pipeline_run_id: int) -> int:
    repo = get_repo()
    raw_df = repo.query("post_raw", {"pipeline_run_id": pipeline_run_id})
    if raw_df.empty:
        return 0

    clean_df = repo.query("post_clean", {"pipeline_run_id": pipeline_run_id})
    spam_df = repo.query("spam_score")
    sent_df = repo.query("sentiment_result")

    # maps for joins
    valid_clean_by_raw: Dict[int, int] = {}
    if not clean_df.empty and {"post_raw_id", "id", "is_valid"}.issubset(clean_df.columns):
        for _, r in clean_df.iterrows():
            raw_id = _safe_int(r.get("post_raw_id"))
            clean_id = _safe_int(r.get("id"))
            is_valid = int(pd.to_numeric(r.get("is_valid"), errors="coerce") or 0)
            if raw_id is not None and clean_id is not None and is_valid == 1:
                valid_clean_by_raw[raw_id] = clean_id

    spam_by_clean: Dict[int, str] = {}
    if not spam_df.empty and {"post_clean_id", "label"}.issubset(spam_df.columns):
        for _, r in spam_df.iterrows():
            cid = _safe_int(r.get("post_clean_id"))
            if cid is not None:
                spam_by_clean[cid] = _normalize_str(r.get("label")).lower() or "normal"

    sentiment_by_clean: Dict[int, str] = {}
    if not sent_df.empty and {"post_clean_id", "polarity"}.issubset(sent_df.columns):
        for _, r in sent_df.iterrows():
            cid = _safe_int(r.get("post_clean_id"))
            if cid is not None:
                sentiment_by_clean[cid] = _normalize_str(r.get("polarity")).lower()

    buckets: Dict[Tuple[int, int, date], Dict[str, int]] = {}
    for _, r in raw_df.iterrows():
        project_id = _safe_int(r.get("project_id")) or 0
        platform_id = _safe_int(r.get("platform_id")) or 0
        d = _safe_date(r.get("publish_time")) or datetime.utcnow().date()
        key = (project_id, platform_id, d)
        if key not in buckets:
            buckets[key] = {
                "total_posts": 0,
                "valid_posts": 0,
                "spam_posts": 0,
                "pos_posts": 0,
                "neu_posts": 0,
                "neg_posts": 0,
            }

        buckets[key]["total_posts"] += 1

        raw_id = _safe_int(r.get("id"))
        clean_id = valid_clean_by_raw.get(raw_id) if raw_id is not None else None
        if clean_id is None:
            continue
        buckets[key]["valid_posts"] += 1

        spam_label = spam_by_clean.get(clean_id, "normal")
        if spam_label in {"spam", "suspect"}:
            buckets[key]["spam_posts"] += 1

        pol = sentiment_by_clean.get(clean_id, "neutral")
        if pol == "positive":
            buckets[key]["pos_posts"] += 1
        elif pol == "negative":
            buckets[key]["neg_posts"] += 1
        else:
            buckets[key]["neu_posts"] += 1

    rows: List[Dict[str, Any]] = []
    for (project_id, platform_id, d), m in buckets.items():
        rows.append(
            {
                "id": _now_ts_ms(),
                "project_id": project_id,
                "platform_id": platform_id,
                "metric_date": datetime(d.year, d.month, d.day),
                **m,
            }
        )
    for row in rows:
        repo.insert("daily_metric", row)
    return len(rows)


def generate_report_for_run(pipeline_run_id: int) -> Optional[int]:
    repo = get_repo()
    run_df = repo.query("pipeline_run", {"id": pipeline_run_id})
    project_id = None
    if not run_df.empty and "project_id" in run_df.columns:
        project_id = _safe_int(run_df.iloc[0].get("project_id"))

    metrics_df = repo.query("daily_metric")
    if metrics_df.empty:
        summary = "本次运行暂无可聚合指标数据。"
    else:
        # best-effort: summarize latest date within this run by matching project/platform
        metrics_df["metric_date"] = pd.to_datetime(metrics_df["metric_date"], errors="coerce")
        latest = metrics_df.sort_values("metric_date", ascending=False).head(50)
        total = int(pd.to_numeric(latest.get("total_posts"), errors="coerce").fillna(0).sum())
        neg = int(pd.to_numeric(latest.get("neg_posts"), errors="coerce").fillna(0).sum())
        spam = int(pd.to_numeric(latest.get("spam_posts"), errors="coerce").fillna(0).sum())
        summary = f"本次监控共采集/导入 {total} 条内容，其中负面 {neg} 条，疑似水军 {spam} 条。"

    now = datetime.utcnow()
    report_id = _now_ts_ms()
    repo.insert(
        "report",
        {
            "id": report_id,
            "pipeline_run_id": pipeline_run_id,
            "project_id": project_id,
            "title": "竞品舆情日报",
            "report_type": "daily",
            "summary": summary,
            "created_at": now,
        },
    )
    return report_id


def process_existing_run(pipeline_run_id: int, *, sentiment_model: str = "rule-based") -> PipelineResult:
    repo = get_repo()
    result = PipelineResult(pipeline_run_id=pipeline_run_id)

    # ensure pipeline_run exists
    run_df = repo.query("pipeline_run", {"id": pipeline_run_id})
    if run_df.empty:
        repo.insert(
            "pipeline_run",
            {
                "id": pipeline_run_id,
                "project_id": None,
                "run_no": datetime.utcnow().strftime("%Y%m%d%H%M%S"),
                "trigger_type": "post_process",
                "status": "running",
                "start_time": datetime.utcnow(),
                "end_time": None,
                "params": None,
                "created_at": datetime.utcnow(),
            },
        )

    cleaned = clean_posts_for_run(pipeline_run_id)
    result.cleaned_posts = cleaned

    spam_scored = score_spam_for_run(pipeline_run_id)
    result.spam_scored = spam_scored

    clean_df = repo.query("post_clean", {"pipeline_run_id": pipeline_run_id})
    sentiment_scored = analyze_sentiment(clean_df, model=sentiment_model)
    result.sentiment_scored = sentiment_scored

    metrics = aggregate_daily_metrics_for_run(pipeline_run_id)
    result.daily_metrics = metrics

    report_id = generate_report_for_run(pipeline_run_id)
    result.report_id = report_id

    if hasattr(repo, "update_by_id"):
        repo.update_by_id(
            "pipeline_run",
            pipeline_run_id,
            {"status": "finished", "end_time": datetime.utcnow()},
        )
    return result


def run_manual_pipeline(
    *,
    project_id: int,
    platform_codes: List[str],
    keyword: str,
    sentiment_model: str = "rule-based",
) -> PipelineResult:
    repo = get_repo()
    run_id = _now_ts_ms()
    now = datetime.utcnow()
    repo.insert(
        "pipeline_run",
        {
            "id": run_id,
            "project_id": project_id,
            "run_no": f"{now.strftime('%Y%m%d')}-{run_id}",
            "trigger_type": "manual",
            "status": "running",
            "start_time": now,
            "end_time": None,
            "params": json.dumps({"platform": platform_codes, "keyword": keyword, "model": sentiment_model}, ensure_ascii=False),
            "created_at": now,
        },
    )

    posts = crawl(project_id, platform_codes, keyword, run_id)
    result = process_existing_run(run_id, sentiment_model=sentiment_model)
    result.crawled_posts = len(posts)
    return result


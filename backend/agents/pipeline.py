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


def _insert_many(repo: Any, sheet: str, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    if hasattr(repo, "insert_many"):
        return int(repo.insert_many(sheet, rows))
    for row in rows:
        repo.insert(sheet, row)
    return len(rows)


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
    base_id = _now_ts_ms() * 1000
    seq = 0
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
                "id": base_id + seq,
                "post_raw_id": raw_id,
                "pipeline_run_id": pipeline_run_id,
                "project_id": _safe_int(r.get("project_id")),
                "project_platform_id": _safe_int(r.get("project_platform_id")),
                "platform_id": _safe_int(r.get("platform_id")),
                "brand_id": _safe_int(r.get("brand_id")),
                "clean_text": clean_text,
                "text_hash": _sha256_hex(clean_text) if clean_text else None,
                "is_valid": is_valid,
                "invalid_reason": invalid_reason,
            }
        )
        seq += 1

    if not rows:
        return 0

    if hasattr(repo, "insert_many"):
        return int(repo.insert_many("post_clean", rows))

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
    base_id = _now_ts_ms() * 1000
    seq = 0
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
        if any(k in text for k in ["加微信", "加V", "VX", "福利", "返现", "免费", "http://", "https://"]):
            hits["promo_or_link"] = True
            score += 0.35
        if (text.count("！") + text.count("!") + text.count("？") + text.count("?")) >= 5:
            hits["excessive_exclaim"] = True
            score += 0.15

        label = "normal"
        if score >= 0.6:
            label = "spam"
        elif score >= 0.3:
            label = "suspect"

        rows.append(
            {
                "id": base_id + seq,
                "post_clean_id": clean_id,
                "project_id": _safe_int(r.get("project_id")),
                "project_platform_id": _safe_int(r.get("project_platform_id")),
                "platform_id": _safe_int(r.get("platform_id")),
                "brand_id": _safe_int(r.get("brand_id")),
                "score_total": round(float(score), 4),
                "label": label,
                "rule_hits": json.dumps(hits, ensure_ascii=False),
            }
        )
        seq += 1

    if hasattr(repo, "insert_many"):
        return int(repo.insert_many("spam_score", rows))

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

    # legacy fallback: project -> first brand id (only used when raw brand_id missing)
    project_brand_fallback: Dict[int, Optional[int]] = {}
    if "brand_id" not in raw_df.columns or not raw_df["brand_id"].notna().any():
        join_df = repo.query("monitor_project_brand")
        if join_df is not None and not join_df.empty and {"project_id", "brand_id"}.issubset(join_df.columns):
            tmp = join_df.copy()
            tmp["project_id"] = pd.to_numeric(tmp["project_id"], errors="coerce")
            tmp["brand_id"] = pd.to_numeric(tmp["brand_id"], errors="coerce")
            tmp = tmp.dropna(subset=["project_id", "brand_id"])
            tmp["project_id"] = tmp["project_id"].astype(int)
            tmp["brand_id"] = tmp["brand_id"].astype(int)
            for pid, g in tmp.groupby("project_id"):
                bids = sorted({int(x) for x in g["brand_id"].tolist() if x is not None})
                project_brand_fallback[int(pid)] = bids[0] if bids else None

    buckets: Dict[Tuple[int, int, int, date], Dict[str, int]] = {}
    for _, r in raw_df.iterrows():
        project_id = _safe_int(r.get("project_id")) or 0
        platform_id = _safe_int(r.get("platform_id")) or 0
        brand_id = _safe_int(r.get("brand_id"))
        if brand_id is None:
            brand_id = project_brand_fallback.get(int(project_id)) if project_id else None
        brand_id = int(brand_id or 0)
        d = _safe_date(r.get("publish_time")) or datetime.utcnow().date()
        key = (project_id, brand_id, platform_id, d)
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
    base_id = _now_ts_ms() * 1000
    seq = 0
    for (project_id, brand_id, platform_id, d), m in buckets.items():
        rows.append(
            {
                "id": base_id + seq,
                "project_id": project_id,
                "brand_id": brand_id if brand_id > 0 else None,
                "platform_id": platform_id,
                "metric_date": datetime(d.year, d.month, d.day),
                "pipeline_run_id": pipeline_run_id,
                **m,
            }
        )
        seq += 1

    if not rows:
        return 0

    # upsert per (project_id, brand_id, platform_id, metric_date)
    existing = repo.query("daily_metric")
    if existing is None or existing.empty:
        if hasattr(repo, "insert_many"):
            return int(repo.insert_many("daily_metric", rows))
        for row in rows:
            repo.insert("daily_metric", row)
        return len(rows)

    ex = existing.copy()
    for c in ["project_id", "brand_id", "platform_id"]:
        if c in ex.columns:
            ex[c] = pd.to_numeric(ex[c], errors="coerce")
    if "metric_date" in ex.columns:
        ex["metric_date"] = pd.to_datetime(ex["metric_date"], errors="coerce")

    keys = set()
    for r in rows:
        keys.add(
            (
                int(r.get("project_id") or 0),
                int(pd.to_numeric(r.get("brand_id"), errors="coerce")) if pd.notna(pd.to_numeric(r.get("brand_id"), errors="coerce")) else 0,
                int(r.get("platform_id") or 0),
                pd.to_datetime(r.get("metric_date"), errors="coerce"),
            )
        )

    def _row_key(sr: pd.Series):
        def _i(v: Any) -> int:
            n = pd.to_numeric(v, errors="coerce")
            return int(n) if pd.notna(n) else 0

        return (
            _i(sr.get("project_id")),
            _i(sr.get("brand_id")),
            _i(sr.get("platform_id")),
            pd.to_datetime(sr.get("metric_date"), errors="coerce"),
        )

    keep_mask = ex.apply(lambda sr: _row_key(sr) not in keys, axis=1)
    kept = ex[keep_mask]
    merged = pd.concat([kept, pd.DataFrame(rows)], ignore_index=True)
    merged = merged.replace([float("inf"), float("-inf")], None)
    merged = merged.astype(object).where(pd.notnull(merged), None)
    repo.replace("daily_metric", merged.to_dict(orient="records"))
    return len(rows)


def infer_topics_for_run(pipeline_run_id: int) -> int:
    repo = get_repo()
    clean_df = repo.query("post_clean", {"pipeline_run_id": pipeline_run_id})
    if clean_df is None or clean_df.empty or "id" not in clean_df.columns:
        return 0

    existing = repo.query("topic_result")
    existing_clean_ids = set()
    if existing is not None and not existing.empty and "post_clean_id" in existing.columns:
        existing_clean_ids = set(pd.to_numeric(existing["post_clean_id"], errors="coerce").dropna().astype(int).tolist())

    rows: List[Dict[str, Any]] = []
    base_id = _now_ts_ms() * 1000
    seq = 0
    for _, r in clean_df.iterrows():
        cid = _safe_int(r.get("id"))
        if cid is None or cid in existing_clean_ids:
            continue
        is_valid = int(pd.to_numeric(r.get("is_valid"), errors="coerce") or 0)
        if is_valid != 1:
            continue
        text = _normalize_str(r.get("clean_text"))

        topic_id = "T01"
        topic_name = "口碑讨论"
        if any(k in text for k in ["差", "失望", "故障", "退货", "售后", "不行", "崩溃"]):
            topic_id = "T02"
            topic_name = "负面反馈"
        elif any(k in text for k in ["价格", "性价比", "优惠", "贵"]):
            topic_id = "T03"
            topic_name = "价格讨论"
        elif any(k in text for k in ["功能", "体验", "续航", "像素", "夜视", "清晰", "误报"]):
            topic_id = "T04"
            topic_name = "功能体验"

        rows.append(
            {
                "id": base_id + seq,
                "post_clean_id": int(cid),
                "pipeline_run_id": pipeline_run_id,
                "project_id": _safe_int(r.get("project_id")),
                "project_platform_id": _safe_int(r.get("project_platform_id")),
                "platform_id": _safe_int(r.get("platform_id")),
                "brand_id": _safe_int(r.get("brand_id")),
                "topic_id": topic_id,
                "topic_name": topic_name,
                "score": 0.7,
                "created_at": datetime.utcnow(),
            }
        )
        seq += 1

    if not rows:
        return 0
    if hasattr(repo, "insert_many"):
        return int(repo.insert_many("topic_result", rows))
    for row in rows:
        repo.insert("topic_result", row)
    return len(rows)


def infer_entities_for_run(pipeline_run_id: int) -> int:
    repo = get_repo()
    clean_df = repo.query("post_clean", {"pipeline_run_id": pipeline_run_id})
    if clean_df is None or clean_df.empty or "id" not in clean_df.columns:
        return 0

    existing = repo.query("entity_result")
    existing_pairs: set[tuple[int, str]] = set()
    if existing is not None and not existing.empty and {"post_clean_id", "entity_text"}.issubset(existing.columns):
        tmp = existing.copy()
        tmp["post_clean_id"] = pd.to_numeric(tmp["post_clean_id"], errors="coerce")
        tmp = tmp.dropna(subset=["post_clean_id"])
        tmp["post_clean_id"] = tmp["post_clean_id"].astype(int)
        for _, rr in tmp.iterrows():
            cid = _safe_int(rr.get("post_clean_id"))
            et = _normalize_str(rr.get("entity_text"))
            if cid is not None and et:
                existing_pairs.add((int(cid), et))

    rows: List[Dict[str, Any]] = []
    base_id = _now_ts_ms() * 1000
    seq = 0
    for _, r in clean_df.iterrows():
        cid = _safe_int(r.get("id"))
        if cid is None:
            continue
        is_valid = int(pd.to_numeric(r.get("is_valid"), errors="coerce") or 0)
        if is_valid != 1:
            continue
        text = _normalize_str(r.get("clean_text"))

        candidates: List[tuple[str, str]] = []
        for kw in ["夜视", "续航", "像素", "清晰", "误报", "售后", "价格", "发热", "卡顿"]:
            if kw and kw in text:
                candidates.append(("feature", kw))
        if not candidates and text:
            candidates.append(("phrase", text[:6]))

        for etype, etext in candidates[:2]:
            if (int(cid), etext) in existing_pairs:
                continue
            rows.append(
                {
                    "id": base_id + seq,
                    "post_clean_id": int(cid),
                    "pipeline_run_id": pipeline_run_id,
                    "project_id": _safe_int(r.get("project_id")),
                    "project_platform_id": _safe_int(r.get("project_platform_id")),
                    "platform_id": _safe_int(r.get("platform_id")),
                    "brand_id": _safe_int(r.get("brand_id")),
                    "entity_type": etype,
                    "entity_text": etext,
                    "normalized": etext,
                    "confidence": 0.75,
                    "created_at": datetime.utcnow(),
                }
            )
            seq += 1

    if not rows:
        return 0
    if hasattr(repo, "insert_many"):
        return int(repo.insert_many("entity_result", rows))
    for row in rows:
        repo.insert("entity_result", row)
    return len(rows)


def generate_report_for_run(pipeline_run_id: int) -> Optional[int]:
    repo = get_repo()
    run_df = repo.query("pipeline_run", {"id": pipeline_run_id})
    project_id = None
    project_platform_id = None
    platform_id = None
    trigger_type = None
    if not run_df.empty and "project_id" in run_df.columns:
        project_id = _safe_int(run_df.iloc[0].get("project_id"))
        project_platform_id = _safe_int(run_df.iloc[0].get("project_platform_id"))
        platform_id = _safe_int(run_df.iloc[0].get("platform_id"))
        trigger_type = _normalize_str(run_df.iloc[0].get("trigger_type")) or None

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

    # Override summary using only rows belonging to this pipeline_run_id.
    raw_df = repo.query("post_raw", {"pipeline_run_id": pipeline_run_id})
    total_posts = int(len(raw_df)) if raw_df is not None else 0

    clean_df = repo.query("post_clean", {"pipeline_run_id": pipeline_run_id})
    valid_clean_ids = set()
    if clean_df is not None and not clean_df.empty and "id" in clean_df.columns:
        valid_mask = None
        if "is_valid" in clean_df.columns:
            valid_mask = pd.to_numeric(clean_df["is_valid"], errors="coerce").fillna(0).astype(int) == 1
        if valid_mask is None:
            valid_mask = pd.Series([True] * len(clean_df))
        valid_ids = pd.to_numeric(clean_df.loc[valid_mask, "id"], errors="coerce").dropna().astype(int).tolist()
        valid_clean_ids = set(valid_ids)

    neg_posts = 0
    spam_posts = 0
    if valid_clean_ids:
        sent_df = repo.query("sentiment_result")
        if sent_df is not None and not sent_df.empty and {"post_clean_id", "polarity"}.issubset(sent_df.columns):
            sent_df = sent_df.copy()
            sent_df["post_clean_id"] = pd.to_numeric(sent_df["post_clean_id"], errors="coerce")
            sent_df = sent_df.dropna(subset=["post_clean_id"])
            sent_df["post_clean_id"] = sent_df["post_clean_id"].astype(int)
            sent_df = sent_df[sent_df["post_clean_id"].isin(list(valid_clean_ids))]
            if not sent_df.empty:
                neg_posts = int((sent_df["polarity"].astype(str).str.lower() == "negative").sum())

        spam_df = repo.query("spam_score")
        if spam_df is not None and not spam_df.empty and {"post_clean_id", "label"}.issubset(spam_df.columns):
            spam_df = spam_df.copy()
            spam_df["post_clean_id"] = pd.to_numeric(spam_df["post_clean_id"], errors="coerce")
            spam_df = spam_df.dropna(subset=["post_clean_id"])
            spam_df["post_clean_id"] = spam_df["post_clean_id"].astype(int)
            spam_df = spam_df[spam_df["post_clean_id"].isin(list(valid_clean_ids))]
            if not spam_df.empty:
                labels = spam_df["label"].astype(str).str.lower()
                spam_posts = int(labels.isin(["spam", "suspect"]).sum())

    if total_posts <= 0:
        summary = f"Run {pipeline_run_id}: no posts found."
    else:
        summary = f"Run {pipeline_run_id}: total_posts={total_posts}, negative={neg_posts}, spam_or_suspect={spam_posts}."

    now = datetime.utcnow()
    report_id = _now_ts_ms() * 1000

    # Build lightweight structured content_json compatible with the report center UI.
    clean_df = repo.query("post_clean", {"pipeline_run_id": pipeline_run_id})
    valid_mask = None
    if clean_df is not None and not clean_df.empty and "is_valid" in clean_df.columns:
        valid_mask = pd.to_numeric(clean_df["is_valid"], errors="coerce").fillna(0).astype(int) == 1
    valid_posts = int(valid_mask.sum()) if valid_mask is not None else 0

    sent_df = repo.query("sentiment_result")
    neg_posts = 0
    pos_posts = 0
    neu_posts = 0
    if clean_df is not None and not clean_df.empty and sent_df is not None and not sent_df.empty and {"post_clean_id", "polarity"}.issubset(sent_df.columns):
        valid_ids = set(
            pd.to_numeric(clean_df.loc[valid_mask if valid_mask is not None else slice(None), "id"], errors="coerce").dropna().astype(int).tolist()
        ) if "id" in clean_df.columns else set()
        tmp = sent_df.copy()
        tmp["post_clean_id"] = pd.to_numeric(tmp["post_clean_id"], errors="coerce")
        tmp = tmp.dropna(subset=["post_clean_id"])
        tmp["post_clean_id"] = tmp["post_clean_id"].astype(int)
        if "id" in tmp.columns:
            tmp["id"] = pd.to_numeric(tmp["id"], errors="coerce")
            tmp = tmp.sort_values(by="id").drop_duplicates(subset=["post_clean_id"], keep="last")
        tmp = tmp[tmp["post_clean_id"].isin(list(valid_ids))] if valid_ids else tmp
        pol = tmp["polarity"].astype(str).str.lower()
        neg_posts = int((pol == "negative").sum())
        pos_posts = int((pol == "positive").sum())
        neu_posts = int((pol == "neutral").sum())

    spam_df = repo.query("spam_score")
    spam_posts = 0
    if clean_df is not None and not clean_df.empty and spam_df is not None and not spam_df.empty and {"post_clean_id", "label"}.issubset(spam_df.columns):
        valid_ids = set(
            pd.to_numeric(clean_df.loc[valid_mask if valid_mask is not None else slice(None), "id"], errors="coerce").dropna().astype(int).tolist()
        ) if "id" in clean_df.columns else set()
        tmp = spam_df.copy()
        tmp["post_clean_id"] = pd.to_numeric(tmp["post_clean_id"], errors="coerce")
        tmp = tmp.dropna(subset=["post_clean_id"])
        tmp["post_clean_id"] = tmp["post_clean_id"].astype(int)
        if "id" in tmp.columns:
            tmp["id"] = pd.to_numeric(tmp["id"], errors="coerce")
            tmp = tmp.sort_values(by="id").drop_duplicates(subset=["post_clean_id"], keep="last")
        tmp = tmp[tmp["post_clean_id"].isin(list(valid_ids))] if valid_ids else tmp
        labels = tmp["label"].astype(str).str.lower()
        spam_posts = int(labels.isin(["spam", "suspect"]).sum())

    topics: List[Dict[str, Any]] = []
    tdf = repo.query("topic_result")
    if tdf is not None and not tdf.empty and {"pipeline_run_id", "topic_name"}.issubset(tdf.columns):
        tmp = tdf.copy()
        tmp["pipeline_run_id"] = pd.to_numeric(tmp["pipeline_run_id"], errors="coerce")
        tmp = tmp.dropna(subset=["pipeline_run_id"])
        tmp["pipeline_run_id"] = tmp["pipeline_run_id"].astype(int)
        tmp = tmp[tmp["pipeline_run_id"] == int(pipeline_run_id)]
        if not tmp.empty:
            vc = tmp["topic_name"].astype(str).value_counts().head(10)
            topics = [{"topic": str(k), "count": int(v)} for k, v in vc.items()]

    entities: List[Dict[str, Any]] = []
    edf = repo.query("entity_result")
    if edf is not None and not edf.empty and {"pipeline_run_id", "entity_text"}.issubset(edf.columns):
        tmp = edf.copy()
        tmp["pipeline_run_id"] = pd.to_numeric(tmp["pipeline_run_id"], errors="coerce")
        tmp = tmp.dropna(subset=["pipeline_run_id"])
        tmp["pipeline_run_id"] = tmp["pipeline_run_id"].astype(int)
        tmp = tmp[tmp["pipeline_run_id"] == int(pipeline_run_id)]
        if not tmp.empty:
            vc = tmp["entity_text"].astype(str).value_counts().head(15)
            entities = [{"entity_text": str(k), "count": int(v)} for k, v in vc.items()]

    content = {
        "range": {"from": now.date().isoformat(), "to": now.date().isoformat()},
        "executive_summary": {
            "overall_trend": "系统自动聚合生成（模拟爬虫）。",
            "main_risks": "关注负面占比上升与水军比例异常。",
            "key_feedback": "可结合热点话题与实体/功能点分析提炼。",
            "strategic_suggestions": ["持续监控负面集中点，快速闭环产品/售后问题。"],
        },
        "overview": {
            "total_posts": int(total_posts),
            "valid_posts": valid_posts,
            "spam_posts": spam_posts,
            "positive_posts": pos_posts,
            "neutral_posts": neu_posts,
            "negative_posts": neg_posts,
        },
        "sentiment_trends": {"dates": [now.date().isoformat()], "positive": [pos_posts], "neutral": [neu_posts], "negative": [neg_posts]},
        "hot_topics": topics,
        "entities": entities,
        "spam": {"spam_posts": spam_posts},
        "competitor_compare": [],
        "strategic_suggestions": ["持续监控负面集中点，快速闭环产品/售后问题。"],
    }

    repo.insert(
        "report",
        {
            "id": report_id,
            "pipeline_run_id": pipeline_run_id,
            "project_platform_id": project_platform_id,
            "project_id": project_id,
            "brand_id": None,
            "platform_id": platform_id,
            "title": "竞品舆情日报",
            "report_type": "daily",
            "summary": summary,
            "created_at": now,
            "time_start": now.date().isoformat(),
            "time_end": now.date().isoformat(),
            "range_from": now.date().isoformat(),
            "range_to": now.date().isoformat(),
            "content_json": json.dumps(content, ensure_ascii=False),
            "status": "generated",
            "trigger_type": trigger_type or "manual",
        },
    )

    # citations: best-effort pick top 5 negative posts in this run
    if raw_df is not None and not raw_df.empty and clean_df is not None and not clean_df.empty and sent_df is not None and not sent_df.empty:
        try:
            raw = raw_df.copy()
            raw["id"] = pd.to_numeric(raw["id"], errors="coerce")
            raw = raw.dropna(subset=["id"])
            raw["id"] = raw["id"].astype(int)

            clean = clean_df.copy()
            clean["post_raw_id"] = pd.to_numeric(clean.get("post_raw_id"), errors="coerce")
            clean["id"] = pd.to_numeric(clean.get("id"), errors="coerce")
            clean = clean.dropna(subset=["post_raw_id", "id"])
            clean["post_raw_id"] = clean["post_raw_id"].astype(int)
            clean["id"] = clean["id"].astype(int)

            sent = sent_df.copy()
            sent["post_clean_id"] = pd.to_numeric(sent.get("post_clean_id"), errors="coerce")
            sent = sent.dropna(subset=["post_clean_id"])
            sent["post_clean_id"] = sent["post_clean_id"].astype(int)
            if "id" in sent.columns:
                sent["id"] = pd.to_numeric(sent["id"], errors="coerce")
                sent = sent.sort_values(by="id").drop_duplicates(subset=["post_clean_id"], keep="last")

            merged = clean.merge(sent[["post_clean_id", "polarity"]], left_on="id", right_on="post_clean_id", how="left")
            merged = merged.merge(raw[["id", "raw_text"]], left_on="post_raw_id", right_on="id", how="left", suffixes=("_clean", "_raw"))
            merged["polarity"] = merged["polarity"].astype(str).str.lower()
            neg = merged[merged["polarity"] == "negative"].head(5)
            if not neg.empty:
                cit_rows = []
                base_cid = _now_ts_ms() * 1000
                for i, rr in enumerate(neg.itertuples(index=False)):
                    cit_rows.append(
                        {
                            "id": base_cid + i,
                            "report_id": report_id,
                            "citation_type": "post",
                            "section_code": "sentiment_analysis",
                            "quote_text": str(getattr(rr, "raw_text", "") or "")[:600],
                            "sort_order": i + 1,
                            "post_raw_id": int(getattr(rr, "post_raw_id", 0) or 0),
                            "reason": "负面样本（自动）",
                            "created_at": now,
                        }
                    )
                if cit_rows:
                    _insert_many(repo, "report_citation", cit_rows)
        except Exception:
            pass

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

    infer_topics_for_run(pipeline_run_id)
    infer_entities_for_run(pipeline_run_id)

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

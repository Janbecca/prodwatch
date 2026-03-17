from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from jinja2 import Environment, FileSystemLoader

from backend.storage.db import get_repo


def _platform_name_map(repo) -> Dict[int, str]:
    df = repo.query("platform")
    m: Dict[int, str] = {}
    if df.empty or not {"id", "name"}.issubset(df.columns):
        return m
    for _, r in df.iterrows():
        pid = pd.to_numeric(r.get("id"), errors="coerce")
        if pd.notna(pid):
            m[int(pid)] = str(r.get("name"))
    return m


def build_report_context(pipeline_run_id: Optional[int] = None) -> Dict[str, Any]:
    repo = get_repo()
    report_df = repo.query("report")
    if report_df.empty:
        return {
            "title": "竞品舆情日报",
            "generated_at": datetime.utcnow().isoformat(),
            "summary": "暂无报告数据。",
            "metrics": [],
            "samples": [],
        }

    if "created_at" in report_df.columns:
        report_df["created_at"] = pd.to_datetime(report_df["created_at"], errors="coerce")
        report_df = report_df.sort_values(by="created_at", ascending=False)

    if pipeline_run_id is not None and "pipeline_run_id" in report_df.columns:
        target = report_df[report_df["pipeline_run_id"] == pipeline_run_id]
        row = target.iloc[0] if not target.empty else report_df.iloc[0]
    else:
        row = report_df.iloc[0]

    pid = row.get("pipeline_run_id")
    project_id = row.get("project_id")

    daily_df = repo.query("daily_metric")
    if not daily_df.empty and "metric_date" in daily_df.columns:
        daily_df["metric_date"] = pd.to_datetime(daily_df["metric_date"], errors="coerce")
        daily_df = daily_df.sort_values("metric_date", ascending=False)

    platform_names = _platform_name_map(repo)
    metrics: List[Dict[str, Any]] = []
    if not daily_df.empty and {"platform_id", "total_posts"}.issubset(daily_df.columns):
        head = daily_df.head(20)
        for _, r in head.iterrows():
            platform_id = pd.to_numeric(r.get("platform_id"), errors="coerce")
            if pd.isna(platform_id):
                continue
            metrics.append(
                {
                    "platform": platform_names.get(int(platform_id), f"platform_{int(platform_id)}"),
                    "total_posts": int(pd.to_numeric(r.get("total_posts"), errors="coerce") or 0),
                    "valid_posts": int(pd.to_numeric(r.get("valid_posts"), errors="coerce") or 0),
                    "spam_posts": int(pd.to_numeric(r.get("spam_posts"), errors="coerce") or 0),
                    "pos_posts": int(pd.to_numeric(r.get("pos_posts"), errors="coerce") or 0),
                    "neu_posts": int(pd.to_numeric(r.get("neu_posts"), errors="coerce") or 0),
                    "neg_posts": int(pd.to_numeric(r.get("neg_posts"), errors="coerce") or 0),
                }
            )

    clean_df = repo.query("post_clean")
    sent_df = repo.query("sentiment_result")
    samples: List[Dict[str, Any]] = []
    if not sent_df.empty and not clean_df.empty and {"post_clean_id", "polarity"}.issubset(sent_df.columns):
        # quick join
        clean_text_by_id: Dict[int, str] = {}
        if {"id", "clean_text"}.issubset(clean_df.columns):
            for _, r in clean_df.iterrows():
                cid = pd.to_numeric(r.get("id"), errors="coerce")
                if pd.notna(cid):
                    clean_text_by_id[int(cid)] = str(r.get("clean_text") or "")

        sent_df = sent_df.head(30)
        for _, r in sent_df.iterrows():
            cid = pd.to_numeric(r.get("post_clean_id"), errors="coerce")
            if pd.isna(cid):
                continue
            cid_int = int(cid)
            samples.append({"clean_text": clean_text_by_id.get(cid_int, ""), "polarity": str(r.get("polarity") or "neutral")})

    # Override with run-scoped metrics/samples when possible (avoid mixing runs).
    effective_run_id = None
    if pipeline_run_id is not None:
        effective_run_id = int(pipeline_run_id)
    else:
        pid_num = pd.to_numeric(pid, errors="coerce")
        if pd.notna(pid_num):
            effective_run_id = int(pid_num)

    if effective_run_id is not None:
        raw_df = repo.query("post_raw", {"pipeline_run_id": effective_run_id})
        clean_df_run = repo.query("post_clean", {"pipeline_run_id": effective_run_id})
        sent_df_run = repo.query("sentiment_result")
        spam_df_run = repo.query("spam_score")

        valid_clean_by_raw: Dict[int, int] = {}
        clean_text_by_id: Dict[int, str] = {}
        if clean_df_run is not None and not clean_df_run.empty and {"id", "post_raw_id"}.issubset(clean_df_run.columns):
            is_valid = (
                pd.to_numeric(clean_df_run.get("is_valid"), errors="coerce").fillna(0).astype(int)
                if "is_valid" in clean_df_run.columns
                else pd.Series([1] * len(clean_df_run))
            )
            for (_, r), v in zip(clean_df_run.iterrows(), is_valid.tolist()):
                if int(v) != 1:
                    continue
                cid = pd.to_numeric(r.get("id"), errors="coerce")
                rid = pd.to_numeric(r.get("post_raw_id"), errors="coerce")
                if pd.notna(cid) and pd.notna(rid):
                    valid_clean_by_raw[int(rid)] = int(cid)
                if pd.notna(cid) and "clean_text" in clean_df_run.columns:
                    clean_text_by_id[int(cid)] = str(r.get("clean_text") or "")

        spam_by_clean: Dict[int, str] = {}
        if spam_df_run is not None and not spam_df_run.empty and {"post_clean_id", "label"}.issubset(spam_df_run.columns):
            spam_df_run = spam_df_run.copy()
            spam_df_run["post_clean_id"] = pd.to_numeric(spam_df_run["post_clean_id"], errors="coerce")
            spam_df_run = spam_df_run.dropna(subset=["post_clean_id"])
            spam_df_run["post_clean_id"] = spam_df_run["post_clean_id"].astype(int)
            for _, r in spam_df_run.iterrows():
                spam_by_clean[int(r["post_clean_id"])] = str(r.get("label") or "normal").lower()

        sent_by_clean: Dict[int, str] = {}
        sent_rows: List[Dict[str, Any]] = []
        if sent_df_run is not None and not sent_df_run.empty and {"post_clean_id", "polarity"}.issubset(sent_df_run.columns):
            sent_df_run = sent_df_run.copy()
            sent_df_run["post_clean_id"] = pd.to_numeric(sent_df_run["post_clean_id"], errors="coerce")
            sent_df_run = sent_df_run.dropna(subset=["post_clean_id"])
            sent_df_run["post_clean_id"] = sent_df_run["post_clean_id"].astype(int)
            if "id" in sent_df_run.columns:
                sent_df_run["id"] = pd.to_numeric(sent_df_run["id"], errors="coerce")
                sent_df_run = sent_df_run.sort_values(by="id", ascending=False)
            for _, r in sent_df_run.iterrows():
                cid = int(r["post_clean_id"])
                pol = str(r.get("polarity") or "neutral").lower()
                sent_by_clean[cid] = pol
                sent_rows.append({"post_clean_id": cid, "polarity": pol})

        metrics_run: List[Dict[str, Any]] = []
        if raw_df is not None and not raw_df.empty and {"id", "platform_id"}.issubset(raw_df.columns):
            raw_df = raw_df.copy()
            raw_df["id"] = pd.to_numeric(raw_df["id"], errors="coerce")
            raw_df["platform_id"] = pd.to_numeric(raw_df["platform_id"], errors="coerce")
            raw_df = raw_df.dropna(subset=["id", "platform_id"])
            raw_df["id"] = raw_df["id"].astype(int)
            raw_df["platform_id"] = raw_df["platform_id"].astype(int)
            for platform_id, g in raw_df.groupby("platform_id"):
                raw_ids = g["id"].tolist()
                clean_ids = [valid_clean_by_raw.get(rid) for rid in raw_ids if rid in valid_clean_by_raw]
                clean_ids = [cid for cid in clean_ids if cid is not None]
                spam_posts = sum(1 for cid in clean_ids if spam_by_clean.get(int(cid)) in {"spam", "suspect"})
                pos = sum(1 for cid in clean_ids if sent_by_clean.get(int(cid)) == "positive")
                neg = sum(1 for cid in clean_ids if sent_by_clean.get(int(cid)) == "negative")
                neu = max(len(clean_ids) - pos - neg, 0)
                metrics_run.append(
                    {
                        "platform": platform_names.get(int(platform_id), f"platform_{int(platform_id)}"),
                        "total_posts": int(len(g)),
                        "valid_posts": int(len(clean_ids)),
                        "spam_posts": int(spam_posts),
                        "pos_posts": int(pos),
                        "neu_posts": int(neu),
                        "neg_posts": int(neg),
                    }
                )
            metrics_run.sort(key=lambda x: x.get("total_posts", 0), reverse=True)

        samples_run: List[Dict[str, Any]] = []
        if valid_clean_by_raw:
            valid_clean_ids = set(valid_clean_by_raw.values())
            for r in sent_rows:
                if r["post_clean_id"] not in valid_clean_ids:
                    continue
                samples_run.append(
                    {
                        "clean_text": clean_text_by_id.get(int(r["post_clean_id"]), ""),
                        "polarity": r["polarity"],
                    }
                )
                if len(samples_run) >= 30:
                    break

        if metrics_run:
            metrics = metrics_run
        if samples_run:
            samples = samples_run

    return {
        "title": row.get("title") or "竞品舆情日报",
        "generated_at": (pd.to_datetime(row.get("created_at"), errors="coerce").isoformat() if row.get("created_at") else datetime.utcnow().isoformat()),
        "summary": row.get("summary") or "",
        "metrics": metrics,
        "samples": samples,
        "pipeline_run_id": pid,
        "project_id": project_id,
    }


def render_report_html(context: Dict[str, Any]) -> str:
    env = Environment(loader=FileSystemLoader("backend/templates"))
    template = env.get_template("report.html.j2")
    return template.render(**context)


def generate_pdf_report(pipeline_run_id: Optional[int] = None, *, filename: str = "daily_report.pdf") -> str:
    """
    Generates a PDF into ./reports and returns its path.

    Manual fill / optional:
    - Playwright requires browsers installed (`playwright install`).
    """
    context = build_report_context(pipeline_run_id)
    html_content = render_report_html(context)

    os.makedirs("reports", exist_ok=True)
    pdf_path = os.path.join("reports", filename)

    from playwright.sync_api import sync_playwright  # type: ignore

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.set_content(html_content)
        page.pdf(path=pdf_path, format="A4")
        browser.close()

    return pdf_path

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


def generate_pdf_report(pipeline_run_id: Optional[int] = None) -> str:
    """
    Generates a PDF into ./reports and returns its path.

    Manual fill / optional:
    - Playwright requires browsers installed (`playwright install`).
    """
    context = build_report_context(pipeline_run_id)
    html_content = render_report_html(context)

    os.makedirs("reports", exist_ok=True)
    pdf_path = os.path.join("reports", "daily_report.pdf")

    from playwright.sync_api import sync_playwright  # type: ignore

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.set_content(html_content)
        page.pdf(path=pdf_path, format="A4")
        browser.close()

    return pdf_path


from fastapi import APIRouter, Depends, Query
from typing import Optional
import pandas as pd
from backend.storage.db import get_repo
from .auth import get_current_user

router = APIRouter(prefix="/moderation", tags=["moderation"])


@router.get("/spam/overview")
def spam_overview(user=Depends(get_current_user)):
    repo = get_repo()
    spam_df = repo.query("spam_score")
    total = len(spam_df)
    spam = 0
    if "label" in spam_df.columns and total:
        labels = spam_df["label"].astype(str).str.lower()
        spam = int(labels.isin(["spam", "suspect"]).sum())
    ratio = round((spam / total), 4) if total else 0.0
    return {"spam_ratio": ratio, "total": total, "spam": spam}


@router.get("/spam/list")
def spam_list(product: Optional[str] = Query(None), user=Depends(get_current_user)):
    repo = get_repo()
    spam_df = repo.query("spam_score")
    clean_df = repo.query("post_clean")
    raw_df = repo.query("post_raw")
    platform_df = repo.query("platform")

    platform_map = {}
    if "id" in platform_df.columns and "name" in platform_df.columns:
        for _, row in platform_df.iterrows():
            pid = pd.to_numeric(row.get("id"), errors="coerce")
            if pd.notna(pid):
                platform_map[int(pid)] = str(row.get("name"))

    raw_by_clean = {}
    if {"id", "post_raw_id"}.issubset(clean_df.columns):
        for _, row in clean_df.iterrows():
            cid = pd.to_numeric(row.get("id"), errors="coerce")
            rid = pd.to_numeric(row.get("post_raw_id"), errors="coerce")
            if pd.notna(cid) and pd.notna(rid):
                raw_by_clean[int(cid)] = int(rid)

    raw_rows = {}
    if {"id", "platform_id"}.issubset(raw_df.columns):
        for _, row in raw_df.iterrows():
            rid = pd.to_numeric(row.get("id"), errors="coerce")
            if pd.notna(rid):
                raw_rows[int(rid)] = row.to_dict()

    items = []
    for _, row in spam_df.iterrows():
        sid = pd.to_numeric(row.get("id"), errors="coerce")
        if pd.isna(sid):
            continue
        cid = pd.to_numeric(row.get("post_clean_id"), errors="coerce")
        if pd.isna(cid):
            continue
        rid = raw_by_clean.get(int(cid))
        raw = raw_rows.get(rid, {})
        pid = pd.to_numeric(raw.get("platform_id"), errors="coerce")
        pname = platform_map.get(int(pid), "unknown") if pd.notna(pid) else "unknown"
        if product and product not in pname:
            continue
        reason = row.get("rule_hits") or row.get("label") or "unknown"
        items.append(
            {
                "id": int(sid),
                "product": pname,
                "reason": str(reason),
            }
        )
    return items

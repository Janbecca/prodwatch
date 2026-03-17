from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
from fastapi import APIRouter, Depends, Query

from backend.storage.db import get_latest_pipeline_run_id, get_repo
from .auth import get_current_user

router = APIRouter(prefix="/posts", tags=["posts"])


def _none_if_nan(x: Any) -> Any:
    try:
        return None if pd.isna(x) else x
    except Exception:
        return x


def _dt_iso(x: Any) -> Optional[str]:
    x = _none_if_nan(x)
    if x is None:
        return None
    dt = pd.to_datetime(x, errors="coerce")
    if pd.isna(dt):
        return None
    try:
        return dt.to_pydatetime().isoformat()
    except Exception:
        try:
            return dt.isoformat()
        except Exception:
            return str(dt)


def _safe_int(x: Any) -> Optional[int]:
    v = pd.to_numeric(x, errors="coerce")
    if pd.isna(v):
        return None
    return int(v)


def _parse_range(days: Optional[int], start_date: Optional[str], end_date: Optional[str]) -> Tuple[Optional[datetime], Optional[datetime]]:
    if days is None and not start_date and not end_date:
        return None, None

    days_n = max(1, min(int(days or 14), 365))
    now = datetime.utcnow()
    start_dt = pd.to_datetime(start_date, errors="coerce") if start_date else pd.NaT
    end_dt = pd.to_datetime(end_date, errors="coerce") if end_date else pd.NaT

    if pd.notna(end_dt):
        end = end_dt.to_pydatetime() if hasattr(end_dt, "to_pydatetime") else now
    else:
        end = now
    end = datetime(end.year, end.month, end.day, 23, 59, 59, 999999)

    if pd.notna(start_dt):
        start = start_dt.to_pydatetime() if hasattr(start_dt, "to_pydatetime") else (end - timedelta(days=days_n - 1))
    else:
        start = end - timedelta(days=days_n - 1)
    start = datetime(start.year, start.month, start.day)

    if start > end:
        start, end = end, start
    return start, end


def _projects_for_brands(repo, brand_ids: List[int]) -> Set[int]:
    allowed = set(int(x) for x in brand_ids if x is not None)

    join_df = repo.query("monitor_project_brand")
    if join_df is not None and not join_df.empty and {"project_id", "brand_id"}.issubset(join_df.columns):
        join_df = join_df.copy()
        join_df["project_id"] = pd.to_numeric(join_df["project_id"], errors="coerce")
        join_df["brand_id"] = pd.to_numeric(join_df["brand_id"], errors="coerce")
        join_df = join_df.dropna(subset=["project_id", "brand_id"])
        join_df["project_id"] = join_df["project_id"].astype(int)
        join_df["brand_id"] = join_df["brand_id"].astype(int)
        return set(join_df[join_df["brand_id"].isin(list(allowed))]["project_id"].tolist())

    projects_df = repo.query("monitor_project")
    if projects_df is None or projects_df.empty or not {"id", "brand_id"}.issubset(projects_df.columns):
        return set()

    out: Set[int] = set()
    for _, row in projects_df.iterrows():
        bid = pd.to_numeric(row.get("brand_id"), errors="coerce")
        pid = pd.to_numeric(row.get("id"), errors="coerce")
        if pd.notna(bid) and pd.notna(pid) and int(bid) in allowed:
            out.add(int(pid))
    return out


def _enrich_posts(repo, df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    df = df.copy()

    # Normalize base numeric columns.
    for c in ["id", "project_id", "pipeline_run_id", "platform_id", "keyword_id", "author_id", "brand_id"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
            if c == "id":
                df = df.dropna(subset=["id"])
                df["id"] = df["id"].astype(int)
            elif df[c].notna().any():
                # keep as int when possible (avoid floats leaking to the UI)
                df.loc[df[c].notna(), c] = df.loc[df[c].notna(), c].astype(int)

    if "publish_time" in df.columns:
        df["publish_time"] = pd.to_datetime(df["publish_time"], errors="coerce")

    # platform map
    plat_df = repo.query("platform")
    plat_map: Dict[int, Dict[str, Any]] = {}
    if plat_df is not None and not plat_df.empty and {"id", "name"}.issubset(plat_df.columns):
        tmp = plat_df.copy()
        tmp["id"] = pd.to_numeric(tmp["id"], errors="coerce")
        tmp = tmp.dropna(subset=["id"])
        tmp["id"] = tmp["id"].astype(int)
        for _, r in tmp.iterrows():
            plat_map[int(r["id"])] = {"name": str(r.get("name") or f"platform_{int(r['id'])}"), "code": _none_if_nan(r.get("code"))}
    if "platform_id" in df.columns:
        df["platform_name"] = df["platform_id"].map(lambda x: plat_map.get(int(x), {}).get("name") if pd.notna(x) else None)

    # brand map (prefer stored brand_id; fallback to project -> first brand)
    if "brand_id" not in df.columns:
        df["brand_id"] = None
    if df["brand_id"].isna().all() and "project_id" in df.columns:
        join_df = repo.query("monitor_project_brand")
        project_brand: Dict[int, Optional[int]] = {}
        if join_df is not None and not join_df.empty and {"project_id", "brand_id"}.issubset(join_df.columns):
            j = join_df.copy()
            j["project_id"] = pd.to_numeric(j["project_id"], errors="coerce")
            j["brand_id"] = pd.to_numeric(j["brand_id"], errors="coerce")
            j = j.dropna(subset=["project_id", "brand_id"])
            j["project_id"] = j["project_id"].astype(int)
            j["brand_id"] = j["brand_id"].astype(int)
            for pid, g in j.groupby("project_id"):
                bids = sorted({int(x) for x in g["brand_id"].tolist() if x is not None})
                project_brand[int(pid)] = bids[0] if bids else None
        df["brand_id"] = df["project_id"].dropna().astype(int).map(project_brand)

    brand_df = repo.query("brand")
    brand_map: Dict[int, str] = {}
    if brand_df is not None and not brand_df.empty and {"id", "name"}.issubset(brand_df.columns):
        b = brand_df.copy()
        b["id"] = pd.to_numeric(b["id"], errors="coerce")
        b = b.dropna(subset=["id"])
        b["id"] = b["id"].astype(int)
        brand_map = dict(zip(b["id"].tolist(), b["name"].tolist()))
    df["brand_name"] = df["brand_id"].map(lambda x: brand_map.get(int(x)) if pd.notna(x) else None)

    # keyword_id -> keyword
    if "keyword_id" in df.columns:
        kw_df = repo.query("monitor_keyword")
        if kw_df is not None and not kw_df.empty and {"id", "keyword"}.issubset(kw_df.columns):
            kw = kw_df.copy()
            kw["id"] = pd.to_numeric(kw["id"], errors="coerce")
            kw = kw.dropna(subset=["id"])
            kw["id"] = kw["id"].astype(int)
            keyword_map = dict(zip(kw["id"].tolist(), kw["keyword"].tolist()))
            df["keyword"] = None
            mask = pd.notna(df["keyword_id"])
            if mask.any():
                df.loc[mask, "keyword"] = df.loc[mask, "keyword_id"].astype(int).map(keyword_map)
        else:
            df["keyword"] = None

    # author_id -> nickname
    if "author_id" in df.columns:
        auth_df = repo.query("author")
        nick_map: Dict[int, str] = {}
        if auth_df is not None and not auth_df.empty and {"id", "nickname"}.issubset(auth_df.columns):
            a = auth_df.copy()
            a["id"] = pd.to_numeric(a["id"], errors="coerce")
            a = a.dropna(subset=["id"])
            a["id"] = a["id"].astype(int)
            nick_map = dict(zip(a["id"].tolist(), a["nickname"].tolist()))
        df["author_nickname"] = df["author_id"].map(lambda x: nick_map.get(int(x)) if pd.notna(x) else None)

    # post_raw.id -> post_clean.*
    raw_ids: List[int] = df["id"].dropna().astype(int).tolist() if "id" in df.columns else []
    raw_to_clean: Dict[int, int] = {}
    clean_by_id: Dict[int, Dict[str, Any]] = {}
    if raw_ids:
        clean_df = repo.query("post_clean")
        if clean_df is not None and not clean_df.empty and {"id", "post_raw_id"}.issubset(clean_df.columns):
            c = clean_df.copy()
            c["id"] = pd.to_numeric(c["id"], errors="coerce")
            c["post_raw_id"] = pd.to_numeric(c["post_raw_id"], errors="coerce")
            c = c.dropna(subset=["id", "post_raw_id"])
            c["id"] = c["id"].astype(int)
            c["post_raw_id"] = c["post_raw_id"].astype(int)
            c = c.sort_values(by="id").drop_duplicates(subset=["post_raw_id"], keep="last")
            raw_to_clean = dict(zip(c["post_raw_id"].tolist(), c["id"].tolist()))
            df["post_clean_id"] = df["id"].map(raw_to_clean)

            for _, r in c.iterrows():
                cid = int(r["id"])
                clean_by_id[cid] = {
                    "clean_text": _none_if_nan(r.get("clean_text")),
                    "text_hash": _none_if_nan(r.get("text_hash")),
                    "is_valid": int(pd.to_numeric(r.get("is_valid"), errors="coerce") or 0),
                    "invalid_reason": _none_if_nan(r.get("invalid_reason")),
                }
        else:
            df["post_clean_id"] = None

    if "post_clean_id" in df.columns:
        df["clean_text"] = df["post_clean_id"].map(lambda x: clean_by_id.get(int(x), {}).get("clean_text") if pd.notna(x) else None)
        df["text_hash"] = df["post_clean_id"].map(lambda x: clean_by_id.get(int(x), {}).get("text_hash") if pd.notna(x) else None)
        df["clean_is_valid"] = df["post_clean_id"].map(lambda x: clean_by_id.get(int(x), {}).get("is_valid") if pd.notna(x) else None)
        df["clean_invalid_reason"] = df["post_clean_id"].map(lambda x: clean_by_id.get(int(x), {}).get("invalid_reason") if pd.notna(x) else None)

    # sentiment_result.*
    sent_df = repo.query("sentiment_result")
    sent_map: Dict[int, Dict[str, Any]] = {}
    if sent_df is not None and not sent_df.empty and {"post_clean_id", "polarity"}.issubset(sent_df.columns):
        s = sent_df.copy()
        s["post_clean_id"] = pd.to_numeric(s["post_clean_id"], errors="coerce")
        s = s.dropna(subset=["post_clean_id"])
        s["post_clean_id"] = s["post_clean_id"].astype(int)
        if "id" in s.columns:
            s["id"] = pd.to_numeric(s["id"], errors="coerce")
            s = s.sort_values(by="id").drop_duplicates(subset=["post_clean_id"], keep="last")
        for _, r in s.iterrows():
            conf = pd.to_numeric(r.get("confidence"), errors="coerce")
            inten = pd.to_numeric(r.get("intensity"), errors="coerce")
            sent_map[int(r["post_clean_id"])] = {
                "polarity": _none_if_nan(r.get("polarity")),
                "confidence": float(conf) if pd.notna(conf) else None,
                "intensity": float(inten) if pd.notna(inten) else None,
                "emotions": _none_if_nan(r.get("emotions")),
            }
    if "post_clean_id" in df.columns:
        df["polarity"] = df["post_clean_id"].map(lambda x: sent_map.get(int(x), {}).get("polarity") if pd.notna(x) else None)
        df["confidence"] = df["post_clean_id"].map(lambda x: sent_map.get(int(x), {}).get("confidence") if pd.notna(x) else None)
        df["intensity"] = df["post_clean_id"].map(lambda x: sent_map.get(int(x), {}).get("intensity") if pd.notna(x) else None)
        df["emotions"] = df["post_clean_id"].map(lambda x: sent_map.get(int(x), {}).get("emotions") if pd.notna(x) else None)

    # spam_score
    spam_df = repo.query("spam_score")
    spam_map: Dict[int, Dict[str, Any]] = {}
    if spam_df is not None and not spam_df.empty and {"post_clean_id", "label"}.issubset(spam_df.columns):
        sp = spam_df.copy()
        sp["post_clean_id"] = pd.to_numeric(sp["post_clean_id"], errors="coerce")
        sp = sp.dropna(subset=["post_clean_id"])
        sp["post_clean_id"] = sp["post_clean_id"].astype(int)
        if "id" in sp.columns:
            sp["id"] = pd.to_numeric(sp["id"], errors="coerce")
            sp = sp.sort_values(by="id").drop_duplicates(subset=["post_clean_id"], keep="last")
        for _, r in sp.iterrows():
            score = pd.to_numeric(r.get("score_total"), errors="coerce")
            spam_map[int(r["post_clean_id"])] = {
                "spam_score": float(score) if pd.notna(score) else None,
                "spam_label": _none_if_nan(r.get("label")),
                "spam_rule_hits": _none_if_nan(r.get("rule_hits")),
            }
    if "post_clean_id" in df.columns:
        df["spam_score"] = df["post_clean_id"].map(lambda x: spam_map.get(int(x), {}).get("spam_score") if pd.notna(x) else None)
        df["spam_label"] = df["post_clean_id"].map(lambda x: spam_map.get(int(x), {}).get("spam_label") if pd.notna(x) else None)
        df["spam_rule_hits"] = df["post_clean_id"].map(lambda x: spam_map.get(int(x), {}).get("spam_rule_hits") if pd.notna(x) else None)

    # topic_result
    topic_df = repo.query("topic_result")
    topic_map: Dict[int, Dict[str, Any]] = {}
    if topic_df is not None and not topic_df.empty and {"post_clean_id", "topic_name"}.issubset(topic_df.columns):
        tp = topic_df.copy()
        tp["post_clean_id"] = pd.to_numeric(tp["post_clean_id"], errors="coerce")
        tp = tp.dropna(subset=["post_clean_id"])
        tp["post_clean_id"] = tp["post_clean_id"].astype(int)
        if "id" in tp.columns:
            tp["id"] = pd.to_numeric(tp["id"], errors="coerce")
            tp = tp.sort_values(by="id").drop_duplicates(subset=["post_clean_id"], keep="last")
        for _, r in tp.iterrows():
            topic_map[int(r["post_clean_id"])] = {"topic_name": _none_if_nan(r.get("topic_name")), "topic_score": _none_if_nan(r.get("score"))}
    if "post_clean_id" in df.columns:
        df["topic_name"] = df["post_clean_id"].map(lambda x: topic_map.get(int(x), {}).get("topic_name") if pd.notna(x) else None)
        df["topic_score"] = df["post_clean_id"].map(lambda x: topic_map.get(int(x), {}).get("topic_score") if pd.notna(x) else None)

    # entity_result (aggregate)
    ent_df = repo.query("entity_result")
    ent_map: Dict[int, List[Dict[str, Any]]] = {}
    if ent_df is not None and not ent_df.empty and {"post_clean_id", "entity_text"}.issubset(ent_df.columns):
        en = ent_df.copy()
        en["post_clean_id"] = pd.to_numeric(en["post_clean_id"], errors="coerce")
        en = en.dropna(subset=["post_clean_id"])
        en["post_clean_id"] = en["post_clean_id"].astype(int)
        if "id" in en.columns:
            en["id"] = pd.to_numeric(en["id"], errors="coerce")
            en = en.sort_values(by="id", ascending=False)
        for _, r in en.iterrows():
            cid = int(r["post_clean_id"])
            conf = pd.to_numeric(r.get("confidence"), errors="coerce")
            ent_map.setdefault(cid, []).append(
                {
                    "entity_type": _none_if_nan(r.get("entity_type")),
                    "entity_text": _none_if_nan(r.get("entity_text")),
                    "normalized": _none_if_nan(r.get("normalized")),
                    "confidence": float(conf) if pd.notna(conf) else None,
                }
            )
    if "post_clean_id" in df.columns:
        df["entities"] = df["post_clean_id"].map(lambda x: ent_map.get(int(x), []) if pd.notna(x) else [])
        df["entity_texts"] = df["post_clean_id"].map(
            lambda x: ",".join([str(i.get("entity_text")) for i in ent_map.get(int(x), []) if i.get("entity_text")]) if pd.notna(x) else None
        )

    # publish_time to iso
    if "publish_time" in df.columns:
        df["publish_time"] = df["publish_time"].map(_dt_iso)

    # sanitize
    df = df.replace([float("inf"), float("-inf")], None)
    df = df.astype(object).where(pd.notnull(df), None)
    return df


def _filter_posts_df(
    repo,
    *,
    platform_id: Optional[int],
    project_id: Optional[int],
    brand_ids: Optional[List[int]],
    keyword_id: Optional[int],
    days: Optional[int],
    start_date: Optional[str],
    end_date: Optional[str],
    mode: Optional[str],
    polarity: Optional[str],
    spam_label: Optional[str],
    is_valid: Optional[bool],
    intensity_min: Optional[float],
    intensity_max: Optional[float],
    like_min: Optional[int],
    like_max: Optional[int],
    comment_min: Optional[int],
    comment_max: Optional[int],
    share_min: Optional[int],
    share_max: Optional[int],
    q_raw: Optional[str],
    q_clean: Optional[str],
    q_topic: Optional[str],
    q_entity: Optional[str],
) -> pd.DataFrame:
    filters: Dict[str, Any] = {}
    if platform_id is not None:
        filters["platform_id"] = int(platform_id)
    if keyword_id is not None:
        filters["keyword_id"] = int(keyword_id)

    allowed_projects: Optional[Set[int]] = None
    if brand_ids:
        allowed_projects = _projects_for_brands(repo, [int(x) for x in brand_ids if x is not None])

    if project_id is not None:
        allowed_projects = ({int(project_id)} if allowed_projects is None else (allowed_projects & {int(project_id)}))

    if mode == "latest_run":
        latest_run_id = get_latest_pipeline_run_id(repo)
        if latest_run_id is None:
            return pd.DataFrame()
        filters["pipeline_run_id"] = latest_run_id

    if allowed_projects is not None:
        if len(allowed_projects) == 0:
            return pd.DataFrame()
        if len(allowed_projects) == 1:
            filters["project_id"] = next(iter(allowed_projects))
    elif project_id is not None:
        filters["project_id"] = int(project_id)

    df = repo.query("post_raw", filters if filters else None)
    if df is None or df.empty:
        return pd.DataFrame()

    if allowed_projects is not None and "project_id" in df.columns and "project_id" not in filters:
        df = df.copy()
        df["project_id"] = pd.to_numeric(df["project_id"], errors="coerce")
        df = df.dropna(subset=["project_id"])
        df = df[df["project_id"].astype(int).isin(list(allowed_projects))]

    start_dt, end_dt = _parse_range(days, start_date, end_date)
    if (start_dt or end_dt) and "publish_time" in df.columns:
        df = df.copy()
        df["publish_time"] = pd.to_datetime(df["publish_time"], errors="coerce")
        df = df.dropna(subset=["publish_time"])
        if start_dt:
            df = df[df["publish_time"] >= start_dt]
        if end_dt:
            df = df[df["publish_time"] <= end_dt]

    df = _enrich_posts(repo, df)

    if polarity:
        pol = str(polarity).lower()
        df = df[df.get("polarity").astype(str).str.lower() == pol]
    if spam_label:
        lab = str(spam_label).lower()
        df = df[df.get("spam_label").astype(str).str.lower() == lab]
    if is_valid is not None:
        target = 1 if bool(is_valid) else 0
        df["clean_is_valid"] = pd.to_numeric(df.get("clean_is_valid"), errors="coerce").fillna(0).astype(int)
        df = df[df["clean_is_valid"] == target]

    if intensity_min is not None:
        df["intensity"] = pd.to_numeric(df.get("intensity"), errors="coerce")
        df = df[df["intensity"].notna() & (df["intensity"] >= float(intensity_min))]
    if intensity_max is not None:
        df["intensity"] = pd.to_numeric(df.get("intensity"), errors="coerce")
        df = df[df["intensity"].notna() & (df["intensity"] <= float(intensity_max))]

    for col, lo, hi in [
        ("like_count", like_min, like_max),
        ("comment_count", comment_min, comment_max),
        ("share_count", share_min, share_max),
    ]:
        if col in df.columns and (lo is not None or hi is not None):
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            if lo is not None:
                df = df[df[col] >= int(lo)]
            if hi is not None:
                df = df[df[col] <= int(hi)]

    def _contains(series: pd.Series, q: str) -> pd.Series:
        q2 = str(q).strip().lower()
        if not q2:
            return pd.Series([True] * len(series), index=series.index)
        return series.astype(str).str.lower().str.contains(q2, na=False)

    if q_raw and "raw_text" in df.columns:
        df = df[_contains(df["raw_text"], q_raw)]
    if q_clean and "clean_text" in df.columns:
        df = df[_contains(df["clean_text"], q_clean)]
    if q_topic and "topic_name" in df.columns:
        df = df[_contains(df["topic_name"], q_topic)]
    if q_entity and "entity_texts" in df.columns:
        df = df[_contains(df["entity_texts"], q_entity)]

    if "publish_time" in df.columns:
        df["_pt"] = pd.to_datetime(df["publish_time"], errors="coerce")
        df = df.sort_values(by=["_pt", "id"], ascending=[False, False], na_position="last")
        df = df.drop(columns=["_pt"])
    elif "id" in df.columns:
        df = df.sort_values(by="id", ascending=False)

    df = df.replace([float("inf"), float("-inf")], None)
    df = df.astype(object).where(pd.notnull(df), None)
    return df


# Backward compatible: returns an array
@router.get("")
def list_posts(
    platform_id: Optional[int] = Query(None),
    project_id: Optional[int] = Query(None),
    brand_ids: Optional[List[int]] = Query(None, description="Filter by brand ids (repeatable)."),
    days: Optional[int] = Query(None, ge=1, le=365, description="Relative time range: last N days."),
    start_date: Optional[str] = Query(None, description="Custom date range start (YYYY-MM-DD)."),
    end_date: Optional[str] = Query(None, description="Custom date range end (YYYY-MM-DD)."),
    mode: Optional[str] = Query("latest_run"),
    user=Depends(get_current_user),
):
    repo = get_repo()
    df = _filter_posts_df(
        repo,
        platform_id=platform_id,
        project_id=project_id,
        brand_ids=brand_ids,
        keyword_id=None,
        days=days,
        start_date=start_date,
        end_date=end_date,
        mode=mode,
        polarity=None,
        spam_label=None,
        is_valid=None,
        intensity_min=None,
        intensity_max=None,
        like_min=None,
        like_max=None,
        comment_min=None,
        comment_max=None,
        share_min=None,
        share_max=None,
        q_raw=None,
        q_clean=None,
        q_topic=None,
        q_entity=None,
    )
    if df is None or df.empty:
        return []
    return df.to_dict(orient="records")


@router.get("/page")
def list_posts_page(
    platform_id: Optional[int] = Query(None),
    project_id: Optional[int] = Query(None),
    brand_ids: Optional[List[int]] = Query(None),
    keyword_id: Optional[int] = Query(None),
    days: Optional[int] = Query(None, ge=1, le=365),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    mode: Optional[str] = Query("latest_run"),
    polarity: Optional[str] = Query(None, description="positive|neutral|negative"),
    spam_label: Optional[str] = Query(None, description="normal|spam|suspect"),
    is_valid: Optional[bool] = Query(None),
    intensity_min: Optional[float] = Query(None, ge=0, le=1),
    intensity_max: Optional[float] = Query(None, ge=0, le=1),
    like_min: Optional[int] = Query(None, ge=0),
    like_max: Optional[int] = Query(None, ge=0),
    comment_min: Optional[int] = Query(None, ge=0),
    comment_max: Optional[int] = Query(None, ge=0),
    share_min: Optional[int] = Query(None, ge=0),
    share_max: Optional[int] = Query(None, ge=0),
    q_raw: Optional[str] = Query(None),
    q_clean: Optional[str] = Query(None),
    q_topic: Optional[str] = Query(None),
    q_entity: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=5, le=100),
    user=Depends(get_current_user),
):
    repo = get_repo()
    df = _filter_posts_df(
        repo,
        platform_id=platform_id,
        project_id=project_id,
        brand_ids=brand_ids,
        keyword_id=keyword_id,
        days=days,
        start_date=start_date,
        end_date=end_date,
        mode=mode,
        polarity=polarity,
        spam_label=spam_label,
        is_valid=is_valid,
        intensity_min=intensity_min,
        intensity_max=intensity_max,
        like_min=like_min,
        like_max=like_max,
        comment_min=comment_min,
        comment_max=comment_max,
        share_min=share_min,
        share_max=share_max,
        q_raw=q_raw,
        q_clean=q_clean,
        q_topic=q_topic,
        q_entity=q_entity,
    )
    total = int(len(df)) if df is not None and not df.empty else 0
    if total == 0:
        return {"total": 0, "items": [], "page": page, "page_size": page_size}
    start_idx = (int(page) - 1) * int(page_size)
    end_idx = start_idx + int(page_size)
    items_df = df.iloc[start_idx:end_idx]
    return {"total": total, "items": items_df.to_dict(orient="records"), "page": page, "page_size": page_size}


@router.get("/stats")
def get_posts_stats(
    platform_id: Optional[int] = Query(None),
    project_id: Optional[int] = Query(None),
    brand_ids: Optional[List[int]] = Query(None),
    keyword_id: Optional[int] = Query(None),
    days: Optional[int] = Query(None, ge=1, le=365),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    mode: Optional[str] = Query("latest_run"),
    polarity: Optional[str] = Query(None),
    spam_label: Optional[str] = Query(None),
    is_valid: Optional[bool] = Query(None),
    intensity_min: Optional[float] = Query(None, ge=0, le=1),
    intensity_max: Optional[float] = Query(None, ge=0, le=1),
    like_min: Optional[int] = Query(None, ge=0),
    like_max: Optional[int] = Query(None, ge=0),
    comment_min: Optional[int] = Query(None, ge=0),
    comment_max: Optional[int] = Query(None, ge=0),
    share_min: Optional[int] = Query(None, ge=0),
    share_max: Optional[int] = Query(None, ge=0),
    q_raw: Optional[str] = Query(None),
    q_clean: Optional[str] = Query(None),
    q_topic: Optional[str] = Query(None),
    q_entity: Optional[str] = Query(None),
    user=Depends(get_current_user),
):
    repo = get_repo()
    df = _filter_posts_df(
        repo,
        platform_id=platform_id,
        project_id=project_id,
        brand_ids=brand_ids,
        keyword_id=keyword_id,
        days=days,
        start_date=start_date,
        end_date=end_date,
        mode=mode,
        polarity=polarity,
        spam_label=spam_label,
        is_valid=is_valid,
        intensity_min=intensity_min,
        intensity_max=intensity_max,
        like_min=like_min,
        like_max=like_max,
        comment_min=comment_min,
        comment_max=comment_max,
        share_min=share_min,
        share_max=share_max,
        q_raw=q_raw,
        q_clean=q_clean,
        q_topic=q_topic,
        q_entity=q_entity,
    )
    if df is None or df.empty:
        return {"total_posts": 0, "valid_posts": 0, "negative_posts": 0, "spam_posts": 0, "hot_topics": 0, "entities": 0}

    valid = pd.to_numeric(df.get("clean_is_valid"), errors="coerce").fillna(0).astype(int) if "clean_is_valid" in df.columns else pd.Series([0] * len(df))
    pol = df.get("polarity").astype(str).str.lower() if "polarity" in df.columns else pd.Series([""] * len(df))
    spam = df.get("spam_label").astype(str).str.lower() if "spam_label" in df.columns else pd.Series([""] * len(df))
    topic_unique = df.get("topic_name").dropna().astype(str).nunique() if "topic_name" in df.columns else 0

    ent_unique = 0
    if "entities" in df.columns:
        seen = set()
        for items in df["entities"].tolist():
            for it in (items or []):
                txt = it.get("entity_text")
                if txt:
                    seen.add(str(txt))
        ent_unique = len(seen)

    return {
        "total_posts": int(len(df)),
        "valid_posts": int((valid == 1).sum()),
        "negative_posts": int((pol == "negative").sum()),
        "spam_posts": int(spam.isin(["spam", "suspect"]).sum()),
        "hot_topics": int(topic_unique),
        "entities": int(ent_unique),
    }


@router.get("/keywords")
def list_keywords(project_id: int = Query(..., ge=1), user=Depends(get_current_user)):
    repo = get_repo()
    df = repo.query("monitor_keyword", {"project_id": int(project_id)})
    if df is None or df.empty or "id" not in df.columns:
        return []
    df = df.copy()
    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    df = df.dropna(subset=["id"])
    df["id"] = df["id"].astype(int)
    if "is_active" in df.columns:
        df["is_active"] = pd.to_numeric(df["is_active"], errors="coerce").fillna(0).astype(int)
        df = df[df["is_active"] == 1]
    out = []
    for _, r in df.sort_values(by="id").iterrows():
        out.append({"id": int(r["id"]), "keyword": str(_none_if_nan(r.get("keyword")) or "")})
    return out


@router.get("/{post_id}/linked_reports")
def linked_reports(post_id: int, user=Depends(get_current_user)):
    repo = get_repo()
    cit = repo.query("report_citation")
    rep = repo.query("report")
    if cit is None or cit.empty or rep is None or rep.empty:
        return []
    if not {"post_raw_id", "report_id"}.issubset(cit.columns):
        return []
    c = cit.copy()
    c["post_raw_id"] = pd.to_numeric(c["post_raw_id"], errors="coerce")
    c["report_id"] = pd.to_numeric(c["report_id"], errors="coerce")
    c = c.dropna(subset=["post_raw_id", "report_id"])
    c["post_raw_id"] = c["post_raw_id"].astype(int)
    c["report_id"] = c["report_id"].astype(int)
    c = c[c["post_raw_id"] == int(post_id)]
    if c.empty:
        return []

    rep2 = rep.copy()
    rep2["id"] = pd.to_numeric(rep2.get("id"), errors="coerce")
    rep2 = rep2.dropna(subset=["id"])
    rep2["id"] = rep2["id"].astype(int)
    rep2 = rep2[rep2["id"].isin(c["report_id"].unique().tolist())]
    out = []
    for _, r in rep2.sort_values(by="id", ascending=False).iterrows():
        out.append({"id": int(r.get("id")), "title": _none_if_nan(r.get("title")), "reportType": _none_if_nan(r.get("report_type")), "createdAt": _dt_iso(r.get("created_at"))})
    return out


@router.get("/{post_id}")
def get_post(post_id: int, user=Depends(get_current_user)):
    repo = get_repo()
    df = repo.query("post_raw", {"id": int(post_id)})
    if df is None or df.empty:
        return {"id": int(post_id), "detail": "not found"}
    df = _enrich_posts(repo, df)
    if df is None or df.empty:
        return {"id": int(post_id), "detail": "not found"}
    return df.iloc[0].to_dict()

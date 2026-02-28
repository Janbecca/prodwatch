from datetime import datetime
from typing import Any, Dict
import pandas as pd


def _now_ts_ms() -> int:
    return int(datetime.utcnow().timestamp() * 1000)


def _normalize_str(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def import_from_excel(repo, excel_path: str) -> Dict[str, Any]:
    upload_df = pd.read_excel(excel_path, sheet_name="post_raw_upload")
    if upload_df.empty:
        return {"imported": 0, "skipped": 0, "pipeline_run_id": None}

    now = datetime.utcnow()
    run_id = _now_ts_ms()
    first_project_id = upload_df["project_id"].dropna().iloc[0] if "project_id" in upload_df.columns else None
    run_no = now.strftime("%Y%m%d%H%M%S")

    repo.insert(
        "pipeline_run",
        {
            "id": run_id,
            "project_id": int(first_project_id) if pd.notna(first_project_id) else None,
            "run_no": run_no,
            "trigger_type": "import_excel",
            "status": "finished",
            "start_time": now,
            "end_time": now,
            "params": None,
            "created_at": now,
        },
    )

    platform_df = repo.query("platform")
    platform_map = {}
    if "code" in platform_df.columns and "id" in platform_df.columns:
        for _, row in platform_df.iterrows():
            code = _normalize_str(row.get("code"))
            if code:
                platform_map[code] = int(row.get("id"))

    keyword_df = repo.query("monitor_keyword")
    keyword_map = {}
    if "keyword" in keyword_df.columns and "id" in keyword_df.columns:
        for _, row in keyword_df.iterrows():
            kw = _normalize_str(row.get("keyword"))
            if kw:
                keyword_map[kw] = int(row.get("id"))

    existing_raw = repo.query("post_raw")
    existing_keys = set()
    if not existing_raw.empty:
        for _, row in existing_raw.iterrows():
            key = (
                row.get("project_id"),
                row.get("platform_id"),
                _normalize_str(row.get("platform_post_id")),
            )
            existing_keys.add(key)

    imported = 0
    skipped = 0
    next_id = _now_ts_ms()

    for _, row in upload_df.iterrows():
        project_id = row.get("project_id")
        platform_code = _normalize_str(row.get("platform_code"))
        platform_id = platform_map.get(platform_code)
        if platform_id is None:
            skipped += 1
            continue

        keyword = _normalize_str(row.get("keyword"))
        keyword_id = None
        if keyword:
            if keyword in keyword_map:
                keyword_id = keyword_map[keyword]
            else:
                new_id = _now_ts_ms()
                repo.insert(
                    "monitor_keyword",
                    {
                        "id": new_id,
                        "project_id": int(project_id) if pd.notna(project_id) else None,
                        "keyword": keyword,
                        "keyword_type": None,
                        "weight": None,
                        "is_active": 1,
                        "created_at": now,
                    },
                )
                keyword_map[keyword] = new_id
                keyword_id = new_id

        platform_post_id = _normalize_str(row.get("platform_post_id"))
        if not platform_post_id:
            platform_post_id = f"{platform_code}_{next_id}"

        dedup_key = (project_id, platform_id, platform_post_id)
        if dedup_key in existing_keys:
            skipped += 1
            continue

        publish_time = row.get("publish_time")
        raw_text = row.get("raw_text")
        like_count = row.get("like_count", 0)
        comment_count = row.get("comment_count", 0)
        share_count = row.get("share_count", 0)

        repo.insert(
            "post_raw",
            {
                "id": next_id,
                "pipeline_run_id": run_id,
                "project_id": int(project_id) if pd.notna(project_id) else None,
                "platform_id": platform_id,
                "keyword_id": keyword_id,
                "content_type": "post",
                "platform_post_id": platform_post_id,
                "author_id": None,
                "publish_time": publish_time,
                "raw_text": raw_text,
                "like_count": like_count,
                "comment_count": comment_count,
                "share_count": share_count,
            },
        )
        existing_keys.add(dedup_key)
        imported += 1
        next_id += 1

    return {"imported": imported, "skipped": skipped, "pipeline_run_id": run_id}

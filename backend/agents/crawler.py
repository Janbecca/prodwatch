from datetime import datetime
from typing import Dict, List

import pandas as pd

from backend.storage.db import get_repo

# Demo-only: platform adapters are mocked for now.
SUPPORTED_PLATFORMS: Dict[str, str] = {
    "weibo": "mock://weibo",
    "xhs": "mock://xhs",
    "douyin": "mock://douyin",
}


def _now_ts_ms() -> int:
    return int(datetime.utcnow().timestamp() * 1000)


def crawl(project_id: int, platform_codes: List[str], keyword: str, pipeline_run_id: int) -> List[dict]:
    """
    Mock crawler: generates one post per platform code and writes into `post_raw`.

    Manual fill / next step:
    - Replace this with real platform adapters and store raw payload fields.
    """
    repo = get_repo()
    results: List[dict] = []
    now = datetime.utcnow()

    platform_df = repo.query("platform")
    platform_map: Dict[str, int] = {}
    if not platform_df.empty and {"code", "id"}.issubset(platform_df.columns):
        for _, p in platform_df.iterrows():
            code = str(p.get("code") or "").strip()
            pid = pd.to_numeric(p.get("id"), errors="coerce")
            if code and pd.notna(pid):
                platform_map[code] = int(pid)

    base_id = _now_ts_ms()
    seq = 0
    for code in platform_codes:
        if code not in platform_map:
            continue
        if code not in SUPPORTED_PLATFORMS:
            continue

        seq += 1
        raw_id = base_id + seq
        mock_text = f"【示例】关键词“{keyword}”在 {code} 的样例内容（mock crawler）"
        row = {
            "id": raw_id,
            "pipeline_run_id": pipeline_run_id,
            "project_id": project_id,
            "platform_id": platform_map[code],
            "keyword_id": None,
            "content_type": "post",
            "platform_post_id": f"{code}_{raw_id}",
            "author_id": None,
            "publish_time": now,
            "raw_text": mock_text,
            "like_count": 0,
            "comment_count": 0,
            "share_count": 0,
        }
        repo.insert("post_raw", row)
        results.append(row)

    return results


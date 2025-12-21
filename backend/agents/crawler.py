import requests
from datetime import datetime
from typing import List
from backend.storage.db import get_repo

PLATFORMS = {
    "weibo": "https://example.com/weibo/mock",
    "xhs": "https://example.com/xhs/mock",
    "douyin": "https://example.com/douyin/mock",
}

def crawl(project_id: int, platform_codes: List[str], keyword: str, pipeline_run_id: int):
    repo = get_repo()
    results = []
    now = datetime.utcnow()

    for code in platform_codes:
        # TODO: replace with real crawler
        mock_text = f"{keyword} 在 {code} 的样例内容"
        row = {
            "id": int(now.timestamp()),
            "pipeline_run_id": pipeline_run_id,
            "project_id": project_id,
            "platform_id": code,
            "keyword_id": None,
            "content_type": "post",
            "platform_post_id": f"{code}_{int(now.timestamp())}",
            "author_id": None,
            "publish_time": now,
            "raw_text": mock_text,
            "like_count": 0,
            "comment_count": 0,
            "share_count": 0
        }
        repo.insert("post_raw", row)
        results.append(row)

    return results

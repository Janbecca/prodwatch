import requests
from datetime import datetime
from typing import List
from backend.storage.db import get_repo
import pandas as pd

# 支持的平台：暂时地址是 mock 的
PLATFORMS = {
    "weibo": "https://example.com/weibo/mock",
    "xhs": "https://example.com/xhs/mock",
    "douyin": "https://example.com/douyin/mock",
}


def crawl(project_id: int, platform_codes: List[str], keyword: str, pipeline_run_id: int):
    repo = get_repo()  # 获取数据库操作对象
    results = []
    now = datetime.utcnow()  # 获取当前 UTC 时间
    platform_df = repo.query("platform")
    platform_map = {}
    if "code" in platform_df.columns and "id" in platform_df.columns:
        for _, p in platform_df.iterrows():
            code = str(p.get("code")).strip()
            pid = pd.to_numeric(p.get("id"), errors="coerce")
            if code and pd.notna(pid):
                platform_map[code] = int(pid)

    for code in platform_codes:  # 遍历传入的平台列表（如 weibo, xhs）
        # TODO: 实际开发中应替换为真实的爬虫逻辑
        if code not in platform_map:
            continue
        mock_text = f"{keyword} 在 {code} 的样例内容"  # 模拟爬取到的文本
        row = {
            "id": int(now.timestamp()),
            "pipeline_run_id": pipeline_run_id,
            "project_id": project_id,
            "platform_id": platform_map[code],
            "keyword_id": None,
            "content_type": "post",
            "platform_post_id": f"{code}_{int(now.timestamp())}",
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

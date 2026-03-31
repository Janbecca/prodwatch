from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from backend.llm.router import get_llm_router


@dataclass(frozen=True)
class CrawlContext:
    project_id: int
    crawl_job_id: int
    stat_date: str
    posts_per_target: int
    platform_id: int
    brand_id: int
    keyword: str
    target_id: int
    platform_code: str
    brand_name: str


class CrawlerGenerationService:
    """
    Service for simulated crawler generation.

    Uses LLM router with task_type=crawler_generation so you can swap providers/models later.
    """

    def generate_posts(self, ctx: CrawlContext, *, con=None) -> list[dict[str, Any]]:
        crawled_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        res = get_llm_router().run(
            task_type="crawler_generation",
            input={
                "project_id": ctx.project_id,
                "platform_id": ctx.platform_id,
                "brand_id": ctx.brand_id,
                "keyword": ctx.keyword,
                "platform_code": ctx.platform_code,
                "brand_name": ctx.brand_name,
                "stat_date": ctx.stat_date,
                "posts_per_target": ctx.posts_per_target,
                "target_id": ctx.target_id,
                "crawled_at": crawled_at,
            },
            con=con,
        )
        posts = (res.output or {}).get("posts") or []

        out: list[dict[str, Any]] = []
        for p in posts:
            post_url = str(p.get("post_url") or "")
            dedup_key = sha1_hex(post_url) if post_url else sha1_hex(json.dumps(p, ensure_ascii=False))
            out.append(
                {
                    "project_id": int(ctx.project_id),
                    "crawl_job_id": int(ctx.crawl_job_id),
                    "platform_id": int(p.get("platform_id") or ctx.platform_id),
                    "brand_id": int(p.get("brand_id") or ctx.brand_id),
                    "external_post_id": str(p.get("external_post_id") or ""),
                    "author_name": str(p.get("author_name") or ""),
                    "title": str(p.get("title") or ""),
                    "content": str(p.get("content") or ""),
                    "post_url": post_url,
                    "publish_time": str(p.get("publish_time") or crawled_at),
                    "crawled_at": str(p.get("crawled_at") or crawled_at),
                    "like_count": int(p.get("like_count") or 0),
                    "comment_count": int(p.get("comment_count") or 0),
                    "share_count": int(p.get("share_count") or 0),
                    "view_count": int(p.get("view_count") or 0),
                    "raw_payload": str(p.get("raw_payload") or ""),
                    "dedup_key": dedup_key,
                    "created_at": str(p.get("crawled_at") or crawled_at),
                }
            )
        return out


_svc: Optional[CrawlerGenerationService] = None


def get_crawler_generation_service() -> CrawlerGenerationService:
    global _svc
    if _svc is None:
        _svc = CrawlerGenerationService()
    return _svc


def sha1_hex(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()

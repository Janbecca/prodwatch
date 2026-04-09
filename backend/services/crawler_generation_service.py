# 作用：后端服务层：爬虫生成相关业务逻辑封装。

from __future__ import annotations

import hashlib
import json
import logging
import random
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional

from backend.llm.router import get_llm_router
from backend.llm.prompts.store import get_prompt_store


log = logging.getLogger("prodwatch.crawler_generation")

_FORBIDDEN_FIELD_STITCH_RE = re.compile(r"\b(topic|brand|feature|feeling)\s*=")


def _pick_style(platform_code: str) -> str:
    c = str(platform_code or "").strip().lower()
    if c in {"zhihu", "知乎"}:
        return "zhihu"
    if c in {"weibo", "微博"}:
        return "weibo"
    if c in {"douyin", "抖音"}:
        return "douyin"
    return "weibo"


def _sentiment_words(polarity: str) -> tuple[str, str]:
    p = str(polarity or "").strip().lower()
    if p in {"good", "positive"}:
        return ("挺满意", "体验不错")
    if p in {"bad", "negative"}:
        return ("有点失望", "体验一般")
    return ("还行", "中规中矩")


def _render_natural_cn_comment(*, style: str, brand: str, keyword: str, feature_term: str, polarity: str) -> str:
    """
    Deterministic/mock fallback should still look like real user comments.
    Must NOT contain topic=/brand=/feature=/feeling= stitched fields.
    """
    style = str(style or "weibo")
    brand = str(brand or "").strip() or "某品牌"
    keyword = str(keyword or "").strip() or "相关体验"
    feature_term = str(feature_term or "").strip() or "体验"
    mood, tone = _sentiment_words(polarity)

    if style == "douyin":
        pool = [
            f"{brand}这{keyword}真{tone}",
            f"{keyword}这块{mood}，{brand}还得加油",
            f"{brand}的{feature_term}我是真服了，{mood}",
            f"{keyword}就图个{tone}，别太较真",
        ]
        return random.choice(pool)

    if style == "weibo":
        pool = [
            f"刚刷到一堆{brand}的{keyword}讨论，{feature_term}这块我感觉{mood}…",
            f"{brand}的{keyword}最近被说得挺多，实际用下来{tone}，但也有小问题。",
            f"{keyword}这点上，{brand}做得{tone}，希望后续优化更稳一点！",
            f"说真的，{brand}的{feature_term}我{mood}，期待下一版能更好。",
        ]
        return random.choice(pool)

    # zhihu: longer and more structured.
    parts = [
        f"最近集中体验了一下{brand}的产品/系统，刚好也在关注「{keyword}」这个点。",
        f"先说结论：整体{tone}，但{feature_term}相关的细节会影响观感。",
        "如果你对这个点比较敏感，建议结合自己的使用场景判断（游戏/刷视频/办公等）。",
        "希望后续系统更新能把边界场景也处理得更稳，至少不要出现明显波动。",
    ]
    text = " ".join(parts)
    if len(text) < 110:
        text += f" 总体来说，我对{brand}在{keyword}方面的表现是{mood}的。"
    return text[:800]


def _merge_raw_payload(raw_payload: Any, extra: dict[str, Any]) -> str:
    """
    raw_payload is stored as TEXT in DB. Keep backward compatibility by always returning a string.
    If raw_payload looks like JSON, merge fields; otherwise wrap it in {"raw": "..."}.
    """
    base: dict[str, Any] = {}
    try:
        if isinstance(raw_payload, str) and raw_payload.strip().startswith("{"):
            base = json.loads(raw_payload)
        elif raw_payload:
            base = {"raw": str(raw_payload)}
    except Exception:
        base = {"raw": str(raw_payload or "")}
    base.update(extra or {})
    return json.dumps(base, ensure_ascii=False, default=str)


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
        """
        Generate simulated posts for a single crawl_job_target.

        Fallback strategy (must be stable for refresh pipeline):
        1) LLMRouter primary + configured fallback (typically mock)
        2) Deterministic fallback (always available)
        """
        crawled_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        posts_raw: list[Any] = []
        router_ok: Optional[bool] = None
        router_provider: Optional[str] = None
        router_model: Optional[str] = None
        prompt_version: str = get_prompt_store().get("crawler_generation").version
        try:
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
            router_ok = bool(res.ok)
            router_provider = str(res.provider or "") if hasattr(res, "provider") else None
            router_model = str(res.model or "") if hasattr(res, "model") else None
            maybe = (res.output or {}).get("posts") if isinstance(res.output, dict) else None
            if isinstance(maybe, list):
                posts_raw = maybe
        except Exception:
            posts_raw = []

        generated_by = "mock" if (router_provider or "").strip().lower() == "mock" else "llm"
        out = self._normalize_posts(
            ctx,
            posts_raw,
            crawled_at=crawled_at,
            generated_by=generated_by,
            provider=router_provider,
            model=router_model,
            prompt_version=prompt_version,
        )
        need = max(0, int(ctx.posts_per_target) - len(out))
        if need > 0:
            log.warning(
                "crawler_generation deterministic_fallback target_id=%s project_id=%s provider=%s model=%s ok=%s need=%s got=%s",
                ctx.target_id,
                ctx.project_id,
                router_provider,
                router_model,
                router_ok,
                need,
                len(out),
            )
            out.extend(
                self._deterministic_posts(
                    ctx,
                    count=need,
                    start_index=len(out),
                    crawled_at=crawled_at,
                    provider=router_provider,
                    model=router_model,
                    prompt_version=prompt_version,
                )
            )
        return out[: max(0, int(ctx.posts_per_target))]

    def _normalize_posts(
        self,
        ctx: CrawlContext,
        posts: list[Any],
        *,
        crawled_at: str,
        generated_by: str,
        provider: Optional[str],
        model: Optional[str],
        prompt_version: str,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for idx, item in enumerate(posts or []):
            if not isinstance(item, dict):
                continue

            platform_id = _safe_int(item.get("platform_id"), ctx.platform_id)
            brand_id = _safe_int(item.get("brand_id"), ctx.brand_id)

            external_post_id = str(item.get("external_post_id") or "").strip()
            if not external_post_id:
                # Include crawl_job_id to avoid repeated manual refresh generating identical IDs
                # (which would be ignored by INSERT OR IGNORE and appear as "no new data").
                external_post_id = sha1_hex(f"{ctx.project_id}|{ctx.crawl_job_id}|{ctx.target_id}|{ctx.stat_date}|{idx}")[:18]

            post_url = str(item.get("post_url") or "").strip()
            if not post_url:
                post_url = f"https://example.local/{ctx.platform_code}/post/{external_post_id}"

            title = str(item.get("title") or "").strip()
            if not title:
                title = f"{ctx.brand_name} {ctx.keyword} 体验分享"

            content = str(item.get("content") or "").strip()
            if not content:
                style = _pick_style(ctx.platform_code)
                content = _render_natural_cn_comment(
                    style=style,
                    brand=ctx.brand_name,
                    keyword=ctx.keyword,
                    feature_term="体验",
                    polarity="ok",
                )
            sanitized = False
            # Safety net: if a model returns stitched fields, rewrite it to a natural Chinese comment.
            if _FORBIDDEN_FIELD_STITCH_RE.search(content):
                sanitized = True
                style = _pick_style(ctx.platform_code)
                content = _render_natural_cn_comment(
                    style=style,
                    brand=ctx.brand_name,
                    keyword=ctx.keyword,
                    feature_term="体验",
                    polarity="ok",
                )

            publish_time = _norm_time(item.get("publish_time"), default=_default_publish_time(ctx, idx))

            raw_payload = item.get("raw_payload")
            if raw_payload is None:
                raw_payload = item
            if not isinstance(raw_payload, str):
                try:
                    raw_payload = json.dumps(raw_payload, ensure_ascii=False, default=str)
                except Exception:
                    raw_payload = str(raw_payload)
            raw_payload = _merge_raw_payload(
                raw_payload,
                {
                    # Observability: allow downstream debugging of generation source.
                    "generated_by": str(generated_by),
                    "provider": str(provider or ""),
                    "model": str(model or ""),
                    "prompt_version": str(prompt_version or ""),
                    **({"sanitized": True} if sanitized else {}),
                },
            )

            dedup_key = sha1_hex(post_url)
            out.append(
                {
                    "project_id": int(ctx.project_id),
                    "crawl_job_id": int(ctx.crawl_job_id),
                    "platform_id": int(platform_id),
                    "brand_id": int(brand_id),
                    "external_post_id": external_post_id,
                    "author_name": str(item.get("author_name") or f"user_{sha1_hex(post_url)[:6]}"),
                    "title": title,
                    "content": content,
                    "post_url": post_url,
                    "publish_time": publish_time,
                    "crawled_at": _norm_time(item.get("crawled_at"), default=crawled_at),
                    "like_count": _safe_int(item.get("like_count"), 0),
                    "comment_count": _safe_int(item.get("comment_count"), 0),
                    "share_count": _safe_int(item.get("share_count"), 0),
                    "view_count": _safe_int(item.get("view_count"), 0),
                    "raw_payload": str(raw_payload or ""),
                    "dedup_key": dedup_key,
                    "created_at": crawled_at,
                }
            )
            if len(out) >= int(ctx.posts_per_target):
                break
        return out

    def _deterministic_posts(
        self,
        ctx: CrawlContext,
        *,
        count: int,
        start_index: int,
        crawled_at: str,
        provider: Optional[str],
        model: Optional[str],
        prompt_version: str,
    ) -> list[dict[str, Any]]:
        """
        Stable deterministic fallback (official long-term fallback, not temporary).
        """
        n = max(0, int(count))
        if n <= 0:
            return []
        # Use Chinese feature terms so fallback content reads naturally in UI.
        feature_terms = ["续航", "拍照", "价格", "系统流畅度", "发热", "售后"]
        out: list[dict[str, Any]] = []
        for j in range(n):
            i = int(start_index) + j
            publish_time = _default_publish_time(ctx, i)
            # Include crawl_job_id to avoid duplicates across repeated refresh runs on the same day/scope.
            external_post_id = sha1_hex(
                f"{ctx.project_id}|{ctx.crawl_job_id}|{ctx.target_id}|{ctx.platform_code}|{ctx.brand_id}|{ctx.keyword}|{ctx.stat_date}|{i}"
            )[
                :18
            ]
            post_url = f"https://example.local/{ctx.platform_code}/post/{external_post_id}"
            dedup_key = sha1_hex(post_url)

            feature_term = feature_terms[(int(ctx.target_id) + i) % len(feature_terms)]
            polarity = "good" if (int(ctx.target_id) + i) % 3 == 0 else ("ok" if (int(ctx.target_id) + i) % 3 == 1 else "bad")
            title = f"{ctx.brand_name} {ctx.keyword} 体验分享"
            style = _pick_style(ctx.platform_code)
            content = _render_natural_cn_comment(
                style=style,
                brand=ctx.brand_name,
                keyword=ctx.keyword,
                feature_term=feature_term,
                polarity=polarity,
            )
            if _FORBIDDEN_FIELD_STITCH_RE.search(content):
                content = f"聊聊{ctx.brand_name}的{ctx.keyword}：整体{_sentiment_words(polarity)[1]}，但也有改进空间。"
            author_name = f"user_{sha1_hex(f'{ctx.platform_code}|{ctx.brand_id}|{ctx.target_id}|{i}')[:6]}"

            raw_payload = _merge_raw_payload(
                json.dumps(
                    {
                        "platform": ctx.platform_code,
                        "platform_id": ctx.platform_id,
                        "brand_id": ctx.brand_id,
                        "keyword": ctx.keyword,
                        "target_id": ctx.target_id,
                        "stat_date": ctx.stat_date,
                        "fallback": "deterministic",
                        "idx": i,
                    },
                    ensure_ascii=False,
                ),
                {
                    "generated_by": "deterministic_fallback",
                    "provider": str(provider or ""),
                    "model": str(model or ""),
                    "prompt_version": str(prompt_version or ""),
                },
            )

            out.append(
                {
                    "project_id": int(ctx.project_id),
                    "crawl_job_id": int(ctx.crawl_job_id),
                    "platform_id": int(ctx.platform_id),
                    "brand_id": int(ctx.brand_id),
                    "external_post_id": external_post_id,
                    "author_name": author_name,
                    "title": title,
                    "content": content,
                    "post_url": post_url,
                    "publish_time": publish_time,
                    "crawled_at": crawled_at,
                    "like_count": 10 + (int(ctx.target_id) + i) % 30,
                    "comment_count": 2 + (int(ctx.target_id) + 2 * i) % 15,
                    "share_count": (int(ctx.target_id) + i) % 7,
                    "view_count": 50 + (int(ctx.target_id) + i) % 500,
                    "raw_payload": raw_payload,
                    "dedup_key": dedup_key,
                    "created_at": crawled_at,
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


def _safe_int(v: Any, default: int) -> int:
    try:
        n = int(v)  # type: ignore[arg-type]
        return n
    except Exception:
        return int(default)


def _norm_time(v: Any, *, default: str) -> str:
    s = str(v or "").strip()
    if not s:
        return str(default)
    # Best-effort: keep as-is (SQLite stores text); truncate to seconds precision.
    return s[:19]


def _default_publish_time(ctx: CrawlContext, idx: int) -> str:
    try:
        base = datetime.strptime(str(ctx.stat_date), "%Y-%m-%d")
    except Exception:
        base = datetime.utcnow()
    dt = base + timedelta(minutes=7 * int(idx) + (int(ctx.target_id) % 5))
    return dt.strftime("%Y-%m-%d %H:%M:%S")

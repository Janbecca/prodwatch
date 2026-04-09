# 作用：LLM：模型提供方实现（Mock 提供方（用于开发/测试））。

from __future__ import annotations

import json
import hashlib
import random
import re
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Any, Optional

from backend.llm.types import LLMTaskRequest, LLMTaskResponse
from backend.report_chain_e import llm_mock_generate_markdown


_FORBIDDEN_FIELD_STITCH_RE = re.compile(r"\b(topic|brand|feature|feeling)\s*=")


def sha1_hex(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


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
    """
    Map the pipeline polarity to (overall sentiment hint, mild tone).
    This is only used for mock content generation and should stay stable.
    """
    p = str(polarity or "").strip().lower()
    if p in {"good", "positive"}:
        return ("挺满意", "体验不错")
    if p in {"bad", "negative"}:
        return ("有点失望", "体验一般")
    return ("还行", "中规中矩")


def _render_natural_cn_comment(*, style: str, brand: str, keyword: str, feature_term: str, polarity: str) -> str:
    """
    Generate "human-like" Chinese comments for demo/dev fallback.
    Requirement: must NOT contain topic=/brand=/feature=/feeling= patterns.
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

    # zhihu: longer, more structured.
    parts = [
        f"最近集中体验了一下{brand}的产品/系统，刚好也在关注「{keyword}」这个点。",
        f"先说结论：整体{tone}，但{feature_term}相关的细节会影响观感。",
        "如果你对这个点比较敏感，建议结合自己的使用场景判断（游戏/刷视频/办公等）。",
        "希望后续系统更新能把边界场景也处理得更稳，至少不要出现明显波动。",
    ]
    # Make zhihu length roughly 100~800 chars.
    text = " ".join(parts)
    if len(text) < 110:
        text += f" 总体来说，我对{brand}在{keyword}方面的表现是{mood}的。"
    return text[:800]


class MockProvider:
    name = "mock"

    def run_task(self, req: LLMTaskRequest) -> LLMTaskResponse:
        task = req.task_type
        inp = req.input or {}
        try:
            if task == "sentiment_analysis":
                out = self._sentiment(inp)
                return LLMTaskResponse(ok=True, provider=self.name, model="mock-v1", prompt_version=req.prompt_version, output=out)
            if task == "spam_detection":
                out = self._spam(inp)
                return LLMTaskResponse(ok=True, provider=self.name, model="mock-v1", prompt_version=req.prompt_version, output=out)
            if task == "keyword_extraction":
                out = self._keywords(inp)
                return LLMTaskResponse(ok=True, provider=self.name, model="mock-v1", prompt_version=req.prompt_version, output=out)
            if task == "feature_extraction":
                out = self._features(inp)
                return LLMTaskResponse(ok=True, provider=self.name, model="mock-v1", prompt_version=req.prompt_version, output=out)
            if task == "post_analysis":
                out = self._post_analysis(inp)
                return LLMTaskResponse(ok=True, provider=self.name, model="mock-v1", prompt_version=req.prompt_version, output=out)
            if task == "report_generation":
                return self._report(req, inp)
            if task == "crawler_generation":
                out = self._crawler(inp, prompt_version=req.prompt_version)
                return LLMTaskResponse(ok=True, provider=self.name, model="mock-v1", prompt_version=req.prompt_version, output=out)
            return LLMTaskResponse(ok=False, provider=self.name, model="mock-v1", prompt_version=req.prompt_version, output={}, error=f"unknown task: {task}")
        except Exception as e:
            return LLMTaskResponse(ok=False, provider=self.name, model="mock-v1", prompt_version=req.prompt_version, output={}, error=str(e))

    def _sentiment(self, inp: dict[str, Any]) -> dict[str, Any]:
        text = str(inp.get("text") or "").lower()
        positive_terms = ["good", "love", "recommend", "satisfied", "great", "awesome"]
        negative_terms = ["bad", "disappointed", "trash", "lag", "overheat", "refund"]
        score = 0.0
        for t in positive_terms:
            if t in text:
                score += 0.2
        for t in negative_terms:
            if t in text:
                score -= 0.2
        score = max(-1.0, min(1.0, score))
        if score > 0.1:
            sentiment = "positive"
        elif score < -0.1:
            sentiment = "negative"
        else:
            sentiment = "neutral"
        out = {
            "sentiment": sentiment,
            "sentiment_score": score,
            "emotion_intensity": abs(score),
            "model_version": "mock-v1",
        }
        return out

    def _spam(self, inp: dict[str, Any]) -> dict[str, Any]:
        text = str(inp.get("text") or "").lower()
        spam_terms = ["free", "discount", "referral", "dm me", "scan qr", "add me"]
        hit = any(t in text for t in spam_terms)
        out = {
            "spam_label": "spam" if hit else "normal",
            "spam_score": 0.9 if hit else 0.1,
            "model_version": "mock-v1",
        }
        return out

    def _keywords(self, inp: dict[str, Any]) -> dict[str, Any]:
        text = str(inp.get("text") or "")
        kws = inp.get("project_keywords") or []
        hits = []
        for kw in kws:
            k = str(kw.get("keyword") or "").strip()
            if not k:
                continue
            if k in text:
                hits.append(
                    {
                        "keyword": k,
                        "keyword_type": kw.get("keyword_type"),
                        "confidence": 0.9,
                        "source": "rule",
                    }
                )
        return {"hits": hits}

    def _features(self, inp: dict[str, Any]) -> dict[str, Any]:
        text = str(inp.get("text") or "").lower()
        terms = inp.get("feature_terms") or []
        if not terms:
            terms = ["battery", "camera", "price", "support", "performance", "design", "lag", "overheat"]
        hits = []
        for term in terms:
            f = str(term or "").strip()
            if not f:
                continue
            if f.lower() not in text:
                continue
            feature_sentiment = "neutral"
            if any(t in text for t in ["bad", "disappointed", "lag", "overheat"]):
                feature_sentiment = "negative"
            elif any(t in text for t in ["good", "recommend", "satisfied", "great"]):
                feature_sentiment = "positive"
            hits.append(
                {
                    "feature_name": f,
                    "feature_sentiment": feature_sentiment,
                    "confidence": 0.8,
                    "source": "rule",
                }
            )
        return {"hits": hits}

    def _post_analysis(self, inp: dict[str, Any]) -> dict[str, Any]:
        sentiment = self._sentiment(inp)
        spam = self._spam(inp)
        kw = self._keywords(inp)
        ft = self._features(inp)
        return {
            "sentiment": sentiment.get("sentiment"),
            "sentiment_score": sentiment.get("sentiment_score"),
            "emotion_intensity": sentiment.get("emotion_intensity"),
            "spam_label": spam.get("spam_label"),
            "spam_score": spam.get("spam_score"),
            "keyword_hits": (kw.get("hits") or []),
            "feature_hits": (ft.get("hits") or []),
        }

    def _report(self, req: LLMTaskRequest, inp: dict[str, Any]) -> LLMTaskResponse:
        """
        Report generation mock.

        - v1 (legacy): returns full markdown when `report` is a sqlite3.Row (used by earlier code paths)
        - v2: returns incremental blocks only (preferred for conservative report skeleton strategy)
        """
        report = inp.get("report")
        if report is not None and hasattr(report, "__getitem__"):
            # Legacy behavior
            summary, md = llm_mock_generate_markdown(
                report=report,
                overview=inp.get("overview") or {},
                trend=inp.get("trend") or [],
                top_keywords=inp.get("top_keywords") or [],
                top_features=inp.get("top_features") or [],
                competitor=inp.get("competitor") or [],
                posts=inp.get("posts") or {},
            )
            md2 = f"<!-- generator: mock/mock-v1 prompt={req.prompt_version} -->\n\n{md}"
            return LLMTaskResponse(
                ok=True,
                provider=self.name,
                model="mock-v1",
                prompt_version=req.prompt_version,
                output={"summary": summary, "content_markdown": md2},
            )

        # v2 incremental blocks (JSON-only, safe fallback)
        overview = inp.get("overview") or {}
        total = int(overview.get("total_post_count") or 0)
        neg_rate = float(overview.get("negative_rate") or 0.0)
        spam_rate = float(overview.get("spam_rate") or 0.0)
        summary = f"total={total}, neg_rate={neg_rate:.1%}, spam_rate={spam_rate:.1%}"
        exec_md = "\n".join(
            [
                "- Key risks concentrate in top negative features and complaint keywords.",
                "- Track negative rate and sentiment score daily; investigate sudden spikes.",
            ]
        )
        strat_md = "\n".join(
            [
                "- Prioritize fixes for the most-mentioned negative features; publish clear progress updates.",
                "- Improve customer support playbook for high-risk keywords; respond faster on major platforms.",
                "- Set a monitoring threshold for negative rate/spam rate and trigger alerts on anomalies.",
            ]
        )
        return LLMTaskResponse(
            ok=True,
            provider=self.name,
            model="mock-v1",
            prompt_version=req.prompt_version,
            output={
                "summary": summary,
                "executive_summary_md": exec_md,
                "strategy_suggestions_md": strat_md,
            },
        )

    def _crawler(self, inp: dict[str, Any], *, prompt_version: str) -> dict[str, Any]:
        # Generate deterministic mock posts matching the existing pipeline shape.
        project_id = int(inp["project_id"])
        platform_id = int(inp["platform_id"])
        brand_id = int(inp["brand_id"])
        keyword = str(inp["keyword"])
        platform_code = str(inp.get("platform_code") or f"p{platform_id}")
        brand_name = str(inp.get("brand_name") or f"b{brand_id}")
        stat_date = str(inp["stat_date"])
        posts_per_target = int(inp.get("posts_per_target") or 1)
        target_id = int(inp.get("target_id") or 0)

        base_date = datetime.strptime(stat_date, "%Y-%m-%d")
        ts = str(inp.get("crawled_at") or datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
        # Use Chinese feature terms so mock content reads naturally in UI.
        feature_terms = ["续航", "拍照", "价格", "系统流畅度", "发热", "售后"]

        posts = []
        for i in range(posts_per_target):
            publish_dt = base_date + timedelta(minutes=7 * i + (target_id % 5))
            publish_time = publish_dt.strftime("%Y-%m-%d %H:%M:%S")
            external_post_id = inp.get("external_post_id_prefix")
            if external_post_id:
                external_post_id = f"{external_post_id}{i}"
            else:
                # Include stat_date + crawled_at so repeated manual refresh will still generate new posts.
                # Keep stable within a single refresh call.
                external_post_id = sha1_hex(
                    f"{project_id}|{platform_code}|{brand_id}|{keyword}|{stat_date}|{target_id}|{ts}|{i}"
                )[:18]

            feature_term = feature_terms[(target_id + i) % len(feature_terms)]
            polarity = "good" if (target_id + i) % 3 == 0 else ("ok" if (target_id + i) % 3 == 1 else "bad")
            title = f"{brand_name} {keyword} 体验分享"
            style = _pick_style(platform_code)
            content = _render_natural_cn_comment(
                style=style,
                brand=brand_name,
                keyword=keyword,
                feature_term=feature_term,
                polarity=polarity,
            )
            # Safety: avoid accidentally reintroducing stitched fields.
            if _FORBIDDEN_FIELD_STITCH_RE.search(content):
                content = f"聊聊{brand_name}的{keyword}：整体{_sentiment_words(polarity)[1]}，但也有改进空间。"
            author_name = f"user_{platform_code}_{brand_id}_{i}"
            post_url = f"https://example.local/{platform_code}/post/{external_post_id}"
            raw_payload = json.dumps(
                {
                    "platform": platform_code,
                    "brand_id": brand_id,
                    "keyword": keyword,
                    "generated": True,
                    "idx": i,
                    # Observability: distinguish source of generated content.
                    "generated_by": "mock",
                    "provider": "mock",
                    "model": "mock-v1",
                    "prompt_version": str(prompt_version or ""),
                },
                ensure_ascii=False,
            )
            posts.append(
                {
                    "project_id": project_id,
                    "platform_id": platform_id,
                    "brand_id": brand_id,
                    "external_post_id": str(external_post_id),
                    "author_name": author_name,
                    "title": title,
                    "content": content,
                    "post_url": post_url,
                    "publish_time": publish_time,
                    "crawled_at": ts,
                    "like_count": 10 + (target_id + i) % 30,
                    "comment_count": 2 + (target_id + 2 * i) % 15,
                    "share_count": (target_id + i) % 7,
                    "view_count": 50 + (target_id + i) % 500,
                    "raw_payload": raw_payload,
                }
            )
        return {"posts": posts}

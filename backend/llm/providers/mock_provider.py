from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Any, Optional

from backend.llm.types import LLMTaskRequest, LLMTaskResponse
from backend.report_chain_e import llm_mock_generate_markdown


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
                out = self._crawler(inp)
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
        # Reuse existing markdown mock generator; evidence selection is handled at service layer from DB posts.
        report = inp["report"]
        summary, md = llm_mock_generate_markdown(
            report=report,
            overview=inp.get("overview") or {},
            trend=inp.get("trend") or [],
            top_keywords=inp.get("top_keywords") or [],
            top_features=inp.get("top_features") or [],
            competitor=inp.get("competitor") or [],
            posts=inp.get("posts") or {},
        )
        # Add a small marker for debugging provider selection (safe for markdown rendering).
        md2 = f"<!-- generator: mock/mock-v1 prompt={req.prompt_version} -->\n\n{md}"
        return LLMTaskResponse(ok=True, provider=self.name, model="mock-v1", prompt_version=req.prompt_version, output={"summary": summary, "content_markdown": md2})

    def _crawler(self, inp: dict[str, Any]) -> dict[str, Any]:
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
        feature_terms = ["battery", "camera", "price", "lag", "overheat", "support"]

        posts = []
        for i in range(posts_per_target):
            publish_dt = base_date + timedelta(minutes=7 * i + (target_id % 5))
            publish_time = publish_dt.strftime("%Y-%m-%d %H:%M:%S")
            external_post_id = inp.get("external_post_id_prefix")
            if external_post_id:
                external_post_id = f"{external_post_id}{i}"
            else:
                external_post_id = f"{project_id}-{platform_code}-{brand_id}-{i}"

            feature_term = feature_terms[(target_id + i) % len(feature_terms)]
            polarity = "good" if (target_id + i) % 3 == 0 else ("ok" if (target_id + i) % 3 == 1 else "bad")
            title = f"{brand_name} {keyword} 体验分享"
            content = f"[{platform_code}] topic={keyword} brand={brand_name} feature={feature_term} feeling={polarity}"
            author_name = f"user_{platform_code}_{brand_id}_{i}"
            post_url = f"https://example.local/{platform_code}/post/{project_id}-{brand_id}-{keyword}-{i}"
            raw_payload = json.dumps(
                {"platform": platform_code, "brand_id": brand_id, "keyword": keyword, "generated": True, "idx": i},
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

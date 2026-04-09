# 作用：LLM：模型提供方实现（DeepSeek 提供方）。

from __future__ import annotations

import os
from typing import Any

from backend.llm.types import LLMTaskRequest, LLMTaskResponse
from backend.llm.providers.openai_compat_client import OpenAICompatConfig, chat_completions_json


class DeepSeekProvider:
    """
    DeepSeek provider stub.

    This repo currently runs in a restricted environment by default; we keep a clean abstraction
    and rely on env-based configuration when you later enable real HTTP calls.
    """

    name = "deepseek"

    def run_task(self, req: LLMTaskRequest) -> LLMTaskResponse:
        api_key = os.environ.get("PRODWATCH_DEEPSEEK_API_KEY") or os.environ.get("DEEPSEEK_API_KEY")
        base_url = os.environ.get("PRODWATCH_DEEPSEEK_BASE_URL") or os.environ.get("DEEPSEEK_BASE_URL") or "https://api.deepseek.com/v1"
        model = req.model or os.environ.get("PRODWATCH_DEEPSEEK_MODEL") or os.environ.get("DEEPSEEK_MODEL") or "deepseek-chat"
        timeout_s = float(os.environ.get("PRODWATCH_LLM_TIMEOUT_S") or "25")
        max_retries = int(os.environ.get("PRODWATCH_LLM_MAX_RETRIES") or "2")
        if not api_key:
            return LLMTaskResponse(ok=False, provider=self.name, model=model, prompt_version=req.prompt_version, output={}, error="DeepSeek API key not configured")

        cfg = OpenAICompatConfig(
            base_url=str(base_url),
            api_key=str(api_key),
            model=str(model),
            timeout_s=timeout_s,
            max_retries=max_retries,
        )
        try:
            parsed, meta = chat_completions_json(cfg=cfg, prompt_text=req.prompt_text)
            out = self._normalize_output(req.task_type, parsed)
            if out is None:
                return LLMTaskResponse(
                    ok=False,
                    provider=self.name,
                    model=str(model),
                    prompt_version=req.prompt_version,
                    output={},
                    error="DeepSeek output parse/normalize failed",
                )
            return LLMTaskResponse(ok=True, provider=self.name, model=str(model), prompt_version=req.prompt_version, output=out)
        except Exception as e:
            return LLMTaskResponse(ok=False, provider=self.name, model=str(model), prompt_version=req.prompt_version, output={}, error=str(e))

    def _normalize_output(self, task_type: str, parsed: Any) -> dict[str, Any] | None:
        t = str(task_type)
        if not isinstance(parsed, dict):
            return None

        if t == "sentiment_analysis":
            return {
                "sentiment": str(parsed.get("sentiment") or "neutral"),
                "sentiment_score": float(parsed.get("sentiment_score") or 0.0),
                "emotion_intensity": float(parsed.get("emotion_intensity") or 0.0),
                "model_version": "llm",
            }
        if t == "spam_detection":
            return {
                "spam_label": str(parsed.get("spam_label") or "normal"),
                "spam_score": float(parsed.get("spam_score") or 0.0),
                "model_version": "llm",
            }
        if t in {"keyword_extraction", "feature_extraction"}:
            hits = parsed.get("hits")
            if not isinstance(hits, list):
                hits = []
            # pass-through hits; router/service will map fields
            return {"hits": hits}
        if t == "crawler_generation":
            posts = parsed.get("posts")
            if not isinstance(posts, list):
                posts = []
            return {"posts": posts}
        if t == "report_generation":
            return {
                "summary": str(parsed.get("summary") or ""),
                # v2: incremental blocks (preferred)
                "executive_summary_md": str(parsed.get("executive_summary_md") or ""),
                "strategy_suggestions_md": str(parsed.get("strategy_suggestions_md") or ""),
                # v1 compatibility (some providers/users may still return full markdown)
                "content_markdown": str(parsed.get("content_markdown") or ""),
            }
        # unknown task: still return parsed for debugging
        return parsed

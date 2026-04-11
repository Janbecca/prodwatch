# 作用：LLM：模型提供方实现（通义千问（Qwen）提供方）。

from __future__ import annotations

import os
from typing import Any

from backend.llm.types import LLMTaskRequest, LLMTaskResponse
from backend.llm.providers.openai_compat_client import OpenAICompatConfig, chat_completions_json


class QwenProvider:
    """
    Qwen 提供商（OpenAI-compatible Chat Completions）。

    未配置 API Key 时会返回 ok=False，由上层决定是否重试/报错。
    """
    name = "qwen"

    def run_task(self, req: LLMTaskRequest) -> LLMTaskResponse:
        api_key = os.environ.get("PRODWATCH_QWEN_API_KEY") or os.environ.get("QWEN_API_KEY")
        # DashScope OpenAI-compatible mode base URL
        base_url = os.environ.get("PRODWATCH_QWEN_BASE_URL") or os.environ.get("QWEN_BASE_URL") or "https://dashscope.aliyuncs.com/compatible-mode/v1"
        model = req.model or os.environ.get("PRODWATCH_QWEN_MODEL") or os.environ.get("QWEN_MODEL") or "qwen-plus"
        task_key = str(getattr(req, "task_type", "") or "").strip().upper()
        timeout_default = "60" if str(getattr(req, "task_type", "") or "") == "crawler_generation" else "25"
        timeout_s = float(os.environ.get(f"PRODWATCH_LLM_TIMEOUT_S_{task_key}") or os.environ.get("PRODWATCH_LLM_TIMEOUT_S") or timeout_default)
        max_retries = int(os.environ.get(f"PRODWATCH_LLM_MAX_RETRIES_{task_key}") or os.environ.get("PRODWATCH_LLM_MAX_RETRIES") or "2")
        if not api_key:
            return LLMTaskResponse(ok=False, provider=self.name, model=model, prompt_version=req.prompt_version, output={}, error="Qwen API key not configured")
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
                    error="Qwen output parse/normalize failed",
                )
            return LLMTaskResponse(ok=True, provider=self.name, model=str(model), prompt_version=req.prompt_version, output=out)
        except Exception as e:
            msg = str(e)
            detail = f"{type(e).__name__}: {msg}" if msg else f"{type(e).__name__}"
            detail = f"{detail} (base_url={base_url}, model={model}, timeout_s={timeout_s}, max_retries={max_retries})"
            return LLMTaskResponse(
                ok=False,
                provider=self.name,
                model=str(model),
                prompt_version=req.prompt_version,
                output={},
                error=detail,
            )

    # 根据不同任务类型规范化输出，确保返回格式一致，方便后续处理。
    def _normalize_output(self, task_type: str, parsed: Any) -> dict[str, Any] | None:
        t = str(task_type)
        if not isinstance(parsed, dict):
            return None

        def md_block(v: Any) -> str:
            # Normalize possible list outputs into markdown bullet-list string.
            if v is None:
                return ""
            if isinstance(v, str):
                return v
            if isinstance(v, list):
                lines: list[str] = []
                for it in v:
                    s = str(it or "").strip()
                    if not s:
                        continue
                    if s.startswith("- "):
                        lines.append(s)
                    elif s.startswith("-"):
                        lines.append("- " + s.lstrip("-").strip())
                    else:
                        lines.append("- " + s)
                return "\n".join(lines)
            return str(v)

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
            return {"hits": hits}
        if t == "crawler_generation":
            posts = parsed.get("posts")
            if not isinstance(posts, list):
                posts = []
            return {"posts": posts}
        if t == "report_generation":
            return {
                "summary": str(parsed.get("summary") or ""),
                "executive_summary_md": md_block(parsed.get("executive_summary_md")),
                "strategy_suggestions_md": md_block(parsed.get("strategy_suggestions_md")),
                "content_markdown": str(parsed.get("content_markdown") or ""),
            }
        return parsed

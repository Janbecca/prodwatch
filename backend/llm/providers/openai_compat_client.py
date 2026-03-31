from __future__ import annotations

import json
import random
import re
import time
from dataclasses import dataclass
from typing import Any, Optional

import httpx


_CODE_FENCE_RE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE)


def _strip_code_fences(text: str) -> str:
    return _CODE_FENCE_RE.sub("", text.strip())


def parse_json_from_text(text: str) -> Any:
    """
    Robustly parse JSON from a model response.

    Handles:
    - Markdown code fences ```json ... ```
    - Leading/trailing text around a single JSON object/array
    """
    s = _strip_code_fences(text or "")
    if not s:
        raise ValueError("empty response")
    # Fast path
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        pass

    # Try to extract first JSON object/array by scanning for '{' or '[' and matching end.
    start_candidates = [i for i in (s.find("{"), s.find("[")) if i >= 0]
    if not start_candidates:
        raise ValueError("no json start found")
    start = min(start_candidates)
    tail = s[start:]

    # Heuristic: find last '}' or ']' and attempt json loads for decreasing slices.
    end_candidates = [tail.rfind("}"), tail.rfind("]")]
    end = max([i for i in end_candidates if i >= 0], default=-1)
    if end < 0:
        raise ValueError("no json end found")
    tail = tail[: end + 1]

    # Retry strict parse
    return json.loads(tail)


@dataclass(frozen=True)
class OpenAICompatConfig:
    base_url: str
    api_key: str
    model: str
    timeout_s: float = 25.0
    max_retries: int = 2


def _is_retryable_status(code: int) -> bool:
    return code in {408, 409, 425, 429, 500, 502, 503, 504}


def _backoff_s(attempt: int) -> float:
    # exponential backoff with jitter
    base = 0.6 * (2**attempt)
    return min(8.0, base + random.random() * 0.25)


def chat_completions_json(
    *,
    cfg: OpenAICompatConfig,
    prompt_text: str,
    system_text: str = "Return valid JSON only. Do not wrap in markdown.",
    extra_body: Optional[dict[str, Any]] = None,
) -> tuple[Any, dict[str, Any]]:
    """
    Call OpenAI-compatible Chat Completions endpoint and parse JSON from assistant content.

    Returns:
      (parsed_json, meta)
    """
    url = cfg.base_url.rstrip("/") + "/chat/completions"
    body: dict[str, Any] = {
        "model": cfg.model,
        "messages": [
            {"role": "system", "content": system_text},
            {"role": "user", "content": prompt_text},
        ],
        "temperature": 0.2,
    }
    if extra_body:
        body.update(extra_body)

    headers = {
        "Authorization": f"Bearer {cfg.api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    last_err: Optional[Exception] = None
    for attempt in range(cfg.max_retries + 1):
        try:
            with httpx.Client(timeout=httpx.Timeout(cfg.timeout_s)) as client:
                resp = client.post(url, headers=headers, json=body)
            if resp.status_code >= 400:
                if _is_retryable_status(resp.status_code) and attempt < cfg.max_retries:
                    time.sleep(_backoff_s(attempt))
                    continue
                # include a short snippet for debugging
                text = resp.text[:4000] if resp.text else ""
                raise RuntimeError(f"http {resp.status_code}: {text}")

            data = resp.json()
            choice0 = (data.get("choices") or [{}])[0]
            msg = choice0.get("message") or {}
            content = msg.get("content")
            if not isinstance(content, str):
                raise ValueError("missing assistant content")
            parsed = parse_json_from_text(content)
            meta = {
                "http_status": resp.status_code,
                "raw_content": content[:8000],
                "response_json": data,
            }
            return parsed, meta
        except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError, httpx.ConnectError) as e:
            last_err = e
            if attempt < cfg.max_retries:
                time.sleep(_backoff_s(attempt))
                continue
            raise
        except Exception as e:
            last_err = e
            raise

    # unreachable
    raise RuntimeError(str(last_err) if last_err else "unknown error")


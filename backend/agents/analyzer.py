import json
import os
from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd

from backend.storage.db import get_repo


def _now_ts_ms() -> int:
    return int(datetime.utcnow().timestamp() * 1000)


def _normalize_str(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def _safe_int(value: Any) -> Optional[int]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _rule_based_sentiment(text: str) -> Dict[str, Any]:
    negative_words = ["差", "糟", "失望", "垃圾", "故障", "卡顿", "发烫", "退货", "售后差", "不行"]
    positive_words = ["好", "满意", "推荐", "清晰", "稳定", "方便", "实用", "性价比", "喜欢"]
    is_negative = any(w in text for w in negative_words)
    is_positive = any(w in text for w in positive_words)
    if is_negative and not is_positive:
        polarity = "negative"
    elif is_positive and not is_negative:
        polarity = "positive"
    else:
        polarity = "neutral"
    return {
        "polarity": polarity,
        "confidence": 0.6,
        "intensity": 0.5,
        "emotions": {"angry": 0.4} if polarity == "negative" else {},
    }


def _try_parse_json_object(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    s = str(text).strip()

    if s.startswith("```"):
        s = s.replace("```json", "```").replace("```JSON", "```")
        parts = [p.strip() for p in s.split("```") if p.strip()]
        if parts:
            s = parts[0]

    try:
        obj = json.loads(s)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    start = s.find("{")
    end = s.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            obj = json.loads(s[start : end + 1])
            if isinstance(obj, dict):
                return obj
        except Exception:
            return None
    return None


def analyze_sentiment(clean_df: pd.DataFrame, *, model: str = "rule-based") -> int:
    """
    Input: post_clean dataframe (must include columns: id, project_id, clean_text, is_valid).
    Output: write sentiment_result rows; returns number of newly inserted rows.
    """
    repo = get_repo()
    if clean_df is None or clean_df.empty:
        return 0

    existing = repo.query("sentiment_result")
    existing_clean_ids = set()
    if not existing.empty and "post_clean_id" in existing.columns:
        existing_clean_ids = set(pd.to_numeric(existing["post_clean_id"], errors="coerce").dropna().astype(int).tolist())

    use_llm = model != "rule-based"
    dashscope = None
    if use_llm:
        try:
            import dashscope as _dashscope  # type: ignore

            dashscope = _dashscope
            dashscope.api_key = os.getenv("DASHSCOPE_API_KEY")
            if not dashscope.api_key:
                use_llm = False
        except Exception:
            use_llm = False

    rows = []
    base_id = _now_ts_ms() * 1000
    seq = 0
    for _, r in clean_df.iterrows():
        clean_id = _safe_int(r.get("id"))
        if clean_id is None or clean_id in existing_clean_ids:
            continue
        is_valid = int(pd.to_numeric(r.get("is_valid"), errors="coerce") or 0)
        if is_valid != 1:
            continue

        text = _normalize_str(r.get("clean_text"))
        project_id = _safe_int(r.get("project_id"))

        if not use_llm:
            data = _rule_based_sentiment(text)
        else:
            prompt = f'请输出 JSON：{{"polarity","confidence","intensity","emotions"}}。内容：{text}'
            prompt = (
                "请只输出 JSON 对象，不要输出多余文字。格式示例："
                '{"polarity":"positive|neutral|negative","confidence":0.0,"intensity":0.0,"emotions":{}}'
                f"\n内容：{text}"
            )
            try:
                resp = dashscope.Generation.call(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.2,
                )
                output = (
                    resp.output.text
                    if hasattr(resp, "output") and hasattr(resp.output, "text")
                    else resp.get("output", {}).get("text")  # type: ignore[union-attr]
                )
                parsed = _try_parse_json_object(str(output or ""))
                data = parsed if parsed is not None else _rule_based_sentiment(text)
            except Exception:
                data = _rule_based_sentiment(text)

        emotions = data.get("emotions")
        if isinstance(emotions, str):
            data["emotions"] = _try_parse_json_object(emotions) or {}
        elif emotions is None or not isinstance(emotions, dict):
            data["emotions"] = {}

        rows.append(
            {
                "id": base_id + seq,
                "post_clean_id": clean_id,
                "project_id": project_id,
                "pipeline_run_id": _safe_int(r.get("pipeline_run_id")),
                "project_platform_id": _safe_int(r.get("project_platform_id")),
                "platform_id": _safe_int(r.get("platform_id")),
                "brand_id": _safe_int(r.get("brand_id")),
                "polarity": data.get("polarity"),
                "confidence": data.get("confidence"),
                "intensity": data.get("intensity"),
                "emotions": json.dumps(data.get("emotions") or {}, ensure_ascii=False),
            }
        )
        seq += 1

    if not rows:
        return 0

    if hasattr(repo, "insert_many"):
        return int(repo.insert_many("sentiment_result", rows))

    for row in rows:
        repo.insert("sentiment_result", row)
    return len(rows)

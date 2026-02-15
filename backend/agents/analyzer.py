import os
import json
from typing import List
from datetime import datetime
from backend.storage.db import get_repo
import dashscope


def analyze(posts: List[dict], model: str, project_id: int):
    repo = get_repo()
    results = []
    if model != "rule-based":
        dashscope.api_key = os.getenv("DASHSCOPE_API_KEY")

    for p in posts:
        text = p["raw_text"]
        if model == "rule-based":
            is_negative = any(word in text for word in ["差", "坏", "失望", "糟糕"])
            sentiment = "negative" if is_negative else "neutral"
            confidence = 0.6
            intensity = 0.5
            emotions = {"angry": 0.4} if sentiment == "negative" else {}
        else:
            prompt = f"请输出JSON: {{polarity, confidence, intensity, emotions}} 内容:{text}"
            resp = dashscope.Generation.call(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
            )
            output = (
                resp.output.text
                if hasattr(resp, "output") and hasattr(resp.output, "text")
                else resp["output"]["text"]
            )
            data = json.loads(output)
            sentiment = data["polarity"]
            confidence = data["confidence"]
            intensity = data["intensity"]
            emotions = data["emotions"]

        row = {
            "id": int(datetime.utcnow().timestamp()),
            "post_clean_id": p["id"],
            "project_id": project_id,
            "polarity": sentiment,
            "confidence": confidence,
            "intensity": intensity,
            "emotions": json.dumps(emotions, ensure_ascii=False),
        }
        repo.insert("sentiment_result", row)
        results.append(row)

    return results

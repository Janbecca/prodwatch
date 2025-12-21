import os
import json
from typing import List
from datetime import datetime
from backend.storage.db import get_repo
import dashscope

def analyze(posts: List[dict], model: str, project_id: int):
    repo = get_repo()
    results = []
    client = dashscope() if model != "rule-based" else None

    for p in posts:
        text = p["raw_text"]
        if model == "rule-based":
            sentiment = "negative" if "差" in text else "neutral"
            confidence = 0.6
            intensity = 0.5
            emotions = {"angry": 0.4} if sentiment == "negative" else {}
        else:
            prompt = f"请输出JSON: {{polarity, confidence, intensity, emotions}} 内容:{text}"
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
                response_format={"type": "json_object"}
            )
            data = json.loads(resp.choices[0].message.content)
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
            "emotions": json.dumps(emotions, ensure_ascii=False)
        }
        repo.insert("sentiment_result", row)
        results.append(row)

    return results

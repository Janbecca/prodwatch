from typing import List
from bs4 import BeautifulSoup
from simhash import Simhash
from backend.storage.db import get_repo


# 过滤智能体：去除 HTML 标签并生成 Simhash
def filter_posts() -> List[dict]:
    repo = get_repo()
    try:
        raw_df = repo.query("post_raw")
    except Exception:
        return []

    results = []
    for _, row in raw_df.iterrows():
        soup = BeautifulSoup(str(row.get("raw_text", "")), "html.parser")
        cleaned_content = soup.get_text()
        simhash_value = Simhash(cleaned_content).value

        clean_row = {
            "id": int(simhash_value),
            "raw_id": row.get("id"),
            "content": cleaned_content,
            "simhash": simhash_value,
            "dedup_group_id": simhash_value % 10,
        }
        try:
            repo.insert("post_clean", clean_row)
        except Exception:
            pass
        results.append(clean_row)

    return results

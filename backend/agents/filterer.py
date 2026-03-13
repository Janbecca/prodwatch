from typing import List

from .pipeline import clean_posts_for_run
from backend.storage.db import get_repo


def filter_posts(pipeline_run_id: int) -> List[dict]:
    """
    Compatibility wrapper for the "filter agent" concept.

    Writes cleaned posts into `post_clean` (aligned with the Excel schema) and returns the
    cleaned rows for this run.
    """
    clean_posts_for_run(pipeline_run_id)
    repo = get_repo()
    df = repo.query("post_clean", {"pipeline_run_id": pipeline_run_id})
    return df.to_dict(orient="records")


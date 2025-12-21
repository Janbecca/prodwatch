from fastapi import APIRouter, Query
from typing import List
from backend.storage.db import get_repo
from backend.agents.crawler import crawl
from backend.agents.analyzer import analyze
from datetime import datetime

router = APIRouter(prefix="/analysis", tags=["analysis"])

@router.post("/run")
def run_analysis(
    project_id: int = Query(...),
    platform: List[str] = Query(...),
    keyword: str = Query(...),
    model: str = Query("rule-based")
):
    repo = get_repo()

    # 1. pipeline_run 记录
    run_id = int(datetime.utcnow().timestamp())
    repo.insert("pipeline_run", {
        "id": run_id,
        "project_id": project_id,
        "run_no": f"{datetime.utcnow().strftime('%Y%m%d')}-{run_id}",
        "trigger_type": "manual",
        "status": "running",
        "start_time": datetime.utcnow(),
        "end_time": None,
        "params": f'{{"platform":{platform},"keyword":"{keyword}","model":"{model}"}}',
        "created_at": datetime.utcnow()
    })

    # 2. 抓取
    posts = crawl(project_id, platform, keyword, run_id)

    # 3. 分析
    results = analyze(posts, model, project_id)

    return {
        "run_id": run_id,
        "count": len(results),
        "items": results
    }

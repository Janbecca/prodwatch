from fastapi import APIRouter

router = APIRouter(prefix="/report", tags=["report"])


@router.get("")
def get_report_summary():
    return {"summary": "这是一个示例报告摘要", "generatedAt": "2025-11-03T00:00:00Z"}

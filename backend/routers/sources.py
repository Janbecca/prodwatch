from fastapi import APIRouter
from backend.storage.db import get_repo, EXCEL_PATH
from backend.agents.importer import import_from_excel

router = APIRouter(prefix="/sources", tags=["sources"])


@router.get("")
def list_sources():
    return [
        {"id": "weibo", "name": "微博"},
        {"id": "zhihu", "name": "知乎"},
        {"id": "bilibili", "name": "哔哩哔哩"},
    ]


@router.get("/{source_id}")
def get_source(source_id: str):
    return {"id": source_id, "name": source_id.capitalize()}


@router.post("/import_excel")
def import_excel():
    repo = get_repo()
    return import_from_excel(repo, EXCEL_PATH)

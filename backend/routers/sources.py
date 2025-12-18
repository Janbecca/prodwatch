from fastapi import APIRouter

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

from fastapi import APIRouter

router = APIRouter(prefix="/posts", tags=["posts"])


@router.get("")
def list_posts():
    # stub: return sample posts
    return [
        {"id": 1, "title": "示例贴文A", "source": "weibo"},
        {"id": 2, "title": "示例贴文B", "source": "zhihu"},
    ]


@router.get("/{post_id}")
def get_post(post_id: int):
    return {"id": post_id, "title": f"示例贴文#{post_id}", "content": "..."}

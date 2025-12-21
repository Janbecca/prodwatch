from fastapi import FastAPI


# 创建 FastAPI 应用实例
from backend.routers import analysis, auth, dashboard, moderation, posts, report, settings, sources

app = FastAPI()


API_PREFIX = "/api"

app.include_router(analysis.router, prefix=API_PREFIX)
app.include_router(auth.router, prefix=API_PREFIX)
app.include_router(dashboard.router, prefix=API_PREFIX)
app.include_router(moderation.router, prefix=API_PREFIX)
app.include_router(posts.router, prefix=API_PREFIX)
app.include_router(report.router, prefix=API_PREFIX)
app.include_router(settings.router, prefix=API_PREFIX)
app.include_router(sources.router, prefix=API_PREFIX)


@app.get("/")
def read_root():
    return {"message": "Hello World"}

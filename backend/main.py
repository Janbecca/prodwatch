from fastapi import FastAPI


# 创建 FastAPI 应用实例
from backend.routers import analysis, auth, brands, dashboard, moderation, posts, projects, report, reports, settings, sources
from backend.tasks.scheduler import start_scheduler, stop_scheduler

app = FastAPI()


API_PREFIX = "/api"

app.include_router(analysis.router, prefix=API_PREFIX)
app.include_router(auth.router, prefix=API_PREFIX)
app.include_router(brands.router, prefix=API_PREFIX)
app.include_router(dashboard.router, prefix=API_PREFIX)
app.include_router(moderation.router, prefix=API_PREFIX)
app.include_router(posts.router, prefix=API_PREFIX)
app.include_router(projects.router, prefix=API_PREFIX)
app.include_router(report.router, prefix=API_PREFIX)
app.include_router(reports.router, prefix=API_PREFIX)
app.include_router(settings.router, prefix=API_PREFIX)
app.include_router(sources.router, prefix=API_PREFIX)


@app.on_event("startup")
def _startup():
    start_scheduler()


@app.on_event("shutdown")
def _shutdown():
    stop_scheduler()


@app.get("/")
def read_root():
    return {"message": "Hello World"}

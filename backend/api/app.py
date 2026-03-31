from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.api.routes_dashboard import router as dashboard_router
from backend.api.routes_posts import router as posts_router
from backend.api.routes_reports import router as reports_router
from backend.api.routes_projects import router as projects_router
from backend.api.routes_project_config import router as project_config_router
from backend.api.routes_project_mutations import router as project_mutations_router
from backend.api.routes_project_refresh import router as project_refresh_router
from backend.api.routes_meta import router as meta_router
from backend.api.routes_scheduler import router as scheduler_router
from backend.services.daily_refresh_scheduler import get_daily_scheduler
from backend.api.routes_crawl_jobs import router as crawl_jobs_router


app = FastAPI(title="ProdWatch API", version="0.1")
app.add_middleware(
    CORSMiddleware,
    # Dev-friendly defaults: allow the Vite dev server (and other local tools) to call the API directly.
    # If you later add cookie-based auth, tighten this to explicit origins + credentials.
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(dashboard_router)
app.include_router(posts_router)
app.include_router(reports_router)
app.include_router(projects_router)
app.include_router(project_config_router)
app.include_router(project_mutations_router)
app.include_router(project_refresh_router)
app.include_router(meta_router)
app.include_router(scheduler_router)
app.include_router(crawl_jobs_router)


@app.on_event("startup")
def _start_scheduler() -> None:
    # Lightweight in-process scheduler; safe no-op when disabled.
    get_daily_scheduler().start()


@app.on_event("shutdown")
def _stop_scheduler() -> None:
    try:
        get_daily_scheduler().stop()
    except Exception:
        # Avoid blocking shutdown.
        pass

from fastapi import APIRouter, HTTPException
from backend.storage.db import EXCEL_PATH, get_repo
from backend.agents.importer import import_from_excel

router = APIRouter(prefix="/sources", tags=["sources"])


@router.get("")
def list_sources():
    repo = get_repo()
    df = repo.query("platform")
    if df.empty:
        return []
    cols = [c for c in ["id", "code", "name", "created_at"] if c in df.columns]
    return df[cols].to_dict(orient="records")


@router.get("/{source_id}")
def get_source(source_id: str):
    repo = get_repo()
    df = repo.query("platform")
    if "code" not in df.columns:
        raise HTTPException(status_code=404, detail="source not found")
    target = df[df["code"].astype(str) == source_id]
    if target.empty:
        raise HTTPException(status_code=404, detail="source not found")
    return target.iloc[0].to_dict()


@router.post("/import_excel")
def import_excel():
    repo = get_repo()
    return import_from_excel(repo, EXCEL_PATH)

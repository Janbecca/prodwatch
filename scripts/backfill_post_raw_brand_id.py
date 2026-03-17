from __future__ import annotations

import os
from typing import Dict, List, Optional

import pandas as pd


EXCEL_PATH = os.path.join("backend", "database", "prodwatch_database.xlsx")


def _read(sheet: str) -> pd.DataFrame:
    try:
        return pd.read_excel(EXCEL_PATH, sheet_name=sheet)
    except Exception:
        return pd.DataFrame()


def _write(sheet: str, df: pd.DataFrame) -> None:
    with pd.ExcelWriter(EXCEL_PATH, mode="a", engine="openpyxl", if_sheet_exists="replace") as w:
        df.to_excel(w, sheet_name=sheet, index=False)


def _to_int(x) -> Optional[int]:
    v = pd.to_numeric(x, errors="coerce")
    if pd.isna(v):
        return None
    return int(v)


def build_project_to_brands() -> Dict[int, List[int]]:
    join_df = _read("monitor_project_brand")
    mapping: Dict[int, List[int]] = {}
    if not join_df.empty and {"project_id", "brand_id"}.issubset(join_df.columns):
        join_df = join_df.copy()
        join_df["project_id"] = pd.to_numeric(join_df["project_id"], errors="coerce")
        join_df["brand_id"] = pd.to_numeric(join_df["brand_id"], errors="coerce")
        join_df = join_df.dropna(subset=["project_id", "brand_id"])
        join_df["project_id"] = join_df["project_id"].astype(int)
        join_df["brand_id"] = join_df["brand_id"].astype(int)
        for pid, g in join_df.groupby("project_id"):
            mapping[int(pid)] = sorted({int(x) for x in g["brand_id"].tolist() if x is not None})

    # legacy fallback
    proj_df = _read("monitor_project")
    if not proj_df.empty and {"id", "brand_id"}.issubset(proj_df.columns):
        proj_df = proj_df.copy()
        proj_df["id"] = pd.to_numeric(proj_df["id"], errors="coerce")
        proj_df["brand_id"] = pd.to_numeric(proj_df["brand_id"], errors="coerce")
        proj_df = proj_df.dropna(subset=["id", "brand_id"])
        proj_df["id"] = proj_df["id"].astype(int)
        proj_df["brand_id"] = proj_df["brand_id"].astype(int)
        for _, r in proj_df.iterrows():
            pid = int(r["id"])
            if pid not in mapping:
                mapping[pid] = [int(r["brand_id"])]
    return mapping


def main() -> None:
    raw_df = _read("post_raw")
    if raw_df.empty:
        print("post_raw empty; nothing to do")
        return

    mapping = build_project_to_brands()

    raw_df = raw_df.copy()
    if "brand_id" not in raw_df.columns:
        raw_df["brand_id"] = None

    raw_df["id"] = pd.to_numeric(raw_df.get("id"), errors="coerce")
    raw_df["project_id"] = pd.to_numeric(raw_df.get("project_id"), errors="coerce")
    raw_df["brand_id"] = pd.to_numeric(raw_df.get("brand_id"), errors="coerce")

    filled = 0
    for idx, row in raw_df.iterrows():
        if pd.notna(row.get("brand_id")):
            continue
        pid = _to_int(row.get("project_id"))
        if pid is None:
            continue
        brands = mapping.get(int(pid)) or []
        if not brands:
            continue
        if len(brands) == 1:
            raw_df.at[idx, "brand_id"] = int(brands[0])
        else:
            rid = _to_int(row.get("id")) or 0
            raw_df.at[idx, "brand_id"] = int(brands[rid % len(brands)])
        filled += 1

    raw_df["brand_id"] = pd.to_numeric(raw_df["brand_id"], errors="coerce")
    _write("post_raw", raw_df)
    print(f"backfilled brand_id for {filled} rows")


if __name__ == "__main__":
    main()


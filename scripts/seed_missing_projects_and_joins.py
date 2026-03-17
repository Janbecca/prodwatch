from __future__ import annotations

import os
from datetime import datetime
from typing import Dict, List, Set, Tuple

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


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _to_int_set(series) -> Set[int]:
    s = pd.to_numeric(series, errors="coerce").dropna()
    return set(s.astype(int).tolist())


def _next_join_id(existing: pd.DataFrame, offset: int) -> int:
    base = int(datetime.utcnow().timestamp() * 1000)
    if existing is not None and not existing.empty and "id" in existing.columns:
        mx = pd.to_numeric(existing["id"], errors="coerce").dropna()
        if not mx.empty:
            base = max(base, int(mx.max()) + 1)
    return base + offset


def main() -> None:
    post_raw = _read("post_raw")
    kw = _read("monitor_keyword")
    projects = _read("monitor_project")
    join = _read("monitor_project_brand")
    brands = _read("brand")

    if projects.empty:
        projects = pd.DataFrame(columns=["id", "brand_id", "name", "description", "is_active", "created_at", "updated_at", "product_category"])
    if join.empty:
        join = pd.DataFrame(columns=["id", "project_id", "brand_id", "created_at"])

    existing_project_ids = _to_int_set(projects.get("id")) if "id" in projects.columns else set()

    needed_ids: Set[int] = set()
    if not post_raw.empty and "project_id" in post_raw.columns:
        needed_ids |= _to_int_set(post_raw["project_id"])
    if not kw.empty and "project_id" in kw.columns:
        needed_ids |= _to_int_set(kw["project_id"])

    missing_ids = sorted([pid for pid in needed_ids if pid not in existing_project_ids and pid > 0])
    if not missing_ids:
        print("no missing projects; nothing to do")
        return

    brand_ids: List[int] = []
    if not brands.empty and "id" in brands.columns:
        brand_ids = sorted(_to_int_set(brands["id"]))
    if not brand_ids:
        # fallback: create a few dummy brand ids
        brand_ids = [1, 2, 3]

    product_categories = ["摄像头", "电视机", "扫地机器人", "手机", "空调"]

    # Existing join pairs to avoid duplicates.
    existing_pairs: Set[Tuple[int, int]] = set()
    if not join.empty and {"project_id", "brand_id"}.issubset(join.columns):
        j = join.copy()
        j["project_id"] = pd.to_numeric(j["project_id"], errors="coerce")
        j["brand_id"] = pd.to_numeric(j["brand_id"], errors="coerce")
        j = j.dropna(subset=["project_id", "brand_id"])
        for _, r in j.iterrows():
            existing_pairs.add((int(r["project_id"]), int(r["brand_id"])))

    now = _now_iso()
    new_proj_rows: List[Dict] = []
    new_join_rows: List[Dict] = []

    for idx, pid in enumerate(missing_ids):
        new_proj_rows.append(
            {
                "id": int(pid),
                "brand_id": None,  # legacy column unused
                "name": f"示例项目{pid}",
                "description": "用于关键词监控展示的示例数据",
                "is_active": 0,
                "created_at": now,
                "updated_at": now,
                "product_category": product_categories[pid % len(product_categories)],
            }
        )

        # Assign 3 brands per project (cyclic).
        picks = [brand_ids[(pid + k) % len(brand_ids)] for k in range(min(3, len(brand_ids)))]
        for b in picks:
            if (int(pid), int(b)) in existing_pairs:
                continue
            new_join_rows.append(
                {
                    "id": _next_join_id(join, len(new_join_rows)),
                    "project_id": int(pid),
                    "brand_id": int(b),
                    "created_at": now,
                }
            )
            existing_pairs.add((int(pid), int(b)))

    projects2 = pd.concat([projects, pd.DataFrame(new_proj_rows)], ignore_index=True)
    projects2["id"] = pd.to_numeric(projects2.get("id"), errors="coerce")
    projects2 = projects2.dropna(subset=["id"])
    projects2["id"] = projects2["id"].astype(int)
    projects2 = projects2.sort_values(by="id")

    join2 = pd.concat([join, pd.DataFrame(new_join_rows)], ignore_index=True)

    _write("monitor_project", projects2)
    _write("monitor_project_brand", join2)
    print(f"inserted {len(new_proj_rows)} projects, {len(new_join_rows)} project-brand rows")


if __name__ == "__main__":
    main()


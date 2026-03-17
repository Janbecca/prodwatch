import os
from datetime import datetime

import pandas as pd


def _now() -> datetime:
    return datetime.utcnow()


def _normalize_id_col(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "id" not in df.columns:
        return df
    ids = pd.to_numeric(df["id"], errors="coerce")
    if ids.notna().any():
        df = df[ids.notna()].copy()
    return df


def main() -> None:
    excel_path = os.getenv("EXCEL_PATH", "backend/database/prodwatch_database.xlsx")

    # Load all sheets we care about.
    project_df = pd.read_excel(excel_path, sheet_name="monitor_project")
    project_df = _normalize_id_col(project_df)

    # Add new columns for project config.
    for col, default in [
        ("product_category", None),
        ("updated_at", None),
    ]:
        if col not in project_df.columns:
            project_df[col] = default

    # Backfill updated_at.
    now = _now()
    if "updated_at" in project_df.columns:
        project_df["updated_at"] = project_df["updated_at"].where(project_df["updated_at"].notna(), now)

    # Create/refresh join sheet (monitor_project_brand) from legacy brand_id column.
    join_rows = []
    if not project_df.empty and "brand_id" in project_df.columns:
        for _, row in project_df.iterrows():
            pid = pd.to_numeric(row.get("id"), errors="coerce")
            bid = pd.to_numeric(row.get("brand_id"), errors="coerce")
            if pd.notna(pid) and pd.notna(bid):
                join_rows.append(
                    {
                        "id": int(now.timestamp() * 1000) + len(join_rows) + 1,
                        "project_id": int(pid),
                        "brand_id": int(bid),
                        "created_at": now,
                    }
                )

    join_df = pd.DataFrame(join_rows, columns=["id", "project_id", "brand_id", "created_at"])

    with pd.ExcelWriter(excel_path, mode="a", engine="openpyxl", if_sheet_exists="replace") as writer:
        project_df.to_excel(writer, sheet_name="monitor_project", index=False)
        join_df.to_excel(writer, sheet_name="monitor_project_brand", index=False)

    print("migrated:", excel_path)


if __name__ == "__main__":
    main()


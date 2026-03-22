import os
import json
import shutil
import time
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional

DB_TYPE = os.getenv("DB_TYPE", "excel")   # excel / mysql
EXCEL_PATH = os.getenv("EXCEL_PATH", "backend/database/prodwatch_database.xlsx")

class ExcelWriteLockedError(RuntimeError):
    def __init__(self, path: str, sheet: str, original: Exception):
        super().__init__(f"Excel database is locked: {path} (sheet={sheet}): {original}")
        self.path = path
        self.sheet = sheet
        self.original = original

class ExcelRepository:
    def __init__(self, path: str):
        self.path = path

    @staticmethod
    def _looks_like_description_row(df: pd.DataFrame) -> bool:
        """
        Many sheets keep a second row as Chinese "说明行" (description row).
        Pandas reads it as the first data row.

        Heuristic (conservative):
        - first data row is mostly short strings
        - and contains common header-like tokens ("ID/名称/时间/说明/是否"...)
        """
        if df is None or df.empty:
            return False

        row = df.iloc[0].to_dict()
        non_empty: List[str] = []
        hit = 0
        numeric_like = 0
        tokens = ["id", "ID", "编号", "名称", "时间", "说明", "是否", "创建", "更新", "项目", "平台", "品牌", "关键词", "帖子", "报告", "日期"]

        for col in df.columns.tolist():
            v = row.get(col)
            if v is None or (isinstance(v, float) and pd.isna(v)):
                continue
            # numeric?
            try:
                n = pd.to_numeric(v, errors="coerce")
                if pd.notna(n):
                    numeric_like += 1
            except Exception:
                pass

            s = str(v).strip()
            if not s:
                continue
            non_empty.append(s)

            if s.lower() == str(col).strip().lower():
                hit += 2
            if any(t in s for t in tokens):
                hit += 1

        if not non_empty:
            return False

        # Real data rows (e.g. post_raw.raw_text) are usually long.
        max_len = max(len(s) for s in non_empty)
        if max_len > 16:
            return False

        # If it looks numeric, keep it (avoid skipping legitimate first row data).
        if numeric_like >= 2:
            return False

        return hit >= 2 and hit >= max(2, len(non_empty) // 3)

    @staticmethod
    def _drop_description_row(df: pd.DataFrame, *, skip_description_row: bool) -> pd.DataFrame:
        if not skip_description_row or df is None or df.empty:
            return df
        try:
            if ExcelRepository._looks_like_description_row(df):
                return df.iloc[1:].reset_index(drop=True)
        except Exception:
            return df
        return df

    def load_sheet_df(self, sheet_name: str, *, skip_description_row: bool = True) -> pd.DataFrame:
        try:
            df = pd.read_excel(self.path, sheet_name=sheet_name, dtype=object)
            df = self._drop_description_row(df, skip_description_row=skip_description_row)
            return df
        except Exception:
            # Missing sheet or invalid workbook state: return an empty dataframe so callers can handle gracefully.
            return pd.DataFrame()

    def _write(self, sheet: str, df: pd.DataFrame):
        # Pandas/OpenPyXL requires a valid excel extension.
        tmp_path = f"{self.path}.tmp.xlsx"
        last_err: Optional[Exception] = None
        for attempt in range(4):
            try:
                if os.path.exists(self.path):
                    shutil.copy2(self.path, tmp_path)
                    with pd.ExcelWriter(tmp_path, mode="a", engine="openpyxl", if_sheet_exists="replace") as writer:
                        df.to_excel(writer, sheet_name=sheet, index=False)
                    os.replace(tmp_path, self.path)
                else:
                    with pd.ExcelWriter(tmp_path, mode="w", engine="openpyxl") as writer:
                        df.to_excel(writer, sheet_name=sheet, index=False)
                    os.replace(tmp_path, self.path)
                last_err = None
                break
            except PermissionError as e:
                last_err = e
                # Common on Windows when the workbook is opened in Excel/WPS.
                # Also happens transiently due to antivirus/file indexers; do a short backoff.
                if attempt < 3:
                    time.sleep(0.2 * (attempt + 1))
                    continue
                raise ExcelWriteLockedError(self.path, sheet, e) from e
            finally:
                try:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except Exception:
                    pass

        if last_err is not None:
            raise last_err

    def insert(self, sheet: str, row: Dict):
        df = self.load_sheet_df(sheet, skip_description_row=True)
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
        self._write(sheet, df)

    def insert_many(self, sheet: str, rows: List[Dict]) -> int:
        if not rows:
            return 0
        df = self.load_sheet_df(sheet, skip_description_row=True)
        df = pd.concat([df, pd.DataFrame(rows)], ignore_index=True)
        self._write(sheet, df)
        return len(rows)

    def append_rows(self, sheet_name: str, rows: List[Dict]) -> int:
        return self.insert_many(sheet_name, rows)

    def replace_sheet(self, sheet_name: str, df: pd.DataFrame):
        df2 = df if df is not None else pd.DataFrame()
        self._write(sheet_name, df2)

    def replace(self, sheet: str, rows: List[Dict]):
        df = pd.DataFrame(rows) if rows is not None else pd.DataFrame()
        self._write(sheet, df)

    def query(self, sheet: str, filters: Optional[Dict] = None) -> pd.DataFrame:
        df = self.load_sheet_df(sheet, skip_description_row=True)
        # Many sheets include a second "Chinese header" row where `id` is non-numeric.
        # Only drop non-numeric `id` rows when the sheet actually contains numeric ids.
        if "id" in df.columns:
            numeric_ids = pd.to_numeric(df["id"], errors="coerce")
            if numeric_ids.notna().any():
                df = df[numeric_ids.notna()]
        if filters:
            for k, v in filters.items():
                if k not in df.columns:
                    continue
                df = df[df[k] == v]
        return df

    def update_by_id(self, sheet: str, row_id, updates: Dict) -> bool:
        df = self.load_sheet_df(sheet, skip_description_row=True)
        if "id" not in df.columns or df.empty:
            return False
        mask = df["id"] == row_id
        if not bool(mask.any()):
            return False
        for k, v in updates.items():
            if k not in df.columns:
                df[k] = None
            df.loc[mask, k] = v
        self._write(sheet, df)
        return True

class SqlRepository:
    def __init__(self, url: str):
        self.url = url
        # TODO: future mysql support via SQLAlchemy

    def insert(self, sheet: str, row: Dict):
        raise NotImplementedError

    def query(self, sheet: str, filters: Optional[Dict] = None):
        raise NotImplementedError

def get_repo():
    if DB_TYPE == "excel":
        return ExcelRepository(EXCEL_PATH)
    return SqlRepository(os.getenv("DATABASE_URL"))


def get_latest_pipeline_run_id(repo: ExcelRepository):
    df = repo.query("post_raw")
    if "pipeline_run_id" not in df.columns or df.empty:
        return None
    values = pd.to_numeric(df["pipeline_run_id"], errors="coerce")
    values = values.dropna()
    if values.empty:
        return None
    return int(values.max())

import os
import json
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional

DB_TYPE = os.getenv("DB_TYPE", "excel")   # excel / mysql
EXCEL_PATH = os.getenv("EXCEL_PATH", "backend/database/prodwatch_database.xlsx")

class ExcelRepository:
    def __init__(self, path: str):
        self.path = path

    def _read(self, sheet: str) -> pd.DataFrame:
        return pd.read_excel(self.path, sheet_name=sheet)

    def _write(self, sheet: str, df: pd.DataFrame):
        with pd.ExcelWriter(self.path, mode="a", engine="openpyxl", if_sheet_exists="replace") as writer:
            df.to_excel(writer, sheet_name=sheet, index=False)

    def insert(self, sheet: str, row: Dict):
        df = self._read(sheet)
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
        self._write(sheet, df)

    def query(self, sheet: str, filters: Optional[Dict] = None) -> pd.DataFrame:
        df = self._read(sheet)
        if filters:
            for k, v in filters.items():
                df = df[df[k] == v]
        return df


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

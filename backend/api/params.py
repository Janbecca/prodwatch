# 作用：后端 API：请求/查询参数与共享数据结构定义。

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from fastapi import Query


@dataclass(frozen=True)
class DateRange:
    start_date: str  # YYYY-MM-DD
    end_date: str  # YYYY-MM-DD


def parse_date(value: str) -> str:
    datetime.strptime(value, "%Y-%m-%d")
    return value


def parse_date_range(start_date: str, end_date: str) -> DateRange:
    s = parse_date(start_date)
    e = parse_date(end_date)
    if s > e:
        raise ValueError("start_date must be <= end_date")
    return DateRange(s, e)


def in_filter(field: str, values: Optional[list[Any]]) -> tuple[str, list[Any]]:
    if values is None:
        return "", []
    if len(values) == 0:
        return " AND 1=0", []
    placeholders = ",".join(["?"] * len(values))
    return f" AND {field} IN ({placeholders})", list(values)


def like_filter(field: str, value: Optional[str]) -> tuple[str, list[Any]]:
    if value is None or value.strip() == "":
        return "", []
    return f" AND {field} LIKE ?", [f"%{value.strip()}%"]


def common_query_params():
    # helper signature for reuse only (not used directly)
    return dict(
        project_id=Query(..., description="Project id"),
        start_date=Query(..., description="YYYY-MM-DD"),
        end_date=Query(..., description="YYYY-MM-DD"),
        platform_ids=Query(None, description="Repeated query: platform_ids=1&platform_ids=2"),
        brand_ids=Query(None, description="Repeated query: brand_ids=1&brand_ids=2"),
    )


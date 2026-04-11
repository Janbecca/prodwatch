from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any, Iterable, Optional


def now_ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def ensure_analysis_tables(con: sqlite3.Connection) -> None:
    """
    Best-effort schema creation for analysis storage (SQLite).

    This repo does not have a dedicated migration runner, so we create tables on demand.
    """
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS keyword_hit (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          post_id INTEGER NOT NULL,
          keyword TEXT NOT NULL,
          matched_text TEXT NOT NULL,
          created_at DATETIME,
          UNIQUE(post_id, keyword)
        );
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_keyword_hit_post ON keyword_hit(post_id);")

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS analysis_result (
          post_id INTEGER PRIMARY KEY,
          entities_json TEXT,
          features_json TEXT,
          issues_json TEXT,
          created_at DATETIME,
          updated_at DATETIME
        );
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS topic_result (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          post_id INTEGER NOT NULL,
          topic TEXT NOT NULL,
          confidence REAL,
          source TEXT NOT NULL,
          created_at DATETIME,
          UNIQUE(post_id, topic, source)
        );
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_topic_result_post ON topic_result(post_id);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_topic_result_topic ON topic_result(topic);")

    # Aggregated topic metric (daily) for dashboard/report usage.
    # This is derived from `topic_result` + `post_raw` and is safe to rebuild.
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_topic_metric (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          project_id INTEGER NOT NULL,
          brand_id INTEGER,
          platform_id INTEGER,
          stat_date DATE NOT NULL,
          topic TEXT NOT NULL,
          hit_count INTEGER,
          created_at DATETIME,
          UNIQUE(project_id, brand_id, platform_id, stat_date, topic)
        );
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_daily_topic_metric_proj_date ON daily_topic_metric(project_id, stat_date);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_daily_topic_metric_topic ON daily_topic_metric(topic);")


def insert_keyword_hits(con: sqlite3.Connection, *, post_id: int, hits: Iterable[dict[str, Any]]) -> None:
    ts = now_ts()
    for it in hits or []:
        if not isinstance(it, dict):
            continue
        kw = str(it.get("keyword") or "").strip()
        mt = str(it.get("matched_text") or "").strip()
        if not kw or not mt:
            continue
        con.execute(
            """
            INSERT INTO keyword_hit(post_id, keyword, matched_text, created_at)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(post_id, keyword) DO UPDATE SET
              matched_text=excluded.matched_text,
              created_at=excluded.created_at;
            """,
            (int(post_id), kw, mt, ts),
        )


def upsert_analysis_result(
    con: sqlite3.Connection,
    *,
    post_id: int,
    entities: Any,
    features: Any,
    issues: Any,
) -> None:
    ts = now_ts()

    def dumps(v: Any) -> str:
        try:
            return json.dumps(v if v is not None else [], ensure_ascii=False, default=str)
        except Exception:
            return "[]"

    ent = dumps(entities)
    feat = dumps(features)
    iss = dumps(issues)

    con.execute(
        """
        INSERT INTO analysis_result(post_id, entities_json, features_json, issues_json, created_at, updated_at)
        VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(post_id) DO UPDATE SET
          entities_json=excluded.entities_json,
          features_json=excluded.features_json,
          issues_json=excluded.issues_json,
          updated_at=excluded.updated_at;
        """,
        (int(post_id), ent, feat, iss, ts, ts),
    )


def insert_topic_results(
    con: sqlite3.Connection,
    *,
    post_id: int,
    topics: Iterable[Any],
    confidence: Optional[float] = 0.7,
    source: str = "llm",
) -> None:
    ts = now_ts()
    conf = None if confidence is None else float(confidence)
    src = str(source or "llm").strip() or "llm"
    for t in topics or []:
        topic = str(t or "").strip()
        if not topic:
            continue
        con.execute(
            """
            INSERT OR IGNORE INTO topic_result(post_id, topic, confidence, source, created_at)
            VALUES(?, ?, ?, ?, ?);
            """,
            (int(post_id), topic, conf, src, ts),
        )

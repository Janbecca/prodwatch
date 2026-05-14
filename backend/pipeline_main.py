# 浣滅敤锛氬悗绔富娴佺▼锛氫覆鑱旀姄鍙栤啋杩囨护鈫掑垎鏋愨啋鎶ュ憡鐢熸垚绛夋祦姘寸嚎銆?
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable, Optional

from backend.services.analyzer_service import (
    CleanPostResult,
    FeatureHit,
    KeywordHit,
    MockRuleAnalyzerService,
    PostInput,
    ProjectKeyword,
    SentimentResult,
    SpamResult,
)
from backend.services.text_analysis_pipeline import (
    PostAnalysisResult,
    keyword_hit,
    keyword_extraction_llm,
    merge_raw_payload,
    post_analysis_llm,
)
from backend.storage.analysis_store import (
    ensure_analysis_tables,
    insert_keyword_hits,
    insert_topic_results,
    upsert_analysis_result,
)
from backend.storage.crawl_job_progress_store import upsert_crawl_job_progress


DB_DEFAULT_PATH = "backend/database/database.sqlite"


def resolve_db_path(db_path: str) -> str:
    """
    Avoid accidentally creating a new empty sqlite db due to a wrong relative path.
    If `db_path` exists but doesn't look like the expected schema, try a sibling fallback.
    """
    if not os.path.exists(db_path):
        folder = os.path.dirname(db_path) or "."
        base = os.path.basename(db_path)
        if base == "database.sqlite":
            alt = os.path.join(folder, "database..sqlite")
            if os.path.exists(alt):
                return alt
        raise FileNotFoundError(db_path)

    def has_project_table(path: str) -> bool:
        try:
            con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            try:
                row = con.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='project' LIMIT 1;"
                ).fetchone()
                return row is not None
            finally:
                con.close()
        except sqlite3.Error:
            return False

    if has_project_table(db_path):
        return db_path

    folder = os.path.dirname(db_path) or "."
    base = os.path.basename(db_path)
    candidates = []
    if base == "database.sqlite":
        candidates.append(os.path.join(folder, "database..sqlite"))
    candidates.append(os.path.join(folder, "database.sqlite"))
    candidates.append(os.path.join(folder, "database..sqlite"))

    for c in candidates:
        if c != db_path and os.path.exists(c) and has_project_table(c):
            return c

    return db_path


def now_ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def parse_stat_date(value: Optional[str]) -> str:
    if value:
        datetime.strptime(value, "%Y-%m-%d")
        return value
    return datetime.utcnow().strftime("%Y-%m-%d")


def connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON;")
    con.execute("PRAGMA journal_mode = WAL;")
    con.execute("PRAGMA synchronous = NORMAL;")
    return con


def sha1_hex(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def normalize_crawl_source(value: Optional[str]) -> str:
    v = str(value or "").strip().lower()
    if v in ("media_crawler", "mediacrawler", "media-crawler"):
        return "media_crawler"
    return "mock_llm"


def default_crawl_source() -> str:
    return normalize_crawl_source(os.environ.get("PRODWATCH_CRAWL_SOURCE"))


def fetch_one_int(con: sqlite3.Connection, sql: str, params: tuple[Any, ...]) -> Optional[int]:
    row = con.execute(sql, params).fetchone()
    if not row:
        return None
    value = row[0]
    return int(value) if value is not None else None


def bootstrap_if_empty(con: sqlite3.Connection) -> int:
    existing_project_id = fetch_one_int(con, "SELECT id FROM project ORDER BY id LIMIT 1;", ())
    if existing_project_id is not None:
        return existing_project_id

    ts = now_ts()

    platforms = [
        ("douyin", "Douyin"),
        ("xhs", "Xiaohongshu"),
    ]
    for code, name in platforms:
        con.execute(
            "INSERT OR IGNORE INTO platform(code, name, is_enabled, created_at) VALUES(?, ?, 1, ?);",
            (code, name, ts),
        )

    con.execute(
        "INSERT OR IGNORE INTO brand(name, alias, category, created_at) VALUES(?, ?, ?, ?);",
        ("OurBrand", "OurBrand", "default", ts),
    )
    con.execute(
        "INSERT OR IGNORE INTO brand(name, alias, category, created_at) VALUES(?, ?, ?, ?);",
        ("CompetitorA", "CompetitorA", "default", ts),
    )
    our_brand_id = fetch_one_int(con, "SELECT id FROM brand WHERE name=?;", ("OurBrand",))
    competitor_id = fetch_one_int(con, "SELECT id FROM brand WHERE name=?;", ("CompetitorA",))

    con.execute(
        """
        INSERT INTO project(
          name, product_category, description, our_brand_id,
          status, is_active, refresh_mode, refresh_cron,
          last_refresh_at, created_at, updated_at, deleted_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (
            "Demo Project",
            "demo",
            "Auto-created demo project",
            our_brand_id,
            "active",
            1,
            "manual",
            None,
            None,
            ts,
            ts,
            None,
        ),
    )
    project_id = int(con.execute("SELECT last_insert_rowid();").fetchone()[0])

    platform_ids = [
        int(r["id"])
        for r in con.execute("SELECT id FROM platform WHERE is_enabled=1 ORDER BY id;").fetchall()
    ]
    for pid in platform_ids:
        con.execute(
            "INSERT OR IGNORE INTO project_platform(project_id, platform_id, created_at) VALUES(?, ?, ?);",
            (project_id, pid, ts),
        )

    if our_brand_id is not None:
        con.execute(
            "INSERT OR IGNORE INTO project_brand(project_id, brand_id, is_core_brand, created_at) VALUES(?, ?, 1, ?);",
            (project_id, our_brand_id, ts),
        )
    if competitor_id is not None:
        con.execute(
            "INSERT OR IGNORE INTO project_brand(project_id, brand_id, is_core_brand, created_at) VALUES(?, ?, 0, ?);",
            (project_id, competitor_id, ts),
        )

    keywords = [
        ("battery", "feature", 5),
        ("camera", "feature", 5),
        ("price", "feature", 4),
        ("lag", "issue", 4),
        ("overheat", "issue", 4),
    ]
    for kw, kw_type, weight in keywords:
        con.execute(
            """
            INSERT INTO project_keyword(project_id, keyword, keyword_type, weight, is_enabled, created_at)
            VALUES(?, ?, ?, ?, 1, ?);
            """,
            (project_id, kw, kw_type, weight, ts),
        )

    return project_id


def ensure_project_exists(con: sqlite3.Connection, project_id: int) -> None:
    row = con.execute("SELECT id FROM project WHERE id=?;", (project_id,)).fetchone()
    if not row:
        raise ValueError(f"project_id not found: {project_id}")


def load_project_scope(
    con: sqlite3.Connection, project_id: int
) -> tuple[list[int], list[int], list[str]]:
    platform_ids = [
        int(r["platform_id"])
        for r in con.execute(
            """
            SELECT pp.platform_id
            FROM project_platform pp
            JOIN platform p ON p.id = pp.platform_id
            WHERE pp.project_id=? AND p.is_enabled=1
            ORDER BY pp.platform_id;
            """,
            (project_id,),
        ).fetchall()
    ]
    brand_ids = [
        int(r["brand_id"])
        for r in con.execute(
            """
            SELECT brand_id
            FROM project_brand
            WHERE project_id=?
            ORDER BY is_core_brand DESC, brand_id;
            """,
            (project_id,),
        ).fetchall()
    ]
    keywords = [
        str(r["keyword"])
        for r in con.execute(
            """
            SELECT keyword
            FROM project_keyword
            WHERE project_id=? AND is_enabled=1
            ORDER BY COALESCE(weight, 0) DESC, id;
            """,
            (project_id,),
        ).fetchall()
    ]
    return platform_ids, brand_ids, keywords


def create_crawl_job(
    con: sqlite3.Connection,
    project_id: int,
    job_type: str = "manual",
    trigger_source: str = "cli",
    schedule_type: str = "manual",
    schedule_expr: Optional[str] = None,
    created_by: str = "system",
) -> int:
    """
    Create a crawl job record in a deterministic lifecycle:
    pending -> running -> (success|failed).

    Notes:
    - `finished_at` is a newer column that may not exist in older DBs; we write it when available.
    - Keep the job creation separate from "start" so it's easy to move to async execution later.
    """
    try:
        con.execute(
            """
            INSERT INTO crawl_job(
              project_id, job_type, trigger_source, schedule_type, schedule_expr,
              status, started_at, ended_at, finished_at, created_by, error_message
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                project_id,
                job_type,
                trigger_source,
                schedule_type,
                schedule_expr,
                "pending",
                None,
                None,
                None,
                created_by,
                None,
            ),
        )
    except sqlite3.OperationalError as e:
        # Backward compatibility: DB without `finished_at`.
        msg = str(e).lower()
        if ("no such column" not in msg) and ("has no column named" not in msg):
            raise
        con.execute(
            """
            INSERT INTO crawl_job(
              project_id, job_type, trigger_source, schedule_type, schedule_expr,
              status, started_at, ended_at, created_by, error_message
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                project_id,
                job_type,
                trigger_source,
                schedule_type,
                schedule_expr,
                "pending",
                None,
                None,
                created_by,
                None,
            ),
        )
    return int(con.execute("SELECT last_insert_rowid();").fetchone()[0])


@dataclass(frozen=True)
class CrawlTarget:
    id: int
    crawl_job_id: int
    platform_id: int
    brand_id: int
    keyword: str


def _crawl_job_target_has_columns(con: sqlite3.Connection, names: list[str]) -> bool:
    try:
        cols = {r[1] for r in con.execute("PRAGMA table_info(crawl_job_target);").fetchall()}
        return all(n in cols for n in names)
    except sqlite3.Error:
        return False


def mark_crawl_job_target_status(con: sqlite3.Connection, target_id: int, status: str) -> None:
    """
    Best-effort per-target status tracking (pending/running/success/failed).
    No-op on older DBs without the `status` column.
    """
    try:
        con.execute("UPDATE crawl_job_target SET status=? WHERE id=?;", (str(status), int(target_id)))
    except sqlite3.OperationalError as e:
        if "no such column" in str(e).lower():
            return
        raise


def mark_all_targets_failed(con: sqlite3.Connection, crawl_job_id: int) -> None:
    """
    Best-effort: mark all pending/running targets as failed when a job aborts.
    """
    try:
        con.execute(
            """
            UPDATE crawl_job_target
            SET status='failed'
            WHERE crawl_job_id=?
              AND (status IS NULL OR status IN ('pending','running'));
            """,
            (int(crawl_job_id),),
        )
    except sqlite3.OperationalError as e:
        if "no such column" in str(e).lower():
            return
        raise


def generate_crawl_job_targets(
    con: sqlite3.Connection,
    crawl_job_id: int,
    platform_ids: list[int],
    brand_ids: list[int],
    keywords: list[str],
) -> list[CrawlTarget]:
    has_rich_cols = _crawl_job_target_has_columns(con, ["project_id", "status", "created_at"])
    project_id: Optional[int] = None
    if has_rich_cols:
        row = con.execute("SELECT project_id FROM crawl_job WHERE id=? LIMIT 1;", (int(crawl_job_id),)).fetchone()
        project_id = int(row["project_id"]) if row and row["project_id"] is not None else None

    for platform_id in platform_ids:
        for brand_id in brand_ids:
            for keyword in keywords:
                if has_rich_cols and project_id is not None:
                    try:
                        con.execute(
                            """
                            INSERT OR IGNORE INTO crawl_job_target(
                              crawl_job_id, project_id, platform_id, brand_id, keyword, status, created_at
                            )
                            VALUES(?, ?, ?, ?, ?, ?, ?);
                            """,
                            (
                                int(crawl_job_id),
                                int(project_id),
                                int(platform_id),
                                int(brand_id),
                                str(keyword),
                                "pending",
                                now_ts(),
                            ),
                        )
                        continue
                    except sqlite3.OperationalError as e:
                        if "no such column" not in str(e).lower():
                            raise

                con.execute(
                    """
                    INSERT INTO crawl_job_target(crawl_job_id, platform_id, brand_id, keyword)
                    SELECT ?, ?, ?, ?
                    WHERE NOT EXISTS(
                      SELECT 1 FROM crawl_job_target
                      WHERE crawl_job_id=? AND platform_id=? AND brand_id=? AND keyword=?
                    );
                    """,
                    (
                        crawl_job_id,
                        platform_id,
                        brand_id,
                        keyword,
                        crawl_job_id,
                        platform_id,
                        brand_id,
                        keyword,
                    ),
                )

    rows = con.execute(
        """
        SELECT id, crawl_job_id, platform_id, brand_id, keyword
        FROM crawl_job_target
        WHERE crawl_job_id=?
        ORDER BY id;
        """,
        (crawl_job_id,),
    ).fetchall()
    return [
        CrawlTarget(
            id=int(r["id"]),
            crawl_job_id=int(r["crawl_job_id"]),
            platform_id=int(r["platform_id"]),
            brand_id=int(r["brand_id"]),
            keyword=str(r["keyword"]),
        )
        for r in rows
    ]


@dataclass(frozen=True)
class PostCandidate:
    project_id: int
    crawl_job_id: int
    platform_id: int
    brand_id: Optional[int]
    external_post_id: str
    author_name: str
    title: str
    content: str
    post_url: str
    publish_time: str
    crawled_at: str
    like_count: int
    comment_count: int
    share_count: int
    view_count: int
    raw_payload: str
    dedup_key: str
    created_at: str


def build_post_candidates(
    con: sqlite3.Connection,
    project_id: int,
    crawl_job_id: int,
    targets: list[CrawlTarget],
    stat_date: str,
    posts_per_target: int,
    crawl_source: str = "mock_llm",
) -> list[PostCandidate]:
    crawl_source = normalize_crawl_source(crawl_source)
    if crawl_source == "media_crawler":
        # Crawler-first strategy (HTTP):
        # - Call MediaCrawler via HTTP for dy/xhs search mode.
        # - If unavailable / errors / insufficient results, fall back to mock LLM generation.
        return _build_post_candidates_media_crawler_http(
            con=con,
            project_id=int(project_id),
            crawl_job_id=int(crawl_job_id),
            targets=list(targets or []),
            stat_date=str(stat_date),
            posts_per_target=int(posts_per_target),
        )

    # Default: mock LLM generation (deterministic seeds -> batch LLM generation).
    # NOTE: this intentionally does NOT generate fixed `posts_per_target` per crawl_job_target.
    return _build_post_candidates_realistic(con, project_id, crawl_job_id, targets, stat_date, posts_per_target)

_FORBIDDEN_FIELD_STITCH_RE = re.compile(r"\b(topic|brand|feature|feeling)\s*=")


def _mc_platform_from_platform_code(code: str) -> Optional[str]:
    c = str(code or "").strip().lower()
    if c in {"xhs", "xiaohongshu", "rednote"}:
        return "xhs"
    if c in {"dy", "douyin"}:
        return "dy"
    # Unsupported platform in this project.
    return None


def _mc_source_keyword(item: dict[str, Any]) -> str:
    # MediaCrawler uses `source_keyword`, but be tolerant.
    for k in ("source_keyword", "sourceKeyword", "keyword", "search_keyword", "searchKeyword"):
        v = item.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def _int_or_zero(v: Any) -> int:
    try:
        if v is None:
            return 0
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip()
        if s == "":
            return 0
        # MediaCrawler stores counts as strings in some stores.
        return int(float(s))
    except Exception:
        return 0


def _ts_from_epoch_s(v: Any) -> str:
    try:
        n = _int_or_zero(v)
        if n <= 0:
            return ""
        # MediaCrawler stores timestamps in seconds.
        return datetime.utcfromtimestamp(int(n)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


def _mk_candidate_from_mc(
    *,
    mc_platform: str,
    item: dict[str, Any],
    project_id: int,
    crawl_job_id: int,
    platform_id: int,
    brand_id: int,
    crawled_at: str,
    created_at: str,
) -> Optional[PostCandidate]:
    p = str(mc_platform or "").strip().lower()
    raw_id = ""
    url = ""
    title = ""
    content = ""
    publish_time = ""
    author = ""
    like_count = 0
    comment_count = 0
    share_count = 0
    view_count = 0

    if p == "dy":
        raw_id = str(item.get("aweme_id") or "").strip()
        url = str(item.get("aweme_url") or "").strip()
        title = str(item.get("title") or "").strip()
        content = str(item.get("desc") or "").strip() or title
        publish_time = _ts_from_epoch_s(item.get("create_time")) or ""
        author = str(item.get("nickname") or item.get("user_id") or "").strip()
        like_count = _int_or_zero(item.get("liked_count"))
        comment_count = _int_or_zero(item.get("comment_count"))
        share_count = _int_or_zero(item.get("share_count"))
        # view_count is not provided by default; keep 0.
    elif p == "xhs":
        raw_id = str(item.get("note_id") or "").strip()
        url = str(item.get("note_url") or "").strip()
        title = str(item.get("title") or "").strip()
        content = str(item.get("desc") or "").strip() or title
        publish_time = _ts_from_epoch_s(item.get("time")) or ""
        author = str(item.get("nickname") or item.get("user_id") or "").strip()
        like_count = _int_or_zero(item.get("liked_count"))
        comment_count = _int_or_zero(item.get("comment_count"))
        share_count = _int_or_zero(item.get("share_count"))
        view_count = 0
    else:
        return None

    if not raw_id:
        # Try a generic id field.
        raw_id = str(item.get("external_post_id") or item.get("post_id") or "").strip()
    if not url:
        url = str(item.get("url") or item.get("post_url") or "").strip()
    if not publish_time:
        publish_time = created_at
    if not title:
        title = content[:60] if content else ""
    if not content:
        content = title
    if not url:
        # Fallback: keep a stable placeholder; downstream will dedup by dedup_key.
        url = f"https://mediacrawler.local/{p}/post/{raw_id or sha1_hex(json.dumps(item, ensure_ascii=False, default=str))[:12]}"

    external_post_id = raw_id or sha1_hex(url)[:18]
    dedup_key = sha1_hex(url)
    raw_payload = json.dumps(item, ensure_ascii=False, default=str)
    return PostCandidate(
        project_id=int(project_id),
        crawl_job_id=int(crawl_job_id),
        platform_id=int(platform_id),
        brand_id=int(brand_id),
        external_post_id=str(external_post_id),
        author_name=str(author or ""),
        title=str(title),
        content=str(content),
        post_url=str(url),
        publish_time=str(publish_time),
        crawled_at=str(crawled_at),
        like_count=int(like_count),
        comment_count=int(comment_count),
        share_count=int(share_count),
        view_count=int(view_count),
        raw_payload=str(raw_payload),
        dedup_key=str(dedup_key),
        created_at=str(created_at),
    )


def _build_post_candidates_media_crawler_http(
    *,
    con: sqlite3.Connection,
    project_id: int,
    crawl_job_id: int,
    targets: list[CrawlTarget],
    stat_date: str,
    posts_per_target: int,
) -> list[PostCandidate]:
    """
    Crawler-first implementation:
    - Use MediaCrawler WebUI API via HTTP (no import of external crawler modules).
    - Platforms supported: dy/xhs.
    - Query: product_category + brand_name
    - If crawler fails/insufficient, fall back to mock LLM generation (per platform+brand).
    """
    from backend.services.mediacrawler_http import MediaCrawlerHttpClient
    import os

    ts = now_ts()
    crawled_at = ts
    created_at = ts

    # Limits: keep small to reduce platform ban risk.
    try:
        max_posts_total = int(str(os.environ.get("PRODWATCH_MANUAL_REFRESH_MAX_POSTS_TOTAL") or "60").strip())
    except Exception:
        max_posts_total = 60
    max_posts_total = max(5, min(int(max_posts_total), 500))

    platform_rows = con.execute("SELECT id, code FROM platform;").fetchall()
    platform_code_by_id = {int(r["id"]): str(r["code"] or "").strip().lower() for r in platform_rows}

    brand_rows = con.execute("SELECT id, name FROM brand;").fetchall()
    brand_name_by_id = {int(r["id"]): str(r["name"] or "").strip() for r in brand_rows}

    project_row = con.execute("SELECT product_category FROM project WHERE id=? LIMIT 1;", (int(project_id),)).fetchone()
    product_category = str(project_row["product_category"] or "").strip() if project_row else ""

    # Build unique platform+brand pairs from targets (ignore target.keyword).
    pair_to_query: dict[tuple[int, int], str] = {}
    pair_to_mc_platform: dict[tuple[int, int], str] = {}
    mc_platform_to_pairs: dict[str, list[tuple[int, int]]] = {}
    for t in targets or []:
        try:
            pid = int(t.platform_id)
            bid = int(t.brand_id)
        except Exception:
            continue
        code = platform_code_by_id.get(pid, "")
        mc_platform = _mc_platform_from_platform_code(code)
        if not mc_platform:
            continue
        brand_name = brand_name_by_id.get(bid, f"b{bid}")
        q = f"{product_category} {brand_name}".strip()
        key = (pid, bid)
        if key not in pair_to_query:
            pair_to_query[key] = q
            pair_to_mc_platform[key] = mc_platform
            mc_platform_to_pairs.setdefault(mc_platform, []).append(key)

    # For unsupported platforms in this project, fall back to mock generation directly.
    unsupported_targets: list[CrawlTarget] = []
    for t in targets or []:
        code = platform_code_by_id.get(int(t.platform_id), "")
        if _mc_platform_from_platform_code(code) is None:
            unsupported_targets.append(t)

    client = MediaCrawlerHttpClient()

    def _progress(message: str, meta: Optional[dict[str, Any]] = None) -> None:
        try:
            upsert_crawl_job_progress(
                con,
                crawl_job_id=int(crawl_job_id),
                stage="simulate",
                message=str(message),
                meta=meta,
            )
            con.commit()
        except Exception:
            pass

    _progress(
        "crawler_first started",
        {
            "strategy": "prefer_media_crawler_then_llm",
            "platforms": sorted(list(mc_platform_to_pairs.keys())),
            "posts_per_target": int(posts_per_target),
            "max_posts_total": int(max_posts_total),
            "product_category": product_category,
        },
    )

    crawler_available = client.health()
    env_ok, env_msg = client.env_check_best_effort()
    if not crawler_available:
        _progress(
            "mediacrawler unavailable; fallback to mock_llm",
            {"ok": False, "health": False, "env_ok": bool(env_ok), "env_msg": str(env_msg)},
        )
        # Full fallback (crawler unreachable).
        candidates = []
        if targets:
            candidates.extend(
                _build_post_candidates_realistic(con, project_id, crawl_job_id, targets, stat_date, posts_per_target)
            )
        return candidates

    if not env_ok:
        # Env check is best-effort; do not block crawling. The actual /crawler/start result is the source of truth.
        _progress(
            "mediacrawler env_check failed; continue anyway",
            {"ok": False, "health": True, "env_ok": False, "env_msg": str(env_msg)},
        )

    candidates: list[PostCandidate] = []
    crawled_count_by_pair: dict[tuple[int, int], int] = {k: 0 for k in pair_to_query.keys()}

    # Run per platform sequentially (MediaCrawler is a global singleton).
    for mc_platform, pairs in mc_platform_to_pairs.items():
        pairs = list(pairs or [])
        # One crawl per platform for all brand queries.
        queries = [pair_to_query[p] for p in pairs if pair_to_query.get(p)]
        # Respect max_posts_total by limiting preview limit; still let crawler fetch whatever it does.
        preview_limit = min(max_posts_total * 4, max(200, len(queries) * max(1, int(posts_per_target)) * 4))

        _progress(
            f"mediacrawler start platform={mc_platform}",
            {"platform": mc_platform, "queries": int(len(queries)), "preview_limit": int(preview_limit)},
        )

        res = client.run_search_and_collect(platform=mc_platform, keywords_csv=",".join(queries), preview_limit=int(preview_limit))
        if not res.ok:
            _progress(
                f"mediacrawler failed platform={mc_platform}; fallback to mock_llm",
                {
                    "platform": mc_platform,
                    "ok": False,
                    "error": str(res.error or ""),
                    "took_ms": int(res.took_ms),
                    "new_files": list(res.new_files),
                    "chosen_file": str(res.chosen_file or ""),
                    "logs_tail": list(res.logs_tail)[-20:],
                },
            )
            continue

        items = list(res.items or [])
        # Attribute items back to queries using source_keyword.
        by_query: dict[str, list[dict[str, Any]]] = {q: [] for q in queries}
        for it in items:
            if not isinstance(it, dict):
                continue
            q = _mc_source_keyword(it)
            if q and q in by_query and len(by_query[q]) < max(1, int(posts_per_target)):
                by_query[q].append(it)

        # Build candidates per pair.
        total_kept = 0
        for platform_id, brand_id in pairs:
            q = pair_to_query.get((platform_id, brand_id), "")
            kept = by_query.get(q) or []
            for it in kept[: max(1, int(posts_per_target))]:
                c = _mk_candidate_from_mc(
                    mc_platform=mc_platform,
                    item=it,
                    project_id=int(project_id),
                    crawl_job_id=int(crawl_job_id),
                    platform_id=int(platform_id),
                    brand_id=int(brand_id),
                    crawled_at=str(crawled_at),
                    created_at=str(created_at),
                )
                if c is not None:
                    candidates.append(c)
                    total_kept += 1
            crawled_count_by_pair[(platform_id, brand_id)] = min(max(0, len(kept)), int(posts_per_target))

        _progress(
            f"mediacrawler done platform={mc_platform}",
            {
                "platform": mc_platform,
                "ok": True,
                "took_ms": int(res.took_ms),
                "new_files": list(res.new_files),
                "chosen_file": str(res.chosen_file or ""),
                "preview_total": int(res.preview_total),
                "kept_total": int(total_kept),
                "kept_by_query": {k: int(len(v or [])) for k, v in by_query.items() if (v or [])},
                "logs_tail": list(res.logs_tail)[-20:],
            },
        )

    # Compute fallback gaps (crawler failure or insufficient results).
    missing_by_pair: dict[tuple[int, int], int] = {}
    offset_by_pair: dict[tuple[int, int], int] = {}
    fallback_targets: list[CrawlTarget] = []
    seen_pairs: set[tuple[int, int]] = set()
    for t in targets or []:
        key = (int(t.platform_id), int(t.brand_id))
        if key in seen_pairs:
            continue
        seen_pairs.add(key)
        want = max(1, int(posts_per_target))
        got = int(crawled_count_by_pair.get(key, 0))
        need = max(0, want - got)
        if need <= 0:
            continue
        missing_by_pair[key] = int(need)
        offset_by_pair[key] = int(got)
        fallback_targets.append(t)

    # Include unsupported platforms in fallback.
    # Keep their unique pairs and generate full posts_per_target for them.
    for t in unsupported_targets:
        key = (int(t.platform_id), int(t.brand_id))
        if key in missing_by_pair:
            continue
        missing_by_pair[key] = max(1, int(posts_per_target))
        offset_by_pair[key] = 0
        fallback_targets.append(t)

    if missing_by_pair:
        _progress(
            "mock_llm fallback start",
            {"missing_pairs": int(len(missing_by_pair)), "missing_total": int(sum(missing_by_pair.values()))},
        )
        fallback_candidates = _build_post_candidates_realistic(
            con,
            int(project_id),
            int(crawl_job_id),
            fallback_targets,
            str(stat_date),
            int(posts_per_target),
            posts_per_pair=missing_by_pair,
            seed_index_offset_by_pair=offset_by_pair,
        )
        candidates.extend(list(fallback_candidates or []))
        _progress(
            "mock_llm fallback done",
            {"generated_total": int(len(fallback_candidates or [])), "missing_pairs": int(len(missing_by_pair))},
        )

    return candidates


def _build_post_candidates_realistic(
    con: sqlite3.Connection,
    project_id: int,
    crawl_job_id: int,
    targets: list[CrawlTarget],
    stat_date: str,
    posts_per_target: int,
    *,
    posts_per_pair: Optional[dict[tuple[int, int], int]] = None,
    seed_index_offset_by_pair: Optional[dict[tuple[int, int], int]] = None,
) -> list[PostCandidate]:
    """
    Realism-oriented simulated crawl:
    - build per-refresh distribution plan with dynamic sentiment mix (floats per job)
    - construct seeds (one seed => one post)
    - ask crawler_generation LLM to write title/content
    - run lightweight anti-template rewrite for repeated patterns
    """
    from backend.llm.router import get_llm_router
    from backend.llm.prompts.store import get_prompt_store
    from backend.llm.schema_log import log_llm_schema
    import logging
    import os

    log = logging.getLogger("prodwatch.pipeline")

    platform_rows = con.execute("SELECT id, code, name FROM platform;").fetchall()
    platform_map = {int(r["id"]): (str(r["code"]), str(r["name"])) for r in platform_rows}
    brand_rows = con.execute("SELECT id, name FROM brand;").fetchall()
    brand_map = {int(r["id"]): str(r["name"]) for r in brand_rows}
    project_row = con.execute("SELECT product_category FROM project WHERE id=? LIMIT 1;", (int(project_id),)).fetchone()
    product_category = str(project_row["product_category"] or "") if project_row else ""
    now_dt = datetime.utcnow()
    crawled_at = now_ts()

    kw_rows = con.execute(
        """
        SELECT keyword
        FROM project_keyword
        WHERE project_id=? AND is_enabled=1
        ORDER BY COALESCE(weight, 0) DESC, id;
        """,
        (int(project_id),),
    ).fetchall()
    monitor_keywords = [str(r["keyword"] or "").strip() for r in kw_rows if (r["keyword"] or "").strip() != ""]

    def pick_hints(seed_id: str, k: int = 3) -> list[str]:
        if not monitor_keywords:
            return []
        uniq: list[str] = []
        base = int(sha1_hex(seed_id)[:8], 16)
        for i in range(min(int(k), 6)):
            idx = (base + i * 131) % len(monitor_keywords)
            kw = str(monitor_keywords[idx])
            if kw and kw not in uniq:
                uniq.append(kw)
        return uniq

    # Build seeds per unique (platform_id, brand_id) pair.
    pairs: list[tuple[int, int]] = []
    seen_pairs: set[tuple[int, int]] = set()
    for t in targets or []:
        key = (int(t.platform_id), int(t.brand_id))
        if key in seen_pairs:
            continue
        seen_pairs.add(key)
        pairs.append(key)

    scene_pool = ["看娃", "看老人", "看宠物", "门口安防", "夜间看家", "店铺监控", "远程看店", "快递看护"]
    topic_pool = [x for x in monitor_keywords if x] or ["画质", "延迟", "报警", "回看", "夜视", "售后", "云存储", "连接稳定"]

    def _pick(items: list[str], key: str) -> str:
        if not items:
            return ""
        h = int(sha1_hex(key)[:8], 16)
        return str(items[h % len(items)])

    def _sentiment_thresholds() -> tuple[float, float, float]:
        h = int(sha1_hex(f"sentiment|{int(crawl_job_id)}|{str(stat_date)}")[:12], 16)

        def _j(idx: int) -> float:
            raw = ((h >> (idx * 8)) & 0xFF) / 255.0
            return (raw - 0.5) * 0.16

        pos = max(0.08, 0.20 + _j(0))
        neu = max(0.12, 0.30 + _j(1))
        neg = max(0.12, 0.30 + _j(2))
        mixed = max(0.08, 1.0 - pos - neu - neg)
        s = pos + neu + neg + mixed
        pos, neu, neg = pos / s, neu / s, neg / s
        return pos, pos + neu, pos + neu + neg

    t_pos, t_neu, t_neg = _sentiment_thresholds()

    def _pick_sentiment(seed_id: str, platform_code: str) -> str:
        h = int(sha1_hex(f"sentiment-pick|{seed_id}")[:8], 16)
        x = (h % 10000) / 10000.0
        pc = str(platform_code or "").strip().lower()
        if pc in {"xhs", "xiaohongshu", "rednote"}:
            x = min(0.9999, x + 0.04)
        elif pc in {"dy", "douyin"}:
            x = max(0.0, x - 0.02)
        if x < t_pos:
            return "positive"
        if x < t_neu:
            return "neutral"
        if x < t_neg:
            return "negative"
        return "mixed"

    def _pick_publish_time(seed_id: str, index_i: int) -> str:
        h = int(sha1_hex(f"time|{seed_id}|{index_i}")[:12], 16)
        day_offset = h % 7
        p = (h // 7) % 1000
        if p < 200:
            hour_base = 8
        elif p < 450:
            hour_base = 12
        elif p < 850:
            hour_base = 19
        else:
            hour_base = 22
        minute = (h // 1000) % 60
        second = (h // 60000) % 60
        hour = hour_base + int(((h // 3600000) % 3) - 1)
        hour = max(0, min(23, hour))
        dt = now_dt - timedelta(days=int(day_offset))
        dt = dt.replace(hour=hour, minute=minute, second=second, microsecond=0)
        if dt > now_dt:
            dt = now_dt - timedelta(minutes=(index_i % 29) + 1)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def _engagement(seed_id: str, sentiment: str, platform_code: str) -> tuple[int, int, int, int]:
        h = int(sha1_hex(f"eng|{seed_id}|{sentiment}|{platform_code}")[:14], 16)
        q = (h % 1000) / 1000.0
        if q < 0.78:
            like = 2 + (h % 65)
            comment = (h // 97) % 18
            share = (h // 571) % 9
        elif q < 0.95:
            like = 30 + (h % 240)
            comment = 4 + ((h // 97) % 70)
            share = 1 + ((h // 571) % 36)
        else:
            like = 220 + (h % 1200)
            comment = 18 + ((h // 97) % 320)
            share = 5 + ((h // 571) % 130)
        if sentiment in {"negative", "mixed"}:
            comment = int(comment * 1.25) + int((h // 1111) % 7)
        if str(platform_code or "").lower() in {"dy", "douyin"}:
            view = like * 35 + comment * 16 + share * 52 + (h % 500)
        else:
            view = like * 20 + comment * 18 + share * 40 + (h % 260)
        return max(0, int(like)), max(0, int(comment)), max(0, int(share)), max(1, int(view))

    seeds: list[dict[str, Any]] = []
    for platform_id, brand_id in pairs:
        platform_code, platform_name = platform_map.get(int(platform_id), (f"p{platform_id}", ""))
        brand_name = brand_map.get(int(brand_id), f"b{brand_id}")
        n = int(posts_per_target)
        if posts_per_pair is not None:
            try:
                n = int(posts_per_pair.get((int(platform_id), int(brand_id)), 0))
            except Exception:
                n = 0
        if n <= 0:
            continue
        offset = 0
        if seed_index_offset_by_pair is not None:
            try:
                offset = int(seed_index_offset_by_pair.get((int(platform_id), int(brand_id)), 0))
            except Exception:
                offset = 0
        offset = max(0, offset)
        for i in range(int(n)):
            seed_i = int(offset) + int(i)
            seed_id = f"{int(crawl_job_id)}|{int(platform_id)}|{int(brand_id)}|{seed_i}"
            publish_time = _pick_publish_time(seed_id, seed_i)
            external_post_id = sha1_hex(f"gen|{seed_id}")[:18]
            post_url = f"https://example.local/{platform_code}/post/{external_post_id}?job={int(crawl_job_id)}"
            scene = _pick(scene_pool, f"scene|{seed_id}")
            topic = _pick(topic_pool, f"topic|{seed_id}")
            sentiment = _pick_sentiment(seed_id, str(platform_code))
            platform_style = "douyin" if str(platform_code).lower() in {"dy", "douyin"} else ("xiaohongshu" if str(platform_code).lower() in {"xhs", "xiaohongshu", "rednote"} else "generic")
            like_count, comment_count, share_count, view_count = _engagement(seed_id, sentiment, str(platform_code))
            author_name = f"u{sha1_hex('author|' + seed_id)[:8]}"

            seeds.append(
                {
                    "seed_id": seed_id,
                    "platform_id": int(platform_id),
                    "platform_code": str(platform_code),
                    "platform_name": str(platform_name),
                    "brand_id": int(brand_id),
                    "brand_name": str(brand_name),
                    "keyword_hints": pick_hints(seed_id, 3),
                    "author_name": author_name,
                    "external_post_id": external_post_id,
                    "post_url": post_url,
                    "publish_time": publish_time,
                    "scene": scene,
                    "topic": topic,
                    "sentiment": sentiment,
                    "platform_style": platform_style,
                    "like_count": like_count,
                    "comment_count": comment_count,
                    "share_count": share_count,
                    "view_count": view_count,
                    "raw_payload": json.dumps(
                        {
                            "generated_seed": {
                                "seed_id": seed_id,
                                "platform_code": platform_code,
                                "platform_name": platform_name,
                                "brand_id": int(brand_id),
                                "brand_name": brand_name,
                                "product_category": product_category,
                                "keyword_hints": pick_hints(seed_id, 3),
                                "post_profile": {
                                    "scene": scene,
                                    "sentiment": sentiment,
                                    "platform_style": platform_style,
                                    "topic": topic,
                                },
                            }
                        },
                        ensure_ascii=False,
                        default=str,
                    ),
                }
            )

    for t in targets:
        mark_crawl_job_target_status(con, int(t.id), "running")

    prompt_version: str = get_prompt_store().get("crawler_generation").version

    try:
        batch_size = int(str(os.environ.get("PRODWATCH_CRAWLER_GENERATION_SEED_BATCH_SIZE") or "4").strip())
    except Exception:
        batch_size = 4
    batch_size = max(1, min(20, int(batch_size)))

    def _seed_payload(s: dict[str, Any]) -> dict[str, Any]:
        return {
            "seed_id": s["seed_id"],
            "platform_id": s["platform_id"],
            "platform_code": s["platform_code"],
            "platform_name": s.get("platform_name"),
            "brand_id": s.get("brand_id"),
            "brand_name": s.get("brand_name"),
            "keyword_hints": s.get("keyword_hints") or [],
            "publish_time": s.get("publish_time"),
            "scene": s.get("scene"),
            "topic": s.get("topic"),
            "sentiment": s.get("sentiment"),
            "platform_style": s.get("platform_style"),
        }

    def _chunks(items: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
        if size <= 0:
            return [items]
        return [items[i : i + size] for i in range(0, len(items), size)]

    generated_map: dict[str, dict[str, Any]] = {}
    router_provider: str = ""
    router_model: str = ""
    batches = _chunks(seeds, batch_size)
    for bi, batch in enumerate(batches, start=1):
        log.info(
            "crawler_generation batch_start project_id=%s crawl_job_id=%s batch=%s/%s seeds=%s",
            int(project_id),
            int(crawl_job_id),
            bi,
            len(batches),
            len(batch),
        )
        res = get_llm_router().run(
            task_type="crawler_generation",
            input={
                "project_id": int(project_id),
                "crawl_job_id": int(crawl_job_id),
                "stat_date": str(stat_date),
                "product_category": product_category,
                "seeds": [_seed_payload(s) for s in batch],
                "crawled_at": str(crawled_at),
            },
            con=con,
            enable_cache=False,
            enable_log=False,
            strict=True,
        )
        try:
            log_llm_schema(con, crawl_job_id=int(crawl_job_id), task_type="crawler_generation", res=res)
        except Exception:
            pass
        log.warning(
            "crawler_generation raw_response crawl_job_id=%s batch=%s provider=%s model=%s ok=%s output_type=%s output_preview=%s",
            int(crawl_job_id),
            bi,
            res.provider,
            res.model,
            res.ok,
            type(res.output).__name__,
            json.dumps(res.output, ensure_ascii=False, default=str)[:2000],
        )
        if not bool(res.ok):
            raise RuntimeError(
                f"crawler_generation failed (batch {bi}/{len(batches)} size={len(batch)}): {res.error or 'unknown error'}"
            )
        cur_provider = str(res.provider or "")
        cur_model = str(res.model or "")
        if str(cur_provider).strip().lower() == "mock":
            raise RuntimeError("crawler_generation failed: strict mode does not allow mock provider")
        if not router_provider:
            router_provider = cur_provider
            router_model = cur_model

        maybe = (res.output or {}).get("posts") if isinstance(res.output, dict) else None
        batch_map: dict[str, dict[str, Any]] = {}
        if isinstance(maybe, list):
            for item in maybe:
                if not isinstance(item, dict):
                    continue
                sid = str(item.get("seed_id") or "").strip()
                if sid:
                    batch_map[sid] = item
                    generated_map[sid] = item

        missing_batch = [str(s["seed_id"]) for s in batch if str(s["seed_id"]) not in batch_map]
        if missing_batch:
            try:
                log.error(
                    "crawler_generation missing outputs for seeds (batch %s/%s) missing=%s total_missing=%s",
                    bi,
                    len(batches),
                    missing_batch[:5],
                    len(missing_batch),
                )
            except Exception:
                pass
            for sid in missing_batch:
                generated_map.setdefault(str(sid), {})

    if not seeds:
        raise RuntimeError("crawler_generation seeds is empty")
    if len(generated_map) < len(seeds):
        missing = [str(s["seed_id"]) for s in seeds if str(s["seed_id"]) not in generated_map]
        try:
            log.error(
                "crawler_generation missing outputs for seeds (final) missing=%s total_missing=%s",
                missing[:5],
                len(missing),
            )
        except Exception:
            pass
        for sid in missing:
            generated_map.setdefault(str(sid), {})

    invalid_fallback_count = 0
    rewrite_count = 0

    repetitive_phrases = {"真实体验", "我是真没想到", "先观望", "有点小问题", "还行但也不是完美"}
    scene_terms = set(scene_pool)

    def _norm_text(v: str) -> str:
        s = str(v or "").strip().lower()
        for ch in [" ", "\n", "\t", "，", "。", ",", ".", "！", "?", "？", "：", "；", "、", "（", "）", "(", ")"]:
            s = s.replace(ch, "")
        return s

    def _prefix15(v: str) -> str:
        s = _norm_text(v)
        return s[:15] if s else ""

    def _contains_scene(content: str, scene: str) -> bool:
        c = str(content or "")
        if scene and scene in c:
            return True
        return any(x in c for x in scene_terms)

    def _fallback_title(seed: dict[str, Any]) -> str:
        sid = str(seed.get("seed_id") or "")
        brand = str(seed.get("brand_name") or "").strip() or "某品牌"
        scene = str(seed.get("scene") or "日常看护")
        topic = str(seed.get("topic") or "体验")
        sentiment = str(seed.get("sentiment") or "neutral")
        h = int(sha1_hex(f"ft|{sid}")[:6], 16)
        if sentiment == "negative":
            pool = [
                f"{brand}{topic}这点有点劝退",
                f"{scene}场景下，{brand}{topic}有点烦",
                f"{brand}用着不算差，但{topic}真要优化",
            ]
        elif sentiment == "mixed":
            pool = [
                f"{brand}整体可以，但{topic}我还在纠结",
                f"{scene}里能用，但{topic}这个点别忽略",
                f"{brand}有优点也有坑，{topic}最明显",
            ]
        elif sentiment == "positive":
            pool = [
                f"{scene}用{brand}，目前{topic}挺稳",
                f"{brand}{topic}比我预期好一些",
                f"{scene}这几天用下来，{brand}还不错",
            ]
        else:
            pool = [
                f"{brand}{topic}是通病吗？",
                f"{scene}里试了下{brand}，说下{topic}",
                f"{brand}这个{topic}，大家体验如何",
            ]
        return str(pool[h % len(pool)])

    def _fallback_content(seed: dict[str, Any]) -> str:
        sid = str(seed.get("seed_id") or "")
        brand = str(seed.get("brand_name") or "").strip() or "某品牌"
        scene = str(seed.get("scene") or "日常看护")
        topic = str(seed.get("topic") or "体验")
        sentiment = str(seed.get("sentiment") or "neutral")
        platform_code = str(seed.get("platform_code") or "").strip().lower()
        h = int(sha1_hex(f"fc|{sid}|{brand}|{scene}|{topic}")[:6], 16)
        if platform_code in {"douyin", "dy"}:
            if sentiment == "negative":
                pool = [
                    f"{scene}的时候，{brand}{topic}反复出问题，评论区有人同款吗。",
                    f"本来想省心，结果{scene}里{topic}挺折腾，先观望。",
                ]
            elif sentiment == "mixed":
                pool = [
                    f"{scene}里能用是能用，但{topic}这块时好时坏，体验有点拧巴。",
                    f"{brand}整体OK，不过{scene}场景下{topic}偶发卡顿，挺影响心情。",
                ]
            elif sentiment == "positive":
                pool = [
                    f"{scene}这几天一直开着，{brand}{topic}基本稳，省了不少心。",
                    f"{scene}场景下表现超预期，{topic}这块没怎么踩坑。",
                ]
            else:
                pool = [
                    f"{scene}里试了一周，{brand}{topic}有优点也有小问题，继续观察。",
                    f"{brand}放在{scene}场景里还行，{topic}算中规中矩。",
                ]
        else:
            if sentiment == "negative":
                pool = [
                    f"我主要用在{scene}，{brand}的{topic}最近确实影响体验，尤其是关键时刻会掉链子。",
                    f"{scene}这个场景对稳定性要求高，{brand}{topic}目前看还有明显改进空间。",
                ]
            elif sentiment == "mixed":
                pool = [
                    f"在{scene}场景里，{brand}整体功能是够用的，但{topic}这块会打断使用节奏。",
                    f"{brand}基础能力不错，不过放到{scene}里，{topic}偶发问题会让人有点膈应。",
                ]
            elif sentiment == "positive":
                pool = [
                    f"我在{scene}连续用了几天，{brand}{topic}表现比较稳定，暂时没遇到大问题。",
                    f"{scene}需求下，{brand}的{topic}比预想更顺手，日常使用负担不大。",
                ]
            else:
                pool = [
                    f"目前主要放在{scene}使用，{brand}{topic}没有特别惊艳，但也不算踩雷。",
                    f"从{scene}的实际需求看，{brand}{topic}处于能用状态，后续继续看稳定性。",
                ]
        return str(pool[h % len(pool)])

    title_seen: dict[str, int] = {}
    content_prefix_seen: dict[str, int] = {}
    phrase_seen: dict[str, int] = {}

    candidates: list[PostCandidate] = []
    for s in seeds:
        sid = str(s["seed_id"])
        gen = generated_map.get(sid) or {}
        title_raw = gen.get("title")
        content_raw = gen.get("content")

        title = str(title_raw or "").strip()
        content = str(content_raw or "").strip()

        if not title or not content:
            log.error(
                "crawler_generation invalid_output seed_id=%s title_raw=%r content_raw=%r gen=%s",
                sid,
                title_raw,
                content_raw,
                json.dumps(gen, ensure_ascii=False, default=str),
            )
            title = title or _fallback_title(s)
            content = content or _fallback_content(s)
            invalid_fallback_count += 1

        if _FORBIDDEN_FIELD_STITCH_RE.search(content):
            raise RuntimeError(f"crawler_generation invalid content (field stitch) seed_id={sid}")

        scene = str(s.get("scene") or "")
        pfx = _prefix15(content)
        tkey = _norm_text(title)
        repeated_phrase_hit = ""
        for p in repetitive_phrases:
            if p in content or p in title:
                if phrase_seen.get(p, 0) >= 2:
                    repeated_phrase_hit = p
                    break

        need_rewrite = False
        if tkey and title_seen.get(tkey, 0) >= 1:
            need_rewrite = True
        if pfx and content_prefix_seen.get(pfx, 0) >= 1:
            need_rewrite = True
        if repeated_phrase_hit:
            need_rewrite = True
        if len(_norm_text(content)) < 28 and not _contains_scene(content, scene):
            need_rewrite = True
        if "真实体验" in title:
            need_rewrite = True

        if need_rewrite:
            title = _fallback_title(s)
            content = _fallback_content(s)
            rewrite_count += 1

        if scene and scene not in content:
            content = f"{content} 我这边主要是{scene}场景在用。"

        t2 = _norm_text(title)
        p2 = _prefix15(content)
        if t2:
            title_seen[t2] = title_seen.get(t2, 0) + 1
        if p2:
            content_prefix_seen[p2] = content_prefix_seen.get(p2, 0) + 1
        for p in repetitive_phrases:
            if p in content or p in title:
                phrase_seen[p] = phrase_seen.get(p, 0) + 1

        raw_payload = _merge_raw_payload_text(
            s.get("raw_payload"),
            {
                "source": "llm_fallback",
                "generation_version": "mock_llm_realistic_v2",
                "generated_by": "llm",
                "provider": str(router_provider or ""),
                "model": str(router_model or ""),
                "prompt_version": str(prompt_version or ""),
                "post_profile": {
                    "scene": s.get("scene"),
                    "sentiment": s.get("sentiment"),
                    "platform_style": s.get("platform_style"),
                    "topic": s.get("topic"),
                },
                "seed": {
                    "seed_id": sid,
                    "platform_code": s.get("platform_code"),
                    "brand_id": s.get("brand_id"),
                    "keyword_hints": s.get("keyword_hints") or [],
                },
            },
        )
        post_url = str(s["post_url"])
        dedup_key = sha1_hex(post_url)
        candidates.append(
            PostCandidate(
                project_id=int(project_id),
                crawl_job_id=int(crawl_job_id),
                platform_id=int(s["platform_id"]),
                brand_id=(int(s["brand_id"]) if s.get("brand_id") is not None else None),
                external_post_id=str(s["external_post_id"]),
                author_name=str(s["author_name"]),
                title=title,
                content=content,
                post_url=post_url,
                publish_time=str(s["publish_time"]),
                crawled_at=str(crawled_at),
                like_count=int(s["like_count"]),
                comment_count=int(s["comment_count"]),
                share_count=int(s["share_count"]),
                view_count=int(s["view_count"]),
                raw_payload=str(raw_payload or ""),
                dedup_key=dedup_key,
                created_at=str(crawled_at),
            )
        )

    for t in targets:
        mark_crawl_job_target_status(con, int(t.id), "success")

    log.warning(
        "crawler_generation completed crawl_job_id=%s total=%s fallback_count=%s rewrite_count=%s",
        crawl_job_id,
        len(seeds),
        invalid_fallback_count,
        rewrite_count,
    )
    return candidates


def _merge_raw_payload_text(raw_payload: Any, extra: dict[str, Any]) -> str:
    base: dict[str, Any] = {}
    try:
        if isinstance(raw_payload, str) and raw_payload.strip().startswith("{"):
            base = json.loads(raw_payload)
        elif raw_payload:
            base = {"raw": str(raw_payload)}
    except Exception:
        base = {"raw": str(raw_payload or "")}
    base.update(extra or {})
    return json.dumps(base, ensure_ascii=False, default=str)


def _fallback_title(seed: dict[str, Any]) -> str:
    topic = str(seed.get("topic") or "experience")
    brand = str(seed.get("brand_name") or "").strip()
    if brand:
        return f"{brand} {topic} discussion"
    return f"{topic} discussion"


def _fallback_content(seed: dict[str, Any]) -> str:
    platform_code = str(seed.get("platform_code") or "")
    topic = str(seed.get("topic") or "related experience")
    brand = str(seed.get("brand_name") or "").strip() or "a brand"
    hints = [str(x) for x in (seed.get("keyword_hints") or []) if str(x).strip()]
    kw = " / ".join(hints) if hints else topic
    if str(platform_code).lower() in {"douyin", "dy"}:
        return f"{brand} {topic}: short note about {kw}."
    return f"Sharing a quick observation on {brand} and {topic}, mainly around {kw}."


def deduplicate_candidates(candidates: Iterable[PostCandidate]) -> list[PostCandidate]:
    seen: set[tuple[int, str]] = set()
    unique: list[PostCandidate] = []
    for c in candidates:
        key = (c.platform_id, c.dedup_key)
        if key in seen:
            continue
        seen.add(key)
        unique.append(c)
    return unique


def insert_posts(con: sqlite3.Connection, candidates: list[PostCandidate], *, return_ids: bool = False) -> list[int]:
    """
    Insert simulated crawl posts into `post_raw`.

    Performance note:
    - Manual refresh can generate many candidates (platform x brand x keyword x posts_per_target).
    - Avoid per-row `SELECT id ...` roundtrips; return ids via a single query when needed.
    """
    if not candidates:
        return []
    sql = """
    INSERT OR IGNORE INTO post_raw(
      project_id, crawl_job_id, platform_id, brand_id,
      external_post_id, author_name, title, content, post_url,
      publish_time, crawled_at,
      like_count, comment_count, share_count, view_count,
      raw_payload, dedup_key, created_at
    )
    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    con.executemany(
        sql,
        [
            (
                c.project_id,
                c.crawl_job_id,
                c.platform_id,
                c.brand_id,
                c.external_post_id,
                c.author_name,
                c.title,
                c.content,
                c.post_url,
                c.publish_time,
                c.crawled_at,
                c.like_count,
                c.comment_count,
                c.share_count,
                c.view_count,
                c.raw_payload,
                c.dedup_key,
                c.created_at,
            )
            for c in candidates
        ],
    )
    if not return_ids:
        return []
    crawl_job_ids = sorted({int(c.crawl_job_id) for c in candidates})
    if not crawl_job_ids:
        return []
    if len(crawl_job_ids) == 1:
        rows = con.execute(
            "SELECT id FROM post_raw WHERE crawl_job_id=? ORDER BY id;",
            (int(crawl_job_ids[0]),),
        ).fetchall()
    else:
        placeholders = ",".join(["?"] * len(crawl_job_ids))
        rows = con.execute(
            f"SELECT id FROM post_raw WHERE crawl_job_id IN ({placeholders}) ORDER BY id;",
            tuple(int(x) for x in crawl_job_ids),
        ).fetchall()
    return [int(r["id"]) for r in rows]


def deduplicate_posts(con: sqlite3.Connection, crawl_job_id: int) -> list[int]:
    rows = con.execute(
        """
        SELECT id, platform_id, dedup_key
        FROM post_raw
        WHERE crawl_job_id=?
        ORDER BY id;
        """,
        (crawl_job_id,),
    ).fetchall()
    seen: set[tuple[int, str]] = set()
    keep: list[int] = []
    for r in rows:
        key = (int(r["platform_id"]), str(r["dedup_key"]))
        if key in seen:
            continue
        seen.add(key)
        keep.append(int(r["id"]))
    return keep


def ensure_post_brand_relation(con: sqlite3.Connection, post_id: int, brand_id: int) -> None:
    con.execute(
        """
        INSERT INTO post_brand_relation(post_id, brand_id, relation_type)
        SELECT ?, ?, ?
        WHERE NOT EXISTS(
          SELECT 1 FROM post_brand_relation WHERE post_id=? AND brand_id=? AND relation_type=?
        );
        """,
        (post_id, brand_id, "target", post_id, brand_id, "target"),
    )


def insert_clean_result(con: sqlite3.Connection, post_id: int, result: CleanPostResult) -> None:
    exists = fetch_one_int(con, "SELECT 1 FROM post_clean_result WHERE post_id=? LIMIT 1;", (post_id,))
    if exists is not None:
        return
    ts = now_ts()
    con.execute(
        """
        INSERT INTO post_clean_result(post_id, is_valid, invalid_reason, clean_text, language, analyzed_at)
        VALUES(?, ?, ?, ?, ?, ?);
        """,
        (post_id, int(result.is_valid or 0), result.invalid_reason, result.clean_text, result.language, ts),
    )


def insert_sentiment_result(con: sqlite3.Connection, post_id: int, result: SentimentResult) -> None:
    exists = fetch_one_int(con, "SELECT 1 FROM post_sentiment_result WHERE post_id=? LIMIT 1;", (post_id,))
    if exists is not None:
        return
    ts = now_ts()
    con.execute(
        """
        INSERT INTO post_sentiment_result(
          post_id, sentiment, sentiment_score, emotion_intensity, model_version, analyzed_at
        )
        VALUES(?, ?, ?, ?, ?, ?);
        """,
        (
            post_id,
            result.sentiment,
            float(result.sentiment_score or 0.0),
            float(result.emotion_intensity or 0.0),
            result.model_version,
            ts,
        ),
    )


def insert_spam_result(con: sqlite3.Connection, post_id: int, result: SpamResult) -> None:
    exists = fetch_one_int(con, "SELECT 1 FROM post_spam_result WHERE post_id=? LIMIT 1;", (post_id,))
    if exists is not None:
        return
    ts = now_ts()
    con.execute(
        """
        INSERT INTO post_spam_result(post_id, spam_label, spam_score, analyzed_at)
        VALUES(?, ?, ?, ?);
        """,
        (post_id, result.spam_label, float(result.spam_score or 0.0), ts),
    )


def insert_keyword_results(
    con: sqlite3.Connection, post_id: int, hits: list[KeywordHit]
) -> None:
    ts = now_ts()
    for hit in hits:
        kw = str(hit.keyword)
        if not kw:
            continue
        kw_type = hit.keyword_type
        kw_type_norm = (str(kw_type).strip() if kw_type is not None else "")
        exists = fetch_one_int(
            con,
            "SELECT 1 FROM post_keyword_result WHERE post_id=? AND keyword=? AND COALESCE(keyword_type,'')=? LIMIT 1;",
            (post_id, kw, kw_type_norm),
        )
        if exists is not None:
            continue
        con.execute(
            """
            INSERT INTO post_keyword_result(post_id, keyword, keyword_type, confidence, analyzed_at)
            VALUES(?, ?, ?, ?, ?);
            """,
            (post_id, kw, hit.keyword_type, float(hit.confidence or 0.0), ts),
        )


def insert_feature_results(
    con: sqlite3.Connection, post_id: int, hits: list[FeatureHit]
) -> None:
    ts = now_ts()
    for hit in hits:
        f = str(hit.feature_name)
        if not f:
            continue
        exists = fetch_one_int(
            con,
            "SELECT 1 FROM post_feature_result WHERE post_id=? AND feature_name=? LIMIT 1;",
            (post_id, f),
        )
        if exists is not None:
            continue
        con.execute(
            """
            INSERT INTO post_feature_result(post_id, feature_name, feature_sentiment, confidence, analyzed_at)
            VALUES(?, ?, ?, ?, ?);
            """,
            (post_id, f, hit.feature_sentiment, float(hit.confidence or 0.0), ts),
        )


def run_analysis(con: sqlite3.Connection, project_id: int, post_ids: list[int]) -> None:
    # Local import to avoid widening pipeline_main module-level responsibilities.
    import logging
    import time

    log = logging.getLogger("prodwatch.pipeline")
    ensure_analysis_tables(con)
    keyword_rows = con.execute(
        """
        SELECT keyword, keyword_type, weight, is_enabled
        FROM project_keyword
        WHERE project_id=? AND is_enabled=1
        ORDER BY COALESCE(weight, 0) DESC, id;
        """,
        (project_id,),
    ).fetchall()
    project_keywords = [
        ProjectKeyword(
            keyword=str(r["keyword"] or ""),
            keyword_type=r["keyword_type"],
            weight=int(r["weight"]) if r["weight"] is not None else None,
            is_enabled=int(r["is_enabled"] or 0),
        )
        for r in keyword_rows
        if (r["keyword"] or "").strip() != ""
    ]
    rule = MockRuleAnalyzerService.for_project(project_keywords)
    monitor_keywords = [str(k.keyword) for k in project_keywords if (k.keyword or "").strip() != ""]

    # Best-effort: infer crawl_job_id from the posts being analyzed so we can log LLM schema to that job.
    crawl_job_id: int | None = None
    try:
        if post_ids:
            row = con.execute(
                """
                SELECT crawl_job_id
                FROM post_raw
                WHERE id=?
                LIMIT 1;
                """,
                (int(post_ids[0]),),
            ).fetchone()
            if row is not None and row["crawl_job_id"] is not None:
                crawl_job_id = int(row["crawl_job_id"])
    except Exception:
        crawl_job_id = None
    monitor_kw_type = {str(k.keyword): k.keyword_type for k in project_keywords if (k.keyword or "").strip() != ""}

    def _is_llm_access_or_billing_error(err: str) -> bool:
        """
        Detect non-retriable LLM provider errors (e.g. billing arrearage / access denied).
        When these happen, we should stop calling the provider for the rest of the job and
        fall back to deterministic/rule-based analysis so the refresh still produces visible data.
        """

        t = (err or "").lower()
        return ("arrearage" in t) or ("overdue-payment" in t) or ("access denied" in t)

    total_t0 = time.perf_counter()
    log.info(
        "analysis start crawl_job_id=%s project_id=%s post_count=%s",
        int(crawl_job_id) if crawl_job_id is not None else None,
        int(project_id),
        len(post_ids or []),
    )

    llm_disabled = False
    llm_error_count = 0
    post_error_count = 0

    for idx, post_id in enumerate(post_ids):
        post_t0 = time.perf_counter()
        log.info(
            "analysis post_start crawl_job_id=%s post_id=%s idx=%s/%s",
            int(crawl_job_id) if crawl_job_id is not None else None,
            int(post_id),
            idx + 1,
            len(post_ids or []),
        )
        row = None
        try:
            row = con.execute(
                "SELECT id, project_id, platform_id, brand_id, title, content, raw_payload FROM post_raw WHERE id=?;",
                (post_id,),
            ).fetchone()
        except Exception:
            post_error_count += 1
            log.exception(
                "analysis post_fetch failed (ignored) crawl_job_id=%s post_id=%s",
                int(crawl_job_id) if crawl_job_id is not None else None,
                int(post_id),
            )
            continue
        if not row:
            continue

        try:
            brand_id = row["brand_id"]
            if brand_id is not None:
                ensure_post_brand_relation(con, int(row["id"]), int(brand_id))
        except Exception:
            # Best-effort only; do not break analysis chain.
            pass

        post = PostInput(
            post_id=int(row["id"]),
            project_id=int(row["project_id"]),
            platform_id=int(row["platform_id"]) if row["platform_id"] is not None else None,
            brand_id=int(row["brand_id"]) if row["brand_id"] is not None else None,
            title=str(row["title"] or ""),
            content=str(row["content"] or ""),
        )

        try:
            # Clean (best-effort)
            try:
                clean = rule.clean_post(post)
            except Exception:
                clean = CleanPostResult(
                    clean_text=post.text,
                    is_valid=1 if post.text else 0,
                    invalid_reason=None,
                    language="zh",
                )
            try:
                insert_clean_result(con, post_id, clean)
            except Exception:
                # Analysis is enrichment; do not abort the whole refresh because one row can't be written.
                log.exception(
                    "analysis clean_persist failed crawl_job_id=%s post_id=%s",
                    int(crawl_job_id) if crawl_job_id is not None else None,
                    int(post_id),
                )

            # --------------------------
            # Stage1: keyword_extraction
            # --------------------------
            step_t0 = time.perf_counter()
            log.info(
                "analysis step_start crawl_job_id=%s post_id=%s step=%s",
                int(crawl_job_id) if crawl_job_id is not None else None,
                int(post_id),
                "keyword_extraction",
            )
            kw_items: list[dict[str, Any]] = []
            kw_source = "rule"
            if not llm_disabled:
                try:
                    # LLM semantic match against configured monitoring words.
                    kw_items = keyword_extraction_llm(
                        text=post.text,
                        project_keywords=monitor_keywords,
                        con=con,
                        crawl_job_id=crawl_job_id,
                    )
                    kw_source = "llm"
                except Exception as e:
                    llm_error_count += 1
                    err = f"{type(e).__name__}: {e}"
                    if _is_llm_access_or_billing_error(err):
                        llm_disabled = True
                    log.warning(
                        "analysis keyword_extraction_llm failed (fallback to rule) crawl_job_id=%s post_id=%s llm_disabled=%s err=%s",
                        int(crawl_job_id) if crawl_job_id is not None else None,
                        int(post_id),
                        bool(llm_disabled),
                        err,
                    )
            if not kw_items:
                # Deterministic fallback: simple string matching against monitoring words.
                try:
                    kr = keyword_hit(post_text=post.text, project_keywords=monitor_keywords)
                    kw_items = [
                        {
                            "keyword": str(it.get("keyword") or "").strip(),
                            "confidence": 0.65,
                            "evidence": str(it.get("matched_text") or "").strip(),
                            "keyword_type": monitor_kw_type.get(str(it.get("keyword") or "").strip()),
                        }
                        for it in (kr.hits or [])
                        if isinstance(it, dict) and str(it.get("keyword") or "").strip() != ""
                    ]
                    kw_source = "rule"
                except Exception:
                    kw_items = []
            log.info(
                "analysis step_done crawl_job_id=%s post_id=%s step=%s dt_s=%.3f hit_count=%s source=%s llm_disabled=%s",
                int(crawl_job_id) if crawl_job_id is not None else None,
                int(post_id),
                "keyword_extraction",
                time.perf_counter() - step_t0,
                len(kw_items or []),
                str(kw_source),
                bool(llm_disabled),
            )

            step_t0 = time.perf_counter()
            log.info(
                "analysis step_start crawl_job_id=%s post_id=%s step=%s",
                int(crawl_job_id) if crawl_job_id is not None else None,
                int(post_id),
                "keyword_persist",
            )
            try:
                if kw_items:
                    insert_keyword_hits(
                        con,
                        post_id=int(post_id),
                        hits=[
                            {
                                "keyword": str(it.get("keyword") or "").strip(),
                                "matched_text": str(it.get("evidence") or "").strip()
                                or str(it.get("keyword") or "").strip(),
                            }
                            for it in (kw_items or [])
                            if str(it.get("keyword") or "").strip() != ""
                        ],
                    )
                    monitor_hits = [
                        KeywordHit(
                            keyword=str(it.get("keyword") or "").strip(),
                            keyword_type=monitor_kw_type.get(str(it.get("keyword") or "").strip()),
                            confidence=float(it.get("confidence") or 0.7),
                            source=str(kw_source),
                        )
                        for it in (kw_items or [])
                        if str(it.get("keyword") or "").strip() != ""
                    ]
                    if monitor_hits:
                        insert_keyword_results(con, post_id, monitor_hits)
            except Exception:
                log.exception(
                    "analysis keyword_persist failed crawl_job_id=%s post_id=%s",
                    int(crawl_job_id) if crawl_job_id is not None else None,
                    int(post_id),
                )
            log.info(
                "analysis step_done crawl_job_id=%s post_id=%s step=%s dt_s=%.3f",
                int(crawl_job_id) if crawl_job_id is not None else None,
                int(post_id),
                "keyword_persist",
                time.perf_counter() - step_t0,
            )

            # --------------------------
            # Stage2: post_analysis
            # --------------------------
            step_t0 = time.perf_counter()
            log.info(
                "analysis step_start crawl_job_id=%s post_id=%s step=%s",
                int(crawl_job_id) if crawl_job_id is not None else None,
                int(post_id),
                "post_analysis",
            )
            pa_source = "fallback"
            if llm_disabled:
                pa = PostAnalysisResult(
                    entities=[],
                    features=[],
                    issues=[],
                    scenarios=[],
                    sentiment_targets=[],
                    raw_keywords=[],
                    topics=[],
                    sentiment="neutral",
                    sentiment_score=0.0,
                    emotion_intensity=0.0,
                    spam_label="normal",
                    spam_score=0.1,
                    meta={"provider": "disabled"},
                )
                pa_source = "disabled"
            else:
                try:
                    pa = post_analysis_llm(text=post.text, con=con, crawl_job_id=crawl_job_id)
                    pa_source = "llm"
                except Exception as e:
                    llm_error_count += 1
                    err = f"{type(e).__name__}: {e}"
                    if _is_llm_access_or_billing_error(err):
                        llm_disabled = True
                    log.warning(
                        "analysis post_analysis_llm failed (fallback to empty) crawl_job_id=%s post_id=%s llm_disabled=%s err=%s",
                        int(crawl_job_id) if crawl_job_id is not None else None,
                        int(post_id),
                        bool(llm_disabled),
                        err,
                    )
                    pa = PostAnalysisResult(
                        entities=[],
                        features=[],
                        issues=[],
                        scenarios=[],
                        sentiment_targets=[],
                        raw_keywords=[],
                        topics=[],
                        sentiment="neutral",
                        sentiment_score=0.0,
                        emotion_intensity=0.0,
                        spam_label="normal",
                        spam_score=0.1,
                        meta={"provider": "fallback", "error": err},
                    )
                    pa_source = "fallback"
            log.info(
                "analysis step_done crawl_job_id=%s post_id=%s step=%s dt_s=%.3f topics=%s features=%s issues=%s entities=%s source=%s llm_disabled=%s",
                int(crawl_job_id) if crawl_job_id is not None else None,
                int(post_id),
                "post_analysis",
                time.perf_counter() - step_t0,
                len(pa.topics or []),
                len(pa.features or []),
                len(pa.issues or []),
                len(pa.entities or []),
                str(pa_source),
                bool(llm_disabled),
            )

            # Persist structured extraction + topics (best-effort; should not block refresh).
            step_t0 = time.perf_counter()
            log.info(
                "analysis step_start crawl_job_id=%s post_id=%s step=%s",
                int(crawl_job_id) if crawl_job_id is not None else None,
                int(post_id),
                "post_analysis_persist",
            )
            try:
                upsert_analysis_result(
                    con,
                    post_id=int(post_id),
                    entities=pa.entities,
                    features=pa.features,
                    issues=pa.issues,
                )
                insert_topic_results(
                    con,
                    post_id=int(post_id),
                    topics=(pa.topics or []),
                    confidence=0.7,
                    source=str(pa_source),
                )

                mv = str((pa.meta or {}).get("model") or (pa.meta or {}).get("provider") or "llm").strip() or "llm"
                sentiment_val = str(pa.sentiment or "neutral").strip().lower()
                if sentiment_val not in {"positive", "neutral", "negative"}:
                    sentiment_val = "neutral"
                score = float(pa.sentiment_score or 0.0)
                score = max(-1.0, min(1.0, score))
                intensity = float(pa.emotion_intensity if pa.emotion_intensity is not None else abs(score))
                intensity = max(0.0, min(1.0, intensity))
                insert_sentiment_result(
                    con,
                    post_id,
                    SentimentResult(
                        sentiment=sentiment_val,
                        sentiment_score=score,
                        emotion_intensity=intensity,
                        model_version=mv,
                    ),
                )

                spam_label = str(pa.spam_label or "normal").strip().lower()
                if spam_label not in {"spam", "normal"}:
                    spam_label = "normal"
                spam_score = float(pa.spam_score if pa.spam_score is not None else 0.1)
                spam_score = max(0.0, min(1.0, spam_score))
                insert_spam_result(
                    con,
                    post_id,
                    SpamResult(spam_label=spam_label, spam_score=spam_score, model_version=mv),
                )
            except Exception:
                log.exception(
                    "analysis post_analysis_persist failed crawl_job_id=%s post_id=%s",
                    int(crawl_job_id) if crawl_job_id is not None else None,
                    int(post_id),
                )

            # Features: write into legacy table (best-effort)
            try:
                feat_hits: list[FeatureHit] = []
                for it in pa.features or []:
                    if not isinstance(it, dict):
                        continue
                    name = str(it.get("normalized") or it.get("text") or "").strip()
                    if not name:
                        continue
                    s = str(it.get("sentiment") or "neutral").strip().lower()
                    if s not in {"positive", "neutral", "negative"}:
                        s = "neutral"
                    conf = float(it.get("confidence") or 0.7)
                    conf = max(0.0, min(1.0, conf))
                    feat_hits.append(
                        FeatureHit(feature_name=name, feature_sentiment=s, confidence=conf, source="llm")
                    )
                    if len(feat_hits) >= 80:
                        break
                if feat_hits:
                    insert_feature_results(con, post_id, feat_hits)
            except Exception:
                pass

            # Persist rich extraction to raw_payload (no schema change).
            try:
                extra = {
                    "analysis_v2": {
                        "keyword_hits": kw_items,
                        "post_analysis": pa.to_dict(),
                    }
                }
                merged = merge_raw_payload(row["raw_payload"], extra)
                con.execute("UPDATE post_raw SET raw_payload=? WHERE id=?;", (merged, int(post_id)))
            except Exception:
                pass

            log.info(
                "analysis step_done crawl_job_id=%s post_id=%s step=%s dt_s=%.3f",
                int(crawl_job_id) if crawl_job_id is not None else None,
                int(post_id),
                "post_analysis_persist",
                time.perf_counter() - step_t0,
            )

            log.info(
                "analysis post_done crawl_job_id=%s post_id=%s dt_s=%.3f",
                int(crawl_job_id) if crawl_job_id is not None else None,
                int(post_id),
                time.perf_counter() - post_t0,
            )
        except Exception:
            post_error_count += 1
            log.exception(
                "analysis post_failed (ignored) crawl_job_id=%s post_id=%s",
                int(crawl_job_id) if crawl_job_id is not None else None,
                int(post_id),
            )
            continue

    log.info(
        "analysis done crawl_job_id=%s project_id=%s total_dt_s=%.3f llm_disabled=%s llm_error_count=%s post_error_count=%s",
        int(crawl_job_id) if crawl_job_id is not None else None,
        int(project_id),
        time.perf_counter() - total_t0,
        bool(llm_disabled),
        int(llm_error_count),
        int(post_error_count),
    )


def upsert_daily_keyword_metric(
    con: sqlite3.Connection,
    project_id: int,
    brand_id: Optional[int],
    platform_id: Optional[int],
    stat_date: str,
    keyword: str,
    hit_count: int,
) -> None:
    ts = now_ts()
    cur = con.execute(
        """
        UPDATE daily_keyword_metric
        SET hit_count=?, created_at=?
        WHERE project_id=? AND brand_id IS ? AND platform_id IS ? AND stat_date=? AND keyword=?;
        """,
        (hit_count, ts, project_id, brand_id, platform_id, stat_date, keyword),
    )
    if cur.rowcount and cur.rowcount > 0:
        return
    con.execute(
        """
        INSERT INTO daily_keyword_metric(project_id, brand_id, platform_id, stat_date, keyword, hit_count, created_at)
        VALUES(?, ?, ?, ?, ?, ?, ?);
        """,
        (project_id, brand_id, platform_id, stat_date, keyword, hit_count, ts),
    )


def upsert_daily_feature_metric(
    con: sqlite3.Connection,
    project_id: int,
    brand_id: Optional[int],
    stat_date: str,
    feature_name: str,
    mention_count: int,
    positive_count: int,
    neutral_count: int,
    negative_count: int,
) -> None:
    cur = con.execute(
        """
        UPDATE daily_feature_metric
        SET mention_count=?, positive_count=?, neutral_count=?, negative_count=?
        WHERE project_id=? AND brand_id IS ? AND stat_date=? AND feature_name=?;
        """,
        (
            mention_count,
            positive_count,
            neutral_count,
            negative_count,
            project_id,
            brand_id,
            stat_date,
            feature_name,
        ),
    )
    if cur.rowcount and cur.rowcount > 0:
        return
    con.execute(
        """
        INSERT INTO daily_feature_metric(
          project_id, brand_id, stat_date, feature_name,
          mention_count, positive_count, neutral_count, negative_count
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (
            project_id,
            brand_id,
            stat_date,
            feature_name,
            mention_count,
            positive_count,
            neutral_count,
            negative_count,
        ),
    )


def upsert_daily_topic_metric(
    con: sqlite3.Connection,
    project_id: int,
    brand_id: Optional[int],
    platform_id: Optional[int],
    stat_date: str,
    topic: str,
    hit_count: int,
) -> None:
    ts = now_ts()
    cur = con.execute(
        """
        UPDATE daily_topic_metric
        SET hit_count=?, created_at=?
        WHERE project_id=? AND brand_id IS ? AND platform_id IS ? AND stat_date=? AND topic=?;
        """,
        (int(hit_count), ts, int(project_id), brand_id, platform_id, str(stat_date), str(topic)),
    )
    if cur.rowcount and cur.rowcount > 0:
        return
    con.execute(
        """
        INSERT INTO daily_topic_metric(project_id, brand_id, platform_id, stat_date, topic, hit_count, created_at)
        VALUES(?, ?, ?, ?, ?, ?, ?);
        """,
        (int(project_id), brand_id, platform_id, str(stat_date), str(topic), int(hit_count), ts),
    )


def aggregate_daily_metrics(con: sqlite3.Connection, project_id: int, stat_date: str) -> None:
    ts = now_ts()
    try:
        ensure_analysis_tables(con)
    except Exception:
        pass

    rows = con.execute(
        """
        WITH base AS (
          SELECT
            pr.id AS post_id,
            pr.project_id,
            pr.brand_id,
            pr.platform_id,
            date(COALESCE(pr.publish_time, pr.crawled_at)) AS stat_date,
            pr.like_count,
            pr.comment_count,
            pr.share_count
          FROM post_raw pr
          WHERE pr.project_id=?
            AND date(COALESCE(pr.publish_time, pr.crawled_at)) = ?
            AND pr.brand_id IS NOT NULL
            AND pr.platform_id IS NOT NULL
        ),
        clean AS (
          SELECT post_id, COALESCE(is_valid, 0) AS is_valid FROM post_clean_result
        ),
        spam AS (
          SELECT post_id, spam_label FROM post_spam_result
        ),
        senti AS (
          SELECT post_id, sentiment, sentiment_score FROM post_sentiment_result
        ),
        kw AS (
          SELECT post_id, COUNT(*) AS kw_hits
          FROM keyword_hit
          GROUP BY post_id
        )
        SELECT
          b.project_id,
          b.brand_id,
          b.platform_id,
          b.stat_date,
          COUNT(*) AS total_post_count,
          SUM(CASE WHEN c.is_valid=1 THEN 1 ELSE 0 END) AS valid_post_count,
          SUM(CASE WHEN s.spam_label='spam' THEN 1 ELSE 0 END) AS spam_post_count,
          SUM(CASE WHEN se.sentiment='positive' THEN 1 ELSE 0 END) AS positive_count,
          SUM(CASE WHEN se.sentiment='neutral' THEN 1 ELSE 0 END) AS neutral_count,
          SUM(CASE WHEN se.sentiment='negative' THEN 1 ELSE 0 END) AS negative_count,
          AVG(COALESCE(se.sentiment_score, 0.0)) AS avg_sentiment_score,
          SUM(COALESCE(b.like_count, 0)) AS total_like_count,
          SUM(COALESCE(b.comment_count, 0)) AS total_comment_count,
          SUM(COALESCE(b.share_count, 0)) AS total_share_count,
          SUM(COALESCE(k.kw_hits, 0)) AS keyword_count
        FROM base b
        LEFT JOIN clean c ON c.post_id=b.post_id
        LEFT JOIN spam s ON s.post_id=b.post_id
        LEFT JOIN senti se ON se.post_id=b.post_id
        LEFT JOIN kw k ON k.post_id=b.post_id
        GROUP BY b.project_id, b.brand_id, b.platform_id, b.stat_date
        ORDER BY b.brand_id, b.platform_id;
        """,
        (project_id, stat_date),
    ).fetchall()

    for r in rows:
        con.execute(
            """
            INSERT INTO daily_metric(
              project_id, brand_id, platform_id, stat_date,
              total_post_count, valid_post_count, spam_post_count,
              positive_count, neutral_count, negative_count, avg_sentiment_score,
              total_like_count, total_comment_count, total_share_count,
              keyword_count, created_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(project_id, brand_id, platform_id, stat_date)
            DO UPDATE SET
              total_post_count=excluded.total_post_count,
              valid_post_count=excluded.valid_post_count,
              spam_post_count=excluded.spam_post_count,
              positive_count=excluded.positive_count,
              neutral_count=excluded.neutral_count,
              negative_count=excluded.negative_count,
              avg_sentiment_score=excluded.avg_sentiment_score,
              total_like_count=excluded.total_like_count,
              total_comment_count=excluded.total_comment_count,
              total_share_count=excluded.total_share_count,
              keyword_count=excluded.keyword_count,
              created_at=excluded.created_at;
            """,
            (
                int(r["project_id"]),
                int(r["brand_id"]),
                int(r["platform_id"]),
                str(r["stat_date"]),
                int(r["total_post_count"] or 0),
                int(r["valid_post_count"] or 0),
                int(r["spam_post_count"] or 0),
                int(r["positive_count"] or 0),
                int(r["neutral_count"] or 0),
                int(r["negative_count"] or 0),
                float(r["avg_sentiment_score"] or 0.0),
                int(r["total_like_count"] or 0),
                int(r["total_comment_count"] or 0),
                int(r["total_share_count"] or 0),
                int(r["keyword_count"] or 0),
                ts,
            ),
        )

    kw_rows = con.execute(
        """
        SELECT
          pr.project_id,
          pr.brand_id,
          pr.platform_id,
          date(COALESCE(pr.publish_time, pr.crawled_at)) AS stat_date,
          kh.keyword AS keyword,
          COUNT(*) AS hit_count
        FROM keyword_hit kh
        JOIN post_raw pr ON pr.id = kh.post_id
        WHERE pr.project_id=?
          AND date(COALESCE(pr.publish_time, pr.crawled_at)) = ?
          AND pr.platform_id IS NOT NULL
        GROUP BY pr.project_id, pr.brand_id, pr.platform_id, stat_date, kh.keyword
        ORDER BY pr.brand_id, pr.platform_id, kh.keyword;
        """,
        (project_id, stat_date),
    ).fetchall()
    for r in kw_rows:
        upsert_daily_keyword_metric(
            con,
            int(r["project_id"]),
            int(r["brand_id"]) if r["brand_id"] is not None else None,
            int(r["platform_id"]) if r["platform_id"] is not None else None,
            str(r["stat_date"]),
            str(r["keyword"]),
            int(r["hit_count"] or 0),
        )

    topic_rows = con.execute(
        """
        SELECT
          pr.project_id,
          pr.brand_id,
          pr.platform_id,
          date(COALESCE(pr.publish_time, pr.crawled_at)) AS stat_date,
          tr.topic AS topic,
          COUNT(*) AS hit_count
        FROM topic_result tr
        JOIN post_raw pr ON pr.id = tr.post_id
        WHERE pr.project_id=?
          AND date(COALESCE(pr.publish_time, pr.crawled_at)) = ?
          AND pr.platform_id IS NOT NULL
        GROUP BY pr.project_id, pr.brand_id, pr.platform_id, stat_date, tr.topic
        ORDER BY pr.brand_id, pr.platform_id, tr.topic;
        """,
        (project_id, stat_date),
    ).fetchall()
    for r in topic_rows:
        upsert_daily_topic_metric(
            con,
            int(r["project_id"]),
            int(r["brand_id"]) if r["brand_id"] is not None else None,
            int(r["platform_id"]) if r["platform_id"] is not None else None,
            str(r["stat_date"]),
            str(r["topic"]),
            int(r["hit_count"] or 0),
        )

    feat_rows = con.execute(
        """
        SELECT
          pr.project_id,
          pr.brand_id,
          date(COALESCE(pr.publish_time, pr.crawled_at)) AS stat_date,
          pfr.feature_name,
          COUNT(*) AS mention_count,
          SUM(CASE WHEN pfr.feature_sentiment='positive' THEN 1 ELSE 0 END) AS positive_count,
          SUM(CASE WHEN pfr.feature_sentiment='neutral' THEN 1 ELSE 0 END) AS neutral_count,
          SUM(CASE WHEN pfr.feature_sentiment='negative' THEN 1 ELSE 0 END) AS negative_count
        FROM post_feature_result pfr
        JOIN post_raw pr ON pr.id = pfr.post_id
        WHERE pr.project_id=?
          AND date(COALESCE(pr.publish_time, pr.crawled_at)) = ?
          AND pr.brand_id IS NOT NULL
        GROUP BY pr.project_id, pr.brand_id, stat_date, pfr.feature_name
        ORDER BY pr.brand_id, pfr.feature_name;
        """,
        (project_id, stat_date),
    ).fetchall()
    for r in feat_rows:
        upsert_daily_feature_metric(
            con,
            int(r["project_id"]),
            int(r["brand_id"]) if r["brand_id"] is not None else None,
            str(r["stat_date"]),
            str(r["feature_name"]),
            int(r["mention_count"] or 0),
            int(r["positive_count"] or 0),
            int(r["neutral_count"] or 0),
            int(r["negative_count"] or 0),
        )


def finalize_job_success(con: sqlite3.Connection, crawl_job_id: int, project_id: int) -> None:
    ts = now_ts()
    finalize_job_success_basic(con, int(crawl_job_id), ended_at=ts)
    con.execute("UPDATE project SET last_refresh_at=?, updated_at=? WHERE id=?;", (ts, ts, int(project_id)))


def finalize_job_success_basic(con: sqlite3.Connection, crawl_job_id: int, *, ended_at: Optional[str] = None) -> None:
    """
    Mark a crawl_job as success without touching the project row.

    Why:
    - We split the pipeline into independent chains (simulate vs analyze).
    - Only "analyze/aggregate" should update project.last_refresh_at; "simulate-only" should not.
    """
    ts = str(ended_at or now_ts())
    try:
        con.execute(
            "UPDATE crawl_job SET status=?, ended_at=?, finished_at=?, error_message=? WHERE id=?;",
            ("success", ts, ts, None, int(crawl_job_id)),
        )
    except sqlite3.OperationalError as e:
        msg = str(e).lower()
        if ("no such column" not in msg) and ("has no column named" not in msg):
            raise
        con.execute(
            "UPDATE crawl_job SET status=?, ended_at=?, error_message=? WHERE id=?;",
            ("success", ts, None, int(crawl_job_id)),
        )


def finalize_job_failed(con: sqlite3.Connection, crawl_job_id: int, error_message: str) -> None:
    ts = now_ts()
    msg = error_message[:500] if error_message else ""
    try:
        con.execute(
            "UPDATE crawl_job SET status=?, ended_at=?, finished_at=?, error_message=? WHERE id=?;",
            ("failed", ts, ts, msg, crawl_job_id),
        )
    except sqlite3.OperationalError as e:
        msg2 = str(e).lower()
        if ("no such column" not in msg2) and ("has no column named" not in msg2):
            raise
        con.execute(
            "UPDATE crawl_job SET status=?, ended_at=?, error_message=? WHERE id=?;",
            ("failed", ts, msg, crawl_job_id),
        )


def mark_job_running(con: sqlite3.Connection, crawl_job_id: int) -> None:
    ts = now_ts()
    con.execute(
        "UPDATE crawl_job SET status=?, started_at=?, error_message=? WHERE id=?;",
        ("running", ts, None, crawl_job_id),
    )


def run_pipeline(
    con: sqlite3.Connection,
    project_id: int,
    stat_date: str,
    posts_per_target: int,
) -> int:
    return run_pipeline_with_trigger(
        con=con,
        project_id=project_id,
        stat_date=stat_date,
        posts_per_target=posts_per_target,
        job_type="manual",
        trigger_source="cli",
        schedule_type="manual",
        schedule_expr=None,
        created_by="system",
    )


def run_pipeline_with_trigger(
    *,
    con: sqlite3.Connection,
    project_id: int,
    stat_date: str,
    posts_per_target: int,
    job_type: str,
    trigger_source: str,
    schedule_type: str,
    schedule_expr: Optional[str],
    created_by: str,
    crawl_source: Optional[str] = None,
) -> int:
    ensure_project_exists(con, project_id)
    platform_ids, brand_ids, keywords = load_project_scope(con, project_id)
    if not platform_ids:
        raise RuntimeError("project_platform is empty for this project")
    if not brand_ids:
        raise RuntimeError("project_brand is empty for this project")
    # Keywords are optional:
    # - Stage1 keyword_extraction uses project_keyword as monitoring words (semantic match via LLM)
    # - Stage2 post_analysis is open extraction (does not rely on project keywords)
    # For crawling simulation (targets), use a stable placeholder when none configured.
    if not keywords:
        keywords = ["__all__"]

    crawl_job_id = create_crawl_job(
        con,
        project_id,
        job_type=job_type,
        trigger_source=trigger_source,
        schedule_type=schedule_type,
        schedule_expr=schedule_expr,
        created_by=created_by,
    )
    try:
        mark_job_running(con, crawl_job_id)
        run_pipeline_existing_job(
            con=con,
            crawl_job_id=int(crawl_job_id),
            project_id=int(project_id),
            stat_date=str(stat_date),
            posts_per_target=int(posts_per_target),
            crawl_source=crawl_source,
        )
        return int(crawl_job_id)
    except Exception as e:
        # run_pipeline_existing_job already finalized the job as failed.
        raise


def run_pipeline_existing_job(
    *,
    con: sqlite3.Connection,
    crawl_job_id: int,
    project_id: int,
    stat_date: str,
    posts_per_target: int,
    crawl_source: Optional[str] = None,
) -> None:
    ensure_project_exists(con, int(project_id))
    platform_ids, brand_ids, keywords = load_project_scope(con, int(project_id))
    if not platform_ids:
        raise RuntimeError("project_platform is empty for this project")
    if not brand_ids:
        raise RuntimeError("project_brand is empty for this project")
    if not keywords:
        keywords = ["__all__"]

    crawl_source_norm = normalize_crawl_source(crawl_source) if crawl_source else default_crawl_source()

    try:
        import logging
        import time

        log = logging.getLogger("prodwatch.pipeline")
        pipeline_t0 = time.perf_counter()

        def _progress(stage: str, message: str, meta: Optional[dict[str, Any]] = None) -> None:
            """
            Best-effort single-row progress upsert for frontend polling.
            """
            try:
                upsert_crawl_job_progress(
                    con,
                    crawl_job_id=int(crawl_job_id),
                    stage=str(stage),
                    message=str(message),
                    meta=meta,
                )
                con.commit()
            except Exception:
                pass

        log.info(
            "pipeline start crawl_job_id=%s project_id=%s stat_date=%s posts_per_target=%s crawl_source=%s",
            int(crawl_job_id),
            int(project_id),
            str(stat_date),
            int(posts_per_target),
            str(crawl_source_norm),
        )

        # Write initial progress so frontend can show the simulate stage immediately.
        _progress(
            "simulate",
            "simulate started",
            {
                "project_id": int(project_id),
                "stat_date": str(stat_date),
                "posts_per_target": int(posts_per_target),
                "crawl_source": str(crawl_source_norm),
            },
        )

        stage = "mark_job_running"
        st0 = time.perf_counter()
        log.info("pipeline stage_start crawl_job_id=%s stage=%s", int(crawl_job_id), stage)
        mark_job_running(con, int(crawl_job_id))
        try:
            con.commit()
        except Exception:
            pass
        log.info(
            "pipeline stage_done crawl_job_id=%s stage=%s dt_s=%.3f",
            int(crawl_job_id),
            stage,
            time.perf_counter() - st0,
        )

        stage = "generate_crawl_job_targets"
        st0 = time.perf_counter()
        log.info("pipeline stage_start crawl_job_id=%s stage=%s", int(crawl_job_id), stage)
        _progress("simulate", f"{stage} started", {"stage": str(stage)})
        targets = generate_crawl_job_targets(con, int(crawl_job_id), platform_ids, brand_ids, keywords)
        _progress("simulate", f"{stage} done", {"stage": str(stage), "target_count": int(len(targets or []))})
        log.info(
            "pipeline stage_done crawl_job_id=%s stage=%s dt_s=%.3f target_count=%s",
            int(crawl_job_id),
            stage,
            time.perf_counter() - st0,
            len(targets or []),
        )

        stage = "build_post_candidates"
        st0 = time.perf_counter()
        log.info("pipeline stage_start crawl_job_id=%s stage=%s", int(crawl_job_id), stage)
        _progress("simulate", f"{stage} started", {"stage": str(stage), "crawl_source": str(crawl_source_norm)})
        candidates = build_post_candidates(
            con,
            int(project_id),
            int(crawl_job_id),
            targets,
            str(stat_date),
            int(posts_per_target),
            crawl_source=str(crawl_source_norm),
        )
        _progress("simulate", f"{stage} done", {"stage": str(stage), "candidate_count": int(len(candidates or []))})
        log.info(
            "pipeline stage_done crawl_job_id=%s stage=%s dt_s=%.3f candidate_count=%s",
            int(crawl_job_id),
            stage,
            time.perf_counter() - st0,
            len(candidates or []),
        )

        stage = "deduplicate_candidates"
        st0 = time.perf_counter()
        log.info("pipeline stage_start crawl_job_id=%s stage=%s", int(crawl_job_id), stage)
        _progress("simulate", f"{stage} started", {"stage": str(stage), "candidate_count": int(len(candidates or []))})
        candidates = deduplicate_candidates(candidates)
        _progress("simulate", f"{stage} done", {"stage": str(stage), "candidate_count": int(len(candidates or []))})
        log.info(
            "pipeline stage_done crawl_job_id=%s stage=%s dt_s=%.3f candidate_count=%s",
            int(crawl_job_id),
            stage,
            time.perf_counter() - st0,
            len(candidates or []),
        )

        stage = "insert_posts"
        st0 = time.perf_counter()
        log.info("pipeline stage_start crawl_job_id=%s stage=%s", int(crawl_job_id), stage)
        _progress("simulate", f"{stage} started", {"stage": str(stage), "candidate_count": int(len(candidates or []))})
        insert_posts(con, candidates)
        # 鍐欏叆杩涘害渚涘墠绔痩og
        post_raw_cnt = None
        try:
            row = con.execute("SELECT count(1) c FROM post_raw WHERE crawl_job_id=?;", (int(crawl_job_id),)).fetchone()
            if row is not None:
                post_raw_cnt = int(row["c"])
        except Exception:
            post_raw_cnt = None
        log.info(
            "pipeline stage_done crawl_job_id=%s stage=%s dt_s=%.3f inserted_candidate_count=%s post_raw_count=%s",
            int(crawl_job_id),
            stage,
            time.perf_counter() - st0,
            len(candidates or []),
            post_raw_cnt,
        )
        _progress(
            "simulate",
            f"{stage} done",
            {
                "stage": str(stage),
                "inserted_candidate_count": int(len(candidates or [])),
                "post_raw_count": post_raw_cnt,
            },
        )

        stage = "deduplicate_posts"
        st0 = time.perf_counter()
        log.info("pipeline stage_start crawl_job_id=%s stage=%s", int(crawl_job_id), stage)
        _progress("simulate", f"{stage} started", {"stage": str(stage)})
        canonical_post_ids = deduplicate_posts(con, int(crawl_job_id))
        _progress(
            "simulate",
            f"{stage} done",
            {"stage": str(stage), "canonical_post_count": int(len(canonical_post_ids or []))},
        )
        log.info(
            "pipeline stage_done crawl_job_id=%s stage=%s dt_s=%.3f canonical_post_count=%s",
            int(crawl_job_id),
            stage,
            time.perf_counter() - st0,
            len(canonical_post_ids or []),
        )

        # simulate -> analyze. 鍐欏叆杩涘害渚涘墠绔痩og
        _progress(
            "analyze",
            "analysis started",
            {"canonical_post_count": int(len(canonical_post_ids or [])), "crawl_source": str(crawl_source_norm)},
        )

        stage = "run_analysis"
        st0 = time.perf_counter()
        log.info("pipeline stage_start crawl_job_id=%s stage=%s", int(crawl_job_id), stage)
        run_analysis(con, int(project_id), canonical_post_ids)
        log.info(
            "pipeline stage_done crawl_job_id=%s stage=%s dt_s=%.3f post_count=%s",
            int(crawl_job_id),
            stage,
            time.perf_counter() - st0,
            len(canonical_post_ids or []),
        )

        # analyze -> aggregate.鍐欏叆杩涘害渚涘墠绔痩og
        _progress(
            "aggregate",
            "aggregate started",
            {"canonical_post_count": int(len(canonical_post_ids or [])), "stat_date": str(stat_date)},
        )

        stage = "aggregate_daily_metrics"
        st0 = time.perf_counter()
        log.info("pipeline stage_start crawl_job_id=%s stage=%s", int(crawl_job_id), stage)
        aggregate_daily_metrics(con, int(project_id), str(stat_date))
        log.info(
            "pipeline stage_done crawl_job_id=%s stage=%s dt_s=%.3f",
            int(crawl_job_id),
            stage,
            time.perf_counter() - st0,
        )

        stage = "finalize_job_success"
        st0 = time.perf_counter()
        log.info("pipeline stage_start crawl_job_id=%s stage=%s", int(crawl_job_id), stage)
        finalize_job_success(con, int(crawl_job_id), int(project_id))
        _progress("done", "done", {"stat_date": str(stat_date)})
        log.info(
            "pipeline stage_done crawl_job_id=%s stage=%s dt_s=%.3f",
            int(crawl_job_id),
            stage,
            time.perf_counter() - st0,
        )

        log.info(
            "pipeline done crawl_job_id=%s total_dt_s=%.3f",
            int(crawl_job_id),
            time.perf_counter() - pipeline_t0,
        )
    except Exception as e:
        # Ensure any pipeline exception is persisted on crawl_job/progress.
        try:
            con.rollback()
        except Exception:
            pass
        try:
            mark_all_targets_failed(con, int(crawl_job_id))
        except Exception:
            pass
        try:
            upsert_crawl_job_progress(
                con,
                crawl_job_id=int(crawl_job_id),
                stage="failed",
                message=str(e),
                meta=None,
            )
        except Exception:
            pass
        finalize_job_failed(con, int(crawl_job_id), str(e))
        try:
            con.commit()
        except Exception:
            pass
        raise


def run_simulate_existing_job(
    *,
    con: sqlite3.Connection,
    crawl_job_id: int,
    project_id: int,
    stat_date: str,
    posts_per_target: int,
    crawl_source: Optional[str] = None,
) -> None:
    """
    Simulate/generate posts only (no analysis, no aggregation).

    Pipeline stages:
    - generate_crawl_job_targets
    - build_post_candidates
    - deduplicate_candidates
    - insert_posts
    - deduplicate_posts
    - finalize_job_success_basic
    """
    ensure_project_exists(con, int(project_id))
    platform_ids, brand_ids, keywords = load_project_scope(con, int(project_id))
    if not platform_ids:
        raise RuntimeError("project_platform is empty for this project")
    if not brand_ids:
        raise RuntimeError("project_brand is empty for this project")
    if not keywords:
        keywords = ["__all__"]

    crawl_source_norm = normalize_crawl_source(crawl_source) if crawl_source else default_crawl_source()

    try:
        import logging
        import time

        log = logging.getLogger("prodwatch.pipeline")
        pipeline_t0 = time.perf_counter()
        log.info(
            "simulate start crawl_job_id=%s project_id=%s stat_date=%s posts_per_target=%s crawl_source=%s",
            int(crawl_job_id),
            int(project_id),
            str(stat_date),
            int(posts_per_target),
            str(crawl_source_norm),
        )

        stage = "mark_job_running"
        st0 = time.perf_counter()
        log.info("simulate stage_start crawl_job_id=%s stage=%s", int(crawl_job_id), stage)
        mark_job_running(con, int(crawl_job_id))
        try:
            con.commit()
        except Exception:
            pass
        log.info(
            "simulate stage_done crawl_job_id=%s stage=%s dt_s=%.3f",
            int(crawl_job_id),
            stage,
            time.perf_counter() - st0,
        )

        stage = "generate_crawl_job_targets"
        st0 = time.perf_counter()
        log.info("simulate stage_start crawl_job_id=%s stage=%s", int(crawl_job_id), stage)
        targets = generate_crawl_job_targets(con, int(crawl_job_id), platform_ids, brand_ids, keywords)
        log.info(
            "simulate stage_done crawl_job_id=%s stage=%s dt_s=%.3f target_count=%s",
            int(crawl_job_id),
            stage,
            time.perf_counter() - st0,
            len(targets or []),
        )

        stage = "build_post_candidates"
        st0 = time.perf_counter()
        log.info("simulate stage_start crawl_job_id=%s stage=%s", int(crawl_job_id), stage)
        candidates = build_post_candidates(
            con,
            int(project_id),
            int(crawl_job_id),
            targets,
            str(stat_date),
            int(posts_per_target),
            crawl_source=str(crawl_source_norm),
        )
        log.info(
            "simulate stage_done crawl_job_id=%s stage=%s dt_s=%.3f candidate_count=%s",
            int(crawl_job_id),
            stage,
            time.perf_counter() - st0,
            len(candidates or []),
        )

        stage = "deduplicate_candidates"
        st0 = time.perf_counter()
        log.info("simulate stage_start crawl_job_id=%s stage=%s", int(crawl_job_id), stage)
        candidates = deduplicate_candidates(candidates)
        log.info(
            "simulate stage_done crawl_job_id=%s stage=%s dt_s=%.3f candidate_count=%s",
            int(crawl_job_id),
            stage,
            time.perf_counter() - st0,
            len(candidates or []),
        )

        stage = "insert_posts"
        st0 = time.perf_counter()
        log.info("simulate stage_start crawl_job_id=%s stage=%s", int(crawl_job_id), stage)
        insert_posts(con, candidates)
        post_raw_cnt = None
        try:
            row = con.execute("SELECT count(1) c FROM post_raw WHERE crawl_job_id=?;", (int(crawl_job_id),)).fetchone()
            if row is not None:
                post_raw_cnt = int(row["c"])
        except Exception:
            post_raw_cnt = None
        log.info(
            "simulate stage_done crawl_job_id=%s stage=%s dt_s=%.3f inserted_candidate_count=%s post_raw_count=%s",
            int(crawl_job_id),
            stage,
            time.perf_counter() - st0,
            len(candidates or []),
            post_raw_cnt,
        )

        stage = "deduplicate_posts"
        st0 = time.perf_counter()
        log.info("simulate stage_start crawl_job_id=%s stage=%s", int(crawl_job_id), stage)
        canonical_post_ids = deduplicate_posts(con, int(crawl_job_id))
        log.info(
            "simulate stage_done crawl_job_id=%s stage=%s dt_s=%.3f canonical_post_count=%s",
            int(crawl_job_id),
            stage,
            time.perf_counter() - st0,
            len(canonical_post_ids or []),
        )

        stage = "finalize_job_success"
        st0 = time.perf_counter()
        log.info("simulate stage_start crawl_job_id=%s stage=%s", int(crawl_job_id), stage)
        finalize_job_success_basic(con, int(crawl_job_id))
        log.info(
            "simulate stage_done crawl_job_id=%s stage=%s dt_s=%.3f",
            int(crawl_job_id),
            stage,
            time.perf_counter() - st0,
        )

        log.info(
            "simulate done crawl_job_id=%s total_dt_s=%.3f",
            int(crawl_job_id),
            time.perf_counter() - pipeline_t0,
        )
    except Exception as e:
        try:
            con.rollback()
        except Exception:
            pass
        try:
            mark_all_targets_failed(con, int(crawl_job_id))
        except Exception:
            pass
        finalize_job_failed(con, int(crawl_job_id), str(e))
        try:
            con.commit()
        except Exception:
            pass
        raise


def run_analyze_existing_job(
    *,
    con: sqlite3.Connection,
    crawl_job_id: int,
    project_id: int,
    source_crawl_job_id: int,
) -> None:
    """
    Analyze an existing batch of posts (identified by source crawl_job_id) and aggregate metrics.

    Pipeline stages:
    - resolve post ids by source_crawl_job_id
    - run_analysis
    - aggregate_daily_metrics (for all dates present in the source batch)
    - finalize_job_success
    """
    ensure_project_exists(con, int(project_id))

    try:
        import logging
        import time

        log = logging.getLogger("prodwatch.pipeline")
        pipeline_t0 = time.perf_counter()
        log.info(
            "analyze start crawl_job_id=%s project_id=%s source_crawl_job_id=%s",
            int(crawl_job_id),
            int(project_id),
            int(source_crawl_job_id),
        )

        stage = "mark_job_running"
        st0 = time.perf_counter()
        log.info("analyze stage_start crawl_job_id=%s stage=%s", int(crawl_job_id), stage)
        mark_job_running(con, int(crawl_job_id))
        try:
            con.commit()
        except Exception:
            pass
        log.info(
            "analyze stage_done crawl_job_id=%s stage=%s dt_s=%.3f",
            int(crawl_job_id),
            stage,
            time.perf_counter() - st0,
        )

        stage = "resolve_posts"
        st0 = time.perf_counter()
        log.info("analyze stage_start crawl_job_id=%s stage=%s", int(crawl_job_id), stage)
        # Canonicalize ids within the batch (dedup_key per platform) to match the full pipeline behavior.
        post_ids = deduplicate_posts(con, int(source_crawl_job_id))
        # Extra guard: ensure these posts belong to this project (avoid cross-project id mixups).
        if post_ids:
            placeholders = ",".join(["?"] * len(post_ids))
            row = con.execute(
                f"SELECT COUNT(*) c FROM post_raw WHERE project_id=? AND id IN ({placeholders});",
                tuple([int(project_id)] + [int(x) for x in post_ids]),
            ).fetchone()
            cnt = int(row["c"] or 0) if row is not None else 0
            if cnt != len(post_ids):
                raise RuntimeError("source_crawl_job_id contains posts outside this project")
        if not post_ids:
            raise RuntimeError("no posts found for source_crawl_job_id")
        log.info(
            "analyze stage_done crawl_job_id=%s stage=%s dt_s=%.3f post_count=%s",
            int(crawl_job_id),
            stage,
            time.perf_counter() - st0,
            len(post_ids or []),
        )

        stage = "run_analysis"
        st0 = time.perf_counter()
        log.info("analyze stage_start crawl_job_id=%s stage=%s", int(crawl_job_id), stage)
        run_analysis(con, int(project_id), post_ids)
        log.info(
            "analyze stage_done crawl_job_id=%s stage=%s dt_s=%.3f post_count=%s",
            int(crawl_job_id),
            stage,
            time.perf_counter() - st0,
            len(post_ids or []),
        )

        stage = "aggregate_daily_metrics"
        st0 = time.perf_counter()
        log.info("analyze stage_start crawl_job_id=%s stage=%s", int(crawl_job_id), stage)
        stat_rows = con.execute(
            """
            SELECT DISTINCT date(COALESCE(publish_time, crawled_at)) AS stat_date
            FROM post_raw
            WHERE project_id=? AND crawl_job_id=?
            ORDER BY stat_date ASC;
            """,
            (int(project_id), int(source_crawl_job_id)),
        ).fetchall()
        stat_dates = [str(r["stat_date"]) for r in stat_rows if r is not None and r["stat_date"] is not None]
        if not stat_dates:
            # Fallback: keep parity with the UI expectation that metrics exist; use today.
            stat_dates = [parse_stat_date(None)]
        for d in stat_dates:
            aggregate_daily_metrics(con, int(project_id), str(d))
        log.info(
            "analyze stage_done crawl_job_id=%s stage=%s dt_s=%.3f stat_date_count=%s",
            int(crawl_job_id),
            stage,
            time.perf_counter() - st0,
            len(stat_dates),
        )

        stage = "finalize_job_success"
        st0 = time.perf_counter()
        log.info("analyze stage_start crawl_job_id=%s stage=%s", int(crawl_job_id), stage)
        finalize_job_success(con, int(crawl_job_id), int(project_id))
        log.info(
            "analyze stage_done crawl_job_id=%s stage=%s dt_s=%.3f",
            int(crawl_job_id),
            stage,
            time.perf_counter() - st0,
        )

        log.info(
            "analyze done crawl_job_id=%s total_dt_s=%.3f",
            int(crawl_job_id),
            time.perf_counter() - pipeline_t0,
        )
    except Exception as e:
        try:
            con.rollback()
        except Exception:
            pass
        try:
            mark_all_targets_failed(con, int(crawl_job_id))
        except Exception:
            pass
        finalize_job_failed(con, int(crawl_job_id), str(e))
        try:
            con.commit()
        except Exception:
            pass
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="ProdWatch pipeline (SQLite)")
    parser.add_argument("--db", default=DB_DEFAULT_PATH)
    parser.add_argument("--project-id", type=int, default=None)
    parser.add_argument("--stat-date", default=None)
    parser.add_argument("--posts-per-target", type=int, default=3)
    args = parser.parse_args()

    stat_date = parse_stat_date(args.stat_date)
    db_path = resolve_db_path(str(args.db))
    con = connect(db_path)
    try:
        with con:
            project_id = int(args.project_id) if args.project_id is not None else bootstrap_if_empty(con)
        with con:
            crawl_job_id = run_pipeline(con, project_id, stat_date, int(args.posts_per_target))
        print(
            json.dumps(
                {"ok": True, "db": db_path, "project_id": project_id, "crawl_job_id": crawl_job_id, "stat_date": stat_date},
                ensure_ascii=False,
            )
        )
    finally:
        con.close()


if __name__ == "__main__":
    main()

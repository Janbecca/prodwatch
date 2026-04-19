# 作用：后端主流程：串联抓取→过滤→分析→报告生成等流水线。

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
        ("weibo", "Weibo"),
        ("zhihu", "Zhihu"),
        ("douyin", "Douyin"),
        ("xhs", "小红书"),
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
        # First version: only XHS uses the real crawler; other platforms fall back to mock LLM generation.
        import logging

        log = logging.getLogger("prodwatch.pipeline")
        platform_rows = con.execute("SELECT id, code FROM platform;").fetchall()
        platform_code_map = {int(r["id"]): str(r["code"] or "").strip().lower() for r in platform_rows}

        xhs_targets: list[CrawlTarget] = []
        other_targets: list[CrawlTarget] = []
        for t in targets or []:
            code = platform_code_map.get(int(t.platform_id), "")
            if code in ("xhs", "xiaohongshu", "rednote"):
                xhs_targets.append(t)
            else:
                other_targets.append(t)

        candidates: list[PostCandidate] = []
        if xhs_targets:
            try:
                candidates.extend(
                    _build_post_candidates_media_crawler(
                        con,
                        project_id,
                        crawl_job_id,
                        xhs_targets,
                        stat_date,
                        posts_per_target,
                    )
                )
            except Exception:
                try:
                    upsert_crawl_job_progress(
                        con,
                        crawl_job_id=int(crawl_job_id),
                        stage="simulate",
                        message="media_crawler failed; fallback to mock_llm",
                        meta={"crawl_source": "media_crawler", "fallback": "mock_llm"},
                    )
                    con.commit()
                except Exception:
                    pass
                log.exception(
                    "media_crawler failed; fallback to mock_llm crawl_job_id=%s",
                    int(crawl_job_id),
                )
                # Hard fallback: keep refresh stable if external crawler is misconfigured.
                candidates.extend(
                    _build_post_candidates_realistic(
                        con, project_id, crawl_job_id, xhs_targets, stat_date, posts_per_target
                    )
                )

        if other_targets:
            candidates.extend(
                _build_post_candidates_realistic(
                    con, project_id, crawl_job_id, other_targets, stat_date, posts_per_target
                )
            )
        return candidates

    # Default: mock LLM generation (deterministic seeds -> batch LLM generation).
    # NOTE: this intentionally does NOT generate fixed `posts_per_target` per crawl_job_target.
    return _build_post_candidates_realistic(con, project_id, crawl_job_id, targets, stat_date, posts_per_target)

    """
    Legacy implementation (kept as reference; no longer executed):

    # Local import to keep pipeline_main's global surface stable (minimal refactor boundary).
    from backend.services.crawler_generation_service import CrawlContext, get_crawler_generation_service

    platform_map = {
        int(r["id"]): (str(r["code"]), str(r["name"]))
        for r in con.execute("SELECT id, code, name FROM platform;").fetchall()
    }
    brand_map = {
        int(r["id"]): str(r["name"])
        for r in con.execute("SELECT id, name FROM brand;").fetchall()
    }
    ts = now_ts()
    base_date = datetime.strptime(stat_date, "%Y-%m-%d")

    candidates: list[PostCandidate] = []
    svc = get_crawler_generation_service()
    for t in targets:
        mark_crawl_job_target_status(con, t.id, "running")
        platform_code, _ = platform_map.get(t.platform_id, (f"p{t.platform_id}", ""))
        brand_name = brand_map.get(t.brand_id, f"b{t.brand_id}")
        ctx = CrawlContext(
            project_id=int(project_id),
            crawl_job_id=int(crawl_job_id),
            stat_date=str(stat_date),
            posts_per_target=int(posts_per_target),
            platform_id=int(t.platform_id),
            brand_id=int(t.brand_id),
            keyword=str(t.keyword),
            target_id=int(t.id),
            platform_code=str(platform_code),
            brand_name=str(brand_name),
        )
        posts = svc.generate_posts(ctx, con=con)
        for p in posts:
            candidates.append(
                PostCandidate(
                    project_id=int(p["project_id"]),
                    crawl_job_id=int(p["crawl_job_id"]),
                    platform_id=int(p["platform_id"]),
                    brand_id=int(p["brand_id"]),
                    external_post_id=str(p["external_post_id"]),
                    author_name=str(p["author_name"]),
                    title=str(p["title"]),
                    content=str(p["content"]),
                    post_url=str(p["post_url"]),
                    publish_time=str(p["publish_time"]),
                    crawled_at=str(p["crawled_at"]),
                    like_count=int(p["like_count"]),
                    comment_count=int(p["comment_count"]),
                    share_count=int(p["share_count"]),
                    view_count=int(p["view_count"]),
                    raw_payload=str(p["raw_payload"]),
                    dedup_key=str(p["dedup_key"]),
                    created_at=str(p["created_at"]),
                )
            )
        mark_crawl_job_target_status(con, t.id, "success")
        continue
        for i in range(posts_per_target):
            publish_dt = base_date + timedelta(minutes=7 * i + (t.id % 5))
            publish_time = publish_dt.strftime("%Y-%m-%d %H:%M:%S")

            external_post_id = sha1_hex(f"{project_id}|{platform_code}|{t.brand_id}|{t.keyword}|{stat_date}|{i}")[
                :18
            ]
            post_url = f"https://example.local/{platform_code}/post/{external_post_id}"
            dedup_key = sha1_hex(post_url)

            feature_terms = ["battery", "camera", "price", "lag", "overheat", "support"]
            feature_term = feature_terms[(t.id + i) % len(feature_terms)]
            polarity = "good" if (t.id + i) % 3 == 0 else ("ok" if (t.id + i) % 3 == 1 else "bad")

            title = f"{brand_name} {t.keyword} 体验分享"
            content = f"[{platform_code}] topic={t.keyword} brand={brand_name} feature={feature_term} feeling={polarity}"
            author_name = f"user_{sha1_hex(f'{platform_code}|{t.brand_id}|{i}')[:6]}"

            raw_payload = json.dumps(
                {
                    "platform": platform_code,
                    "brand_id": t.brand_id,
                    "keyword": t.keyword,
                    "generated": True,
                    "idx": i,
                },
                ensure_ascii=False,
            )

            candidates.append(
                PostCandidate(
                    project_id=project_id,
                    crawl_job_id=crawl_job_id,
                    platform_id=t.platform_id,
                    brand_id=t.brand_id,
                    external_post_id=external_post_id,
                    author_name=author_name,
                    title=title,
                    content=content,
                    post_url=post_url,
                    publish_time=publish_time,
                    crawled_at=ts,
                    like_count=10 + (t.id + i) % 30,
                    comment_count=2 + (t.id + 2 * i) % 15,
                    share_count=(t.id + i) % 7,
                    view_count=50 + (t.id + i) % 500,
                    raw_payload=raw_payload,
                    dedup_key=dedup_key,
                    created_at=ts,
                )
            )
        mark_crawl_job_target_status(con, t.id, "success")
    return candidates
    """


_FORBIDDEN_FIELD_STITCH_RE = re.compile(r"\b(topic|brand|feature|feeling)\s*=")


def _build_post_candidates_realistic(
    con: sqlite3.Connection,
    project_id: int,
    crawl_job_id: int,
    targets: list[CrawlTarget],
    stat_date: str,
    posts_per_target: int,
) -> list[PostCandidate]:
    """
    Realism-oriented simulated crawl:
    - build per-refresh distribution_plan (deterministic, non-uniform; no random)
    - construct seeds (one seed => one post)
    - ask crawler_generation LLM to write only (title/content) for each seed
    - fail fast when generation fails (no fallback)

    This intentionally avoids the legacy "platform x brand x keyword => fixed N posts per target" strategy.
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

    base_dt = datetime.strptime(str(stat_date), "%Y-%m-%d")
    seeds: list[dict[str, Any]] = []
    for platform_id, brand_id in pairs:
        platform_code, platform_name = platform_map.get(int(platform_id), (f"p{platform_id}", ""))
        brand_name = brand_map.get(int(brand_id), f"b{brand_id}")
        for i in range(max(1, int(posts_per_target))):
            seed_id = f"{int(crawl_job_id)}|{int(platform_id)}|{int(brand_id)}|{i}"
            publish_dt = base_dt + timedelta(minutes=(int(platform_id) % 7) * 11 + i * 7 + (int(brand_id) % 5))
            publish_time = publish_dt.strftime("%Y-%m-%d %H:%M:%S")
            external_post_id = sha1_hex(f"gen|{seed_id}")[:18]
            post_url = f"https://example.local/{platform_code}/post/{external_post_id}?job={int(crawl_job_id)}"

            h = int(sha1_hex(seed_id)[:6], 16)
            like_count = int(h % 320)
            comment_count = int((h // 7) % 120)
            share_count = int((h // 29) % 70)
            view_count = int(like_count * 30 + comment_count * 18 + share_count * 50 + (h % 500))
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
                            }
                        },
                        ensure_ascii=False,
                        default=str,
                    ),
                }
            )

    # Best-effort status tracking: targets are hints; mark them as processed for this job.
    for t in targets:
        mark_crawl_job_target_status(con, int(t.id), "running")

    prompt_version: str = get_prompt_store().get("crawler_generation").version

    # Prevent request timeouts by batching seeds into smaller LLM calls.
    # Default batch size is conservative; override via env when needed.
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
            json.dumps(res.output, ensure_ascii=False, default=str)[:2000],)
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
                log.warning(
                    "crawler_generation item seed_id=%s keys=%s item_preview=%s",
                    str(item.get("seed_id") or "").strip(),
                    list(item.keys()) if isinstance(item, dict) else None,
                    json.dumps(item, ensure_ascii=False, default=str)[:1000],
                    )
                if not isinstance(item, dict):
                    continue
                sid = str(item.get("seed_id") or "").strip()
                if sid:
                    batch_map[sid] = item
                    generated_map[sid] = item

        missing_batch = [str(s["seed_id"]) for s in batch if str(s["seed_id"]) not in batch_map]
        if missing_batch:
            raise RuntimeError(
                f"crawler_generation missing outputs for seeds (batch {bi}/{len(batches)}): "
                f"{missing_batch[:5]} (total_missing={len(missing_batch)})"
            )

    if not seeds:
        raise RuntimeError("crawler_generation seeds is empty")
    if len(generated_map) < len(seeds):
        missing = [str(s["seed_id"]) for s in seeds if str(s["seed_id"]) not in generated_map]
        raise RuntimeError(f"crawler_generation missing outputs for seeds: {missing[:5]} (total_missing={len(missing)})")

    invalid_fallback_count = 0

    def _fallback_title(seed: dict[str, Any]) -> str:
        brand = str(seed.get("brand_name") or "").strip() or "某品牌"
        hints = seed.get("keyword_hints") or []
        kw = ""
        if isinstance(hints, list) and hints:
            kw = str(hints[0] or "").strip()
        if not kw:
            kw = "体验"
        return f"{brand} {kw} 真实体验"

    def _fallback_content(seed: dict[str, Any]) -> str:
        sid = str(seed.get("seed_id") or "").strip()
        brand = str(seed.get("brand_name") or "").strip() or "某品牌"
        hints = seed.get("keyword_hints") or []
        kw = ""
        if isinstance(hints, list) and hints:
            kw = str(hints[0] or "").strip()
        if not kw:
            kw = "使用感受"
        platform_code = str(seed.get("platform_code") or "").strip().lower()
        h = int(sha1_hex(sid or brand)[:6], 16)
        if platform_code == "douyin":
            pool = [
                f"{brand}这{kw}我是真没想到…",
                f"{kw}这块还行，但也不是完美",
                f"用下来{kw}有点小问题，先观望",
                f"{brand}整体还可以，{kw}看个人需求",
            ]
        elif platform_code == "zhihu":
            pool = [
                f"最近在用{brand}，主要关注点是「{kw}」。总体来说体验还行，但也有一些细节需要适应。",
                f"如果你特别在意{kw}，建议先看一圈真实反馈再决定。我的结论是：可用，但别抱过高预期。",
                f"围绕{kw}这个点，我的感受是中规中矩。优点有，短板也有，取决于你的使用场景。",
            ]
        else:
            # weibo (default)
            pool = [
                f"这两天用{brand}，{kw}这块我感觉还行，但也有点小槽点。",
                f"{brand}的{kw}被讨论挺多，我自己用下来是：能接受，但还有优化空间。",
                f"说实话{kw}这点不算惊艳，不过整体也没翻车。",
                f"刚体验了一下，{kw}有好有坏，后面再多用几天看看。",
            ]
        return pool[h % len(pool)]

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

            # fallback（关键：避免整批失败）
            title = title or _fallback_title(s)
            content = content or _fallback_content(s)

            invalid_fallback_count += 1
        if _FORBIDDEN_FIELD_STITCH_RE.search(content):
            raise RuntimeError(f"crawler_generation invalid content (field stitch) seed_id={sid}")

        raw_payload = _merge_raw_payload_text(
            s.get("raw_payload"),
            {
                "generated_by": "llm",
                "provider": str(router_provider or ""),
                "model": str(router_model or ""),
                "prompt_version": str(prompt_version or ""),
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
        "crawler_generation completed crawl_job_id=%s total=%s fallback_count=%s",
        crawl_job_id,
        len(seeds),
        invalid_fallback_count,
    )
    return candidates


def _build_post_candidates_media_crawler(
    con: sqlite3.Connection,
    project_id: int,
    crawl_job_id: int,
    targets: list[CrawlTarget],
    stat_date: str,
    posts_per_target: int,
) -> list[PostCandidate]:
    """
    Real crawl mode (external MediaCrawler integration), first version:
    - Only Xiaohongshu (XHS) search results
    - Crawl by each target's keyword (conservative fallback if keyword is empty/"__all__")
    - Best-effort per-target error isolation (do not fail the whole refresh)
    """
    import logging
    import os

    from backend.services.external_crawlers.media_crawler_adapter import MediaCrawlerAdapter
    from backend.services.external_crawlers.media_crawler_mapper import map_xhs_item_to_post_candidate

    log = logging.getLogger("prodwatch.pipeline")

    if not targets:
        return []

    brand_rows = con.execute("SELECT id, name FROM brand;").fetchall()
    brand_map = {int(r["id"]): str(r["name"] or "").strip() for r in brand_rows}

    cookies = os.environ.get("PRODWATCH_XHS_COOKIES")
    adapter = MediaCrawlerAdapter()
    crawled_at = now_ts()

    candidates: list[PostCandidate] = []
    errors: list[str] = []
    for t in targets:
        try:
            mark_crawl_job_target_status(con, int(t.id), "running")
        except Exception:
            pass

        kw = str(t.keyword or "").strip()
        if kw == "" or kw == "__all__":
            # Conservative strategy: avoid broad crawling; use brand name as keyword if available.
            kw = brand_map.get(int(t.brand_id), "")

        if kw == "":
            try:
                mark_crawl_job_target_status(con, int(t.id), "success")
            except Exception:
                pass
            continue

        try:
            items = adapter.fetch_xhs_posts(keyword=str(kw), limit=int(posts_per_target), cookies=cookies)
            for it in items or []:
                if not isinstance(it, dict):
                    continue
                raw_json = it.get("raw_json") if isinstance(it.get("raw_json"), str) else None
                candidates.append(
                    map_xhs_item_to_post_candidate(
                        it,
                        project_id=int(project_id),
                        crawl_job_id=int(crawl_job_id),
                        platform_id=int(t.platform_id),
                        brand_id=int(t.brand_id),
                        crawled_at=str(crawled_at),
                        created_at=str(crawled_at),
                        raw_payload_json=raw_json,
                    )
                )
            try:
                mark_crawl_job_target_status(con, int(t.id), "success")
            except Exception:
                pass
        except Exception as e:
            # Best-effort: keep the overall refresh stable.
            errors.append(f"{type(e).__name__}: {e}")
            log.warning(
                "media_crawler target failed crawl_job_id=%s target_id=%s platform_id=%s brand_id=%s keyword=%s err=%s",
                int(crawl_job_id),
                int(t.id),
                int(t.platform_id),
                int(t.brand_id),
                str(kw),
                str(e),
            )
            try:
                mark_crawl_job_target_status(con, int(t.id), "failed")
            except Exception:
                pass
            continue

    # If media_crawler was requested but produced nothing due to errors, let caller decide fallback policy.
    if not candidates and errors:
        raise RuntimeError(
            "media_crawler produced 0 candidates due to errors. "
            "Check PRODWATCH_XHS_COOKIES / MediaCrawler deps. "
            f"First_error={errors[0]}"
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
    platform_code = str(seed.get("platform_code") or "")
    topic = str(seed.get("topic") or "体验")
    brand = str(seed.get("brand_name") or "").strip()
    if str(platform_code).lower() == "zhihu":
        return f"关于「{topic}」的一些观察"
    if brand:
        return f"{brand} {topic} 讨论"
    return f"{topic} 讨论"


def _fallback_content(seed: dict[str, Any]) -> str:
    platform_code = str(seed.get("platform_code") or "")
    topic = str(seed.get("topic") or "相关体验")
    relevance = str(seed.get("relevance") or "general")
    brand = str(seed.get("brand_name") or "").strip() or "某品牌"
    kw = " / ".join([str(x) for x in (seed.get("keyword_hints") or []) if str(x).strip()]) or topic
    sid = str(seed.get("seed_id") or "")
    h = sha1_hex(f"{sid}|{platform_code}|{topic}|{relevance}")
    mood = "还行"
    if relevance == "strong":
        mood = "挺有感触"
    elif relevance == "weak":
        mood = "有点纠结"
    elif relevance == "noise":
        mood = "跑个题"

    c = str(platform_code).lower()
    if c == "douyin":
        pool = [
            f"{topic}这块{mood}，{kw}确实有讨论点",
            f"{mood}…{topic}我是真的没想到会这样",
            f"{topic}就图个{mood}，别太上头",
            f"{kw}随便聊聊，{topic}最近挺火",
        ]
    elif c == "zhihu":
        pool = [
            f"最近看到很多人在讨论「{topic}」。结合我这段时间的体验，整体感受是：{mood}。",
            f"如果从「{topic}」这个维度看，很多结论其实取决于使用场景（通勤/游戏/办公）。我个人更在意的是稳定性。",
            f"关于「{topic}」，我更倾向于先看一段时间的口碑沉淀，而不是只看单次热度。{kw}这些线索只能作为参考。",
        ]
        pool = [p + " 希望后续能有更多真实样本来验证，而不是只看参数。" for p in pool]
    else:
        pool = [
            f"刷到一堆{topic}的讨论，{mood}…{kw}也太真实了",
            f"{topic}这事儿又上热搜了？我看大家说得挺分裂的",
            f"讲真，{topic}我站中立，但{kw}这点确实要注意",
            f"{brand}相关的{topic}最近挺多，不过也不排除有噪音",
        ]

    idx = int(h[:4], 16) % len(pool)
    return str(pool[idx])


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

        # 进程启动时就写入一次进度，确保UI能看到"simulate"阶段的日志和状态。
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
        # 写入进度供前端log
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

        # simulate -> analyze. 写入进度供前端log
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

        # analyze -> aggregate.写入进度供前端log
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
        #确保任何异常都能被捕获并记录到crawl_job中
        #避免出现"卡在simulate阶段但UI不显示日志"的情况。
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

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable, Optional


DB_DEFAULT_PATH = "backend/database/database.sqlite"


def resolve_db_path(db_path: str) -> str:
    """
    Avoid accidentally creating a new empty sqlite db due to a wrong relative path.
    If `db_path` exists but doesn't look like the expected schema, try a sibling fallback.
    """
    if not os.path.exists(db_path):
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
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON;")
    con.execute("PRAGMA journal_mode = WAL;")
    con.execute("PRAGMA synchronous = NORMAL;")
    return con


def sha1_hex(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


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
    ts = now_ts()
    con.execute(
        """
        INSERT INTO crawl_job(
          project_id, job_type, trigger_source, schedule_type, schedule_expr,
          status, started_at, ended_at, created_by, error_message
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (project_id, job_type, trigger_source, schedule_type, schedule_expr, "running", ts, None, created_by, None),
    )
    return int(con.execute("SELECT last_insert_rowid();").fetchone()[0])


@dataclass(frozen=True)
class CrawlTarget:
    id: int
    crawl_job_id: int
    platform_id: int
    brand_id: int
    keyword: str


def generate_crawl_job_targets(
    con: sqlite3.Connection,
    crawl_job_id: int,
    platform_ids: list[int],
    brand_ids: list[int],
    keywords: list[str],
) -> list[CrawlTarget]:
    for platform_id in platform_ids:
        for brand_id in brand_ids:
            for keyword in keywords:
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
    brand_id: int
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
) -> list[PostCandidate]:
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
    for t in targets:
        platform_code, _ = platform_map.get(t.platform_id, (f"p{t.platform_id}", ""))
        brand_name = brand_map.get(t.brand_id, f"b{t.brand_id}")
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
    return candidates


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


def insert_posts(con: sqlite3.Connection, candidates: list[PostCandidate]) -> list[int]:
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
    post_ids: list[int] = []
    for c in candidates:
        pid = fetch_one_int(
            con,
            "SELECT id FROM post_raw WHERE project_id=? AND platform_id=? AND dedup_key=?;",
            (c.project_id, c.platform_id, c.dedup_key),
        )
        if pid is not None:
            post_ids.append(pid)
    return sorted(set(post_ids))


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


def insert_clean_result(con: sqlite3.Connection, post_id: int, clean_text: str) -> None:
    exists = fetch_one_int(con, "SELECT 1 FROM post_clean_result WHERE post_id=? LIMIT 1;", (post_id,))
    if exists is not None:
        return
    ts = now_ts()
    text = clean_text.strip()
    is_valid = 1 if text else 0
    invalid_reason = None if is_valid else "empty"
    language = "zh" if any("\u4e00" <= ch <= "\u9fff" for ch in text) else "en"
    con.execute(
        """
        INSERT INTO post_clean_result(post_id, is_valid, invalid_reason, clean_text, language, analyzed_at)
        VALUES(?, ?, ?, ?, ?, ?);
        """,
        (post_id, is_valid, invalid_reason, text, language, ts),
    )


def insert_sentiment_result(con: sqlite3.Connection, post_id: int, text: str) -> None:
    exists = fetch_one_int(con, "SELECT 1 FROM post_sentiment_result WHERE post_id=? LIMIT 1;", (post_id,))
    if exists is not None:
        return
    ts = now_ts()
    positive_terms = ["good", "love", "recommend", "satisfied", "great", "awesome"]
    negative_terms = ["bad", "disappointed", "trash", "lag", "overheat", "refund"]
    score = 0.0
    for t in positive_terms:
        if t in text:
            score += 0.2
    for t in negative_terms:
        if t in text:
            score -= 0.2
    score = max(-1.0, min(1.0, score))
    if score > 0.1:
        sentiment = "positive"
    elif score < -0.1:
        sentiment = "negative"
    else:
        sentiment = "neutral"
    con.execute(
        """
        INSERT INTO post_sentiment_result(
          post_id, sentiment, sentiment_score, emotion_intensity, model_version, analyzed_at
        )
        VALUES(?, ?, ?, ?, ?, ?);
        """,
        (post_id, sentiment, score, abs(score), "mock-v1", ts),
    )


def insert_spam_result(con: sqlite3.Connection, post_id: int, text: str) -> None:
    exists = fetch_one_int(con, "SELECT 1 FROM post_spam_result WHERE post_id=? LIMIT 1;", (post_id,))
    if exists is not None:
        return
    ts = now_ts()
    spam_terms = ["free", "discount", "referral", "dm me", "scan qr", "add me"]
    hit = any(t in text for t in spam_terms)
    spam_label = "spam" if hit else "normal"
    spam_score = 0.9 if hit else 0.1
    con.execute(
        """
        INSERT INTO post_spam_result(post_id, spam_label, spam_score, analyzed_at)
        VALUES(?, ?, ?, ?);
        """,
        (post_id, spam_label, spam_score, ts),
    )


def insert_keyword_results(
    con: sqlite3.Connection, post_id: int, text: str, project_keywords: list[sqlite3.Row]
) -> None:
    ts = now_ts()
    for kw_row in project_keywords:
        kw = str(kw_row["keyword"])
        if kw and kw in text:
            exists = fetch_one_int(
                con,
                "SELECT 1 FROM post_keyword_result WHERE post_id=? AND keyword=? LIMIT 1;",
                (post_id, kw),
            )
            if exists is not None:
                continue
            kw_type = kw_row["keyword_type"]
            con.execute(
                """
                INSERT INTO post_keyword_result(post_id, keyword, keyword_type, confidence, analyzed_at)
                VALUES(?, ?, ?, ?, ?);
                """,
                (post_id, kw, kw_type, 0.9, ts),
            )


def insert_feature_results(
    con: sqlite3.Connection, post_id: int, text: str, feature_terms: list[str]
) -> None:
    ts = now_ts()
    for f in feature_terms:
        if f in text:
            exists = fetch_one_int(
                con,
                "SELECT 1 FROM post_feature_result WHERE post_id=? AND feature_name=? LIMIT 1;",
                (post_id, f),
            )
            if exists is not None:
                continue
            feature_sentiment = "neutral"
            if any(t in text for t in ["bad", "disappointed", "lag", "overheat"]):
                feature_sentiment = "negative"
            elif any(t in text for t in ["good", "recommend", "satisfied", "great"]):
                feature_sentiment = "positive"
            con.execute(
                """
                INSERT INTO post_feature_result(post_id, feature_name, feature_sentiment, confidence, analyzed_at)
                VALUES(?, ?, ?, ?, ?);
                """,
                (post_id, f, feature_sentiment, 0.8, ts),
            )


def run_analysis(con: sqlite3.Connection, project_id: int, post_ids: list[int]) -> None:
    project_keywords = con.execute(
        """
        SELECT keyword, keyword_type
        FROM project_keyword
        WHERE project_id=? AND is_enabled=1
        ORDER BY COALESCE(weight, 0) DESC, id;
        """,
        (project_id,),
    ).fetchall()
    default_features = ["battery", "camera", "price", "support", "performance", "design", "lag", "overheat"]
    feature_terms = [
        str(r["keyword"]) for r in project_keywords if (r["keyword_type"] or "").lower() == "feature"
    ]
    for f in default_features:
        if f not in feature_terms:
            feature_terms.append(f)

    for post_id in post_ids:
        row = con.execute(
            "SELECT id, brand_id, title, content FROM post_raw WHERE id=?;",
            (post_id,),
        ).fetchone()
        if not row:
            continue
        brand_id = row["brand_id"]
        if brand_id is not None:
            ensure_post_brand_relation(con, int(row["id"]), int(brand_id))
        title = (row["title"] or "").strip()
        content = (row["content"] or "").strip()
        text = (title + "\n" + content).strip()

        insert_clean_result(con, post_id, text)
        insert_sentiment_result(con, post_id, text)
        insert_keyword_results(con, post_id, text, project_keywords)
        insert_spam_result(con, post_id, text)
        insert_feature_results(con, post_id, text, feature_terms)


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


def aggregate_daily_metrics(con: sqlite3.Connection, project_id: int, stat_date: str) -> None:
    ts = now_ts()

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
          SELECT post_id, COUNT(*) AS kw_hits FROM post_keyword_result GROUP BY post_id
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
          pkr.keyword,
          COUNT(*) AS hit_count
        FROM post_keyword_result pkr
        JOIN post_raw pr ON pr.id = pkr.post_id
        WHERE pr.project_id=?
          AND date(COALESCE(pr.publish_time, pr.crawled_at)) = ?
          AND pr.brand_id IS NOT NULL
          AND pr.platform_id IS NOT NULL
        GROUP BY pr.project_id, pr.brand_id, pr.platform_id, stat_date, pkr.keyword
        ORDER BY pr.brand_id, pr.platform_id, pkr.keyword;
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
    con.execute(
        "UPDATE crawl_job SET status=?, ended_at=?, error_message=? WHERE id=?;",
        ("success", ts, None, crawl_job_id),
    )
    con.execute("UPDATE project SET last_refresh_at=?, updated_at=? WHERE id=?;", (ts, ts, project_id))


def finalize_job_failed(con: sqlite3.Connection, crawl_job_id: int, error_message: str) -> None:
    ts = now_ts()
    con.execute(
        "UPDATE crawl_job SET status=?, ended_at=?, error_message=? WHERE id=?;",
        ("failed", ts, error_message[:500], crawl_job_id),
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
) -> int:
    ensure_project_exists(con, project_id)
    platform_ids, brand_ids, keywords = load_project_scope(con, project_id)
    if not platform_ids:
        raise RuntimeError("project_platform is empty for this project")
    if not brand_ids:
        raise RuntimeError("project_brand is empty for this project")
    if not keywords:
        raise RuntimeError("project_keyword is empty for this project")

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
        targets = generate_crawl_job_targets(con, crawl_job_id, platform_ids, brand_ids, keywords)
        candidates = build_post_candidates(con, project_id, crawl_job_id, targets, stat_date, posts_per_target)

        candidates = deduplicate_candidates(candidates)
        insert_posts(con, candidates)

        canonical_post_ids = deduplicate_posts(con, crawl_job_id)
        run_analysis(con, project_id, canonical_post_ids)
        aggregate_daily_metrics(con, project_id, stat_date)
        finalize_job_success(con, crawl_job_id, project_id)
        return crawl_job_id
    except Exception as e:
        finalize_job_failed(con, crawl_job_id, str(e))
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

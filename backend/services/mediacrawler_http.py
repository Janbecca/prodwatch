"""
MediaCrawler HTTP client (best-effort).

Design constraints:
- ProdWatch must NOT import MediaCrawler code; interact via HTTP only.
- MediaCrawler is currently a global singleton (no run_id). We therefore:
  - run crawls sequentially per platform,
  - snapshot data files before/after to locate newly generated outputs,
    - parse `source_keyword` to attribute items back to queries.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urlencode, quote
from urllib.request import Request, urlopen
from urllib.error import HTTPError


def _env_str(name: str, default: str) -> str:
    try:
        v = str(os.environ.get(name) or "").strip()
        return v if v else default
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.environ.get(name) or str(default)).strip())
    except Exception:
        return int(default)


def _http_json(method: str, url: str, *, body: Any = None, timeout_s: int = 30) -> Any:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = Request(url, data=data, method=str(method).upper(), headers=headers)
    try:
        with urlopen(req, timeout=float(timeout_s)) as resp:
            raw = resp.read()
    except HTTPError as e:
        try:
            raw = e.read()
        except Exception:
            raw = b""
        # Re-raise with best-effort detail.
        detail = ""
        try:
            j = json.loads(raw.decode("utf-8", errors="ignore"))
            if isinstance(j, dict) and j.get("detail"):
                detail = str(j.get("detail"))
        except Exception:
            detail = raw.decode("utf-8", errors="ignore")[:300] if raw else ""
        msg = f"HTTP {getattr(e, 'code', '')}: {detail or getattr(e, 'reason', '') or 'request failed'}"
        raise RuntimeError(msg) from None
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8", errors="ignore"))
    except Exception:
        return {}


@dataclass(frozen=True)
class MediaCrawlerFile:
    path: str
    modified_at: float
    record_count: Optional[int] = None


@dataclass(frozen=True)
class MediaCrawlerRunResult:
    ok: bool
    platform: str
    error: str = ""
    took_ms: int = 0
    new_files: tuple[str, ...] = ()
    chosen_file: str = ""
    preview_total: int = 0
    items: tuple[dict[str, Any], ...] = ()
    logs_tail: tuple[str, ...] = ()


class MediaCrawlerHttpClient:
    def __init__(self, base_url: Optional[str] = None):
        self.base_url = (base_url or _env_str("PRODWATCH_MEDIACRAWLER_BASE_URL", "http://localhost:8080")).rstrip("/")
        self.timeout_s = max(5, _env_int("PRODWATCH_MEDIACRAWLER_TIMEOUT_S", 30))
        self.poll_interval_ms = max(300, _env_int("PRODWATCH_MEDIACRAWLER_POLL_INTERVAL_MS", 1000))
        self.poll_timeout_s = max(10, _env_int("PRODWATCH_MEDIACRAWLER_POLL_TIMEOUT_S", 120))
        self.logs_limit = max(20, _env_int("PRODWATCH_MEDIACRAWLER_LOGS_LIMIT", 80))
        self.login_type = _env_str("PRODWATCH_MEDIACRAWLER_LOGIN_TYPE", "qrcode")  # qrcode|cookie
        self.cookies = _env_str("PRODWATCH_MEDIACRAWLER_COOKIES", "")
        self.headless = _env_str("PRODWATCH_MEDIACRAWLER_HEADLESS", "true").lower() not in {"0", "false", "no"}
        self.enable_comments = _env_str("PRODWATCH_MEDIACRAWLER_ENABLE_COMMENTS", "false").lower() in {"1", "true", "yes"}
        self.enable_sub_comments = _env_str("PRODWATCH_MEDIACRAWLER_ENABLE_SUB_COMMENTS", "false").lower() in {
            "1",
            "true",
            "yes",
        }
        self.save_option = _env_str("PRODWATCH_MEDIACRAWLER_SAVE_OPTION", "json")  # use json to support /data preview

    def _url(self, path: str) -> str:
        p = str(path or "")
        if not p.startswith("/"):
            p = "/" + p
        return f"{self.base_url}{p}"

    def health(self) -> bool:
        try:
            j = _http_json("GET", self._url("/api/health"), timeout_s=self.timeout_s)
            return str(j.get("status") or "").strip().lower() == "ok"
        except Exception:
            return False

    def env_check_best_effort(self) -> tuple[bool, str]:
        try:
            j = _http_json("GET", self._url("/api/env/check"), timeout_s=min(self.timeout_s, 30))
            ok = bool(j.get("success"))
            msg = str(j.get("message") or "") or (str(j.get("error") or "") if not ok else "")
            return ok, msg
        except Exception as e:
            return False, f"{type(e).__name__}: {e}"

    def list_files(self, *, platform: str, file_type: str = "json") -> list[MediaCrawlerFile]:
        qs = urlencode({"platform": str(platform), "file_type": str(file_type)})
        j = _http_json("GET", self._url(f"/api/data/files?{qs}"), timeout_s=self.timeout_s)
        out: list[MediaCrawlerFile] = []
        for it in (j.get("files") or []) if isinstance(j, dict) else []:
            try:
                path = str(it.get("path") or "").strip()
                if not path:
                    continue
                mod = float(it.get("modified_at") or 0.0)
                rc = it.get("record_count")
                out.append(MediaCrawlerFile(path=path, modified_at=mod, record_count=int(rc) if rc is not None else None))
            except Exception:
                continue
        out.sort(key=lambda x: x.modified_at, reverse=True)
        return out

    def preview_file(self, *, path: str, limit: int = 200) -> tuple[list[dict[str, Any]], int]:
        p = quote(str(path or "").lstrip("/"))
        qs = urlencode({"preview": "true", "limit": str(int(limit))})
        j = _http_json("GET", self._url(f"/api/data/files/{p}?{qs}"), timeout_s=self.timeout_s)
        if not isinstance(j, dict):
            return [], 0
        data = j.get("data")
        total = j.get("total")
        items = data if isinstance(data, list) else ([] if data is None else [data])
        return [x for x in items if isinstance(x, dict)], int(total) if isinstance(total, int) else len(items)

    def start_search(self, *, platform: str, keywords_csv: str, start_page: int = 1) -> tuple[bool, str]:
        body = {
            "platform": str(platform),
            "login_type": str(self.login_type),
            "crawler_type": "search",
            "keywords": str(keywords_csv or ""),
            "start_page": int(start_page),
            "enable_comments": bool(self.enable_comments),
            "enable_sub_comments": bool(self.enable_sub_comments),
            "save_option": str(self.save_option),
            "cookies": str(self.cookies),
            "headless": bool(self.headless),
        }
        try:
            _http_json("POST", self._url("/api/crawler/start"), body=body, timeout_s=self.timeout_s)
            return True, ""
        except Exception as e:
            # MediaCrawler uses HTTP 400 detail="Crawler is already running"
            msg = str(e)
            return False, msg

    def get_status(self) -> dict[str, Any]:
        j = _http_json("GET", self._url("/api/crawler/status"), timeout_s=self.timeout_s)
        return j if isinstance(j, dict) else {}

    def get_logs_tail(self) -> list[str]:
        try:
            qs = urlencode({"limit": str(int(self.logs_limit))})
            j = _http_json("GET", self._url(f"/api/crawler/logs?{qs}"), timeout_s=self.timeout_s)
            logs = j.get("logs") if isinstance(j, dict) else []
            out: list[str] = []
            if isinstance(logs, list):
                for it in logs[-self.logs_limit :]:
                    if not isinstance(it, dict):
                        continue
                    ts = str(it.get("timestamp") or "").strip()
                    lv = str(it.get("level") or "").strip().lower()
                    msg = str(it.get("message") or "").strip()
                    if not msg:
                        continue
                    out.append(f"{ts} [{lv}] {msg}" if ts else f"[{lv}] {msg}")
            return out[-self.logs_limit :]
        except Exception:
            return []

    def wait_until_idle(self) -> tuple[bool, str, dict[str, Any], list[str]]:
        t0 = time.time()
        last = {}
        while True:
            st = self.get_status()
            last = st
            status = str(st.get("status") or "").strip().lower()
            if status in {"idle"}:
                return True, "", last, self.get_logs_tail()
            if status in {"error"}:
                err = str(st.get("error_message") or "").strip() or "crawler error"
                return False, err, last, self.get_logs_tail()

            if time.time() - t0 > float(self.poll_timeout_s):
                return False, "crawler poll timeout", last, self.get_logs_tail()
            time.sleep(float(self.poll_interval_ms) / 1000.0)

    def run_search_and_collect(
        self,
        *,
        platform: str,
        keywords_csv: str,
        preview_limit: int = 500,
    ) -> MediaCrawlerRunResult:
        plat = str(platform or "").strip().lower()
        t0 = time.perf_counter()
        before = self.list_files(platform=plat, file_type="json")
        before_max_mtime = max([f.modified_at for f in before], default=0.0)

        ok_start, start_err = self.start_search(platform=plat, keywords_csv=keywords_csv, start_page=1)
        if not ok_start:
            took_ms = int((time.perf_counter() - t0) * 1000)
            return MediaCrawlerRunResult(
                ok=False,
                platform=plat,
                error=start_err or "failed to start crawler",
                took_ms=took_ms,
                logs_tail=tuple(self.get_logs_tail()),
            )

        ok_wait, wait_err, _st, logs_tail = self.wait_until_idle()
        after = self.list_files(platform=plat, file_type="json")

        new_files = [f.path for f in after if float(f.modified_at) > float(before_max_mtime)]
        chosen = new_files[0] if new_files else ""

        items: list[dict[str, Any]] = []
        total = 0
        preview_err = ""
        if chosen:
            try:
                items, total = self.preview_file(path=chosen, limit=int(preview_limit))
            except Exception as e:
                preview_err = f"{type(e).__name__}: {e}"
        else:
            preview_err = "no new output file detected"

        took_ms = int((time.perf_counter() - t0) * 1000)
        if not ok_wait:
            return MediaCrawlerRunResult(
                ok=False,
                platform=plat,
                error=wait_err or "crawler failed",
                took_ms=took_ms,
                new_files=tuple(new_files),
                chosen_file=chosen,
                preview_total=int(total),
                items=tuple(items),
                logs_tail=tuple(logs_tail),
            )
        if preview_err:
            return MediaCrawlerRunResult(
                ok=False,
                platform=plat,
                error=f"preview failed: {preview_err}",
                took_ms=took_ms,
                new_files=tuple(new_files),
                chosen_file=chosen,
                preview_total=int(total),
                items=tuple(items),
                logs_tail=tuple(logs_tail),
            )

        return MediaCrawlerRunResult(
            ok=True,
            platform=plat,
            error="",
            took_ms=took_ms,
            new_files=tuple(new_files),
            chosen_file=chosen,
            preview_total=int(total),
            items=tuple(items),
            logs_tail=tuple(logs_tail),
        )

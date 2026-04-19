from __future__ import annotations

import asyncio
import importlib
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Coroutine, Iterable, Mapping, Optional


def _default_mediacrawler_root_dir() -> Path:
    # 尝试通过当前文件路径推断 MediaCrawler 根目录
    # 适用于标准 Python 环境中的相对路径导入。
    try:
        repo_root = next(
            p for p in Path(__file__).resolve().parents if (p / "backend" / "pipeline_main.py").exists()
        )
    except StopIteration:
        # =回退到当前工作目录，适用于在非标准环境（如 Jupyter Notebook）中运行时无法正确解析 __file__ 的情况=
        repo_root = Path.cwd()
    return repo_root.parent / "MediaCrawler" / "MediaCrawler-main"


MEDIACRAWLER_ROOT_DIR = Path(r"E:\毕设\MediaCrawler\MediaCrawler-main")


@dataclass(frozen=True)
class MediaCrawlerImportSpec:
    """
    How to import MediaCrawler from an external directory.

    Notes:
    - ProdWatch and MediaCrawler are not in the same folder, so we cannot rely on relative imports.
    - The caller should provide:
      - `project_dir`: the filesystem path that should be added to `sys.path`
      - `module_name`: the top-level Python module name to import (optional in this adapter; reserved)
    """

    project_dir: Path
    module_name: str


class MediaCrawlerAdapter:
    """
    External crawler adapter for integrating an out-of-repo MediaCrawler project.

    First version scope:
    - Only Xiaohongshu (XHS) search results by keyword(s).
    - No comments, no multi-platform.
    """

    def __init__(self, *, import_spec: Optional[MediaCrawlerImportSpec] = None) -> None:
        self._import_spec = import_spec
        self._media_crawler_module: Any | None = None

    def crawl_xhs_posts_by_keyword(
        self,
        keyword: str,
        *,
        limit: int = 20,
        options: Optional[Mapping[str, Any]] = None,
    ) -> list[Mapping[str, Any]]:
        """
        Backwards-compatible name; prefer `fetch_xhs_posts`.
        """
        return self.fetch_xhs_posts(keyword=keyword, limit=limit, options=options)

    def crawl_xhs_posts_by_keywords(
        self,
        keywords: Iterable[str],
        *,
        limit_per_keyword: int = 20,
        options: Optional[Mapping[str, Any]] = None,
    ) -> list[Mapping[str, Any]]:
        items: list[Mapping[str, Any]] = []
        for kw in keywords:
            items.extend(self.fetch_xhs_posts(keyword=kw, limit=limit_per_keyword, options=options))
        return items

    def fetch_xhs_posts(
        self,
        keyword: str,
        limit: int = 20,
        *,
        options: Optional[Mapping[str, Any]] = None,
        cookies: Optional[str] = None,
        sort: str = "GENERAL",
    ) -> list[dict]:
        """
        Minimal XHS keyword search via MediaCrawler's internal API client.

        Located entry in MediaCrawler (as of your local checkout):
        - `media_platform/xhs/client.py` -> `XiaoHongShuClient.get_note_by_keyword(...)`

        Returns a `list[dict]` normalized to (best-effort):
        - note_id, title, desc, nickname, time, liked_count, comment_count, share_count, note_url, source_keyword
        - raw_json (string): raw item JSON string

        Dependency prerequisites (runtime):
        - MediaCrawler path import: `PRODWATCH_MEDIACRAWLER_ROOT` (or `MEDIACRAWLER_ROOT_DIR`) points to MediaCrawler root.
        - Python deps required by MediaCrawler's XHS client: typically `httpx`, `tenacity`, `xhshow`.
        - A valid XHS cookie string: pass `cookies=...` or set MediaCrawler `config.COOKIES`.

        Browser/Playwright:
        - This minimal adapter does NOT launch a browser / Playwright.
        - If cookies expire, you may need to obtain fresh cookies (or later extend this adapter to use MediaCrawler login).
        """
        opts = dict(options or {})
        return self._run_async(
            self._async_fetch_xhs_posts(
                keyword=keyword,
                limit=limit,
                cookies=cookies,
                sort=sort,
                options=opts,
            )
        )

    async def _async_fetch_xhs_posts(
        self,
        *,
        keyword: str,
        limit: int,
        cookies: Optional[str],
        sort: str,
        options: Mapping[str, Any],
    ) -> list[dict]:
        self._ensure_media_crawler_importable()

        try:
            import config as mc_config  # type: ignore
            from tools.crawler_util import convert_str_cookie_to_dict  # type: ignore
            from media_platform.xhs.client import XiaoHongShuClient  # type: ignore
            from media_platform.xhs.field import SearchSortType  # type: ignore
            from media_platform.xhs.help import get_search_id  # type: ignore
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(
                "Failed to import MediaCrawler modules. "
                "Install MediaCrawler dependencies into the current Python environment and "
                "ensure PRODWATCH_MEDIACRAWLER_ROOT points to the correct project root."
            ) from e

        cookie_str = cookies if cookies is not None else getattr(mc_config, "COOKIES", "")
        cookie_dict = convert_str_cookie_to_dict(cookie_str)

        is_international = bool(getattr(mc_config, "XHS_INTERNATIONAL", False))
        index_url = "https://www.rednote.com" if is_international else "https://www.xiaohongshu.com"

        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "zh-CN,zh;q=0.9",
            "cache-control": "no-cache",
            "content-type": "application/json;charset=UTF-8",
            "origin": index_url,
            "pragma": "no-cache",
            "referer": f"{index_url}/",
            "user-agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
            ),
            "Cookie": cookie_str,
        }

        # NOTE:
        # MediaCrawler's XiaoHongShuClient __init__ requires `playwright_page: Page` (type-level),
        # but keyword search/signing path does not use it in current MediaCrawler code.
        client = XiaoHongShuClient(headers=headers, playwright_page=None, cookie_dict=cookie_dict)  # type: ignore[arg-type]

        page_size = int(options.get("page_size") or 20)
        search_id = str(options.get("search_id") or get_search_id())
        sort_enum = getattr(SearchSortType, str(sort).upper(), SearchSortType.GENERAL)

        out: list[dict] = []
        page = 1
        while len(out) < max(0, int(limit)):
            try:
                res = await client.get_note_by_keyword(
                    keyword=keyword,
                    search_id=search_id,
                    page=page,
                    page_size=page_size,
                    sort=sort_enum,
                )
            except Exception as e:  # noqa: BLE001
                raise RuntimeError(
                    "MediaCrawler XHS keyword search failed. "
                    "Common causes: missing/invalid cookies, missing xhshow dependency, or network/proxy issues."
                ) from e

            items = self._extract_items(res)
            if not items:
                break

            for it in items:
                out.append(self._normalize_xhs_search_item(it, source_keyword=keyword, index_url=index_url))
                if len(out) >= limit:
                    break

            page += 1
            if len(items) < page_size:
                break

        return out

    @staticmethod
    def _extract_items(res: Any) -> list[dict]:
        if not isinstance(res, dict):
            return []
        items = res.get("items")
        if isinstance(items, list):
            return [i for i in items if isinstance(i, dict)]
        data = res.get("data")
        if isinstance(data, dict) and isinstance(data.get("items"), list):
            return [i for i in data.get("items") if isinstance(i, dict)]
        return []

    @staticmethod
    def _normalize_xhs_search_item(item: Mapping[str, Any], *, source_keyword: str, index_url: str) -> dict:
        note_card = item.get("note_card") if isinstance(item.get("note_card"), dict) else {}

        note_id = (
            str(item.get("id") or "")
            or str(item.get("note_id") or "")
            or str(note_card.get("note_id") or "")
        )

        xsec_token = str(item.get("xsec_token") or note_card.get("xsec_token") or "")
        xsec_source = str(item.get("xsec_source") or "pc_search")

        user = note_card.get("user") if isinstance(note_card.get("user"), dict) else {}
        interact = note_card.get("interact_info") if isinstance(note_card.get("interact_info"), dict) else {}

        title = str(note_card.get("title") or note_card.get("display_title") or item.get("title") or "")
        desc = str(note_card.get("desc") or item.get("desc") or item.get("description") or "")
        nickname = str(user.get("nickname") or item.get("nickname") or "")
        time_ms = note_card.get("time") or item.get("time")

        liked_count = interact.get("liked_count", item.get("liked_count", ""))
        comment_count = interact.get("comment_count", item.get("comment_count", ""))
        share_count = interact.get("share_count", item.get("share_count", ""))

        note_url = str(item.get("note_url") or "")
        if not note_url and note_id:
            base = "https://www.rednote.com" if "rednote.com" in index_url else "https://www.xiaohongshu.com"
            if xsec_token:
                note_url = f"{base}/explore/{note_id}?xsec_token={xsec_token}&xsec_source={xsec_source}"
            else:
                note_url = f"{base}/explore/{note_id}"

        raw_json = json.dumps(item, ensure_ascii=False, separators=(",", ":"))

        return {
            "note_id": note_id,
            "title": title,
            "desc": desc,
            "nickname": nickname,
            "time": time_ms,
            "liked_count": liked_count,
            "comment_count": comment_count,
            "share_count": share_count,
            "note_url": note_url,
            "source_keyword": source_keyword,
            "raw_json": raw_json,
        }

    @staticmethod
    def _run_async(coro: Coroutine[Any, Any, list[dict]]) -> list[dict]:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop and loop.is_running():
            raise RuntimeError(
                "fetch_xhs_posts() cannot be called from within a running event loop. "
                "Call `_async_fetch_xhs_posts(...)` directly."
            )
        return asyncio.run(coro)

    def _load_media_crawler(self) -> Any:
        """
        Import MediaCrawler lazily, optionally from an external directory.

        Kept for future expansion (e.g. importing MediaCrawler's main entry module).
        """
        if self._media_crawler_module is not None:
            return self._media_crawler_module

        if not self._import_spec:
            raise RuntimeError(
                "MediaCrawler import spec is not configured. "
                "Provide MediaCrawlerImportSpec(project_dir=..., module_name=...)."
            )

        project_dir = self._import_spec.project_dir
        module_name = self._import_spec.module_name
        if not module_name or not module_name.strip():
            raise ValueError("MediaCrawlerImportSpec.module_name must be a non-empty string.")

        self._ensure_on_syspath(project_dir)
        try:
            self._media_crawler_module = importlib.import_module(module_name)
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(
                f"Failed to import MediaCrawler module '{module_name}' from '{project_dir}'."
            ) from e

        return self._media_crawler_module

    def _ensure_media_crawler_importable(self) -> None:
        root = self._import_spec.project_dir if self._import_spec else MEDIACRAWLER_ROOT_DIR
        self._ensure_on_syspath(root)

    @staticmethod
    def _ensure_on_syspath(project_dir: Path) -> None:
        project_dir = Path(project_dir).expanduser().resolve()
        if not project_dir.exists():
            raise FileNotFoundError(f"MediaCrawler project_dir does not exist: {project_dir}")

        s = str(project_dir)
        if s not in sys.path:
            sys.path.insert(0, s)


#!/usr/bin/env python3
# 作用：为项目源代码文件自动添加“作用”顶部注释（按语言选择注释语法，并尽量保留 shebang/编码声明/换行风格/BOM）。

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


RE_PY_ENCODING = re.compile(r"^[ \t\f]*#.*coding[:=][ \t]*([-\w.]+)")


@dataclass(frozen=True)
class FileEdits:
    path: Path
    added: bool
    reason: str


def _iter_target_files(repo_root: Path) -> list[Path]:
    targets: list[Path] = []

    def add_glob(pattern: str) -> None:
        targets.extend(repo_root.glob(pattern))

    add_glob("backend/**/*.py")
    add_glob("backend/database/migrations/*.sql")
    add_glob("backend/llm/tasks_config.example.yml")

    add_glob("frontend/src/**/*.js")
    add_glob("frontend/src/**/*.vue")
    add_glob("frontend/index.html")
    add_glob("frontend/vite.config.js")

    filtered: list[Path] = []
    for p in targets:
        if not p.is_file():
            continue
        parts = {part.lower() for part in p.parts}
        if ".venv" in parts:
            continue
        if "node_modules" in parts:
            continue
        if "__pycache__" in parts:
            continue
        filtered.append(p)

    return sorted(set(filtered))


def _guess_purpose(path: Path) -> str:
    p = path.as_posix()
    name = path.stem

    if p == "backend/__init__.py":
        return "后端包入口：声明 backend 为 Python 包。"

    if p.startswith("backend/api/"):
        if name == "app":
            return "后端 API：FastAPI 应用入口，注册路由与中间件。"
        if name == "db":
            return "后端 API：数据库/存储访问封装与依赖注入。"
        if name == "params":
            return "后端 API：请求/查询参数与共享数据结构定义。"
        if name.startswith("routes_"):
            subject = name.removeprefix("routes_").replace("_", " ")
            subject_cn = {
                "posts": "帖子",
                "projects": "项目",
                "project config": "项目配置",
                "project mutations": "项目配置变更",
                "project refresh": "项目刷新",
                "reports": "报告",
                "dashboard": "仪表盘",
                "crawl jobs": "抓取任务",
                "scheduler": "调度器",
                "meta": "元信息",
                "llm config": "LLM 配置",
                "llm settings": "LLM 设置",
            }.get(subject, subject)
            return f"后端 API：{subject_cn}相关路由与接口实现。"
        if name == "__init__":
            return "后端 API：路由模块包入口。"

    if p.startswith("backend/services/"):
        subject_cn = {
            "analyzer_service": "分析",
            "crawler_generation_service": "爬虫生成",
            "daily_refresh_scheduler": "每日刷新调度",
            "refresh_service": "数据刷新",
            "report_generation_service": "报告生成",
            "__init__": "服务模块",
        }.get(name, name)
        if name == "__init__":
            return "后端服务层：服务模块包入口。"
        return f"后端服务层：{subject_cn}相关业务逻辑封装。"

    if p.startswith("backend/llm/"):
        if p.startswith("backend/llm/prompts/"):
            if name == "store":
                return "LLM：Prompt 模板存储与版本管理。"
            if name == "__init__":
                return "LLM：Prompt 模块包入口。"
        if p.startswith("backend/llm/providers/"):
            provider_cn = {
                "base": "基类与通用接口",
                "deepseek_provider": "DeepSeek 提供方",
                "qwen_provider": "通义千问（Qwen）提供方",
                "mock_provider": "Mock 提供方（用于开发/测试）",
                "openai_compat_client": "OpenAI 兼容协议客户端封装",
                "__init__": "提供方模块包入口",
            }.get(name, name)
            return f"LLM：模型提供方实现（{provider_cn}）。"
        if name == "router":
            return "LLM：统一调用入口，路由到不同模型提供方并记录调用信息。"
        if name == "provider_factory":
            return "LLM：根据配置构建/选择模型提供方实例。"
        if name == "types":
            return "LLM：类型定义（请求/响应/追踪字段等）。"
        if name == "config_store":
            return "LLM：配置读取/存储与运行时开关管理。"
        if name == "file_task_config":
            return "LLM：从文件加载任务配置（支持热更新/校验）。"
        if name == "call_log":
            return "LLM：调用日志记录与持久化字段定义。"
        if name == "__init__":
            return "LLM：模块包入口。"

    if p.startswith("backend/selftest/"):
        return "后端自测：用于验证 LLM/配置/链路的最小可运行用例。"

    if p == "backend/pipeline_main.py":
        return "后端主流程：串联抓取→过滤→分析→报告生成等流水线。"
    if p == "backend/refresh_chain_b.py":
        return "后端链路：数据刷新/抓取链路编排（链路 B）。"
    if p == "backend/dashboard_chain_c.py":
        return "后端链路：仪表盘分析链路编排（链路 C）。"
    if p == "backend/report_chain_e.py":
        return "后端链路：报告生成链路编排（链路 E）。"

    if p == "frontend/index.html":
        return "前端：Vite 应用 HTML 入口。"
    if p == "frontend/vite.config.js":
        return "前端：Vite 构建与开发服务器配置。"

    if p.startswith("frontend/src/api/"):
        subject = name
        subject_cn = {
            "http": "HTTP 客户端与拦截器封装",
            "dashboard": "仪表盘",
            "posts": "帖子",
            "projects": "项目",
            "projectConfig": "项目配置",
            "projectMutations": "项目配置变更",
            "projectRefresh": "项目刷新",
            "reports": "报告",
            "llmConfig": "LLM 配置",
            "meta": "元信息",
        }.get(subject, subject)
        return f"前端 API：{subject_cn}相关后端接口调用封装。"

    if p.startswith("frontend/src/stores/"):
        subject_cn = {
            "dashboard": "仪表盘",
            "posts": "帖子",
            "projects": "项目",
            "reports": "报告",
        }.get(name, name)
        return f"前端状态：{subject_cn}相关状态管理（store）。"

    if p == "frontend/src/router/index.js":
        return "前端路由：页面路由表与导航守卫配置。"

    if p == "frontend/src/main.js":
        return "前端入口：创建 Vue 应用并挂载路由/布局/全局样式。"

    if p == "frontend/src/App.vue":
        return "前端根组件：应用顶层容器。"

    if p.startswith("frontend/src/views/"):
        view_cn = {
            "Dashboard": "仪表盘",
            "Posts": "帖子列表",
            "Reports": "报告列表",
            "ReportDetail": "报告详情",
            "ProjectConfig": "项目配置",
            "LLMConfig": "LLM 配置",
        }.get(path.stem, path.stem)
        return f"前端页面：{view_cn}视图。"

    if p.startswith("frontend/src/layouts/"):
        return "前端布局：页面通用布局与导航框架。"

    if p.startswith("frontend/src/components/"):
        parts = path.parts
        group = parts[3] if len(parts) >= 4 else "components"
        group_cn = {
            "charts": "图表",
            "common": "通用",
            "dashboard": "仪表盘",
            "posts": "帖子",
            "project": "项目",
            "project-config": "项目配置",
            "reports": "报告",
        }.get(group, group)
        return f"前端组件：{group_cn}模块组件（{path.stem}）。"

    if p.startswith("frontend/src/composables/"):
        return f"前端组合式函数：{path.stem}。"

    if p.startswith("frontend/src/utils/"):
        return f"前端工具：{path.stem}通用工具函数。"

    if p.startswith("backend/database/migrations/") and path.suffix.lower() == ".sql":
        # 文件名形如 20260331_add_report_task_fields.sql
        base = path.name
        base_no_ext = base[:-4]
        m = re.match(r"^\d{8}_(.+)$", base_no_ext)
        action = m.group(1) if m else base_no_ext
        return f"数据库迁移：{action.replace('_', ' ')}。"

    if p == "backend/llm/tasks_config.example.yml":
        return "LLM：任务配置示例文件（用于本地/自测）。"

    return f"文件作用：{path.name}。"


def _comment_line(ext: str, purpose: str) -> str:
    if ext == ".py":
        return f"# 作用：{purpose}"
    if ext in (".js", ".ts", ".tsx", ".jsx"):
        return f"// 作用：{purpose}"
    if ext in (".vue", ".html", ".xml"):
        return f"<!-- 作用：{purpose} -->"
    if ext in (".sql",):
        return f"-- 作用：{purpose}"
    if ext in (".yml", ".yaml"):
        return f"# 作用：{purpose}"
    if ext in (".css", ".scss", ".less"):
        return f"/* 作用：{purpose} */"
    raise ValueError(f"Unsupported extension: {ext}")


def _already_has_purpose_header(first_lines: list[str], ext: str) -> bool:
    head = "\n".join(first_lines[:5]).lstrip("\ufeff")
    if "作用：" not in head:
        return False
    if ext == ".py":
        return bool(re.search(r"^#\s*作用：", head, flags=re.M))
    if ext in (".js", ".ts", ".tsx", ".jsx"):
        return bool(re.search(r"^(//|/\*)\s*作用：", head, flags=re.M))
    if ext in (".vue", ".html", ".xml"):
        return bool(re.search(r"^<!--\s*作用：", head, flags=re.M))
    if ext == ".sql":
        return bool(re.search(r"^--\s*作用：", head, flags=re.M))
    if ext in (".yml", ".yaml"):
        return bool(re.search(r"^#\s*作用：", head, flags=re.M))
    if ext in (".css", ".scss", ".less"):
        return bool(re.search(r"^/\*\s*作用：", head, flags=re.M))
    return False


def _detect_newline(data: bytes) -> str:
    return "\r\n" if b"\r\n" in data else "\n"


def _read_text_preserve_bom(path: Path) -> tuple[str, bool, str]:
    data = path.read_bytes()
    newline = _detect_newline(data)
    has_bom = data.startswith(b"\xef\xbb\xbf")
    text = data.decode("utf-8-sig")
    return text, has_bom, newline


def _write_text_preserve_bom(path: Path, text: str, has_bom: bool, newline: str) -> None:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    out = normalized.replace("\n", newline)
    encoded = out.encode("utf-8")
    if has_bom:
        encoded = b"\xef\xbb\xbf" + encoded
    path.write_bytes(encoded)


def _insert_header_for_python(lines: list[str], header_line: str) -> list[str]:
    insert_at = 0
    if lines and lines[0].startswith("#!"):
        insert_at = 1
        if len(lines) >= 2 and RE_PY_ENCODING.match(lines[1]):
            insert_at = 2
    elif lines and RE_PY_ENCODING.match(lines[0]):
        insert_at = 1
    return lines[:insert_at] + [header_line, ""] + lines[insert_at:]


def _insert_header_generic(lines: list[str], header_line: str) -> list[str]:
    return [header_line, ""] + lines


def add_headers(repo_root: Path) -> list[FileEdits]:
    edits: list[FileEdits] = []
    for path in _iter_target_files(repo_root):
        ext = path.suffix.lower()
        purpose = _guess_purpose(path)
        header = _comment_line(ext, purpose)

        text, has_bom, newline = _read_text_preserve_bom(path)
        lines = text.splitlines()

        if _already_has_purpose_header(lines, ext):
            edits.append(FileEdits(path=path, added=False, reason="already_has_header"))
            continue

        if ext == ".py":
            new_lines = _insert_header_for_python(lines, header)
        else:
            new_lines = _insert_header_generic(lines, header)

        new_text = "\n".join(new_lines) + ("\n" if text.endswith(("\n", "\r\n", "\r")) else "")
        if new_text == text:
            edits.append(FileEdits(path=path, added=False, reason="no_change"))
            continue

        _write_text_preserve_bom(path, new_text, has_bom=has_bom, newline=newline)
        edits.append(FileEdits(path=path, added=True, reason="header_added"))

    return edits


def _print_summary(edits: Iterable[FileEdits]) -> int:
    total = 0
    changed = 0
    skipped = 0
    for e in edits:
        total += 1
        if e.added:
            changed += 1
        else:
            skipped += 1
    print(f"Scanned: {total}")
    print(f"Changed: {changed}")
    print(f"Skipped: {skipped}")
    return 0


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    edits = add_headers(repo_root)
    return _print_summary(edits)


if __name__ == "__main__":
    raise SystemExit(main())


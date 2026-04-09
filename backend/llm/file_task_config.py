# 作用：LLM：从文件加载任务配置（支持热更新/校验）。

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

try:
    import tomllib  # py3.11+
except Exception:  # pragma: no cover
    tomllib = None


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _parse_yaml_lite(text: str) -> dict[str, Any]:
    """
    Very small YAML subset parser for our config shape only.

    Supports:
      llm_tasks:
        task:
          provider: xxx
          model: yyy

    No lists, no quotes, no escapes. Intended for local dev convenience only.
    """
    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(-1, root)]
    for raw in text.splitlines():
        line = raw.rstrip()
        if not line or line.lstrip().startswith("#"):
            continue
        indent = len(line) - len(line.lstrip(" "))
        if ":" not in line:
            continue
        key, rest = line.lstrip().split(":", 1)
        key = key.strip()
        value = rest.strip()
        # find parent
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1] if stack else root
        if value == "":
            node: dict[str, Any] = {}
            parent[key] = node
            stack.append((indent, node))
        else:
            parent[key] = value
    return root


def load_llm_tasks_from_file(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    suffix = p.suffix.lower()
    text = _read_text(p)
    if suffix in {".json"}:
        return json.loads(text)
    if suffix in {".toml"} and tomllib is not None:
        return tomllib.loads(text)
    if suffix in {".yml", ".yaml"}:
        return _parse_yaml_lite(text)
    # default: try json
    try:
        return json.loads(text)
    except Exception:
        return {}


def get_llm_config_path() -> Optional[str]:
    v = os.environ.get("PRODWATCH_LLM_CONFIG_PATH")
    if not v:
        return None
    v = v.strip()
    return v or None


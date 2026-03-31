from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


@dataclass(frozen=True)
class PromptTemplate:
    task_type: str
    version: str
    template: str


class PromptStore:
    """
    File-based prompt store.

    Layout:
      backend/llm/prompts/templates/<task_type>.json

    JSON format:
      { "task_type": "...", "version": "v1", "template": "..." }
    """

    def __init__(self, base_dir: Optional[Path] = None):
        self.base_dir = base_dir or (Path(__file__).resolve().parent / "templates")
        self._cache: dict[str, PromptTemplate] = {}

    def get(self, task_type: str) -> PromptTemplate:
        t = str(task_type)
        if t in self._cache:
            return self._cache[t]
        path = self.base_dir / f"{t}.json"
        if not path.exists():
            # Fallback default template (still versioned)
            pt = PromptTemplate(task_type=t, version="v0", template="{{input_json}}")
            self._cache[t] = pt
            return pt
        data = json.loads(path.read_text(encoding="utf-8"))
        pt = PromptTemplate(
            task_type=str(data.get("task_type") or t),
            version=str(data.get("version") or "v1"),
            template=str(data.get("template") or "{{input_json}}"),
        )
        self._cache[t] = pt
        return pt


_store: Optional[PromptStore] = None


def get_prompt_store() -> PromptStore:
    global _store
    if _store is None:
        _store = PromptStore()
    return _store


def render_prompt(template: str, variables: dict[str, Any]) -> str:
    """
    Minimal variable injection: replaces `{{var}}` with stringified value.

    - If value is not a string, it is JSON encoded (ensure_ascii=False).
    - Missing variables are replaced with empty string.
    """
    out = str(template)
    for k, v in (variables or {}).items():
        key = "{{" + str(k) + "}}"
        if isinstance(v, str):
            val = v
        else:
            try:
                val = json.dumps(v, ensure_ascii=False, default=str)
            except Exception:
                val = str(v)
        out = out.replace(key, val)

    # Replace unresolved placeholders (best-effort) to avoid leaking template markers to providers.
    # Only handles the common {{name}} pattern.
    while True:
        start = out.find("{{")
        if start < 0:
            break
        end = out.find("}}", start + 2)
        if end < 0:
            break
        out = out[:start] + "" + out[end + 2 :]
    return out


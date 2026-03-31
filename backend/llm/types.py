from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


TaskType = str


@dataclass(frozen=True)
class LLMTaskConfig:
    task_type: str
    provider: str
    model: Optional[str] = None
    fallback_provider: str = "mock"
    fallback_model: Optional[str] = None


@dataclass(frozen=True)
class LLMTaskRequest:
    task_type: str
    input: dict[str, Any]
    prompt_text: str
    prompt_version: str
    provider: Optional[str] = None
    model: Optional[str] = None


@dataclass(frozen=True)
class LLMTaskResponse:
    ok: bool
    provider: str
    model: Optional[str]
    prompt_version: str
    output: dict[str, Any]
    error: Optional[str] = None

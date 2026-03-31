from __future__ import annotations

from typing import Any, Protocol

from backend.llm.types import LLMTaskRequest, LLMTaskResponse


class BaseLLMProvider(Protocol):
    """
    Provider interface.

    Providers should be stateless or manage their own internal clients.
    The router is responsible for picking the provider per task_type.
    """

    name: str

    def run_task(self, req: LLMTaskRequest) -> LLMTaskResponse:
        ...


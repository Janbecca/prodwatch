# 作用：LLM：根据配置构建/选择模型提供方实例。

from __future__ import annotations

from typing import Optional

from backend.llm.providers.deepseek_provider import DeepSeekProvider
from backend.llm.providers.mock_provider import MockProvider
from backend.llm.providers.qwen_provider import QwenProvider


class ProviderFactory:
    def __init__(self):
        self._providers = {
            "mock": MockProvider(),
            "deepseek": DeepSeekProvider(),
            "qwen": QwenProvider(),
        }

    def get(self, name: str):
        key = str(name or "").strip().lower()
        return self._providers.get(key)

    def get_or_mock(self, name: str):
        return self.get(name) or self._providers["mock"]

    def list_provider_names(self) -> list[str]:
        return sorted(self._providers.keys())


_factory: Optional[ProviderFactory] = None


def get_provider_factory() -> ProviderFactory:
    global _factory
    if _factory is None:
        _factory = ProviderFactory()
    return _factory

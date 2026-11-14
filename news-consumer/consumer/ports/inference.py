from __future__ import annotations

from typing import Protocol, runtime_checkable
from consumer.domain.models import MarketSignalSchema


@runtime_checkable
class InferencePort(Protocol):
    """Port interface for pluggable LLM market-signal extraction backends."""

    def extract_signal(self, article_text: str) -> MarketSignalSchema:
        """
        Extracts a structured MarketSignalSchema from raw financial news text
        using constrained JSON decoding.
        """
        ...

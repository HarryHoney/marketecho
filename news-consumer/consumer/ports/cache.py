from __future__ import annotations

from typing import Protocol, runtime_checkable
from consumer.domain.models import MarketSignalSchema


@runtime_checkable
class SemanticCachePort(Protocol):
    """
    Port interface for L2 Vector Semantic Cache.
    Short-circuits duplicate breaking news across sources when cosine similarity >= threshold.
    """

    def lookup(self, text: str, threshold: float = 0.92) -> MarketSignalSchema | None:
        """
        Embeds `text` and searches the vector index for the nearest cached news event.
        Returns the cached MarketSignalSchema if cosine similarity >= threshold, else None.
        """
        ...

    def store(self, text: str, signal: MarketSignalSchema) -> None:
        """
        Embeds `text` and stores the vector + MarketSignalSchema in the cache with TTL.
        """
        ...

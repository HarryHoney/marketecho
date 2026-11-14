from __future__ import annotations

from typing import Protocol, runtime_checkable
import schema.article_pb2 as article_pb2
from consumer.domain.models import MarketSignalSchema


@runtime_checkable
class SinkPort(Protocol):
    """Port interface for persisting validated MarketSignal outputs."""

    def save(
        self,
        article: article_pb2.Article,
        signal: MarketSignalSchema,
        semantic_cache_hit: bool = False,
    ) -> article_pb2.MarketSignal:
        """
        Persists the extracted MarketSignal and returns the constructed Protobuf message.
        """
        ...

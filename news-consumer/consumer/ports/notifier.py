from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable
import schema.article_pb2 as article_pb2
from consumer.domain.models import MarketSignalSchema


@runtime_checkable
class NotificationPort(Protocol):
    """Port interface for sending real-time market signal alerts and EOD scorecards."""

    def notify_signal(
        self,
        article: article_pb2.Article,
        signal: MarketSignalSchema,
        semantic_cache_hit: bool = False,
    ) -> bool:
        """
        Sends a real-time alert when a directional (BULLISH or BEARISH) market signal is extracted.
        Returns True if a notification was dispatched, False if skipped or disabled.
        """
        ...

    def notify_eod_summary(self, evaluated_records: Sequence[dict]) -> bool:
        """
        Sends an End-of-Day scorecard summarizing which BULLISH/BEARISH predictions
        matched actual US market close data and which missed.
        """
        ...

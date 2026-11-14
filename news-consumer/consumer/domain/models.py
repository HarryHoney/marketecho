from __future__ import annotations

import time
from enum import Enum
from typing import List
from pydantic import BaseModel, Field, field_validator

import schema.article_pb2 as article_pb2


class SentimentEnum(str, Enum):
    NEUTRAL = "NEUTRAL"
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"


class MarketSignalSchema(BaseModel):
    """
    Pydantic schema used for LLM Constrained Decoding (structured JSON output)
    and validated before converting to the shared MarketSignal Protobuf message.
    """

    tickers: List[str] = Field(
        default_factory=list,
        description="List of affected stock or crypto ticker symbols (e.g. ['NVDA', 'AAPL']). Empty if macroeconomic/general.",
    )
    sentiment: SentimentEnum = Field(
        default=SentimentEnum.NEUTRAL,
        description="Market sentiment classification: NEUTRAL, BULLISH, or BEARISH.",
    )
    sentiment_score: float = Field(
        default=0.0,
        ge=-1.0,
        le=1.0,
        description="Continuous sentiment score ranging from -1.0 (extremely bearish) to 1.0 (extremely bullish).",
    )
    confidence: float = Field(
        default=0.5,
        ge=0.0,
        le=1.0,
        description="Model confidence in the extracted market signal from 0.0 to 1.0.",
    )
    reasoning: str = Field(
        default="",
        description="Concise 1-2 sentence explanation of the market impact and catalyst.",
    )

    @field_validator("tickers", mode="before")
    @classmethod
    def normalize_tickers(cls, v: list[str] | None) -> list[str]:
        if not v:
            return []
        cleaned: list[str] = []
        seen: set[str] = set()
        for item in v:
            if not isinstance(item, str):
                continue
            sym = item.strip().upper().lstrip("$")
            if sym and sym not in seen:
                seen.add(sym)
                cleaned.append(sym)
        return cleaned

    def to_protobuf(
        self,
        article_id: str,
        semantic_cache_hit: bool = False,
        processed_at_unix: int | None = None,
    ) -> article_pb2.MarketSignal:
        """Converts this validated Pydantic schema into a Protobuf MarketSignal message."""
        pb_sentiment_map = {
            SentimentEnum.NEUTRAL: article_pb2.NEUTRAL,
            SentimentEnum.BULLISH: article_pb2.BULLISH,
            SentimentEnum.BEARISH: article_pb2.BEARISH,
        }
        pb_signal = article_pb2.MarketSignal()
        pb_signal.article_id = article_id or ""
        pb_signal.tickers.extend(self.tickers)
        pb_signal.sentiment = pb_sentiment_map.get(self.sentiment, article_pb2.NEUTRAL)
        pb_signal.sentiment_score = float(self.sentiment_score)
        pb_signal.confidence = float(self.confidence)
        pb_signal.reasoning = self.reasoning
        pb_signal.semantic_cache_hit = bool(semantic_cache_hit)
        pb_signal.processed_at_unix = (
            int(processed_at_unix) if processed_at_unix is not None else int(time.time())
        )
        return pb_signal

from __future__ import annotations

import time
from enum import Enum
from typing import List
from pydantic import BaseModel, Field, field_validator, model_validator

import schema.article_pb2 as article_pb2


class SentimentEnum(str, Enum):
    NEUTRAL = "NEUTRAL"
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"


SECTOR_TO_DEFAULT_INDICES: dict[str, list[str]] = {
    "technology": ["NASDAQ-100", "S&P 500 Information Technology", "XLK"],
    "software": ["NASDAQ-100", "IGV", "XLK"],
    "semiconductors": ["SOX", "SMH", "NASDAQ-100"],
    "artificial intelligence": ["NASDAQ-100", "SOX", "BOTZ"],
    "financials": ["S&P 500 Financials", "XLF", "KBW Bank Index"],
    "banking": ["KBW Bank Index", "XLF", "NIFTY BANK"],
    "energy": ["S&P 500 Energy", "XLE"],
    "oil & gas": ["S&P 500 Energy", "XLE"],
    "healthcare": ["S&P 500 Health Care", "XLV"],
    "biotechnology": ["NASDAQ Biotechnology", "IBB", "XBI"],
    "retail": ["S&P Retail Select Industry", "XRT", "XLY"],
    "consumer discretionary": ["S&P 500 Consumer Discretionary", "XLY"],
    "consumer staples": ["S&P 500 Consumer Staples", "XLP"],
    "industrials": ["S&P 500 Industrials", "XLI", "DJIA"],
    "aerospace & defense": ["S&P Aerospace & Defense", "ITA", "XLI"],
    "airlines": ["NYSE Arca Airline Index", "JETS"],
    "real estate": ["S&P 500 Real Estate", "XLRE"],
    "utilities": ["S&P 500 Utilities", "XLU"],
    "crypto": ["CoinDesk 20", "BITW"],
    "macroeconomics": ["S&P 500", "NASDAQ-100", "DJIA"],
}


class MarketSignalSchema(BaseModel):
    """
    Pydantic schema used for LLM Constrained Decoding (structured JSON output)
    and validated before converting to the shared MarketSignal Protobuf message.
    Handles single-stock, multi-company, generic single-sector, multi-industry, and macro news.
    """

    companies: List[str] = Field(
        default_factory=list,
        description=(
            "List of specific company or organization names mentioned "
            "(e.g. ['NVIDIA Corporation', 'Apple Inc.']). Leave empty [] if the news is "
            "generic to an entire industry, multiple sectors, or macroeconomic."
        ),
    )
    tickers: List[str] = Field(
        default_factory=list,
        description=(
            "List of affected stock, ETF, or crypto ticker symbols (e.g. ['NVDA', 'AAPL', 'XLK']). "
            "If the news is generic to an industry with no single company named, include relevant "
            "sector ETF tickers (e.g. ['XLK', 'QQQ'] for Tech, ['XLE'] for Energy, ['XLF'] for Financials) or leave []."
        ),
    )
    sectors: List[str] = Field(
        default_factory=list,
        description=(
            "List of affected industries or market sectors (e.g. ['Technology', 'Semiconductors'], "
            "['Energy', 'Airlines', 'Retail'], or ['Macroeconomics']). Always populate at least one sector."
        ),
    )
    index_identifiers: List[str] = Field(
        default_factory=list,
        description=(
            "List of relevant market indices or sector benchmark identifiers "
            "(e.g. ['NASDAQ-100', 'S&P 500 Information Technology', 'SOX', 'S&P 500', 'DJIA', 'NIFTY 50']). "
            "For generic tech or multi-industry news, map each affected industry to its benchmark index."
        ),
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

    @field_validator("companies", "sectors", "index_identifiers", mode="before")
    @classmethod
    def normalize_string_lists(cls, v: list[str] | None) -> list[str]:
        if not v:
            return []
        cleaned: list[str] = []
        seen: set[str] = set()
        for item in v:
            if not isinstance(item, str):
                continue
            val = item.strip()
            key = val.lower()
            if val and key not in seen:
                seen.add(key)
                cleaned.append(val)
        return cleaned

    @model_validator(mode="after")
    def ensure_sector_and_index_fallbacks(self) -> "MarketSignalSchema":
        """
        Ensures generic industry/multi-sector news always has a populated sector and
        benchmark index identifier even if no single stock/company was named.
        """
        if not self.sectors:
            self.sectors = ["General Market"] if not self.companies else ["Equities"]

        if not self.index_identifiers:
            inferred_indices: list[str] = []
            seen: set[str] = set()
            for sec in self.sectors:
                sec_lower = sec.lower()
                for key, defaults in SECTOR_TO_DEFAULT_INDICES.items():
                    if key in sec_lower or sec_lower in key:
                        for idx in defaults:
                            if idx.lower() not in seen:
                                seen.add(idx.lower())
                                inferred_indices.append(idx)
            if not inferred_indices:
                inferred_indices = ["S&P 500"]
            self.index_identifiers = inferred_indices

        return self

    def display_company_or_scope(self) -> str:
        """
        Returns comma-separated company names if specific companies are affected,
        or a clear 'INDUSTRY-WIDE (<sectors>)' label for generic sector/macro news.
        """
        if self.companies:
            return ", ".join(self.companies)
        if self.sectors:
            return f"INDUSTRY-WIDE ({', '.join(self.sectors)})"
        return "BROAD MARKET"

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
        pb_signal.companies.extend(self.companies)
        pb_signal.tickers.extend(self.tickers)
        pb_signal.index_identifiers.extend(self.index_identifiers)
        pb_signal.sectors.extend(self.sectors)
        pb_signal.sentiment = pb_sentiment_map.get(self.sentiment, article_pb2.NEUTRAL)
        pb_signal.sentiment_score = float(self.sentiment_score)
        pb_signal.confidence = float(self.confidence)
        pb_signal.reasoning = self.reasoning
        pb_signal.semantic_cache_hit = bool(semantic_cache_hit)
        pb_signal.processed_at_unix = (
            int(processed_at_unix) if processed_at_unix is not None else int(time.time())
        )
        return pb_signal

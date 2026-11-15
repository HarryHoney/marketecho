from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class EODQuote:
    """Represents End-of-Day (or latest trading session) market price action for a symbol."""

    symbol: str
    open_price: float
    close_price: float
    high_price: float
    low_price: float
    previous_close: float
    change_pct: float
    direction: str  # 'UP', 'DOWN', or 'FLAT'
    trading_date: str
    currency: str = "USD"


@runtime_checkable
class MarketDataPort(Protocol):
    """Port interface for fetching free End-of-Day stock, ETF, and index market data."""

    def get_eod_quote(self, symbol: str) -> EODQuote | None:
        """
        Fetches the latest trading day Open/Close/Change% quote for the given ticker or index symbol.
        Returns None if the symbol cannot be resolved.
        """
        ...

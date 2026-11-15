from __future__ import annotations

import csv
import datetime
import io
import json
import logging
import urllib.parse
import urllib.request
from typing import Any

from consumer.ports.market_data import EODQuote

logger = logging.getLogger(__name__)

# Maps common index names extracted by the LLM to Yahoo Finance ticker symbols
INDEX_TO_YAHOO_SYMBOL: dict[str, str] = {
    "s&p 500": "^GSPC",
    "s&p500": "^GSPC",
    "sp500": "^GSPC",
    "nasdaq-100": "^NDX",
    "nasdaq 100": "^NDX",
    "nasdaq composite": "^IXIC",
    "nasdaq": "^IXIC",
    "djia": "^DJI",
    "dow jones": "^DJI",
    "dow jones industrial average": "^DJI",
    "sox": "^SOX",
    "phlx semiconductor": "^SOX",
    "nifty 50": "^NSEI",
    "nifty": "^NSEI",
    "nifty bank": "^NSEBANK",
    "nifty it": "^CNXIT",
    "bse sensex": "^BSESN",
    "sensex": "^BSESN",
    "s&p 500 information technology": "XLK",
    "s&p 500 financials": "XLF",
    "s&p 500 energy": "XLE",
    "s&p 500 health care": "XLV",
    "s&p 500 industrials": "XLI",
    "s&p 500 consumer discretionary": "XLY",
    "s&p 500 consumer staples": "XLP",
    "s&p 500 utilities": "XLU",
    "s&p 500 real estate": "XLRE",
    "s&p retail select industry": "XRT",
    "s&p aerospace & defense": "ITA",
    "nyse arca airline index": "JETS",
    "kbw bank index": "KBE",
    "nasdaq biotechnology": "IBB",
    "coindesk 20": "BTC-USD",
}

INDEX_ETF_FALLBACKS: dict[str, str] = {
    "^GSPC": "SPY",
    "^NDX": "QQQ",
    "^IXIC": "QQQ",
    "^DJI": "DIA",
    "^SOX": "SMH",
}

FLAT_THRESHOLD_PCT = 0.15


def resolve_market_symbol(tickers: list[str], index_identifiers: list[str]) -> tuple[str, str]:
    """
    Determines the primary market symbol to evaluate for a news signal.
    - If specific stock/ETF tickers exist (e.g. ['NVDA']), returns ('NVDA', 'NVDA').
    - Otherwise, maps the first recognized benchmark index in `index_identifiers`
      (e.g. 'S&P 500' -> ('^GSPC', 'S&P 500 (^GSPC)')).
    - Defaults to ('^GSPC', 'S&P 500 (^GSPC)').
    """
    for raw_ticker in tickers:
        cleaned = raw_ticker.strip().upper().lstrip("$")
        if cleaned:
            return cleaned, cleaned

    for raw_idx in index_identifiers:
        norm = raw_idx.strip().lower()
        if norm in INDEX_TO_YAHOO_SYMBOL:
            yf_sym = INDEX_TO_YAHOO_SYMBOL[norm]
            return yf_sym, f"{raw_idx.strip()} ({yf_sym})"
        for key, yf_sym in INDEX_TO_YAHOO_SYMBOL.items():
            if key in norm:
                return yf_sym, f"{raw_idx.strip()} ({yf_sym})"

    return "^GSPC", "S&P 500 (^GSPC)"


def classify_direction(change_pct: float) -> str:
    if change_pct > FLAT_THRESHOLD_PCT:
        return "UP"
    if change_pct < -FLAT_THRESHOLD_PCT:
        return "DOWN"
    return "FLAT"


class YahooFinanceMarketDataAdapter:
    """
    Zero-cost, API-key-free MarketDataPort adapter using Yahoo Finance v8 chart API
    with automatic ETF proxy fallback and Stooq EOD CSV fallback.
    """

    def __init__(
        self,
        http_client: Any | None = None,
        timeout_seconds: float = 10.0,
    ) -> None:
        self.http_client = http_client
        self.timeout_seconds = timeout_seconds
        self._headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        }
        self._cache: dict[str, EODQuote] = {}

    def get_eod_quote(self, symbol: str) -> EODQuote | None:
        clean_sym = symbol.strip().upper()
        if not clean_sym:
            return None
        if clean_sym in self._cache:
            return self._cache[clean_sym]

        quote = self._fetch_from_yahoo(clean_sym)
        if quote is None and "." in clean_sym:
            # Yahoo Finance uses hyphens for share classes (e.g. BRK.A -> BRK-A, BRK.B -> BRK-B)
            quote = self._fetch_from_yahoo(clean_sym.replace(".", "-"))
        if quote is None and clean_sym in INDEX_ETF_FALLBACKS:
            quote = self._fetch_from_yahoo(INDEX_ETF_FALLBACKS[clean_sym])
        if quote is None and not clean_sym.startswith("^"):
            # Try Indian NSE suffix (.NS) if bare Indian ticker (e.g. RELIANCE, TCS)
            quote = self._fetch_from_yahoo(f"{clean_sym}.NS")

        if quote is not None:
            self._cache[clean_sym] = quote
        return quote

    def _http_get_json(self, url: str) -> dict | None:
        if self.http_client is not None:
            resp = self.http_client.get(url)
            if resp.status_code != 200:
                return None
            return resp.json()

        req = urllib.request.Request(url, headers=self._headers)
        with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
            if resp.status != 200:
                return None
            return json.loads(resp.read().decode("utf-8"))

    def _fetch_from_yahoo(self, symbol: str) -> EODQuote | None:
        encoded = urllib.parse.quote(symbol, safe="^")
        for host in ("query1.finance.yahoo.com", "query2.finance.yahoo.com"):
            url = f"https://{host}/v8/finance/chart/{encoded}?interval=1d&range=5d"
            try:
                payload = self._http_get_json(url)
                if not payload:
                    continue
                results = (payload.get("chart") or {}).get("result") or []
                if not results:
                    continue

                res = results[0]
                meta = res.get("meta") or {}
                currency = meta.get("currency", "USD")
                timestamps = res.get("timestamp") or []
                indicators = ((res.get("indicators") or {}).get("quote") or [{}])[0]

                opens = indicators.get("open") or []
                closes = indicators.get("close") or []
                highs = indicators.get("high") or []
                lows = indicators.get("low") or []

                # Walk backward to find the most recent trading session with valid open & close
                valid_idx = -1
                for idx in range(len(closes) - 1, -1, -1):
                    if idx < len(opens) and closes[idx] is not None and opens[idx] is not None:
                        valid_idx = idx
                        break

                if valid_idx < 0:
                    reg_price = meta.get("regularMarketPrice")
                    prev_close = meta.get("chartPreviousClose") or meta.get("previousClose") or reg_price
                    if reg_price is None or not prev_close:
                        continue
                    open_p = float(prev_close)
                    close_p = float(reg_price)
                    high_p = close_p
                    low_p = open_p
                    prev_p = float(prev_close)
                    trade_date = datetime.date.today().isoformat()
                else:
                    open_p = float(opens[valid_idx])
                    close_p = float(closes[valid_idx])
                    high_p = (
                        float(highs[valid_idx])
                        if valid_idx < len(highs) and highs[valid_idx] is not None
                        else max(open_p, close_p)
                    )
                    low_p = (
                        float(lows[valid_idx])
                        if valid_idx < len(lows) and lows[valid_idx] is not None
                        else min(open_p, close_p)
                    )
                    prev_p = (
                        float(closes[valid_idx - 1])
                        if valid_idx > 0 and closes[valid_idx - 1] is not None
                        else float(meta.get("chartPreviousClose") or open_p)
                    )
                    ts = (
                        timestamps[valid_idx]
                        if valid_idx < len(timestamps)
                        else int(datetime.datetime.now().timestamp())
                    )
                    trade_date = datetime.datetime.fromtimestamp(
                        ts, tz=datetime.timezone.utc
                    ).strftime("%Y-%m-%d")

                base_price = open_p if open_p > 0 else prev_p
                change_pct = (
                    ((close_p - base_price) / base_price) * 100.0
                    if base_price > 0
                    else 0.0
                )

                return EODQuote(
                    symbol=symbol,
                    open_price=round(open_p, 2),
                    close_price=round(close_p, 2),
                    high_price=round(high_p, 2),
                    low_price=round(low_p, 2),
                    previous_close=round(prev_p, 2),
                    change_pct=round(change_pct, 2),
                    direction=classify_direction(change_pct),
                    trading_date=trade_date,
                    currency=currency,
                )
            except Exception as exc:
                logger.debug("Yahoo Finance lookup on %s failed for %s: %s", host, symbol, exc)
                continue
        return None

import json
import httpx
import pytest

import schema.article_pb2 as article_pb2
from consumer.domain.models import MarketSignalSchema, SentimentEnum
from consumer.ports.market_data import EODQuote, MarketDataPort
from consumer.adapters.market_data_adapter import (
    YahooFinanceMarketDataAdapter,
    resolve_market_symbol,
)
from consumer.adapters.sink_adapter import SQLiteSinkAdapter
from evaluate_eod import evaluate_prediction_accuracy, run_eod_evaluation
from query_signals import fetch_last_n_signals


def test_resolve_market_symbol_prefers_ticker_then_maps_index():
    # 1. Explicit stock ticker
    yf_sym, display = resolve_market_symbol(["$NVDA", "MSFT"], ["S&P 500"])
    assert yf_sym == "NVDA"
    assert display == "NVDA"

    # 2. Empty tickers -> maps index identifier to Yahoo Finance symbol
    yf_sym, display = resolve_market_symbol([], ["NASDAQ-100", "S&P 500"])
    assert yf_sym == "^NDX"
    assert display == "NASDAQ-100 (^NDX)"

    yf_sym_in, display_in = resolve_market_symbol([], ["NIFTY 50"])
    assert yf_sym_in == "^NSEI"
    assert display_in == "NIFTY 50 (^NSEI)"


def test_yahoo_finance_adapter_parses_v8_chart_response():
    mock_chart_json = {
        "chart": {
            "result": [
                {
                    "meta": {"currency": "USD", "symbol": "NVDA"},
                    "timestamp": [1763078400, 1763164800],
                    "indicators": {
                        "quote": [
                            {
                                "open": [138.0, 140.0],
                                "close": [139.5, 145.6],
                                "high": [140.2, 146.5],
                                "low": [137.5, 139.8],
                            }
                        ]
                    },
                }
            ]
        }
    }

    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=mock_chart_json)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    adapter = YahooFinanceMarketDataAdapter(http_client=client)
    assert isinstance(adapter, MarketDataPort)

    quote = adapter.get_eod_quote("NVDA")
    assert quote is not None
    assert quote.symbol == "NVDA"
    assert quote.open_price == 140.0
    assert quote.close_price == 145.6
    assert pytest.approx(quote.change_pct, 0.01) == 4.0
    assert quote.direction == "UP"


def test_eod_evaluation_updates_sqlite_columns(tmp_path):
    db_path = tmp_path / "market_signals.db"
    sink = SQLiteSinkAdapter(db_path=db_path)

    # 1. Insert a BULLISH NVDA signal and a BEARISH generic S&P 500 macro signal
    art_nvda = article_pb2.Article(
        id="art-nvda-1",
        title="NVIDIA Posts Record Datacenter Revenue",
        source="CNBC",
    )
    sig_nvda = MarketSignalSchema(
        companies=["NVIDIA Corporation"],
        tickers=["NVDA"],
        sectors=["Technology", "Semiconductors"],
        index_identifiers=["NASDAQ-100"],
        sentiment=SentimentEnum.BULLISH,
        sentiment_score=0.90,
        confidence=0.95,
        reasoning="Record GPU demand.",
    )
    sink.save(article=art_nvda, signal=sig_nvda)

    art_macro = article_pb2.Article(
        id="art-macro-2",
        title="Inflation Surprise Sparks Broad Market Selloff",
        source="Yahoo Finance",
    )
    sig_macro = MarketSignalSchema(
        companies=[],
        tickers=[],
        sectors=["Macroeconomics"],
        index_identifiers=["S&P 500"],
        sentiment=SentimentEnum.BEARISH,
        sentiment_score=-0.80,
        confidence=0.90,
        reasoning="Hotter CPI print pressures equities.",
    )
    sink.save(article=art_macro, signal=sig_macro)

    class MockMarketDataAdapter:
        def get_eod_quote(self, symbol: str) -> EODQuote | None:
            if symbol == "NVDA":
                return EODQuote(
                    symbol="NVDA",
                    open_price=140.0,
                    close_price=147.0,
                    high_price=148.0,
                    low_price=139.5,
                    previous_close=139.0,
                    change_pct=5.0,
                    direction="UP",
                    trading_date="2026-11-14",
                    currency="USD",
                )
            if symbol == "^GSPC":
                return EODQuote(
                    symbol="^GSPC",
                    open_price=6000.0,
                    close_price=5910.0,
                    high_price=6010.0,
                    low_price=5900.0,
                    previous_close=6005.0,
                    change_pct=-1.5,
                    direction="DOWN",
                    trading_date="2026-11-14",
                    currency="USD",
                )
            return None

    updated = run_eod_evaluation(
        db_path=db_path,
        market_data=MockMarketDataAdapter(),
        limit=10,
        force_all=True,
    )
    assert len(updated) == 2

    rows = fetch_last_n_signals(db_path, limit=10)
    assert len(rows) == 2

    # Latest row is macro bearish (-1.5% DOWN -> ACCURATE)
    macro_row = rows[0]
    assert macro_row["evaluated_symbol"] == "S&P 500 (^GSPC)"
    assert macro_row["actual_change_pct"] == -1.5
    assert macro_row["prediction_outcome"] == "ACCURATE"
    assert "Aligned with BEARISH signal" in macro_row["performance_summary"]

    # Second row is NVDA bullish (+5.0% UP -> ACCURATE)
    nvda_row = rows[1]
    assert nvda_row["evaluated_symbol"] == "NVDA"
    assert nvda_row["actual_change_pct"] == 5.0
    assert nvda_row["prediction_outcome"] == "ACCURATE"
    assert "Aligned with BULLISH signal" in nvda_row["performance_summary"]

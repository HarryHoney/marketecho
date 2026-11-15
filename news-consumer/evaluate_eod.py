#!/usr/bin/env python3
"""
Phase 5: End-of-Day (EOD) Stock Performance & Signal Validation Service.

Evaluates analyzed news signals stored in `market_signals.db` against actual
End-of-Day stock/ETF/index market performance from Yahoo Finance
and populates the EOD performance columns in SQLite.

Usage:
  python3 news-consumer/evaluate_eod.py           # Evaluate all rows (default limit 50)
  python3 news-consumer/evaluate_eod.py -n 10     # Evaluate latest 10 rows
  python3 news-consumer/evaluate_eod.py --only-new
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path

from consumer.adapters.market_data_adapter import (
    YahooFinanceMarketDataAdapter,
    resolve_market_symbol,
)
from consumer.ports.market_data import EODQuote, MarketDataPort
from query_signals import resolve_db_path


def ensure_eod_columns(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        existing_cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(market_signals)").fetchall()
        }
        migrations = [
            ("company_name", "TEXT DEFAULT ''"),
            ("companies_json", "TEXT DEFAULT '[]'"),
            ("sector", "TEXT DEFAULT ''"),
            ("sectors_json", "TEXT DEFAULT '[]'"),
            ("index_identifier", "TEXT DEFAULT ''"),
            ("index_identifiers_json", "TEXT DEFAULT '[]'"),
            ("evaluated_symbol", "TEXT DEFAULT ''"),
            ("price_open", "REAL DEFAULT 0.0"),
            ("eod_close_price", "REAL DEFAULT 0.0"),
            ("actual_change_pct", "REAL DEFAULT 0.0"),
            ("actual_direction", "TEXT DEFAULT ''"),
            ("prediction_outcome", "TEXT DEFAULT ''"),
            ("performance_summary", "TEXT DEFAULT ''"),
            ("evaluated_at_unix", "INTEGER DEFAULT 0"),
        ]
        for col_name, col_def in migrations:
            if col_name not in existing_cols:
                conn.execute(
                    f"ALTER TABLE market_signals ADD COLUMN {col_name} {col_def}"
                )
        conn.commit()


def evaluate_prediction_accuracy(
    sentiment: str,
    sentiment_score: float,
    quote: EODQuote,
    display_symbol: str,
) -> tuple[str, str]:
    """
    Compares our AI-extracted sentiment ('BULLISH', 'BEARISH', 'NEUTRAL')
    against the actual EOD market move (`quote.change_pct` and `quote.direction`).
    Returns (prediction_outcome, performance_summary).
    """
    sent = (sentiment or "NEUTRAL").strip().upper()
    direction = quote.direction
    pct = quote.change_pct

    if sent == "BULLISH":
        if direction == "UP":
            outcome = "ACCURATE"
            verdict = f"Aligned with BULLISH signal ({sentiment_score:+.2f})"
        elif direction == "DOWN":
            outcome = "DIVERGENT"
            verdict = f"Diverged from BULLISH signal ({sentiment_score:+.2f})"
        else:
            outcome = "FLAT_MARKET"
            verdict = f"Market flat despite BULLISH signal ({sentiment_score:+.2f})"
    elif sent == "BEARISH":
        if direction == "DOWN":
            outcome = "ACCURATE"
            verdict = f"Aligned with BEARISH signal ({sentiment_score:+.2f})"
        elif direction == "UP":
            outcome = "DIVERGENT"
            verdict = f"Diverged from BEARISH signal ({sentiment_score:+.2f})"
        else:
            outcome = "FLAT_MARKET"
            verdict = f"Market flat despite BEARISH signal ({sentiment_score:+.2f})"
    else:  # NEUTRAL
        if direction == "FLAT":
            outcome = "ACCURATE"
            verdict = "Aligned with NEUTRAL signal (flat session)"
        else:
            outcome = "NEUTRAL_BASELINE"
            verdict = f"Market moved {direction} ({pct:+.2f}%) on NEUTRAL signal"

    summary = (
        f"{display_symbol} closed at {quote.close_price:.2f} {quote.currency} "
        f"({pct:+.2f}% vs open {quote.open_price:.2f} on {quote.trading_date}) — {verdict}"
    )
    return outcome, summary


def run_eod_evaluation(
    db_path: Path,
    market_data: MarketDataPort | None = None,
    limit: int = 100,
    force_all: bool = True,
) -> list[dict]:
    """
    Ensures the SQLite schema has the Phase 5 EOD columns, fetches EOD quotes for
    each analyzed signal's tickers (or benchmark index), and updates the database rows.
    """
    ensure_eod_columns(db_path)

    adapter = market_data or YahooFinanceMarketDataAdapter()
    now_unix = int(time.time())

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        if force_all:
            rows = conn.execute(
                "SELECT * FROM market_signals ORDER BY id DESC LIMIT ?",
                (max(1, limit),),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM market_signals
                WHERE evaluated_at_unix IS NULL OR evaluated_at_unix = 0
                ORDER BY id DESC LIMIT ?
                """,
                (max(1, limit),),
            ).fetchall()

        updated_records: list[dict] = []
        for r in rows:
            row_dict = dict(r)
            row_id = row_dict["id"]
            tickers = json.loads(row_dict.get("tickers_json") or "[]")
            indices = json.loads(row_dict.get("index_identifiers_json") or "[]")
            if not indices and row_dict.get("index_identifier"):
                indices = [
                    s.strip()
                    for s in str(row_dict["index_identifier"]).split(",")
                    if s.strip()
                ]

            yf_symbol, display_symbol = resolve_market_symbol(tickers, indices)
            quote = adapter.get_eod_quote(yf_symbol)
            if quote is None and yf_symbol != "^GSPC":
                # Fallback to S&P 500 benchmark if a niche/private ticker has no public quote
                quote = adapter.get_eod_quote("^GSPC")
                if quote is not None:
                    display_symbol = f"S&P 500 Benchmark (^GSPC, fallback for {yf_symbol})"

            if quote is None:
                continue

            sentiment = row_dict.get("sentiment") or "NEUTRAL"
            sentiment_score = float(row_dict.get("sentiment_score") or 0.0)
            outcome, summary = evaluate_prediction_accuracy(
                sentiment=sentiment,
                sentiment_score=sentiment_score,
                quote=quote,
                display_symbol=display_symbol,
            )

            conn.execute(
                """
                UPDATE market_signals
                SET evaluated_symbol = ?,
                    price_open = ?,
                    eod_close_price = ?,
                    actual_change_pct = ?,
                    actual_direction = ?,
                    prediction_outcome = ?,
                    performance_summary = ?,
                    evaluated_at_unix = ?
                WHERE id = ?
                """,
                (
                    display_symbol,
                    quote.open_price,
                    quote.close_price,
                    quote.change_pct,
                    quote.direction,
                    outcome,
                    summary,
                    now_unix,
                    row_id,
                ),
            )

            updated_records.append(
                {
                    "id": row_id,
                    "title": row_dict.get("title"),
                    "sentiment": sentiment,
                    "sentiment_score": round(sentiment_score, 2),
                    "evaluated_symbol": display_symbol,
                    "price_open": quote.open_price,
                    "eod_close_price": quote.close_price,
                    "actual_change_pct": quote.change_pct,
                    "actual_direction": quote.direction,
                    "prediction_outcome": outcome,
                    "performance_summary": summary,
                }
            )
        conn.commit()

    return updated_records


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Evaluate analyzed news signals against actual End-of-Day stock/index market data."
    )
    parser.add_argument(
        "-n",
        "--limit",
        type=int,
        default=50,
        help="Maximum number of latest signals to evaluate (default: 50).",
    )
    parser.add_argument(
        "--only-new",
        action="store_true",
        help="Only evaluate rows that have not been evaluated yet.",
    )
    parser.add_argument(
        "--db",
        type=str,
        default=None,
        help="Optional custom path to market_signals.db.",
    )
    args = parser.parse_args()

    db_path = resolve_db_path(args.db)
    if not db_path.exists():
        print(f"Database not found at: {db_path}", file=sys.stderr)
        sys.exit(1)

    results = run_eod_evaluation(
        db_path=db_path,
        limit=args.limit,
        force_all=not args.only_new,
    )
    print(f"=== Evaluated {len(results)} Market Signal(s) with EOD Stock/Index Data ===\n")
    for item in results:
        print(f"[{item['id']}] {item['title']}")
        print(
            f"    Predicted Signal : {item['sentiment']} ({item['sentiment_score']:+.2f})"
        )
        print(
            f"    EOD Market Move  : {item['evaluated_symbol']} | "
            f"Open: {item['price_open']:.2f} -> Close: {item['eod_close_price']:.2f} "
            f"({item['actual_change_pct']:+.2f}%, {item['actual_direction']})"
        )
        print(f"    Outcome          : [{item['prediction_outcome']}] {item['performance_summary']}")
        print("-" * 88)


if __name__ == "__main__":
    main()

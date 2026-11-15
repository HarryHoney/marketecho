#!/usr/bin/env python3
"""
CLI utility to inspect the last N extracted MarketSignal records from the local SQLite database.

Usage examples:
  python news-consumer/query_signals.py            # Show last 10 signals
  python news-consumer/query_signals.py 5          # Show last 5 signals
  python news-consumer/query_signals.py -n 20      # Show last 20 signals
  python news-consumer/query_signals.py -n 5 --json
"""
from __future__ import annotations

import argparse
import datetime
import json
import sqlite3
import sys
from pathlib import Path


def resolve_db_path(custom_path: str | None = None) -> Path:
    if custom_path:
        return Path(custom_path).expanduser().resolve()

    script_dir = Path(__file__).resolve().parent
    candidates = [
        script_dir / "data" / "market_signals.db",
        Path.cwd() / "news-consumer" / "data" / "market_signals.db",
        Path.cwd() / "data" / "market_signals.db",
    ]
    for path in candidates:
        if path.exists():
            return path
    return candidates[0]


def fetch_last_n_signals(db_path: Path, limit: int) -> list[dict]:
    if not db_path.exists():
        return []

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        existing_cols = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(market_signals)").fetchall()
        }
        if not existing_cols:
            return []

        rows = conn.execute(
            "SELECT * FROM market_signals ORDER BY id DESC LIMIT ?",
            (max(1, limit),),
        ).fetchall()

    results: list[dict] = []
    for r in rows:
        row_dict = dict(r)
        tickers = json.loads(row_dict.get("tickers_json") or "[]")
        companies = json.loads(row_dict.get("companies_json") or "[]")
        sectors = json.loads(row_dict.get("sectors_json") or "[]")
        indices = json.loads(row_dict.get("index_identifiers_json") or "[]")

        company_name = row_dict.get("company_name") or (
            ", ".join(companies) if companies else ("INDUSTRY-WIDE" if not tickers else ", ".join(tickers))
        )
        sector = row_dict.get("sector") or (", ".join(sectors) if sectors else "General Market")
        index_identifier = row_dict.get("index_identifier") or (
            ", ".join(indices) if indices else "S&P 500"
        )
        ts = row_dict.get("processed_at_unix") or 0
        processed_time = (
            datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
            if ts
            else "N/A"
        )

        results.append(
            {
                "id": row_dict.get("id"),
                "article_id": row_dict.get("article_id"),
                "processed_at": processed_time,
                "source": row_dict.get("source") or "Unknown",
                "title": row_dict.get("title") or "",
                "company_name": company_name,
                "sector": sector,
                "tickers": tickers,
                "index_identifier": index_identifier,
                "sentiment": row_dict.get("sentiment"),
                "sentiment_score": round(float(row_dict.get("sentiment_score") or 0.0), 2),
                "confidence": round(float(row_dict.get("confidence") or 0.0), 2),
                "semantic_cache_hit": bool(row_dict.get("semantic_cache_hit")),
                "reasoning": row_dict.get("reasoning") or "",
                "evaluated_symbol": row_dict.get("evaluated_symbol") or "",
                "price_open": round(float(row_dict.get("price_open") or 0.0), 2),
                "eod_close_price": round(float(row_dict.get("eod_close_price") or 0.0), 2),
                "actual_change_pct": round(float(row_dict.get("actual_change_pct") or 0.0), 2),
                "actual_direction": row_dict.get("actual_direction") or "",
                "prediction_outcome": row_dict.get("prediction_outcome") or "PENDING_EOD",
                "performance_summary": row_dict.get("performance_summary") or "Not evaluated yet (run: make evaluate-eod)",
                "url": row_dict.get("url") or "",
            }
        )
    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Query the last N market signals from the local SQLite database."
    )
    parser.add_argument(
        "n_pos",
        nargs="?",
        type=int,
        default=None,
        help="Number of latest news signals to retrieve (positional shortcut).",
    )
    parser.add_argument(
        "-n",
        "--limit",
        type=int,
        default=10,
        help="Number of latest news signals to retrieve (default: 10).",
    )
    parser.add_argument(
        "--db",
        type=str,
        default=None,
        help="Optional custom path to market_signals.db.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as formatted JSON.",
    )
    args = parser.parse_args()

    limit = args.n_pos if args.n_pos is not None else args.limit
    db_path = resolve_db_path(args.db)

    if not db_path.exists():
        print(f"Database not found at: {db_path}", file=sys.stderr)
        sys.exit(1)

    signals = fetch_last_n_signals(db_path, limit=limit)
    if args.json:
        print(json.dumps(signals, indent=2))
        return

    print(f"=== Showing Latest {len(signals)} Market Signal(s) from {db_path.name} ===\n")
    for item in signals:
        tickers_str = ", ".join(item["tickers"]) if item["tickers"] else "None (Macro/Sector)"
        cache_badge = "HIT (L2 Vector)" if item["semantic_cache_hit"] else "MISS (LLM)"
        print(f"[{item['id']}] {item['title']}")
        print(f"    Source           : {item['source']} | Processed: {item['processed_at']} | Cache: {cache_badge}")
        print(f"    Company / Scope  : {item['company_name']}")
        print(f"    Sector(s)        : {item['sector']}")
        print(f"    Tickers          : {tickers_str}")
        print(f"    Index Identifier : {item['index_identifier']}")
        print(
            f"    Sentiment        : {item['sentiment']} "
            f"(Score: {item['sentiment_score']:+.2f}, Confidence: {item['confidence']:.2f})"
        )
        print(f"    EOD Performance  : [{item['prediction_outcome']}] {item['performance_summary']}")
        print(f"    Reasoning        : {item['reasoning']}")
        print("-" * 88)


if __name__ == "__main__":
    main()

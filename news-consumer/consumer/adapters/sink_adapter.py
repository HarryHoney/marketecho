from __future__ import annotations

import json
import os
import sqlite3
import threading
from pathlib import Path
from typing import Sequence

import schema.article_pb2 as article_pb2
from consumer.domain.models import MarketSignalSchema


class JSONLSinkAdapter:
    """Appends validated MarketSignal records as newline-delimited JSON objects."""

    def __init__(self, file_path: str | Path = "data/market_signals.jsonl") -> None:
        self.file_path = Path(file_path)
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def save(
        self,
        article: article_pb2.Article,
        signal: MarketSignalSchema,
        semantic_cache_hit: bool = False,
    ) -> article_pb2.MarketSignal:
        pb_signal = signal.to_protobuf(
            article_id=article.id,
            semantic_cache_hit=semantic_cache_hit,
        )
        company_name_str = signal.display_company_or_scope()
        sector_str = ", ".join(pb_signal.sectors)
        index_identifier_str = ", ".join(pb_signal.index_identifiers)
        record = {
            "article_id": pb_signal.article_id,
            "title": article.title,
            "source": article.source,
            "url": article.url,
            "trace_id": article.trace_id,
            "published_at_unix": article.published_at_unix,
            "company_name": company_name_str,
            "companies": list(pb_signal.companies),
            "sectors": list(pb_signal.sectors),
            "sector": sector_str,
            "tickers": list(pb_signal.tickers),
            "index_identifier": index_identifier_str,
            "index_identifiers": list(pb_signal.index_identifiers),
            "sentiment": signal.sentiment.value,
            "sentiment_score": pb_signal.sentiment_score,
            "confidence": pb_signal.confidence,
            "reasoning": pb_signal.reasoning,
            "semantic_cache_hit": pb_signal.semantic_cache_hit,
            "processed_at_unix": pb_signal.processed_at_unix,
        }
        with self._lock:
            with self.file_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")
        return pb_signal


class SQLiteSinkAdapter:
    """Persists validated MarketSignal records into a local SQLite relational table."""

    def __init__(self, db_path: str | Path = "data/market_signals.db") -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_schema()

    def _init_schema(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS market_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    article_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    source TEXT,
                    url TEXT,
                    trace_id TEXT,
                    published_at_unix INTEGER,
                    company_name TEXT DEFAULT '',
                    companies_json TEXT DEFAULT '[]',
                    sector TEXT DEFAULT '',
                    sectors_json TEXT DEFAULT '[]',
                    tickers_json TEXT NOT NULL,
                    index_identifier TEXT DEFAULT '',
                    index_identifiers_json TEXT DEFAULT '[]',
                    sentiment TEXT NOT NULL,
                    sentiment_score REAL NOT NULL,
                    confidence REAL NOT NULL,
                    reasoning TEXT,
                    semantic_cache_hit INTEGER NOT NULL,
                    processed_at_unix INTEGER NOT NULL,
                    evaluated_symbol TEXT DEFAULT '',
                    price_open REAL DEFAULT 0.0,
                    eod_close_price REAL DEFAULT 0.0,
                    actual_change_pct REAL DEFAULT 0.0,
                    actual_direction TEXT DEFAULT '',
                    prediction_outcome TEXT DEFAULT '',
                    performance_summary TEXT DEFAULT '',
                    evaluated_at_unix INTEGER DEFAULT 0
                )
                """
            )
            # Auto-migrate existing SQLite tables created in Phase 3/4 without dropping data
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

    def save(
        self,
        article: article_pb2.Article,
        signal: MarketSignalSchema,
        semantic_cache_hit: bool = False,
    ) -> article_pb2.MarketSignal:
        pb_signal = signal.to_protobuf(
            article_id=article.id,
            semantic_cache_hit=semantic_cache_hit,
        )
        company_name_str = signal.display_company_or_scope()
        sector_str = ", ".join(pb_signal.sectors)
        index_identifier_str = ", ".join(pb_signal.index_identifiers)

        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO market_signals (
                        article_id, title, source, url, trace_id, published_at_unix,
                        company_name, companies_json, sector, sectors_json, tickers_json,
                        index_identifier, index_identifiers_json,
                        sentiment, sentiment_score, confidence, reasoning,
                        semantic_cache_hit, processed_at_unix
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        pb_signal.article_id,
                        article.title,
                        article.source,
                        article.url,
                        article.trace_id,
                        article.published_at_unix,
                        company_name_str,
                        json.dumps(list(pb_signal.companies)),
                        sector_str,
                        json.dumps(list(pb_signal.sectors)),
                        json.dumps(list(pb_signal.tickers)),
                        index_identifier_str,
                        json.dumps(list(pb_signal.index_identifiers)),
                        signal.sentiment.value,
                        pb_signal.sentiment_score,
                        pb_signal.confidence,
                        pb_signal.reasoning,
                        1 if pb_signal.semantic_cache_hit else 0,
                        pb_signal.processed_at_unix,
                    ),
                )
                conn.commit()
        return pb_signal


class CompositeSinkAdapter:
    """Fan-out SinkPort adapter that writes to multiple sinks (e.g., SQLite + JSONL)."""

    def __init__(self, sinks: Sequence[object]) -> None:
        if not sinks:
            raise ValueError("CompositeSinkAdapter requires at least one sink.")
        self.sinks = list(sinks)

    def save(
        self,
        article: article_pb2.Article,
        signal: MarketSignalSchema,
        semantic_cache_hit: bool = False,
    ) -> article_pb2.MarketSignal:
        result: article_pb2.MarketSignal | None = None
        for sink in self.sinks:
            result = sink.save(
                article=article,
                signal=signal,
                semantic_cache_hit=semantic_cache_hit,
            )
        assert result is not None
        return result


def create_sink_from_env(data_dir: str | Path | None = None):
    """
    Factory that builds the configured SinkPort based on SINK_BACKEND:
    - 'sqlite' (default): SQLiteSinkAdapter only (compact binary relational DB)
    - 'jsonl': JSONLSinkAdapter
    - 'both': CompositeSinkAdapter([SQLiteSinkAdapter, JSONLSinkAdapter])
    """
    base_dir = Path(data_dir or os.environ.get("SINK_DATA_DIR", "data"))
    backend = os.environ.get("SINK_BACKEND", "sqlite").strip().lower()

    sqlite_path = base_dir / "market_signals.db"
    jsonl_path = base_dir / "market_signals.jsonl"

    if backend == "both":
        return CompositeSinkAdapter(
            [
                SQLiteSinkAdapter(db_path=sqlite_path),
                JSONLSinkAdapter(file_path=jsonl_path),
            ]
        )
    if backend == "jsonl":
        return JSONLSinkAdapter(file_path=jsonl_path)
    return SQLiteSinkAdapter(db_path=sqlite_path)

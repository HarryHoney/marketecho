import json
import sqlite3
import numpy as np
import pytest

import schema.article_pb2 as article_pb2
from consumer.domain.models import MarketSignalSchema, SentimentEnum
from consumer.ports.cache import SemanticCachePort
from consumer.adapters.redis_vector_cache import InMemoryVectorSemanticCache
from consumer.adapters.sink_adapter import CompositeSinkAdapter, JSONLSinkAdapter, SQLiteSinkAdapter


class FakeSemanticEmbedder:
    """
    Deterministic 384-d embedder for fast unit testing of L2 cosine similarity.
    Maps articles about the same topic (e.g., 'nvidia'/'jensen') to nearly identical vectors
    (cosine similarity > 0.98) even when phrased differently, and unrelated topics to orthogonal vectors.
    """

    def embed(self, text: str) -> np.ndarray:
        lower = text.lower()
        vec = np.zeros(384, dtype=np.float32)
        if "nvidia" in lower or "chipmaker" in lower or "gpu" in lower:
            vec[0] = 0.99
            vec[1] = 0.05 if "yahoo" in lower else 0.08
        elif "fed" in lower or "interest rate" in lower or "powell" in lower:
            vec[10] = 0.99
            vec[11] = 0.04
        else:
            vec[100] = 1.0
        norm = float(np.linalg.norm(vec))
        return vec / norm


def test_l2_semantic_cache_hit_and_miss_across_different_framing():
    current_time = [1000.0]
    cache = InMemoryVectorSemanticCache(
        embedder=FakeSemanticEmbedder(),
        ttl_seconds=60,
        clock=lambda: current_time[0],
    )
    assert isinstance(cache, SemanticCachePort)

    source_a_text = "Yahoo Finance: NVIDIA reports record Q3 datacenter GPU revenue of $35B."
    signal_a = MarketSignalSchema(
        companies=["NVIDIA Corporation"],
        tickers=["NVDA"],
        index_identifiers=["S&P 500", "NASDAQ-100", "SOX"],
        sentiment=SentimentEnum.BULLISH,
        sentiment_score=0.92,
        confidence=0.96,
        reasoning="Record datacenter AI chip demand.",
    )

    # 1. Initially empty -> Miss
    assert cache.lookup(source_a_text, threshold=0.92) is None

    # 2. Store signal from Source A
    cache.store(source_a_text, signal_a)

    # 3. Source B frames the exact same news event in different words -> HIT (cosine >= 0.92)
    source_b_text = "CNBC Breaking: AI chipmaker surges after posting $35 billion quarterly GPU sales."
    hit = cache.lookup(source_b_text, threshold=0.92)
    assert hit is not None
    assert hit.companies == ["NVIDIA Corporation"]
    assert hit.tickers == ["NVDA"]
    assert hit.index_identifiers == ["S&P 500", "NASDAQ-100", "SOX"]
    assert hit.sentiment == SentimentEnum.BULLISH

    # 4. Completely different market event (Fed rates) -> MISS
    unrelated_text = "Federal Reserve Chair Powell holds benchmark interest rates steady."
    assert cache.lookup(unrelated_text, threshold=0.92) is None

    # 5. Advance clock past TTL -> expires
    current_time[0] += 61.0
    assert cache.lookup(source_b_text, threshold=0.92) is None


def test_sink_persists_company_name_and_index_identifier_columns(tmp_path):
    db_path = tmp_path / "market_signals.db"
    jsonl_path = tmp_path / "market_signals.jsonl"

    sink = CompositeSinkAdapter(
        [
            SQLiteSinkAdapter(db_path=db_path),
            JSONLSinkAdapter(file_path=jsonl_path),
        ]
    )

    article = article_pb2.Article(
        id="art-900",
        title="Reliance and TCS Lead NIFTY 50 Rally",
        source="CNBC",
        url="https://cnbc.com/india-nifty-rally",
        published_at_unix=1761386000,
        trace_id="00-trace900-span900-01",
    )
    signal = MarketSignalSchema(
        companies=["Reliance Industries Limited", "Tata Consultancy Services"],
        tickers=["RELIANCE", "TCS"],
        index_identifiers=["NIFTY 50", "BSE SENSEX"],
        sentiment=SentimentEnum.BULLISH,
        sentiment_score=0.84,
        confidence=0.91,
        reasoning="Strong institutional inflows lifted Indian blue-chip IT and energy stocks.",
    )

    pb_out = sink.save(article=article, signal=signal, semantic_cache_hit=True)
    assert list(pb_out.companies) == ["Reliance Industries Limited", "Tata Consultancy Services"]
    assert list(pb_out.index_identifiers) == ["NIFTY 50", "BSE SENSEX"]
    assert pb_out.semantic_cache_hit is True

    # Verify JSONL columns
    row_json = json.loads(jsonl_path.read_text(encoding="utf-8").strip())
    assert row_json["company_name"] == "Reliance Industries Limited, Tata Consultancy Services"
    assert row_json["companies"] == ["Reliance Industries Limited", "Tata Consultancy Services"]
    assert row_json["tickers"] == ["RELIANCE", "TCS"]
    assert row_json["index_identifier"] == "NIFTY 50, BSE SENSEX"
    assert row_json["index_identifiers"] == ["NIFTY 50", "BSE SENSEX"]
    assert row_json["semantic_cache_hit"] is True

    # Verify SQLite columns
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT company_name, companies_json, tickers_json,
                   index_identifier, index_identifiers_json, semantic_cache_hit
            FROM market_signals
            WHERE article_id = 'art-900'
            """
        ).fetchone()

    assert row is not None
    assert row[0] == "Reliance Industries Limited, Tata Consultancy Services"
    assert json.loads(row[1]) == ["Reliance Industries Limited", "Tata Consultancy Services"]
    assert json.loads(row[2]) == ["RELIANCE", "TCS"]
    assert row[3] == "NIFTY 50, BSE SENSEX"
    assert json.loads(row[4]) == ["NIFTY 50", "BSE SENSEX"]
    assert row[5] == 1


def test_generic_industry_and_multi_sector_edge_cases(tmp_path):
    db_path = tmp_path / "market_signals.db"
    jsonl_path = tmp_path / "market_signals.jsonl"
    sink = CompositeSinkAdapter(
        [
            SQLiteSinkAdapter(db_path=db_path),
            JSONLSinkAdapter(file_path=jsonl_path),
        ]
    )

    # Edge Case 1: Generic Tech & Semiconductor news with NO specific company mentioned
    generic_tech_signal = MarketSignalSchema(
        companies=[],
        tickers=["XLK", "SMH"],
        sectors=["Technology", "Semiconductors"],
        index_identifiers=[],  # Even if LLM omits index_identifiers, model_validator auto-maps from sectors!
        sentiment=SentimentEnum.BEARISH,
        sentiment_score=-0.65,
        confidence=0.88,
        reasoning="New export controls and enterprise SaaS budget tightening weigh on the broader technology and chip sectors.",
    )
    assert generic_tech_signal.companies == []
    assert "NASDAQ-100" in generic_tech_signal.index_identifiers
    assert "SOX" in generic_tech_signal.index_identifiers
    assert generic_tech_signal.display_company_or_scope() == "INDUSTRY-WIDE (Technology, Semiconductors)"

    art_tech = article_pb2.Article(
        id="art-generic-tech",
        title="Enterprise Cloud Spending Slows Across Tech and Chip Sector",
        source="Yahoo Finance",
    )
    sink.save(article=art_tech, signal=generic_tech_signal)

    # Edge Case 2: Multi-industry news (Oil surge impacts Energy, Airlines, and Retail)
    multi_sector_signal = MarketSignalSchema(
        companies=[],
        tickers=["XLE", "JETS", "XRT"],
        sectors=["Energy", "Airlines", "Retail"],
        index_identifiers=[],
        sentiment=SentimentEnum.BEARISH,
        sentiment_score=-0.55,
        confidence=0.85,
        reasoning="Surging crude oil prices boost energy producers but squeeze airline fuel margins and consumer retail spending.",
    )
    assert "S&P 500 Energy" in multi_sector_signal.index_identifiers
    assert "NYSE Arca Airline Index" in multi_sector_signal.index_identifiers
    assert "S&P Retail Select Industry" in multi_sector_signal.index_identifiers
    assert multi_sector_signal.display_company_or_scope() == "INDUSTRY-WIDE (Energy, Airlines, Retail)"

    art_multi = article_pb2.Article(
        id="art-multi-sector",
        title="Crude Oil Spike Pressures Airlines and Retailers While Lifting Energy Sector",
        source="CNBC",
    )
    sink.save(article=art_multi, signal=multi_sector_signal)

    lines = [json.loads(line) for line in jsonl_path.read_text(encoding="utf-8").strip().splitlines()]
    assert len(lines) == 2
    assert lines[0]["company_name"] == "INDUSTRY-WIDE (Technology, Semiconductors)"
    assert lines[0]["sectors"] == ["Technology", "Semiconductors"]
    assert "NASDAQ-100" in lines[0]["index_identifiers"]
    assert lines[1]["company_name"] == "INDUSTRY-WIDE (Energy, Airlines, Retail)"
    assert lines[1]["sectors"] == ["Energy", "Airlines", "Retail"]


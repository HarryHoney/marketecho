import json
import sqlite3
import httpx
import pytest

import schema.article_pb2 as article_pb2
from consumer.domain.models import MarketSignalSchema, SentimentEnum
from consumer.ports.inference import InferencePort
from consumer.ports.sink import SinkPort
from consumer.adapters.gemini_adapter import GeminiAdapter
from consumer.adapters.ollama_adapter import OllamaAdapter
from consumer.adapters.sink_adapter import (
    CompositeSinkAdapter,
    JSONLSinkAdapter,
    SQLiteSinkAdapter,
    create_sink_from_env,
)
import main as consumer_main


def test_market_signal_schema_validation_and_protobuf_conversion():
    schema = MarketSignalSchema(
        tickers=["$nvda", " tsm ", "NVDA"],
        sentiment=SentimentEnum.BULLISH,
        sentiment_score=0.88,
        confidence=0.95,
        reasoning="Strong AI GPU datacenter demand.",
    )
    # Deduplicated and uppercased tickers
    assert schema.tickers == ["NVDA", "TSM"]

    pb = schema.to_protobuf(
        article_id="art-001",
        semantic_cache_hit=True,
        processed_at_unix=1761386500,
    )
    assert pb.article_id == "art-001"
    assert list(pb.tickers) == ["NVDA", "TSM"]
    assert pb.sentiment == article_pb2.BULLISH
    assert pytest.approx(pb.sentiment_score, 0.001) == 0.88
    assert pytest.approx(pb.confidence, 0.001) == 0.95
    assert pb.semantic_cache_hit is True
    assert pb.processed_at_unix == 1761386500


def test_gemini_adapter_constrained_decoding_and_retry(mocker):
    mock_client = mocker.MagicMock()

    # Simulate first call raising HTTP 429 rate limit, second call succeeding with JSON
    valid_json = json.dumps(
        {
            "tickers": ["AAPL"],
            "sentiment": "BULLISH",
            "sentiment_score": 0.72,
            "confidence": 0.91,
            "reasoning": "Record iPhone services revenue.",
        }
    )
    mock_response = mocker.MagicMock()
    mock_response.parsed = None
    mock_response.text = valid_json

    mock_client.models.generate_content.side_effect = [
        RuntimeError("429 RESOURCE_EXHAUSTED: Rate limit exceeded"),
        mock_response,
    ]

    sleep_calls = []
    adapter = GeminiAdapter(
        api_key="test-key",
        model_name="gemini-2.5-flash",
        max_retries=2,
        base_delay_seconds=0.05,
        client=mock_client,
        sleep_fn=lambda d: sleep_calls.append(d),
    )

    assert isinstance(adapter, InferencePort)

    result = adapter.extract_signal("Apple reports record services revenue in Q4.")
    assert result.tickers == ["AAPL"]
    assert result.sentiment == SentimentEnum.BULLISH
    assert pytest.approx(result.sentiment_score, 0.001) == 0.72
    assert len(sleep_calls) == 1
    assert mock_client.models.generate_content.call_count == 2


def test_ollama_adapter_structured_json_output():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/chat"
        body = json.loads(request.content.decode("utf-8"))
        assert body["stream"] is False
        assert "properties" in body["format"]
        return httpx.Response(
            200,
            json={
                "message": {
                    "role": "assistant",
                    "content": json.dumps(
                        {
                            "tickers": ["TSLA"],
                            "sentiment": "BEARISH",
                            "sentiment_score": -0.65,
                            "confidence": 0.84,
                            "reasoning": "Margin compression from EV price cuts.",
                        }
                    ),
                }
            },
        )

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport)

    adapter = OllamaAdapter(
        base_url="http://localhost:11434",
        model_name="qwen2.5:3b",
        http_client=http_client,
    )
    assert isinstance(adapter, InferencePort)

    signal = adapter.extract_signal("Tesla cuts EV prices across major markets.")
    assert signal.tickers == ["TSLA"]
    assert signal.sentiment == SentimentEnum.BEARISH
    assert pytest.approx(signal.sentiment_score, 0.001) == -0.65


def test_composite_sink_persists_to_sqlite_and_jsonl(tmp_path):
    db_path = tmp_path / "market_signals.db"
    jsonl_path = tmp_path / "market_signals.jsonl"

    sink = CompositeSinkAdapter(
        [
            SQLiteSinkAdapter(db_path=db_path),
            JSONLSinkAdapter(file_path=jsonl_path),
        ]
    )
    assert isinstance(sink, SinkPort)

    article = article_pb2.Article(
        id="art-777",
        title="Microsoft Cloud Revenue Surges",
        source="CNBC",
        url="https://cnbc.com/msft-cloud",
        published_at_unix=1761386000,
        trace_id="00-trace777-span777-01",
    )
    signal = MarketSignalSchema(
        tickers=["MSFT"],
        sentiment=SentimentEnum.BULLISH,
        sentiment_score=0.81,
        confidence=0.93,
        reasoning="Azure AI growth accelerated.",
    )

    pb_out = sink.save(article=article, signal=signal, semantic_cache_hit=False)
    assert pb_out.article_id == "art-777"

    # Verify JSONL file
    lines = jsonl_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    row_json = json.loads(lines[0])
    assert row_json["article_id"] == "art-777"
    assert row_json["tickers"] == ["MSFT"]
    assert row_json["sentiment"] == "BULLISH"

    # Verify SQLite table
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT article_id, title, tickers_json, sentiment, sentiment_score FROM market_signals"
        ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "art-777"
    assert json.loads(rows[0][2]) == ["MSFT"]
    assert rows[0][3] == "BULLISH"


def test_auto_detect_inference_factory(monkeypatch, mocker):
    monkeypatch.delenv("LLM_PROVIDER", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    mocker.patch("main._load_env")

    # 1. Without GEMINI_API_KEY -> falls back to OllamaAdapter
    adapter_fallback = consumer_main.create_inference_adapter("")
    assert isinstance(adapter_fallback, OllamaAdapter)

    # 2. With GEMINI_API_KEY -> selects GeminiAdapter
    mock_genai_client = mocker.patch("consumer.adapters.gemini_adapter.genai.Client")
    adapter_gemini = consumer_main.create_inference_adapter("fake-gemini-key")
    assert isinstance(adapter_gemini, GeminiAdapter)
    mock_genai_client.assert_called_once_with(api_key="fake-gemini-key")

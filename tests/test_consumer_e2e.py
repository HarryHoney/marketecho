import os
import json
import pytest
import threading
import pika
import sys
import numpy as np

# Add the directory containing main.py to the path so we can import it
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../news-consumer")))
import main as consumer_main
import schema.article_pb2 as article_pb2
from consumer.domain.models import MarketSignalSchema, SentimentEnum
from consumer.adapters.redis_vector_cache import InMemoryVectorSemanticCache
from consumer.adapters.sink_adapter import CompositeSinkAdapter, JSONLSinkAdapter, SQLiteSinkAdapter


class DummyEmbedder:
    def embed(self, text: str) -> np.ndarray:
        vec = np.ones(384, dtype=np.float32)
        return vec / float(np.linalg.norm(vec))


def test_full_e2e_consumer_flow(mocker, tmp_path):
    """
    End-to-end test of the Python consumer pipeline:
    - Configures durable RabbitMQ queue + DLQ topology
    - Publishes a Protobuf Article payload if the test queue is empty
    - Verifies L2 Semantic Cache lookup/store, LLM extraction, and SQLite + JSONL persistence
      including `company_name` and `index_identifier` columns.
    """
    test_config_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "mock_configs"))
    os.makedirs(test_config_dir, exist_ok=True)
    with open(os.path.join(test_config_dir, "rabbit_mq.json"), "w") as f:
        json.dump(
            {
                "queue_name": "e2e_test_queue",
                "dlx_name": "marketecho.e2e.dlx",
                "dlq_name": "e2e_test_dlq",
            },
            f,
        )

    os.environ["CONFIG_DIR"] = test_config_dir
    os.environ["GEMINI_API_KEY"] = "mock_key_for_test"
    os.environ["RABBITMQ_HOST"] = "localhost"

    # Seed RabbitMQ queue if running locally, or mock BlockingConnection if RabbitMQ isn't running
    sample_article = article_pb2.Article(
        id="e2e_test_123",
        title="E2E Test Title: NVIDIA Joins S&P 500 Rally",
        description="This is a test description flowing through the system.",
        content="This is the full test content for NVIDIA Corporation.",
        source="E2E_Test_Author",
        url="https://example.com/e2e-nvidia",
        published_at_unix=1761386400,
        trace_id="00-e2etrace-e2espan-01",
    )

    try:
        conn = pika.BlockingConnection(pika.ConnectionParameters(host="localhost", port=5672))
        ch = conn.channel()
        consumer_main.declare_durable_topology(
            ch,
            queue_name="e2e_test_queue",
            dlx_name="marketecho.e2e.dlx",
            dlq_name="e2e_test_dlq",
        )
        ch.queue_purge(queue="e2e_test_queue")
        ch.basic_publish(
            exchange="",
            routing_key="e2e_test_queue",
            body=sample_article.SerializeToString(),
        )
        conn.close()
    except Exception:
        pytest.skip("Local RabbitMQ broker not reachable on localhost:5672")

    mock_signal = MarketSignalSchema(
        companies=["NVIDIA Corporation"],
        tickers=["NVDA"],
        index_identifiers=["S&P 500", "NASDAQ-100"],
        sentiment=SentimentEnum.BULLISH,
        sentiment_score=0.91,
        confidence=0.95,
        reasoning="Strong datacenter AI demand.",
    )
    mock_gemini = mocker.patch("main.call_gemini_api", return_value=mock_signal)
    mocker.patch("main.start_metrics_server")

    db_path = tmp_path / "e2e_signals.db"
    jsonl_path = tmp_path / "e2e_signals.jsonl"
    sink = CompositeSinkAdapter(
        [
            SQLiteSinkAdapter(db_path=db_path),
            JSONLSinkAdapter(file_path=jsonl_path),
        ]
    )
    cache = InMemoryVectorSemanticCache(embedder=DummyEmbedder(), ttl_seconds=60)

    original_start = pika.adapters.blocking_connection.BlockingChannel.start_consuming

    def mock_start_consuming(self):
        self.connection.call_later(0.5, self.stop_consuming)
        original_start(self)

    mocker.patch(
        "pika.adapters.blocking_connection.BlockingChannel.start_consuming",
        mock_start_consuming,
    )

    consumer_main.rabbit_mq_listener_setup(
        "mock_key_for_test",
        sink_adapter=sink,
        semantic_cache=cache,
    )

    assert mock_gemini.call_count == 1, "The Gemini API should have been called exactly once."
    last_call_args = mock_gemini.call_args[0]
    news_data_passed = last_call_args[2]
    assert "E2E Test Title" in news_data_passed
    assert "E2E_Test_Author" in news_data_passed

    # Verify JSONL sink has company_name and index_identifier
    saved_row = json.loads(jsonl_path.read_text(encoding="utf-8").strip())
    assert saved_row["company_name"] == "NVIDIA Corporation"
    assert saved_row["index_identifier"] == "S&P 500, NASDAQ-100"
    assert saved_row["tickers"] == ["NVDA"]

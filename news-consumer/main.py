from pathlib import Path
import json
import logging
import os
import sys
import time
import pika
from dotenv import load_dotenv

import schema.article_pb2 as article_pb2
from consumer.domain.models import MarketSignalSchema
from consumer.ports.cache import SemanticCachePort
from consumer.ports.inference import InferencePort
from consumer.ports.sink import SinkPort
from consumer.adapters.gemini_adapter import GeminiAdapter
from consumer.adapters.ollama_adapter import OllamaAdapter
from consumer.adapters.redis_vector_cache import create_semantic_cache_from_env
from consumer.adapters.sink_adapter import create_sink_from_env
from consumer.metrics import (
    ARTICLES_CONSUMED_TOTAL,
    DLQ_MESSAGES_TOTAL,
    L2_SEMANTIC_CACHE_HITS_TOTAL,
    L2_SEMANTIC_CACHE_MISSES_TOTAL,
    LLM_INFERENCE_DURATION_SECONDS,
    start_metrics_server,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("news-consumer")

DEFAULT_DLX_NAME = "marketecho.dlx"
DEFAULT_DLQ_NAME = "news_articles_dlq"
REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = REPO_ROOT / ".env"


def _load_env() -> None:
    if ENV_FILE.exists():
        load_dotenv(dotenv_path=ENV_FILE, override=True)
    else:
        load_dotenv(override=True)


def get_gemini_api_key() -> str:
    """Reads the API key from environment variables (or repo root .env)."""
    _load_env()
    key = os.environ.get("GEMINI_API_KEY", "").strip()
    if key == "your_gemini_api_key_here":
        return ""
    return key


def create_inference_adapter(gemini_api_key: str | None = None) -> InferencePort:
    """
    Factory that builds the configured InferencePort adapter:
    - If LLM_PROVIDER='ollama', uses local OllamaAdapter ($0 local inference).
    - If LLM_PROVIDER='gemini', uses GeminiAdapter (requires GEMINI_API_KEY).
    - Otherwise (auto-detect), uses GeminiAdapter if GEMINI_API_KEY is available,
      or falls back automatically to local OllamaAdapter.
    """
    _load_env()
    provider_choice = os.environ.get("LLM_PROVIDER", "auto").strip().lower()
    resolved_key = (
        gemini_api_key
        if gemini_api_key is not None
        else get_gemini_api_key()
    )

    if provider_choice == "ollama":
        logger.info("Using local OllamaAdapter (LLM_PROVIDER=ollama)")
        return OllamaAdapter()

    if provider_choice == "gemini":
        if not resolved_key:
            raise ValueError("LLM_PROVIDER=gemini requires GEMINI_API_KEY to be set.")
        model_name = os.environ.get("GEMINI_MODEL", "gemini-3-flash-preview")
        logger.info("Using GeminiAdapter (model=%s)", model_name)
        return GeminiAdapter(api_key=resolved_key, model_name=model_name)

    # Auto-detect mode
    if resolved_key:
        model_name = os.environ.get("GEMINI_MODEL", "gemini-3-flash-preview")
        logger.info("Auto-detected GEMINI_API_KEY; using singleton GeminiAdapter (model=%s)", model_name)
        return GeminiAdapter(api_key=resolved_key, model_name=model_name)

    logger.info("No GEMINI_API_KEY found; auto-falling back to local OllamaAdapter")
    return OllamaAdapter()


def declare_durable_topology(
    channel,
    queue_name: str,
    dlx_name: str = DEFAULT_DLX_NAME,
    dlq_name: str = DEFAULT_DLQ_NAME,
):
    """
    Declares the durable RabbitMQ topology:
    1. Durable Dead-Letter Exchange (marketecho.dlx)
    2. Durable Dead-Letter Queue (news_articles_dlq) bound to marketecho.dlx
    3. Durable primary queue wired with x-dead-letter-exchange and x-dead-letter-routing-key
    """
    channel.exchange_declare(
        exchange=dlx_name,
        exchange_type="direct",
        durable=True,
    )
    channel.queue_declare(
        queue=dlq_name,
        durable=True,
    )
    channel.queue_bind(
        queue=dlq_name,
        exchange=dlx_name,
        routing_key=dlq_name,
    )
    channel.queue_declare(
        queue=queue_name,
        durable=True,
        arguments={
            "x-dead-letter-exchange": dlx_name,
            "x-dead-letter-routing-key": dlq_name,
        },
    )


def call_gemini_api(
    gemini_api_key: str,
    prompt: str,
    news_data: str,
    inference_adapter: InferencePort | None = None,
) -> MarketSignalSchema:
    """
    Extracts structured MarketSignalSchema using the singleton InferencePort adapter.
    Retained with backward-compatible signature for existing E2E tests.
    """
    adapter = inference_adapter or create_inference_adapter(gemini_api_key)
    start_time = time.perf_counter()
    try:
        return adapter.extract_signal(f"{prompt}\n\n{news_data}")
    finally:
        LLM_INFERENCE_DURATION_SECONDS.observe(time.perf_counter() - start_time)


def rabbit_mq_listener_setup(
    gemini_api_key: str | None = None,
    inference_adapter: InferencePort | None = None,
    sink_adapter: SinkPort | None = None,
    semantic_cache: SemanticCachePort | None = None,
):
    config_dir = os.environ.get("CONFIG_DIR")
    if not config_dir:
        config_dir = "configs" if os.path.exists("configs/rabbit_mq.json") else "../configs"
    config_path = os.path.join(config_dir, "rabbit_mq.json")

    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    queue_name = config.get("queue_name", "news_articles_queue")
    dlx_name = config.get("dlx_name", DEFAULT_DLX_NAME)
    dlq_name = config.get("dlq_name", DEFAULT_DLQ_NAME)

    # Start Prometheus /metrics HTTP server
    start_metrics_server()

    # Initialize singleton InferencePort, SemanticCachePort, and SinkPort once at startup
    active_inference = inference_adapter or create_inference_adapter(gemini_api_key)
    active_sink = sink_adapter or create_sink_from_env()
    active_cache = semantic_cache or create_semantic_cache_from_env()
    cache_threshold = float(os.environ.get("L2_CACHE_SIMILARITY_THRESHOLD", "0.92"))

    rmq_user = os.environ.get("RABBITMQ_USER", "guest")
    rmq_pass = os.environ.get("RABBITMQ_PASSWORD", "guest")
    rmq_host = os.environ.get("RABBITMQ_HOST", "localhost")
    rmq_port = int(os.environ.get("RABBITMQ_PORT", 5672))

    credentials = pika.PlainCredentials(rmq_user, rmq_pass)
    connection_params = pika.ConnectionParameters(
        host=rmq_host,
        port=rmq_port,
        credentials=credentials,
    )

    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()

    declare_durable_topology(
        channel,
        queue_name=queue_name,
        dlx_name=dlx_name,
        dlq_name=dlq_name,
    )

    channel.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        print(" [x] Received a message")
        ARTICLES_CONSUMED_TOTAL.inc()

        article = article_pb2.Article()
        try:
            article.ParseFromString(body)
            if not article.title and not article.content:
                raise ValueError("Empty protobuf Article payload")
        except Exception as e:
            DLQ_MESSAGES_TOTAL.labels(reason="parse_error").inc()
            logger.error("Failed to parse protobuf message (routing to DLQ '%s'): %s", dlq_name, e)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        print(f"     Title: {article.title}")

        semantic_query_text = f"{article.title}\n{article.description or article.content}".strip()
        news_data_text = (
            f"Title: {article.title}\n"
            f"Source: {article.source}\n"
            f"Description: {article.description}\n"
            f"Content: {article.content}"
        )

        try:
            # 1. Check L2 Semantic Vector Cache first
            cached_signal = active_cache.lookup(semantic_query_text, threshold=cache_threshold)
            if cached_signal is not None:
                L2_SEMANTIC_CACHE_HITS_TOTAL.inc()
                pb_signal = active_sink.save(
                    article=article,
                    signal=cached_signal,
                    semantic_cache_hit=True,
                )
                logger.info(
                    "L2 Semantic Cache HIT for article_id=%s companies=%s tickers=%s indices=%s sentiment=%s",
                    pb_signal.article_id,
                    list(pb_signal.companies),
                    list(pb_signal.tickers),
                    list(pb_signal.index_identifiers),
                    cached_signal.sentiment.value,
                )
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            # 2. On L2 Cache Miss -> Call LLM
            L2_SEMANTIC_CACHE_MISSES_TOTAL.inc()
            signal = call_gemini_api(
                gemini_api_key or "",
                "Extract structured market signal from the following news data:",
                news_data_text,
                inference_adapter=active_inference,
            )
            if isinstance(signal, MarketSignalSchema):
                active_cache.store(semantic_query_text, signal)
                pb_signal = active_sink.save(
                    article=article,
                    signal=signal,
                    semantic_cache_hit=False,
                )
                logger.info(
                    "Persisted MarketSignal for article_id=%s companies=%s tickers=%s indices=%s sentiment=%s score=%.2f",
                    pb_signal.article_id,
                    list(pb_signal.companies),
                    list(pb_signal.tickers),
                    list(pb_signal.index_identifiers),
                    signal.sentiment.value,
                    pb_signal.sentiment_score,
                )
        except Exception as e:
            DLQ_MESSAGES_TOTAL.labels(reason="inference_error").inc()
            logger.error("Inference/Sink failure for article '%s' (routing to DLQ): %s", article.title, e)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=queue_name, on_message_callback=callback)

    print(" [*] Waiting for news data. To exit press CTRL+C")
    channel.start_consuming()


def main():
    api_key = get_gemini_api_key()
    rabbit_mq_listener_setup(api_key)


if __name__ == "__main__":
    main()

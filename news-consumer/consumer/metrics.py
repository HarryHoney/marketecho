from __future__ import annotations

import logging
import os
from prometheus_client import Counter, Histogram, start_http_server

logger = logging.getLogger(__name__)

ARTICLES_CONSUMED_TOTAL = Counter(
    "articles_consumed_total",
    "Total number of articles received by the Python consumer from RabbitMQ",
)

L2_SEMANTIC_CACHE_HITS_TOTAL = Counter(
    "l2_semantic_cache_hits_total",
    "Total number of L2 vector semantic cache hits (short-circuiting LLM inference)",
)

L2_SEMANTIC_CACHE_MISSES_TOTAL = Counter(
    "l2_semantic_cache_misses_total",
    "Total number of L2 vector semantic cache misses requiring LLM inference",
)

LLM_INFERENCE_DURATION_SECONDS = Histogram(
    "llm_inference_duration_seconds",
    "Latency of LLM market signal extraction in seconds",
    buckets=(0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0),
)

DLQ_MESSAGES_TOTAL = Counter(
    "dlq_messages_total",
    "Total number of messages rejected and routed to the Dead-Letter Queue (news_articles_dlq)",
    ["reason"],
)

_metrics_server_started = False


def start_metrics_server(port: int | None = None) -> None:
    """Starts the Prometheus /metrics HTTP server on the configured port (default 8000)."""
    global _metrics_server_started
    if _metrics_server_started:
        return
    resolved_port = port if port is not None else int(os.environ.get("CONSUMER_METRICS_PORT", "8000"))
    try:
        start_http_server(resolved_port)
        _metrics_server_started = True
        logger.info("Prometheus metrics server listening on http://0.0.0.0:%d/metrics", resolved_port)
    except OSError as exc:
        logger.warning("Could not bind Prometheus metrics server on port %d: %s", resolved_port, exc)

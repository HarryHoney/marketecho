# MarketEcho: Distributed Architecture & Implementation Blueprint

## 1. Executive Summary & Engineering Bar
**MarketEcho** is an event-driven, polyglot (Go + Python + Protobuf) real-time financial news ingestion and AI market-signal extraction pipeline.
- **Core Architectural Pattern:** Hexagonal Architecture (Ports and Adapters) / Strategy Pattern.
- **Strict Zero-Cost Constraint ($0.00):** Every default component MUST run 100% free locally (Docker Compose, RSS feeds, local CPU embeddings, local Apple Silicon Ollama or Free-Tier Gemini API) while maintaining strict interface boundaries so enterprise/paid providers (Kafka, Vertex AI, Pinecone, Bloomberg/Polygon) can be swapped via configuration with zero business-logic changes.

---

## 2. Current Repository Audit (Tech Debt & Bugs to Fix First)

### A. Repository & Build Hygiene
1. **Committed Binaries:** Remove tracked compiled binaries (`news-fetcher/main`, `news-fetcher/news-fetcher`, `news-fetcher/fetcher_test`).
2. **Broken `.gitignore`:** Current `.gitignore` has a literal `creds\n.env` string on one line. Fix with proper line breaks and ignore binaries, `__pycache__/`, `.venv/`, `tests/mock_configs/`, and `.DS_Store`.
3. **Hardcoded Artifacts:** Remove or clean up the commented-out Supabase URL in `dao/golang/news_client.go`.

### B. Go Ingestion Service (`news-fetcher`)
1. **One-Shot Execution Bug:** `internal/fetcher/worker.go` (`StartFetching`) accepts `interval time.Duration` but ignores it, runs `FetchLatest` once, and exits. Fix with a continuous `time.Ticker` loop respecting `ctx.Done()`.
2. **HTTP Context & Timeouts:** `internal/provider/newsdata_io.go` uses `http.Get()` (no timeout, ignores `ctx`). Refactor to `http.NewRequestWithContext(ctx, ...)` using an `*http.Client` with a 10s timeout.
3. **Premature Context Expiry:** `news-fetcher/main.go` creates a 5-second `context.WithTimeout` at startup and reuses it for `ch.PublishWithContext()`, causing timeouts if fetching takes >5s. Create per-operation timeouts instead.
4. **Duplicate Articles:** `configs/news_provider_configs.json` polls overlapping regions (`us,gb,in` and `in`) with no deduplication.

### C. Message Broker (`RabbitMQ`)
1. **Volatile Queues:** Both `news-fetcher/main.go` and `news-consumer/main.py` declare `news_articles_queue` with `durable=False` and non-persistent delivery.
2. **Missing Dead-Letter Queue (DLQ):** `news-consumer/main.py` calls `basic_nack(..., requeue=False)` on parse failure without a Dead-Letter Exchange (`x-dead-letter-exchange`), silently dropping failed messages.

### D. Python Inference Consumer (`news-consumer`)
1. **Per-Message Client Instantiation:** `call_gemini_api()` in `main.py` initializes `genai.Client()` on every single message. Move to a singleton initialized at startup.
2. **Unstructured Text Output:** Currently prints raw `response.text` to stdout. Needs structured JSON schema enforcement (Constrained Decoding) and persistence.
3. **Uncaught API Exceptions:** No retry/backoff or error handling around LLM calls; a `429` or `503` crashes the consumer loop.

---

## 3. High-Level Design (HLD) — Target Architecture
[Free RSS Feeds / NewsData.io]
│
▼ (Go Goroutines + Token Bucket Rate Limiter)
┌─────────────────┐
│  news-fetcher   │◄───► [Redis Stack: L1 SHA-256 Deduplication (SETNX + TTL)]
│   (Golang)      │
└────────┬────────┘
│ Protobuf (Article) + W3C Trace-ID Header
▼
┌─────────────────┐
│    RabbitMQ     │───(Max Retries / Parse Fail)──► [news_articles_dlq (DLQ)]
│ (Durable Queue) │
└────────┬────────┘
│ Prefetch=1 (Fair Dispatch)
▼
┌─────────────────┐
│  news-consumer  │◄───► [Redis Stack: L2 HNSW Vector Semantic Cache]
│    (Python)     │      (Local FastEmbed BAAI/bge-small-en-v1.5)
└────────┬────────┘
│ (On Cache Miss: Constrained Decoding JSON Schema)
▼
┌─────────────────────────────────────────┐
│ Pluggable LLM Inference Backend         │
│ - Default 1: Gemini Flash (Free Tier)   │
│ - Default 2: Local Ollama (Llama/Qwen)  │
└────────┬────────────────────────────────┘
│ Validated MarketSignal (Protobuf / Pydantic)
▼
┌─────────────────┐
│   Sink Store    │ (Local Postgres / SQLite / JSONL + Prometheus Metrics)
└─────────────────┘

---

## 4. Low-Level Design (LLD) — Interfaces & Schemas

### 4.1 Protobuf Schema Evolution (`schema/proto/article.proto`)
Update `article.proto` to include timestamps, URLs, and the output `MarketSignal` message:
```protobuf
syntax = "proto3";
package article;
option go_package = "./news-fetcher/internal/model";

message Article {
  string id = 1;
  string title = 2;
  string description = 3;
  string content = 4;
  string source = 5;
  string url = 6;
  int64 published_at_unix = 7;
  string trace_id = 8;
}

enum SentimentLabel {
  NEUTRAL = 0;
  BULLISH = 1;
  BEARISH = 2;
}

message MarketSignal {
  string article_id = 1;
  repeated string tickers = 2;
  SentimentLabel sentiment = 3;
  float sentiment_score = 4; // -1.0 to 1.0
  float confidence = 5;      // 0.0 to 1.0
  string reasoning = 6;
  bool semantic_cache_hit = 7;
  int64 processed_at_unix = 8;
}

4.2 Go Ports & Adapters (news-fetcher)
internal/provider/provider.go (NewsProvider Port):

Name() string

FetchLatest(ctx context.Context) ([]*model.Article, error)

Adapters to build:

RSSProvider: Uses mmcdole/gofeed (or stdlib encoding/xml) to parse free, high-volume financial RSS feeds (Yahoo Finance, CNBC, SEC filings, CoinDesk) with zero API key cost.

NewsDataIO: Refactored with http.Client timeout, http.NewRequestWithContext, and golang.org/x/time/rate limiter.

internal/dedup/deduplicator.go (Deduplicator Port):

IsDuplicate(ctx context.Context, key string) (bool, error)

Adapters: RedisDeduplicator (uses SETNX with 24h TTL on SHA-256 of normalized Title+URL) and InMemoryDeduplicator (fallback for unit tests).

internal/publisher/publisher.go (EventPublisher Port):

Publish(ctx context.Context, article *model.Article) error

Close() error

Adapter: RabbitMQPublisher declaring durable queues, Dead-Letter Exchange (marketecho.dlx), and persistent delivery mode.

4.3 Python Ports & Adapters (news-consumer)
Organize news-consumer/ into modular packages (consumer/ports/, consumer/adapters/, consumer/domain/):

domain/models.py:

Pydantic MarketSignalSchema matching MarketSignal proto for Constrained Decoding.

ports/cache.py (SemanticCachePort Protocol):

lookup(text: str, threshold: float = 0.92) -> MarketSignalSchema | None

store(text: str, signal: MarketSignalSchema) -> None

Adapter (adapters/redis_vector_cache.py): Uses local CPU embedding (fastembed with BAAI/bge-small-en-v1.5, 384 dims, $0 cost) + Redis Stack HNSW vector index (or cosine similarity over Redis hashes) to short-circuit duplicate breaking news across sources.

ports/inference.py (InferencePort Protocol):

extract_signal(article_text: str) -> MarketSignalSchema

Adapters:

GeminiAdapter: Singleton genai.Client, response_mime_type="application/json", response_schema=MarketSignalSchema, exponential backoff with jitter on 429/503.

OllamaAdapter: Local HTTP call to http://host.docker.internal:11434/api/chat with structured JSON format (format=MarketSignalSchema.model_json_schema()).

5. Phased Implementation Plan (For Antigravity Execution)
Execute the refactoring in the following sequential phases. After each phase, run unit/integration tests and wait for review before proceeding.

Phase 1: Repo Hygiene, Protobuf Update & Go Continuous Streaming

Clean .gitignore, remove committed binaries, update article.proto, and regenerate Go & Python protobuf files via Makefile.

Refactor news-fetcher with http.Client timeouts, continuous time.Ticker polling, graceful SIGTERM/SIGINT shutdown, and the free RSSProvider adapter.

Add unit tests for RSSProvider and NewsDataIO.

Phase 2: Durable Broker Topology (DLQ) & Redis L1 Deduplication

Add redis/redis-stack-server:latest to docker-compose.yml.

Implement Deduplicator interface in Go (Redis SETNX + in-memory test mock).

Upgrade RabbitMQ declarations in both Go and Python to use durable=True, persistent messages, and a Dead-Letter Queue (news_articles_dlq).

Phase 3: Python Hexagonal Refactor, Constrained Decoding & Pluggable LLMs

Refactor news-consumer into domain, ports, and adapters.

Implement GeminiAdapter (singleton client + structured Pydantic output + retry with exponential backoff) and OllamaAdapter (local free LLM fallback).

Add unit tests mocking InferencePort.

Phase 4: L2 Semantic Vector Cache & Prometheus Observability

Implement SemanticCachePort using local CPU embeddings (fastembed) + Redis vector similarity search.

Expose /metrics via prometheus_client (Python) and promhttp (Go) tracking articles_ingested_total, l1_dedup_hits_total, l2_semantic_cache_hits_total, llm_inference_duration_seconds, and dlq_messages_total.

Update tests/e2e_test.py and tests/test_consumer_e2e.py to validate the full pipeline end-to-end.

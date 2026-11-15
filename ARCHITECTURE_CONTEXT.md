# MarketEcho: Distributed Architecture & Implementation Blueprint

## 1. Executive Summary & Engineering Bar
**MarketEcho** is an event-driven, polyglot (**Go + Python + Protobuf**) real-time financial news ingestion and AI market-signal extraction pipeline.
- **Core Architectural Pattern:** Hexagonal Architecture (Ports and Adapters) / Strategy Pattern.
- **Strict Zero-Cost Constraint ($0.00):** Every default component runs 100% free locally (Docker Compose, free Yahoo Finance / CNBC RSS feeds, local CPU embeddings via `fastembed`, Free-Tier Gemini 3 Flash or local Apple Silicon Ollama, and local SQLite persistence) while maintaining strict interface boundaries so enterprise/paid providers (Kafka, Vertex AI, Pinecone, Postgres/ClickHouse) can be swapped via configuration with zero business-logic changes.

---

## 2. Resolved Repository Audit (Tech Debt & Bugs Fixed)

### A. Repository & Build Hygiene (Resolved in Phase 1)
1. **Committed Binaries Removed:** Untracked and deleted compiled Go binaries (`news-fetcher/main`, `news-fetcher/news-fetcher`, `news-fetcher/fetcher_test`).
2. **Clean `.gitignore`:** Proper rules for Go binaries, Python caches/venvs (`__pycache__/`, `.venv/`), test mock configs (`tests/mock_configs/`), runtime databases (`*.db`, `*.jsonl`, `data/`), credentials (`creds/`, `.env`), and `.DS_Store`.
3. **Hardcoded Artifacts Removed:** Cleaned up legacy commented-out Supabase client in `dao/golang/news_client.go`.

### B. Go Ingestion Service (`news-fetcher`) (Resolved in Phases 1 & 2)
1. **Continuous Streaming Loop:** `internal/fetcher/worker.go` (`StartFetching`) executes an immediate startup fetch followed by a continuous `time.NewTicker(interval)` loop respecting `ctx.Done()`.
2. **HTTP Context & Timeouts:** Both `RSSProvider` (`internal/provider/rss.go`) and `NewsDataIO` (`internal/provider/newsdata_io.go`) use `http.NewRequestWithContext(ctx, ...)` with a 10-second `*http.Client` timeout.
3. **Free-Tier Content Fallback:** `NewsDataIO` automatically falls back to `description` (or `title`) when the free-tier API returns `"ONLY AVAILABLE IN PAID PLANS"` or empty `content`.
4. **Per-Operation Publish Timeouts & Graceful Shutdown:** `news-fetcher/main.go` uses `signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)` and per-message 5-second `PublishWithContext` timeouts.
5. **Hybrid L1 Deduplication:** Eliminates duplicate articles across polling cycles and syndicated wire copies before RabbitMQ using exact **SHA-256** (`Title + URL`) and **64-bit SimHash** (Hamming distance $\le 3$ on content tokens/shingles) with a 24h TTL.

### C. Message Broker (`RabbitMQ`) (Resolved in Phase 2)
1. **Durable Queues & Persistent Messages:** Both Go (`internal/publisher/publisher.go`) and Python (`news-consumer/main.py`) declare `news_articles_queue` with `durable=True` and `DeliveryMode: Persistent (2)`.
2. **Dead-Letter Exchange & Queue (DLQ):** Unparseable or failed messages (`basic_nack(..., requeue=False)`) route automatically to durable exchange `marketecho.dlx` and dead-letter queue `news_articles_dlq`.

### D. Python Inference Consumer (`news-consumer`) (Resolved in Phases 3 & 4)
1. **Singleton LLM Client:** `GeminiAdapter` initializes `genai.Client()` once at startup (defaulting to `gemini-3-flash-preview`), with automatic fallback to local `OllamaAdapter` (`qwen2.5:3b`).
2. **Constrained Decoding & Industry/Sector Edge Cases:** Enforces `MarketSignalSchema` via `response_mime_type="application/json"` and `response_schema=MarketSignalSchema`, extracting `companies`, `tickers`, `sectors`, `index_identifiers`, `sentiment`, `sentiment_score`, `confidence`, and `reasoning`. Handles single-stock, multi-company, generic industry-wide (e.g., Tech/Semiconductors), multi-sector, and macroeconomic news.
3. **Resilience & Backoff:** Exponential backoff with random jitter on HTTP `429`/`503` errors.
4. **L2 Vector Semantic Cache:** Local CPU embeddings (`fastembed` `BAAI/bge-small-en-v1.5`, 384-d) + Redis Stack vector similarity search (cosine similarity $\ge 0.92$, 12h TTL) short-circuit differently framed stories covering the same market event before calling the LLM.

---

## 3. High-Level Design (HLD) — Implemented Architecture

```text
[Free RSS Feeds (Yahoo Finance, CNBC) + NewsData.io API]
│
▼ (Go Goroutines + Continuous time.Ticker + 10s HTTP Timeout)
┌─────────────────┐
│  news-fetcher   │◄───► [Redis Stack L1 Cache (24h TTL, 256MB volatile-lru cap)]
│   (Golang)      │      1. Exact SHA-256 (Normalized Title + URL via SETNX)
│  :2112/metrics  │      2. Near-Duplicate 64-bit SimHash (Hamming Distance <= 3)
└────────┬────────┘
│ Protobuf (Article) + W3C traceparent Header + Persistent Delivery
▼
┌─────────────────┐
│    RabbitMQ     │───(Parse Fail / Unrecoverable Error)──► [marketecho.dlx]
│ (Durable Queue) │                                                │
└────────┬────────┘                                                ▼
│ Prefetch=1 (Fair Dispatch)                            [news_articles_dlq (DLQ)]
▼
┌─────────────────┐
│  news-consumer  │◄───► [Redis Stack L2 Vector Semantic Cache (12h TTL)]
│    (Python)     │      Local CPU FastEmbed (BAAI/bge-small-en-v1.5, 384 dims)
│  :8000/metrics  │      Cosine Similarity >= 0.92 -> Instant Cache HIT (~5ms, $0)
└────────┬────────┘
│ (On L2 Cache Miss: Constrained Decoding JSON Schema)
▼
┌──────────────────────────────────────────────────────────┐
│ Pluggable LLM Inference Backend (`InferencePort`)        │
│ - Auto Default 1: Gemini 3 Flash (`gemini-3-flash-preview`) │
│ - Auto Default 2: Local Ollama (`qwen2.5:3b`)            │
└────────┬─────────────────────────────────────────────────┘
│ Validated MarketSignal (Companies, Sectors, Tickers, Indices, Sentiment)
▼
┌──────────────────────────────────────────────────────────┐
│ Local SQLite Sink Store (`SinkPort`)                     │
│ - `news-consumer/data/market_signals.db`                 │
│ - Query CLI: `python3 news-consumer/query_signals.py N`  │
└──────────────────────────────────────────────────────────┘
```

---

## 4. Low-Level Design (LLD) — Interfaces, Schemas & Storage

### 4.1 Protobuf Schema (`schema/proto/article.proto`)
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

message ArticleList {
  repeated Article articles = 1;
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
  repeated string companies = 9;
  repeated string index_identifiers = 10;
  repeated string sectors = 11;
}
```

### 4.2 Go Ports & Adapters (`news-fetcher`)
- **`internal/provider/provider.go` (`NewsProvider` Port):**
  - `Name() string`
  - `FetchLatest(ctx context.Context) ([]*model.Article, error)`
  - **Adapters:** `RSSProvider` (`internal/provider/rss.go` using stdlib `encoding/xml` for Yahoo Finance & CNBC) and `NewsDataIO` (`internal/provider/newsdata_io.go`).
- **`internal/dedup/deduplicator.go` (`Deduplicator` Port):**
  - `IsDuplicate(ctx context.Context, key string) (bool, error)`
  - `IsArticleDuplicate(ctx context.Context, article *model.Article) (bool, string, error)`
  - **Adapters:** `RedisDeduplicator` (`internal/dedup/redis.go`, 24h TTL via `L1_DEDUP_TTL_SECONDS=86400`) and `InMemoryDeduplicator` (`internal/dedup/memory.go`, fallback & unit test mock).
- **`internal/publisher/publisher.go` (`EventPublisher` Port):**
  - `Publish(ctx context.Context, article *model.Article) error`
  - `Close() error`
  - **Adapter:** `RabbitMQPublisher` declaring durable `marketecho.dlx`, `news_articles_dlq`, and `news_articles_queue` with W3C `traceparent` headers.
- **`internal/metrics/metrics.go`:**
  - Exposes `/metrics` on `:2112` tracking `articles_ingested_total`, `l1_dedup_hits_total{reason="sha256|simhash"}`, and `publish_errors_total`.

### 4.3 Python Ports & Adapters (`news-consumer`)
- **`consumer/domain/models.py` (`MarketSignalSchema`):**
  - Fields: `companies`, `tickers`, `sectors`, `index_identifiers`, `sentiment`, `sentiment_score`, `confidence`, `reasoning`.
  - **Generic Industry & Multi-Sector Handling:**
    - When news applies to an entire industry (e.g., Tech, Semiconductors) or multiple industries (e.g., Energy + Airlines + Retail) with no specific company named, `companies` stays `[]`, `company_name` formats as `"INDUSTRY-WIDE (<sectors>)"`, `tickers` captures sector ETFs (`XLK`, `SMH`, `JETS`, `XLE`), and `index_identifiers` maps to benchmark indices (`NASDAQ-100`, `S&P 500 Information Technology`, `SOX`, `S&P 500`, `NIFTY 50`) with deterministic fallback mapping via `SECTOR_TO_DEFAULT_INDICES`.
- **`consumer/ports/cache.py` & `consumer/adapters/redis_vector_cache.py` (`SemanticCachePort`):**
  - `FastEmbedder` (`BAAI/bge-small-en-v1.5`, 384-d unit vectors).
  - `RedisVectorSemanticCache` (12h default TTL via `L2_CACHE_TTL_SECONDS=43200`, cosine similarity threshold `0.92`, RediSearch HNSW index `idx:marketecho_l2_vec` + hash scan fallback) and `InMemoryVectorSemanticCache`.
- **`consumer/ports/inference.py` (`InferencePort`):**
  - `GeminiAdapter` (`consumer/adapters/gemini_adapter.py`) and `OllamaAdapter` (`consumer/adapters/ollama_adapter.py`).
- **`consumer/ports/sink.py` & `consumer/adapters/sink_adapter.py` (`SinkPort`):**
  - Defaults to `SQLiteSinkAdapter` (`news-consumer/data/market_signals.db`, `SINK_BACKEND=sqlite`), storing `company_name`, `companies_json`, `sector`, `sectors_json`, `tickers_json`, `index_identifier`, `index_identifiers_json`, `sentiment`, `sentiment_score`, `confidence`, `reasoning`, `semantic_cache_hit`, and `processed_at_unix`.
- **`consumer/metrics.py`:**
  - Exposes `/metrics` on `:8000` tracking `articles_consumed_total`, `l2_semantic_cache_hits_total`, `l2_semantic_cache_misses_total`, `llm_inference_duration_seconds`, and `dlq_messages_total`.

---

## 5. Cache Lifecycle & Memory Management (Redis Stack)
- **Container:** `marketecho-redis` (`redis/redis-stack-server:latest`) configured with `--maxmemory 256mb --maxmemory-policy volatile-lru`.
- **Storage Location:** Lives in container RAM (with periodic RDB snapshots at `/data/dump.rdb` inside Docker's internal virtual filesystem).
- **TTLs:**
  - **L1 Ingestion Cache:** `24 hours` (`L1_DEDUP_TTL_SECONDS=86400`, `< 1 MB` RAM for 10k articles/day).
  - **L2 Vector Semantic Cache:** `12 hours` (`L2_CACHE_TTL_SECONDS=43200`, `~4 MB` RAM for 2k vectors).
- **Manual Cache Flush (for testing):**
  ```bash
  docker exec -it marketecho-redis redis-cli flushall
  ```

---

## 6. Developer Operations & Verification Runbook

### 1. Start Infrastructure (RabbitMQ + Redis Stack)
```bash
open -a Docker
docker compose up -d rabbitmq redis
```

### 2. Run Producer & Consumer Locally
- **Terminal 1 (Go Fetcher):**
  ```bash
  make run-news-fetcher
  ```
- **Terminal 2 (Python Consumer):**
  ```bash
  cd news-consumer && .venv/bin/python main.py
  ```

### 3. Query Last N Extracted Signals from SQLite
```bash
# Show last 5 signals (formatted report)
python3 news-consumer/query_signals.py 5
# Or via Makefile:
make signals N=5

# Output as JSON
python3 news-consumer/query_signals.py 10 --json
```

### 4. Inspect Prometheus Metrics & RabbitMQ UI
- **Go Fetcher Metrics:** `http://localhost:2112/metrics`
- **Python Consumer Metrics:** `http://localhost:8000/metrics`
- **RabbitMQ Management UI:** `http://localhost:15672` (`guest` / `guest`)

### 5. Run Full Automated Test Suite (Go + Python + E2E)
```bash
make test
```

---

## 7. Implementation Status
- [x] **Phase 1:** Repo Hygiene, Protobuf Evolution & Go Continuous Streaming (`RSSProvider` + `NewsDataIO`)
- [x] **Phase 2:** Durable Broker Topology (`marketecho.dlx` / `news_articles_dlq`) & Redis Hybrid L1 Deduplication (`SHA-256` + `64-bit SimHash`)
- [x] **Phase 3:** Python Hexagonal Refactor, Constrained Decoding & Pluggable LLMs (`GeminiAdapter` + `OllamaAdapter`)
- [x] **Phase 4:** L2 Semantic Vector Cache (`FastEmbed` + Redis), Company/Sector/Index Extraction, SQLite CLI Inspector & Prometheus Observability
- [x] **Phase 5:** End-of-Day (EOD) Stock Performance & Signal Validation Service (`MarketDataPort`, `YahooFinanceMarketDataAdapter`, `evaluate_eod.py`, and SQLite EOD evaluation columns)

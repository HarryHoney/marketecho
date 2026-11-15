from __future__ import annotations

import hashlib
import logging
import os
import threading
import time
from typing import Callable, Protocol

import numpy as np
import redis

from consumer.domain.models import MarketSignalSchema

logger = logging.getLogger(__name__)

EMBEDDING_DIM = 384
DEFAULT_MODEL_NAME = "BAAI/bge-small-en-v1.5"
DEFAULT_L2_TTL_SECONDS = 43200  # 12 hours
REDIS_KEY_PREFIX = "cache:l2:vec:"
REDIS_INDEX_NAME = "idx:marketecho_l2_vec"


class TextEmbedder(Protocol):
    """Protocol for converting text into a normalized 1-D float32 numpy vector."""

    def embed(self, text: str) -> np.ndarray:
        ...


class FastEmbedder:
    """
    Local CPU embedding wrapper using FastEmbed (BAAI/bge-small-en-v1.5, 384 dims, $0 cost).
    Lazily initializes the ONNX model as a singleton.
    """

    _instance_lock = threading.Lock()
    _shared_model = None

    def __init__(self, model_name: str = DEFAULT_MODEL_NAME) -> None:
        self.model_name = model_name

    def _get_model(self):
        if FastEmbedder._shared_model is None:
            with FastEmbedder._instance_lock:
                if FastEmbedder._shared_model is None:
                    from fastembed import TextEmbedding

                    logger.info("Initializing local CPU FastEmbed model: %s", self.model_name)
                    FastEmbedder._shared_model = TextEmbedding(model_name=self.model_name)
        return FastEmbedder._shared_model

    def embed(self, text: str) -> np.ndarray:
        model = self._get_model()
        vec = next(iter(model.embed([text])))
        arr = np.asarray(vec, dtype=np.float32)
        norm = float(np.linalg.norm(arr))
        if norm > 0.0:
            arr = arr / norm
        return arr


class InMemoryVectorSemanticCache:
    """
    Thread-safe in-memory L2 semantic vector cache with TTL expiration and cosine similarity.
    Used for unit tests and as an automatic fallback when Redis is unreachable.
    """

    def __init__(
        self,
        embedder: TextEmbedder | None = None,
        ttl_seconds: int = DEFAULT_L2_TTL_SECONDS,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.embedder: TextEmbedder = embedder or FastEmbedder()
        self.ttl_seconds = max(1, int(ttl_seconds))
        self._clock = clock
        self._lock = threading.Lock()
        # List of tuples: (key_hash, unit_vector, MarketSignalSchema, expires_at)
        self._entries: list[tuple[str, np.ndarray, MarketSignalSchema, float]] = []

    def lookup(self, text: str, threshold: float = 0.92) -> MarketSignalSchema | None:
        cleaned = (text or "").strip()
        if not cleaned:
            return None

        query_vec = self._normalize(self.embedder.embed(cleaned))
        now = self._clock()

        with self._lock:
            self._evict_expired_locked(now)
            if not self._entries:
                return None

            best_score = -1.0
            best_signal: MarketSignalSchema | None = None
            for _, stored_vec, signal, _ in self._entries:
                score = float(np.dot(query_vec, stored_vec))
                if score > best_score:
                    best_score = score
                    best_signal = signal

            if best_signal is not None and best_score >= threshold:
                logger.info(
                    "L2 InMemory Semantic Cache HIT (cosine_similarity=%.4f >= %.2f)",
                    best_score,
                    threshold,
                )
                return best_signal.model_copy(deep=True)

        return None

    def store(self, text: str, signal: MarketSignalSchema) -> None:
        cleaned = (text or "").strip()
        if not cleaned:
            return

        vec = self._normalize(self.embedder.embed(cleaned))
        key_hash = hashlib.sha256(cleaned.encode("utf-8")).hexdigest()
        now = self._clock()
        expires_at = now + self.ttl_seconds

        with self._lock:
            self._evict_expired_locked(now)
            self._entries = [e for e in self._entries if e[0] != key_hash]
            self._entries.append((key_hash, vec, signal.model_copy(deep=True), expires_at))

    def _evict_expired_locked(self, now: float) -> None:
        self._entries = [e for e in self._entries if now < e[3]]

    @staticmethod
    def _normalize(vec: np.ndarray) -> np.ndarray:
        arr = np.asarray(vec, dtype=np.float32)
        norm = float(np.linalg.norm(arr))
        if norm > 0.0:
            arr = arr / norm
        return arr


class RedisVectorSemanticCache:
    """
    Redis Stack L2 Semantic Vector Cache implementing SemanticCachePort.
    - Stores 384-d float32 embeddings + MarketSignalSchema JSON in Redis Hashes with 12h TTL.
    - Uses RediSearch HNSW vector index (FT.CREATE / FT.SEARCH) when available,
      and transparently supports exact cosine scan over active `cache:l2:vec:*` hashes.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        embedder: TextEmbedder | None = None,
        ttl_seconds: int = DEFAULT_L2_TTL_SECONDS,
        dim: int = EMBEDDING_DIM,
    ) -> None:
        self.redis = redis_client
        self.embedder: TextEmbedder = embedder or FastEmbedder()
        self.ttl_seconds = max(1, int(ttl_seconds))
        self.dim = dim
        self._ft_available = False
        self._ensure_hnsw_index()

    def _ensure_hnsw_index(self) -> None:
        """Creates the RediSearch HNSW vector index on `cache:l2:vec:*` if RediSearch is loaded."""
        try:
            self.redis.execute_command("FT.INFO", REDIS_INDEX_NAME)
            self._ft_available = True
            return
        except Exception:
            pass

        try:
            self.redis.execute_command(
                "FT.CREATE",
                REDIS_INDEX_NAME,
                "ON",
                "HASH",
                "PREFIX",
                "1",
                REDIS_KEY_PREFIX,
                "SCHEMA",
                "embedding",
                "VECTOR",
                "HNSW",
                "6",
                "TYPE",
                "FLOAT32",
                "DIM",
                str(self.dim),
                "DISTANCE_METRIC",
                "COSINE",
                "signal_json",
                "TEXT",
            )
            self._ft_available = True
            logger.info("Created Redis Stack HNSW vector index: %s", REDIS_INDEX_NAME)
        except Exception as exc:
            self._ft_available = False
            logger.debug("RediSearch HNSW index setup skipped (using Redis hash vector scan): %s", exc)

    def lookup(self, text: str, threshold: float = 0.92) -> MarketSignalSchema | None:
        cleaned = (text or "").strip()
        if not cleaned:
            return None

        query_vec = InMemoryVectorSemanticCache._normalize(self.embedder.embed(cleaned))

        # 1. Try RediSearch HNSW KNN query if available
        if self._ft_available:
            try:
                res = self.redis.execute_command(
                    "FT.SEARCH",
                    REDIS_INDEX_NAME,
                    "*=>[KNN 1 @embedding $vec AS vector_distance]",
                    "PARAMS",
                    "2",
                    "vec",
                    query_vec.tobytes(),
                    "SORTBY",
                    "vector_distance",
                    "ASC",
                    "RETURN",
                    "2",
                    "vector_distance",
                    "signal_json",
                    "DIALECT",
                    "2",
                )
                if isinstance(res, list) and len(res) >= 3 and res[0] >= 1:
                    fields = res[2]
                    field_map = {}
                    for i in range(0, len(fields), 2):
                        k = fields[i].decode("utf-8") if isinstance(fields[i], bytes) else str(fields[i])
                        v = fields[i + 1].decode("utf-8") if isinstance(fields[i + 1], bytes) else str(fields[i + 1])
                        field_map[k] = v
                    dist = float(field_map.get("vector_distance", "1.0"))
                    similarity = 1.0 - dist
                    if similarity >= threshold and "signal_json" in field_map:
                        logger.info(
                            "L2 Redis HNSW Vector Cache HIT (cosine_similarity=%.4f >= %.2f)",
                            similarity,
                            threshold,
                        )
                        return MarketSignalSchema.model_validate_json(field_map["signal_json"])
            except Exception as exc:
                logger.debug("HNSW FT.SEARCH fallback to hash scan: %s", exc)

        # 2. Vectorized cosine similarity scan over active Redis L2 hashes
        best_score = -1.0
        best_json: str | None = None
        for key in self.redis.scan_iter(match=f"{REDIS_KEY_PREFIX}*", count=100):
            raw_emb = self.redis.hget(key, "embedding")
            raw_json = self.redis.hget(key, "signal_json")
            if not raw_emb or not raw_json:
                continue
            stored_vec = np.frombuffer(raw_emb, dtype=np.float32)
            if stored_vec.shape != query_vec.shape:
                continue
            score = float(np.dot(query_vec, stored_vec))
            if score > best_score:
                best_score = score
                best_json = raw_json.decode("utf-8") if isinstance(raw_json, bytes) else str(raw_json)

        if best_json is not None and best_score >= threshold:
            logger.info(
                "L2 Redis Vector Cache HIT (cosine_similarity=%.4f >= %.2f)",
                best_score,
                threshold,
            )
            return MarketSignalSchema.model_validate_json(best_json)

        return None

    def store(self, text: str, signal: MarketSignalSchema) -> None:
        cleaned = (text or "").strip()
        if not cleaned:
            return

        vec = InMemoryVectorSemanticCache._normalize(self.embedder.embed(cleaned))
        key_hash = hashlib.sha256(cleaned.encode("utf-8")).hexdigest()
        redis_key = f"{REDIS_KEY_PREFIX}{key_hash}"

        pipe = self.redis.pipeline()
        pipe.hset(
            redis_key,
            mapping={
                "embedding": vec.tobytes(),
                "signal_json": signal.model_dump_json(),
            },
        )
        pipe.expire(redis_key, self.ttl_seconds)
        pipe.execute()


def create_semantic_cache_from_env(embedder: TextEmbedder | None = None):
    """
    Factory that connects to Redis Stack for L2 vector caching (12h default TTL),
    or falls back cleanly to InMemoryVectorSemanticCache if Redis is unreachable.
    """
    ttl_seconds = int(os.environ.get("L2_CACHE_TTL_SECONDS", str(DEFAULT_L2_TTL_SECONDS)))
    redis_host = os.environ.get("REDIS_HOST", "localhost")
    redis_port = int(os.environ.get("REDIS_PORT", "6379"))

    try:
        client = redis.Redis(
            host=redis_host,
            port=redis_port,
            socket_connect_timeout=2.0,
            socket_timeout=2.0,
        )
        client.ping()
        logger.info(
            "Connected to Redis L2 Semantic Vector Cache (%s:%d, TTL=%ds)",
            redis_host,
            redis_port,
            ttl_seconds,
        )
        return RedisVectorSemanticCache(
            redis_client=client,
            embedder=embedder,
            ttl_seconds=ttl_seconds,
        )
    except Exception as exc:
        logger.warning(
            "Redis unavailable for L2 Semantic Cache (%s); falling back to InMemoryVectorSemanticCache (TTL=%ds)",
            exc,
            ttl_seconds,
        )
        return InMemoryVectorSemanticCache(
            embedder=embedder,
            ttl_seconds=ttl_seconds,
        )

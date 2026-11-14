from __future__ import annotations

import logging
import os
import random
import time
from typing import Callable

import httpx

from consumer.domain.models import MarketSignalSchema

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "You are a quantitative financial market analyst. Analyze the provided financial news article "
    "and respond strictly with a JSON object matching the requested schema: "
    "tickers (array of uppercase strings), sentiment (NEUTRAL, BULLISH, or BEARISH), "
    "sentiment_score (-1.0 to 1.0), confidence (0.0 to 1.0), and reasoning (string)."
)


class OllamaAdapter:
    """
    Zero-cost local Ollama inference adapter implementing InferencePort.
    Calls POST /api/chat with structured JSON schema output enforcement
    (format=MarketSignalSchema.model_json_schema()).
    """

    def __init__(
        self,
        base_url: str | None = None,
        model_name: str | None = None,
        timeout_seconds: float = 30.0,
        max_retries: int = 2,
        base_delay_seconds: float = 0.5,
        http_client: httpx.Client | None = None,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        self.base_url = (
            base_url
            or os.environ.get("OLLAMA_BASE_URL")
            or "http://localhost:11434"
        ).rstrip("/")
        self.model_name = (
            model_name
            or os.environ.get("OLLAMA_MODEL")
            or "qwen2.5:3b"
        )
        self.max_retries = max(0, int(max_retries))
        self.base_delay_seconds = float(base_delay_seconds)
        self._sleep = sleep_fn
        self.client: httpx.Client = (
            http_client
            if http_client is not None
            else httpx.Client(timeout=timeout_seconds)
        )

    def extract_signal(self, article_text: str) -> MarketSignalSchema:
        endpoint = f"{self.base_url}/api/chat"
        payload = {
            "model": self.model_name,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"News Article:\n{article_text}"},
            ],
            "stream": False,
            "format": MarketSignalSchema.model_json_schema(),
            "options": {
                "temperature": 0.1,
            },
        }

        last_exc: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                response = self.client.post(endpoint, json=payload)
                if response.status_code in (429, 500, 502, 503, 504):
                    raise httpx.HTTPStatusError(
                        f"Ollama returned transient HTTP {response.status_code}",
                        request=response.request,
                        response=response,
                    )
                response.raise_for_status()
                data = response.json()
                content = (data.get("message") or {}).get("content", "").strip()
                if not content:
                    raise ValueError("Ollama returned empty message.content")
                return MarketSignalSchema.model_validate_json(content)
            except Exception as exc:
                last_exc = exc
                if attempt < self.max_retries and self._is_transient_error(exc):
                    jitter = random.uniform(0.0, 0.2 * self.base_delay_seconds)
                    delay = (self.base_delay_seconds * (2**attempt)) + jitter
                    logger.warning(
                        "Transient Ollama error (attempt %d/%d): %s. Retrying in %.2fs...",
                        attempt + 1,
                        self.max_retries + 1,
                        exc,
                        delay,
                    )
                    self._sleep(delay)
                    continue
                break

        assert last_exc is not None
        raise last_exc

    @staticmethod
    def _is_transient_error(exc: Exception) -> bool:
        if isinstance(exc, (httpx.TimeoutException, httpx.ConnectError)):
            return True
        if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None:
            return exc.response.status_code in (429, 500, 502, 503, 504)
        return False

from __future__ import annotations

import logging
import random
import time
from typing import Callable

from google import genai
from google.genai import types

from consumer.domain.models import MarketSignalSchema

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "You are a quantitative financial market analyst. Analyze the provided financial news article "
    "and extract structured market signals:\n"
    "1. companies: Full names of specific companies mentioned (leave empty [] if the news is generic to an industry or macro).\n"
    "2. tickers: Specific stock/crypto tickers, or sector ETF tickers (like XLK, QQQ, SMH, XLE, XLF) if industry-wide.\n"
    "3. sectors: All affected industries/sectors (e.g. ['Technology', 'Semiconductors'], or multiple industries like ['Energy', 'Airlines', 'Retail']).\n"
    "4. index_identifiers: Benchmark market or sector indices (e.g. 'NASDAQ-100', 'S&P 500 Information Technology', 'SOX', 'S&P 500', 'DJIA', 'NIFTY 50'). "
    "Even when no single stock is mentioned, always map the affected industry or macro theme to its benchmark index identifiers.\n"
    "5. sentiment (NEUTRAL, BULLISH, BEARISH), sentiment_score (-1.0 to 1.0), confidence (0.0 to 1.0), and concise 1-2 sentence reasoning."
)


class GeminiAdapter:
    """
    Google Gemini inference adapter implementing InferencePort.
    - Initializes a singleton genai.Client once per adapter instance.
    - Enforces Constrained Decoding via response_mime_type='application/json' and response_schema=MarketSignalSchema.
    - Retries transient 429 (Rate Limit) and 503/500 errors with exponential backoff and random jitter.
    """

    def __init__(
        self,
        api_key: str,
        model_name: str = "gemini-3-flash-preview",
        max_retries: int = 3,
        base_delay_seconds: float = 1.0,
        client: genai.Client | None = None,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        if not api_key and client is None:
            raise ValueError("GeminiAdapter requires a non-empty GEMINI_API_KEY.")
        self.api_key = api_key
        self.model_name = model_name
        self.max_retries = max(0, int(max_retries))
        self.base_delay_seconds = float(base_delay_seconds)
        self._sleep = sleep_fn
        # Singleton client initialized once at startup
        self.client: genai.Client = client if client is not None else genai.Client(api_key=api_key)

    def extract_signal(self, article_text: str) -> MarketSignalSchema:
        prompt = f"{SYSTEM_PROMPT}\n\nNews Article:\n{article_text}"
        config = types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=MarketSignalSchema,
            temperature=0.1,
        )

        last_exc: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                response = self.client.models.generate_content(
                    model=self.model_name,
                    contents=prompt,
                    config=config,
                )
                if getattr(response, "parsed", None) is not None and isinstance(
                    response.parsed, MarketSignalSchema
                ):
                    return response.parsed

                raw_text = (getattr(response, "text", None) or "").strip()
                if not raw_text:
                    raise ValueError("Gemini returned an empty response body.")
                return MarketSignalSchema.model_validate_json(raw_text)

            except Exception as exc:
                last_exc = exc
                if attempt < self.max_retries and self._is_transient_error(exc):
                    jitter = random.uniform(0.0, 0.25 * self.base_delay_seconds)
                    delay = (self.base_delay_seconds * (2**attempt)) + jitter
                    logger.warning(
                        "Transient Gemini error (attempt %d/%d): %s. Retrying in %.2fs...",
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
        msg = str(exc).lower()
        status_code = getattr(exc, "code", None) or getattr(exc, "status_code", None)
        if status_code in (429, 500, 502, 503, 504):
            return True
        transient_tokens = ("429", "503", "502", "504", "rate limit", "resource_exhausted", "unavailable", "quota", "timeout")
        return any(token in msg for token in transient_tokens)

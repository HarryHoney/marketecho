from __future__ import annotations

import datetime
import json
import logging
import os
import urllib.request
from typing import Any, Sequence

import schema.article_pb2 as article_pb2
from consumer.domain.models import MarketSignalSchema, SentimentEnum

logger = logging.getLogger(__name__)

COLOR_BULLISH = 0x2ECC71  # Emerald Green
COLOR_BEARISH = 0xE74C3C  # Crimson Red
COLOR_SUMMARY = 0x3498DB  # Quantitative Blue


class DiscordWebhookAdapter:
    """
    Zero-cost Discord Webhook adapter implementing NotificationPort.
    - Sends real-time rich embeds for every BULLISH or BEARISH signal.
    - Sends an End-of-Day (EOD) US Market Close Scorecard showing Matched vs. Missed trades.
    """

    def __init__(
        self,
        webhook_url: str | None = None,
        http_client: Any | None = None,
        timeout_seconds: float = 10.0,
        notify_on_cache_hit: bool = False,
    ) -> None:
        raw_url = (
            webhook_url
            if webhook_url is not None
            else os.environ.get("DISCORD_WEBHOOK_URL", "")
        ).strip()
        if raw_url == "your_discord_webhook_url_here":
            raw_url = ""
        self.webhook_url = raw_url
        self.http_client = http_client
        self.timeout_seconds = timeout_seconds
        self.notify_on_cache_hit = notify_on_cache_hit

    @property
    def enabled(self) -> bool:
        return bool(self.webhook_url)

    def notify_signal(
        self,
        article: article_pb2.Article,
        signal: MarketSignalSchema,
        semantic_cache_hit: bool = False,
    ) -> bool:
        if not self.enabled:
            return False

        # Only send alerts on directional BULLISH or BEARISH signals
        if signal.sentiment not in (SentimentEnum.BULLISH, SentimentEnum.BEARISH):
            return False

        if semantic_cache_hit and not self.notify_on_cache_hit:
            return False

        is_bullish = signal.sentiment == SentimentEnum.BULLISH
        emoji = "🟢" if is_bullish else "🔴"
        color = COLOR_BULLISH if is_bullish else COLOR_BEARISH
        primary_targets = (
            ", ".join(signal.tickers)
            if signal.tickers
            else ", ".join(signal.index_identifiers)
        )
        scope_label = signal.display_company_or_scope()
        sectors_label = ", ".join(signal.sectors) if signal.sectors else "General Market"
        indices_label = ", ".join(signal.index_identifiers) if signal.index_identifiers else "S&P 500"

        embed: dict[str, Any] = {
            "title": f"{emoji} {signal.sentiment.value} ({signal.sentiment_score:+.2f}) — {primary_targets}",
            "description": f"**{article.title}**\n\n{signal.reasoning}",
            "color": color,
            "fields": [
                {
                    "name": "Company / Scope",
                    "value": scope_label[:1024],
                    "inline": True,
                },
                {
                    "name": "Sector(s)",
                    "value": sectors_label[:1024],
                    "inline": True,
                },
                {
                    "name": "Tickers / Indices",
                    "value": f"Tickers: `{', '.join(signal.tickers) or 'N/A'}`\nIndices: `{indices_label}`"[:1024],
                    "inline": False,
                },
                {
                    "name": "Confidence & Source",
                    "value": f"`{signal.confidence * 100:.0f}%` | {article.source or 'RSS'}",
                    "inline": True,
                },
            ],
            "footer": {
                "text": f"MarketEcho Real-Time Signal • Trace: {(article.trace_id or 'N/A')[:24]}"
            },
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        if article.url:
            embed["url"] = article.url

        return self._post_payload({"username": "MarketEcho AI", "embeds": [embed]})

    def notify_eod_summary(self, evaluated_records: Sequence[dict]) -> bool:
        if not self.enabled or not evaluated_records:
            return False

        matched: list[str] = []
        missed: list[str] = []
        neutral_or_flat: list[str] = []

        for rec in evaluated_records:
            outcome = rec.get("prediction_outcome", "")
            sym = rec.get("evaluated_symbol", "N/A")
            sent = rec.get("sentiment", "NEUTRAL")
            score = float(rec.get("sentiment_score") or 0.0)
            open_p = float(rec.get("price_open") or 0.0)
            close_p = float(rec.get("eod_close_price") or 0.0)
            pct = float(rec.get("actual_change_pct") or 0.0)
            title = (rec.get("title") or "")[:65]

            line = (
                f"• **`{sym}`** | Pred: `{sent} ({score:+.2f})` → "
                f"Actual: `{open_p:.2f} → {close_p:.2f} ({pct:+.2f}%)` — *{title}...*"
            )
            if outcome == "ACCURATE" and sent in ("BULLISH", "BEARISH"):
                matched.append(line)
            elif outcome == "DIVERGENT":
                missed.append(line)
            else:
                neutral_or_flat.append(line)

        directional_total = len(matched) + len(missed)
        win_rate = (len(matched) / directional_total * 100.0) if directional_total > 0 else 0.0

        matched_text = "\n".join(matched[:8]) if matched else "_No matched directional trades today._"
        missed_text = "\n".join(missed[:8]) if missed else "_No missed directional trades today._"

        embed: dict[str, Any] = {
            "title": (
                f"📊 US Market Close EOD Scorecard — "
                f"Win Rate: {win_rate:.1f}% ({len(matched)}/{directional_total} Directional)"
            ),
            "description": (
                f"Evaluated **{len(evaluated_records)}** total signals at US Market Close.\n"
                f"✅ **Matched:** `{len(matched)}` | ❌ **Missed:** `{len(missed)}` | "
                f"➖ **Neutral / Flat:** `{len(neutral_or_flat)}`"
            ),
            "color": COLOR_BULLISH if win_rate >= 50.0 else COLOR_SUMMARY,
            "fields": [
                {
                    "name": f"✅ Matched Predictions ({len(matched)})",
                    "value": matched_text[:1024],
                    "inline": False,
                },
                {
                    "name": f"❌ Missed / Divergent Predictions ({len(missed)})",
                    "value": missed_text[:1024],
                    "inline": False,
                },
            ],
            "footer": {"text": "MarketEcho EOD Backtest Evaluator (Yahoo Finance EOD)"},
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }

        return self._post_payload({"username": "MarketEcho EOD Scorecard", "embeds": [embed]})

    def _post_payload(self, payload: dict[str, Any]) -> bool:
        try:
            if self.http_client is not None:
                resp = self.http_client.post(self.webhook_url, json=payload)
                return 200 <= resp.status_code < 300

            body = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                self.webhook_url,
                data=body,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "MarketEcho-Webhook/1.0",
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
                return 200 <= resp.status < 300
        except Exception as exc:
            logger.error("Failed to send Discord webhook notification: %s", exc)
            return False


def create_notifier_from_env() -> DiscordWebhookAdapter:
    return DiscordWebhookAdapter()

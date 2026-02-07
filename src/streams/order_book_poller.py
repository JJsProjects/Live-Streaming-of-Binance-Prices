"""Order book REST poller — periodic full depth snapshots from Binance REST API.

Polls GET /api/v3/depth at a configurable interval, producing
OrderBookSnapshot objects that flow through the same callback pipeline
as WebSocket events.

Rate limit weights (per Binance docs):
    limit 5/10/20/50/100 → weight 5
    limit 500             → weight 10
    limit 1000            → weight 20
    limit 5000            → weight 50
    (1200 weight / minute budget)
"""

from __future__ import annotations

import asyncio

import aiohttp
import structlog

from ..core.callbacks import CallbackManager
from ..core.config import Settings
from .models import OrderBookSnapshot

logger = structlog.get_logger()

# Weight cost per limit value — used for rate limit awareness logging
_LIMIT_WEIGHTS = {
    5: 5, 10: 5, 20: 5, 50: 5, 100: 5,
    500: 10, 1000: 20, 5000: 50,
}


class OrderBookPoller:
    """Async loop that polls the Binance REST API for full order book snapshots."""

    def __init__(self, settings: Settings, callbacks: CallbackManager) -> None:
        self._symbol = settings.symbol.upper()
        self._limit = settings.streams.order_book_snapshot.limit
        self._interval = settings.streams.order_book_snapshot.interval_seconds
        self._base_url = settings.connection.rest_base_url
        self._callbacks = callbacks
        self._running = False
        self._session: aiohttp.ClientSession | None = None
        self._poll_count = 0

    async def start(self) -> None:
        """Poll the REST API in a loop until stopped."""
        self._running = True
        self._session = aiohttp.ClientSession()

        url = f"{self._base_url}/api/v3/depth"
        params = {"symbol": self._symbol, "limit": self._limit}

        weight = _LIMIT_WEIGHTS.get(self._limit, 50)
        polls_per_min = 60 / self._interval
        weight_per_min = polls_per_min * weight
        if weight_per_min > 1000:
            logger.warning(
                "high_rate_limit_usage",
                weight_per_min=round(weight_per_min),
                limit=self._limit,
                interval=self._interval,
                budget=1200,
            )

        logger.info(
            "order_book_poller_started",
            symbol=self._symbol,
            limit=self._limit,
            interval_seconds=self._interval,
            weight_per_request=weight,
        )

        while self._running:
            try:
                await self._poll(url, params)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("order_book_poll_error")

            try:
                await asyncio.sleep(self._interval)
            except asyncio.CancelledError:
                break

        logger.info(
            "order_book_poller_stopped",
            total_polls=self._poll_count,
        )

    async def stop(self) -> None:
        """Signal the poller to shut down and close the HTTP session."""
        self._running = False
        if self._session and not self._session.closed:
            await self._session.close()

    async def _poll(self, url: str, params: dict) -> None:
        """Execute a single REST poll and dispatch the result."""
        async with self._session.get(url, params=params) as resp:
            if resp.status != 200:
                logger.warning(
                    "order_book_http_error",
                    status=resp.status,
                    body=(await resp.text())[:200],
                )
                return

            data = await resp.json()

        snapshot = OrderBookSnapshot.from_rest(data, self._symbol)
        self._poll_count += 1
        await self._callbacks.dispatch_order_book_snapshot(snapshot)

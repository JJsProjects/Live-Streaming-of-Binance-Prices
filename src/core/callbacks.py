"""Callback dispatch system — decouples stream producers from consumers.

Consumers (console printer, storage, ML engine) register async callbacks.
The stream manager dispatches validated messages through this system.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

import structlog

from ..streams.models import AggTrade, BookTicker, DepthSnapshot, DepthUpdate, Kline, OrderBookSnapshot, Trade

logger = structlog.get_logger()

# Type alias for async callbacks
AsyncCallback = Callable[..., Coroutine[Any, Any, None]]


class CallbackManager:
    """Fan-out dispatcher for stream events."""

    def __init__(self) -> None:
        self._trade_cbs: list[AsyncCallback] = []
        self._agg_trade_cbs: list[AsyncCallback] = []
        self._kline_cbs: list[AsyncCallback] = []
        self._book_ticker_cbs: list[AsyncCallback] = []
        self._depth_snapshot_cbs: list[AsyncCallback] = []
        self._depth_update_cbs: list[AsyncCallback] = []
        self._order_book_snapshot_cbs: list[AsyncCallback] = []

    # ── registration ────────────────────────────────────────────────────

    def on_trade(self, cb: AsyncCallback) -> None:
        self._trade_cbs.append(cb)

    def on_agg_trade(self, cb: AsyncCallback) -> None:
        self._agg_trade_cbs.append(cb)

    def on_kline(self, cb: AsyncCallback) -> None:
        self._kline_cbs.append(cb)

    def on_book_ticker(self, cb: AsyncCallback) -> None:
        self._book_ticker_cbs.append(cb)

    def on_depth_snapshot(self, cb: AsyncCallback) -> None:
        self._depth_snapshot_cbs.append(cb)

    def on_depth_update(self, cb: AsyncCallback) -> None:
        self._depth_update_cbs.append(cb)

    def on_order_book_snapshot(self, cb: AsyncCallback) -> None:
        self._order_book_snapshot_cbs.append(cb)

    # ── dispatch ────────────────────────────────────────────────────────

    async def dispatch_trade(self, trade: Trade) -> None:
        await self._dispatch(self._trade_cbs, trade, "trade")

    async def dispatch_agg_trade(self, agg_trade: AggTrade) -> None:
        await self._dispatch(self._agg_trade_cbs, agg_trade, "agg_trade")

    async def dispatch_kline(self, kline: Kline) -> None:
        await self._dispatch(self._kline_cbs, kline, "kline")

    async def dispatch_book_ticker(self, book_ticker: BookTicker) -> None:
        await self._dispatch(self._book_ticker_cbs, book_ticker, "book_ticker")

    async def dispatch_depth_snapshot(self, depth: DepthSnapshot) -> None:
        await self._dispatch(self._depth_snapshot_cbs, depth, "depth_snapshot")

    async def dispatch_depth_update(self, depth: DepthUpdate) -> None:
        await self._dispatch(self._depth_update_cbs, depth, "depth_update")

    async def dispatch_order_book_snapshot(self, snapshot: OrderBookSnapshot) -> None:
        await self._dispatch(self._order_book_snapshot_cbs, snapshot, "order_book_snapshot")

    # ── internal ────────────────────────────────────────────────────────

    async def _dispatch(
        self, callbacks: list[AsyncCallback], event: Any, label: str
    ) -> None:
        for cb in callbacks:
            try:
                await cb(event)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("callback_error", callback=cb.__name__, stream=label)

"""WebSocket manager — connection lifecycle, reconnection, and dispatch.

Defensive features:
- Exponential backoff with jitter on reconnect
- Configurable max reconnect attempts (0 = infinite)
- Stale connection detection via message timeout
- Automatic ping/pong via websockets library
- Clock offset calibration (local vs Binance server time)
- Per-message error isolation
- Graceful shutdown on cancellation
"""

from __future__ import annotations

import asyncio
import random

import msgspec.json
import structlog
import websockets

from ..core.callbacks import CallbackManager
from ..core.config import Settings
from .handlers import MessageHandler

logger = structlog.get_logger()


class WebSocketManager:
    """Manages a single combined-stream WebSocket connection to Binance."""

    def __init__(self, settings: Settings, callbacks: CallbackManager) -> None:
        self._settings = settings
        self._callbacks = callbacks
        self._running = False
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._reconnect_count = 0
        self._msg_count = 0
        self._handler = MessageHandler(settings.connection.stale_threshold_ms)

    # ── public ──────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Connect and listen forever (until cancelled or stop() called)."""
        self._running = True
        conn = self._settings.connection
        max_attempts = conn.reconnect_max_attempts

        while self._running:
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                logger.info("manager_cancelled")
                break
            except Exception as exc:
                if not self._running:
                    break

                self._reconnect_count += 1
                if max_attempts and self._reconnect_count > max_attempts:
                    logger.error(
                        "max_reconnect_attempts",
                        attempts=self._reconnect_count,
                    )
                    break

                delay = self._backoff_delay()
                logger.warning(
                    "connection_lost",
                    error=str(exc),
                    attempt=self._reconnect_count,
                    reconnect_in=round(delay, 2),
                )
                await asyncio.sleep(delay)

        self._running = False
        logger.info("manager_stopped", total_messages=self._msg_count)

    async def stop(self) -> None:
        """Signal the manager to shut down gracefully."""
        self._running = False
        if self._ws is not None:
            await self._ws.close()

    # ── connection ──────────────────────────────────────────────────────

    def _build_url(self) -> str:
        streams: list[str] = []
        symbol = self._settings.symbol.lower()
        s = self._settings.streams

        if s.trade:
            streams.append(f"{symbol}@trade")
        if s.agg_trade:
            streams.append(f"{symbol}@aggTrade")
        if s.kline.enabled:
            streams.append(f"{symbol}@kline_{s.kline.interval}")
        if s.book_ticker:
            streams.append(f"{symbol}@bookTicker")
        if s.depth_snapshot.enabled:
            streams.append(f"{symbol}@depth{s.depth_snapshot.levels}@{s.depth_snapshot.speed}")
        if s.depth_update.enabled:
            streams.append(f"{symbol}@depth@{s.depth_update.speed}")

        stream_path = "/".join(streams)
        return f"{self._settings.connection.base_url}/stream?streams={stream_path}"

    async def _connect_and_listen(self) -> None:
        url = self._build_url()
        conn = self._settings.connection

        # Reset clock calibration on each fresh connection
        self._handler.reset()

        async with websockets.connect(
            url,
            ping_interval=conn.ping_interval,
            ping_timeout=conn.ping_timeout,
        ) as ws:
            self._ws = ws
            self._reconnect_count = 0
            logger.info("connected", url=url)

            while self._running:
                try:
                    raw = await asyncio.wait_for(
                        ws.recv(),
                        timeout=conn.message_timeout,
                    )
                    await self._handle_message(raw)
                except asyncio.TimeoutError:
                    logger.warning("message_timeout", seconds=conn.message_timeout)
                    break
                except websockets.ConnectionClosed as exc:
                    logger.warning("connection_closed", code=exc.code, reason=exc.reason)
                    break

            self._ws = None

    # ── message handling ────────────────────────────────────────────────

    async def _handle_message(self, raw: str | bytes) -> None:
        try:
            if isinstance(raw, str):
                raw = raw.encode("utf-8")
            wrapper = msgspec.json.decode(raw)
        except msgspec.DecodeError as exc:
            logger.error("json_decode_error", error=str(exc))
            return

        if not isinstance(wrapper, dict):
            logger.warning("unexpected_message_type", type=type(wrapper).__name__)
            return

        stream_name = wrapper.get("stream", "")
        data = wrapper.get("data")

        if not stream_name or not isinstance(data, dict):
            logger.warning("malformed_wrapper", keys=list(wrapper.keys()))
            return

        self._msg_count += 1
        await self._dispatch(stream_name, data)

    async def _dispatch(self, stream_name: str, data: dict) -> None:
        """Route a message to the correct handler and callback chain."""
        # Parse stream type from e.g. "btcusdt@trade", "btcusdt@kline_1m"
        parts = stream_name.split("@", 1)
        if len(parts) != 2:
            logger.warning("unrecognised_stream", stream=stream_name)
            return
        stream_type = parts[1]

        if stream_type == "trade":
            trade = self._handler.parse_trade(data)
            if trade:
                await self._callbacks.dispatch_trade(trade)

        elif stream_type == "aggTrade":
            agg = self._handler.parse_agg_trade(data)
            if agg:
                await self._callbacks.dispatch_agg_trade(agg)

        elif stream_type.startswith("kline_"):
            kline = self._handler.parse_kline(data)
            if kline:
                await self._callbacks.dispatch_kline(kline)

        elif stream_type == "bookTicker":
            bt = self._handler.parse_book_ticker(data)
            if bt:
                await self._callbacks.dispatch_book_ticker(bt)

        elif stream_type.startswith("depth") and stream_type[5:6].isdigit():
            # Partial depth snapshot: depth5@100ms, depth10@100ms, depth20@100ms
            snap = self._handler.parse_depth_snapshot(data)
            if snap:
                await self._callbacks.dispatch_depth_snapshot(snap)

        elif stream_type.startswith("depth"):
            # Incremental depth diff: depth@100ms, depth@1000ms
            upd = self._handler.parse_depth_update(data)
            if upd:
                await self._callbacks.dispatch_depth_update(upd)

        else:
            logger.warning("unknown_stream_type", stream_type=stream_type)

    # ── reconnection ────────────────────────────────────────────────────

    def _backoff_delay(self) -> float:
        conn = self._settings.connection
        delay = min(
            conn.reconnect_delay_base * (2 ** (self._reconnect_count - 1)),
            conn.reconnect_delay_max,
        )
        jitter = random.uniform(0, delay * 0.1)
        return delay + jitter

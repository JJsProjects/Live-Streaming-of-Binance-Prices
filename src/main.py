"""Entry point — wires config, streams, callbacks, and storage together.

Usage:
    python -m src.main                           # use config/settings.yaml defaults
    python -m src.main -s ethusdt --no-storage   # override symbol, stream-only
    python -m src.main --no-trade --no-agg-trade # klines only
"""

from __future__ import annotations

import asyncio

import structlog

from .core.callbacks import CallbackManager
from .core.config import Settings, load_settings
from .storage.parquet_store import ParquetStore
from .streams.manager import WebSocketManager
from .streams.models import AggTrade, BookTicker, DepthSnapshot, DepthUpdate, Kline, OrderBookSnapshot, Trade
from .streams.order_book_poller import OrderBookPoller


def _setup_logging() -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )


# ── console sink callbacks ──────────────────────────────────────────────


async def _print_trade(trade: Trade) -> None:
    print(trade)


async def _print_agg_trade(agg_trade: AggTrade) -> None:
    print(agg_trade)


async def _print_kline(kline: Kline) -> None:
    print(kline)


async def _print_book_ticker(bt: BookTicker) -> None:
    print(bt)


async def _print_depth_snapshot(depth: DepthSnapshot) -> None:
    print(depth)


async def _print_depth_update(depth: DepthUpdate) -> None:
    print(depth)


async def _print_order_book_snapshot(snapshot: OrderBookSnapshot) -> None:
    print(snapshot)


# ── banner ──────────────────────────────────────────────────────────────


def _print_banner(settings: Settings) -> None:
    s = settings.streams
    streams = []
    if s.trade:
        streams.append("trade")
    if s.agg_trade:
        streams.append("aggTrade")
    if s.kline.enabled:
        streams.append(f"kline_{s.kline.interval}")
    if s.book_ticker:
        streams.append("bookTicker")
    if s.depth_snapshot.enabled:
        streams.append(f"depth{s.depth_snapshot.levels}@{s.depth_snapshot.speed}")
    if s.depth_update.enabled:
        streams.append(f"depth@{s.depth_update.speed}")
    if s.order_book_snapshot.enabled:
        streams.append(f"orderBook({s.order_book_snapshot.limit})@{s.order_book_snapshot.interval_seconds}s")

    print()
    print("=" * 60)
    print("  Binance Live Price Streamer")
    print("=" * 60)
    print(f"  Symbol  : {settings.symbol.upper()}")
    print(f"  Streams : {', '.join(streams)}")
    print(f"  Storage : {'ON -> ' + settings.storage.output_dir if settings.storage.enabled else 'OFF'}")
    print("=" * 60)
    print("  Press Ctrl+C to stop")
    print("=" * 60)
    print()


# ── main async entry ────────────────────────────────────────────────────


async def _run(settings: Settings) -> None:
    callbacks = CallbackManager()

    # Always register console output
    callbacks.on_trade(_print_trade)
    callbacks.on_agg_trade(_print_agg_trade)
    callbacks.on_kline(_print_kline)
    callbacks.on_book_ticker(_print_book_ticker)
    callbacks.on_depth_snapshot(_print_depth_snapshot)
    callbacks.on_depth_update(_print_depth_update)
    callbacks.on_order_book_snapshot(_print_order_book_snapshot)

    # Optionally register storage
    storage: ParquetStore | None = None
    if settings.storage.enabled:
        storage = ParquetStore(settings.storage, settings.symbol)
        callbacks.on_trade(storage.store_trade)
        callbacks.on_agg_trade(storage.store_agg_trade)
        callbacks.on_kline(storage.store_kline)
        callbacks.on_book_ticker(storage.store_book_ticker)
        callbacks.on_depth_snapshot(storage.store_depth_snapshot)
        callbacks.on_depth_update(storage.store_depth_update)
        callbacks.on_order_book_snapshot(storage.store_order_book_snapshot)

    manager = WebSocketManager(settings, callbacks)

    # Order book REST poller (optional)
    poller: OrderBookPoller | None = None
    if settings.streams.order_book_snapshot.enabled:
        poller = OrderBookPoller(settings, callbacks)

    # Build task list
    tasks: list[asyncio.Task] = [
        asyncio.create_task(manager.start(), name="ws_manager"),
    ]
    if poller:
        tasks.append(
            asyncio.create_task(poller.start(), name="ob_poller")
        )
    if storage:
        tasks.append(
            asyncio.create_task(storage.periodic_flush_loop(), name="flush_loop")
        )

    try:
        await asyncio.gather(*tasks)
    finally:
        await manager.stop()
        if poller:
            await poller.stop()
        if storage:
            await storage.close()


# ── sync entry ──────────────────────────────────────────────────────────


def main() -> None:
    _setup_logging()
    settings = load_settings()
    _print_banner(settings)

    try:
        asyncio.run(_run(settings))
    except KeyboardInterrupt:
        print("\nShutdown complete.")


if __name__ == "__main__":
    main()

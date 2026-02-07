"""Abstract storage interface.

Allows swapping storage backends (Parquet, SQLite, etc.)
without changing the rest of the application.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from ..streams.models import AggTrade, BookTicker, DepthSnapshot, DepthUpdate, Kline, OrderBookSnapshot, Trade


class BaseStore(ABC):
    """Contract that all storage backends must implement."""

    @abstractmethod
    async def store_trade(self, trade: Trade) -> None: ...

    @abstractmethod
    async def store_agg_trade(self, agg_trade: AggTrade) -> None: ...

    @abstractmethod
    async def store_kline(self, kline: Kline) -> None: ...

    @abstractmethod
    async def store_book_ticker(self, bt: BookTicker) -> None: ...

    @abstractmethod
    async def store_depth_snapshot(self, depth: DepthSnapshot) -> None: ...

    @abstractmethod
    async def store_depth_update(self, depth: DepthUpdate) -> None: ...

    @abstractmethod
    async def store_order_book_snapshot(self, snapshot: OrderBookSnapshot) -> None: ...

    @abstractmethod
    async def flush_all(self) -> None:
        """Flush any buffered data to disk."""

    @abstractmethod
    async def close(self) -> None:
        """Flush and release resources."""

"""Parquet storage backend — buffered writes with time-based file rotation.

Files are organized as:
    <output_dir>/<stream_type>/<symbol>_<timestamp>.parquet

Writes are non-blocking (run in a thread executor) so the async event loop
is never stalled by disk I/O.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from ..core.config import StorageConfig
from ..streams.models import AggTrade, Kline, Trade
from .base import BaseStore

logger = structlog.get_logger()


class ParquetStore(BaseStore):
    def __init__(self, config: StorageConfig, symbol: str) -> None:
        self._config = config
        self._symbol = symbol.lower()
        self._base_dir = Path(config.output_dir)
        self._flush_interval = config.rotation_minutes * 60
        self._buffer_max = config.buffer_max
        self._running = True

        self._buffers: dict[str, list[dict]] = {
            "trade": [],
            "agg_trade": [],
            "kline": [],
        }
        self._last_flush: dict[str, float] = {
            k: time.monotonic() for k in self._buffers
        }

    # ── public sink callbacks ───────────────────────────────────────────

    async def store_trade(self, trade: Trade) -> None:
        self._buffers["trade"].append(trade.to_dict())
        await self._maybe_flush("trade")

    async def store_agg_trade(self, agg_trade: AggTrade) -> None:
        self._buffers["agg_trade"].append(agg_trade.to_dict())
        await self._maybe_flush("agg_trade")

    async def store_kline(self, kline: Kline) -> None:
        self._buffers["kline"].append(kline.to_dict())
        await self._maybe_flush("kline")

    # ── periodic flush loop (run as background task) ────────────────────

    async def periodic_flush_loop(self) -> None:
        """Check all buffers every 30s and flush if rotation interval elapsed."""
        while self._running:
            await asyncio.sleep(30)
            for stream_type in list(self._buffers.keys()):
                elapsed = time.monotonic() - self._last_flush[stream_type]
                if elapsed >= self._flush_interval and self._buffers[stream_type]:
                    await self._flush(stream_type)

    # ── flush ───────────────────────────────────────────────────────────

    async def flush_all(self) -> None:
        """Flush every buffer — call on shutdown."""
        for stream_type in list(self._buffers.keys()):
            await self._flush(stream_type)

    async def close(self) -> None:
        self._running = False
        await self.flush_all()

    async def _maybe_flush(self, stream_type: str) -> None:
        buf = self._buffers[stream_type]

        size_trigger = len(buf) >= self._buffer_max
        time_trigger = (
            time.monotonic() - self._last_flush[stream_type]
        ) >= self._flush_interval

        if size_trigger or time_trigger:
            await self._flush(stream_type)

    async def _flush(self, stream_type: str) -> None:
        buf = self._buffers[stream_type]
        if not buf:
            return

        # Swap buffer so new messages aren't blocked during write
        self._buffers[stream_type] = []
        self._last_flush[stream_type] = time.monotonic()

        # Run I/O in a thread to avoid blocking the event loop
        try:
            await asyncio.to_thread(self._sync_write, stream_type, buf)
        except Exception:
            # Put records back so they aren't lost
            self._buffers[stream_type] = buf + self._buffers[stream_type]
            logger.exception("flush_error", stream_type=stream_type, records=len(buf))

    def _sync_write(self, stream_type: str, records: list[dict]) -> None:
        """Blocking Parquet write — called from thread executor."""
        table = pa.Table.from_pylist(records)

        out_dir = self._base_dir / stream_type
        out_dir.mkdir(parents=True, exist_ok=True)

        ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"{self._symbol}_{ts}.parquet"
        filepath = out_dir / filename

        pq.write_table(table, filepath, compression="snappy")
        logger.info(
            "parquet_written",
            path=str(filepath),
            rows=len(records),
            stream_type=stream_type,
        )

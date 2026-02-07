"""Message handlers — defensive parsing and validation layer.

Sits between raw WebSocket JSON and typed models. Handles:
- JSON structure validation
- Clock offset calibration (local clock vs Binance server time)
- Stale message detection (adjusted for clock drift)
- Parse error isolation (one bad message never kills the stream)
"""

from __future__ import annotations

import time

import structlog

from .models import AggTrade, BookTicker, DepthSnapshot, DepthUpdate, Kline, Trade

logger = structlog.get_logger()


def _now_ms() -> int:
    return int(time.time() * 1000)


class MessageHandler:
    """Stateful handler that calibrates clock offset on first messages."""

    def __init__(self, stale_threshold_ms: int) -> None:
        self._stale_threshold_ms = stale_threshold_ms
        self._clock_offset_ms: int | None = None
        self._calibration_samples: list[int] = []
        self._calibrated = False

    def _calibrate(self, event_time_ms: int) -> None:
        """Collect first 5 samples and use the median as clock offset."""
        sample = _now_ms() - event_time_ms
        self._calibration_samples.append(sample)

        if len(self._calibration_samples) >= 5:
            self._calibration_samples.sort()
            self._clock_offset_ms = self._calibration_samples[2]  # median
            self._calibrated = True
            logger.info(
                "clock_calibrated",
                offset_ms=self._clock_offset_ms,
                samples=self._calibration_samples,
            )

    def _is_stale(self, event_time_ms: int) -> bool:
        if not self._calibrated:
            self._calibrate(event_time_ms)
            return False  # never drop during calibration

        adjusted_age = (_now_ms() - event_time_ms) - self._clock_offset_ms
        return adjusted_age > self._stale_threshold_ms

    def _adjusted_age(self, event_time_ms: int) -> int:
        offset = self._clock_offset_ms or 0
        return (_now_ms() - event_time_ms) - offset

    def reset(self) -> None:
        """Reset calibration — call on reconnect."""
        self._clock_offset_ms = None
        self._calibration_samples.clear()
        self._calibrated = False

    def parse_trade(self, data: dict) -> Trade | None:
        try:
            event_time = data["E"]
            if self._is_stale(event_time):
                logger.warning(
                    "stale_trade",
                    adjusted_age_ms=self._adjusted_age(event_time),
                    trade_id=data.get("t"),
                )
                return None
            return Trade.from_raw(data)
        except (KeyError, ValueError, TypeError) as exc:
            logger.error("trade_parse_error", error=str(exc), keys=list(data.keys()))
            return None

    def parse_agg_trade(self, data: dict) -> AggTrade | None:
        try:
            event_time = data["E"]
            if self._is_stale(event_time):
                logger.warning(
                    "stale_agg_trade",
                    adjusted_age_ms=self._adjusted_age(event_time),
                    agg_id=data.get("a"),
                )
                return None
            return AggTrade.from_raw(data)
        except (KeyError, ValueError, TypeError) as exc:
            logger.error("agg_trade_parse_error", error=str(exc), keys=list(data.keys()))
            return None

    def parse_kline(self, data: dict) -> Kline | None:
        try:
            event_time = data["E"]
            if self._is_stale(event_time):
                logger.warning(
                    "stale_kline",
                    adjusted_age_ms=self._adjusted_age(event_time),
                )
                return None
            return Kline.from_raw(data)
        except (KeyError, ValueError, TypeError) as exc:
            logger.error("kline_parse_error", error=str(exc), keys=list(data.keys()))
            return None

    # ── order book streams (no event_time → no stale check) ─────────

    def parse_book_ticker(self, data: dict) -> BookTicker | None:
        try:
            return BookTicker.from_raw(data)
        except (KeyError, ValueError, TypeError) as exc:
            logger.error("book_ticker_parse_error", error=str(exc), keys=list(data.keys()))
            return None

    def parse_depth_snapshot(self, data: dict) -> DepthSnapshot | None:
        try:
            return DepthSnapshot.from_raw(data)
        except (KeyError, ValueError, TypeError) as exc:
            logger.error("depth_snapshot_parse_error", error=str(exc), keys=list(data.keys()))
            return None

    def parse_depth_update(self, data: dict) -> DepthUpdate | None:
        try:
            event_time = data["E"]
            if self._is_stale(event_time):
                logger.warning(
                    "stale_depth_update",
                    adjusted_age_ms=self._adjusted_age(event_time),
                )
                return None
            return DepthUpdate.from_raw(data)
        except (KeyError, ValueError, TypeError) as exc:
            logger.error("depth_update_parse_error", error=str(exc), keys=list(data.keys()))
            return None

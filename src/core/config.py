"""Configuration loader — merges YAML defaults with CLI argument overrides."""

from __future__ import annotations

import argparse
import dataclasses
import sys
from pathlib import Path

import yaml

VALID_KLINE_INTERVALS = {
    "1s", "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d", "3d", "1w", "1M",
}

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "settings.yaml"


# ── dataclass tree ──────────────────────────────────────────────────────────


@dataclasses.dataclass
class KlineConfig:
    enabled: bool = True
    interval: str = "1m"


@dataclasses.dataclass
class StreamsConfig:
    trade: bool = True
    agg_trade: bool = True
    kline: KlineConfig = dataclasses.field(default_factory=KlineConfig)


@dataclasses.dataclass
class StorageConfig:
    enabled: bool = True
    format: str = "parquet"
    output_dir: str = "./data"
    rotation_minutes: int = 60
    buffer_max: int = 10_000


@dataclasses.dataclass
class ConnectionConfig:
    base_url: str = "wss://stream.binance.com:9443"
    reconnect_delay_base: float = 1.0
    reconnect_delay_max: float = 60.0
    reconnect_max_attempts: int = 0
    ping_interval: int = 20
    ping_timeout: int = 10
    message_timeout: int = 30
    stale_threshold_ms: int = 30_000


@dataclasses.dataclass
class ApiConfig:
    key: str = ""
    secret: str = ""


@dataclasses.dataclass
class Settings:
    symbol: str = "btcusdt"
    streams: StreamsConfig = dataclasses.field(default_factory=StreamsConfig)
    storage: StorageConfig = dataclasses.field(default_factory=StorageConfig)
    connection: ConnectionConfig = dataclasses.field(default_factory=ConnectionConfig)
    api: ApiConfig = dataclasses.field(default_factory=ApiConfig)


# ── YAML loader ─────────────────────────────────────────────────────────────


def _load_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data if isinstance(data, dict) else {}


def _settings_from_dict(raw: dict) -> Settings:
    """Build a Settings instance from a (possibly partial) dict."""
    streams_raw = raw.get("streams", {})
    kline_raw = streams_raw.get("kline", {})

    kline = KlineConfig(
        enabled=kline_raw.get("enabled", True),
        interval=kline_raw.get("interval", "1m"),
    )
    streams = StreamsConfig(
        trade=streams_raw.get("trade", True),
        agg_trade=streams_raw.get("agg_trade", True),
        kline=kline,
    )

    stor_raw = raw.get("storage", {})
    storage = StorageConfig(
        enabled=stor_raw.get("enabled", True),
        format=stor_raw.get("format", "parquet"),
        output_dir=stor_raw.get("output_dir", "./data"),
        rotation_minutes=stor_raw.get("rotation_minutes", 60),
        buffer_max=stor_raw.get("buffer_max", 10_000),
    )

    conn_raw = raw.get("connection", {})
    connection = ConnectionConfig(
        base_url=conn_raw.get("base_url", "wss://stream.binance.com:9443"),
        reconnect_delay_base=conn_raw.get("reconnect_delay_base", 1.0),
        reconnect_delay_max=conn_raw.get("reconnect_delay_max", 60.0),
        reconnect_max_attempts=conn_raw.get("reconnect_max_attempts", 0),
        ping_interval=conn_raw.get("ping_interval", 20),
        ping_timeout=conn_raw.get("ping_timeout", 10),
        message_timeout=conn_raw.get("message_timeout", 30),
        stale_threshold_ms=conn_raw.get("stale_threshold_ms", 30_000),
    )

    api_raw = raw.get("api", {})
    api = ApiConfig(
        key=api_raw.get("key", ""),
        secret=api_raw.get("secret", ""),
    )

    return Settings(
        symbol=raw.get("symbol", "btcusdt"),
        streams=streams,
        storage=storage,
        connection=connection,
        api=api,
    )


# ── CLI parser ──────────────────────────────────────────────────────────────


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Stream live Binance prices via WebSocket.",
    )
    p.add_argument(
        "-c", "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Path to YAML config file (default: config/settings.yaml)",
    )
    p.add_argument(
        "-s", "--symbol",
        type=str,
        help="Trading pair symbol, e.g. btcusdt",
    )
    p.add_argument(
        "--no-trade",
        action="store_true",
        help="Disable the raw trade stream",
    )
    p.add_argument(
        "--no-agg-trade",
        action="store_true",
        help="Disable the aggregated trade stream",
    )
    p.add_argument(
        "--no-kline",
        action="store_true",
        help="Disable the kline stream",
    )
    p.add_argument(
        "--kline-interval",
        type=str,
        choices=sorted(VALID_KLINE_INTERVALS),
        help="Kline interval (e.g. 1m, 5m, 1h)",
    )
    p.add_argument(
        "--no-storage",
        action="store_true",
        help="Disable data persistence (stream-only mode)",
    )
    p.add_argument(
        "--output-dir",
        type=str,
        help="Override storage output directory",
    )
    return p


# ── public API ──────────────────────────────────────────────────────────────


def load_settings(argv: list[str] | None = None) -> Settings:
    """Load settings from YAML config, then apply CLI overrides."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    raw = _load_yaml(args.config)
    settings = _settings_from_dict(raw)

    # CLI overrides
    if args.symbol:
        settings.symbol = args.symbol.lower().strip()
    if args.no_trade:
        settings.streams.trade = False
    if args.no_agg_trade:
        settings.streams.agg_trade = False
    if args.no_kline:
        settings.streams.kline.enabled = False
    if args.kline_interval:
        settings.streams.kline.interval = args.kline_interval
    if args.no_storage:
        settings.storage.enabled = False
    if args.output_dir:
        settings.storage.output_dir = args.output_dir

    # validation
    _validate(settings)
    return settings


def _validate(settings: Settings) -> None:
    """Fail fast on invalid configuration."""
    if not settings.symbol:
        print("Error: symbol cannot be empty", file=sys.stderr)
        sys.exit(1)

    if settings.streams.kline.interval not in VALID_KLINE_INTERVALS:
        print(
            f"Error: invalid kline interval '{settings.streams.kline.interval}'. "
            f"Valid: {sorted(VALID_KLINE_INTERVALS)}",
            file=sys.stderr,
        )
        sys.exit(1)

    any_stream = (
        settings.streams.trade
        or settings.streams.agg_trade
        or settings.streams.kline.enabled
    )
    if not any_stream:
        print("Error: at least one stream must be enabled", file=sys.stderr)
        sys.exit(1)

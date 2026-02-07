# Live Streaming of Binance Prices

Real-time WebSocket streaming of Binance market data with optional Parquet storage. Built as the data foundation for a live ML prediction and trading pipeline.

## Features

- **Live streaming** via Binance combined WebSocket — trade, aggTrade, kline, bookTicker, depth snapshots, depth diffs
- **Full order book snapshots** via REST API polling — up to 5000 levels, configurable interval
- **Configurable** — YAML config with full CLI override support
- **Storage toggle** — stream-only or stream + Parquet persistence
- **Defensive design** — auto-reconnect with backoff, clock drift calibration, stale message detection, per-message error isolation
- **Extensible** — async callback system for plugging in ML models, order execution, or custom consumers

## Quick Start

```bash
pip install -r requirements.txt
python -m src                           # stream BTCUSDT with storage
python -m src --no-storage              # stream only, no files
python -m src -s ethusdt --no-trade     # ETH, aggTrade + kline only
python -m src --order-book-snapshot     # enable full order book REST polling
python -m src --help                    # all options
```

## Order Book Snapshot (REST API)

Poll the full order book from the Binance REST API at a configurable interval:

```bash
# 1000-level snapshots every 5 seconds (default)
python -m src --order-book-snapshot --no-storage

# 5000-level deep snapshots every 10 seconds with storage
python -m src --order-book-snapshot --order-book-limit 5000 --order-book-interval 10

# Lightweight 20-level snapshots every 2 seconds
python -m src --order-book-snapshot --order-book-limit 20 --order-book-interval 2

# Hybrid: REST snapshots + WebSocket depth diffs for cross-validation
python -m src --order-book-snapshot --depth-update --no-storage
```

Valid limits: 5, 10, 20, 50, 100, 500, 1000, 5000. Rate limit weights vary by depth (5–50 per request out of 1200/min budget).

## Project Structure

```
config/settings.yaml             — symbol, streams, storage, connection config
src/
  core/config.py                 — YAML loader + CLI argparse overrides
  core/callbacks.py              — async callback fan-out dispatcher
  streams/models.py              — Trade, AggTrade, Kline, OrderBookSnapshot dataclasses
  streams/handlers.py            — clock calibration + stale detection
  streams/manager.py             — WebSocket lifecycle + reconnect
  streams/order_book_poller.py   — REST API order book polling loop
  storage/base.py                — abstract storage interface
  storage/parquet_store.py       — buffered Parquet writer with rotation
  main.py                        — entry point
```

## Architecture

```
Binance WebSocket ──► Manager ──► Handler ──► Callbacks ──┬──► Console
                      (reconnect)  (validate)              ├──► Parquet Storage
                                                           └──► [Future: ML / Execution]
Binance REST API ──► OrderBookPoller ────────► Callbacks ──┘
                     (periodic poll)
```

## Roadmap

- [ ] ML feature pipeline + inference callbacks
- [ ] Paper trading executor
- [ ] Binance REST API order execution
- [ ] Risk management layer
- [ ] C++ hot-path extraction via pybind11

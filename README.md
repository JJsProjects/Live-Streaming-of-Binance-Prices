# Live Streaming of Binance Prices

Real-time WebSocket streaming of Binance market data with optional Parquet storage. Built as the data foundation for a live ML prediction and trading pipeline.

## Features

- **Live streaming** via Binance combined WebSocket — trade, aggTrade, kline
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
python -m src --help                    # all options
```

## Project Structure

```
config/settings.yaml         — symbol, streams, storage, connection config
src/
  core/config.py             — YAML loader + CLI argparse overrides
  core/callbacks.py          — async callback fan-out dispatcher
  streams/models.py          — Trade, AggTrade, Kline dataclasses
  streams/handlers.py        — clock calibration + stale detection
  streams/manager.py         — WebSocket lifecycle + reconnect
  storage/base.py            — abstract storage interface
  storage/parquet_store.py   — buffered Parquet writer with rotation
  main.py                    — entry point
```

## Architecture

```
Binance WebSocket ──► Manager ──► Handler ──► Callbacks ──┬──► Console
                      (reconnect)  (validate)              ├──► Parquet Storage
                                                           └──► [Future: ML / Execution]
```

## Roadmap

- [ ] ML feature pipeline + inference callbacks
- [ ] Paper trading executor
- [ ] Binance REST API order execution
- [ ] Risk management layer
- [ ] C++ hot-path extraction via pybind11

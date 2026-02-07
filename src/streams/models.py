"""Data models for Binance stream messages.

Each model maps to a Binance stream type and converts raw JSON dicts
(with single-letter keys) into clean, typed Python objects.
"""

from __future__ import annotations

import dataclasses
import time
from datetime import datetime, timezone


@dataclasses.dataclass
class Trade:
    """Individual trade event from <symbol>@trade stream.

    Raw Binance keys:
        e=event_type, E=event_time, s=symbol, t=trade_id, p=price,
        q=quantity, b=buyer_order_id, a=seller_order_id, T=trade_time,
        m=is_buyer_maker
    """

    event_time: int
    symbol: str
    trade_id: int
    price: float
    quantity: float
    trade_time: int
    is_buyer_maker: bool
    buyer_order_id: int | None = None
    seller_order_id: int | None = None

    @classmethod
    def from_raw(cls, d: dict) -> Trade:
        return cls(
            event_time=d["E"],
            symbol=d["s"],
            trade_id=d["t"],
            price=float(d["p"]),
            quantity=float(d["q"]),
            trade_time=d["T"],
            is_buyer_maker=d["m"],
            buyer_order_id=d.get("b"),
            seller_order_id=d.get("a"),
        )

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    def __str__(self) -> str:
        side = "SELL" if self.is_buyer_maker else "BUY"
        ts = datetime.fromtimestamp(
            self.trade_time / 1000, tz=timezone.utc
        ).strftime("%H:%M:%S.%f")[:-3]
        return (
            f"[TRADE]  {self.symbol} | "
            f"Price: {self.price:<12.4f} | "
            f"Qty: {self.quantity:<12.6f} | "
            f"Side: {side:<4} | {ts}"
        )


@dataclasses.dataclass
class AggTrade:
    """Aggregated trade event from <symbol>@aggTrade stream.

    Raw Binance keys:
        e=event_type, E=event_time, s=symbol, a=agg_trade_id, p=price,
        q=quantity, f=first_trade_id, l=last_trade_id, T=trade_time,
        m=is_buyer_maker
    """

    event_time: int
    symbol: str
    agg_trade_id: int
    price: float
    quantity: float
    first_trade_id: int
    last_trade_id: int
    trade_time: int
    is_buyer_maker: bool

    @property
    def num_trades(self) -> int:
        return self.last_trade_id - self.first_trade_id + 1

    @classmethod
    def from_raw(cls, d: dict) -> AggTrade:
        return cls(
            event_time=d["E"],
            symbol=d["s"],
            agg_trade_id=d["a"],
            price=float(d["p"]),
            quantity=float(d["q"]),
            first_trade_id=d["f"],
            last_trade_id=d["l"],
            trade_time=d["T"],
            is_buyer_maker=d["m"],
        )

    def to_dict(self) -> dict:
        d = dataclasses.asdict(self)
        d["num_trades"] = self.num_trades
        return d

    def __str__(self) -> str:
        side = "SELL" if self.is_buyer_maker else "BUY"
        ts = datetime.fromtimestamp(
            self.trade_time / 1000, tz=timezone.utc
        ).strftime("%H:%M:%S.%f")[:-3]
        return (
            f"[AGG]    {self.symbol} | "
            f"Price: {self.price:<12.4f} | "
            f"Qty: {self.quantity:<12.6f} | "
            f"Side: {side:<4} | "
            f"Trades: {self.num_trades:<4} | {ts}"
        )


@dataclasses.dataclass
class Kline:
    """Kline/candlestick event from <symbol>@kline_<interval> stream.

    Raw Binance keys (nested under 'k'):
        t=open_time, T=close_time, s=symbol, i=interval, f=first_trade_id,
        L=last_trade_id, o=open, c=close, h=high, l=low, v=volume,
        n=num_trades, x=is_closed, q=quote_volume, V=taker_buy_volume,
        Q=taker_buy_quote_volume
    """

    event_time: int
    symbol: str
    interval: str
    open_time: int
    close_time: int
    open: float
    close: float
    high: float
    low: float
    volume: float
    num_trades: int
    is_closed: bool
    quote_volume: float
    taker_buy_volume: float
    taker_buy_quote_volume: float

    @classmethod
    def from_raw(cls, d: dict) -> Kline:
        k = d["k"]
        return cls(
            event_time=d["E"],
            symbol=d["s"],
            interval=k["i"],
            open_time=k["t"],
            close_time=k["T"],
            open=float(k["o"]),
            close=float(k["c"]),
            high=float(k["h"]),
            low=float(k["l"]),
            volume=float(k["v"]),
            num_trades=k["n"],
            is_closed=k["x"],
            quote_volume=float(k["q"]),
            taker_buy_volume=float(k["V"]),
            taker_buy_quote_volume=float(k["Q"]),
        )

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    def __str__(self) -> str:
        closed = "YES" if self.is_closed else "NO"
        ts = datetime.fromtimestamp(
            self.event_time / 1000, tz=timezone.utc
        ).strftime("%H:%M:%S")
        return (
            f"[KLINE]  {self.symbol} | "
            f"{self.interval:<3} | "
            f"O:{self.open:<10.2f} H:{self.high:<10.2f} "
            f"L:{self.low:<10.2f} C:{self.close:<10.2f} | "
            f"Vol: {self.volume:<12.2f} | "
            f"Closed: {closed} | {ts}"
        )


# ── Order book models ───────────────────────────────────────────────────


@dataclasses.dataclass
class BookTicker:
    """Best bid/ask from <symbol>@bookTicker stream.

    Raw Binance keys:
        u=update_id, s=symbol, b=bid_price, B=bid_qty,
        a=ask_price, A=ask_qty
    """

    update_id: int
    symbol: str
    bid_price: float
    bid_quantity: float
    ask_price: float
    ask_quantity: float
    local_time: int = 0  # capture time since stream has no event_time

    @classmethod
    def from_raw(cls, d: dict) -> BookTicker:
        return cls(
            update_id=d["u"],
            symbol=d["s"],
            bid_price=float(d["b"]),
            bid_quantity=float(d["B"]),
            ask_price=float(d["a"]),
            ask_quantity=float(d["A"]),
            local_time=int(time.time() * 1000),
        )

    @property
    def spread(self) -> float:
        return self.ask_price - self.bid_price

    @property
    def mid_price(self) -> float:
        return (self.bid_price + self.ask_price) / 2

    def to_dict(self) -> dict:
        d = dataclasses.asdict(self)
        d["spread"] = self.spread
        d["mid_price"] = self.mid_price
        return d

    def __str__(self) -> str:
        return (
            f"[BOOK]   {self.symbol} | "
            f"Bid: {self.bid_price:<12.4f} ({self.bid_quantity:<10.4f}) | "
            f"Ask: {self.ask_price:<12.4f} ({self.ask_quantity:<10.4f}) | "
            f"Spread: {self.spread:.4f}"
        )


@dataclasses.dataclass
class DepthSnapshot:
    """Partial order book snapshot from <symbol>@depth<levels> stream.

    Raw Binance keys:
        lastUpdateId, bids=[[price,qty],...], asks=[[price,qty],...]
    """

    last_update_id: int
    bids: list[list[float]]  # [[price, qty], ...]
    asks: list[list[float]]
    local_time: int = 0

    @classmethod
    def from_raw(cls, d: dict) -> DepthSnapshot:
        bids = [[float(p), float(q)] for p, q in d["bids"]]
        asks = [[float(p), float(q)] for p, q in d["asks"]]
        return cls(
            last_update_id=d["lastUpdateId"],
            bids=bids,
            asks=asks,
            local_time=int(time.time() * 1000),
        )

    @property
    def best_bid(self) -> float:
        return self.bids[0][0] if self.bids else 0.0

    @property
    def best_ask(self) -> float:
        return self.asks[0][0] if self.asks else 0.0

    @property
    def spread(self) -> float:
        return self.best_ask - self.best_bid

    def to_dict(self) -> dict:
        return {
            "last_update_id": self.last_update_id,
            "local_time": self.local_time,
            "bid_prices": [b[0] for b in self.bids],
            "bid_quantities": [b[1] for b in self.bids],
            "ask_prices": [a[0] for a in self.asks],
            "ask_quantities": [a[1] for a in self.asks],
        }

    def __str__(self) -> str:
        levels = max(len(self.bids), len(self.asks))
        return (
            f"[DEPTH]  Levels: {levels} | "
            f"Best Bid: {self.best_bid:<12.4f} | "
            f"Best Ask: {self.best_ask:<12.4f} | "
            f"Spread: {self.spread:.4f}"
        )


@dataclasses.dataclass
class DepthUpdate:
    """Incremental order book diff from <symbol>@depth stream.

    Raw Binance keys:
        e=event_type, E=event_time, s=symbol, U=first_update_id,
        u=final_update_id, b=bids, a=asks
    """

    event_time: int
    symbol: str
    first_update_id: int
    final_update_id: int
    bids: list[list[float]]  # [[price, qty], ...] — qty=0 means remove
    asks: list[list[float]]

    @classmethod
    def from_raw(cls, d: dict) -> DepthUpdate:
        bids = [[float(p), float(q)] for p, q in d["b"]]
        asks = [[float(p), float(q)] for p, q in d["a"]]
        return cls(
            event_time=d["E"],
            symbol=d["s"],
            first_update_id=d["U"],
            final_update_id=d["u"],
            bids=bids,
            asks=asks,
        )

    def to_dict(self) -> dict:
        return {
            "event_time": self.event_time,
            "symbol": self.symbol,
            "first_update_id": self.first_update_id,
            "final_update_id": self.final_update_id,
            "bid_prices": [b[0] for b in self.bids],
            "bid_quantities": [b[1] for b in self.bids],
            "ask_prices": [a[0] for a in self.asks],
            "ask_quantities": [a[1] for a in self.asks],
        }

    def __str__(self) -> str:
        ts = datetime.fromtimestamp(
            self.event_time / 1000, tz=timezone.utc
        ).strftime("%H:%M:%S.%f")[:-3]
        return (
            f"[DDIFF]  {self.symbol} | "
            f"Bid updates: {len(self.bids):<3} | "
            f"Ask updates: {len(self.asks):<3} | {ts}"
        )


@dataclasses.dataclass
class OrderBookSnapshot:
    """Full order book snapshot from REST API GET /api/v3/depth.

    Response keys:
        lastUpdateId, bids=[[price,qty],...], asks=[[price,qty],...]
    """

    last_update_id: int
    symbol: str
    bids: list[list[float]]  # [[price, qty], ...]
    asks: list[list[float]]
    local_time: int = 0

    @classmethod
    def from_rest(cls, data: dict, symbol: str) -> OrderBookSnapshot:
        bids = [[float(p), float(q)] for p, q in data["bids"]]
        asks = [[float(p), float(q)] for p, q in data["asks"]]
        return cls(
            last_update_id=data["lastUpdateId"],
            symbol=symbol.upper(),
            bids=bids,
            asks=asks,
            local_time=int(time.time() * 1000),
        )

    @property
    def best_bid(self) -> float:
        return self.bids[0][0] if self.bids else 0.0

    @property
    def best_ask(self) -> float:
        return self.asks[0][0] if self.asks else 0.0

    @property
    def spread(self) -> float:
        return self.best_ask - self.best_bid

    def to_dict(self) -> dict:
        return {
            "last_update_id": self.last_update_id,
            "symbol": self.symbol,
            "local_time": self.local_time,
            "bid_levels": len(self.bids),
            "ask_levels": len(self.asks),
            "bid_prices": [b[0] for b in self.bids],
            "bid_quantities": [b[1] for b in self.bids],
            "ask_prices": [a[0] for a in self.asks],
            "ask_quantities": [a[1] for a in self.asks],
        }

    def __str__(self) -> str:
        levels = max(len(self.bids), len(self.asks))
        ts = datetime.fromtimestamp(
            self.local_time / 1000, tz=timezone.utc
        ).strftime("%H:%M:%S.%f")[:-3]
        return (
            f"[OBOOK]  {self.symbol} | "
            f"Levels: {levels:<5} | "
            f"Best Bid: {self.best_bid:<12.4f} | "
            f"Best Ask: {self.best_ask:<12.4f} | "
            f"Spread: {self.spread:.4f} | {ts}"
        )

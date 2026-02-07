"""Data models for Binance WebSocket stream messages.

Each model maps to a Binance stream type and converts raw JSON dicts
(with single-letter keys) into clean, typed Python objects.
"""

from __future__ import annotations

import dataclasses
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

"""
trade_metrics.py — derive per-trade performance metrics from the event log.

Pure functions, no DB / FastAPI / nautilus dependency (same idea as
position_sizing.py): easy to unit test, and the single place where "what is
PnL / a win / ROI" is defined, so every client (WPF app, dashboards, ...)
shows identical numbers instead of recomputing them.

Input is exactly what TradeEventsDB.get_trades_history() returns per trade:
the trade row (instrument, side, target, size, entry_fill_price, exit_price,
terminal_type, filled_at, finished_at) plus its `events` list.

Definitions (all money in USDT, spot USDT-quoted instruments only):

    gross_pnl  = direction * (exit_price - entry_fill_price) * size
                 direction = +1 for BUY, -1 for SELL
    fees       = every `metadata.commission` found on the trade's fill events,
                 split by currency:
                   USDT        -> fee_usdt
                   base asset  -> converted at that fill's price, into fee_usdt
                                  (Binance charges BUY fees in the received
                                  coin unless BNB-for-fees is on)
                   BNB         -> fee_bnb (native BNB), reported only and
                                  deliberately NOT part of net_pnl (see below)
    net_pnl    = gross_pnl - fee_usdt
    win        = net_pnl > 0            (NOT "take-profit hit": after fees a
                                         tiny TP can still lose money)
    roi_pct    = net_pnl / (entry_fill_price * size) * 100   (return on the
                 entry notional; if you trade with leverage, ROI on margin
                 would be a different number)
    duration_s = finished_at - filled_at, i.e. time IN the position
                 (None if the entry never filled)

BNB fees: ignored when calculating PnL by design. They are still summed into
`fee_bnb` (native units) so they stay visible, but they are never converted
to USDT, so a trade paying fees in BNB still gets a net_pnl / win / roi_pct.
`fee_bnb_usdt` is therefore always None. `bnb_usdt_fallback` is still accepted
by compute_trade_metrics() for backward compatibility but has no effect.

Known data limitation: the strategy currently records only the FIRST fill of
each order. `fills_complete` says whether the recorded fill quantities add up
to the trade size; when it is False the fees (and the assumption that the
whole `size` was executed) may be understated.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set

QUOTE = "USDT"
QTY_TOLERANCE = Decimal("0.02")   # recorded fills within 2% of size count as "complete"
                                  # (orders are rounded to the lot step, size is not)

# Nautilus Money.__str__ -> "0.00507962 USDT"
_COMMISSION_RE = re.compile(r"^\s*([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)\s+([A-Za-z0-9]+)\s*$")


@dataclass(frozen=True)
class TradeMetrics:
    ticker: str                    # "PROMUSDT" (venue suffix stripped)
    side: str                      # "BUY" | "SELL"
    target: str                    # "virtual" | "real"
    win: Optional[bool]
    gross_pnl: Optional[float]
    fee_usdt: float                # fees paid in USDT (incl. base-asset fees converted at fill price)
    fee_bnb: float                 # fees paid in BNB, native units
    fee_bnb_usdt: Optional[float]  # always None: BNB fees are not valued in USDT / not part of PnL
    net_pnl: Optional[float]
    roi_pct: Optional[float]
    duration_s: Optional[float]
    fills_complete: Optional[bool]  # None = no fill quantities recorded at all
    notes: List[str] = field(default_factory=list)


def _d(v: Any) -> Optional[Decimal]:
    """Any number-ish -> Decimal (via str, so floats don't drag in binary noise)."""
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError):
        return None


def _f(v: Optional[Decimal]) -> Optional[float]:
    return float(v) if v is not None else None


def _parse_commission(raw: Any) -> Optional[tuple[Decimal, str]]:
    if not isinstance(raw, str):
        return None
    m = _COMMISSION_RE.match(raw)
    if not m:
        return None
    amount = _d(m.group(1))
    return (amount, m.group(2).upper()) if amount is not None else None


def _covers(recorded: Decimal, size: Decimal) -> bool:
    return abs(recorded - size) <= size * QTY_TOLERANCE


def compute_trade_metrics(
    trade: Mapping[str, Any],
    events: Optional[Sequence[Mapping[str, Any]]] = None,
    *,
    bnb_usdt_fallback: Optional[float] = None,
) -> Optional[TradeMetrics]:
    """
    Returns None for trades that are still open (nothing to report yet) and
    for instruments not quoted in USDT. Otherwise always returns a
    TradeMetrics; fields that cannot be determined honestly are None and
    `notes` says why.
    """
    if trade.get("terminal_type") is None:
        return None

    instrument = str(trade["instrument"])
    ticker = instrument.rsplit(".", 1)[0]
    if not ticker.endswith(QUOTE):
        return None
    base = ticker[: -len(QUOTE)].upper()

    side = str(trade["side"]).upper()
    direction = {"BUY": 1, "SELL": -1}.get(side)
    size = _d(trade.get("size"))
    entry = _d(trade.get("entry_fill_price"))
    exit_ = _d(trade.get("exit_price"))

    notes: List[str] = []

    # ---- fees + recorded fill quantities --------------------------------
    fee_usdt = Decimal(0)
    fee_bnb = Decimal(0)
    unpriced = False
    entry_qty = Decimal(0)
    exit_qty = Decimal(0)
    seen: Set[str] = set()

    for ev in events or []:
        meta = ev.get("metadata")
        if not isinstance(meta, Mapping):
            continue
        if "last_qty" not in meta and "commission" not in meta:
            continue  # not a fill event

        trade_key = meta.get("exchange_trade_id")
        if trade_key is not None:
            if trade_key in seen:
                continue  # same exchange fill recorded twice
            seen.add(str(trade_key))

        qty = _d(meta.get("last_qty"))
        role = meta.get("order_role")
        if qty is not None:
            if role == "entry":
                entry_qty += qty
            elif role in ("sl", "tp", "close"):
                exit_qty += qty

        raw = meta.get("commission")
        if raw is None:
            continue
        parsed = _parse_commission(raw)
        if parsed is None:
            notes.append(f"unparseable commission {raw!r}; fees not priced")
            unpriced = True
            continue

        amount, ccy = parsed
        if ccy == QUOTE:
            fee_usdt += amount
        elif ccy == "BNB":
            fee_bnb += amount  # reported, but intentionally excluded from PnL
        elif ccy == base:
            px = _d(meta.get("last_px")) or _d(ev.get("fill_price"))
            if px is None:
                notes.append(f"{ccy} fee without a fill price; fees not priced")
                unpriced = True
            else:
                fee_usdt += amount * px
        else:
            notes.append(f"fee in unsupported asset {ccy}; fees not priced")
            unpriced = True

    if fee_bnb > 0:
        notes.append(f"{fee_bnb:f} BNB in fees not included in net_pnl")

    # ---- completeness of what was recorded ------------------------------
    fills_complete: Optional[bool]
    if size is None or size <= 0 or (entry_qty == 0 and exit_qty == 0):
        fills_complete = None
    else:
        needs_exit = trade.get("terminal_type") == "Closed" and exit_ is not None
        fills_complete = _covers(entry_qty, size) and (_covers(exit_qty, size) if needs_exit else True)
        if not fills_complete:
            notes.append(
                f"recorded fills cover entry {entry_qty:f} / exit {exit_qty:f} of size {size:f}: "
                "fees and PnL assume the full size executed and fees may be understated"
            )

    # ---- PnL -------------------------------------------------------------
    gross = net = roi = None
    win: Optional[bool] = None
    if (
        trade.get("terminal_type") == "Closed"
        and direction is not None
        and size is not None and entry is not None and exit_ is not None
    ):
        gross = direction * (exit_ - entry) * size
        if not unpriced:
            net = gross - fee_usdt
            win = net > 0
            notional = entry * size
            roi = net / notional * 100 if notional > 0 else None
    elif trade.get("terminal_type") == "Cancelled" and entry is not None:
        notes.append("trade was cancelled after an entry fill; exit unknown, PnL unavailable")

    # ---- duration --------------------------------------------------------
    filled_at, finished_at = trade.get("filled_at"), trade.get("finished_at")
    duration = None
    if filled_at is not None and finished_at is not None and finished_at >= filled_at:
        duration = (finished_at - filled_at).total_seconds()

    return TradeMetrics(
        ticker=ticker,
        side=side,
        target=str(trade.get("target", "")),
        win=win,
        gross_pnl=_f(gross),
        fee_usdt=float(fee_usdt),
        fee_bnb=float(fee_bnb),
        fee_bnb_usdt=None,
        net_pnl=_f(net),
        roi_pct=_f(roi),
        duration_s=duration,
        fills_complete=fills_complete,
        notes=notes,
    )

"""
candle_utils.py — pure candle helpers (no nautilus_trader / asyncpg dependency).

Used by trade_strategy_smart.py to judge a closed candle against an entry zone.
Everything here is plain float maths, so it is trivial to unit-test.

Wick definitions (all ratios are relative to the candle's full high-low range):
    body_low    = min(open, close)
    body_high   = max(open, close)
    lower_wick  = body_low - low          (the wick pointing DOWN)
    upper_wick  = high - body_high        (the wick pointing UP)
    lower_ratio = lower_wick / (high - low)
    upper_ratio = upper_wick / (high - low)
A candle with high == low has no range: both ratios are 0.0 (no wick can be detected).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

DEFAULT_MIN_WICK_RATIO = 0.30


@dataclass(frozen=True)
class CandleShape:
    open: float
    high: float
    low: float
    close: float

    @property
    def range(self) -> float:
        return self.high - self.low

    @property
    def body_low(self) -> float:
        return min(self.open, self.close)

    @property
    def body_high(self) -> float:
        return max(self.open, self.close)

    @property
    def lower_wick(self) -> float:
        return max(self.body_low - self.low, 0.0)

    @property
    def upper_wick(self) -> float:
        return max(self.high - self.body_high, 0.0)

    @property
    def lower_wick_ratio(self) -> float:
        return self.lower_wick / self.range if self.range > 0 else 0.0

    @property
    def upper_wick_ratio(self) -> float:
        return self.upper_wick / self.range if self.range > 0 else 0.0


def candle_wicks(open_: float, high: float, low: float, close: float) -> CandleShape:
    """Return the candle with its wick measurements. Raises ValueError on an impossible candle."""
    o, h, l, c = float(open_), float(high), float(low), float(close)
    if h < l or h < max(o, c) or l > min(o, c):
        raise ValueError(f"inconsistent candle o={o} h={h} l={l} c={c}")
    return CandleShape(o, h, l, c)


# Decisions of evaluate_long_zone_candle
ENTER = "enter"
CANCEL = "cancel"
WAIT = "wait"

CASE_BODY_IN_ZONE = "A"        # the whole body is inside [SL, EP]
CASE_CLOSE_WITH_WICK = "B"     # closed inside [SL, EP] and the lower wick is >= the minimum


@dataclass(frozen=True)
class ZoneVerdict:
    decision: str              # ENTER | CANCEL | WAIT
    case: str | None           # "A" | "B" | None
    reason: str
    shape: CandleShape

    def as_metadata(self) -> Dict[str, Any]:
        s = self.shape
        return {
            "open": s.open, "high": s.high, "low": s.low, "close": s.close,
            "body_low": s.body_low, "body_high": s.body_high,
            "lower_wick_pct": round(s.lower_wick_ratio * 100, 2),
            "upper_wick_pct": round(s.upper_wick_ratio * 100, 2),
            "decision": self.decision, "case": self.case, "reason": self.reason,
        }


def evaluate_long_zone_candle(
    open_: float,
    high: float,
    low: float,
    close: float,
    ep: float,
    sl: float,
    min_wick_ratio: float = DEFAULT_MIN_WICK_RATIO,
) -> ZoneVerdict:
    """Judge one CLOSED candle for a LONG whose entry zone is [sl, ep] (sl < ep).

    Order of the checks:
      1. close <  sl                       -> CANCEL  "candle_closed_below_sl"
      2. body fully inside [sl, ep]        -> ENTER   case A "body_in_zone"
      3. close inside [sl, ep] and the lower wick (the one pointing at the SL)
         is >= min_wick_ratio of the range -> ENTER   case B "close_in_zone_with_wick"
      4. close >  ep                       -> WAIT    "close_above_zone"
      5. otherwise (closed in the zone, body partly outside, wick too small)
                                           -> WAIT    "close_in_zone_no_setup"

    Boundaries are inclusive (a close exactly on SL or EP is "inside"; only a close
    strictly below SL cancels).
    """
    if not sl < ep:
        raise ValueError(f"long zone needs sl < ep, got sl={sl} ep={ep}")
    s = candle_wicks(open_, high, low, close)

    if s.close < sl:
        return ZoneVerdict(CANCEL, None, "candle_closed_below_sl", s)

    if s.body_low >= sl and s.body_high <= ep:
        return ZoneVerdict(ENTER, CASE_BODY_IN_ZONE, "body_in_zone", s)

    close_in_zone = sl <= s.close <= ep
    if close_in_zone and s.lower_wick_ratio >= min_wick_ratio:
        return ZoneVerdict(ENTER, CASE_CLOSE_WITH_WICK, "close_in_zone_with_wick", s)

    if s.close > ep:
        return ZoneVerdict(WAIT, None, "close_above_zone", s)
    return ZoneVerdict(WAIT, None, "close_in_zone_no_setup", s)

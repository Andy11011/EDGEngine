"""Streaming SuperTrend (one instance per pair), a port of Pine's ta.supertrend.

Same usage pattern as mlmi.MLMI: call update() once per closed bar, in order.
Unlike MLMI it needs the whole candle (high / low / close), not just the close.

Reference: TradingView ta.supertrend(factor, atrPeriod)
  - ATR is Wilder's RMA, seeded with the SMA of the first `period` true ranges
  - direction starts as "down" until ATR is available
  - bands only ratchet toward price unless the previous close broke through them
  - trend flips when close crosses the opposite (final) band
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class SuperTrendResult:
    uptrend: bool           # True = bullish (Pine direction < 0)
    value: float            # the SuperTrend line (lower band in uptrend, upper in downtrend)
    flipped: bool           # trend changed on this bar


class SuperTrend:
    def __init__(self, period: int = 10, factor: float = 3.0):
        if period < 1:
            raise ValueError("period must be >= 1")
        self.period = period
        self.factor = factor

        self._n = 0                                   # bars seen
        self._tr_sum = 0.0
        self._atr: Optional[float] = None
        self._prev_close: Optional[float] = None
        self._prev_atr_ready = False                  # Pine: not na(atr[1])
        self._prev_upper = 0.0                        # previous final bands
        self._prev_lower = 0.0
        self._uptrend = False                         # Pine direction starts at 1 (= down)

    @property
    def bars_to_ready(self) -> int:
        return self.period + 1

    def update(self, high: float, low: float, close: float) -> Optional[SuperTrendResult]:
        # ---- true range / Wilder ATR (Pine ta.atr = rma(tr, period)) ----
        pc = self._prev_close
        tr = (high - low) if pc is None else max(high - low, abs(high - pc), abs(low - pc))
        self._n += 1
        if self._n <= self.period:
            self._tr_sum += tr
            self._atr = self._tr_sum / self.period if self._n == self.period else None
        else:
            self._atr = (self._atr * (self.period - 1) + tr) / self.period

        atr = self._atr
        if atr is None:                               # ATR not available yet
            self._prev_close = close
            return None

        # ---- bands ----
        hl2 = (high + low) / 2.0
        upper = hl2 + self.factor * atr
        lower = hl2 - self.factor * atr
        if self._prev_atr_ready:                      # first banded bar: nothing to ratchet against
            if not (lower > self._prev_lower or pc < self._prev_lower):
                lower = self._prev_lower
            if not (upper < self._prev_upper or pc > self._prev_upper):
                upper = self._prev_upper

        # ---- direction ----
        was_up = self._uptrend
        if not self._prev_atr_ready:
            up = False                                # Pine: na(atr[1]) -> direction = 1 (down)
        elif was_up:
            up = not (close < lower)
        else:
            up = close > upper

        self._uptrend = up
        self._prev_upper, self._prev_lower = upper, lower
        self._prev_atr_ready = True
        self._prev_close = close
        return SuperTrendResult(uptrend=up, value=lower if up else upper, flipped=(up != was_up))

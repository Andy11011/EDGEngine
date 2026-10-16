"""Streaming "Z-Score Volume with CVD Clustering" (one instance per pair).

Port of the dashboard's Z-Score-volume-with-CVD-Clustering.js, bar for bar, so a
"Strong Bullish" marker on the chart and a "Strong Bullish" here are the same event.

Per closed bar:
  z      = (volume - SMA(volume, length)) / population-stdev(volume, length)
  cvd    = signed volume: +volume if close > open, -volume if close < open, else 0
  cluster (needs >= 10 samples, thresholds = 25th / 75th percentile of the last
  `max_points` samples, current bar included):
      z >= Q3(z) and cvd >= Q3(cvd)  -> "Strong Bullish"
      z >= Q3(z) and cvd <= Q1(cvd)  -> "Bearish Divergence"
      otherwise                      -> "Neutral"

cvd_mode decides what the cvd sample is:
  "anchored" (default, what the JS does): running CVD since the start of the anchor
             period (UTC day by default), i.e. a cumulative value.
  "bar":     only this bar's signed volume. Closer to the Pine original, whose
             cvdDelta = closeCVD - openCVD is the delta of the current bar, but
             still approximated from candle direction (Pine uses 1-minute
             intrabar data via ta.requestVolumeDelta).
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Deque, Optional

STRONG_BULLISH = "Strong Bullish"
BEARISH_DIVERGENCE = "Bearish Divergence"
NEUTRAL = "Neutral"

_MIN_SAMPLES = 10


@dataclass(frozen=True)
class ZScoreVolumeResult:
    z: Optional[float]            # volume z-score (None while std == 0 or window not full)
    cvd: float                    # the CVD sample used for clustering (see cvd_mode)
    label: Optional[str]          # STRONG_BULLISH / BEARISH_DIVERGENCE / NEUTRAL, None until warm

    @property
    def strong_bullish(self) -> bool:
        return self.label == STRONG_BULLISH


def _anchor_key(ts_ms: int, anchor: str):
    d = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    if anchor == "1D":
        return (d.year, d.month, d.day)
    if anchor == "1W":
        iso_year, iso_week, _ = d.isocalendar()
        return (iso_year, iso_week)
    if anchor == "1M":
        return (d.year, d.month)
    return "all"


def _sorted_finite(values) -> list[float]:
    return sorted(v for v in values if v is not None and math.isfinite(v))


def _percentile(sorted_vals: list[float], pct: float) -> Optional[float]:
    """Linear-interpolation percentile (same as Pine / the JS calculatePercentile)."""
    if not sorted_vals:
        return None
    idx = (pct / 100.0) * (len(sorted_vals) - 1)
    lo, hi = math.floor(idx), math.ceil(idx)
    if lo == hi:
        return sorted_vals[lo]
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (idx - lo)


class ZScoreVolume:
    def __init__(
        self,
        length: int = 20,
        max_points: int = 200,
        anchor: str = "1D",
        cvd_mode: str = "anchored",
    ):
        if length < 2:
            raise ValueError("length must be >= 2")
        if cvd_mode not in ("anchored", "bar"):
            raise ValueError("cvd_mode must be 'anchored' or 'bar'")
        self.length = length
        self.anchor = anchor
        self.cvd_mode = cvd_mode

        self._idx = -1                                    # 0-based bar index
        self._window: Deque[float] = deque(maxlen=length)
        self._z_hist: Deque[Optional[float]] = deque(maxlen=max_points)
        self._cvd_hist: Deque[float] = deque(maxlen=max_points)
        self._anchor_key = None
        self._running_cvd = 0.0

    @property
    def bars_to_ready(self) -> int:
        return self.length + _MIN_SAMPLES

    def update(self, ts_ms: int, open_: float, close: float, volume: float) -> ZScoreVolumeResult:
        self._idx += 1
        signed = volume if close > open_ else -volume if close < open_ else 0.0

        key = _anchor_key(ts_ms, self.anchor)
        if key != self._anchor_key:
            self._anchor_key = key
            self._running_cvd = 0.0
        self._running_cvd += signed
        cvd = self._running_cvd if self.cvd_mode == "anchored" else signed

        # z-score over the last `length` volumes (population stdev, like Pine's ta.stdev)
        self._window.append(volume)
        z: Optional[float] = None
        if len(self._window) == self.length:
            mean = sum(self._window) / self.length
            var = sum((v - mean) ** 2 for v in self._window) / self.length
            std = math.sqrt(var)
            if std > 0:
                z = (volume - mean) / std

        label: Optional[str] = None
        if self._idx >= self.length:
            self._z_hist.append(z)
            self._cvd_hist.append(cvd)
            if len(self._z_hist) >= _MIN_SAMPLES:
                z_q3 = _percentile(_sorted_finite(self._z_hist), 75)
                cvd_sorted = _sorted_finite(self._cvd_hist)
                cvd_q1 = _percentile(cvd_sorted, 25)
                cvd_q3 = _percentile(cvd_sorted, 75)
                if z is not None and z_q3 is not None and cvd_q1 is not None and cvd_q3 is not None:
                    if z >= z_q3 and cvd >= cvd_q3:
                        label = STRONG_BULLISH
                    elif z >= z_q3 and cvd <= cvd_q1:
                        label = BEARISH_DIVERGENCE
                    else:
                        label = NEUTRAL

        return ZScoreVolumeResult(z=z, cvd=cvd, label=label)

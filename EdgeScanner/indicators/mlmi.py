"""Machine Learning Momentum Index (MLMI) - streaming Python port.

Port of the TradingView indicator "Machine Learning Momentum Index (MLMI)
[Zeiierman]" (Pine v6, (c) Zeiierman, CC BY-NC-SA 4.0 -
https://creativecommons.org/licenses/by-nc-sa/4.0/). As a derivative work this
module carries the same licence: attribution, non-commercial use, share-alike.

The algorithm is k-Nearest-Neighbours (NOT k-means clustering):

  1. Features (every bar), both on a 0-100 scale like TradingView's RSI:
         slow = WMA(RSI(close, 20), momentum_window)
         quick = WMA(RSI(close, 5),  momentum_window)
  2. Trigger: WMA(close, 5) crossing WMA(close, 20) in either direction.
     At each trigger the (slow, quick, close) point is stored as a training
     sample and the previous sample receives a +1 / -1 label (close >= its close).
  3. Prediction (every bar): the labels of the k samples nearest to the current
     (slow, quick) point are summed -> the MLMI line.
  4. Adaptive bands from the last `band_window` predictions, zone state machines
     and four level-crossing signals (see SIGNAL_TYPES).

One `MLMI` instance holds the state of ONE symbol / timeframe. Feed it closed
bars in chronological order via `update(close)`.

Differences from the Pine script (all deliberate, to make it safe for a live
scanner):
  * Bars before both features exist are skipped (Pine would store `na`).
  * The highest/lowest band uses an expanding window until `band_window` bars
    exist (Pine's `ta.highest` is `na` until then). Signals are suppressed until
    `ready_after` predictions exist so the bands are meaningful.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Optional

import numpy as np

# -----------------------------------------------------------------------------
# Constants hard-coded in the Pine script
# -----------------------------------------------------------------------------
FAST_MA_LEN = 5
SLOW_MA_LEN = 20
FAST_RSI_LEN = 5
SLOW_RSI_LEN = 20
PREDICTION_MA_LEN = 20
VOLATILITY_LEN = 20

# Signal names: the prediction line crossing the two adaptive levels.
#   OB_CROSSOVER   prediction crosses OVER  the overbought level
#   OB_CROSSUNDER  prediction crosses UNDER the overbought level
#   OS_CROSSOVER   prediction crosses OVER  the oversold level  (also starts the "confirmation" zone)
#   OS_CROSSUNDER  prediction crosses UNDER the oversold level
SIGNAL_TYPES: tuple[str, ...] = (
    "OB_CROSSOVER", "OB_CROSSUNDER", "OS_CROSSOVER", "OS_CROSSUNDER",
)


# -----------------------------------------------------------------------------
# Small streaming building blocks
# -----------------------------------------------------------------------------
class _WMA:
    """Linearly weighted moving average (newest value has the largest weight)."""

    __slots__ = ("n", "_w", "_buf")

    def __init__(self, n: int):
        self.n = n
        self._w = np.arange(1, n + 1, dtype=float) / (n * (n + 1) / 2)
        self._buf: deque[float] = deque(maxlen=n)

    def update(self, x: float) -> Optional[float]:
        self._buf.append(x)
        if len(self._buf) < self.n:
            return None
        return float(np.dot(self._w, self._buf))


class _RSI:
    """Wilder RSI on a 0-100 scale, seeded with an SMA like Pine's ta.rma."""

    __slots__ = ("n", "_prev", "_up", "_dn", "_seed_up", "_seed_dn", "_count")

    def __init__(self, n: int):
        self.n = n
        self._prev: Optional[float] = None
        self._up: Optional[float] = None
        self._dn: Optional[float] = None
        self._seed_up = 0.0
        self._seed_dn = 0.0
        self._count = 0

    def update(self, close: float) -> Optional[float]:
        if self._prev is None:
            self._prev = close
            return None
        change = close - self._prev
        self._prev = close
        up = change if change > 0 else 0.0
        dn = -change if change < 0 else 0.0

        if self._up is None:
            self._count += 1
            self._seed_up += up
            self._seed_dn += dn
            if self._count < self.n:
                return None
            self._up = self._seed_up / self.n
            self._dn = self._seed_dn / self.n
        else:
            a = 1.0 / self.n
            self._up += a * (up - self._up)
            self._dn += a * (dn - self._dn)

        if self._dn == 0:
            return 100.0
        if self._up == 0:
            return 0.0
        return 100.0 - 100.0 / (1.0 + self._up / self._dn)


class _EMA:
    """EMA seeded with the first value (matches Pine's ta.ema)."""

    __slots__ = ("alpha", "value")

    def __init__(self, n: int):
        self.alpha = 2.0 / (n + 1)
        self.value: Optional[float] = None

    def update(self, x: float) -> float:
        if self.value is None:
            self.value = x
        else:
            self.value = self.alpha * x + (1.0 - self.alpha) * self.value
        return self.value


class _RollingMax:
    """O(1) amortised rolling maximum over the last n values."""

    __slots__ = ("n", "_i", "_dq")

    def __init__(self, n: int):
        self.n = n
        self._i = 0
        self._dq: deque[tuple[int, float]] = deque()

    def update(self, x: float) -> float:
        i = self._i
        self._i += 1
        dq = self._dq
        while dq and dq[-1][1] <= x:
            dq.pop()
        dq.append((i, x))
        while dq[0][0] <= i - self.n:
            dq.popleft()
        return dq[0][1]


# -----------------------------------------------------------------------------
# k-NN memory
# -----------------------------------------------------------------------------
class _KNNMemory:
    """Training samples + k-NN vote.

    aligned_labels=False  Faithful to the Pine script. Arrays start with one
        all-zero seed sample, and a label pushed at trigger N describes the move
        of sample N-1 but sits next to sample N's features (one-step offset);
        the newest sample is also part of the vote.
    aligned_labels=True   Each sample gets its OWN outcome label (close at the
        next trigger >= its close). The newest, still-unlabelled sample is held
        back and not used for voting.
    """

    def __init__(self, aligned_labels: bool):
        self.aligned = aligned_labels
        cap = 64
        self._x = np.zeros(cap)
        self._y = np.zeros(cap)
        self._price = np.zeros(cap)
        self._label = np.zeros(cap)
        # Pine seeds every array with a single 0 element
        self._n = 0 if aligned_labels else 1
        self._pending: Optional[tuple[float, float, float]] = None

    def __len__(self) -> int:
        return self._n

    def _ensure(self, size: int) -> None:
        cap = len(self._x)
        if size <= cap:
            return
        new_cap = max(size, cap * 2)
        for name in ("_x", "_y", "_price", "_label"):
            old = getattr(self, name)
            grown = np.zeros(new_cap)
            grown[:cap] = old
            setattr(self, name, grown)

    def add_trigger(self, p1: float, p2: float, close: float) -> None:
        if self.aligned:
            if self._pending is not None:
                px, py, pp = self._pending
                n = self._n
                self._ensure(n + 1)
                self._x[n], self._y[n], self._price[n] = px, py, pp
                self._label[n] = 1.0 if close >= pp else -1.0
                self._n = n + 1
            self._pending = (p1, p2, close)
            return

        # Faithful port of storePreviousTrade()
        n = self._n
        self._ensure(n + 1)
        self._x[n] = self._x[n - 1]
        self._y[n] = self._y[n - 1]
        self._price[n] = self._price[n - 1]
        self._label[n] = 1.0 if close >= self._price[n - 1] else -1.0
        self._n = n + 1
        self._x[n], self._y[n], self._price[n] = p1, p2, close

    def predict(self, p1: float, p2: float, k: int) -> float:
        n = self._n
        if n == 0:
            return 0.0
        dist = np.hypot(self._x[:n] - p1, self._y[:n] - p2)
        kk = min(k, n)
        max_dist = np.partition(dist, kk - 1)[kk - 1]        # distance of the k-th neighbour
        return float(self._label[:n][dist <= max_dist].sum())  # ties at the cutoff all vote


# -----------------------------------------------------------------------------
# Result
# -----------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class MLMIResult:
    ready: bool                          # bands warm; signals / zones are meaningful
    prediction: float                    # the MLMI line
    prev_prediction: Optional[float]
    prediction_ma: Optional[float]       # WMA(prediction, 20)
    upper: Optional[float]               # highest prediction in the band window
    lower: Optional[float]               # lowest prediction in the band window
    ob_level: Optional[float]            # Pine upper_  (overbought boundary)
    os_level: Optional[float]            # Pine lower_  (oversold boundary)
    buy_zone: bool                       # prediction in / recovering from oversold
    sell_zone: bool                      # prediction in / recovering from overbought
    middle_zone: bool
    confirmation_zone: bool              # after OS_CROSSOVER until recovery threshold
    signals: tuple[str, ...]             # subset of SIGNAL_TYPES firing on this bar

    @property
    def zone(self) -> str:
        if self.buy_zone:
            return "BUY"
        if self.sell_zone:
            return "SELL"
        if self.middle_zone:
            return "MIDDLE"
        return "NONE"


def _crossover(a: float, b: float, pa: Optional[float], pb: Optional[float]) -> bool:
    return pa is not None and pb is not None and a > b and pa <= pb


def _crossunder(a: float, b: float, pa: Optional[float], pb: Optional[float]) -> bool:
    return pa is not None and pb is not None and a < b and pa >= pb


# -----------------------------------------------------------------------------
# Indicator
# -----------------------------------------------------------------------------
class MLMI:
    def __init__(
        self,
        *,
        num_neighbors: int = 200,
        momentum_window: int = 20,
        band_window: int = 2000,
        band_min_bars: int = 200,
        zone_threshold: float = 0.0,
        confirmation_threshold: float = 0.0,
        aligned_labels: bool = False,
    ):
        if num_neighbors < 1:
            raise ValueError("num_neighbors must be >= 1")
        if momentum_window < 1:
            raise ValueError("momentum_window must be >= 1")
        if band_window < VOLATILITY_LEN:
            raise ValueError(f"band_window must be >= {VOLATILITY_LEN}")
        if band_min_bars < 1:
            raise ValueError("band_min_bars must be >= 1")

        self.k = num_neighbors
        self.momentum_window = momentum_window
        self.zone_threshold = zone_threshold
        self.confirmation_threshold = confirmation_threshold
        self._ready_after = max(band_min_bars, PREDICTION_MA_LEN, VOLATILITY_LEN)

        # trigger
        self._ma_fast = _WMA(FAST_MA_LEN)
        self._ma_slow = _WMA(SLOW_MA_LEN)
        self._prev_ma: Optional[tuple[float, float]] = None
        # features
        self._rsi_slow = _RSI(SLOW_RSI_LEN)
        self._rsi_quick = _RSI(FAST_RSI_LEN)
        self._feat_slow = _WMA(momentum_window)
        self._feat_quick = _WMA(momentum_window)
        # model
        self._knn = _KNNMemory(aligned_labels)
        # prediction post-processing
        self._pred_ma = _WMA(PREDICTION_MA_LEN)
        self._pred_max = _RollingMax(band_window)
        self._pred_neg_max = _RollingMax(band_window)   # min(x) == -max(-x)
        self._pred_win: deque[float] = deque(maxlen=VOLATILITY_LEN)
        self._vol_ema = _EMA(VOLATILITY_LEN)
        self._pred_count = 0
        # previous-bar values for crossings
        self._prev_pred: Optional[float] = None
        self._prev_pred_ma: Optional[float] = None
        self._prev_ob: Optional[float] = None
        self._prev_os: Optional[float] = None
        # persistent zone state (Pine `var`s)
        self._buy = False
        self._sell = False
        self._mid = False
        self._confirm = False

    @property
    def bars_to_ready(self) -> int:
        """Closed bars needed before signals can fire (features + band warm-up)."""
        return SLOW_RSI_LEN + self.momentum_window + self._ready_after - 1

    @property
    def num_samples(self) -> int:
        return len(self._knn)

    def update(self, close: float) -> Optional[MLMIResult]:
        """Feed one closed bar. Returns None until both features exist."""
        # ---- trigger: fast/slow WMA of price crossing in either direction ----
        ma_f = self._ma_fast.update(close)
        ma_s = self._ma_slow.update(close)
        trigger = False
        if ma_f is not None and ma_s is not None:
            if self._prev_ma is not None:
                pf, ps = self._prev_ma
                trigger = (ma_f > ma_s and pf <= ps) or (ma_f < ma_s and pf >= ps)
            self._prev_ma = (ma_f, ma_s)

        # ---- features ----
        r_s = self._rsi_slow.update(close)
        r_q = self._rsi_quick.update(close)
        f_slow = self._feat_slow.update(r_s) if r_s is not None else None
        f_quick = self._feat_quick.update(r_q) if r_q is not None else None
        if f_slow is None or f_quick is None:
            return None

        # ---- learn (on trigger) then predict (every bar) ----
        if trigger:
            self._knn.add_trigger(f_slow, f_quick, close)
        pred = self._knn.predict(f_slow, f_quick, self.k)

        # ---- smoothing / adaptive bands ----
        self._pred_count += 1
        pred_ma = self._pred_ma.update(pred)
        upper = self._pred_max.update(pred)
        lower = -self._pred_neg_max.update(-pred)
        self._pred_win.append(pred)
        vol: Optional[float] = None
        if len(self._pred_win) == VOLATILITY_LEN:
            vol = self._vol_ema.update(float(np.std(self._pred_win)))   # population stdev, like Pine
        ob = upper - vol if vol is not None else None
        os_ = lower + vol if vol is not None else None

        ready = (
            self._pred_count >= self._ready_after
            and pred_ma is not None
            and ob is not None
            and os_ is not None
        )

        signals: list[str] = []
        if ready:
            ob_over = _crossover(pred, ob, self._prev_pred, self._prev_ob)
            ob_under = _crossunder(pred, ob, self._prev_pred, self._prev_ob)
            os_over = _crossover(pred, os_, self._prev_pred, self._prev_os)
            os_under = _crossunder(pred, os_, self._prev_pred, self._prev_os)

            for flag, name in (
                (ob_over, "OB_CROSSOVER"), (ob_under, "OB_CROSSUNDER"),
                (os_over, "OS_CROSSOVER"), (os_under, "OS_CROSSUNDER"),
            ):
                if flag:
                    signals.append(name)

            # confirmation zone: starts on OS_CROSSOVER
            if os_over:
                self._confirm = True
            if pred >= self.confirmation_threshold:
                self._confirm = False
            if pred < os_:
                self._confirm = False

            # buy / sell / middle zones (same order as the Pine script)
            if pred < os_:
                self._buy = True
                self._mid = False
            elif pred >= self.zone_threshold:
                self._buy = False

            if pred > ob:
                self._sell = True
                self._mid = False
            elif pred <= self.zone_threshold:
                self._sell = False

            if os_ <= pred <= ob:
                self._mid = True
                self._buy = False
                self._sell = False

        result = MLMIResult(
            ready=ready,
            prediction=pred,
            prev_prediction=self._prev_pred,
            prediction_ma=pred_ma,
            upper=upper,
            lower=lower,
            ob_level=ob,
            os_level=os_,
            buy_zone=self._buy,
            sell_zone=self._sell,
            middle_zone=self._mid,
            confirmation_zone=self._confirm,
            signals=tuple(signals),
        )

        self._prev_pred = pred
        self._prev_pred_ma = pred_ma
        self._prev_ob = ob
        self._prev_os = os_
        return result

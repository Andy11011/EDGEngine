"""
portfolio_metrics.py — year-to-date portfolio statistics from closed trades.

Pure functions, no DB / FastAPI dependency (same idea as trade_metrics.py and
position_sizing.py): easy to unit test, and the single place where PnL, win
rate, drawdown, CAGR, Calmar, Sharpe, Sortino, Rdyn and the REAL/PAPER switch
are defined for the API.

The formulas are a port of Trading_portfolio.gs (plotPnL, plotEquityCurvePercent,
plotTradingModeAndRisk + its utility functions), so the API and the Google
Sheet agree. Where the script is inconsistent with itself the choice made here
is noted next to the code.

Input: one `ClosedTrade` per trade that closed inside the period, in any order
(they are sorted by close time here), plus the equity the period started with.

Definitions
-----------
    pnl_i        net PnL of the trade in USDT (trade_metrics.net_pnl)
    equity_i     start_equity + sum(pnl_1..pnl_i)
    roi_i        pnl_i / equity_(i-1)          (return on ACCOUNT equity before
                 the trade, like Day_Trading.gs: profit / previous balance — NOT
                 the return on trade notional that trade_metrics.roi_pct reports)
    win          pnl_i > 0
    gains/losses roi_i > 0 / roi_i < 0 (breakeven is neither)

    win_rate     wins / trades
    ER           wr * avg_gain + (1 - wr) * avg_loss         (per trade, on roi)
    Sharpe       mean(roi) / std(roi)         population std; per trade, no
                                              risk-free rate, not annualised
    Sortino      mean(roi) / sqrt(mean(roi^2 over roi < 0))  (denominator counts
                                              the losing trades only)
    max_dd       max over the equity curve (incl. the starting point) of
                 (peak - equity) / peak
    duration     calendar days from the first to the last close, inclusive
    CAGR         (end/start)^(365/duration) - 1
    Calmar       CAGR / max_dd                               (fractions)
    monthly rate trades / duration * 30.44
    Rdyn (%)     Rmin + (Rmax - Rmin) / (1 + exp(-k * (CAGR% - Cmid)))
    trade_mode   starts REAL. REAL -> PAPER when drawdown from the running peak
                 >= 15% or 10 losses in a row. PAPER -> REAL when equity is back
                 at its running peak (new all-time high); streak resets.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Sequence

DAYS_PER_MONTH = 30.44
DAYS_PER_YEAR = 365.0


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except ValueError:
        return default


@dataclass(frozen=True)
class RdynParams:
    """Sigmoid risk curve; defaults are the constants in plotTradingModeAndRisk()."""
    r_min: float = field(default_factory=lambda: _env_float("RDYN_R_MIN", 0.1))    # % risk at low CAGR
    r_max: float = field(default_factory=lambda: _env_float("RDYN_R_MAX", 1.0))    # % risk at high CAGR
    c_mid: float = field(default_factory=lambda: _env_float("RDYN_C_MID", 2.5))    # CAGR % where risk is halfway
    k: float = field(default_factory=lambda: _env_float("RDYN_K", 1.5))            # slope


@dataclass(frozen=True)
class ModeParams:
    """REAL -> PAPER triggers, as MAX_DD_THRESHOLD / CONSECUTIVE_LOSS_THRESHOLD in the script."""
    max_dd: float = field(default_factory=lambda: _env_float("MODE_MAX_DD", 0.15))
    max_consecutive_losses: int = field(default_factory=lambda: int(_env_float("MODE_MAX_LOSSES", 10)))


@dataclass(frozen=True)
class ClosedTrade:
    finished_at: datetime
    net_pnl: float


@dataclass(frozen=True)
class PortfolioStats:
    trades_count: int
    wins: int
    losses: int                       # net_pnl <= 0 (what feeds the loss-streak counter)
    pnl: float                        # USDT
    start_equity: float
    end_equity: float
    win_rate_pct: Optional[float]
    monthly_trades_rate: Optional[float]
    equity_percent: float             # (end / start - 1) * 100
    max_dd_percent: float
    max_dd_usdt: float
    avg_gain_pct: Optional[float]     # mean roi of winning trades, in %
    avg_loss_pct: Optional[float]     # mean roi of losing trades, in % (negative)
    er_pct: Optional[float]           # expected return per trade, in % of equity
    cagr_pct: Optional[float]         # None if it overflows or cannot be defined
    calmar: Optional[float]
    sharpe: Optional[float]
    sortino: Optional[float]
    trade_mode: int                   # 1 = REAL, 0 = PAPER
    rdyn_pct: float                   # risk per trade in % (risk_ratio * 100)
    duration_days: int
    first_close: Optional[datetime]
    last_close: Optional[datetime]


def _mean(xs: Sequence[float]) -> float:
    return sum(xs) / len(xs)


def _std_pop(xs: Sequence[float]) -> float:
    m = _mean(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / len(xs))


def _downside_dev(xs: Sequence[float]) -> float:
    neg = [x for x in xs if x < 0]
    if not neg:
        return 0.0
    return math.sqrt(sum(x * x for x in neg) / len(neg))


def _sigmoid(x: float) -> float:
    if x >= 0:
        return 1.0 / (1.0 + math.exp(-x))
    e = math.exp(x)
    return e / (1.0 + e)


def rdyn_from_cagr(cagr_pct: float, p: Optional[RdynParams] = None) -> float:
    """Rdyn in % for a given CAGR in %. cagr_pct may be +/-inf (saturates at r_max / r_min)."""
    p = p or RdynParams()
    if math.isinf(cagr_pct):
        s = 1.0 if cagr_pct > 0 else 0.0
    else:
        s = _sigmoid(p.k * (cagr_pct - p.c_mid))
    return p.r_min + (p.r_max - p.r_min) * s


def _cagr_pct_raw(start: float, end: float, days: int) -> float:
    years = days / DAYS_PER_YEAR
    if years <= 0 or start <= 0:
        return 0.0
    if end <= 0:
        return -100.0
    exponent = math.log(end / start) / years
    if exponent > 700:                      # exp() would overflow
        return math.inf
    return (math.exp(exponent) - 1.0) * 100.0


def compute_portfolio_stats(
    trades: Sequence[ClosedTrade],
    start_equity: float,
    *,
    rdyn: Optional[RdynParams] = None,
    mode: Optional[ModeParams] = None,
) -> PortfolioStats:
    if not (start_equity > 0) or math.isinf(start_equity):
        raise ValueError(f"start_equity must be a positive number, got {start_equity}")
    rdyn = rdyn or RdynParams()
    mode = mode or ModeParams()

    ordered: List[ClosedTrade] = sorted(trades, key=lambda t: t.finished_at)

    equity = start_equity
    peak = start_equity            # the script keeps peakEquity and allTimeHighEquity; they are always equal
    max_dd = 0.0
    max_dd_usdt = 0.0
    rois: List[float] = []
    wins = 0
    losing_streak = 0
    real = True

    for t in ordered:
        roi = t.net_pnl / equity if equity > 0 else -1.0
        rois.append(roi)
        equity += t.net_pnl

        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak if peak > 0 else 0.0
        max_dd = max(max_dd, dd)
        max_dd_usdt = max(max_dd_usdt, peak - equity)

        if t.net_pnl > 0:
            wins += 1
            losing_streak = 0
        else:
            losing_streak += 1

        if real:
            if dd >= mode.max_dd or losing_streak >= mode.max_consecutive_losses:
                real = False
        elif equity >= peak:       # back at the all-time high
            real = True
            losing_streak = 0

    n = len(ordered)
    pnl = equity - start_equity

    if n:
        first, last = ordered[0].finished_at, ordered[-1].finished_at
        duration_days = (last.date() - first.date()).days + 1
    else:
        first = last = None
        duration_days = 0

    gains = [r for r in rois if r > 0]
    losses = [r for r in rois if r < 0]
    avg_gain = _mean(gains) if gains else 0.0
    avg_loss = _mean(losses) if losses else 0.0

    win_rate = wins / n if n else None
    er = win_rate * avg_gain + (1 - win_rate) * avg_loss if n else None

    std = _std_pop(rois) if n else 0.0
    sharpe = _mean(rois) / std if n and std > 0 else None
    dd_dev = _downside_dev(rois) if n else 0.0
    sortino = _mean(rois) / dd_dev if n and dd_dev > 0 else None

    cagr_raw = _cagr_pct_raw(start_equity, equity, duration_days) if n else 0.0
    cagr_pct = cagr_raw if math.isfinite(cagr_raw) else None
    calmar = (cagr_raw / 100.0) / max_dd if math.isfinite(cagr_raw) and max_dd > 0 else None

    return PortfolioStats(
        trades_count=n,
        wins=wins,
        losses=n - wins,
        pnl=pnl,
        start_equity=start_equity,
        end_equity=equity,
        win_rate_pct=win_rate * 100 if win_rate is not None else None,
        monthly_trades_rate=(n / duration_days) * DAYS_PER_MONTH if n else None,
        equity_percent=(equity / start_equity - 1.0) * 100.0,
        max_dd_percent=max_dd * 100.0,
        max_dd_usdt=max_dd_usdt,
        avg_gain_pct=avg_gain * 100.0 if n else None,
        avg_loss_pct=avg_loss * 100.0 if n else None,
        er_pct=er * 100.0 if er is not None else None,
        cagr_pct=cagr_pct,
        calmar=calmar,
        sharpe=sharpe,
        sortino=sortino,
        trade_mode=1 if real else 0,
        rdyn_pct=rdyn_from_cagr(cagr_raw, rdyn),
        duration_days=duration_days,
        first_close=first,
        last_close=last,
    )

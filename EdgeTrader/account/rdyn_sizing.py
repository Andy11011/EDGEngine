"""
rdyn_sizing.py — risk ratio for position sizing, computed on the fly from Rdyn.

Rdyn (portfolio_metrics.rdyn_from_cagr) is a sigmoid of the year-to-date CAGR of
the closed trades. Each node process calls resolve_risk_ratio() right before it
sizes an order, so the risk per trade follows the node's own track record:

    virtual node -> stats of the year's closed VIRTUAL trades
    real node    -> stats of the year's closed REAL trades

This is exactly what the API reports for GET /rdyn?target=virtual|real (same
trades, same formulas, same start-equity rules), so what the API shows for that
target is what the node uses. GET /rdyn with the default target=all is a
combined figure that no node uses.

Start equity of the year (same rules as main-api.py):
    virtual: the configured virtual balance (trades_config.virtual_balance_usdt)
    real:    current free USDT balance - the year's (pnl - fees) of real trades
             (the balance already has the fees taken out)

Safety: this function NEVER raises. If anything is missing or fails (DB error,
no valid start equity, too few trades, disabled by env) it returns the static
trades_config.risk_ratio and says why in `source`, so sizing keeps working
exactly as before.

Env vars:
  RDYN_SIZING_ENABLED   default "1"   "0"/"false"/"off" -> always use trades_config.risk_ratio
  RDYN_MIN_TRADES       default 0     fewer closed trades this year than this -> static risk_ratio
                                      (CAGR annualised over a few days is very noisy; raise this
                                      if you do not want 1-2 early trades to swing the risk)
  RDYN_R_MIN / RDYN_R_MAX / RDYN_C_MID / RDYN_K   curve constants (see portfolio_metrics.py)
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, List

from portfolio_metrics import ClosedTrade, compute_portfolio_stats
from trade_metrics import compute_trade_metrics


@dataclass(frozen=True)
class RiskDecision:
    risk_ratio: float          # fraction, e.g. 0.004 = 0.4% of equity at risk per trade
    source: str                # "rdyn" | "static:<reason>"
    detail: str = ""


def _log(text: str) -> None:
    print(text, file=sys.stderr, flush=True)


def _enabled() -> bool:
    return os.getenv("RDYN_SIZING_ENABLED", "1").strip().lower() not in ("0", "false", "no", "off")


def _min_trades() -> int:
    try:
        return int(os.getenv("RDYN_MIN_TRADES", "0"))
    except ValueError:
        return 0


async def _closed_trades_ytd(db: Any, target: str, year: int) -> List[ClosedTrade]:
    trades = await db.get_trades_history(
        limit=100_000, target=target, status="Closed", include_events=True,
        finished_from=datetime(year, 1, 1), finished_to=datetime(year + 1, 1, 1),  # naive UTC, like trade_events
    )
    closed: List[ClosedTrade] = []
    for t in trades:
        try:
            m = compute_trade_metrics(t, t.get("events", []))
        except Exception as e:
            _log(f"⚠️ rdyn: metrics failed for {t.get('trade_id')}: {type(e).__name__}: {e}")
            m = None
        if m is None or m.pnl is None or t.get("finished_at") is None:
            continue
        closed.append(ClosedTrade(finished_at=t["finished_at"], pnl=m.pnl, fees=m.fee_usdt, target=m.target))
    return closed


async def resolve_risk_ratio(
    db: Any,
    target: str,                 # "virtual" | "real"
    current_equity: float,       # the equity this order is sized against
    static_risk_ratio: float,    # trades_config.risk_ratio, the fallback
) -> RiskDecision:
    if not _enabled():
        return RiskDecision(static_risk_ratio, "static:disabled")
    try:
        year = datetime.now(timezone.utc).year
        closed = await _closed_trades_ytd(db, target, year)

        if target == "virtual":
            equity0 = float(current_equity)
        elif target == "real":
            equity0 = float(current_equity) - sum(c.pnl - c.fees for c in closed)
        else:
            return RiskDecision(static_risk_ratio, f"static:unsupported target {target!r}")

        if not equity0 > 0:
            return RiskDecision(static_risk_ratio, "static:no valid start equity", f"equity0={equity0}")

        min_trades = _min_trades()
        if len(closed) < min_trades:
            return RiskDecision(
                static_risk_ratio, "static:too few trades",
                f"{len(closed)} closed trades in {year} < RDYN_MIN_TRADES={min_trades}",
            )

        stats = compute_portfolio_stats(closed, equity0)
        risk_ratio = stats.rdyn_pct / 100.0
        if not (risk_ratio > 0) or risk_ratio != risk_ratio:
            return RiskDecision(static_risk_ratio, "static:invalid rdyn", f"rdyn_pct={stats.rdyn_pct}")

        cagr = "n/a" if stats.cagr_pct is None else f"{stats.cagr_pct:.2f}%"
        return RiskDecision(
            risk_ratio, "rdyn",
            f"{target} {year}: {stats.trades_count} trades, CAGR {cagr}, start equity {equity0:.2f}",
        )
    except Exception as e:
        _log(f"⚠️ rdyn: could not compute ({type(e).__name__}: {e}); using static risk_ratio")
        return RiskDecision(static_risk_ratio, "static:error", f"{type(e).__name__}: {e}")

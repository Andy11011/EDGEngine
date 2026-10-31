"""
main-api.py — the single, centralized, stateless read-api service from
MultiVenueTOD.md. Replaces api.py + the dashboard-facing half of the old
EDGETrader.py.

This module defines ONLY the FastAPI `app` and its routes — no server
startup code. Run it via edge-point.py (this file's hyphen means it can't
be imported with a normal `import` statement, so edge-point.py loads it by
file path via importlib instead — see that file for details).

Deliberately holds ONLY a Postgres connection pool:
  - no AWS credentials (no boto3, no Secrets Manager, no SQS send)
  - no trading logic (no nautilus_trader import at all)
  - no reference to the node processes (binance_real_node.py,
    binance_virtual_mainnet_node.py) — it can't be, they're separate
    containers now.

Everything it serves comes from Postgres:
  - /health           — node_heartbeats rows (written by the node processes
                         every ~15s) + a trivial DB ping.
  - /balance/virtual   — trades_config.virtual_balance_usdt (plain DB read);
                         PUT on the same path updates it (virtual only).
  - /balance/mainnet   — the real node's latest heartbeat balance snapshot.
  - /active_trades     — derived from trade_events (Opened without a later
                         Closed/Cancelled), not in-memory process state.
  - /trades_history    — every trade (open, closed, cancelled), one row per
                         trade plus its full event-sourced chain (entry
                         submitted/opened/filled, SL/TP placed, SL/TP hit,
                         closed/cancelled, rejections ...). Also
                         /trades_history/{trade_id} for a single trade.
  - /pnl /wr /total_trades_count /mounthly_trades_rate /equity_percent
    /max_dd_percent /er /cagr /calmar /sharp /sortino /trade_mode /rdyn
                       — year-to-date portfolio statistics, computed from the
                         closed trades in trade_events (portfolio_metrics.py).
                         By DEFAULT they combine real AND virtual trades into
                         one portfolio (target=all); pass ?target=virtual or
                         ?target=real to restrict. /stats returns all of them
                         at once.
  - /help              — lists every route this service exposes.
"""

from __future__ import annotations

import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union

from fastapi import FastAPI, HTTPException, Query
from fastapi.routing import APIRoute
from pydantic import BaseModel, Field, StrictFloat, StrictInt

from portfolio_metrics import ClosedTrade, PortfolioStats, compute_portfolio_stats
from trade_metrics import compute_trade_metrics
from trades_db_async import TradeEventsDB

app = FastAPI()

VENUE = "binance"

# Optional BNB/USDT rate used to value BNB-denominated fees when the fill events
# themselves don't carry `bnb_usdt_price`. Unset -> BNB fees stay unpriced and the
# trade's net_pnl / win / roi_pct are null instead of being guessed.
_bnb_fallback_raw = os.getenv("BNB_USDT_FALLBACK_PRICE")
BNB_USDT_FALLBACK: Optional[float] = float(_bnb_fallback_raw) if _bnb_fallback_raw else None


class TradeStatus(BaseModel):
    trade_id: str
    state: str
    instrument: str
    side: str
    size: float
    entry_price: Optional[float]
    sl_price: Optional[float]
    tp_price: Optional[float]


class TradeEventOut(BaseModel):
    event_id: int
    previous_event_id: Optional[int]
    event_type: str
    size: float
    ep: Optional[float]
    sl: Optional[float]
    tp: Optional[float]
    fill_price: Optional[float]
    close_reason: Optional[str]
    cancel_reason: Optional[str]
    metadata: Optional[Dict[str, Any]] = None  # order role, order ids, qty, commission, ...
    occurred_at: datetime


class TradeMetricsOut(BaseModel):
    """Derived from the trade's events by trade_metrics.compute_trade_metrics
    (definitions live there). Null for trades that are still open."""
    ticker: str                         # instrument without the venue suffix
    side: str
    target: str
    win: Optional[bool]                 # net_pnl > 0; null when net_pnl can't be determined
    gross_pnl: Optional[float]          # (exit - entry fill) * size, sign-flipped for SELL; before fees
    fee_usdt: float                     # fees paid in USDT (base-asset fees converted at fill price)
    fee_bnb: float                      # fees paid in BNB (native units)
    fee_bnb_usdt: Optional[float]       # BNB fees valued in USDT; null if no rate is known
    net_pnl: Optional[float]            # gross_pnl - fee_usdt - fee_bnb_usdt
    roi_pct: Optional[float]            # net_pnl / (entry fill * size) * 100
    duration_s: Optional[float]         # seconds in the position (filled_at -> finished_at)
    fills_complete: Optional[bool]      # False: recorded fills don't add up to size, fees may be understated
    notes: List[str] = []               # why a field is null / caveats


class TradeHistory(BaseModel):
    trade_id: str
    target: str  # "virtual" | "real"
    instrument: str
    side: str
    size: float
    entry_price: Optional[float]
    sl_price: Optional[float]
    tp_price: Optional[float]
    status: Literal["Open", "Closed", "Cancelled"]
    entry_fill_price: Optional[float]  # None if the entry never filled
    exit_price: Optional[float]        # price of the last fill (SL/TP/manual close)
    close_reason: Optional[str]        # sl_hit | tp_hit | user_close
    cancel_reason: Optional[str]
    started_at: datetime               # first event of the trade
    filled_at: Optional[datetime]
    finished_at: Optional[datetime]    # None while the trade is still open
    events: Optional[List[TradeEventOut]] = None  # full event chain (default on)
    metrics: Optional[TradeMetricsOut] = None     # PnL / fees / ROI / duration; null while open


class HelpEndpoint(BaseModel):
    method: str
    path: str
    description: str


class BalanceResponse(BaseModel):
    source: str  # "virtual" | "testnet" | "mainnet"
    balance_usdt: Optional[float] = None
    available: bool
    detail: Optional[str] = None


class SetVirtualBalanceRequest(BaseModel):
    # gt=0: sizing raises PositionSizingError for equity <= 0, so reject it here
    # rather than storing a value that makes every virtual trade fail.
    # le=1e12 also keeps inf out of the NUMERIC column (NaN already fails gt=0).
    balance_usdt: float = Field(..., gt=0, le=1e12, description="New virtual equity in USDT (> 0)")


async def _get_db() -> TradeEventsDB:
    return await TradeEventsDB.get_instance()


def _heartbeat_stale(hb: Optional[Dict[str, Any]], max_age_seconds: float = 60.0) -> bool:
    if hb is None or hb.get("updated_at") is None:
        return True
    updated_at = hb["updated_at"]
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - updated_at).total_seconds() > max_age_seconds


@app.get("/health", summary="Node, SQS and Postgres health (from node heartbeats)")
async def health_check():
    """
    Reads node_heartbeats (written independently by binance_real_node.py
    and binance_virtual_mainnet_node.py every ~15s) instead of holding any
    live reference to those processes. A heartbeat older than 60s is
    treated the same as "not running" — a dead process stops writing,
    it doesn't write "not running".
    """
    try:
        db = await _get_db()
        postgres_status = {"status": "connected", "detail": None}
    except Exception as e:
        return {
            "status": "degraded",
            "active_trades_count": None,
            "dependencies": {
                "postgres": {"status": "failed", "detail": str(e)},
                "nautilus_real": {"status": "unknown"},
                "nautilus_virtual": {"status": "unknown"},
            },
        }

    real_hb = await db.get_heartbeat(VENUE, "real")
    virtual_hb = await db.get_heartbeat(VENUE, "virtual")

    def node_status(hb: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if hb is None:
            return {"status": "not_started", "trader_running": False}
        if _heartbeat_stale(hb):
            return {"status": "stale_heartbeat", "trader_running": False, "last_seen": hb["updated_at"].isoformat()}
        return {
            "status": "running" if hb["is_running"] else "starting",
            "trader_running": bool(hb["is_running"]),
            "detail": hb.get("detail"),
        }

    def sqs_status(hb: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Each node process reports its own SQS connectivity in its
        heartbeat row (see sqs.get_sqs_status / common_tasks.heartbeat_loop)
        — edge-api never talks to AWS itself, it just relays this."""
        if hb is None:
            return {"status": "unknown", "detail": "no heartbeat yet", "updated_at": None}
        if _heartbeat_stale(hb):
            return {
                "status": "unknown",
                "detail": "heartbeat is stale, sqs status may be outdated",
                "updated_at": hb["updated_at"].isoformat(),
            }
        sqs_ok = hb.get("sqs_ok")
        if sqs_ok is None:
            return {"status": "unknown", "detail": "node has not reported sqs status yet", "updated_at": hb["updated_at"].isoformat()}
        return {
            "status": "connected" if sqs_ok else "disconnected",
            "detail": hb.get("sqs_detail"),
            "updated_at": hb["updated_at"].isoformat(),
        }

    real_status = node_status(real_hb)
    virtual_status = node_status(virtual_hb)
    sqs_real_status = sqs_status(real_hb)
    sqs_virtual_status = sqs_status(virtual_hb)

    try:
        active_trades = await db.get_active_trades()
        active_count = len(active_trades)
    except Exception:
        active_count = None

    overall_ok = (
        real_status.get("trader_running") is True
        and virtual_status.get("trader_running") is True
        and sqs_real_status.get("status") == "connected"
        and sqs_virtual_status.get("status") == "connected"
    )

    return {
        "status": "ok" if overall_ok else "degraded",
        "active_trades_count": active_count,
        "dependencies": {
            "postgres": postgres_status,
            "nautilus_real": real_status,
            "nautilus_virtual": virtual_status,
            "sqs_real": sqs_real_status,
            "sqs_virtual": sqs_virtual_status,
        },
    }


@app.get("/balance/virtual", response_model=BalanceResponse, summary="Configured virtual equity (USDT)")
async def get_virtual_usdt_balance():
    """Configured virtual equity (trades_config.virtual_balance_usdt) — a
    plain DB read, unchanged in behavior from the old api.py."""
    try:
        db = await _get_db()
        cfg = await db.get_trades_config()
        return BalanceResponse(source="virtual", balance_usdt=cfg["virtual_balance_usdt"], available=True)
    except Exception as e:
        return BalanceResponse(source="virtual", available=False, detail=str(e))


@app.put("/balance/virtual", response_model=BalanceResponse, summary="Set the virtual equity (USDT)")
async def set_virtual_usdt_balance(body: SetVirtualBalanceRequest):
    """Overwrites trades_config.virtual_balance_usdt (risk_ratio is left
    untouched). Virtual only — there is deliberately no way to set the real
    balance. The virtual node reads this value from the DB each time it sizes
    a trade, so the new equity applies to the next trade without a restart;
    trades already open keep the size they were opened with."""
    try:
        db = await _get_db()
        cfg = await db.update_trades_config(virtual_balance_usdt=body.balance_usdt)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Could not update virtual balance: {e}")
    print(f"💾 PUT /balance/virtual: virtual_balance_usdt set to {cfg['virtual_balance_usdt']}", file=sys.stderr)
    return BalanceResponse(source="virtual", balance_usdt=cfg["virtual_balance_usdt"], available=True)


@app.get("/balance/mainnet", response_model=BalanceResponse, summary="Real account balance from the real node's latest heartbeat")
async def get_mainnet_usdt_balance():
    """Real node's latest balance snapshot, from its heartbeat row (edge-api
    has no live account/node reference anymore)."""
    try:
        db = await _get_db()
        hb = await db.get_heartbeat(VENUE, "real")
    except Exception as e:
        return BalanceResponse(source="mainnet", available=False, detail=str(e))

    if hb is None:
        return BalanceResponse(source="mainnet", available=False, detail="Real node has not reported a heartbeat yet")
    if _heartbeat_stale(hb):
        return BalanceResponse(source="mainnet", available=False, detail=f"Real node heartbeat is stale (last seen {hb['updated_at'].isoformat()})")
    if hb.get("balance_usdt") is None:
        return BalanceResponse(source="mainnet", available=False, detail=hb.get("detail") or "Balance not available yet")
    return BalanceResponse(source="mainnet", balance_usdt=float(hb["balance_usdt"]), available=True)


@app.get("/active_trades", response_model=List[TradeStatus], summary="Currently open trades (Opened with no Closed/Cancelled yet)")
async def get_active_trades():
    """Derived from trade_events (Opened without a later Closed/Cancelled),
    per MultiVenueTOD.md — no in-memory process state to read anymore."""
    db = await _get_db()
    events = await db.get_active_trades()
    print(f"📊 /active_trades: {len(events)} open trade(s)", file=sys.stderr)

    result = []
    for ev in events:
        try:
            result.append(TradeStatus(
                trade_id=ev["trade_id"],
                state="Opened",
                instrument=ev["instrument"],
                side=ev["side"],
                size=float(ev["size"]),
                entry_price=float(ev["ep"]) if ev["ep"] is not None else None,
                sl_price=float(ev["sl"]) if ev["sl"] is not None else None,
                tp_price=float(ev["tp"]) if ev["tp"] is not None else None,
            ))
        except Exception as e:
            print(f"⚠️ Error building TradeStatus for {ev.get('trade_id')}: {e}", file=sys.stderr)
    return result


def _to_float(v: Any) -> Optional[float]:
    return float(v) if v is not None else None


def _build_event(e: Dict[str, Any]) -> TradeEventOut:
    return TradeEventOut(
        event_id=e["event_id"],
        previous_event_id=e["previous_event_id"],
        event_type=e["event_type"],
        size=float(e["size"]),
        ep=_to_float(e["ep"]),
        sl=_to_float(e["sl"]),
        tp=_to_float(e["tp"]),
        fill_price=_to_float(e["fill_price"]),
        close_reason=e["close_reason"],
        cancel_reason=e["cancel_reason"],
        metadata=e.get("metadata") or None,
        occurred_at=e["occurred_at"],
    )


def _build_metrics(t: Dict[str, Any]) -> Optional[TradeMetricsOut]:
    """A metrics failure must never hide the trade itself, so log and return null."""
    try:
        m = compute_trade_metrics(t, t.get("events", []), bnb_usdt_fallback=BNB_USDT_FALLBACK)
    except Exception as e:
        print(f"⚠️ Error computing metrics for {t.get('trade_id')}: {type(e).__name__}: {e}", file=sys.stderr)
        return None
    return TradeMetricsOut(**asdict(m)) if m is not None else None


def _build_trade_history(t: Dict[str, Any], include_events: bool) -> TradeHistory:
    return TradeHistory(
        trade_id=t["trade_id"],
        target=t["target"],
        instrument=t["instrument"],
        side=t["side"],
        size=float(t["size"]),
        entry_price=_to_float(t["ep"]),
        sl_price=_to_float(t["sl"]),
        tp_price=_to_float(t["tp"]),
        status=t["terminal_type"] or "Open",
        entry_fill_price=_to_float(t["entry_fill_price"]),
        exit_price=_to_float(t["exit_price"]),
        close_reason=t["close_reason"],
        cancel_reason=t["cancel_reason"],
        started_at=t["started_at"],
        filled_at=t["filled_at"],
        finished_at=t["finished_at"],
        events=[_build_event(e) for e in t.get("events", [])] if include_events else None,
        metrics=_build_metrics(t),
    )


@app.get(
    "/trades_history",
    response_model=List[TradeHistory],
    summary="Trade history with the full event chain of each trade, newest first",
)
async def get_trades_history(
    limit: int = Query(50, ge=1, le=500, description="Max trades to return"),
    offset: int = Query(0, ge=0, description="Skip this many trades (paging)"),
    target: Optional[Literal["virtual", "real"]] = Query(None, description="Only this execution target"),
    status: Optional[Literal["Open", "Closed", "Cancelled"]] = Query(None, description="Only trades in this state"),
    instrument: Optional[str] = Query(None, description="Only this instrument, exactly as stored"),
    include_events: bool = Query(True, description="Include each trade's event chain (set false for a light listing)"),
):
    """Every trade (open, closed or cancelled) with its event-sourced chain:
    EntrySubmitted -> Opened -> Filled -> SLPlaced/TPPlaced -> SLHit/TPHit ->
    Closed (or Cancelled / OrderRejected / OrderDenied ...), in chronological
    order, each with its `metadata`."""
    db = await _get_db()
    trades = await db.get_trades_history(
        limit=limit, offset=offset, target=target, status=status,
        instrument=instrument,
        include_events=True,  # metrics are derived from the events; they are only echoed back when include_events is set
    )
    print(f"📊 /trades_history: {len(trades)} trade(s) returned", file=sys.stderr)

    result = []
    for t in trades:
        try:
            result.append(_build_trade_history(t, include_events))
        except Exception as e:
            print(f"⚠️ Error building TradeHistory for {t.get('trade_id')}: {e}", file=sys.stderr)
    return result


@app.get(
    "/trades_history/{trade_id}",
    response_model=TradeHistory,
    summary="One trade with its full event chain",
)
async def get_trade_history(trade_id: str):
    db = await _get_db()
    trades = await db.get_trades_history(limit=1, trade_id=trade_id, include_events=True)
    if not trades:
        raise HTTPException(status_code=404, detail=f"No trade with trade_id {trade_id}")
    return _build_trade_history(trades[0], include_events=True)


# ---------------------------------------------------------------------------
# Portfolio statistics (year to date)
#
# Every number comes from the closed trades in trade_events: only trades whose
# terminal "Closed" event falls inside the calendar year (UTC) count, so the
# statistics start from scratch on 1 January. By DEFAULT both real AND virtual
# trades are combined into a single portfolio (target=all). Use ?target=virtual
# or ?target=real to restrict. Definitions: portfolio_metrics.py.
# ---------------------------------------------------------------------------

STATS_CACHE_SECONDS = float(os.getenv("STATS_CACHE_SECONDS", "5"))


@dataclass
class _StatsContext:
    stats: PortfolioStats
    start_equity_source: str
    excluded_trades: int


_stats_cache: Dict[Tuple[str, int, Optional[float]], Tuple[float, _StatsContext]] = {}


class MetricResponse(BaseModel):
    metric: str
    value: Optional[Union[StrictInt, StrictFloat]]   # null when it cannot be computed (e.g. Sharpe with no variance)
    unit: str
    target: str                                      # "virtual" | "real" | "all"
    year: int
    trades_count: int
    start_equity: float
    start_equity_source: str
    details: Dict[str, Any] = {}


class StatsResponse(BaseModel):
    target: str
    year: int
    start_equity_source: str
    excluded_trades: int       # closed trades left out: PnL could not be determined (see /trades_history notes)
    stats: Dict[str, Any]


def _current_year() -> int:
    return datetime.now(timezone.utc).year


async def _load_year_stats(
    target: str,                # "virtual" | "real" | "all"
    year: int,
    start_equity: Optional[float],
) -> _StatsContext:
    key = (target, year, start_equity)
    hit = _stats_cache.get(key)
    if hit is not None and time.monotonic() - hit[0] < STATS_CACHE_SECONDS:
        return hit[1]

    db = await _get_db()
    db_target = None if target == "all" else target     # None = no DB-side filter
    try:
        trades = await db.get_trades_history(
            limit=100_000, target=db_target, status="Closed", include_events=True,
            finished_from=datetime(year, 1, 1), finished_to=datetime(year + 1, 1, 1),  # naive UTC, like trade_events
        )
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Could not read trades: {e}")

    closed: List[ClosedTrade] = []
    excluded = 0
    for t in trades:
        try:
            m = compute_trade_metrics(t, t.get("events", []), bnb_usdt_fallback=BNB_USDT_FALLBACK)
        except Exception as e:
            print(f"⚠️ stats: metrics failed for {t.get('trade_id')}: {type(e).__name__}: {e}", file=sys.stderr)
            m = None
        if m is None or m.net_pnl is None or t.get("finished_at") is None:
            excluded += 1
            continue
        closed.append(ClosedTrade(
            finished_at=t["finished_at"],
            net_pnl=m.net_pnl,
            target=m.target,
        ))

    # Equity the year started with. Per-trade returns are measured against the account
    # equity before each trade (Day_Trading.gs: profit / previous balance).
    if start_equity is not None:
        equity0, source = start_equity, "query_param"
    elif target == "virtual":
        try:
            cfg = await db.get_trades_config()
        except Exception as e:
            raise HTTPException(status_code=503, detail=f"Could not read virtual balance: {e}")
        equity0, source = float(cfg["virtual_balance_usdt"]), "virtual_balance_usdt"
    elif target == "real":
        try:
            hb = await db.get_heartbeat(VENUE, "real")
        except Exception as e:
            raise HTTPException(status_code=503, detail=f"Could not read real balance: {e}")
        if hb is None or _heartbeat_stale(hb) or hb.get("balance_usdt") is None:
            raise HTTPException(
                status_code=503,
                detail="Real balance unavailable (no fresh heartbeat); pass ?start_equity=<USDT at the start of the year>",
            )
        equity0 = float(hb["balance_usdt"]) - sum(c.net_pnl for c in closed)
        source = "mainnet_balance_minus_ytd_pnl"
    else:  # target == "all"
        # Combined start_equity = virtual_start + real_start.
        #   virtual_start = trades_config.virtual_balance_usdt (same figure
        #                   virtual sizing uses; this is what the virtual
        #                   account "has" at the start of the year).
        #   real_start    = current real free USDT - real YTD PnL.
        # The real side needs a fresh heartbeat; if it isn't there, we refuse
        # rather than guess (an explicit ?start_equity= overrides this branch).
        try:
            cfg = await db.get_trades_config()
            hb = await db.get_heartbeat(VENUE, "real")
        except Exception as e:
            raise HTTPException(status_code=503, detail=f"Could not read balances for target=all: {e}")
        if hb is None or _heartbeat_stale(hb) or hb.get("balance_usdt") is None:
            raise HTTPException(
                status_code=503,
                detail="target=all needs a fresh real heartbeat and the virtual balance; "
                       "either wait for the real node or pass ?start_equity=<USDT at the start of the year>",
            )
        real_pnl = sum(c.net_pnl for c in closed if c.target == "real")
        virtual_start = float(cfg["virtual_balance_usdt"])
        real_start = float(hb["balance_usdt"]) - real_pnl
        equity0 = virtual_start + real_start
        source = "virtual_balance_usdt+mainnet_balance_minus_real_ytd_pnl"

    try:
        stats = compute_portfolio_stats(closed, equity0)
    except ValueError as e:
        raise HTTPException(status_code=503, detail=f"Cannot compute statistics: {e}")

    ctx = _StatsContext(stats=stats, start_equity_source=source, excluded_trades=excluded)
    _stats_cache[key] = (time.monotonic(), ctx)
    return ctx


_TARGET_Q = Query(
    "all",
    description="Which trades to evaluate: 'virtual', 'real', or 'all' (default). "
                "'all' combines real and virtual trades into a single portfolio.",
)
_YEAR_Q = Query(None, ge=2000, le=2100, description="Calendar year (UTC); default = the current year")
_EQUITY_Q = Query(
    None, gt=0, le=1e12,
    description="Override: account equity (USDT) at the start of the year. Default: virtual_balance_usdt "
                "(virtual), latest real balance minus the year's PnL (real), or "
                "virtual_balance_usdt + real_balance - real YTD PnL (all).",
)


def _metric_route(
    path: str,
    metric: str,
    unit: str,
    getter: Callable[[PortfolioStats], Any],
    summary: str,
    *,
    integer: bool = False,
    details: Optional[Callable[[PortfolioStats], Dict[str, Any]]] = None,
    aliases: Tuple[str, ...] = (),
) -> None:
    async def endpoint(
        target: Literal["virtual", "real", "all"] = _TARGET_Q,
        year: Optional[int] = _YEAR_Q,
        start_equity: Optional[float] = _EQUITY_Q,
    ) -> MetricResponse:
        y = year or _current_year()
        ctx = await _load_year_stats(target, y, start_equity)
        st: PortfolioStats = ctx.stats
        raw = getter(st)
        value = None if raw is None else (int(raw) if integer else float(raw))
        return MetricResponse(
            metric=metric, value=value, unit=unit, target=target, year=y,
            trades_count=st.trades_count, start_equity=st.start_equity,
            start_equity_source=ctx.start_equity_source,
            details=details(st) if details else {},
        )

    endpoint.__name__ = f"get_{metric}"
    endpoint.__doc__ = summary
    app.get(path, response_model=MetricResponse, summary=summary)(endpoint)
    for alias in aliases:   # correctly spelled twins, hidden from /help
        app.get(alias, response_model=MetricResponse, include_in_schema=False)(endpoint)


_metric_route("/pnl", "pnl", "USDT", lambda s: s.pnl,
              "Year-to-date net PnL (USDT), after fees — aggregate over all closed trades",
              details=lambda s: {"start_equity": s.start_equity, "end_equity": s.end_equity})
_metric_route("/wr", "wr", "%", lambda s: s.win_rate_pct,
              "Year-to-date win rate (% of trades with net PnL > 0)",
              details=lambda s: {"wins": s.wins, "losses": s.losses})
_metric_route("/total_trades_count", "total_trades_count", "trades", lambda s: s.trades_count,
              "Number of trades closed this year", integer=True)
_metric_route("/mounthly_trades_rate", "monthly_trades_rate", "trades/month", lambda s: s.monthly_trades_rate,
              "Trades per month: trades / days between first and last close * 30.44",
              details=lambda s: {"duration_days": s.duration_days},
              aliases=("/monthly_trades_rate",))
_metric_route("/equity_percent", "equity_percent", "%", lambda s: s.equity_percent,
              "Equity change since the start of the year (%)",
              details=lambda s: {"start_equity": s.start_equity, "end_equity": s.end_equity})
_metric_route("/max_dd_percent", "max_dd_percent", "%", lambda s: s.max_dd_percent,
              "Maximum drawdown of the equity curve this year (%)",
              details=lambda s: {"max_dd_usdt": s.max_dd_usdt})
_metric_route("/er", "er", "USDT per trade", lambda s: s.er_usdt,
              "Expected return per trade (USDT): WR * avg win USDT + (1 - WR) * avg loss USDT",
              details=lambda s: {
                  "avg_win_usdt": s.avg_win_usdt,
                  "avg_loss_usdt": s.avg_loss_usdt,
                  "avg_gain_pct": s.avg_gain_pct,
                  "avg_loss_pct": s.avg_loss_pct,
              })
_metric_route("/cagr", "cagr", "%", lambda s: s.cagr_pct,
              "CAGR annualised from the first to the last close of the year (%)",
              details=lambda s: {"duration_days": s.duration_days})
_metric_route("/calmar", "calmar", "ratio", lambda s: s.calmar,
              "Calmar ratio: CAGR / max drawdown (null while there is no drawdown)")
_metric_route("/sharp", "sharpe", "ratio", lambda s: s.sharpe,
              "Sharpe ratio: mean / std across ALL trades' per-trade returns (not annualised)",
              aliases=("/sharpe",))
_metric_route("/sortino", "sortino", "ratio", lambda s: s.sortino,
              "Sortino ratio: mean per-trade return / downside deviation, over ALL trades "
              "(null while there is no losing trade)")
_metric_route("/trade_mode", "trade_mode", "1=real, 0=paper", lambda s: s.trade_mode,
              "Trading mode: 1 = real, 0 = paper (paper after 15% drawdown or 10 losses in a row, "
              "back to real at a new equity high)",
              integer=True,
              details=lambda s: {"mode": "REAL" if s.trade_mode == 1 else "PAPER"})
_metric_route("/rdyn", "rdyn", "% risk per trade", lambda s: s.rdyn_pct,
              "Dynamic risk per trade (%): sigmoid of the year's CAGR, between 0.1% and 1.0%",
              details=lambda s: {"risk_ratio": s.rdyn_pct / 100.0, "cagr_pct": s.cagr_pct})


@app.get("/stats", response_model=StatsResponse, summary="All year-to-date portfolio statistics in one call")
async def get_stats(
    target: Literal["virtual", "real", "all"] = _TARGET_Q,
    year: Optional[int] = _YEAR_Q,
    start_equity: Optional[float] = _EQUITY_Q,
):
    y = year or _current_year()
    ctx = await _load_year_stats(target, y, start_equity)
    return StatsResponse(
        target=target, year=y, start_equity_source=ctx.start_equity_source,
        excluded_trades=ctx.excluded_trades, stats=asdict(ctx.stats),
    )


@app.get("/help", response_model=List[HelpEndpoint], summary="List all available API endpoints")
async def help_endpoints():
    """Built from the app's registered routes at request time, so it can't
    drift out of date when endpoints are added."""
    endpoints: List[HelpEndpoint] = []
    for route in app.routes:
        if not isinstance(route, APIRoute) or not route.include_in_schema:
            continue
        description = route.summary or (route.endpoint.__doc__ or "").strip().split("\n")[0]
        for method in sorted(route.methods - {"HEAD", "OPTIONS"}):
            endpoints.append(HelpEndpoint(method=method, path=route.path, description=description))
    return endpoints

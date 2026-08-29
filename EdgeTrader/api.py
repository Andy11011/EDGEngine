import sys
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, List, Optional

app = FastAPI()

# Shared mutable reference – will be assigned from EDGETrader
active_strategies: Dict[str, Any] = {}

# ---------------------------------------------------------------------------
# Aggregated dependency status, updated by EDGETrader.py as connections are
# established or lost (Postgres, SQS). Each entry is fully replaced (never
# mutated in place) on update, so a concurrent /health read can never see a
# half-written entry.
# ---------------------------------------------------------------------------
service_state: Dict[str, Dict[str, Any]] = {
    "postgres": {"status": "unknown", "detail": None, "updated_at": None},
    "sqs": {"status": "unknown", "detail": None, "updated_at": None},
}

# Live references to the two running TradingNode instances, set once by
# EDGETrader.py right after each node's node.build(). Both nodes connect to
# Binance MAINNET data; "real" additionally has a live Binance exec client,
# "virtual" has a sandbox exec client instead. node.trader.is_running is a
# cheap attribute read (no I/O), so /health checks both live rather than
# relying on push updates that could go stale.
#
# TESTNET support is intentionally not built at all right now (see
# ENABLE_TESTNET in EDGETrader.py) — deprioritized since MAINNET paper vs.
# real is the only distinction that currently matters. /balance/testnet
# below reports that plainly rather than guessing.
node_ref: Dict[str, Any] = {"real": None, "virtual": None}

# ---------------------------------------------------------------------------
# Balance-check endpoints (/balance/virtual, /balance/testnet,
# /balance/mainnet). Populated by EDGETrader.py once the nodes/DB are ready.
# Kept as thin closures here (rather than importing nautilus_trader or
# trades_db_async into api.py) so this module stays free of heavy
# trading-specific dependencies.
# ---------------------------------------------------------------------------
balance_refs: Dict[str, Any] = {
    "get_real_balance": None,     # sync callable -> float, set once the real node's account is ready
    "get_virtual_balance": None,  # async callable -> float, set once the DB is ready
}


def set_status(component: str, status: str, detail: Optional[str] = None) -> None:
    """Record a status transition for a dependency (e.g. 'postgres', 'sqs').

    Replaces the whole dict for `component` in a single assignment so
    concurrent readers never observe a torn/half-updated entry.
    """
    service_state[component] = {
        "status": status,
        "detail": detail,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

class TradeStatus(BaseModel):
    trade_id: str
    state: str
    instrument: str
    side: str
    size: float
    entry_price: Optional[float]
    sl_price: Optional[float]
    tp_price: Optional[float]


class BalanceResponse(BaseModel):
    source: str                        # "virtual" | "testnet" | "mainnet"
    balance_usdt: Optional[float] = None
    available: bool
    detail: Optional[str] = None

def _node_status(node: Optional[Any]) -> Dict[str, Any]:
    if node is None:
        return {"status": "not_started", "trader_running": False}
    try:
        trader_running = bool(node.trader.is_running)
        return {
            "status": "running" if trader_running else "starting",
            "trader_running": trader_running,
        }
    except Exception as e:
        return {"status": "unknown", "detail": str(e), "trader_running": False}


@app.get("/health")
async def health_check():
    """
    Liveness + dependency status probe.

    Reports Postgres/SQS connectivity as last observed by the worker loop in
    EDGETrader.py (push-based, no I/O here), plus the live running state of
    both TradingNode instances (real exec + virtual/sandbox exec), read
    directly off node.trader.is_running (cheap attribute read, no I/O).
    """
    real_status = _node_status(node_ref.get("real"))
    virtual_status = _node_status(node_ref.get("virtual"))

    overall_ok = (
        service_state["postgres"]["status"] == "connected"
        and service_state["sqs"]["status"] == "connected"
        and real_status.get("trader_running") is True
        and virtual_status.get("trader_running") is True
    )

    return {
        "status": "ok" if overall_ok else "degraded",
        "active_trades_count": len(active_strategies),
        "dependencies": {
            "postgres": service_state["postgres"],
            "sqs": service_state["sqs"],
            "nautilus_real": real_status,
            "nautilus_virtual": virtual_status,
        },
    }

@app.get("/balance/virtual", response_model=BalanceResponse)
async def get_virtual_usdt_balance():
    """Configured virtual equity used for sizing trades on the sandbox/virtual
    exec node (trades_config.virtual_balance_usdt). Always applicable — this
    is a plain DB read, independent of which nodes are up."""
    getter = balance_refs.get("get_virtual_balance")
    if getter is None:
        return BalanceResponse(source="virtual", available=False, detail="Virtual balance getter not wired up yet (DB not ready)")
    try:
        balance = await getter()
        return BalanceResponse(source="virtual", balance_usdt=balance, available=True)
    except Exception as e:
        return BalanceResponse(source="virtual", available=False, detail=str(e))


@app.get("/balance/testnet", response_model=BalanceResponse)
async def get_testnet_usdt_balance():
    """TESTNET support is intentionally not built right now (see
    ENABLE_TESTNET in EDGETrader.py) — this deployment only ever runs
    against MAINNET, for both the real and virtual/sandbox exec nodes."""
    return BalanceResponse(
        source="testnet",
        available=False,
        detail="TESTNET is disabled (ENABLE_TESTNET=false) — this deployment only connects to Binance MAINNET.",
    )


@app.get("/balance/mainnet", response_model=BalanceResponse)
async def get_mainnet_usdt_balance():
    """Live free USDT balance from the real (non-sandbox) Binance MAINNET
    exec node's account."""
    getter = balance_refs.get("get_real_balance")
    if getter is None:
        return BalanceResponse(source="mainnet", available=False, detail="Real exec node not ready yet (no account state received)")
    try:
        balance = getter()
        return BalanceResponse(source="mainnet", balance_usdt=balance, available=True)
    except Exception as e:
        return BalanceResponse(source="mainnet", available=False, detail=str(e))


@app.get("/active_trades", response_model=List[TradeStatus])
async def get_active_trades():
    """List all currently active trades (strategies) with their state."""
    print(f"📊 /active_trades called, active_strategies length: {len(active_strategies)}", file=sys.stderr)
    if active_strategies:
        print(f"   Keys: {list(active_strategies.keys())}", file=sys.stderr)

    result = []
    for trade_id, strategy in active_strategies.items():
        try:
            result.append(TradeStatus(
                trade_id=trade_id,
                state=strategy.sm.state.name,
                instrument=str(strategy.config.instrument_id),
                side=strategy.config.side,
                size=strategy.config.size,
                entry_price=strategy.config.entry_price,
                sl_price=strategy.config.sl_price,
                tp_price=strategy.config.tp_price,
            ))
        except Exception as e:
            print(f"⚠️ Error building TradeStatus for {trade_id}: {e}", file=sys.stderr)

    print(f"   Returning {len(result)} trade(s)", file=sys.stderr)
    return result

@app.post("/cancel/{trade_id}")
async def cancel_trade(trade_id: str):
    """Manually cancel/close an active trade."""
    print(f"🛑 /cancel/{trade_id} called", file=sys.stderr)
    strategy = active_strategies.get(trade_id)
    if not strategy:
        print(f"   Trade {trade_id} not active", file=sys.stderr)
        raise HTTPException(404, f"Trade {trade_id} not active")
    try:
        strategy.request_cancel()
        print(f"   Cancel request sent for {trade_id}", file=sys.stderr)
    except Exception as e:
        print(f"   Cancel failed: {e}", file=sys.stderr)
        raise HTTPException(500, f"Cancel failed: {e}")
    return {"status": "cancel_requested"}

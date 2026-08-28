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

# Live reference to the running TradingNode, set once by EDGETrader.py right
# after node.build(). node.trader.is_running is a cheap attribute read (no
# I/O), so /health checks it live instead of relying on push updates that
# could go stale.
node_ref: Dict[str, Any] = {"node": None}

# ---------------------------------------------------------------------------
# Balance-check endpoints (/balance/virtual, /balance/testnet,
# /balance/mainnet). Populated by EDGETrader.py once the node/DB are ready.
# Kept as thin closures here (rather than importing nautilus_trader or
# trades_db_async into api.py) so this module stays free of heavy
# trading-specific dependencies.
#
# A single running instance is only ever connected to ONE real Binance
# environment at a time (TESTNET or MAINNET, chosen via TRADING_MODE) — it
# never runs both simultaneously. So "environment" below records which one
# (if any) this deployment is actually wired to, and the testnet/mainnet
# endpoints report clearly when they're asked about the other one, or about
# a mode (TEST1/TEST2) where execution is simulated and no real account is
# connected at all — rather than fabricating a number.
balance_refs: Dict[str, Any] = {
    "environment": None,        # "TESTNET" | "MAINNET" | None (not ready yet)
    "get_real_balance": None,   # sync callable -> float, or None if not applicable
    "get_virtual_balance": None,  # async callable -> float, or None if not ready yet
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
    environment: Optional[str] = None  # environment this deployment is actually connected to, if any
    balance_usdt: Optional[float] = None
    available: bool
    detail: Optional[str] = None

@app.get("/health")
async def health_check():
    """
    Liveness + dependency status probe.

    Reports Postgres/SQS connectivity as last observed by the worker loops
    in EDGETrader.py (push-based, no I/O here), plus the live TradingNode
    running state (read directly off node.trader.is_running, cheap/no I/O).
    """
    node = node_ref.get("node")
    if node is None:
        nautilus_status: Dict[str, Any] = {"status": "not_started", "trader_running": False}
    else:
        try:
            trader_running = bool(node.trader.is_running)
            nautilus_status = {
                "status": "running" if trader_running else "starting",
                "trader_running": trader_running,
            }
        except Exception as e:
            nautilus_status = {"status": "unknown", "detail": str(e), "trader_running": False}

    overall_ok = (
        service_state["postgres"]["status"] == "connected"
        and service_state["sqs"]["status"] == "connected"
        and nautilus_status.get("trader_running") is True
    )

    return {
        "status": "ok" if overall_ok else "degraded",
        "active_trades_count": len(active_strategies),
        "dependencies": {
            "postgres": service_state["postgres"],
            "sqs": service_state["sqs"],
            "nautilus": nautilus_status,
        },
    }

@app.get("/balance/virtual", response_model=BalanceResponse)
async def get_virtual_usdt_balance():
    """Configured virtual equity used for TEST1/TEST2 sizing (trades_config.
    virtual_balance_usdt). Mode-independent — this is a plain DB read, so it
    works regardless of which real Binance environment (if any) this
    deployment is connected to."""
    getter = balance_refs.get("get_virtual_balance")
    if getter is None:
        return BalanceResponse(source="virtual", available=False, detail="Virtual balance getter not wired up yet (DB not ready)")
    try:
        balance = await getter()
        return BalanceResponse(source="virtual", balance_usdt=balance, available=True)
    except Exception as e:
        return BalanceResponse(source="virtual", available=False, detail=str(e))


async def _real_balance_response(source: str, expected_environment: str) -> BalanceResponse:
    """Shared logic for the testnet/mainnet endpoints below. Only the
    environment this instance is actually connected to (per TRADING_MODE)
    can report a real number — the other one, and any simulated-exec mode
    (TEST1/TEST2), report clearly why they can't rather than guessing."""
    current_env = balance_refs.get("environment")
    if current_env != expected_environment:
        return BalanceResponse(
            source=source,
            environment=current_env,
            available=False,
            detail=(
                f"This deployment is connected to {current_env or 'no'} Binance "
                f"environment, not {expected_environment}. Only one real "
                "environment is live per running instance (set via "
                "TRADING_MODE)."
            ),
        )
    getter = balance_refs.get("get_real_balance")
    if getter is None:
        return BalanceResponse(
            source=source,
            environment=current_env,
            available=False,
            detail=(
                f"Running against {current_env} but in simulated-exec mode "
                "(TEST1/TEST2) — no real Binance account is connected, so "
                "there's no real balance to report."
            ),
        )
    try:
        balance = getter()
        return BalanceResponse(source=source, environment=current_env, balance_usdt=balance, available=True)
    except Exception as e:
        return BalanceResponse(source=source, environment=current_env, available=False, detail=str(e))


@app.get("/balance/testnet", response_model=BalanceResponse)
async def get_testnet_usdt_balance():
    """Live free USDT balance from Binance TESTNET — only available when
    this deployment is actually running TEST3 (real orders on testnet)."""
    return await _real_balance_response("testnet", "TESTNET")


@app.get("/balance/mainnet", response_model=BalanceResponse)
async def get_mainnet_usdt_balance():
    """Live free USDT balance from Binance MAINNET — only available when
    this deployment is actually running LIVE (real orders, real funds)."""
    return await _real_balance_response("mainnet", "MAINNET")


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

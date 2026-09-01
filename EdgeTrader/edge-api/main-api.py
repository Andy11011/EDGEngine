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
  - /balance/virtual   — trades_config.virtual_balance_usdt (plain DB read,
                         unchanged from the old api.py).
  - /balance/testnet   — static "disabled" response (unchanged).
  - /balance/mainnet   — the real node's latest heartbeat balance snapshot.
  - /active_trades     — derived from trade_events (Opened without a later
                         Closed/Cancelled), not in-memory process state.
  - /cancel/{trade_id} — writes a row to cancel_requests; the owning node
                         process (real or virtual, whichever the trade_id's
                         `target` says) polls that table and calls
                         strategy.request_cancel() itself. See
                         node_common.cancel_requests_loop.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from trades_db_async import TradeEventsDB

app = FastAPI()

VENUE = "binance"


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
    source: str  # "virtual" | "testnet" | "mainnet"
    balance_usdt: Optional[float] = None
    available: bool
    detail: Optional[str] = None


async def _get_db() -> TradeEventsDB:
    return await TradeEventsDB.get_instance()


def _heartbeat_stale(hb: Optional[Dict[str, Any]], max_age_seconds: float = 60.0) -> bool:
    if hb is None or hb.get("updated_at") is None:
        return True
    updated_at = hb["updated_at"]
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - updated_at).total_seconds() > max_age_seconds


@app.get("/health")
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

    real_status = node_status(real_hb)
    virtual_status = node_status(virtual_hb)

    try:
        active_trades = await db.get_active_trades()
        active_count = len(active_trades)
    except Exception:
        active_count = None

    overall_ok = (
        real_status.get("trader_running") is True
        and virtual_status.get("trader_running") is True
    )

    return {
        "status": "ok" if overall_ok else "degraded",
        "active_trades_count": active_count,
        "dependencies": {
            "postgres": postgres_status,
            "nautilus_real": real_status,
            "nautilus_virtual": virtual_status,
        },
    }


@app.get("/balance/virtual", response_model=BalanceResponse)
async def get_virtual_usdt_balance():
    """Configured virtual equity (trades_config.virtual_balance_usdt) — a
    plain DB read, unchanged in behavior from the old api.py."""
    try:
        db = await _get_db()
        cfg = await db.get_trades_config()
        return BalanceResponse(source="virtual", balance_usdt=cfg["virtual_balance_usdt"], available=True)
    except Exception as e:
        return BalanceResponse(source="virtual", available=False, detail=str(e))


@app.get("/balance/testnet", response_model=BalanceResponse)
async def get_testnet_usdt_balance():
    """TESTNET support is intentionally not built — MAINNET only."""
    return BalanceResponse(
        source="testnet",
        available=False,
        detail="TESTNET is disabled (ENABLE_TESTNET=false) — this deployment only connects to Binance MAINNET.",
    )


@app.get("/balance/mainnet", response_model=BalanceResponse)
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


@app.get("/active_trades", response_model=List[TradeStatus])
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


@app.post("/cancel/{trade_id}")
async def cancel_trade(trade_id: str):
    """
    Writes a cancel_requests row for the node process that owns this trade
    (determined from the trade's own recorded `target`) rather than calling
    strategy.request_cancel() directly — edge-api has no process-memory
    reference to the strategy object anymore.
    """
    db = await _get_db()
    latest = await db.get_latest_event_for_trade(trade_id)
    if latest is None:
        raise HTTPException(404, f"Trade {trade_id} not found")
    if latest["event_type"] in ("Closed", "Cancelled"):
        raise HTTPException(409, f"Trade {trade_id} is already {latest['event_type'].lower()}")

    target = latest["target"]
    request_id = await db.enqueue_cancel_request(trade_id, target)
    print(f"🛑 /cancel/{trade_id} -> cancel_requests row {request_id} (target={target})", file=sys.stderr)
    return {"status": "cancel_requested", "target": target}

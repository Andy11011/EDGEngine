"""
common_tasks.py — async background tasks shared by headless node processes.

Includes:
- Heartbeat loop: writes (venue, target) liveness + balance to Postgres.
- Cancel‑requests loop: polls the cancel_requests table and triggers in‑memory
  strategy cancellation.

These loops run alongside the TradingNode's run_async() and are intended to be
started as asyncio tasks in the main async entry point.

All functions assume a TradeEventsDB instance is available (passed in), and
they do not contain any Nautilus wrapper code.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any, Callable, Dict, Optional

from trades_db_async import TradeEventsDB

# Constant used in the heartbeat row – matches the single venue this deployment
# supports (per MultiVenueTOD.md). In future multi‑venue deployments this could
# be made configurable, but for now it's hard‑coded.
VENUE = "binance"


async def heartbeat_loop(
    db: TradeEventsDB,
    node: Any,                 # TradingNode from nautilus_trader.live.node
    target: str,
    get_balance: Optional[Callable[[], float]] = None,
    interval_seconds: float = 15.0,
) -> None:
    """
    Periodically write a heartbeat row to node_heartbeats for (venue, target).

    Args:
        db: TradeEventsDB instance (with write_heartbeat method).
        node: The TradingNode instance (used to read `node.trader.is_running`).
        target: 'virtual' or 'real'.
        get_balance: Optional zero‑arg callable (sync or async) that returns the
                     current free USDT balance as a float. Errors are caught and
                     recorded in the `detail` column.
        interval_seconds: How often to write the heartbeat.
    """
    while True:
        is_running = bool(node.trader.is_running)
        balance: Optional[float] = None
        detail: Optional[str] = None
        if get_balance is not None:
            try:
                result = get_balance()
                if asyncio.iscoroutine(result):
                    result = await result
                balance = float(result)
            except Exception as e:
                detail = f"balance lookup failed: {e}"
        try:
            await db.write_heartbeat(VENUE, target, is_running, balance_usdt=balance, detail=detail)
        except Exception as e:
            print(f"⚠️ Failed to write heartbeat for target={target}: {e}", file=sys.stderr)
        await asyncio.sleep(interval_seconds)


async def cancel_requests_loop(
    db: TradeEventsDB,
    target: str,
    active_strategies: Dict[str, Any],
    poll_seconds: float = 5.0,
) -> None:
    """
    Poll the cancel_requests table for pending cancellation requests for this target.

    For each pending request, look up the strategy in active_strategies by trade_id
    and call strategy.request_cancel(). If the strategy is not found, log a warning
    and mark the request as processed anyway (so it doesn't block the queue).

    Args:
        db: TradeEventsDB instance (with fetch_pending_cancel_requests and
            mark_cancel_request_processed methods).
        target: 'virtual' or 'real' – only requests for this target are polled.
        active_strategies: In‑memory dict mapping trade_id -> TradeStrategy instance.
        poll_seconds: How often to poll the database.
    """
    while True:
        try:
            pending = await db.fetch_pending_cancel_requests(target)
        except Exception as e:
            print(f"⚠️ Failed to poll cancel_requests: {e}", file=sys.stderr)
            pending = []

        for req in pending:
            trade_id = req["trade_id"]
            strategy = active_strategies.get(trade_id)
            if strategy is None:
                print(f"⚠️ Cancel request for {trade_id} but no in-memory strategy (already closed?)", file=sys.stderr)
            else:
                try:
                    strategy.request_cancel()
                    print(f"🛑 Cancel requested (via edge-api) for {trade_id}", file=sys.stderr)
                except Exception as e:
                    print(f"❌ Failed to cancel {trade_id}: {e}", file=sys.stderr)
            # Mark processed regardless of whether we found the strategy – this prevents
            # a stuck request from being retried indefinitely. If the strategy truly
            # disappeared, the trade is already closed or the node restarted.
            await db.mark_cancel_request_processed(req["id"])

        await asyncio.sleep(poll_seconds)
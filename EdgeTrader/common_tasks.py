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
import sys
import time
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
    get_sqs_status: Optional[Callable[[], Any]] = None,
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
        get_sqs_status: Optional zero‑arg callable (sync or async) returning
                         (ok: bool, detail: Optional[str]) — this target's own
                         SQS connectivity, e.g. sqs.get_sqs_status. Written to
                         node_heartbeats so edge-api's /health can report
                         sqs_real / sqs_virtual without needing AWS creds itself.
        interval_seconds: How often to write the heartbeat.
    """
    print(
        f"💓 heartbeat_loop starting for venue={VENUE} target={target} "
        f"interval={interval_seconds}s balance_tracking={'on' if get_balance else 'off'}",
        file=sys.stderr,
    )
    iteration = 0
    while True:
        iteration += 1
        loop_start = time.monotonic()
        is_running = bool(node.trader.is_running)
        balance: Optional[float] = None
        detail: Optional[str] = None

        if get_balance is not None:
            try:
                result = get_balance()
                if asyncio.iscoroutine(result):
                    result = await result
                balance = float(result)
                print(
                    f"💰 [{target}] balance lookup ok (iteration={iteration}): {balance:.4f} USDT",
                    file=sys.stderr,
                )
            except Exception as e:
                detail = f"balance lookup failed: {e}"
                print(
                    f"⚠️ [{target}] balance lookup failed (iteration={iteration}): "
                    f"{type(e).__name__}: {e}",
                    file=sys.stderr,
                )

        sqs_ok: Optional[bool] = None
        sqs_detail: Optional[str] = None
        if get_sqs_status is not None:
            try:
                result = get_sqs_status()
                if asyncio.iscoroutine(result):
                    result = await result
                sqs_ok, sqs_detail = result
                print(
                    f"📡 [{target}] sqs status (iteration={iteration}): "
                    f"ok={sqs_ok} detail={sqs_detail or 'none'}",
                    file=sys.stderr,
                )
            except Exception as e:
                sqs_ok = False
                sqs_detail = f"sqs status lookup failed: {e}"
                print(
                    f"⚠️ [{target}] sqs status lookup failed (iteration={iteration}): "
                    f"{type(e).__name__}: {e}",
                    file=sys.stderr,
                )

        print(
            f"💓 [{target}] heartbeat (iteration={iteration}): is_running={is_running} "
            f"balance={balance if balance is not None else 'n/a'} detail={detail or 'none'} "
            f"sqs_ok={sqs_ok if sqs_ok is not None else 'n/a'}",
            file=sys.stderr,
        )

        try:
            await db.write_heartbeat(
                VENUE, target, is_running,
                balance_usdt=balance, detail=detail,
                sqs_ok=sqs_ok, sqs_detail=sqs_detail,
            )
            print(
                f"✅ [{target}] heartbeat row written (iteration={iteration})",
                file=sys.stderr,
            )
        except Exception as e:
            print(
                f"⚠️ Failed to write heartbeat for target={target} "
                f"(iteration={iteration}): {type(e).__name__}: {e}",
                file=sys.stderr,
            )

        elapsed = time.monotonic() - loop_start
        print(
            f"⏱️ [{target}] heartbeat iteration={iteration} took {elapsed:.3f}s, "
            f"sleeping {interval_seconds}s",
            file=sys.stderr,
        )
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

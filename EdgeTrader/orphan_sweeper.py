"""
orphan_sweeper.py — reconcile trade_events with the strategies that really exist.

Why
---
Each open trade is a TradeStrategy kept in the node process' `active_strategies`
dict. After a restart/redeploy that dict is empty, but trade_events still says the
trades are open, so nothing manages them any more (no SL/TP/BE handling, no Closed
event, scanner exits are swallowed by cancel_requests_loop).

What it does (call ONCE at node start, before the listeners are started)
------------------------------------------------------------------------
  1. Find every trade of this node's TARGET that has events but no Closed/Cancelled
     event, and is not in `active_strategies`  -> an "orphan".
  2. virtual: the sandbox exchange lived in the dead process (orders, positions,
     simulated SL/TP are gone), so the trade can never progress. Write a terminal
     'Cancelled' event (cancel_reason = 'node_restart_lost') so it leaves
     /active_trades.
  3. real: Binance still holds the real orders/positions, so NOTHING is closed in
     the DB. A loud Telegram alert lists every orphan with what to check on Binance.
     (An optional `cancel_entry` callback can cancel unfilled entry orders; see below.)
  4. One summary Telegram message either way.

Env vars
--------
  ORPHAN_SWEEP_ENABLED   default "1"
  ORPHAN_SWEEP_DRY_RUN   default "0"  -> "1": only log/alert what WOULD be done, write nothing

Never raises: a failing sweep must not stop the node from starting.
"""

from __future__ import annotations

import html
import os
import sys
from typing import Any, Awaitable, Callable, Dict, List, Optional

REASON_NODE_RESTART_LOST = "node_restart_lost"

# Last event types that mean "the position was already gone, only the final
# Closed event was missing" (crash between SLHit/TPHit/CloseFilled and Closed).
_EXIT_ALREADY_FILLED = ("SLHit", "TPHit", "CloseFilled")

_ORPHANS_SQL = """
SELECT t.trade_id,
       (array_agg(t.instrument ORDER BY t.occurred_at, t.event_id))[1] AS instrument,
       (array_agg(t.side       ORDER BY t.occurred_at, t.event_id))[1] AS side,
       (array_agg(t.size       ORDER BY t.occurred_at, t.event_id))[1] AS size,
       (array_agg(t.ep         ORDER BY t.occurred_at, t.event_id))[1] AS ep,
       (array_agg(t.sl         ORDER BY t.occurred_at, t.event_id))[1] AS sl,
       (array_agg(t.tp         ORDER BY t.occurred_at, t.event_id))[1] AS tp,
       min(t.occurred_at)                                              AS started_at,
       bool_or(t.event_type = 'Filled')                                AS filled,
       (array_agg(t.event_type ORDER BY t.occurred_at DESC, t.event_id DESC))[1] AS last_event
FROM trade_events t
WHERE t.target = $1
GROUP BY t.trade_id
HAVING NOT bool_or(t.event_type IN ('Closed', 'Cancelled'))
ORDER BY min(t.occurred_at)
"""


def _log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def _env_on(name: str, default: str = "1") -> bool:
    return os.getenv(name, default).strip().lower() not in ("0", "false", "no", "off")


def _f(v: Any) -> Optional[float]:
    return None if v is None else float(v)


def _short(v: Any) -> str:
    return "n/a" if v is None else f"{float(v):.8g}"


async def find_orphans(db: Any, target: str, active_strategies: Dict[str, Any]) -> List[Dict[str, Any]]:
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(_ORPHANS_SQL, target)
    return [dict(r) for r in rows if r["trade_id"] not in active_strategies]


async def _telegram(text: str) -> None:
    if not _env_on("TELEGRAM_ENABLED"):
        return
    try:
        from Telegrammer import Telegrammer
        await Telegrammer().send_message_async(text)
    except Exception as e:
        _log(f"[sweeper] telegram failed: {type(e).__name__}: {e}")


def _describe(o: Dict[str, Any]) -> str:
    ticker = str(o["instrument"]).rsplit(".", 1)[0]
    if o["last_event"] in _EXIT_ALREADY_FILLED:
        state = f"exit already filled ({o['last_event']}), only the Closed event is missing"
    elif o["filled"]:
        state = "entry FILLED, position open"
    else:
        state = "entry not filled"
    return (f"<b>{html.escape(ticker)}</b> {html.escape(str(o['side']).upper())} "
            f"size {_short(o['size'])} ep {_short(o['ep'])} sl {_short(o['sl'])} tp {_short(o['tp'])}\n"
            f"   {html.escape(state)} | last event: {html.escape(str(o['last_event']))}\n"
            f"   <code>{html.escape(o['trade_id'])}</code>")


async def sweep_orphans(
    db: Any,
    target: str,
    active_strategies: Dict[str, Any],
    *,
    cancel_entry: Optional[Callable[[Dict[str, Any]], Awaitable[bool]]] = None,
) -> int:
    """Returns the number of orphans found. Never raises.

    `cancel_entry` (real node only, optional): async callable(orphan) -> bool that cancels
    the resting entry order `edge-<trade_id>-entry` on Binance. It is only called for orphans
    whose entry is NOT filled. Not wired by default (see the notes in the README text)."""
    if not _env_on("ORPHAN_SWEEP_ENABLED"):
        _log("[sweeper] disabled (ORPHAN_SWEEP_ENABLED=0)")
        return 0
    dry = _env_on("ORPHAN_SWEEP_DRY_RUN", "0")

    try:
        orphans = await find_orphans(db, target, active_strategies)
    except Exception as e:
        _log(f"❌ [sweeper] could not query orphans: {type(e).__name__}: {e}")
        return 0

    if not orphans:
        _log(f"[sweeper] target={target}: no orphaned trades")
        return 0

    _log(f"[sweeper] target={target}: {len(orphans)} orphaned trade(s){' (DRY RUN)' if dry else ''}")
    lines: List[str] = []

    if target == "virtual":
        for o in orphans:
            outcome = "would cancel" if dry else "cancelled"
            if not dry:
                try:
                    await db.insert_trade_event(
                        trade_id=o["trade_id"],
                        event_type="Cancelled",
                        instrument=o["instrument"],
                        side=o["side"],
                        size=_f(o["size"]),
                        occurred_at=None,
                        ep=_f(o["ep"]), sl=_f(o["sl"]), tp=_f(o["tp"]),
                        cancel_reason=REASON_NODE_RESTART_LOST,
                        metadata={
                            "source": "orphan_sweeper",
                            "entry_filled": bool(o["filled"]),
                            "last_event": o["last_event"],
                            "note": "node restarted; sandbox exchange state was lost",
                        },
                        target=target,
                    )
                except Exception as e:
                    outcome = f"FAILED ({type(e).__name__}: {e})"
                    _log(f"❌ [sweeper] could not close {o['trade_id']}: {e!r}")
            _log(f"[sweeper] {outcome}: {o['trade_id']} (filled={o['filled']}, last={o['last_event']})")
            lines.append(f"{_describe(o)}\n   → {html.escape(outcome)} ({REASON_NODE_RESTART_LOST})")

        head = (f"🟠 [VIRTUAL] node restarted: {len(orphans)} orphaned trade(s) "
                f"{'would be ' if dry else ''}closed as {REASON_NODE_RESTART_LOST} (sandbox state is lost)")

    else:  # real: never auto-close in the DB, Binance still has the real state
        for o in orphans:
            action = ""
            if o["last_event"] in _EXIT_ALREADY_FILLED:
                action = "check Binance; the position should already be flat"
            elif o["filled"]:
                action = (f"CHECK BINANCE: position may be open; SL/TP orders "
                          f"edge-{o['trade_id']}-sl / -tp may still be resting. Unmanaged: no break-even, no exit signals")
            else:
                action = f"CHECK BINANCE: entry order edge-{o['trade_id']}-entry may still be resting and could fill UNPROTECTED"
                if cancel_entry is not None and not dry:
                    try:
                        ok = await cancel_entry(o)
                        action = "entry order cancelled on Binance" if ok else action + " (auto-cancel failed)"
                    except Exception as e:
                        action += f" (auto-cancel failed: {type(e).__name__}: {e})"
            _log(f"[sweeper] REAL orphan {o['trade_id']} filled={o['filled']} last={o['last_event']} -> {action}")
            lines.append(f"{_describe(o)}\n   → {html.escape(action)}")

        head = (f"🚨 [REAL] node restarted: {len(orphans)} trade(s) are NO LONGER MANAGED by the node. "
                f"Nothing was changed in the database.")

    # Telegram messages are limited to 4096 chars: split the list.
    chunk, size = [head], len(head)
    for line in lines:
        if size + len(line) > 3500:
            await _telegram("\n\n".join(chunk))
            chunk, size = [], 0
        chunk.append(line)
        size += len(line) + 2
    await _telegram("\n\n".join(chunk))
    return len(orphans)

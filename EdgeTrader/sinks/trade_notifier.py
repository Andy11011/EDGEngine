"""
trade_notifier.py — Telegram + Google Sheets side effects for finished/ongoing trades.

TradeStrategy calls `get_notifier().submit(db, event)` after every trade_events
write. This module:

  * pushes each event to ONE in-process queue drained by ONE worker task, so
    messages arrive in the order the SM produced them, and slow/failing
    Telegram or Google calls can never block order handling or DB writes;
  * for every non-terminal event -> blue-circle Telegram message;
  * for terminal events (Closed / Cancelled) -> reloads the trade from the DB,
    computes metrics with trade_metrics.compute_trade_metrics (the same code
    the API uses), sends the green/red result message, and, only if we were
    actually in the trade (entry filled and trade closed), appends one row to
    Google Sheets.

Nothing here ever raises into the strategy. Missing config just disables the
channel with a log line.

Env vars:
  TELEGRAM_ENABLED        default "1"   (also needs TELEGRAM_BOT_TOKEN/CHAT_ID or telegrammer.json)
  GSHEETS_ENABLED         default "1"
  GSHEETS_CREDENTIALS_FILE   service-account json path (required for sheets)
  GSHEETS_SPREADSHEET_KEY    spreadsheet key (required for sheets)
  GSHEETS_SHEET_ID           optional worksheet id (first sheet if unset); real and
                             virtual trades share it, told apart by the Virtual column
"""

from __future__ import annotations

import asyncio
import datetime
import os
import sys
from typing import Any, Dict, Optional

import telegram_messages as msgs
from trade_metrics import compute_trade_metrics

TERMINAL_EVENTS = ("Closed", "Cancelled")


def _log(text: str) -> None:
    print(f"[notifier] {text}", file=sys.stderr)


def _env_on(name: str) -> bool:
    return os.getenv(name, "1").strip().lower() not in ("0", "false", "no", "off")


class TradeNotifier:
    def __init__(self) -> None:
        self._queue: "asyncio.Queue[tuple[Any, Dict[str, Any]]]" = asyncio.Queue()
        self._worker: Optional[asyncio.Task] = None

        self._telegram = None
        if _env_on("TELEGRAM_ENABLED"):
            try:
                from Telegrammer import Telegrammer
                self._telegram = Telegrammer()
            except Exception as e:
                _log(f"Telegram disabled: {type(e).__name__}: {e}")

        self._sheets_enabled = _env_on("GSHEETS_ENABLED")
        self._sheet_client = None  # one GSHClient, created lazily

    # ---- public, non-blocking ------------------------------------------
    def submit(self, db: Any, event: Dict[str, Any]) -> None:
        try:
            if self._worker is None or self._worker.done():
                self._worker = asyncio.get_running_loop().create_task(self._run())
            self._queue.put_nowait((db, event))
        except Exception as e:
            _log(f"submit failed: {type(e).__name__}: {e}")

    # ---- worker ---------------------------------------------------------
    async def _run(self) -> None:
        while True:
            db, ev = await self._queue.get()
            try:
                await self._handle(db, ev)
            except Exception as e:  # never let one bad event kill the worker
                _log(f"handling {ev.get('event_type')} failed: {type(e).__name__}: {e}")

    async def _handle(self, db: Any, ev: Dict[str, Any]) -> None:
        if ev["event_type"] not in TERMINAL_EVENTS:
            text = msgs.render_action(ev)
            if text:
                await self._send(text)
            return

        trade, metrics = None, None
        try:
            rows = await db.get_trades_history(trade_id=ev["trade_id"], limit=1, include_events=True)
            if rows:
                trade = rows[0]
                metrics = compute_trade_metrics(trade, trade.get("events", []))
        except Exception as e:
            _log(f"result lookup failed for {ev['trade_id']}: {type(e).__name__}: {e}")

        await self._send(msgs.render_result(ev, trade, metrics))

        # Sheets: only if we were really engaged and the trade closed with a computed result.
        if (
            ev["event_type"] == "Closed"
            and trade is not None
            and metrics is not None
            and trade.get("entry_fill_price") is not None
        ):
            await self._write_sheet(ev, trade, metrics)

    # ---- channels ---------------------------------------------------------
    async def _send(self, text: str) -> None:
        if self._telegram is None:
            return
        ok = await self._telegram.send_message_async(text)
        if not ok:
            _log("telegram send failed")

    def _get_sheet(self):
        if self._sheet_client is not None:
            return self._sheet_client
        creds = os.getenv("GSHEETS_CREDENTIALS_FILE")
        key = os.getenv("GSHEETS_SPREADSHEET_KEY")
        if not creds or not key:
            raise RuntimeError("GSHEETS_CREDENTIALS_FILE / GSHEETS_SPREADSHEET_KEY not set")
        from GSHClient import GSHClient
        self._sheet_client = GSHClient(creds, key, os.getenv("GSHEETS_SHEET_ID"))
        return self._sheet_client

    @staticmethod
    def _sheet_row(ev: Dict[str, Any], trade: Dict[str, Any], m: Any) -> Dict[str, Any]:
        return {
            "win_lose": None if m.win is None else int(m.win),
            "profit_trade": m.net_pnl,
            "total_fee_bnb": m.fee_bnb,
            "fee_usdt_equiv": m.fee_usdt,
            "virtual": 1 if m.target == "virtual" else 0,
            # ASSUMPTION: "good trade" = closed by take-profit. Change here if you define it differently.
            "good_trade": 1 if ev.get("close_reason") == "tp_hit" else 0,
            "type": "Long" if m.side == "BUY" else "Short",
            "pair": m.ticker,
            "Duration": datetime.timedelta(seconds=m.duration_s or 0),
        }

    async def _write_sheet(self, ev: Dict[str, Any], trade: Dict[str, Any], m: Any) -> None:
        if not self._sheets_enabled:
            return
        loop = asyncio.get_running_loop()
        try:
            client = await loop.run_in_executor(None, self._get_sheet)
            row = self._sheet_row(ev, trade, m)
            comment = f"{'Virtual' if m.target == 'virtual' else 'Live'} | {ev['trade_id']}"
            await loop.run_in_executor(
                None, lambda: client.write_trade_to_sheets(row, comment=comment)
            )
        except Exception as e:
            _log(f"sheets write failed for {ev['trade_id']}: {type(e).__name__}: {e}")


_notifier: Optional[TradeNotifier] = None


def get_notifier() -> TradeNotifier:
    global _notifier
    if _notifier is None:
        _notifier = TradeNotifier()
    return _notifier

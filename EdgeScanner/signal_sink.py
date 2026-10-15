"""SignalSink: strategy callbacks -> queues -> Postgres (and optional webhook).

Extracted from EDGEScanner.py. Strategies call SignalSink.submit() (sync,
non-blocking); background writer tasks drain the queues.
"""

from __future__ import annotations

import asyncio
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import aiohttp
import asyncpg

from signals_db_async import SignalsDB


def log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


# -----------------------------------------------------------------------------
# Signal sink: strategy callbacks -> queue -> Postgres
# -----------------------------------------------------------------------------
_CROSS_LABELS = {
    "OB_CROSSOVER": "OVERBOUGHT CROSSOVER",
    "OB_CROSSUNDER": "OVERBOUGHT CROSSUNDER",
    "OS_CROSSOVER": "OVERSOLD CROSSOVER",
    "OS_CROSSUNDER": "OVERSOLD CROSSUNDER",
}

# Symbol per signal type (red/green triangle, up/down).
_SIGNAL_SYMBOLS = {
    "OS_CROSSOVER": "🔴▼",
    "OS_CROSSUNDER": "🔴▲",
    "OB_CROSSOVER": "🟢▼",
    "OB_CROSSUNDER": "🟢▲",
}


def _bar_time_utc(bar_time_ms: int) -> str:
    """Bar timestamp (epoch ms) as an ISO-8601 UTC string, e.g. 2026-10-15T14:30:00Z."""
    return datetime.fromtimestamp(bar_time_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


_TRANSIENT_DB_ERRORS = (
    OSError,
    ConnectionError,
    asyncpg.PostgresConnectionError,
    asyncpg.InterfaceError,
    asyncpg.CannotConnectNowError,
)


class SignalSink:
    """
    Strategies call submit() (sync, non-blocking). Two independent writer tasks
    drain their queues:

      - DB queue -> Postgres, retries forever on transient errors (signals are
        kept in memory meanwhile); non-transient errors drop only the offending
        signal so one bad row can't block the queue.
      - Webhook queue -> HTTP POST, best-effort: bounded retries with backoff,
        then drop + log. Kept separate so a hung webhook endpoint can't stall
        DB writes (and vice versa).
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        ttl_hours: int,
        *,
        webhook_url: Optional[str] = None,
        webhook_timeout: float = 10.0,
        webhook_max_retries: int = 3,
    ):
        self._loop = loop
        self._queue: asyncio.Queue = asyncio.Queue()
        self._webhook_queue: asyncio.Queue = asyncio.Queue()
        self.ttl_hours = ttl_hours
        self._webhook_url = webhook_url
        self._webhook_timeout = webhook_timeout
        self._webhook_max_retries = webhook_max_retries

    def min_bar_ts_ms(self) -> int:
        """Historical signals older than this are not persisted (outside TTL)."""
        return int((time.time() - self.ttl_hours * 3600) * 1000)

    def submit(self, item: dict) -> None:
        # call_soon_threadsafe: safe whether or not the caller is on the loop thread
        self._loop.call_soon_threadsafe(self._queue.put_nowait, item)
        if self._webhook_url and item.get("source") == "live":
            self._loop.call_soon_threadsafe(self._webhook_queue.put_nowait, item)

    async def run(self) -> None:
        db = await SignalsDB.get_instance()
        tasks = [asyncio.create_task(self._db_worker(db), name="signal-db")]
        if self._webhook_url:
            tasks.append(asyncio.create_task(self._webhook_worker(), name="signal-webhook"))
        await asyncio.gather(*tasks)

    # ---- DB writer (unchanged semantics) ----
    async def _db_worker(self, db: SignalsDB) -> None:
        while True:
            item = await self._queue.get()
            await self._write_db(db, item)

    async def _write_db(self, db: SignalsDB, item: dict) -> None:
        delay = 2.0
        while True:
            try:
                await db.insert_signal(**item)
                return
            except _TRANSIENT_DB_ERRORS as e:
                log(f"⚠️ DB unavailable ({e!r}); retrying in {delay:.0f}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60.0)
            except Exception as e:
                log(f"❌ Dropping signal {item.get('symbol')} {item.get('signal_type')}: {e!r}")
                return

    # ---- Webhook writer ----
    async def _webhook_worker(self) -> None:
        async with aiohttp.ClientSession() as session:
            while True:
                item = await self._webhook_queue.get()
                await self._post_webhook(session, item)

    async def _post_webhook(self, session: aiohttp.ClientSession, item: dict) -> None:
        # Human-friendly crossover label for the receiver.
        cross = _CROSS_LABELS.get(item["signal_type"], item["signal_type"])
        symbol = _SIGNAL_SYMBOLS.get(item["signal_type"], "")
        utc_time = _bar_time_utc(item["bar_time_ms"])

        payload = {
            "symbol": item["symbol"],            # e.g. "BTCUSDT"
            "signal_type": item["signal_type"],  # OB_/OS_ CROSSOVER / CROSSUNDER
            "crossover": cross,                  # e.g. "OVERSOLD CROSSOVER"
            "timeframe": item["timeframe"],      # e.g. "15-MINUTE"
            "mlmi": item["rsi"],                 # MLMI prediction
            "rsi": item["rsi"],                  # legacy alias of "mlmi" for existing receivers
            "close": item["close"],
            "bar_time_ms": item["bar_time_ms"],
            "utc_time": utc_time,                # bar time in UTC, ISO-8601
            "signal_symbol": symbol,             # e.g. "🟢▲"
            "message": (
                f"{symbol} {item['symbol']} {item['timeframe']} MLMI "
                f"{cross} ({item['rsi']:.1f}) @ {item['close']} [{utc_time}]"
            ),
        }
        delay = 1.0
        for attempt in range(1, self._webhook_max_retries + 1):
            try:
                async with session.post(
                    self._webhook_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=self._webhook_timeout),
                ) as resp:
                    if 200 <= resp.status < 300:
                        return
                    body = await resp.text()
                    log(f"⚠️ Webhook -> {resp.status}: {body[:200]!r}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log(
                    f"⚠️ Webhook attempt {attempt}/{self._webhook_max_retries} "
                    f"failed for {item['symbol']} {item['signal_type']}: {e!r}"
                )
            if attempt < self._webhook_max_retries:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 15.0)
        log(
            f"❌ Dropping webhook for {item['symbol']} {item['signal_type']} "
            f"after {self._webhook_max_retries} attempts"
        )

    # ---- shutdown flush ----
    async def drain(self, timeout: float = 10.0) -> None:
        db = await SignalsDB.get_instance()

        async def _flush_db() -> None:
            while not self._queue.empty():
                await self._write_db(db, self._queue.get_nowait())

        async def _flush_webhooks() -> None:
            if not self._webhook_url:
                return
            async with aiohttp.ClientSession() as session:
                while not self._webhook_queue.empty():
                    item = self._webhook_queue.get_nowait()
                    if item.get("source") != "live":
                        continue
                    await self._post_webhook(session, item)

        try:
            await asyncio.wait_for(
                asyncio.gather(_flush_db(), _flush_webhooks()),
                timeout,
            )
        except asyncio.TimeoutError:
            remaining = self._queue.qsize() + self._webhook_queue.qsize()
            log(f"⚠️ {remaining} signal(s) not flushed before shutdown")

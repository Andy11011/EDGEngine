"""SignalSink: strategy callbacks -> queues -> Postgres (and optional webhook / SQS).

Extracted from EDGEScanner.py. Strategies call SignalSink.submit() (sync,
non-blocking); background writer tasks drain the queues.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

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
SIGNAL_SYMBOLS = {
    "OS_CROSSOVER": "🔴▼",
    "OS_CROSSUNDER": "🔴▲",
    "OB_CROSSOVER": "🟢▼",
    "OB_CROSSUNDER": "🟢▲",
}


def bar_time_utc(bar_time_ms: int) -> str:
    """Bar timestamp (epoch ms) as an ISO-8601 UTC string, e.g. 2026-10-15T14:30:00Z."""
    return datetime.fromtimestamp(bar_time_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# -----------------------------------------------------------------------------
# Display timezone (env DISPLAY_TIMEZONE; default = Kazakhstan, UTC+5 since 2024-03-01)
# Accepts an IANA name ("Asia/Almaty") or a fixed offset ("+05:00", "UTC+5").
# Fixed offsets need no tz database; IANA names need tzdata >= 2024a for the
# 2024 Kazakhstan change (python: `pip install tzdata`).
# -----------------------------------------------------------------------------
DEFAULT_DISPLAY_TIMEZONE = "Asia/Almaty"


def resolve_timezone(name: str) -> tzinfo:
    name = (name or "").strip() or DEFAULT_DISPLAY_TIMEZONE
    if name.upper() in ("UTC", "Z"):
        return timezone.utc
    m = re.fullmatch(r"(?:UTC)?([+-])(\d{1,2})(?::?(\d{2}))?", name, re.IGNORECASE)
    if m:
        delta = timedelta(hours=int(m.group(2)), minutes=int(m.group(3) or 0))
        return timezone(-delta if m.group(1) == "-" else delta)
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        log(f"⚠️ Unknown DISPLAY_TIMEZONE {name!r} (is the tzdata package installed?); falling back to UTC")
        return timezone.utc


DISPLAY_TZ_NAME = os.getenv("DISPLAY_TIMEZONE", DEFAULT_DISPLAY_TIMEZONE).strip() or DEFAULT_DISPLAY_TIMEZONE
DISPLAY_TZ = resolve_timezone(DISPLAY_TZ_NAME)


def _fmt_offset(dt: datetime) -> str:
    off = dt.strftime("%z")                 # "+0500"
    return f"UTC{off[:3]}:{off[3:]}"


def bar_time_local(bar_time_ms: int) -> str:
    """Bar timestamp in the display timezone, e.g. '2026-10-14 22:44:59 UTC+05:00'."""
    dt = datetime.fromtimestamp(bar_time_ms / 1000, tz=DISPLAY_TZ)
    return f"{dt:%Y-%m-%d %H:%M:%S} {_fmt_offset(dt)}"


def display_timezone_info() -> str:
    """One-line description for the startup log (shows the offset actually in effect)."""
    return f"{DISPLAY_TZ_NAME} ({_fmt_offset(datetime.now(DISPLAY_TZ))})"


_TRANSIENT_DB_ERRORS = (
    OSError,
    ConnectionError,
    asyncpg.PostgresConnectionError,
    asyncpg.InterfaceError,
    asyncpg.CannotConnectNowError,
)

# Signal types that are forwarded to the webhook. Everything else is still
# written to Postgres, but only logged. Add types here to enable them later.
WEBHOOK_SIGNAL_TYPES = frozenset({"OS_CROSSOVER"})

# Live signal types published to the scanner-events SQS queue (SQS_SCANNER_EVENTS_QUEUE_URL).
# Only the overbought crossover for now; override with SQS_SIGNAL_TYPES.
SQS_SIGNAL_TYPES = frozenset({"OB_CROSSOVER"})


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
      - SQS queue -> scanner-events (live signals of sqs_signal_types only),
        same best-effort policy as the webhook, again fully independent.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        ttl_hours: int,
        *,
        webhook_url: Optional[str] = None,
        webhook_timeout: float = 10.0,
        webhook_max_retries: int = 3,
        webhook_signal_types: frozenset[str] = WEBHOOK_SIGNAL_TYPES,
        db_enabled: bool = True,
        sqs_queue_url: Optional[str] = None,
        sqs_region: str = "ap-southeast-1",
        sqs_max_retries: int = 3,
        sqs_signal_types: frozenset[str] = SQS_SIGNAL_TYPES,
    ):
        self._loop = loop
        self.db_enabled = db_enabled        # False = webhook only, Postgres is never touched
        self._queue: asyncio.Queue = asyncio.Queue()
        self._webhook_queue: asyncio.Queue = asyncio.Queue()
        self.ttl_hours = ttl_hours
        self._webhook_url = webhook_url
        self._webhook_timeout = webhook_timeout
        self._webhook_max_retries = webhook_max_retries
        self._webhook_signal_types = webhook_signal_types

        self._sqs_queue: asyncio.Queue = asyncio.Queue()
        self._sqs_queue_url = sqs_queue_url
        self._sqs_max_retries = sqs_max_retries
        self._sqs_signal_types = sqs_signal_types
        self._sqs_client = None
        if sqs_queue_url:
            unknown = sorted(sqs_signal_types - set(_CROSS_LABELS))
            if unknown:
                log(f"⚠️ SQS_SIGNAL_TYPES has unknown type(s) {unknown}; valid: {sorted(_CROSS_LABELS)}")
            # Created here (not lazily) so a missing boto3 fails at startup, not on the first signal.
            import boto3
            self._sqs_client = boto3.client("sqs", region_name=sqs_region)

    def _goes_to_sqs(self, item: dict) -> bool:
        return bool(
            self._sqs_queue_url
            and item.get("source") == "live"
            and item.get("signal_type") in self._sqs_signal_types
        )

    def min_bar_ts_ms(self) -> int:
        """Historical signals older than this are not persisted (outside TTL)."""
        return int((time.time() - self.ttl_hours * 3600) * 1000)

    def submit(self, item: dict) -> None:
        # call_soon_threadsafe: safe whether or not the caller is on the loop thread
        if self.db_enabled:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, item)

        if self._goes_to_sqs(item):
            self._loop.call_soon_threadsafe(self._sqs_queue.put_nowait, item)

        if self._webhook_url and item.get("source") == "live":
            if item.get("signal_type") in self._webhook_signal_types:
                self._loop.call_soon_threadsafe(self._webhook_queue.put_nowait, item)
            elif not self._goes_to_sqs(item):
                # Not forwarded for now: log only (the DB write above still happens)
                cross = _CROSS_LABELS.get(item.get("signal_type"), item.get("signal_type"))
                log(
                    f"🔕 Webhook skipped (log only): {item.get('symbol')} "
                    f"{item.get('timeframe')} {cross} "
                    f"({item.get('rsi', float('nan')):.1f}) @ {item.get('close')} "
                    f"[{bar_time_utc(item['bar_time_ms'])}]"
                )

    async def run(self) -> None:
        tasks = []
        if self.db_enabled:
            db = await SignalsDB.get_instance()
            tasks.append(asyncio.create_task(self._db_worker(db), name="signal-db"))
        if self._webhook_url:
            tasks.append(asyncio.create_task(self._webhook_worker(), name="signal-webhook"))
        if self._sqs_queue_url:
            tasks.append(asyncio.create_task(self._sqs_worker(), name="signal-sqs"))
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

    @staticmethod
    def _build_payload(item: dict) -> dict:
        """Receiver-facing JSON for a signal; the webhook body and the SQS message body are identical."""
        # Human-friendly crossover label for the receiver.
        cross = _CROSS_LABELS.get(item["signal_type"], item["signal_type"])
        symbol = SIGNAL_SYMBOLS.get(item["signal_type"], "")
        utc_time = bar_time_utc(item["bar_time_ms"])
        local_time = bar_time_local(item["bar_time_ms"])

        payload = {
            "symbol": item["symbol"],            # e.g. "BTCUSDT"
            "signal_type": item["signal_type"],  # OB_/OS_ CROSSOVER / CROSSUNDER
            "crossover": cross,                  # e.g. "OVERSOLD CROSSOVER"
            "timeframe": item["timeframe"],      # e.g. "15-MINUTE"
            "mlmi": item["rsi"],                 # MLMI prediction
            "rsi": item["rsi"],                  # legacy alias of "mlmi" for existing receivers
            "close": item["close"],
            "bar_time_ms": item["bar_time_ms"],
            "utc_time": utc_time,                # bar timestamp in UTC, ISO-8601
            "local_time": local_time,            # same instant in DISPLAY_TIMEZONE
            "signal_symbol": symbol,             # e.g. "🟢▲"
            "message": (
                f"{symbol} {item['symbol']} {item['timeframe']} MLMI "
                f"{cross} ({item['rsi']:.1f}) @ {item['close']} [{local_time} | {utc_time}]"
            ),
        }
        return payload

    async def _post_webhook(self, session: aiohttp.ClientSession, item: dict) -> None:
        payload = self._build_payload(item)
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

    # ---- SQS writer (scanner-events) ----
    async def _sqs_worker(self) -> None:
        while True:
            item = await self._sqs_queue.get()
            await self._send_sqs(item)

    async def _send_sqs(self, item: dict) -> None:
        payload = self._build_payload(item)
        body = json.dumps(payload)
        attributes = {
            "signal_type": {"DataType": "String", "StringValue": str(item["signal_type"])},
            "symbol": {"DataType": "String", "StringValue": str(item["symbol"])},
            "timeframe": {"DataType": "String", "StringValue": str(item["timeframe"])},
        }
        loop = asyncio.get_running_loop()
        delay = 1.0
        for attempt in range(1, self._sqs_max_retries + 1):
            try:
                resp = await loop.run_in_executor(
                    None,
                    lambda: self._sqs_client.send_message(
                        QueueUrl=self._sqs_queue_url,
                        MessageBody=body,
                        MessageAttributes=attributes,
                    ),
                )
                log(
                    f"📤 scanner-events: {item['symbol']} {item['signal_type']} "
                    f"[{payload['utc_time']}] (MessageId {resp.get('MessageId')})"
                )
                return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log(
                    f"⚠️ SQS attempt {attempt}/{self._sqs_max_retries} "
                    f"failed for {item['symbol']} {item['signal_type']}: {e!r}"
                )
            if attempt < self._sqs_max_retries:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 15.0)
        log(
            f"❌ Dropping SQS message for {item['symbol']} {item['signal_type']} "
            f"after {self._sqs_max_retries} attempts"
        )

    # ---- shutdown flush ----
    async def drain(self, timeout: float = 10.0) -> None:
        db = await SignalsDB.get_instance() if self.db_enabled else None

        async def _flush_db() -> None:
            if db is None:
                return
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

        async def _flush_sqs() -> None:
            if not self._sqs_queue_url:
                return
            while not self._sqs_queue.empty():
                await self._send_sqs(self._sqs_queue.get_nowait())

        try:
            await asyncio.wait_for(
                asyncio.gather(_flush_db(), _flush_webhooks(), _flush_sqs()),
                timeout,
            )
        except asyncio.TimeoutError:
            remaining = self._queue.qsize() + self._webhook_queue.qsize() + self._sqs_queue.qsize()
            log(f"⚠️ {remaining} signal(s) not flushed before shutdown")

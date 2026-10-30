"""
scanner_signals.py — turns edge-scanner signals (SQS "scanner-events") into trade exits.

What it does, per message
-------------------------
  1. Parse the signal (the JSON edge-scanner's SignalSink publishes).
  2. Ignore it unless its type is one we act on (default: OB_CROSSOVER) and it is
     fresh (SQS SentTimestamp not older than SCANNER_SIGNAL_MAX_AGE_SECONDS).
  3. For each target (virtual, real): is there an ACTIVE trade for that ticker?
       - no  -> nothing to do for that target;
       - yes -> insert a row into `cancel_requests` with reason
                'scanner_ob_crossover' (claimed once per signal and target).
  4. Delete the message.

It never touches a TradeStrategy directly. The existing `cancel_requests_loop` of
each node process picks the row up and calls strategy.request_cancel(reason=...):
  - entry not filled yet -> the entry order is cancelled (Cancelled event,
    cancel_reason = 'scanner_ob_crossover');
  - position open        -> SL/TP are cancelled, then a market exit is sent
    (Closed event, close_reason = 'scanner_ob_crossover').

Why it goes through the DB instead of calling the strategy
-----------------------------------------------------------
The real and the virtual node are separate processes that each own their own
strategies in memory, and a standard SQS queue hands every message to only ONE
consumer. Going through cancel_requests lets any single consumer reach both nodes.
It also makes running more than one consumer harmless: the claim in
claimed_events means each (signal, target) is queued once.

Runs as one more asyncio task in the same event loop as the node (see the nodes'
`asyncio.gather`), or on its own: `python scanner_signals.py`.

Env vars:
  SQS_SCANNER_EVENTS_QUEUE_URL     (required to enable it) scanner-events queue URL
  SCANNER_EXIT_SIGNAL_TYPES        default "OB_CROSSOVER"   comma separated
  SCANNER_EXIT_TARGETS             default "virtual,real"
  SCANNER_SIGNAL_MAX_AGE_SECONDS   default 600   older signals are dropped (e.g. after downtime)
  SCANNER_POLL_WAIT_SECONDS        default 20    SQS long-poll wait
  AWS_REGION                       default "ap-southeast-1"
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import sqs
from trades_db_async import TradeEventsDB

ENV_QUEUE_URL = "SQS_SCANNER_EVENTS_QUEUE_URL"
VENUE_SUFFIX = "BINANCE"      # trade_events.instrument looks like "BTCUSDT.BINANCE"


def _log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def _csv(name: str, default: str, upper: bool) -> tuple[str, ...]:
    raw = os.getenv(name, default)
    return tuple((v.strip().upper() if upper else v.strip().lower()) for v in raw.split(",") if v.strip())


@dataclass(frozen=True)
class ScannerSignalConfig:
    signal_types: tuple[str, ...] = ("OB_CROSSOVER",)
    targets: tuple[str, ...] = ("virtual", "real")
    max_age_seconds: float = 600.0
    poll_wait_seconds: int = 20
    max_messages: int = 10

    @classmethod
    def from_env(cls) -> "ScannerSignalConfig":
        return cls(
            signal_types=_csv("SCANNER_EXIT_SIGNAL_TYPES", "OB_CROSSOVER", upper=True),
            targets=_csv("SCANNER_EXIT_TARGETS", "virtual,real", upper=False),
            max_age_seconds=float(os.getenv("SCANNER_SIGNAL_MAX_AGE_SECONDS", "600")),
            poll_wait_seconds=int(os.getenv("SCANNER_POLL_WAIT_SECONDS", "20")),
        )


# -----------------------------------------------------------------------------
# helpers
# -----------------------------------------------------------------------------
def _sent_ms(msg: Dict[str, Any]) -> Optional[int]:
    """When SQS received the message from the scanner (epoch ms), if reported."""
    try:
        return int(msg["Attributes"]["SentTimestamp"])
    except (KeyError, TypeError, ValueError):
        return None


def _naive_utc_to_ms(dt: datetime) -> int:
    """trade_events.occurred_at is a naive UTC timestamp."""
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def _occurred_at(signal: Dict[str, Any], sent_ms: Optional[int]) -> str:
    """Deterministic ISO-8601 UTC string identifying the signal's bar (claim key)."""
    utc_time = signal.get("utc_time")
    if isinstance(utc_time, str) and utc_time:
        return utc_time
    ms = signal.get("bar_time_ms") or sent_ms or int(time.time() * 1000)
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


async def _receive(sqs_client, queue_url: str, cfg: ScannerSignalConfig) -> list[Dict[str, Any]]:
    """Long-poll the queue (in a thread: boto3 is blocking). Raises on AWS errors.

    Deliberately NOT sqs.receive_trade_events: that one also records this
    process's trade-events queue health for the heartbeat, and the scanner queue
    must not overwrite it. This call also asks for SentTimestamp."""
    loop = asyncio.get_running_loop()
    response = await loop.run_in_executor(
        None,
        lambda: sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=cfg.max_messages,
            WaitTimeSeconds=cfg.poll_wait_seconds,
            AttributeNames=["SentTimestamp"],
            MessageAttributeNames=["All"],
        ),
    )
    return response.get("Messages", [])


# -----------------------------------------------------------------------------
# one message
# -----------------------------------------------------------------------------
async def handle_message(
    db: TradeEventsDB,
    sqs_client,
    queue_url: str,
    cfg: ScannerSignalConfig,
    msg: Dict[str, Any],
) -> None:
    """Process one SQS message. Raises on a DB/AWS problem so the message stays in
    the queue and is retried after its visibility timeout; every deliberate
    'nothing to do' outcome deletes it."""
    msg_id = msg.get("MessageId", "?")
    receipt = msg["ReceiptHandle"]

    async def done() -> None:
        await sqs.delete_message(sqs_client, queue_url, receipt)

    signal = sqs.parse_event(msg)
    if not isinstance(signal, dict) or not signal.get("symbol") or not signal.get("signal_type"):
        _log(f"⚠️ [scanner {msg_id}] not a signal; deleting")
        await done()
        return

    symbol = str(signal["symbol"]).strip().upper()          # "BTCUSDT"
    sig_type = str(signal["signal_type"]).strip().upper()   # "OB_CROSSOVER"
    if sig_type not in cfg.signal_types:
        _log(f"ℹ️ [scanner {msg_id}] {symbol} {sig_type}: not an exit signal; ignored")
        await done()
        return

    sent_ms = _sent_ms(msg)
    if sent_ms is not None:
        age_s = time.time() - sent_ms / 1000
        if age_s > cfg.max_age_seconds:
            _log(f"⌛ [scanner {msg_id}] {symbol} {sig_type}: {age_s:.0f}s old (> {cfg.max_age_seconds:.0f}s); dropped")
            await done()
            return

    instrument = f"{symbol}.{VENUE_SUFFIX}"
    reason = f"scanner_{sig_type.lower()}"                  # -> cancel_reason / close_reason
    occurred_at = _occurred_at(signal, sent_ms)

    queued = 0
    for target in cfg.targets:
        trade = await db.get_active_trade_details(instrument, target)
        if trade is None:
            continue

        # A signal emitted BEFORE the trade existed says nothing about that trade.
        opened_ms = _naive_utc_to_ms(trade["opened_at"]) if trade.get("opened_at") else None
        if sent_ms is not None and opened_ms is not None and sent_ms < opened_ms:
            _log(f"ℹ️ [scanner {msg_id}] {instrument} [{target}]: signal predates trade {trade['trade_id']}; ignored")
            continue

        details = {
            "source": "scanner-events",
            "signal_type": sig_type,
            "symbol": symbol,
            "timeframe": signal.get("timeframe"),
            "bar_time_utc": signal.get("utc_time"),
            "mlmi": signal.get("mlmi"),
            "close": signal.get("close"),
            "sqs_message_id": msg_id,
            "entry_filled": bool(trade.get("filled")),
        }
        request_id = await db.enqueue_signal_cancel(
            ticker=instrument,
            event_type=f"sc_{sig_type}"[:20],               # claimed_events.event_type is VARCHAR(20)
            occurred_at=occurred_at,
            trade_id=trade["trade_id"],
            target=target,
            reason=reason,
            details=details,
        )
        if request_id is None:
            _log(f"♻️ [scanner {msg_id}] {instrument} [{target}]: already handled (duplicate delivery)")
        else:
            queued += 1
            what = "exit at market" if trade.get("filled") else "cancel entry"
            _log(f"🛑 [scanner {msg_id}] {sig_type} {instrument} -> {what} for {trade['trade_id']} (request #{request_id})")

    if queued == 0:
        _log(f"ℹ️ [scanner {msg_id}] {sig_type} {instrument}: no active trade to act on")
    await done()


# -----------------------------------------------------------------------------
# the loop
# -----------------------------------------------------------------------------
async def listen_scanner_signals(
    db: TradeEventsDB,
    sqs_client,
    queue_url: str,
    cfg: Optional[ScannerSignalConfig] = None,
) -> None:
    """Long-poll scanner-events forever. Never raises (except cancellation)."""
    cfg = cfg or ScannerSignalConfig.from_env()
    _log(
        f"📡 scanner-events listener up: acting on {list(cfg.signal_types)} for targets {list(cfg.targets)} "
        f"(max age {cfg.max_age_seconds:.0f}s) <- {queue_url}"
    )
    backoff = 5.0
    while True:
        try:
            messages = await _receive(sqs_client, queue_url, cfg)
            backoff = 5.0
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _log(f"❌ scanner-events receive failed: {e!r}; retrying in {backoff:.0f}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)
            continue

        for msg in messages:
            try:
                await handle_message(db, sqs_client, queue_url, cfg, msg)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                _log(f"❌ scanner-events message {msg.get('MessageId', '?')} failed: {e!r}; left in the queue for retry")


# -----------------------------------------------------------------------------
# standalone: python scanner_signals.py
# -----------------------------------------------------------------------------
async def _main() -> None:
    queue_url = os.getenv(ENV_QUEUE_URL, "").strip()
    if not queue_url:
        _log(f"❌ {ENV_QUEUE_URL} is not set")
        sys.exit(1)
    db = await TradeEventsDB.get_instance()
    await listen_scanner_signals(db, sqs.get_sqs_client(), queue_url)


if __name__ == "__main__":
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass

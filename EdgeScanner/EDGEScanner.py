"""Live MLMI (Machine Learning Momentum Index) signal detector for Binance using NautilusTrader.

Pairs are no longer hard-coded: they are read from travis-scanner's
`scanner_results_l1` table and re-checked periodically. Pairs are added
to / removed from the running Nautilus node as the pair set changes.
(A single multi-pair strategy is used: Nautilus can't add strategies to a running
trader, but subscribe_bars / unsubscribe_bars work at runtime.)

Every MLMI signal (live, plus historical ones inside the TTL window) is
written to the `edge_signals` table via signals_db_async.SignalsDB, which is
what travis' dashboard reads. Rows older than SIGNAL_TTL_HOURS are purged.

The indicator lives in mlmi.py, the pair reconciler in pair_reconciler.py.

Environment (new in this version):
    MLMI_K                        200                 k of the k-NN vote
    MLMI_MOMENTUM_WINDOW          20                  WMA smoothing of the two RSIs
    MLMI_BAND_WINDOW              2000                lookback of the adaptive prediction bands
    MLMI_BAND_MIN_BARS            200                 predictions required before signals fire
    MLMI_ZONE_THRESHOLD           0.0                 exit level of buy / sell zones
    MLMI_CONFIRM_THRESHOLD        0.0                 end of the confirmation zone
    MLMI_ALIGNED_LABELS           0                   1 = label each sample with its own outcome (see mlmi.py)
    MLMI_SIGNALS                  OB_CROSS,OS_CROSS   any of OB_CROSS,OB_EXIT,OS_CROSS,OS_EXIT,MID_UP,MID_DOWN,MA_UP,MA_DOWN
    PAIR_TYPES                    filtered,newcomer   which travis pair_types to scan
    PAIR_POLL_SECONDS             300                 how often to re-read travis' table
    PAIR_REMOVE_AFTER_MISSES      2                   drop a pair only after N consecutive polls without it
    PAIR_ADD_STAGGER_SECONDS      1.0                 delay between adding pairs (Binance backfill rate limits)
    SIGNAL_TTL_HOURS              48
    SIGNAL_PURGE_INTERVAL_SECONDS 3600
    DB_* / DB_POOL_MAX_SIZE       same as trades_db_async.py
"""

from __future__ import annotations

import asyncio
import os
import sys
import json
import re
import time
import aiohttp
from datetime import datetime, timedelta, timezone
from typing import Optional

import asyncpg

from nautilus_trader.adapters.binance import (
    BINANCE,
    BinanceAccountType,
    BinanceDataClientConfig,
    BinanceLiveDataClientFactory,
)
from nautilus_trader.common.enums import LogColor
from nautilus_trader.config import (
    InstrumentProviderConfig,
    LiveDataEngineConfig,
    LiveExecEngineConfig,
    LoggingConfig,
    StrategyConfig,
    TradingNodeConfig,
)
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

from mlmi import MLMI, MLMIResult, SIGNAL_TYPES
from pair_reconciler import PairReconciler
from signals_db_async import SignalsDB


def log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


# -----------------------------------------------------------------------------
# Environment credential loader
# -----------------------------------------------------------------------------
def load_credentials_from_env(sandbox: bool = False) -> tuple[str, str]:
    """Load Binance API key and secret from environment variables."""
    key_var = "BINANCE_SANDBOX_API_KEY" if sandbox else "BINANCE_API_KEY"
    secret_var = "BINANCE_SANDBOX_API_SECRET" if sandbox else "BINANCE_API_SECRET"

    if sandbox:
        print("🏖️ Using sandbox credentials from environment...", file=sys.stderr)

    api_key = os.getenv(key_var, "").strip()
    api_secret = os.getenv(secret_var, "").strip()

    missing = [name for name, val in ((key_var, api_key), (secret_var, api_secret)) if not val]
    if missing:
        raise RuntimeError(f"Missing required environment variable(s): {', '.join(missing)}")
    return api_key, api_secret


# -----------------------------------------------------------------------------
# Helper: parse bar interval string to minutes
# -----------------------------------------------------------------------------
def _parse_interval_minutes(interval_str: str) -> int:
    """Convert Nautilus interval string like '15-MINUTE' or '1-HOUR' to minutes."""
    # Common patterns: <number>-MINUTE, <number>-HOUR, <number>-DAY
    match = re.match(r"(\d+)-(MINUTE|HOUR|DAY)", interval_str, re.IGNORECASE)
    if not match:
        raise ValueError(f"Unsupported interval format: {interval_str}")
    value = int(match.group(1))
    unit = match.group(2).upper()
    if unit == "MINUTE":
        return value
    elif unit == "HOUR":
        return value * 60
    elif unit == "DAY":
        return value * 1440
    raise ValueError(f"Unknown interval unit: {unit}")


# -----------------------------------------------------------------------------
# Signal sink: strategy callbacks -> queue -> Postgres
# -----------------------------------------------------------------------------
_CROSS_LABELS = {"OB_CROSS": "OVERBOUGHT", "OS_CROSS": "OVERSOLD"}

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

        payload = {
            "symbol": item["symbol"],            # e.g. "BTCUSDT"
            "signal_type": item["signal_type"],  # "OB_CROSS" / "OS_CROSS" / ...
            "crossover": cross,                  # "OVERBOUGHT" / "OVERSOLD" / signal name
            "timeframe": item["timeframe"],      # e.g. "15-MINUTE"
            "mlmi": item["rsi"],                 # MLMI prediction
            "rsi": item["rsi"],                  # legacy alias of "mlmi" for existing receivers
            "close": item["close"],
            "bar_time_ms": item["bar_time_ms"],
            "message": (
                f"{item['symbol']} {item['timeframe']} MLMI "
                f"{cross} ({item['rsi']:.1f}) @ {item['close']}"
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


# -----------------------------------------------------------------------------
# MLMI Signal Strategy (ONE strategy, many pairs)
#
# Nautilus refuses to add strategies to a running trader, so a single strategy
# is registered before the node starts and pairs are watched / unwatched at
# runtime via subscribe_bars / unsubscribe_bars (allowed while running).
#
# The indicator itself lives in mlmi.py (one MLMI instance per pair).
# -----------------------------------------------------------------------------
class MLMISignalConfig(StrategyConfig, frozen=True):
    bar_interval: str = "15-MINUTE"
    num_neighbors: int = 200            # k of the k-NN vote
    momentum_window: int = 20           # WMA smoothing of the two RSIs
    band_window: int = 2000             # lookback for the adaptive prediction bands
    band_min_bars: int = 200            # predictions needed before signals may fire
    zone_threshold: float = 0.0
    confirmation_threshold: float = 0.0
    aligned_labels: bool = False        # False = exact Pine behaviour (see mlmi.py)
    enabled_signals: tuple[str, ...] = ("OB_CROSS", "OS_CROSS")
    historical_bars: int = 3000         # signal-eligible past bars (warm-up is added on top)


class _PairState:
    __slots__ = ("bar_type", "mlmi", "warming_up")

    def __init__(self, bar_type: BarType, mlmi: MLMI):
        self.bar_type = bar_type
        self.mlmi = mlmi
        self.warming_up: bool = True


class MLMISignalStrategy(Strategy):
    def __init__(self, config: MLMISignalConfig, sink: Optional[SignalSink] = None):
        super().__init__(config)
        unknown = set(config.enabled_signals) - set(SIGNAL_TYPES)
        if unknown:
            raise ValueError(f"Unknown MLMI signal(s) {sorted(unknown)}; valid: {SIGNAL_TYPES}")
        self._enabled = frozenset(config.enabled_signals)
        self._sink = sink
        self._states: dict[InstrumentId, _PairState] = {}
        self._minutes_per_bar = _parse_interval_minutes(config.bar_interval)

    def _new_mlmi(self) -> MLMI:
        c = self.config
        return MLMI(
            num_neighbors=c.num_neighbors,
            momentum_window=c.momentum_window,
            band_window=c.band_window,
            band_min_bars=c.band_min_bars,
            zone_threshold=c.zone_threshold,
            confirmation_threshold=c.confirmation_threshold,
            aligned_labels=c.aligned_labels,
        )

    # ---- runtime pair management (called by PairReconciler) ----
    @property
    def watched(self) -> set[str]:
        return {iid.symbol.value for iid in self._states}

    def watch(self, instrument_id: InstrumentId) -> bool:
        if instrument_id in self._states:
            return True
        if self.cache.instrument(instrument_id) is None:
            self.log.error(f"Instrument {instrument_id} not found (bad or delisted symbol?). Not subscribing.")
            return False

        bar_type = BarType.from_str(f"{instrument_id}-{self.config.bar_interval}-LAST-EXTERNAL")
        mlmi = self._new_mlmi()
        self._states[instrument_id] = _PairState(bar_type, mlmi)

        total_bars_needed = self.config.historical_bars + mlmi.bars_to_ready
        days_needed = (total_bars_needed * self._minutes_per_bar) / (24 * 60)
        start_dt = datetime.now(timezone.utc) - timedelta(days=days_needed + 1)

        self.log.info(
            f"{instrument_id}: requesting ~{total_bars_needed} historical bars (since {start_dt.date()})",
            color=LogColor.BLUE,
        )
        self.request_bars(bar_type, start=start_dt)
        self.subscribe_bars(bar_type)
        return True

    def unwatch(self, instrument_id: InstrumentId) -> None:
        state = self._states.pop(instrument_id, None)
        if state is None:
            return
        self.unsubscribe_bars(state.bar_type)   # late historical bars for it are ignored (no state)

    # ---- lifecycle ----
    def on_start(self) -> None:
        c = self.config
        self.log.info(
            f"MLMI Signal Strategy started | interval={c.bar_interval}, k={c.num_neighbors}, "
            f"momentum_window={c.momentum_window}, band_window={c.band_window}, "
            f"signals={sorted(self._enabled)}, aligned_labels={c.aligned_labels}",
            color=LogColor.GREEN,
        )

    def on_stop(self) -> None:
        self.log.info("MLMI Signal Strategy stopped", color=LogColor.YELLOW)

    # ---- signals ----
    def _log_signal(self, instrument_id: InstrumentId, bar: Bar, signal_type: str, res: MLMIResult, live: bool) -> None:
        """Log an MLMI signal and hand it to the DB sink."""
        ts_ms = round(bar.ts_event / 1_000_000)
        payload = {
            "symbol": str(instrument_id),
            "type": signal_type,
            "rsi": res.prediction,          # legacy key/column name: now carries the MLMI prediction
            "mlmi": {
                "prediction": res.prediction,
                "prediction_ma": res.prediction_ma,
                "upper": res.upper,
                "lower": res.lower,
                "ob_level": res.ob_level,
                "os_level": res.os_level,
                "zone": res.zone,
                "confirmation_zone": res.confirmation_zone,
            },
            "close": float(bar.close),
            "timestamp": ts_ms,
            "source": "live" if live else "historical",
        }
        self.log.info(f"📝 Signal: {json.dumps(payload)}", color=LogColor.GREEN)

        # Persist: all live signals, and historical ones only inside the TTL window
        if self._sink is not None and (live or ts_ms >= self._sink.min_bar_ts_ms()):
            self._sink.submit({
                "symbol": instrument_id.symbol.value,   # "BTCUSDT" (same format as travis)
                "signal_type": signal_type,
                "timeframe": self.config.bar_interval,
                "bar_time_ms": ts_ms,
                "rsi": res.prediction,                  # column name kept; value is the MLMI prediction
                "close": float(bar.close),
                "source": payload["source"],
                "payload": payload,
            })

    def _emit_signals(self, instrument_id: InstrumentId, bar: Bar, res: MLMIResult, live: bool) -> None:
        for signal_type in res.signals:
            if signal_type not in self._enabled:
                continue
            if live:
                prev = res.prev_prediction
                self.log.warning(
                    f"🚨 {instrument_id} MLMI {signal_type} "
                    f"(prediction {prev:.1f} -> {res.prediction:.1f}, "
                    f"OS {res.os_level:.1f} / OB {res.ob_level:.1f}, zone {res.zone}) 🚨",
                    color=LogColor.MAGENTA if signal_type.startswith("OB") else LogColor.CYAN,
                )
            self._log_signal(instrument_id, bar, signal_type, res, live)

    def on_historical_data(self, data) -> None:
        if not isinstance(data, Bar):
            return
        iid = data.bar_type.instrument_id
        state = self._states.get(iid)
        if state is None:           # pair was unwatched while history was in flight
            return

        res = state.mlmi.update(float(data.close))   # per-pair indicator is updated manually
        if res is None:             # features (smoothed RSIs) not available yet
            return

        self._emit_signals(iid, data, res, live=False)

        if state.warming_up:
            state.warming_up = False
            self.log.info(f"{iid}: features warm. First MLMI prediction={res.prediction:.1f}")

    def on_bar(self, bar: Bar) -> None:
        iid = bar.bar_type.instrument_id
        state = self._states.get(iid)
        if state is None or state.warming_up:
            return

        res = state.mlmi.update(float(bar.close))
        if res is None:
            return
        self._emit_signals(iid, bar, res, live=True)


async def purge_loop(ttl_hours: int, interval_seconds: float) -> None:
    db = await SignalsDB.get_instance()
    while True:
        try:
            deleted = await db.purge_expired(ttl_hours)
            if deleted:
                log(f"🧹 Purged {deleted} expired signal(s) (> {ttl_hours}h)")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log(f"❌ Signal purge failed: {e!r}")
        await asyncio.sleep(interval_seconds)


# -----------------------------------------------------------------------------
# Node construction
# -----------------------------------------------------------------------------
def _resolve_binance_config_kwargs(environment_name: str) -> dict[str, object]:
    normalized = environment_name.upper()
    try:
        from nautilus_trader.adapters.binance.common.enums import BinanceEnvironment
    except ImportError:
        try:
            from nautilus_trader.adapters.binance import BinanceEnvironment
        except ImportError:
            BinanceEnvironment = None

    if BinanceEnvironment is not None:
        if normalized == "LIVE" and hasattr(BinanceEnvironment, "LIVE"):
            return {"environment": BinanceEnvironment.LIVE}
        if normalized == "MAINNET" and hasattr(BinanceEnvironment, "MAINNET"):
            return {"environment": BinanceEnvironment.MAINNET}
        if normalized == "TESTNET" and hasattr(BinanceEnvironment, "TESTNET"):
            return {"environment": BinanceEnvironment.TESTNET}
        if normalized == "DEMO" and hasattr(BinanceEnvironment, "DEMO"):
            return {"environment": BinanceEnvironment.DEMO}

    if normalized in {"LIVE", "MAINNET"}:
        return {"testnet": False}
    if normalized == "TESTNET":
        return {"testnet": True}
    raise ValueError(f"Unsupported Binance environment: {environment_name}. Use LIVE/MAINNET or TESTNET.")


def build_data_only_node(
    *,
    trader_id: str,
    data_clients: dict,
    log_level: str = "INFO",
) -> TradingNode:
    config = TradingNodeConfig(
        trader_id=trader_id,
        logging=LoggingConfig(log_level=log_level, use_pyo3=True),
        data_engine=LiveDataEngineConfig(
            validate_data_sequence=True,
            time_bars_timestamp_on_close=False   # bar timestamps are open time to match frontend
        ),
        exec_engine=LiveExecEngineConfig(
            reconciliation=False,
            generate_missing_orders=False,
            snapshot_orders=False,
            snapshot_positions=False,
        ),
        data_clients=data_clients,
        exec_clients={},
        timeout_connection=30.0,
        timeout_reconciliation=0.0,
        timeout_portfolio=10.0,
        timeout_disconnection=10.0,
        timeout_post_stop=0.0,
    )
    return TradingNode(config=config)


async def _shutdown_background(tasks: list, sink: SignalSink) -> None:
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await sink.drain()                       # flush signals still queued
    db = await SignalsDB.get_instance()
    await db.close()


def run_data_node(
    node: TradingNode,
    strategy: Strategy,
    register_data_client_factories: callable,
    make_background_tasks: callable,
    sink: SignalSink,
) -> None:
    node.trader.add_strategy(strategy)       # must happen BEFORE the node runs
    register_data_client_factories(node)
    node.build()

    loop = node.get_event_loop()
    tasks = make_background_tasks(loop)      # scheduled now, they start once node.run() spins the loop

    try:
        node.run()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            loop.run_until_complete(_shutdown_background(tasks, sink))
        except Exception as e:
            log(f"⚠️ Background shutdown incomplete: {e!r}")
        try:
            node.stop()
        finally:
            node.dispose()


def register_binance_data_client_factory(node: TradingNode) -> None:
    node.add_data_client_factory(BINANCE, BinanceLiveDataClientFactory)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    trader_id = os.getenv("TRADER_ID", "EDGENGINE-001")
    environment = os.getenv("BINANCE_ENV", "LIVE").upper()
    bar_interval = os.getenv("BINANCE_BAR_INTERVAL", "15-MINUTE")
    mlmi_k = int(os.getenv("MLMI_K", "200"))
    momentum_window = int(os.getenv("MLMI_MOMENTUM_WINDOW", "20"))
    band_window = int(os.getenv("MLMI_BAND_WINDOW", "2000"))
    band_min_bars = int(os.getenv("MLMI_BAND_MIN_BARS", "200"))
    zone_threshold = float(os.getenv("MLMI_ZONE_THRESHOLD", "0.0"))
    confirm_threshold = float(os.getenv("MLMI_CONFIRM_THRESHOLD", "0.0"))
    aligned_labels = os.getenv("MLMI_ALIGNED_LABELS", "0") == "1"
    enabled_signals = tuple(
        s.strip().upper() for s in os.getenv("MLMI_SIGNALS", "OB_CROSS,OS_CROSS").split(",") if s.strip()
    )
    log_level = os.getenv("LOG_LEVEL", "INFO")
    sandbox = os.getenv("BINANCE_SANDBOX", "0") == "1"
    aws_region = os.getenv("AWS_REGION", "ap-southeast-1")
    historical_bars = int(os.getenv("HISTORICAL_BARS", "3000"))

    pair_types = tuple(t.strip() for t in os.getenv("PAIR_TYPES", "filtered,newcomer").split(",") if t.strip())
    poll_seconds = float(os.getenv("PAIR_POLL_SECONDS", "300"))
    remove_after = int(os.getenv("PAIR_REMOVE_AFTER_MISSES", "2"))
    add_stagger = float(os.getenv("PAIR_ADD_STAGGER_SECONDS", "1.0"))
    ttl_hours = int(os.getenv("SIGNAL_TTL_HOURS", "48"))
    purge_interval = float(os.getenv("SIGNAL_PURGE_INTERVAL_SECONDS", "3600"))

    webhook_url = os.getenv("WEBHOOK_URL", "").strip() or None
    webhook_timeout = float(os.getenv("WEBHOOK_TIMEOUT_SECONDS", "10"))
    webhook_max_retries = int(os.getenv("WEBHOOK_MAX_RETRIES", "3"))

    try:
        api_key, api_secret = load_credentials_from_env(sandbox=sandbox)
        print("✅ Credentials loaded from environment", file=sys.stderr)
    except Exception as e:
        print(f"❌ Failed to load credentials: {e}", file=sys.stderr)
        sys.exit(1)

    # The pair list is dynamic now, so we can't pre-declare load_ids. Load all
    # Binance spot instruments once at startup (one exchangeInfo call); any pair
    # travis hands us later is then already in the instrument cache.
    binance_config_kwargs = _resolve_binance_config_kwargs(environment)
    binance_client_config = BinanceDataClientConfig(
        api_key=api_key,
        api_secret=api_secret,
        account_type=BinanceAccountType.SPOT,
        instrument_provider=InstrumentProviderConfig(load_all=True),
        **binance_config_kwargs,
    )

    node = build_data_only_node(
        trader_id=trader_id,
        data_clients={BINANCE: binance_client_config},
        log_level=log_level,
    )

    sink = SignalSink(
        node.get_event_loop(),
        ttl_hours=ttl_hours,
        webhook_url=webhook_url,
        webhook_timeout=webhook_timeout,
        webhook_max_retries=webhook_max_retries,
    )

    strategy = MLMISignalStrategy(
        MLMISignalConfig(
            bar_interval=bar_interval,
            num_neighbors=mlmi_k,
            momentum_window=momentum_window,
            band_window=band_window,
            band_min_bars=band_min_bars,
            zone_threshold=zone_threshold,
            confirmation_threshold=confirm_threshold,
            aligned_labels=aligned_labels,
            enabled_signals=enabled_signals,
            historical_bars=historical_bars,
        ),
        sink=sink,
    )
    reconciler = PairReconciler(
        strategy,
        pair_types=pair_types,
        poll_seconds=poll_seconds,
        remove_after_misses=remove_after,
        add_stagger_seconds=add_stagger,
    )

    def make_background_tasks(loop: asyncio.AbstractEventLoop) -> list:
        return [
            loop.create_task(sink.run(), name="signal-sink"),
            loop.create_task(reconciler.run(), name="pair-reconciler"),
            loop.create_task(purge_loop(ttl_hours, purge_interval), name="signal-purge"),
        ]

    run_data_node(node, strategy, register_binance_data_client_factory, make_background_tasks, sink)


if __name__ == "__main__":
    main()

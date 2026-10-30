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

Debug mode (--debug): no Postgres at all (no travis pair table, no edge_signals
writes, no purge). Runs the full MLMI computation for the hard-coded
DEBUG_TICKERS below instead, logging signals to the console and still posting
live signals to WEBHOOK_URL if it is set.

Environment (new in this version):
    MLMI_K                        200                 k of the k-NN vote
    MLMI_MOMENTUM_WINDOW          20                  WMA smoothing of the two RSIs
    MLMI_BAND_WINDOW              2000                lookback of the adaptive prediction bands
    MLMI_BAND_MIN_BARS            200                 predictions required before signals fire
    MLMI_ZONE_THRESHOLD           0.0                 exit level of buy / sell zones
    MLMI_CONFIRM_THRESHOLD        0.0                 end of the confirmation zone
    MLMI_ALIGNED_LABELS           0                   1 = label each sample with its own outcome (see mlmi.py)
    SUPERTREND_PERIOD             10                  ATR length of the SuperTrend
    SUPERTREND_FACTOR             3.0                 ATR multiplier of the SuperTrend
    SUPERTREND_FILTER             1                   1 = MLMI signals only count in an UPTREND; others are dropped (not stored)
    ZVOL_LENGTH                   20                  Z-score length of the volume indicator
    ZVOL_MAX_POINTS               200                 history used for the cluster percentiles
    ZVOL_ANCHOR                   1D                  CVD anchor period: 1D | 1W | 1M | all (UTC)
    ZVOL_CVD_MODE                 anchored            anchored = same as the dashboard JS | bar = per-bar signed volume
    VOLUME_FILTER                 1                   1 = signals in VOLUME_FILTER_TYPES also need 'Strong Bullish' volume
    VOLUME_FILTER_TYPES           OS_CROSSOVER        which signal types need it (comma separated)
    VOLUME_LOOKBACK_BARS          0                   0 = Strong Bullish on the signal bar itself; N = within the last N bars too
    DISPLAY_TIMEZONE              Asia/Almaty         timezone of the local time in logs / webhook (IANA name or +05:00)
    PAIR_TYPES                    filtered,newcomer   which travis pair_types to scan
    PAIR_POLL_SECONDS             300                 how often to re-read travis' table
    PAIR_REMOVE_AFTER_MISSES      2                   drop a pair only after N consecutive polls without it
    PAIR_ADD_STAGGER_SECONDS      1.0                 delay between adding pairs (Binance backfill rate limits)
    SIGNAL_TTL_HOURS              48
    SIGNAL_PURGE_INTERVAL_SECONDS 3600
    SQS_SCANNER_EVENTS_QUEUE_URL  (unset)             SQS queue URL of scanner-events (stack output ScannerEventsQueueUrl); unset = SQS off
    SQS_SIGNAL_TYPES              OB_CROSSOVER        live signal types published to that queue (comma separated)
    SQS_MAX_RETRIES               3                   send attempts per signal before it is dropped
    AWS_REGION                    ap-southeast-1      region of the queue (credentials: instance role / standard AWS chain)
    DB_* / DB_POOL_MAX_SIZE       same as trades_db_async.py
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

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

from mlmi import MLMI, MLMIResult
from supertrend import SuperTrend, SuperTrendResult
from zscore_volume import ZScoreVolume, ZScoreVolumeResult
from pair_reconciler import PairReconciler
from signal_sink import (
    SIGNAL_SYMBOLS,
    SignalSink,
    bar_time_local,
    bar_time_utc,
    display_timezone_info,
)
from signals_db_async import SignalsDB


def log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


# --debug mode: fixed list of Binance spot symbols to scan. Symbols that are
# not listed on spot are skipped with an error log.
DEBUG_TICKERS: tuple[str, ...] = (
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "SOLUSDT",
    "TRXUSDT",
    "DOGEUSDT",
    "LINKUSDT",
    "ADAUSDT",
    "LTCUSDT",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Live MLMI signal scanner for Binance")
    parser.add_argument(
        "--debug",
        action="store_true",
        help="No Postgres: compute MLMI for the hard-coded DEBUG_TICKERS; signals are logged and still sent to the webhook",
    )
    return parser.parse_args()


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
    historical_bars: int = 3000         # signal-eligible past bars (warm-up is added on top)
    supertrend_period: int = 10         # SuperTrend ATR length
    supertrend_factor: float = 3.0      # SuperTrend ATR multiplier
    supertrend_filter: bool = True      # True = drop MLMI signals unless SuperTrend is in an uptrend
    zvol_length: int = 20               # Z-Score Volume: z-score length
    zvol_max_points: int = 200          # Z-Score Volume: history for the cluster percentiles
    zvol_anchor: str = "1D"             # Z-Score Volume: CVD anchor period (UTC)
    zvol_cvd_mode: str = "anchored"     # "anchored" = dashboard JS behaviour, "bar" = per-bar signed volume
    volume_filter: bool = True          # True = signals in volume_filter_types also need Strong Bullish volume
    volume_filter_types: tuple[str, ...] = ("OS_CROSSOVER",)
    volume_lookback_bars: int = 0       # 0 = same bar only; N = Strong Bullish within the last N bars also counts


class _PairState:
    __slots__ = ("bar_type", "mlmi", "st", "zv", "n_bars", "last_strong_bar", "warming_up")

    def __init__(self, bar_type: BarType, mlmi: MLMI, st: SuperTrend, zv: ZScoreVolume):
        self.bar_type = bar_type
        self.mlmi = mlmi
        self.st = st
        self.zv = zv
        self.n_bars: int = 0                    # bars processed so far (history + live)
        self.last_strong_bar: int = -1          # n_bars value of the latest "Strong Bullish" bar
        self.warming_up: bool = True


class MLMISignalStrategy(Strategy):
    def __init__(self, config: MLMISignalConfig, sink: Optional[SignalSink] = None):
        super().__init__(config)
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

    def _new_supertrend(self) -> SuperTrend:
        c = self.config
        return SuperTrend(period=c.supertrend_period, factor=c.supertrend_factor)

    def _new_zvol(self) -> ZScoreVolume:
        c = self.config
        return ZScoreVolume(
            length=c.zvol_length,
            max_points=c.zvol_max_points,
            anchor=c.zvol_anchor,
            cvd_mode=c.zvol_cvd_mode,
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
        self._states[instrument_id] = _PairState(bar_type, mlmi, self._new_supertrend(), self._new_zvol())

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
            f"aligned_labels={c.aligned_labels}, "
            f"supertrend=({c.supertrend_period}, {c.supertrend_factor}) filter={'ON (uptrend only)' if c.supertrend_filter else 'off'}, "
            f"zvol=({c.zvol_length}, {c.zvol_anchor}, {c.zvol_cvd_mode}) "
            f"filter={('ON for ' + ','.join(c.volume_filter_types) + f' (lookback {c.volume_lookback_bars})') if c.volume_filter else 'off'}",
            color=LogColor.GREEN,
        )

    def on_stop(self) -> None:
        self.log.info("MLMI Signal Strategy stopped", color=LogColor.YELLOW)

    # ---- signals ----
    def _log_signal(
        self,
        instrument_id: InstrumentId,
        bar: Bar,
        signal_type: str,
        res: MLMIResult,
        st: Optional[SuperTrendResult],
        zv: Optional[ZScoreVolumeResult],
        strong_bars_ago: Optional[int],
        live: bool,
    ) -> None:
        """Log an MLMI signal and hand it to the DB sink."""
        ts_ms = round(bar.ts_event / 1_000_000)
        utc_time = bar_time_utc(ts_ms)
        local_time = bar_time_local(ts_ms)
        symbol_char = SIGNAL_SYMBOLS.get(signal_type, "")
        payload = {
            "symbol": str(instrument_id),
            "type": signal_type,
            "signal_symbol": symbol_char,
            "utc_time": utc_time,           # bar timestamp, UTC
            "local_time": local_time,       # same instant in DISPLAY_TIMEZONE
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
            "supertrend": (
                {"uptrend": st.uptrend, "value": st.value, "flipped": st.flipped} if st is not None else None
            ),
            "zvolume": (
                {
                    "z": zv.z,
                    "cvd": zv.cvd,
                    "cluster": zv.label,
                    "strong_bullish_bars_ago": strong_bars_ago,     # 0 = on the signal bar, None = never
                }
                if zv is not None else None
            ),
            "close": float(bar.close),
            "timestamp": ts_ms,
            "source": "live" if live else "historical",
        }
        tag = "LIVE" if live else "HIST"
        self.log.info(
            f"📝 [{tag}] {symbol_char} {instrument_id.symbol.value} {signal_type} "
            f"{local_time} ({utc_time}) close={float(bar.close)} mlmi={res.prediction:.1f} | {json.dumps(payload)}",
            color=LogColor.GREEN,
        )

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

    def _reject_reason(self, signal_type: str, st: Optional[SuperTrendResult], vol_ok: bool) -> Optional[str]:
        """Why a signal must be dropped (None = it passes every filter)."""
        c = self.config
        if c.supertrend_filter and (st is None or not st.uptrend):
            return "SuperTrend not in uptrend"
        if c.volume_filter and signal_type in c.volume_filter_types and not vol_ok:
            return "no Strong Bullish volume"
        return None

    def _emit_signals(
        self,
        instrument_id: InstrumentId,
        bar: Bar,
        res: MLMIResult,
        st: Optional[SuperTrendResult],
        zv: Optional[ZScoreVolumeResult],
        strong_bars_ago: Optional[int],
        live: bool,
    ) -> None:
        vol_ok = strong_bars_ago is not None and strong_bars_ago <= self.config.volume_lookback_bars
        for signal_type in res.signals:
            # Filters: SuperTrend uptrend (all types) + Strong Bullish volume (volume_filter_types).
            # Rejected signals are not stored and not sent to the webhook (live ones are logged).
            reason = self._reject_reason(signal_type, st, vol_ok)
            if reason is not None:
                if live:
                    self.log.info(
                        f"🚫 {instrument_id.symbol.value} MLMI {signal_type} dropped: {reason} "
                        f"[{bar_time_local(round(bar.ts_event / 1_000_000))}]",
                        color=LogColor.YELLOW,
                    )
                continue
            if live:
                prev = res.prev_prediction
                self.log.warning(
                    f"🚨 {SIGNAL_SYMBOLS.get(signal_type, '')} {instrument_id} MLMI {signal_type} "
                    f"[{bar_time_local(round(bar.ts_event / 1_000_000))}] "
                    f"(prediction {prev:.1f} -> {res.prediction:.1f}, "
                    f"OS {res.os_level:.1f} / OB {res.ob_level:.1f}, zone {res.zone}) 🚨",
                    color=LogColor.MAGENTA if signal_type.startswith("OB") else LogColor.CYAN,
                )
            self._log_signal(instrument_id, bar, signal_type, res, st, zv, strong_bars_ago, live)

    def _advance(self, state: _PairState, bar: Bar):
        """Feed one closed bar to every indicator of a pair.

        All indicators are updated on EVERY bar, before any early return, so that
        SuperTrend / volume never skip bars while MLMI is still warming up.
        Returns (mlmi_result, supertrend_result, zvol_result, strong_bullish_bars_ago).
        """
        ts_ms = round(bar.ts_event / 1_000_000)
        o, h, l, c, v = float(bar.open), float(bar.high), float(bar.low), float(bar.close), float(bar.volume)

        state.n_bars += 1
        st = state.st.update(h, l, c)
        zv = state.zv.update(ts_ms, o, c, v)
        if zv.strong_bullish:
            state.last_strong_bar = state.n_bars
        strong_bars_ago = state.n_bars - state.last_strong_bar if state.last_strong_bar >= 0 else None

        res = state.mlmi.update(c)
        return res, st, zv, strong_bars_ago

    def on_historical_data(self, data) -> None:
        if not isinstance(data, Bar):
            return
        iid = data.bar_type.instrument_id
        state = self._states.get(iid)
        if state is None:           # pair was unwatched while history was in flight
            return

        res, st, zv, strong_bars_ago = self._advance(state, data)   # indicators are updated manually
        if res is None:             # features (smoothed RSIs) not available yet
            return

        self._emit_signals(iid, data, res, st, zv, strong_bars_ago, live=False)

        if state.warming_up:
            state.warming_up = False
            self.log.info(f"{iid}: features warm. First MLMI prediction={res.prediction:.1f}")

    def on_bar(self, bar: Bar) -> None:
        iid = bar.bar_type.instrument_id
        state = self._states.get(iid)
        if state is None or state.warming_up:
            return

        res, st, zv, strong_bars_ago = self._advance(state, bar)
        if res is None:
            return
        self._emit_signals(iid, bar, res, st, zv, strong_bars_ago, live=True)


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
async def debug_watch_loop(strategy: MLMISignalStrategy, symbols: tuple[str, ...], stagger_seconds: float) -> None:
    """Debug replacement for PairReconciler: watch a fixed ticker list once the strategy is running."""
    while not strategy.is_running:
        await asyncio.sleep(0.5)
    for symbol in symbols:
        try:
            strategy.watch(InstrumentId.from_str(f"{symbol}.{BINANCE}"))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log(f"❌ Debug watch failed for {symbol}: {e!r}")
        await asyncio.sleep(stagger_seconds)
    log(f"🐞 Debug: watching {len(strategy.watched)}/{len(symbols)} ticker(s): {sorted(strategy.watched)}")


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
    if sink.db_enabled:                      # debug mode never opened the DB
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
    args = _parse_args()
    debug = args.debug
    if debug:
        log("🐞 DEBUG mode: no Postgres, no pair reconciler; "
            f"scanning {len(DEBUG_TICKERS)} hard-coded ticker(s)")

    log(f"🕒 Display timezone: {display_timezone_info()}")
    trader_id = os.getenv("TRADER_ID", "EDGENGINE-001")
    environment = os.getenv("BINANCE_ENV", "LIVE").upper()
    bar_interval = os.getenv("BINANCE_BAR_INTERVAL", "15-MINUTE")
    mlmi_k = int(os.getenv("MLMI_K", "200"))
    momentum_window = int(os.getenv("MLMI_MOMENTUM_WINDOW", "20"))
    band_window = int(os.getenv("MLMI_BAND_WINDOW", "30000"))
    band_min_bars = int(os.getenv("MLMI_BAND_MIN_BARS", "20000"))
    zone_threshold = float(os.getenv("MLMI_ZONE_THRESHOLD", "0.0"))
    confirm_threshold = float(os.getenv("MLMI_CONFIRM_THRESHOLD", "0.0"))
    aligned_labels = os.getenv("MLMI_ALIGNED_LABELS", "0") == "1"
    supertrend_period = int(os.getenv("SUPERTREND_PERIOD", "30"))
    supertrend_factor = float(os.getenv("SUPERTREND_FACTOR", "8.0"))
    supertrend_filter = os.getenv("SUPERTREND_FILTER", "1") == "1"
    zvol_length = int(os.getenv("ZVOL_LENGTH", "20"))
    zvol_max_points = int(os.getenv("ZVOL_MAX_POINTS", "200"))
    zvol_anchor = os.getenv("ZVOL_ANCHOR", "1D").strip()
    zvol_cvd_mode = os.getenv("ZVOL_CVD_MODE", "anchored").strip().lower()
    volume_filter = os.getenv("VOLUME_FILTER", "1") == "1"
    volume_filter_types = tuple(
        t.strip().upper() for t in os.getenv("VOLUME_FILTER_TYPES", "OS_CROSSOVER").split(",") if t.strip()
    )
    volume_lookback_bars = int(os.getenv("VOLUME_LOOKBACK_BARS", "2"))
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

    sqs_queue_url = os.getenv("SQS_SCANNER_EVENTS_QUEUE_URL", "").strip() or None
    sqs_max_retries = int(os.getenv("SQS_MAX_RETRIES", "3"))
    sqs_signal_types = frozenset(
        t.strip().upper() for t in os.getenv("SQS_SIGNAL_TYPES", "OB_CROSSOVER").split(",") if t.strip()
    )

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

    if debug and not webhook_url:
        log("⚠️ DEBUG mode without WEBHOOK_URL: signals will only be logged to the console")
    if sqs_queue_url:
        log(f"📤 scanner-events SQS: live {sorted(sqs_signal_types)} -> {sqs_queue_url}")
    else:
        log("ℹ️ SQS_SCANNER_EVENTS_QUEUE_URL not set: nothing is published to SQS")
    sink = SignalSink(
        node.get_event_loop(),
        ttl_hours=ttl_hours,
        webhook_url=webhook_url,
        webhook_timeout=webhook_timeout,
        webhook_max_retries=webhook_max_retries,
        db_enabled=not debug,
        sqs_queue_url=sqs_queue_url,
        sqs_region=aws_region,
        sqs_max_retries=sqs_max_retries,
        sqs_signal_types=sqs_signal_types,
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
            historical_bars=historical_bars,
            supertrend_period=supertrend_period,
            supertrend_factor=supertrend_factor,
            supertrend_filter=supertrend_filter,
            zvol_length=zvol_length,
            zvol_max_points=zvol_max_points,
            zvol_anchor=zvol_anchor,
            zvol_cvd_mode=zvol_cvd_mode,
            volume_filter=volume_filter,
            volume_filter_types=volume_filter_types,
            volume_lookback_bars=volume_lookback_bars,
        ),
        sink=sink,
    )
    reconciler = None if debug else PairReconciler(
        strategy,
        pair_types=pair_types,
        poll_seconds=poll_seconds,
        remove_after_misses=remove_after,
        add_stagger_seconds=add_stagger,
    )

    def make_background_tasks(loop: asyncio.AbstractEventLoop) -> list:
        if debug:
            return [loop.create_task(debug_watch_loop(strategy, DEBUG_TICKERS, add_stagger), name="debug-watch")]
        return [
            loop.create_task(sink.run(), name="signal-sink"),
            loop.create_task(reconciler.run(), name="pair-reconciler"),
            loop.create_task(purge_loop(ttl_hours, purge_interval), name="signal-purge"),
        ]

    run_data_node(node, strategy, register_binance_data_client_factory, make_background_tasks, sink)


if __name__ == "__main__":
    main()

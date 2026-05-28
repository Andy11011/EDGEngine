"""Live Donchian Channel regime detector for Binance using NautilusTrader.

This module loads Binance API credentials from AWS Secrets Manager
and runs a live strategy that monitors the Donchian Channel regime
(bullish/bearish) for a given symbol without placing any trades.
"""

from __future__ import annotations

import os
import sys
import json
import redis
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import List, Optional

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

# -----------------------------------------------------------------------------
# AWS Secrets Manager credential loader
# -----------------------------------------------------------------------------
try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:
    print("❌ boto3 not installed. Run: pip install boto3", file=sys.stderr)
    sys.exit(1)


def load_credentials_from_aws(
    region: str = "ap-southeast-1",
    sandbox: bool = False,
) -> tuple[str, str]:
    """Load Binance API key and secret from AWS Secrets Manager."""
    key_secret_name = "binance-sandbox-api-key" if sandbox else "binance-api-key"
    secret_secret_name = "binance-sandbox-api-secret" if sandbox else "binance-api-secret"

    if sandbox:
        print("🏖️ Using sandbox credentials from AWS...", file=sys.stderr)

    session = boto3.session.Session()
    client = session.client("secretsmanager", region_name=region)

    def get_secret(name: str) -> str:
        try:
            response = client.get_secret_value(SecretId=name)
            if "SecretString" not in response:
                raise ValueError(f"Secret {name} has no string value")
            return response["SecretString"]
        except (BotoCoreError, ClientError) as e:
            raise RuntimeError(f"Failed to fetch AWS secret {name}: {e}")

    api_key = get_secret(key_secret_name)
    api_secret = get_secret(secret_secret_name)

    if not api_key or not api_secret:
        raise RuntimeError("AWS secrets returned empty values")
    return api_key, api_secret


# -----------------------------------------------------------------------------
# Donchian Channel Indicator (pure Python, no external TA libs)
# -----------------------------------------------------------------------------
class DonchianChannel:
    """Donchian Channel indicator with signal calculation (matching donchian.js)."""

    def __init__(
        self,
        donchian_period: int = 20,
        ma_period: int = 50,
        ma_type: str = "EMA",  # "EMA" or "SMA"
        use_close: bool = True,
    ):
        self.donchian_period = donchian_period
        self.ma_period = ma_period
        self.ma_type = ma_type.upper()
        self.use_close = use_close

        # Rolling buffers
        self.highs: List[float] = []
        self.lows: List[float] = []
        self.closes: List[float] = []

        # Indicator values (latest)
        self.upper: Optional[float] = None
        self.lower: Optional[float] = None
        self.middle: Optional[float] = None
        self.donchian_ma: Optional[float] = None
        self.signal: Optional[bool] = None
        self.crossover: int = 0  # 1 = up, -1 = down, 0 = none

        # For EMA calculation
        self._ema_prev: Optional[float] = None
        self._ema_alpha: float = 2.0 / (ma_period + 1)

        # For SMA calculation
        self._sma_buffer: List[float] = []

    def _calc_sma(self, values: List[float]) -> Optional[float]:
        """Calculate simple moving average for the last `ma_period` values."""
        if len(values) < self.ma_period:
            return None
        return sum(values[-self.ma_period :]) / self.ma_period

    def _calc_ema(self, value: float) -> Optional[float]:
        """Update exponential moving average recursively."""
        if self._ema_prev is None:
            self._ema_prev = value
            return value
        self._ema_prev = (value - self._ema_prev) * self._ema_alpha + self._ema_prev
        return self._ema_prev

    def _calc_donchian_ma(self, middle_values: List[float]) -> Optional[float]:
        """Calculate MA of the Donchian middle line."""
        if len(middle_values) < self.ma_period:
            return None
        if self.ma_type == "SMA":
            return sum(middle_values[-self.ma_period :]) / self.ma_period
        else:  # EMA
            # For simplicity, recompute EMA from scratch each time using all available middle values
            # (EMA is stateful; we reuse the stored EMA from previous call)
            latest = middle_values[-1]
            if self._ema_prev is None:
                self._ema_prev = latest
                return latest
            alpha = 2.0 / (self.ma_period + 1)
            self._ema_prev = (latest - self._ema_prev) * alpha + self._ema_prev
            return self._ema_prev

    def update(self, high: float, low: float, close: float):
        """Update the indicator with a new bar."""
        self.highs.append(high)
        self.lows.append(low)
        self.closes.append(close)

        # Keep only needed history
        max_period = max(self.donchian_period, self.ma_period) + 5
        if len(self.highs) > max_period:
            self.highs.pop(0)
            self.lows.pop(0)
            self.closes.pop(0)

        # Compute Donchian bands
        if len(self.highs) >= self.donchian_period:
            self.upper = max(self.highs[-self.donchian_period :])
            self.lower = min(self.lows[-self.donchian_period :])
            self.middle = (self.upper + self.lower) / 2
        else:
            self.upper = self.lower = self.middle = None

        # Compute MA of middle line (need at least ma_period middle values)
        # We'll collect middle values over time
        if not hasattr(self, "_middle_buffer"):
            self._middle_buffer = []
        if self.middle is not None:
            self._middle_buffer.append(self.middle)
            if len(self._middle_buffer) > self.ma_period:
                self._middle_buffer.pop(0)

        if len(self._middle_buffer) >= self.ma_period:
            if self.ma_type == "SMA":
                self.donchian_ma = sum(self._middle_buffer[-self.ma_period :]) / self.ma_period
            else:  # EMA
                # Recompute EMA from scratch using the whole buffer (simple for online)
                ema_val = None
                alpha = 2.0 / (self.ma_period + 1)
                for val in self._middle_buffer:
                    if ema_val is None:
                        ema_val = val
                    else:
                        ema_val = (val - ema_val) * alpha + ema_val
                self.donchian_ma = ema_val
        else:
            self.donchian_ma = None

        # Compute signal
        if self.donchian_ma is None:
            self.signal = None
            self.crossover = 0
            return

        if self.use_close:
            # Original: close > MA
            new_signal = close > self.donchian_ma
        else:
            # High breakout above previous upper band
            if len(self.highs) < 2 or self.upper is None:
                new_signal = False
            else:
                # Upper band of previous bar (we don't store previous, just use current? In JS they use upper[i-1])
                # For simplicity, we'll use the previous bar's upper band by keeping a history.
                if not hasattr(self, "_prev_upper"):
                    self._prev_upper = self.upper
                new_signal = high > self._prev_upper
                self._prev_upper = self.upper

        # Crossover detection
        if self.signal is not None and new_signal != self.signal:
            self.crossover = 1 if new_signal else -1
        else:
            self.crossover = 0
        self.signal = new_signal


# -----------------------------------------------------------------------------
# Donchian Regime Strategy (read‑only, logs regime changes)
# -----------------------------------------------------------------------------
class DonchianRegimeConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    donchian_period: int = 20
    ma_period: int = 50
    ma_type: str = "EMA"  # "EMA" or "SMA"
    use_close: bool = True


class DonchianRegimeStrategy(Strategy):
    def __init__(self, config: DonchianRegimeConfig):
        super().__init__(config)
        self.donchian = DonchianChannel(
            donchian_period=config.donchian_period,
            ma_period=config.ma_period,
            ma_type=config.ma_type,
            use_close=config.use_close,
        )
        self._last_regime: Optional[bool] = None
        self._warming_up: bool = True  # True until historical data is fully processed

    def on_start(self) -> None:
        # ── Redis connection ──────────────────────────────────────────────────
        redis_host = os.getenv("REDIS_HOST", "localhost")
        redis_port = int(os.getenv("REDIS_PORT", 6379))
        self.log.info(
            f"Connecting to Redis at {redis_host}:{redis_port} ...",
            color=LogColor.BLUE,
        )
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            decode_responses=True,
        )
        try:
            pong = self.redis_client.ping()
            if pong:
                self.log.info(
                    f"✅ Redis connection OK ({redis_host}:{redis_port})",
                    color=LogColor.GREEN,
                )
            else:
                self.log.warning(
                    f"⚠️  Redis ping returned unexpected value: {pong}",
                    color=LogColor.YELLOW,
                )
        except Exception as e:
            self.log.error(
                f"❌ Redis connection FAILED ({redis_host}:{redis_port}): {e}"
            )

        # ── Historical bar warmup ─────────────────────────────────────────────
        # request_bars() is async/callback-based in NautilusTrader — it does NOT
        # return bars. Results arrive in on_historical_data(). We calculate a
        # start datetime far enough back to cover warmup_bars weekly candles.
        warmup_bars = max(self.config.donchian_period, self.config.ma_period) + 5
        # Add 20% buffer to account for weekends / missing candles
        lookback_weeks = int(warmup_bars * 1.2) + 1
        start_dt = datetime.now(timezone.utc) - timedelta(weeks=lookback_weeks)
        self.log.info(
            f"Requesting ~{warmup_bars} historical bars (start={start_dt.date()}) "
            f"to warm up Donchian indicator ...",
            color=LogColor.BLUE,
        )
        self.request_bars(self.config.bar_type, start=start_dt)

        # ── Subscribe to live bars ────────────────────────────────────────────
        self.subscribe_bars(self.config.bar_type)

        self.log.info(
            f"Donchian Regime Strategy started for {self.config.instrument_id} | "
            f"Donchian={self.config.donchian_period}, MA({self.config.ma_type})={self.config.ma_period}, "
            f"use_close={self.config.use_close}",
            color=LogColor.GREEN,
        )

    def _write_regime_to_redis(self, bar: Bar, regime: bool) -> None:
        payload = {
            "symbol": str(self.config.instrument_id),
            "regime": "BULLISH" if regime else "BEARISH",
            "upper": self.donchian.upper,
            "lower": self.donchian.lower,
            "ma": self.donchian.donchian_ma,
            "close": float(bar.close),
            "timestamp": bar.ts_event,
        }
        try:
            self.redis_client.xadd(
                f"regime:{self.config.instrument_id.symbol}",
                {"data": json.dumps(payload)},
                maxlen=1000,
            )
        except Exception as e:
            self.log.error(f"Redis write failed: {e}")

    def on_historical_data(self, data) -> None:
        """Called by NautilusTrader for each bar returned by request_bars()."""
        self.log.info(
            f"on_historical_data called | type={type(data).__name__} | data={str(data)[:120]}",
            color=LogColor.BLUE,
        )
        if not isinstance(data, Bar):
            self.log.warning(f"on_historical_data: unexpected type {type(data).__name__}, skipping")
            return

        self.donchian.update(
            high=float(data.high),
            low=float(data.low),
            close=float(data.close),
        )

        # First time signal becomes valid — write startup regime and mark warmup done
        if self.donchian.signal is not None and self._last_regime is None:
            self._last_regime = self.donchian.signal
            self._warming_up = False
            self.log.info(
                f"Warmup complete | Initial regime: {'BULLISH' if self.donchian.signal else 'BEARISH'} | "
                f"Upper={self.donchian.upper:.2f} Lower={self.donchian.lower:.2f} "
                f"MA={self.donchian.donchian_ma:.2f} Close={data.close:.2f}",
                color=LogColor.YELLOW,
            )
            self._write_regime_to_redis(data, self.donchian.signal)

    def on_bar(self, bar: Bar) -> None:
        self.log.info(
            f"on_bar called | warming_up={self._warming_up} | bar={str(bar)[:120]}",
            color=LogColor.BLUE,
        )
        # Skip bars that arrive while historical data is still being processed
        if self._warming_up:
            return

        self.donchian.update(high=float(bar.high), low=float(bar.low), close=float(bar.close))

        if self.donchian.signal is None:
            return

        regime = self.donchian.signal

        # First live bar after warmup — only write if regime differs or warmup never fired
        if self._last_regime is None:
            self._last_regime = regime
            self._warming_up = False
            self.log.info(
                f"Initial regime (from live bar): {'BULLISH' if regime else 'BEARISH'} | "
                f"Upper={self.donchian.upper:.2f} Lower={self.donchian.lower:.2f} "
                f"MA={self.donchian.donchian_ma:.2f} Close={bar.close:.2f}",
                color=LogColor.YELLOW,
            )
            self._write_regime_to_redis(bar, regime)
            return

        # Regime change on a live bar
        if regime != self._last_regime:
            self._last_regime = regime
            self.log.warning(
                f"🚨 REGIME CHANGE: {'BULLISH' if regime else 'BEARISH'} 🚨",
                color=LogColor.GREEN if regime else LogColor.RED,
            )
            self.log.info(
                f"Upper={self.donchian.upper:.2f} Lower={self.donchian.lower:.2f} "
                f"MA={self.donchian.donchian_ma:.2f} Close={bar.close:.2f}",
                color=LogColor.CYAN,
            )
            self._write_regime_to_redis(bar, regime)

    def on_stop(self) -> None:
        self.log.info("Donchian Regime Strategy stopped", color=LogColor.YELLOW)


# -----------------------------------------------------------------------------
# Node construction (data‑only, no execution)
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
    """Build a TradingNode with data client only (no execution)."""
    config = TradingNodeConfig(
        trader_id=trader_id,
        logging=LoggingConfig(log_level=log_level, use_pyo3=True),
        data_engine=LiveDataEngineConfig(validate_data_sequence=True),
        exec_engine=LiveExecEngineConfig(  # Still needed but reconciliation disabled
            reconciliation=False,
            generate_missing_orders=False,
            snapshot_orders=False,
            snapshot_positions=False,
        ),
        data_clients=data_clients,
        exec_clients={},  # No execution clients
        timeout_connection=30.0,
        timeout_reconciliation=0.0,
        timeout_portfolio=10.0,
        timeout_disconnection=10.0,
        timeout_post_stop=0.0,
    )
    return TradingNode(config=config)


def run_data_node(
    node: TradingNode,
    strategy: Strategy,
    register_data_client_factories: callable,
) -> None:
    """Run the node with the given strategy."""
    node.trader.add_strategy(strategy)
    register_data_client_factories(node)
    node.build()
    try:
        node.run()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            node.stop()
        finally:
            node.dispose()


def register_binance_data_client_factory(node: TradingNode) -> None:
    """Register only the data client factory for Binance."""
    node.add_data_client_factory(BINANCE, BinanceLiveDataClientFactory)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    # Environment variables
    symbol = os.getenv("BINANCE_SYMBOL", "BTCUSDT")
    trader_id = os.getenv("TRADER_ID", "EDGENGINE-001")
    environment = os.getenv("BINANCE_ENV", "LIVE").upper()
    account_type_name = os.getenv("BINANCE_ACCOUNT_TYPE", "SPOT").upper()
    bar_interval = os.getenv("BINANCE_BAR_INTERVAL", "1-WEEK")
    donchian_period = int(os.getenv("DONCHIAN_PERIOD", "20"))
    ma_period = int(os.getenv("MA_PERIOD", "50"))
    ma_type = os.getenv("MA_TYPE", "EMA").upper()
    use_close = os.getenv("USE_CLOSE", "1") == "1"
    log_level = os.getenv("LOG_LEVEL", "INFO")
    sandbox = os.getenv("BINANCE_SANDBOX", "0") == "1"
    aws_region = os.getenv("AWS_REGION", "ap-southeast-1")

    # Load credentials from AWS Secrets Manager
    try:
        api_key, api_secret = load_credentials_from_aws(region=aws_region, sandbox=sandbox)
        print("✅ Credentials loaded from AWS Secrets Manager", file=sys.stderr)
    except Exception as e:
        print(f"❌ Failed to load credentials from AWS: {e}", file=sys.stderr)
        sys.exit(1)

    # Force spot account type (no futures)
    account_type = BinanceAccountType.SPOT

    instrument_id = InstrumentId.from_str(f"{symbol}.{BINANCE}")
    bar_type = BarType.from_str(f"{instrument_id}-{bar_interval}-LAST-EXTERNAL")

    # Binance client configuration
    binance_config_kwargs = _resolve_binance_config_kwargs(environment)
    binance_client_config = BinanceDataClientConfig(
        api_key=api_key,
        api_secret=api_secret,
        account_type=account_type,
        instrument_provider=InstrumentProviderConfig(load_ids=frozenset([instrument_id])),
        **binance_config_kwargs,
    )

    # Build data‑only node
    node = build_data_only_node(
        trader_id=trader_id,
        data_clients={BINANCE: binance_client_config},
        log_level=log_level,
    )

    # Create strategy
    strategy = DonchianRegimeStrategy(
        DonchianRegimeConfig(
            instrument_id=instrument_id,
            bar_type=bar_type,
            donchian_period=donchian_period,
            ma_period=ma_period,
            ma_type=ma_type,
            use_close=use_close,
        )
    )

    run_data_node(node, strategy, register_binance_data_client_factory)


if __name__ == "__main__":
    main()
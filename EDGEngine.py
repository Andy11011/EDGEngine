"""Live RSI crossover signal detector for Binance using NautilusTrader.

This module loads Binance API credentials from AWS Secrets Manager
and runs a live strategy that monitors RSI and writes to Redis when
overbought (RSI > OB) or oversold (RSI < OS) crossovers occur.
Historical bars (configurable lookback, e.g., 3000 15min candles) are also
processed for crossover detection.
"""

from __future__ import annotations

import os
import sys
import json
import re
import redis
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
from nautilus_trader.indicators import RelativeStrengthIndex

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
    else:
        raise ValueError(f"Unknown interval unit: {unit}")


# -----------------------------------------------------------------------------
# RSI Crossover Signal Strategy
# -----------------------------------------------------------------------------
class RSISignalConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    rsi_period: int = 14
    overbought_threshold: float = 70.0
    oversold_threshold: float = 30.0
    historical_bars: int = 3000  # number of past bars to fetch for historical signals


class RSISignalStrategy(Strategy):
    def __init__(self, config: RSISignalConfig):
        super().__init__(config)
        self.rsi = RelativeStrengthIndex(period=config.rsi_period)
        self._prev_rsi: Optional[float] = None
        self._warming_up: bool = True

    def on_start(self) -> None:
        # Redis connection
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
                self.log.warning(f"⚠️ Redis ping unexpected: {pong}", color=LogColor.YELLOW)
        except Exception as e:
            self.log.error(f"❌ Redis connection FAILED ({redis_host}:{redis_port}): {e}")

        # ---- HISTORICAL BACKFILL ----
        # Calculate start date based on desired number of bars and bar interval
        bar_interval_str = str(self.config.bar_type).split("-")[1] + "-" + str(self.config.bar_type).split("-")[2]
        # e.g., "BTCUSDT.BINANCE-15-MINUTE-LAST-EXTERNAL" -> extract "15-MINUTE"
        # Simpler: use the bar_type's string representation and parse
        # Actually the BarType has .resolution property, but we can parse from env again
        # Re-parse from config's bar_type string
        type_str = str(self.config.bar_type)
        # Expected format: SYMBOL-INTERVAL-LAST-EXTERNAL
        parts = type_str.split("-")
        if len(parts) >= 3:
            interval_str = f"{parts[1]}-{parts[2]}"  # e.g., "15-MINUTE"
        else:
            interval_str = "15-MINUTE"  # fallback
        minutes_per_bar = _parse_interval_minutes(interval_str)

        lookback_bars = self.config.historical_bars
        # Add a small buffer (10 bars) to ensure we have enough for warmup
        total_bars_needed = lookback_bars + self.config.rsi_period
        days_needed = (total_bars_needed * minutes_per_bar) / (24 * 60)
        start_dt = datetime.now(timezone.utc) - timedelta(days=days_needed + 1)

        self.log.info(
            f"Requesting ~{lookback_bars} historical bars (since {start_dt.date()}) to compute historical signals...",
            color=LogColor.BLUE,
        )
        self.request_bars(self.config.bar_type, start=start_dt)

        # Subscribe to live bars
        self.subscribe_bars(self.config.bar_type)

        self.log.info(
            f"RSI Signal Strategy started for {self.config.instrument_id} | "
            f"RSI period={self.config.rsi_period}, OB={self.config.overbought_threshold}, OS={self.config.oversold_threshold}",
            color=LogColor.GREEN,
        )

    def _write_signal_to_redis(self, bar: Bar, signal_type: str, rsi_value: float) -> None:
        """Write crossover signal to Redis stream 'signals:{symbol}'."""
        payload = {
            "symbol": str(self.config.instrument_id),
            "type": signal_type,          # "OB_CROSS" or "OS_CROSS"
            "rsi": rsi_value,
            "close": float(bar.close),
            "timestamp": bar.ts_event,
        }
        try:
            self.redis_client.xadd(
                f"signals:{self.config.instrument_id.symbol}",
                {"data": json.dumps(payload)},
                maxlen=1000,
            )
        except Exception as e:
            self.log.error(f"Redis write failed: {e}")

    def _check_crossovers(self, bar: Bar, rsi_val: float) -> None:
        """Detect OB/OS crossovers and write signals."""
        if self._prev_rsi is None:
            return

        # Overbought crossover (crosses above OB threshold)
        if self._prev_rsi <= self.config.overbought_threshold and rsi_val > self.config.overbought_threshold:
            self.log.warning(
                f"🚨 OVERBOUGHT CROSS (RSI {self._prev_rsi:.1f} -> {rsi_val:.1f} > {self.config.overbought_threshold}) 🚨",
                color=LogColor.MAGENTA,
            )
            self._write_signal_to_redis(bar, "OB_CROSS", rsi_val)

        # Oversold crossover (crosses below OS threshold)
        if self._prev_rsi >= self.config.oversold_threshold and rsi_val < self.config.oversold_threshold:
            self.log.warning(
                f"🚨 OVERSOLD CROSS (RSI {self._prev_rsi:.1f} -> {rsi_val:.1f} < {self.config.oversold_threshold}) 🚨",
                color=LogColor.CYAN,
            )
            self._write_signal_to_redis(bar, "OS_CROSS", rsi_val)

    def on_historical_data(self, data) -> None:
        if not isinstance(data, Bar):
            self.log.warning(f"on_historical_data: unexpected type {type(data).__name__}, skipping")
            return

        close_float = float(data.close)
        self.rsi.update_raw(close_float)

        if self.rsi.initialized:
            rsi_val = self.rsi.value
            # Detect crossovers on historical bars (if we have a previous RSI)
            self._check_crossovers(data, rsi_val)

            # Store current RSI for next bar's crossover detection
            self._prev_rsi = rsi_val

            # Mark warmup as done after first RSI value
            if self._warming_up:
                self._warming_up = False
                self.log.info(f"Warmup complete. First RSI={rsi_val:.2f}", color=LogColor.YELLOW)

    def on_bar(self, bar: Bar) -> None:
        if self._warming_up:
            return

        close_float = float(bar.close)
        self.rsi.update_raw(close_float)
        if not self.rsi.initialized:
            return

        rsi_val = self.rsi.value
        # Detect crossovers using previous bar's RSI
        self._check_crossovers(bar, rsi_val)

        # Store current RSI for next bar's crossover detection
        self._prev_rsi = rsi_val

    def on_stop(self) -> None:
        self.log.info("RSI Signal Strategy stopped", color=LogColor.YELLOW)


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
        data_engine=LiveDataEngineConfig(validate_data_sequence=True),
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


def run_data_node(
    node: TradingNode,
    strategy: Strategy,
    register_data_client_factories: callable,
) -> None:
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
    node.add_data_client_factory(BINANCE, BinanceLiveDataClientFactory)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    symbol = os.getenv("BINANCE_SYMBOL", "BTCUSDT")
    trader_id = os.getenv("TRADER_ID", "EDGENGINE-001")
    environment = os.getenv("BINANCE_ENV", "LIVE").upper()
    bar_interval = os.getenv("BINANCE_BAR_INTERVAL", "15-MINUTE")
    rsi_period = int(os.getenv("RSI_PERIOD", "14"))
    overbought = float(os.getenv("RSI_OVERBOUGHT", "70.0"))
    oversold = float(os.getenv("RSI_OVERSOLD", "30.0"))
    log_level = os.getenv("LOG_LEVEL", "INFO")
    sandbox = os.getenv("BINANCE_SANDBOX", "0") == "1"
    aws_region = os.getenv("AWS_REGION", "ap-southeast-1")
    historical_bars = int(os.getenv("HISTORICAL_BARS", "3000"))

    try:
        api_key, api_secret = load_credentials_from_aws(region=aws_region, sandbox=sandbox)
        print("✅ Credentials loaded from AWS Secrets Manager", file=sys.stderr)
    except Exception as e:
        print(f"❌ Failed to load credentials from AWS: {e}", file=sys.stderr)
        sys.exit(1)

    account_type = BinanceAccountType.SPOT
    instrument_id = InstrumentId.from_str(f"{symbol}.{BINANCE}")
    bar_type = BarType.from_str(f"{instrument_id}-{bar_interval}-LAST-EXTERNAL")

    binance_config_kwargs = _resolve_binance_config_kwargs(environment)
    binance_client_config = BinanceDataClientConfig(
        api_key=api_key,
        api_secret=api_secret,
        account_type=account_type,
        instrument_provider=InstrumentProviderConfig(load_ids=frozenset([instrument_id])),
        **binance_config_kwargs,
    )

    node = build_data_only_node(
        trader_id=trader_id,
        data_clients={BINANCE: binance_client_config},
        log_level=log_level,
    )

    strategy = RSISignalStrategy(
        RSISignalConfig(
            instrument_id=instrument_id,
            bar_type=bar_type,
            rsi_period=rsi_period,
            overbought_threshold=overbought,
            oversold_threshold=oversold,
            historical_bars=historical_bars,
        )
    )

    run_data_node(node, strategy, register_binance_data_client_factory)


if __name__ == "__main__":
    main()
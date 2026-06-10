"""Live RSI regime detector for Binance using NautilusTrader.

This module loads Binance API credentials from AWS Secrets Manager
and runs a live strategy that monitors the RSI regime (bullish/bearish)
for a given symbol without placing any trades.
"""

from __future__ import annotations

import os
import sys
import json
import redis
from datetime import datetime, timedelta, timezone
from decimal import Decimal
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
from nautilus_trader.indicators import RelativeStrengthIndex   # <-- Rust-core RSI

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
# RSI Regime Strategy (read‑only, logs regime changes)
# -----------------------------------------------------------------------------
class RSIRegimeConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    rsi_period: int = 14
    bullish_threshold: float = 50.0   # RSI > this → BULLISH, else BEARISH


class RSIRegimeStrategy(Strategy):
    def __init__(self, config: RSIRegimeConfig):
        super().__init__(config)
        # Create Rust‑based RSI indicator
        self.rsi = RelativeStrengthIndex(period=config.rsi_period)
        self._last_regime: Optional[bool] = None
        self._warming_up: bool = True

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
        warmup_bars = self.config.rsi_period + 10   # enough to initialise RSI
        # Use 20% extra buffer
        lookback_weeks = int(warmup_bars * 1.2 / (7 * 24 * 4)) + 1   # crude, but fine
        # Better: calculate from bar duration; but 15min bars -> ~96 per day
        # We'll request a safe amount: 7 days of 15min bars = 672 bars
        lookback_days = 7
        start_dt = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        self.log.info(
            f"Requesting ~{warmup_bars} historical bars (start={start_dt.date()}) "
            f"to warm up RSI indicator ...",
            color=LogColor.BLUE,
        )
        self.request_bars(self.config.bar_type, start=start_dt)

        # ── Subscribe to live bars ────────────────────────────────────────────
        self.subscribe_bars(self.config.bar_type)

        self.log.info(
            f"RSI Regime Strategy started for {self.config.instrument_id} | "
            f"RSI period={self.config.rsi_period}, "
            f"bullish threshold={self.config.bullish_threshold}",
            color=LogColor.GREEN,
        )

    def _write_regime_to_redis(self, bar: Bar, regime: bool, rsi_value: float) -> None:
        payload = {
            "symbol": str(self.config.instrument_id),
            "regime": "BULLISH" if regime else "BEARISH",
            "rsi": rsi_value,
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
        """Called for each bar returned by request_bars()."""
        if not isinstance(data, Bar):
            self.log.warning(f"on_historical_data: unexpected type {type(data).__name__}, skipping")
            return

        close_float = float(data.close)   # Convert Price to float
        self.rsi.update_raw(close_float)

        if self.rsi.initialized and self._last_regime is None:
            rsi_val = self.rsi.value
            regime = rsi_val > self.config.bullish_threshold
            self._last_regime = regime
            self._warming_up = False
            self.log.info(
                f"Warmup complete | Initial regime: {'BULLISH' if regime else 'BEARISH'} | "
                f"RSI={rsi_val:.2f} Close={close_float:.2f}",
                color=LogColor.YELLOW,
            )
            # Pass the original bar (or use close_float in the payload)
            self._write_regime_to_redis(data, regime, rsi_val)

    def on_bar(self, bar: Bar) -> None:
        # Skip until warmup finished
        if self._warming_up:
            return

        close_float = float(bar.close)
        self.rsi.update_raw(close_float)
        if not self.rsi.initialized:
            return

        rsi_val = self.rsi.value
        regime = rsi_val > self.config.bullish_threshold

        if self._last_regime is None:
            self._last_regime = regime
            self._warming_up = False
            self.log.info(
                f"Initial regime (from live bar): {'BULLISH' if regime else 'BEARISH'} | "
                f"RSI={rsi_val:.2f} Close={close_float:.2f}",
                color=LogColor.YELLOW,
            )
            self._write_regime_to_redis(bar, regime, rsi_val)
            return

        # Regime change
        if regime != self._last_regime:
            self._last_regime = regime
            self.log.warning(
                f"🚨 REGIME CHANGE: {'BULLISH' if regime else 'BEARISH'} 🚨",
                color=LogColor.GREEN if regime else LogColor.RED,
            )
            self.log.info(
                f"RSI={rsi_val:.2f} Close={close_float:.2f}",
                color=LogColor.CYAN,
            )
            self._write_regime_to_redis(bar, regime, rsi_val)

    def on_stop(self) -> None:
        self.log.info("RSI Regime Strategy stopped", color=LogColor.YELLOW)


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
    """Build a TradingNode with data client only (no execution)."""
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
    bar_interval = os.getenv("BINANCE_BAR_INTERVAL", "15-MINUTE")
    rsi_period = int(os.getenv("RSI_PERIOD", "14"))
    bullish_threshold = float(os.getenv("BULLISH_THRESHOLD", "50.0"))  # RSI > 50 = bullish
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

    # Create strategy with RSI
    strategy = RSIRegimeStrategy(
        RSIRegimeConfig(
            instrument_id=instrument_id,
            bar_type=bar_type,
            rsi_period=rsi_period,
            bullish_threshold=bullish_threshold,
        )
    )

    run_data_node(node, strategy, register_binance_data_client_factory)


if __name__ == "__main__":
    main()
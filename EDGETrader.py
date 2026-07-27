"""EdgeTrader Blueprint - Barebones Trading Node for Execution.

This module connects to Binance via AWS Secrets Manager, subscribes to live bars,
and logs prices. It serves as the foundation for the trading/execution backend
without any indicator or scanning logic.
"""

from __future__ import annotations

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
    BinanceExecClientConfig,
    BinanceLiveDataClientFactory,
    BinanceLiveExecClientFactory,
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
# AWS Secrets Manager credential loader (copied from EDGEngine)
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
# Dummy Blueprint Strategy (No indicators, no scanning)
# -----------------------------------------------------------------------------
class BlueprintConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType


class BlueprintStrategy(Strategy):
    """
    A minimal strategy that subscribes to bars and logs them.
    This is the blank canvas for your future execution logic.
    """

    def __init__(self, config: BlueprintConfig):
        super().__init__(config)
        self._bar_count = 0

    def on_start(self) -> None:
        self.log.info(
            f"🚀 BlueprintStrategy started for {self.config.instrument_id}",
            color=LogColor.GREEN,
        )
        # Subscribe to live bars (the bare minimum to get data)
        self.subscribe_bars(self.config.bar_type)

    def on_bar(self, bar: Bar) -> None:
        self._bar_count += 1
        # Log every 10th bar so we know it's alive
        if self._bar_count % 10 == 0:
            self.log.info(
                f"📊 Bar #{self._bar_count}: {self.config.instrument_id} "
                f"Close = {bar.close:.2f} | Volume = {bar.volume:.2f}",
                color=LogColor.BLUE,
            )

    def on_stop(self) -> None:
        self.log.info("🛑 BlueprintStrategy stopped", color=LogColor.YELLOW)


# -----------------------------------------------------------------------------
# Node construction (includes EXECUTION client this time!)
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


def build_trading_node(
    *,
    trader_id: str,
    data_clients: dict,
    exec_clients: dict,
    log_level: str = "INFO",
) -> TradingNode:
    """Build a full trading node with both data and execution clients."""
    config = TradingNodeConfig(
        trader_id=trader_id,
        logging=LoggingConfig(log_level=log_level, use_pyo3=True),
        data_engine=LiveDataEngineConfig(
            validate_data_sequence=True,
            time_bars_timestamp_on_close=False,
        ),
        exec_engine=LiveExecEngineConfig(
            reconciliation=True,          # Now we care about order state
            generate_missing_orders=False,
            snapshot_orders=True,         # Keep state across restarts
            snapshot_positions=True,
        ),
        data_clients=data_clients,
        exec_clients=exec_clients,
        timeout_connection=30.0,
        timeout_reconciliation=0.0,
        timeout_portfolio=10.0,
        timeout_disconnection=10.0,
        timeout_post_stop=0.0,
    )
    return TradingNode(config=config)


def run_node(
    node: TradingNode,
    strategy: Strategy,
    register_data_factory: callable,
    register_exec_factory: callable,
) -> None:
    node.trader.add_strategy(strategy)
    register_data_factory(node)
    register_exec_factory(node)
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


def register_binance_data(node: TradingNode) -> None:
    node.add_data_client_factory(BINANCE, BinanceLiveDataClientFactory)

def register_binance_exec(node: TradingNode) -> None:
    node.add_exec_client_factory(BINANCE, BinanceLiveExecClientFactory)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    symbol = os.getenv("BINANCE_SYMBOL", "BTCUSDT")
    trader_id = os.getenv("TRADER_ID", "EDGETRADER-001")
    environment = os.getenv("BINANCE_ENV", "LIVE").upper()
    bar_interval = os.getenv("BINANCE_BAR_INTERVAL", "15-MINUTE")
    log_level = os.getenv("LOG_LEVEL", "INFO")
    sandbox = os.getenv("BINANCE_SANDBOX", "0") == "1"
    aws_region = os.getenv("AWS_REGION", "ap-southeast-1")

    # Load credentials
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

    # Shared instrument provider config
    instrument_provider_config = InstrumentProviderConfig(load_ids=frozenset([instrument_id]))

    # Data client config
    data_config = BinanceDataClientConfig(
        api_key=api_key,
        api_secret=api_secret,
        account_type=account_type,
        instrument_provider=instrument_provider_config,
        **binance_config_kwargs,
    )

    # Execution client config (crucial for trading)
    exec_config = BinanceExecClientConfig(
        api_key=api_key,
        api_secret=api_secret,
        account_type=account_type,
        instrument_provider=instrument_provider_config,
        **binance_config_kwargs,
    )

    # Build the full trading node
    node = build_trading_node(
        trader_id=trader_id,
        data_clients={BINANCE: data_config},
        exec_clients={BINANCE: exec_config},
        log_level=log_level,
    )

    # Instantiate the dummy blueprint strategy
    strategy = BlueprintStrategy(
        BlueprintConfig(
            instrument_id=instrument_id,
            bar_type=bar_type,
        )
    )

    # Run
    run_node(node, strategy, register_binance_data, register_binance_exec)


if __name__ == "__main__":
    main()
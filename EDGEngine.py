"""Live EMA cross trading bot for Binance using NautilusTrader.

This module loads Binance API credentials from AWS Secrets Manager
and runs a live strategy that trades BTCUSDT (or any specified symbol)
based on fast/slow EMA crossovers.
"""

from __future__ import annotations

import os
import sys
from decimal import Decimal

from nautilus_trader.adapters.binance import BINANCE, BinanceAccountType, BinanceDataClientConfig, BinanceLiveDataClientFactory, BinanceLiveExecClientFactory
from nautilus_trader.common.enums import LogColor
from nautilus_trader.config import InstrumentProviderConfig, LiveDataEngineConfig, LiveExecEngineConfig, LoggingConfig, StrategyConfig, TradingNodeConfig
from nautilus_trader.indicators import ExponentialMovingAverage
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import OrderSide, OrderType, PositionSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Quantity
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
# EMA Cross Strategy (places real orders)
# -----------------------------------------------------------------------------
class EMACrossConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    fast_ema_period: int = 10
    slow_ema_period: int = 20
    order_quantity: Decimal = Decimal("0.001")  # BTC quantity
    use_limit_orders: bool = False               # If False, market orders are used
    limit_offset_pct: Decimal = Decimal("0.001") # 0.1% offset for limit orders


class EMACrossStrategy(Strategy):
    def __init__(self, config: EMACrossConfig):
        super().__init__(config)
        self.fast_ema = ExponentialMovingAverage(config.fast_ema_period)
        self.slow_ema = ExponentialMovingAverage(config.slow_ema_period)
        self._last_bias_is_long: bool | None = None

    def on_start(self) -> None:
        """Called when the strategy is started."""
        self.register_indicator_for_bars(self.config.bar_type, self.fast_ema)
        self.register_indicator_for_bars(self.config.bar_type, self.slow_ema)
        self.subscribe_bars(self.config.bar_type)
        self.log.info(f"EMA Cross Strategy started for {self.config.instrument_id}", color=LogColor.GREEN)

    def on_bar(self, bar: Bar) -> None:
        """Called when a new bar is received."""
        if not self.indicators_initialized():
            return

        bias_is_long = self.fast_ema.value >= self.slow_ema.value
        if self._last_bias_is_long is None:
            self._last_bias_is_long = bias_is_long
            return
        if bias_is_long == self._last_bias_is_long:
            return

        # Crossover detected – trade
        self._last_bias_is_long = bias_is_long
        if bias_is_long:
            self.log.info(f"EMA CROSS: BUY signal for {self.config.instrument_id}", color=LogColor.GREEN)
            self._place_order(OrderSide.BUY)
        else:
            self.log.info(f"EMA CROSS: SELL signal for {self.config.instrument_id}", color=LogColor.RED)
            self._place_order(OrderSide.SELL)

    def _place_order(self, side: OrderSide) -> None:
        """Place an order (market or limit) based on configuration."""
        instrument = self.cache.instrument(self.config.instrument_id)
        if instrument is None:
            self.log.error(f"Instrument {self.config.instrument_id} not found in cache")
            return

        quantity = Quantity(self.config.order_quantity, instrument.precision)

        # Flatten any existing position first (optional – here we reverse position)
        position = self.cache.position(self.config.instrument_id)
        if position and position.side != PositionSide.FLAT:
            # If we have an opposite position, close it with a market order first.
            # For simplicity, we just submit the new order, which will increase/close
            # the position depending on side. To be more robust, you could implement
            # position flattening logic.
            self.log.info(f"Current position: {position.side} {position.quantity}. Submitting opposite order.")

        if self.config.use_limit_orders:
            # Create a limit order at current price + offset
            price = instrument.get_price_for_side(side, True)  # last price
            offset = price * self.config.limit_offset_pct
            if side == OrderSide.BUY:
                limit_price = price - offset
            else:
                limit_price = price + offset
            order = self.order_factory.limit(
                instrument_id=self.config.instrument_id,
                order_side=side,
                quantity=quantity,
                price=limit_price,
            )
            self.log.info(f"Submitting LIMIT order {order}")
        else:
            order = self.order_factory.market(
                instrument_id=self.config.instrument_id,
                order_side=side,
                quantity=quantity,
            )
            self.log.info(f"Submitting MARKET order {order}")

        self.submit_order(order)

    def on_order_filled(self, order) -> None:
        """Called when an order is fully or partially filled."""
        self.log.info(f"Order filled: {order}", color=LogColor.CYAN)

    def on_stop(self) -> None:
        """Clean up on strategy stop."""
        self.log.info("EMA Cross Strategy stopped", color=LogColor.YELLOW)


# -----------------------------------------------------------------------------
# Node construction (with execution enabled)
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


def build_live_trading_node(
    *,
    trader_id: str,
    data_clients: dict,
    exec_clients: dict,
    log_level: str = "INFO",
) -> TradingNode:
    """Build a TradingNode with both data and execution clients."""
    config = TradingNodeConfig(
        trader_id=trader_id,
        logging=LoggingConfig(log_level=log_level, use_pyo3=True),
        data_engine=LiveDataEngineConfig(validate_data_sequence=True),
        exec_engine=LiveExecEngineConfig(
            reconciliation=True,          # Enable reconciliation for live trading
            generate_missing_orders=True,
            snapshot_orders=True,
            snapshot_positions=True,
        ),
        data_clients=data_clients,
        exec_clients=exec_clients,
        timeout_connection=30.0,
        timeout_reconciliation=10.0,
        timeout_portfolio=10.0,
        timeout_disconnection=10.0,
        timeout_post_stop=0.0,
    )
    return TradingNode(config=config)


def run_trading_node(
    node: TradingNode,
    strategy: Strategy,
    register_client_factories: callable,
) -> None:
    """Run the trading node with the given strategy."""
    node.trader.add_strategy(strategy)
    register_client_factories(node)
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


def register_binance_client_factories(node: TradingNode) -> None:
    """Register both data and execution client factories for Binance."""
    node.add_data_client_factory(BINANCE, BinanceLiveDataClientFactory)
    node.add_exec_client_factory(BINANCE, BinanceLiveExecClientFactory)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    # Environment variables
    symbol = os.getenv("BINANCE_SYMBOL", "BTCUSDT")
    trader_id = os.getenv("TRADER_ID", "EDGENGINE-001")
    environment = os.getenv("BINANCE_ENV", "LIVE").upper()
    account_type_name = os.getenv("BINANCE_ACCOUNT_TYPE", "SPOT").upper()
    bar_interval = os.getenv("BINANCE_BAR_INTERVAL", "1-MINUTE")
    fast_ema = int(os.getenv("FAST_EMA", "10"))
    slow_ema = int(os.getenv("SLOW_EMA", "20"))
    order_quantity = Decimal(os.getenv("ORDER_QUANTITY", "0.001"))
    use_limit_orders = os.getenv("USE_LIMIT_ORDERS", "0") == "1"
    limit_offset_pct = Decimal(os.getenv("LIMIT_OFFSET_PCT", "0.001"))
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

    # Map account type string to BinanceAccountType enum
    account_type_map = {
        "SPOT": BinanceAccountType.SPOT,
        "FUTURE": BinanceAccountType.FUTURE,
        "MARGIN": BinanceAccountType.MARGIN,
        "FUNDING": BinanceAccountType.FUNDING,
    }
    account_type = account_type_map.get(account_type_name, BinanceAccountType.SPOT)

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

    # Build node with data and execution clients
    node = build_live_trading_node(
        trader_id=trader_id,
        data_clients={BINANCE: binance_client_config},
        exec_clients={BINANCE: binance_client_config},  # same config for exec
        log_level=log_level,
    )

    # Create strategy
    strategy = EMACrossStrategy(
        EMACrossConfig(
            instrument_id=instrument_id,
            bar_type=bar_type,
            fast_ema_period=fast_ema,
            slow_ema_period=slow_ema,
            order_quantity=order_quantity,
            use_limit_orders=use_limit_orders,
            limit_offset_pct=limit_offset_pct,
        )
    )

    run_trading_node(node, strategy, register_binance_client_factories)


if __name__ == "__main__":
    main()
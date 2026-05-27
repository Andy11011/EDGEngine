"""Signal-only live strategy starter for NautilusTrader.

This module consumes real-time market data and posts trading signals to a webhook.
Binance API credentials are loaded from AWS Secrets Manager (two secrets:
binance-api-key and binance-api-secret, or binance-sandbox-* when sandbox mode is enabled).
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field
from decimal import Decimal
from queue import Empty, Full, Queue
from threading import Event, Thread
from typing import Any, Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from nautilus_trader.adapters.binance import BINANCE, BinanceAccountType, BinanceDataClientConfig, BinanceLiveDataClientFactory
from nautilus_trader.common.enums import LogColor
from nautilus_trader.config import InstrumentProviderConfig, LiveDataEngineConfig, LiveExecEngineConfig, LoggingConfig, StrategyConfig, TradingNodeConfig
from nautilus_trader.indicators import ExponentialMovingAverage
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
# Webhook dispatcher and strategy (unchanged except for credential integration)
# -----------------------------------------------------------------------------

@dataclass
class WebhookDispatcher:
    url: str
    timeout_secs: float
    bearer_token: str | None = None
    max_queue_size: int = 1_000
    _queue: Queue = field(init=False, repr=False)
    _stop_event: Event = field(init=False, repr=False)
    _thread: Thread = field(init=False, repr=False)
    _started: bool = field(init=False, repr=False, default=False)

    def __post_init__(self) -> None:
        self._queue = Queue(maxsize=self.max_queue_size)
        self._stop_event = Event()
        self._thread = Thread(target=self._run, name="nautilus-webhook-dispatcher", daemon=True)

    def start(self) -> None:
        if self._started:
            return
        self._thread.start()
        self._started = True

    def enqueue(self, payload: Mapping[str, Any]) -> bool:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        try:
            self._queue.put_nowait(body)
            return True
        except Full:
            return False

    def close(self, drain_timeout_secs: float = 2.0) -> None:
        if not self._started:
            return
        self._stop_event.set()
        try:
            self._queue.put_nowait(None)
        except Full:
            try:
                self._queue.get_nowait()
            except Empty:
                pass
            self._queue.put_nowait(None)
        self._thread.join(timeout=drain_timeout_secs)

    def _run(self) -> None:
        while not self._stop_event.is_set():
            payload = self._queue.get()
            if payload is None:
                break
            self._post(payload)

    def _post(self, payload: bytes) -> None:
        headers = {"Content-Type": "application/json"}
        if self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"
        request = Request(self.url, data=payload, headers=headers, method="POST")
        try:
            with urlopen(request, timeout=self.timeout_secs):
                return
        except (HTTPError, URLError, TimeoutError):
            return


class WebhookEMACrossConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    webhook_url: str
    signal_size: Decimal = Decimal("1")
    fast_ema_period: int = 10
    slow_ema_period: int = 20
    webhook_timeout_secs: float = 5.0
    webhook_bearer_token: str | None = None
    emit_initial_signal: bool = False


class WebhookEMACrossStrategy(Strategy):
    def __init__(self, config: WebhookEMACrossConfig) -> None:
        super().__init__(config)
        self.fast_ema = ExponentialMovingAverage(config.fast_ema_period)
        self.slow_ema = ExponentialMovingAverage(config.slow_ema_period)
        self.instrument = None
        self._last_bias_is_long: bool | None = None
        self._dispatcher = WebhookDispatcher(
            url=config.webhook_url,
            timeout_secs=config.webhook_timeout_secs,
            bearer_token=config.webhook_bearer_token,
        )

    def on_start(self) -> None:
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            self.log.error(f"Could not find instrument for {self.config.instrument_id}", color=LogColor.RED)
            self.stop()
            return

        self._dispatcher.start()
        self.register_indicator_for_bars(self.config.bar_type, self.fast_ema)
        self.register_indicator_for_bars(self.config.bar_type, self.slow_ema)
        self.subscribe_bars(self.config.bar_type)
        self.log.info(f"Subscribed to {self.config.bar_type} for webhook-only execution", color=LogColor.YELLOW)

    def on_bar(self, bar: Bar) -> None:
        if not self.indicators_initialized():
            return

        bias_is_long = self.fast_ema.value >= self.slow_ema.value
        if self._last_bias_is_long is None:
            self._last_bias_is_long = bias_is_long
            if not self.config.emit_initial_signal:
                return
        elif bias_is_long == self._last_bias_is_long:
            return

        side = "BUY" if bias_is_long else "SELL"
        payload = {
            "source": "nautilus",
            "strategy": type(self).__name__,
            "instrument_id": str(self.config.instrument_id),
            "bar_type": str(self.config.bar_type),
            "signal": side,
            "signal_size": str(self.config.signal_size),
            "bar_close": str(bar.close),
            "fast_ema": str(self.fast_ema.value),
            "slow_ema": str(self.slow_ema.value),
            "ts_event": bar.ts_event,
        }

        if self._dispatcher.enqueue(payload):
            self.log.info(f"Emitted {side} webhook for {self.config.instrument_id}",
                          color=LogColor.GREEN if side == "BUY" else LogColor.BLUE)
            self._last_bias_is_long = bias_is_long
        else:
            self.log.warning("Dropped webhook signal because the dispatch queue is full")

    def on_stop(self) -> None:
        self._dispatcher.close()
        self.log.info("Webhook strategy stopped", color=LogColor.YELLOW)


# -----------------------------------------------------------------------------
# Node construction helpers
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


def build_signal_only_node(
    *,
    trader_id: str,
    data_clients: Mapping[str, object],
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
        data_clients=dict(data_clients),
        timeout_connection=30.0,
        timeout_reconciliation=0.0,
        timeout_portfolio=10.0,
        timeout_disconnection=10.0,
        timeout_post_stop=0.0,
    )
    return TradingNode(config=config)


def run_signal_only_node(
    node: TradingNode,
    strategy: Strategy,
    register_data_client_factories: Callable[[TradingNode], None],
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


def build_binance_signal_only_node(
    *,
    trader_id: str,
    instrument_id: InstrumentId,
    environment: str = "LIVE",
    account_type: BinanceAccountType = BinanceAccountType.SPOT,
    api_key: str,
    api_secret: str,
    log_level: str = "INFO",
) -> TradingNode:
    binance_config_kwargs = _resolve_binance_config_kwargs(environment)
    return build_signal_only_node(
        trader_id=trader_id,
        log_level=log_level,
        data_clients={
            BINANCE: BinanceDataClientConfig(
                api_key=api_key,
                api_secret=api_secret,
                account_type=account_type,
                instrument_provider=InstrumentProviderConfig(
                    load_ids=frozenset([instrument_id]),
                ),
                **binance_config_kwargs,
            ),
        },
    )


def register_binance_data_client_factory(node: TradingNode) -> None:
    node.add_data_client_factory(BINANCE, BinanceLiveDataClientFactory)


def create_binance_webhook_strategy(
    *,
    symbol: str,
    webhook_url: str,
    api_key: str,
    api_secret: str,
    trader_id: str = "EDGENGINE-001",
    environment: str = "LIVE",
    account_type: BinanceAccountType = BinanceAccountType.SPOT,
    bar_interval: str = "1-MINUTE",
    fast_ema_period: int = 10,
    slow_ema_period: int = 20,
    signal_size: str = "1",
    webhook_bearer_token: str | None = None,
    emit_initial_signal: bool = False,
    log_level: str = "INFO",
) -> tuple[TradingNode, WebhookEMACrossStrategy]:
    instrument_id = InstrumentId.from_str(f"{symbol}.{BINANCE}")
    bar_type = BarType.from_str(f"{instrument_id}-{bar_interval}-LAST-EXTERNAL")
    node = build_binance_signal_only_node(
        trader_id=trader_id,
        instrument_id=instrument_id,
        environment=environment,
        account_type=account_type,
        api_key=api_key,
        api_secret=api_secret,
        log_level=log_level,
    )
    strategy = WebhookEMACrossStrategy(
        WebhookEMACrossConfig(
            instrument_id=instrument_id,
            bar_type=bar_type,
            webhook_url=webhook_url,
            signal_size=Decimal(signal_size),
            fast_ema_period=fast_ema_period,
            slow_ema_period=slow_ema_period,
            webhook_bearer_token=webhook_bearer_token,
            emit_initial_signal=emit_initial_signal,
        ),
    )
    return node, strategy


# -----------------------------------------------------------------------------
# Main entry point – loads credentials from AWS and runs the strategy
# -----------------------------------------------------------------------------

def main():
    # All configuration via environment variables
    webhook_url = os.getenv("WEBHOOK_URL")
    if not webhook_url:
        print("❌ WEBHOOK_URL environment variable is required", file=sys.stderr)
        sys.exit(1)

    symbol = os.getenv("BINANCE_SYMBOL", "BTCUSDT")
    trader_id = os.getenv("TRADER_ID", "EDGENGINE-001")
    environment = os.getenv("BINANCE_ENV", "LIVE").upper()
    account_type_name = os.getenv("BINANCE_ACCOUNT_TYPE", "SPOT").upper()
    bar_interval = os.getenv("BINANCE_BAR_INTERVAL", "1-MINUTE")
    fast_ema = int(os.getenv("FAST_EMA", "10"))
    slow_ema = int(os.getenv("SLOW_EMA", "20"))
    signal_size = os.getenv("SIGNAL_SIZE", "1")
    webhook_bearer_token = os.getenv("WEBHOOK_BEARER_TOKEN")
    emit_initial_signal = os.getenv("EMIT_INITIAL_SIGNAL", "0") == "1"
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

    node, strategy = create_binance_webhook_strategy(
        symbol=symbol,
        webhook_url=webhook_url,
        api_key=api_key,
        api_secret=api_secret,
        trader_id=trader_id,
        environment=environment,
        account_type=account_type,
        bar_interval=bar_interval,
        fast_ema_period=fast_ema,
        slow_ema_period=slow_ema,
        signal_size=signal_size,
        webhook_bearer_token=webhook_bearer_token,
        emit_initial_signal=emit_initial_signal,
        log_level=log_level,
    )

    run_signal_only_node(node, strategy, register_binance_data_client_factory)


if __name__ == "__main__":
    main()
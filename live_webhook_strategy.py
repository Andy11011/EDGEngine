"""Signal-only live strategy starter for NautilusTrader.

This module is designed for a live node which consumes real-time market data but
does not submit orders through Nautilus. Instead, trading signals are posted to
an outbound webhook endpoint so a downstream service can decide what to do with
them.

The venue adapter is intentionally left open. To run this file, supply concrete
`data_clients` in `build_signal_only_node(...)` and register the matching client
factories before calling `run_signal_only_node(...)`.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from decimal import Decimal
from queue import Empty
from queue import Full
from queue import Queue
from threading import Event
from threading import Thread
from typing import Any
from typing import Callable
from typing import Mapping
from urllib.error import HTTPError
from urllib.error import URLError
from urllib.request import Request
from urllib.request import urlopen

from nautilus_trader.adapters.binance import BINANCE
from nautilus_trader.adapters.binance import BinanceAccountType
from nautilus_trader.adapters.binance import BinanceDataClientConfig
from nautilus_trader.adapters.binance import BinanceLiveDataClientFactory
from nautilus_trader.common.enums import LogColor
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.config import LiveDataEngineConfig
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import StrategyConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.indicators import ExponentialMovingAverage
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

# Older Nautilus installs require a non-empty api_key even for public data.
# Bar/exchange-info endpoints are unauthenticated; this placeholder is never
# sent to Binance but satisfies the credential-check path in those installs.
_PUBLIC_DATA_KEY_PLACEHOLDER = "PUBLIC_DATA_ONLY"

try:
    from nautilus_trader.adapters.binance.common.enums import BinanceEnvironment
except ImportError:
    try:
        from nautilus_trader.adapters.binance import BinanceEnvironment
    except ImportError:
        BinanceEnvironment = None


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
        self._thread = Thread(
            target=self._run,
            name="nautilus-webhook-dispatcher",
            daemon=True,
        )

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
            self.log.error(
                f"Could not find instrument for {self.config.instrument_id}",
                color=LogColor.RED,
            )
            self.stop()
            return

        self._dispatcher.start()
        self.register_indicator_for_bars(self.config.bar_type, self.fast_ema)
        self.register_indicator_for_bars(self.config.bar_type, self.slow_ema)
        self.subscribe_bars(self.config.bar_type)
        self.log.info(
            f"Subscribed to {self.config.bar_type} for webhook-only execution",
            color=LogColor.YELLOW,
        )

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
            self.log.info(
                f"Emitted {side} webhook for {self.config.instrument_id}",
                color=LogColor.GREEN if side == "BUY" else LogColor.BLUE,
            )
            self._last_bias_is_long = bias_is_long
        else:
            self.log.warning("Dropped webhook signal because the dispatch queue is full")

    def on_stop(self) -> None:
        self._dispatcher.close()
        self.log.info("Webhook strategy stopped", color=LogColor.YELLOW)


def _resolve_binance_config_kwargs(environment_name: str) -> dict[str, object]:
    normalized = environment_name.upper()

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

    raise ValueError(
        "Unsupported Binance environment for this Nautilus version: "
        f"{environment_name}. Use LIVE/MAINNET or TESTNET.",
    )


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
    api_key: str | None = None,
    api_secret: str | None = None,
    log_level: str = "INFO",
) -> TradingNode:
    binance_config_kwargs = _resolve_binance_config_kwargs(environment)
    return build_signal_only_node(
        trader_id=trader_id,
        log_level=log_level,
        data_clients={
            BINANCE: BinanceDataClientConfig(
                api_key=api_key or _PUBLIC_DATA_KEY_PLACEHOLDER,
                api_secret=api_secret or _PUBLIC_DATA_KEY_PLACEHOLDER,
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
    trader_id: str = "EDGENGINE-001",
    environment: str = "LIVE",
    account_type: BinanceAccountType = BinanceAccountType.SPOT,
    api_key: str | None = None,
    api_secret: str | None = None,
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


def example_usage() -> None:
    # Hardcoded webhook URL – replace with your actual endpoint
    webhook_url = "http://localhost:9999"   # or "http://edgengine:8443/webhook"

    symbol = os.getenv("BINANCE_SYMBOL", "BTCUSDT")
    environment_name = os.getenv("BINANCE_ENV", "LIVE").upper()
    account_type_name = os.getenv("BINANCE_ACCOUNT_TYPE", "SPOT").upper()

    # Real API keys improve rate limits but are not required for public data.
    binance_api_key = os.getenv("BINANCE_API_KEY") or None
    binance_api_secret = os.getenv("BINANCE_API_SECRET") or None

    node, strategy = create_binance_webhook_strategy(
        symbol=symbol,
        webhook_url=webhook_url,
        trader_id=os.getenv("TRADER_ID", "EDGENGINE-001"),
        environment=environment_name,
        account_type=BinanceAccountType[account_type_name],
        api_key=binance_api_key,
        api_secret=binance_api_secret,
        bar_interval=os.getenv("BINANCE_BAR_INTERVAL", "1-MINUTE"),
        fast_ema_period=int(os.getenv("FAST_EMA", "10")),
        slow_ema_period=int(os.getenv("SLOW_EMA", "20")),
        signal_size=os.getenv("SIGNAL_SIZE", "1"),
        webhook_bearer_token=os.getenv("WEBHOOK_BEARER_TOKEN"),
        emit_initial_signal=os.getenv("EMIT_INITIAL_SIGNAL", "0") == "1",
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )
    run_signal_only_node(node, strategy, register_binance_data_client_factory)


if __name__ == "__main__":
    example_usage()
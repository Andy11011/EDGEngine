"""
binance_real_node.py — headless real-execution Binance MAINNET trading
node. One of the two processes split out of the old EDGETrader.py per
MultiVenueTOD.md.

Scope of this process:
  - Binance MAINNET data feed + a REAL Binance exec client (Ed25519 keys) —
    orders placed here are real orders against a real account.
  - NO HTTP listener. Liveness is exec-based (see the Dockerfile HEALTHCHECK
    calling into this process, e.g. a `python -c "..."` sentinel file check,
    or `nautilus`'s own health hook if you wire one) plus the heartbeat row
    this process writes to Postgres every 15s (common_tasks.heartbeat_loop).
  - Write-only against Postgres: trade_events, order_fills, claimed_events,
    node_heartbeats. It never answers a read request — that's edge-api.py.
  - Consumes ONE dedicated SQS queue, already filtered upstream to
    target=real.

Env vars:
  SQS_TRADE_EVENTS_QUEUE_URL_REAL   (required) — this process's own queue
  TRADER_ID                        default "EDGETRADER"
  BINANCE_BAR_INTERVAL              default "15-MINUTE"
  LOG_LEVEL                        default "INFO"
  AWS_REGION                        default "ap-southeast-1"
  ENABLE_TESTNET                    must be false/unset (see main())
"""

from __future__ import annotations

import asyncio
import os
import sys
from typing import Dict

from nautilus_trader.adapters.binance import (
    BINANCE,
    BinanceAccountType,
    BinanceDataClientConfig,
    BinanceExecClientConfig,
    BinanceLiveDataClientFactory,
    BinanceLiveExecClientFactory,
)
from nautilus_trader.adapters.binance.common.enums import BinanceEnvironment
from nautilus_trader.config import (
    InstrumentProviderConfig,
    LiveDataEngineConfig,
    LiveExecEngineConfig,
    LoggingConfig,
    TradingNodeConfig,
)
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Currency
from nautilus_trader.trading.config import ImportableControllerConfig

# New split modules
import auth
import sqs
from common_tasks import heartbeat_loop, cancel_requests_loop

from position_sizing import PositionSizingError, calculate_position_size
from trade_strategy import TradeStrategy, TradeStrategyConfig
from trades_db_async import TradeEventsDB

TARGET = "real"

active_strategies: Dict[str, TradeStrategy] = {}

# -----------------------------------------------------------------------------
# Nautilus helper functions (formerly in node_common.py, now local)
# -----------------------------------------------------------------------------

def build_trading_node(
    *,
    trader_id: str,
    data_clients: dict,
    exec_clients: dict,
    log_level: str = "INFO",
    loop: asyncio.AbstractEventLoop,
) -> TradingNode:
    """
    Build a full trading node with both data and execution clients.

    `loop` must be the SAME loop object later used to run the node (e.g.
    via `loop.run_until_complete(...)`).
    """
    config = TradingNodeConfig(
        trader_id=trader_id,
        controller=ImportableControllerConfig(
            controller_path="nautilus_trader.trading.controller:Controller",
            config_path="nautilus_trader.common.config:ActorConfig",
            config={},
        ),
        logging=LoggingConfig(log_level=log_level, use_pyo3=True),
        data_engine=LiveDataEngineConfig(
            validate_data_sequence=True,
            time_bars_timestamp_on_close=False,
        ),
        exec_engine=LiveExecEngineConfig(
            reconciliation=True,
            generate_missing_orders=False,
            snapshot_orders=True,
            snapshot_positions=True,
        ),
        data_clients=data_clients,
        exec_clients=exec_clients,
        timeout_connection=30.0,
        timeout_reconciliation=10.0,
        timeout_portfolio=10.0,
        timeout_disconnection=10.0,
        timeout_post_stop=5.0,
    )
    return TradingNode(config=config, loop=loop)


def register_binance_data(node: TradingNode) -> None:
    node.add_data_client_factory(BINANCE, BinanceLiveDataClientFactory)


def register_binance_exec(node: TradingNode) -> None:
    node.add_exec_client_factory(BINANCE, BinanceLiveExecClientFactory)


def new_instrument_provider() -> InstrumentProviderConfig:
    return InstrumentProviderConfig(load_all=True)


# -----------------------------------------------------------------------------
# Real balance helper
# -----------------------------------------------------------------------------

def get_real_usdt_balance(node: TradingNode) -> float:
    """
    Live free (not total) USDT balance from the connected Binance account —
    used both for position sizing and for the heartbeat's balance snapshot.
    """
    account = node.portfolio.account(BINANCE)
    if account is None:
        raise RuntimeError(
            "No account available yet for venue BINANCE — the exec client may "
            "not have received its first AccountState update yet."
        )
    usdt = Currency.from_str("USDT")
    balance = account.balance_free(usdt)
    if balance is None:
        raise RuntimeError("Account has no USDT balance entry yet.")
    return float(balance.as_double())


# -----------------------------------------------------------------------------
# SQS listener for this target
# -----------------------------------------------------------------------------

async def listen_trade_events(
    node: TradingNode,
    db: TradeEventsDB,
    sqs_client,
    queue_url: str,
    bar_interval: str,
) -> None:
    """Long-poll this process's dedicated (target=real) SQS queue."""
    poll_wait_seconds = int(os.getenv("SQS_POLL_WAIT_SECONDS", "20"))

    print("⏳ Waiting for the TradingNode to finish starting...", file=sys.stderr)
    while not node.trader.is_running:
        await asyncio.sleep(0.5)
    print("✅ TradingNode RUNNING; processing trade events (target=real)", file=sys.stderr)

    def make_close_callback(trade_id: str):
        def callback() -> None:
            strategy = active_strategies.pop(trade_id, None)
            if strategy is not None:
                try:
                    node.trader.remove_strategy(strategy.id)
                    print(f"🧹 Removed strategy for trade {trade_id}")
                except Exception as e:
                    print(f"⚠️ Error removing strategy for {trade_id}: {e}")
        return callback

    poll_count = 0
    while True:
        poll_count += 1
        print(f"📡 [poll #{poll_count}] Long-polling SQS (real)...", file=sys.stderr)
        messages = await sqs.receive_trade_events(
            sqs_client, queue_url,
            max_messages=int(os.getenv("SQS_MAX_MESSAGES", "10")),
            wait_seconds=poll_wait_seconds,
        )
        if not messages:
            continue

        for msg in messages:
            receipt_handle = msg["ReceiptHandle"]
            msg_id = msg.get("MessageId", "?")
            event_data = sqs.parse_event(msg)
            if event_data is None:
                print(f"⚠️ [msg {msg_id}] Failed to parse; deleting", file=sys.stderr)
                await sqs.delete_message(sqs_client, queue_url, receipt_handle)
                continue

            ticker = event_data.get("ticker")
            event_type = event_data.get("event_type")
            occurred_at = event_data.get("occurred_at")
            payload_target = event_data.get("target", TARGET)
            if isinstance(payload_target, list):
                payload_target = payload_target[0] if payload_target else TARGET
            if payload_target != TARGET:
                print(
                    f"⚠️ [msg {msg_id}] Received a target={payload_target} event on the "
                    f"real-node queue — check the SNS filter policy. Skipping without deleting.",
                    file=sys.stderr,
                )
                continue

            if not ticker or not event_type or not occurred_at:
                print(f"⚠️ [msg {msg_id}] Missing required fields; deleting", file=sys.stderr)
                await sqs.delete_message(sqs_client, queue_url, receipt_handle)
                continue

            claimed = await db.claim_event(ticker, event_type, occurred_at, target=TARGET)
            if not claimed:
                print(f"♻️ [msg {msg_id}] Already claimed; deleting (dup delivery)", file=sys.stderr)
                await sqs.delete_message(sqs_client, queue_url, receipt_handle)
                continue

            processed = False

            if event_type.lower() == "open":
                side = event_data.get("side")
                ep = event_data.get("ep")
                sl = event_data.get("sl")
                tp = event_data.get("tp")

                if not side or ep is None or sl is None:
                    print(f"⚠️ [msg {msg_id}] Missing open fields; unclaiming", file=sys.stderr)
                    await db.unclaim_event(ticker, event_type, occurred_at, target=TARGET)
                else:
                    try:
                        sizing_config = await db.get_trades_config()
                        equity = get_real_usdt_balance(node)
                        computed_size = calculate_position_size(
                            equity=equity,
                            risk_ratio=sizing_config["risk_ratio"],
                            entry_price=float(ep),
                            stop_loss_price=float(sl),
                        )
                    except (PositionSizingError, Exception) as e:
                        print(f"❌ [msg {msg_id}] Sizing failed: {e}", file=sys.stderr)
                        await db.unclaim_event(ticker, event_type, occurred_at, target=TARGET)
                        computed_size = None

                    if computed_size is not None:
                        trade_id = f"{ticker}_{occurred_at.replace(':', '').replace('.', '').replace('-', '').replace('Z', '')}_{TARGET}"
                        instrument_id = InstrumentId.from_str(ticker)
                        bar_type = BarType.from_str(f"{ticker}-{bar_interval}-LAST-EXTERNAL")

                        config = TradeStrategyConfig(
                            instrument_id=instrument_id,
                            bar_type=bar_type,
                            trade_id=trade_id,
                            side=side,
                            size=computed_size,
                            entry_price=float(ep),
                            sl_price=float(sl) if sl is not None else None,
                            tp_price=float(tp) if tp is not None else None,
                            strategy_id=trade_id,
                            target=TARGET,
                        )
                        strategy = TradeStrategy(config, close_callback=make_close_callback(trade_id))
                        active_strategies[trade_id] = strategy

                        try:
                            controller = node.kernel._controller
                            controller.create_strategy(strategy, start=True)
                            if strategy.is_running:
                                processed = True
                                print(f"🚀 [msg {msg_id}] Started strategy for trade {trade_id}", file=sys.stderr)
                            else:
                                raise RuntimeError("Strategy stopped immediately after start")
                        except Exception as e:
                            print(f"❌ [msg {msg_id}] Failed to start strategy: {e}", file=sys.stderr)
                            active_strategies.pop(trade_id, None)
                            await db.unclaim_event(ticker, event_type, occurred_at, target=TARGET)

            elif event_type.lower() == "cancel":
                active_trade_id = await db.get_active_trade_for_ticker(ticker, target=TARGET)
                if active_trade_id is None:
                    print(f"⚠️ [msg {msg_id}] No active trade; unclaiming", file=sys.stderr)
                    await db.unclaim_event(ticker, event_type, occurred_at, target=TARGET)
                else:
                    strategy = active_strategies.get(active_trade_id)
                    if strategy is None:
                        print(f"⚠️ [msg {msg_id}] Strategy {active_trade_id} not in memory; unclaiming", file=sys.stderr)
                        await db.unclaim_event(ticker, event_type, occurred_at, target=TARGET)
                    else:
                        strategy.request_cancel()
                        processed = True
                        print(f"🛑 [msg {msg_id}] Cancel requested for {active_trade_id}", file=sys.stderr)
            else:
                print(f"⚠️ [msg {msg_id}] Unknown event_type '{event_type}'; unclaiming", file=sys.stderr)
                await db.unclaim_event(ticker, event_type, occurred_at, target=TARGET)

            if processed:
                await sqs.delete_message(sqs_client, queue_url, receipt_handle)
                print(f"🗑️ [msg {msg_id}] Acked", file=sys.stderr)
            else:
                print(f"⚠️ [msg {msg_id}] Not processed; message retained for retry", file=sys.stderr)


# -----------------------------------------------------------------------------
# Main entry point
# -----------------------------------------------------------------------------

def main() -> None:
    trader_id = os.getenv("TRADER_ID", "EDGETRADER") + "-REAL"
    bar_interval = os.getenv("BINANCE_BAR_INTERVAL", "15-MINUTE")
    log_level = os.getenv("LOG_LEVEL", "INFO")
    aws_region = os.getenv("AWS_REGION", "ap-southeast-1")

    enable_testnet = os.getenv("ENABLE_TESTNET", "false").strip().lower() in ("1", "true", "yes")
    if enable_testnet:
        print("❌ ENABLE_TESTNET=true, but TESTNET support isn't implemented — MAINNET only.", file=sys.stderr)
        sys.exit(1)

    queue_url = os.getenv("SQS_TRADE_EVENTS_QUEUE_URL_REAL")
    if not queue_url:
        print("❌ SQS_TRADE_EVENTS_QUEUE_URL_REAL environment variable not set.", file=sys.stderr)
        sys.exit(1)

    account_type = BinanceAccountType.SPOT
    binance_config_kwargs = {"environment": BinanceEnvironment.LIVE}

    try:
        api_key, api_secret = auth.load_credentials(region=aws_region, sandbox=False)
    except Exception as e:
        print(f"❌ Failed to load MAINNET data credentials: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        ed25519_public_key, ed25519_private_key = auth.load_ed25519_credentials(region=aws_region)
        auth.validate_ed25519_private_key(ed25519_private_key)
        auth.warn_if_api_key_looks_like_raw_pem(ed25519_public_key)
        print("🔐 Using Ed25519 credentials for real order execution", file=sys.stderr)
    except Exception as e:
        print(f"❌ Ed25519 credentials required but not loaded: {e}", file=sys.stderr)
        sys.exit(1)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    instrument_provider = new_instrument_provider()
    data_config = BinanceDataClientConfig(
        api_key=api_key, api_secret=api_secret, account_type=account_type,
        instrument_provider=instrument_provider, **binance_config_kwargs,
    )
    exec_config = BinanceExecClientConfig(
        api_key=ed25519_public_key, api_secret=ed25519_private_key, account_type=account_type,
        instrument_provider=instrument_provider, **binance_config_kwargs,
    )
    node = build_trading_node(
        trader_id=trader_id,
        data_clients={BINANCE: data_config},
        exec_clients={BINANCE: exec_config},
        log_level=log_level,
        loop=loop,
    )
    register_binance_data(node)
    register_binance_exec(node)
    node.build()

    sqs_client = sqs.get_sqs_client()

    async def run() -> None:
        while True:
            try:
                db = await TradeEventsDB.get_instance()
                break
            except Exception as e:
                print(f"⚠️ Postgres connection failed: {e}, retrying in 5s...", file=sys.stderr)
                await asyncio.sleep(5)

        try:
            await asyncio.gather(
                node.run_async(),
                listen_trade_events(node, db, sqs_client, queue_url, bar_interval),
                heartbeat_loop(db, node, TARGET, get_balance=lambda: get_real_usdt_balance(node)),
                cancel_requests_loop(db, TARGET, active_strategies),
            )
        except asyncio.CancelledError:
            pass
        finally:
            try:
                await node.stop_async()
            finally:
                node.dispose()

    try:
        loop.run_until_complete(run())
    except KeyboardInterrupt:
        print("\n🛑 Shutdown requested (Ctrl+C)", file=sys.stderr)
    finally:
        loop.close()


if __name__ == "__main__":
    main()

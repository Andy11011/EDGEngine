"""
binance_virtual_mainnet_node.py — headless virtual/sandbox-execution
Binance MAINNET trading node. The other of the two processes split out of
the old EDGETrader.py per MultiVenueTOD.md.

Scope of this process:
  - Binance MAINNET data feed (real live prices) + Nautilus's sandbox exec
    client — fills are computed locally against this live feed; no order
    from this process ever reaches Binance.
  - NO HTTP listener — same exec-based liveness + heartbeat-row story as
    binance_real_node.py (see that file's and node_common.py's docstrings).
  - Consumes ONE dedicated SQS queue, filtered upstream to target=virtual.
    Do NOT point this at the same queue as binance_real_node.py — see
    node_common.py's module docstring for why.

Env vars:
  SQS_TRADE_EVENTS_QUEUE_URL_VIRTUAL   (required) — this process's own queue
  SANDBOX_STARTING_BALANCES            default "10000 USDT,1 BTC"
  SANDBOX_ACCOUNT_TYPE                  default "CASH"
  TRADER_ID                            default "EDGETRADER"
  BINANCE_BAR_INTERVAL                  default "15-MINUTE"
  LOG_LEVEL                            default "INFO"
  AWS_REGION                            default "ap-southeast-1"
  ENABLE_TESTNET                        must be false/unset (see main())
"""

from __future__ import annotations

import asyncio
import os
import sys
from typing import Dict

from nautilus_trader.adapters.binance import BINANCE, BinanceAccountType, BinanceDataClientConfig
from nautilus_trader.adapters.binance.common.enums import BinanceEnvironment
from nautilus_trader.adapters.sandbox.config import SandboxExecutionClientConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId

import node_common as nc
from position_sizing import PositionSizingError, calculate_position_size
from trade_strategy import TradeStrategy, TradeStrategyConfig
from trades_db_async import TradeEventsDB

TARGET = "virtual"

active_strategies: Dict[str, TradeStrategy] = {}


async def listen_trade_events(
    node: TradingNode,
    db: TradeEventsDB,
    sqs_client,
    queue_url: str,
    bar_interval: str,
) -> None:
    """Long-poll this process's dedicated (target=virtual) SQS queue."""
    poll_wait_seconds = int(os.getenv("SQS_POLL_WAIT_SECONDS", "20"))

    print("⏳ Waiting for the TradingNode to finish starting...", file=sys.stderr)
    while not node.trader.is_running:
        await asyncio.sleep(0.5)
    print("✅ TradingNode RUNNING; processing trade events (target=virtual)", file=sys.stderr)

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
        print(f"📡 [poll #{poll_count}] Long-polling SQS (virtual)...", file=sys.stderr)
        messages = await nc.receive_trade_events(
            sqs_client, queue_url,
            max_messages=int(os.getenv("SQS_MAX_MESSAGES", "10")),
            wait_seconds=poll_wait_seconds,
        )
        if not messages:
            continue

        for msg in messages:
            receipt_handle = msg["ReceiptHandle"]
            msg_id = msg.get("MessageId", "?")
            event_data = nc.parse_event(msg)
            if event_data is None:
                print(f"⚠️ [msg {msg_id}] Failed to parse; deleting", file=sys.stderr)
                await nc.delete_message(sqs_client, queue_url, receipt_handle)
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
                    f"virtual-node queue — check the SNS filter policy. Skipping without deleting.",
                    file=sys.stderr,
                )
                continue

            if not ticker or not event_type or not occurred_at:
                print(f"⚠️ [msg {msg_id}] Missing required fields; deleting", file=sys.stderr)
                await nc.delete_message(sqs_client, queue_url, receipt_handle)
                continue

            claimed = await db.claim_event(ticker, event_type, occurred_at, target=TARGET)
            if not claimed:
                print(f"♻️ [msg {msg_id}] Already claimed; deleting (dup delivery)", file=sys.stderr)
                await nc.delete_message(sqs_client, queue_url, receipt_handle)
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
                        equity = sizing_config["virtual_balance_usdt"]
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
                await nc.delete_message(sqs_client, queue_url, receipt_handle)
                print(f"🗑️ [msg {msg_id}] Acked", file=sys.stderr)
            else:
                print(f"⚠️ [msg {msg_id}] Not processed; message retained for retry", file=sys.stderr)


def main() -> None:
    trader_id = os.getenv("TRADER_ID", "EDGETRADER") + "-VIRTUAL"
    bar_interval = os.getenv("BINANCE_BAR_INTERVAL", "15-MINUTE")
    log_level = os.getenv("LOG_LEVEL", "INFO")
    aws_region = os.getenv("AWS_REGION", "ap-southeast-1")

    enable_testnet = os.getenv("ENABLE_TESTNET", "false").strip().lower() in ("1", "true", "yes")
    if enable_testnet:
        print("❌ ENABLE_TESTNET=true, but TESTNET support isn't implemented — MAINNET only.", file=sys.stderr)
        sys.exit(1)

    queue_url = os.getenv("SQS_TRADE_EVENTS_QUEUE_URL_VIRTUAL")
    if not queue_url:
        print("❌ SQS_TRADE_EVENTS_QUEUE_URL_VIRTUAL environment variable not set.", file=sys.stderr)
        sys.exit(1)

    account_type = BinanceAccountType.SPOT
    binance_config_kwargs = {"environment": BinanceEnvironment.LIVE}

    # Virtual node's data client uses the same MAINNET data credentials as
    # the real node (it needs a live feed, never sends real orders).
    try:
        api_key, api_secret = nc.load_credentials(region=aws_region, sandbox=False)
    except Exception as e:
        print(f"❌ Failed to load MAINNET data credentials: {e}", file=sys.stderr)
        sys.exit(1)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    instrument_provider = nc.new_instrument_provider()
    data_config = BinanceDataClientConfig(
        api_key=api_key, api_secret=api_secret, account_type=account_type,
        instrument_provider=instrument_provider, **binance_config_kwargs,
    )
    starting_balances = [
        b.strip() for b in os.getenv("SANDBOX_STARTING_BALANCES", "10000 USDT,1 BTC").split(",") if b.strip()
    ]
    exec_config = SandboxExecutionClientConfig(
        venue=BINANCE,
        account_type=os.getenv("SANDBOX_ACCOUNT_TYPE", "CASH"),
        starting_balances=starting_balances,
        instrument_provider=instrument_provider,
    )
    node = nc.build_trading_node(
        trader_id=trader_id, data_clients={BINANCE: data_config}, exec_clients={BINANCE: exec_config},
        log_level=log_level, loop=loop,
    )
    nc.register_binance_data(node)
    nc.register_sandbox_exec(node)
    node.build()

    sqs_client = nc.get_sqs_client()

    async def get_virtual_balance(db: TradeEventsDB) -> float:
        cfg = await db.get_trades_config()
        return cfg["virtual_balance_usdt"]

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
                nc.heartbeat_loop(db, node, TARGET, get_balance=lambda: get_virtual_balance(db)),
                nc.cancel_requests_loop(db, node, TARGET, active_strategies),
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

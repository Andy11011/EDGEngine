"""EdgeTrader Blueprint - Barebones Trading Node for Execution.

This module connects to Binance via AWS Secrets Manager, subscribes to live bars,
and logs prices. It serves as the foundation for the trading/execution backend
without any indicator or scanning logic.
"""

from __future__ import annotations

import asyncio
from logging import config
import os
import sys
import json
import re
import base64
import uvicorn
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from api import app
from api import active_strategies as api_active_strategies
from api import set_status, node_ref, balance_refs

from nautilus_trader.adapters.binance import (
    BINANCE,
    BinanceAccountType,
    BinanceDataClientConfig,
    BinanceExecClientConfig,
    BinanceLiveDataClientFactory,
    BinanceLiveExecClientFactory,
)
from nautilus_trader.adapters.sandbox.config import SandboxExecutionClientConfig
from nautilus_trader.adapters.sandbox.factory import SandboxLiveExecClientFactory
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
from nautilus_trader.adapters.binance.common.enums import BinanceEnvironment
from position_sizing import PositionSizingError, calculate_position_size
from trade_strategy import TradeStrategy, TradeStrategyConfig
from trades_db_async import TradeEventsDB

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


def load_credentials_from_env(sandbox: bool = False) -> Optional[tuple[str, str]]:
    """
    Load Binance API key/secret from environment variables, for local runs
    where AWS Secrets Manager isn't reachable or desired.

    Returns None (rather than raising) if the env vars aren't set, so callers
    can fall back to AWS.
    """
    prefix = "BINANCE_SANDBOX" if sandbox else "BINANCE"
    api_key = os.getenv(f"{prefix}_API_KEY")
    api_secret = os.getenv(f"{prefix}_API_SECRET")
    if api_key and api_secret:
        return api_key, api_secret
    return None


def load_credentials(region: str = "ap-southeast-1", sandbox: bool = False) -> tuple[str, str]:
    """
    Load Binance API key/secret, preferring local environment variables
    (BINANCE_API_KEY / BINANCE_API_SECRET, or the BINANCE_SANDBOX_* variants)
    and falling back to AWS Secrets Manager if they're not set.
    """
    env_creds = load_credentials_from_env(sandbox=sandbox)
    if env_creds is not None:
        print("✅ Credentials loaded from environment variables (local override)", file=sys.stderr)
        return env_creds
    return load_credentials_from_aws(region=region, sandbox=sandbox)


def load_ed25519_credentials_from_aws(
    region: str = "ap-southeast-1",
    secret_name: str = "Binance_async_keys_Ed25519",
    public_key_field: str = "binance-api-public-key-ed25519",
    private_key_field: str = "binance-api-private-key-ed25519",
) -> tuple[str, str]:
    """
    Load Binance Ed25519 API key and private key from AWS Secrets Manager.

    Unlike the HMAC loader, these two values live as fields inside a single
    JSON secret (secret_name), not as two separate secrets.
    """
    print(f"🔑 (Ed25519) Fetching secret: {secret_name}", file=sys.stderr)

    session = boto3.session.Session()
    client = session.client("secretsmanager", region_name=region)

    try:
        response = client.get_secret_value(SecretId=secret_name)
    except (BotoCoreError, ClientError) as e:
        raise RuntimeError(f"Failed to fetch AWS secret {secret_name}: {e}")

    if "SecretString" not in response:
        raise ValueError(f"Secret {secret_name} has no string value")

    try:
        secret_dict = json.loads(response["SecretString"])
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Secret {secret_name} is not valid JSON: {e}")

    public_key = secret_dict.get(public_key_field)
    private_key = secret_dict.get(private_key_field)

    if not public_key or not private_key:
        raise RuntimeError(
            f"Secret {secret_name} is missing '{public_key_field}' or "
            f"'{private_key_field}' fields (found keys: {list(secret_dict.keys())})"
        )
    return public_key, private_key

def load_ed25519_credentials_from_env() -> Optional[tuple[str, str]]:
    """
    Load Binance Ed25519 public/private key from environment variables, for
    local runs where AWS Secrets Manager isn't reachable or desired.

    Returns None (rather than raising) if the env vars aren't set, so callers
    can fall back to AWS.
    """
    public_key = os.getenv("BINANCE_ED25519_PUBLIC_KEY")
    private_key = os.getenv("BINANCE_ED25519_PRIVATE_KEY")
    if public_key and private_key:
        return public_key, private_key
    return None


def load_ed25519_credentials(
    region: str = "ap-southeast-1",
    secret_name: str = "Binance_async_keys_Ed25519",
    public_key_field: str = "binance-api-public-key-ed25519",
    private_key_field: str = "binance-api-private-key-ed25519",
) -> tuple[str, str]:
    """
    Load Ed25519 credentials, preferring local environment variables
    (BINANCE_ED25519_PUBLIC_KEY / BINANCE_ED25519_PRIVATE_KEY) and falling
    back to AWS Secrets Manager if they're not set.
    """
    env_creds = load_ed25519_credentials_from_env()
    if env_creds is not None:
        print("✅ Ed25519 credentials loaded from environment variables (local override)", file=sys.stderr)
        return env_creds
    return load_ed25519_credentials_from_aws(
        region=region,
        secret_name=secret_name,
        public_key_field=public_key_field,
        private_key_field=private_key_field,
    )


def _validate_ed25519_private_key(pem_str: str) -> None:
    """
    Raise a clear RuntimeError if the given Ed25519 private key looks like an
    ENCRYPTED (password-protected) PKCS8 key. Binance/NautilusTrader only
    accept unencrypted Ed25519 PEM private keys.

    An unencrypted Ed25519 PKCS8 key is always a fixed 48-byte DER structure
    starting with a short-form length header (b'\\x30\\x2e...'). An encrypted
    PKCS8 key (EncryptedPrivateKeyInfo) is larger and uses a long-form length
    header, and contains the PBES2 OID.
    """
    body = pem_str.strip()
    if "BEGIN" in body:
        lines = [ln for ln in body.splitlines() if "BEGIN" not in ln and "END" not in ln]
        body = "".join(lines)
    try:
        der = base64.b64decode(body)
    except Exception:
        return  # not decodable here; let the downstream client surface the real error

    pbes2_oid = bytes([0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x05, 0x0D])
    if len(der) >= 2 and der[0] == 0x30 and der[1] >= 0x81 and pbes2_oid in der:
        raise RuntimeError(
            "The Ed25519 private key appears to be ENCRYPTED (password-protected) "
            "PKCS8. Binance/NautilusTrader only accept unencrypted Ed25519 PEM keys. "
            "Decrypt it first with: openssl pkey -in encrypted.pem -out decrypted.pem "
            "then store the decrypted PEM content back in AWS Secrets Manager."
        )


def _warn_if_api_key_looks_like_raw_pem(api_key: str) -> None:
    """Binance-issued API keys are opaque alphanumeric strings, not PEM/DER blobs."""
    if api_key.startswith(("MC", "-----BEGIN")):
        print(
            "⚠️ WARNING: the Ed25519 'public key' value looks like a raw PEM/DER "
            "public key blob, not a Binance-issued API key. When you register a "
            "self-generated Ed25519 public key under Binance API Management, "
            "Binance issues a separate opaque API key string for it (like your "
            "existing HMAC api_key) — that issued string is what belongs in "
            "api_key, not the public key PEM itself. Double check API Management.",
            file=sys.stderr,
        )


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
    loop: asyncio.AbstractEventLoop,
) -> TradingNode:
    """
    Build a full trading node with both data and execution clients.

    `loop` must be explicitly passed and must be the SAME loop object that
    will later be used to run the node (e.g. via `loop.run_until_complete(...)`).
    TradingNode.__init__ stores whatever loop is current *at construction time*
    (self.kernel.loop) and later checks `self.kernel.loop.is_running()` to
    decide whether it's safe to create client-connect tasks. If we let it fall
    back to `asyncio.get_event_loop()` here (i.e. before any loop is actually
    running) and then hand execution to a *different* loop later — which is
    exactly what `asyncio.run()` does, since it always creates a brand-new
    loop internally — the two loop objects never match. The result: client
    connect() tasks get scheduled on a loop that's never running, and are
    silently dropped ("Async task '_connect' created but event loop is not
    running" / "coroutine ... was never awaited"), so live data never
    connects. Passing `loop` explicitly removes the ambiguity.
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
            reconciliation=True,          # Now we care about order state
            generate_missing_orders=False,
            snapshot_orders=True,         # Keep state across restarts
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


def register_sandbox_exec(node: TradingNode) -> None:
    """Register Nautilus's own simulated exec client (virtual/paper trading)."""
    node.add_exec_client_factory(BINANCE, SandboxLiveExecClientFactory)

# -----------------------------------------------------------------------------
# SQS Consumer helpers
# -----------------------------------------------------------------------------

_sqs_client = None

def get_sqs_client() -> boto3.client:
    """Return a cached SQS client."""
    global _sqs_client
    if _sqs_client is None:
        _sqs_client = boto3.client(
            "sqs",
            region_name=os.getenv("AWS_REGION", "ap-southeast-1")
        )
    return _sqs_client


async def receive_trade_events(
    sqs_client,
    queue_url: str,
    max_messages: int = 10,
    wait_seconds: int = 20,
) -> list[dict]:
    loop = asyncio.get_running_loop()
    try:
        response = await loop.run_in_executor(
            None,
            lambda: sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_seconds,
                MessageAttributeNames=["All"],
            ),
        )
    except Exception as e:
        print(f"❌ SQS receive error: {e}", file=sys.stderr)
        return []
    return response.get("Messages", [])


async def delete_message(sqs_client, queue_url: str, receipt_handle: str) -> bool:
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(
            None,
            lambda: sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle),
        )
        return True
    except Exception as e:
        print(f"❌ SQS delete error: {e}", file=sys.stderr)
        return False


# -----------------------------------------------------------------------------
# Real-mode balance lookup (used for position sizing when not virtual)
# -----------------------------------------------------------------------------

def _get_real_usdt_balance(node: TradingNode) -> float:
    """
    Live free USDT balance from the connected Binance account — used for
    REAL-mode position sizing (TEST3 / LIVE, i.e. use_sim_exec=False).

    We use *free* (not total) balance: total includes funds already locked
    in other open orders/positions, which aren't actually available to size
    a *new* trade against. This mirrors "what could I actually spend right
    now", which is the closest real-account equivalent to Pine's
    strategy.equity for a spot, non-margin bot.

    NOTE: verify this against your installed nautilus_trader version — the
    Portfolio/Account API has moved around across versions. This uses the
    documented Account.balance_free(Currency) pattern; if that doesn't match
    what you have installed, this is the one place to adjust it.
    """
    account = node.portfolio.account(BINANCE)
    if account is None:
        raise RuntimeError(
            "No account available yet for venue BINANCE — the exec client may "
            "not have received its first AccountState update yet. Cannot size "
            "a real-mode trade without a live balance."
        )
    usdt = Currency.from_str("USDT")
    balance = account.balance_free(usdt)
    if balance is None:
        raise RuntimeError("Account has no USDT balance entry yet — cannot size a real-mode trade.")
    return float(balance.as_double())


# -----------------------------------------------------------------------------
# Standalone SQS Trade-Event Listener Coroutine
# -----------------------------------------------------------------------------

async def listen_trade_events(
    nodes: Dict[str, TradingNode],
    sqs_client,
    queue_url: str,
    bar_interval: str,
    active_strategies: Dict[str, TradeStrategy],
) -> None:
    """Long‑poll SQS for trade events, claim via Postgres, and route to target nodes."""
    set_status("postgres", "connecting")
    while True:
        try:
            db = await TradeEventsDB.get_instance()
            break
        except Exception as e:
            print(f"⚠️ Postgres connection failed: {e}, retrying in 5s...", file=sys.stderr)
            set_status("postgres", "failed", detail=str(e))
            await asyncio.sleep(5)
    set_status("postgres", "connected")

    async def _get_virtual_balance() -> float:
        cfg = await db.get_trades_config()
        return cfg["virtual_balance_usdt"]

    balance_refs["get_virtual_balance"] = _get_virtual_balance

    def make_close_callback(node: TradingNode, trade_id: str):
        def callback() -> None:
            strategy = active_strategies.pop(trade_id, None)
            if strategy is not None:
                try:
                    node.trader.remove_strategy(strategy.id)
                    print(f"🧹 Removed strategy for trade {trade_id} from {node.trader.id}")
                except Exception as e:
                    print(f"⚠️ Error removing strategy for {trade_id}: {e}")
        return callback

    poll_wait_seconds = int(os.getenv("SQS_POLL_WAIT_SECONDS", "20"))
    poll_count = 0

    # Wait for both nodes to be running
    print("⏳ Waiting for all TradingNodes to finish starting...", file=sys.stderr)
    for name, node in nodes.items():
        while not node.trader.is_running:
            await asyncio.sleep(0.5)
    print("✅ All TradingNodes are RUNNING; starting to process trade events", file=sys.stderr)

    while True:
        poll_count += 1
        print(f"📡 [poll #{poll_count}] Long-polling SQS...", file=sys.stderr)
        try:
            messages = await receive_trade_events(
                sqs_client,
                queue_url,
                max_messages=int(os.getenv("SQS_MAX_MESSAGES", "10")),
                wait_seconds=poll_wait_seconds,
            )
            set_status("sqs", "connected")
        except Exception as e:
            print(f"❌ SQS receive error: {e}", file=sys.stderr)
            set_status("sqs", "failed", detail=str(e))
            await asyncio.sleep(1)
            continue

        if not messages:
            continue

        print(f"📥 Received {len(messages)} message(s)", file=sys.stderr)

        for msg in messages:
            receipt_handle = msg["ReceiptHandle"]
            msg_id = msg.get("MessageId", "?")

            # Parse SNS wrapper
            try:
                body = json.loads(msg["Body"])
                if body.get("Type") == "Notification":
                    event_data = json.loads(body["Message"])
                else:
                    event_data = body
            except (json.JSONDecodeError, KeyError) as e:
                print(f"⚠️ [msg {msg_id}] Failed to parse: {e}", file=sys.stderr)
                await delete_message(sqs_client, queue_url, receipt_handle)
                continue

            ticker = event_data.get("ticker")
            event_type = event_data.get("event_type")
            occurred_at = event_data.get("occurred_at")
            targets = event_data.get("target", ["virtual"])
            if isinstance(targets, str):
                targets = [targets]

            if not ticker or not event_type or not occurred_at:
                print(f"⚠️ [msg {msg_id}] Missing required fields; deleting", file=sys.stderr)
                await delete_message(sqs_client, queue_url, receipt_handle)
                continue

            # We'll collect successfully processed targets to decide whether to ack
            processed_targets = []

            for target in targets:
                # Claim for this target
                claimed = await db.claim_event(ticker, event_type, occurred_at, target=target)
                if not claimed:
                    print(f"♻️ [msg {msg_id}] Already claimed for target={target}; skipping", file=sys.stderr)
                    continue

                print(f"✅ [msg {msg_id}] Claimed for target={target}", file=sys.stderr)

                if event_type.lower() == "open":
                    side = event_data.get("side")
                    ep = event_data.get("ep")
                    sl = event_data.get("sl")
                    tp = event_data.get("tp")

                    if not side or ep is None or sl is None:
                        print(f"⚠️ [msg {msg_id}] Missing open fields; unclaiming target={target}", file=sys.stderr)
                        await db.unclaim_event(ticker, event_type, occurred_at, target=target)
                        continue

                    # Compute size
                    try:
                        sizing_config = await db.get_trades_config()
                        risk_ratio = sizing_config["risk_ratio"]
                        if target == "real":
                            equity = _get_real_usdt_balance(nodes["real"])
                        else:
                            equity = sizing_config["virtual_balance_usdt"]
                        computed_size = calculate_position_size(
                            equity=equity,
                            risk_ratio=risk_ratio,
                            entry_price=float(ep),
                            stop_loss_price=float(sl),
                        )
                    except Exception as e:
                        print(f"❌ [msg {msg_id}] Sizing failed for target={target}: {e}", file=sys.stderr)
                        await db.unclaim_event(ticker, event_type, occurred_at, target=target)
                        continue

                    # Generate trade_id with target suffix
                    trade_id = f"{ticker}_{occurred_at.replace(':', '').replace('.', '').replace('-', '').replace('Z', '')}_{target}"
                    print(f"🆔 [msg {msg_id}] Generated trade_id={trade_id} for target={target}", file=sys.stderr)

                    instrument_id = InstrumentId.from_str(ticker)
                    bar_type_str = f"{ticker}-{bar_interval}-LAST-EXTERNAL"
                    bar_type = BarType.from_str(bar_type_str)

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
                        target=target,
                    )

                    node = nodes[target]
                    close_callback = make_close_callback(node, trade_id)
                    strategy = TradeStrategy(config, close_callback=close_callback)
                    active_strategies[trade_id] = strategy

                    # Add to the node's trader
                    try:
                        controller = node.kernel._controller
                        controller.create_strategy(strategy, start=True)
                        if strategy.is_running:
                            processed_targets.append(target)
                            print(f"🚀 [msg {msg_id}] Started strategy for trade {trade_id} on {target}", file=sys.stderr)
                        else:
                            raise RuntimeError("Strategy stopped immediately after start")
                    except Exception as e:
                        print(f"❌ [msg {msg_id}] Failed to start strategy for target={target}: {e}", file=sys.stderr)
                        active_strategies.pop(trade_id, None)
                        await db.unclaim_event(ticker, event_type, occurred_at, target=target)
                        # Not acking this target – will retry

                elif event_type.lower() == "cancel":
                    active_trade_id = await db.get_active_trade_for_ticker(ticker, target=target)
                    if active_trade_id is None:
                        print(f"⚠️ [msg {msg_id}] No active trade for target={target}; unclaiming", file=sys.stderr)
                        await db.unclaim_event(ticker, event_type, occurred_at, target=target)
                        continue

                    strategy = active_strategies.get(active_trade_id)
                    if strategy is None:
                        print(f"⚠️ [msg {msg_id}] Strategy for {active_trade_id} not in memory; unclaiming", file=sys.stderr)
                        await db.unclaim_event(ticker, event_type, occurred_at, target=target)
                        continue

                    strategy.request_cancel()
                    processed_targets.append(target)
                    print(f"🛑 [msg {msg_id}] Cancel requested for trade {active_trade_id} (target={target})", file=sys.stderr)

                else:
                    print(f"⚠️ [msg {msg_id}] Unknown event_type '{event_type}'; unclaiming", file=sys.stderr)
                    await db.unclaim_event(ticker, event_type, occurred_at, target=target)
                    continue

            # If at least one target was successfully processed, ack the SQS message.
            # If no target was processed, leave the message to be retried/DLQ.
            if processed_targets:
                await delete_message(sqs_client, queue_url, receipt_handle)
                print(f"🗑️ [msg {msg_id}] Acked after processing targets: {processed_targets}", file=sys.stderr)
            else:
                print(f"⚠️ [msg {msg_id}] No target processed; message retained for retry", file=sys.stderr)
# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    trader_id = os.getenv("TRADER_ID", "EDGETRADER-001")
    bar_interval = os.getenv("BINANCE_BAR_INTERVAL", "15-MINUTE")  # global interval for all strategies
    log_level = os.getenv("LOG_LEVEL", "INFO")
    aws_region = os.getenv("AWS_REGION", "ap-southeast-1")

    # ------------------------------------------------------------------
    # Scope: Binance MAINNET only, with TWO always-on TradingNode instances
    # sharing this process — a real exec node and a virtual (sandbox) exec
    # node, both fed by their own MAINNET data client. "Mode" is no longer a
    # startup-time choice: each incoming SQS 'open' event carries its own
    # `targets` list (["virtual"], ["real"], or both — see
    # listen_trade_events), and is routed to whichever node(s) match.
    #
    # Real and virtual deliberately do NOT share one TradingNode: Nautilus
    # routes orders to exactly one exec client per venue, so a real
    # BinanceExecClientConfig and a SandboxExecutionClientConfig can't both
    # be independently addressable under the same venue in one node. Two
    # separate nodes (each with its own data+exec pair, same pattern this
    # file already used pre-refactor) sidesteps that entirely, at the cost
    # of loading the Binance instrument set twice at startup.
    #
    # TESTNET support is deprioritized for now. ENABLE_TESTNET exists as the
    # switch for it, but isn't wired to anything yet — flipping it fails
    # fast with a clear message rather than silently doing nothing, so a
    # misconfigured deployment can't mistake "the flag exists" for "the
    # feature works".
    # ------------------------------------------------------------------
    enable_testnet = os.getenv("ENABLE_TESTNET", "false").strip().lower() in ("1", "true", "yes")
    if enable_testnet:
        print(
            "❌ ENABLE_TESTNET=true, but TESTNET support isn't implemented yet — "
            "this build only connects to Binance MAINNET (real + virtual exec "
            "nodes). Set ENABLE_TESTNET=false (or leave it unset) to run.",
            file=sys.stderr,
        )
        sys.exit(1)

    environment = "MAINNET"
    account_type = BinanceAccountType.SPOT
    binance_config_kwargs = {"environment": BinanceEnvironment.LIVE}

    print("=" * 78, file=sys.stderr)
    print(f"BINANCE_ENV={environment}  |  exec nodes: real + virtual (both always on)", file=sys.stderr)
    print("=" * 78, file=sys.stderr)

    # Load MAINNET data credentials (env vars first for local runs, AWS
    # Secrets Manager otherwise). Both nodes' data clients use these — the
    # real-vs-virtual split only affects the exec client, not the feed.
    try:
        api_key, api_secret = load_credentials(region=aws_region, sandbox=False)
    except Exception as e:
        print(f"❌ Failed to load MAINNET data credentials (env vars and AWS both failed): {e}", file=sys.stderr)
        sys.exit(1)

    # --- Load Ed25519 keys for the REAL exec node only ---
    # Binance's WebSocket API session.logon (used by Nautilus's live exec
    # client) rejects HMAC-SHA-256 keys, so real order execution must use
    # Ed25519 (or RSA, though RSA isn't supported here). The virtual/sandbox
    # node never sends real orders, so it doesn't need these at all.
    try:
        ed25519_public_key, ed25519_private_key = load_ed25519_credentials(region=aws_region)
        _validate_ed25519_private_key(ed25519_private_key)
        _warn_if_api_key_looks_like_raw_pem(ed25519_public_key)
        print("🔐 Using Ed25519 credentials for the real execution node", file=sys.stderr)
    except Exception as e:
        print(f"❌ Ed25519 credentials required for the real exec node but not loaded: {e}", file=sys.stderr)
        sys.exit(1)

    # ----------------------------------------------------------------------
    # Own the event loop explicitly rather than using asyncio.run(). Both
    # TradingNode instances below are built against this SAME loop object —
    # see build_trading_node()'s docstring for why that matters.
    # ----------------------------------------------------------------------
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Each node gets its own InstrumentProviderConfig instance. load_all=True
    # loads the full Binance Spot instrument set independently per node —
    # duplicated network cost at startup (two full instrument loads instead
    # of one), but it keeps each node's internal state (cache, portfolio,
    # msgbus) fully isolated, which is what lets a real exec client and a
    # sandbox exec client coexist safely in one process.
    real_instrument_provider = InstrumentProviderConfig(load_all=True)
    virtual_instrument_provider = InstrumentProviderConfig(load_all=True)

    # ---- Real exec node: MAINNET data + real Binance exec (Ed25519) ----
    real_data_config = BinanceDataClientConfig(
        api_key=api_key,
        api_secret=api_secret,
        account_type=account_type,
        instrument_provider=real_instrument_provider,
        **binance_config_kwargs,
    )
    real_exec_config = BinanceExecClientConfig(
        api_key=ed25519_public_key,
        api_secret=ed25519_private_key,
        account_type=account_type,
        instrument_provider=real_instrument_provider,
        **binance_config_kwargs,
    )
    real_node = build_trading_node(
        trader_id=f"{trader_id}-REAL",
        data_clients={BINANCE: real_data_config},
        exec_clients={BINANCE: real_exec_config},
        log_level=log_level,
        loop=loop,
    )
    register_binance_data(real_node)
    register_binance_exec(real_node)
    real_node.build()

    breakpoint()

    # ---- Virtual exec node: MAINNET data + Nautilus sandbox exec ----
    # Fills are computed locally against this node's own live MAINNET data
    # feed; no order from this node ever reaches Binance.
    virtual_data_config = BinanceDataClientConfig(
        api_key=api_key,
        api_secret=api_secret,
        account_type=account_type,
        instrument_provider=virtual_instrument_provider,
        **binance_config_kwargs,
    )
    starting_balances = [
        b.strip()
        for b in os.getenv("SANDBOX_STARTING_BALANCES", "10000 USDT,1 BTC").split(",")
        if b.strip()
    ]
    virtual_exec_config = SandboxExecutionClientConfig(
        venue=BINANCE,
        account_type=os.getenv("SANDBOX_ACCOUNT_TYPE", "CASH"),
        starting_balances=starting_balances,
        instrument_provider=virtual_instrument_provider,
    )
    virtual_node = build_trading_node(
        trader_id=f"{trader_id}-VIRTUAL",
        data_clients={BINANCE: virtual_data_config},
        exec_clients={BINANCE: virtual_exec_config},
        log_level=log_level,
        loop=loop,
    )
    register_binance_data(virtual_node)
    register_sandbox_exec(virtual_node)
    virtual_node.build()

    breakpoint()

    # Expose both nodes to the API layer so /health can read
    # node.trader.is_running live (cheap attribute lookup, no I/O) instead
    # of relying on push updates that could go stale.
    node_ref["real"] = real_node
    node_ref["virtual"] = virtual_node

    # Wire up /balance/mainnet. get_virtual_balance is wired up inside
    # listen_trade_events once the DB is ready (same as before).
    balance_refs["get_real_balance"] = lambda: _get_real_usdt_balance(real_node)

    nodes = {"real": real_node, "virtual": virtual_node}

    # Get SQS queue URL (required)
    queue_url = os.getenv("SQS_TRADE_EVENTS_QUEUE_URL")
    if not queue_url:
        print("❌ SQS_TRADE_EVENTS_QUEUE_URL environment variable not set.", file=sys.stderr)
        sys.exit(1)

    sqs_client = get_sqs_client()
    set_status("sqs", "connecting")

    # Define the async entry point that runs everything concurrently
    async def run_event_driven():
        config = uvicorn.Config(app, host="0.0.0.0", port=8000, loop="asyncio")
        server = uvicorn.Server(config)

        try:
            await asyncio.gather(
                real_node.run_async(),
                virtual_node.run_async(),
                listen_trade_events(nodes, sqs_client, queue_url, bar_interval, api_active_strategies),
                server.serve(),
            )
        except asyncio.CancelledError:
            # Normal shutdown
            pass
        finally:
            # Ensure clean teardown of both nodes using async stop
            try:
                await asyncio.gather(
                    real_node.stop_async(),
                    virtual_node.stop_async(),
                    return_exceptions=True,
                )
            finally:
                real_node.dispose()
                virtual_node.dispose()

    # Run the main loop we created and handed to both nodes above
    try:
        loop.run_until_complete(run_event_driven())
    except KeyboardInterrupt:
        print("\n🛑 Shutdown requested (Ctrl+C)", file=sys.stderr)
    finally:
        loop.close()


if __name__ == "__main__":
    main()

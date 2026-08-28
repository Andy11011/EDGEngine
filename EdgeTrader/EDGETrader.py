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
from api import set_status, node_ref

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
    node: TradingNode,
    sqs_client,
    queue_url: str,
    bar_interval: str,
    active_strategies: Dict[str, TradeStrategy],
    is_virtual_mode: bool,
) -> None:
    """
    Long‑poll SQS for trade‑event messages, claim them via Postgres,
    and spin up a TradeStrategy for each new 'open' event.
    'cancel' events are handled by calling request_cancel on the active strategy.
    """
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

    def on_strategy_closed(trade_id: str) -> None:
        """Callback invoked by the strategy when it reaches a terminal state."""
        strategy = active_strategies.pop(trade_id, None)
        if strategy is not None:
            try:
                # remove_strategy()/stop_strategy() take a StrategyId, not the
                # Strategy object — passing the object raised a TypeError that
                # was being swallowed here, so strategies never actually got
                # deregistered from the trader (zombie strategies piling up).
                # remove_strategy() already stops the strategy first if it's
                # still RUNNING, so a separate stop_strategy() call afterwards
                # is both redundant and would raise ValueError (strategy_id no
                # longer registered once removed).
                node.trader.remove_strategy(strategy.id)
                print(f"🧹 Removed strategy for trade {trade_id}")
            except Exception as e:
                print(f"⚠️ Error removing strategy for {trade_id}: {e}")

    poll_wait_seconds = int(os.getenv("SQS_POLL_WAIT_SECONDS", "20"))
    poll_count = 0

    # Wait for the TradingNode to actually finish starting (this includes
    # instrument loading from load_all=True, which can take several seconds)
    # before pulling anything off SQS. Without this gate, listen_trade_events()
    # and node.run_async() race each other (they're launched together via
    # asyncio.gather in main()), and the first message(s) can get processed
    # before any instrument is in the cache — the exact "No instrument found
    # in cache" failure this was hitting, even with load_all=True configured
    # correctly, because it just hadn't finished loading yet.
    print("⏳ Waiting for TradingNode to finish starting before polling SQS...", file=sys.stderr)
    while not node.trader.is_running:
        await asyncio.sleep(0.5)
    print("✅ TradingNode is RUNNING; starting to process trade events", file=sys.stderr)

    while True:
        poll_count += 1
        print(f"📡 [poll #{poll_count}] Long-polling SQS (wait={poll_wait_seconds}s)...", file=sys.stderr)
        try:
            messages = await receive_trade_events(
                sqs_client,
                queue_url,
                max_messages=int(os.getenv("SQS_MAX_MESSAGES", "10")),
                wait_seconds=poll_wait_seconds,
            )
            # A successful call (even with 0 messages) proves connectivity.
            set_status("sqs", "connected")
        except Exception as e:
            print(f"❌ SQS receive error: {e}", file=sys.stderr)
            set_status("sqs", "failed", detail=str(e))
            await asyncio.sleep(1)
            continue

        if not messages:
            print(f"💤 [poll #{poll_count}] No messages received", file=sys.stderr)
            continue

        print(f"📥 [poll #{poll_count}] Received {len(messages)} message(s)", file=sys.stderr)

        for msg in messages:
            receipt_handle = msg["ReceiptHandle"]
            msg_id = msg.get("MessageId", "?")
            raw_body_preview = msg.get("Body", "")[:500]
            print(f"📨 [msg {msg_id}] Raw body: {raw_body_preview}", file=sys.stderr)

            # ---------- Parse SNS notification ----------
            try:
                body = json.loads(msg["Body"])
                if body.get("Type") == "Notification":
                    event_data = json.loads(body["Message"])
                    print(f"📨 [msg {msg_id}] Unwrapped SNS notification envelope", file=sys.stderr)
                else:
                    event_data = body
            except (json.JSONDecodeError, KeyError) as e:
                print(f"⚠️ [msg {msg_id}] Failed to parse message: {e}", file=sys.stderr)
                await delete_message(sqs_client, queue_url, receipt_handle)
                print(f"🗑️ [msg {msg_id}] Deleted (unparseable)", file=sys.stderr)
                continue

            # ---------- Extract required fields ----------
            ticker = event_data.get("ticker")
            event_type = event_data.get("event_type")   # "open" or "cancel"
            occurred_at = event_data.get("occurred_at")

            if not ticker or not event_type or not occurred_at:
                print(f"⚠️ [msg {msg_id}] Missing ticker/event_type/occurred_at; deleting", file=sys.stderr)
                await delete_message(sqs_client, queue_url, receipt_handle)
                continue

            # ---------- Claim (dedup) ----------
            claimed = await db.claim_event(ticker, event_type, occurred_at)
            if not claimed:
                print(f"♻️ [msg {msg_id}] Already claimed (duplicate) — acking and skipping", file=sys.stderr)
                await delete_message(sqs_client, queue_url, receipt_handle)
                continue
            print(f"✅ [msg {msg_id}] Claimed for processing", file=sys.stderr)

            # ---------- Process based on event_type ----------
            print(f"🔎 [msg {msg_id}] Processing event_type='{event_type}' for ticker={ticker}", file=sys.stderr)

            if event_type.lower() == "open":
                # Required fields for an open trade. Position size is not
                # part of the payload at all — it's always computed by us
                # (see below), off our own real/virtual equity, since a
                # size derived from TradingView's strategy.equity has no
                # relation to our real or virtual account balance.
                side = event_data.get("side")
                ep = event_data.get("ep")
                sl = event_data.get("sl")
                tp = event_data.get("tp")

                print(
                    f"📥 [msg {msg_id}] OPEN payload: side={side} ep={ep} sl={sl} tp={tp}",
                    file=sys.stderr,
                )

                if not side or ep is None or sl is None:
                    print(f"⚠️ [msg {msg_id}] Missing required open fields (side, ep, sl); deleting", file=sys.stderr)
                    await delete_message(sqs_client, queue_url, receipt_handle)
                    continue

                # ---------- Compute position size ourselves ----------
                # Real mode (TEST3/LIVE): size off the live account balance.
                # Virtual mode (TEST1/TEST2): size off the configured virtual
                # balance. Either way, the risk-ratio/equity formula matches
                # LevelsBot_v1_0_7.pine's entry_zone_calc_routine() exactly.
                try:
                    sizing_config = await db.get_trades_config()
                    risk_ratio = sizing_config["risk_ratio"]

                    if is_virtual_mode:
                        equity = sizing_config["virtual_balance_usdt"]
                        equity_source = "virtual"
                    else:
                        equity = _get_real_usdt_balance(node)
                        equity_source = "real"

                    computed_size = calculate_position_size(
                        equity=equity,
                        risk_ratio=risk_ratio,
                        entry_price=float(ep),
                        stop_loss_price=float(sl),
                    )
                    print(
                        f"💰 [msg {msg_id}] Sized trade: equity={equity:.2f} USDT ({equity_source}) "
                        f"risk_ratio={risk_ratio} -> size={computed_size:.6f}",
                        file=sys.stderr,
                    )
                except (PositionSizingError, RuntimeError) as e:
                    print(f"❌ [msg {msg_id}] Position sizing failed: {e}; will retry via DLQ", file=sys.stderr)
                    # Do not delete SQS – will go to DLQ. But undo the claim
                    # too, otherwise a redelivery is treated as a duplicate
                    # and silently dropped instead of actually retrying.
                    await db.unclaim_event(ticker, event_type, occurred_at)
                    continue

                # Generate a trade_id (deterministic from ticker + occurred_at)
                trade_id = f"{ticker}_{occurred_at.replace(':', '').replace('.', '').replace('-', '').replace('Z', '')}"
                print(f"🆔 [msg {msg_id}] Generated trade_id={trade_id}", file=sys.stderr)

                instrument_id = InstrumentId.from_str(ticker)
                bar_type_str = f"{ticker}-{bar_interval}-LAST-EXTERNAL"
                bar_type = BarType.from_str(bar_type_str)
                print(f"📊 [msg {msg_id}] instrument_id={instrument_id} bar_type={bar_type_str}", file=sys.stderr)

                config = TradeStrategyConfig(
                    instrument_id=instrument_id,
                    bar_type=bar_type,
                    trade_id=trade_id,
                    side=side,
                    size=computed_size,
                    entry_price=float(ep),
                    sl_price=float(sl) if sl is not None else None,
                    tp_price=float(tp) if tp is not None else None,
                    strategy_id=trade_id,   # use your trade_id as the strategy ID
                )
                print(f"🧩 [msg {msg_id}] TradeStrategyConfig built for trade_id={trade_id}", file=sys.stderr)

                async def add_strategy_to_trader(trader, strategy) -> bool:
                    controller = node.kernel._controller  # registered above
                    try:
                        controller.create_strategy(strategy, start=True)  # add_strategy() + start(), controller-bypassed
                        print(f"✅ Strategy {strategy.config.trade_id} added and started")
                    except Exception as e:
                        print(f"❌ Failed to add strategy: {e}")
                        raise

                    if strategy.is_running:
                        print(f"✅ Strategy {strategy.config.trade_id} added and started")
                        return True
                    else:
                        # on_start() ran, hit an internal problem (e.g. instrument not
                        # in cache yet), logged it, and stopped itself. No exception was
                        # raised, so we only catch this by checking state explicitly.
                        print(
                            f"❌ Strategy {strategy.config.trade_id} did not stay running "
                            f"after start (see strategy log above for the cause)"
                        )
                        return False

                try:
                    strategy = TradeStrategy(config, close_callback=on_strategy_closed)
                    active_strategies[trade_id] = strategy
                    print(
                        f"🗂️ [msg {msg_id}] Registered strategy in active_strategies "
                        f"(now tracking {len(active_strategies)} active)",
                        file=sys.stderr,
                    )
                    started_ok = await add_strategy_to_trader(node.trader, strategy)
                except Exception as e:
                    print(f"❌ [msg {msg_id}] Failed to start strategy: {e}", file=sys.stderr)
                    active_strategies.pop(trade_id, None)
                    # Do not delete SQS – will go to DLQ. But undo the claim too,
                    # otherwise a redelivery will be treated as a duplicate and
                    # silently dropped instead of actually retrying.
                    await db.unclaim_event(ticker, event_type, occurred_at)
                    continue

                if not started_ok:
                    # Belt-and-braces: on_strategy_closed should already have popped
                    # this via the close_callback, but pop defensively in case that
                    # path didn't fire for some reason.
                    active_strategies.pop(trade_id, None)
                    unclaimed = await db.unclaim_event(ticker, event_type, occurred_at)
                    print(
                        f"⚠️ [msg {msg_id}] Strategy failed to start for trade {trade_id}; "
                        f"NOT acking SQS message so it can be retried "
                        f"(unclaimed={unclaimed})",
                        file=sys.stderr,
                    )
                    # Do not ack — leave the message for SQS to redeliver/DLQ.
                    continue

                print(f"🚀 [msg {msg_id}] Started TradeStrategy for trade {trade_id} on {ticker}")

            elif event_type.lower() == "cancel":
                print(f"🔍 [msg {msg_id}] Looking up active trade for ticker={ticker}", file=sys.stderr)
                # Cancel the active trade for this ticker
                active_trade_id = await db.get_active_trade_for_ticker(ticker)
                if active_trade_id is None:
                    print(f"⚠️ [msg {msg_id}] No active open trade found for {ticker}; acking and skipping", file=sys.stderr)
                    await delete_message(sqs_client, queue_url, receipt_handle)
                    continue
                print(f"🔗 [msg {msg_id}] Found active_trade_id={active_trade_id} for ticker={ticker}", file=sys.stderr)

                strategy = active_strategies.get(active_trade_id)
                if strategy is None:
                    print(f"⚠️ [msg {msg_id}] Active trade {active_trade_id} not in memory; acking", file=sys.stderr)
                    await delete_message(sqs_client, queue_url, receipt_handle)
                    continue

                # Ask the strategy to cancel/close the trade
                try:
                    strategy.request_cancel()
                    print(f"🛑 [msg {msg_id}] Sent cancel request to trade {active_trade_id}")
                except Exception as e:
                    # request_cancel() isn't wrapped anywhere upstream — an uncaught
                    # exception here would previously crash the whole listener
                    # coroutine, not just this one message.
                    print(f"❌ [msg {msg_id}] request_cancel() failed for trade {active_trade_id}: {e}", file=sys.stderr)
                    await db.unclaim_event(ticker, event_type, occurred_at)
                    print(f"⚠️ [msg {msg_id}] NOT acking SQS message so the cancel can be retried", file=sys.stderr)
                    continue

            else:
                print(f"⚠️ [msg {msg_id}] Unknown event_type '{event_type}'; deleting", file=sys.stderr)
                await delete_message(sqs_client, queue_url, receipt_handle)
                continue

            # ---------- Acknowledge SQS ----------
            print(f"📨 [msg {msg_id}] Acknowledging SQS message", file=sys.stderr)
            acked = await delete_message(sqs_client, queue_url, receipt_handle)
            print(f"{'🗑️' if acked else '⚠️'} [msg {msg_id}] SQS message {'acked' if acked else 'ack FAILED'}", file=sys.stderr)

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    trader_id = os.getenv("TRADER_ID", "EDGETRADER-001")
    bar_interval = os.getenv("BINANCE_BAR_INTERVAL", "15-MINUTE")  # global interval for all strategies
    log_level = os.getenv("LOG_LEVEL", "INFO")
    aws_region = os.getenv("AWS_REGION", "ap-southeast-1")

    # MODE is the single source of truth for how this node behaves. The
    # Binance data environment, whether execution is simulated, and which
    # credentials are required are all *derived* from it below rather than
    # independently settable, so it's impossible for them to drift out of
    # sync with each other (e.g. believing you're on testnet while the venue
    # or credentials silently point at mainnet).
    #
    #   TEST1 (default) - Nautilus-simulated orders (SandboxExecutionClient),
    #                      fed by live market data from Binance TESTNET. No
    #                      real orders are ever sent to Binance. Needs only
    #                      BINANCE_SANDBOX_API_KEY/SECRET (for the testnet
    #                      data feed) — no Ed25519 exec credentials.
    #   TEST2           - Nautilus-simulated orders (SandboxExecutionClient),
    #                      fed by live market data from Binance MAINNET. No
    #                      real orders are ever sent to Binance. Needs only
    #                      BINANCE_API_KEY/SECRET (for the mainnet data feed)
    #                      — no Ed25519 exec credentials.
    #   TEST3           - Real orders sent to Binance's Spot/Futures TESTNET
    #                      (not simulated). Needs BINANCE_SANDBOX_API_KEY/SECRET
    #                      for data, plus BINANCE_ED25519_PUBLIC_KEY/PRIVATE_KEY
    #                      for order execution.
    #   LIVE            - Real trading on Binance MAINNET with real funds.
    #                      Needs BINANCE_API_KEY/SECRET for data, plus
    #                      BINANCE_ED25519_PUBLIC_KEY/PRIVATE_KEY for order
    #                      execution.
    mode = os.getenv("TRADING_MODE", "TEST1").upper()

    # mode -> (Binance environment used for data/exec, use simulated exec?)
    _MODE_TABLE = {
        "TEST1": ("TESTNET", True),
        "TEST2": ("LIVE", True),
        "TEST3": ("TESTNET", False),
        "LIVE": ("LIVE", False),
    }
    if mode not in _MODE_TABLE:
        print(f"❌ Unsupported TRADING_MODE: {mode}. Use TEST1, TEST2, TEST3, or LIVE.", file=sys.stderr)
        sys.exit(1)

    environment, use_sim_exec = _MODE_TABLE[mode]
    sandbox = environment == "TESTNET"  # drives which data credentials to load

    _MODE_DESCRIPTIONS = {
        "TEST1": "🧪 Nautilus-simulated trades, fed by live market data from Binance TESTNET. "
                 "No real orders will be sent to Binance.",
        "TEST2": "🧪 Nautilus-simulated trades, fed by live market data from Binance MAINNET. "
                 "No real orders will be sent to Binance.",
        "TEST3": "🟡 Real orders placed on Binance TESTNET (not simulated) — no real funds at risk.",
        "LIVE": "🔴 Doing LIVE trades on Binance MAINNET. Real orders with real funds.",
    }
    print("=" * 78, file=sys.stderr)
    print(
        f"MODE={mode}  |  BINANCE_ENV={environment}  |  "
        f"SIM_EXEC={'1' if use_sim_exec else '0'}  |  "
        f"BINANCE_SANDBOX={'1' if sandbox else '0'}",
        file=sys.stderr,
    )
    print(_MODE_DESCRIPTIONS[mode], file=sys.stderr)
    print("=" * 78, file=sys.stderr)

    # Load credentials (env vars first for local runs, AWS Secrets Manager otherwise)
    try:
        api_key, api_secret = load_credentials(region=aws_region, sandbox=sandbox)
    except Exception as e:
        print(f"❌ Failed to load credentials (env vars and AWS both failed): {e}", file=sys.stderr)
        sys.exit(1)

    # --- Load Ed25519 keys and use them for the EXEC client ---
    # Only needed when real orders are actually sent to Binance (TEST3, LIVE).
    # Binance's WebSocket API session.logon (used by Nautilus exec clients)
    # rejects HMAC-SHA-256 keys, so the exec client must use Ed25519 (or RSA,
    # though RSA is not supported for execution). Nautilus auto-detects the
    # key type from the api_secret format, so no key_type config is needed.
    # Skipped entirely when use_sim_exec is True, since no real exec
    # connection is ever made.
    #
    # This now hard-fails (rather than warning and silently continuing) if a
    # real-exec mode is missing credentials. Continuing with api_key=None
    # previously caused Nautilus's own internal fallback to look for its own
    # differently-named env var and raise a confusing, unrelated-looking
    # error deep inside the library instead of a clear failure here.
    exec_api_key = exec_api_secret = None
    if not use_sim_exec:
        try:
            ed25519_public_key, ed25519_private_key = load_ed25519_credentials(region=aws_region)

            _validate_ed25519_private_key(ed25519_private_key)
            _warn_if_api_key_looks_like_raw_pem(ed25519_public_key)

            exec_api_key = ed25519_public_key
            exec_api_secret = ed25519_private_key
            print("🔐 Using Ed25519 credentials for the execution client", file=sys.stderr)
        except Exception as e:
            print(f"❌ Ed25519 credentials required for MODE={mode} but not loaded: {e}", file=sys.stderr)
            sys.exit(1)

    account_type = BinanceAccountType.SPOT

    binance_config_kwargs = _resolve_binance_config_kwargs(environment)

    # Shared instrument provider config. load_all=True fetches every instrument
    # for the account type (SPOT) at startup and keeps the cache populated —
    # without this (or load_ids), the instrument cache stays permanently empty
    # and every strategy fails at on_start() with "No instrument found in
    # cache", as confirmed by Nautilus's own startup warning:
    #   "No loading configured: ensure either `load_all=True` or there are `load_ids`"
    # Trade-off: loads thousands of Binance Spot instruments at startup, which
    # adds some latency before the node is ready to trade, but tickers arrive
    # dynamically via SQS so we can't know load_ids in advance.
    instrument_provider_config = InstrumentProviderConfig(load_all=True)

    # Data client config
    data_config = BinanceDataClientConfig(
        api_key=api_key,
        api_secret=api_secret,
        account_type=account_type,
        instrument_provider=instrument_provider_config,
        **binance_config_kwargs,
    )

    # Execution client config
    if use_sim_exec:
        # Nautilus's own simulated exec client: fills are computed locally
        # against the live data feed above, no order ever reaches Binance.
        starting_balances = [
            b.strip()
            for b in os.getenv("SANDBOX_STARTING_BALANCES", "10000 USDT,1 BTC").split(",")
            if b.strip()
        ]
        exec_config = SandboxExecutionClientConfig(
            venue=BINANCE,
            account_type=os.getenv("SANDBOX_ACCOUNT_TYPE", "CASH"),
            starting_balances=starting_balances,
            instrument_provider=instrument_provider_config,
        )
    else:
        # Real Binance execution (testnet or mainnet). Uses Ed25519 credentials
        # when available/valid (see above), since Binance's WebSocket API
        # session.logon rejects HMAC keys for the exec client.
        exec_config = BinanceExecClientConfig(
            api_key=exec_api_key,
            api_secret=exec_api_secret,
            account_type=account_type,
            instrument_provider=instrument_provider_config,
            **binance_config_kwargs,
        )

    # ----------------------------------------------------------------------
    # Own the event loop explicitly rather than using asyncio.run().
    # ----------------------------------------------------------------------
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Build the full trading node
    node = build_trading_node(
        trader_id=trader_id,
        data_clients={BINANCE: data_config},
        exec_clients={BINANCE: exec_config},
        log_level=log_level,
        loop=loop,
    )

    # ----------------------------------------------------------------------
    # Event‑driven trade execution
    # ----------------------------------------------------------------------
    # 1. Register data and execution factories
    register_binance_data(node)
    register_exec = register_sandbox_exec if use_sim_exec else register_binance_exec
    register_exec(node)

    # 2. Build the node's clients (now that factories are registered)
    node.build()

    # Expose the node to the API layer so /health can read node.trader.is_running
    # live (a cheap attribute lookup, no I/O) instead of relying on push updates.
    node_ref["node"] = node

    # 3. Get SQS queue URL (required)
    queue_url = os.getenv("SQS_TRADE_EVENTS_QUEUE_URL")
    if not queue_url:
        print("❌ SQS_TRADE_EVENTS_QUEUE_URL environment variable not set.", file=sys.stderr)
        sys.exit(1)

    sqs_client = get_sqs_client()
    set_status("sqs", "connecting")

    # 4. Define the async entry point that runs both coroutines concurrently
    async def run_event_driven():
        config = uvicorn.Config(app, host="0.0.0.0", port=8000, loop="asyncio")
        server = uvicorn.Server(config)

        try:
            await asyncio.gather(
                node.run_async(),
                listen_trade_events(node, sqs_client, queue_url, bar_interval, api_active_strategies, use_sim_exec),
                server.serve(),
            )
        except asyncio.CancelledError:
            # Normal shutdown
            pass
        finally:
            # Ensure clean teardown using async stop
            try:
                await node.stop_async()
            finally:
                node.dispose()

    # 5. Run the main loop we created and handed to the node above
    try:
        loop.run_until_complete(run_event_driven())
    except KeyboardInterrupt:
        print("\n🛑 Shutdown requested (Ctrl+C)", file=sys.stderr)
    finally:
        loop.close()


if __name__ == "__main__":
    main()

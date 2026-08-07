"""EdgeTrader Blueprint - Barebones Trading Node for Execution.

This module connects to Binance via AWS Secrets Manager, subscribes to live bars,
and logs prices. It serves as the foundation for the trading/execution backend
without any indicator or scanning logic.
"""

from __future__ import annotations

import asyncio
import os
import sys
import json
import re
import base64
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

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
    """
    Long‑poll SQS for trade event messages.

    Returns a list of messages, each containing:
        - 'Body' (JSON string with event data)
        - 'ReceiptHandle'
        - 'MessageAttributes' (event_id, trade_id, event_type)
    """
    try:
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_seconds,
            MessageAttributeNames=["All"],
        )
    except Exception as e:
        # Log error but don't crash; return empty list
        print(f"❌ SQS receive error: {e}", file=sys.stderr)
        return []

    return response.get("Messages", [])


async def delete_message(sqs_client, queue_url: str, receipt_handle: str) -> bool:
    """Delete a processed SQS message. Returns True on success."""
    try:
        sqs_client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle,
        )
        return True
    except Exception as e:
        print(f"❌ SQS delete error: {e}", file=sys.stderr)
        return False


# -----------------------------------------------------------------------------
# Standalone SQS Trade-Event Listener Coroutine
# -----------------------------------------------------------------------------

async def listen_trade_events(
    node: TradingNode,
    sqs_client,
    queue_url: str,
    bar_interval: str,
) -> None:
    """
    Long‑poll SQS for trade‑event messages, claim them via Postgres,
    and spin up a TradeStrategy for each new 'open' event.
    'cancel' events are handled by calling request_cancel on the active strategy.
    """
    while True:
        try:
            db = await TradeEventsDB.get_instance()
            break
        except Exception as e:
            print(f"⚠️ Postgres connection failed: {e}, retrying in 5s...", file=sys.stderr)
            await asyncio.sleep(5)

    active_strategies: Dict[str, TradeStrategy] = {}  # trade_id -> strategy

    def on_strategy_closed(trade_id: str) -> None:
        """Callback invoked by the strategy when it reaches a terminal state."""
        strategy = active_strategies.pop(trade_id, None)
        if strategy is not None:
            try:
                node.trader.remove_strategy(strategy)
                node.trader.stop_strategy(strategy)
                print(f"🧹 Removed strategy for trade {trade_id}")
            except Exception as e:
                print(f"⚠️ Error removing strategy for {trade_id}: {e}")

    poll_wait_seconds = int(os.getenv("SQS_POLL_WAIT_SECONDS", "20"))
    poll_count = 0

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
        except Exception as e:
            print(f"❌ SQS receive error: {e}", file=sys.stderr)
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
            if event_type.lower() == "open":
                # Required fields for an open trade
                side = event_data.get("side")
                size = event_data.get("size")
                ep = event_data.get("ep")
                sl = event_data.get("sl")
                tp = event_data.get("tp")

                if not side or size is None or ep is None:
                    print(f"⚠️ [msg {msg_id}] Missing required open fields (side, size, ep); deleting", file=sys.stderr)
                    await delete_message(sqs_client, queue_url, receipt_handle)
                    continue

                # Generate a trade_id (deterministic from ticker + occurred_at)
                trade_id = f"{ticker}_{occurred_at.replace(':', '').replace('.', '').replace('-', '').replace('Z', '')}"

                instrument_id = InstrumentId.from_str(ticker)
                bar_type_str = f"{ticker}-{bar_interval}-LAST-EXTERNAL"
                bar_type = BarType.from_str(bar_type_str)

                config = TradeStrategyConfig(
                    instrument_id=instrument_id,
                    bar_type=bar_type,
                    trade_id=trade_id,
                    side=side,
                    size=float(size),
                    entry_price=float(ep),
                    sl_price=float(sl) if sl is not None else None,
                    tp_price=float(tp) if tp is not None else None,
                    strategy_id=trade_id,   # use your trade_id as the strategy ID
                )

                async def add_strategy_to_trader(trader, strategy):
                    """Stop the trader, add the strategy, then restart it."""
                    try:
                        if trader.is_running:
                            trader.stop()                     # synchronous
                        trader.add_strategy(strategy)         # add the strategy
                        trader.start_strategy(strategy.id)    # pass its ID, not the object
                        if not trader.is_running:
                            trader.start()                    # synchronous
                        print(f"✅ Strategy {strategy.config.trade_id} added and started")
                    except Exception as e:
                        print(f"❌ Failed to add strategy: {e}")
                        raise

                try:
                    strategy = TradeStrategy(config, close_callback=on_strategy_closed)
                    active_strategies[trade_id] = strategy
                    await add_strategy_to_trader(node.trader, strategy)
                    print(f"🚀 [msg {msg_id}] Started TradeStrategy for trade {trade_id} on {ticker}")
                except Exception as e:
                    print(f"❌ [msg {msg_id}] Failed to start strategy: {e}", file=sys.stderr)
                    # Do not delete SQS – will go to DLQ
                    continue

            elif event_type.lower() == "cancel":
                # Cancel the active trade for this ticker
                active_trade_id = await db.get_active_trade_for_ticker(ticker)
                if active_trade_id is None:
                    print(f"⚠️ [msg {msg_id}] No active open trade found for {ticker}; acking and skipping", file=sys.stderr)
                    await delete_message(sqs_client, queue_url, receipt_handle)
                    continue

                strategy = active_strategies.get(active_trade_id)
                if strategy is None:
                    print(f"⚠️ [msg {msg_id}] Active trade {active_trade_id} not in memory; acking", file=sys.stderr)
                    await delete_message(sqs_client, queue_url, receipt_handle)
                    continue

                # Ask the strategy to cancel/close the trade
                strategy.request_cancel()
                print(f"🛑 [msg {msg_id}] Sent cancel request to trade {active_trade_id}")

            else:
                print(f"⚠️ [msg {msg_id}] Unknown event_type '{event_type}'; deleting", file=sys.stderr)
                await delete_message(sqs_client, queue_url, receipt_handle)
                continue

            # ---------- Acknowledge SQS ----------
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

    # TRADING_MODE is the single source of truth for how this node behaves.
    # BINANCE_ENV and BINANCE_SANDBOX are *derived* from it below rather than
    # being independently settable, so it's impossible for them to drift out
    # of sync with each other (e.g. TRADING_MODE=TESTNET while BINANCE_ENV
    # silently defaults to LIVE, which would have sent real orders to mainnet
    # with live credentials while believing you were on testnet).
    #
    #   VIRTUAL  (default) - live market data from Binance MAINNET, but orders
    #                        are simulated locally by Nautilus's own
    #                        SandboxExecutionClient. Nothing is ever sent to
    #                        Binance. No exec credentials are needed.
    #   TESTNET  - orders go to Binance's real Spot/Futures Testnet, using
    #              sandbox credentials (BINANCE_SANDBOX_API_KEY/SECRET, or the
    #              sandbox AWS secret).
    #   LIVE     - real trading on Binance mainnet (current default behaviour).
    trading_mode = os.getenv("TRADING_MODE", "VIRTUAL").upper()
    if trading_mode not in {"VIRTUAL", "TESTNET", "LIVE"}:
        print(f"❌ Unsupported TRADING_MODE: {trading_mode}. Use VIRTUAL, TESTNET, or LIVE.", file=sys.stderr)
        sys.exit(1)

    # Derived from TRADING_MODE — not independently configurable.
    sandbox = trading_mode == "TESTNET"
    environment = "TESTNET" if trading_mode == "TESTNET" else "LIVE"

    _MODE_DESCRIPTIONS = {
        "VIRTUAL": "🧪 Doing Nautilus-simulated trades locally, fed by live market data from Binance MAINNET. "
                   "No real orders will be sent to Binance.",
        "TESTNET": "🟡 Doing live trades on Binance TESTNET. Real orders are placed, but on the testnet — no real funds at risk.",
        "LIVE":    "🔴 Doing LIVE trades on Binance MAINNET. Real orders with real funds.",
    }
    print("=" * 78, file=sys.stderr)
    print(
        f"MODE  TRADING_MODE={trading_mode}  |  BINANCE_ENV={environment}  |  "
        f"BINANCE_SANDBOX={'1' if sandbox else '0'}",
        file=sys.stderr,
    )
    print(_MODE_DESCRIPTIONS[trading_mode], file=sys.stderr)
    print("=" * 78, file=sys.stderr)

    # Load credentials (env vars first for local runs, AWS Secrets Manager otherwise)
    try:
        api_key, api_secret = load_credentials(region=aws_region, sandbox=sandbox)
    except Exception as e:
        print(f"❌ Failed to load credentials (env vars and AWS both failed): {e}", file=sys.stderr)
        sys.exit(1)

    # --- Load Ed25519 keys and use them for the EXEC client ---
    # Binance's WebSocket API session.logon (used by Nautilus exec clients)
    # rejects HMAC-SHA-256 keys, so the exec client must use Ed25519 (or RSA,
    # though RSA is not supported for execution). Nautilus auto-detects the
    # key type from the api_secret format, so no key_type config is needed.
    # Skipped entirely in VIRTUAL mode since no real exec connection is made.
    exec_api_key = exec_api_secret = None
    if trading_mode != "VIRTUAL":
        try:
            ed25519_public_key, ed25519_private_key = load_ed25519_credentials(region=aws_region)

            _validate_ed25519_private_key(ed25519_private_key)
            _warn_if_api_key_looks_like_raw_pem(ed25519_public_key)

            exec_api_key = ed25519_public_key
            exec_api_secret = ed25519_private_key
            print("🔐 Using Ed25519 credentials for the execution client", file=sys.stderr)
        except Exception as e:
            print(f"⚠️ Ed25519 credentials not loaded: {e}", file=sys.stderr)

    account_type = BinanceAccountType.SPOT

    binance_config_kwargs = _resolve_binance_config_kwargs(environment)

    # Shared instrument provider config – now WITHOUT load_ids to allow dynamic loading
    instrument_provider_config = InstrumentProviderConfig()  # loads instruments on demand

    # Data client config
    data_config = BinanceDataClientConfig(
        api_key=api_key,
        api_secret=api_secret,
        account_type=account_type,
        instrument_provider=instrument_provider_config,
        **binance_config_kwargs,
    )

    # Execution client config
    if trading_mode == "VIRTUAL":
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
    register_exec = register_sandbox_exec if trading_mode == "VIRTUAL" else register_binance_exec
    register_exec(node)

    # 2. Build the node's clients (now that factories are registered)
    node.build()

    # 3. Get SQS queue URL (required)
    queue_url = os.getenv("SQS_TRADE_EVENTS_QUEUE_URL")
    if not queue_url:
        print("❌ SQS_TRADE_EVENTS_QUEUE_URL environment variable not set.", file=sys.stderr)
        sys.exit(1)

    sqs_client = get_sqs_client()

    # 4. Define the async entry point that runs both coroutines concurrently
    async def run_event_driven():
        try:
            await asyncio.gather(
                node.run_async(),
                listen_trade_events(node, sqs_client, queue_url, bar_interval),
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

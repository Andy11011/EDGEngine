"""
node_common.py — shared plumbing for the per-target headless trading node
processes (binance_real_node.py, binance_virtual_mainnet_node.py).

Split out of the old EDGETrader.py so the two node processes don't each
carry their own copy of credential-loading / TradingNode-construction /
SQS-consumer code. Per MultiVenueTOD.md, these node processes:

  - have NO HTTP listener at all (health is exec-based / heartbeat-row-
    based, not HTTP — see write_heartbeat below).
  - are write-only against Postgres (heartbeat + trade_events + fills).
    They never serve a read request; that's edge-api.py's job.
  - each own ONE dedicated SQS queue, already filtered to this process's
    `target` upstream (via an SNS subscription filter policy on a
    `target` message attribute — the same fan-out pattern the plan uses
    for `venue`, just applied one level deeper). That's what makes it
    safe for two separate processes to each ack messages independently:
    neither one ever sees a message meant for the other, so there's no
    "who gets to delete it" race. If your publisher/SNS setup doesn't
    filter by target yet, do NOT point both node processes at the same
    queue URL — a dual-target event would only ever be handled by
    whichever process's poll happened to win.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import sys
from typing import Any, Dict, Optional

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:
    print("❌ boto3 not installed. Run: pip install boto3", file=sys.stderr)
    sys.exit(1)

from nautilus_trader.adapters.binance import (
    BINANCE,
    BinanceLiveDataClientFactory,
    BinanceLiveExecClientFactory,
)
from nautilus_trader.adapters.sandbox.factory import SandboxLiveExecClientFactory
from nautilus_trader.config import (
    InstrumentProviderConfig,
    LiveDataEngineConfig,
    LiveExecEngineConfig,
    LoggingConfig,
    TradingNodeConfig,
)
from nautilus_trader.live.node import TradingNode
from nautilus_trader.trading.config import ImportableControllerConfig

from trades_db_async import TradeEventsDB

VENUE = "binance"  # only venue this deployment runs; matches the `venue`
                    # column added elsewhere per MultiVenueTOD.md

# -----------------------------------------------------------------------
# Credential loading (unchanged from EDGETrader.py — env vars first for
# local runs, AWS Secrets Manager as fallback)
# -----------------------------------------------------------------------


def load_credentials_from_aws(region: str = "ap-southeast-1", sandbox: bool = False) -> tuple[str, str]:
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
    """Load Binance API key/secret from env vars. Returns None if unset."""
    prefix = "BINANCE_SANDBOX" if sandbox else "BINANCE"
    api_key = os.getenv(f"{prefix}_API_KEY")
    api_secret = os.getenv(f"{prefix}_API_SECRET")
    if api_key and api_secret:
        return api_key, api_secret
    return None


def load_credentials(region: str = "ap-southeast-1", sandbox: bool = False) -> tuple[str, str]:
    """Env vars first (local override), AWS Secrets Manager fallback."""
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
    """Load Binance Ed25519 API key + private key (real exec node only)."""
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


def validate_ed25519_private_key(pem_str: str) -> None:
    """Raise if the Ed25519 private key looks ENCRYPTED (unsupported)."""
    body = pem_str.strip()
    if "BEGIN" in body:
        lines = [ln for ln in body.splitlines() if "BEGIN" not in ln and "END" not in ln]
        body = "".join(lines)
    try:
        der = base64.b64decode(body)
    except Exception:
        return

    pbes2_oid = bytes([0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x05, 0x0D])
    if len(der) >= 2 and der[0] == 0x30 and der[1] >= 0x81 and pbes2_oid in der:
        raise RuntimeError(
            "The Ed25519 private key appears to be ENCRYPTED (password-protected) "
            "PKCS8. Binance/NautilusTrader only accept unencrypted Ed25519 PEM keys. "
            "Decrypt it first with: openssl pkey -in encrypted.pem -out decrypted.pem "
            "then store the decrypted PEM content back in AWS Secrets Manager."
        )


def warn_if_api_key_looks_like_raw_pem(api_key: str) -> None:
    if api_key.startswith(("MC", "-----BEGIN")):
        print(
            "⚠️ WARNING: the Ed25519 'public key' value looks like a raw PEM/DER "
            "public key blob, not a Binance-issued API key. Double check API Management.",
            file=sys.stderr,
        )


# -----------------------------------------------------------------------
# Node construction
# -----------------------------------------------------------------------


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
    via `loop.run_until_complete(...)`) — see original EDGETrader.py
    history for why a mismatched loop silently drops connect() tasks.
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


def register_sandbox_exec(node: TradingNode) -> None:
    node.add_exec_client_factory(BINANCE, SandboxLiveExecClientFactory)


def new_instrument_provider() -> InstrumentProviderConfig:
    return InstrumentProviderConfig(load_all=True)


# -----------------------------------------------------------------------
# SQS consumer helpers (single-target queue — no more per-message target
# looping; that's now handled upstream by the SNS filter policy)
# -----------------------------------------------------------------------

_sqs_client = None


def get_sqs_client():
    global _sqs_client
    if _sqs_client is None:
        _sqs_client = boto3.client("sqs", region_name=os.getenv("AWS_REGION", "ap-southeast-1"))
    return _sqs_client


async def receive_trade_events(sqs_client, queue_url: str, max_messages: int = 10, wait_seconds: int = 20) -> list[dict]:
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


def parse_event(msg: dict) -> Optional[Dict[str, Any]]:
    """Unwrap the (optional) SNS envelope and return the raw event dict, or
    None if the message body isn't parseable JSON."""
    try:
        body = json.loads(msg["Body"])
        if body.get("Type") == "Notification":
            return json.loads(body["Message"])
        return body
    except (json.JSONDecodeError, KeyError):
        return None


# -----------------------------------------------------------------------
# Heartbeat loop — replaces the old HTTP /health + /balance push updates.
# Runs alongside the node's own run_async(); writes one upserted row per
# (venue, target) so edge-api.py (a separate process with no reference to
# this node) can still report liveness + balance.
# -----------------------------------------------------------------------


async def heartbeat_loop(
    db: TradeEventsDB,
    node: TradingNode,
    target: str,
    get_balance: Optional[Any] = None,
    interval_seconds: float = 15.0,
) -> None:
    """
    `get_balance`, if given, is a zero-arg callable (sync or async)
    returning the current free USDT balance as a float. Balance errors are
    recorded in `detail` rather than raised — a stale/missing balance
    should never take down the heartbeat itself.
    """
    while True:
        is_running = bool(node.trader.is_running)
        balance: Optional[float] = None
        detail: Optional[str] = None
        if get_balance is not None:
            try:
                result = get_balance()
                if asyncio.iscoroutine(result):
                    result = await result
                balance = float(result)
            except Exception as e:
                detail = f"balance lookup failed: {e}"
        try:
            await db.write_heartbeat(VENUE, target, is_running, balance_usdt=balance, detail=detail)
        except Exception as e:
            print(f"⚠️ Failed to write heartbeat for target={target}: {e}", file=sys.stderr)
        await asyncio.sleep(interval_seconds)


async def cancel_requests_loop(
    db: TradeEventsDB,
    node: TradingNode,
    target: str,
    active_strategies: Dict[str, Any],
    poll_seconds: float = 5.0,
) -> None:
    """
    Polls cancel_requests for this process's target — edge-api.py (DB-only,
    no AWS access) writes rows here instead of calling strategy.request_cancel()
    directly, since it no longer shares process memory with the node.
    """
    while True:
        try:
            pending = await db.fetch_pending_cancel_requests(target)
        except Exception as e:
            print(f"⚠️ Failed to poll cancel_requests: {e}", file=sys.stderr)
            pending = []

        for req in pending:
            trade_id = req["trade_id"]
            strategy = active_strategies.get(trade_id)
            if strategy is None:
                print(f"⚠️ Cancel request for {trade_id} but no in-memory strategy (already closed?)", file=sys.stderr)
            else:
                try:
                    strategy.request_cancel()
                    print(f"🛑 Cancel requested (via edge-api) for {trade_id}", file=sys.stderr)
                except Exception as e:
                    print(f"❌ Failed to cancel {trade_id}: {e}", file=sys.stderr)
            await db.mark_cancel_request_processed(req["id"])

        await asyncio.sleep(poll_seconds)

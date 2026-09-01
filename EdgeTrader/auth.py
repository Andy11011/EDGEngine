"""
auth.py — credential loading from environment and AWS Secrets Manager.

This module provides functions to load Binance API keys (HMAC and Ed25519)
with a preference for environment variables (local development) and a fallback
to AWS Secrets Manager (production). It also validates Ed25519 private keys
to catch encrypted (password-protected) PEMs early.

All functions are synchronous and have no dependency on nautilus_trader or
asyncpg – they are pure helpers for node setup.
"""

from __future__ import annotations

import base64
import json
import os
import sys
from typing import Optional, Tuple

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:
    print("❌ boto3 not installed. Run: pip install boto3", file=sys.stderr)
    sys.exit(1)


# ---- HMAC credentials (standard API key/secret) ----

def load_credentials_from_aws(region: str = "ap-southeast-1", sandbox: bool = False) -> Tuple[str, str]:
    """Load Binance HMAC API key and secret from AWS Secrets Manager."""
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


def load_credentials_from_env(sandbox: bool = False) -> Optional[Tuple[str, str]]:
    """Load HMAC credentials from environment variables. Returns None if unset."""
    prefix = "BINANCE_SANDBOX" if sandbox else "BINANCE"
    api_key = os.getenv(f"{prefix}_API_KEY")
    api_secret = os.getenv(f"{prefix}_API_SECRET")
    if api_key and api_secret:
        return api_key, api_secret
    return None


def load_credentials(region: str = "ap-southeast-1", sandbox: bool = False) -> Tuple[str, str]:
    """Env vars first (local override), AWS Secrets Manager fallback."""
    env_creds = load_credentials_from_env(sandbox=sandbox)
    if env_creds is not None:
        print("✅ Credentials loaded from environment variables (local override)", file=sys.stderr)
        return env_creds
    return load_credentials_from_aws(region=region, sandbox=sandbox)


# ---- Ed25519 credentials (for live Binance execution) ----

def load_ed25519_credentials_from_aws(
    region: str = "ap-southeast-1",
    secret_name: str = "Binance_async_keys_Ed25519",
    public_key_field: str = "binance-api-public-key-ed25519",
    private_key_field: str = "binance-api-private-key-ed25519",
) -> Tuple[str, str]:
    """Load Binance Ed25519 API key (public) and private key from a single JSON secret."""
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


def load_ed25519_credentials_from_env() -> Optional[Tuple[str, str]]:
    """Load Ed25519 credentials from environment variables. Returns None if unset."""
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
) -> Tuple[str, str]:
    """Env vars first, AWS Secrets Manager fallback."""
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
    """
    Raise a clear RuntimeError if the Ed25519 private key looks encrypted (PKCS8
    EncryptedPrivateKeyInfo). Binance/NautilusTrader only accept unencrypted PEM.
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


def warn_if_api_key_looks_like_raw_pem(api_key: str) -> None:
    """Warn if the Ed25519 public key looks like a PEM/DER blob instead of a Binance-issued API key."""
    if api_key.startswith(("MC", "-----BEGIN")):
        print(
            "⚠️ WARNING: the Ed25519 'public key' value looks like a raw PEM/DER "
            "public key blob, not a Binance-issued API key. Double check API Management.",
            file=sys.stderr,
        )
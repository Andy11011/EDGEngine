"""
test_balances.py — checks the two balance-check endpoints on api.py:
/balance/virtual, /balance/mainnet.

SCOPE NOTE: this test assumes the CI container always has a real Binance
mainnet account connection configured (via the BINANCE_ED25519_* secrets
already present in buildspec.yml). There is no TRADING_MODE gate anymore:
  - /balance/virtual  is expected to return the configured virtual equity
    (DB-only, always available).
  - /balance/mainnet  is expected to return a real, nonzero balance from
    the live Binance mainnet account on every run.

Because this hits a real account with real funds on every push/PR that
triggers the smoke_tests job, make sure the key backing
BINANCE_ED25519_PRIVATE_KEY is minimally scoped (read-only / balance-only,
no withdrawal or trading permissions) before relying on this in the
default pipeline.
"""

import pytest
import requests

BASE_URL = "http://localhost:8000"

# Mirrors the seed row inserted by trades_db_async.py's CREATE TABLE
# IF NOT EXISTS / INSERT ... ON CONFLICT DO NOTHING for trades_config.
# Same caveat as in test_open_and_close.py: only holds against a
# freshly-seeded DB with untouched defaults.
DEFAULT_VIRTUAL_BALANCE_USDT = 500


def get_balance(source: str) -> dict:
    resp = requests.get(f"{BASE_URL}/balance/{source}", timeout=5)
    resp.raise_for_status()
    return resp.json()


def test_virtual_balance_available():
    """/balance/virtual is DB-only — should always report the configured
    virtual equity."""
    data = get_balance("virtual")
    assert data["source"] == "virtual"
    assert data["available"] is True, f"virtual balance not available: {data}"
    assert data["balance_usdt"] == pytest.approx(DEFAULT_VIRTUAL_BALANCE_USDT)


def test_mainnet_balance_returns_a_real_number():
    """/balance/mainnet should always return a real balance from the live
    Binance mainnet account connected via BINANCE_ED25519_*."""
    data = get_balance("mainnet")
    assert data["available"] is True, f"expected a real mainnet balance; got: {data}"
    assert isinstance(data["balance_usdt"], (int, float))
    assert data["balance_usdt"] >= 0

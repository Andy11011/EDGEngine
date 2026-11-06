"""
test_balances.py — checks the balance endpoints on main-api.py.

CI runs only the VIRTUAL node (no real node, no real-account keys), so:
  - /balance/virtual  must return the configured virtual equity (DB-only).
  - /balance/mainnet  has no real node to report from; it must answer 200
    with available=False rather than erroring.
"""

import pytest
import requests

BASE_URL = "http://localhost:8000"

# Seed row inserted by trades_db_async.py (trades_config); only holds against
# a freshly-seeded DB, which is what CI always has.
DEFAULT_VIRTUAL_BALANCE_USDT = 500


def get_balance(source: str) -> dict:
    resp = requests.get(f"{BASE_URL}/balance/{source}", timeout=5)
    resp.raise_for_status()
    return resp.json()


def test_virtual_balance_available():
    data = get_balance("virtual")
    assert data["source"] == "virtual"
    assert data["available"] is True, f"virtual balance not available: {data}"
    assert data["balance_usdt"] == pytest.approx(DEFAULT_VIRTUAL_BALANCE_USDT)


def test_mainnet_balance_unavailable_without_real_node():
    data = get_balance("mainnet")
    assert data["source"] == "mainnet"
    assert data["available"] is False, f"no real node runs in CI, yet got: {data}"

"""
test_balances.py — checks the three balance-check endpoints added to
api.py: /balance/virtual, /balance/testnet, /balance/mainnet.

IMPORTANT SCOPE NOTE: buildspec.yml runs the CI container with
TRADING_MODE=TEST1 (simulated exec, fed by Binance TESTNET data). Under
that mode:
  - /balance/virtual   IS expected to return a real number (DB-only, mode
    independent).
  - /balance/testnet   is expected to report "available: false" with a
    simulated-exec detail message — TEST1 never opens a real Binance
    account connection, even though its data feed happens to be TESTNET.
  - /balance/mainnet    is expected to report "available: false" with a
    wrong-environment detail message.

This test therefore verifies the three endpoints are wired up and degrade
correctly — it does NOT verify a real nonzero balance actually comes back
from Binance TESTNET or MAINNET. Doing that needs a container actually
running TEST3 (real orders on testnet, uses the BINANCE_SANDBOX_*/ED25519
secrets already present in buildspec.yml) or LIVE (real funds — see the
earlier discussion on gating this behind a manual/scheduled job with a
dedicated, minimally-scoped key rather than the default push/PR pipeline).
"""

import os

import pytest
import requests

BASE_URL = "http://localhost:8000"

# Mirrors the seed row inserted by trades_db_async.py's CREATE TABLE
# IF NOT EXISTS / INSERT ... ON CONFLICT DO NOTHING for trades_config.
# Same caveat as in test_open_and_close.py: only holds against a
# freshly-seeded DB with untouched defaults.
DEFAULT_VIRTUAL_BALANCE_USDT = 500

# What TRADING_MODE this CI run is using — buildspec.yml currently hardcodes
# TEST1. If that ever changes, override via env rather than editing this
# file, so the expectations below stay accurate.
TRADING_MODE = os.environ.get("TRADING_MODE", "TEST1").upper()


def get_balance(source: str) -> dict:
    resp = requests.get(f"{BASE_URL}/balance/{source}", timeout=5)
    resp.raise_for_status()
    return resp.json()


def test_virtual_balance_available():
    """/balance/virtual is mode-independent — should always report the
    configured virtual equity, regardless of TRADING_MODE."""
    data = get_balance("virtual")
    assert data["source"] == "virtual"
    assert data["available"] is True, f"virtual balance not available: {data}"
    assert data["balance_usdt"] == pytest.approx(DEFAULT_VIRTUAL_BALANCE_USDT)


@pytest.mark.skipif(
    TRADING_MODE not in ("TEST1", "TEST2"),
    reason="Only TEST1/TEST2 run simulated exec — this checks that /balance/testnet "
           "and /balance/mainnet correctly report 'not available' rather than a "
           "real number when there's no real account connected.",
)
def test_real_balance_endpoints_report_unavailable_in_simulated_mode():
    testnet = get_balance("testnet")
    mainnet = get_balance("mainnet")

    # Whichever of the two matches this instance's data-feed environment
    # should report the "simulated-exec, no real account" reason; the other
    # should report the "wrong environment" reason. Rather than hardcode
    # which is which (that depends on TEST1 vs TEST2), just assert both are
    # unavailable and, between them, that both distinct reasons show up.
    assert testnet["available"] is False
    assert mainnet["available"] is False
    reasons = {testnet["detail"], mainnet["detail"]}
    assert any("simulated-exec" in r for r in reasons), f"expected a simulated-exec detail; got: {reasons}"
    assert any("not MAINNET" in r or "not TESTNET" in r for r in reasons), (
        f"expected a wrong-environment detail; got: {reasons}"
    )


@pytest.mark.skipif(
    TRADING_MODE not in ("TEST3", "LIVE"),
    reason="Only TEST3/LIVE connect a real Binance account, so only they can "
           "return an actual nonzero balance from /balance/testnet or /balance/mainnet.",
)
def test_matching_real_balance_endpoint_returns_a_number():
    source = "testnet" if TRADING_MODE == "TEST3" else "mainnet"
    other_source = "mainnet" if source == "testnet" else "testnet"

    data = get_balance(source)
    assert data["available"] is True, f"expected a real {source} balance; got: {data}"
    assert isinstance(data["balance_usdt"], (int, float))
    assert data["balance_usdt"] >= 0

    other = get_balance(other_source)
    assert other["available"] is False, f"expected {other_source} to be unavailable in {TRADING_MODE}; got: {other}"

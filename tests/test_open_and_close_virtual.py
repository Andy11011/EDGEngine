"""
test_open_and_close_virtual.py — end-to-end lifecycle test for the VIRTUAL
node: publish an Open trade-event via SNS (routed to the virtual node's own
SQS queue via the 'venue' message attribute), confirm the strategy starts,
reaches AWAITING_FILL, is visible via /active_trades, then publish a Cancel
and confirm clean teardown (strategy removed, Cancelled event logged, trade
gone from /active_trades) with no regression of the double-stop bug.

WHAT THIS TEST CHECKS, END TO END:
  1. The app is healthy (Postgres connected, virtual node's Nautilus trader
     running, virtual node's own SQS connectivity reported ok) before doing
     anything else.
  2. Publishing an Open message via SNS with venue=binance-virtual-mainnet
     reaches the virtual node (proves the SNS topic's subscription filter
     policy is correctly routing to the virtual queue).
  3. The node builds the expected trade_id (ticker + stripped timestamp +
     '_virtual' target suffix), starts a TradeStrategy for it, and the
     entry order is accepted (awaiting fill).
  4. /active_trades reflects the new trade with the correct instrument,
     side, entry/SL/TP prices, and a computed size matching
     calculate_position_size() run against the known default
     trades_config (risk_ratio=0.001, virtual_balance_usdt=500).
  5. Publishing a Cancel message for the same ticker correctly finds and
     cancels the active trade, the strategy is cleanly removed from
     active_strategies (regression check: this used to raise
     InvalidStateTrigger('STOPPED -> STOP') from a double-stop bug —
     see _finalize_and_stop()'s docstring in trade_strategy.py), a
     Cancelled event is logged, and the trade disappears from
     /active_trades.
  6. The app is still healthy afterwards.

WHAT THIS TEST DOES NOT COVER: fills, protection orders (SL/TP), or the
Filled/Protected/Closed path — this is the Open->Cancel lifecycle only.
See build_fast_test_trade.py for constructing a message likely to reach
a real fill/SL/TP quickly if you need to exercise that path too.
"""

import json
import os
import subprocess
import time
from datetime import datetime, timezone

import boto3
import pytest
import requests

BASE_URL = "http://localhost:8000"
HEALTH_URL = f"{BASE_URL}/health"
ACTIVE_TRADES_URL = f"{BASE_URL}/active_trades"

AWS_REGION = os.environ["AWS_REGION"]
# NOTE: this is a new env var this test needs — buildspec.yml currently
# only passes SQS_TRADE_EVENTS_QUEUE_URL_VIRTUAL/_REAL (direct queue URLs),
# not a topic ARN. Since publishing now goes through SNS (see RUNBOOK.md's
# "Testing with AWS SNS" section), buildspec.yml's env-vars-for-codebuild
# list needs SNS_TRADE_EVENTS_TOPIC_ARN added, sourced the same way as the
# other AWS_* vars.
TOPIC_ARN = os.environ["SNS_TRADE_EVENTS_TOPIC_ARN"]

TARGET = "virtual"
VENUE_ATTR_VALUE = "binance-virtual-mainnet"  # must match the SNS subscription filter policy exactly
CONTAINER_NAME = "binance-virtual-mainnet-node"

TEST_TICKER = "ATMUSDT.BINANCE"
TEST_SIDE = "BUY"
TEST_EP = 1.598
TEST_SL = 1.587
TEST_TP = 1.618

# Mirrors the seed row inserted by trades_db_async.py's CREATE TABLE
# IF NOT EXISTS / INSERT ... ON CONFLICT DO NOTHING for trades_config.
# This test only holds against a freshly-seeded DB with untouched
# defaults — if trades_config has been tuned away from these, update
# these two constants (or better, fetch them from the DB/API) rather
# than the assertion below.
DEFAULT_RISK_RATIO = 0.001
DEFAULT_VIRTUAL_BALANCE_USDT = 500


def expected_position_size(equity, risk_ratio, entry_price, stop_loss_price):
    """Self-contained copy of position_sizing.calculate_position_size()'s
    math, kept deliberately independent of the source tree (no sys.path
    tricks, no import of the app package) so this test doesn't care where
    the app's modules live on disk. If the real formula changes, update
    this copy too — see position_sizing.py for the authoritative version
    and its unit tests.
    """
    risk = abs(entry_price - stop_loss_price)
    risk_amount = equity * risk_ratio
    position_size = risk_amount / risk
    max_position_size = 0.99 * (equity / entry_price)
    return min(position_size, max_position_size)


def wait_for_healthy(timeout=120, interval=2):
    """Waits for postgres connected, the virtual node's trader running, and
    the virtual node's own SQS connectivity ok — deliberately does NOT wait
    on the top-level overall 'status'=='ok', since that also depends on the
    real node (a separate, independently-funded account this test doesn't
    touch) and coupling a virtual-only test to real-node health would make
    it fail for reasons outside this test's scope."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.get(HEALTH_URL, timeout=5)
            if resp.status_code == 200:
                deps = resp.json().get("dependencies", {})
                if (
                    deps.get("postgres", {}).get("status") == "connected"
                    and deps.get("nautilus_virtual", {}).get("trader_running") is True
                    and deps.get("sqs_virtual", {}).get("status") == "connected"
                ):
                    return True
        except Exception:
            pass
        time.sleep(interval)
    return False


def get_active_trades():
    resp = requests.get(ACTIVE_TRADES_URL, timeout=5)
    resp.raise_for_status()
    return resp.json()


def find_active_trade(trade_id):
    return next((t for t in get_active_trades() if t["trade_id"] == trade_id), None)


def wait_for_trade_state(trade_id, state, timeout=30, interval=2):
    start = time.time()
    while time.time() - start < timeout:
        trade = find_active_trade(trade_id)
        if trade and trade["state"] == state:
            return trade
        time.sleep(interval)
    return None


def wait_for_trade_gone(trade_id, timeout=30, interval=2):
    start = time.time()
    while time.time() - start < timeout:
        if find_active_trade(trade_id) is None:
            return True
        time.sleep(interval)
    return False


def get_container_logs():
    result = subprocess.run(
        ["docker", "logs", CONTAINER_NAME],
        capture_output=True,
        text=True,
        check=False,
    )
    return (result.stdout or "") + (result.stderr or "")


def wait_for_logs(substrings, timeout=30, interval=2):
    """Wait until every string in `substrings` shows up somewhere in the
    container logs. Returns the logs once found, or the last logs seen if
    it times out (so callers can assert with a useful message)."""
    start = time.time()
    logs = ""
    while time.time() - start < timeout:
        logs = get_container_logs()
        if all(s in logs for s in substrings):
            return logs
        time.sleep(interval)
    return logs


def iso_ms_now():
    """UTC timestamp like '2026-08-07T10:00:00.123Z' — the format the node
    expects for occurred_at, and uses (along with ticker and target) to
    derive trade_id."""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


def expected_trade_id(ticker, occurred_at, target):
    """Mirrors binance_virtual_mainnet_node.py's deterministic trade_id
    generation:
        trade_id = ticker + "_" + occurred_at (':','.','-','Z' stripped) + "_" + target
    The '_<target>' suffix is required — without it this won't match what
    the node actually generates, since the same ticker/timestamp can be
    open on both virtual and real simultaneously and needs distinct ids.
    """
    stripped = occurred_at.replace(":", "").replace(".", "").replace("-", "").replace("Z", "")
    return f"{ticker}_{stripped}_{target}"


def publish_event(body):
    """Publish to the SNS topic with the venue attribute set to this
    target's value, so the topic's subscription filter policy routes it
    to the virtual node's SQS queue. See RUNBOOK.md's "Testing with AWS
    SNS" section — this is the boto3 equivalent of the aws sns publish
    CLI examples there."""
    sns = boto3.client("sns", region_name=AWS_REGION)
    resp = sns.publish(
        TopicArn=TOPIC_ARN,
        Message=json.dumps(body),
        MessageAttributes={
            "venue": {"DataType": "String", "StringValue": VENUE_ATTR_VALUE}
        },
    )
    return resp["MessageId"]


def test_open_then_cancel_trade_lifecycle_virtual():
    assert wait_for_healthy(), "Virtual node did not become healthy before starting the trade lifecycle test"

    occurred_at_open = iso_ms_now()
    trade_id = expected_trade_id(TEST_TICKER, occurred_at_open, TARGET)

    # ---- open ----
    publish_event({
        "ticker": TEST_TICKER,
        "event_type": "open",
        "occurred_at": occurred_at_open,
        "side": TEST_SIDE,
        "ep": TEST_EP,
        "sl": TEST_SL,
        "tp": TEST_TP,
    })

    logs = wait_for_logs([
        f"Started strategy for trade {trade_id}",
        f"TradeStrategy started for {trade_id}",
        f"Entry order accepted, awaiting fill (trade={trade_id})",
        f"Logged Opened event for {trade_id} (target={TARGET})",
    ])
    assert f"Started strategy for trade {trade_id}" in logs, \
        f"Strategy for {trade_id} never started; logs:\n{logs[-3000:]}"

    resp = requests.get(HEALTH_URL, timeout=5)
    assert resp.status_code == 200
    assert resp.json()["active_trades_count"] >= 1

    trade = wait_for_trade_state(trade_id, "AWAITING_FILL")
    assert trade is not None, f"{trade_id} never reached AWAITING_FILL in /active_trades"
    assert trade["instrument"] == TEST_TICKER
    assert trade["side"] == TEST_SIDE
    # Size is computed by the node from trades_config (risk_ratio /
    # virtual_balance_usdt) and entry/stop via calculate_position_size().
    # Duplicate that math here (see expected_position_size() above) using
    # the known default config so we can assert an exact expected size.
    expected_size = expected_position_size(
        equity=DEFAULT_VIRTUAL_BALANCE_USDT,
        risk_ratio=DEFAULT_RISK_RATIO,
        entry_price=TEST_EP,
        stop_loss_price=TEST_SL,
    )
    assert trade["size"] == pytest.approx(expected_size)
    assert trade["entry_price"] == pytest.approx(TEST_EP)
    assert trade["sl_price"] == pytest.approx(TEST_SL)
    assert trade["tp_price"] == pytest.approx(TEST_TP)

    # ---- small pause before cancelling, like a real caller would ----
    time.sleep(3)

    # ---- cancel ----
    publish_event({
        "ticker": TEST_TICKER,
        "event_type": "cancel",
        "occurred_at": iso_ms_now(),
    })

    entry_client_order_id = f"edge-{trade_id}-entry"
    logs = wait_for_logs([
        f"Cancel requested for {trade_id}",
        f"Cancelling entry order for {trade_id}",
        f"Order canceled: {entry_client_order_id}",
        f"Trade {trade_id} reached terminal state",
        f"TradeStrategy stopped for {trade_id}",
        f"Removed strategy for trade {trade_id}",
        f"Logged Cancelled event for {trade_id} (target={TARGET})",
    ])
    assert f"Cancel requested for {trade_id}" in logs, \
        f"Cancel never found an active trade for {trade_id}; logs:\n{logs[-3000:]}"
    assert f"Removed strategy for trade {trade_id}" in logs, \
        f"Strategy for {trade_id} was never removed; logs:\n{logs[-3000:]}"

    # Regression check for the double-stop bug: _finalize_and_stop() used to
    # call self.stop() again after the close_callback's remove_strategy()
    # had already stopped the strategy, raising
    # InvalidStateTrigger('STOPPED -> STOP'). Make sure it stays fixed.
    assert "InvalidStateTrigger" not in logs, f"InvalidStateTrigger regression detected; logs:\n{logs[-3000:]}"

    assert wait_for_trade_gone(trade_id), f"{trade_id} still in /active_trades after cancel"

    resp = requests.get(HEALTH_URL, timeout=5)
    assert resp.status_code == 200
    data = resp.json()
    assert data["dependencies"]["postgres"]["status"] == "connected"
    assert data["dependencies"]["sqs_virtual"]["status"] == "connected"
    assert data["dependencies"]["nautilus_virtual"]["trader_running"] is True

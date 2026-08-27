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
QUEUE_URL = os.environ["SQS_TRADE_EVENTS_QUEUE_URL"]
CONTAINER_NAME = "edgetrader-ci"

TEST_TICKER = "ATMUSDT.BINANCE"
TEST_SIDE = "BUY"
TEST_SIZE = 45.455
TEST_EP = 1.598
TEST_SL = 1.587
TEST_TP = 1.618


def wait_for_healthy(timeout=120, interval=2):
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.get(HEALTH_URL, timeout=5)
            if resp.status_code == 200 and resp.json().get("status") == "ok":
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
    """UTC timestamp like '2026-08-07T10:00:00.123Z' — the format
    EDGETrader.py expects for occurred_at, and uses to derive trade_id."""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


def expected_trade_id(ticker, occurred_at):
    """Mirrors EDGETrader.py's deterministic trade_id generation:
    trade_id = ticker + "_" + occurred_at with ':', '.', '-', 'Z' stripped.
    """
    stripped = occurred_at.replace(":", "").replace(".", "").replace("-", "").replace("Z", "")
    return f"{ticker}_{stripped}"


def send_event(body):
    sqs = boto3.client("sqs", region_name=AWS_REGION)
    resp = sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(body))
    return resp["MessageId"]


def test_open_then_cancel_trade_lifecycle():
    assert wait_for_healthy(), "App did not become healthy before starting the trade lifecycle test"

    occurred_at_open = iso_ms_now()
    trade_id = expected_trade_id(TEST_TICKER, occurred_at_open)

    # ---- open ----
    send_event({
        "ticker": TEST_TICKER,
        "event_type": "open",
        "occurred_at": occurred_at_open,
        "side": TEST_SIDE,
        "size": TEST_SIZE,
        "ep": TEST_EP,
        "sl": TEST_SL,
        "tp": TEST_TP,
    })

    logs = wait_for_logs([
        f"Generated trade_id={trade_id}",
        "Registered strategy in active_strategies",
        f"TradeStrategy started for {trade_id}",
        f"Entry order accepted, awaiting fill (trade={trade_id})",
    ])
    assert f"Generated trade_id={trade_id}" in logs, f"Strategy for {trade_id} never started; logs:\n{logs[-3000:]}"

    resp = requests.get(HEALTH_URL, timeout=5)
    assert resp.status_code == 200
    assert resp.json()["active_trades_count"] >= 1

    trade = wait_for_trade_state(trade_id, "AWAITING_FILL")
    assert trade is not None, f"{trade_id} never reached AWAITING_FILL in /active_trades"
    assert trade["instrument"] == TEST_TICKER
    assert trade["side"] == TEST_SIDE
    assert trade["size"] == pytest.approx(TEST_SIZE)
    assert trade["entry_price"] == pytest.approx(TEST_EP)
    assert trade["sl_price"] == pytest.approx(TEST_SL)
    assert trade["tp_price"] == pytest.approx(TEST_TP)

    # ---- small pause before cancelling, like a real caller would ----
    time.sleep(3)

    # ---- cancel ----
    send_event({
        "ticker": TEST_TICKER,
        "event_type": "cancel",
        "occurred_at": iso_ms_now(),
    })

    logs = wait_for_logs([
        f"Looking up active trade for ticker={TEST_TICKER}",
        f"Found active_trade_id={trade_id} for ticker={TEST_TICKER}",
        f"Cancelling entry order for {trade_id}",
        f"Trade {trade_id} reached terminal state",
        f"TradeStrategy stopped for {trade_id}",
        f"Removed strategy for trade {trade_id}",
        f"Logged Cancelled event for {trade_id}",
    ])
    assert f"Found active_trade_id={trade_id}" in logs, f"Cancel never found {trade_id}; logs:\n{logs[-3000:]}"
    assert f"Removed strategy for trade {trade_id}" in logs, f"Strategy for {trade_id} was never removed; logs:\n{logs[-3000:]}"

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
    assert data["dependencies"]["sqs"]["status"] == "connected"
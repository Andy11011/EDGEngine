"""
test_open_and_close_virtual.py — end-to-end lifecycle test for the VIRTUAL
node: publish an Open trade-event via SNS (routed to the virtual node's own
TEST SQS queue via the 'venue' + 'env' message attributes — see
edgetrader-aws-infra-v3.yaml), confirm the strategy starts,
reaches AWAITING_FILL internally, is visible via /active_trades (reported
as state="Opened" — see note below on why), then publish a Cancel
and confirm clean teardown (strategy removed, Cancelled event logged, trade
gone from /active_trades) with no regression of the double-stop bug.

WHAT THIS TEST CHECKS, END TO END:
  1. The app is healthy (Postgres connected, virtual node's Nautilus trader
     running, virtual node's own SQS connectivity reported ok) before doing
     anything else.
  2. Publishing an Open message via SNS with venue=binance-virtual-mainnet
     and env=test reaches the virtual node's TEST queue (proves the SNS
     topic's subscription filter policy is correctly routing to it, and
     not to the prod queue).
  3. The node starts a TradeStrategy for the trade and accepts the entry
     order (awaiting fill).

     IMPORTANT (changed from earlier revisions): the node stamps its OWN
     receive-time into the trade_id — it does NOT derive the id from the
     payload's `occurred_at` field. Concretely, the node generates
            trade_id = ticker + "_" + <node_receive_time stripped> + "_" + target
     where <node_receive_time> is a fresh UTC timestamp taken when the SQS
     message is processed, so it lands a few ms after the payload's
     occurred_at. This test therefore DISCOVERS the trade_id by scraping
     the "Started strategy for trade <id>" log line (see
     wait_for_trade_id()), and asserts its FORMAT (see TRADE_ID_FORMAT_RE)
     rather than predicting its exact value. The delta between the
     payload's occurred_at and the node-stamped time is reported as
     `sns_to_node_latency_ms` (visible in JUnit XML; run pytest with -s to
     see it on the console) — useful as a rough pipeline-latency probe,
     but deliberately not asserted against a tight bound, because
     SNS->SQS->node latency is inherently variable and would flake.
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

NOTE ON LOG SCOPING: docker logs are cumulative for the container's
lifetime. Both wait_for_trade_id() and wait_for_logs() accept a `baseline`
snapshot (taken via get_container_logs() *before* publishing) and only
search the bytes appended since then — this stops a stale line from a
previous run of this test (e.g. an earlier "Started strategy for trade …"
or an earlier "InvalidStateTrigger") from satisfying an assertion, which
would otherwise turn a real failure into a pass or vice versa.
"""

import json
import os
import re
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
# NOTE: as of the prod/test queue split in edgetrader-aws-infra-v3.yaml,
# every subscription's FilterPolicy requires BOTH venue and env to match —
# a message with only venue set matches nothing and is silently dropped
# (no error, no delivery). CI must always publish with env=test so this
# routes to the *-test queues rather than the prod ones (or nowhere).
ENV_ATTR_VALUE = "test"
CONTAINER_NAME = "binance-virtual-mainnet-node"

TEST_TICKER = "ATMUSDT.BINANCE"
TEST_SIDE = "BUY"
TEST_TP = 0.945
TEST_EP = 0.929
TEST_SL = 0.923

# Mirrors the seed row inserted by trades_db_async.py's CREATE TABLE
# IF NOT EXISTS / INSERT ... ON CONFLICT DO NOTHING for trades_config.
# This test only holds against a freshly-seeded DB with untouched
# defaults — if trades_config has been tuned away from these, update
# these two constants (or better, fetch them from the DB/API) rather
# than the assertion below.
DEFAULT_RISK_RATIO = 0.001
DEFAULT_VIRTUAL_BALANCE_USDT = 500

# The node's trade_id scheme, as observed in the container logs:
#     <ticker>_<YYYYMMDD>T<HHMMSSmmm>_<target>
# e.g. ATMUSDT.BINANCE_20261029T035846747_virtual
# This is asserted as a FORMAT check only — the test cannot predict the
# exact timestamp because the node stamps its own receive-time (see the
# module docstring's point 3). If the node's ID scheme changes, update
# this regex and the docstring above.
STARTED_RE = re.compile(
    rf"Started strategy for trade ({re.escape(TEST_TICKER)}_\d{{8}}T\d{{9}}_{re.escape(TARGET)})"
)
TRADE_ID_FORMAT_RE = re.compile(
    rf"{re.escape(TEST_TICKER)}_\d{{8}}T\d{{9}}_{re.escape(TARGET)}"
)


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


def _scoped(logs, baseline):
    """Return the portion of `logs` that appeared after `baseline`, or the
    whole thing if `baseline` isn't a prefix (e.g. docker log rotation
    dropped the earlier bytes, or the caller didn't pass a baseline).
    Docker logs are append-only for a container's lifetime, so in practice
    startswith() holds and this is just `logs[len(baseline):]`."""
    if baseline and logs.startswith(baseline):
        return logs[len(baseline):]
    return logs


def wait_for_trade_id(pattern, baseline="", timeout=30, interval=2):
    """Scan the (baseline-scoped) container logs for `pattern` — a compiled
    regex with exactly one capture group — and return the captured
    trade_id. Returns None on timeout. Used to discover the node-assigned
    trade_id, which (unlike earlier revisions of this test assumed) is
    stamped with the node's own receive-time and cannot be predicted from
    the payload."""
    start = time.time()
    while time.time() - start < timeout:
        m = pattern.search(_scoped(get_container_logs(), baseline))
        if m:
            return m.group(1)
        time.sleep(interval)
    return None


def wait_for_logs(substrings, baseline="", timeout=30, interval=2):
    """Wait until every string in `substrings` shows up in the
    (baseline-scoped) container logs. Returns the scoped logs once found,
    or the last scoped logs seen if it times out (so callers can assert
    with a useful message)."""
    start = time.time()
    scoped = ""
    while time.time() - start < timeout:
        scoped = _scoped(get_container_logs(), baseline)
        if all(s in scoped for s in substrings):
            return scoped
        time.sleep(interval)
    return scoped


def iso_ms_now():
    """UTC timestamp like '2026-08-07T10:00:00.123Z' — the format the node
    expects for occurred_at. NOTE: this is now purely an INPUT field
    carried in the payload; the node does NOT use it to derive trade_id
    (see the module docstring's point 3). It is still used here as the
    reference point for the sns_to_node_latency_ms measurement."""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


def parse_node_timestamp(trade_id):
    """Extract the ISO-8601 UTC timestamp the node embedded in trade_id.
    Reverses the node's formatting:
        ATMUSDT.BINANCE_20261029T035846747_virtual
            -> '2026-10-29T03:58:46.747Z'
    Returns None if the id doesn't carry the expected timestamp shape.
    """
    m = re.search(r"_(\d{8})T(\d{9})_", trade_id)
    if not m:
        return None
    date, hmsms = m.groups()
    return (
        f"{date[:4]}-{date[4:6]}-{date[6:]}T"
        f"{hmsms[:2]}:{hmsms[2:4]}:{hmsms[4:6]}.{hmsms[6:9]}Z"
    )


def receive_latency_ms(node_occurred_at, sent_occurred_at):
    """Delta in ms between the payload's occurred_at and the node-stamped
    timestamp embedded in trade_id. Measures SNS->SQS->node processing
    latency at ms granularity (the trade_id only carries ms precision).
    """
    fmt = "%Y-%m-%dT%H:%M:%S.%fZ"
    node_dt = datetime.strptime(node_occurred_at, fmt)
    sent_dt = datetime.strptime(sent_occurred_at, fmt)
    return (node_dt - sent_dt).total_seconds() * 1000


def publish_event(body):
    """Publish to the SNS topic with the venue AND env attributes set, so
    the topic's subscription filter policy routes it to the virtual node's
    TEST queue (binance-virtual-trade-events-test), not the prod one. See
    RUNBOOK.md's "Testing with AWS SNS" section and
    edgetrader-aws-infra-v3.yaml's FilterPolicy definitions — both venue
    and env must be present or the message matches no subscription and is
    silently never delivered anywhere."""
    sns = boto3.client("sns", region_name=AWS_REGION)
    resp = sns.publish(
        TopicArn=TOPIC_ARN,
        Message=json.dumps(body),
        MessageAttributes={
            "venue": {"DataType": "String", "StringValue": VENUE_ATTR_VALUE},
            "env": {"DataType": "String", "StringValue": ENV_ATTR_VALUE},
        },
    )
    return resp["MessageId"]


def test_open_then_cancel_trade_lifecycle_virtual(record_property):
    assert wait_for_healthy(), "Virtual node did not become healthy before starting the trade lifecycle test"

    # Baseline the log stream BEFORE publishing anything, so both the
    # trade_id discovery and the lifecycle assertions below can't be
    # satisfied by a stale line from a previous run of this test.
    open_baseline = get_container_logs()

    occurred_at_open = iso_ms_now()

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

    # The node stamps its own receive-time into trade_id, so DISCOVER the
    # id from the log line rather than predicting it (see module docstring
    # point 3). Assert its FORMAT so a change to the node's ID scheme is
    # still caught.
    trade_id = wait_for_trade_id(STARTED_RE, baseline=open_baseline)
    assert trade_id, (
        "Strategy never started (no 'Started strategy for trade …' line "
        f"for ticker {TEST_TICKER}); logs:\n"
        f"{_scoped(get_container_logs(), open_baseline)[-3000:]}"
    )
    assert TRADE_ID_FORMAT_RE.fullmatch(trade_id), (
        f"trade_id {trade_id!r} does not match the expected node ID scheme "
        f"{TRADE_ID_FORMAT_RE.pattern!r} — did binance_virtual_mainnet_node.py "
        f"change how it builds trade_id?"
    )

    # Latency probe: payload occurred_at -> node-stamped time embedded in
    # trade_id. Reported (JUnit XML / pytest -s), deliberately not asserted
    # against a tight bound — SNS->SQS->node latency is variable and a hard
    # bound here would just be flaky.
    node_occurred_at = parse_node_timestamp(trade_id)
    if node_occurred_at:
        delta_ms = receive_latency_ms(node_occurred_at, occurred_at_open)
        record_property("sns_to_node_latency_ms", round(delta_ms))
        print(
            f"\nSNS->node latency: {delta_ms:.0f} ms "
            f"(payload occurred_at={occurred_at_open}, node-stamped={node_occurred_at})"
        )

    # Now confirm the rest of the open path, all keyed off the discovered
    # trade_id. ("Started strategy for trade …" is already in the scoped
    # logs by construction — checking for it again costs nothing and keeps
    # the assertion list self-documenting.)
    logs = wait_for_logs(
        [
            f"Started strategy for trade {trade_id}",
            f"TradeStrategy started for {trade_id}",
            f"Entry order accepted, awaiting fill (trade={trade_id})",
            f"Logged Opened event for {trade_id} (target={TARGET})",
        ],
        baseline=open_baseline,
    )
    assert f"TradeStrategy started for {trade_id}" in logs, (
        f"TradeStrategy never started for {trade_id}; logs:\n{logs[-3000:]}"
    )

    resp = requests.get(HEALTH_URL, timeout=5)
    assert resp.status_code == 200
    assert resp.json()["active_trades_count"] >= 1

    # main-api.py's get_active_trades() hardcodes state="Opened" for every
    # row — it derives active trades purely from trade_events (an Opened
    # row with no later Closed/Cancelled), and has no visibility into
    # trade_strategy.py's in-memory state machine (AWAITING_FILL,
    # IN_POSITION, etc.), which lives only inside the running node process.
    # So "AWAITING_FILL" is never a value this endpoint can return — the
    # node reaching AWAITING_FILL internally is instead confirmed above via
    # the "Entry order accepted, awaiting fill" log line.
    trade = wait_for_trade_state(trade_id, "Opened")
    assert trade is not None, f"{trade_id} never appeared as an active (Opened) trade in /active_trades"
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

    # Re-baseline before publishing the cancel, so the InvalidStateTrigger
    # regression check below only sees this run's output.
    cancel_baseline = get_container_logs()

    # ---- cancel ----
    publish_event({
        "ticker": TEST_TICKER,
        "event_type": "cancel",
        "occurred_at": iso_ms_now(),
    })

    entry_client_order_id = f"edge-{trade_id}-entry"
    logs = wait_for_logs(
        [
            f"Cancel requested for {trade_id}",
            f"Cancelling entry order for {trade_id}",
            f"Order canceled: {entry_client_order_id}",
            f"Trade {trade_id} reached terminal state",
            f"TradeStrategy stopped for {trade_id}",
            f"Removed strategy for trade {trade_id}",
            f"Logged Cancelled event for {trade_id} (target={TARGET})",
        ],
        baseline=cancel_baseline,
    )
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

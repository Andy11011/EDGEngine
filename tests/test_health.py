import time
import requests
import pytest

HEALTH_URL = "http://localhost:8000/health"

def wait_for_healthy(timeout=120, interval=2):
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.get(HEALTH_URL, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                # We want overall "ok" – all dependencies connected and trader running
                if data.get("status") == "ok":
                    return True
        except Exception:
            pass
        time.sleep(interval)
    return False

def test_health_ok():
    assert wait_for_healthy(), "Health endpoint did not become 'ok' within timeout"

def test_health_structure():
    resp = requests.get(HEALTH_URL)
    assert resp.status_code == 200
    data = resp.json()
    assert "status" in data
    assert "dependencies" in data
    deps = data["dependencies"]
    assert "postgres" in deps
    assert "nautilus_real" in deps
    assert "nautilus_virtual" in deps
    assert "sqs_real" in deps
    assert "sqs_virtual" in deps


def test_health_sqs_connected():
    """Both nodes' own SQS connectivity (written via their heartbeat, see
    sqs.get_sqs_status / common_tasks.heartbeat_loop) should report
    'connected' once the pipeline is healthy."""
    resp = requests.get(HEALTH_URL)
    assert resp.status_code == 200
    deps = resp.json()["dependencies"]

    sqs_real = deps["sqs_real"]
    sqs_virtual = deps["sqs_virtual"]

    assert sqs_real["status"] == "connected", f"sqs_real not connected: {sqs_real}"
    assert sqs_virtual["status"] == "connected", f"sqs_virtual not connected: {sqs_virtual}"

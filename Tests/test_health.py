import time
import requests
import pytest

HEALTH_URL = "http://localhost:8000/health"

def wait_for_healthy(timeout=120, interval=2):
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.get(HEALTH_URL, timeout=5)
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
    assert "sqs" in deps
    assert "nautilus" in deps
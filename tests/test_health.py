import time
import requests

HEALTH_URL = "http://localhost:8000/health"


def wait_for_healthy(timeout=120, interval=2):
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.get(HEALTH_URL, timeout=10)
            if resp.status_code == 200:
                # "ok" = postgres up and AT LEAST ONE node healthy (trader running AND the
                # trade-events queue reachable)
                if resp.json().get("status") == "ok":
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
    assert "healthy_nodes" in data
    assert "dependencies" in data
    deps = data["dependencies"]
    # There is ONE trade-events queue shared by the nodes, so ONE "sqs" dependency
    # (the old per-node sqs_real / sqs_virtual entries are gone).
    for key in ("postgres", "nautilus_real", "nautilus_virtual", "sqs"):
        assert key in deps, f"missing dependency {key!r}: {deps}"
    for gone in ("sqs_real", "sqs_virtual"):
        assert gone not in deps, f"{gone!r} should no longer be reported: {deps}"


def test_health_virtual_node_connected():
    """CI runs only the virtual node (no real node / real keys), so the virtual
    node must be running and the shared trade-events queue (ElasticMQ) connected.
    The real node is expected to be absent and must not make the overall status
    degraded."""
    data = requests.get(HEALTH_URL).json()
    deps = data["dependencies"]

    assert deps["nautilus_virtual"]["trader_running"] is True, deps["nautilus_virtual"]
    assert deps["sqs"]["status"] == "connected", deps["sqs"]
    assert "virtual" in data["healthy_nodes"], data["healthy_nodes"]
    # real node absent in CI: not running, and that is fine
    assert deps["nautilus_real"].get("trader_running") is not True, deps["nautilus_real"]
    assert "real" not in data["healthy_nodes"], data["healthy_nodes"]
    assert data["status"] == "ok"

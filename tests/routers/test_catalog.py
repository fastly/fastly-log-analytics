from fastapi.testclient import TestClient

from backend.main import app

client = TestClient(app)


def test_get_catalog():
    response = client.get("/api/log-fields/catalog")
    assert response.status_code == 200
    data = response.json()

    # Check groups
    groups = data["groups"]
    group_ids = [g["id"] for g in groups]
    assert "METRICS" in group_ids

    # Check fields
    fields = data["fields"]
    field_ids = [f["id"] for f in fields]

    # Required metrics
    required_metrics = [
        "requests",
        "hit_rate",
        "5xx",
        "4xx",
        "p50_latency",
        "p95_latency",
        "p99_latency",
        "throughput",
        "req_size",
        "ttfb_ms",
    ]
    for rm in required_metrics:
        assert rm in field_ids, f"Metric {rm} missing from catalog"
        # Verify group
        field = next(f for f in fields if f["id"] == rm)
        assert field["group"] == "METRICS"

    # Check metadata for hit_rate
    hit_rate = next(f for f in fields if f["id"] == "hit_rate")
    assert hit_rate["formatter"] == "percent"
    assert hit_rate["unit"] == "%"

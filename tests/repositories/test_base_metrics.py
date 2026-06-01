from backend.repositories._base import CANONICAL_METRICS


def test_canonical_metrics_exist():
    assert "hit_rate" in CANONICAL_METRICS
    assert "requests" in CANONICAL_METRICS
    assert "avg_ttfb" in CANONICAL_METRICS
    assert "p95_ttfb" in CANONICAL_METRICS
    assert "throughput" in CANONICAL_METRICS
    assert "req_size" in CANONICAL_METRICS
    assert "ttfb_ms" in CANONICAL_METRICS

    # Verify SQL snippets for units and rounding
    assert "AVG(ttfb) * 1000.0" in CANONICAL_METRICS["avg_ttfb"]
    assert "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ttfb) * 1000.0" in CANONICAL_METRICS["p95_ttfb"]
    assert "ROUND(" in CANONICAL_METRICS["avg_ttfb"]
    assert "ROUND(" in CANONICAL_METRICS["p95_ttfb"]
    assert "ROUND(" in CANONICAL_METRICS["throughput"]
    assert "ROUND(" in CANONICAL_METRICS["req_size"]
    assert "ROUND(" in CANONICAL_METRICS["ttfb_ms"]

    # Verify throughput components
    assert "resp_bytes_col" in CANONICAL_METRICS["throughput"]
    assert "elapsed_col" in CANONICAL_METRICS["throughput"]
    assert "* 1e6" in CANONICAL_METRICS["throughput"]

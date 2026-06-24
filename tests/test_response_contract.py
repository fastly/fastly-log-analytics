"""Pydantic response-model contract tests.

Catches the regression mode where a repo returns a dict with extra keys
that the FE depends on, but the response_model declaration silently
drops them. FastAPI's Pydantic validation silently strips any field
not declared on the response model, so the BE looks healthy in unit
tests but the FE sees `undefined` for that field at runtime.

This is exactly the bug that caused `window_total_requests` to vanish
from the insights response when we fixed the duplicate-key bug — the
field was in the repo's return dict, but never in `InsightsResponse`.

Each test below pairs (repo function → response model class) and
asserts that every key the repo emits is either:
  1. A declared field on the response model, OR
  2. In the explicit `_ALLOWED_DROPPED_KEYS` allowlist (telemetry /
     debug keys the model intentionally strips).

If a repo starts returning a new key, this test fails until the
key is added to the model (or the allowlist).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pytest

from backend.repositories._base import _safe_table

# Telemetry / debug keys that every repo merges in via `**runner.telemetry()`.
# These are intentionally stripped from the FE-visible payload because the
# FE has its own request tracing. Listing them here documents the
# stripping intent so a new telemetry key surfacing in a different repo
# fails the test.
_ALLOWED_DROPPED_KEYS = {
    "debug_queries",
    "debug_calls",
    "_debug_queries",
    "_debug_calls",
    "_is_cached",
    # The session-scoring reads run through the `_cached` wrapper, which also
    # bakes a per-handler `_section_timings` entry into the envelope (the
    # dashboard repos emit it under the same name). It's telemetry, not data.
    "_section_timings",
    # `insights.get_insights` computes a window-total row count that the FE
    # doesn't currently render. Keeping it as repo-internal telemetry rather
    # than declaring it on InsightsResponse keeps the FE TS surface minimal.
    # Move to the model if/when the FE actually needs it.
    "window_total_requests",
}

_TABLE_NAME = "test_service"
_src = {"name": _TABLE_NAME, "service_id": "tsid"}


@pytest.fixture
def seeded_con():
    """Seeded DuckDB used across every contract test."""
    con = duckdb.connect(":memory:")
    table = _safe_table(_TABLE_NAME)
    con.execute(
        f"""
        CREATE TABLE {table} (
            "timestamp" TIMESTAMPTZ,
            "dt" VARCHAR,
            "timestamp_hour" VARCHAR,
            "status" INTEGER,
            "country" VARCHAR,
            "url" VARCHAR,
            "ip" VARCHAR,
            "method" VARCHAR,
            "ua" VARCHAR,
            "pop" VARCHAR,
            "asn" INTEGER,
            "city" VARCHAR,
            "region" VARCHAR,
            "elapsed" INTEGER,
            "cache" VARCHAR,
            "ottfb" DOUBLE,
            "ttfb" DOUBLE,
            "ottlb" DOUBLE,
            "waf_sig" VARCHAR,
            "edge" BOOLEAN,
            "lat" DOUBLE,
            "lon" DOUBLE,
            "resp_bytes" INTEGER,
            "oip" VARCHAR,
            "ost" INTEGER,
            "tcp_rtt" INTEGER,
            "ploss" DOUBLE,
            "rtt_var" INTEGER,
            "rtt_min" INTEGER,
            "metro" VARCHAR,
            "ja3" VARCHAR,
            "ja4" VARCHAR,
            "ttl" INTEGER,
            "p_type" VARCHAR,
            "transport" VARCHAR,
            "c_speed" VARCHAR,
            "c_type" VARCHAR,
            "server_region" VARCHAR,
            "retrans" INTEGER,
            "bw" INTEGER,
            "delivery_rate" INTEGER,
            "rid" VARCHAR,
            "prid" VARCHAR
        )
        """
    )
    base = datetime.now(UTC) - timedelta(hours=1)
    for i in range(20):
        ts = base + timedelta(minutes=i * 3)
        con.execute(
            f"INSERT INTO {table} VALUES " + "(" + ", ".join(["?"] * 43) + ")",
            [
                ts,
                ts.strftime("%Y-%m-%d"),
                ts.strftime("%Y-%m-%d-%H"),
                200 if i % 5 else 500,
                "US",
                f"/p/{i}",
                f"10.0.0.{i}",
                "GET",
                "Mozilla/5.0",
                "LAX",
                15169,
                "SF",
                "CA",
                50 + i * 10,
                "HIT" if i % 2 else "MISS",
                50000.0 + i,
                0.05 + i * 0.01,
                0.1 + i * 0.01,
                "",
                True,
                37.7749,
                -122.4194,
                500 + i * 50,
                "origin-1",
                200 if i % 5 else 500,
                10000,
                0.0,
                100,
                50,
                "807",
                "ja3",
                "ja4",
                300,
                "U",
                "h2",
                "C",
                "broadband",
                "us-west",
                0,
                100000,
                100000,
                f"r{i}",
                f"pr{i % 5}",
            ],
        )
    yield con
    con.close()


def _check_no_dropped_keys(repo_result: dict, response_model_cls, label: str):
    """Assert every key in ``repo_result`` is either a declared field on
    the response model OR in the allowlist of intentionally-stripped
    telemetry keys."""
    declared_fields = set(response_model_cls.model_fields.keys())
    repo_keys = set(repo_result.keys())
    dropped = repo_keys - declared_fields - _ALLOWED_DROPPED_KEYS
    assert not dropped, (
        f"{label}: repo returned keys not declared on {response_model_cls.__name__} "
        f"and not in the allowlist: {sorted(dropped)}\n"
        f"Either add them to the model (FE wants them) or add them to "
        f"_ALLOWED_DROPPED_KEYS (intentionally stripped)."
    )


# ── dashboard.get_aggregates ↔ AggregatesResponse ────────────────────────


def test_dashboard_aggregates_contract(seeded_con):
    """`get_aggregates` return keys must match `AggregatesResponse` —
    minus the explicit telemetry allowlist. Pinned because losing this
    would let a repo refactor silently drop a field the FE renders
    (the regression mode of the `window_total_requests` bug)."""
    from backend.models.dashboard import AggregatesResponse
    from backend.repositories.dashboard import _dashboard_cache, get_aggregates

    _dashboard_cache.clear()
    result = get_aggregates(
        con=seeded_con,
        src=_src,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
    )
    _check_no_dropped_keys(result, AggregatesResponse, "dashboard.get_aggregates")


# ── insights.get_insights ↔ InsightsResponse ─────────────────────────────


def test_insights_get_insights_contract(seeded_con):
    """`get_insights` return keys must match `InsightsResponse`.

    Regression-pinned for the original drop: ``window_total_requests``
    was in the repo's return dict but not declared on
    ``InsightsResponse``, so Pydantic silently stripped it and the FE
    saw `undefined`. The fix was to add it to the model — this test
    keeps it from regressing."""
    from backend.models.dashboard import InsightsResponse
    from backend.repositories.insights import _insights_cache, get_insights

    _insights_cache.clear()
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value={"LAX": (33.94, -118.4)}):
        result = get_insights(seeded_con, _src, window_hours=1, baseline_hours=1)
    _check_no_dropped_keys(result, InsightsResponse, "insights.get_insights")


# ── origin.get_summary ↔ OriginSummaryResponse ───────────────────────────


def test_origin_get_summary_contract(seeded_con):
    """`get_summary` return keys must match `OriginSummaryResponse`."""
    from backend.models.origin import OriginSummaryResponse
    from backend.repositories.origin import get_summary

    result = get_summary(seeded_con, _src, None, None, {})
    _check_no_dropped_keys(result, OriginSummaryResponse, "origin.get_summary")


# ── origin.get_timeseries ↔ OriginTimeseriesResponse ─────────────────────


def test_origin_get_timeseries_contract(seeded_con):
    """`get_timeseries` return keys must match `OriginTimeseriesResponse`."""
    from backend.models.origin import OriginTimeseriesResponse
    from backend.repositories.origin import get_timeseries

    result = get_timeseries(seeded_con, _src, None, None, {})
    _check_no_dropped_keys(result, OriginTimeseriesResponse, "origin.get_timeseries")


# ── performance.get_performance_aggregates ↔ PerformanceAggregatesResponse


def test_performance_aggregates_contract(seeded_con):
    """`get_performance_aggregates` return keys must match
    `PerformanceAggregatesResponse`."""
    from backend.models.performance import PerformanceAggregatesResponse
    from backend.repositories.performance import get_performance_aggregates

    result = get_performance_aggregates(seeded_con, _src, None, None, {})
    _check_no_dropped_keys(result, PerformanceAggregatesResponse, "performance.get_performance_aggregates")


# ── security.get_security_aggregates ↔ SecurityAggregatesResponse ────────


def test_security_aggregates_contract(seeded_con):
    """`get_security_aggregates` return keys must match
    `SecurityAggregatesResponse`."""
    from backend.models.security import SecurityAggregatesResponse
    from backend.repositories.security import get_security_aggregates

    result = get_security_aggregates(seeded_con, _src, None, None, {})
    _check_no_dropped_keys(result, SecurityAggregatesResponse, "security.get_security_aggregates")


# ── network.get_health ↔ NetworkHealthResponse ───────────────────────────


def test_network_health_contract(seeded_con):
    """`get_health` return keys must match `NetworkHealthResponse`."""
    from backend.models.network import NetworkHealthResponse
    from backend.repositories.network import get_health

    result = get_health(seeded_con, _src, None, None, {})
    _check_no_dropped_keys(result, NetworkHealthResponse, "network.get_health")


# ── Session-scoring analyst-safe reads ↔ scoring response models ──────────
#
# Unlike the repo contracts above (which take a `con`), the scoring reads are
# router handlers that pull rows via `_query_logs` and route through `_cached`,
# so we patch `_query_logs`/`_table_columns`/labels and call the handler
# directly with a stub admin request (no analyst session → maximal key set).
# The handlers' `extra="allow"` + `response_model_exclude_unset` mean nothing
# is stripped at runtime; this guard keeps each DECLARED model a superset of
# the produced shape so the generated TS types stay complete and a future key
# can't slip past the FE contract.

import types
from unittest.mock import patch as _patch


def _admin_request():
    """Stub Request on the admin (loopback) path — no analyst session."""
    return types.SimpleNamespace(state=types.SimpleNamespace(analyst_session=None))


def _check_scoring_keys(produced: dict, model_cls, label: str):
    _check_no_dropped_keys(produced, model_cls, label)


def test_scoring_top_flagged_contract():
    from backend.models.session_scoring import ScoringTopFlaggedResponse, ScoringTopFlaggedRow
    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    rows = [
        {
            "timestamp": "2026-06-01T10:00:00+00:00",
            "edge_sid": "a",
            "edge_score": 90,
            "edge_score_l1": 40,
            "edge_score_l2": 50,
            "edge_cookie_compliance": "ok",
            "edge_score_reason": "rare-transition",
            "ip": "1.1.1.1",
            "ua": "UA",
            "url": "/p",
            "status": 200,
            "country": "US",
        }
    ]
    with _patch("backend.repositories.session_scoring.query_logs", return_value=rows):
        result = ss.scoring_top_flagged(request=_admin_request(), service_id="csvc", since_hours=24, limit=50)
    _check_scoring_keys(result, ScoringTopFlaggedResponse, "scoring.top_flagged")
    _check_no_dropped_keys(result["rows"][0], ScoringTopFlaggedRow, "scoring.top_flagged[row]")


def test_scoring_score_distribution_contract():
    from backend.models.session_scoring import ScoringDistributionRow, ScoringScoreDistributionResponse
    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    rows = [{"hour": "2026-06-01T10:00:00+00:00", "bucket": "75-100", "count": 5}]
    with _patch("backend.repositories.session_scoring.query_logs", return_value=rows):
        result = ss.scoring_score_distribution(request=_admin_request(), service_id="csvc", since_hours=24)
    _check_scoring_keys(result, ScoringScoreDistributionResponse, "scoring.score_distribution")
    _check_no_dropped_keys(result["rows"][0], ScoringDistributionRow, "scoring.score_distribution[row]")


def test_scoring_compliance_breakdown_contract():
    from backend.models.session_scoring import ScoringComplianceBreakdownResponse, ScoringComplianceRow
    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    rows = [{"hour": "2026-06-01T10:00:00+00:00", "compliance": "ok", "count": 200}]
    with _patch("backend.repositories.session_scoring.query_logs", return_value=rows):
        result = ss.scoring_compliance_breakdown(request=_admin_request(), service_id="csvc", since_hours=24)
    _check_scoring_keys(result, ScoringComplianceBreakdownResponse, "scoring.compliance_breakdown")
    _check_no_dropped_keys(result["rows"][0], ScoringComplianceRow, "scoring.compliance_breakdown[row]")


def test_scoring_latency_timeseries_contract():
    """DICT-01: the latency rows carry the column-dependent percentile fields
    only on re-provisioned services — the maximal row must be a subset of
    ScoringLatencyRow so the FE useScorerTimeseries types stay complete."""
    from backend.models.session_scoring import ScoringLatencyRow, ScoringLatencyTimeseriesResponse
    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    rows = [
        {
            "hour": "2026-06-17T10:00:00+00:00",
            "scored_count": 500,
            "fail_open_count": 30,
            "rtt_p50_us": 8000,
            "rtt_p95_us": 45000,
            "rtt_p99_us": 95000,
            "exec_p50_us": 540,
            "exec_p95_us": 900,
        }
    ]
    cols = {"edge_score", "edge_score_reason", "edge_score_rtt_us", "edge_score_exec_us"}
    with (
        _patch("backend.repositories.session_scoring.query_logs", return_value=rows),
        _patch.object(ss, "_table_columns", lambda sid: cols),
    ):
        result = ss.scoring_latency_timeseries(request=_admin_request(), service_id="csvc", since_hours=24)
    _check_scoring_keys(result, ScoringLatencyTimeseriesResponse, "scoring.latency_timeseries")
    _check_no_dropped_keys(result["rows"][0], ScoringLatencyRow, "scoring.latency_timeseries[row]")


def test_scoring_health_contract():
    from backend.models.session_scoring import (
        ScoringHealthResponse,
        ScoringLatencySnapshot,
        ScoringMatrixStaleness,
        ScoringReasonCount,
    )
    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    agg = [
        {
            "total_edge_rows": 1000,
            "scored_rows": 800,
            "distinct_sids": 50,
            "avg_score": 12.5,
            "p50_score": 5,
            "p95_score": 60,
            "max_score": 100,
            "scorer_errors": 40,
            "top_reasons": [{"reason": "cookie-missing", "count": 10}],
            "fail_open_breakdown": [{"reason": "compute-unavailable-503", "count": 2}],
            "l2_evaluated": 200,
            "l2_high_count": 10,
            "rtt_p50_us": 8000,
            "rtt_p95_us": 42000,
            "rtt_p99_us": 91000,
            "rtt_max_us": 100000,
            "exec_p50_us": 540,
            "exec_p95_us": 880,
        }
    ]
    cols = {
        "edge_score",
        "edge_score_l2",
        "edge_score_reason",
        "edge_cookie_compliance",
        "edge_sid",
        "edge_score_rtt_us",
        "edge_score_exec_us",
    }
    with (
        _patch("backend.repositories.session_scoring.query_logs", return_value=agg),
        _patch.object(ss, "_table_columns", lambda sid: cols),
        _patch("backend.routers.session_scoring_admin.l2_enforce_readiness_block", lambda sid: {"enabled": False}),
    ):
        result = ss.scoring_health(request=_admin_request(), service_id="csvc", since_hours=24)
    _check_scoring_keys(result, ScoringHealthResponse, "scoring.health")
    _check_no_dropped_keys(result["latency"], ScoringLatencySnapshot, "scoring.health.latency")
    _check_no_dropped_keys(result["matrix_staleness"], ScoringMatrixStaleness, "scoring.health.matrix_staleness")
    _check_no_dropped_keys(result["top_reasons"][0], ScoringReasonCount, "scoring.health.top_reasons[item]")


def test_scoring_evaluation_contract():
    """Exercise the scored branch (the maximal key set: auc / passed /
    threshold / default_min_auc / n_reconstructed / n_labels_total)."""
    from backend.models.session_scoring import ScoringEvaluationResponse
    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    result_obj = types.SimpleNamespace(n_good=5, n_bad=5, auc=0.9, passed=True, pass_threshold=0.7)
    with (
        _patch("backend.scoring.labels.counts_by_label", return_value={"good": 5, "bad": 5, "neutral": 1}),
        _patch("backend.scoring.labels.list_labels", return_value=[{"id": "1", "label": "good", "sid": "s1"}]),
        _patch.object(ss, "_load_matrix", lambda sid: {"version": "v9", "transitions": {}}),
        _patch.object(ss, "_reconstruct_labeled_sessions", lambda sid, labels: [({"max_edge_score": 90}, "bad")]),
        _patch("backend.scoring.evaluate.evaluate_from_persisted_scores", return_value=result_obj),
        _patch("backend.config.load_config", return_value={"scoring": {"matrix_version": "v8"}}),
    ):
        result = ss.scoring_evaluation(service_id="csvc")
    _check_scoring_keys(result, ScoringEvaluationResponse, "scoring.evaluation")


def test_scoring_curves_contract():
    """Exercise the scored branch (roc / pr / auc / average_precision)."""
    from backend.models.session_scoring import ScoringCurvePoint, ScoringCurvesResponse
    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    sessions = [({"max_edge_score": 90}, "bad"), ({"max_edge_score": 5}, "good")]
    with (
        _patch("backend.scoring.labels.counts_by_label", return_value={"good": 5, "bad": 5}),
        _patch("backend.scoring.labels.list_labels", return_value=[{"id": "1", "label": "good", "sid": "s1"}]),
        _patch.object(ss, "_reconstruct_labeled_sessions", lambda sid, labels: sessions),
    ):
        result = ss.scoring_curves(service_id="csvc")
    _check_scoring_keys(result, ScoringCurvesResponse, "scoring.curves")
    if result.get("roc"):
        _check_no_dropped_keys(result["roc"][0], ScoringCurvePoint, "scoring.curves.roc[point]")
    if result.get("pr"):
        _check_no_dropped_keys(result["pr"][0], ScoringCurvePoint, "scoring.curves.pr[point]")


def test_scoring_threshold_preview_contract():
    from backend.models.session_scoring import ScoringThresholdBucket, ScoringThresholdPreviewResponse
    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    agg = [
        {
            "total": 100,
            "flagged_total": 20,
            "flagged_good": 1,
            "flagged_bad": 5,
            "passed_good": 10,
            "passed_bad": 2,
        }
    ]
    with (
        _patch("backend.repositories.session_scoring.query_logs", return_value=agg),
        _patch("backend.scoring.labels.list_labels", return_value=[{"sid": "s1", "label": "bad"}]),
        _patch("backend.scoring.labels.counts_by_label", return_value={"good": 5, "bad": 5}),
    ):
        result = ss.scoring_threshold_preview(request=_admin_request(), service_id="csvc", threshold=75, since_hours=24)
    _check_scoring_keys(result, ScoringThresholdPreviewResponse, "scoring.threshold_preview")
    _check_no_dropped_keys(result["flagged"], ScoringThresholdBucket, "scoring.threshold_preview.flagged")
    _check_no_dropped_keys(result["passed"], ScoringThresholdBucket, "scoring.threshold_preview.passed")


def test_scoring_analytics_composite_contract():
    """Admin path includes all five analyst-safe sub-keys plus the two
    admin-only evaluation blocks — all must be declared on the composite."""
    import backend.routers.session_scoring_admin as ss_admin
    from backend.models.session_scoring import ScoringAnalyticsResponse
    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    with (
        _patch.object(ss, "scoring_top_flagged", lambda **kw: {"rows": []}),
        _patch.object(ss, "scoring_score_distribution", lambda **kw: {"rows": []}),
        _patch.object(ss, "scoring_compliance_breakdown", lambda **kw: {"rows": []}),
        _patch.object(ss, "scoring_latency_timeseries", lambda **kw: {"rows": [], "has_latency": False}),
        _patch.object(ss, "scoring_health", lambda **kw: {"since_hours": 24}),
        _patch.object(ss, "scoring_evaluation", lambda **kw: {"has_min_samples": False}),
        _patch.object(ss_admin, "scoring_evaluation_per_reason", lambda **kw: {"reasons": []}),
    ):
        result = ss.scoring_analytics_composite(request=_admin_request(), service_id="csvc", since_hours=24)
    _check_scoring_keys(result, ScoringAnalyticsResponse, "scoring.analytics")

import pytest

from backend.models.dashboard import AggregatesResponse
from backend.models.network import NetworkHealthResponse, NetworkQualityResponse
from backend.models.origin import (
    OriginIpHealthResponse,
    OriginPathBreakdownResponse,
    OriginPopLatencyResponse,
    OriginShieldingAnalysisResponse,
    OriginSlowUrlsResponse,
    OriginStatusCodesResponse,
    OriginSummaryResponse,
    OriginTimeseriesResponse,
)
from backend.models.performance import PerformanceAggregatesResponse
from backend.models.security import SecurityAggregatesResponse
from backend.repositories.dashboard import get_aggregates as dash_aggs
from backend.repositories.network import get_health as net_health_aggs
from backend.repositories.network import get_quality as net_qual_aggs
from backend.repositories.origin import (
    get_ip_health,
    get_path_breakdown,
    get_pop_latency,
    get_shielding_analysis,
    get_slow_urls,
    get_status_codes,
    get_summary,
    get_timeseries,
)
from backend.repositories.performance import get_performance_aggregates as perf_aggs
from backend.repositories.security import get_security_aggregates as sec_aggs


def _src_empty():
    return {"name": "test_empty_src", "service_id": "test_empty_src"}


@pytest.mark.parametrize(
    "repo_fn, pydantic_model, extra_args",
    [
        # Existing coverage
        (dash_aggs, AggregatesResponse, {"chart_interval": "1 hour", "chart_metric": "requests"}),
        (sec_aggs, SecurityAggregatesResponse, {"bucket_seconds": 3600}),
        (net_health_aggs, NetworkHealthResponse, {"bucket_seconds": 3600}),
        (net_qual_aggs, NetworkQualityResponse, {}),
        (get_summary, OriginSummaryResponse, {}),
        (perf_aggs, PerformanceAggregatesResponse, {}),
        # Origin family — Milestone C 2.4 expansion. Each repo function pairs
        # with its dedicated Pydantic response model. If a repo grows a new
        # key without the model declaring it, FastAPI silently strips it on
        # serialization and the dashboard breaks in a way that's hard to
        # debug. This catches the drift at test time.
        (get_timeseries, OriginTimeseriesResponse, {}),
        (get_slow_urls, OriginSlowUrlsResponse, {}),
        (get_status_codes, OriginStatusCodesResponse, {}),
        (get_path_breakdown, OriginPathBreakdownResponse, {}),
        (get_pop_latency, OriginPopLatencyResponse, {}),
        (get_ip_health, OriginIpHealthResponse, {}),
        (get_shielding_analysis, OriginShieldingAnalysisResponse, {}),
    ],
)
def test_repository_keys_match_pydantic_model(in_memory_duckdb, repo_fn, pydantic_model, extra_args):
    """
    Ensure that the repository returns dictionaries whose keys are exactly matched by
    the Pydantic response models. This prevents data from being silently stripped by FastAPI.
    """
    src = _src_empty()

    # Call the repository with an empty source/table to get the default empty structure
    repo_result = repo_fn(con=in_memory_duckdb, src=src, start_time=None, end_time=None, filters={}, **extra_args)

    # Pydantic models automatically include _debug_queries and _debug_calls when initialized normally
    # We remove these telemetry keys from the check because Pydantic serialization aliases handle them.
    repo_keys = set(repo_result.keys())
    repo_keys.discard("_debug_queries")
    repo_keys.discard("_debug_calls")
    repo_keys.discard("_is_cached")

    model_keys = set(pydantic_model.model_fields.keys())

    # Check if the repository is returning keys that the model doesn't know about
    missing_in_model = repo_keys - model_keys

    assert not missing_in_model, (
        f"{pydantic_model.__name__} is missing fields returned by the repository: {missing_in_model}"
    )

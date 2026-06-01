"""Network router — health heatmap and quality metrics."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from backend.deps import AnalyticsDeps
from backend.models.common import FilteredRequest, Limit100, Seconds14400
from backend.models.network import NetworkHealthResponse, NetworkQualityResponse
from backend.repositories import network as repo
from backend.utils.router_utils import query_errors

router = APIRouter(prefix="/api", tags=["network"])


class NetworkHealthRequest(FilteredRequest):
    metric: str = "health_score"
    bucket_seconds: Seconds14400 = 300
    top_n: Limit100 = 30
    map_asn: str = "all"


class NetworkQualityRequest(FilteredRequest):
    region_country: str = "US"


@router.post("/network-health", response_model=NetworkHealthResponse)
@query_errors()
def network_health(req: NetworkHealthRequest, deps: AnalyticsDeps = Depends()):
    res = repo.get_health(
        con=deps.con,
        src=deps.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
        metric=req.metric,
        bucket_seconds=req.bucket_seconds,
        top_n=req.top_n,
        map_asn=req.map_asn,
    )
    return NetworkHealthResponse.with_telemetry(**res)


@router.post("/network-quality", response_model=NetworkQualityResponse)
@query_errors()
def network_quality(req: NetworkQualityRequest, deps: AnalyticsDeps = Depends()):
    res = repo.get_quality(
        con=deps.con,
        src=deps.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
        region_country=req.region_country,
    )
    return NetworkQualityResponse.with_telemetry(**res)

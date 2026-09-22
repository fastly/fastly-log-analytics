from __future__ import annotations

from datetime import datetime
from typing import Any

from backend.high_scale.registry import HighScaleService
from backend.models.network import (
    NetworkHealthResponse,
    NetworkHealthSummary,
    NetworkQualityResponse,
)
from backend.routers.network import PopHealthListResponse


def _range_value(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def network_health(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None
) -> NetworkHealthResponse:
    start = _range_value(start_time)
    end = _range_value(end_time)

    query = """
        SELECT
            toStartOfMinute(event_timestamp) AS bucket,
            toInt32OrNull(custom_fields['asn']) AS asn,
            count() AS reqs,
            sum(if(toInt32OrZero(custom_fields['status']) >= 500, 1, 0)) AS err_count,
            avg(toFloat64OrZero(custom_fields['tcp_rtt']) / 1000.0) AS avg_rtt
        FROM fastly_log_analytics.request_facts
        WHERE service_id={service_id:String}
          AND publication_state='visible'
          AND event_timestamp >= {start:DateTime64(3)}
          AND event_timestamp < {end:DateTime64(3)}
        GROUP BY bucket, asn
        ORDER BY reqs DESC
        LIMIT 500
    """
    params = {
        "service_id": service.service_id,
        "start": start,
        "end": end,
    }

    try:
        heatmap_rows = service.client.execute(query, params)
    except Exception:
        heatmap_rows = []

    heatmap = [
        {
            "bucket": row["bucket"].isoformat() if hasattr(row["bucket"], "isoformat") else row["bucket"],
            "asn": row["asn"] if row["asn"] is not None else 0,
            "reqs": int(row["reqs"]),
            "error_pct": float(row["err_count"]) * 100.0 / float(row["reqs"]) if float(row["reqs"]) > 0 else 0.0,
            "rtt_med_us": float(row["avg_rtt"]) * 1000.0 if row["avg_rtt"] is not None else 0.0,
            "avg_ploss": 0.0,
        }
        for row in heatmap_rows
    ]

    total_reqs = sum(int(r["reqs"]) for r in heatmap_rows)
    avg_rtt_ms = sum(float(r["avg_rtt"] or 0) * int(r["reqs"]) for r in heatmap_rows) / max(total_reqs, 1)

    summary = NetworkHealthSummary(
        global_health_score=100.0,
        avg_rtt_ms=round(avg_rtt_ms, 2),
        total_reqs=total_reqs,
    )

    return NetworkHealthResponse.with_telemetry(
        available=True,
        has_data=True,
        buckets=[],
        heatmap=heatmap,
        map_buckets=[],
        cities=[],
        leaderboard=[],
        metro_leaderboard=[],
        summary=summary,
        countries=[],
        has_metro=False,
    )


def network_quality(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None
) -> NetworkQualityResponse:
    start = _range_value(start_time)
    end = _range_value(end_time)

    def _top_by(dimension: str, limit: int = 25):
        query = f"""
            SELECT
                {dimension} AS label,
                count() AS reqs,
                median(toFloat64OrZero(custom_fields['tcp_rtt']) / 1000.0) AS rtt_ms
            FROM fastly_log_analytics.request_facts
            WHERE service_id={{service_id:String}}
              AND publication_state='visible'
              AND event_timestamp >= {{start:DateTime64(3)}}
              AND event_timestamp < {{end:DateTime64(3)}}
            GROUP BY label
            ORDER BY reqs DESC
            LIMIT {limit}
        """
        try:
            return service.client.execute(query, {"service_id": service.service_id, "start": start, "end": end})
        except Exception:
            return []

    by_asn_rows = _top_by("toInt32OrNull(custom_fields['asn'])")
    for row in by_asn_rows:
        if row["label"] is None:
            row["label"] = "Unknown"
        else:
            row["label"] = str(row["label"])

    by_country_rows = _top_by("country")
    by_pop_rows = _top_by("custom_fields['pop']")

    return NetworkQualityResponse.with_telemetry(
        available=True,
        by_country=by_country_rows,
        by_asn=by_asn_rows,
        by_region=[],
        region_country="US",
        by_pop=by_pop_rows,
        scatter=[],
        countries=[],
    )


def get_pop_health(
    service: HighScaleService, start_time: datetime | None, end_time: datetime | None
) -> PopHealthListResponse:
    return PopHealthListResponse.with_telemetry(
        data=[],
    )

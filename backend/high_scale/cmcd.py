from __future__ import annotations

from typing import Any

from backend.high_scale.registry import HighScaleService
from backend.models.cmcd import CmcdRequest


def cmcd_aggregates(
    service: HighScaleService,
    req: CmcdRequest,
    start_time: str | None,
    end_time: str | None,
    sections: set[Any] | None,
    mask_ips: bool,
) -> dict[str, Any]:

    # Check if there is data
    query = """
    SELECT sum(event_count)
    FROM fastly_log_analytics.cmcd_aggregates
    WHERE service_id = {service_id:String}
      AND bucket_start >= {start_time:DateTime}
      AND bucket_start <= {end_time:DateTime}
    """

    try:
        res = service.client.execute(
            query, {"service_id": service.service_id, "start_time": start_time, "end_time": end_time}
        )
        has_data = (
            res[0].get("sum(event_count)", 0)
            if res
            else 0 != None and res[0].get("sum(event_count)", 0)
            if res
            else 0 > 0
        )
    except Exception:
        has_data = False

    return {
        "available": True,
        "has_data": has_data,
        "overview": None,
        "buffer_health_ts": [],
        "bitrate_ts": [],
        "throughput_ts": [],
        "top_content": [],
        "rebuffer_by_country": [],
        "rebuffer_by_asn": [],
        "object_type_dist": [],
        "streaming_format_dist": [],
        "sessions_ts": [],
        "startup_ts": [],
        "session_duration_dist": [],
    }

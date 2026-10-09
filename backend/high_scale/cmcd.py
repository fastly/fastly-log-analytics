from datetime import UTC, datetime, timedelta
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
    now = datetime.now(UTC)
    end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00")) if end_time else now
    start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00")) if start_time else (end_dt - timedelta(days=1))

    # Check if there is data
    query = """
    SELECT sum(event_count)
    FROM cmcd_aggregates
    WHERE service_id = {service_id:String}
      AND bucket_start >= {start_time:DateTime64(3)}
      AND bucket_start <= {end_time:DateTime64(3)}
    """

    try:
        res = service.client.execute(
            query,
            {
                "service_id": service.service_id,
                "start_time": start_dt,
                "end_time": end_dt,
            },
        )
        count = 0
        if res and len(res) > 0:
            val = next(iter(res[0].values()), 0)
            count = int(val) if val is not None else 0
        has_data = count > 0
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

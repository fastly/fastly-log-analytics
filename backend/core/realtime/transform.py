"""Transform rt.fastly.com JSON into SSE metrics_tick payloads.

Pure function — no I/O, no state. The ``Data`` array from the RT API
contains one entry per second; we aggregate into a single tick.
"""

from __future__ import annotations

from datetime import UTC, datetime


def transform_rt_response(rt_json: dict) -> dict:
    """Aggregate a rt.fastly.com response into a ``metrics_tick`` payload.

    The RT API returns ``{"Data": [...], "Timestamp": ..., "AggregateDelay": ...}``.
    Each ``Data`` entry contains per-second counters. We sum/average them
    into a single SSE payload the frontend can render directly.
    """
    data_points = rt_json.get("Data") or []
    n = len(data_points) or 1

    total_requests = 0
    total_errors = 0
    total_hits = 0
    total_miss = 0
    total_pass = 0
    total_synth = 0
    total_bandwidth = 0
    total_shield = 0
    total_shield_resp_bytes = 0
    total_waf_blocked = 0
    total_waf_logged = 0
    total_waf_passed = 0
    total_bereq_bytes = 0
    status_breakdown: dict[str, int] = {}
    pop_counters: dict[str, dict[str, int]] = {}

    for point in data_points:
        dc_data = point.get("datacenter", {})
        if isinstance(dc_data, dict) and dc_data:
            for pop_name, pop_stats in dc_data.items():
                if not isinstance(pop_stats, dict):
                    continue
                pop_reqs = int(pop_stats.get("requests", 0))
                if pop_reqs > 0:
                    existing = pop_counters.get(pop_name, {"requests": 0, "errors": 0})
                    existing["requests"] = existing["requests"] + pop_reqs
                    existing["errors"] = existing["errors"] + int(pop_stats.get("status_5xx", 0))
                    pop_counters[pop_name] = existing

        agg = point.get("aggregated") or dc_data
        if isinstance(agg, dict):
            if "all" in agg:
                agg = agg["all"]
            elif not any(k in agg for k in ("requests", "status_2xx")):
                first: dict = next(iter(agg.values()), {})
                if isinstance(first, dict):
                    agg = first

        requests = int(agg.get("requests", 0))
        total_requests += requests
        total_errors += int(agg.get("status_5xx", 0)) + int(agg.get("status_4xx", 0))
        total_hits += int(agg.get("hits", 0))
        total_miss += int(agg.get("miss", 0))
        total_pass += int(agg.get("pass", 0))
        total_synth += int(agg.get("synth", 0))
        total_bandwidth += int(agg.get("resp_body_bytes", 0)) + int(agg.get("resp_header_bytes", 0))
        total_shield += int(agg.get("shield", 0))
        total_shield_resp_bytes += int(agg.get("shield_resp_body_bytes", 0)) + int(
            agg.get("shield_resp_header_bytes", 0)
        )
        total_waf_blocked += int(agg.get("waf_blocked", 0))
        total_waf_logged += int(agg.get("waf_logged", 0))
        total_waf_passed += int(agg.get("waf_passed", 0))
        total_bereq_bytes += int(agg.get("bereq_header_bytes", 0)) + int(agg.get("bereq_body_bytes", 0))

        for code_key in ("status_1xx", "status_2xx", "status_3xx", "status_4xx", "status_5xx"):
            count = int(agg.get(code_key, 0))
            if count:
                status_breakdown[code_key] = status_breakdown.get(code_key, 0) + count

    cache_total = total_hits + total_miss
    cache_hit_ratio = (total_hits / cache_total) if cache_total > 0 else 0.0
    error_rate = (total_errors / total_requests) if total_requests > 0 else 0.0
    shield_hit_ratio = (total_shield / total_requests) if total_requests > 0 else 0.0

    degraded_pops = [
        name
        for name, c in pop_counters.items()
        if c["errors"] > 0 and c["requests"] > 0 and c["errors"] / c["requests"] > 0.05
    ]

    estimated_cost = (total_requests / 10_000) * 0.008 + (total_bandwidth / 1_073_741_824) * 0.08

    return {
        "event": "metrics_tick",
        "event_schema_version": 2,
        "timestamp": datetime.now(UTC).isoformat(),
        "status": "ok",
        "data": {
            "requests_per_second": round(total_requests / n, 2),
            "error_rate": round(error_rate, 4),
            "cache_hit_ratio": round(cache_hit_ratio, 4),
            "bandwidth_mbps": round((total_bandwidth / n) * 8 / 1_000_000, 3),
            "status_breakdown": status_breakdown,
            "estimated_cost_usd": round(estimated_cost, 6),
            "origin_requests_per_second": round(total_miss / n, 2),
            "origin_bandwidth_mbps": round((total_bereq_bytes / n) * 8 / 1_000_000, 3),
            "shield_requests": total_shield,
            "shield_hit_ratio": round(shield_hit_ratio, 4),
            "pass_requests": total_pass,
            "synth_requests": total_synth,
            "waf_blocked": total_waf_blocked,
            "waf_logged": total_waf_logged,
            "waf_passed": total_waf_passed,
            "pop_count": len(pop_counters),
            "degraded_pops": degraded_pops,
        },
        "aggregate_delay": rt_json.get("AggregateDelay", 0),
    }


def error_tick_payload(last_good: dict | None = None) -> dict:
    """Return an error-state tick when the RT API is unreachable.

    Carries the last-known-good data (if any) so the frontend can dim
    values rather than showing blanks.
    """
    fallback_data = {
        "requests_per_second": 0,
        "error_rate": 0.0,
        "cache_hit_ratio": 0.0,
        "bandwidth_mbps": 0.0,
        "status_breakdown": {},
        "estimated_cost_usd": 0.0,
    }
    if last_good and "data" in last_good:
        fallback_data = last_good["data"]

    return {
        "event": "metrics_tick",
        "event_schema_version": 1,
        "timestamp": datetime.now(UTC).isoformat(),
        "status": "rt_down",
        "data": fallback_data,
        "aggregate_delay": 0,
    }

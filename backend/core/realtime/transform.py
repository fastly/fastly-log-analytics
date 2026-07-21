"""Transform rt.fastly.com JSON into SSE metrics_tick payloads.

Pure function — no I/O, no state. The ``Data`` array from the RT API
contains one entry per second; we aggregate into a single tick.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import cast


def transform_single_second(data_point: dict, timestamp: str) -> dict:
    """Transform one entry from the RT API ``Data[]`` array into a ``metrics_tick`` payload.

    Used during backfill to publish individual per-second ticks rather than
    aggregating an entire 120-entry response into one combined tick.
    """
    tick = transform_rt_response({"Data": [data_point], "Timestamp": 0, "AggregateDelay": 0})
    tick["timestamp"] = timestamp
    return tick


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

    total_origin_offload = 0.0

    sum_hit_time = 0.0
    sum_miss_time = 0.0
    sum_pass_time = 0.0

    total_http2 = 0
    total_http3 = 0
    total_ipv6 = 0
    total_tls_v12 = 0
    total_tls_v13 = 0

    total_ddos_action_blackhole = 0
    total_ddos_action_tarpit = 0
    total_ddos_action_close = 0
    total_ddos_action_downgrade = 0
    total_ddos_detect = 0
    total_ddos_mitigate = 0

    _status_codes = (200, 204, 301, 302, 304, 400, 401, 403, 404, 429, 500, 502, 503, 504)
    status_detail: dict[str, int] = {}

    _obj_size_keys = ("1k", "10k", "100k", "1m", "10m", "100m", "1g", "other")
    object_size_distribution: dict[str, int] = {}

    total_origin_fetches = 0
    total_origin_revalidations = 0
    total_origin_cache_fetches = 0

    total_shield_hit_requests = 0
    total_shield_miss_requests = 0
    total_shield_revalidations = 0
    total_shield_fetch_body_bytes = 0

    total_request_collapse_usable = 0
    total_request_collapse_unusable = 0

    total_segblock_origin = 0
    total_segblock_shield = 0

    total_bot_challenge_starts = 0
    total_bot_challenges_issued = 0
    total_bot_challenges_succeeded = 0
    total_bot_challenges_failed = 0
    total_bot_detected = 0
    total_bot_verified = 0
    total_bot_ai_crawlers = 0

    sum_compute_exec_time = 0.0
    sum_compute_req_time = 0.0
    total_compute_ram = 0
    total_compute_bereq_errors = 0
    total_compute_guest_errors = 0
    total_compute_resource_exceeded = 0

    total_restarts = 0
    total_recv_sub_count = 0
    sum_recv_sub_time = 0.0
    total_fetch_sub_count = 0
    sum_fetch_sub_time = 0.0
    total_deliver_sub_count = 0
    sum_deliver_sub_time = 0.0
    total_error_sub_count = 0
    sum_error_sub_time = 0.0

    miss_histogram: dict[str, int] = {}

    for point in data_points:
        dc_data = point.get("datacenter", {})
        if isinstance(dc_data, dict) and dc_data:
            for pop_name, pop_stats in dc_data.items():
                if not isinstance(pop_stats, dict):
                    continue
                pop_reqs = int(pop_stats.get("requests", 0))
                if pop_reqs > 0:
                    existing = pop_counters.get(pop_name, {"requests": 0, "errors": 0, "hits": 0, "miss": 0})
                    existing["requests"] = existing["requests"] + pop_reqs
                    existing["errors"] = existing["errors"] + int(pop_stats.get("status_5xx", 0))
                    existing["hits"] = existing["hits"] + int(pop_stats.get("hits", 0))
                    existing["miss"] = existing["miss"] + int(pop_stats.get("miss", 0))
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

        total_origin_offload += float(agg.get("origin_offload", 0))

        sum_hit_time += float(agg.get("hit_time", 0))
        sum_miss_time += float(agg.get("miss_time", 0))
        sum_pass_time += float(agg.get("pass_time", 0))

        total_http2 += int(agg.get("http2", 0))
        total_http3 += int(agg.get("http3", 0))
        total_ipv6 += int(agg.get("ipv6", 0))
        total_tls_v12 += int(agg.get("tls_v12", 0))
        total_tls_v13 += int(agg.get("tls_v13", 0))

        total_ddos_action_blackhole += int(agg.get("ddos_action_blackhole", 0))
        total_ddos_action_tarpit += int(agg.get("ddos_action_tarpit", 0))
        total_ddos_action_close += int(agg.get("ddos_action_close", 0))
        total_ddos_action_downgrade += int(agg.get("ddos_action_downgrade", 0))
        total_ddos_detect += int(agg.get("ddos_protection_requests_detect_count", 0))
        total_ddos_mitigate += int(agg.get("ddos_protection_requests_mitigate_count", 0))

        for code in _status_codes:
            count = int(agg.get(f"status_{code}", 0))
            if count:
                key = str(code)
                status_detail[key] = status_detail.get(key, 0) + count

        for size_key in _obj_size_keys:
            count = int(agg.get(f"object_size_{size_key}", 0))
            if count:
                object_size_distribution[size_key] = object_size_distribution.get(size_key, 0) + count

        total_origin_fetches += int(agg.get("origin_fetches", 0))
        total_origin_revalidations += int(agg.get("origin_revalidations", 0))
        total_origin_cache_fetches += int(agg.get("origin_cache_fetches", 0))

        total_shield_hit_requests += int(agg.get("shield_hit_requests", 0))
        total_shield_miss_requests += int(agg.get("shield_miss_requests", 0))
        total_shield_revalidations += int(agg.get("shield_revalidations", 0))
        total_shield_fetch_body_bytes += int(agg.get("shield_fetch_body_bytes", 0))

        total_request_collapse_usable += int(agg.get("request_collapse_usable_count", 0))
        total_request_collapse_unusable += int(agg.get("request_collapse_unusable_count", 0))

        total_segblock_origin += int(agg.get("segblock_origin_fetches", 0))
        total_segblock_shield += int(agg.get("segblock_shield_fetches", 0))

        total_bot_challenge_starts += int(agg.get("bot_challenge_starts", 0))
        total_bot_challenges_issued += int(agg.get("bot_challenges_issued", 0))
        total_bot_challenges_succeeded += int(agg.get("bot_challenges_succeeded", 0))
        total_bot_challenges_failed += int(agg.get("bot_challenges_failed", 0))
        total_bot_detected += int(agg.get("bot_edge_requests_detected_count", 0))
        total_bot_verified += int(agg.get("bot_edge_requests_verified_count", 0))
        total_bot_ai_crawlers += int(agg.get("bot_edge_requests_ai_crawler_count", 0))

        sum_compute_exec_time += float(agg.get("compute_execution_time_ms", 0))
        sum_compute_req_time += float(agg.get("compute_request_time_ms", 0))
        total_compute_ram += int(agg.get("compute_ram_used", 0))
        total_compute_bereq_errors += int(agg.get("compute_bereq_errors", 0))
        total_compute_guest_errors += int(agg.get("compute_guest_errors", 0))
        total_compute_resource_exceeded += int(agg.get("compute_resource_limit_exceeded", 0))

        total_restarts += int(agg.get("restarts", 0))
        total_recv_sub_count += int(agg.get("recv_sub_count", 0))
        sum_recv_sub_time += float(agg.get("recv_sub_time", 0))
        total_fetch_sub_count += int(agg.get("fetch_sub_count", 0))
        sum_fetch_sub_time += float(agg.get("fetch_sub_time", 0))
        total_deliver_sub_count += int(agg.get("deliver_sub_count", 0))
        sum_deliver_sub_time += float(agg.get("deliver_sub_time", 0))
        total_error_sub_count += int(agg.get("error_sub_count", 0))
        sum_error_sub_time += float(agg.get("error_sub_time", 0))

        point_hist = agg.get("miss_histogram")
        if isinstance(point_hist, dict):
            for bucket, count in point_hist.items():
                miss_histogram[bucket] = miss_histogram.get(bucket, 0) + int(count)

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

    origin_offload = round(total_origin_offload / n, 4)

    hit_latency_ms = round((sum_hit_time / n) * 1000, 2)
    miss_latency_ms = round((sum_miss_time / n) * 1000, 2)
    pass_latency_ms = round((sum_pass_time / n) * 1000, 2)

    if total_requests > 0:
        h2_pct = round(total_http2 / total_requests * 100, 2)
        h3_pct = round(total_http3 / total_requests * 100, 2)
        ipv6_pct = round(total_ipv6 / total_requests * 100, 2)
        tls12_pct = round(total_tls_v12 / total_requests * 100, 2)
        tls13_pct = round(total_tls_v13 / total_requests * 100, 2)
    else:
        h2_pct = h3_pct = ipv6_pct = tls12_pct = tls13_pct = 0.0

    compute_exec_time_ms = round(sum_compute_exec_time / n, 2)
    compute_req_time_ms = round(sum_compute_req_time / n, 2)
    compute_ram_avg = round(total_compute_ram / n)

    vcl_recv_time_ms = round((sum_recv_sub_time / n) * 1000, 2)
    vcl_fetch_time_ms = round((sum_fetch_sub_time / n) * 1000, 2)
    vcl_deliver_time_ms = round((sum_deliver_sub_time / n) * 1000, 2)
    vcl_error_time_ms = round((sum_error_sub_time / n) * 1000, 2)

    top_pops = sorted(
        [
            {
                "name": name,
                "requests": c["requests"],
                "errors": c["errors"],
                "hits": c.get("hits", 0),
                "miss": c.get("miss", 0),
                "hit_ratio": round(c.get("hits", 0) / c["requests"], 4) if c["requests"] > 0 else 0.0,
                "error_rate": round(c["errors"] / c["requests"], 4) if c["requests"] > 0 else 0.0,
            }
            for name, c in pop_counters.items()
        ],
        key=lambda p: cast(int, p["requests"]),
        reverse=True,
    )[:15]

    all_pops = {name: {"r": c["requests"], "e": c["errors"]} for name, c in pop_counters.items() if c["requests"] > 0}

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
            "total_requests": total_requests,
            "total_hits": total_hits,
            "total_miss": total_miss,
            "total_pass": total_pass,
            "total_errors": total_errors,
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
            "origin_offload": origin_offload,
            "hit_latency_ms": hit_latency_ms,
            "miss_latency_ms": miss_latency_ms,
            "pass_latency_ms": pass_latency_ms,
            "http2": total_http2,
            "http3": total_http3,
            "ipv6": total_ipv6,
            "tls_v12": total_tls_v12,
            "tls_v13": total_tls_v13,
            "h2_pct": h2_pct,
            "h3_pct": h3_pct,
            "ipv6_pct": ipv6_pct,
            "tls12_pct": tls12_pct,
            "tls13_pct": tls13_pct,
            "ddos_action_blackhole": total_ddos_action_blackhole,
            "ddos_action_tarpit": total_ddos_action_tarpit,
            "ddos_action_close": total_ddos_action_close,
            "ddos_action_downgrade": total_ddos_action_downgrade,
            "ddos_detect": total_ddos_detect,
            "ddos_mitigate": total_ddos_mitigate,
            "status_detail": status_detail,
            "object_size_distribution": object_size_distribution,
            "origin_fetches": total_origin_fetches,
            "origin_revalidations": total_origin_revalidations,
            "origin_cache_fetches": total_origin_cache_fetches,
            "shield_hit_requests": total_shield_hit_requests,
            "shield_miss_requests": total_shield_miss_requests,
            "shield_revalidations": total_shield_revalidations,
            "shield_fetch_body_bytes": total_shield_fetch_body_bytes,
            "request_collapse_usable": total_request_collapse_usable,
            "request_collapse_unusable": total_request_collapse_unusable,
            "segblock_origin_fetches": total_segblock_origin,
            "segblock_shield_fetches": total_segblock_shield,
            "bot_challenge_starts": total_bot_challenge_starts,
            "bot_challenges_issued": total_bot_challenges_issued,
            "bot_challenges_succeeded": total_bot_challenges_succeeded,
            "bot_challenges_failed": total_bot_challenges_failed,
            "bot_detected": total_bot_detected,
            "bot_verified": total_bot_verified,
            "bot_ai_crawlers": total_bot_ai_crawlers,
            "compute_exec_time_ms": compute_exec_time_ms,
            "compute_req_time_ms": compute_req_time_ms,
            "compute_ram_used": compute_ram_avg,
            "compute_bereq_errors": total_compute_bereq_errors,
            "compute_guest_errors": total_compute_guest_errors,
            "compute_resource_exceeded": total_compute_resource_exceeded,
            "restarts": total_restarts,
            "vcl_recv_count": total_recv_sub_count,
            "vcl_recv_time_ms": vcl_recv_time_ms,
            "vcl_fetch_count": total_fetch_sub_count,
            "vcl_fetch_time_ms": vcl_fetch_time_ms,
            "vcl_deliver_count": total_deliver_sub_count,
            "vcl_deliver_time_ms": vcl_deliver_time_ms,
            "vcl_error_count": total_error_sub_count,
            "vcl_error_time_ms": vcl_error_time_ms,
            "miss_histogram": miss_histogram,
            "top_pops": top_pops,
            "all_pops": all_pops,
        },
        "aggregate_delay": rt_json.get("AggregateDelay", 0),
    }


def gap_tick_payload() -> dict:
    """Zero-value tick emitted when the rt.fastly.com long-poll hasn't
    responded within 1 second.  Keeps the SSE cadence at 1 tick/s so the
    frontend chart scrolls smoothly even during low-traffic lulls."""
    return {
        "event": "metrics_tick",
        "event_schema_version": 2,
        "timestamp": datetime.now(UTC).isoformat(),
        "status": "ok",
        "data": {
            "requests_per_second": 0,
            "error_rate": 0.0,
            "cache_hit_ratio": 0.0,
            "bandwidth_mbps": 0.0,
            "total_requests": 0,
            "total_hits": 0,
            "total_miss": 0,
            "total_pass": 0,
            "total_errors": 0,
            "status_breakdown": {},
            "estimated_cost_usd": 0.0,
            "origin_requests_per_second": 0.0,
            "origin_bandwidth_mbps": 0.0,
            "shield_requests": 0,
            "shield_hit_ratio": 0.0,
            "pass_requests": 0,
            "synth_requests": 0,
            "waf_blocked": 0,
            "waf_logged": 0,
            "waf_passed": 0,
            "pop_count": 0,
            "degraded_pops": [],
            "origin_offload": 0.0,
            "hit_latency_ms": 0.0,
            "miss_latency_ms": 0.0,
            "pass_latency_ms": 0.0,
            "http2": 0,
            "http3": 0,
            "ipv6": 0,
            "tls_v12": 0,
            "tls_v13": 0,
            "h2_pct": 0.0,
            "h3_pct": 0.0,
            "ipv6_pct": 0.0,
            "tls12_pct": 0.0,
            "tls13_pct": 0.0,
            "ddos_action_blackhole": 0,
            "ddos_action_tarpit": 0,
            "ddos_action_close": 0,
            "ddos_action_downgrade": 0,
            "ddos_detect": 0,
            "ddos_mitigate": 0,
            "status_detail": {},
            "object_size_distribution": {},
            "origin_fetches": 0,
            "origin_revalidations": 0,
            "origin_cache_fetches": 0,
            "shield_hit_requests": 0,
            "shield_miss_requests": 0,
            "shield_revalidations": 0,
            "shield_fetch_body_bytes": 0,
            "request_collapse_usable": 0,
            "request_collapse_unusable": 0,
            "segblock_origin_fetches": 0,
            "segblock_shield_fetches": 0,
            "bot_challenge_starts": 0,
            "bot_challenges_issued": 0,
            "bot_challenges_succeeded": 0,
            "bot_challenges_failed": 0,
            "bot_detected": 0,
            "bot_verified": 0,
            "bot_ai_crawlers": 0,
            "compute_exec_time_ms": 0.0,
            "compute_req_time_ms": 0.0,
            "compute_ram_used": 0,
            "compute_bereq_errors": 0,
            "compute_guest_errors": 0,
            "compute_resource_exceeded": 0,
            "restarts": 0,
            "vcl_recv_count": 0,
            "vcl_recv_time_ms": 0.0,
            "vcl_fetch_count": 0,
            "vcl_fetch_time_ms": 0.0,
            "vcl_deliver_count": 0,
            "vcl_deliver_time_ms": 0.0,
            "vcl_error_count": 0,
            "vcl_error_time_ms": 0.0,
            "miss_histogram": {},
            "top_pops": [],
            "all_pops": {},
        },
        "aggregate_delay": 0,
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

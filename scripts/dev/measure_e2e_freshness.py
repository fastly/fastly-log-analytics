#!/usr/bin/env python3
"""Measure edge-to-serving visibility and dashboard latency across Fastly Log Analytics environments."""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.lib.admin_tls import admin_origin, admin_urlopen, validate_admin_tls_config  # noqa: E402
from scripts.lib.environments import parse_iso_utc  # noqa: E402
from scripts.lib.readiness import (  # noqa: E402, F401
    ActiveTrafficStreamer,
    _NoProbeRedirect,
    _normalize_probe_key,
    _spoof_ip,
    _urlopen_status,
    compute_lag_statistics,
    configured_polling,
    correlated_vitals_value,
    cron_completion_time,
    escape_sql_literal,
    new_terminal_run,
    parse_iso,
    print_active_scorecard,
    print_burst_scorecard,
    rum_beacon_count,
    seconds_between,
    send_edge_request_probe,
    send_edge_rum_probe,
    summarize_correlated,
)
from scripts.lib.section_measurements import BUDGETS, describe_response  # noqa: E402

# fmt: off
ENVIRONMENTS = {"local-std": "Local Standard", "remote-std": "Remote Standard", "remote-hs": "Remote High-Scale"}
DEFAULT_ENV_CONFIGS: dict[str, dict[str, str]] = {}


def _load_persisted_deploy_env() -> None:
    if "ADMIN_GATEWAY_ENDPOINTS" in os.environ:
        return
    p = Path(os.environ.get("FLA_ADMIN_DEPLOY_ENV", str(Path.home() / ".config/fastly-log-analytics/admin/deploy.env")))
    if p.is_file():
        try:
            from dotenv import dotenv_values
            for k, v in dotenv_values(p).items():
                if v is not None and k not in os.environ:
                    os.environ[k] = v
        except Exception:
            pass


def load_environment_config(env_key: str, environ: dict[str, str] | None = None) -> dict[str, str]:
    prefix = f"FLA_FRESHNESS_{env_key.upper().replace('-', '_')}_"
    src = environ if environ is not None else (_load_persisted_deploy_env() or os.environ)
    settings = {k.lower(): src.get(f"{prefix}{k}", "").strip() for k in ("SERVICE_ID", "BACKEND_URL", "CDN_DOMAIN")}
    if environ is None:
        defaults = DEFAULT_ENV_CONFIGS.get(env_key, {})
        settings["backend_url"] = settings["backend_url"] or admin_origin(env_key) or defaults.get("backend_url", "")
        for k in ("service_id", "cdn_domain"):
            settings[k] = settings[k] or defaults.get(k, "")
    if missing := [k for k, v in settings.items() if not v]:
        raise ValueError(f"Missing environment configuration: {', '.join(f'{prefix}{k.upper()}' for k in missing)}")
    for k in ("backend_url", "cdn_domain"):
        if not settings[k].startswith(("http://", "https://")):
            raise ValueError(f"{prefix}{k.upper()} must be an absolute HTTP(S) URL")
    return {**settings, "name": ENVIRONMENTS[env_key]}


def _http_json(url: str, method: str = "GET", body: dict | None = None, headers: dict[str, str] | None = None, timeout: float = 35.0) -> tuple[int, Any]:
    req_headers = {"Accept": "application/json", "User-Agent": "FLA-FreshnessProbe/1.0", **(headers or {}), **({"Content-Type": "application/json"} if method == "POST" else {})}
    data = json.dumps(body if body is not None else {}).encode("utf-8") if method == "POST" else None
    try:
        with admin_urlopen(urllib.request.Request(url, data=data, headers=req_headers, method=method), timeout=timeout) as r:
            return r.status, json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode("utf-8"))
        except Exception:
            return e.code, None
    except Exception as e:
        return 0, str(e)


def http_get(url: str, headers: dict[str, str] | None = None, timeout: float = 35.0) -> tuple[int, Any]:
    return _http_json(url, "GET", None, headers, timeout)


def http_post(url: str, body: dict | None = None, headers: dict[str, str] | None = None, timeout: float = 35.0) -> tuple[int, Any]:
    return _http_json(url, "POST", body, headers, timeout)


def send_correlated_probe(url: str, stream: str, probe_value: float | str | None = None) -> int:
    body = None if stream == "request" else ({"measurements": [{"type": "web-vitals", "values": {"LCP": float(probe_value or 1250.0)}}]} if stream == "vitals" else {"exceptions": [{"type": "FreshnessProbe", "value": str(probe_value or "freshness probe")}]})
    req = urllib.request.Request(url, data=json.dumps(body).encode() if body is not None else None, headers={"User-Agent": "FLA-FreshnessProbe/1.0", "Content-Type": "application/json"})
    return _urlopen_status(req, timeout=5, opener=urllib.request.build_opener(_NoProbeRedirect()))


def poll_correlated_stream(backend_url: str, service_id: str, stream: str, probes: list[dict[str, Any]], high_scale: bool = False) -> dict[str, Any]:
    dataset, column = {"request": ("logs", "url"), "vitals": ("client_vitals", "metric_value"), "errors": ("client_errors", "error_message")}[stream]
    keys = ", ".join(str(int(p["key"])) for p in probes) if stream == "vitals" else ", ".join(f"'{escape_sql_literal(p['key'])}'" for p in probes)
    metric = " AND metric_name = 'LCP'" if stream == "vitals" else ""
    sql = f"SELECT {column} AS probe_key, MIN(timestamp) AS timestamp, COUNT(*) AS matches FROM {dataset} WHERE {column} IN ({keys}) AND timestamp >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR AND timestamp <= CURRENT_TIMESTAMP + INTERVAL 1 MINUTE{metric} GROUP BY {column} LIMIT 100"
    started = time.monotonic()
    status, payload = http_post(f"{backend_url.rstrip('/')}/api/query?{urllib.parse.urlencode({'service_id': service_id})}", {"sql": sql, "dataset": dataset, "max_rows": 100}, {"x-fastly-service-id": service_id}, timeout=5)
    ended = time.monotonic()
    observation = {"stream": stream, "started": started, "ended": ended, "http_status": status, "error": None, "cache_state": "unknown", "negative_observation_current": False}
    rows = payload.get("data") if isinstance(payload, dict) else None
    if status != 200 or not isinstance(rows, list) or payload.get("truncated") is not False:
        return {**observation, "error": "query_failed_or_incomplete"}
    cache = describe_response(payload)["components"][0]
    current = cache["cache_state"] in {"miss", "bypass"} and not cache["execution_reused"]
    observation.update(cache_state=cache["cache_state"], negative_observation_current=current)
    expected, found = {p["key"] for p in probes}, {}
    for row in rows:
        if isinstance(row, dict):
            row = {**row, "probe_key": _normalize_probe_key(row.get("probe_key"), stream)}
        if not isinstance(row, dict) or not isinstance(row.get("probe_key"), str) or row.get("probe_key") not in expected or row["probe_key"] in found or row.get("matches") != 1 or parse_iso_utc(row.get("timestamp")) is None:
            return {**observation, "error": "invalid_or_ambiguous_correlation"}
        found[row["probe_key"]] = row
    for probe in probes:
        if (row := found.get(probe["key"])) is None:
            if current:
                probe["last_absent"] = started
        else:
            probe.update(first_visible=ended, source_timestamp=row["timestamp"], first_positive_query_started=started)
    return observation


def measure_correlated_freshness(env_info: dict[str, str], edge_url: str, num_requests: int = 10, num_rum: int = 5, num_errors: int = 0, poll_interval_s: float = 10, max_wait_s: float = 120) -> dict[str, Any]:
    parsed, counts = urllib.parse.urlsplit(edge_url), (num_requests, num_rum, num_errors)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username is not None or parsed.password is not None or parsed.fragment or any(k in {"probe", "seq"} for k, _ in urllib.parse.parse_qsl(parsed.query)):
        raise ValueError("--url must be the approved absolute edge target, without credentials, fragment or probe keys")
    if any(n < 0 for n in counts) or not 1 <= sum(counts) <= 100 or not math.isfinite(poll_interval_s) or poll_interval_s < 10 or not math.isfinite(max_wait_s) or not sum(counts) + poll_interval_s <= max_wait_s <= 600:
        raise ValueError("Require 1–100 total probes, polling >=10s, and timeout between total probes + interval and 600s")
    marker = uuid.uuid4().hex
    status, settings = http_get(f"{env_info['backend_url'].rstrip('/')}/api/services?{urllib.parse.urlencode({'service_id': env_info['service_id']})}", {"x-fastly-service-id": env_info["service_id"]}, timeout=5)
    services = settings.get("services") if isinstance(settings, dict) else None
    service = next((s for s in (services if isinstance(services, list) else []) if isinstance(s, dict) and s.get("service_id") == env_info["service_id"]), None)
    if status != 200 or service is None:
        raise ValueError(f"correlated preflight failed (HTTP {status}); no traffic sent")
    high_scale = bool(service.get("is_high_scale"))
    if (num_rum or num_errors) and not service.get("rum_enabled"):
        raise ValueError("RUM is not enabled; no traffic sent")
    queue = [(stream, idx) for idx in range(max(counts)) for stream, count in zip(("request", "vitals", "errors"), counts, strict=True) if idx < count]
    probes, queries, started = [], [], time.monotonic()
    deadline, next_send, next_poll = started + max_wait_s, started, started + poll_interval_s
    while time.monotonic() < deadline:
        now = time.monotonic()
        if queue and now >= next_send:
            stream, idx = queue.pop(0)
            cid = f"{marker}-{stream}-{idx}"
            if stream == "request":
                query = urllib.parse.urlencode([*urllib.parse.parse_qsl(parsed.query), ("probe", marker), ("seq", idx)])
                url = urllib.parse.urlunsplit(parsed._replace(path=parsed.path or "/", query=query))
                key = urllib.parse.urlsplit(url).path + "?" + query
            elif stream == "vitals":
                value = correlated_vitals_value(marker, idx)
                url, key = urllib.parse.urlunsplit(parsed._replace(path="/rum-beacon", query=urllib.parse.urlencode({"cid": cid, "rum_metric_name": "LCP", "rum_metric_value": value}))), str(value)
            else:
                key = f"fla-probe-{cid}"
                url = urllib.parse.urlunsplit(parsed._replace(path="/rum-beacon", query=urllib.parse.urlencode({"cid": cid, "rum_error_message": key})))
            sent = time.monotonic()
            status = send_correlated_probe(url, stream, None if stream == "request" else key)
            probes.append({"stream": stream, "key": key, "sent": sent, "response": time.monotonic(), "http_status": status})
            next_send = time.monotonic() + 1
        if time.monotonic() >= next_poll:
            for stream in ("request", "vitals", "errors"):
                pending = [p for p in probes if p["stream"] == stream and p.get("first_visible") is None]
                if pending and time.monotonic() + 5 <= deadline:
                    queries.append(poll_correlated_stream(env_info["backend_url"], env_info["service_id"], stream, pending, high_scale))
            next_poll = time.monotonic() + poll_interval_s
        if not queue and all(p.get("first_visible") is not None for p in probes):
            break
        time.sleep(min(0.1, max(0, deadline - time.monotonic())))
    ended = time.monotonic()
    streams = summarize_correlated(probes, ended)
    for stream, requested in zip(("request", "vitals", "errors"), counts, strict=True):
        if streams[stream]["sent"] != requested:
            streams[stream].update(complete=False, p95_population_sufficient=False, p99_population_sufficient=False, target_assessment="incomplete_not_proven_failure")
    cache_gaps = ["query_cache_effects_unseparated"] if any(not q["negative_observation_current"] for q in queries) else []
    return {
        "qualification": "incomplete",
        "qualification_gaps": ["peak_conditions_not_verified", *[f"{stream}_population_incomplete_or_below_floor" for stream, s in streams.items() if not s["p95_population_sufficient"]], *(["query_errors_observed"] if any(q["error"] is not None for q in queries) else []), *cache_gaps],
        "measurement_contract": {
            "correlated_to_probe_marker": True, "measure": "monotonic_send_to_first_correlated_query_response_upper_bound",
            "source_timestamp": "Fastly time.start (request timestamp / RUM log_data.timestamp)",
            "event_clock_point_latency_s": None, "clock_uncertainty_s": None, "poll_interval_s": poll_interval_s,
            "poll_resolution": "minimum interval after sweep; actual per-query start/end bounds recorded",
            "query_window": "server now - 1 hour through server now + 1 minute", "query_timeout_s": 5, "query_row_cap": 100,
            "high_scale": high_scale, "aggregate_send_rps_cap": 1, "target_p95_s": BUDGETS["peak_freshness_p95_ms"] / 1000,
        },
        "unknowns": ["peak_load_not_verified", "event_clock_point_latency_unknown", "source_clock_window_clipping_unknown", *cache_gaps],
        "unsent": len(queue), "requested": dict(zip(("request", "vitals", "errors"), counts, strict=True)),
        "preflight_request_count": 1, "elapsed_s": ended - started, "query_count": len(queries),
        "query_errors": sum(q["error"] is not None for q in queries),
        "queries": [{**q, "started": q["started"] - started, "ended": q["ended"] - started} for q in queries], "streams": streams,
    }


def probe_analytics_pages(be_url: str, sid: str, auth_headers: dict[str, str], now_ref: datetime | None = None) -> dict[str, dict[str, Any]]:
    now_final = now_ref or datetime.now(UTC)
    page_probes: dict[str, tuple[str, str, dict[str, Any] | None]] = {
        "Header Badge": (f"{be_url}/api/log-extents?service_id={sid}", "GET", None),
        "Dashboard Bundle": (f"{be_url}/api/dashboard/bundle?service_id={sid}", "POST", {"range_token": "24h"}),
        "Performance / RUM": (f"{be_url}/api/performance/aggregates", "POST", {"filters": {}}),
        "Security Aggregates": (f"{be_url}/api/security/aggregates", "POST", {"filters": {}}),
        "Origin Timeseries": (f"{be_url}/api/origin/timeseries", "POST", {"filters": {}}),
        "Network Health": (f"{be_url}/api/network-health", "POST", {"filters": {}, "metric": "health_score", "bucket_seconds": 60, "top_n": 10}),
    }
    page_results = {}
    for page_name, (p_url, p_method, p_payload) in page_probes.items():
        t_start = time.perf_counter()
        st, res_data = http_get(p_url, auth_headers, timeout=15.0) if p_method == "GET" else http_post(p_url, p_payload, auth_headers, timeout=35.0)
        t_latency_ms = (time.perf_counter() - t_start) * 1000
        page_ts = parse_iso(res_data.get("latest_log_at") if "latest_log_at" in res_data else res_data["aggregates"].get("latest_log_at") if "aggregates" in res_data else res_data["timeseries"][-1].get("time") if res_data.get("timeseries") else res_data["series"][-1].get("time") if res_data.get("series") else None) if st == 200 and isinstance(res_data, dict) else None
        lag_s = (now_final - page_ts).total_seconds() if page_ts else None
        page_results[page_name] = {"status": st, "latency_ms": round(t_latency_ms, 1), "latest_log_at": page_ts.isoformat() if page_ts else None, "freshness_lag_s": round(lag_s, 1) if lag_s is not None else None}
    return page_results


def _measure_clock_uncertainty(be_url: str, hdrs: dict[str, str]) -> float | None:
    from email.utils import parsedate_to_datetime
    try:
        t0, u0 = time.perf_counter(), datetime.now(UTC)
        with admin_urlopen(urllib.request.Request(f"{be_url.rstrip('/')}/api/health", headers={"Accept": "application/json", **hdrs}), timeout=5.0) as r:
            rtt, u1, date_hdr = time.perf_counter() - t0, datetime.now(UTC), r.headers.get("Date")
        skew = max(0.0, abs(((u0 + (u1 - u0) / 2) - parsedate_to_datetime(date_hdr)).total_seconds() - 0.5) - 0.5) if date_hdr else 0.0
        return round(skew + rtt / 2, 2)
    except Exception:
        return None


def _sample_rum_health(be_url: str, sid: str, hdrs: dict[str, str], timeout: float = 5.0) -> tuple[int | None, datetime | None]:
    st, d = http_get(f"{be_url}/api/services/{sid}/rum/beacon-health", hdrs, timeout=timeout)
    if st != 200:
        st, d = http_get(f"{be_url}/api/{sid}/rum/beacon-health", hdrs, timeout=timeout)
    return (rum_beacon_count(d), parse_iso(d.get("last_beacon_time"))) if st == 200 and isinstance(d, dict) else (None, None)


def _sample_serving_state(be_url: str, sid: str, hdrs: dict[str, str], base_rows: int = 0, timeout_get: float = 6.0, timeout_post: float = 15.0) -> tuple[datetime | None, int, datetime | None, int]:
    s_le, d_le = http_get(f"{be_url}/api/log-extents?service_id={sid}", hdrs, timeout=timeout_get)
    s_ss, d_ss = http_get(f"{be_url}/api/sync-status?service_id={sid}", hdrs, timeout=timeout_get)
    s_db, d_db = http_post(f"{be_url}/api/dashboard/bundle?service_id={sid}", {"range_token": "24h"}, hdrs, timeout=timeout_post)
    cur_ts = parse_iso(d_le.get("latest_log_at") if s_le == 200 and isinstance(d_le, dict) else None)
    cur_rows = int(d_ss.get("local_rows") or 0) if s_ss == 200 and isinstance(d_ss, dict) else base_rows
    dash_ag = (d_db.get("aggregates") or {}) if s_db == 200 and isinstance(d_db, dict) else {}
    return cur_ts, cur_rows, parse_iso(dash_ag.get("latest_log_at")), int(dash_ag.get("total_rows") or 0)


def measure_active_traffic_benchmark(targets: list[str], duration_s: float = 30.0, rps: float = 10.0, sample_interval_s: float = 2.0) -> list[dict[str, Any]]:
    contexts: dict[str, dict[str, Any]] = {}
    for tgt in targets:
        try:
            env_info = load_environment_config(tgt)
            sid, be_url, cdn = env_info["service_id"], env_info["backend_url"], env_info["cdn_domain"]
            auth_headers = {"x-fastly-service-id": sid}
            svc_status, svc_data = http_get(f"{be_url}/api/services?service_id={urllib.parse.quote(sid)}", auth_headers)
            if svc_status != 200:
                raise ValueError(f"services query failed (HTTP {svc_status}): {svc_data}")
            svc_info = next(e for e in svc_data["services"] if e.get("service_id") == sid)
            base_latest_ts, base_rows, _, base_dash_rows = _sample_serving_state(be_url, sid, auth_headers, 0, 5.0, 20.0)
            clock_unc = _measure_clock_uncertainty(be_url, auth_headers)
            marker = f"act-{uuid.uuid4().hex[:6]}"
            contexts[tgt] = {"env": tgt, "name": env_info["name"], "service_id": sid, "backend_url": be_url, "cdn_domain": cdn, "auth_headers": auth_headers, "is_high_scale": bool(svc_info.get("is_high_scale")), "rum_enabled": bool(svc_info.get("rum_enabled")), "polling": configured_polling(svc_data, sid), "base_latest_ts": base_latest_ts, "base_rows": base_rows, "base_dash_rows": base_dash_rows, "clock_uncertainty_s": clock_unc, "marker": marker, "streamer": ActiveTrafficStreamer(cdn, marker, rps=rps), "samples": []}
        except Exception as exc:
            contexts[tgt] = {"env": tgt, "name": ENVIRONMENTS.get(tgt, tgt), "error": str(exc)}

    active_targets = [tgt for tgt, c in contexts.items() if "streamer" in c]
    for tgt in active_targets:
        contexts[tgt]["streamer"].start()

    start_mono, deadline = time.perf_counter(), time.perf_counter() + duration_s
    while time.perf_counter() < deadline:
        time.sleep(sample_interval_s)
        now_utc, elapsed = datetime.now(UTC), time.perf_counter() - start_mono

        def _sample_target(tgt: str) -> dict[str, Any]:
            c = contexts[tgt]
            cur_ts, cur_rows, dash_ts, dash_rows = _sample_serving_state(c["backend_url"], c["service_id"], c["auth_headers"], c["base_rows"])
            head_lag = (now_utc - cur_ts).total_seconds() if cur_ts else None
            dash_lag = (now_utc - dash_ts).total_seconds() if dash_ts else None
            sample = {"elapsed_s": round(elapsed, 1), "timestamp_utc": now_utc.isoformat(), "header_latest_ts": cur_ts, "header_latest_log_at": cur_ts.isoformat() if cur_ts else None, "header_lag_s": round(head_lag, 1) if head_lag is not None else None, "dashboard_latest_ts": dash_ts, "dashboard_latest_log_at": dash_ts.isoformat() if dash_ts else None, "dashboard_lag_s": round(dash_lag, 1) if dash_lag is not None else None, "local_rows": cur_rows, "dash_rows": dash_rows}
            c["samples"].append(sample)
            return {"target": tgt, "sample": sample}

        with ThreadPoolExecutor(max_workers=max(1, len(active_targets))) as pool:
            list(pool.map(_sample_target, active_targets))

    for tgt in active_targets:
        contexts[tgt]["streamer"].stop()

    now_final, results = datetime.now(UTC), []
    for tgt, c in contexts.items():
        if "error" in c:
            results.append(c)
            continue
        stats = compute_lag_statistics(c["samples"], c["base_latest_ts"])
        page_matrix = probe_analytics_pages(c["backend_url"], c["service_id"], c["auth_headers"], now_ref=now_final)
        cur_rows = c["samples"][-1]["local_rows"] if c["samples"] else c["base_rows"]
        dash_rows = c["samples"][-1]["dash_rows"] if c["samples"] else c["base_dash_rows"]
        head_stats, dash_stats = stats["header_lag"], stats["dashboard_lag"]
        clock_unc = c.get("clock_uncertainty_s")
        req_lat = next((s["elapsed_s"] for s in c["samples"] if (c["base_latest_ts"] and s.get("header_latest_ts") and s["header_latest_ts"] > c["base_latest_ts"]) or s.get("local_rows", 0) > c["base_rows"]), head_stats["avg"])
        rum_cnt, rum_ts = _sample_rum_health(c["backend_url"], c["service_id"], c["auth_headers"]) if c["rum_enabled"] else (None, None)
        rum_lag = round(max(0.0, (now_final - rum_ts).total_seconds()), 1) if rum_ts else page_matrix.get("Performance / RUM", {}).get("freshness_lag_s")
        unknowns = [*([] if req_lat is not None else ["per_event_latency_uncorrelated"]), *([] if (not c["rum_enabled"] or rum_lag is not None) else ["rum_freshness_unmeasured"]), *([] if clock_unc is not None else ["clock_uncertainty_unmeasured"])]
        results.append({
            "env": tgt, "name": c["name"], "service_id": c["service_id"], "cdn_domain": c["cdn_domain"],
            "mode": "active_continuous_traffic", "deployment_mode": "high_scale" if c["is_high_scale"] else "standard",
            "measurement": "request_watermark_recency", "qualification": "verified" if not unknowns else "incomplete",
            "unknowns": unknowns,
            "configured_polling": c["polling"],
            "active_benchmark": {
                "duration_seconds": duration_s, "target_rps": rps,
                "requests_sent": c["streamer"].requests_sent, "rum_beacons_sent": c["streamer"].rum_sent,
                "header_lag": head_stats, "dashboard_lag": dash_stats,
                "request_event_latency_s": req_lat, "rum_lag_s": rum_lag,
                "rum_beacons_recent": rum_cnt, "clock_uncertainty_s": clock_unc,
                "sample_count": stats["sample_count"], "total_samples": stats["total_samples"],
                "target_p95_s": stats["target_p95_s"], "target_status": stats["target_status"],
                "rows_added": max(0, cur_rows - c["base_rows"]), "dash_rows_added": max(0, dash_rows - c["base_dash_rows"]),
            },
            "wall_clock_delays": {
                "header_min_s": head_stats["min"], "header_max_s": head_stats["max"],
                "header_avg_s": head_stats["avg"], "header_p95_s": head_stats["p95"], "dashboard_avg_s": dash_stats["avg"],
            },
            "page_freshness_matrix": page_matrix,
        })
    print_active_scorecard(results)
    return results


def measure_environment(env_key: str, env_info: dict[str, str], num_requests: int = 10, num_rum: int = 5, max_wait_s: float = 60.0) -> dict[str, Any]:
    name, sid, be_url, cdn = env_info["name"], env_info["service_id"], env_info["backend_url"], env_info["cdn_domain"]
    auth_headers = {"x-fastly-service-id": sid}
    service_status, service_data = http_get(f"{be_url}/api/services?service_id={urllib.parse.quote(sid)}", auth_headers)
    if service_status != 200:
        raise ValueError(f"services settings request failed with HTTP {service_status}: {service_data}")
    polling = configured_polling(service_data, sid)
    service_info = next(entry for entry in service_data["services"] if entry.get("service_id") == sid)
    is_high_scale, rum_enabled = bool(service_info.get("is_high_scale")), bool(service_info.get("rum_enabled"))

    le_status, le_data = http_get(f"{be_url}/api/log-extents?service_id={sid}", auth_headers, timeout=5.0)
    sync_status_code, sync_data = http_get(f"{be_url}/api/sync-status?service_id={sid}", auth_headers, timeout=5.0)
    if le_status != 200 or not isinstance(le_data, dict):
        raise ValueError(f"log-extents baseline failed with HTTP {le_status}: {le_data}")
    if sync_status_code != 200 or not isinstance(sync_data, dict):
        raise ValueError(f"sync-status baseline failed with HTTP {sync_status_code}: {sync_data}")
    base_latest_ts, base_local_rows = parse_iso(le_data.get("latest_log_at")), int(sync_data.get("local_rows") or 0)

    cr_req_s, cr_req_data = http_get(f"{be_url}/api/cron-runs?service_id={sid}&task=log_discovery&per_page=1", auth_headers)
    rum_task = "rum_discovery" if is_high_scale else "rum_sync"
    cr_rum_s, cr_rum_data = http_get(f"{be_url}/api/cron-runs?service_id={sid}&task={rum_task}&per_page=1", auth_headers)
    if cr_req_s != 200 or not isinstance(cr_req_data, dict):
        raise ValueError(f"request cron baseline failed with HTTP {cr_req_s}: {cr_req_data}")
    if cr_rum_s != 200 or not isinstance(cr_rum_data, dict):
        raise ValueError(f"RUM cron baseline failed with HTTP {cr_rum_s}: {cr_rum_data}")
    base_req_run_id = cr_req_data["entries"][0]["id"] if cr_req_data.get("entries") else 0
    base_rum_run_id = cr_rum_data["entries"][0]["id"] if cr_rum_data.get("entries") else 0

    base_rum_beacons, _ = _sample_rum_health(be_url, sid, auth_headers)
    rum_health_available = rum_enabled and base_rum_beacons is not None

    d_status, d_data = http_post(f"{be_url}/api/dashboard/bundle?service_id={sid}", {"range_token": "24h"}, auth_headers, timeout=30.0)
    if d_status != 200 or not isinstance(d_data, dict):
        raise ValueError(f"dashboard baseline failed with HTTP {d_status}: {d_data}")
    aggs = d_data.get("aggregates") or {}
    base_d_rows, base_d_latest = int(aggs.get("total_rows") or 0), parse_iso(aggs.get("latest_log_at"))

    marker = f"prb-{uuid.uuid4().hex[:8]}"
    send_start_mono, send_start_utc = time.perf_counter(), datetime.now(UTC)
    with ThreadPoolExecutor(max_workers=8) as pool:
        for f in [*[pool.submit(send_edge_request_probe, cdn, marker, i) for i in range(num_requests)], *[pool.submit(send_edge_rum_probe, cdn, marker, i) for i in range(num_rum)]]:
            f.result()

    header_detected_at = cron_req_detected_at = cron_rum_detected_at = rum_serving_detected_at = dashboard_detected_at = None
    header_detected_utc = cron_req_completed_utc = cron_rum_completed_utc = rum_serving_detected_utc = dashboard_detected_utc = None
    cron_req_status = None
    final_rum_beacons, final_local_rows, final_d_rows = base_rum_beacons, base_local_rows, base_d_rows
    deadline = time.perf_counter() + max_wait_s

    while time.perf_counter() < deadline:
        time.sleep(1.0)
        elapsed_s, now_utc = time.perf_counter() - send_start_mono, datetime.now(UTC)
        s_le, data_le = http_get(f"{be_url}/api/log-extents?service_id={sid}", auth_headers, timeout=8.0)
        s_ss, data_ss = http_get(f"{be_url}/api/sync-status?service_id={sid}", auth_headers, timeout=8.0)
        cur_ts = parse_iso(data_le.get("latest_log_at") if s_le == 200 and isinstance(data_le, dict) else None)
        cur_rows = int(data_ss.get("local_rows") or 0) if s_ss == 200 and isinstance(data_ss, dict) else base_local_rows

        if header_detected_at is None and (cur_rows > base_local_rows or (cur_ts and cur_ts > (base_latest_ts or send_start_utc))):
            header_detected_at, header_detected_utc, final_local_rows = elapsed_s, now_utc, cur_rows

        if rum_health_available and rum_serving_detected_at is None:
            cur_rum_beacons, rum_ts = _sample_rum_health(be_url, sid, auth_headers, timeout=4.0)
            if cur_rum_beacons is not None and (cur_rum_beacons > (base_rum_beacons or 0) or (rum_ts and rum_ts > send_start_utc)):
                rum_serving_detected_at, rum_serving_detected_utc, final_rum_beacons = elapsed_s, now_utc, cur_rum_beacons

        if cron_req_detected_at is None:
            s_cr, d_cr = http_get(f"{be_url}/api/cron-runs?service_id={sid}&task=log_discovery&per_page=1", auth_headers, timeout=4.0)
            if (top_entry := new_terminal_run(d_cr, base_req_run_id) if s_cr == 200 else None) is not None:
                cron_req_detected_at, cron_req_completed_utc, cron_req_status = elapsed_s, cron_completion_time(top_entry), top_entry["status"]

        if cron_rum_detected_at is None:
            s_rm, d_rm = http_get(f"{be_url}/api/cron-runs?service_id={sid}&task={rum_task}&per_page=1", auth_headers, timeout=4.0)
            if (top_entry := new_terminal_run(d_rm, base_rum_run_id) if s_rm == 200 else None) is not None:
                cron_rum_detected_at, cron_rum_completed_utc = elapsed_s, cron_completion_time(top_entry)

        if dashboard_detected_at is None and (header_detected_at is not None or cron_req_detected_at is not None or int(elapsed_s) % 4 == 0):
            s_d, data_d = http_post(f"{be_url}/api/dashboard/bundle?service_id={sid}", {"range_token": "24h"}, auth_headers, timeout=20.0)
            if s_d == 200 and isinstance(data_d, dict):
                ag = data_d.get("aggregates") or {}
                cur_d_rows, cur_d_ts = int(ag.get("total_rows") or 0), parse_iso(ag.get("latest_log_at"))
                if cur_d_rows > base_d_rows or (cur_d_ts and cur_d_ts > (base_d_latest or send_start_utc)):
                    dashboard_detected_at, dashboard_detected_utc, final_d_rows = elapsed_s, now_utc, cur_d_rows

        if header_detected_at is not None and cron_req_detected_at is not None and dashboard_detected_at is not None and (not rum_health_available or rum_serving_detected_at is not None):
            break

    page_results = probe_analytics_pages(be_url, sid, auth_headers, now_ref=datetime.now(UTC))
    return {
        "env": env_key, "name": name, "service_id": sid, "cdn_domain": cdn, "sent_at_utc": send_start_utc.isoformat(),
        "measurement_contract": {"source_timestamp": "probe_send_time", "fos_last_modified_measured": False, "serving_detection": "aggregate row count or latest event timestamp advanced", "correlated_to_probe_marker": False},
        "configured_polling": polling, "deployment_mode": "high_scale" if is_high_scale else "standard", "rum_enabled": rum_enabled,
        "wall_clock_delays": {
            "header_seconds": round(header_detected_at, 1) if header_detected_at else None,
            "cron_request_seconds": round(cron_req_detected_at, 1) if cron_req_detected_at else None,
            "cron_rum_seconds": round(cron_rum_detected_at, 1) if cron_rum_detected_at else None,
            "dashboard_bundle_seconds": round(dashboard_detected_at, 1) if dashboard_detected_at else None,
        },
        "observed_discovery_to_serving_seconds": {
            "request_discovery_status": cron_req_status, "includes_high_scale_queue_and_publication_delay": is_high_scale,
            "request_discovery_to_header": seconds_between(header_detected_utc, cron_req_completed_utc),
            "request_discovery_to_dashboard": seconds_between(dashboard_detected_utc, cron_req_completed_utc),
            "rum_discovery_to_serving": seconds_between(rum_serving_detected_utc, cron_rum_completed_utc),
        },
        "row_deltas": {
            "total_rows_added": final_local_rows - base_local_rows, "dashboard_rows_added": final_d_rows - base_d_rows,
            "rum_beacons_added": final_rum_beacons - base_rum_beacons if final_rum_beacons is not None and base_rum_beacons is not None else None,
        },
        "page_freshness_matrix": page_results,
    }


def main():
    validate_admin_tls_config()
    parser = argparse.ArgumentParser(description="Measure edge-probe-to-active-serving visibility and delay")
    for flag, kw in (
        ("--env", {"type": str, "default": "all"}), ("--active", {"action": "store_true", "default": True}),
        ("--burst", {"action": "store_true"}), ("--duration", {"type": float, "default": 30.0}),
        ("--rps", {"type": float, "default": 10.0}), ("--interval", {"type": float, "default": 2.0}),
        ("--requests", {"type": int, "default": 10}), ("--rum-beacons", {"type": int, "default": 5}),
        ("--timeout", {"type": float, "default": 45.0}), ("--correlated", {"action": "store_true"}),
        ("--url", {}), ("--rum-errors", {"type": int, "default": 0}),
        ("--correlated-interval", {"type": float, "default": 10}),
        ("--output", {"type": str, "default": "reports/deploys/current/relics/e2e_freshness_report.json"}),
    ):
        parser.add_argument(flag, **kw)
    args = parser.parse_args()
    _load_persisted_deploy_env()

    targets = list(ENVIRONMENTS.keys()) if args.env == "all" else ([t.strip() for t in args.env.split(",") if t.strip()] if "," in args.env else [args.env])
    if args.correlated:
        if len(targets) != 1 or not args.url or args.burst:
            parser.error("--correlated requires exactly one --env and an explicit approved --url, without --burst")
        results = [measure_correlated_freshness(load_environment_config(targets[0]), args.url, args.requests, args.rum_beacons, args.rum_errors, args.correlated_interval, args.timeout)]
    elif args.burst:
        results = []
        for tgt in targets:
            try:
                res = measure_environment(tgt, load_environment_config(tgt), num_requests=args.requests, num_rum=args.rum_beacons, max_wait_s=args.timeout)
            except (ValueError, KeyError) as exc:
                res = {"env": tgt, "name": ENVIRONMENTS.get(tgt, tgt), "error": str(exc)}
            results.append(res)
        print_burst_scorecard(results)
    else:
        results = measure_active_traffic_benchmark(targets=targets, duration_s=args.duration, rps=args.rps, sample_interval_s=args.interval)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(out_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(results, f, indent=2, default=str)


if __name__ == "__main__":
    main()

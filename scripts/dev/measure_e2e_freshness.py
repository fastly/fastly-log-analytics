#!/usr/bin/env python3
"""Measure edge-to-serving visibility and dashboard latency across Fastly Log Analytics environments."""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from backend.utils.sql_validator import escape_sql_literal  # noqa: E402
from scripts.lib.admin_tls import admin_origin, admin_urlopen, validate_admin_tls_config  # noqa: E402
from scripts.lib.environments import parse_iso_utc  # noqa: E402
from scripts.lib.section_measurements import BUDGETS  # noqa: E402

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
        if not settings["backend_url"]:
            settings["backend_url"] = admin_origin(env_key) or defaults.get("backend_url", "")
        for k in ("service_id", "cdn_domain"):
            if not settings[k] and defaults.get(k):
                settings[k] = defaults[k]
    if missing := [k for k, v in settings.items() if not v]:
        raise ValueError(f"Missing environment configuration: {', '.join(f'{prefix}{k.upper()}' for k in missing)}")
    for k in ("backend_url", "cdn_domain"):
        if not settings[k].startswith(("http://", "https://")):
            raise ValueError(f"{prefix}{k.upper()} must be an absolute HTTP(S) URL")
    settings["name"] = ENVIRONMENTS[env_key]
    return settings


def configured_polling(settings_response: Any, service_id: str) -> dict[str, Any]:
    services = settings_response.get("services") if isinstance(settings_response, dict) else None
    if not isinstance(services, list):
        raise ValueError("services response is missing its service list")
    service = next((e for e in services if isinstance(e, dict) and e.get("service_id") == service_id), None)
    if service is None:
        raise ValueError("requested service was not returned by the services endpoint")
    cron_sync = service.get("cron_sync")
    if cron_sync is None:
        cron_sync = {}
    if not isinstance(cron_sync, dict):
        raise ValueError("service cron_sync settings are invalid")
    mode = cron_sync.get("polling_mode", "adaptive")
    if mode not in {"regular", "adaptive"}:
        raise ValueError("service polling_mode must be regular or adaptive")
    log_period = int(service.get("log_period") or 60)
    if interval_mins := cron_sync.get("interval_mins"):
        interval, interval_source = max(5, int(interval_mins) * 60), "interval_mins"
    elif interval_seconds := cron_sync.get("interval_seconds"):
        interval, interval_source = max(5, int(interval_seconds)), "interval_seconds"
    else:
        interval, interval_source = max(5, log_period // 2 if log_period >= 60 else log_period), "log_period"
    return {
        "mode": mode, "interval_seconds": interval, "interval_source": interval_source,
        "adaptive_followup_interval_seconds": 3 if mode == "adaptive" else None,
        "adaptive_max_followups": 2 if mode == "adaptive" else 0,
    }


def cron_completion_time(entry: Any) -> datetime | None:
    if not isinstance(entry, dict) or (started_at := parse_iso(entry.get("started_at"))) is None:
        return None
    try:
        return started_at + timedelta(seconds=float(entry["duration_s"]))
    except (KeyError, TypeError, ValueError):
        return None


def new_terminal_run(response: Any, baseline_run_id: int) -> dict[str, Any] | None:
    entries = response.get("entries") if isinstance(response, dict) else None
    if not isinstance(entries, list) or not entries or not isinstance(entries[0], dict):
        return None
    entry = entries[0]
    try:
        run_id = int(entry["id"])
    except (KeyError, TypeError, ValueError):
        return None
    return entry if run_id > baseline_run_id and entry.get("status") in {"success", "warning", "error"} else None


def seconds_between(later: datetime | None, earlier: datetime | None) -> float | None:
    return None if later is None or earlier is None else round((later - earlier).total_seconds(), 1)


def rum_beacon_count(response: Any) -> int | None:
    if not isinstance(response, dict):
        return None
    count = response.get("beacons") if response.get("beacons") is not None else response.get("recent_beacons")
    try:
        return int(count) if count is not None else None
    except (TypeError, ValueError):
        return None


def parse_iso(ts_str: str | None) -> datetime | None:
    return parse_iso_utc(ts_str)


def _http_json(url: str, method: str = "GET", body: dict | None = None, headers: dict[str, str] | None = None, timeout: float = 35.0) -> tuple[int, Any]:
    req_headers = {"Accept": "application/json", "User-Agent": "FLA-FreshnessProbe/1.0", **(headers or {})}
    data = None
    if method == "POST":
        req_headers["Content-Type"] = "application/json"
        data = json.dumps(body if body is not None else {}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=req_headers, method=method)
    try:
        with admin_urlopen(req, timeout=timeout) as r:
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


def _spoof_ip() -> str:
    return f"{random.randint(12, 223)}.{random.randint(1, 254)}.{random.randint(1, 254)}.{random.randint(1, 254)}"


def _urlopen_status(req: urllib.request.Request, timeout: float = 8, opener: Any = None) -> int:
    try:
        fn = opener.open if opener else urllib.request.urlopen
        with fn(req, timeout=timeout) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return 0


def send_edge_request_probe(cdn_domain: str, marker: str, idx: int) -> int:
    url = f"{cdn_domain.rstrip('/')}/test-freshness?probe={marker}&seq={idx}"
    hdrs = {"User-Agent": f"FLA-Probe-Agent/1.0 ({marker})", "X-Source-Ip": _spoof_ip(), "Fastly-Request-ID": f"req-{marker}-{idx:04d}"}
    return _urlopen_status(urllib.request.Request(url, headers=hdrs))


def send_edge_rum_probe(cdn_domain: str, marker: str, idx: int) -> int:
    url = f"{cdn_domain.rstrip('/')}/rum-beacon?cid={marker}-{idx}&rum_metric_name=LCP&rum_metric_value=1250&rum_pathname=%2Ftest-freshness"
    faro_payload = {
        "meta": {"browser": {"name": "Chrome", "mobile": False}, "os": {"name": "macOS"}, "page": {"url": f"{cdn_domain}/test-freshness"}},
        "measurements": [{"type": "web-vitals", "values": {"LCP": 1250.0}, "context": {"rating": "good"}}],
        "events": [{"name": "freshness_test_click", "timestamp": datetime.now(UTC).isoformat(), "attributes": {"probe": marker}}],
    }
    hdrs = {"Content-Type": "application/json", "User-Agent": f"FLA-RUM-Probe/1.0 ({marker})", "X-Source-Ip": _spoof_ip()}
    return _urlopen_status(urllib.request.Request(url, data=json.dumps(faro_payload).encode("utf-8"), headers=hdrs, method="POST"))


class _NoProbeRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> None:
        return None


def send_correlated_probe(url: str, stream: str, probe_value: float | str | None = None) -> int:
    body = None
    if stream != "request":
        body = (
            {"measurements": [{"type": "web-vitals", "values": {"LCP": float(probe_value or 1250.0)}}]}
            if stream == "vitals"
            else {"exceptions": [{"type": "FreshnessProbe", "value": str(probe_value or "freshness probe")}]}
        )
    req = urllib.request.Request(
        url, data=json.dumps(body).encode() if body is not None else None,
        headers={"User-Agent": "FLA-FreshnessProbe/1.0", "Content-Type": "application/json"},
    )
    return _urlopen_status(req, timeout=5, opener=urllib.request.build_opener(_NoProbeRedirect()))


def _normalize_probe_key(value: Any, stream: str) -> Any:
    if stream != "vitals" or isinstance(value, bool):
        return value
    try:
        number = float(value)
    except (TypeError, ValueError):
        return value
    return str(int(number)) if number.is_integer() else value


def correlated_vitals_value(marker: str, idx: int) -> int:
    return 10_000_000 + (int(marker[:6], 16) % 1_000_000) * 100 + idx


def poll_correlated_stream(backend_url: str, service_id: str, stream: str, probes: list[dict[str, Any]], high_scale: bool = False) -> dict[str, Any]:
    dataset, column = {"request": ("logs", "url"), "vitals": ("client_vitals", "metric_value"), "errors": ("client_errors", "error_message")}[stream]
    keys = ", ".join(str(int(p["key"])) for p in probes) if stream == "vitals" else ", ".join(f"'{escape_sql_literal(p['key'])}'" for p in probes)
    metric = " AND metric_name = 'LCP'" if stream == "vitals" else ""
    sql = (
        f"SELECT {column} AS probe_key, MIN(timestamp) AS timestamp, COUNT(*) AS matches "
        f"FROM {dataset} WHERE {column} IN ({keys}) "
        f"AND timestamp >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR AND timestamp <= CURRENT_TIMESTAMP + INTERVAL 1 MINUTE{metric} "
        f"GROUP BY {column} LIMIT 100"
    )
    started = time.monotonic()
    status, payload = http_post(
        f"{backend_url.rstrip('/')}/api/query?{urllib.parse.urlencode({'service_id': service_id})}",
        {"sql": sql, "dataset": dataset, "max_rows": 100}, {"x-fastly-service-id": service_id}, timeout=5,
    )
    ended = time.monotonic()
    observation = {"stream": stream, "started": started, "ended": ended, "http_status": status, "error": None}
    rows = payload.get("data") if isinstance(payload, dict) else None
    if status != 200 or not isinstance(rows, list) or payload.get("truncated") is not False:
        observation["error"] = "query_failed_or_incomplete"
        return observation
    expected, found = {p["key"] for p in probes}, {}
    for row in rows:
        if isinstance(row, dict):
            row = {**row, "probe_key": _normalize_probe_key(row.get("probe_key"), stream)}
        if (
            not isinstance(row, dict) or not isinstance(row.get("probe_key"), str)
            or row.get("probe_key") not in expected or row["probe_key"] in found
            or row.get("matches") != 1 or parse_iso_utc(row.get("timestamp")) is None
        ):
            observation["error"] = "invalid_or_ambiguous_correlation"
            return observation
        found[row["probe_key"]] = row
    for probe in probes:
        if (row := found.get(probe["key"])) is None:
            probe["last_absent"] = started
        else:
            probe["first_visible"] = ended
            probe["source_timestamp"] = row["timestamp"]
            probe["first_positive_query_started"] = started
    return observation


def summarize_correlated(probes: list[dict[str, Any]], ended: float) -> dict[str, Any]:
    streams = {}
    floor = BUDGETS["minimum_samples_per_state"]
    for stream in ("request", "vitals", "errors"):
        pop = [p for p in probes if p["stream"] == stream]
        visible = [p for p in pop if p.get("first_visible") is not None and 200 <= p["http_status"] < 300]
        bounds = sorted(p["first_visible"] - p["sent"] for p in visible)
        count = len(bounds)
        complete = bool(pop) and count == len(pop)
        p95 = bounds[math.ceil(count * 0.95) - 1] if count else None
        streams[stream] = {
            "sent": len(pop), "correlated": sum(p.get("first_visible") is not None for p in pop), "observed": count,
            "right_censored": sum(p.get("first_visible") is None for p in pop),
            "http_errors": sum(not 200 <= p["http_status"] < 300 for p in pop),
            "left_censored": sum(p.get("last_absent") is None for p in visible),
            "complete": complete, "p95_sample_floor": floor, "p95_population_sufficient": complete and count >= floor,
            "upper_bound_s": {"p95": p95, "p99": bounds[math.ceil(count * 0.99) - 1] if count else None, "max": max(bounds) if count else None},
            "p99_population_sufficient": complete and count >= 100,
            "target_assessment": (
                "within_target_upper_bound"
                if complete and count >= floor and p95 <= BUDGETS["peak_freshness_p95_ms"] / 1000
                else "incomplete_not_proven_failure"
            ),
            "samples": [
                {
                    "http_status": p["http_status"], "send_to_response_s": p["response"] - p["sent"],
                    "followup_duration_s": ended - p["sent"], "source_timestamp": p.get("source_timestamp"),
                    "event_to_queryable_upper_bound_s": p["first_visible"] - p["sent"] if p.get("first_visible") is not None else None,
                    "send_to_queryable_interval_s": [
                        p["last_absent"] - p["sent"] if p.get("last_absent") is not None else 0,
                        p["first_visible"] - p["sent"] if p.get("first_visible") is not None else None,
                    ],
                    "right_censored": p.get("first_visible") is None,
                    "left_censored": p.get("first_visible") is not None and p.get("last_absent") is None,
                }
                for p in pop
            ],
        }
    return streams


def measure_correlated_freshness(
    env_info: dict[str, str], edge_url: str, num_requests: int = 10, num_rum: int = 5,
    num_errors: int = 0, poll_interval_s: float = 10, max_wait_s: float = 120,
) -> dict[str, Any]:
    parsed = urllib.parse.urlsplit(edge_url)
    counts = (num_requests, num_rum, num_errors)
    if (
        parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username is not None
        or parsed.password is not None or parsed.fragment
        or any(k in {"probe", "seq"} for k, _ in urllib.parse.parse_qsl(parsed.query))
    ):
        raise ValueError("--url must be the approved absolute edge target, without credentials, fragment or probe keys")
    if (
        any(n < 0 for n in counts) or not 1 <= sum(counts) <= 100 or not math.isfinite(poll_interval_s)
        or poll_interval_s < 10 or not math.isfinite(max_wait_s) or not sum(counts) + poll_interval_s <= max_wait_s <= 600
    ):
        raise ValueError("Require 1–100 total probes, polling >=10s, and timeout between total probes + interval and 600s")
    marker = uuid.uuid4().hex
    status, settings = http_get(
        f"{env_info['backend_url'].rstrip('/')}/api/services?{urllib.parse.urlencode({'service_id': env_info['service_id']})}",
        {"x-fastly-service-id": env_info["service_id"]}, timeout=5,
    )
    services = settings.get("services") if isinstance(settings, dict) else None
    service = next((s for s in (services if isinstance(services, list) else []) if isinstance(s, dict) and s.get("service_id") == env_info["service_id"]), None)
    if status != 200 or service is None:
        raise ValueError(f"correlated preflight failed (HTTP {status}); no traffic sent")
    high_scale = bool(service.get("is_high_scale"))
    if (num_rum or num_errors) and not service.get("rum_enabled"):
        raise ValueError("RUM is not enabled; no traffic sent")
    queue = [(stream, idx) for idx in range(max(counts)) for stream, count in zip(("request", "vitals", "errors"), counts, strict=True) if idx < count]
    probes: list[dict[str, Any]] = []
    queries = []
    started = time.monotonic()
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
                query = urllib.parse.urlencode({"cid": cid, "rum_metric_name": "LCP", "rum_metric_value": value})
                url = urllib.parse.urlunsplit(parsed._replace(path="/rum-beacon", query=query))
                key = str(value)
            else:
                key = f"fla-probe-{cid}"
                query = urllib.parse.urlencode({"cid": cid, "rum_error_message": key})
                url = urllib.parse.urlunsplit(parsed._replace(path="/rum-beacon", query=query))
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
            streams[stream].update(
                complete=False, p95_population_sufficient=False,
                p99_population_sufficient=False, target_assessment="incomplete_not_proven_failure",
            )
    return {
        "qualification": "incomplete",
        "qualification_gaps": [
            "peak_conditions_not_verified",
            *[f"{stream}_population_incomplete_or_below_floor" for stream, s in streams.items() if not s["p95_population_sufficient"]],
            *(["query_errors_observed"] if any(q["error"] is not None for q in queries) else []),
        ],
        "measurement_contract": {
            "correlated_to_probe_marker": True,
            "measure": "monotonic_send_to_first_correlated_query_response_upper_bound",
            "source_timestamp": "Fastly time.start (request timestamp / RUM log_data.timestamp)",
            "event_clock_point_latency_s": None, "clock_uncertainty_s": None, "poll_interval_s": poll_interval_s,
            "poll_resolution": "minimum interval after sweep; actual per-query start/end bounds recorded",
            "query_window": "server now - 1 hour through server now + 1 minute",
            "query_timeout_s": 5, "query_row_cap": 100, "high_scale": high_scale, "aggregate_send_rps_cap": 1,
            "target_p95_s": BUDGETS["peak_freshness_p95_ms"] / 1000,
        },
        "unknowns": ["peak_load_not_verified", "event_clock_point_latency_unknown", "source_clock_window_clipping_unknown"],
        "unsent": len(queue), "requested": dict(zip(("request", "vitals", "errors"), counts, strict=True)),
        "preflight_request_count": 1, "elapsed_s": ended - started, "query_count": len(queries),
        "query_errors": sum(q["error"] is not None for q in queries),
        "queries": [{**q, "started": q["started"] - started, "ended": q["ended"] - started} for q in queries],
        "streams": streams,
    }


class ActiveTrafficStreamer:
    """Continuously dispatches edge HTTP requests and RUM beacons in parallel."""

    def __init__(self, cdn_domain: str, marker: str, rps: float = 10.0):
        self.cdn_domain = cdn_domain
        self.marker = marker
        self.rps = max(1.0, rps)
        self.stop_event = threading.Event()
        self.requests_sent = 0
        self.rum_sent = 0
        self.errors = 0
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self._thread:
            self._thread.join(timeout=4.0)

    def _run(self) -> None:
        interval, seq = 1.0 / self.rps, 0
        with ThreadPoolExecutor(max_workers=6) as pool:
            while not self.stop_event.is_set():
                t0 = time.perf_counter()
                seq += 1
                try:
                    pool.submit(self._send_req, seq)
                    if seq % 5 == 0:
                        pool.submit(self._send_rum, seq // 5)
                except Exception:
                    self.errors += 1
                if (sleep_time := interval - (time.perf_counter() - t0)) > 0:
                    time.sleep(sleep_time)

    def _send_req(self, seq: int) -> None:
        if send_edge_request_probe(self.cdn_domain, self.marker, seq) in (200, 204, 301, 302):
            self.requests_sent += 1
        else:
            self.errors += 1

    def _send_rum(self, seq: int) -> None:
        if send_edge_rum_probe(self.cdn_domain, self.marker, seq) in (200, 204):
            self.rum_sent += 1


def compute_lag_statistics(samples: list[dict[str, Any]], baseline_latest_ts: datetime | None = None, min_elapsed_s: float = 6.0) -> dict[str, Any]:
    """Summarize request-watermark recency, not individual event latency."""
    active_samples = [s for s in samples if s.get("header_latest_ts") and s["header_latest_ts"] > baseline_latest_ts] if baseline_latest_ts else []
    if not active_samples:
        active_samples = [s for s in samples if s.get("elapsed_s", 0) >= min_elapsed_s] or list(samples)
    head_lags = [s["header_lag_s"] for s in active_samples if s.get("header_lag_s") is not None]
    dash_lags = [s["dashboard_lag_s"] for s in active_samples if s.get("dashboard_lag_s") is not None]

    def _stats(vals: list[float]) -> dict[str, float | None]:
        if not vals:
            return {"min": None, "max": None, "avg": None, "p95": None, "p99": None}
        s = sorted(vals)
        return {"min": round(min(vals), 1), "max": round(max(vals), 1), "avg": round(sum(vals) / len(vals), 1), "p95": s[math.ceil(len(s) * 0.95) - 1], "p99": s[math.ceil(len(s) * 0.99) - 1]}

    header, dashboard = _stats(head_lags), _stats(dash_lags)
    target_p95_s = BUDGETS["peak_freshness_p95_ms"] / 1000
    header_p95, dashboard_p95 = header["p95"], dashboard["p95"]
    target_status = "UNMEASURED" if header_p95 is None or dashboard_p95 is None else ("ELEVATED" if max(header_p95, dashboard_p95) > target_p95_s else "WITHIN TARGET")
    return {
        "header_lag": header, "dashboard_lag": dashboard, "sample_count": len(active_samples),
        "total_samples": len(samples), "target_p95_s": target_p95_s, "target_status": target_status,
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
        page_ts = None
        if st == 200 and isinstance(res_data, dict):
            if "latest_log_at" in res_data:
                page_ts = parse_iso(res_data.get("latest_log_at"))
            elif "aggregates" in res_data:
                page_ts = parse_iso(res_data["aggregates"].get("latest_log_at"))
            elif "timeseries" in res_data and res_data["timeseries"]:
                page_ts = parse_iso(res_data["timeseries"][-1].get("time"))
            elif "series" in res_data and res_data["series"]:
                page_ts = parse_iso(res_data["series"][-1].get("time"))
        lag_s = (now_final - page_ts).total_seconds() if page_ts else None
        page_results[page_name] = {
            "status": st, "latency_ms": round(t_latency_ms, 1),
            "latest_log_at": page_ts.isoformat() if page_ts else None,
            "freshness_lag_s": round(lag_s, 1) if lag_s is not None else None,
        }
    return page_results


def measure_active_traffic_benchmark(targets: list[str], duration_s: float = 30.0, rps: float = 10.0, sample_interval_s: float = 2.0) -> list[dict[str, Any]]:
    print(f"\n{'=' * 96}\n  STARTING ACTIVE CONTINUOUS TRAFFIC BENCHMARK ({rps:.0f} RPS, duration={duration_s:.0f}s)\n  Targets: {', '.join(targets)}\n{'=' * 96}\n", flush=True)
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
            polling = configured_polling(svc_data, sid)
            le_status, le_data = http_get(f"{be_url}/api/log-extents?service_id={sid}", auth_headers, timeout=5.0)
            sync_s, sync_data = http_get(f"{be_url}/api/sync-status?service_id={sid}", auth_headers, timeout=5.0)
            d_s, d_data = http_post(f"{be_url}/api/dashboard/bundle?service_id={sid}", {"range_token": "24h"}, auth_headers, timeout=20.0)
            base_latest_ts = parse_iso(le_data.get("latest_log_at") if le_status == 200 and isinstance(le_data, dict) else None)
            base_rows = int(sync_data.get("local_rows") or 0) if sync_s == 200 and isinstance(sync_data, dict) else 0
            base_dash_rows = int((d_data.get("aggregates") or {}).get("total_rows") or 0) if d_s == 200 and isinstance(d_data, dict) else 0
            marker = f"act-{uuid.uuid4().hex[:6]}"
            contexts[tgt] = {
                "env": tgt, "name": env_info["name"], "service_id": sid, "backend_url": be_url, "cdn_domain": cdn,
                "auth_headers": auth_headers, "is_high_scale": bool(svc_info.get("is_high_scale")),
                "rum_enabled": bool(svc_info.get("rum_enabled")), "polling": polling, "base_latest_ts": base_latest_ts,
                "base_rows": base_rows, "base_dash_rows": base_dash_rows, "marker": marker,
                "streamer": ActiveTrafficStreamer(cdn, marker, rps=rps), "samples": [],
            }
            print(f"  ✓ [{env_info['name']}] Baseline established (Latest: {base_latest_ts}, Rows: {base_rows:,}). Streamer initialized.", flush=True)
        except Exception as exc:
            print(f"  ✗ [{tgt}] Setup failed: {exc}", flush=True)
            contexts[tgt] = {"env": tgt, "name": ENVIRONMENTS.get(tgt, tgt), "error": str(exc)}

    active_targets = [tgt for tgt, c in contexts.items() if "streamer" in c]
    for tgt in active_targets:
        contexts[tgt]["streamer"].start()
    print(f"\n🚀 Continuous traffic streaming at {rps:.0f} RPS per target! Sampling every {sample_interval_s:.1f}s...\n", flush=True)

    start_mono = time.perf_counter()
    deadline = start_mono + duration_s
    while time.perf_counter() < deadline:
        time.sleep(sample_interval_s)
        now_utc = datetime.now(UTC)
        elapsed = time.perf_counter() - start_mono

        def _sample_target(tgt: str) -> dict[str, Any]:
            c = contexts[tgt]
            sid, be, hdrs = c["service_id"], c["backend_url"], c["auth_headers"]
            s_le, d_le = http_get(f"{be}/api/log-extents?service_id={sid}", hdrs, timeout=6.0)
            s_ss, d_ss = http_get(f"{be}/api/sync-status?service_id={sid}", hdrs, timeout=6.0)
            s_db, d_db = http_post(f"{be}/api/dashboard/bundle?service_id={sid}", {"range_token": "24h"}, hdrs, timeout=15.0)
            cur_ts = parse_iso(d_le.get("latest_log_at") if s_le == 200 and isinstance(d_le, dict) else None)
            cur_rows = int(d_ss.get("local_rows") or 0) if s_ss == 200 and isinstance(d_ss, dict) else c["base_rows"]
            dash_ag = d_db.get("aggregates") or {} if s_db == 200 and isinstance(d_db, dict) else {}
            dash_ts = parse_iso(dash_ag.get("latest_log_at"))
            dash_rows = int(dash_ag.get("total_rows") or 0)
            head_lag = (now_utc - cur_ts).total_seconds() if cur_ts else None
            dash_lag = (now_utc - dash_ts).total_seconds() if dash_ts else None
            sample = {
                "elapsed_s": round(elapsed, 1), "timestamp_utc": now_utc.isoformat(),
                "header_latest_ts": cur_ts, "header_latest_log_at": cur_ts.isoformat() if cur_ts else None,
                "header_lag_s": round(head_lag, 1) if head_lag is not None else None,
                "dashboard_latest_ts": dash_ts, "dashboard_latest_log_at": dash_ts.isoformat() if dash_ts else None,
                "dashboard_lag_s": round(dash_lag, 1) if dash_lag is not None else None,
                "local_rows": cur_rows, "dash_rows": dash_rows,
            }
            c["samples"].append(sample)
            return {"target": tgt, "sample": sample}

        with ThreadPoolExecutor(max_workers=max(1, len(active_targets))) as pool:
            sample_results = list(pool.map(_sample_target, active_targets))
        parts = [f"{contexts[r['target']]['name']}: {(str(round(r['sample']['header_lag_s'], 1)) + 's' if r['sample']['header_lag_s'] is not None else '—'):>5} lag" for r in sample_results]
        print(f"  [{now_utc.strftime('%H:%M:%S')}] (+{elapsed:4.1f}s) " + " │ ".join(parts), flush=True)

    print("\n🛑 Stopping continuous traffic streamers...", flush=True)
    for tgt in active_targets:
        contexts[tgt]["streamer"].stop()

    now_final = datetime.now(UTC)
    results = []
    for tgt, c in contexts.items():
        if "error" in c:
            results.append(c)
            continue
        stats = compute_lag_statistics(c["samples"], c["base_latest_ts"])
        page_matrix = probe_analytics_pages(c["backend_url"], c["service_id"], c["auth_headers"], now_ref=now_final)
        cur_rows = c["samples"][-1]["local_rows"] if c["samples"] else c["base_rows"]
        dash_rows = c["samples"][-1]["dash_rows"] if c["samples"] else c["base_dash_rows"]
        head_stats, dash_stats = stats["header_lag"], stats["dashboard_lag"]
        results.append({
            "env": tgt, "name": c["name"], "service_id": c["service_id"], "cdn_domain": c["cdn_domain"],
            "mode": "active_continuous_traffic", "deployment_mode": "high_scale" if c["is_high_scale"] else "standard",
            "measurement": "request_watermark_recency", "qualification": "incomplete",
            "unknowns": ["per_event_latency_uncorrelated", "rum_freshness_unmeasured", "clock_uncertainty_unmeasured"],
            "configured_polling": c["polling"],
            "active_benchmark": {
                "duration_seconds": duration_s, "target_rps": rps,
                "requests_sent": c["streamer"].requests_sent, "rum_beacons_sent": c["streamer"].rum_sent,
                "header_lag": head_stats, "dashboard_lag": dash_stats,
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


def print_active_scorecard(results: list[dict[str, Any]]) -> None:
    print(f"\n{'=' * 110}\n                      REQUEST WATERMARK RECENCY SCORECARD (DIAGNOSTIC ONLY)\n{'=' * 110}", flush=True)
    print(f"{'Environment':<28} │ {'Mode':<11} │ {'Min Lag':<8} │ {'Max Lag':<8} │ {'Avg Lag':<8} │ {'P95 Lag':<8} │ {'Dash Avg':<9} │ {'Requests':<9} │ {'Target Status':<15}", flush=True)
    print(f"{'-' * 28}─┼─{'-' * 11}─┼─{'-' * 8}─┼─{'-' * 8}─┼─{'-' * 8}─┼─{'-' * 8}─┼─{'-' * 9}─┼─{'-' * 9}─┼─{'-' * 15}", flush=True)
    for r in results:
        if "error" in r:
            print(f"{r.get('name', r.get('env', 'Unknown')):<28} │ ERROR: {r['error']}", flush=True)
            continue
        bench = r.get("active_benchmark", {})
        hl, dl = bench.get("header_lag", {}), bench.get("dashboard_lag", {})
        fmt = lambda d, k: f"{d[k]}s" if d.get(k) is not None else "—"  # noqa: E731
        reqs = f"{bench.get('requests_sent', 0):,}"
        print(
            f"{r['name']:<28} │ {r.get('deployment_mode', '—'):<11} │ {fmt(hl, 'min'):>8} │ {fmt(hl, 'max'):>8} │ "
            f"{fmt(hl, 'avg'):>8} │ {fmt(hl, 'p95'):>8} │ {fmt(dl, 'avg'):>9} │ {reqs:>9} │ {bench.get('target_status', '—'):<15}",
            flush=True,
        )
    print(f"Qualification incomplete: request/RUM event latency and clock uncertainty are unverified.\n{'=' * 110}\n", flush=True)


def print_burst_scorecard(results: list[dict[str, Any]]) -> None:
    print(f"\n{'=' * 96}\n                      END-TO-END FRESHNESS & INGEST LAG SCORECARD (SINGLE BURST)\n{'=' * 96}", flush=True)
    print(f"{'Environment':<28} │ {'Header Seen':<11} │ {'Cron Ingest':<11} │ {'Dash Seen':<12} │ {'Dashboard Lag':<14} │ {'Header Lag':<11}", flush=True)
    print(f"{'-' * 28}─┼─{'-' * 11}─┼─{'-' * 11}─┼─{'-' * 12}─┼─{'-' * 14}─┼─{'-' * 11}", flush=True)
    for r in results:
        if "error" in r:
            print(f"{r.get('name', r.get('env', 'Unknown')):<28} │ ERROR: {r['error']}", flush=True)
            continue
        delays, matrix = r.get("wall_clock_delays", {}), r.get("page_freshness_matrix", {})
        dash_lag = matrix.get("Dashboard Bundle", {}).get("freshness_lag_s")
        head_lag = matrix.get("Header Badge", {}).get("freshness_lag_s")
        f_del = lambda k: f"{delays[k]}s" if delays.get(k) is not None else "TIMEOUT"  # noqa: E731
        dash_lag_str = f"{dash_lag:.1f}s ago" if dash_lag is not None else "—"
        head_lag_str = f"{head_lag:.1f}s ago" if head_lag is not None else "—"
        print(f"{r['name']:<28} │ {f_del('header_seconds'):<11} │ {f_del('cron_request_seconds'):<11} │ {f_del('dashboard_bundle_seconds'):<12} │ {dash_lag_str:<14} │ {head_lag_str:<11}", flush=True)
    print(f"{'=' * 96}\n", flush=True)


def measure_environment(env_key: str, env_info: dict[str, str], num_requests: int = 10, num_rum: int = 5, max_wait_s: float = 60.0) -> dict[str, Any]:
    name, sid, be_url, cdn = env_info["name"], env_info["service_id"], env_info["backend_url"], env_info["cdn_domain"]
    auth_headers = {"x-fastly-service-id": sid}
    print(f"\n{'=' * 78}\n  TARGET: {name}\n  Service ID: {sid} │ Backend: {be_url} │ CDN: {cdn}\n{'=' * 78}", flush=True)

    service_status, service_data = http_get(f"{be_url}/api/services?service_id={urllib.parse.quote(sid)}", auth_headers)
    if service_status != 200:
        raise ValueError(f"services settings request failed with HTTP {service_status}: {service_data}")
    polling = configured_polling(service_data, sid)
    service_info = next(entry for entry in service_data["services"] if entry.get("service_id") == sid)
    is_high_scale, rum_enabled = bool(service_info.get("is_high_scale")), bool(service_info.get("rum_enabled"))
    print(f"  [Polling] Mode: {polling['mode']} │ Interval: {polling['interval_seconds']}s (from {polling['interval_source']})", flush=True)

    le_status, le_data = http_get(f"{be_url}/api/log-extents?service_id={sid}", auth_headers, timeout=5.0)
    sync_status_code, sync_data = http_get(f"{be_url}/api/sync-status?service_id={sid}", auth_headers, timeout=5.0)
    if le_status != 200 or not isinstance(le_data, dict):
        raise ValueError(f"log-extents baseline failed with HTTP {le_status}: {le_data}")
    if sync_status_code != 200 or not isinstance(sync_data, dict):
        raise ValueError(f"sync-status baseline failed with HTTP {sync_status_code}: {sync_data}")
    base_latest_ts = parse_iso(le_data.get("latest_log_at"))
    base_local_rows = int(sync_data.get("local_rows") or 0)

    cr_req_s, cr_req_data = http_get(f"{be_url}/api/cron-runs?service_id={sid}&task=log_discovery&per_page=1", auth_headers)
    rum_task = "rum_discovery" if is_high_scale else "rum_sync"
    cr_rum_s, cr_rum_data = http_get(f"{be_url}/api/cron-runs?service_id={sid}&task={rum_task}&per_page=1", auth_headers)
    if cr_req_s != 200 or not isinstance(cr_req_data, dict):
        raise ValueError(f"request cron baseline failed with HTTP {cr_req_s}: {cr_req_data}")
    if cr_rum_s != 200 or not isinstance(cr_rum_data, dict):
        raise ValueError(f"RUM cron baseline failed with HTTP {cr_rum_s}: {cr_rum_data}")
    base_req_run_id = cr_req_data["entries"][0]["id"] if cr_req_data.get("entries") else 0
    base_rum_run_id = cr_rum_data["entries"][0]["id"] if cr_rum_data.get("entries") else 0

    rum_health_status, rum_health_data = http_get(f"{be_url}/api/{sid}/rum/beacon-health", auth_headers)
    base_rum_beacons = rum_beacon_count(rum_health_data)
    rum_health_available = rum_enabled and rum_health_status == 200 and base_rum_beacons is not None

    print(f"  [Baseline] Header Latest Log: {base_latest_ts}\n  [Baseline] Total Rows:        {base_local_rows:,}", flush=True)
    print(f"  [Baseline] Last Cron Runs:    log_discovery=#{base_req_run_id}, {rum_task}=#{base_rum_run_id}", flush=True)
    if rum_health_available:
        print(f"  [Baseline] Visible RUM beacons: {base_rum_beacons}", flush=True)

    d_status, d_data = http_post(f"{be_url}/api/dashboard/bundle?service_id={sid}", {"range_token": "24h"}, auth_headers, timeout=30.0)
    if d_status != 200 or not isinstance(d_data, dict):
        raise ValueError(f"dashboard baseline failed with HTTP {d_status}: {d_data}")
    aggs = d_data.get("aggregates") or {}
    base_d_rows, base_d_latest = int(aggs.get("total_rows") or 0), parse_iso(aggs.get("latest_log_at"))
    print(f"  [Baseline] Dashboard:         {base_d_rows:,} rows (Latest: {base_d_latest})", flush=True)

    marker = f"prb-{uuid.uuid4().hex[:8]}"
    send_start_mono, send_start_utc = time.perf_counter(), datetime.now(UTC)
    print(f"\n🚀 Sending {num_requests} edge requests + {num_rum} RUM beacons to {cdn} (marker={marker})...", flush=True)
    with ThreadPoolExecutor(max_workers=8) as pool:
        req_futures = [pool.submit(send_edge_request_probe, cdn, marker, i) for i in range(num_requests)]
        rum_futures = [pool.submit(send_edge_rum_probe, cdn, marker, i) for i in range(num_rum)]
        req_statuses, rum_statuses = [f.result() for f in req_futures], [f.result() for f in rum_futures]

    send_duration_s = time.perf_counter() - send_start_mono
    print(f"   Sent in {send_duration_s:.2f}s! Edge HTTP codes: Req={set(req_statuses)}, RUM={set(rum_statuses)}", flush=True)
    print(f"   Watching ingest pipeline every 1.0s (timeout={max_wait_s}s)...", flush=True)

    header_detected_at: float | None = None
    header_detected_utc: datetime | None = None
    cron_req_detected_at: float | None = None
    cron_req_completed_utc: datetime | None = None
    cron_req_status: str | None = None
    cron_rum_detected_at: float | None = None
    cron_rum_completed_utc: datetime | None = None
    rum_serving_detected_at: float | None = None
    rum_serving_detected_utc: datetime | None = None
    final_rum_beacons = base_rum_beacons
    dashboard_detected_at: float | None = None
    dashboard_detected_utc: datetime | None = None
    final_local_rows, final_d_rows = base_local_rows, base_d_rows
    deadline = time.perf_counter() + max_wait_s

    while time.perf_counter() < deadline:
        time.sleep(1.0)
        elapsed_s = time.perf_counter() - send_start_mono
        now_utc = datetime.now(UTC)

        s_le, data_le = http_get(f"{be_url}/api/log-extents?service_id={sid}", auth_headers, timeout=8.0)
        s_ss, data_ss = http_get(f"{be_url}/api/sync-status?service_id={sid}", auth_headers, timeout=8.0)
        cur_ts = parse_iso(data_le.get("latest_log_at") if s_le == 200 and isinstance(data_le, dict) else None)
        cur_rows = int(data_ss.get("local_rows") or 0) if s_ss == 200 and isinstance(data_ss, dict) else base_local_rows

        if header_detected_at is None and (cur_rows > base_local_rows or (cur_ts and cur_ts > (base_latest_ts or send_start_utc))):
            header_detected_at, header_detected_utc, final_local_rows = elapsed_s, now_utc, cur_rows
            fresh_lag = (now_utc - cur_ts).total_seconds() if cur_ts else 0.0
            print(f"   ⏱️  [{elapsed_s:5.1f}s] HEADER BADGE UPDATED! Rows: {cur_rows:,} (+{cur_rows - base_local_rows}) │ Latest Log: {cur_ts} (Lag: {fresh_lag:.1f}s)", flush=True)

        if rum_health_available and rum_serving_detected_at is None:
            s_rum, data_rum = http_get(f"{be_url}/api/{sid}/rum/beacon-health", auth_headers, timeout=4.0)
            cur_rum_beacons = rum_beacon_count(data_rum) if s_rum == 200 else None
            rum_ts = parse_iso(data_rum.get("last_beacon_time")) if isinstance(data_rum, dict) else None
            if cur_rum_beacons is not None and (cur_rum_beacons > (base_rum_beacons or 0) or (rum_ts and rum_ts > send_start_utc)):
                rum_serving_detected_at, rum_serving_detected_utc, final_rum_beacons = elapsed_s, now_utc, cur_rum_beacons
                print(f"   ⏱️  [{elapsed_s:5.1f}s] RUM VISIBLE TO SERVING API! Beacons: {cur_rum_beacons:,} (+{cur_rum_beacons - (base_rum_beacons or 0)})", flush=True)

        if cron_req_detected_at is None:
            s_cr, d_cr = http_get(f"{be_url}/api/cron-runs?service_id={sid}&task=log_discovery&per_page=1", auth_headers, timeout=4.0)
            if (top_entry := new_terminal_run(d_cr, base_req_run_id) if s_cr == 200 else None) is not None:
                cron_req_detected_at, cron_req_completed_utc, cron_req_status = elapsed_s, cron_completion_time(top_entry), top_entry["status"]
                print(f"   ⏱️  [{elapsed_s:5.1f}s] CRON REQUEST INGEST {cron_req_status.upper()}! Run #{top_entry['id']}: Ingested {top_entry.get('rows_ingested', 0)} rows in {float(top_entry.get('duration_s') or 0):.2f}s", flush=True)

        if cron_rum_detected_at is None:
            s_rm, d_rm = http_get(f"{be_url}/api/cron-runs?service_id={sid}&task={rum_task}&per_page=1", auth_headers, timeout=4.0)
            if (top_entry := new_terminal_run(d_rm, base_rum_run_id) if s_rm == 200 else None) is not None:
                cron_rum_detected_at, cron_rum_completed_utc = elapsed_s, cron_completion_time(top_entry)
                print(f"   ⏱️  [{elapsed_s:5.1f}s] CRON RUM INGEST {top_entry['status'].upper()}! Run #{top_entry['id']}: Ingested {top_entry.get('rows_ingested', 0)} rows in {float(top_entry.get('duration_s') or 0):.2f}s", flush=True)

        if dashboard_detected_at is None and (header_detected_at is not None or cron_req_detected_at is not None or int(elapsed_s) % 4 == 0):
            s_d, data_d = http_post(f"{be_url}/api/dashboard/bundle?service_id={sid}", {"range_token": "24h"}, auth_headers, timeout=20.0)
            if s_d == 200 and isinstance(data_d, dict):
                ag = data_d.get("aggregates") or {}
                cur_d_rows, cur_d_ts = int(ag.get("total_rows") or 0), parse_iso(ag.get("latest_log_at"))
                if cur_d_rows > base_d_rows or (cur_d_ts and cur_d_ts > (base_d_latest or send_start_utc)):
                    dashboard_detected_at, dashboard_detected_utc, final_d_rows = elapsed_s, now_utc, cur_d_rows
                    fresh_lag = (now_utc - cur_d_ts).total_seconds() if cur_d_ts else 0.0
                    print(f"   ⏱️  [{elapsed_s:5.1f}s] DASHBOARD BUNDLE UPDATED!   Rows: {cur_d_rows:,} (+{cur_d_rows - base_d_rows}) │ Latest: {cur_d_ts} (Lag: {fresh_lag:.1f}s)", flush=True)

        if header_detected_at is not None and cron_req_detected_at is not None and dashboard_detected_at is not None and (not rum_health_available or rum_serving_detected_at is not None):
            break

    now_final = datetime.now(UTC)
    print("\n📊 Probing All Analytics Pages to measure freshness & query latency...", flush=True)
    page_results = probe_analytics_pages(be_url, sid, auth_headers, now_ref=now_final)
    for p_name, p_res in page_results.items():
        lag_s = p_res["freshness_lag_s"]
        lag_str = f"{lag_s:.1f}s behind" if lag_s is not None else "no data"
        print(f"   • {p_name:<22} Status: {p_res['status']} │ Latency: {p_res['latency_ms']:6.1f}ms │ Freshness: {lag_str}", flush=True)

    return {
        "env": env_key, "name": name, "service_id": sid, "cdn_domain": cdn, "sent_at_utc": send_start_utc.isoformat(),
        "measurement_contract": {
            "source_timestamp": "probe_send_time", "fos_last_modified_measured": False,
            "serving_detection": "aggregate row count or latest event timestamp advanced", "correlated_to_probe_marker": False,
        },
        "configured_polling": polling, "deployment_mode": "high_scale" if is_high_scale else "standard", "rum_enabled": rum_enabled,
        "wall_clock_delays": {
            "header_seconds": round(header_detected_at, 1) if header_detected_at else None,
            "cron_request_seconds": round(cron_req_detected_at, 1) if cron_req_detected_at else None,
            "cron_rum_seconds": round(cron_rum_detected_at, 1) if cron_rum_detected_at else None,
            "dashboard_bundle_seconds": round(dashboard_detected_at, 1) if dashboard_detected_at else None,
        },
        "observed_discovery_to_serving_seconds": {
            "request_discovery_status": cron_req_status,
            "includes_high_scale_queue_and_publication_delay": is_high_scale,
            "request_discovery_to_header": seconds_between(header_detected_utc, cron_req_completed_utc),
            "request_discovery_to_dashboard": seconds_between(dashboard_detected_utc, cron_req_completed_utc),
            "rum_discovery_to_serving": seconds_between(rum_serving_detected_utc, cron_rum_completed_utc),
        },
        "row_deltas": {
            "total_rows_added": final_local_rows - base_local_rows,
            "dashboard_rows_added": final_d_rows - base_d_rows,
            "rum_beacons_added": final_rum_beacons - base_rum_beacons if final_rum_beacons is not None and base_rum_beacons is not None else None,
        },
        "page_freshness_matrix": page_results,
    }


def main():
    validate_admin_tls_config()
    parser = argparse.ArgumentParser(description="Measure edge-probe-to-active-serving visibility and delay")
    parser.add_argument("--env", type=str, default="all", help="Target environments ('all', or comma-separated list: local-std,remote-std,remote-hs)")
    parser.add_argument("--active", action="store_true", default=True, help="Run continuous active-traffic benchmark measuring Min/Max/Avg lag (default: True)")
    parser.add_argument("--burst", action="store_true", help="Run legacy single-burst visibility delay benchmark instead of active traffic")
    parser.add_argument("--duration", type=float, default=30.0, help="Duration in seconds to stream active traffic and sample lag (default: 30s)")
    parser.add_argument("--rps", type=float, default=10.0, help="Continuous edge requests per second to send per target (default: 10 RPS)")
    parser.add_argument("--interval", type=float, default=2.0, help="Sampling poll interval in seconds (default: 2.0s)")
    parser.add_argument("--requests", type=int, default=10, help="Number of edge requests to send (burst mode)")
    parser.add_argument("--rum-beacons", type=int, default=5, help="Number of RUM beacons to send (burst mode)")
    parser.add_argument("--timeout", type=float, default=45.0, help="Max wait seconds for ingest (burst mode)")
    parser.add_argument("--correlated", action="store_true", help="Bounded exact URL/cid freshness upper bounds")
    parser.add_argument("--url", help="Approved edge target URL, including its existing path (correlated mode)")
    parser.add_argument("--rum-errors", type=int, default=0, help="Separate exception beacons (correlated mode)")
    parser.add_argument("--correlated-interval", type=float, default=10, help="Correlated poll interval, >=10 seconds")
    parser.add_argument("--output", type=str, default="reports/deploys/current/relics/e2e_freshness_report.json")
    args = parser.parse_args()
    _load_persisted_deploy_env()

    targets = list(ENVIRONMENTS.keys()) if args.env == "all" else ([t.strip() for t in args.env.split(",") if t.strip()] if "," in args.env else [args.env])
    if args.correlated:
        if len(targets) != 1 or not args.url or args.burst:
            parser.error("--correlated requires exactly one --env and an explicit approved --url, without --burst")
        results = [measure_correlated_freshness(load_environment_config(targets[0]), args.url, args.requests, args.rum_beacons, args.rum_errors, args.correlated_interval, args.timeout)]
    elif args.burst:
        print(f"\n{'#' * 80}\n# FASTLY LOG ANALYTICS — SINGLE-BURST VISIBILITY DELAY BENCHMARK\n# Targets: {targets}\n{'#' * 80}", flush=True)
        results = []
        for tgt in targets:
            try:
                res = measure_environment(tgt, load_environment_config(tgt), num_requests=args.requests, num_rum=args.rum_beacons, max_wait_s=args.timeout)
            except (ValueError, KeyError) as exc:
                res = {"env": tgt, "name": ENVIRONMENTS.get(tgt, tgt), "error": str(exc)}
                print(f"  ERROR: {exc}", flush=True)
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
    print(f"✅ Full benchmark report written to {out_path}\n", flush=True)


if __name__ == "__main__":
    main()

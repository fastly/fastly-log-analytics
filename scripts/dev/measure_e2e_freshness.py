#!/usr/bin/env python3
"""Measure edge-to-serving visibility and dashboard latency.

Sends controlled edge traffic (HTTP requests + Faro RUM beacons) through Fastly CDN
and monitors how long until:
  1. Header badge updates (Request and RUM timestamps + row counts via /api/log-extents and /api/sync-status)
  2. Ingest cron runs complete (/api/cron-runs for log_discovery and rum_sync)
  3. Dashboard bundle reflects new data (/api/dashboard/bundle)
  4. Other analytics pages reflect the live state (/api/performance, /api/security, etc.)

Modes:
  - Active Continuous Traffic (--active, default): Streams edge requests in background
    threads (e.g. 10 RPS) for --duration seconds while polling serving APIs every 2s,
    calculating Min, Max, Avg, and P95 lag across the active window.
  - Single-Burst Probe (--burst): Dispatches an initial batch of requests and measures
    time-to-first-serving-visibility.

Reports:
  - Live sampling progress during active continuous traffic
  - End-to-end Min / Max / Avg / P95 lag scorecard
  - Freshness lag for all individual analytics pages

Configure each target with FLA_FRESHNESS_<ENV>_SERVICE_ID,
FLA_FRESHNESS_<ENV>_BACKEND_URL, and FLA_FRESHNESS_<ENV>_CDN_DOMAIN, where
<ENV> is LOCAL_STD, REMOTE_STD, or REMOTE_HS. Direct TLS origins or admin authentication
come from scripts.lib.admin_tls, REMOTE_ADMIN_TOKEN, ADMIN_SHARED_SECRET, or ADMIN_TOKEN.
"""

from __future__ import annotations

import argparse
import json
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

# Direct path invocation and python -m share the same explicit repo root.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.lib.admin_tls import admin_origin, admin_urlopen, is_admin_url, validate_admin_tls_config  # noqa: E402

ENVIRONMENTS = {
    "local-std": "Local Standard",
    "remote-std": "Remote Standard",
    "remote-hs": "Remote High-Scale",
}

DEFAULT_ENV_CONFIGS: dict[str, dict[str, str]] = {}


def _load_persisted_deploy_env() -> None:
    if "ADMIN_GATEWAY_ENDPOINTS" in os.environ:
        return
    deploy_env = os.environ.get(
        "FLA_ADMIN_DEPLOY_ENV",
        str(Path.home() / ".config/fastly-log-analytics/admin/deploy.env"),
    )
    p = Path(deploy_env)
    if p.is_file():
        try:
            from dotenv import dotenv_values

            for k, v in dotenv_values(p).items():
                if v is not None and k not in os.environ:
                    os.environ[k] = v
        except Exception:
            pass


def load_environment_config(env_key: str, environ: dict[str, str] | None = None) -> dict[str, str]:
    if environ is not None:
        prefix = f"FLA_FRESHNESS_{env_key.upper().replace('-', '_')}_"
        settings = {
            key.lower(): environ.get(f"{prefix}{key}", "").strip()
            for key in ("SERVICE_ID", "BACKEND_URL", "CDN_DOMAIN")
        }
        missing = [key for key, value in settings.items() if not value]
        if missing:
            variable_names = ", ".join(f"{prefix}{key.upper()}" for key in missing)
            raise ValueError(f"Missing environment configuration: {variable_names}")
        for key in ("backend_url", "cdn_domain"):
            if not settings[key].startswith(("http://", "https://")):
                raise ValueError(f"{prefix}{key.upper()} must be an absolute HTTP(S) URL")
        settings["name"] = ENVIRONMENTS[env_key]
        return settings

    _load_persisted_deploy_env()
    prefix = f"FLA_FRESHNESS_{env_key.upper().replace('-', '_')}_"
    defaults = DEFAULT_ENV_CONFIGS.get(env_key, {})
    settings = {
        key.lower(): os.environ.get(f"{prefix}{key}", "").strip() for key in ("SERVICE_ID", "BACKEND_URL", "CDN_DOMAIN")
    }
    if not settings["backend_url"]:
        if origin := admin_origin(env_key):
            settings["backend_url"] = origin
        elif defaults.get("backend_url"):
            settings["backend_url"] = defaults["backend_url"]
    if not settings["service_id"] and defaults.get("service_id"):
        settings["service_id"] = defaults["service_id"]
    if not settings["cdn_domain"] and defaults.get("cdn_domain"):
        settings["cdn_domain"] = defaults["cdn_domain"]

    missing = [key for key, value in settings.items() if not value]
    if missing:
        variable_names = ", ".join(f"{prefix}{key.upper()}" for key in missing)
        raise ValueError(f"Missing environment configuration: {variable_names}")
    for key in ("backend_url", "cdn_domain"):
        if not settings[key].startswith(("http://", "https://")):
            raise ValueError(f"{prefix}{key.upper()} must be an absolute HTTP(S) URL")
    settings["name"] = ENVIRONMENTS[env_key]
    return settings


def admin_token_from_environment(environ: dict[str, str] | None = None) -> str:
    env = os.environ if environ is None else environ
    for key in ("REMOTE_ADMIN_TOKEN", "ADMIN_SHARED_SECRET", "ADMIN_TOKEN"):
        token = env.get(key, "").strip()
        if token:
            return token
    raise ValueError("Set REMOTE_ADMIN_TOKEN, ADMIN_SHARED_SECRET, or ADMIN_TOKEN for admin API access")


def configured_polling(settings_response: Any, service_id: str) -> dict[str, Any]:
    services = settings_response.get("services") if isinstance(settings_response, dict) else None
    if not isinstance(services, list):
        raise ValueError("services response is missing its service list")
    service = next(
        (entry for entry in services if isinstance(entry, dict) and entry.get("service_id") == service_id), None
    )
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
    interval_mins = cron_sync.get("interval_mins")
    interval_seconds = cron_sync.get("interval_seconds")
    if interval_mins:
        interval = max(5, int(interval_mins) * 60)
        interval_source = "interval_mins"
    elif interval_seconds:
        interval = max(5, int(interval_seconds))
        interval_source = "interval_seconds"
    else:
        interval = max(5, log_period // 2 if log_period >= 60 else log_period)
        interval_source = "log_period"

    return {
        "mode": mode,
        "interval_seconds": interval,
        "interval_source": interval_source,
        "adaptive_followup_interval_seconds": 3 if mode == "adaptive" else None,
        "adaptive_max_followups": 2 if mode == "adaptive" else 0,
    }


def cron_completion_time(entry: Any) -> datetime | None:
    if not isinstance(entry, dict):
        return None
    started_at = parse_iso(entry.get("started_at"))
    try:
        duration_s = float(entry["duration_s"])
    except (KeyError, TypeError, ValueError):
        return None
    if started_at is None:
        return None
    return started_at + timedelta(seconds=duration_s)


def new_terminal_run(response: Any, baseline_run_id: int) -> dict[str, Any] | None:
    if not isinstance(response, dict):
        return None
    entries = response.get("entries")
    if not isinstance(entries, list) or not entries or not isinstance(entries[0], dict):
        return None
    entry = entries[0]
    try:
        run_id = int(entry["id"])
    except (KeyError, TypeError, ValueError):
        return None
    if run_id <= baseline_run_id or entry.get("status") not in {"success", "warning", "error"}:
        return None
    return entry


def seconds_between(later: datetime | None, earlier: datetime | None) -> float | None:
    if later is None or earlier is None:
        return None
    return round((later - earlier).total_seconds(), 1)


def rum_beacon_count(response: Any) -> int | None:
    if not isinstance(response, dict):
        return None
    count = response.get("beacons")
    if count is None:
        count = response.get("recent_beacons")
    try:
        return int(count) if count is not None else None
    except (TypeError, ValueError):
        return None


def parse_iso(ts_str: str | None) -> datetime | None:
    if not ts_str:
        return None
    try:
        clean = ts_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(clean)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt
    except Exception:
        return None


def http_get(url: str, headers: dict[str, str] | None = None, timeout: float = 35.0) -> tuple[int, Any]:
    req_headers = {"Accept": "application/json", "User-Agent": "FLA-FreshnessProbe/1.0"}
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(url, headers=req_headers)
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


def http_post(
    url: str, body: dict | None = None, headers: dict[str, str] | None = None, timeout: float = 35.0
) -> tuple[int, Any]:
    req_headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "FLA-FreshnessProbe/1.0",
    }
    if headers:
        req_headers.update(headers)
    payload = json.dumps(body if body is not None else {}).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers=req_headers, method="POST")
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


def send_edge_request_probe(cdn_domain: str, marker: str, idx: int) -> int:
    spoof_ip = f"{random.randint(12, 223)}.{random.randint(1, 254)}.{random.randint(1, 254)}.{random.randint(1, 254)}"
    url = f"{cdn_domain.rstrip('/')}/test-freshness?probe={marker}&seq={idx}"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": f"FLA-Probe-Agent/1.0 ({marker})",
            "X-Source-Ip": spoof_ip,
            "Fastly-Request-ID": f"req-{marker}-{idx:04d}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return 0


def send_edge_rum_probe(cdn_domain: str, marker: str, idx: int) -> int:
    spoof_ip = f"{random.randint(12, 223)}.{random.randint(1, 254)}.{random.randint(1, 254)}.{random.randint(1, 254)}"
    url = (
        f"{cdn_domain.rstrip('/')}/rum-beacon?cid={marker}-{idx}&rum_metric_name=LCP&rum_metric_value=1250"
        f"&rum_pathname=%2Ftest-freshness"
    )
    faro_payload = {
        "meta": {
            "browser": {"name": "Chrome", "mobile": False},
            "os": {"name": "macOS"},
            "page": {"url": f"{cdn_domain}/test-freshness"},
        },
        "measurements": [{"type": "web-vitals", "values": {"LCP": 1250.0}, "context": {"rating": "good"}}],
        "events": [
            {
                "name": "freshness_test_click",
                "timestamp": datetime.now(UTC).isoformat(),
                "attributes": {"probe": marker},
            }
        ],
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(faro_payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "User-Agent": f"FLA-RUM-Probe/1.0 ({marker})",
            "X-Source-Ip": spoof_ip,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return 0


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
        interval = 1.0 / self.rps
        seq = 0
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
                dt = time.perf_counter() - t0
                sleep_time = interval - dt
                if sleep_time > 0:
                    time.sleep(sleep_time)

    def _send_req(self, seq: int) -> None:
        code = send_edge_request_probe(self.cdn_domain, self.marker, seq)
        if code in (200, 204, 301, 302):
            self.requests_sent += 1
        else:
            self.errors += 1

    def _send_rum(self, seq: int) -> None:
        code = send_edge_rum_probe(self.cdn_domain, self.marker, seq)
        if code in (200, 204):
            self.rum_sent += 1


def compute_lag_statistics(
    samples: list[dict[str, Any]],
    baseline_latest_ts: datetime | None = None,
    min_elapsed_s: float = 6.0,
) -> dict[str, Any]:
    """Computes Min, Max, Avg, and P95 lag metrics for header and dashboard."""
    if baseline_latest_ts:
        active_samples = [
            s for s in samples if s.get("header_latest_ts") and s["header_latest_ts"] > baseline_latest_ts
        ]
    else:
        active_samples = []

    if not active_samples:
        active_samples = [s for s in samples if s.get("elapsed_s", 0) >= min_elapsed_s]
    if not active_samples:
        active_samples = list(samples)

    head_lags = [s["header_lag_s"] for s in active_samples if s.get("header_lag_s") is not None]
    dash_lags = [s["dashboard_lag_s"] for s in active_samples if s.get("dashboard_lag_s") is not None]

    def _stats(vals: list[float]) -> dict[str, float | None]:
        if not vals:
            return {"min": None, "max": None, "avg": None, "p95": None}
        s = sorted(vals)
        p95_idx = min(int(len(s) * 0.95), len(s) - 1)
        return {
            "min": round(min(vals), 1),
            "max": round(max(vals), 1),
            "avg": round(sum(vals) / len(vals), 1),
            "p95": round(s[p95_idx], 1),
        }

    return {
        "header_lag": _stats(head_lags),
        "dashboard_lag": _stats(dash_lags),
        "sample_count": len(active_samples),
        "total_samples": len(samples),
    }


def probe_analytics_pages(
    be_url: str,
    sid: str,
    auth_headers: dict[str, str],
    now_ref: datetime | None = None,
) -> dict[str, dict[str, Any]]:
    now_final = now_ref or datetime.now(UTC)
    page_probes: dict[str, tuple[str, str, dict[str, Any] | None]] = {
        "Header Badge": (f"{be_url}/api/log-extents?service_id={sid}", "GET", None),
        "Dashboard Bundle": (f"{be_url}/api/dashboard/bundle?service_id={sid}", "POST", {"range_token": "24h"}),
        "Performance / RUM": (f"{be_url}/api/performance/aggregates", "POST", {"filters": {}}),
        "Security Aggregates": (f"{be_url}/api/security/aggregates", "POST", {"filters": {}}),
        "Origin Timeseries": (f"{be_url}/api/origin/timeseries", "POST", {"filters": {}}),
        "Network Health": (
            f"{be_url}/api/network-health",
            "POST",
            {"filters": {}, "metric": "health_score", "bucket_seconds": 60, "top_n": 10},
        ),
    }

    page_results = {}
    for page_name, (p_url, p_method, p_payload) in page_probes.items():
        t_start = time.perf_counter()
        if p_method == "GET":
            st, res_data = http_get(p_url, auth_headers, timeout=15.0)
        else:
            st, res_data = http_post(p_url, p_payload, auth_headers, timeout=35.0)
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
            "status": st,
            "latency_ms": round(t_latency_ms, 1),
            "latest_log_at": page_ts.isoformat() if page_ts else None,
            "freshness_lag_s": round(lag_s, 1) if lag_s is not None else None,
        }
    return page_results


def measure_active_traffic_benchmark(
    targets: list[str],
    admin_token: str,
    duration_s: float = 30.0,
    rps: float = 10.0,
    sample_interval_s: float = 2.0,
) -> list[dict[str, Any]]:
    print(f"\n{'=' * 96}", flush=True)
    print(f"  STARTING ACTIVE CONTINUOUS TRAFFIC BENCHMARK ({rps:.0f} RPS, duration={duration_s:.0f}s)", flush=True)
    print(f"  Targets: {', '.join(targets)}", flush=True)
    print(f"{'=' * 96}\n", flush=True)

    contexts: dict[str, dict[str, Any]] = {}
    for tgt in targets:
        try:
            env_info = load_environment_config(tgt)
            sid = env_info["service_id"]
            be_url = env_info["backend_url"]
            cdn = env_info["cdn_domain"]
            auth_headers = {"x-fastly-service-id": sid}
            if not is_admin_url(be_url) and admin_token:
                auth_headers["x-admin-token"] = admin_token

            svc_status, svc_data = http_get(f"{be_url}/api/services?service_id={urllib.parse.quote(sid)}", auth_headers)
            if svc_status != 200:
                raise ValueError(f"services query failed (HTTP {svc_status}): {svc_data}")
            svc_info = next(e for e in svc_data["services"] if e.get("service_id") == sid)
            polling = configured_polling(svc_data, sid)
            is_high_scale = bool(svc_info.get("is_high_scale"))
            rum_enabled = bool(svc_info.get("rum_enabled"))

            # Baseline
            le_status, le_data = http_get(f"{be_url}/api/log-extents?service_id={sid}", auth_headers, timeout=5.0)
            sync_s, sync_data = http_get(f"{be_url}/api/sync-status?service_id={sid}", auth_headers, timeout=5.0)
            d_s, d_data = http_post(
                f"{be_url}/api/dashboard/bundle?service_id={sid}", {"range_token": "24h"}, auth_headers, timeout=20.0
            )

            base_latest_ts = parse_iso(
                le_data.get("latest_log_at") if le_status == 200 and isinstance(le_data, dict) else None
            )
            base_rows = int(sync_data.get("local_rows") or 0) if sync_s == 200 and isinstance(sync_data, dict) else 0
            base_dash_rows = (
                int((d_data.get("aggregates") or {}).get("total_rows") or 0)
                if d_s == 200 and isinstance(d_data, dict)
                else 0
            )

            marker = f"act-{uuid.uuid4().hex[:6]}"
            streamer = ActiveTrafficStreamer(cdn, marker, rps=rps)

            contexts[tgt] = {
                "env": tgt,
                "name": env_info["name"],
                "service_id": sid,
                "backend_url": be_url,
                "cdn_domain": cdn,
                "auth_headers": auth_headers,
                "is_high_scale": is_high_scale,
                "rum_enabled": rum_enabled,
                "polling": polling,
                "base_latest_ts": base_latest_ts,
                "base_rows": base_rows,
                "base_dash_rows": base_dash_rows,
                "marker": marker,
                "streamer": streamer,
                "samples": [],
            }
            print(
                f"  ✓ [{env_info['name']}] Baseline established (Latest: {base_latest_ts}, Rows: {base_rows:,}). Streamer initialized.",
                flush=True,
            )
        except Exception as exc:
            print(f"  ✗ [{tgt}] Setup failed: {exc}", flush=True)
            contexts[tgt] = {"env": tgt, "name": ENVIRONMENTS.get(tgt, tgt), "error": str(exc)}

    # Start streamers for healthy targets
    active_targets = [tgt for tgt, c in contexts.items() if "streamer" in c]
    for tgt in active_targets:
        contexts[tgt]["streamer"].start()

    print(
        f"\n🚀 Continuous traffic streaming at {rps:.0f} RPS per target! Sampling every {sample_interval_s:.1f}s...\n",
        flush=True,
    )

    start_mono = time.perf_counter()
    deadline = start_mono + duration_s

    # Sampling loop
    while time.perf_counter() < deadline:
        time.sleep(sample_interval_s)
        now_utc = datetime.now(UTC)
        elapsed = time.perf_counter() - start_mono

        def _sample_target(tgt: str) -> dict[str, Any]:
            c = contexts[tgt]
            sid = c["service_id"]
            be = c["backend_url"]
            hdrs = c["auth_headers"]

            s_le, d_le = http_get(f"{be}/api/log-extents?service_id={sid}", hdrs, timeout=6.0)
            s_ss, d_ss = http_get(f"{be}/api/sync-status?service_id={sid}", hdrs, timeout=6.0)
            s_db, d_db = http_post(
                f"{be}/api/dashboard/bundle?service_id={sid}", {"range_token": "24h"}, hdrs, timeout=15.0
            )

            cur_ts = parse_iso(d_le.get("latest_log_at") if s_le == 200 and isinstance(d_le, dict) else None)
            cur_rows = int(d_ss.get("local_rows") or 0) if s_ss == 200 and isinstance(d_ss, dict) else c["base_rows"]

            dash_ag = d_db.get("aggregates") or {} if s_db == 200 and isinstance(d_db, dict) else {}
            dash_ts = parse_iso(dash_ag.get("latest_log_at"))
            dash_rows = int(dash_ag.get("total_rows") or 0)

            head_lag = (now_utc - cur_ts).total_seconds() if cur_ts else None
            dash_lag = (now_utc - dash_ts).total_seconds() if dash_ts else None

            sample = {
                "elapsed_s": round(elapsed, 1),
                "timestamp_utc": now_utc.isoformat(),
                "header_latest_ts": cur_ts,
                "header_latest_log_at": cur_ts.isoformat() if cur_ts else None,
                "header_lag_s": round(head_lag, 1) if head_lag is not None else None,
                "dashboard_latest_ts": dash_ts,
                "dashboard_latest_log_at": dash_ts.isoformat() if dash_ts else None,
                "dashboard_lag_s": round(dash_lag, 1) if dash_lag is not None else None,
                "local_rows": cur_rows,
                "dash_rows": dash_rows,
            }
            c["samples"].append(sample)
            return {"target": tgt, "sample": sample}

        with ThreadPoolExecutor(max_workers=max(1, len(active_targets))) as pool:
            sample_results = list(pool.map(_sample_target, active_targets))

        # Format synchronized status line
        parts = []
        for res in sample_results:
            tgt = res["target"]
            c = contexts[tgt]
            s = res["sample"]
            hl = f"{s['header_lag_s']:.1f}s" if s["header_lag_s"] is not None else "—"
            parts.append(f"{c['name']}: {hl:>5} lag")
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

        total_reqs = c["streamer"].requests_sent
        total_rum = c["streamer"].rum_sent
        cur_rows = c["samples"][-1]["local_rows"] if c["samples"] else c["base_rows"]
        dash_rows = c["samples"][-1]["dash_rows"] if c["samples"] else c["base_dash_rows"]

        head_stats = stats["header_lag"]
        dash_stats = stats["dashboard_lag"]

        # Target status assessment
        if c["is_high_scale"]:
            target_status = (
                "PASS (<15s)" if (head_stats["avg"] is not None and head_stats["avg"] <= 15.0) else "ELEVATED"
            )
        else:
            if head_stats["avg"] is not None and head_stats["avg"] <= 20.0:
                target_status = "PASS (~10-15s)"
            elif head_stats["avg"] is not None and head_stats["avg"] <= 26.0:
                target_status = "PASS (Near Target)"
            else:
                target_status = "ELEVATED"

        res = {
            "env": tgt,
            "name": c["name"],
            "service_id": c["service_id"],
            "cdn_domain": c["cdn_domain"],
            "mode": "active_continuous_traffic",
            "deployment_mode": "high_scale" if c["is_high_scale"] else "standard",
            "configured_polling": c["polling"],
            "active_benchmark": {
                "duration_seconds": duration_s,
                "target_rps": rps,
                "requests_sent": total_reqs,
                "rum_beacons_sent": total_rum,
                "header_lag": head_stats,
                "dashboard_lag": dash_stats,
                "target_status": target_status,
                "rows_added": max(0, cur_rows - c["base_rows"]),
                "dash_rows_added": max(0, dash_rows - c["base_dash_rows"]),
            },
            "wall_clock_delays": {
                "header_min_s": head_stats["min"],
                "header_max_s": head_stats["max"],
                "header_avg_s": head_stats["avg"],
                "header_p95_s": head_stats["p95"],
                "dashboard_avg_s": dash_stats["avg"],
            },
            "page_freshness_matrix": page_matrix,
        }
        results.append(res)

    print_active_scorecard(results)
    return results


def print_active_scorecard(results: list[dict[str, Any]]) -> None:
    print(f"\n{'=' * 110}", flush=True)
    print("                      END-TO-END FRESHNESS & INGEST LAG SCORECARD (ACTIVE TRAFFIC)", flush=True)
    print(f"{'=' * 110}", flush=True)
    print(
        f"{'Environment':<28} │ {'Mode':<11} │ {'Min Lag':<8} │ {'Max Lag':<8} │ {'Avg Lag':<8} │ {'P95 Lag':<8} │ {'Dash Avg':<9} │ {'Requests':<9} │ {'Target Status':<15}",
        flush=True,
    )
    print(
        f"{'-' * 28}─┼─{'-' * 11}─┼─{'-' * 8}─┼─{'-' * 8}─┼─{'-' * 8}─┼─{'-' * 8}─┼─{'-' * 9}─┼─{'-' * 9}─┼─{'-' * 15}",
        flush=True,
    )

    for r in results:
        if "error" in r:
            print(f"{r.get('name', r.get('env', 'Unknown')):<28} │ ERROR: {r['error']}", flush=True)
            continue
        bench = r.get("active_benchmark", {})
        hl = bench.get("header_lag", {})
        dl = bench.get("dashboard_lag", {})

        min_s = f"{hl['min']}s" if hl.get("min") is not None else "—"
        max_s = f"{hl['max']}s" if hl.get("max") is not None else "—"
        avg_s = f"{hl['avg']}s" if hl.get("avg") is not None else "—"
        p95_s = f"{hl['p95']}s" if hl.get("p95") is not None else "—"
        dash_avg_s = f"{dl['avg']}s" if dl.get("avg") is not None else "—"
        reqs = f"{bench.get('requests_sent', 0):,}"
        status = bench.get("target_status", "—")
        mode = r.get("deployment_mode", "—")

        print(
            f"{r['name']:<28} │ {mode:<11} │ {min_s:>8} │ {max_s:>8} │ {avg_s:>8} │ {p95_s:>8} │ {dash_avg_s:>9} │ {reqs:>9} │ {status:<15}",
            flush=True,
        )

    print(f"{'=' * 110}\n", flush=True)


def print_burst_scorecard(results: list[dict[str, Any]]) -> None:
    print(f"\n{'=' * 96}", flush=True)
    print("                      END-TO-END FRESHNESS & INGEST LAG SCORECARD (SINGLE BURST)", flush=True)
    print(f"{'=' * 96}", flush=True)
    print(
        f"{'Environment':<28} │ {'Header Seen':<11} │ {'Cron Ingest':<11} │ {'Dash Seen':<12} │ {'Dashboard Lag':<14} │ {'Header Lag':<11}",
        flush=True,
    )
    print(f"{'-' * 28}─┼─{'-' * 11}─┼─{'-' * 11}─┼─{'-' * 12}─┼─{'-' * 14}─┼─{'-' * 11}", flush=True)

    for r in results:
        if "error" in r:
            print(f"{r.get('name', r.get('env', 'Unknown')):<28} │ ERROR: {r['error']}", flush=True)
            continue
        delays = r.get("wall_clock_delays", {})
        matrix = r.get("page_freshness_matrix", {})
        dash_lag = matrix.get("Dashboard Bundle", {}).get("freshness_lag_s")
        head_lag = matrix.get("Header Badge", {}).get("freshness_lag_s")

        head_seen_str = f"{delays['header_seconds']}s" if delays.get("header_seconds") is not None else "TIMEOUT"
        cron_str = f"{delays['cron_request_seconds']}s" if delays.get("cron_request_seconds") is not None else "TIMEOUT"
        dash_str = (
            f"{delays['dashboard_bundle_seconds']}s"
            if delays.get("dashboard_bundle_seconds") is not None
            else "TIMEOUT"
        )
        dash_lag_str = f"{dash_lag:.1f}s ago" if dash_lag is not None else "—"
        head_lag_str = f"{head_lag:.1f}s ago" if head_lag is not None else "—"

        print(
            f"{r['name']:<28} │ {head_seen_str:<11} │ {cron_str:<11} │ {dash_str:<12} │ {dash_lag_str:<14} │ {head_lag_str:<11}",
            flush=True,
        )

    print(f"{'=' * 96}\n", flush=True)


def measure_environment(
    env_key: str,
    env_info: dict[str, str],
    admin_token: str,
    num_requests: int = 10,
    num_rum: int = 5,
    max_wait_s: float = 60.0,
) -> dict[str, Any]:
    name = env_info["name"]
    sid = env_info["service_id"]
    be_url = env_info["backend_url"]
    cdn = env_info["cdn_domain"]
    auth_headers = {"x-fastly-service-id": sid}
    if not is_admin_url(be_url) and admin_token:
        auth_headers["x-admin-token"] = admin_token

    print(f"\n{'=' * 78}", flush=True)
    print(f"  TARGET: {name}", flush=True)
    print(f"  Service ID: {sid} │ Backend: {be_url} │ CDN: {cdn}", flush=True)
    print(f"{'=' * 78}", flush=True)

    # 1. Capture the active serving baseline and effective cron configuration.
    service_status, service_data = http_get(f"{be_url}/api/services?service_id={urllib.parse.quote(sid)}", auth_headers)
    if service_status != 200:
        raise ValueError(f"services settings request failed with HTTP {service_status}: {service_data}")
    polling = configured_polling(service_data, sid)
    service_info = next(entry for entry in service_data["services"] if entry.get("service_id") == sid)
    is_high_scale = bool(service_info.get("is_high_scale"))
    rum_enabled = bool(service_info.get("rum_enabled"))
    print(
        f"  [Polling] Mode: {polling['mode']} │ Interval: {polling['interval_seconds']}s "
        f"(from {polling['interval_source']})",
        flush=True,
    )

    le_status, le_data = http_get(f"{be_url}/api/log-extents?service_id={sid}", auth_headers, timeout=5.0)
    sync_status_code, sync_data = http_get(f"{be_url}/api/sync-status?service_id={sid}", auth_headers, timeout=5.0)
    if le_status != 200 or not isinstance(le_data, dict):
        raise ValueError(f"log-extents baseline failed with HTTP {le_status}: {le_data}")
    if sync_status_code != 200 or not isinstance(sync_data, dict):
        raise ValueError(f"sync-status baseline failed with HTTP {sync_status_code}: {sync_data}")

    base_latest_ts = parse_iso(le_data.get("latest_log_at"))
    base_local_rows = int(sync_data.get("local_rows") or 0)

    cr_req_s, cr_req_data = http_get(
        f"{be_url}/api/cron-runs?service_id={sid}&task=log_discovery&per_page=1", auth_headers
    )
    rum_task = "rum_discovery" if is_high_scale else "rum_sync"
    cr_rum_s, cr_rum_data = http_get(
        f"{be_url}/api/cron-runs?service_id={sid}&task={rum_task}&per_page=1", auth_headers
    )
    if cr_req_s != 200 or not isinstance(cr_req_data, dict):
        raise ValueError(f"request cron baseline failed with HTTP {cr_req_s}: {cr_req_data}")
    if cr_rum_s != 200 or not isinstance(cr_rum_data, dict):
        raise ValueError(f"RUM cron baseline failed with HTTP {cr_rum_s}: {cr_rum_data}")

    base_req_run_id = cr_req_data["entries"][0]["id"] if cr_req_data.get("entries") else 0
    base_rum_run_id = cr_rum_data["entries"][0]["id"] if cr_rum_data.get("entries") else 0

    rum_health_status, rum_health_data = http_get(f"{be_url}/api/{sid}/rum/beacon-health", auth_headers)
    base_rum_beacons = rum_beacon_count(rum_health_data)
    rum_health_available = rum_enabled and rum_health_status == 200 and base_rum_beacons is not None

    print(f"  [Baseline] Header Latest Log: {base_latest_ts}", flush=True)
    print(f"  [Baseline] Total Rows:        {base_local_rows:,}", flush=True)
    print(
        f"  [Baseline] Last Cron Runs:    log_discovery=#{base_req_run_id}, {rum_task}=#{base_rum_run_id}",
        flush=True,
    )
    if rum_health_available:
        print(f"  [Baseline] Visible RUM beacons: {base_rum_beacons}", flush=True)

    d_status, d_data = http_post(
        f"{be_url}/api/dashboard/bundle?service_id={sid}", {"range_token": "24h"}, auth_headers, timeout=30.0
    )
    if d_status != 200 or not isinstance(d_data, dict):
        raise ValueError(f"dashboard baseline failed with HTTP {d_status}: {d_data}")
    base_d_rows = 0
    base_d_latest = None
    aggs = d_data.get("aggregates") or {}
    base_d_rows = int(aggs.get("total_rows") or 0)
    base_d_latest = parse_iso(aggs.get("latest_log_at"))
    print(f"  [Baseline] Dashboard:         {base_d_rows:,} rows (Latest: {base_d_latest})", flush=True)

    # 2. Dispatch Probe Edge Traffic
    marker = f"prb-{uuid.uuid4().hex[:8]}"
    send_start_mono = time.perf_counter()
    send_start_utc = datetime.now(UTC)
    print(
        f"\n🚀 Sending {num_requests} edge requests + {num_rum} RUM beacons to {cdn} (marker={marker})...", flush=True
    )

    with ThreadPoolExecutor(max_workers=8) as pool:
        req_futures = [pool.submit(send_edge_request_probe, cdn, marker, i) for i in range(num_requests)]
        rum_futures = [pool.submit(send_edge_rum_probe, cdn, marker, i) for i in range(num_rum)]
        req_statuses = [f.result() for f in req_futures]
        rum_statuses = [f.result() for f in rum_futures]

    send_finish_mono = time.perf_counter()
    send_duration_s = send_finish_mono - send_start_mono
    print(
        f"   Sent in {send_duration_s:.2f}s! Edge HTTP codes: Req={set(req_statuses)}, RUM={set(rum_statuses)}",
        flush=True,
    )
    print(f"   Watching ingest pipeline every 1.0s (timeout={max_wait_s}s)...", flush=True)

    # 3. Continuous Polling Loop
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

    final_latest_ts = base_latest_ts
    final_local_rows = base_local_rows
    final_d_rows = base_d_rows
    final_d_ts = base_d_latest

    deadline = time.perf_counter() + max_wait_s

    while time.perf_counter() < deadline:
        time.sleep(1.0)
        elapsed_s = time.perf_counter() - send_start_mono
        now_utc = datetime.now(UTC)

        # Check Header Extents & Sync Status (<0.5s response)
        s_le, data_le = http_get(f"{be_url}/api/log-extents?service_id={sid}", auth_headers, timeout=8.0)
        s_ss, data_ss = http_get(f"{be_url}/api/sync-status?service_id={sid}", auth_headers, timeout=8.0)

        cur_ts = parse_iso(data_le.get("latest_log_at") if s_le == 200 and isinstance(data_le, dict) else None)
        cur_rows = int(data_ss.get("local_rows") or 0) if s_ss == 200 and isinstance(data_ss, dict) else base_local_rows

        if header_detected_at is None:
            if cur_rows > base_local_rows or (cur_ts and cur_ts > (base_latest_ts or send_start_utc)):
                header_detected_at = elapsed_s
                header_detected_utc = now_utc
                final_latest_ts = cur_ts
                final_local_rows = cur_rows
                fresh_lag = (now_utc - cur_ts).total_seconds() if cur_ts else 0.0
                print(
                    f"   ⏱️  [{elapsed_s:5.1f}s] HEADER BADGE UPDATED! Rows: {cur_rows:,} (+{cur_rows - base_local_rows}) │ Latest Log: {cur_ts} (Lag: {fresh_lag:.1f}s)",
                    flush=True,
                )

        if rum_health_available and rum_serving_detected_at is None:
            s_rum, data_rum = http_get(f"{be_url}/api/{sid}/rum/beacon-health", auth_headers, timeout=4.0)
            cur_rum_beacons = rum_beacon_count(data_rum) if s_rum == 200 else None
            rum_ts = parse_iso(data_rum.get("last_beacon_time")) if isinstance(data_rum, dict) else None
            if cur_rum_beacons is not None and (
                cur_rum_beacons > (base_rum_beacons or 0) or (rum_ts and rum_ts > send_start_utc)
            ):
                rum_serving_detected_at = elapsed_s
                rum_serving_detected_utc = now_utc
                final_rum_beacons = cur_rum_beacons
                print(
                    f"   ⏱️  [{elapsed_s:5.1f}s] RUM VISIBLE TO SERVING API! "
                    f"Beacons: {cur_rum_beacons:,} (+{cur_rum_beacons - (base_rum_beacons or 0)})",
                    flush=True,
                )

        # Check Ingest Crons (log_discovery and rum_sync)
        if cron_req_detected_at is None:
            s_cr, d_cr = http_get(
                f"{be_url}/api/cron-runs?service_id={sid}&task=log_discovery&per_page=1", auth_headers, timeout=4.0
            )
            top_entry = new_terminal_run(d_cr, base_req_run_id) if s_cr == 200 else None
            if top_entry is not None:
                cron_req_detected_at = elapsed_s
                cron_req_completed_utc = cron_completion_time(top_entry)
                cron_req_status = top_entry["status"]
                print(
                    f"   ⏱️  [{elapsed_s:5.1f}s] CRON REQUEST INGEST {cron_req_status.upper()}! "
                    f"Run #{top_entry['id']}: Ingested {top_entry.get('rows_ingested', 0)} rows "
                    f"in {float(top_entry.get('duration_s') or 0):.2f}s",
                    flush=True,
                )

        if cron_rum_detected_at is None:
            s_rm, d_rm = http_get(
                f"{be_url}/api/cron-runs?service_id={sid}&task={rum_task}&per_page=1", auth_headers, timeout=4.0
            )
            top_entry = new_terminal_run(d_rm, base_rum_run_id) if s_rm == 200 else None
            if top_entry is not None:
                cron_rum_detected_at = elapsed_s
                cron_rum_completed_utc = cron_completion_time(top_entry)
                print(
                    f"   ⏱️  [{elapsed_s:5.1f}s] CRON RUM INGEST {top_entry['status'].upper()}! "
                    f"Run #{top_entry['id']}: Ingested {top_entry.get('rows_ingested', 0)} rows "
                    f"in {float(top_entry.get('duration_s') or 0):.2f}s",
                    flush=True,
                )

        # Check Dashboard Bundle every 4s or immediately once cron/header updates
        should_check_dash = dashboard_detected_at is None and (
            header_detected_at is not None or cron_req_detected_at is not None or int(elapsed_s) % 4 == 0
        )
        if should_check_dash:
            s_d, data_d = http_post(
                f"{be_url}/api/dashboard/bundle?service_id={sid}", {"range_token": "24h"}, auth_headers, timeout=20.0
            )
            if s_d == 200 and isinstance(data_d, dict):
                ag = data_d.get("aggregates") or {}
                cur_d_rows = int(ag.get("total_rows") or 0)
                cur_d_ts = parse_iso(ag.get("latest_log_at"))

                if cur_d_rows > base_d_rows or (cur_d_ts and cur_d_ts > (base_d_latest or send_start_utc)):
                    dashboard_detected_at = elapsed_s
                    dashboard_detected_utc = now_utc
                    final_d_rows = cur_d_rows
                    final_d_ts = cur_d_ts
                    fresh_lag = (now_utc - cur_d_ts).total_seconds() if cur_d_ts else 0.0
                    print(
                        f"   ⏱️  [{elapsed_s:5.1f}s] DASHBOARD BUNDLE UPDATED!   Rows: {cur_d_rows:,} (+{cur_d_rows - base_d_rows}) │ Latest: {cur_d_ts} (Lag: {fresh_lag:.1f}s)",
                        flush=True,
                    )

        # Stop early if all updated
        if (
            header_detected_at is not None
            and cron_req_detected_at is not None
            and dashboard_detected_at is not None
            and (not rum_health_available or rum_serving_detected_at is not None)
        ):
            break

    now_final = datetime.now(UTC)
    print("\n📊 Probing All Analytics Pages to measure freshness & query latency...", flush=True)
    page_results = probe_analytics_pages(be_url, sid, auth_headers, now_ref=now_final)
    for p_name, p_res in page_results.items():
        st = p_res["status"]
        lat = p_res["latency_ms"]
        lag_s = p_res["freshness_lag_s"]
        lag_str = f"{lag_s:.1f}s behind" if lag_s is not None else "no data"
        print(f"   • {p_name:<22} Status: {st} │ Latency: {lat:6.1f}ms │ Freshness: {lag_str}", flush=True)

    return {
        "env": env_key,
        "name": name,
        "service_id": sid,
        "cdn_domain": cdn,
        "sent_at_utc": send_start_utc.isoformat(),
        "measurement_contract": {
            "source_timestamp": "probe_send_time",
            "fos_last_modified_measured": False,
            "serving_detection": "aggregate row count or latest event timestamp advanced",
            "correlated_to_probe_marker": False,
        },
        "configured_polling": polling,
        "deployment_mode": "high_scale" if is_high_scale else "standard",
        "rum_enabled": rum_enabled,
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
            "rum_beacons_added": (
                final_rum_beacons - base_rum_beacons
                if final_rum_beacons is not None and base_rum_beacons is not None
                else None
            ),
        },
        "page_freshness_matrix": page_results,
    }


def main():
    validate_admin_tls_config()
    parser = argparse.ArgumentParser(description="Measure edge-probe-to-active-serving visibility and delay")
    parser.add_argument(
        "--env",
        type=str,
        default="all",
        help="Target environments ('all', or comma-separated list: local-std,remote-std,remote-hs)",
    )
    parser.add_argument(
        "--active",
        action="store_true",
        default=True,
        help="Run continuous active-traffic benchmark measuring Min/Max/Avg lag (default: True)",
    )
    parser.add_argument(
        "--burst",
        action="store_true",
        help="Run legacy single-burst visibility delay benchmark instead of active traffic",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=30.0,
        help="Duration in seconds to stream active traffic and sample lag (default: 30s)",
    )
    parser.add_argument(
        "--rps",
        type=float,
        default=10.0,
        help="Continuous edge requests per second to send per target (default: 10 RPS)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="Sampling poll interval in seconds (default: 2.0s)",
    )
    parser.add_argument("--requests", type=int, default=10, help="Number of edge requests to send (burst mode)")
    parser.add_argument("--rum-beacons", type=int, default=5, help="Number of RUM beacons to send (burst mode)")
    parser.add_argument("--timeout", type=float, default=45.0, help="Max wait seconds for ingest (burst mode)")
    parser.add_argument("--output", type=str, default="reports/deploys/current/relics/e2e_freshness_report.json")
    args = parser.parse_args()
    _load_persisted_deploy_env()

    if args.env == "all":
        targets = list(ENVIRONMENTS.keys())
    elif "," in args.env:
        targets = [t.strip() for t in args.env.split(",") if t.strip()]
    else:
        targets = [args.env]

    try:
        admin_token = "" if all(admin_origin(target) for target in targets) else admin_token_from_environment()
    except ValueError as exc:
        if args.burst:
            parser.error(str(exc))
        admin_token = ""

    if args.burst:
        print(f"\n{'#' * 80}", flush=True)
        print("# FASTLY LOG ANALYTICS — SINGLE-BURST VISIBILITY DELAY BENCHMARK", flush=True)
        print(f"# Targets: {targets}", flush=True)
        print(f"{'#' * 80}", flush=True)

        results = []
        for tgt in targets:
            try:
                env_info = load_environment_config(tgt)
                res = measure_environment(
                    tgt,
                    env_info,
                    admin_token,
                    num_requests=args.requests,
                    num_rum=args.rum_beacons,
                    max_wait_s=args.timeout,
                )
            except (ValueError, KeyError) as exc:
                res = {"env": tgt, "name": ENVIRONMENTS.get(tgt, tgt), "error": str(exc)}
                print(f"  ERROR: {exc}", flush=True)
            results.append(res)
        print_burst_scorecard(results)
    else:
        results = measure_active_traffic_benchmark(
            targets=targets,
            admin_token=admin_token,
            duration_s=args.duration,
            rps=args.rps,
            sample_interval_s=args.interval,
        )

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"✅ Full benchmark report written to {out_path}\n", flush=True)


if __name__ == "__main__":
    main()

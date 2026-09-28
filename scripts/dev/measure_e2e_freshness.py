#!/usr/bin/env python3
"""Measure end-to-end edge-to-dashboard latency and freshness across environments.

Sends controlled edge traffic (HTTP requests + Faro RUM beacons) through Fastly CDN
and monitors how long until:
  1. Header badge updates (Request and RUM timestamps + row counts via /api/log-extents and /api/sync-status)
  2. Ingest cron runs complete (/api/cron-runs for log_discovery and rum_sync)
  3. Dashboard bundle reflects new data (/api/dashboard/bundle)
  4. Other analytics pages reflect the live state (/api/performance, /api/security, etc.)

Reports:
  - Wall-clock seconds from edge send to UI visibility
  - Freshness lag (seconds behind live real-time) for Header, Dashboard, and all pages.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ADMIN_SECRET = os.getenv("ADMIN_SHARED_SECRET", "RZ8qGEFbCYeGGI-PFRRTvw2sUp3x_sZs_asrqC9ENw0")

ENVIRONMENTS = {
    "local-std": {
        "name": "Local Standard (Native/Docker)",
        "service_id": "ZU15BvY2LX7WcEp43T9VwU",
        "backend_url": "http://127.0.0.1:80",
        "cdn_domain": "https://fla-local-standard-test.global.ssl.fastly.net",
    },
    "local-hs": {
        "name": "Local High-Scale (Docker Multipod)",
        "service_id": "qI4D8yXXFYOIpZEMrkJy65",
        "backend_url": "http://127.0.0.1:8081",
        "cdn_domain": "https://fla-local-high-scale-test.global.ssl.fastly.net",
    },
    "remote-std": {
        "name": "Remote Standard (GCE VM)",
        "service_id": "cVnu9mYB3Cvmob3lsqjQU3",
        "backend_url": "http://127.0.0.1:8001",
        "cdn_domain": "https://fastly-se-demo.global.ssl.fastly.net",
    },
    "remote-hs": {
        "name": "Remote High-Scale (Elevation K8s)",
        "service_id": "ZEZ4mcAjoSFDTg7tpkDKV2",
        "backend_url": "http://127.0.0.1:8002",
        "cdn_domain": "https://fla-k8s-scaling-test-elevation.global.ssl.fastly.net",
    },
}


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


def http_get(url: str, headers: dict[str, str] | None = None, timeout: float = 8.0) -> tuple[int, Any]:
    req_headers = {"Accept": "application/json", "User-Agent": "FLA-FreshnessProbe/1.0"}
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(url, headers=req_headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode("utf-8"))
        except Exception:
            return e.code, None
    except Exception as e:
        return 0, str(e)


def http_post(
    url: str, body: dict | None = None, headers: dict[str, str] | None = None, timeout: float = 12.0
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
        with urllib.request.urlopen(req, timeout=timeout) as r:
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


def measure_environment(
    env_key: str, env_info: dict[str, str], num_requests: int = 10, num_rum: int = 5, max_wait_s: float = 60.0
) -> dict[str, Any]:
    name = env_info["name"]
    sid = env_info["service_id"]
    be_url = env_info["backend_url"]
    cdn = env_info["cdn_domain"]
    auth_headers = {"x-admin-token": ADMIN_SECRET, "x-fastly-service-id": sid}

    print(f"\n{'=' * 78}", flush=True)
    print(f"  TARGET: {name}", flush=True)
    print(f"  Service ID: {sid} │ Backend: {be_url} │ CDN: {cdn}", flush=True)
    print(f"{'=' * 78}", flush=True)

    # 1. Capture Pre-Test Baseline
    le_status, le_data = http_get(f"{be_url}/api/log-extents?service_id={sid}", auth_headers, timeout=5.0)
    sync_status_code, sync_data = http_get(f"{be_url}/api/sync-status?service_id={sid}", auth_headers, timeout=5.0)

    base_latest_ts = parse_iso(le_data.get("latest_log_at") if isinstance(le_data, dict) else None)
    base_local_rows = int(sync_data.get("local_rows") or 0) if isinstance(sync_data, dict) else 0

    cr_req_s, cr_req_data = http_get(
        f"{be_url}/api/cron-runs?service_id={sid}&task=log_discovery&per_page=1", auth_headers
    )
    cr_rum_s, cr_rum_data = http_get(f"{be_url}/api/cron-runs?service_id={sid}&task=rum_sync&per_page=1", auth_headers)

    base_req_run_id = cr_req_data["entries"][0]["id"] if cr_req_s == 200 and cr_req_data.get("entries") else 0
    base_rum_run_id = cr_rum_data["entries"][0]["id"] if cr_rum_s == 200 and cr_rum_data.get("entries") else 0

    print(f"  [Baseline] Header Latest Log: {base_latest_ts}", flush=True)
    print(f"  [Baseline] Total Rows:        {base_local_rows:,}", flush=True)
    print(f"  [Baseline] Last Cron Runs:    log_discovery=#{base_req_run_id}, rum_sync=#{base_rum_run_id}", flush=True)

    d_status, d_data = http_post(
        f"{be_url}/api/dashboard/bundle?service_id={sid}", {"range_token": "24h"}, auth_headers, timeout=10.0
    )
    base_d_rows = 0
    base_d_latest = None
    if d_status == 200 and isinstance(d_data, dict):
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
    cron_req_detected_at: float | None = None
    cron_rum_detected_at: float | None = None
    dashboard_detected_at: float | None = None

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
        s_le, data_le = http_get(f"{be_url}/api/log-extents?service_id={sid}", auth_headers, timeout=4.0)
        s_ss, data_ss = http_get(f"{be_url}/api/sync-status?service_id={sid}", auth_headers, timeout=4.0)

        cur_ts = parse_iso(data_le.get("latest_log_at") if s_le == 200 and isinstance(data_le, dict) else None)
        cur_rows = int(data_ss.get("local_rows") or 0) if s_ss == 200 and isinstance(data_ss, dict) else base_local_rows

        if header_detected_at is None:
            if cur_rows > base_local_rows or (cur_ts and cur_ts > (base_latest_ts or send_start_utc)):
                header_detected_at = elapsed_s
                final_latest_ts = cur_ts
                final_local_rows = cur_rows
                fresh_lag = (now_utc - cur_ts).total_seconds() if cur_ts else 0.0
                print(
                    f"   ⏱️  [{elapsed_s:5.1f}s] HEADER BADGE UPDATED! Rows: {cur_rows:,} (+{cur_rows - base_local_rows}) │ Latest Log: {cur_ts} (Lag: {fresh_lag:.1f}s)",
                    flush=True,
                )

        # Check Ingest Crons (log_discovery and rum_sync)
        if cron_req_detected_at is None:
            s_cr, d_cr = http_get(
                f"{be_url}/api/cron-runs?service_id={sid}&task=log_discovery&per_page=1", auth_headers, timeout=4.0
            )
            if s_cr == 200 and d_cr.get("entries"):
                top_entry = d_cr["entries"][0]
                if top_entry["id"] > base_req_run_id and top_entry["status"] == "success":
                    cron_req_detected_at = elapsed_s
                    print(
                        f"   ⏱️  [{elapsed_s:5.1f}s] CRON REQUEST INGEST COMPLETE! Run #{top_entry['id']}: Ingested {top_entry.get('rows_ingested', 0)} rows in {top_entry.get('duration_s', 0):.2f}s",
                        flush=True,
                    )

        if cron_rum_detected_at is None:
            s_rm, d_rm = http_get(
                f"{be_url}/api/cron-runs?service_id={sid}&task=rum_sync&per_page=1", auth_headers, timeout=4.0
            )
            if s_rm == 200 and d_rm.get("entries"):
                top_entry = d_rm["entries"][0]
                if top_entry["id"] > base_rum_run_id and top_entry["status"] == "success":
                    cron_rum_detected_at = elapsed_s
                    print(
                        f"   ⏱️  [{elapsed_s:5.1f}s] CRON RUM INGEST COMPLETE!     Run #{top_entry['id']}: Ingested {top_entry.get('rows_ingested', 0)} rows in {top_entry.get('duration_s', 0):.2f}s",
                        flush=True,
                    )

        # Check Dashboard Bundle
        if dashboard_detected_at is None:
            s_d, data_d = http_post(
                f"{be_url}/api/dashboard/bundle?service_id={sid}", {"range_token": "24h"}, auth_headers, timeout=10.0
            )
            if s_d == 200 and isinstance(data_d, dict):
                ag = data_d.get("aggregates") or {}
                cur_d_rows = int(ag.get("total_rows") or 0)
                cur_d_ts = parse_iso(ag.get("latest_log_at"))

                if cur_d_rows > base_d_rows or (cur_d_ts and cur_d_ts > (base_d_latest or send_start_utc)):
                    dashboard_detected_at = elapsed_s
                    final_d_rows = cur_d_rows
                    final_d_ts = cur_d_ts
                    fresh_lag = (now_utc - cur_d_ts).total_seconds() if cur_d_ts else 0.0
                    print(
                        f"   ⏱️  [{elapsed_s:5.1f}s] DASHBOARD BUNDLE UPDATED!   Rows: {cur_d_rows:,} (+{cur_d_rows - base_d_rows}) │ Latest: {cur_d_ts} (Lag: {fresh_lag:.1f}s)",
                        flush=True,
                    )

        # Stop early if all updated
        if header_detected_at is not None and cron_req_detected_at is not None and dashboard_detected_at is not None:
            break

    now_final = datetime.now(UTC)

    # 4. Probe All Analytics Pages for End-to-End Freshness Comparison
    print("\n📊 Probing All Analytics Pages to measure freshness & query latency...", flush=True)
    page_probes = {
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
            st, res_data = http_get(p_url, auth_headers)
        else:
            st, res_data = http_post(p_url, p_payload, auth_headers)
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
        lag_str = f"{lag_s:.1f}s behind" if lag_s is not None else "no data"
        print(f"   • {page_name:<22} Status: {st} │ Latency: {t_latency_ms:6.1f}ms │ Freshness: {lag_str}", flush=True)

    return {
        "env": env_key,
        "name": name,
        "service_id": sid,
        "cdn_domain": cdn,
        "sent_at_utc": send_start_utc.isoformat(),
        "wall_clock_delays": {
            "header_seconds": round(header_detected_at, 1) if header_detected_at else None,
            "cron_request_seconds": round(cron_req_detected_at, 1) if cron_req_detected_at else None,
            "cron_rum_seconds": round(cron_rum_detected_at, 1) if cron_rum_detected_at else None,
            "dashboard_bundle_seconds": round(dashboard_detected_at, 1) if dashboard_detected_at else None,
        },
        "row_deltas": {
            "total_rows_added": final_local_rows - base_local_rows,
            "dashboard_rows_added": final_d_rows - base_d_rows,
        },
        "page_freshness_matrix": page_results,
    }


def main():
    parser = argparse.ArgumentParser(description="Measure end-to-end edge-to-dashboard pipeline lag")
    parser.add_argument("--env", choices=["all", "local-std", "local-hs", "remote-std", "remote-hs"], default="all")
    parser.add_argument("--requests", type=int, default=10, help="Number of edge requests to send")
    parser.add_argument("--rum-beacons", type=int, default=5, help="Number of RUM beacons to send")
    parser.add_argument("--timeout", type=float, default=45.0, help="Max wait seconds for ingest")
    parser.add_argument("--output", type=str, default="reports/deploys/current/relics/e2e_freshness_report.json")
    args = parser.parse_args()

    targets = list(ENVIRONMENTS.keys()) if args.env == "all" else [args.env]
    results = []

    print(f"\n{'#' * 80}", flush=True)
    print("# FASTLY LOG ANALYTICS — END-TO-END PIPELINE FRESHNESS & LAG BENCHMARK", flush=True)
    print(f"# Targets: {targets}", flush=True)
    print("# Ingest Cadence: 10s (Target Freshness: ~10s - 20s behind live real-time)", flush=True)
    print(f"{'#' * 80}", flush=True)

    for tgt in targets:
        env_info = ENVIRONMENTS[tgt]
        res = measure_environment(
            tgt, env_info, num_requests=args.requests, num_rum=args.rum_beacons, max_wait_s=args.timeout
        )
        results.append(res)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n✅ Full benchmark report written to {out_path}", flush=True)

    print(f"\n{'=' * 96}", flush=True)
    print("                      END-TO-END FRESHNESS & INGEST LAG SCORECARD", flush=True)
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
        delays = r["wall_clock_delays"]
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


if __name__ == "__main__":
    main()

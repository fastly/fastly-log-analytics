#!/usr/bin/env python3
"""Multi-Environment Infrastructure & Data Pipeline Audit Tool.

Audits all Fastly Log Analytics deployment environments (Local Standard,
Local High-Scale, GCE Standard, Elevation High-Scale):
  - Host & container vitals: CPU load, vCPUs, memory, data mount and root disk.
  - Scheduler & cron health: Scheduler tick freshness, in-flight runs, backlog detection,
    recent cron run errors/warnings.
  - Celery & Valkey pipeline (High-Scale): Queue depth, worker count, broker reachability.
  - Log ingest currency: Request and RUM latest log timestamps vs wall clock.
  - FOS queue verification: Verifies whether bucket is actively streaming, genuinely idle
    (0 files waiting in FOS), or stalled/backlogged.
  - Parquet compactions & rollups: Partition sprawl (partitions > 3 or 10 files), daily/weekly
    tier files, DuckLake/Iceberg durable bytes and files.
  - DuckDB pool health: Active/idle connections, p95/p99 wait latencies, saturation rejects.
  - ClickHouse state (High-Scale): Health, published/pending manifests.

Modes:
  - Point-in-time snapshot (--once, default)
  - Live continuous monitor (--watch, with --duration and --interval)
  - Machine-readable JSON output (--json)
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

# ANSI Color codes for clean terminal output
RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
MAGENTA = "\033[35m"
CYAN = "\033[36m"
WHITE = "\033[37m"
BG_RED = "\033[41m"
BG_GREEN = "\033[42m"
BG_YELLOW = "\033[43m"


@dataclass
class EnvironmentConfig:
    name: str
    architecture: str
    backend_url: str
    frontend_url: str
    service_id: str
    is_high_scale: bool = False
    admin_token: str | None = None


def load_dotenv_manually() -> None:
    """Manually parse .env file from the workspace root and set environment variables."""
    dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), ".env")
    if os.path.exists(dotenv_path):
        with open(dotenv_path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip("'\"")
                    if k and k not in os.environ:
                        os.environ[k] = v


load_dotenv_manually()


ENVIRONMENTS: dict[str, EnvironmentConfig] = {
    "local-standard": EnvironmentConfig(
        name="local-standard",
        architecture="Standard (Local Docker/Native)",
        backend_url="http://127.0.0.1:80",
        frontend_url="http://127.0.0.1:80/dashboard",
        service_id=os.getenv("LOCAL_STANDARD_SERVICE_ID", ""),
        is_high_scale=False,
    ),
    "local-high-scale": EnvironmentConfig(
        name="local-high-scale",
        architecture="High-Scale (Local Docker Multipod)",
        backend_url="http://127.0.0.1:8081",
        frontend_url="http://127.0.0.1:8081/dashboard",
        service_id=os.getenv("LOCAL_HIGH_SCALE_SERVICE_ID", ""),
        is_high_scale=True,
    ),
    "remote-standard": EnvironmentConfig(
        name="remote-standard",
        architecture="Standard (Remote/VM)",
        backend_url="http://127.0.0.1:8001",
        frontend_url="http://127.0.0.1:3001/dashboard",
        service_id=os.getenv("REMOTE_STANDARD_SERVICE_ID", ""),
        is_high_scale=False,
    ),
    "remote-high-scale": EnvironmentConfig(
        name="remote-high-scale",
        architecture="High-Scale (Remote/K8s)",
        backend_url="http://127.0.0.1:8002",
        frontend_url="http://127.0.0.1:3002/dashboard",
        service_id=os.getenv("REMOTE_HIGH_SCALE_SERVICE_ID", ""),
        is_high_scale=True,
    ),
}


def auto_resolve_remote_admin_token() -> str | None:
    """Attempts to resolve the remote admin token from env, or dynamically via the configured command."""
    load_dotenv_manually()
    for env_var in ("REMOTE_ADMIN_TOKEN", "ADMIN_SHARED_SECRET", "ADMIN_TOKEN"):
        val = os.getenv(env_var)
        if val:
            return val

    # Try discovering token from local SSH tunnel to remote-standard backend
    try:
        req = urllib.request.Request(
            "http://127.0.0.1:8001/api/bootstrap", headers={"User-Agent": "audit_environments"}
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            if resp.status == 200:
                data = json.loads(resp.read().decode("utf-8"))
                tok = data.get("settings", {}).get("admin_token")
                if tok:
                    return tok
    except Exception:
        pass

    # Try resolving via configured shell command
    resolve_cmd = os.getenv("REMOTE_ADMIN_TOKEN_RESOLVE_CMD")
    if not resolve_cmd:
        # Build standard fallback command if gcloud is present
        if shutil.which("gcloud"):
            gce_project = os.getenv("GCE_PROJECT", "")
            gce_zone = os.getenv("GCE_ZONE", "us-central1-a")
            gce_vm_name = os.getenv("GCE_VM_NAME", "fastly-log-analysis")
            if gce_project and gce_zone and gce_vm_name:
                resolve_cmd = (
                    f"gcloud compute ssh {gce_vm_name} --project={gce_project} --zone={gce_zone} "
                    '--command "docker exec app-backend-1 env | grep ADMIN_SHARED_SECRET"'
                )

    if resolve_cmd:
        try:
            # Run the configured shell command
            out = (
                subprocess.check_output(resolve_cmd, shell=True, stderr=subprocess.DEVNULL, timeout=8).decode().strip()
            )
            if "=" in out:
                return out.split("=", 1)[1].strip()
            elif out and "=" not in out:
                # If command directly outputs the token
                return out
        except Exception:
            pass
    return None


@dataclass
class AuditResult:
    target: str
    architecture: str
    service_id: str
    backend_url: str
    reachable: bool = False
    latency_ms: float = 0.0
    status_code: int = 0
    version: str | None = None
    vcpus: int = 1
    cpu_load_1m: float = 0.0
    cpu_load_5m: float = 0.0
    cpu_load_15m: float = 0.0
    mem_used_pct: float = 0.0
    mem_available_mb: float = 0.0
    mem_total_mb: float = 0.0
    data_mount_used_pct: float = 0.0
    data_mount_free_gb: float = 0.0
    root_disk_used_pct: float = 0.0
    root_disk_free_gb: float = 0.0
    scheduler_tick_age_s: float | None = None
    in_flight_runs: list[dict[str, Any]] = field(default_factory=list)
    recent_failures: list[dict[str, Any]] = field(default_factory=list)
    celery_reachable: bool | None = None
    celery_queue_depth: int | None = None
    celery_workers: int | None = None
    request_latest_log_at: str | None = None
    request_latest_log_age_s: float | None = None
    request_total_rows: int | None = None
    request_last_sync_at: str | None = None
    rum_latest_log_at: str | None = None
    rum_latest_log_age_s: float | None = None
    rum_total_rows: int | None = None
    rum_last_sync_at: str | None = None
    ingest_state: str = "UNKNOWN"  # STREAMING, IDLE, STALLED, NO_LOGS
    fos_reachable: bool = False
    fos_error: str | None = None
    compaction_total_files: int = 0
    compaction_daily_files: int = 0
    compaction_weekly_files: int = 0
    compaction_partitions_above_3: int = 0
    compaction_partitions_above_10: int = 0
    duckdb_size_bytes: int = 0
    iceberg_bytes: int = 0
    iceberg_files: int = 0
    pool_saturated_rejects: int = 0
    pool_wait_p95_ms: float = 0.0
    clickhouse_health: str | None = None
    clickhouse_pending: int | None = None
    clickhouse_published: int | None = None
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def http_get_json(url: str, headers: dict[str, str] | None = None, timeout: float = 5.0) -> tuple[int, Any]:
    req_headers = {
        "User-Agent": "FLA-AuditTool/1.0",
        "Accept": "application/json",
        "Connection": "close",
    }
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(url, headers=req_headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return resp.status, data
    except urllib.error.HTTPError as e:
        try:
            err_body = json.loads(e.read().decode("utf-8"))
            return e.code, err_body
        except Exception:
            return e.code, {"error": str(e)}
    except Exception as e:
        return 0, {"error": str(e)}


def http_post_json(
    url: str, payload: dict | list | None = None, headers: dict[str, str] | None = None, timeout: float = 5.0
) -> tuple[int, Any]:
    req_headers = {
        "User-Agent": "FLA-AuditTool/1.0",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Connection": "close",
    }
    if headers:
        req_headers.update(headers)
    body = json.dumps(payload if payload is not None else {}).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=req_headers, method="POST")
    for attempt in range(2):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return resp.status, data
        except urllib.error.HTTPError as e:
            try:
                err_body = json.loads(e.read().decode("utf-8"))
                return e.code, err_body
            except Exception:
                return e.code, {"error": str(e)}
        except (ConnectionResetError, urllib.error.URLError) as e:
            if attempt == 0:
                time.sleep(0.5)
                continue
            return 0, {"error": str(e)}
        except Exception as e:
            return 0, {"error": str(e)}
    return 0, {"error": "request failed"}


def parse_timestamp_age(ts_str: str | None) -> float | None:
    if not ts_str:
        return None
    try:
        clean_ts = ts_str.replace("Z", "+00:00")
        if " " in clean_ts and "T" not in clean_ts:
            parts = clean_ts.split(" ")
            clean_ts = f"{parts[0]}T{parts[1]}+00:00"
        dt = datetime.fromisoformat(clean_ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return max(0.0, (datetime.now(UTC) - dt).total_seconds())
    except Exception:
        return None


def format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "—"
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        return f"{seconds / 60:.1f}m"
    if seconds < 86400:
        return f"{seconds / 3600:.1f}h"
    return f"{seconds / 86400:.1f}d"


def format_bytes(b: int | float | None) -> str:
    if b is None or b == 0:
        return "0 B"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(b) < 1024.0:
            return f"{b:3.1f} {unit}"
        b /= 1024.0
    return f"{b:.1f} PB"


def audit_target(env: EnvironmentConfig, timeout: float = 5.0) -> AuditResult:
    res = AuditResult(
        target=env.name,
        architecture=env.architecture,
        service_id=env.service_id,
        backend_url=env.backend_url,
    )

    req_headers: dict[str, str] = {}
    if env.admin_token:
        req_headers["X-Admin-Token"] = env.admin_token

    # 1. Ping /api/health first — every other probe depends on the backend
    # being alive, and there is no reason to fire five more requests at one
    # that's already down.
    t0 = time.perf_counter()
    status, health_data = http_get_json(f"{env.backend_url}/api/health?deep=1", headers=req_headers, timeout=timeout)
    res.latency_ms = (time.perf_counter() - t0) * 1000.0
    res.status_code = status

    if status != 200 or health_data.get("status") != "ok":
        res.reachable = False
        res.errors.append(f"Backend unhealthy: HTTP {status} (data: {health_data})")
        return res

    res.reachable = True
    res.version = health_data.get("version")

    # Discover active service_id if not explicitly configured
    active_sid = env.service_id
    if not active_sid and health_data.get("services"):
        active_sid = health_data["services"][0].get("service_id", "")
    res.service_id = active_sid

    # 2-6. The remaining probes don't depend on one another — fire them
    # concurrently instead of paying for 4-5 sequential round trips per
    # environment. Keeps a single audit_target() call fast enough that
    # --watch's --interval isn't routinely blown by its own probing.
    probes: dict[str, tuple[Any, tuple, dict]] = {
        "bootstrap": (
            http_get_json,
            (f"{env.backend_url}/api/bootstrap",),
            {"headers": req_headers, "timeout": timeout},
        ),
        "health_snapshot": (
            http_get_json,
            (f"{env.backend_url}/api/admin/health-snapshot?probe_fos=1",),
            {"headers": req_headers, "timeout": timeout},
        ),
        "cron_runs": (
            http_get_json,
            (f"{env.backend_url}/api/cron-runs?service_id={active_sid}&task=log_discovery&per_page=3",),
            {"headers": req_headers, "timeout": timeout},
        ),
        # NOTE: the scan window MUST be passed as the `range_token` body
        # field, not a `range=` query param — /api/dashboard/bundle has no
        # such query param, and an empty body (both start_time/end_time
        # None) hits build_where_clause's no-bounds branch, which adds NO
        # time predicate at all: an unbounded full-table scan with zero
        # partition pruning, on every audit tick. `range_token: "24h"` is
        # the real wire contract (see backend/utils/time_window.py) and is
        # what actually bounds this probe to the 24h window it claims to
        # exercise.
        "dashboard_bundle": (
            http_post_json,
            (f"{env.backend_url}/api/dashboard/bundle?service_id={active_sid}",),
            {"payload": {"range_token": "24h"}, "headers": req_headers, "timeout": max(timeout, 45.0)},
        ),
    }
    if env.is_high_scale:
        probes["clickhouse_status"] = (
            http_get_json,
            (f"{env.backend_url}/api/admin/clickhouse/status?service_id={active_sid}",),
            {"headers": req_headers, "timeout": timeout},
        )

    with ThreadPoolExecutor(max_workers=len(probes)) as pool:
        futures = {name: pool.submit(fn, *args, **kwargs) for name, (fn, args, kwargs) in probes.items()}
        outcomes = {name: fut.result() for name, fut in futures.items()}

    b_status, b_data = outcomes["bootstrap"]
    hs_status, hs_data = outcomes["health_snapshot"]
    cron_status, cron_data = outcomes["cron_runs"]
    d_status, d_data = outcomes["dashboard_bundle"]
    ch_status, ch_data = outcomes.get("clickhouse_status", (0, {}))

    # Bootstrap parsing (unauthenticated baseline)
    if b_status == 200 and isinstance(b_data, dict):
        status_sec = b_data.get("sync_status") or b_data.get("status") or {}
        req_sec = status_sec.get("request") or (b_data.get("header_badge") or {}).get("request") or {}
        rum_sec = status_sec.get("rum") or (b_data.get("header_badge") or {}).get("rum") or {}

        res.request_latest_log_at = req_sec.get("latest_log_at")
        res.request_latest_log_age_s = parse_timestamp_age(res.request_latest_log_at)
        res.request_total_rows = req_sec.get("total_rows") or status_sec.get("local_rows")
        res.request_last_sync_at = req_sec.get("last_sync_at")

        res.rum_latest_log_at = rum_sec.get("latest_log_at")
        res.rum_latest_log_age_s = parse_timestamp_age(res.rum_latest_log_at)
        res.rum_total_rows = rum_sec.get("total_rows")
        res.rum_last_sync_at = rum_sec.get("last_sync_at")

        res.duckdb_size_bytes = status_sec.get("duckdb_size_bytes", 0)
        res.iceberg_bytes = status_sec.get("iceberg_bytes", 0)
        res.iceberg_files = status_sec.get("iceberg_files", 0)

    if hs_status == 200 and isinstance(hs_data, dict):
        # CPU
        load = hs_data.get("load") or {}
        res.cpu_load_1m = float(load.get("avg_1m", 0.0))
        res.cpu_load_5m = float(load.get("avg_5m", 0.0))
        res.cpu_load_15m = float(load.get("avg_15m", 0.0))
        res.vcpus = int(hs_data.get("vcpus", 1))

        if res.cpu_load_1m > (res.vcpus * 2.0):
            res.warnings.append(f"High CPU load: 1m avg {res.cpu_load_1m:.1f} exceeds 2x vCPUs ({res.vcpus})")

        # Memory
        mem = hs_data.get("memory") or {}
        res.mem_used_pct = float(mem.get("used_pct", 0.0))
        res.mem_available_mb = float(mem.get("available_mb", 0.0))
        res.mem_total_mb = float(mem.get("total_mb", 0.0))
        if res.mem_used_pct > 92.0:
            res.errors.append(f"Critical memory usage: {res.mem_used_pct:.1f}%")
        elif res.mem_used_pct > 85.0:
            res.warnings.append(f"Elevated memory usage: {res.mem_used_pct:.1f}%")

        # Disks
        dm = hs_data.get("data_mount") or {}
        res.data_mount_used_pct = float(dm.get("used_pct", 0.0))
        res.data_mount_free_gb = float(dm.get("free_gb", 0.0))
        if res.data_mount_used_pct > 95.0:
            res.errors.append(
                f"Data mount nearly full: {res.data_mount_used_pct:.1f}% used ({res.data_mount_free_gb:.1f} GB free)"
            )
        elif res.data_mount_used_pct > 85.0:
            res.warnings.append(
                f"Data mount high: {res.data_mount_used_pct:.1f}% used ({res.data_mount_free_gb:.1f} GB free)"
            )

        rd = hs_data.get("root_disk") or {}
        res.root_disk_used_pct = float(rd.get("used_pct", 0.0))
        res.root_disk_free_gb = float(rd.get("free_gb", 0.0))

        # Scheduler
        res.scheduler_tick_age_s = hs_data.get("scheduler_last_tick_age_s")
        if res.scheduler_tick_age_s is not None:
            if res.scheduler_tick_age_s > 60.0:
                res.errors.append(f"Scheduler stalled! Last tick was {res.scheduler_tick_age_s:.0f}s ago")
            elif res.scheduler_tick_age_s > 35.0:
                res.warnings.append(f"Scheduler behind schedule: tick age {res.scheduler_tick_age_s:.0f}s")

        res.in_flight_runs = hs_data.get("in_flight_runs") or []
        for ifr in res.in_flight_runs:
            dur = ifr.get("duration_s", 0.0)
            task = ifr.get("task", "unknown")
            if dur > 300:
                res.warnings.append(f"Long running job: {task} has been running for {dur:.0f}s")

        res.recent_failures = hs_data.get("recent_cron_failures") or []
        for fail in res.recent_failures:
            started = fail.get("started_at")
            age_s = parse_timestamp_age(started)
            age_str = f" ({format_duration(age_s)} ago)" if started else ""
            err_msg = fail.get("error_message") or fail.get("summary") or fail.get("status") or "error"
            # Only alert on failures within the last 24h
            if age_s is None or age_s <= 86400:
                res.warnings.append(f"Cron failure in {fail.get('task')}{age_str}: {err_msg}")

        # Celery / Valkey
        celery = hs_data.get("celery")
        if celery and isinstance(celery, dict):
            res.celery_reachable = celery.get("broker_reachable")
            res.celery_queue_depth = celery.get("queue_depth")
            res.celery_workers = celery.get("active_workers")
            if res.celery_reachable is False:
                res.errors.append("Celery broker unreachable!")
            elif (res.celery_workers or 0) == 0:
                res.errors.append("No active Celery workers online!")
            elif (res.celery_queue_depth or 0) > 100:
                res.warnings.append(f"High Celery queue backlog: {res.celery_queue_depth} tasks queued")

        # FOS probe
        fos = hs_data.get("fos")
        if fos and isinstance(fos, dict):
            svc_fos = fos.get(env.service_id) or {}
            res.fos_reachable = svc_fos.get("reachable", False)
            res.fos_error = svc_fos.get("error")
            if not res.fos_reachable and res.fos_error:
                res.errors.append(f"FOS probe failed: {res.fos_error}")

        # Compaction stats
        comp = hs_data.get("compaction") or {}
        svc_comp = comp.get(env.service_id) or {}
        res.compaction_total_files = svc_comp.get("total_files", 0)
        res.compaction_daily_files = svc_comp.get("daily_files", 0)
        res.compaction_weekly_files = svc_comp.get("weekly_files", 0)
        res.compaction_partitions_above_3 = svc_comp.get("partitions_above_3", 0)
        res.compaction_partitions_above_10 = svc_comp.get("partitions_above_10", 0)
        if res.compaction_partitions_above_10 > 0:
            res.warnings.append(
                f"Uncompacted partitions: {res.compaction_partitions_above_10} partitions have >10 files"
            )

        # Pool stats
        pools = hs_data.get("pool_wait") or []
        for p in pools:
            if p.get("service") == active_sid:
                res.pool_saturated_rejects += p.get("saturated_rejects_total", 0)
                wait_stats = p.get("wait") or {}
                res.pool_wait_p95_ms = max(res.pool_wait_p95_ms, float(wait_stats.get("p95_ms", 0.0)))
        if res.pool_saturated_rejects > 0:
            res.warnings.append(f"DuckDB pool saturated: {res.pool_saturated_rejects} rejected queries (lifetime)")

    elif hs_status == 401:
        res.errors.append("Authentication required for /api/admin/health-snapshot (pass --admin-token)")

    # Ingest state determination (uses the bootstrap + cron-runs results fetched above)
    last_disc_summary = ""
    if cron_status == 200 and isinstance(cron_data, dict):
        entries = cron_data.get("entries") or []
        if entries:
            last_disc_summary = entries[0].get("summary") or ""

    if res.request_latest_log_age_s is not None:
        if res.request_latest_log_age_s <= 900:  # <= 15m
            res.ingest_state = "STREAMING"
        else:
            if "No new log files" in last_disc_summary or "Discovered 0" in last_disc_summary:
                res.ingest_state = "IDLE (0 in FOS)"
            elif "Ingested" in last_disc_summary or "Discovered" in last_disc_summary:
                res.ingest_state = "STREAMING (Catching up)"
            else:
                res.ingest_state = "IDLE (No recent traffic)"
    else:
        res.ingest_state = "NO LOGS"

    # ClickHouse status (if high scale; fetched above)
    if env.is_high_scale:
        if ch_status == 200 and isinstance(ch_data, dict):
            res.clickhouse_health = ch_data.get("health")
            pub_counts = ch_data.get("publication_counts") or {}
            res.clickhouse_pending = pub_counts.get("pending", 0)
            res.clickhouse_published = pub_counts.get("published", 0)
            if res.clickhouse_health not in ("ok", "disabled", None):
                res.warnings.append(f"ClickHouse state: {res.clickhouse_health}")

    # Active Dashboard Analytical Query Probe (fetched above, bounded to a 24h range_token)
    if d_status != 200:
        res.errors.append(f"Dashboard query failed: HTTP {d_status} (data: {d_data})")
    elif isinstance(d_data, dict):
        if d_data.get("unhandled_error"):
            res.errors.append(f"Dashboard query unhandled error: {d_data.get('unhandled_error')}")
        elif "error" in d_data:
            res.errors.append(f"Dashboard query error: {d_data.get('error')}")
        elif "aggregates" not in d_data:
            res.warnings.append("Dashboard bundle response missing aggregates")

    return res


def print_audit_report(results: list[AuditResult], clear_screen: bool = False) -> None:
    if clear_screen and sys.stdout.isatty():
        sys.stdout.write("\033[H\033[J")

    now_str = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(
        f"\n{BOLD}{CYAN}╔═══════════════════════════════════════════════════════════════════════════════════════════╗{RESET}"
    )
    print(
        f"{BOLD}{CYAN}║                     FASTLY LOG ANALYTICS — MULTI-ENVIRONMENT AUDIT                       ║{RESET}"
    )
    print(f"{BOLD}{CYAN}║                     Timestamp: {now_str:<50} ║{RESET}")
    print(
        f"{BOLD}{CYAN}╚═══════════════════════════════════════════════════════════════════════════════════════════╝{RESET}\n"
    )

    for r in results:
        status_tag = f"{BG_GREEN}{WHITE} REACHABLE {RESET}" if r.reachable else f"{BG_RED}{WHITE} UNREACHABLE {RESET}"
        print(f"{BOLD}{WHITE}● {r.target.upper()}{RESET} — {DIM}{r.architecture}{RESET} {status_tag}")
        print(
            f"  {DIM}URL:{RESET} {r.backend_url:<28} {DIM}Service ID:{RESET} {r.service_id} {DIM}Latency:{RESET} {r.latency_ms:.1f}ms"
        )

        if not r.reachable:
            for err in r.errors:
                print(f"  {RED}✖ {err}{RESET}")
            print()
            continue

        # Resources Line
        cpu_color = RED if r.cpu_load_1m > (r.vcpus * 2) else (YELLOW if r.cpu_load_1m > r.vcpus else GREEN)
        mem_color = RED if r.mem_used_pct > 90 else (YELLOW if r.mem_used_pct > 80 else GREEN)
        disk_color = RED if r.data_mount_used_pct > 90 else (YELLOW if r.data_mount_used_pct > 80 else GREEN)

        print(
            f"  {BOLD}Resources:{RESET} "
            f"CPU: {cpu_color}{r.cpu_load_1m:.2f}{RESET}/{r.cpu_load_5m:.2f}/{r.cpu_load_15m:.2f} ({r.vcpus} vCPUs) │ "
            f"RAM: {mem_color}{r.mem_used_pct:.1f}%{RESET} ({r.mem_available_mb / 1024:.1f}/{r.mem_total_mb / 1024:.1f} GB free) │ "
            f"Disk: {disk_color}{r.data_mount_used_pct:.1f}%{RESET} ({r.data_mount_free_gb:.1f} GB free)"
        )

        # Ingest & Storage Line
        ingest_color = GREEN if "STREAMING" in r.ingest_state else (CYAN if "IDLE" in r.ingest_state else YELLOW)
        req_age_str = format_duration(r.request_latest_log_age_s)
        rum_age_str = format_duration(r.rum_latest_log_age_s)

        print(
            f"  {BOLD}Ingest:{RESET}    "
            f"State: {ingest_color}{r.ingest_state}{RESET} │ "
            f"Request: {r.request_total_rows or 0:,} rows (latest {req_age_str} ago) │ "
            f"RUM: {r.rum_total_rows or 0:,} rows (latest {rum_age_str} ago)"
        )

        # Scheduler & Storage / Compaction Line
        sched_color = RED if (r.scheduler_tick_age_s or 0) > 45 else GREEN
        comp_color = YELLOW if r.compaction_partitions_above_10 > 0 else GREEN

        sched_str = f"{r.scheduler_tick_age_s:.0f}s ago" if r.scheduler_tick_age_s is not None else "—"
        storage_parts = [f"Scheduler Tick: {sched_color}{sched_str}{RESET}"]
        if r.duckdb_size_bytes > 0:
            storage_parts.append(f"DuckDB: {format_bytes(r.duckdb_size_bytes)}")
        if r.iceberg_files > 0 or r.iceberg_bytes > 0:
            storage_parts.append(f"DuckLake: {format_bytes(r.iceberg_bytes)} ({r.iceberg_files} files)")
        storage_parts.append(
            f"Compaction: {comp_color}{r.compaction_total_files} files ({r.compaction_daily_files} daily, {r.compaction_weekly_files} weekly){RESET}"
        )

        storage_line = f"  {BOLD}Pipeline:{RESET}  {' │ '.join(storage_parts)}"
        if r.compaction_partitions_above_10 > 0:
            storage_line += f" {YELLOW}[sprawl: {r.compaction_partitions_above_10} dirs >10 files]{RESET}"
        print(storage_line)

        # High-Scale & Pool Line
        extra_parts: list[str] = []
        if r.celery_reachable is not None:
            c_color = GREEN if r.celery_reachable and (r.celery_workers or 0) > 0 else RED
            extra_parts.append(
                f"Celery: {c_color}{r.celery_workers} workers, queue depth {r.celery_queue_depth}{RESET}"
            )
        if r.clickhouse_health:
            ch_color = GREEN if r.clickhouse_health == "ok" else YELLOW
            extra_parts.append(
                f"ClickHouse: {ch_color}{r.clickhouse_health}{RESET} ({r.clickhouse_published} published, {r.clickhouse_pending} pending)"
            )
        if r.pool_wait_p95_ms > 0:
            extra_parts.append(f"DuckDB Pool p95: {r.pool_wait_p95_ms:.2f}ms")

        if extra_parts:
            print(f"  {BOLD}Scale/Pool:{RESET} {' │ '.join(extra_parts)}")

        # Warnings / Errors
        if r.warnings:
            for w in r.warnings:
                print(f"  {YELLOW}⚠ Warning: {w}{RESET}")
        if r.errors:
            for e in r.errors:
                print(f"  {RED}✖ Error: {e}{RESET}")

        print()

    # Global Summary
    total_envs = len(results)
    healthy_envs = sum(1 for r in results if r.reachable and not r.errors and not r.warnings)
    warning_envs = sum(1 for r in results if r.reachable and r.warnings and not r.errors)
    unhealthy_envs = sum(1 for r in results if not r.reachable or r.errors)

    print(
        f"{BOLD}SUMMARY:{RESET} {total_envs} Environments Audited │ {GREEN}{healthy_envs} Optimal{RESET} │ {YELLOW}{warning_envs} Warnings{RESET} │ {RED}{unhealthy_envs} Degraded{RESET}"
    )


def run_audit(
    target_names: list[str] | None = None,
    admin_token: str | None = None,
    timeout: float = 5.0,
) -> list[AuditResult]:
    # Resolve targets
    envs_to_check: list[EnvironmentConfig] = []
    if target_names and "all" not in target_names:
        for name in target_names:
            if name in ENVIRONMENTS:
                envs_to_check.append(ENVIRONMENTS[name])
            else:
                print(
                    f"{YELLOW}Warning: Unknown environment '{name}'. Available: {list(ENVIRONMENTS.keys())}{RESET}",
                    file=sys.stderr,
                )
    else:
        envs_to_check = list(ENVIRONMENTS.values())

    if os.getenv("IGNORE_LOCAL_STD") == "1":
        envs_to_check = [e for e in envs_to_check if e.name != "local-standard"]

    # Configure tokens
    remote_token = admin_token or auto_resolve_remote_admin_token()
    for env in envs_to_check:
        if env.name == "remote-standard" and remote_token:
            env.admin_token = remote_token

    results: list[AuditResult] = []
    with ThreadPoolExecutor(max_workers=min(len(envs_to_check), 4)) as executor:
        future_to_env = {executor.submit(audit_target, env, timeout): env for env in envs_to_check}
        for future in as_completed(future_to_env):
            try:
                results.append(future.result())
            except Exception as e:
                env = future_to_env[future]
                res = AuditResult(
                    target=env.name,
                    architecture=env.architecture,
                    service_id=env.service_id,
                    backend_url=env.backend_url,
                    errors=[f"Audit execution exception: {e}"],
                )
                results.append(res)

    # Sort results in consistent order
    env_order = list(ENVIRONMENTS.keys())
    results.sort(key=lambda r: env_order.index(r.target) if r.target in env_order else 999)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fastly Log Analytics Multi-Environment Diagnostic & Health Audit",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--env",
        nargs="+",
        default=["all"],
        help=f"Environments to audit (default: all). Choices: {list(ENVIRONMENTS.keys())} or 'all'",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Run continuously in watch monitor mode",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=0.0,
        help="Duration in minutes to run in watch mode (default: 0 = indefinite until Ctrl+C)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=5.0,
        help="Polling interval in seconds for watch mode (default: 5.0)",
    )
    parser.add_argument(
        "--admin-token",
        type=str,
        default=None,
        help="Admin token for authenticated endpoints (defaults to auto-detection from env/gcloud)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="HTTP request timeout in seconds (default: 10.0)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output raw audit results as JSON",
    )

    args = parser.parse_args()

    if not args.watch:
        results = run_audit(target_names=args.env, admin_token=args.admin_token, timeout=args.timeout)
        if args.json:
            print(json.dumps([asdict(r) for r in results], indent=2))
        else:
            print_audit_report(results, clear_screen=False)
        has_critical = any(not r.reachable or len(r.errors) > 0 for r in results)
        return 1 if has_critical else 0

    # Watch mode
    start_time = time.time()
    max_duration_s = args.duration * 60.0 if args.duration > 0 else float("inf")
    last_results: list[AuditResult] = []
    try:
        while True:
            elapsed = time.time() - start_time
            if elapsed >= max_duration_s:
                print(f"\n{GREEN}Completed watch duration of {args.duration:.1f} minute(s). Exiting.{RESET}")
                break

            last_results = run_audit(target_names=args.env, admin_token=args.admin_token, timeout=args.timeout)
            if args.json:
                print(json.dumps([asdict(r) for r in last_results]))
            else:
                print_audit_report(last_results, clear_screen=True)
                if max_duration_s < float("inf"):
                    remaining_s = max_duration_s - elapsed
                    print(
                        f"{DIM}Watching... Elapsed: {elapsed:.0f}s / {max_duration_s:.0f}s (Remaining: {remaining_s:.0f}s, Interval: {args.interval}s, Ctrl+C to stop){RESET}"
                    )
                else:
                    print(
                        f"{DIM}Watching... Elapsed: {elapsed:.0f}s (Interval: {args.interval}s, Ctrl+C to stop){RESET}"
                    )

            time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\n{YELLOW}Audit watch stopped by operator.{RESET}")

    has_critical = any(not r.reachable or len(r.errors) > 0 for r in last_results)
    return 1 if has_critical else 0


if __name__ == "__main__":
    sys.exit(main())

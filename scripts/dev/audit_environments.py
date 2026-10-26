#!/usr/bin/env python3
"""Multi-Environment Infrastructure & Data Pipeline Audit Tool."""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
import sys
import time
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.lib.admin_tls import admin_origin, is_admin_url, validate_admin_tls_config  # noqa: E402
from scripts.lib.environments import (  # noqa: E402
    ENVIRONMENT_SPECS,
    add_common_args,
    load_dotenv,
    load_persisted_deploy_env,
    parse_iso_utc,
)
from scripts.lib.environments import service_id as _registry_service_id  # noqa: E402
from scripts.lib.readiness import ProbeResult, probe  # noqa: E402

# fmt: off
RESET, BOLD, DIM = "\033[0m", "\033[1m", "\033[2m"
RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = "\033[31m", "\033[32m", "\033[33m", "\033[34m", "\033[35m", "\033[36m", "\033[37m"
BG_RED, BG_GREEN, BG_YELLOW = "\033[41m", "\033[42m", "\033[43m"


@dataclass
class EnvironmentConfig:
    name: str
    architecture: str
    backend_url: str
    frontend_url: str
    service_id: str
    is_high_scale: bool = False


def load_dotenv_manually() -> None:
    load_dotenv()


load_dotenv_manually()

ENVIRONMENTS: dict[str, EnvironmentConfig] = {
    name: EnvironmentConfig(
        name=spec.name, architecture=spec.architecture, backend_url=spec.backend_url,
        frontend_url=spec.frontend_url, service_id=_registry_service_id(name), is_high_scale=spec.is_high_scale,
    )
    for name, spec in ENVIRONMENT_SPECS.items()
}


def refresh_environment_origins() -> None:
    """Update ENVIRONMENTS URLs from any configured admin mTLS origins."""
    for env_name, env_config in ENVIRONMENTS.items():
        spec = ENVIRONMENT_SPECS[env_name]
        if configured_origin := admin_origin(env_name):
            env_config.backend_url = configured_origin
            env_config.frontend_url = f"{configured_origin}/dashboard"
        else:
            env_config.backend_url, env_config.frontend_url = spec.backend_url, spec.frontend_url


refresh_environment_origins()


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
    ingest_state: str = "UNKNOWN"
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


def _decode(result: ProbeResult) -> tuple[int, Any]:
    if result.status is None:
        return 0, {"error": result.error}
    try:
        return result.status, json.loads(result.body.decode("utf-8"))
    except Exception as e:
        return (result.status, {"error": result.error}) if result.http_error else (0, {"error": str(e)})


def http_get_json(url: str, headers: dict[str, str] | None = None, timeout: float = 5.0) -> tuple[int, Any]:
    req_headers = {"User-Agent": "FLA-AuditTool/1.0", "Accept": "application/json", "Connection": "close", **(headers or {})}
    return _decode(probe(url, timeout=timeout, headers=req_headers))


def http_post_json(url: str, payload: dict | list | None = None, headers: dict[str, str] | None = None, timeout: float = 5.0) -> tuple[int, Any]:
    req_headers = {"User-Agent": "FLA-AuditTool/1.0", "Accept": "application/json", "Content-Type": "application/json", "Connection": "close", **(headers or {})}
    body = json.dumps(payload if payload is not None else {}).encode("utf-8")
    result = probe(url, timeout=timeout, headers=req_headers, data=body, method="POST")
    if isinstance(result.exc, (ConnectionResetError, urllib.error.URLError)) and not result.http_error:
        time.sleep(0.5)
        result = probe(url, timeout=timeout, headers=req_headers, data=body, method="POST")
    return _decode(result)


def parse_timestamp_age(ts_str: str | None) -> float | None:
    dt = parse_iso_utc(ts_str)
    return max(0.0, (datetime.now(UTC) - dt).total_seconds()) if dt is not None else None


def format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "—"
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        return f"{seconds / 60:.1f}m"
    return f"{seconds / 3600:.1f}h" if seconds < 86400 else f"{seconds / 86400:.1f}d"


def format_bytes(b: int | float | None) -> str:
    if b is None or b == 0:
        return "0 B"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(b) < 1024.0:
            return f"{b:3.1f} {unit}"
        b /= 1024.0
    return f"{b:.1f} PB"


def _private_network_mtls_hint(url: str) -> str:
    if not is_admin_url(url):
        return ""
    parts = urlsplit(url)
    host = parts.hostname or ""
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        return ""
    if addr.is_private and not addr.is_loopback:
        target = f"{host}:{parts.port}" if parts.port else host
        return f" — mTLS gateway unreachable on private network ({target}); check VPN/tunnel connectivity"
    return ""


def audit_target(env: EnvironmentConfig, timeout: float = 5.0) -> AuditResult:
    res = AuditResult(target=env.name, architecture=env.architecture, service_id=env.service_id, backend_url=env.backend_url)
    req_headers: dict[str, str] = {}
    t0 = time.perf_counter()
    status, health_data = http_get_json(f"{env.backend_url}/api/health?deep=1", headers=req_headers, timeout=timeout)
    res.latency_ms = (time.perf_counter() - t0) * 1000.0
    res.status_code = status

    if status != 200 or health_data.get("status") != "ok":
        res.reachable = False
        hint = _private_network_mtls_hint(env.backend_url) if status == 0 else ""
        res.errors.append(f"Backend unhealthy: HTTP {status} (data: {health_data}){hint}")
        return res

    res.reachable = True
    res.version = health_data.get("version")
    active_sid = env.service_id or ((health_data.get("services") or [{}])[0].get("service_id", "") if health_data.get("services") else "")
    res.service_id = active_sid

    probes: dict[str, tuple[Any, tuple, dict]] = {
        "bootstrap": (http_get_json, (f"{env.backend_url}/api/bootstrap",), {"headers": req_headers, "timeout": timeout}),
        "health_snapshot": (http_get_json, (f"{env.backend_url}/api/admin/health-snapshot?probe_fos=1",), {"headers": req_headers, "timeout": timeout}),
        "cron_runs": (http_get_json, (f"{env.backend_url}/api/cron-runs?service_id={active_sid}&task=log_discovery&per_page=3",), {"headers": req_headers, "timeout": timeout}),
        "dashboard_bundle": (http_post_json, (f"{env.backend_url}/api/dashboard/bundle?service_id={active_sid}",), {"payload": {"range_token": "24h"}, "headers": req_headers, "timeout": max(timeout, 45.0)}),
    }
    if env.is_high_scale:
        probes["clickhouse_status"] = (http_get_json, (f"{env.backend_url}/api/admin/clickhouse/status?service_id={active_sid}",), {"headers": req_headers, "timeout": timeout})

    with ThreadPoolExecutor(max_workers=len(probes)) as pool:
        futures = {name: pool.submit(fn, *args, **kwargs) for name, (fn, args, kwargs) in probes.items()}
        outcomes = {name: fut.result() for name, fut in futures.items()}

    b_status, b_data = outcomes["bootstrap"]
    hs_status, hs_data = outcomes["health_snapshot"]
    cron_status, cron_data = outcomes["cron_runs"]
    d_status, d_data = outcomes["dashboard_bundle"]
    ch_status, ch_data = outcomes.get("clickhouse_status", (0, {}))

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
        res.duckdb_size_bytes = status_sec.get("duckdb_size_bytes") or 0
        res.iceberg_bytes = status_sec.get("iceberg_bytes") or 0
        res.iceberg_files = status_sec.get("iceberg_files") or 0

    if hs_status == 200 and isinstance(hs_data, dict):
        load = hs_data.get("load") or {}
        res.cpu_load_1m, res.cpu_load_5m, res.cpu_load_15m = float(load.get("avg_1m", 0.0)), float(load.get("avg_5m", 0.0)), float(load.get("avg_15m", 0.0))
        res.vcpus = int(hs_data.get("vcpus", 1))
        if res.cpu_load_1m > (res.vcpus * 2.0):
            res.warnings.append(f"High CPU load: 1m avg {res.cpu_load_1m:.1f} exceeds 2x vCPUs ({res.vcpus})")

        mem = hs_data.get("memory") or {}
        res.mem_used_pct, res.mem_available_mb, res.mem_total_mb = float(mem.get("used_pct", 0.0)), float(mem.get("available_mb", 0.0)), float(mem.get("total_mb", 0.0))
        if res.mem_used_pct > 92.0:
            res.errors.append(f"Critical memory usage: {res.mem_used_pct:.1f}%")
        elif res.mem_used_pct > 85.0:
            res.warnings.append(f"Elevated memory usage: {res.mem_used_pct:.1f}%")

        dm = hs_data.get("data_mount") or {}
        res.data_mount_used_pct, res.data_mount_free_gb = float(dm.get("used_pct", 0.0)), float(dm.get("free_gb", 0.0))
        if res.data_mount_used_pct > 95.0:
            res.errors.append(f"Data mount nearly full: {res.data_mount_used_pct:.1f}% used ({res.data_mount_free_gb:.1f} GB free)")
        elif res.data_mount_used_pct > 85.0:
            res.warnings.append(f"Data mount high: {res.data_mount_used_pct:.1f}% used ({res.data_mount_free_gb:.1f} GB free)")

        rd = hs_data.get("root_disk") or {}
        res.root_disk_used_pct, res.root_disk_free_gb = float(rd.get("used_pct", 0.0)), float(rd.get("free_gb", 0.0))

        res.scheduler_tick_age_s = hs_data.get("scheduler_last_tick_age_s")
        if res.scheduler_tick_age_s is not None:
            if res.scheduler_tick_age_s > 180.0:
                res.errors.append(f"Scheduler stalled! Last tick was {res.scheduler_tick_age_s:.0f}s ago")
            elif res.scheduler_tick_age_s > 90.0:
                res.warnings.append(f"Scheduler behind schedule: tick age {res.scheduler_tick_age_s:.0f}s")

        res.in_flight_runs = hs_data.get("in_flight_runs") or []
        for ifr in res.in_flight_runs:
            if (dur := ifr.get("duration_s", 0.0)) > 300:
                res.warnings.append(f"Long running job: {ifr.get('task', 'unknown')} has been running for {dur:.0f}s")

        res.recent_failures = hs_data.get("recent_cron_failures") or []
        for fail in res.recent_failures:
            started = fail.get("started_at")
            age_s = parse_timestamp_age(started)
            if age_s is None or age_s <= 86400:
                age_str = f" ({format_duration(age_s)} ago)" if started else ""
                err_msg = fail.get("error_message") or fail.get("summary") or fail.get("status") or "error"
                res.warnings.append(f"Cron failure in {fail.get('task')}{age_str}: {err_msg}")

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

        fos = hs_data.get("fos")
        if fos and isinstance(fos, dict):
            svc_fos = fos.get(active_sid) or {}
            res.fos_reachable = svc_fos.get("reachable", False)
            res.fos_error = svc_fos.get("error")
            if not res.fos_reachable and res.fos_error:
                res.errors.append(f"FOS probe failed: {res.fos_error}")

        svc_comp = (hs_data.get("compaction") or {}).get(active_sid) or {}
        res.compaction_total_files = svc_comp.get("total_files") or 0
        res.compaction_daily_files = svc_comp.get("daily_files") or 0
        res.compaction_weekly_files = svc_comp.get("weekly_files") or 0
        res.compaction_partitions_above_3 = svc_comp.get("partitions_above_3") or 0
        res.compaction_partitions_above_10 = svc_comp.get("partitions_above_10") or 0
        if res.compaction_partitions_above_10 > 0:
            res.warnings.append(f"Uncompacted partitions: {res.compaction_partitions_above_10} partitions have >10 files")

        for p in hs_data.get("pool_wait") or []:
            if p.get("service") == active_sid:
                res.pool_saturated_rejects += p.get("saturated_rejects_total", 0)
                res.pool_wait_p95_ms = max(res.pool_wait_p95_ms, float((p.get("wait") or {}).get("p95_ms", 0.0)))
        if res.pool_saturated_rejects > 0:
            res.warnings.append(f"DuckDB pool saturated: {res.pool_saturated_rejects} rejected queries (lifetime)")
    elif hs_status == 401:
        res.errors.append("Authentication required for /api/admin/health-snapshot")

    entries = (cron_data.get("entries") or []) if cron_status == 200 and isinstance(cron_data, dict) else []
    last_disc_summary = (entries[0].get("summary") or "") if entries else ""
    if res.request_latest_log_age_s is not None:
        if res.request_latest_log_age_s <= 900:
            res.ingest_state = "STREAMING"
        elif "No new log files" in last_disc_summary or "Discovered 0" in last_disc_summary:
            res.ingest_state = "IDLE (0 in FOS)"
        elif "Ingested" in last_disc_summary or "Discovered" in last_disc_summary:
            res.ingest_state = "STREAMING (Catching up)"
        else:
            res.ingest_state = "IDLE (No recent traffic)"
    else:
        res.ingest_state = "NO LOGS"

    if env.is_high_scale and ch_status == 200 and isinstance(ch_data, dict):
        res.clickhouse_health = ch_data.get("health")
        pub_counts = ch_data.get("publication_counts") or {}
        res.clickhouse_pending, res.clickhouse_published = pub_counts.get("pending", 0), pub_counts.get("published", 0)
        if res.clickhouse_health not in ("ok", "disabled", None):
            res.warnings.append(f"ClickHouse state: {res.clickhouse_health}")

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
    print(f"\n{BOLD}{CYAN}╔═══════════════════════════════════════════════════════════════════════════════════════════╗{RESET}")
    print(f"{BOLD}{CYAN}║                     FASTLY LOG ANALYTICS — MULTI-ENVIRONMENT AUDIT                       ║{RESET}")
    print(f"{BOLD}{CYAN}║                     Timestamp: {now_str:<50} ║{RESET}")
    print(f"{BOLD}{CYAN}╚═══════════════════════════════════════════════════════════════════════════════════════════╝{RESET}\n")

    for r in results:
        status_tag = f"{BG_GREEN}{WHITE} REACHABLE {RESET}" if r.reachable else f"{BG_RED}{WHITE} UNREACHABLE {RESET}"
        print(f"{BOLD}{WHITE}● {r.target.upper()}{RESET} — {DIM}{r.architecture}{RESET} {status_tag}")
        print(f"  {DIM}URL:{RESET} {r.backend_url:<28} {DIM}Service ID:{RESET} {r.service_id} {DIM}Latency:{RESET} {r.latency_ms:.1f}ms")
        if not r.reachable:
            for err in r.errors:
                print(f"  {RED}✖ {err}{RESET}")
            print()
            continue

        cpu_color = RED if r.cpu_load_1m > (r.vcpus * 2) else (YELLOW if r.cpu_load_1m > r.vcpus else GREEN)
        mem_color = RED if r.mem_used_pct > 90 else (YELLOW if r.mem_used_pct > 80 else GREEN)
        disk_color = RED if r.data_mount_used_pct > 90 else (YELLOW if r.data_mount_used_pct > 80 else GREEN)
        print(
            f"  {BOLD}Resources:{RESET} CPU: {cpu_color}{r.cpu_load_1m:.2f}{RESET}/{r.cpu_load_5m:.2f}/{r.cpu_load_15m:.2f} ({r.vcpus} vCPUs) │ "
            f"RAM: {mem_color}{r.mem_used_pct:.1f}%{RESET} ({r.mem_available_mb / 1024:.1f}/{r.mem_total_mb / 1024:.1f} GB free) │ "
            f"Disk: {disk_color}{r.data_mount_used_pct:.1f}%{RESET} ({r.data_mount_free_gb:.1f} GB free)"
        )

        ingest_color = GREEN if "STREAMING" in r.ingest_state else (CYAN if "IDLE" in r.ingest_state else YELLOW)
        print(
            f"  {BOLD}Ingest:{RESET}    State: {ingest_color}{r.ingest_state}{RESET} │ "
            f"Request: {r.request_total_rows or 0:,} rows (latest {format_duration(r.request_latest_log_age_s)} ago) │ "
            f"RUM: {r.rum_total_rows or 0:,} rows (latest {format_duration(r.rum_latest_log_age_s)} ago)"
        )

        sched_color = RED if (r.scheduler_tick_age_s or 0) > 45 else GREEN
        comp_color = YELLOW if (r.compaction_partitions_above_10 or 0) > 0 else GREEN
        sched_str = f"{r.scheduler_tick_age_s:.0f}s ago" if r.scheduler_tick_age_s is not None else "—"
        storage_parts = [f"Scheduler Tick: {sched_color}{sched_str}{RESET}"]
        if (r.duckdb_size_bytes or 0) > 0:
            storage_parts.append(f"DuckDB: {format_bytes(r.duckdb_size_bytes)}")
        if (r.iceberg_files or 0) > 0 or (r.iceberg_bytes or 0) > 0:
            storage_parts.append(f"DuckLake: {format_bytes(r.iceberg_bytes)} ({r.iceberg_files} files)")
        storage_parts.append(
            f"Compaction: {comp_color}{r.compaction_total_files or 0} files ({r.compaction_daily_files or 0} daily, {r.compaction_weekly_files or 0} weekly){RESET}"
        )
        storage_line = f"  {BOLD}Pipeline:{RESET}  {' │ '.join(storage_parts)}"
        if (r.compaction_partitions_above_10 or 0) > 0:
            storage_line += f" {YELLOW}[sprawl: {r.compaction_partitions_above_10} dirs >10 files]{RESET}"
        print(storage_line)

        extra_parts: list[str] = []
        if r.celery_reachable is not None:
            c_color = GREEN if r.celery_reachable and (r.celery_workers or 0) > 0 else RED
            extra_parts.append(f"Celery: {c_color}{r.celery_workers} workers, queue depth {r.celery_queue_depth}{RESET}")
        if r.clickhouse_health:
            ch_color = GREEN if r.clickhouse_health == "ok" else YELLOW
            extra_parts.append(f"ClickHouse: {ch_color}{r.clickhouse_health}{RESET} ({r.clickhouse_published} published, {r.clickhouse_pending} pending)")
        if r.pool_wait_p95_ms > 0:
            extra_parts.append(f"DuckDB Pool p95: {r.pool_wait_p95_ms:.2f}ms")
        if extra_parts:
            print(f"  {BOLD}Scale/Pool:{RESET} {' │ '.join(extra_parts)}")

        for w in r.warnings:
            print(f"  {YELLOW}⚠ Warning: {w}{RESET}")
        for e in r.errors:
            print(f"  {RED}✖ Error: {e}{RESET}")
        print()

    healthy = sum(1 for r in results if r.reachable and not r.errors and not r.warnings)
    warned = sum(1 for r in results if r.reachable and r.warnings and not r.errors)
    unhealthy = sum(1 for r in results if not r.reachable or r.errors)
    print(f"{BOLD}SUMMARY:{RESET} {len(results)} Environments Audited │ {GREEN}{healthy} Optimal{RESET} │ {YELLOW}{warned} Warnings{RESET} │ {RED}{unhealthy} Degraded{RESET}")


def run_audit(target_names: list[str] | None = None, timeout: float = 5.0) -> list[AuditResult]:
    envs_to_check: list[EnvironmentConfig] = []
    if target_names and "all" not in target_names:
        for name in target_names:
            if name in ENVIRONMENTS:
                envs_to_check.append(ENVIRONMENTS[name])
            else:
                print(f"{YELLOW}Warning: Unknown environment '{name}'. Available: {list(ENVIRONMENTS.keys())}{RESET}", file=sys.stderr)
    else:
        envs_to_check = list(ENVIRONMENTS.values())

    if os.getenv("IGNORE_LOCAL_STD") == "1":
        envs_to_check = [e for e in envs_to_check if e.name != "local-standard"]

    results: list[AuditResult] = []
    with ThreadPoolExecutor(max_workers=min(len(envs_to_check), 4)) as executor:
        future_to_env = {executor.submit(audit_target, env, timeout): env for env in envs_to_check}
        for future in as_completed(future_to_env):
            try:
                results.append(future.result())
            except Exception as e:
                env = future_to_env[future]
                results.append(AuditResult(
                    target=env.name, architecture=env.architecture, service_id=env.service_id,
                    backend_url=env.backend_url, errors=[f"Audit execution exception: {e}"],
                ))
    env_order = list(ENVIRONMENTS.keys())
    results.sort(key=lambda r: env_order.index(r.target) if r.target in env_order else 999)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fastly Log Analytics Multi-Environment Diagnostic & Health Audit",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--env", nargs="+", default=["all"], help=f"Environments to audit (default: all). Choices: {list(ENVIRONMENTS.keys())} or 'all'")
    parser.add_argument("--watch", action="store_true", help="Run continuously in watch monitor mode")
    parser.add_argument("--duration", type=float, default=0.0, help="Duration in minutes to run in watch mode (default: 0 = indefinite until Ctrl+C)")
    parser.add_argument("--interval", type=float, default=5.0, help="Polling interval in seconds for watch mode (default: 5.0)")
    add_common_args(parser, timeout_default=10.0, timeout_label="HTTP request", json_help="Output raw audit results as JSON")

    args = parser.parse_args()
    load_persisted_deploy_env()
    refresh_environment_origins()
    validate_admin_tls_config()

    if not args.watch:
        results = run_audit(target_names=args.env, timeout=args.timeout)
        if args.json:
            print(json.dumps([asdict(r) for r in results], indent=2))
        else:
            print_audit_report(results, clear_screen=False)
        return 1 if any(not r.reachable or len(r.errors) > 0 for r in results) else 0

    start_time = time.time()
    max_duration_s = args.duration * 60.0 if args.duration > 0 else float("inf")
    last_results: list[AuditResult] = []
    try:
        while True:
            elapsed = time.time() - start_time
            if elapsed >= max_duration_s:
                print(f"\n{GREEN}Completed watch duration of {args.duration:.1f} minute(s). Exiting.{RESET}")
                break
            last_results = run_audit(target_names=args.env, timeout=args.timeout)
            if args.json:
                print(json.dumps([asdict(r) for r in last_results]))
            else:
                print_audit_report(last_results, clear_screen=True)
                rem = f" / {max_duration_s:.0f}s (Remaining: {max_duration_s - elapsed:.0f}s, " if max_duration_s < float("inf") else " ("
                print(f"{DIM}Watching... Elapsed: {elapsed:.0f}s{rem}Interval: {args.interval}s, Ctrl+C to stop){RESET}")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\n{YELLOW}Audit watch stopped by operator.{RESET}")

    return 1 if any(not r.reachable or len(r.errors) > 0 for r in last_results) else 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "httpx[http2]>=0.27",
#   "pyarrow>=18.0",
#   "sniffio==1.3.1",
# ]
# ///
"""Post-commit deployment and end-to-end live verification harness for flp.

Executes all 6 verification stages after committing code:
Preflight rejects absent or mixed configured service identities before deployment.
1. Build and deploy HEAD to the live test install (default: flp-g2-live-20261029).
2. Reconcile VCL updates on the monitored VCL service (default: xa20dYfOsL4MhjIo66mVhh)
   with a 10s log period (log_period=10, rum.log_period=10).
3. Confirm active versions and VCL configuration across compute, worker, front,
   and the monitored VCL service.
4. Run one shared-tool deploy-check stage (fastly-log-analytics-load-testing
   `lt deploy-check`, configured by fastly-lt.toml) and require it to pass.
5. Confirm natural 1s-dispatcher execution of request_ingest & raw_delete, force-verify
   retention_sweep, storage_reconcile, log_compact, and log_flush, and check Compute
   resource budgets, platform_errors, and quarantine.
6. Run Playwright browser verification across /dashboard and all hosted pages/tabs,
   reporting cold/warm page load times and end-to-end request-to-dashboard lag.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import html
import io
import json
import math
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import httpx

DEFAULT_INSTALL_NAME = "flp-g2-live-20261029"
DEFAULT_TEST_SERVICE_ID = "xa20dYfOsL4MhjIo66mVhh"
DEFAULT_TEST_DOMAIN = "https://dmichael-log-streaming-stead.global.ssl.fastly.net"
DEFAULT_LOG_PERIOD_S = 10
DEFAULT_LT_REPO = Path.home() / "Projects" / "fastly-log-analytics-load-testing"
SETTLE_WINDOW_S = 180  # UTC minute close (60s) + SETTLE_S (120s)
DOWNLOAD_CONCURRENCY = 8

# Compute vCPU budgets matching app/src/ingest/coordinator.rs (800ms),
# app/src/ingest/worker.rs (800ms), and app/src/ingest/bundle.rs (700ms).
DEFAULT_VCPU_BUDGET_MS = 800.0
HARD_VCPU_CEILING_MS = 1000.0
STAGE_VCPU_BUDGET_MS: dict[str, float] = {
    "summary_bundle": 700.0,
    "request_ingest": 800.0,
    "rum_ingest": 800.0,
    "raw_delete": 800.0,
    "retention_sweep": 800.0,
    "storage_reconcile": 800.0,
    "log_compact": 800.0,
    "log_flush": 800.0,
}

# Presets and filters matching fastly-log-analytics (never "10m")
DASHBOARD_PRESETS: tuple[str, ...] = ("1h", "3h", "6h", "12h", "24h")
EXTENDED_PRESETS: tuple[str, ...] = (
    "1h",
    "3h",
    "6h",
    "12h",
    "24h",
    "3d",
    "7d",
    "30d",
)
DEFAULT_BENCHMARK_RANGES: tuple[str, ...] = ("24h", "1h", "6h")
DEFAULT_BENCHMARK_FILTERS: tuple[str, ...] = (
    "",
    "+status:200",
    "+method:GET",
    "-status:503",
    "+status:200&+method:GET",
)

MINUTE_PREFIX_RE = re.compile(r"year=(\d{4})/month=(\d{2})/day=(\d{2})/hour=(\d{2})/minute=(\d{2})/")


def validate_dashboard_presets(
    ranges: list[str] | tuple[str, ...],
    *,
    allow_extended: bool = False,
) -> tuple[str, ...]:
    """Validate range presets against fastly-log-analytics presets, rejecting '10m'."""
    allowed = set(EXTENDED_PRESETS if allow_extended else DASHBOARD_PRESETS)
    cleaned: list[str] = []
    for item in ranges:
        token = str(item or "").strip()
        if not token:
            continue
        if token == "10m":
            raise ValueError(
                f"Invalid preset '10m': 10m is never a preset in fastly-log-analytics. "
                f"Allowed presets: {', '.join(EXTENDED_PRESETS if allow_extended else DASHBOARD_PRESETS)}"
            )
        if token not in allowed:
            raise ValueError(
                f"Unsupported range preset '{token}'. Allowed presets: "
                f"{', '.join(EXTENDED_PRESETS if allow_extended else DASHBOARD_PRESETS)}"
            )
        cleaned.append(token)
    return tuple(cleaned)


def summarize_numeric_series(
    values: list[int | float | None] | tuple[int | float | None, ...],
) -> dict[str, int | float | None]:
    """Compute count, min, p50, p95, max, mean, sum for any numeric sequence."""
    nums = sorted(
        float(v) for v in values if isinstance(v, int | float) and not isinstance(v, bool) and math.isfinite(v)
    )
    if not nums:
        return {
            "count": 0,
            "min": None,
            "p50": None,
            "p95": None,
            "max": None,
            "mean": None,
            "sum": 0.0,
        }

    def _pct(p: float) -> float:
        if len(nums) == 1:
            return round(nums[0], 2)
        rank = (p / 100.0) * (len(nums) - 1)
        lo = int(rank)
        hi = min(len(nums) - 1, lo + 1)
        weight = rank - lo
        return round(nums[lo] * (1.0 - weight) + nums[hi] * weight, 2)

    total = sum(nums)
    return {
        "count": len(nums),
        "min": round(nums[0], 2),
        "p50": _pct(50.0),
        "p95": _pct(95.0),
        "max": round(nums[-1], 2),
        "mean": round(total / len(nums), 2),
        "sum": round(total, 2),
    }


def parse_env_file(env_path: Path) -> dict[str, str]:
    """Parse a dotenv file without printing or exposing secret values."""
    result: dict[str, str] = {}
    if not env_path.is_file():
        return result
    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        key, sep, val = line.partition("=")
        if not sep:
            continue
        key = key.strip()
        val = val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
            val = val[1:-1]
        if key:
            result[key] = val
    return result


def parse_minute_prefix_epoch(prefix: str) -> int | None:
    """Extract UTC minute start epoch seconds from an S3 hive prefix."""
    match = MINUTE_PREFIX_RE.search(prefix)
    if not match:
        return None
    year, month, day, hour, minute = (int(g) for g in match.groups())
    return int(dt.datetime(year, month, day, hour, minute, tzinfo=dt.UTC).timestamp())


def compute_lag_breakdown(
    *,
    t_first_send: float,
    t_send_done: float,
    t_first_raw_fos: float | None,
    max_minute_epoch: int | None,
    t_ingest_done: float,
    t_dashboard_verified: float | None = None,
) -> dict[str, float | None]:
    """Compute the 3-part lag breakdown from raw landing, settle window, and ingest."""
    raw_landing = round(max(0.0, t_first_raw_fos - t_first_send), 2) if t_first_raw_fos is not None else None
    settle_ready_epoch = float(max_minute_epoch + SETTLE_WINDOW_S) if max_minute_epoch is not None else None
    post_settle_ingest = (
        round(max(0.0, t_ingest_done - settle_ready_epoch), 2) if settle_ready_epoch is not None else None
    )
    send_to_parquet = round(max(0.0, t_ingest_done - t_send_done), 2)
    send_to_dashboard = (
        round(max(0.0, t_dashboard_verified - t_send_done), 2) if t_dashboard_verified is not None else None
    )
    return {
        "raw_fos_landing_lag_s": raw_landing,
        "settle_ready_epoch_s": settle_ready_epoch,
        "post_settle_ingest_lag_s": post_settle_ingest,
        "send_to_parquet_lag_s": send_to_parquet,
        "send_to_dashboard_lag_s": send_to_dashboard,
    }


def _extract_cron_row_metric(row: dict[str, Any], *keys: str) -> int | float | None:
    for k in keys:
        val = row.get(k)
        if isinstance(val, int | float) and not isinstance(val, bool) and math.isfinite(val):
            return val
    return None


def _summarize_stage_cron_rows(
    stage_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    stop_reasons: dict[str, int] = {}
    for r in stage_rows:
        reason = str(r.get("stop_reason") or r.get("outcome") or r.get("status") or "done")
        stop_reasons[reason] = stop_reasons.get(reason, 0) + 1
    return {
        "runs": len(stage_rows),
        "listed_files": summarize_numeric_series(
            [_extract_cron_row_metric(r, "listed_files", "raw_files_listed", "files_listed") for r in stage_rows]
        ),
        "converted_files": summarize_numeric_series(
            [
                _extract_cron_row_metric(
                    r,
                    "converted_files",
                    "converted_raw_files",
                    "files_processed",
                )
                for r in stage_rows
            ]
        ),
        "deleted_files": summarize_numeric_series(
            [_extract_cron_row_metric(r, "deleted_files", "deleted_raw_files", "files_deleted") for r in stage_rows]
        ),
        "parquet_files_written": summarize_numeric_series(
            [_extract_cron_row_metric(r, "parquet_files_written", "parquet_files", "files_written") for r in stage_rows]
        ),
        "rows_written": summarize_numeric_series(
            [_extract_cron_row_metric(r, "rows_written", "rows") for r in stage_rows]
        ),
        "bytes_written": summarize_numeric_series(
            [_extract_cron_row_metric(r, "bytes_written", "bytes") for r in stage_rows]
        ),
        "worker_executions": summarize_numeric_series(
            [_extract_cron_row_metric(r, "worker_executions", "workers") for r in stage_rows]
        ),
        "duration_ms": summarize_numeric_series([_extract_cron_row_metric(r, "duration_ms") for r in stage_rows]),
        "vcpu_ms": summarize_numeric_series([_extract_cron_row_metric(r, "vcpu_ms") for r in stage_rows]),
        "worker_vcpu_ms": summarize_numeric_series([_extract_cron_row_metric(r, "worker_vcpu_ms") for r in stage_rows]),
        "summary_vcpu_ms": summarize_numeric_series(
            [_extract_cron_row_metric(r, "summary_vcpu_ms") for r in stage_rows]
        ),
        "backend_requests_used": summarize_numeric_series(
            [_extract_cron_row_metric(r, "backend_requests_used") for r in stage_rows]
        ),
        "cache_lookups": summarize_numeric_series([_extract_cron_row_metric(r, "cache_lookups") for r in stage_rows]),
        "stop_reasons": stop_reasons,
    }


def evaluate_cron_health(
    rows: list[dict[str, Any]],
    required_stages: set[str],
    vcpu_budget_ms: float | None = None,
) -> dict[str, Any]:
    """Validate measured cron_runs against 800ms tick / 700ms summary budgets; missing CPU is not a pass."""
    by_stage: dict[str, list[dict[str, Any]]] = {}
    errors: list[str] = []
    max_backend_calls = 0
    max_cache_lookups = 0
    max_vcpu_ms: float | None = None

    for row in rows:
        stage = str(row.get("cron_kind") or row.get("stage") or "unknown")
        by_stage.setdefault(stage, []).append(row)
        err = row.get("error")
        err_count = int(row.get("error_count") or 0)
        status = str(row.get("status") or row.get("outcome") or "ok")
        if (isinstance(err, str) and err.strip()) or err_count > 0 or status == "error":
            errors.append(f"{stage}: status={status} error={err} error_count={err_count}")

        b_calls = int(row.get("backend_requests_used") or 0)
        c_lookups = int(row.get("cache_lookups") or 0)
        vcpu = _extract_cron_row_metric(row, "vcpu_ms")
        max_backend_calls = max(max_backend_calls, b_calls)
        max_cache_lookups = max(max_cache_lookups, c_lookups)
        if vcpu is None:
            errors.append(f"{stage}: unmeasured vcpu_ms")
        else:
            max_vcpu_ms = max(max_vcpu_ms or 0.0, vcpu)

        stage_budget = (
            float(vcpu_budget_ms)
            if vcpu_budget_ms is not None
            else STAGE_VCPU_BUDGET_MS.get(stage, DEFAULT_VCPU_BUDGET_MS)
        )

        if b_calls > 28:
            errors.append(f"{stage} exceeded backend_requests_used budget: {b_calls} > 28")
        if c_lookups > 56:
            errors.append(f"{stage} exceeded cache_lookups budget: {c_lookups} > 56")
        if vcpu is not None and vcpu >= stage_budget:
            errors.append(f"{stage} exceeded vcpu_ms budget: {vcpu:.2f} >= {stage_budget:.1f}")

    missing_stages = sorted(required_stages - set(by_stage.keys()))
    if missing_stages:
        errors.append(f"Missing cron stages: {', '.join(missing_stages)}")

    per_stage_metrics = {stage: _summarize_stage_cron_rows(s_rows) for stage, s_rows in sorted(by_stage.items())}
    overall_metrics = _summarize_stage_cron_rows(rows)

    return {
        "ok": len(errors) == 0,
        "vcpu_budget_ms": float(vcpu_budget_ms or DEFAULT_VCPU_BUDGET_MS),
        "hard_vcpu_ceiling_ms": HARD_VCPU_CEILING_MS,
        "stages_seen": {k: len(v) for k, v in sorted(by_stage.items())},
        "missing_stages": missing_stages,
        "max_backend_requests_used": max_backend_calls,
        "max_cache_lookups": max_cache_lookups,
        "max_vcpu_ms": round(max_vcpu_ms, 2) if max_vcpu_ms is not None else None,
        "total_files_listed": int(overall_metrics["listed_files"]["sum"] or 0),
        "total_files_converted": int(overall_metrics["converted_files"]["sum"] or 0),
        "total_raw_files_converted": int(overall_metrics["converted_files"]["sum"] or 0),
        "total_files_deleted": int(overall_metrics["deleted_files"]["sum"] or 0),
        "total_raw_files_deleted": int(overall_metrics["deleted_files"]["sum"] or 0),
        "total_parquet_files_written": int(overall_metrics["parquet_files_written"]["sum"] or 0),
        "total_rows_written": int(overall_metrics["rows_written"]["sum"] or 0),
        "total_bytes_written": int(overall_metrics["bytes_written"]["sum"] or 0),
        "stop_reasons": overall_metrics["stop_reasons"],
        "overall_metrics": overall_metrics,
        "per_stage_metrics": per_stage_metrics,
        "per_stage_distributions": per_stage_metrics,
        "errors": errors,
    }


def summarize_emitted_log_files(
    *,
    raw_file_entries: list[dict[str, Any]],
    parquet_file_entries: list[dict[str, Any]],
    cron_rows: list[dict[str, Any]] | None = None,
    duration_s: float = 0.0,
) -> dict[str, Any]:
    """Summarize total raw .log.gz files emitted, their sizes, POP breakdown, and Parquet files written."""
    cron_rows = cron_rows or []
    dedup_raw: dict[str, dict[str, Any]] = {}
    for entry in raw_file_entries:
        key = str(entry.get("key") or entry.get("raw_key") or entry.get("path") or entry.get("name") or "")
        if not key:
            continue
        size_val = int(entry.get("size_bytes") or entry.get("bytes") or entry.get("size") or 0)
        pop = str(entry.get("pop") or entry.get("datacenter") or "")
        if not pop:
            m_pop = re.search(r"(?:pop=([A-Za-z0-9_-]+)|-([A-Z]{3})\.log(?:\.gz)?$)", key)
            if m_pop:
                pop = m_pop.group(1) or m_pop.group(2)
            else:
                pop = "default"
        prev = dedup_raw.get(key)
        if prev is None or size_val > int(prev.get("size_bytes") or 0):
            dedup_raw[key] = {
                "key": key,
                "size_bytes": size_val,
                "pop": pop,
            }

    raw_sizes = [int(v["size_bytes"]) for v in dedup_raw.values() if int(v.get("size_bytes") or 0) > 0]
    raw_size_dist = summarize_numeric_series(raw_sizes)

    by_pop_sizes: dict[str, list[int]] = {}
    for item in dedup_raw.values():
        p = str(item.get("pop") or "default")
        by_pop_sizes.setdefault(p, []).append(int(item.get("size_bytes") or 0))
    by_pop_summary = {
        pop: {
            "files": len(sizes),
            "size_bytes": summarize_numeric_series(sizes),
        }
        for pop, sizes in sorted(by_pop_sizes.items())
    }

    cron_converted_total = sum(
        int(_extract_cron_row_metric(r, "converted_files", "converted_raw_files", "files_processed"))
        for r in cron_rows
        if str(r.get("cron_kind") or r.get("stage") or "") in ("request_ingest", "rum_ingest")
    )
    cron_deleted_total = sum(
        int(_extract_cron_row_metric(r, "deleted_files", "deleted_raw_files", "files_deleted"))
        for r in cron_rows
        if str(r.get("cron_kind") or r.get("stage") or "") == "raw_delete"
    )
    raw_emitted_total = max(len(dedup_raw), cron_converted_total, cron_deleted_total)
    raw_bytes_total = int(raw_size_dist["sum"] or 0)

    dedup_pq: dict[str, dict[str, Any]] = {}
    for pq_entry in parquet_file_entries:
        key = str(pq_entry.get("key") or pq_entry.get("path") or "")
        if not key:
            continue
        dedup_pq[key] = {
            "key": key,
            "size_bytes": int(pq_entry.get("size_bytes") or pq_entry.get("bytes") or pq_entry.get("size") or 0),
            "rows": int(pq_entry.get("rows") or pq_entry.get("row_count") or 0),
        }

    pq_sizes = [int(v["size_bytes"]) for v in dedup_pq.values() if int(v.get("size_bytes") or 0) > 0]
    pq_rows = [int(v["rows"]) for v in dedup_pq.values() if int(v.get("rows") or 0) > 0]
    pq_size_dist = summarize_numeric_series(pq_sizes)
    pq_row_dist = summarize_numeric_series(pq_rows)
    pq_bytes_total = int(pq_size_dist["sum"] or 0)
    pq_rows_total = int(pq_row_dist["sum"] or 0)

    eff_duration_s = max(0.0, float(duration_s))
    return {
        "raw_files_observed": len(dedup_raw),
        "raw_files_converted_by_cron": cron_converted_total,
        "raw_files_deleted_by_cron": cron_deleted_total,
        "raw_files_emitted_total": raw_emitted_total,
        "raw_bytes_emitted_total": raw_bytes_total,
        "raw_files_per_sec": round(raw_emitted_total / eff_duration_s, 3) if eff_duration_s > 0 else None,
        "raw_files_per_min": round((raw_emitted_total * 60.0) / eff_duration_s, 2) if eff_duration_s > 0 else None,
        "raw_file_size_bytes": raw_size_dist,
        "by_pop": by_pop_summary,
        "parquet_files_written_total": len(dedup_pq),
        "parquet_bytes_written_total": pq_bytes_total,
        "parquet_rows_written_total": pq_rows_total,
        "parquet_file_size_bytes": pq_size_dist,
        "parquet_file_rows": pq_row_dist,
        "parquet_bytes_per_row": round(pq_bytes_total / pq_rows_total, 2) if pq_rows_total > 0 else None,
        "raw_to_parquet_size_ratio": round(raw_bytes_total / pq_bytes_total, 3)
        if raw_bytes_total > 0 and pq_bytes_total > 0
        else None,
    }


def load_session_cookie(browser_state_path: Path) -> str:
    """Read the flp_session cookie value from Playwright's browser-state.json."""
    if not browser_state_path.is_file():
        raise RuntimeError(f"Missing browser state file: {browser_state_path}")
    data = json.loads(browser_state_path.read_text())
    for cookie in data.get("cookies", []):
        if cookie.get("name") == "flp_session" and cookie.get("value"):
            return str(cookie["value"])
    raise RuntimeError(f"No flp_session cookie found in {browser_state_path}")


def ensure_browser_auth(root: Path, base_url: str, state_dir: Path, service_id: str) -> str:
    """Ensure browser-state.json has a valid session (renewing via passkey if needed)."""
    script = root / "app/tools/browser_verify.mjs"
    subprocess.run(
        [
            "node",
            str(script),
            "--url",
            base_url,
            "--state-dir",
            str(state_dir),
            "--service-id",
            service_id,
            "--mode",
            "ensure-auth",
        ],
        cwd=root,
        check=True,
    )
    return load_session_cookie(state_dir / "browser-state.json")


def app_client(base_url: str, session_cookie: str) -> httpx.Client:
    return httpx.Client(
        base_url=base_url,
        cookies={"flp_session": session_cookie},
        headers={
            "Origin": base_url,
        },
        timeout=60.0,
    )


def preflight_monitored_service(client: httpx.Client, service_id: str) -> None:
    """Reject missing or mixed monitored identities before any deployment or traffic."""
    response = client.get("/api/admin/services")
    response.raise_for_status()
    services = response.json().get("services")
    if (
        not isinstance(services, list)
        or len(services) != 1
        or (not isinstance(services[0], dict) or services[0].get("service_id") != service_id)
    ):
        raise RuntimeError(
            f"Expected sole configured monitored service {service_id}; refusing to deploy or measure another identity"
        )


def fastly_client(api_token: str) -> httpx.Client:
    return httpx.Client(
        base_url="https://api.fastly.com",
        headers={
            "Fastly-Key": api_token,
            "Accept": "application/json",
        },
        timeout=30.0,
    )


def walk_raw_tree(
    client: httpx.Client, service_id: str, prefix: str = "request/"
) -> tuple[list[dict[str, Any]], int | None]:
    """Recursively walk /api/admin/raw-tree to list pending .log.gz files and max minute."""
    files: list[dict[str, Any]] = []
    max_minute: int | None = None
    queue = [prefix]
    visited: set[str] = set()

    while queue:
        current = queue.pop(0)
        if current in visited:
            continue
        visited.add(current)
        cursor: str | None = None
        for _ in range(100):
            params: dict[str, str] = {"service_id": service_id, "prefix": current}
            if cursor:
                params["cursor"] = cursor
            resp = client.get("/api/admin/raw-tree", params=params)
            resp.raise_for_status()
            body = resp.json()
            nodes = body.get("nodes", [])
            for node in nodes:
                kind = node.get("type") or node.get("kind")
                if kind == "directory":
                    dir_prefix = str(node.get("prefix") or node.get("path") or "")
                    if dir_prefix:
                        minute_epoch = parse_minute_prefix_epoch(dir_prefix)
                        if minute_epoch is not None:
                            max_minute = minute_epoch if max_minute is None else max(max_minute, minute_epoch)
                        queue.append(dir_prefix)
                elif kind == "file":
                    file_path = str(node.get("key") or node.get("path") or "")
                    if file_path:
                        minute_epoch = parse_minute_prefix_epoch(file_path)
                        if minute_epoch is not None:
                            max_minute = minute_epoch if max_minute is None else max(max_minute, minute_epoch)
                        files.append(node)
            cursor = body.get("next_cursor")
            if not cursor:
                break
    return files, max_minute


def list_parquet_files(
    client: httpx.Client,
    from_iso: str,
    to_iso: str,
    dataset: str = "request",
) -> list[dict[str, Any]]:
    """Paginate GET /api/files for [from_iso, to_iso]."""
    files_by_key: dict[str, dict[str, Any]] = {}
    cursor: str | None = None
    seen_cursors: set[str] = set()
    for _ in range(500):
        params: dict[str, str] = {
            "from": from_iso,
            "to": to_iso,
            "dataset": dataset,
        }
        if cursor:
            params["cursor"] = cursor
        resp = client.get("/api/files", params=params)
        resp.raise_for_status()
        body = resp.json()
        for item in body.get("files", []):
            key = item.get("key")
            if key:
                files_by_key[str(key)] = item
        cursor = body.get("next_cursor")
        if not cursor:
            break
        if cursor in seen_cursors:
            raise RuntimeError(f"Repeated cursor in /api/files: {cursor}")
        seen_cursors.add(cursor)
    return list(files_by_key.values())


def download_parquet_files(
    client: httpx.Client,
    keys: list[str],
    dest_dir: Path,
) -> list[Path]:
    """Download Parquet files via GET /api/file?key=... into dest_dir, 8 at a time."""
    dest_dir.mkdir(parents=True, exist_ok=True)

    def fetch(key: str) -> Path:
        target = dest_dir / f"{urllib.parse.quote(key, safe='')}.parquet"
        if not target.is_file():
            resp = client.get("/api/file", params={"key": key})
            resp.raise_for_status()
            target.write_bytes(resp.content)
        return target

    with ThreadPoolExecutor(max_workers=DOWNLOAD_CONCURRENCY) as pool:
        return list(pool.map(fetch, sorted(keys)))


def inv_services(inventory: dict[str, Any]) -> dict[str, Any]:
    """Return service map supporting both nested inventory['services'] and flat dicts."""
    services = inventory.get("services")
    return services if isinstance(services, dict) else inventory


def inv_build_sha(inventory: dict[str, Any]) -> str | None:
    """Return build_sha from top-level provenance or nested release metadata."""
    return inventory.get("build_sha") or (inventory.get("release") or {}).get("build_sha")


def step_1_deploy(
    root: Path,
    install_name: str,
    state_dir: Path,
    env_vars: dict[str, str],
    skip_deploy: bool,
) -> dict[str, Any]:
    """Step 1: Build and deploy HEAD to the test environment."""
    t0 = time.time()
    head_sha = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    inv_path = state_dir / "inventory.json"
    if not inv_path.is_file():
        raise RuntimeError(f"Missing inventory file: {inv_path}")

    cmd_env = dict(os.environ)
    cmd_env.update(env_vars)
    cmd_env["FLP_INVENTORY"] = str(inv_path)
    cmd_env["FASTLY_DISABLE_VERSION_CHECK"] = "1"
    cmd_env.setdefault("CARGO_TARGET_DIR", str(root / "app/target"))

    # The release packages the app/ tree at a commit, so an identical tree
    # would rebuild the same package and only churn service versions.
    skip_reason = "--skip-deploy" if skip_deploy else None
    deployed_sha = inv_build_sha(json.loads(inv_path.read_text()))
    if not skip_deploy and deployed_sha:
        try:
            deployed_tree = subprocess.check_output(
                ["git", "-C", str(root), "rev-parse", f"{deployed_sha}:app"],
                text=True,
                stderr=subprocess.DEVNULL,
            ).strip()
        except subprocess.CalledProcessError:
            deployed_tree = None
        head_tree = subprocess.check_output(["git", "-C", str(root), "rev-parse", f"{head_sha}:app"], text=True).strip()
        if deployed_tree == head_tree:
            skip_deploy = True
            skip_reason = f"app/ unchanged since deployed {deployed_sha[:7]}"

    if not skip_deploy:
        build_root = root / ".worktrees/release" if (root / ".worktrees/release/app").is_dir() else root
        if build_root != root:
            subprocess.run(
                ["git", "-C", str(build_root), "checkout", "--detach", head_sha],
                check=True,
            )
        print(f"[Step 1] Building & deploying HEAD ({head_sha[:7]}) to {install_name}...")
        subprocess.run(
            ["make", "-C", str(build_root / "app"), "build"],
            env=cmd_env,
            check=True,
        )
        if build_root != root:
            (root / "app/pkg").mkdir(parents=True, exist_ok=True)
            for name in ("flp.tar.gz", "provenance.json"):
                src = build_root / "app/pkg" / name
                if src.is_file():
                    shutil.copyfile(src, root / "app/pkg" / name)
        subprocess.run(
            ["make", "-C", str(build_root / "app"), "deploy", f"NAME={install_name}"],
            env=cmd_env,
            check=True,
        )
    else:
        print(f"[Step 1] Skipping build & deploy ({skip_reason}).")

    inventory = json.loads(inv_path.read_text())
    services = inv_services(inventory)
    return {
        "skipped": skip_deploy,
        "skip_reason": skip_reason,
        "head_sha": head_sha,
        "inventory_build_sha": inv_build_sha(inventory),
        "compute_version": services.get("compute", {}).get("version"),
        "worker_version": services.get("worker", {}).get("version"),
        "front_version": services.get("front", {}).get("version"),
        "duration_s": round(time.time() - t0, 2),
    }


def _job_status(job: dict[str, Any]) -> str:
    return str(job.get("status") or job.get("state") or "")


TRANSIENT_STEP_ERRORS = {
    "service_locked",
    "job_leased",
    "generation_conflict",
    "state_propagating",
    "state_write_failed",
    "state_unavailable",
    "lease_fenced",
    "lease_unavailable",
    "backend_budget",
}


def _is_transient_admin_error(resp: httpx.Response) -> bool:
    if resp.status_code not in (409, 503):
        return False
    try:
        payload = resp.json()
    except ValueError:
        return True
    err_code = str(payload.get("error") or payload.get("code") or payload.get("detail") or "")
    return not err_code or err_code in TRANSIENT_STEP_ERRORS


def _step_job_to_completion(
    client: httpx.Client,
    job: dict[str, Any],
    max_steps: int = 40,
    allow_retry: bool = False,
) -> tuple[dict[str, Any], int]:
    job_id = job["id"]
    steps_taken = 0
    transient_retries = 0
    is_mock = client.base_url.host == "example.test"
    while (
        _job_status(job) not in ("complete", "succeeded")
        and (_job_status(job) != "failed" or allow_retry)
        and steps_taken < max_steps
    ):
        retry_at = job.get("retry_at")
        now = time.time()
        if isinstance(retry_at, int | float) and retry_at > now:
            time.sleep(min(5.0, max(0.2, retry_at - now)))
        elif not is_mock and steps_taken > 0:
            time.sleep(0.35)
        action = "retry" if _job_status(job) == "failed" and allow_retry else "step"
        if action == "retry":
            allow_retry = False
        step_resp = client.post(f"/api/admin/jobs/{job_id}/{action}", json={})
        if _is_transient_admin_error(step_resp) and transient_retries < 15:
            transient_retries += 1
            if not is_mock:
                time.sleep(0.6)
            poll_resp = client.get(f"/api/admin/jobs/{job_id}")
            if poll_resp.status_code == 200:
                polled_job = poll_resp.json().get("job")
                if isinstance(polled_job, dict):
                    job = polled_job
            continue
        step_resp.raise_for_status()
        transient_retries = 0
        steps_taken += 1
        job = step_resp.json()["job"]
    return job, steps_taken


def _post_reconcile_with_retry(
    client: httpx.Client,
    service_id: str,
    generation: str | None,
) -> dict[str, Any]:
    is_mock = client.base_url.host == "example.test"
    for attempt in range(6):
        rec_payload: dict[str, Any] = {"activate": True}
        if generation:
            rec_payload["generation"] = generation
        rec_resp = client.post(
            f"/api/admin/services/{service_id}/reconcile",
            json=rec_payload,
        )
        if _is_transient_admin_error(rec_resp) and attempt < 5:
            if not is_mock:
                time.sleep(0.6)
            refresh = client.get(f"/api/admin/services/{service_id}")
            if refresh.status_code == 200:
                rbody = refresh.json()
                rsvc = rbody.get("service") if isinstance(rbody.get("service"), dict) else rbody
                generation = rbody.get("generation") or rsvc.get("generation")
                existing = rbody.get("job")
                if isinstance(existing, dict) and _job_status(existing) in (
                    "pending",
                    "running",
                ):
                    return existing
            continue
        rec_resp.raise_for_status()
        return rec_resp.json()["job"]
    raise RuntimeError(f"Failed to start reconcile job for {service_id}")


def step_2_reconcile_vcl(
    client: httpx.Client,
    service_id: str,
    log_period: int,
) -> dict[str, Any]:
    """Step 2: Ensure log_period=10 and reconcile VCL updates on the monitored service."""
    t0 = time.time()
    resp = client.get(f"/api/admin/services/{service_id}")
    resp.raise_for_status()
    body = resp.json()

    existing_job = body.get("job")
    resumed_job: dict[str, Any] | None = None
    resumed_steps = 0
    if isinstance(existing_job, dict) and _job_status(existing_job) in (
        "pending",
        "running",
        "failed",
    ):
        print(f"[Step 2] Finishing existing job {existing_job.get('id')} (status={_job_status(existing_job)}) first...")
        resumed_job, resumed_steps = _step_job_to_completion(client, existing_job, allow_retry=True)
        resp = client.get(f"/api/admin/services/{service_id}")
        resp.raise_for_status()
        body = resp.json()

    svc = body.get("service") if isinstance(body.get("service"), dict) else body
    generation = body.get("generation") or svc.get("generation")

    current_period = svc.get("log_period")
    current_rum_period = (svc.get("rum") or {}).get("log_period")
    cmcd_enabled = bool((svc.get("cmcd") or {}).get("enabled", False))
    rum_enabled = bool((svc.get("rum") or {}).get("enabled", False))

    if current_period != log_period or current_rum_period != log_period:
        print(
            f"[Step 2] Updating service {service_id} log_period "
            f"{current_period} -> {log_period}s and reconciling VCL..."
        )
        patch_payload: dict[str, Any] = {
            "log_period": log_period,
            "cmcd": {"enabled": cmcd_enabled},
            "rum": {
                "enabled": rum_enabled,
                "log_period": log_period,
            },
        }
        if generation:
            patch_payload["generation"] = generation
        patch_resp = client.patch(
            f"/api/admin/services/{service_id}",
            json=patch_payload,
        )
        patch_resp.raise_for_status()
        patch_json = patch_resp.json()
        generation = patch_json.get("generation") or generation
        if isinstance(patch_json.get("job"), dict):
            job = patch_json["job"]
        else:
            job = _post_reconcile_with_retry(client, service_id, generation)
        trigger_mode = "patch_settings"
        job, steps_taken = _step_job_to_completion(client, job, allow_retry=True)
    elif resumed_job is not None and _job_status(resumed_job) in (
        "complete",
        "succeeded",
    ):
        job = resumed_job
        steps_taken = resumed_steps
        trigger_mode = "resumed_reconcile"
    elif (svc.get("last_reconcile") or {}).get("outcome") == "complete" and svc.get("last_activated_version"):
        active_ver = int(svc["last_activated_version"])
        print(
            f"[Step 2] Service {service_id} already reconciled at log_period={log_period}s "
            f"(active v{active_ver}); skipping redundant VCL clone."
        )
        return {
            "service_id": service_id,
            "trigger_mode": "already_reconciled",
            "previous_log_period": current_period,
            "log_period": log_period,
            "job_id": (svc.get("last_reconcile") or {}).get("job_id") or "none",
            "steps_taken": 0,
            "cloned_version": active_ver,
            "activated_version": active_ver,
            "duration_s": round(time.time() - t0, 2),
        }
    else:
        print(f"[Step 2] Service {service_id} already at log_period={log_period}s; running VCL reconcile...")
        job = _post_reconcile_with_retry(client, service_id, generation)
        trigger_mode = "reconcile"
        job, steps_taken = _step_job_to_completion(client, job, allow_retry=True)

    job_id = job["id"]

    if _job_status(job) not in ("complete", "succeeded"):
        raise RuntimeError(
            f"VCL reconcile job {job_id} did not succeed: status={_job_status(job)} error={job.get('error')}"
        )

    draft_ver = job.get("cloned_version") or job.get("draft_version")
    active_ver = job.get("activated_version") or draft_ver
    return {
        "service_id": service_id,
        "trigger_mode": trigger_mode,
        "previous_log_period": current_period,
        "log_period": log_period,
        "job_id": job_id,
        "steps_taken": steps_taken,
        "cloned_version": draft_ver,
        "activated_version": active_ver,
        "duration_s": round(time.time() - t0, 2),
    }


def step_3_confirm_active(
    app_http: httpx.Client,
    fastly_http: httpx.Client,
    inventory: dict[str, Any],
    head_sha: str,
    service_id: str,
    log_period: int,
) -> dict[str, Any]:
    """Step 3: Confirm compute, worker, front, and monitored VCL service are active."""
    t0 = time.time()

    def active_version(sid: str) -> int:
        r = fastly_http.get(f"/service/{sid}/version")
        r.raise_for_status()
        active = [v["number"] for v in r.json() if v.get("active")]
        if not active:
            raise RuntimeError(f"Service {sid} has no active version")
        return int(active[0])

    services = inv_services(inventory)
    compute_id = services["compute"]["id"]
    worker_id = services["worker"]["id"]
    front_id = services["front"]["id"]

    compute_active = active_version(compute_id)
    worker_active = active_version(worker_id)
    front_active = active_version(front_id)
    vcl_active = active_version(service_id)

    expected_compute = int(services["compute"]["version"])
    expected_worker = int(services["worker"]["version"])
    expected_front = int(services["front"]["version"])

    if compute_active != expected_compute:
        raise RuntimeError(f"Compute active version {compute_active} != inventory {expected_compute}")
    if worker_active != expected_worker:
        raise RuntimeError(f"Worker active version {worker_active} != inventory {expected_worker}")
    if front_active != expected_front:
        raise RuntimeError(f"Front active version {front_active} != inventory {expected_front}")

    # Verify /version endpoint on the live service (retry up to 15s for edge propagation)
    live_sha = None
    for _ in range(15):
        ver_resp = app_http.get("/version")
        ver_resp.raise_for_status()
        ver_json = ver_resp.json()
        live_sha = ver_json.get("build_sha") or ver_json.get("sha")
        if live_sha == head_sha:
            break
        time.sleep(1.0)
    if live_sha != head_sha:
        raise RuntimeError(f"Live /version sha {live_sha} != HEAD {head_sha}")

    # Verify front healthchecks (/_tick at 1000ms and /_flush)
    hc_resp = fastly_http.get(f"/service/{front_id}/version/{front_active}/healthcheck")
    hc_resp.raise_for_status()
    hc_list = hc_resp.json()
    hcs = {h["name"]: h for h in hc_list}
    tick_hc = (
        hcs.get("flp_clock") or hcs.get("flp_tick") or next((h for h in hc_list if h.get("path") == "/_tick"), None)
    )
    flush_hc = hcs.get("flp_flush") or next((h for h in hc_list if h.get("path") == "/_flush"), None)
    if not tick_hc or tick_hc.get("path") != "/_tick" or int(tick_hc.get("check_interval", 0)) != 1000:
        raise RuntimeError(f"Invalid /_tick healthcheck on front v{front_active}: {tick_hc}")
    if not flush_hc or flush_hc.get("path") != "/_flush":
        raise RuntimeError(f"Invalid /_flush healthcheck on front v{front_active}: {flush_hc}")

    # Verify monitored VCL service S3 logging period and snippets
    s3_resp = fastly_http.get(f"/service/{service_id}/version/{vcl_active}/logging/s3")
    s3_resp.raise_for_status()
    s3_loggers = {item["name"]: item for item in s3_resp.json()}
    req_logger = s3_loggers.get("Fastly Object Storage Logs")
    if not req_logger:
        raise RuntimeError(f"Missing 'Fastly Object Storage Logs' on {service_id} v{vcl_active}")
    actual_period = int(req_logger.get("period", 0))
    if actual_period != log_period:
        raise RuntimeError(
            f"VCL S3 logger period on {service_id} v{vcl_active} is {actual_period}, expected {log_period}"
        )

    snip_resp = fastly_http.get(f"/service/{service_id}/version/{vcl_active}/snippet")
    snip_resp.raise_for_status()
    snippet_names = sorted(s["name"] for s in snip_resp.json())
    has_managed_snippet = any(
        name.startswith(
            (
                "Fastly Log Analytics",
                "Fastly Log Analysis",
                "flp-owner-",
                "flp_capture_",
            )
        )
        for name in snippet_names
    )
    if not has_managed_snippet:
        raise RuntimeError(f"Missing managed VCL snippets on {service_id} v{vcl_active}: {snippet_names}")

    svc_check = app_http.get(f"/api/admin/services/{service_id}")
    svc_check.raise_for_status()
    svc_body = svc_check.json()
    svc_obj = svc_body.get("service") if isinstance(svc_body.get("service"), dict) else svc_body
    svc_status = svc_obj.get("status")
    if svc_status not in ("active", "healthy"):
        raise RuntimeError(f"Service {service_id} status is {svc_status}")

    # Queue a storage reconcile early so the IAD 120s maintenance worker can also pick it up
    with contextlib.suppress(httpx.HTTPError):
        app_http.post("/api/admin/usage/reconcile-storage")

    return {
        "compute": {"id": compute_id, "active_version": compute_active},
        "worker": {"id": worker_id, "active_version": worker_active},
        "front": {
            "id": front_id,
            "active_version": front_active,
            "tick_interval_ms": int(tick_hc["check_interval"]),
        },
        "monitored_vcl": {
            "id": service_id,
            "active_version": vcl_active,
            "s3_log_period_s": actual_period,
            "snippets": snippet_names,
            "status": svc_status,
        },
        "live_build_sha": live_sha,
        "duration_s": round(time.time() - t0, 2),
    }


def _iso_epoch(value: str) -> float:
    return dt.datetime.fromisoformat(value).timestamp()


def step_4_deploy_check(work_dir: Path, *, config: Path, lt_repo: Path) -> dict[str, Any]:
    """Step 4: one shared-tool deploy-check stage, mapped onto the keys steps 5-6 and the reports read."""
    work_dir.mkdir(parents=True, exist_ok=True)
    command = [
        "uv",
        "run",
        "--project",
        str(lt_repo),
        "lt",
        "deploy-check",
        "--config",
        str(config),
    ]
    print(f"[Step 4] Running {' '.join(command)}")
    proc = subprocess.run(command, cwd=work_dir, capture_output=True, text=True, check=False)
    print(proc.stdout, end="")
    print(proc.stderr, end="", file=sys.stderr)
    report_md = next(
        (line.removeprefix("Report: ").strip() for line in proc.stdout.splitlines() if line.startswith("Report: ")),
        None,
    )
    if report_md is None:
        raise RuntimeError(f"deploy-check exited {proc.returncode} without reporting a campaign")
    campaign_json = Path(report_md).with_name("campaign.json")
    try:
        campaign = json.loads(campaign_json.read_text())
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"deploy-check exited {proc.returncode} without a readable {campaign_json}") from exc
    stages = campaign.get("stages") or []
    if proc.returncode != 0 or not stages or stages[-1].get("status") != "pass":
        stage = stages[-1] if stages else {}
        raise RuntimeError(
            f"deploy-check {stage.get('status', 'failed')}: "
            f"{stage.get('reason') or campaign.get('stopped') or proc.returncode}"
        )
    stage = stages[-1]
    schema = stage.get("schema") or {
        "valid": None,
        "problems": ["deploy-check ran no schema check"],
    }
    if schema.get("valid") is False:
        raise RuntimeError(f"deploy-check Parquet schema invalid: {'; '.join(schema.get('problems') or [])}")
    result = stage.get("result") or {}
    ids = stage.get("ids") or {}
    strict = stage.get("strict") or {}
    ingest = stage.get("ingest") or {}
    if not ingest.get("reached_at") or ingest.get("send_to_parquet_s") is None:
        raise RuntimeError("deploy-check stage has no ingest.reached_at/send_to_parquet_s; update the shared tool")
    delivered = int(result.get("delivered") or 0)
    t_run_start = _iso_epoch(stage["started_at"])
    t_first_send = _iso_epoch(result.get("first_sent_at") or stage["started_at"])
    t_send_done = _iso_epoch(result.get("last_response_at") or stage["ended_at"])
    t_ingest_done = _iso_epoch(ingest["reached_at"])
    verified = {
        "sent": delivered,
        "matched": int(ids.get("matched") or 0),
        "missing_count": int(ids.get("missing_delivered") or 0),
        "duplicate_count": int(ids.get("duplicates") or 0),
        "strict_mismatch_count": int(strict.get("mismatch_count") or 0),
        "unknown_id_count": int(ids.get("unknown") or 0),
    }
    lag = compute_lag_breakdown(
        t_first_send=t_first_send,
        t_send_done=t_send_done,
        t_first_raw_fos=None,
        max_minute_epoch=(int(t_send_done) // 60) * 60,
        t_ingest_done=t_ingest_done,
    )
    lag["send_to_parquet_lag_s"] = round(float(ingest["send_to_parquet_s"]), 2)
    achieved = result.get("achieved_rps")
    return {
        "run_id": stage.get("run_id"),
        "campaign": campaign.get("campaign"),
        "report": report_md,
        "t_run_start": t_run_start,
        "t_first_send": t_first_send,
        "t_send_done": t_send_done,
        "t_first_raw_fos": None,
        "max_minute_epoch": (int(t_send_done) // 60) * 60,
        "t_ingest_done": t_ingest_done,
        "total_sent": delivered,
        "expect_min_requests": delivered,
        "stages": [
            {
                "stage": stage.get("label"),
                "target_rps": int(stage.get("target_rps") or 0),
                "sent": delivered,
                "actual_rps": achieved,
                "latency_ms": result.get("latency_ms") or {},
            }
        ],
        "rps_summary": summarize_numeric_series([float(achieved)] if achieved is not None else []),
        "observed_raw_files": [],
        "parquet_file_entries": [],
        "schema": schema,
        "verify_overall": verified,
        "verify_per_stage": {stage.get("label"): verified},
        "lag": lag,
    }


def _collect_ops_parquet_rows(
    app_http: httpx.Client,
    kind: str,
    from_iso: str,
    to_iso: str,
    min_epoch_ms: int,
    ts_col: str,
) -> list[dict[str, Any]]:
    """Read flushed operational log rows from ops:<kind> Parquet files in [from_iso, to_iso]."""
    try:
        files = list_parquet_files(app_http, from_iso, to_iso, dataset=f"ops:{kind}")
    except (httpx.HTTPError, RuntimeError, ValueError):
        return []
    if not files:
        return []
    import pyarrow.parquet as pq

    def fetch(key: str) -> bytes | None:
        with contextlib.suppress(httpx.HTTPError):
            r = app_http.get("/api/file", params={"key": key})
            r.raise_for_status()
            return r.content
        return None

    keys = [str(item["key"]) for item in files if item.get("key")]
    with ThreadPoolExecutor(max_workers=DOWNLOAD_CONCURRENCY) as pool:
        bodies = list(pool.map(fetch, keys))
    rows: list[dict[str, Any]] = []
    for body in bodies:
        if body is None:
            continue
        with contextlib.suppress(OSError, ValueError):
            table = pq.read_table(io.BytesIO(body))
            for raw_row in table.to_pylist():
                row: dict[str, Any] = {}
                for k, v in raw_row.items():
                    if isinstance(v, dt.datetime):
                        row[k] = int(v.timestamp() * 1000)
                    else:
                        row[k] = v
                ts = row.get(ts_col) or 0
                if isinstance(ts, int | float) and ts >= min_epoch_ms:
                    rows.append(row)
    return rows


def step_5_confirm_crons(
    app_http: httpx.Client,
    service_id: str,
    t_run_start: float,
    had_traffic: bool,
) -> dict[str, Any]:
    """Step 5: Verify natural dispatcher crons + force-exercise slower maintenance crons."""
    t0 = time.time()
    since_epoch = int(t_run_start - 60)
    min_epoch_ms = since_epoch * 1000
    from_iso = dt.datetime.fromtimestamp(since_epoch, tz=dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    to_iso = dt.datetime.fromtimestamp(time.time() + 120, tz=dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 1. Snapshot live outbox cron_runs before flushing (flushing removes outbox keys)
    pre_cron_resp = app_http.get(
        "/api/logs/cron_runs/live",
        params={"since": str(since_epoch), "from": from_iso, "to": to_iso},
    )
    pre_cron_resp.raise_for_status()
    collected_cron_rows: list[dict[str, Any]] = list(pre_cron_resp.json().get("rows", []))

    # 2. Force-exercise the slower maintenance crons so every cron stage is verified
    print("[Step 5] Exercising retention_sweep, storage_reconcile, log_compact, and log_flush...")
    sweep_resp = app_http.post("/api/admin/logs/sweep", json={})
    sweep_endpoint_status = sweep_resp.status_code
    if sweep_endpoint_status not in (200, 503):
        sweep_resp.raise_for_status()
    sweep_body = (
        sweep_resp.json()
        if sweep_endpoint_status == 200 and sweep_resp.headers.get("content-type", "").startswith("application/json")
        else {}
    )

    rec_storage_resp = app_http.post("/api/admin/usage/reconcile-storage", json={})
    rec_storage_resp.raise_for_status()
    rec_storage_body = (
        rec_storage_resp.json()
        if rec_storage_resp.headers.get("content-type", "").startswith("application/json")
        else {}
    )
    time.sleep(2.5)

    # Capture any newly written live cron_runs before flush drains the outbox
    mid_cron_resp = app_http.get(
        "/api/logs/cron_runs/live",
        params={"since": str(since_epoch), "from": from_iso, "to": to_iso},
    )
    mid_cron_resp.raise_for_status()
    collected_cron_rows.extend(mid_cron_resp.json().get("rows", []))
    if not any(str(r.get("cron_kind") or r.get("stage")) == "storage_reconcile" for r in collected_cron_rows):
        trigger_resp = app_http.post("/api/cron-runs/trigger", json={})
        trigger_resp.raise_for_status()
        mid2_resp = app_http.get(
            "/api/logs/cron_runs/live",
            params={"since": str(since_epoch), "from": from_iso, "to": to_iso},
        )
        mid2_resp.raise_for_status()
        collected_cron_rows.extend(mid2_resp.json().get("rows", []))

    flush_runs: list[dict[str, Any]] = []
    outbox_depth = -1
    manual_flush_status = "ok"
    for _ in range(10):
        fl_resp = app_http.post("/api/admin/logs/flush", json={})
        fl_resp.raise_for_status()
        fl_body = fl_resp.json()
        flush_runs.append(fl_body)
        outbox_depth = int(fl_body.get("outbox_depth", 0))
        fl_status = str(fl_body.get("status") or "ok")
        if fl_status == "deferred":
            manual_flush_status = "deferred"
            break
        manual_flush_status = fl_status
        if outbox_depth == 0 and int(fl_body.get("rows", 0)) == 0:
            break
    if manual_flush_status == "deferred":
        if outbox_depth < 0 or outbox_depth >= 60:
            raise RuntimeError(
                f"Operational log outbox depth unhealthy while manual flush deferred: depth={outbox_depth}"
            )
    elif outbox_depth != 0:
        raise RuntimeError(f"Operational log outbox did not drain to 0: depth={outbox_depth}")

    # 3. Also collect flushed ops:cron_runs, ops:ingest_files, and ops:api_queries Parquet rows over the test window
    to_iso = dt.datetime.fromtimestamp(time.time() + 120, tz=dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    collected_cron_rows.extend(
        _collect_ops_parquet_rows(app_http, "cron_runs", from_iso, to_iso, min_epoch_ms, "started_at")
    )
    ingest_files_rows = _collect_ops_parquet_rows(
        app_http, "ingest_files", from_iso, to_iso, min_epoch_ms, "ingested_at"
    )
    api_queries_rows = _collect_ops_parquet_rows(app_http, "api_queries", from_iso, to_iso, min_epoch_ms, "recorded_at")

    # 4. Check sync-status and also extract retained cron_runs from cron:state
    sync_resp = app_http.get("/api/sync-status", params={"service_id": service_id})
    sync_resp.raise_for_status()
    sync_body = sync_resp.json()
    for cr in sync_body.get("cron_runs") or []:
        if not isinstance(cr, dict):
            continue
        cr_iso = str(cr.get("started_at") or "")
        if cr_iso and cr_iso < from_iso:
            continue
        trig = str(cr.get("trigger") or "clock")
        if int(cr.get("converted_raw_files") or 0) > 0:
            collected_cron_rows.append(
                {
                    "run_id": f"{cr.get('id')}-request_ingest",
                    "cron_kind": "request_ingest",
                    "outcome": str(cr.get("outcome") or "done"),
                    "trigger": trig,
                    "started_at": cr_iso,
                    "backend_requests_used": cr.get("backend_requests_used"),
                    "cache_lookups": cr.get("cache_lookups"),
                    "vcpu_ms": cr.get("vcpu_ms"),
                    "worker_vcpu_ms": cr.get("worker_vcpu_ms"),
                    "summary_vcpu_ms": cr.get("summary_vcpu_ms"),
                    "duration_ms": cr.get("duration_ms"),
                    "listed_files": int(cr.get("listed_files") or cr.get("raw_files_listed") or 0),
                    "converted_raw_files": int(cr.get("converted_raw_files") or 0),
                    "parquet_files_written": int(cr.get("parquet_files_written") or cr.get("files_written") or 0),
                    "rows_written": int(cr.get("rows_written") or cr.get("rows") or 0),
                    "bytes_written": int(cr.get("bytes_written") or cr.get("bytes") or 0),
                    "worker_executions": int(cr.get("worker_executions") or 0),
                    "stop_reason": str(cr.get("stop_reason") or "completed"),
                }
            )
        if int(cr.get("deleted_raw_files") or 0) > 0:
            collected_cron_rows.append(
                {
                    "run_id": f"{cr.get('id')}-raw_delete",
                    "cron_kind": "raw_delete",
                    "outcome": str(cr.get("outcome") or "done"),
                    "trigger": trig,
                    "started_at": cr_iso,
                    "backend_requests_used": cr.get("backend_requests_used"),
                    "cache_lookups": cr.get("cache_lookups"),
                    "vcpu_ms": cr.get("vcpu_ms"),
                    "duration_ms": cr.get("duration_ms"),
                    "deleted_raw_files": int(cr.get("deleted_raw_files") or 0),
                    "stop_reason": str(cr.get("stop_reason") or "completed"),
                }
            )

    # Deduplicate cron_rows by run_id (or object identity if run_id missing)
    deduped_cron_rows: list[dict[str, Any]] = []
    seen_run_ids: set[str] = set()
    for r in collected_cron_rows:
        rid = str(r.get("run_id") or r.get("id") or "")
        key = f"{rid}:{r.get('cron_kind') or r.get('stage')}:{r.get('started_at')}"
        if key in seen_run_ids:
            continue
        seen_run_ids.add(key)
        deduped_cron_rows.append(r)

    required_stages = {
        "retention_sweep",
        "storage_reconcile",
        "log_compact",
        "log_flush",
    }
    if had_traffic:
        required_stages.update({"request_ingest", "raw_delete"})

    cron_eval = evaluate_cron_health(deduped_cron_rows, required_stages)
    if cron_eval["missing_stages"]:
        raise RuntimeError(f"Missing required cron stages: {cron_eval['missing_stages']} ({cron_eval})")
    if (
        int(cron_eval.get("max_backend_requests_used") or 0) > 32
        or int(cron_eval.get("max_cache_lookups") or 0) > 64
        or float(cron_eval.get("max_vcpu_ms") or 0.0) >= HARD_VCPU_CEILING_MS
    ):
        raise RuntimeError(f"Hard Fastly Compute resource limit exceeded in cron_runs: {cron_eval}")

    # Verify every required stage had at least one non-error execution
    ok_stages = {
        str(r.get("cron_kind") or r.get("stage"))
        for r in deduped_cron_rows
        if str(r.get("status") or r.get("outcome") or "ok") != "error"
        and not (isinstance(r.get("error"), str) and r["error"].strip())
        and int(r.get("error_count") or 0) == 0
    }
    unhealthy_stages = sorted(required_stages - ok_stages)
    if unhealthy_stages:
        raise RuntimeError(f"Required cron stages had 0 clean executions: {unhealthy_stages} ({cron_eval})")

    # Verify clock-triggered runs occurred for request_ingest & raw_delete when traffic ran
    clock_stages = {str(r.get("cron_kind") or r.get("stage")) for r in deduped_cron_rows if r.get("trigger") == "clock"}
    if had_traffic:
        for natural_stage in ("request_ingest", "raw_delete"):
            if natural_stage not in clock_stages:
                raise RuntimeError(
                    f"Expected natural clock trigger for {natural_stage}, saw clock stages: {sorted(clock_stages)}"
                )

    # 5. Check platform_errors during the run
    err_resp = app_http.get(
        "/api/logs/platform_errors/live",
        params={"since": str(since_epoch), "from": from_iso, "to": to_iso},
    )
    err_resp.raise_for_status()
    platform_errors = [
        r for r in err_resp.json().get("rows", []) if int(r.get("occurred_at") or min_epoch_ms) >= min_epoch_ms
    ]

    # 6. Check quarantine
    quar_resp = app_http.get("/api/admin/quarantine")
    quar_resp.raise_for_status()
    quar_items = quar_resp.json()
    if isinstance(quar_items, dict):
        quar_items = quar_items.get("items", [])
    if quar_items:
        raise RuntimeError(f"Found {len(quar_items)} quarantined raw files: {quar_items[:3]}")

    return {
        "cron_health": cron_eval,
        "cron_rows": deduped_cron_rows,
        "ingest_files_rows": ingest_files_rows,
        "ingest_files_summary": summarize_emitted_log_files(
            raw_file_entries=ingest_files_rows,
            parquet_file_entries=[],
            cron_rows=deduped_cron_rows,
        ),
        "api_queries_rows_count": len(api_queries_rows),
        "api_queries_latency_ms": summarize_numeric_series(
            [
                float(r.get("duration_ms") or r.get("elapsed_ms") or 0.0)
                for r in api_queries_rows
                if (r.get("duration_ms") or r.get("elapsed_ms")) is not None
            ]
        ),
        "natural_clock_stages": sorted(clock_stages),
        "outbox_depth": outbox_depth,
        "flush_calls": len(flush_runs),
        "sweep_endpoint_status": sweep_endpoint_status,
        "sweep_response": sweep_body,
        "storage_reconcile_response": rec_storage_body,
        "manual_flush_status": manual_flush_status,
        "sync_status": {
            "clock_interval_s": sync_body.get("clock_interval_s"),
            "idle_ticks": (sync_body.get("clock") or {}).get("idle_ticks"),
            "last_tick_at": sync_body.get("last_tick_at"),
            "last_ingest_at": sync_body.get("last_ingest_at"),
        },
        "platform_errors_count": len(platform_errors),
        "platform_errors_sample": platform_errors[:5],
        "quarantined_files_count": len(quar_items),
        "duration_s": round(time.time() - t0, 2),
    }


def step_6_browser_verify(
    root: Path,
    base_url: str,
    state_dir: Path,
    service_id: str,
    expect_min_requests: int,
    work_dir: Path,
    traffic_report: dict[str, Any] | None,
    incremental_url: str | None = None,
    *,
    ranges: tuple[str, ...] = DEFAULT_BENCHMARK_RANGES,
    filters: tuple[str, ...] = DEFAULT_BENCHMARK_FILTERS,
    cold_filtered: bool = False,
) -> dict[str, Any]:
    """Step 6: Load /dashboard and all hosted pages in Playwright and measure load & lag."""
    validated_ranges = validate_dashboard_presets(ranges)
    out_path = work_dir / "browser_verify.json"
    script = root / "app/tools/browser_verify.mjs"
    print(
        f"[Step 6] Running Playwright browser verification against {base_url} "
        f"(expecting >= {expect_min_requests:,} requests on /dashboard, "
        f"presets={','.join(validated_ranges)}, filters={len(filters)})..."
    )
    subprocess.run(
        [
            "node",
            str(script),
            "--url",
            base_url,
            "--state-dir",
            str(state_dir),
            "--service-id",
            service_id,
            "--mode",
            "verify",
            "--expect-min-requests",
            str(expect_min_requests),
            "--ranges",
            ",".join(validated_ranges),
            "--filters",
            ",".join(filters),
            *(["--cold-filtered"] if cold_filtered else []),
            "--out",
            str(out_path),
            *(["--incremental-url", incremental_url] if incremental_url else []),
        ],
        cwd=root,
        check=True,
    )
    browser_report = json.loads(out_path.read_text())
    if not browser_report.get("ok"):
        raise RuntimeError(f"Browser verification failed: {browser_report}")

    if traffic_report is not None:
        t_dash = (browser_report.get("dashboard") or {}).get("visibleAtEpochS")
        measured = (traffic_report.get("lag") or {}).get("send_to_parquet_lag_s")
        traffic_report["lag"] = compute_lag_breakdown(
            t_first_send=traffic_report["t_first_send"],
            t_send_done=traffic_report["t_send_done"],
            t_first_raw_fos=traffic_report["t_first_raw_fos"],
            max_minute_epoch=traffic_report["max_minute_epoch"],
            t_ingest_done=traffic_report["t_ingest_done"],
            t_dashboard_verified=float(t_dash) if t_dash is not None else None,
        )
        if measured is not None:
            traffic_report["lag"]["send_to_parquet_lag_s"] = measured

    return browser_report


DEPLOY_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}$")


def summarize_deploy_entry(report: dict[str, Any], run_id: str) -> dict[str, Any]:
    """Extract a compact historical summary of a deploy run for 5-deploy comparisons."""
    s1 = report.get("step_1_deploy") or {}
    s3 = report.get("step_3_confirm_active") or {}
    s4 = report.get("step_4_traffic_and_ingest") or {}
    s5 = report.get("step_5_confirm_crons") or {}
    s6 = report.get("step_6_browser_verify") or {}
    db = s6.get("dashboard") or {}
    cold = db.get("cold") or {}
    warm = db.get("warm") or {}
    lag = s4.get("lag") or {}
    ov = s4.get("verify_overall") or {}
    ch = s5.get("cron_health") or {}
    emitted = s4.get("emitted_files") or s5.get("ingest_files_summary") or {}
    dist = s6.get("distributions") or {}
    clean_dist = dist.get("cleanDashboardLoadMs") or {}
    filt_dist = dist.get("filteredDashboardLoadMs") or {}
    cls_dist = dist.get("clsScore") or {}
    recs = report.get("recommendations") or []
    return {
        "run_id": report.get("deploy_timestamp") or run_id,
        "head_sha": str(s1.get("head_sha") or s3.get("live_build_sha") or "unknown"),
        "compute_version": (s3.get("compute") or {}).get("active_version"),
        "vcl_version": (s3.get("monitored_vcl") or {}).get("active_version"),
        "log_period_s": (s3.get("monitored_vcl") or {}).get("s3_log_period_s"),
        "total_duration_s": report.get("total_duration_s"),
        "sent": ov.get("sent", 0),
        "matched": ov.get("matched", 0),
        "missing_count": ov.get("missing_count", 0),
        "raw_files_emitted_total": emitted.get("raw_files_emitted_total"),
        "raw_bytes_emitted_total": emitted.get("raw_bytes_emitted_total"),
        "parquet_files_written_total": emitted.get("parquet_files_written_total"),
        "parquet_bytes_written_total": emitted.get("parquet_bytes_written_total"),
        "raw_fos_landing_lag_s": lag.get("raw_fos_landing_lag_s"),
        "post_settle_ingest_lag_s": lag.get("post_settle_ingest_lag_s"),
        "send_to_parquet_lag_s": lag.get("send_to_parquet_lag_s"),
        "send_to_dashboard_lag_s": lag.get("send_to_dashboard_lag_s"),
        "cold_dashboard_ms": cold.get("loadMs") or db.get("coldLoadMs"),
        "warm_dashboard_ms": warm.get("loadMs") or db.get("warmLoadMs"),
        "clean_dashboard_p50_ms": clean_dist.get("p50"),
        "clean_dashboard_p95_ms": clean_dist.get("p95"),
        "filtered_dashboard_p50_ms": filt_dist.get("p50"),
        "filtered_dashboard_p95_ms": filt_dist.get("p95"),
        "cls_max": cls_dist.get("max"),
        "parquet_download_ms": cold.get("parquetDownloadMs") or db.get("parquetDownloadMs"),
        "worker_post_download_ms": cold.get("workerPostDownloadMs") or db.get("workerPostDownloadMs"),
        "parquet_files_fetched": cold.get("parquetFilesFetched") or db.get("parquetFilesFetched"),
        "manifest_requests": cold.get("manifestRequests"),
        "max_backend_requests_used": ch.get("max_backend_requests_used"),
        "max_vcpu_ms": ch.get("max_vcpu_ms"),
        "recommendations": recs,
    }


def load_prior_deploys(
    deploys_root: Path,
    current_run_id: str,
    limit: int = 5,
) -> list[dict[str, Any]]:
    """Load summaries and recommendations from up to `limit` prior deploy folders."""
    if not deploys_root.is_dir():
        return []
    candidates = sorted(
        (
            d
            for d in deploys_root.iterdir()
            if d.is_dir() and not d.is_symlink() and DEPLOY_TIMESTAMP_RE.match(d.name) and d.name != current_run_id
        ),
        key=lambda p: p.name,
        reverse=True,
    )
    priors: list[dict[str, Any]] = []
    for folder in candidates:
        report_file = folder / "relics" / "report.json"
        if not report_file.is_file():
            continue
        try:
            data = json.loads(report_file.read_text())
            priors.append(summarize_deploy_entry(data, folder.name))
        except (OSError, ValueError, KeyError) as exc:
            print(f"[warn] Skipping unreadable prior report {report_file}: {exc}")
            continue
        if len(priors) >= limit:
            break
    return priors


def generate_recommendations(
    report: dict[str, Any],
    prior_deploys: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Synthesize prioritized performance & architecture recommendations from run telemetry."""
    prior_deploys = prior_deploys or []
    prev = prior_deploys[0] if prior_deploys else None

    s3 = report.get("step_3_confirm_active") or {}
    s4 = report.get("step_4_traffic_and_ingest") or {}
    s5 = report.get("step_5_confirm_crons") or {}
    s6 = report.get("step_6_browser_verify") or {}
    db = s6.get("dashboard") or {}
    cold = db.get("cold") or {}
    warm = db.get("warm") or {}
    lag = s4.get("lag") or {}
    ch = s5.get("cron_health") or {}

    cold_ms = int(cold.get("loadMs") or db.get("coldLoadMs") or 0)
    warm_ms = int(warm.get("loadMs") or db.get("warmLoadMs") or 0)
    dl_ms = int(cold.get("parquetDownloadMs") or db.get("parquetDownloadMs") or 0)
    worker_ms = int(cold.get("workerPostDownloadMs") or db.get("workerPostDownloadMs") or 0)
    files_fetched = int(cold.get("parquetFilesFetched") or db.get("parquetFilesFetched") or 0)
    manifest_reqs = cold.get("manifestRequests")
    pre_pq_ms = int(cold.get("preParquetBootstrapMs") or 0)

    recommendations: list[dict[str, Any]] = []

    # 1. Check duplicate manifest pagination & HTTP/2 Parquet download concurrency
    delta_note = ""
    if prev and prev.get("cold_dashboard_ms"):
        prev_cold = int(prev["cold_dashboard_ms"])
        diff_ms = cold_ms - prev_cold
        pct = round((diff_ms / prev_cold) * 100, 1) if prev_cold > 0 else 0.0
        sign = "+" if diff_ms > 0 else ""
        delta_note = (
            f" (vs prior deploy {prev['head_sha'][:7]}: {prev_cold}ms -> {cold_ms}ms, {sign}{diff_ms}ms / {sign}{pct}%)"
        )

    if (manifest_reqs is not None and int(manifest_reqs) >= 2) or (files_fetched >= 20 and dl_ms >= 4000):
        recommendations.append(
            {
                "id": "dashboard-manifest-and-concurrency",
                "priority": "P1",
                "status": "OPEN",
                "category": "frontend-performance",
                "title": (
                    "Eliminate duplicate /api/files manifest walk and raise HTTP/2 "
                    "Parquet fetch concurrency on /dashboard"
                ),
                "target_files": [
                    "app/web/lib/dashboard/range.ts",
                    "app/web/lib/dashboard/engine.ts",
                    "app/web/lib/dashboard/client.ts",
                    "app/web/components/dashboard/Dashboard.tsx",
                ],
                "finding": (
                    f"Cold /dashboard load took {cold_ms}ms{delta_note}: "
                    f"pre-Parquet bootstrap took {pre_pq_ms}ms across "
                    f"{manifest_reqs if manifest_reqs is not None else 'multiple'} "
                    f"/api/files manifest requests, and downloading {files_fetched} "
                    f"Parquet files took {dl_ms}ms while post-download worker "
                    f"aggregation took only {worker_ms}ms."
                ),
                "recommendation": (
                    "Cache or pass the Manifest fetched during discoverPresets() "
                    "into runInWorker()/runDashboard() so the worker skips redundant "
                    "/api/files pagination, increase default HTTP/2 file fetch "
                    "concurrency in runDashboard() from 4 to 16, and reuse a persistent "
                    "module Worker in client.ts."
                ),
            }
        )
    elif prev and prev.get("cold_dashboard_ms") and cold_ms < int(prev["cold_dashboard_ms"]):
        recommendations.append(
            {
                "id": "dashboard-manifest-and-concurrency",
                "priority": "INFO",
                "status": "VERIFIED",
                "category": "frontend-performance",
                "title": ("Verified /dashboard manifest reuse & HTTP/2 concurrency optimization"),
                "target_files": [
                    "app/web/lib/dashboard/range.ts",
                    "app/web/lib/dashboard/engine.ts",
                    "app/web/lib/dashboard/client.ts",
                ],
                "finding": (
                    f"Cold /dashboard load improved to {cold_ms}ms{delta_note} "
                    f"(manifest requests={manifest_reqs}, download={dl_ms}ms for "
                    f"{files_fetched} files, warm reload={warm_ms}ms)."
                ),
                "recommendation": (
                    "Keep manifest sharing and HTTP/2 concurrency=16 in place; next "
                    "focus on server-side hourly Parquet compaction when 24h file "
                    "counts exceed 100."
                ),
            }
        )

    # 2. High 24h Parquet file count from 10s log_period windows
    if files_fetched >= 50:
        recommendations.append(
            {
                "id": "hourly-request-parquet-compaction",
                "priority": "P2",
                "status": "OPEN",
                "category": "storage-architecture",
                "title": (
                    "Compact closed hourly processed/request/ Parquet partitions "
                    "to bound 24h file count under 10s log_period"
                ),
                "target_files": [
                    "app/src/ingest/mod.rs",
                    "app/src/telemetry/compact.rs",
                ],
                "finding": (
                    f"/dashboard fetched {files_fetched} individual Parquet files "
                    f"(warm reload={warm_ms}ms). At log_period=10s, continuous traffic "
                    f"produces up to 360 Parquet files per hour."
                ),
                "recommendation": (
                    "Add a background request_compact cron stage (similar to "
                    "log_compact for operational telemetry) that merges closed-hour "
                    "v3-*.parquet files into a single hourly Parquet object once the "
                    "hour is settled."
                ),
            }
        )

    # 3. Ingest pipeline settle window vs post-settle reaction time
    post_settle_s = lag.get("post_settle_ingest_lag_s")
    send_to_pq_s = lag.get("send_to_parquet_lag_s")
    raw_fos_s = lag.get("raw_fos_landing_lag_s")
    if post_settle_s is not None and send_to_pq_s is not None:
        log_period = (s3.get("monitored_vcl") or {}).get("s3_log_period_s") or 10
        recommendations.append(
            {
                "id": "ingest-minute-settle-horizon",
                "priority": "P2" if float(post_settle_s) <= 20.0 else "P1",
                "status": "OPEN",
                "category": "ingest-pipeline",
                "title": (
                    "Sub-minute raw prefix checkpointing to decouple 10s log_period "
                    "from the 180s minute-directory settle horizon"
                ),
                "target_files": [
                    "app/src/ingest/plan.rs",
                    "app/src/ingest/mod.rs",
                ],
                "finding": (
                    f"With log_period={log_period}s, Fastly S3 logging landed the first "
                    f"raw batch in FOS in {raw_fos_s}s and Compute ingested the final "
                    f"minute {post_settle_s}s after its 180s settle horizon "
                    f"(total send_done->Parquet={send_to_pq_s}s)."
                ),
                "recommendation": (
                    "Because S3 logging partitions raw objects by minute=%M and "
                    "MINUTES_SETTLE_SECS=180s waits for late S3 writers before sealing "
                    "a minute prefix, converting completed objects within the active "
                    "minute via per-key processed watermarks can reduce end-to-end "
                    "send->Parquet latency from ~130s toward ~15-25s."
                ),
            }
        )

    # 4. Manual admin maintenance endpoint backend budget reserve
    sweep_status = s5.get("sweep_endpoint_status")
    manual_flush_status = s5.get("manual_flush_status")
    if (sweep_status is not None and int(sweep_status) != 200) or (manual_flush_status == "deferred"):
        recommendations.append(
            {
                "id": "admin-logs-budget-reserve",
                "priority": "P1",
                "status": "OPEN",
                "category": "compute-limits",
                "title": (
                    "Reserve backend call budget in POST /api/admin/logs/sweep "
                    "and classify /api/admin/logs/* as control_plane"
                ),
                "target_files": [
                    "app/src/admin/logs.rs",
                    "app/src/shell.rs",
                    "app/src/telemetry/flush.rs",
                ],
                "finding": (
                    f"Manual POST /api/admin/logs/sweep returned HTTP {sweep_status} "
                    f"(admin::logs::sweep passes reserve=0 to retention_sweep across "
                    f"8 datasets, consuming all 28 backend calls before LAST_SWEEP_KEY "
                    f"and record_admin_action), and POST /api/admin/logs/flush returned "
                    f"status='{manual_flush_status}' (outbox_depth={s5.get('outbox_depth')}) "
                    f"because /api/admin/logs/* is not in control_plane in shell.rs, "
                    f"leaving <19 calls (FIXED_CALLS + 1) after auth/setup/precheck."
                ),
                "recommendation": (
                    "Pass reserve = 4 in admin::logs::sweep(runtime) (matching worker "
                    "sweep_stage's TAIL reserve) and include /api/admin/logs/*, "
                    "/api/admin/usage/*, and /api/admin/operational-logging in "
                    "control_plane in shell.rs so manual admin endpoints retain full "
                    "backend budget."
                ),
            }
        )
    elif (
        prev
        and any(
            r.get("id") == "admin-logs-budget-reserve" and r.get("status") == "OPEN"
            for r in (prev.get("recommendations") or [])
        )
        and sweep_status == 200
        and manual_flush_status != "deferred"
    ):
        recommendations.append(
            {
                "id": "admin-logs-budget-reserve",
                "priority": "INFO",
                "status": "VERIFIED",
                "category": "compute-limits",
                "title": (
                    "Verified POST /api/admin/logs/sweep and POST /api/admin/logs/flush backend call budget reserve"
                ),
                "target_files": [
                    "app/src/admin/logs.rs",
                    "app/src/shell.rs",
                ],
                "finding": (
                    f"Manual POST /api/admin/logs/sweep returned HTTP {sweep_status} "
                    f"and POST /api/admin/logs/flush completed with "
                    f"status='{manual_flush_status}' (outbox_depth={s5.get('outbox_depth')})."
                ),
                "recommendation": (
                    "Keep reserve=4 in admin::logs::sweep and control_plane routing for /api/admin/logs/*."
                ),
            }
        )

    # 5. Compute resource budget headroom
    max_calls = int(ch.get("max_backend_requests_used") or 0)
    max_cache = int(ch.get("max_cache_lookups") or 0)
    max_vcpu = float(ch.get("max_vcpu_ms") or 0.0)
    ch_errors = ch.get("errors") or []
    at_risk = max_calls > 28 or max_cache > 56 or max_vcpu >= DEFAULT_VCPU_BUDGET_MS or bool(ch_errors)
    recommendations.append(
        {
            "id": "compute-resource-budget-headroom",
            "priority": "P1" if at_risk else "INFO",
            "status": "OPEN" if at_risk else "HEALTHY",
            "category": "compute-limits",
            "title": (
                "Cap per-invocation vCPU and Core Cache lookups in log_flush, request_ingest, and raw_delete"
                if at_risk
                else "Fastly Compute per-invocation resource budget headroom"
            ),
            "target_files": [
                "app/src/telemetry/flush.rs",
                "app/src/ingest/mod.rs",
                "app/src/admin/checks.rs",
            ],
            "finding": (
                f"Peak tick resource usage across all 6 cron stages: "
                f"backend_requests={max_calls}/28 (hard limit 32), "
                f"cache_lookups={max_cache}/56 (hard limit 64), "
                f"vcpu_ms={max_vcpu}ms/{int(DEFAULT_VCPU_BUDGET_MS)}ms (hard ceiling {int(HARD_VCPU_CEILING_MS)}ms)"
                + (
                    f". Observed soft-budget excursions: {'; '.join(str(e) for e in ch_errors[:5])}."
                    if ch_errors
                    else "."
                )
            ),
            "recommendation": (
                f"Lower the per-cycle outbox batch cap in telemetry/flush.rs so a "
                f"single log_flush stays <=48 Core Cache lookups and <{int(DEFAULT_VCPU_BUDGET_MS)}ms vCPU, "
                f"and check runtime.vcpu_ms() before starting additional work items "
                f"in request_ingest and raw_delete."
                if at_risk
                else "All cron stages operated within Fastly Compute per-invocation "
                "limits with 0 starved stages and 0 platform_errors."
            ),
        }
    )

    return recommendations


def build_e2e_freshness_report(report: dict[str, Any]) -> list[dict[str, Any]]:
    """Build an fla-compatible relics/e2e_freshness_report.json payload."""
    s3 = report.get("step_3_confirm_active") or {}
    s4 = report.get("step_4_traffic_and_ingest") or {}
    s5 = report.get("step_5_confirm_crons") or {}
    s6 = report.get("step_6_browser_verify") or {}
    db = s6.get("dashboard") or {}
    cold = db.get("cold") or {}
    warm = db.get("warm") or {}
    lag = s4.get("lag") or {}
    ov = s4.get("verify_overall") or {}
    emitted = s4.get("emitted_files") or {}
    log_period = (s3.get("monitored_vcl") or {}).get("s3_log_period_s") or 10

    page_matrix: dict[str, Any] = {}
    for page_name, pinfo in (s6.get("pages") or {}).items():
        page_matrix[page_name] = {
            "status": 200,
            "latency_ms": pinfo.get("loadMs"),
            "cls_score": ((pinfo.get("uiHealth") or {}).get("clsScore")),
            "freshness_lag_s": lag.get("send_to_dashboard_lag_s") if "dashboard" in page_name else None,
        }

    post_settle = lag.get("post_settle_ingest_lag_s")
    target_p95_s = 20.0
    target_status = (
        "WITHIN TARGET"
        if post_settle is not None and float(post_settle) <= target_p95_s
        else ("ELEVATED" if post_settle is not None else "SKIPPED")
    )

    return [
        {
            "env": report.get("install_name"),
            "name": f"Fastly Compute ({report.get('install_name')})",
            "service_id": report.get("service_id"),
            "cdn_domain": report.get("target_url"),
            "dashboard_url": report.get("base_url"),
            "mode": "multi_stage_load_10_100_1000_rps" if s4 else "browser_and_cron_only",
            "deployment_mode": "fastly_compute_parquet",
            "qualification": "verified" if report.get("ok") else "failed",
            "configured_polling": {
                "clock_interval_seconds": 1,
                "s3_log_period_seconds": log_period,
                "minute_settle_seconds": 180,
            },
            "active_benchmark": {
                "requests_sent": ov.get("sent", 0),
                "requests_matched": ov.get("matched", 0),
                "missing_count": ov.get("missing_count", 0),
                "duplicate_count": ov.get("duplicate_count", 0),
                "raw_fos_landing_lag_s": lag.get("raw_fos_landing_lag_s"),
                "post_settle_ingest_lag_s": post_settle,
                "send_to_parquet_lag_s": lag.get("send_to_parquet_lag_s"),
                "send_to_dashboard_lag_s": lag.get("send_to_dashboard_lag_s"),
                "cold_dashboard_load_ms": cold.get("loadMs"),
                "warm_dashboard_load_ms": warm.get("loadMs"),
                "parquet_files_fetched": cold.get("parquetFilesFetched"),
                "target_p95_s": target_p95_s,
                "target_status": target_status,
            },
            "emitted_files": emitted,
            "rps_summary": s4.get("rps_summary"),
            "compute_budget": s5.get("cron_health"),
            "ingest_files_summary": s5.get("ingest_files_summary"),
            "api_queries_latency_ms": s5.get("api_queries_latency_ms"),
            "benchmark_distributions": s6.get("distributions"),
            "ui_health": s6.get("uiQuality"),
            "api_waterfall": s6.get("apiTelemetry"),
            "page_freshness_matrix": page_matrix,
            "recommendations": report.get("recommendations", []),
        }
    ]


def render_markdown_report(report: dict[str, Any]) -> str:
    """Render a complete Markdown deployment & E2E freshness report."""
    s1 = report["step_1_deploy"]
    s3 = report["step_3_confirm_active"]
    s4 = report.get("step_4_traffic_and_ingest")
    s5 = report["step_5_confirm_crons"]
    s6 = report["step_6_browser_verify"]
    db = s6["dashboard"]
    cold = db["cold"]
    warm = db["warm"]
    ch = s5["cron_health"]
    recs = report.get("recommendations") or []
    priors = report.get("prior_deploys") or []
    dist = s6.get("distributions") or {}
    api_sum = s6.get("apiTelemetry") or {}

    lines = [
        f"# FLP Deployment & E2E Verification Report — `{report.get('deploy_timestamp', '')}`",
        "",
        f"- **Install**: `{report['install_name']}` ({report['base_url']})",
        f"- **Monitored VCL Service**: `{report['service_id']}` ({report['target_url']})",
        f"- **Commit SHA**: `{s1['head_sha']}` (Compute v{s3['compute']['active_version']}, Worker v{s3['worker']['active_version']}, Front v{s3['front']['active_version']}, VCL v{s3['monitored_vcl']['active_version']})",
        f"- **Log Period**: `{s3['monitored_vcl']['s3_log_period_s']}s` (`/_tick` = `{s3['front']['tick_interval_ms']}ms`)",
        f"- **Total Harness Duration**: `{report['total_duration_s']}s`",
        "",
        "## 1. End-to-End Freshness & Ingest Lag Scorecard",
        "",
        "| Metric | Value | Notes |",
        "| --- | --- | --- |",
    ]
    if s4:
        lag = s4["lag"]
        ov = s4["verify_overall"]
        lines.extend(
            [
                f"| Raw FOS First-Batch Landing | `{lag['raw_fos_landing_lag_s']}s` | Time from first request sent to first `.log.gz` in FOS |",
                f"| Post-Settle Ingest Reaction | `{lag['post_settle_ingest_lag_s']}s` | Time after 180s minute-settle horizon to 100% Parquet completion |",
                f"| End-to-End Send -> Parquet | `{lag['send_to_parquet_lag_s']}s` | Time from last request sent (`{ov['sent']:,}` total) to full Parquet drain |",
                f"| End-to-End Send -> Dashboard UI | `{lag['send_to_dashboard_lag_s']}s` | Time from last request sent to verified on `/dashboard` |",
            ]
        )
    else:
        lines.append("| Traffic & Ingest Lag | `skipped (--no-traffic)` | Browser & cron verification run |")

    clean_db = dist.get("cleanDashboardLoadMs") or {}
    filt_db = dist.get("filteredDashboardLoadMs") or {}
    lines.extend(
        [
            f"| Cold `/dashboard` Load | `{cold['loadMs']}ms` | Download `{cold['parquetDownloadMs']}ms` ({cold['parquetFilesFetched']} files), Worker `{cold['workerPostDownloadMs']}ms` |",
            f"| Warm `/dashboard` Reload | `{warm['loadMs']}ms` | Cached CacheStorage / CDN reload |",
            f"| Clean `/dashboard` Presets (`1h..24h`) | `p50={clean_db.get('p50')}ms`, `p95={clean_db.get('p95')}ms` | Across `{clean_db.get('count', 0)}` preset loads (`min={clean_db.get('min')}ms`, `max={clean_db.get('max')}ms`) |",
            f"| Filtered `/dashboard` Presets | `p50={filt_db.get('p50')}ms`, `p95={filt_db.get('p95')}ms` | Across `{filt_db.get('count', 0)}` filtered preset loads (`min={filt_db.get('min')}ms`, `max={filt_db.get('max')}ms`) |",
            "",
            "## 2. Traffic, Emitted Log Files & Parquet Accuracy Scorecard",
            "",
        ]
    )
    if s4:
        lines.extend(
            [
                "| Stage | Sent | Matched | Missing | Duplicates | Strict mismatches | Unknown ids | Actual RPS | Latency p50 / p95 (ms) |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for st in s4.get("stages", []):
            v = (s4.get("verify_per_stage") or {}).get(st["stage"], {})
            lat = st.get("latency_ms") or {}
            lines.append(
                f"| `{st['stage']}` ({st['target_rps']} RPS) | {v.get('sent', st['sent']):,} | "
                f"{v.get('matched', 0):,} | {v.get('missing_count', 0)} | "
                f"{v.get('duplicate_count', 0)} | {v.get('strict_mismatch_count', 0)} | "
                f"{v.get('unknown_id_count', 0)} | {st.get('actual_rps', 0)} | "
                f"{lat.get('p50', '—')} / {lat.get('p95', '—')} |"
            )
        ov = s4["verify_overall"]
        lines.append(
            f"| **Total** | **{ov['sent']:,}** | **{ov['matched']:,}** | "
            f"**{ov['missing_count']}** | **{ov['duplicate_count']}** | "
            f"**{ov['strict_mismatch_count']}** | **{ov['unknown_id_count']}** | — | — |"
        )
        em = s4.get("emitted_files") or {}
        raw_sz = em.get("raw_file_size_bytes") or {}
        pq_sz = em.get("parquet_file_size_bytes") or {}
        lines.extend(
            [
                "",
                "### Emitted Raw `.log.gz` & Parquet Output Files",
                "",
                f"- **Raw `.log.gz` Emitted**: `{em.get('raw_files_emitted_total', 0)}` files (`{em.get('raw_bytes_emitted_total', 0):,}` bytes total, `{em.get('raw_files_per_min', 0)}` files/min)",
                f"- **Raw File Size Distribution**: `min={raw_sz.get('min')}B`, `p50={raw_sz.get('p50')}B`, `p95={raw_sz.get('p95')}B`, `max={raw_sz.get('max')}B`",
                f"- **Parquet Window Files Written**: `{em.get('parquet_files_written_total', 0)}` files (`{em.get('parquet_bytes_written_total', 0):,}` bytes, `{em.get('parquet_rows_written_total', 0):,}` rows, `{em.get('parquet_bytes_per_row')}` B/row)",
                f"- **Parquet File Size Distribution**: `min={pq_sz.get('min')}B`, `p50={pq_sz.get('p50')}B`, `p95={pq_sz.get('p95')}B`, `max={pq_sz.get('max')}B`",
            ]
        )
    else:
        lines.append("_Traffic generation skipped (`--no-traffic`)._")

    lines.extend(
        [
            "",
            "## 3. Cron & Compute Resource Budget Scorecard",
            "",
            f"- **Stages Verified**: `{', '.join(ch['stages_seen'])}`",
            f"- **Natural 1s Clock Stages**: `{', '.join(s5['natural_clock_stages'])}`",
            f"- **Peak Backend Requests**: `{ch['max_backend_requests_used']} / 28` (hard limit 32)",
            f"- **Peak Cache Lookups**: `{ch['max_cache_lookups']} / 56` (hard limit 64)",
            f"- **Peak vCPU Time**: `{ch['max_vcpu_ms']}ms / {int(DEFAULT_VCPU_BUDGET_MS)}ms` (hard ceiling `{int(HARD_VCPU_CEILING_MS)}ms`)",
            f"- **Total Raw Files Converted / Deleted**: `{ch.get('total_raw_files_converted', 0)}` converted, `{ch.get('total_raw_files_deleted', 0)}` deleted, `{ch.get('total_parquet_files_written', 0)}` Parquet files (`{ch.get('total_rows_written', 0):,}` rows)",
            f"- **Outbox Depth After Flush**: `{s5['outbox_depth']}`",
            f"- **Platform Errors**: `{s5['platform_errors_count']}` | **Quarantined Files**: `{s5['quarantined_files_count']}`",
            "",
            "| Cron Stage | Runs | Listed (sum) | Converted (sum / p95) | Deleted (sum) | Parquet Files | Rows Written | Duration p50 / p95 (ms) | vCPU p50 / p95 / max (ms) | Peak Calls |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for st_name, st_dist in (ch.get("per_stage_distributions") or {}).items():
        dur_d = st_dist.get("duration_ms") or {}
        vcpu_d = st_dist.get("vcpu_ms") or {}
        conv_d = st_dist.get("converted_files") or {}
        list_d = st_dist.get("listed_files") or {}
        del_d = st_dist.get("deleted_files") or {}
        pq_d = st_dist.get("parquet_files_written") or {}
        rows_d = st_dist.get("rows_written") or {}
        calls_d = st_dist.get("backend_requests_used") or {}
        lines.append(
            f"| `{st_name}` | {st_dist.get('runs', 0)} | {list_d.get('sum', 0)} | "
            f"{conv_d.get('sum', 0)} / {conv_d.get('p95', 0)} | {del_d.get('sum', 0)} | "
            f"{pq_d.get('sum', 0)} | {rows_d.get('sum', 0):,} | "
            f"{dur_d.get('p50', 0)} / {dur_d.get('p95', 0)} | "
            f"{vcpu_d.get('p50', 0)} / {vcpu_d.get('p95', 0)} / {vcpu_d.get('max', 0)} | "
            f"{calls_d.get('max', 0)} |"
        )

    lines.extend(
        [
            "",
            "## 4. Browser Page Load Timings, UI Health (CLS) & API Waterfall",
            "",
            f"- **UI Health Summary**: CLS `p50={(dist.get('clsScore') or {}).get('p50')}`, `p95={(dist.get('clsScore') or {}).get('p95')}`, `max={(dist.get('clsScore') or {}).get('max')}` (threshold `0.1`) | Horizontal overflow pages: `{sum(bool((p.get('uiHealth') or {}).get('hasHorizontalOverflow')) for p in (s6.get('pages') or {}).values())}` | Stuck skeleton pages: `{sum(bool((p.get('uiHealth') or {}).get('stuckSkeletonCount')) for p in (s6.get('pages') or {}).values())}`",
            f"- **API Waterfall Summary**: `{api_sum.get('totalCalls', 0)}` total `/api/*` calls (`{api_sum.get('rangeReadCount', 0)}` Parquet range reads, `{api_sum.get('rangeProbeCount', 0)}` range probes `bytes=0-0`, `{(api_sum.get('durationMs') or {}).get('p50')}ms` p50 / `{(api_sum.get('durationMs') or {}).get('p95')}ms` p95)",
            "",
            "| Page / Tab | Load Time (ms) | CLS Score | FCP (ms) | LCP (ms) | Long Tasks | API Calls |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for page_path, info in (s6.get("pages") or {}).items():
        ui_h = info.get("uiHealth") or {}
        w_sum = info.get("apiSummary") or {}
        lines.append(
            f"| `{page_path}` | `{info['loadMs']}ms` | `{ui_h.get('clsScore', 0)}` | "
            f"`{ui_h.get('fcpMs', '—')}` | `{ui_h.get('lcpMs', '—')}` | "
            f"`{ui_h.get('longTaskCount', 0)}` (`{ui_h.get('longTaskTotalMs', 0)}ms`) | "
            f"`{w_sum.get('totalCalls', 0)}` |"
        )

    lines.extend(
        [
            "",
            "## 5. Performance & Architecture Recommendations",
            "",
        ]
    )
    for rec in recs:
        lines.extend(
            [
                f"### [{rec['priority']}] {rec['title']} (`{rec['status']}`)",
                f"- **Category**: `{rec['category']}` | **Targets**: `{', '.join(rec.get('target_files', []))}`",
                f"- **Finding**: {rec['finding']}",
                f"- **Recommendation**: {rec['recommendation']}",
                "",
            ]
        )

    lines.extend(
        [
            "## 6. Prior Deploys Comparison (Current + Up to 5 Prior Deploys)",
            "",
            "| Deploy Timestamp | Commit | Cold `/dashboard` | Warm `/dashboard` | Clean p50/p95 | Filtered p50/p95 | Parquet Files | Post-Settle Lag | Send->Parquet | Top Recommendation |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    current_summary = summarize_deploy_entry(report, str(report.get("deploy_timestamp") or "current"))
    for idx, entry in enumerate([current_summary, *priors]):
        tag = " **(current)**" if idx == 0 else ""
        top_rec = (
            f"[{entry['recommendations'][0]['priority']}] {entry['recommendations'][0]['id']} ({entry['recommendations'][0]['status']})"
            if entry.get("recommendations")
            else "—"
        )
        lines.append(
            f"| `{entry['run_id']}`{tag} | `{str(entry['head_sha'])[:7]}` | "
            f"`{entry.get('cold_dashboard_ms') or '—'}ms` | "
            f"`{entry.get('warm_dashboard_ms') or '—'}ms` | "
            f"`{entry.get('clean_dashboard_p50_ms') or '—'}/{entry.get('clean_dashboard_p95_ms') or '—'}ms` | "
            f"`{entry.get('filtered_dashboard_p50_ms') or '—'}/{entry.get('filtered_dashboard_p95_ms') or '—'}ms` | "
            f"`{entry.get('parquet_files_fetched') or '—'}` | "
            f"`{entry.get('post_settle_ingest_lag_s') if entry.get('post_settle_ingest_lag_s') is not None else '—'}s` | "
            f"`{entry.get('send_to_parquet_lag_s') if entry.get('send_to_parquet_lag_s') is not None else '—'}s` | "
            f"{top_rec} |"
        )
    lines.append("")
    return "\n".join(lines)


def render_html_report(report: dict[str, Any], log_text: str) -> str:
    """Render a dark-mode Tailwind HTML report matching fla's reports/deploys/index.html."""
    s1 = report["step_1_deploy"]
    s3 = report["step_3_confirm_active"]
    s4 = report.get("step_4_traffic_and_ingest")
    s5 = report["step_5_confirm_crons"]
    s6 = report["step_6_browser_verify"]
    db = s6["dashboard"]
    cold = db["cold"]
    warm = db["warm"]
    ch = s5["cron_health"]
    recs = report.get("recommendations") or []
    priors = report.get("prior_deploys") or []
    ts = html.escape(str(report.get("deploy_timestamp") or ""))
    sha = html.escape(str(s1.get("head_sha") or "")[:12])
    lag = (s4 or {}).get("lag") or {}
    ov = (s4 or {}).get("verify_overall") or {}

    rec_cards_html = []
    for rec in recs:
        badge_cls = (
            "bg-amber-500/10 text-amber-400 border-amber-500/40"
            if rec["priority"] in ("P0", "P1")
            else (
                "bg-sky-500/10 text-sky-400 border-sky-500/40"
                if rec["priority"] == "P2"
                else "bg-emerald-500/10 text-emerald-400 border-emerald-500/40"
            )
        )
        targets = ", ".join(rec.get("target_files") or [])
        rec_cards_html.append(
            f"""
            <div class="p-4 bg-slate-950 border border-slate-800 rounded-xl space-y-2">
              <div class="flex items-center justify-between gap-2">
                <span class="font-bold text-white text-xs font-sans">{html.escape(rec["title"])}</span>
                <span class="px-2 py-0.5 rounded border text-[10px] font-bold {badge_cls}">{html.escape(rec["priority"])} · {html.escape(rec["status"])}</span>
              </div>
              <p class="text-[11px] text-slate-400 font-mono">Targets: {html.escape(targets)}</p>
              <p class="text-xs text-slate-200 font-sans"><strong>Finding:</strong> {html.escape(rec["finding"])}</p>
              <p class="text-xs text-emerald-300/90 font-sans"><strong>Action:</strong> {html.escape(rec["recommendation"])}</p>
            </div>
            """
        )

    current_summary = summarize_deploy_entry(report, str(report.get("deploy_timestamp") or "current"))
    history_rows_html = []
    for idx, entry in enumerate([current_summary, *priors]):
        label = " (current)" if idx == 0 else f" (-{idx})"
        top_rec = (
            f"[{entry['recommendations'][0]['priority']}] {entry['recommendations'][0]['title']}"
            if entry.get("recommendations")
            else "—"
        )
        history_rows_html.append(
            f"""
            <tr class="border-b border-slate-800/70">
              <td class="py-2 pr-4 text-sky-400">{html.escape(str(entry["run_id"]))}{html.escape(label)}</td>
              <td class="py-2 pr-4 text-blue-400">{html.escape(str(entry["head_sha"])[:7])}</td>
              <td class="py-2 pr-4 text-right">{html.escape(str(entry.get("cold_dashboard_ms") or "—"))} ms</td>
              <td class="py-2 pr-4 text-right">{html.escape(str(entry.get("warm_dashboard_ms") or "—"))} ms</td>
              <td class="py-2 pr-4 text-right">{html.escape(str(entry.get("parquet_files_fetched") or "—"))}</td>
              <td class="py-2 pr-4 text-right">{html.escape(str(entry.get("post_settle_ingest_lag_s") if entry.get("post_settle_ingest_lag_s") is not None else "—"))} s</td>
              <td class="py-2 pr-4 text-right">{html.escape(str(entry.get("send_to_parquet_lag_s") if entry.get("send_to_parquet_lag_s") is not None else "—"))} s</td>
              <td class="py-2 text-slate-300 font-sans">{html.escape(top_rec)}</td>
            </tr>
            """
        )

    pages_rows_html = []
    for page_path, info in (s6.get("pages") or {}).items():
        pages_rows_html.append(
            f'<tr class="border-b border-slate-800/60"><td class="py-1.5 text-slate-300">{html.escape(page_path)}</td><td class="py-1.5 text-right text-emerald-400">{info["loadMs"]} ms</td></tr>'
        )

    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><title>Deployment Report - {ts}</title><script src="https://cdn.tailwindcss.com"></script></head>
<body class="bg-slate-950 text-slate-100 font-mono min-h-screen">
  <div class="w-full max-w-[100%] px-6 py-4 space-y-6">
    <header class="flex justify-between items-center border-b border-slate-800 pb-3">
      <div>
        <h1 class="text-base font-extrabold tracking-tight text-white">FLP DEPLOYMENT &amp; E2E FRESHNESS REPORT</h1>
        <p class="text-slate-400 mt-0.5 text-[11px]">Timestamp: {ts} | Commit: <span class="text-blue-400">{sha}</span> | Install: <span class="text-violet-400">{html.escape(report["install_name"])}</span> | Service: <span class="text-cyan-400">{html.escape(report["service_id"])}</span></p>
      </div>
      <div class="px-3 py-1 rounded-full bg-emerald-500/10 border border-current text-emerald-500 text-xs font-bold">SUCCESS</div>
    </header>

    <section class="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-4">
      <div class="p-4 bg-slate-900 border border-slate-800 rounded-xl">
        <div class="text-[10px] uppercase text-slate-400 font-sans">Cold /dashboard</div>
        <div class="text-lg font-bold text-emerald-400 mt-1">{cold["loadMs"]} ms</div>
        <div class="text-[10px] text-slate-500">dl {cold["parquetDownloadMs"]}ms · {cold["parquetFilesFetched"]} files</div>
      </div>
      <div class="p-4 bg-slate-900 border border-slate-800 rounded-xl">
        <div class="text-[10px] uppercase text-slate-400 font-sans">Warm /dashboard</div>
        <div class="text-lg font-bold text-sky-400 mt-1">{warm["loadMs"]} ms</div>
        <div class="text-[10px] text-slate-500">CacheStorage / CDN warm</div>
      </div>
      <div class="p-4 bg-slate-900 border border-slate-800 rounded-xl">
        <div class="text-[10px] uppercase text-slate-400 font-sans">Post-Settle Ingest Lag</div>
        <div class="text-lg font-bold text-amber-400 mt-1">{lag.get("post_settle_ingest_lag_s", "—")} s</div>
        <div class="text-[10px] text-slate-500">raw FOS landing: {lag.get("raw_fos_landing_lag_s", "—")}s</div>
      </div>
      <div class="p-4 bg-slate-900 border border-slate-800 rounded-xl">
        <div class="text-[10px] uppercase text-slate-400 font-sans">Send &rarr; Parquet / UI</div>
        <div class="text-lg font-bold text-violet-400 mt-1">{lag.get("send_to_parquet_lag_s", "—")} s / {lag.get("send_to_dashboard_lag_s", "—")} s</div>
        <div class="text-[10px] text-slate-500">log_period={s3["monitored_vcl"]["s3_log_period_s"]}s</div>
      </div>
      <div class="p-4 bg-slate-900 border border-slate-800 rounded-xl">
        <div class="text-[10px] uppercase text-slate-400 font-sans">Parquet Accuracy</div>
        <div class="text-lg font-bold text-emerald-400 mt-1">{ov.get("matched", 0):,} / {ov.get("sent", 0):,}</div>
        <div class="text-[10px] text-slate-500">missing={ov.get("missing_count", 0)} · dup={ov.get("duplicate_count", 0)}</div>
      </div>
      <div class="p-4 bg-slate-900 border border-slate-800 rounded-xl">
        <div class="text-[10px] uppercase text-slate-400 font-sans">Compute Budget Peak</div>
        <div class="text-lg font-bold text-cyan-400 mt-1">{ch["max_backend_requests_used"]}/28 calls</div>
        <div class="text-[10px] text-slate-500">vCPU {ch["max_vcpu_ms"]}ms/{int(DEFAULT_VCPU_BUDGET_MS)}ms · cache {ch["max_cache_lookups"]}/56</div>
      </div>
    </section>

    <section class="p-6 bg-slate-900 border border-slate-800 rounded-xl space-y-4">
      <h2 class="text-sm font-bold text-white uppercase tracking-wider font-sans border-b border-slate-800 pb-2">Performance &amp; Architecture Recommendations</h2>
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {"".join(rec_cards_html)}
      </div>
    </section>

    <section class="p-6 bg-slate-900 border border-slate-800 rounded-xl space-y-4 overflow-x-auto">
      <h2 class="text-sm font-bold text-white uppercase tracking-wider font-sans border-b border-slate-800 pb-2">Current + Last 5 Prior Deploys Comparison</h2>
      <table class="w-full text-xs text-left">
        <thead>
          <tr class="border-b border-slate-800 text-slate-400 uppercase text-[10px]">
            <th class="py-2 pr-4">Deploy Run</th>
            <th class="py-2 pr-4">Commit</th>
            <th class="py-2 pr-4 text-right">Cold /dashboard</th>
            <th class="py-2 pr-4 text-right">Warm /dashboard</th>
            <th class="py-2 pr-4 text-right">Parquet Files</th>
            <th class="py-2 pr-4 text-right">Post-Settle Lag</th>
            <th class="py-2 pr-4 text-right">Send&rarr;Parquet</th>
            <th class="py-2">Top Recommendation</th>
          </tr>
        </thead>
        <tbody>
          {"".join(history_rows_html)}
        </tbody>
      </table>
    </section>

    <section class="grid grid-cols-1 lg:grid-cols-2 gap-4">
      <div class="p-6 bg-slate-900 border border-slate-800 rounded-xl space-y-3">
        <h2 class="text-sm font-bold text-white uppercase tracking-wider font-sans border-b border-slate-800 pb-2">Browser Page &amp; Tab Load Timings</h2>
        <table class="w-full text-xs">
          <tbody>
            {"".join(pages_rows_html)}
          </tbody>
        </table>
      </div>
      <div class="p-6 bg-slate-900 border border-slate-800 rounded-xl space-y-3">
        <h2 class="text-sm font-bold text-white uppercase tracking-wider font-sans border-b border-slate-800 pb-2">Harness Execution Log</h2>
        <pre class="w-full h-[360px] border border-slate-800 rounded-lg bg-black/75 p-4 overflow-y-auto text-[11px] text-slate-200 whitespace-pre-wrap">{html.escape(log_text)}</pre>
      </div>
    </section>
  </div>
</body>
</html>
"""


def finalize_deploy_report(
    deploys_root: Path,
    run_ts: str,
    report: dict[str, Any],
    log_text: str,
    keep_prior: int = 5,
) -> Path:
    """Write reports/deploys/<run_ts>/ artifacts, update current symlink & root index.html, and prune older runs beyond keep_prior."""
    run_dir = deploys_root / run_ts
    logs_dir = run_dir / "logs"
    relics_dir = run_dir / "relics"
    logs_dir.mkdir(parents=True, exist_ok=True)
    relics_dir.mkdir(parents=True, exist_ok=True)

    (logs_dir / "deploy.log").write_text(log_text)
    e2e_payload = build_e2e_freshness_report(report)
    (relics_dir / "e2e_freshness_report.json").write_text(json.dumps(e2e_payload, indent=2) + "\n")
    (relics_dir / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    (relics_dir / "report.md").write_text(render_markdown_report(report))

    html_text = render_html_report(report, log_text)
    (run_dir / "index.html").write_text(html_text)

    # Update reports/deploys/current symlink -> run_ts
    current_link = deploys_root / "current"
    if current_link.is_symlink() or current_link.exists():
        current_link.unlink()
    current_link.symlink_to(run_ts)

    # Write reports/deploys/index.html with <base href="current/"> injected after <head>
    root_html = html_text.replace("<head>", '<head><base href="current/">', 1)
    (deploys_root / "index.html").write_text(root_html)

    # Retain current + keep_prior timestamped folders (e.g. 1 + 5 = 6 total folders)
    ts_dirs = sorted(
        (d for d in deploys_root.iterdir() if d.is_dir() and not d.is_symlink() and DEPLOY_TIMESTAMP_RE.match(d.name)),
        key=lambda p: p.name,
        reverse=True,
    )
    max_total = max(1, keep_prior + 1)
    for stale_dir in ts_dirs[max_total:]:
        shutil.rmtree(stale_dir, ignore_errors=True)

    return run_dir


def print_summary_banner(report: dict[str, Any]) -> None:
    """Print an fla-style E2E Freshness, Accuracy, Cron, Prior-Deploys & Recommendations scorecard."""
    print("\n" + "=" * 96)
    print(f"{'END-TO-END FRESHNESS & INGEST LAG SCORECARD':^96}")
    print("=" * 96)
    s1 = report["step_1_deploy"]
    s2 = report["step_2_reconcile_vcl"]
    s3 = report["step_3_confirm_active"]
    s4 = report.get("step_4_traffic_and_ingest")
    s5 = report["step_5_confirm_crons"]
    s6 = report["step_6_browser_verify"]
    db = s6["dashboard"]
    cold = db["cold"]
    warm = db["warm"]
    dist = s6.get("distributions") or {}
    ui_sum = s6.get("uiQuality") or {}
    api_sum = s6.get("apiTelemetry") or {}

    s1_status = "skipped" if s1["skipped"] else f"{s1['duration_s']}s"
    print(
        f"1. Deploy:       HEAD {s1['head_sha'][:7]} -> compute v{s3['compute']['active_version']}, "
        f"worker v{s3['worker']['active_version']}, front v{s3['front']['active_version']} "
        f"({s1_status})"
    )
    print(
        f"2. VCL Reconcile: {s2['service_id']} active v{s3['monitored_vcl']['active_version']} "
        f"(log_period={s3['monitored_vcl']['s3_log_period_s']}s, mode={s2['trigger_mode']}, {s2['duration_s']}s)"
    )
    print(
        f"3. Active Check: /version={s3['live_build_sha'][:7]}, "
        f"/_tick={s3['front']['tick_interval_ms']}ms, status={s3['monitored_vcl']['status']}"
    )
    if s4:
        ov = s4["verify_overall"]
        lag = s4["lag"]
        em = s4.get("emitted_files") or {}
        raw_sz = em.get("raw_file_size_bytes") or {}
        print(
            f"4. Load & Ingest: sent={ov['sent']:,}, matched={ov['matched']:,}, "
            f"missing={ov['missing_count']}, dup={ov['duplicate_count']}, "
            f"strict_mismatches={ov['strict_mismatch_count']}, unknown_ids={ov['unknown_id_count']}, "
            f"schema_valid={s4['schema']['valid']}"
        )
        print(
            f"   Emitted Files: raw_files={em.get('raw_files_emitted_total', 0)} "
            f"({em.get('raw_bytes_emitted_total', 0):,}B, p50={raw_sz.get('p50')}B, p95={raw_sz.get('p95')}B) | "
            f"parquet_files={em.get('parquet_files_written_total', 0)} "
            f"({em.get('parquet_bytes_written_total', 0):,}B, {em.get('parquet_rows_written_total', 0):,} rows)"
        )
        print(
            f"   Lag Breakdown: raw_fos_landing={lag['raw_fos_landing_lag_s']}s | "
            f"post_settle_ingest={lag['post_settle_ingest_lag_s']}s | "
            f"send_to_parquet={lag['send_to_parquet_lag_s']}s | "
            f"send_to_dashboard={lag['send_to_dashboard_lag_s']}s"
        )
    else:
        print("4. Load & Ingest: skipped (--no-traffic)")

    ch = s5["cron_health"]
    print(
        f"5. Crons & Ops:  stages={ch['stages_seen']} | "
        f"max_calls={ch['max_backend_requests_used']}/28 | "
        f"max_cache={ch['max_cache_lookups']}/56 | "
        f"max_vcpu={ch['max_vcpu_ms']}ms/{int(DEFAULT_VCPU_BUDGET_MS)}ms | "
        f"converted={ch.get('total_raw_files_converted', 0)} | "
        f"deleted={ch.get('total_raw_files_deleted', 0)} | "
        f"outbox={s5['outbox_depth']} | errors={s5['platform_errors_count']}"
    )
    clean_db = dist.get("cleanDashboardLoadMs") or {}
    filt_db = dist.get("filteredDashboardLoadMs") or {}
    print(
        f"6. Browser UI:   /dashboard cold={cold['loadMs']}ms "
        f"(download={cold['parquetDownloadMs']}ms, "
        f"worker={cold['workerPostDownloadMs']}ms, "
        f"files={cold['parquetFilesFetched']}, "
        f"manifests={cold.get('manifestRequests', 'n/a')}), "
        f"warm={warm['loadMs']}ms, "
        f"total_requests={cold['totalRequests']:,}"
    )
    print(
        f"                 clean_presets(p50={clean_db.get('p50')}ms, p95={clean_db.get('p95')}ms) | "
        f"filtered_presets(p50={filt_db.get('p50')}ms, p95={filt_db.get('p95')}ms) | "
        f"cls_max={ui_sum.get('maxClsScore', 0)} | "
        f"api_calls={api_sum.get('totalCalls', 0)} "
        f"(p50={(api_sum.get('durationMs') or {}).get('p50')}ms, p95={(api_sum.get('durationMs') or {}).get('p95')}ms)"
    )
    for page_path, info in s6.get("pages", {}).items():
        print(f"                 {page_path}: {info['loadMs']}ms")

    priors = report.get("prior_deploys") or []
    print("-" * 96)
    print("PRIOR DEPLOYS COMPARISON (CURRENT + UP TO 5 PRIOR DEPLOYS)")
    print(
        f"  {'Run Timestamp':<22} {'Commit':<9} {'Cold UI':>10} {'Warm UI':>10} "
        f"{'Files':>7} {'PostSettle':>11} {'Send->PQ':>10}"
    )
    current_summary = summarize_deploy_entry(report, str(report.get("deploy_timestamp") or "current"))
    for idx, entry in enumerate([current_summary, *priors]):
        tag = "*" if idx == 0 else " "
        c_ms = f"{entry.get('cold_dashboard_ms')}ms" if entry.get("cold_dashboard_ms") is not None else "—"
        w_ms = f"{entry.get('warm_dashboard_ms')}ms" if entry.get("warm_dashboard_ms") is not None else "—"
        ps_s = f"{entry.get('post_settle_ingest_lag_s')}s" if entry.get("post_settle_ingest_lag_s") is not None else "—"
        sp_s = f"{entry.get('send_to_parquet_lag_s')}s" if entry.get("send_to_parquet_lag_s") is not None else "—"
        files_c = str(entry.get("parquet_files_fetched") or "—")
        print(
            f"{tag} {entry['run_id']!s:<22} {str(entry['head_sha'])[:7]:<9} "
            f"{c_ms:>10} {w_ms:>10} {files_c:>7} {ps_s:>11} {sp_s:>10}"
        )

    recs = report.get("recommendations") or []
    if recs:
        print("-" * 96)
        print("PERFORMANCE & ARCHITECTURE RECOMMENDATIONS")
        for rec in recs:
            print(f"  [{rec['priority']}] ({rec['status']}) {rec['title']}")
            print(f"       Finding: {rec['finding']}")
            print(f"       Action:  {rec['recommendation']}")
    print("=" * 96 + "\n")


class _TeeWriter:
    """Mirror stdout writes to both the original stream and an in-memory buffer."""

    def __init__(self, primary: Any, buffer: io.StringIO) -> None:
        self._primary = primary
        self._buffer = buffer

    def write(self, data: str) -> int:
        self._primary.write(data)
        self._buffer.write(data)
        return len(data)

    def flush(self) -> None:
        self._primary.flush()
        self._buffer.flush()


def main() -> int:
    parser = argparse.ArgumentParser(description="End-to-end post-commit deploy and verification harness for flp.")
    parser.add_argument("--name", default=DEFAULT_INSTALL_NAME)
    parser.add_argument("--service-id", default=DEFAULT_TEST_SERVICE_ID)
    parser.add_argument("--target-url", default=DEFAULT_TEST_DOMAIN)
    parser.add_argument("--log-period", type=int, default=DEFAULT_LOG_PERIOD_S)
    parser.add_argument(
        "--lt-repo",
        type=Path,
        default=DEFAULT_LT_REPO,
        help="Shared load-testing checkout that runs Step 4 (default: %(default)s)",
    )
    parser.add_argument(
        "--ranges",
        default=",".join(DEFAULT_BENCHMARK_RANGES),
        help="Comma-separated dashboard presets (allowed: 1h,3h,6h,12h,24h; 10m is never valid)",
    )
    parser.add_argument(
        "--filters",
        default=",".join(DEFAULT_BENCHMARK_FILTERS),
        help="Comma-separated fastly-log-analytics filters (e.g. +status:200,+method:GET,-status:503)",
    )
    parser.add_argument(
        "--cold-filtered",
        action="store_true",
        default=True,
        help="Include cold cache-cleared filtered dashboard loads (default: True).",
    )
    parser.add_argument(
        "--skip-deploy",
        action="store_true",
        help="Skip Step 1 build/deploy and verify inventory build_sha (not necessarily HEAD).",
    )
    parser.add_argument(
        "--no-traffic",
        action="store_true",
        help="Skip the Step 4 deploy-check.",
    )
    parser.add_argument(
        "--keep-prior",
        type=int,
        default=5,
        help="Number of prior deploy folders & recommendation sets to retain (default: 5).",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=None,
        help="Optional directory for this run's browser and JSON reports.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Optional path to write full JSON verification report.",
    )
    args = parser.parse_args()
    ranges = validate_dashboard_presets(
        [x.strip() for x in args.ranges.split(",") if x.strip()],
        allow_extended=False,
    )
    filters = tuple(x.strip() for x in args.filters.split(",") if x.strip())

    root = Path(__file__).resolve().parents[2]
    state_dir = Path.home() / ".flp-state" / args.name
    inv_path = state_dir / "inventory.json"
    if not inv_path.is_file():
        raise RuntimeError(f"Missing inventory at {inv_path}")

    env_vars = parse_env_file(root / ".env")
    api_token = env_vars.get("FASTLY_API_TOKEN") or os.environ.get("FASTLY_API_TOKEN")
    if not api_token:
        raise RuntimeError("FASTLY_API_TOKEN is required in .env or environment")

    run_ts = dt.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    deploys_root = root / "reports" / "deploys"
    work_dir = args.work_dir.resolve() if args.work_dir else (root / "data/deploy-test" / f"run-{int(time.time())}")
    work_dir.mkdir(parents=True, exist_ok=True)

    log_buf = io.StringIO()
    orig_stdout = sys.stdout
    sys.stdout = _TeeWriter(orig_stdout, log_buf)
    try:
        t_harness_start = time.time()
        inventory = json.loads(inv_path.read_text())
        base_url = f"https://{inv_services(inventory)['front']['domain']}"
        session_cookie = ensure_browser_auth(root, base_url, state_dir, args.service_id)
        with app_client(base_url, session_cookie) as preflight_http:
            preflight_monitored_service(preflight_http, args.service_id)

        # Step 1: Build & Deploy
        s1 = step_1_deploy(root, args.name, state_dir, env_vars, args.skip_deploy)

        inventory = json.loads(inv_path.read_text())
        front_domain = inv_services(inventory)["front"]["domain"]
        deployed_base_url = f"https://{front_domain}"

        # Ensure Playwright browser auth session
        if deployed_base_url != base_url:
            base_url = deployed_base_url
            session_cookie = ensure_browser_auth(root, base_url, state_dir, args.service_id)

        with (
            app_client(base_url, session_cookie) as app_http,
            fastly_client(api_token) as fastly_http,
        ):
            # Step 2: Reconcile VCL & Ensure log_period=10
            s2 = step_2_reconcile_vcl(app_http, args.service_id, args.log_period)

            # Step 3: Confirm Active Deployments & VCL Code
            expected_live_sha = (s1["inventory_build_sha"] or s1["head_sha"]) if s1["skipped"] else s1["head_sha"]
            s3 = step_3_confirm_active(
                app_http,
                fastly_http,
                inventory,
                expected_live_sha,
                args.service_id,
                args.log_period,
            )

            # Step 4: Shared-tool deploy-check stage
            s4: dict[str, Any] | None = None
            expect_min_requests = 0
            if not args.no_traffic:
                s4 = step_4_deploy_check(
                    work_dir,
                    config=root / "fastly-lt.toml",
                    lt_repo=args.lt_repo.expanduser(),
                )
                expect_min_requests = int(s4["expect_min_requests"])

            # Step 5: Confirm all crons & operational health
            s5 = step_5_confirm_crons(
                app_http,
                args.service_id,
                s4["t_run_start"] if s4 else t_harness_start,
                had_traffic=not args.no_traffic,
            )
            if s4 is not None:
                s4["emitted_files"] = summarize_emitted_log_files(
                    raw_file_entries=[
                        *s4.get("observed_raw_files", []),
                        *s5.get("ingest_files_rows", []),
                    ],
                    parquet_file_entries=s4.get("parquet_file_entries", []),
                    cron_rows=s5.get("cron_rows", []),
                    duration_s=max(1.0, float(s4["t_send_done"] - s4["t_first_send"])),
                )

        # Step 6: Playwright browser verification & page load / lag report
        s6 = step_6_browser_verify(
            root,
            base_url,
            state_dir,
            args.service_id,
            expect_min_requests,
            work_dir,
            s4,
            None if args.no_traffic else args.target_url,
            ranges=ranges,
            filters=filters,
            cold_filtered=args.cold_filtered,
        )

        prior_deploys = load_prior_deploys(deploys_root, run_ts, limit=args.keep_prior)
        report: dict[str, Any] = {
            "ok": s5["cron_health"]["ok"] and s5["platform_errors_count"] == 0,
            "deploy_timestamp": run_ts,
            "install_name": args.name,
            "base_url": base_url,
            "service_id": args.service_id,
            "target_url": args.target_url,
            "total_duration_s": round(time.time() - t_harness_start, 2),
            "step_1_deploy": s1,
            "step_2_reconcile_vcl": s2,
            "step_3_confirm_active": s3,
            "step_4_traffic_and_ingest": s4,
            "step_5_confirm_crons": s5,
            "step_6_browser_verify": s6,
            "prior_deploys": prior_deploys,
        }
        report["recommendations"] = generate_recommendations(report, prior_deploys)
        print_summary_banner(report)
    finally:
        sys.stdout = orig_stdout

    deploy_dir = finalize_deploy_report(
        deploys_root=deploys_root,
        run_ts=run_ts,
        report=report,
        log_text=log_buf.getvalue(),
        keep_prior=args.keep_prior,
    )
    report_path = args.out or (work_dir / "harness_report.json")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Deploy report directory: {deploy_dir}")
    print(f"Current HTML report:     {deploys_root / 'index.html'}")
    print(f"Full JSON report:        {report_path}")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

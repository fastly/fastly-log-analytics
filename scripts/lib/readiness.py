"""Bounded app readiness + private Prometheus UI access for the canonical deploy runner."""

from __future__ import annotations

import argparse
import base64
import html
import json
import math
import os
import random
import re
import signal
import socket
import subprocess
import sys
import threading
import time
import tomllib
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import Request
from uuid import uuid4

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import scripts  # noqa: E402,F401
from scripts.lib.environments import admin_urlopen, parse_iso_utc, validate_admin_tls_config  # noqa: E402
from scripts.lib.section_measurements import BUDGETS  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
_MAX_HTML_BYTES = 2 * 1024 * 1024
_MAX_JSON_BYTES = 32 * 1024 * 1024


def normalize_version(version: str) -> str:
    value = version.strip().lower().lstrip("v")
    for pat, repl in ((r"(alpha|a)", "a"), (r"(beta|b)", "b"), (r"(rc|c)", "rc")):
        value = re.sub(rf"[-_.]?{pat}[-_.]?(?=\d)", repl, value)
    return value


def pyproject_version(path: Path = ROOT / "pyproject.toml") -> str:
    with path.open("rb") as handle:
        return tomllib.load(handle)["project"]["version"]


@dataclass(frozen=True)
class ProbeResult:
    status: int | None
    latency_ms: float
    content_type: str = ""
    body: bytes = b""
    error: str | None = None
    reason: str = ""
    exc: BaseException | None = field(default=None, repr=False)

    @property
    def failed(self) -> bool:
        return self.error is not None

    @property
    def http_error(self) -> bool:
        return self.error is not None and self.status is not None

    def json(self) -> object:
        return json.loads(self.body)


def probe(
    url: str,
    *,
    timeout: float,
    headers: Mapping[str, str] | None = None,
    method: str | None = None,
    data: bytes | None = None,
    limit: int = _MAX_JSON_BYTES,
) -> ProbeResult:
    request, start = Request(url, data=data, headers=dict(headers or {}), method=method), time.perf_counter()
    try:
        with admin_urlopen(request, timeout=timeout) as resp:
            return ProbeResult(
                resp.status,
                (time.perf_counter() - start) * 1000.0,
                resp.headers.get("Content-Type", ""),
                resp.read(limit + 1),
            )
    except HTTPError as exc:
        ms = (time.perf_counter() - start) * 1000.0
        try:
            try:
                body = exc.read(limit + 1)
            except OSError:
                body = b""
            return ProbeResult(exc.code, ms, body=body, error=str(exc), reason=str(exc.reason), exc=exc)
        finally:
            exc.close()
    except Exception as exc:
        return ProbeResult(None, (time.perf_counter() - start) * 1000.0, error=str(exc), exc=exc)


if "scripts.lib.environments" in sys.modules:
    sys.modules["scripts.lib.environments"].probe = probe  # type: ignore[attr-defined]


def _get(url: str, timeout: float, limit: int) -> tuple[int, str, bytes]:
    res = probe(url, timeout=timeout, limit=limit)
    if res.status is None:
        assert res.exc is not None
        raise res.exc
    return (res.status, "", b"") if res.http_error else (res.status, res.content_type, res.body)


def _safe(value: object, limit: int = 40) -> str:
    if not isinstance(value, str):
        return f"<{type(value).__name__}>"
    return "".join(ch if ch.isprintable() else "?" for ch in value[:limit]) + ("..." if len(value) > limit else "")


def check_frontend(origin: str, timeout: float) -> str | None:
    status, content_type, body = _get(f"{origin}/dashboard", timeout, _MAX_HTML_BYTES)
    if status != 200:
        return f"frontend /dashboard HTTP {status}"
    if "text/html" not in content_type.lower():
        return "frontend /dashboard is not HTML"
    if len(body) > _MAX_HTML_BYTES:
        return "frontend /dashboard response exceeds size limit"
    return None if body.strip() else "frontend /dashboard returned an empty body"


def _json(url: str, timeout: float, label: str) -> tuple[dict | None, str | None]:
    status, _, body = _get(url, timeout, _MAX_JSON_BYTES)
    if status != 200:
        return None, f"{label} HTTP {status}"
    if len(body) > _MAX_JSON_BYTES:
        return None, f"{label} response exceeds size limit"
    try:
        data = json.loads(body)
    except ValueError:
        return None, f"{label} is not JSON"
    return (data, None) if isinstance(data, dict) else (None, f"{label} is not a JSON object")


def check_backend(origin: str, timeout: float, expect_version: str | None) -> str | None:
    data, error = _json(f"{origin}/api/health", timeout, "backend /api/health")
    if error:
        return error
    if data.get("status") != "ok":
        return f"backend /api/health status={_safe(data.get('status'))}"
    version = data.get("version")
    if expect_version and (
        not isinstance(version, str) or normalize_version(version) != normalize_version(expect_version)
    ):
        return f"backend version {_safe(version)} != expected {_safe(expect_version)}"
    return None


def check_admin_bootstrap(origin: str, timeout: float, min_services: int) -> str | None:
    data, error = _json(f"{origin}/api/bootstrap", timeout, "backend /api/bootstrap")
    if error:
        return error
    settings = data.get("settings")
    if not isinstance(settings, dict):
        return "bootstrap has no settings object"
    if settings.get("needs_login") or settings.get("is_remote_analyst") is not False:
        return "bootstrap is not an admin session (classified as analyst/login)"
    services = data.get("services")
    if not isinstance(services, list):
        return "bootstrap services is not a list"
    return (
        None
        if len(services) >= min_services
        else f"bootstrap lists {len(services)} services, expected >= {min_services}"
    )


def wait_ready(
    name: str,
    frontend: str,
    backend: str,
    *,
    attempts: int,
    interval: float,
    timeout: float,
    bootstrap_timeout: float,
    expect_version: str | None,
    admin: bool,
    min_services: int,
    log: Callable[[str], None] = print,
    sleep: Callable[[float], None] = time.sleep,
) -> bool:
    checks: list[tuple[str, Callable[[], str | None]]] = [
        ("frontend", lambda: check_frontend(frontend, timeout)),
        ("backend", lambda: check_backend(backend, timeout, expect_version)),
    ]
    if admin:
        checks.append(("admin", lambda: check_admin_bootstrap(backend, bootstrap_timeout, min_services)))
    last: list[str] = []
    for attempt in range(1, attempts + 1):
        failures = []
        for phase, check in checks:
            try:
                error = check()
            except (OSError, ValueError) as exc:
                error = f"{phase} request failed: {type(exc).__name__}"
            if error:
                failures.append(error)
        if not failures:
            log(f"[{name}] ready on attempt {attempt}/{attempts}: " + ", ".join(p for p, _ in checks) + " OK")
            return True
        last = failures
        log(f"[{name}] attempt {attempt}/{attempts} not ready: {'; '.join(failures)}")
        if attempt < attempts:
            sleep(interval)
    log(f"[{name}] FAILED readiness after {attempts} attempts: {'; '.join(last)}")
    return False


def readiness_main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Bounded frontend + backend (+ admin bootstrap) readiness")
    for flag in ("--name", "--frontend", "--backend"):
        p.add_argument(flag, required=True)
    for flag, tp, df in (
        ("--attempts", int, 40),
        ("--interval", float, 3),
        ("--timeout", float, 5),
        ("--bootstrap-timeout", float, 20),
        ("--min-services", int, 1),
    ):
        p.add_argument(flag, type=tp, default=df)
    ver = p.add_mutually_exclusive_group()
    ver.add_argument("--expect-version")
    ver.add_argument("--expect-pyproject-version", action="store_true")
    p.add_argument("--no-admin", action="store_true")
    args = p.parse_args(argv)
    if (
        args.attempts < 1
        or args.interval < 0
        or args.timeout <= 0
        or args.bootstrap_timeout <= 0
        or args.min_services < 0
    ):
        p.error("attempts >= 1, interval >= 0, timeout > 0, bootstrap-timeout > 0 and min-services >= 0 are required")
    try:
        validate_admin_tls_config()
        expect = pyproject_version() if args.expect_pyproject_version else args.expect_version
    except (OSError, ValueError, KeyError) as exc:
        print(f"[{args.name}] readiness configuration invalid: {type(exc).__name__}", file=sys.stderr)
        return 2
    ready = wait_ready(
        args.name,
        args.frontend.rstrip("/"),
        args.backend.rstrip("/"),
        attempts=args.attempts,
        interval=args.interval,
        timeout=args.timeout,
        bootstrap_timeout=args.bootstrap_timeout,
        expect_version=expect,
        admin=not args.no_admin,
        min_services=args.min_services,
        log=lambda line: print(line, flush=True),
    )
    return 0 if ready else 1


def configured_polling(settings_response: Any, service_id: str) -> dict[str, Any]:
    services = settings_response.get("services") if isinstance(settings_response, dict) else None
    if not isinstance(services, list):
        raise ValueError("services response is missing its service list")
    service = next((e for e in services if isinstance(e, dict) and e.get("service_id") == service_id), None)
    if service is None:
        raise ValueError("requested service was not returned by the services endpoint")
    cron_sync = {} if (cs := service.get("cron_sync")) is None else cs
    if not isinstance(cron_sync, dict):
        raise ValueError("service cron_sync settings are invalid")
    mode = cron_sync.get("polling_mode", "adaptive")
    if mode not in {"regular", "adaptive"}:
        raise ValueError("service polling_mode must be regular or adaptive")
    log_period = int(service.get("log_period") or 60)
    if interval_mins := cron_sync.get("interval_mins"):
        interval, src = max(5, int(interval_mins) * 60), "interval_mins"
    elif interval_seconds := cron_sync.get("interval_seconds"):
        interval, src = max(5, int(interval_seconds)), "interval_seconds"
    else:
        interval, src = max(5, log_period // 2 if log_period >= 60 else log_period), "log_period"
    return {
        "mode": mode,
        "interval_seconds": interval,
        "interval_source": src,
        "adaptive_followup_interval_seconds": 3 if mode == "adaptive" else None,
        "adaptive_max_followups": 2 if mode == "adaptive" else 0,
    }


def parse_iso(ts_str: str | None) -> datetime | None:
    return parse_iso_utc(ts_str)


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


def _spoof_ip() -> str:
    return f"{random.randint(12, 223)}.{random.randint(1, 254)}.{random.randint(1, 254)}.{random.randint(1, 254)}"


def _urlopen_status(req: urllib.request.Request, timeout: float = 8, opener: Any = None) -> int:
    try:
        with (opener.open if opener else urllib.request.urlopen)(req, timeout=timeout) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return 0


def send_edge_request_probe(cdn_domain: str, marker: str, idx: int) -> int:
    url = f"{cdn_domain.rstrip('/')}/test-freshness?probe={marker}&seq={idx}"
    hdrs = {
        "User-Agent": f"FLA-Probe-Agent/1.0 ({marker})",
        "X-Source-Ip": _spoof_ip(),
        "Fastly-Request-ID": f"req-{marker}-{idx:04d}",
    }
    return _urlopen_status(urllib.request.Request(url, headers=hdrs))


def send_edge_rum_probe(cdn_domain: str, marker: str, idx: int) -> int:
    url = f"{cdn_domain.rstrip('/')}/rum-beacon?cid={marker}-{idx}&rum_metric_name=LCP&rum_metric_value=1250&rum_pathname=%2Ftest-freshness"
    payload = {
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
    hdrs = {
        "Content-Type": "application/json",
        "User-Agent": f"FLA-RUM-Probe/1.0 ({marker})",
        "X-Source-Ip": _spoof_ip(),
    }
    return _urlopen_status(
        urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"), headers=hdrs, method="POST")
    )


class _NoProbeRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> None:
        return None


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


def summarize_correlated(probes: list[dict[str, Any]], ended: float) -> dict[str, Any]:
    streams, floor = {}, BUDGETS["minimum_samples_per_state"]
    for stream in ("request", "vitals", "errors"):
        pop = [p for p in probes if p["stream"] == stream]
        visible = [p for p in pop if p.get("first_visible") is not None and 200 <= p["http_status"] < 300]
        bounds = sorted(p["first_visible"] - p["sent"] for p in visible)
        count, complete = len(bounds), bool(pop) and len(bounds) == len(pop)
        p95 = bounds[math.ceil(count * 0.95) - 1] if count else None
        streams[stream] = {
            "sent": len(pop),
            "correlated": sum(p.get("first_visible") is not None for p in pop),
            "observed": count,
            "right_censored": sum(p.get("first_visible") is None for p in pop),
            "http_errors": sum(not 200 <= p["http_status"] < 300 for p in pop),
            "left_censored": sum(p.get("last_absent") is None for p in visible),
            "complete": complete,
            "p95_sample_floor": floor,
            "p95_population_sufficient": complete and count >= floor,
            "upper_bound_s": {
                "p95": p95,
                "p99": bounds[math.ceil(count * 0.99) - 1] if count else None,
                "max": max(bounds) if count else None,
            },
            "p99_population_sufficient": complete and count >= 100,
            "target_assessment": "within_target_upper_bound"
            if complete and count >= floor and p95 <= BUDGETS["peak_freshness_p95_ms"] / 1000
            else "incomplete_not_proven_failure",
            "samples": [
                {
                    "http_status": p["http_status"],
                    "send_to_response_s": p["response"] - p["sent"],
                    "followup_duration_s": ended - p["sent"],
                    "source_timestamp": p.get("source_timestamp"),
                    "event_to_queryable_upper_bound_s": p["first_visible"] - p["sent"]
                    if p.get("first_visible") is not None
                    else None,
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


class ActiveTrafficStreamer:
    def __init__(self, cdn_domain: str, marker: str, rps: float = 10.0):
        self.cdn_domain, self.marker, self.rps = cdn_domain, marker, max(1.0, rps)
        self.stop_event, self._thread = threading.Event(), None
        self.requests_sent = self.rum_sent = self.errors = 0

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
                t0, seq = time.perf_counter(), seq + 1
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


def compute_lag_statistics(
    samples: list[dict[str, Any]], baseline_latest_ts: datetime | None = None, min_elapsed_s: float = 6.0
) -> dict[str, Any]:
    active = (
        (
            [s for s in samples if s.get("header_latest_ts") and s["header_latest_ts"] > baseline_latest_ts]
            if baseline_latest_ts
            else []
        )
        or [s for s in samples if s.get("elapsed_s", 0) >= min_elapsed_s]
        or list(samples)
    )
    head_lags = [s["header_lag_s"] for s in active if s.get("header_lag_s") is not None]
    dash_lags = [s["dashboard_lag_s"] for s in active if s.get("dashboard_lag_s") is not None]

    def _stats(vals: list[float]) -> dict[str, float | None]:
        if not vals:
            return {"min": None, "max": None, "avg": None, "p95": None, "p99": None}
        s = sorted(vals)
        return {
            "min": round(min(vals), 1),
            "max": round(max(vals), 1),
            "avg": round(sum(vals) / len(vals), 1),
            "p95": s[math.ceil(len(s) * 0.95) - 1],
            "p99": s[math.ceil(len(s) * 0.99) - 1],
        }

    header, dashboard = _stats(head_lags), _stats(dash_lags)
    target_p95_s, hp, dp = BUDGETS["peak_freshness_p95_ms"] / 1000, header["p95"], dashboard["p95"]
    status = (
        "UNMEASURED" if hp is None or dp is None else ("ELEVATED" if max(hp, dp) > target_p95_s else "WITHIN TARGET")
    )
    return {
        "header_lag": header,
        "dashboard_lag": dashboard,
        "sample_count": len(active),
        "total_samples": len(samples),
        "target_p95_s": target_p95_s,
        "target_status": status,
    }


def print_active_scorecard(results: list[dict[str, Any]]) -> None:
    print(
        f"\n{'=' * 110}\n                      REQUEST WATERMARK RECENCY SCORECARD (DIAGNOSTIC ONLY)\n{'=' * 110}\n{'Environment':<28} │ {'Mode':<11} │ {'Min Lag':<8} │ {'Max Lag':<8} │ {'Avg Lag':<8} │ {'P95 Lag':<8} │ {'Dash Avg':<9} │ {'Requests':<9} │ {'Target Status':<15}\n{'-' * 28}─┼─{'-' * 11}─┼─{'-' * 8}─┼─{'-' * 8}─┼─{'-' * 8}─┼─{'-' * 8}─┼─{'-' * 9}─┼─{'-' * 9}─┼─{'-' * 15}",
        flush=True,
    )
    for r in results:
        if "error" in r:
            print(f"{r.get('name', r.get('env', 'Unknown')):<28} │ ERROR: {r['error']}", flush=True)
            continue
        bench = r.get("active_benchmark", {})
        hl, dl = bench.get("header_lag", {}), bench.get("dashboard_lag", {})
        fmt = lambda d, k: f"{d[k]}s" if d.get(k) is not None else "—"  # noqa: E731
        print(
            f"{r['name']:<28} │ {r.get('deployment_mode', '—'):<11} │ {fmt(hl, 'min'):>8} │ {fmt(hl, 'max'):>8} │ {fmt(hl, 'avg'):>8} │ {fmt(hl, 'p95'):>8} │ {fmt(dl, 'avg'):>9} │ {bench.get('requests_sent', 0):>9,} │ {bench.get('target_status', '—'):<15}",
            flush=True,
        )
    print(
        f"Qualification incomplete: request/RUM event latency and clock uncertainty are unverified.\n{'=' * 110}\n",
        flush=True,
    )


def print_burst_scorecard(results: list[dict[str, Any]]) -> None:
    print(
        f"\n{'=' * 96}\n                      END-TO-END FRESHNESS & INGEST LAG SCORECARD (SINGLE BURST)\n{'=' * 96}\n{'Environment':<28} │ {'Header Seen':<11} │ {'Cron Ingest':<11} │ {'Dash Seen':<12} │ {'Dashboard Lag':<14} │ {'Header Lag':<11}\n{'-' * 28}─┼─{'-' * 11}─┼─{'-' * 11}─┼─{'-' * 12}─┼─{'-' * 14}─┼─{'-' * 11}",
        flush=True,
    )
    for r in results:
        if "error" in r:
            print(f"{r.get('name', r.get('env', 'Unknown')):<28} │ ERROR: {r['error']}", flush=True)
            continue
        delays, matrix = r.get("wall_clock_delays", {}), r.get("page_freshness_matrix", {})
        dash_lag, head_lag = (
            matrix.get("Dashboard Bundle", {}).get("freshness_lag_s"),
            matrix.get("Header Badge", {}).get("freshness_lag_s"),
        )
        f_del = lambda k: f"{delays[k]}s" if delays.get(k) is not None else "TIMEOUT"  # noqa: E731
        dash_s, head_s = (
            (f"{dash_lag:.1f}s ago" if dash_lag is not None else "—"),
            (f"{head_lag:.1f}s ago" if head_lag is not None else "—"),
        )
        print(
            f"{r['name']:<28} │ {f_del('header_seconds'):<11} │ {f_del('cron_request_seconds'):<11} │ {f_del('dashboard_bundle_seconds'):<12} │ {dash_s:<14} │ {head_s:<11}",
            flush=True,
        )
    print(f"{'=' * 96}\n", flush=True)


def escape_sql_literal(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"escape_sql_literal expected str, got {type(value).__name__}")
    return value.replace("'", "''")


# ── Private Prometheus UI Access (folded from scripts/lib/observability_ui.py) ───────────────────

LIFETIME = 3600
MAX_BODY = 1024 * 1024
CONTAINER_REQUEST = """import base64,http.client,json,sys
r=json.load(sys.stdin); c=http.client.HTTPConnection("127.0.0.1",9090,timeout=10)
c.request(r["method"],r["path"],body=base64.b64decode(r["body"]),headers=r["headers"]); s=c.getresponse(); b=s.read(16777217)
if len(b)>16777216: raise ValueError("Prometheus response too large")
print(json.dumps({"status":s.status,"type":s.getheader("Content-Type","application/octet-stream"),"body":base64.b64encode(b).decode()})); c.close()"""
BROWSER_CHECK = """const { chromium } = require('./frontend/node_modules/@playwright/test');
(async () => {
  const browser = await chromium.launch({headless: true});
  try {
    const page = await browser.newPage();
    await page.route('**/*', route => new URL(route.request().url()).origin === process.argv[1] ? route.continue() : route.abort());
    const response = await page.goto(process.argv[1] + '/query', {timeout: 30000});
    if (!response || response.status() !== 200) throw Error('UI document unavailable');
    if (!(await page.title()).includes('Prometheus')) throw Error('Wrong UI');
    await page.locator('.cm-editor').first().waitFor({state:'visible', timeout:30000});
    await page.getByRole('button', {name:'Execute',exact:true}).waitFor({state:'visible',timeout:30000});
    if (new URL(page.url()).origin !== process.argv[1]) throw Error('UI redirected');
    console.log(JSON.stringify({ui_verified:true}));
  } finally { await browser.close(); }
})().catch(() => { console.error('Prometheus browser verification failed'); process.exit(1); });"""


def private_origin(origin: str) -> str:
    p = urlsplit(origin)
    if (
        p.scheme != "http"
        or p.hostname != "127.0.0.1"
        or p.username is not None
        or p.password is not None
        or p.path
        or p.query
        or p.fragment
        or not p.port
        or origin != f"http://127.0.0.1:{p.port}"
    ):
        raise ValueError("Metrics UI requires a credential-free loopback origin")
    return origin


def metrics_ui_link(origin: str, verified: bool) -> str:
    if not verified:
        return ""
    url = html.escape(private_origin(origin) + "/query", quote=True)
    return f'<a href="{url}" target="_blank" rel="noopener noreferrer" class="text-sky-400 hover:underline">Metrics UI</a> <span class="text-slate-400 text-[9px]">(private; expires within 1h)</span>'


def verify_ui(origin: str, receipt_path: Path | None = None) -> None:
    private_origin(origin)
    if receipt_path is not None:
        import httpx

        from scripts.dev.verify_observability import ProbeReceipt, _metric_matches, _receiver_json

        receipt = ProbeReceipt.model_validate_json(receipt_path.read_text())
        if not -5 <= time.time() - receipt.emitted_at.timestamp() <= 300:
            raise ValueError("UI attribution receipt expired")
        query = (
            f'app_observability_probe_total{{job="{receipt.service_name}",instance="{receipt.service_instance_id}"}}'
        )
        with httpx.Client(verify=True, trust_env=False, follow_redirects=False) as client:
            payload = _receiver_json(client, origin + "/api/v1/query", params={"query": query}, remaining=10)
        if payload is None or not _metric_matches(
            payload, job=receipt.service_name, instance=str(receipt.service_instance_id), emitted_at=receipt.emitted_at
        ):
            raise ValueError("Metrics UI forward does not reach the verified receiver")
    res = subprocess.run(
        ["node", "-e", BROWSER_CHECK, origin], cwd=ROOT, capture_output=True, text=True, check=True, timeout=75
    )
    if json.loads(res.stdout).get("ui_verified") is not True:
        raise ValueError("Prometheus UI was not verified")


def unused_loopback_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def write_private(path: Path, data: dict) -> None:
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as stream:
        json.dump(data, stream)


def start_access(directory: Path, mode: str, command: list[str], port: int) -> tuple[Path, str]:
    if mode not in {"proxy", "forward"} or not command:
        raise ValueError("Unknown private UI access mode")
    token = uuid4().hex
    state = directory / f"ui-{token}.json"
    write_private(state, {"token": token, "mode": mode, "command": command, "port": port})
    proc = subprocess.Popen(
        [sys.executable, "-m", "scripts.lib.observability_ui", "serve", str(state), token],
        cwd=ROOT,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    write_private(state.with_suffix(".pid"), {"pid": proc.pid, "token": token})
    return state, private_origin(f"http://127.0.0.1:{port}")


def _owned_supervisor_pid(state: Path) -> int | None:
    data, pid = json.loads(state.read_text()), int(json.loads(state.with_suffix(".pid").read_text())["pid"])
    cmd = subprocess.run(
        ["ps", "-p", str(pid), "-o", "command="], capture_output=True, text=True, check=False, timeout=5
    ).stdout
    return pid if "scripts.lib.observability_ui serve" in cmd and data["token"] in cmd else None


def stop_access(state: Path) -> None:
    if (pid := _owned_supervisor_pid(state)) is not None:
        try:
            os.killpg(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass


def report_link(path: Path) -> str:
    if not path.exists():
        return ""
    data = json.loads(path.read_text())
    if (
        data.get("ui_verified") is not True
        or data["expires_at"] <= time.time()
        or _owned_supervisor_pid(Path(data["state"])) is None
    ):
        return ""
    origin = private_origin(data["origin"])
    with socket.create_connection(("127.0.0.1", urlsplit(origin).port), timeout=1):
        pass
    return metrics_ui_link(origin, True)


def _proxy_server(port: int, command: list[str]) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            self.relay()

        def do_POST(self):
            self.relay()

        def relay(self):
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length < 0 or length > MAX_BODY or self.headers.get("Transfer-Encoding"):
                    raise ValueError("Unsupported body")
                if not self.path.startswith("/") or self.path.startswith("//"):
                    raise ValueError("Origin-form request required")
                payload = {
                    "method": self.command,
                    "path": self.path,
                    "body": base64.b64encode(self.rfile.read(length)).decode(),
                    "headers": {"Content-Type": self.headers.get("Content-Type", "application/json")},
                }
                res = subprocess.run(
                    [*command, "python", "-c", CONTAINER_REQUEST],
                    input=json.dumps(payload),
                    capture_output=True,
                    text=True,
                    check=True,
                    timeout=15,
                )
                data = json.loads(res.stdout)
                body = base64.b64decode(data["body"], validate=True)
                self.send_response(int(data["status"]))
                for k, v in (
                    ("Content-Type", data["type"]),
                    ("Content-Length", str(len(body))),
                    ("X-Content-Type-Options", "nosniff"),
                ):
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(body)
            except (ValueError, OSError, subprocess.SubprocessError):
                self.send_error(502, "Private Metrics UI unavailable")

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.daemon_threads = True
    return server


def _serve(state: Path, token: str) -> None:
    config = json.loads(state.read_text())
    if config["token"] != token or state.stat().st_mode & 0o077:
        raise ValueError("Invalid supervisor ownership")
    child, server, stop = None, None, threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    try:
        if config["mode"] == "proxy":
            server = _proxy_server(config["port"], config["command"])
            threading.Thread(target=server.serve_forever, daemon=True).start()
        else:
            child = subprocess.Popen(config["command"], stdin=subprocess.DEVNULL)
        deadline = time.monotonic() + LIFETIME
        while time.monotonic() < deadline and not stop.wait(1):
            if child and child.poll() is not None:
                return
    finally:
        if server:
            server.shutdown()
            server.server_close()
        if child and child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=5)


def observability_ui_main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("operation", choices=["serve", "stop", "stop-directory", "link"])
    p.add_argument("state", type=Path)
    p.add_argument("token", nargs="?")
    args = p.parse_args(argv)
    if args.operation == "link":
        try:
            print(report_link(args.state))
        except (OSError, ValueError, KeyError, subprocess.SubprocessError):
            print("")
    elif args.operation == "stop-directory":
        for path in args.state.glob("*-link.json"):
            path.unlink()
        for path in args.state.glob("ui-*.json"):
            stop_access(path)
    elif args.operation == "stop":
        stop_access(args.state)
    else:
        _serve(args.state, args.token)


def main(argv: list[str] | None = None) -> int:
    eff_argv = list(sys.argv[1:] if argv is None else argv)
    prog = Path(sys.argv[0]).name if sys.argv else ""
    spec_name = (
        globals().get("__spec__").name if globals().get("__name__") == "__main__" and globals().get("__spec__") else ""
    )
    if (
        spec_name == "scripts.lib.observability_ui"
        or "observability_ui" in prog
        or (eff_argv and eff_argv[0] in ("serve", "stop", "stop-directory", "link"))
    ):
        observability_ui_main(argv)
        return 0
    return readiness_main(argv)


if __name__ == "__main__":
    sys.exit(main())

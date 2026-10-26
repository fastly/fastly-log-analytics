"""Bounded app readiness: frontend HTML + backend health + admin bootstrap.

Proves the app tier answers on BOTH sides before data checks run. It does not
prove ingestion or data freshness; those stay with the later health/verify
gates. Uses ``admin_urlopen`` so a configured mTLS origin presents its client
identity and every other origin is plain HTTP(S). Output is sanitized: status
codes, exception type names and shape verdicts only, never response bodies.
"""

import argparse
import json
import math
import random
import re
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
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request

# Keep direct `python scripts/lib/readiness.py` invocations working.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.lib.admin_tls import admin_urlopen, validate_admin_tls_config  # noqa: E402
from scripts.lib.environments import parse_iso_utc  # noqa: E402
from scripts.lib.section_measurements import BUDGETS  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
_MAX_HTML_BYTES = 2 * 1024 * 1024
_MAX_JSON_BYTES = 32 * 1024 * 1024


def normalize_version(version: str) -> str:
    """Compare PEP 440 and display spellings (3.0.0b3 == 3.0.0-beta3)."""
    value = version.strip().lower().lstrip("v")
    value = re.sub(r"[-_.]?(alpha|a)[-_.]?(?=\d)", "a", value)
    value = re.sub(r"[-_.]?(beta|b)[-_.]?(?=\d)", "b", value)
    value = re.sub(r"[-_.]?(rc|c)[-_.]?(?=\d)", "rc", value)
    return value


def pyproject_version(path: Path = ROOT / "pyproject.toml") -> str:
    with path.open("rb") as handle:
        return tomllib.load(handle)["project"]["version"]


@dataclass(frozen=True)
class ProbeResult:
    """Outcome of one bounded HTTP probe; never raises for network failures.

    ``status`` is None only for transport failures; ``error`` is set for every
    failure (``str(exc)``, including HTTP >= 400, which keeps ``status`` and the
    bounded error ``body``). ``latency_ms`` is time to response headers.
    """

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
    """One request through ``admin_urlopen`` (mTLS only for a configured origin).

    At most ``limit + 1`` body bytes are read so callers can detect oversize.
    """
    request = Request(url, data=data, headers=dict(headers or {}), method=method)
    start = time.perf_counter()
    try:
        with admin_urlopen(request, timeout=timeout) as response:
            latency_ms = (time.perf_counter() - start) * 1000.0
            return ProbeResult(
                status=response.status,
                latency_ms=latency_ms,
                content_type=response.headers.get("Content-Type", ""),
                body=response.read(limit + 1),
            )
    except HTTPError as exc:
        latency_ms = (time.perf_counter() - start) * 1000.0
        try:
            try:
                body = exc.read(limit + 1)
            except OSError:
                body = b""
            return ProbeResult(exc.code, latency_ms, body=body, error=str(exc), reason=str(exc.reason), exc=exc)
        finally:
            exc.close()
    except Exception as exc:
        latency_ms = (time.perf_counter() - start) * 1000.0
        return ProbeResult(None, latency_ms, error=str(exc), exc=exc)


def _get(url: str, timeout: float, limit: int) -> tuple[int, str, bytes]:
    result = probe(url, timeout=timeout, limit=limit)
    if result.status is None:
        assert result.exc is not None
        raise result.exc
    if result.http_error:
        return result.status, "", b""
    return result.status, result.content_type, result.body


def _safe(value: object, limit: int = 40) -> str:
    """Bounded, printable rendering of an untrusted response value."""
    if not isinstance(value, str):
        return f"<{type(value).__name__}>"
    cleaned = "".join(ch if ch.isprintable() else "?" for ch in value[:limit])
    return cleaned + ("..." if len(value) > limit else "")


def check_frontend(origin: str, timeout: float) -> str | None:
    status, content_type, body = _get(f"{origin}/dashboard", timeout, _MAX_HTML_BYTES)
    if status != 200:
        return f"frontend /dashboard HTTP {status}"
    if "text/html" not in content_type.lower():
        return "frontend /dashboard is not HTML"
    if len(body) > _MAX_HTML_BYTES:
        return "frontend /dashboard response exceeds size limit"
    if not body.strip():
        return "frontend /dashboard returned an empty body"
    return None


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
    if not isinstance(data, dict):
        return None, f"{label} is not a JSON object"
    return data, None


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
    if len(services) < min_services:
        return f"bootstrap lists {len(services)} services, expected >= {min_services}"
    return None


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
            log(f"[{name}] ready on attempt {attempt}/{attempts}: " + ", ".join(phase for phase, _ in checks) + " OK")
            return True
        last = failures
        log(f"[{name}] attempt {attempt}/{attempts} not ready: {'; '.join(failures)}")
        if attempt < attempts:
            sleep(interval)
    log(f"[{name}] FAILED readiness after {attempts} attempts: {'; '.join(last)}")
    return False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bounded frontend + backend (+ admin bootstrap) readiness")
    parser.add_argument("--name", required=True)
    parser.add_argument("--frontend", required=True, help="Frontend origin; /dashboard is probed")
    parser.add_argument("--backend", required=True, help="Backend origin; /api/health and /api/bootstrap")
    parser.add_argument("--attempts", type=int, default=40)
    parser.add_argument("--interval", type=float, default=3)
    parser.add_argument("--timeout", type=float, default=5)
    parser.add_argument("--bootstrap-timeout", type=float, default=20)
    version = parser.add_mutually_exclusive_group()
    version.add_argument("--expect-version")
    version.add_argument("--expect-pyproject-version", action="store_true")
    parser.add_argument("--no-admin", action="store_true", help="Skip the admin bootstrap check")
    parser.add_argument("--min-services", type=int, default=1)
    args = parser.parse_args(argv)
    if (
        args.attempts < 1
        or args.interval < 0
        or args.timeout <= 0
        or args.bootstrap_timeout <= 0
        or args.min_services < 0
    ):
        parser.error(
            "attempts >= 1, interval >= 0, timeout > 0, bootstrap-timeout > 0 and min-services >= 0 are required"
        )
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


# ── Shared E2E Freshness & Watermark Recency Helpers ─────────────────────────────────────────────


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
        "mode": mode,
        "interval_seconds": interval,
        "interval_source": interval_source,
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
        fn = opener.open if opener else urllib.request.urlopen
        with fn(req, timeout=timeout) as r:
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
    hdrs = {
        "Content-Type": "application/json",
        "User-Agent": f"FLA-RUM-Probe/1.0 ({marker})",
        "X-Source-Ip": _spoof_ip(),
    }
    return _urlopen_status(
        urllib.request.Request(url, data=json.dumps(faro_payload).encode("utf-8"), headers=hdrs, method="POST")
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
            "target_assessment": (
                "within_target_upper_bound"
                if complete and count >= floor and p95 <= BUDGETS["peak_freshness_p95_ms"] / 1000
                else "incomplete_not_proven_failure"
            ),
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


def compute_lag_statistics(
    samples: list[dict[str, Any]], baseline_latest_ts: datetime | None = None, min_elapsed_s: float = 6.0
) -> dict[str, Any]:
    """Summarize request-watermark recency, not individual event latency."""
    active_samples = (
        [s for s in samples if s.get("header_latest_ts") and s["header_latest_ts"] > baseline_latest_ts]
        if baseline_latest_ts
        else []
    )
    if not active_samples:
        active_samples = [s for s in samples if s.get("elapsed_s", 0) >= min_elapsed_s] or list(samples)
    head_lags = [s["header_lag_s"] for s in active_samples if s.get("header_lag_s") is not None]
    dash_lags = [s["dashboard_lag_s"] for s in active_samples if s.get("dashboard_lag_s") is not None]

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
    target_p95_s = BUDGETS["peak_freshness_p95_ms"] / 1000
    header_p95, dashboard_p95 = header["p95"], dashboard["p95"]
    target_status = (
        "UNMEASURED"
        if header_p95 is None or dashboard_p95 is None
        else ("ELEVATED" if max(header_p95, dashboard_p95) > target_p95_s else "WITHIN TARGET")
    )
    return {
        "header_lag": header,
        "dashboard_lag": dashboard,
        "sample_count": len(active_samples),
        "total_samples": len(samples),
        "target_p95_s": target_p95_s,
        "target_status": target_status,
    }


def print_active_scorecard(results: list[dict[str, Any]]) -> None:
    print(
        f"\n{'=' * 110}\n                      REQUEST WATERMARK RECENCY SCORECARD (DIAGNOSTIC ONLY)\n{'=' * 110}",
        flush=True,
    )
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
        hl, dl = bench.get("header_lag", {}), bench.get("dashboard_lag", {})
        fmt = lambda d, k: f"{d[k]}s" if d.get(k) is not None else "—"  # noqa: E731
        reqs = f"{bench.get('requests_sent', 0):,}"
        print(
            f"{r['name']:<28} │ {r.get('deployment_mode', '—'):<11} │ {fmt(hl, 'min'):>8} │ {fmt(hl, 'max'):>8} │ "
            f"{fmt(hl, 'avg'):>8} │ {fmt(hl, 'p95'):>8} │ {fmt(dl, 'avg'):>9} │ {reqs:>9} │ {bench.get('target_status', '—'):<15}",
            flush=True,
        )
    print(
        f"Qualification incomplete: request/RUM event latency and clock uncertainty are unverified.\n{'=' * 110}\n",
        flush=True,
    )


def print_burst_scorecard(results: list[dict[str, Any]]) -> None:
    print(
        f"\n{'=' * 96}\n                      END-TO-END FRESHNESS & INGEST LAG SCORECARD (SINGLE BURST)\n{'=' * 96}",
        flush=True,
    )
    print(
        f"{'Environment':<28} │ {'Header Seen':<11} │ {'Cron Ingest':<11} │ {'Dash Seen':<12} │ {'Dashboard Lag':<14} │ {'Header Lag':<11}",
        flush=True,
    )
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
        print(
            f"{r['name']:<28} │ {f_del('header_seconds'):<11} │ {f_del('cron_request_seconds'):<11} │ {f_del('dashboard_bundle_seconds'):<12} │ {dash_lag_str:<14} │ {head_lag_str:<11}",
            flush=True,
        )
    print(f"{'=' * 96}\n", flush=True)


def escape_sql_literal(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"escape_sql_literal expected str, got {type(value).__name__}")
    return value.replace("'", "''")


if __name__ == "__main__":
    sys.exit(main())

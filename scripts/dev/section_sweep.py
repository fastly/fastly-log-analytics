"""Per-section latency sweep over every aggregate endpoint.

Phase 4 benchmarked whole requests on 24h / 30d windows, which hid two things:
which *section* of a bundle is slow, and whether it was served by a rollup or by
the live scan. This sweep calls each aggregate endpoint across windows that can
and cannot be rollup-served, twice per window (cold, then warm), and reads the
``_section_timings`` the server returns. Output is a JSON record plus a ranked
table of live sections over the budget.

Windows: rollup readers need an unfiltered window of at least 48 h and at least
50 % closed-hour coverage, so ``24h`` always runs live, ``3d`` is the smallest
window that can hit a rollup, and ``30d`` only does once a service has enough
history. A section that stays live on a window that should be eligible is the
finding this sweep exists to surface.

Usage (never run from CI; it queries a deployed environment):

    uv run python scripts/dev/section_sweep.py --env remote-standard \
        --service-id "$FASTLY_SERVICE_ID" --output performance-report/section-sweep.json

Authentication reuses the admin-gateway client certificates configured in
``ADMIN_GATEWAY_ENDPOINTS`` (loaded from the persisted deploy env when unset).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from urllib.request import Request

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DEFAULT_WINDOWS = ("24h", "3d", "7d", "30d")
DEFAULT_BUDGET_MS = 500.0
PERSISTED_DEPLOY_ENV = Path("~/.config/fastly-log-analytics/admin/deploy.env").expanduser()

# Sections that only describe the request, not data work.
_OVERHEAD_SECTIONS = frozenset({"get_schema_cols", "build_where_clause", "request_setup", "connection_acquire"})
# Time spent queued rather than computing (pool / heavy-slot wait).
_QUEUE_SECTIONS = frozenset({"connection_acquire"})

SECURITY_SECTIONS = [
    "ngwaf_verified_bots",
    "ngwaf_verified_bots_ts",
    "wellknown_bots",
    "bot_efficiency",
    "tls_fingerprints",
    "fingerprint_coverage",
    "req_size_dist",
    "top_ips_header",
    "ipv6_adoption",
    "proxy_dist",
    "conn_reuse_dist",
    "proxies",
]


def _body(window: str, **extra: Any) -> dict[str, Any]:
    return {"range_token": window, "filters": {}, **extra}


# (label, path, body builder). Bodies carry no ``sections`` unless the page sends a fixed list,
# so the server computes the page's default section set.
ENDPOINTS: tuple[tuple[str, str, Callable[[str], dict[str, Any]]], ...] = (
    ("security", "/api/security/aggregates", lambda w: _body(w, bucket_seconds=3600, sections=SECURITY_SECTIONS)),
    ("origin", "/api/origin/aggregates", lambda w: _body(w)),
    ("performance", "/api/performance/aggregates", lambda w: _body(w)),
    ("network", "/api/network/bundle", lambda w: _body(w, bucket_seconds=3600)),
    ("streaming", "/api/cmcd/aggregates", lambda w: _body(w, bucket_seconds=3600)),
    ("dashboard", "/api/dashboard/bundle", lambda w: _body(w)),
)


def classify_section(name: str) -> str:
    """``rollup`` | ``skipped`` | ``queue`` | ``overhead`` | ``live`` for one ``_section_timings`` name."""
    if name in _QUEUE_SECTIONS:
        return "queue"
    if name in _OVERHEAD_SECTIONS:
        return "overhead"
    if "temp_skipped" in name or name.endswith("_skipped"):
        return "skipped"
    if "rollup" in name or name.endswith("_cache_hit"):
        return "rollup"
    return "live"


def summarize_sections(timings: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Collapse one response's section timings into totals per class plus the raw rows."""
    rows: list[dict[str, Any]] = []
    for t in timings:
        if not isinstance(t, dict) or t.get("section") is None:
            continue
        name = str(t["section"])
        rows.append({"section": name, "ms": float(t.get("time_ms") or 0.0), "kind": classify_section(name)})
    totals = {"rollup": 0.0, "live": 0.0, "queue": 0.0, "skipped": 0.0, "overhead": 0.0}
    counts = {"rollup": 0, "live": 0, "skipped": 0}
    for row in rows:
        totals[row["kind"]] += row["ms"]
        if row["kind"] in counts:
            counts[row["kind"]] += 1
    return {
        "sections": rows,
        "totals_ms": {k: round(v, 1) for k, v in totals.items()},
        "counts": counts,
        "temp_built": any(r["section"] == "temp_table_create" for r in rows),
    }


def find_offenders(records: Iterable[dict[str, Any]], budget_ms: float = DEFAULT_BUDGET_MS) -> list[dict[str, Any]]:
    """Live sections over ``budget_ms`` on the cold run, slowest first.

    A record is ``{"endpoint", "window", "cold": {...summary}, "warm": {...}}``; error records
    (no ``cold`` summary) are skipped.
    """
    offenders: list[dict[str, Any]] = []
    for rec in records:
        cold = rec.get("cold") or {}
        for row in cold.get("sections", []):
            if row["kind"] == "live" and row["ms"] >= budget_ms:
                offenders.append(
                    {"endpoint": rec["endpoint"], "window": rec["window"], "section": row["section"], "ms": row["ms"]}
                )
    return sorted(offenders, key=lambda o: o["ms"], reverse=True)


def render_report(records: list[dict[str, Any]], budget_ms: float = DEFAULT_BUDGET_MS) -> str:
    lines = ["| endpoint | window | cold ms | warm ms | rollup / live / skipped | live ms | queue ms | temp built |"]
    lines.append("|---|---|---:|---:|---|---:|---:|---|")
    for rec in records:
        if "error" in rec:
            lines.append(f"| {rec['endpoint']} | {rec['window']} | error: {rec['error']} | | | | | |")
            continue
        cold = rec["cold"]
        counts = cold["counts"]
        lines.append(
            f"| {rec['endpoint']} | {rec['window']} | {rec['cold_wall_ms']:.0f} | {rec['warm_wall_ms']:.0f} | "
            f"{counts['rollup']} / {counts['live']} / {counts['skipped']} | {cold['totals_ms']['live']:.0f} | "
            f"{cold['totals_ms']['queue']:.0f} | {'yes' if cold['temp_built'] else 'no'} |"
        )
    offenders = find_offenders(records, budget_ms)
    lines.append("")
    lines.append(f"Live sections >= {budget_ms:.0f} ms on the cold run ({len(offenders)}):")
    lines.append("")
    lines.append("| endpoint | window | section | ms |")
    lines.append("|---|---|---|---:|")
    for o in offenders:
        lines.append(f"| {o['endpoint']} | {o['window']} | {o['section']} | {o['ms']:.0f} |")
    return "\n".join(lines)


def _load_persisted_gateway_env() -> None:
    if os.environ.get("ADMIN_GATEWAY_ENDPOINTS") or not PERSISTED_DEPLOY_ENV.is_file():
        return
    for line in PERSISTED_DEPLOY_ENV.read_text().splitlines():
        if line.startswith("ADMIN_GATEWAY_ENDPOINTS="):
            os.environ["ADMIN_GATEWAY_ENDPOINTS"] = line.split("=", 1)[1].strip().strip("'\"")


def _post(
    origin: str, path: str, service_id: str, body: dict[str, Any], timeout: float
) -> tuple[dict[str, Any], float]:
    from scripts.lib.admin_tls import admin_urlopen

    req = Request(
        f"{origin}{path}?service_id={service_id}",
        data=json.dumps(body).encode(),
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    t0 = time.perf_counter()
    with admin_urlopen(req, timeout=timeout) as resp:
        payload = json.loads(resp.read())
    return payload, (time.perf_counter() - t0) * 1000.0


def _timings(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return payload.get("_section_timings") or payload.get("section_timings") or []


def sweep_one(
    origin: str, service_id: str, label: str, path: str, body: dict[str, Any], window: str, timeout: float
) -> dict[str, Any]:
    rec: dict[str, Any] = {"endpoint": label, "path": path, "window": window}
    try:
        cold_payload, cold_ms = _post(origin, path, service_id, body, timeout)
        warm_payload, warm_ms = _post(origin, path, service_id, body, timeout)
    except Exception as exc:  # noqa: BLE001 — record any transport/HTTP failure per cell and keep sweeping
        rec["error"] = f"{type(exc).__name__}: {str(exc)[:160]}"
        return rec
    rec.update(
        cold_wall_ms=round(cold_ms, 1),
        warm_wall_ms=round(warm_ms, 1),
        cold=summarize_sections(_timings(cold_payload)),
        warm=summarize_sections(_timings(warm_payload)),
    )
    return rec


def run_sweep(
    origin: str,
    service_id: str,
    windows: Iterable[str],
    endpoints: Iterable[str] | None,
    timeout: float,
) -> list[dict[str, Any]]:
    wanted = set(endpoints) if endpoints else None
    records: list[dict[str, Any]] = []
    for label, path, build in ENDPOINTS:
        if wanted is not None and label not in wanted:
            continue
        for window in windows:
            print(f"[sweep] {label} {window} ...", file=sys.stderr)
            records.append(sweep_one(origin, service_id, label, path, build(window), window, timeout))
    return records


def run_concurrency_probe(
    origin: str, service_id: str, window: str, parallel: int, timeout: float
) -> list[dict[str, Any]]:
    """Fire every aggregate endpoint at once and report wall time vs queue wait, which is what a
    user sees when pages are opened in quick succession (the pool's heavy-slot queue)."""

    def one(item: tuple[str, str, Callable[[str], dict[str, Any]]]) -> dict[str, Any]:
        label, path, build = item
        try:
            payload, wall = _post(origin, path, service_id, build(window), timeout)
        except Exception as exc:  # noqa: BLE001
            return {"endpoint": label, "error": f"{type(exc).__name__}: {str(exc)[:160]}"}
        summary = summarize_sections(_timings(payload))
        return {
            "endpoint": label,
            "wall_ms": round(wall, 1),
            "queue_ms": summary["totals_ms"]["queue"],
            "compute_ms": round(summary["totals_ms"]["live"] + summary["totals_ms"]["rollup"], 1),
        }

    with ThreadPoolExecutor(max_workers=max(1, parallel)) as pool:
        return list(pool.map(one, ENDPOINTS))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env", required=True, choices=["local-standard", "remote-standard", "remote-high-scale"])
    parser.add_argument("--service-id", default=os.environ.get("FASTLY_SERVICE_ID", ""))
    parser.add_argument("--windows", default=",".join(DEFAULT_WINDOWS))
    parser.add_argument(
        "--endpoints", default="", help="comma-separated subset of: " + ",".join(e[0] for e in ENDPOINTS)
    )
    parser.add_argument("--budget-ms", type=float, default=DEFAULT_BUDGET_MS)
    parser.add_argument("--timeout", type=float, default=180.0)
    parser.add_argument("--parallel", type=int, default=0, help="also fire all endpoints concurrently on --windows[0]")
    parser.add_argument("--output", default="", help="write the full JSON record here")
    args = parser.parse_args(argv)

    if not args.service_id:
        parser.error("--service-id (or FASTLY_SERVICE_ID) is required")
    _load_persisted_gateway_env()
    from scripts.lib.admin_tls import admin_origin

    origin = admin_origin(args.env)
    if not origin:
        print(f"no admin gateway configured for {args.env}", file=sys.stderr)
        return 2

    windows = [w for w in args.windows.split(",") if w]
    endpoints = [e for e in args.endpoints.split(",") if e] or None
    records = run_sweep(origin, args.service_id, windows, endpoints, args.timeout)
    result: dict[str, Any] = {"env": args.env, "windows": windows, "budget_ms": args.budget_ms, "records": records}
    if args.parallel:
        result["concurrency_probe"] = run_concurrency_probe(
            origin, args.service_id, windows[0], args.parallel, args.timeout
        )

    print(render_report(records, args.budget_ms))
    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(result, indent=1) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

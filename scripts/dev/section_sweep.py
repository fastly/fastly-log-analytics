"""Attributed repeated-sample reporting for aggregate endpoints.

Live requests are blocked unless a matching prerequisite-evidence artifact is
supplied. Evidence must be reviewed before use; this gate checks its presence,
not the truth of arbitrary claims in a JSON file. Samples never infer cache state
from request order, infer engine compute from wall time, or certify a page from
an API response. Response-cache, history, coverage and browser gaps stay explicit.
"""

from __future__ import annotations

import argparse
import json
import os
import stat
import sys
import time
from collections import Counter
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request
from uuid import uuid4

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.section_measurements import (
    BUDGETS,
    active_filter_count,
    describe_response,
    distribution,
    rank_sections,
    visible_results_budget_ms,
)

DEFAULT_WINDOWS = ("24h", "3d", "7d", "30d")
DEFAULT_BUDGET_MS = float(BUDGETS["live_section_triage_ms"])
PERSISTED_DEPLOY_ENV = Path("~/.config/fastly-log-analytics/admin/deploy.env").expanduser()
PREREQUISITES = (
    "cache_reporting",
    "controlled_miss",
    "filter_scenarios",
    "history_coverage",
    "query_io_attribution",
    "browser_budgets",
    "analyst_lifecycle",
    "live_traffic",
)

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


ENDPOINTS: tuple[tuple[str, str, Callable[[str], dict[str, Any]]], ...] = (
    ("security", "/api/security/aggregates", lambda w: _body(w, bucket_seconds=3600, sections=SECURITY_SECTIONS)),
    ("origin", "/api/origin/aggregates", lambda w: _body(w)),
    ("performance", "/api/performance/aggregates", lambda w: _body(w)),
    ("network", "/api/network/bundle", lambda w: _body(w, bucket_seconds=3600)),
    ("streaming", "/api/cmcd/aggregates", lambda w: _body(w, bucket_seconds=3600)),
    ("dashboard", "/api/dashboard/bundle", lambda w: _body(w)),
)


def classify_section(name: str) -> str:
    section = summarize_sections([{"section": name}])["sections"][0]
    return section["activity"] if section["activity"] != "unknown" else section["source"]


def summarize_sections(timings: Iterable[dict[str, Any]]) -> dict[str, Any]:
    component = describe_response({"_section_timings": list(timings)})["components"][0]
    return {
        "sections": component["sections"],
        "temp_built": any(row["section"] == "temp_table_create" for row in component["sections"]),
        "gaps": component["gaps"],
    }


def find_offenders(records: Iterable[dict[str, Any]], budget_ms: float = DEFAULT_BUDGET_MS) -> list[dict[str, Any]]:
    return rank_sections((sample for record in records for sample in record.get("samples", [])), budget_ms)


def render_report(records: list[dict[str, Any]], budget_ms: float = DEFAULT_BUDGET_MS) -> str:
    lines = [
        "Discovery evidence only; qualification incomplete. API timing is not visible-results completion.",
        "",
        "| endpoint | window | samples | request p50 ms | request p95 ms | component cache observations | evidence gaps |",
        "|---|---|---:|---:|---:|---|---|",
    ]
    for record in records:
        samples = record.get("samples", [])
        times = [sample["wall_ms"] for sample in samples if sample.get("wall_ms") is not None]
        stats = distribution(times)
        states = Counter(
            component["cache_state"]
            for sample in samples
            for component in sample.get("response", {}).get("components", [])
        )
        gaps = set(record.get("gaps", []))
        for sample in samples:
            gaps.update(sample.get("gaps", []))
        if record.get("error"):
            gaps.add(record["error"])
        lines.append(
            f"| {record['endpoint']} | {record['window']} | {len(samples)} | "
            f"{stats['p50'] if times else 'unknown'} | {stats['p95'] if times else 'unknown'} | "
            f"{dict(states)} | {', '.join(sorted(gaps)) or 'unverified'} |"
        )
    lines.extend(
        [
            "",
            f"Sections at or above {budget_ms:.0f} ms (triage, not acceptance):",
            "",
            "| environment | role | endpoint | window | component | cache | source | section | n | p95 ms | repeated offender |",
            "|---|---|---|---|---|---|---|---|---:|---:|---|",
        ]
    )
    for offender in find_offenders(records, budget_ms):
        stats = offender["distribution_ms"]
        lines.append(
            f"| {offender['environment']} | {offender['role']} | {offender['endpoint']} | {offender['window']} | "
            f"{offender['component']} | {offender['cache_state']} | {offender['source']} | "
            f"{offender['section']} | {stats['count']} | {stats['p95']} | {offender['repeatable']} |"
        )
    return "\n".join(lines)


def prerequisite_gaps(report: Any, environment: str, evidence_root: Path | None = None) -> list[str]:
    if not isinstance(report, dict):
        return ["invalid_prerequisite_report"]
    gaps = []
    if report.get("environment") != environment:
        gaps.append("environment_mismatch")
    checks = report.get("checks")
    checks = checks if isinstance(checks, dict) else {}
    for name in PREREQUISITES:
        check = checks.get(name)
        if not isinstance(check, dict) or check.get("status") != "verified":
            gaps.append(name)
            continue
        evidence = check.get("evidence")
        if not isinstance(evidence, str) or not evidence.strip():
            gaps.append(name)
        elif evidence_root is not None:
            path = evidence_root / evidence
            if not path.is_file() or path.stat().st_size == 0:
                gaps.append(name)
    return gaps


def _load_persisted_gateway_env() -> None:
    if os.environ.get("ADMIN_GATEWAY_ENDPOINTS") or not PERSISTED_DEPLOY_ENV.is_file():
        return
    for line in PERSISTED_DEPLOY_ENV.read_text().splitlines():
        if line.startswith("ADMIN_GATEWAY_ENDPOINTS="):
            os.environ["ADMIN_GATEWAY_ENDPOINTS"] = line.split("=", 1)[1].strip().strip("'\"")


def _post(
    origin: str,
    path: str,
    service_id: str,
    body: dict[str, Any],
    timeout: float,
    *,
    page_load_id: str,
) -> tuple[Any, float]:
    from scripts.lib.admin_tls import admin_urlopen, is_admin_url

    if not is_admin_url(origin):
        raise ValueError("No exact-origin administrator TLS identity configured")
    request = Request(
        f"{origin}{path}?{urlencode({'service_id': service_id})}",
        data=json.dumps(body).encode(),
        method="POST",
        headers={"Content-Type": "application/json", "X-Page-Load-ID": page_load_id, "X-Debug-Responses": "1"},
    )
    started = time.perf_counter()
    with admin_urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read())
    return payload, (time.perf_counter() - started) * 1000.0


def sweep_one(
    origin: str,
    service_id: str,
    label: str,
    path: str,
    body: dict[str, Any],
    window: str,
    timeout: float,
    *,
    sample_count: int = 30,
    environment: str = "unknown",
) -> dict[str, Any]:
    if not 1 <= sample_count <= 1000:
        raise ValueError("sample_count must be between 1 and 1000")
    budget = visible_results_budget_ms(window, body.get("filters", {}))
    record: dict[str, Any] = {
        "endpoint": label,
        "path": path,
        "window": window,
        "payload": deepcopy(body),
        "samples": [],
        "visible_results_budget_ms": budget,
        "whole_request_budget_ms": BUDGETS["whole_request_p95_ms"],
        "qualification": "incomplete",
        "gaps": [
            "history_unverified",
            "reader_coverage_unverified",
            "filters_unverified",
            "query_io_attribution_unverified",
            "page_ready_unmeasured",
            "interaction_completion_unmeasured",
            "controlled_miss_unverified",
            "database_cache_state_unknown",
        ],
    }
    for index in range(sample_count):
        sample_id = uuid4().hex
        sample: dict[str, Any] = {
            "sample_id": sample_id,
            "page_load_id": sample_id,
            "sample_order": index + 1,
            "environment": environment,
            "role": "admin",
            "endpoint": label,
            "window": window,
            "payload": deepcopy(body),
            "response": {"components": [], "gaps": ["response_unavailable"]},
            "wall_ms": None,
        }
        try:
            payload, wall = _post(origin, path, service_id, deepcopy(body), timeout, page_load_id=sample_id)
            sample["wall_ms"] = wall
            sample["response"] = describe_response(payload)
            sample["gaps"] = sample["response"]["gaps"]
        except Exception as exc:  # noqa: BLE001 — retain the failed sample without recording credential-bearing error text
            sample["error"] = type(exc).__name__
            sample["gaps"] = ["request_failed"]
        states = {component["cache_state"] for component in sample["response"]["components"]}
        sample["cache_population"] = next(iter(states)) if len(states) == 1 else "mixed" if states else "unknown"
        record["samples"].append(sample)
    record["request_distribution_ms"] = distribution(
        sample["wall_ms"] for sample in record["samples"] if sample["wall_ms"] is not None
    )
    record["request_distribution_by_cache_state_ms"] = {
        state: distribution(
            sample["wall_ms"]
            for sample in record["samples"]
            if sample["wall_ms"] is not None and sample["cache_population"] == state
        )
        for state in ("hit", "miss", "bypass", "unknown", "mixed")
    }
    return record


def run_sweep(
    origin: str,
    service_id: str,
    windows: Iterable[str],
    endpoints: Iterable[str] | None,
    timeout: float,
    *,
    sample_count: int = 30,
    environment: str = "unknown",
    filters: dict[str, Any] | None = None,
    anchor: str | None = None,
) -> list[dict[str, Any]]:
    wanted = set(endpoints) if endpoints else {item[0] for item in ENDPOINTS}
    unknown = wanted - {item[0] for item in ENDPOINTS}
    if unknown:
        raise ValueError("Unsupported endpoint selection")
    records = []
    for label, path, build in ENDPOINTS:
        if label not in wanted:
            continue
        for window in windows:
            shapes = [{}, filters] if filters else [{}]
            for shape in shapes:
                body = build(window)
                body["filters"] = deepcopy(shape)
                if anchor:
                    body["anchor"] = anchor
                records.append(
                    sweep_one(
                        origin,
                        service_id,
                        label,
                        path,
                        body,
                        window,
                        timeout,
                        sample_count=sample_count,
                        environment=environment,
                    )
                )
    return records


def run_concurrency_probe(
    origin: str, service_id: str, window: str, parallel: int, timeout: float, *, environment: str = "unknown"
) -> list[dict[str, Any]]:
    def one(item: tuple[str, str, Callable[[str], dict[str, Any]]]) -> dict[str, Any]:
        label, path, build = item
        return sweep_one(
            origin, service_id, label, path, build(window), window, timeout, sample_count=1, environment=environment
        )

    with ThreadPoolExecutor(max_workers=max(1, parallel)) as pool:
        return list(pool.map(one, ENDPOINTS))


def write_report(path: Path, report: dict[str, Any]) -> None:
    data = json.dumps(report, indent=2, allow_nan=False) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
            raise OSError("Report output must be an owned regular file")
        os.fchmod(fd, 0o600)
        os.ftruncate(fd, 0)
        with os.fdopen(fd, "w") as output:
            fd = -1
            output.write(data)
    finally:
        if fd >= 0:
            os.close(fd)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", required=True, choices=["local-standard", "remote-standard", "remote-high-scale"])
    parser.add_argument("--service-id", default=os.environ.get("FASTLY_SERVICE_ID", ""))
    parser.add_argument("--windows", default=",".join(DEFAULT_WINDOWS))
    parser.add_argument("--endpoints", default="")
    parser.add_argument("--budget-ms", type=float, default=DEFAULT_BUDGET_MS)
    parser.add_argument("--timeout", type=float, default=180.0)
    parser.add_argument("--parallel", type=int, default=0)
    parser.add_argument("--samples", type=int, default=BUDGETS["minimum_samples_per_state"])
    parser.add_argument("--filters-json", default="{}")
    parser.add_argument("--anchor", default="")
    parser.add_argument("--prerequisites", type=Path)
    parser.add_argument("--output", default="")
    args = parser.parse_args(argv)

    if args.prerequisites is None:
        print("BLOCKED: reviewed prerequisite evidence is required before live requests.", file=sys.stderr)
        return 2
    try:
        evidence = json.loads(args.prerequisites.read_text())
        gaps = prerequisite_gaps(evidence, args.env, args.prerequisites.parent)
    except (OSError, ValueError):
        print("BLOCKED: prerequisite evidence is unreadable or invalid.", file=sys.stderr)
        return 2
    if gaps:
        print("BLOCKED: " + ", ".join(gaps), file=sys.stderr)
        return 2
    if not args.service_id or evidence.get("service_id") != args.service_id:
        parser.error("prerequisite evidence must match the explicit target service")
    windows = [window for window in args.windows.split(",") if window]
    if not windows or any(window not in DEFAULT_WINDOWS for window in windows):
        parser.error("unsupported or empty window selection")
    if not args.anchor:
        parser.error("--anchor is required to keep repeated request bounds equivalent")
    if not 1 <= args.samples <= 1000 or not 0 <= args.parallel <= 5:
        parser.error("--samples must be 1–1000 and --parallel must be 0–5")
    try:
        filters = json.loads(args.filters_json)
    except ValueError:
        parser.error("--filters-json must be a compound-filter object")
    try:
        if active_filter_count(filters) < 2:
            raise ValueError("Compound filters require at least two active dimensions")
    except ValueError:
        parser.error("--filters-json requires at least two supported active filter dimensions")
    _load_persisted_gateway_env()
    from scripts.lib.admin_tls import admin_origin

    origin = admin_origin(args.env)
    if not origin:
        print("BLOCKED: no configured administrator gateway for the selected environment.", file=sys.stderr)
        return 2
    records = run_sweep(
        origin,
        args.service_id,
        windows,
        [item for item in args.endpoints.split(",") if item] or None,
        args.timeout,
        sample_count=args.samples,
        environment=args.env,
        filters=filters,
        anchor=args.anchor,
    )
    result = {
        "env": args.env,
        "windows": windows,
        "budgets": BUDGETS,
        "records": records,
        "offenders": find_offenders(records, args.budget_ms),
        "qualification": "incomplete",
    }
    if args.parallel:
        result["concurrency_probe"] = run_concurrency_probe(
            origin, args.service_id, windows[0], args.parallel, args.timeout, environment=args.env
        )
    print(render_report(records, args.budget_ms))
    if args.output:
        write_report(Path(args.output), result)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

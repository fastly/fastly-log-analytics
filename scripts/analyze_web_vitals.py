#!/usr/bin/env python3
"""Analyze collected Web Vitals samples into a report a dev (or a coding
agent) can act on.

Reads the JSONL sink written by ``POST /api/web-vitals`` when collection
is enabled (``WEB_VITALS_COLLECT=1`` — see backend/core/web_vitals_store.py)
and emits a per-route / per-metric breakdown: sample counts, p50/p75/p95,
and the rating distribution, ranked by how far each route's p75 sits past
Google's Core Web Vitals "good" threshold. The "Focus" section at the top
is the short list of where real users are actually hurting.

Typical loop::

    WEB_VITALS_COLLECT=1   # enable collection, restart backend
    # ... let real traffic accumulate ...
    uv run python scripts/analyze_web_vitals.py                 # print report
    uv run python scripts/analyze_web_vitals.py --out wv.md     # write to file
    uv run python scripts/analyze_web_vitals.py --purge         # report, then delete the data file

Flags:
    --input PATH      data file (default: data/system/web_vitals.jsonl)
    --hours N         only consider samples from the last N hours
    --min-samples N   hide route/metric pairs with fewer than N samples (default 3)
    --cohort C        all | admin | analyst (default all)
    --format F        md | json (default md)
    --out PATH        write the report here instead of stdout
    --purge           delete the input data file after a successful report
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent

# Mirrors backend/core/web_vitals_store.py:LOG_PATH (resolved against the
# repo root so the script works regardless of the caller's CWD).
DEFAULT_INPUT = REPO_ROOT / "data" / "system" / "web_vitals.jsonl"

# Core Web Vitals thresholds: (good_max, poor_min, unit). A value <= good_max
# is "good"; > poor_min is "poor"; in between is "needs-improvement".
# Source: https://web.dev/articles/vitals (LCP/INP/CLS) + FCP/TTFB guidance.
THRESHOLDS: dict[str, tuple[float, float, str]] = {
    "LCP": (2500.0, 4000.0, "ms"),
    "INP": (200.0, 500.0, "ms"),
    "FID": (100.0, 300.0, "ms"),  # deprecated, superseded by INP
    "CLS": (0.1, 0.25, ""),
    "FCP": (1800.0, 3000.0, "ms"),
    "TTFB": (800.0, 1800.0, "ms"),
}


def _percentile(values: list[float], q: float) -> float:
    """Linear-interpolation percentile (q in [0, 1]). Empty -> 0.0."""
    if not values:
        return 0.0
    s = sorted(values)
    if len(s) == 1:
        return s[0]
    pos = (len(s) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (pos - lo)


def _verdict(metric: str, value: float) -> str:
    """Bucket a metric value against its Core Web Vitals threshold."""
    t = THRESHOLDS.get(metric)
    if not t:
        return "unknown"
    good_max, poor_min, _ = t
    if value <= good_max:
        return "good"
    if value <= poor_min:
        return "needs-improvement"
    return "poor"


def _fmt(metric: str, value: float) -> str:
    """Format a metric value: CLS is unitless 3-decimals, others ms ints."""
    unit = THRESHOLDS.get(metric, (0.0, 0.0, ""))[2]
    if unit == "ms":
        return f"{value:.0f}ms"
    return f"{value:.3f}"


def load_samples(path: Path, *, since: datetime | None, cohort: str) -> list[dict[str, Any]]:
    """Parse the JSONL file, filtering by time window and cohort."""
    out: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue  # tolerate a torn final line
            if cohort != "all" and rec.get("cohort") != cohort:
                continue
            if since is not None:
                ts = rec.get("ts")
                if not ts:
                    continue
                try:
                    when = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                except ValueError:
                    continue
                if when < since:
                    continue
            out.append(rec)
    return out


def aggregate(samples: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    """Group samples by (pathname, metric) -> stats dict."""
    buckets: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for rec in samples:
        name = rec.get("name")
        if name not in THRESHOLDS:
            continue
        route = rec.get("pathname") or "(unknown)"
        buckets.setdefault((route, name), []).append(rec)

    stats: dict[tuple[str, str], dict[str, Any]] = {}
    for key, recs in buckets.items():
        values = [float(r["value"]) for r in recs if r.get("value") is not None]
        if not values:
            continue
        ratings = {"good": 0, "needs-improvement": 0, "poor": 0}
        for r in recs:
            rt = r.get("rating")
            if rt in ratings:
                ratings[rt] += 1
        p75 = _percentile(values, 0.75)
        stats[key] = {
            "count": len(values),
            "p50": _percentile(values, 0.50),
            "p75": p75,
            "p95": _percentile(values, 0.95),
            "ratings": ratings,
            "verdict": _verdict(key[1], p75),
        }
    return stats


def _severity(metric: str, p75: float, count: int) -> float:
    """Rank key: how far past the 'good' threshold p75 sits, log-weighted by
    sample count so a heavily-hit bad route outranks a rarely-hit one."""
    good_max = THRESHOLDS.get(metric, (1.0, 0.0, ""))[0] or 1.0
    overshoot = max(0.0, (p75 / good_max) - 1.0)
    # +1 so a single sample still contributes; volume nudges, doesn't dominate.
    return overshoot * (1.0 + (count**0.5))


def build_report_data(stats: dict[tuple[str, str], dict[str, Any]], min_samples: int) -> dict[str, Any]:
    rows = [
        {
            "route": route,
            "metric": metric,
            **s,
            "severity": _severity(metric, s["p75"], s["count"]),
        }
        for (route, metric), s in stats.items()
        if s["count"] >= min_samples
    ]
    focus = sorted(
        (r for r in rows if r["verdict"] in ("needs-improvement", "poor")),
        key=lambda r: r["severity"],
        reverse=True,
    )
    return {"rows": rows, "focus": focus}


def render_markdown(
    *,
    samples: list[dict[str, Any]],
    report: dict[str, Any],
    source: Path,
    window_label: str,
    min_samples: int,
) -> str:
    total = len(samples)
    cohorts: dict[str, int] = {}
    for r in samples:
        cohorts[r.get("cohort", "?")] = cohorts.get(r.get("cohort", "?"), 0) + 1
    cohort_str = ", ".join(f"{k}={v}" for k, v in sorted(cohorts.items())) or "none"

    lines: list[str] = []
    lines.append("# Web Vitals Analysis")
    lines.append("")
    lines.append(f"- Generated: {datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')}")
    lines.append(f"- Source: `{source}`")
    lines.append(f"- Window: {window_label}")
    lines.append(f"- Samples: {total} ({cohort_str})")
    lines.append(f"- Min samples per row: {min_samples}")
    lines.append("")

    focus = report["focus"]
    lines.append("## Focus — routes where real users are hurting")
    lines.append("")
    if not focus:
        lines.append("_No route/metric pair is past its 'good' threshold. 🎉_")
    else:
        lines.append("Ranked by how far p75 sits past the Core Web Vitals 'good' bar, weighted by volume.")
        lines.append("")
        for r in focus:
            metric = r["metric"]
            good_max = THRESHOLDS[metric][0]
            lines.append(
                f"- **{r['route']}** · {metric} **{r['verdict'].upper()}** — "
                f"p75 {_fmt(metric, r['p75'])} (good ≤ {_fmt(metric, good_max)}), "
                f"p95 {_fmt(metric, r['p95'])}, n={r['count']}"
            )
    lines.append("")

    lines.append("## All route × metric breakdown")
    lines.append("")
    lines.append("| Route | Metric | n | p50 | p75 | p95 | good% | ni% | poor% | verdict |")
    lines.append("|---|---|--:|--:|--:|--:|--:|--:|--:|---|")
    for r in sorted(report["rows"], key=lambda x: (x["route"], x["metric"])):
        metric = r["metric"]
        n = r["count"]
        rt = r["ratings"]

        def _pct(part: int) -> str:
            return f"{(100.0 * part / n):.0f}%" if n else "0%"

        lines.append(
            f"| {r['route']} | {metric} | {n} | {_fmt(metric, r['p50'])} | "
            f"{_fmt(metric, r['p75'])} | {_fmt(metric, r['p95'])} | "
            f"{_pct(rt['good'])} | {_pct(rt['needs-improvement'])} | {_pct(rt['poor'])} | "
            f"{r['verdict']} |"
        )
    lines.append("")
    return "\n".join(lines)


def render_json(*, samples: list[dict[str, Any]], report: dict[str, Any], source: Path, window_label: str) -> str:
    payload = {
        "generated": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": str(source),
        "window": window_label,
        "total_samples": len(samples),
        "focus": [
            {"route": r["route"], "metric": r["metric"], "verdict": r["verdict"], "p75": r["p75"], "count": r["count"]}
            for r in report["focus"]
        ],
        "rows": report["rows"],
    }
    return json.dumps(payload, indent=2, sort_keys=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Analyze collected Web Vitals samples")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="JSONL data file")
    parser.add_argument("--hours", type=float, default=None, help="only samples from the last N hours")
    parser.add_argument("--min-samples", type=int, default=3, help="hide rows with fewer samples")
    parser.add_argument("--cohort", choices=["all", "admin", "analyst"], default="all")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    parser.add_argument("--out", type=Path, default=None, help="write report to this file (default stdout)")
    parser.add_argument("--purge", action="store_true", help="delete the data file(s) after a successful report")
    args = parser.parse_args(argv)

    path: Path = args.input
    # The store rotates the active file to a single ".1" backup at its size
    # cap (backend/core/web_vitals_store.py:rotated_path); include it so the
    # report + purge span the full retained window. Oldest segment first.
    rotated = path.with_name(path.name + ".1")
    segments = [p for p in (rotated, path) if p.exists()]
    if not segments:
        print(
            f"No web-vitals data at {path}.\n"
            "Enable collection (WEB_VITALS_COLLECT=1, restart the backend), let some\n"
            "real traffic accumulate, then re-run this script.",
            file=sys.stderr,
        )
        return 0

    since = None
    window_label = "all time"
    if args.hours is not None:
        since = datetime.now(UTC) - timedelta(hours=args.hours)
        window_label = f"last {args.hours:g}h"

    samples: list[dict[str, Any]] = []
    for seg in segments:
        samples.extend(load_samples(seg, since=since, cohort=args.cohort))
    if not samples:
        print(f"No matching samples in {path} (window={window_label}, cohort={args.cohort}).", file=sys.stderr)
        return 0

    stats = aggregate(samples)
    report = build_report_data(stats, args.min_samples)

    if args.format == "json":
        text = render_json(samples=samples, report=report, source=path, window_label=window_label)
    else:
        text = render_markdown(
            samples=samples, report=report, source=path, window_label=window_label, min_samples=args.min_samples
        )

    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text + "\n", encoding="utf-8")
        print(f"Wrote report to {args.out}", file=sys.stderr)
    else:
        print(text)

    # --purge runs only after the report is safely emitted, so an error
    # above never costs you the data. Removes every segment (active + any
    # rotated .1 backup).
    if args.purge:
        removed: list[str] = []
        for seg in segments:
            try:
                seg.unlink()
                removed.append(str(seg))
            except OSError as exc:
                print(f"Failed to purge {seg}: {exc}", file=sys.stderr)
                return 1
        print(f"Purged: {', '.join(removed)}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())

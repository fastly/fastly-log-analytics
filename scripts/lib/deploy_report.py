"""Shared 5-run deployment report rotation and summary helpers (`reports/deploys/<ts>/`)."""

from __future__ import annotations

import json
import math
import re
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

DEPLOY_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}$")

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


def rotate_and_prune_deploy_dirs(
    deploys_root: Path,
    run_ts: str,
    *,
    keep_prior: int = 5,
) -> list[Path]:
    """Update `reports/deploys/current` symlink and prune timestamped deploy dirs beyond `keep_prior`."""
    current_link = deploys_root / "current"
    if current_link.is_symlink() or current_link.exists():
        current_link.unlink()
    current_link.symlink_to(run_ts)

    ts_dirs = sorted(
        (d for d in deploys_root.iterdir() if d.is_dir() and not d.is_symlink() and DEPLOY_TIMESTAMP_RE.match(d.name)),
        key=lambda p: p.name,
        reverse=True,
    )
    max_total = max(1, keep_prior + 1)
    for stale_dir in ts_dirs[max_total:]:
        shutil.rmtree(stale_dir, ignore_errors=True)
    return ts_dirs[:max_total]


def load_prior_deploy_reports(
    deploys_root: Path,
    current_run_id: str,
    summarize_fn: Callable[[dict[str, Any], str], dict[str, Any]],
    *,
    limit: int = 5,
) -> list[dict[str, Any]]:
    """Load and summarize up to `limit` prior deploy reports from `reports/deploys/`."""
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
            priors.append(summarize_fn(data, folder.name))
        except (OSError, ValueError, KeyError):
            continue
        if len(priors) >= limit:
            break
    return priors

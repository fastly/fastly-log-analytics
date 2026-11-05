"""Shared 5-run deployment report rotation and summary helpers (`reports/deploys/<ts>/`)."""

from __future__ import annotations

import json
import re
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

DEPLOY_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}$")


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

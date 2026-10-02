"""Guards the false-OOM protections in ``clickhouse/config.d/resource_limits.xml``.

Under concurrent dashboard-bundle bursts (the verify phase fires N bundles, each
fanning out per-dimension subqueries), ClickHouse's global memory tracker
over-counts unreleased jemalloc arenas and falsely trips MEMORY_LIMIT_EXCEEDED
(Code 241) — measured at 6.01 GiB *tracked* while real RSS stayed <2 GiB on a
<50 MiB dataset. The backend maps that to a 400 and the dashboard shows
"Failed to fetch". Reproduction before the fix: 12 concurrent 30d bundles →
11/12 failed; after: 60/60 succeeded.

Two settings prevent the drift from reaching the cap:

  * ``memory_worker_correct_memory_tracker`` + a non-zero ``memory_worker_period_ms``
    periodically reconcile the global tracker against real cgroup RSS, so the
    arena over-count is corrected at its source rather than accumulating; and
  * a ``max_server_memory_usage_to_ram_ratio`` with genuine headroom above the
    tiny real working set (the cgroup OOM watcher at 0.95 remains the real guard).

A regression that drops the memory-worker reconciliation or lowers the ratio back
toward the phantom-tripping 0.75 reintroduces the concurrency false-OOM, so it
must fail here instead of only as a flaky high-scale deploy.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

_CONFIG = Path(__file__).resolve().parents[2] / "clickhouse" / "config.d" / "resource_limits.xml"


def _root() -> ET.Element:
    return ET.parse(_CONFIG).getroot()


def test_memory_tracker_is_reconciled_against_real_rss() -> None:
    root = _root()
    correct = root.findtext("memory_worker_correct_memory_tracker")
    period = root.findtext("memory_worker_period_ms")
    assert correct == "1", (
        "memory_worker_correct_memory_tracker must be enabled so the jemalloc-arena "
        "over-count is reconciled to real RSS and cannot false-trip MEMORY_LIMIT_EXCEEDED"
    )
    assert period is not None and int(period) > 0, (
        "memory_worker_period_ms must be non-zero for the reconciliation to run periodically"
    )


def test_server_memory_ratio_has_headroom_over_real_usage() -> None:
    root = _root()
    ratio = root.findtext("max_server_memory_usage_to_ram_ratio")
    assert ratio is not None, "max_server_memory_usage_to_ram_ratio must be set"
    assert float(ratio) >= 0.85, (
        f"max_server_memory_usage_to_ram_ratio={ratio} is too low; 0.75 phantom-tripped "
        "at ~1.5 GiB real RSS on an 8 GiB box with a <50 MiB dataset. Keep headroom "
        "(cgroup OOM watcher at 0.95 is the real backstop)."
    )

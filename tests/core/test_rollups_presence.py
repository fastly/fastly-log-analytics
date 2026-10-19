"""Regression: the dashboard rollup-eligibility probe must recognise rollups
that live only in the bundled/day tiers, not just the per-field ``hour`` tier.

Day compaction deletes the per-field ``rollups/hour`` tree once a day closes,
leaving the fully-usable ``day_bundled`` / ``hour_bundled`` / ``day`` tiers
behind. The old probe keyed solely on ``os.path.isdir(rollups/hour)`` and so
reported "no rollups" for any service whose hours had all been compacted —
flipping every unfiltered dashboard request onto the slow wide-temp base-table
scan (prod: 30d/24h /api/dashboard/bundle hung >120s → pool saturation →
"Crunching logs..." timeout), even though the reader could serve sub-second
from the bundle tiers.
"""

from __future__ import annotations

import os

from backend.core.rollups import rollups_present


def _src(tmp_path) -> dict:
    return {"name": "svc", "bucket": "b", "_cache_dir_override": str(tmp_path)}


def _touch_parquet(root: str, partition: str, name: str = "all_fields.parquet") -> None:
    d = os.path.join(root, partition)
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, name), "wb") as f:
        f.write(b"PAR1")


def test_absent_rollups_tree_is_not_present(tmp_path):
    assert rollups_present(_src(tmp_path)) is False


def test_empty_rollups_root_is_not_present(tmp_path):
    os.makedirs(os.path.join(tmp_path, "rollups"), exist_ok=True)
    assert rollups_present(_src(tmp_path)) is False


def test_day_bundled_only_is_present(tmp_path):
    # The regression shape: per-field hour tier compacted away, day bundles remain.
    _touch_parquet(os.path.join(tmp_path, "rollups", "day_bundled"), "day=2026-10-01")
    assert not os.path.isdir(os.path.join(tmp_path, "rollups", "hour"))
    assert rollups_present(_src(tmp_path)) is True


def test_hour_bundled_only_is_present(tmp_path):
    _touch_parquet(os.path.join(tmp_path, "rollups", "hour_bundled"), "hour=2026-10-01-15")
    assert rollups_present(_src(tmp_path)) is True


def test_per_field_hour_tier_still_counts(tmp_path):
    _touch_parquet(os.path.join(tmp_path, "rollups", "hour", "field=ip"), "hour=2026-10-01-15")
    assert rollups_present(_src(tmp_path)) is True


def test_empty_partition_dirs_without_parquet_are_not_present(tmp_path):
    # Defensive: a partition dir left behind with no parquet must not count.
    os.makedirs(os.path.join(tmp_path, "rollups", "day_bundled", "day=2026-10-01"), exist_ok=True)
    assert rollups_present(_src(tmp_path)) is False

"""The day-compactor and admin-backfill registries must cover what the package ships."""

from __future__ import annotations

import re

from backend.core import rollups
from backend.core.rollups import day_bundles
from backend.core.rollups.spec import ADMIN_BACKFILLS, DAY_COMPACTORS


def test_day_compactors_resolve_to_callables_with_unique_labels() -> None:
    assert len({s.label for s in DAY_COMPACTORS}) == len(DAY_COMPACTORS)
    assert len({s.compactor for s in DAY_COMPACTORS}) == len(DAY_COMPACTORS)
    assert all(callable(getattr(rollups, s.compactor)) for s in DAY_COMPACTORS)


def test_every_per_rollup_day_compactor_is_registered() -> None:
    shipped = {
        name
        for name in vars(day_bundles)
        if re.fullmatch(r"compact_\w+_closed_days_to_daily", name) and name != "compact_closed_days_to_daily"
    }
    assert shipped == {s.compactor for s in DAY_COMPACTORS}


def test_admin_backfills_resolve_to_callables_with_unique_keys() -> None:
    assert len({k for k, _ in ADMIN_BACKFILLS}) == len(ADMIN_BACKFILLS)
    assert all(callable(getattr(rollups, fn)) for _, fn in ADMIN_BACKFILLS)


def test_admin_response_keys_do_not_collide() -> None:
    day_keys = [s.admin_key for s in DAY_COMPACTORS if s.admin_key]
    backfill_keys = [k for k, _ in ADMIN_BACKFILLS]
    assert len(set(day_keys) | set(backfill_keys)) == len(day_keys) + len(backfill_keys)

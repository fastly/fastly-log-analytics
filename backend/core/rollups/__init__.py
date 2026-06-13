"""Hourly Top-N + bundled rollups for the dashboard.

Carved out of a single 2,045-line ``rollups.py`` for 10.9 file-size
sweep. Submodules:

- :mod:`._common` — constants, validators, paths, query builders, atomic
  marker IO. Imported by every other sub-module.
- :mod:`.time_series` — per-hour 1-minute time_series.parquet bundles.
- :mod:`.sessions` — per-hour per-(ip, ja4) sessions.parquet bundles.
- :mod:`.hour_bundles` — combine per-field hour parquets into
  ``hour_bundled/hour=H/all_fields.parquet`` + retention sweep + backfill.
- :mod:`.day_bundles` — combine per-field day parquets into
  ``day_bundled/day=D/all_fields.parquet`` + closed-day compactor.
- :mod:`.recompute` — per-tick recompute + one-shot backfill + cleanup +
  the shared per-field COPY driver.
- :mod:`.wellknown_bots` — pre-materialised bot-prefiltered rollup +
  read path.

External surface (preserved verbatim from the pre-split file — every
symbol on the right-hand side of an existing
``from backend.core.rollups import ...`` keeps working):

  Public writers:
    build_time_series_bundles, backfill_time_series_bundles
    build_session_bundles, backfill_session_bundles
    bundle_hours, backfill_hour_bundles
    bundle_days, backfill_day_bundles, compact_closed_days_to_daily
    recompute_touched_hours, backfill_rollups, ensure_field_backfills
    cleanup_old_rollups
    recompute_wellknown_bots_rollup, read_wellknown_bots_rollup

  Test-touched / cross-package internals:
    _is_safe_ident, _safe_table_for, _VIRTUAL_FIELD_BACKING
    _rollups_root, _day_rollups_root, _hour_bundled_root, _day_bundled_root
    _markers_path, _load_markers, _save_markers
    _build_copy_query, _build_virtual_field_copy_query
    _publish_field_partitions, _get_fields
    TOP_K, DAY_BUNDLE_FILENAME, DAY_BUNDLE_TOP_K,
    TIME_SERIES_BUNDLE_FILENAME, SESSIONS_BUNDLE_FILENAME
    _time_series_bundle_path, _sessions_bundle_path
    _wellknown_bots_root, _parse_iso_to_hour, _run_per_field_copy
    _cleanup_per_field_after_bundle
"""

from __future__ import annotations

# Re-exports — pull every public symbol up so callers continue to use
# the flat ``backend.core.rollups.X`` path. Order doesn't matter; no
# side-effect imports here.
from ._common import (
    _SAFE_IDENT_RE,
    _VIRTUAL_FIELD_BACKING,
    DAY_BUNDLE_FILENAME,
    DAY_BUNDLE_TOP_K,
    SESSIONS_BUNDLE_FILENAME,
    TIME_SERIES_BUNDLE_FILENAME,
    TOP_K,
    _build_copy_query,
    _build_virtual_field_copy_query,
    _day_bundled_root,
    _day_rollups_root,
    _get_fields,
    _hour_bundled_root,
    _is_safe_ident,
    _load_markers,
    _markers_path,
    _publish_field_partitions,
    _rollups_root,
    _safe_table_for,
    _save_markers,
    _sessions_bundle_path,
    _time_series_bundle_path,
)
from .day_bundles import (
    backfill_day_bundles,
    bundle_days,
    compact_closed_days_to_daily,
)
from .hour_bundles import (
    _cleanup_per_field_after_bundle,
    backfill_hour_bundles,
    bundle_hours,
)
from .recompute import (
    _run_per_field_copy,
    backfill_rollups,
    cleanup_old_rollups,
    ensure_field_backfills,
    recompute_touched_hours,
)
from .sessions import backfill_session_bundles, build_session_bundles
from .time_series import backfill_time_series_bundles, build_time_series_bundles
from .wellknown_bots import (
    _parse_iso_to_hour,
    _wellknown_bots_root,
    read_wellknown_bots_rollup,
    recompute_wellknown_bots_rollup,
)

__all__ = [
    # Public writers
    "build_time_series_bundles",
    "backfill_time_series_bundles",
    "build_session_bundles",
    "backfill_session_bundles",
    "bundle_hours",
    "backfill_hour_bundles",
    "bundle_days",
    "backfill_day_bundles",
    "compact_closed_days_to_daily",
    "recompute_touched_hours",
    "backfill_rollups",
    "ensure_field_backfills",
    "cleanup_old_rollups",
    "recompute_wellknown_bots_rollup",
    "read_wellknown_bots_rollup",
    # Module-level constants
    "TOP_K",
    "DAY_BUNDLE_FILENAME",
    "DAY_BUNDLE_TOP_K",
    "TIME_SERIES_BUNDLE_FILENAME",
    "SESSIONS_BUNDLE_FILENAME",
    # Cross-package + test-touched internals
    "_is_safe_ident",
    "_safe_table_for",
    "_VIRTUAL_FIELD_BACKING",
    "_SAFE_IDENT_RE",
    "_rollups_root",
    "_day_rollups_root",
    "_hour_bundled_root",
    "_day_bundled_root",
    "_markers_path",
    "_load_markers",
    "_save_markers",
    "_build_copy_query",
    "_build_virtual_field_copy_query",
    "_publish_field_partitions",
    "_get_fields",
    "_time_series_bundle_path",
    "_sessions_bundle_path",
    "_wellknown_bots_root",
    "_parse_iso_to_hour",
    "_run_per_field_copy",
    "_cleanup_per_field_after_bundle",
]

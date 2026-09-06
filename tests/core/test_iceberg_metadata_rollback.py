"""Regression coverage for the 2026-08 Iceberg metadata-rollback incident.

The SE-demo service silently lost 41 days of data (2026-07-01 → 2026-08-10)
while ingest kept reporting success. Mechanism:

  1. ``_read_metadata_pointer`` reads ``metadata_location.txt``. Both candidate
     keys are wrapped in ``except Exception: continue``, so ONE transient CDN
     5xx/timeout drops it into the ``metadata/`` discovery fallback.
  2. That fallback called ``list_objects_v2`` WITHOUT pagination. A single
     response caps at 1000 keys, and pyiceberg zero-pads the version prefix,
     so the first page holds the OLDEST metadata. On the affected service the
     page ended at ``00952-….metadata.json`` while ``08999`` was current.
  3. ``sorted(metadata_files)[-1]`` therefore resolved v952. The table
     committed forward from that stale base (reaching v1247), so every data
     file referenced only by v953…v8999 became unreachable.

The data was never deleted — only dereferenced. These tests pin the three
independent defenses: paginate, order by parsed version, and refuse to
resolve backwards.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from backend.core.iceberg._core import (
    _list_metadata_json_keys,
    _newest_metadata_key,
    metadata_version,
)

BUCKET = "b"
PREFIX = "iceberg/default/logs/metadata/"


def _key(version: int) -> str:
    return f"{PREFIX}{version:05d}-abcd1234-0000-0000-0000-00000000{version:04d}.metadata.json"


def _paginator_s3(all_keys: list[str], page_size: int = 1000):
    """boto3-shaped mock: get_paginator yields pages; list_objects_v2 truncates.

    Mirrors the real contract — the unpaginated call only ever sees page one.
    """
    s3 = MagicMock()
    pages = [
        {"Contents": [{"Key": k} for k in all_keys[i : i + page_size]]} for i in range(0, len(all_keys), page_size)
    ] or [{}]

    paginator = MagicMock()
    paginator.paginate.return_value = pages
    s3.get_paginator.return_value = paginator
    s3.list_objects_v2.return_value = pages[0]
    return s3


# ── metadata_version ─────────────────────────────────────────────────────────


def test_metadata_version_parses_padded_prefix():
    assert metadata_version(_key(952)) == 952
    assert metadata_version(_key(8999)) == 8999
    assert metadata_version("s3://b/iceberg/default/logs/metadata/01247-be8ac46a.metadata.json") == 1247


def test_metadata_version_returns_negative_for_unparseable():
    assert metadata_version("metadata/v2.metadata.json") == -1
    assert metadata_version("") == -1


# ── pagination ───────────────────────────────────────────────────────────────


def test_list_metadata_json_keys_paginates_past_1000():
    """THE ROOT CAUSE. 9,314 objects must all be seen, not just the first page."""
    all_keys = [_key(v) for v in range(9314)]
    s3 = _paginator_s3(all_keys)

    found = _list_metadata_json_keys(s3, BUCKET, PREFIX)

    assert len(found) == 9314, "listing was truncated — this is the 2026-08 rollback bug"
    s3.get_paginator.assert_called_once_with("list_objects_v2")


def test_list_metadata_json_keys_filters_manifests():
    """``metadata/`` also holds .avro manifests; only metadata.json counts."""
    keys = [_key(1), f"{PREFIX}abc-m0.avro", f"{PREFIX}snap-123-1-def.avro", _key(2)]
    found = _list_metadata_json_keys(_paginator_s3(keys), BUCKET, PREFIX)
    assert sorted(found) == sorted([_key(1), _key(2)])


def test_newest_key_from_full_listing_beats_truncated_page():
    """Head-to-head: the paginated+version-ordered path picks 8999 where the
    old truncated path picked 952. Reproduces the exact observed numbers."""
    all_keys = [_key(v) for v in range(9000)]
    s3 = _paginator_s3(all_keys)

    # Old behaviour, preserved here as the thing we must never do again.
    truncated = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)["Contents"]]
    old_pick = sorted(truncated)[-1]
    assert metadata_version(old_pick) == 999, "sanity: truncated page tops out early"

    new_pick = _newest_metadata_key(_list_metadata_json_keys(s3, BUCKET, PREFIX))
    assert metadata_version(new_pick) == 8999
    assert metadata_version(new_pick) > metadata_version(old_pick)


# ── version ordering, not string ordering ────────────────────────────────────


def test_newest_metadata_key_orders_by_version_not_lexicographically():
    """Unpadded/mixed-width names must still order numerically."""
    keys = [
        f"{PREFIX}9-aaa.metadata.json",
        f"{PREFIX}10000-bbb.metadata.json",
        f"{PREFIX}872-ccc.metadata.json",
    ]
    assert metadata_version(_newest_metadata_key(keys)) == 10000
    # A naive string sort would have picked "9-aaa" here.
    assert sorted(keys)[-1].endswith("9-aaa.metadata.json")


def test_newest_metadata_key_empty_is_none():
    assert _newest_metadata_key([]) is None


def test_newest_metadata_key_is_deterministic_on_version_tie():
    a = f"{PREFIX}00007-aaa.metadata.json"
    b = f"{PREFIX}00007-bbb.metadata.json"
    assert _newest_metadata_key([a, b]) == _newest_metadata_key([b, a]) == b


# ── monotonicity guard ───────────────────────────────────────────────────────


def _src(**kw):
    base = {"name": "svc-rollback", "bucket": BUCKET, "prefix": ""}
    base.update(kw)
    return base


def _clear_pointer_cache():
    import backend.core.iceberg._core as core

    with core._pointer_cache_lock:
        core._pointer_cache.clear()


def test_pointer_read_refuses_to_resolve_backwards():
    """THE SAFETY NET. Even if discovery yields an older version, the known-good
    location wins. This alone would have prevented the incident."""
    import backend.core.iceberg._core as core

    _clear_pointer_cache()
    known = f"s3://{BUCKET}/{_key(8999)}"
    stale = [_key(v) for v in range(953)]  # discovery can only see up to 952
    s3 = _paginator_s3(stale)
    s3.get_object.side_effect = RuntimeError("CDN 502")  # force the fallback

    with patch("backend.core.duckdb._get_fos_client", return_value=s3):
        out = core._read_metadata_pointer(_src(iceberg_metadata_location=known), ("default", "logs"))

    assert out == known, "resolved a rollback instead of keeping the known-good pointer"
    _clear_pointer_cache()


def test_pointer_read_still_advances_forwards():
    """The guard must not freeze a legitimately-advancing pointer."""
    import backend.core.iceberg._core as core

    _clear_pointer_cache()
    known = f"s3://{BUCKET}/{_key(100)}"
    newer_keys = [_key(v) for v in range(200)]
    s3 = _paginator_s3(newer_keys)
    s3.get_object.side_effect = RuntimeError("CDN 502")

    with patch("backend.core.duckdb._get_fos_client", return_value=s3):
        out = core._read_metadata_pointer(_src(iceberg_metadata_location=known), ("default", "logs"))

    assert metadata_version(out) == 199, "guard wrongly blocked a forward move"
    _clear_pointer_cache()


def test_pointer_object_wins_over_discovery():
    """When the pointer object reads cleanly, discovery must not run at all."""
    import backend.core.iceberg._core as core

    _clear_pointer_cache()
    pointed = f"s3://{BUCKET}/{_key(1247)}"
    s3 = _paginator_s3([_key(v) for v in range(50)])
    s3.get_object.return_value = {"Body": MagicMock(read=lambda: pointed.encode())}

    with patch("backend.core.duckdb._get_fos_client", return_value=s3):
        out = core._read_metadata_pointer(_src(), ("default", "logs"))

    assert out == pointed
    s3.get_paginator.assert_not_called()
    _clear_pointer_cache()


# ── local SQLite catalog must not be rolled back either ──────────────────────


def _seed_catalog(db_path, loc):
    import sqlite3

    with sqlite3.connect(db_path) as con:
        con.execute(
            "CREATE TABLE iceberg_tables (table_namespace TEXT, table_name TEXT, "
            "metadata_location TEXT, previous_metadata_location TEXT)"
        )
        con.execute(
            "INSERT INTO iceberg_tables VALUES (?, ?, ?, NULL)",
            ("default", "logs", loc),
        )


def _catalog_loc(db_path):
    import sqlite3

    with sqlite3.connect(db_path) as con:
        return con.execute("SELECT metadata_location FROM iceberg_tables").fetchone()[0]


def test_refresh_local_catalog_refuses_backwards_write(tmp_path):
    """A stale resolution must not be persisted into the local catalog —
    that is what made the rollback sticky across restarts."""
    import backend.core.iceberg._core as core

    db = tmp_path / "cat.db"
    current = f"s3://{BUCKET}/{_key(8999)}"
    _seed_catalog(str(db), current)

    with (
        patch.object(core, "_catalog_db_path", return_value=str(db)),
        patch.object(core, "_read_metadata_pointer", return_value=f"s3://{BUCKET}/{_key(952)}"),
    ):
        changed = core._refresh_local_catalog_metadata(MagicMock(), _src(), ("default", "logs"))

    assert changed is False
    assert _catalog_loc(str(db)) == current, "local catalog was rolled back to an older metadata version"


def test_refresh_local_catalog_still_advances(tmp_path):
    """Forward moves must still be written — analysts rely on this to see
    snapshots the admin host committed."""
    import backend.core.iceberg._core as core

    db = tmp_path / "cat.db"
    _seed_catalog(str(db), f"s3://{BUCKET}/{_key(100)}")
    newer = f"s3://{BUCKET}/{_key(8999)}"

    with (
        patch.object(core, "_catalog_db_path", return_value=str(db)),
        patch.object(core, "_read_metadata_pointer", return_value=newer),
    ):
        changed = core._refresh_local_catalog_metadata(MagicMock(), _src(), ("default", "logs"))

    assert changed is True
    assert _catalog_loc(str(db)) == newer

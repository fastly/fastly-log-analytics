"""Characterization of TTL-dict caches migrated to BoundedTTLCache (B22).

Pins hit/miss/invalidate behavior (not wall-clock expiry) so the storage swap
stays behavior-neutral. Each cache is also verified to drain through
``CacheRegistry`` (the conftest isolation path).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import duckdb
import pytest

from backend.utils.cache_registry import CacheRegistry


@pytest.fixture(autouse=True)
def _drain_caches():
    CacheRegistry.clear_all()
    yield
    CacheRegistry.clear_all()


def _describe_con(columns: list[str]) -> MagicMock:
    con = MagicMock()
    con.execute.return_value.fetchall.return_value = [(c,) for c in columns]
    return con


def test_pii_cols_cached_per_table_and_drained_by_registry():
    from backend.repositories import query

    con = _describe_con(["ip", "url", "status"])
    first = query._pii_redact_cols(con, "tbl_pii_a")
    second = query._pii_redact_cols(con, "tbl_pii_a")
    assert first == second
    assert "ip" in first and "url" not in first
    assert con.execute.call_count == 1

    query._pii_redact_cols(con, "tbl_pii_b")
    assert con.execute.call_count == 2

    CacheRegistry.clear("query._PII_COLS_CACHE")
    query._pii_redact_cols(con, "tbl_pii_a")
    assert con.execute.call_count == 3


def test_pii_cols_describe_failure_is_empty_and_not_cached():
    from backend.repositories import query

    con = MagicMock()
    con.execute.side_effect = duckdb.CatalogException("stale")
    assert query._pii_redact_cols(con, "tbl_pii_c") == frozenset()
    good = _describe_con(["ip"])
    assert "ip" in query._pii_redact_cols(good, "tbl_pii_c")


def test_compaction_stats_memoised_and_drained(tmp_path):
    from backend.core import local_compaction

    root = tmp_path / "cache"
    part = root / "data" / "timestamp_hour=2026-10-19-00"
    part.mkdir(parents=True)
    (part / "a.parquet").write_bytes(b"x")
    with patch("backend.core.duckdb._cache_dir", return_value=str(root)):
        first = local_compaction.compaction_stats({"name": "svc"})
        (part / "b.parquet").write_bytes(b"x")
        second = local_compaction.compaction_stats({"name": "svc"})
        assert first == second
        assert first["total_files"] == 1

        CacheRegistry.clear("core.local_compaction._COMPACTION_STATS_CACHE")
        assert local_compaction.compaction_stats({"name": "svc"})["total_files"] == 2


def test_compaction_stats_missing_dir_is_memoised(tmp_path):
    from backend.core import local_compaction

    root = tmp_path / "nothing"
    with patch("backend.core.duckdb._cache_dir", return_value=str(root)):
        assert local_compaction.compaction_stats({"name": "svc"})["total_files"] == 0
        (root / "data").mkdir(parents=True)
        assert local_compaction.compaction_stats({"name": "svc"})["total_files"] == 0


def test_storage_stats_cache_hit_force_and_invalidate():
    from backend.core.metadata import reconciliation as rec

    con = MagicMock()
    con.execute.return_value.fetchone.return_value = [3]
    rec._storage_stats_cache.clear()
    with (
        patch.object(rec, "get_con_readonly", return_value=con) as opener,
        patch.object(rec, "_open_usage_log", return_value=None),
    ):
        a = rec.get_metadata_storage_stats("svc-ttl")
        b = rec.get_metadata_storage_stats("svc-ttl")
        assert a is b
        assert opener.call_count == 1

        rec.get_metadata_storage_stats("svc-ttl", force=True)
        assert opener.call_count == 2

        rec.invalidate_storage_stats_cache("svc-ttl")
        rec.get_metadata_storage_stats("svc-ttl")
        assert opener.call_count == 3

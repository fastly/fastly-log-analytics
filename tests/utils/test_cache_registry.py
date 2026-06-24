"""A-3: CacheRegistry process-wide drain helper."""

from __future__ import annotations

import pytest

from backend.utils.cache_registry import CacheRegistry


@pytest.fixture(autouse=True)
def _isolate_registry():
    # Tests run after the autouse conftest fixture has already triggered
    # CacheRegistry.clear_all() once — registration state is stable, but
    # to avoid polluting other tests' assertions we snapshot the registry
    # before each test here and restore after.
    snapshot_names = CacheRegistry.names()
    yield
    # Drop any test-only registrations.
    for n in CacheRegistry.names():
        if n not in snapshot_names:
            with CacheRegistry._lock:
                CacheRegistry._entries.pop(n, None)


def test_register_and_clear_dict():
    d: dict[str, int] = {}
    CacheRegistry.register("test.dict_a", d)
    d["x"] = 1
    d["y"] = 2
    CacheRegistry.clear_all()
    assert d == {}


def test_register_and_clear_set():
    s: set[str] = set()
    CacheRegistry.register("test.set_a", s)
    s.update({"a", "b", "c"})
    CacheRegistry.clear_all()
    assert s == set()


def test_register_object_with_clear_method():
    class Box:
        def __init__(self):
            self.items = [1, 2, 3]
            self.cleared = False

        def clear(self):
            self.items = []
            self.cleared = True

    b = Box()
    CacheRegistry.register("test.box", b)
    CacheRegistry.clear_all()
    assert b.cleared is True
    assert b.items == []


def test_clear_all_skips_entries_without_clear_method():
    # An int doesn't have .clear(); the registry must not blow up.
    CacheRegistry.register("test.bare_int", 42)
    CacheRegistry.clear_all()  # no exception


def test_iceberg_caches_are_registered_at_import():
    # Pin that the production modules registered their caches —
    # otherwise the conftest fixture would be a no-op for those modules.
    # Importing each module triggers its register() calls; the test must
    # do that explicitly because the cache registry runs at module-load,
    # not at module-import-into-some-other-place.
    import backend.core.duckdb  # noqa: F401 — pulls in duckdb registrations
    import backend.core.iceberg  # noqa: F401 — pulls in iceberg registrations
    import backend.repositories.dashboard  # noqa: F401
    import backend.repositories.network  # noqa: F401

    names = set(CacheRegistry.names())
    for required in (
        "iceberg._view_cache",
        "iceberg._snapshot_files_cache",
        "iceberg._catalog_cache",
        "iceberg._table_object_cache",
        "iceberg._table_summary_hash_cache",
        "iceberg._pointer_cache",
        "iceberg._manifest_metadata_cache",
        "iceberg._ui_metadata_cache",
        "dashboard._dashboard_cache",
        "network._response_cache",
        "duckdb._fos_client_cache",
        "duckdb._initialized_paths",
        "duckdb._schema_cache",
    ):
        assert required in names, f"missing registration: {required}"

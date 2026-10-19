"""``rum_duckdb_path`` / ``rum_source_for`` derive the sibling RUM DB path."""

from __future__ import annotations

import pytest

from backend.core.duckdb import rum_duckdb_path, rum_source_for


@pytest.mark.parametrize(
    ("given", "expected"),
    [
        ("/data/.duckdb/svc.duckdb", "/data/.duckdb/svc.rum.duckdb"),
        ("svc", "svc.rum.duckdb"),
        ("/a.duckdb.duckdb", "/a.duckdb.rum.duckdb"),
    ],
)
def test_rum_duckdb_path(given: str, expected: str) -> None:
    assert rum_duckdb_path(given) == expected


def test_rum_source_for_only_rewrites_suffix() -> None:
    out = rum_source_for({"name": "svc", "duckdb_path": "/data/.duckdb/svc.duckdb"})
    assert out["duckdb_path"] == "/data/.duckdb/svc.rum.duckdb"
    assert out["name"] == "svc::rum"


def test_rum_source_for_is_shallow_and_leaves_original_untouched() -> None:
    nested = {"k": "v"}
    source = {"name": "svc", "duckdb_path": "/d/svc.duckdb", "opts": nested}

    out = rum_source_for(source)

    assert source == {"name": "svc", "duckdb_path": "/d/svc.duckdb", "opts": nested}
    assert out is not source
    # Nested values are shared by design: callers only read them.
    assert out["opts"] is nested

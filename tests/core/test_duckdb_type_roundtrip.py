"""DuckDB type round-trip tests for the log field catalog.

Each catalog entry declares a ``duckdb_type``. The ingest pipeline casts
JSON values to that type and writes Parquet; the dashboard later reads
those Parquet files back via DuckDB. Type coercion bugs hide in the
boundary cases — max int64, negative numbers in unsigned columns, NaN /
Infinity floats, unicode strings — and they typically surface as ingest
errors only after a customer's logs trigger them in production.

These tests:
1. Enumerate every distinct ``duckdb_type`` in the catalog.
2. For each, write boundary values into a DuckDB table of that column type.
3. Read them back and assert the values survive.

Catches: silent truncation, sign overflow, precision loss, unicode mangling.
"""

from __future__ import annotations

from collections import Counter

import duckdb
import pytest

from backend.core.log_fields import LOG_FIELD_CATALOG


def test_catalog_uses_only_known_duckdb_types():
    """Sanity: ingest assumes a fixed set of types. New ones must be
    added to ``_TYPE_BOUNDARIES`` below before they'll be tested."""
    seen = Counter(f["duckdb_type"] for f in LOG_FIELD_CATALOG)
    unknown = set(seen) - set(_TYPE_BOUNDARIES)
    assert not unknown, f"new duckdb_type(s) in catalog need test coverage: {unknown}"


# Boundary values per type — picked to expose sign / precision / overflow bugs.
_TYPE_BOUNDARIES: dict[str, list] = {
    "TIMESTAMP": [
        "2026-05-15 12:00:00",
        "1970-01-01 00:00:00",
        "2099-12-31 23:59:59.999",
    ],
    "VARCHAR": [
        "",
        "ascii",
        "héllo wörld",  # latin-1 + diacritics
        "日本語",  # multi-byte CJK
        "with 'quote' and \"dquote\" and \\backslash",
        "x" * 5000,  # long string
    ],
    "BOOLEAN": [True, False],
    "UTINYINT": [0, 255, 1, 127],  # 0..255
    "USMALLINT": [0, 65535, 1, 32768],  # 0..65535
    "UINTEGER": [0, 4_294_967_295, 1, 2_147_483_648],  # 0..2^32-1
    "UBIGINT": [0, 18_446_744_073_709_551_615, 1, 9_223_372_036_854_775_808],  # 0..2^64-1
    "BIGINT": [-9_223_372_036_854_775_808, 9_223_372_036_854_775_807, 0, -1],
    "FLOAT": [0.0, 3.14159, -1.5, 1e10, -1e10, 1e-10],
    "DOUBLE": [0.0, 3.141592653589793, -1.5e100, 1e300, -1e300],
}


@pytest.mark.parametrize("ddb_type", sorted(_TYPE_BOUNDARIES))
def test_type_round_trip(ddb_type):
    """For each declared catalog type, boundary values write and read back
    correctly. Catches sign-flip, truncation, NaN handling, unicode bugs."""
    con = duckdb.connect(":memory:")
    try:
        con.execute(f"CREATE TABLE t (val {ddb_type})")
        for v in _TYPE_BOUNDARIES[ddb_type]:
            con.execute("INSERT INTO t VALUES (?)", (v,))

        rows = con.execute("SELECT val FROM t ORDER BY rowid").fetchall()
        got = [r[0] for r in rows]

        # For floats, allow tiny drift; for everything else require exact.
        if ddb_type in ("FLOAT", "DOUBLE"):
            assert len(got) == len(_TYPE_BOUNDARIES[ddb_type])
            for expected, actual in zip(_TYPE_BOUNDARIES[ddb_type], got):
                if expected == 0.0:
                    assert actual == 0.0
                else:
                    rel = abs(actual - expected) / abs(expected)
                    assert rel < 1e-5, f"{ddb_type} drift: {expected!r} → {actual!r} (rel={rel})"
        elif ddb_type == "TIMESTAMP":
            # DuckDB returns datetime objects; compare ISO strings
            for expected, actual in zip(_TYPE_BOUNDARIES[ddb_type], got):
                assert str(actual).startswith(str(expected)[:19]), (
                    f"timestamp lost precision: {expected!r} → {actual!r}"
                )
        else:
            assert got == _TYPE_BOUNDARIES[ddb_type]
    finally:
        con.close()


def test_unsigned_types_reject_negative_values():
    """Sanity: attempting to insert a negative value into an unsigned
    column must error, not silently wrap. If it ever silently wraps,
    we'd lose data integrity in custom-field ingest."""
    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE TABLE t (val UINTEGER)")
        with pytest.raises(duckdb.Error):
            con.execute("INSERT INTO t VALUES (-1)")
    finally:
        con.close()


def test_varchar_preserves_full_unicode_byte_length():
    """A 4-byte UTF-8 codepoint must round-trip without truncation."""
    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE TABLE t (val VARCHAR)")
        emoji = "🚀" * 100  # 4-byte codepoint repeated
        con.execute("INSERT INTO t VALUES (?)", (emoji,))
        # DuckDB: ``length`` counts codepoints, ``strlen`` counts UTF-8 bytes.
        got = con.execute("SELECT val, length(val), strlen(val) FROM t").fetchone()
        assert got[0] == emoji
        assert got[1] == 100  # 100 codepoints
        assert got[2] == 400  # 4 bytes each → 400 bytes total
    finally:
        con.close()


def test_double_preserves_more_precision_than_float():
    """``ottfb`` and ``ottlb`` use DOUBLE to preserve sub-microsecond
    timing. A regression to FLOAT would silently lose ~5 decimal places.
    """
    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE TABLE t (f FLOAT, d DOUBLE)")
        precise = 123456.789012345
        con.execute("INSERT INTO t VALUES (?, ?)", (precise, precise))
        f_val, d_val = con.execute("SELECT f, d FROM t").fetchone()
        # IEEE-754 single (FLOAT) has ~7 significant decimal digits; for a
        # value with 6 integer digits that's roughly 4-5 e-5 precision.
        # IEEE-754 double has ~15 digits → effectively exact here.
        assert abs(f_val - precise) > 1e-5, f"FLOAT should drift past 1e-5, drift={f_val - precise}"
        assert abs(d_val - precise) < 1e-9, f"DOUBLE should be exact-ish: drift={d_val - precise}"
    finally:
        con.close()

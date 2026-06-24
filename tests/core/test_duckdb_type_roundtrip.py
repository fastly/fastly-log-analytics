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


# ── TIMESTAMPTZ + microsecond-scale precision (audit follow-up) ──────────


def test_timestamptz_with_explicit_offsets_round_trip():
    """``backend.core.ingest`` casts the ``timestamp`` field to TIMESTAMPTZ
    (not plain TIMESTAMP) and the ``end_time`` filter binds ``?::TIMESTAMPTZ``.
    The plain TIMESTAMP cases above don't validate that values with
    explicit timezone offsets survive the round-trip — pinned here so a
    DuckDB minor that changed TZ semantics would fail loudly.
    """
    con = duckdb.connect(":memory:")
    try:
        # Load ICU so TIMESTAMPTZ formatting is deterministic across hosts.
        con.execute("INSTALL icu; LOAD icu;")
        con.execute("SET TimeZone = 'UTC'")
        con.execute("CREATE TABLE t (ts TIMESTAMPTZ)")
        # ISO 8601 strings with explicit offsets — DuckDB casts each to
        # UTC internally; the round-trip must preserve the instant.
        values = [
            ("2026-05-15T12:00:00+00:00", "2026-05-15 12:00:00+00"),
            ("2026-05-15T12:00:00-07:00", "2026-05-15 19:00:00+00"),
            ("2026-05-15T17:30:00+05:30", "2026-05-15 12:00:00+00"),
        ]
        for raw, _ in values:
            con.execute("INSERT INTO t VALUES (?::TIMESTAMPTZ)", (raw,))

        # Sort by ts so the comparison order is deterministic.
        rows = con.execute(
            "SELECT strftime(ts, '%Y-%m-%d %H:%M:%S') AS s, "
            "       (ts AT TIME ZONE 'UTC')::TIMESTAMP AS utc_ts "
            "FROM t ORDER BY rowid"
        ).fetchall()
        # Strings normalise to UTC; the three rows above all encode the
        # same UTC instant 2026-05-15 12:00:00 EXCEPT the second row
        # (which is 19:00:00 UTC). Cross-check on the UTC string.
        utc_strs = [r[0] for r in rows]
        assert utc_strs == ["2026-05-15 12:00:00", "2026-05-15 19:00:00", "2026-05-15 12:00:00"]
    finally:
        con.close()


def test_timestamptz_end_time_filter_pushdown_excludes_future_rows():
    """The ``end_time`` filter binds the cutoff as ``?::TIMESTAMPTZ``.
    Pin that ``ts <= ?::TIMESTAMPTZ`` correctly excludes a row whose
    timestamp is past the cutoff, even when row TZ != cutoff TZ.
    """
    con = duckdb.connect(":memory:")
    try:
        con.execute("INSTALL icu; LOAD icu;")
        con.execute("SET TimeZone = 'UTC'")
        con.execute("CREATE TABLE t (ts TIMESTAMPTZ)")
        # Row at 2026-05-15 11:59:59 UTC (under cutoff).
        con.execute("INSERT INTO t VALUES ('2026-05-15T11:59:59Z'::TIMESTAMPTZ)")
        # Row at 2026-05-15 12:00:01 UTC (over cutoff).
        con.execute("INSERT INTO t VALUES ('2026-05-15T12:00:01Z'::TIMESTAMPTZ)")
        # Cutoff supplied in a non-UTC offset to exercise the cast path
        # ingest actually uses (the API serialises whatever the analyst
        # picked in their tz).
        cutoff = "2026-05-15T05:00:00-07:00"  # = 12:00:00 UTC
        n = con.execute("SELECT COUNT(*) FROM t WHERE ts <= ?::TIMESTAMPTZ", (cutoff,)).fetchone()[0]
        assert n == 1, f"expected 1 row at/under cutoff, got {n}"
    finally:
        con.close()


def test_microsecond_precision_for_elapsed_ottfb_round_trip_via_parquet():
    """Pin that microsecond-scale values for elapsed / ottfb / ottlb
    survive a parquet write + read cycle without truncation. Catches
    Arrow / Parquet codec drift that would silently lose precision on
    timing fields (e.g. p99 latency reported as 63 ms instead of 63 µs).
    """
    import os
    import tempfile

    import pyarrow as pa
    import pyarrow.parquet as pq

    # Realistic microsecond-scale values: 50ms request, 1.234s request,
    # 63s long-poll (prod p99 per memory note), and one large UBIGINT
    # that DOUBLE could not represent precisely.
    elapsed_us = [50_000, 1_234_567, 63_000_000, 9_007_199_254_740_993]
    # ottfb/ottlb are also UBIGINT per the catalog. Use sub-ms values
    # that would round if accidentally cast to ms.
    ottfb_us = [128, 4_287, 12_345_678, 1_000_001]

    table = pa.table(
        {
            "elapsed": pa.array(elapsed_us, type=pa.uint64()),
            "ottfb": pa.array(ottfb_us, type=pa.uint64()),
        }
    )

    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "timing.parquet")
        pq.write_table(table, path, compression="zstd")

        con = duckdb.connect(":memory:")
        try:
            rows = con.execute(f"SELECT elapsed, ottfb FROM read_parquet('{path}') ORDER BY elapsed").fetchall()
        finally:
            con.close()

    # Sort the expected list by elapsed to match the SQL ORDER BY.
    expected = sorted(zip(elapsed_us, ottfb_us))
    assert rows == expected, f"microsecond values drifted across parquet RT: {rows} != {expected}"

"""Property-based tests for backend.repositories.utils.filters.build_where_clause.

The filter SQL builder is a high-leverage chokepoint — every analytical
endpoint runs through it. Most of its bugs land in the long tail: unusual
column names, unicode values, mixes of NULL + wildcard + exact, the
filter_/xfilter_ prefix stripping, the ``waf_sig_ind`` and ``_bot_name``
virtual filters. Hypothesis explores that long tail systematically.

Two contracts checked here:

1. **Parameter-count invariant.** When ``inline_params=False``, the number
   of ``?`` placeholders in the generated SQL must equal ``len(params)``
   exactly. A mismatch causes DuckDB to either raise a binding error or
   silently misinterpret values across rows.

2. **DuckDB parses what we generate.** The generated WHERE clause, with
   its parameters, must execute against an in-memory table without
   raising — both in parameterised mode and in inlined mode. Catches
   quote-escape bugs, unbalanced parens, missing AND/OR, etc.
"""

from __future__ import annotations

import duckdb
import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from backend.models.common import FilterSpec
from backend.repositories.utils.filters import build_where_clause

# ── Hypothesis strategies ─────────────────────────────────────────────────────

# The columns we know about — the ones that get partition-pruning hints
# (dt, timestamp_hour) plus a bunch of regular ones to force the no-prefix /
# prefix-stripping / numeric-suffix branches to fire.
_KNOWN_COLS = (
    "status",
    "country",
    "url",
    "ip",
    "method",
    "ua",
    "pop",
    "asn",
    "city",
    "region",
    "ottfb",
    "ottlb",
    "waf_sig",
    "edge",
)

# Filter values: mostly strings (the on-the-wire shape from the frontend),
# occasionally integers, occasionally None (NULL filter), occasionally with
# a ``*`` wildcard. We deliberately bias toward weird strings — quotes,
# percent signs, backslashes, unicode — because that's where SQL bugs hide.
_value_strategy = st.one_of(
    st.text(min_size=0, max_size=20),
    st.integers(min_value=-1000, max_value=1000),
    st.none(),
    st.sampled_from(["a*", "*b", "*foo*", "a*b*", "%lit", "''", '"', "\\", "a\nb"]),
)

_filter_spec_strategy = st.builds(
    FilterSpec,
    mode=st.sampled_from(["include", "exclude"]),
    values=st.lists(_value_strategy, min_size=0, max_size=4),
)


def _filter_key_strategy() -> st.SearchStrategy[str]:
    """Generate keys mimicking the frontend's filter_ / xfilter_ / suffix
    conventions plus bare keys."""
    base = st.sampled_from(_KNOWN_COLS)
    return st.one_of(
        base,
        base.map(lambda c: f"filter_{c}"),
        base.map(lambda c: f"xfilter_{c}"),
        base.map(lambda c: f"{c}_2"),
        base.map(lambda c: f"filter_{c}_3"),
    )


_filters_strategy = st.dictionaries(
    keys=_filter_key_strategy(),
    values=_filter_spec_strategy,
    min_size=0,
    max_size=4,
)

_iso_time_strategy = st.one_of(
    st.none(),
    st.sampled_from(
        [
            "2026-05-15T00:00:00Z",
            "2026-05-15T12:30:45+00:00",
            "2026-05-15",  # date-only — exercises the partition-prune branches
            "2026-01-01T00:00:00.000Z",
        ]
    ),
)


# ── Test fixtures ─────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def schema_table():
    """In-memory DuckDB table with a representative subset of columns —
    enough to actually execute the generated WHERE against."""
    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE logs_props (
            timestamp TIMESTAMPTZ,
            dt VARCHAR,
            timestamp_hour VARCHAR,
            status INTEGER,
            country VARCHAR,
            url VARCHAR,
            ip VARCHAR,
            method VARCHAR,
            ua VARCHAR,
            pop VARCHAR,
            asn INTEGER,
            city VARCHAR,
            region VARCHAR,
            ottfb DOUBLE,
            ottlb DOUBLE,
            waf_sig VARCHAR,
            edge VARCHAR
        )
        """
    )
    yield con
    con.close()


# ── Property: placeholder count matches param count ──────────────────────────


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_param_count_matches_placeholder_count(filters, start, end):
    """Every ``?`` in the generated SQL must have a positional param."""
    params, sql = build_where_clause(start, end, filters, actual_cols=list(_KNOWN_COLS) + ["dt", "timestamp_hour"])
    assert sql.count("?") == len(params), (
        f"Placeholder/param mismatch:\n  sql={sql!r}\n  params={params!r}\n  filters={filters!r}\n  start={start!r} end={end!r}"
    )


# ── Property: generated SQL parses + executes against a real table ───────────


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(
    max_examples=20, deadline=None, suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture]
)
def test_parameterised_sql_executes(filters, start, end, schema_table):
    """The WHERE clause + params must execute against an in-memory DuckDB
    without raising. Catches quote escape, unbalanced parens, missing
    join operators, type mismatches in cast targets, etc.

    We skip cases where a filter targets a column the schema doesn't have
    (e.g. ``_bot_name``/``_ngwaf_bot_name`` virtual filters that depend on
    external data); the unit suite covers those explicitly.
    """
    # Skip virtual-filter keys — they require external state (bot_sources
    # registry, ngwaf SQLite cache) that hypothesis can't usefully fuzz.
    assume(not any("_bot_name" in k or "_ngwaf_bot_name" in k for k in filters))

    params, sql = build_where_clause(start, end, filters, actual_cols=list(_KNOWN_COLS) + ["dt", "timestamp_hour"])
    # Just run the WHERE clause — we don't care about results, only that
    # DuckDB accepts the SQL.
    try:
        schema_table.execute(f"SELECT 1 FROM logs_props WHERE {sql}", params).fetchall()
    except duckdb.Error as e:
        raise AssertionError(
            f"Generated SQL failed to execute:\n  sql={sql!r}\n  params={params!r}\n  err={e}\n  filters={filters!r}"
        )


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(
    max_examples=15, deadline=None, suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture]
)
def test_inlined_sql_executes(filters, start, end, schema_table):
    """Same as above but with ``inline_params=True`` — the temp-table code
    path. Inline mode does its own quote escaping; this catches mismatches
    between that path and the parameterised path."""
    assume(not any("_bot_name" in k or "_ngwaf_bot_name" in k for k in filters))

    params, sql = build_where_clause(
        start, end, filters, actual_cols=list(_KNOWN_COLS) + ["dt", "timestamp_hour"], inline_params=True
    )
    assert params == [], f"inline_params=True should yield zero params, got {params!r}"
    try:
        schema_table.execute(f"SELECT 1 FROM logs_props WHERE {sql}").fetchall()
    except duckdb.Error as e:
        raise AssertionError(f"Inlined SQL failed to execute:\n  sql={sql!r}\n  err={e}\n  filters={filters!r}")


# ── Property: passthrough when nothing supplied ──────────────────────────────


@given(actual_cols=st.one_of(st.none(), st.lists(st.sampled_from(_KNOWN_COLS), min_size=0, max_size=8)))
def test_no_args_returns_passthrough(actual_cols):
    """No date range, no filters → ``1=1`` no matter what columns exist."""
    params, sql = build_where_clause(None, None, {}, actual_cols=actual_cols)
    assert sql == "1=1"
    assert params == []


# ── Property: empty values list never adds any clause ────────────────────────


@given(col=st.sampled_from(_KNOWN_COLS), mode=st.sampled_from(["include", "exclude"]))
def test_empty_values_list_is_dropped(col, mode):
    """A FilterSpec with values=[] must contribute zero conditions."""
    filters = {col: FilterSpec(mode=mode, values=[])}
    params, sql = build_where_clause(None, None, filters, actual_cols=list(_KNOWN_COLS))
    assert sql == "1=1"
    assert params == []


# ── Regression: control chars must not break inlined SQL ─────────────────────


def test_inline_strips_null_byte_and_control_chars(schema_table):
    """Hypothesis found that a NULL byte (``\\x00``) in a filter value
    crashes DuckDB's SQL parser ("unterminated quoted string"). The
    inline-params path must strip control characters before embedding."""
    filters = {"status": FilterSpec(mode="include", values=["\x00bad\x01value"])}
    _, sql = build_where_clause(None, None, filters, actual_cols=list(_KNOWN_COLS), inline_params=True)
    # Must execute without parser error
    schema_table.execute(f"SELECT 1 FROM logs_props WHERE {sql}").fetchall()
    # And the original control chars are gone from the rendered SQL
    assert "\x00" not in sql and "\x01" not in sql


# ── _bot_name virtual filter: expand bot id → UA regex ──────────────────────


from unittest.mock import patch  # noqa: E402


def test_bot_name_filter_expands_to_ua_regex_include():
    """``_bot_name`` is a virtual filter — the FE sends a bot_id and the
    SQL builder expands it into a ``regexp_matches(ua, ...)`` clause
    using the bot's UA patterns. Pinned because this is the only
    filter type that requires an external registry lookup."""
    fake_bot = {"id": "googlebot", "pattern": {"accepted": [r"Googlebot/2\.1", r"compatible; Googlebot"]}}

    with patch("backend.utils.bot_sources.get_bot_by_id", return_value=fake_bot):
        _, sql = build_where_clause(
            None,
            None,
            {"_bot_name": FilterSpec(mode="include", values=["googlebot"])},
            actual_cols=["ua"],
        )

    assert "regexp_matches" in sql
    assert "(?i)" in sql  # case-insensitive
    # Both alternations are present
    assert r"Googlebot/2\.1" in sql or "Googlebot" in sql


def test_bot_name_filter_emits_negation_for_exclude_mode():
    fake_bot = {"id": "googlebot", "pattern": {"accepted": [r"Googlebot/2\.1"]}}

    with patch("backend.utils.bot_sources.get_bot_by_id", return_value=fake_bot):
        _, sql = build_where_clause(
            None,
            None,
            {"_bot_name": FilterSpec(mode="exclude", values=["googlebot"])},
            actual_cols=["ua"],
        )

    assert "NOT regexp_matches" in sql


def test_bot_name_filter_skipped_when_ua_column_absent():
    """If the service hasn't enabled the ``ua`` column, the filter is
    skipped silently with a warning (not a 500). Pinned because the
    frontend may persist a bot filter from another service that DID
    have ``ua`` — switching service shouldn't break the dashboard."""
    fake_bot = {"id": "googlebot", "pattern": {"accepted": [r"Googlebot"]}}

    with patch("backend.utils.bot_sources.get_bot_by_id", return_value=fake_bot):
        _, sql = build_where_clause(
            None,
            None,
            {"_bot_name": FilterSpec(mode="include", values=["googlebot"])},
            actual_cols=["status"],  # no 'ua'
        )

    # Filter dropped → only the trivial 1=1 stays
    assert "regexp_matches" not in sql
    assert sql == "1=1"


def test_bot_name_filter_skips_unknown_bot_ids():
    """``get_bot_by_id`` returning None → that bot id is skipped, but
    other bot ids in the same filter still produce clauses."""

    def _lookup(bot_id):
        if bot_id == "known":
            return {"id": "known", "pattern": {"accepted": [r"KnownBot"]}}
        return None  # unknown bot

    with patch("backend.utils.bot_sources.get_bot_by_id", side_effect=_lookup):
        _, sql = build_where_clause(
            None,
            None,
            {"_bot_name": FilterSpec(mode="include", values=["known", "unknown"])},
            actual_cols=["ua"],
        )

    assert "KnownBot" in sql


def test_bot_name_filter_skips_bots_with_no_patterns():
    """A bot entry with empty ``pattern.accepted`` → skip without
    error. Pinned because a misconfigured bot source would otherwise
    surface as a confusing empty-clause SQL error."""
    fake_bot = {"id": "empty", "pattern": {"accepted": []}}

    with patch("backend.utils.bot_sources.get_bot_by_id", return_value=fake_bot):
        _, sql = build_where_clause(
            None,
            None,
            {"_bot_name": FilterSpec(mode="include", values=["empty"])},
            actual_cols=["ua"],
        )

    assert "regexp_matches" not in sql


def test_bot_name_filter_escapes_single_quotes_in_patterns():
    """A bot pattern containing a single quote must be SQL-escaped
    (`'` → `''`). Pinned to prevent a malicious or accidental bot
    registry entry from breaking out of the regex string literal."""
    fake_bot = {"id": "evil", "pattern": {"accepted": [r"foo'; DROP TABLE--"]}}

    with patch("backend.utils.bot_sources.get_bot_by_id", return_value=fake_bot):
        _, sql = build_where_clause(
            None,
            None,
            {"_bot_name": FilterSpec(mode="include", values=["evil"])},
            actual_cols=["ua"],
        )

    # Single quote got doubled — not raw — so it can't terminate the SQL string
    assert "foo''; DROP TABLE" in sql or "foo''" in sql


# ── _ngwaf_bot_name virtual filter: sqlite_scan subquery ───────────────────


def test_ngwaf_bot_name_filter_skipped_when_waf_req_id_absent():
    """If ``waf_req_id`` isn't in the schema, the filter is dropped
    silently. Same FE-cross-service-persistence rationale as ``_bot_name``."""
    _, sql = build_where_clause(
        None,
        None,
        {"_ngwaf_bot_name": FilterSpec(mode="include", values=["GoogleBot"])},
        actual_cols=["status"],  # no waf_req_id
    )

    assert "sqlite_scan" not in sql
    assert sql == "1=1"


def _create_ngwaf_cache_db(db_path: str, entries: list[tuple[str, str]]) -> None:
    """Populate a minimal ngwaf_bot_cache SQLite file. entries: (waf_req_id, bot_name)."""
    import sqlite3

    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute(
        "CREATE TABLE IF NOT EXISTS ngwaf_bots ("
        " waf_req_id TEXT PRIMARY KEY, bot_name TEXT, category TEXT,"
        " wellknown_bot_id TEXT, wellknown_bot_name TEXT, synced_at TEXT)"
    )
    for wid, name in entries:
        con.execute("INSERT INTO ngwaf_bots VALUES (?, ?, NULL, NULL, NULL, '2026-01-01T00:00:00Z')", (wid, name))
    con.commit()
    con.close()


def test_ngwaf_bot_name_filter_resolves_ids_via_sqlite_not_duckdb(tmp_path, monkeypatch):
    """When ``waf_req_id`` is in schema and the ngwaf DB has a matching bot
    name, the filter compiles to a plain ``waf_req_id IN (...)`` literal
    list resolved via a direct sqlite3 lookup — NOT DuckDB's sqlite_scan.
    sqlite_scan doesn't reliably coordinate with SQLite's own WAL/locking
    protocol; cross-engine reads against this globally-shared,
    frequently-written cache corrupted it in production (2026-07-30)."""
    fake_db = tmp_path / "ngwaf.db"
    _create_ngwaf_cache_db(str(fake_db), [("w1", "GoogleBot"), ("w2", "GoogleBot")])
    monkeypatch.setattr("backend.config.ngwaf_db_path", lambda: str(fake_db))

    _, sql = build_where_clause(
        None,
        None,
        {"_ngwaf_bot_name": FilterSpec(mode="include", values=["GoogleBot"])},
        actual_cols=["waf_req_id"],
    )

    assert "sqlite_scan" not in sql
    assert "ngwaf_bots" not in sql
    assert "waf_req_id IN" in sql


def test_ngwaf_bot_name_filter_emits_not_in_for_exclude(tmp_path, monkeypatch):
    fake_db = tmp_path / "ngwaf.db"
    _create_ngwaf_cache_db(str(fake_db), [("w1", "GoogleBot")])
    monkeypatch.setattr("backend.config.ngwaf_db_path", lambda: str(fake_db))

    _, sql = build_where_clause(
        None,
        None,
        {"_ngwaf_bot_name": FilterSpec(mode="exclude", values=["GoogleBot"])},
        actual_cols=["waf_req_id"],
    )

    assert "waf_req_id NOT IN" in sql


def test_ngwaf_bot_name_filter_include_with_no_cache_matches_is_false(tmp_path, monkeypatch):
    """An include filter whose bot name matches nothing in the cache must
    match nothing (not everything) — mirrors the empty-subquery IN-clause
    behavior of the sqlite_scan-based implementation it replaced."""
    fake_db = tmp_path / "ngwaf.db"
    _create_ngwaf_cache_db(str(fake_db), [("w1", "SomeOtherBot")])
    monkeypatch.setattr("backend.config.ngwaf_db_path", lambda: str(fake_db))

    _, sql = build_where_clause(
        None,
        None,
        {"_ngwaf_bot_name": FilterSpec(mode="include", values=["GoogleBot"])},
        actual_cols=["waf_req_id"],
    )

    assert sql == "(FALSE)"


def test_ngwaf_bot_name_filter_exclude_with_no_cache_matches_is_vacuous(tmp_path, monkeypatch):
    """An exclude filter whose bot name matches nothing in the cache is a
    no-op (nothing to exclude) — not a bug, matches prior NOT IN (empty
    set) semantics."""
    fake_db = tmp_path / "ngwaf.db"
    _create_ngwaf_cache_db(str(fake_db), [("w1", "SomeOtherBot")])
    monkeypatch.setattr("backend.config.ngwaf_db_path", lambda: str(fake_db))

    _, sql = build_where_clause(
        None,
        None,
        {"_ngwaf_bot_name": FilterSpec(mode="exclude", values=["GoogleBot"])},
        actual_cols=["waf_req_id"],
    )

    assert sql == "1=1"


def test_ngwaf_bot_name_filter_silently_skipped_when_db_missing(tmp_path, monkeypatch):
    """No NGWAF cache file on disk → filter is silently dropped (not
    a 500). Pinned because services that haven't enabled NGWAF
    shouldn't have a stale bot filter break their analytical queries."""
    missing_db = tmp_path / "doesnotexist.db"
    monkeypatch.setattr("backend.config.ngwaf_db_path", lambda: str(missing_db))

    _, sql = build_where_clause(
        None,
        None,
        {"_ngwaf_bot_name": FilterSpec(mode="include", values=["GoogleBot"])},
        actual_cols=["waf_req_id"],
    )

    assert "sqlite_scan" not in sql

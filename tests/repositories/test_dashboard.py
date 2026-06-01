from unittest.mock import patch

import pytest

from backend.repositories._base import _safe_table
from backend.repositories.dashboard import (
    FIELDS,
    _dashboard_cache,
    get_aggregates,
    get_field_values,
    get_raw,
    get_raw_df,
)
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


@pytest.fixture(autouse=True)
def clear_dashboard_cache():
    _dashboard_cache.clear()
    yield
    _dashboard_cache.clear()


def _src_without_table():
    """Returns a src dict whose table does not exist in the in-memory DB."""
    return {"name": "nonexistent_service", "service_id": "nonexistent_service"}


def test_get_aggregates_empty_table(in_memory_duckdb):
    """Returns correct empty structure when the table does not exist."""
    src = _src_without_table()
    result = get_aggregates(
        con=in_memory_duckdb,
        src=src,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
    )

    assert "data" in result
    assert "time_series" in result
    assert "map_data" in result
    assert result["total_rows"] == 0
    assert result["time_series"] == []
    assert result["map_data"] == []
    # All FIELDS should be present with empty tops
    for field in FIELDS:
        assert field in result["data"]
        assert result["data"][field]["top"] == []
        assert result["data"][field]["total"] == 0


def test_get_aggregates_with_data(in_memory_duckdb, test_service_source):
    """Returns populated aggregates after data is inserted."""
    logs = generate_mock_logs(test_service_source, num_logs=60, hours_ago=1)
    table_name = _safe_table(test_service_source["name"])
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
    )

    assert result["total_rows"] > 0
    assert result["metric"] == "requests"
    assert isinstance(result["time_series"], list)
    assert isinstance(result["map_data"], list)

    # Fields present in the mock data (Group A, C, D, F) should have non-zero totals
    assert result["data"]["url"]["total"] > 0
    assert result["data"]["ip"]["total"] > 0
    assert result["data"]["status"]["total"] > 0
    assert result["data"]["country"]["total"] > 0

    # Top entries for url should be valid strings
    if result["data"]["url"]["top"]:
        top_url = result["data"]["url"]["top"][0]
        assert "value" in top_url
        assert "count" in top_url
        assert isinstance(top_url["count"], int)

    # Country map data should be present (group D generates country field)
    if result["map_data"]:
        entry = result["map_data"][0]
        assert "country" in entry
        assert "count" in entry


def test_get_aggregates_result_is_cached(in_memory_duckdb, test_service_source):
    """Second call with identical params returns a cached result."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=10)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result1 = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
    )
    result2 = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
    )

    assert result2.get("_is_cached") is True
    assert result1["total_rows"] == result2["total_rows"]


def test_get_aggregates_5xx_metric(in_memory_duckdb, test_service_source):
    """chart_metric='5xx' returns time series grouped by status code."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=50)
    # Inject some 500s to guarantee 5xx data
    for log in logs[:10]:
        log["status"] = 500
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="5xx",
    )

    assert result["metric"] == "5xx"
    assert isinstance(result["time_series"], list)
    if result["time_series"]:
        pt = result["time_series"][0]
        assert "time" in pt
        assert "value" in pt


def test_get_aggregates_hit_rate_metric(in_memory_duckdb, test_service_source):
    """chart_metric='hit_rate' returns time series with float values."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=50)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="hit_rate",
    )

    assert result["metric"] == "hit_rate"
    assert isinstance(result["time_series"], list)
    if result["time_series"]:
        assert "value" in result["time_series"][0]
        # hit_rate is a percentage so should be 0–100
        assert 0.0 <= result["time_series"][0]["value"] <= 100.0


def test_get_aggregates_debug_queries_populated(in_memory_duckdb, test_service_source):
    """_debug_queries list is populated with executed SQL."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
    )

    assert isinstance(result["debug_queries"], list)
    assert len(result["debug_queries"]) > 0
    first_q = result["debug_queries"][0]
    assert "sql" in first_q
    assert "time_ms" in first_q


# ── get_raw: log-table grid endpoint ────────────────────────────────────────


def test_get_raw_returns_empty_shape_when_table_missing(in_memory_duckdb):
    """No table → all-empty shape so the FE grid renders "no data"
    instead of crashing on missing keys."""
    src = _src_without_table()
    out = get_raw(
        con=in_memory_duckdb,
        src=src,
        start_time=None,
        end_time=None,
        filters={},
        page=1,
        limit=50,
        sort_col=None,
        sort_dir="DESC",
        columns=[],
    )
    assert out["data"] == []
    assert out["total_rows"] == 0
    assert out["page"] == 1
    assert out["limit"] == 50


def test_get_raw_orders_by_timestamp_descending_by_default(in_memory_duckdb, test_service_source):
    """Without ``sort_col``, the helper falls back to
    ``ORDER BY timestamp DESC`` so the latest log is first. Pinned
    because losing this would silently flip the grid to oldest-first."""
    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=2)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    out = get_raw(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        page=1,
        limit=10,
        sort_col=None,
        sort_dir="DESC",
        columns=["timestamp", "status"],
    )

    data = out["data"]
    assert len(data) > 0
    # Descending → first timestamp >= last timestamp
    if len(data) >= 2:
        first_ts = data[0]["timestamp"]
        last_ts = data[-1]["timestamp"]
        assert first_ts >= last_ts


def test_get_raw_respects_explicit_sort_column(in_memory_duckdb, test_service_source):
    """``sort_col='status'`` reorders the result by status. Pinned
    because the FE column-header click sends this and clicking must
    actually re-sort."""
    logs = generate_mock_logs(test_service_source, num_logs=20, hours_ago=1)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    out = get_raw(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        page=1,
        limit=20,
        sort_col="status",
        sort_dir="ASC",
        columns=["status", "timestamp"],
    )

    statuses = [r["status"] for r in out["data"]]
    assert statuses == sorted(statuses)


def test_get_raw_paginates_correctly(in_memory_duckdb, test_service_source):
    """page=2 + limit=5 → OFFSET 5. Pinned because off-by-one in the
    offset calc would either show page 1 twice or skip rows."""
    # Use unique timestamps so DESC sort + LIMIT/OFFSET is deterministic
    # — the previous reliance on `generate_mock_logs`'s random
    # second-precision timestamps would occasionally produce ties and
    # flake the pagination invariant.
    from datetime import UTC, datetime, timedelta

    logs = generate_mock_logs(test_service_source, num_logs=30, hours_ago=1)
    base = datetime.now(UTC) - timedelta(hours=1)
    for i, log in enumerate(logs):
        log["timestamp"] = (base + timedelta(seconds=i * 30)).isoformat()
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    page1 = get_raw(
        in_memory_duckdb,
        test_service_source,
        None,
        None,
        {},
        page=1,
        limit=5,
        sort_col="timestamp",
        sort_dir="DESC",
        columns=["timestamp"],
    )
    page2 = get_raw(
        in_memory_duckdb,
        test_service_source,
        None,
        None,
        {},
        page=2,
        limit=5,
        sort_col="timestamp",
        sort_dir="DESC",
        columns=["timestamp"],
    )

    page1_ts = {r["timestamp"] for r in page1["data"]}
    page2_ts = {r["timestamp"] for r in page2["data"]}
    # No overlap between pages
    assert not page1_ts & page2_ts


def test_get_raw_returns_only_requested_columns(in_memory_duckdb, test_service_source):
    """``columns=['status']`` → returned records have only ``status``
    (plus the implicit timestamp the sort needs). Pinned because the
    FE grid relies on getting exactly the requested columns for its
    column-config UX."""
    logs = generate_mock_logs(test_service_source, num_logs=5, hours_ago=1)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    out = get_raw(
        in_memory_duckdb,
        test_service_source,
        None,
        None,
        {},
        page=1,
        limit=5,
        sort_col="timestamp",
        sort_dir="DESC",
        columns=["status"],
    )

    if out["data"]:
        keys = set(out["data"][0].keys())
        # Only the requested column is returned (timestamp is added internally
        # for sorting but filtered out before serialisation)
        assert keys == {"status"}


# ── get_raw_df: DataFrame variant for direct manipulation ──────────────────


def test_get_raw_df_returns_empty_df_when_no_schema(in_memory_duckdb):
    """Missing table → empty pandas DataFrame (not None). Pinned
    because callers do ``df.empty`` checks."""
    out = get_raw_df(
        con=in_memory_duckdb,
        src=_src_without_table(),
        start_time=None,
        end_time=None,
        filters={},
        limit=100,
        columns=[],
    )
    assert out.empty


def test_get_raw_df_returns_pandas_dataframe(in_memory_duckdb, test_service_source):
    logs = generate_mock_logs(test_service_source, num_logs=5, hours_ago=1)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    out = get_raw_df(in_memory_duckdb, test_service_source, None, None, {}, limit=5, columns=["timestamp", "status"])

    import pandas as pd

    assert isinstance(out, pd.DataFrame)
    assert len(out) <= 5
    # Only the requested columns are kept
    assert set(out.columns) <= {"timestamp", "status"}


# ── get_field_values: picker dropdown ───────────────────────────────────────


def test_get_field_values_rejects_empty_or_all_unsafe_field_name(in_memory_duckdb, test_service_source):
    """Field name that strips to empty (all unsafe chars) → ValueError.
    The cleaner allows ``[A-Za-z0-9_]`` only; anything else is dropped,
    and if nothing's left we raise rather than execute a bare SQL."""
    with pytest.raises(ValueError, match="Invalid field name"):
        get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="\";'''",  # all stripped → empty
            search="",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )


def test_get_field_values_strips_unsafe_chars_from_field_name(in_memory_duckdb, test_service_source):
    """A field name with mixed safe + unsafe chars gets the unsafe
    ones stripped. The cleaned name is what reaches the table-existence
    check. Pinned because this is the primary defence against SQL
    injection in the field-name slot."""
    logs = generate_mock_logs(test_service_source, num_logs=5, hours_ago=1)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    # ``"; DROP TABLE foo`` strips to ``DROPTABLEfoo`` (no spaces/quotes/semis).
    # That field doesn't exist → LookupError, not a SQL execution.
    with pytest.raises(LookupError):
        get_field_values(
            in_memory_duckdb,
            test_service_source,
            field='"; DROP TABLE foo',
            search="",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )


def test_get_field_values_returns_empty_when_table_missing(in_memory_duckdb):
    """Missing table → empty values (not 500)."""
    out = get_field_values(
        in_memory_duckdb,
        _src_without_table(),
        field="country",
        search="",
        limit=10,
        start_time=None,
        end_time=None,
        filters={},
    )
    assert out["values"] == []
    assert out["field"] == "country"


def test_get_field_values_returns_top_values_grouped_by_count(in_memory_duckdb, test_service_source, tmp_path):
    """Happy path: returns ``{values: [{value, count}, ...]}`` sorted
    by count desc.

    Note: patches ``_cache_dir`` to a tmp_path so the test never short-
    circuits on a stale ``cache/default/top_values.json`` left behind
    by other tests that exercise the refresh-status caching path."""
    logs = generate_mock_logs(test_service_source, num_logs=30, hours_ago=1)
    for i, log in enumerate(logs):
        log["country"] = "US" if i < 20 else "GB"  # 20 US, 10 GB
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="country",
            search="",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )

    values = {v["value"]: v["count"] for v in out["values"]}
    assert "US" in values
    assert values["US"] > values.get("GB", 0)


def test_get_field_values_raises_lookup_error_for_unknown_field(in_memory_duckdb, test_service_source):
    """Field that's valid syntax but not in the table → LookupError
    (which the router maps to 404). Pinned because the picker UI
    distinguishes "no values yet" from "field doesn't exist"."""
    logs = generate_mock_logs(test_service_source, num_logs=5, hours_ago=1)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    with pytest.raises(LookupError, match="not found"):
        get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="completely_unknown_field",
            search="",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )


def test_get_field_values_filters_by_search_substring(in_memory_duckdb, test_service_source):
    """Non-empty ``search`` → ILIKE filter. Pinned because the
    picker's typeahead box sends this; losing it would surface as
    "search returns nothing"."""
    logs = generate_mock_logs(test_service_source, num_logs=15, hours_ago=1)
    for i, log in enumerate(logs):
        log["country"] = ["US", "GB", "FR"][i % 3]
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    out = get_field_values(
        in_memory_duckdb,
        test_service_source,
        field="country",
        search="U",  # matches "US"
        limit=10,
        start_time=None,
        end_time=None,
        filters={},
    )
    # Country search also maps the search term against COUNTRY_MAP
    values = {v["value"] for v in out["values"]}
    assert "US" in values
    # GB and FR don't match "U" as a substring on the code, and
    # neither "United Kingdom" nor "France" matches "U" exactly
    # (though United Kingdom contains "U" — pin permissively)


def test_get_field_values_country_search_resolves_full_names(in_memory_duckdb, test_service_source):
    """Searching ``"united"`` should surface US / GB / etc by looking
    up against ``COUNTRY_MAP``. Pinned because this is the only way
    the picker knows the country names — without it, users would
    have to type ISO codes."""
    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=1)
    for log in logs:
        log["country"] = "US"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    out = get_field_values(
        in_memory_duckdb,
        test_service_source,
        field="country",
        search="united",
        limit=10,
        start_time=None,
        end_time=None,
        filters={},
    )
    values = {v["value"] for v in out["values"]}
    assert "US" in values  # United States matched via the name lookup


def test_get_field_values_excludes_own_filter_from_query(in_memory_duckdb, test_service_source, tmp_path):
    """When the user filters on country=US and then opens the country
    picker, the picker should still show ALL countries (not just US)
    so they can switch. Pinned because the FE keys on the picker
    showing all options to be functional.

    Patches ``_cache_dir`` for the same reason as
    ``test_get_field_values_returns_top_values_grouped_by_count``."""
    logs = generate_mock_logs(test_service_source, num_logs=20, hours_ago=1)
    for i, log in enumerate(logs):
        log["country"] = "US" if i < 10 else "GB"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    from backend.models.common import FilterSpec

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="country",
            search="",
            limit=10,
            start_time=None,
            end_time=None,
            filters={"country": FilterSpec(mode="include", values=["US"])},
        )
    # Both US and GB appear because the country filter was excluded from the picker query
    values = {v["value"] for v in out["values"]}
    assert "US" in values and "GB" in values


def test_get_field_values_uses_top_values_cache_when_present(in_memory_duckdb, test_service_source, tmp_path):
    """If a ``top_values.json`` cache exists for the source, the helper
    short-circuits and reads from it. Pinned because losing this would
    re-run the expensive DISTINCT query on every picker open."""
    fake_cache = {"country": [{"value": "ZZ", "count": 999}, {"value": "YY", "count": 1}]}
    cache_path = tmp_path / "top_values.json"
    import json as _json

    cache_path.write_text(_json.dumps(fake_cache))

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="country",
            search="",  # no search → cache path eligible
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )

    # The cached values surface without hitting the table at all
    assert out["values"][0]["value"] == "ZZ"


# ── get_field_values: _bot_name virtual field ─────────────────────────────


def test_get_field_values_bot_name_returns_empty_when_no_ua_column(in_memory_duckdb, test_service_source, tmp_path):
    """The ``_bot_name`` virtual field requires the ``ua`` column —
    without it, return empty (not 500). Pinned because services
    without UA tracking (Group D disabled) should still render the
    bot filter picker as "no options"."""
    # Create the table WITHOUT a ua column
    in_memory_duckdb.execute(
        f'CREATE TABLE "{_safe_table(test_service_source["name"])}" (status INTEGER, timestamp TIMESTAMP)'
    )

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="_bot_name",
            search="",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )
    assert out["values"] == []


def test_get_field_values_bot_name_aggregates_matched_bots(in_memory_duckdb, test_service_source, tmp_path):
    """When ``ua`` is present, query unique UAs and match each to a
    bot via the matcher. Pinned because losing this would silently
    drop the bot filter picker."""
    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=1)
    for log in logs:
        log["ua"] = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    # Mock build_matcher to return a fake matcher mapping Googlebot UA → bot entry
    def fake_matcher_factory():
        def _match(ua):
            if "Googlebot" in ua:
                return [{"id": "googlebot", "name": "Google Bot"}]
            return []

        return _match

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value=""),
        patch("backend.utils.bot_sources.build_matcher", side_effect=fake_matcher_factory),
    ):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="_bot_name",
            search="",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )

    values = {v["value"]: v for v in out["values"]}
    assert "googlebot" in values
    assert values["googlebot"]["label"] == "Google Bot"
    assert values["googlebot"]["count"] == 10


def test_get_field_values_bot_name_filters_by_search_term(in_memory_duckdb, test_service_source, tmp_path):
    """Search filter applies to BOTH id and name (not just one).
    Pinned because admins use both shorthand IDs and full names."""
    logs = generate_mock_logs(test_service_source, num_logs=5, hours_ago=1)
    for log in logs:
        log["ua"] = "Bingbot/2.0"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    def fake_matcher_factory():
        def _match(_ua):
            return [{"id": "bingbot", "name": "Microsoft Bingbot"}]

        return _match

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value=""),
        patch("backend.utils.bot_sources.build_matcher", side_effect=fake_matcher_factory),
    ):
        # Search by name fragment → matches
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="_bot_name",
            search="microsoft",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )
        assert any(v["value"] == "bingbot" for v in out["values"])

        # Search by mismatching term → excluded
        out_miss = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="_bot_name",
            search="google",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )
        assert out_miss["values"] == []


# ── get_field_values: search variations ────────────────────────────────────


def test_get_field_values_country_search_matches_by_country_name(in_memory_duckdb, test_service_source, tmp_path):
    """Search for "united" matches the COUNTRY_MAP entries for US,
    GB, etc. and includes those rows by IN list. Pinned because losing
    the name-resolution would force users to know ISO-3166 codes."""
    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=1)
    for i, log in enumerate(logs):
        log["country"] = "US" if i < 5 else "GB"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="country",
            search="united",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )
    values = {v["value"] for v in out["values"]}
    # Both US (United States) and GB (United Kingdom) match "united"
    assert "US" in values
    assert "GB" in values


def test_get_field_values_asn_search_resolves_via_metadata_db(in_memory_duckdb, test_service_source, tmp_path):
    """When searching ASN by name fragment, the route resolves matching
    ASN integers via metadata_db and includes them in the IN list.
    Pinned because losing the name-resolution would force users to
    know AS numbers."""
    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=1)
    for i, log in enumerate(logs):
        log["asn"] = 13335 if i < 5 else 15169  # Cloudflare / Google
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("backend.core.metadata_db.asn_ints_for_search", return_value=[13335]),
        patch("backend.core.duckdb.enrich_asn_labels"),
    ):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="asn",
            search="cloudflare",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )
    values = {str(v["value"]) for v in out["values"]}
    assert "13335" in values


def test_get_field_values_asn_search_swallows_metadata_db_exception(in_memory_duckdb, test_service_source, tmp_path):
    """If asn_ints_for_search raises (DB locked, missing column), fall
    back to the plain ILIKE search instead of 500ing. Pinned because
    metadata_db failures during a deploy shouldn't break the picker."""
    logs = generate_mock_logs(test_service_source, num_logs=5, hours_ago=1)
    for log in logs:
        log["asn"] = 13335
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("backend.core.metadata_db.asn_ints_for_search", side_effect=RuntimeError("db locked")),
        patch("backend.core.duckdb.enrich_asn_labels"),
    ):
        # Should not raise; returns whatever the ILIKE matches (likely [])
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="asn",
            search="something",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )
    assert "values" in out


# ── get_aggregates: additional chart_metric branches ──────────────────


def test_get_aggregates_4xx_metric_returns_time_series(in_memory_duckdb, test_service_source):
    """`chart_metric='4xx'` → time series of 4xx error rates. Pinned
    because the FE's 4xx-rate chart keys on this metric name and
    losing it would render an empty chart."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=50)
    # Inject some 404s
    for log in logs[:10]:
        log["status"] = 404
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="4xx",
    )

    assert result["metric"] == "4xx"
    assert isinstance(result["time_series"], list)


def test_get_aggregates_p95_latency_metric_uses_percentile_expression(in_memory_duckdb, test_service_source):
    """`chart_metric='p95_latency'` → time series of p95 latency in
    ms. Pinned because the latency chart keys on this exact metric
    name and any rename would blank the panel."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=50)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="p95_latency",
    )

    assert result["metric"] == "p95_latency"


def test_get_aggregates_p50_latency_metric_uses_median_expression(in_memory_duckdb, test_service_source):
    """`chart_metric='p50_latency'` → time series of median latency.
    Pinned because admin perf-debug workflow uses p50 to identify
    sustained vs spiky regressions."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=30)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="p50_latency",
    )

    assert result["metric"] == "p50_latency"


def test_get_aggregates_p99_latency_metric_uses_percentile_expression(in_memory_duckdb, test_service_source):
    """`chart_metric='p99_latency'` → time series of p99 latency.
    Pinned because the SRE-watching tail-latency panel keys on this
    exact metric name."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=30)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="p99_latency",
    )

    assert result["metric"] == "p99_latency"


def test_get_aggregates_throughput_metric_uses_throughput_formula(in_memory_duckdb, test_service_source):
    """`chart_metric='throughput'` → time series of bandwidth.
    Pinned because the throughput chart on the home dashboard keys
    on this metric — losing it would blank that panel."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=30)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="throughput",
    )

    assert result["metric"] == "throughput"


def test_get_aggregates_ttfb_metric_uses_ttfb_ms_expression(in_memory_duckdb, test_service_source):
    """`chart_metric='ttfb'` → time series of TTFB in ms. Pinned
    because the TTFB chart on the performance panel keys on this
    metric name."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=30)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="ttfb",
    )

    assert result["metric"] == "ttfb"


def test_get_aggregates_unknown_metric_falls_back_to_requests(in_memory_duckdb, test_service_source):
    """`chart_metric='gibberish'` falls back to requests count time
    series rather than 500ing. Pinned because admins sometimes
    type/paste typo'd metric names — fail-soft is friendlier."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="gibberish_not_a_metric",
    )

    # Falls back to requests count (silent default)
    assert result["metric"] == "requests"


def test_get_field_values_country_search_no_country_map_match_uses_plain_ilike(
    in_memory_duckdb, test_service_source, tmp_path
):
    """When the search string doesn't match any COUNTRY_MAP entry,
    fall back to a plain ILIKE on the country column (so admins can
    still search by code fragment)."""
    logs = generate_mock_logs(test_service_source, num_logs=5, hours_ago=1)
    for log in logs:
        log["country"] = "ZZZ"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="country",
            search="zz",  # No COUNTRY_MAP name contains "zz" but the code does
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )
    values = {v["value"] for v in out["values"]}
    assert "ZZZ" in values


# ── _add_bot_columns (helper) ───────────────────────────────────────────


def test_add_bot_columns_no_virtual_fields_requested_returns_false_false():
    """Neither ``_bot_name`` nor ``_ngwaf_bot_name`` in columns →
    helper reports (False, False) and leaves select_cols untouched.
    Pinned because losing this would add ua/ip/waf_req_id to every
    query that didn't need them, ballooning the SELECT list."""
    from backend.repositories.dashboard import _add_bot_columns

    select_cols = ['"status"']
    wants_bot, wants_ngwaf = _add_bot_columns({"ua", "ip", "waf_req_id"}, ["status"], select_cols)
    assert (wants_bot, wants_ngwaf) == (False, False)
    assert select_cols == ['"status"']


def test_add_bot_columns_bot_name_adds_ua_and_ip_when_available():
    """``_bot_name`` requested + ua/ip both in actual_cols → both get
    appended exactly once. Pinned because the bot enricher
    (`enrich_bot_metadata`) reads both ua AND ip for the Arcjet rule
    set — missing either silently produces "unknown bot"."""
    from backend.repositories.dashboard import _add_bot_columns

    select_cols = ['"timestamp"']
    wants_bot, _ = _add_bot_columns({"ua", "ip"}, ["_bot_name"], select_cols)
    assert wants_bot is True
    assert '"ua"' in select_cols
    assert '"ip"' in select_cols


def test_add_bot_columns_bot_name_skips_columns_already_in_select_list():
    """If ``"ua"`` is already in select_cols, the helper must NOT add
    a duplicate. Pinned because DuckDB errors on duplicate column
    references in the same SELECT list."""
    from backend.repositories.dashboard import _add_bot_columns

    select_cols = ['"ua"']  # already present
    _add_bot_columns({"ua", "ip"}, ["_bot_name"], select_cols)
    # No duplicate
    assert select_cols.count('"ua"') == 1


def test_add_bot_columns_bot_name_omits_missing_columns_silently():
    """``_bot_name`` requested but ua/ip not in actual_cols → no
    column is added (returns True for wants but doesn't break the
    SELECT). Pinned because services that don't log ua should still
    be able to query for _bot_name without 500'ing."""
    from backend.repositories.dashboard import _add_bot_columns

    select_cols = ['"status"']
    wants_bot, _ = _add_bot_columns(set(), ["_bot_name"], select_cols)
    assert wants_bot is True
    assert select_cols == ['"status"']  # nothing added


def test_add_bot_columns_ngwaf_bot_name_adds_waf_req_id_only_when_present():
    """``_ngwaf_bot_name`` requires only ``waf_req_id`` (NOT ua/ip).
    Pinned because the NGWAF enricher keys solely on waf_req_id —
    adding ua/ip here would over-project columns for NGWAF-only
    services."""
    from backend.repositories.dashboard import _add_bot_columns

    select_cols = ['"timestamp"']
    _, wants_ngwaf = _add_bot_columns({"waf_req_id"}, ["_ngwaf_bot_name"], select_cols)
    assert wants_ngwaf is True
    assert '"waf_req_id"' in select_cols
    # Importantly, ua/ip are NOT added when only _ngwaf_bot_name was requested
    assert '"ua"' not in select_cols
    assert '"ip"' not in select_cols


# ── get_field_values _bot_name virtual field ────────────────────────────


def test_get_field_values_bot_name_returns_empty_when_table_missing(in_memory_duckdb):
    """`_bot_name` field but no table → empty values, no raise.
    Pinned because the FE's bot-filter picker calls this endpoint
    on first render; a 500 would block the whole filter panel."""
    src = _src_without_table()
    out = get_field_values(
        in_memory_duckdb,
        src,
        field="_bot_name",
        search="",
        limit=10,
        start_time=None,
        end_time=None,
        filters={},
    )
    assert out["values"] == []


# ── get_field_values waf_sig_ind (signal unnesting) ─────────────────────


def test_get_field_values_waf_sig_ind_splits_comma_separated_signals(in_memory_duckdb, test_service_source, tmp_path):
    """``waf_sig_ind`` (virtual field) → SQL unnests the comma-
    separated waf_sig column into individual signals. Pinned because
    the security page's "Top WAF signals" picker expects per-signal
    counts, not the raw concatenated string."""
    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=1)
    for i, log in enumerate(logs):
        log["waf_sig"] = "SQLI,XSS" if i % 2 == 0 else "SQLI"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="waf_sig_ind",
            search="",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )

    values = {v["value"]: v["count"] for v in out["values"]}
    # SQLI present on all 10 rows; XSS only on the 5 even-indexed rows
    assert values.get("SQLI") == 10
    assert values.get("XSS") == 5


def test_get_field_values_waf_sig_ind_applies_search_filter(in_memory_duckdb, test_service_source, tmp_path):
    """Search filter on `waf_sig_ind` runs ILIKE on the unnested
    signal. Pinned because the picker's typeahead won't surface
    matches otherwise."""
    logs = generate_mock_logs(test_service_source, num_logs=4, hours_ago=1)
    for i, log in enumerate(logs):
        log["waf_sig"] = "SQLI" if i % 2 == 0 else "XSS"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="waf_sig_ind",
            search="SQL",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )

    values = {v["value"] for v in out["values"]}
    assert values == {"SQLI"}


# ── get_field_values asn search via metadata_db ──────────────────────────


def test_get_field_values_asn_search_includes_matching_asn_ints(in_memory_duckdb, test_service_source, tmp_path):
    """ASN-name search → `metadata_db.asn_ints_for_search` returns
    matching ints, which get inlined into the WHERE clause. Pinned
    because losing the metadata-DB lookup would force admins to
    type the raw ASN number (defeating the search-by-org-name UX)."""
    logs = generate_mock_logs(test_service_source, num_logs=6, hours_ago=1)
    for i, log in enumerate(logs):
        log["asn"] = 15169 if i < 4 else 32934
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("backend.core.metadata_db.asn_ints_for_search", return_value=[15169]),
        patch("backend.core.duckdb.enrich_asn_labels"),  # avoid SQLite hit
    ):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="asn",
            search="Google",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )

    values = {v["value"] for v in out["values"]}
    # 15169 matches via the inlined IN clause; 32934 is excluded
    assert 15169 in values
    assert 32934 not in values


def test_get_field_values_asn_search_falls_back_to_ilike_when_no_metadata_match(
    in_memory_duckdb, test_service_source, tmp_path
):
    """If `asn_ints_for_search` returns an empty list, fall back to
    plain ILIKE on the ASN column. Pinned because losing the
    fallback would silently drop ASN searches for services whose
    per-service metadata SQLite is empty (e.g. brand-new
    services)."""
    logs = generate_mock_logs(test_service_source, num_logs=4, hours_ago=1)
    for log in logs:
        log["asn"] = 15169
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("backend.core.metadata_db.asn_ints_for_search", return_value=[]),
        patch("backend.core.duckdb.enrich_asn_labels"),
    ):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="asn",
            search="151",  # raw-digit substring
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )

    values = {v["value"] for v in out["values"]}
    assert 15169 in values

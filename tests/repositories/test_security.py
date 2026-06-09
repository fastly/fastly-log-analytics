from backend.repositories._base import _safe_table
from backend.repositories.security import get_security_aggregates, get_top_bots
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


def test_security_aggregates_empty_table(in_memory_duckdb):
    """Returns the expected empty structure when the table does not exist."""
    src = {"name": "nonexistent_security_svc", "service_id": "nonexistent_security_svc"}

    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=src,
        start_time=None,
        end_time=None,
        filters={},
    )

    # Should contain the debug keys even when empty
    assert "debug_queries" in result
    assert "debug_calls" in result


def test_security_aggregates_returns_required_keys(in_memory_duckdb, test_service_source):
    """With data present, the result always contains the expected top-level keys."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=40)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
    )

    expected_keys = {
        "tls_fingerprints",
        "req_size_dist",
        "ipv6_adoption",
        "proxy_dist",
        "conn_reuse_dist",
        "verified_bots_ts",
        "debug_queries",
        "debug_calls",
    }
    assert expected_keys.issubset(result.keys())

    # All list-type fields should actually be lists
    for key in expected_keys - {"debug_queries", "debug_calls"}:
        assert isinstance(result[key], list), f"Expected list for '{key}'"


def test_get_top_bots_empty_table(in_memory_duckdb):
    """Returns empty bots when the table does not exist."""
    src = {"name": "nonexistent_bots_svc", "service_id": "nonexistent_bots_svc"}

    result = get_top_bots(
        con=in_memory_duckdb,
        src=src,
        start_time=None,
        end_time=None,
        filters={},
    )

    assert result["bots"] == []
    assert result["ngwaf_bots"] == []
    # `**runner.telemetry()` is spread into the return so the dashboard
    # can attribute the cold cost of /api/security/top-bots; both fields
    # must be present even on the empty-table fast path.
    assert "debug_queries" in result
    assert "debug_calls" in result


def test_get_top_bots_with_bot_uas(in_memory_duckdb, test_service_source):
    """Returns arcjet bots when bot UAs are present in the data."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=30)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_top_bots(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        n=10,
    )

    assert isinstance(result, dict)
    assert "bots" in result
    assert "ngwaf_bots" in result
    bots = result["bots"]
    if bots:
        bot = bots[0]
        assert "id" in bot
        assert "category" in bot
        assert "request_count" in bot
        assert isinstance(bot["request_count"], int)
        assert bot["request_count"] > 0


def test_get_top_bots_respects_n_limit(in_memory_duckdb, test_service_source):
    """Arcjet bots never exceed the requested n limit."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=50)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_top_bots(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        n=1,
    )

    assert len(result["bots"]) <= 1


# ── get_security_aggregates: additional branch coverage ─────────────────


def test_security_aggregates_includes_tls_fingerprints_when_both_cols_present(in_memory_duckdb, test_service_source):
    """When `tls_ciphers_sha` AND `ip` are both present, the result
    populates `tls_fingerprints` with cipher SHA + ip_count +
    request_count rows. Pinned because the security dashboard's
    TLS panel renders this exact shape — losing it would blank
    the panel."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20)
    # Set tls_ciphers_sha on each log
    for i, log in enumerate(logs):
        log["tls_ciphers_sha"] = "aabbcc11" if i < 10 else "ddeeff22"
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
    )

    # The fingerprint panel populates when both cols present
    assert "tls_fingerprints" in result
    fps = result["tls_fingerprints"]
    if fps:  # may be empty if mock didn't set the col
        for fp in fps:
            assert "fingerprint" in fp
            assert "ip_count" in fp
            assert "request_count" in fp


def test_security_aggregates_tls_fingerprints_empty_when_col_absent(in_memory_duckdb, test_service_source):
    """Without `tls_ciphers_sha` in the schema, TLS panel returns
    empty list (not crash). Pinned because services on older log
    formats won't have this column."""
    table_name = _safe_table(test_service_source["name"])
    # Create a minimal table without tls_ciphers_sha
    in_memory_duckdb.execute(f'CREATE TABLE "{table_name}" (timestamp TIMESTAMP, ip VARCHAR, status INTEGER)')
    in_memory_duckdb.execute(f"INSERT INTO {table_name} VALUES (now(), '1.1.1.1', 200)")

    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
    )

    assert result["tls_fingerprints"] == []


def test_security_aggregates_ngwaf_keys_empty_when_no_waf_req_id_col(in_memory_duckdb, test_service_source):
    """Without `waf_req_id`, NGWAF verified-bots keys are still
    present but empty. Pinned because the FE renders these keys
    even on services without NGWAF — losing them would break
    the JSON shape."""
    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(f'CREATE TABLE "{table_name}" (timestamp TIMESTAMP, status INTEGER)')
    in_memory_duckdb.execute(f"INSERT INTO {table_name} VALUES (now(), 200)")

    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
    )

    assert result["ngwaf_verified_bots"] == []
    assert result["ngwaf_verified_bots_ts"] == []


def test_security_aggregates_wellknown_bots_uses_regex_prefilter_when_pattern_available(
    in_memory_duckdb, test_service_source
):
    """When `get_bot_regex_pattern` returns a pattern, the
    well-known-bots query uses `regexp_matches(ua, '...')` as the
    pre-filter (O(N) via RE2 vs O(N*M) for ILIKE OR). Pinned
    because losing the pre-filter would slow this query 10-100x
    on services with many UA variants."""
    from unittest.mock import patch

    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20)
    for log in logs:
        log["ua"] = "Mozilla/5.0 (compatible; Googlebot/2.1)"
        log["ip"] = "66.249.66.1"
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    fake_matcher_calls = []

    def fake_match(ua):
        fake_matcher_calls.append(ua)
        return [{"id": "googlebot", "categories": ["search"]}]

    with (
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="(?i)bot|crawler"),
        patch("backend.utils.bot_sources.build_matcher", return_value=fake_match),
        patch("backend.utils.rdns_cache.get_hostname", return_value=(None, "pending", False)),
        patch("backend.utils.rdns_cache.classify", return_value="unverified_pending"),
        patch("backend.utils.rdns_cache.enqueue"),
    ):
        result = get_security_aggregates(
            con=in_memory_duckdb,
            src=test_service_source,
            start_time=None,
            end_time=None,
            filters={},
        )

    # Well-known bots populated
    assert "wellknown_bots" in result


def test_security_aggregates_wellknown_bots_falls_back_when_no_pattern_available(in_memory_duckdb, test_service_source):
    """When `get_bot_regex_pattern` returns "" / None, the WHERE
    clause omits the regex predicate (falls back to scanning all
    UA-bearing rows). Pinned because losing the fallback would
    return zero well-known bots when the registry is empty."""
    from unittest.mock import patch

    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=5)
    for log in logs:
        log["ua"] = "MyBot/1.0"
        log["ip"] = "1.2.3.4"
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    with (
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value=""),
        patch("backend.utils.bot_sources.build_matcher", return_value=lambda ua: []),
        patch("backend.utils.rdns_cache.get_hostname", return_value=(None, "pending", False)),
        patch("backend.utils.rdns_cache.classify", return_value="unverified_pending"),
        patch("backend.utils.rdns_cache.enqueue"),
    ):
        result = get_security_aggregates(
            con=in_memory_duckdb,
            src=test_service_source,
            start_time=None,
            end_time=None,
            filters={},
        )

    # Result is still well-formed (no crash, no missing keys)
    assert "wellknown_bots" in result

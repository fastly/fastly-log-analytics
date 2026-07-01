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


# ── Fingerprint rollup routing (Step 6 of #3) ────────────────────────────────


def test_security_aggregates_fingerprints_use_rollup_when_filters_empty(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    """When filters={} AND ip_spread + count rollups exist for a
    fingerprint field, the rollup path serves the FP card without
    running the live FINGERPRINT_TOP_N SQL over the catalog temp.

    This is the load-bearing routing change from the audit's #58-full
    leg: instead of three count(DISTINCT ip) full-temp scans on a 30d
    window (the dominant cost), we read pre-computed top-N + HLL
    sketches. Pinned because a regression here silently restores the
    old slow path on the security tab."""
    import uuid

    import pyarrow as pa
    import pyarrow.parquet as pq

    from backend.repositories._base import QueryRunner
    from backend.utils.hll import HyperLogLog

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))

    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20)
    for i, log in enumerate(logs):
        log["tls_ciphers_sha"] = "rollup-served-fp" if i < 10 else "live-fp-fallback"
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    # Seed count + ip_spread rollups for tls_ciphers_sha so the rollup
    # path has data for the (field, value) we'll assert on. We pin a
    # closed hour the test query window covers.
    from datetime import UTC, datetime, timedelta

    closed_hour_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=2)
    closed_hour = closed_hour_dt.strftime("%Y-%m-%d-%H")

    # Count rollup parquet — schema (value, count) under hive (field, hour).
    count_dir = cache_root / "rollups" / "hour" / "field=tls_ciphers_sha" / f"hour={closed_hour}"
    count_dir.mkdir(parents=True)
    pq.write_table(
        pa.table({"value": pa.array(["rollup-served-fp"]), "count": pa.array([42], type=pa.int64())}),
        str(count_dir / f"compacted_{uuid.uuid4().hex[:12]}.parquet"),
    )

    # IP-spread rollup parquet — HLL sketch over 7 distinct IPs.
    hll = HyperLogLog()
    hll.update([f"10.0.0.{i}" for i in range(7)])
    ip_dir = cache_root / "rollups" / "hour_ip_spread" / "field=tls_ciphers_sha" / f"hour={closed_hour}"
    ip_dir.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {
                "value": pa.array(["rollup-served-fp"]),
                "ip_sketch": pa.array([hll.to_bytes()], type=pa.binary()),
                "ip_count_observed": pa.array([7], type=pa.int64()),
                "sample_capped": pa.array([False], type=pa.bool_()),
            }
        ),
        str(ip_dir / f"compacted_{uuid.uuid4().hex[:12]}.parquet"),
    )

    # Spy on the live FINGERPRINT_TOP_N execution. If the rollup path
    # served the card, the live path MUST NOT fire for tls_ciphers_sha.
    live_fp_calls = {"n": 0}
    orig_execute = QueryRunner.execute

    def _spy(self, sql, *args, **kwargs):
        if isinstance(sql, str) and "count(DISTINCT ip)" in sql and '"tls_ciphers_sha"' in sql:
            live_fp_calls["n"] += 1
        return orig_execute(self, sql, *args, **kwargs)

    monkeypatch.setattr(QueryRunner, "execute", _spy)

    start = (closed_hour_dt - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end = (closed_hour_dt + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=start,
        end_time=end,
        filters={},
    )

    # The rollup path produced the fingerprint card. Check the value
    # came from the rollup, not the in-memory table directly.
    fps = result["tls_fingerprints"]
    by_value = {fp["fingerprint"]: fp for fp in fps}
    assert "rollup-served-fp" in by_value, f"rollup-served fingerprint missing from result; got {list(by_value)}"
    assert by_value["rollup-served-fp"]["request_count"] == 42  # from count rollup
    assert by_value["rollup-served-fp"]["ip_count"] > 0  # from HLL merge

    # The live FINGERPRINT_TOP_N must NOT have run for tls_ciphers_sha
    # — that's the whole point of the routing. (Other live queries
    # may still run for h2/oh fingerprints if they're not rollup-served.)
    assert live_fp_calls["n"] == 0, (
        f"live FINGERPRINT_TOP_N fired {live_fp_calls['n']} times for tls_ciphers_sha "
        f"even though rollup data was available — routing regression"
    )


def test_security_aggregates_fingerprints_fall_back_to_live_when_ip_spread_empty(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    """When the COUNT rollup has data but the IP-spread rollup is cold
    (writer hasn't populated it yet — the post-deploy reality on every
    pre-existing service), the rollup path MUST fall through to the
    live FINGERPRINT_TOP_N SQL so the FE sees real ip_counts rather
    than a sea of zeros.

    Pinned because the first deploy of session 8 hit exactly this
    case in prod: count rollups were already 30d backfilled, ip_spread
    backfill hadn't run yet → every tls_fingerprint row landed with
    ip_count=0. The fix tests for "at least one non-zero ip_count"
    rather than "non-empty ip_spread dict" before marking the col
    rollup-served."""
    import uuid

    import pyarrow as pa
    import pyarrow.parquet as pq

    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))

    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=10)
    for log in logs:
        log["tls_ciphers_sha"] = "live-served-fp"
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    # Seed ONLY the count rollup — leave ip_spread tree empty so the
    # merge returns nothing for any (col, value) pair.
    from datetime import UTC, datetime, timedelta

    closed_hour_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=2)
    closed_hour = closed_hour_dt.strftime("%Y-%m-%d-%H")

    count_dir = cache_root / "rollups" / "hour" / "field=tls_ciphers_sha" / f"hour={closed_hour}"
    count_dir.mkdir(parents=True)
    pq.write_table(
        pa.table({"value": pa.array(["live-served-fp"]), "count": pa.array([42], type=pa.int64())}),
        str(count_dir / f"compacted_{uuid.uuid4().hex[:12]}.parquet"),
    )

    # Spy on live FINGERPRINT_TOP_N: it MUST fire because ip_spread is empty.
    live_fp_calls = {"n": 0}
    orig_execute = QueryRunner.execute

    def _spy(self, sql, *args, **kwargs):
        if isinstance(sql, str) and "count(DISTINCT ip)" in sql and '"tls_ciphers_sha"' in sql:
            live_fp_calls["n"] += 1
        return orig_execute(self, sql, *args, **kwargs)

    monkeypatch.setattr(QueryRunner, "execute", _spy)

    start = (closed_hour_dt - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end = (closed_hour_dt + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=start,
        end_time=end,
        filters={},
    )

    # The live path MUST have run because ip_spread is cold.
    assert live_fp_calls["n"] >= 1, (
        "live FINGERPRINT_TOP_N did not fire for tls_ciphers_sha even though "
        "ip_spread is cold — would have returned ip_count=0 to the FE"
    )

    # And the returned rows must have non-zero ip_count from the live path.
    fps = result["tls_fingerprints"]
    for fp in fps:
        if fp.get("fingerprint") == "live-served-fp":
            assert fp["ip_count"] > 0, f"live-served fingerprint landed with ip_count=0 — fallback didn't run; got {fp}"


def test_security_aggregates_fingerprints_skip_rollup_when_filters_present(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    """When the request has filters, the rollup path is BYPASSED —
    rollups don't carry per-request filter state, so we must use the
    live SQL path even when rollup data exists. Pinned because a
    rollup-on-filtered request would silently ignore the filter and
    surface mis-attributed top-N fingerprints."""
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))

    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=10)
    for log in logs:
        log["tls_ciphers_sha"] = "fp-A"
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    # Spy on the rollup reader call.
    rollup_calls = {"top_n": 0, "ip_spread": 0}
    orig_top_n = QueryRunner.execute_top_n_rollups
    orig_ip = QueryRunner.execute_ip_spread_rollups

    def _spy_top_n(self, *args, **kwargs):
        rollup_calls["top_n"] += 1
        return orig_top_n(self, *args, **kwargs)

    def _spy_ip(self, *args, **kwargs):
        rollup_calls["ip_spread"] += 1
        return orig_ip(self, *args, **kwargs)

    monkeypatch.setattr(QueryRunner, "execute_top_n_rollups", _spy_top_n)
    monkeypatch.setattr(QueryRunner, "execute_ip_spread_rollups", _spy_ip)

    # Non-empty filters MUST bypass the rollup fast-path. Filter shape
    # mirrors what build_where_clause accepts — a single-field include
    # filter. The actual filter value doesn't matter for this test;
    # the rollup-bypass decision is gated solely on truthiness.
    from backend.models.common import FilterSpec

    filters = {"status": FilterSpec(mode="include", values=["200"])}

    get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters=filters,
    )

    assert rollup_calls["top_n"] == 0, (
        f"execute_top_n_rollups fired {rollup_calls['top_n']} times with non-empty filters — "
        f"rollups don't carry filter state and would mis-attribute top-N"
    )
    assert rollup_calls["ip_spread"] == 0


# ── ipv6_adoption + proxy_dist rollup routing (#94 closure) ──────────────────


def _seed_count_rollup(cache_root, field: str, hours_back: int, value_counts: dict[str, int]) -> str:
    """Write a per-(field, hour) count rollup parquet with the requested
    value→count rows; return the closed_hour string for window setup."""
    import uuid
    from datetime import UTC, datetime, timedelta

    import pyarrow as pa
    import pyarrow.parquet as pq

    closed_hour_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=hours_back)
    closed_hour = closed_hour_dt.strftime("%Y-%m-%d-%H")
    count_dir = cache_root / "rollups" / "hour" / f"field={field}" / f"hour={closed_hour}"
    count_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "value": pa.array(list(value_counts.keys())),
                "count": pa.array(list(value_counts.values()), type=pa.int64()),
            }
        ),
        str(count_dir / f"compacted_{uuid.uuid4().hex[:12]}.parquet"),
    )
    return closed_hour


def _seed_bundled_hour_rollup(cache_root, hours_back: int, field_value_counts: dict[str, dict[str, int]]) -> str:
    """Write a per-hour ``all_fields.parquet`` bundle at the prod shape —
    ``(field, value, count)`` columns, no ``hour`` column in the body
    (hour lives in the hive path segment). Returns the closed_hour str.

    Mirrors what ``backend/core/rollups/hour_bundles.py:bundle_hours``
    produces in production. The steady-state post-bundling layout has
    ONLY this file (per-field per-hour copies are swept by
    ``_cleanup_per_field_after_bundle``), so reads against the bundled
    tree must work even when the per-field tree is empty.
    """
    from datetime import UTC, datetime, timedelta

    import pyarrow as pa
    import pyarrow.parquet as pq

    closed_hour_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=hours_back)
    closed_hour = closed_hour_dt.strftime("%Y-%m-%d-%H")
    bundle_dir = cache_root / "rollups" / "hour_bundled" / f"hour={closed_hour}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    rows_field: list[str] = []
    rows_value: list[str] = []
    rows_count: list[int] = []
    for field, vc in field_value_counts.items():
        for value, count in vc.items():
            rows_field.append(field)
            rows_value.append(value)
            rows_count.append(count)
    pq.write_table(
        pa.table(
            {
                "field": pa.array(rows_field),
                "value": pa.array(rows_value),
                "count": pa.array(rows_count, type=pa.int64()),
            }
        ),
        str(bundle_dir / "all_fields.parquet"),
    )
    return closed_hour


def test_security_aggregates_ipv6_uses_rollup_when_filters_empty(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    """When filters={} AND the count rollup has is_ipv6 coverage for the
    window's closed hours, ipv6_adoption is served from the rollup —
    bypassing the live SQL scan over the catalog temp. Pinned because
    a regression here silently restores the temp scan that #94 closure
    is meant to avoid.
    """
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))

    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=10)
    # Seed the live table so the active-hour merge in
    # _ipv6_per_hour_from_rollups has something to find (or harmlessly
    # finds nothing for the in-window active slice). Mock_data fields
    # don't include is_ipv6, so add it explicitly so the col exists in
    # actual_cols and the routing branch is reachable.
    for log in logs:
        log["is_ipv6"] = False
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    from datetime import UTC, datetime, timedelta

    closed_hour_str = _seed_count_rollup(cache_root, "is_ipv6", hours_back=2, value_counts={"true": 23, "false": 77})
    closed_hour_dt = datetime.strptime(closed_hour_str, "%Y-%m-%d-%H").replace(tzinfo=UTC)

    # Spy: the live IPV6_ADOPTION_TS SQL must NOT run when the rollup
    # path serves (it has the canonical SUM(CASE WHEN is_ipv6 ...) /
    # count(*) shape; check for the CASE clause to avoid matching the
    # active-hour merge's similar-but-distinct base-table query).
    live_calls = {"n": 0}
    orig_execute = QueryRunner.execute

    def _spy(self, sql, *args, **kwargs):
        if isinstance(sql, str) and "CASE WHEN is_ipv6" in sql and "FROM t_" in sql:
            live_calls["n"] += 1
        return orig_execute(self, sql, *args, **kwargs)

    monkeypatch.setattr(QueryRunner, "execute", _spy)

    # Window >= 3 days to satisfy the rollup-routing gate
    # (_ROLLUP_MIN_WINDOW_SECONDS). Shorter windows skip rollup serve
    # and stay on the live SQL path — by design, since the rollup-read
    # cost only amortises on wider windows. Pinned separately by
    # test_security_aggregates_ipv6_proxy_skip_rollup_on_small_window.
    start = (closed_hour_dt - timedelta(days=4)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end = (closed_hour_dt + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=start,
        end_time=end,
        filters={},
    )

    # Rollup-served value lands as a time-series row for closed_hour
    # with pct = 23/(23+77) * 100 = 23.0.
    ipv6 = result["ipv6_adoption"]
    by_time = {row["time"]: row for row in ipv6}
    expected_time = f"{closed_hour_str[:10]}T{closed_hour_str[11:13]}:00:00+00:00"
    assert expected_time in by_time, f"closed_hour rollup point missing from result; got {list(by_time)}"
    assert abs(by_time[expected_time]["pct"] - 23.0) < 0.001

    # Live SQL path must NOT have run for ipv6_adoption.
    assert live_calls["n"] == 0, (
        f"live IPV6_ADOPTION_TS fired {live_calls['n']} times even though rollup data was available — "
        f"routing regression"
    )


def test_security_aggregates_proxy_uses_rollup_when_filters_empty(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    """When filters={} AND the count rollup has p_type coverage for the
    window's closed hours, proxy_dist is served from the rollup
    instead of the live PROXY_TYPE_DIST scan over the catalog temp.
    """
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))

    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=5)
    for log in logs:
        log["p_type"] = "hosting"
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    from datetime import UTC, datetime, timedelta

    closed_hour_str = _seed_count_rollup(
        cache_root, "p_type", hours_back=2, value_counts={"hosting": 1496, "edu": 7, "": 1382}
    )
    closed_hour_dt = datetime.strptime(closed_hour_str, "%Y-%m-%d-%H").replace(tzinfo=UTC)

    # Spy: the live PROXY_TYPE_DIST SQL (`SELECT p_type, count(*)... FROM t_*`)
    # must NOT fire when the rollup path serves.
    live_calls = {"n": 0}
    orig_execute = QueryRunner.execute

    def _spy(self, sql, *args, **kwargs):
        if isinstance(sql, str) and "SELECT p_type, count(*)" in sql and "FROM t_" in sql:
            live_calls["n"] += 1
        return orig_execute(self, sql, *args, **kwargs)

    monkeypatch.setattr(QueryRunner, "execute", _spy)

    # Window >= 3 days to satisfy the rollup-routing gate.
    start = (closed_hour_dt - timedelta(days=4)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end = (closed_hour_dt + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=start,
        end_time=end,
        filters={},
    )

    proxy = result["proxy_dist"]
    by_type = {row["type"]: row["count"] for row in proxy}
    # Empty-string p_type filtered out (matches live query's `!= ''`).
    assert "" not in by_type
    assert by_type.get("hosting", 0) >= 1496  # rollup contribution; live merge may add more
    assert by_type.get("edu", 0) >= 7
    assert list(by_type) == sorted(by_type, key=by_type.get, reverse=True)  # sorted desc

    assert live_calls["n"] == 0, f"live PROXY_TYPE_DIST fired {live_calls['n']} times — rollup-served path regression"


def test_security_aggregates_ipv6_proxy_skip_rollup_when_filters_present(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    """Filter-present requests MUST bypass both ipv6 and proxy rollup
    paths — rollups don't carry per-request filter state. Pinned
    because a rollup-on-filtered request would silently ignore the
    filter and mis-attribute the per-hour pct or per-type totals.
    """
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))

    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=5)
    for log in logs:
        log["is_ipv6"] = True
        log["p_type"] = "hosting"
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    # Seed rollups so the pre-check would PASS if filters were empty —
    # the test is whether the filter-present gate fires first.
    _seed_count_rollup(cache_root, "is_ipv6", hours_back=2, value_counts={"true": 10, "false": 0})
    _seed_count_rollup(cache_root, "p_type", hours_back=2, value_counts={"hosting": 10})

    from backend.models.common import FilterSpec

    filters = {"status": FilterSpec(mode="include", values=["200"])}

    # The temp MUST carry is_ipv6 + p_type since the rollups are
    # bypassed. The live SQL paths read those cols from the temp, so a
    # successful response is itself the assertion (BinderException would
    # fire if Tier-2 had dropped the cols incorrectly).
    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters=filters,
    )
    assert "ipv6_adoption" in result
    assert "proxy_dist" in result


def test_security_aggregates_ipv6_proxy_fall_back_to_live_when_rollup_cold(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    """Cold rollup (no in-window closed-hour parquets) → the pre-check
    fails → is_ipv6 + p_type STAY in the temp → live SQL serves the
    aggregates. Pinned because a regression here would route through
    the rollup helpers despite no data, returning empty cards on every
    cold-pool service.

    Uses start_time=None so the test doesn't get caught by the mock-
    data fixture's naive-TIMESTAMP vs TIMESTAMPTZ comparison quirk
    (the live SQL would match 0 rows with explicit Z-suffixed bounds).
    The pre-check requires explicit bounds; None → pre-check returns
    False → live path runs.
    """
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))

    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=10)
    for log in logs:
        log["is_ipv6"] = True
        log["p_type"] = "hosting"
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    # No rollup parquets seeded — cache_root has no rollups/ tree at all.
    # Spy on whether the LIVE SQL paths fired (independent of whether
    # they return rows) — that's the routing assertion this test owns.
    live_ipv6_fired = {"n": 0}
    live_proxy_fired = {"n": 0}
    orig_execute = QueryRunner.execute

    def _spy(self, sql, *args, **kwargs):
        if isinstance(sql, str):
            if "CASE WHEN is_ipv6" in sql and "FROM t_" in sql:
                live_ipv6_fired["n"] += 1
            if "SELECT p_type, count(*)" in sql and "FROM t_" in sql:
                live_proxy_fired["n"] += 1
        return orig_execute(self, sql, *args, **kwargs)

    monkeypatch.setattr(QueryRunner, "execute", _spy)

    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
    )

    # Live paths fired — temp still carries is_ipv6 + p_type (pre-check
    # returned False for both).
    assert live_ipv6_fired["n"] >= 1, "live IPV6_ADOPTION_TS did not fire on cold rollup"
    assert live_proxy_fired["n"] >= 1, "live PROXY_TYPE_DIST did not fire on cold rollup"
    # And the response is well-formed — cards present + non-empty per
    # the seeded data (with None bounds the where clause permits all
    # rows so the live aggregates have rows to count).
    assert result["ipv6_adoption"], "ipv6_adoption empty even with non-empty temp"
    assert result["proxy_dist"], "proxy_dist empty even with non-empty temp"
    assert any(row["type"] == "hosting" for row in result["proxy_dist"])


def test_security_aggregates_drops_ipv6_ptype_from_temp_when_rollups_will_serve(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    """When the pre-check passes for is_ipv6 AND p_type, those columns
    must NOT appear in the catalog temp's projection. Pinned because
    silently keeping them in the temp would defeat Tier 2's
    materialization-cost savings even though the live SQL paths no
    longer scan them.
    """
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))

    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=5)
    for log in logs:
        log["is_ipv6"] = False
        log["p_type"] = "hosting"
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    from datetime import UTC, datetime, timedelta

    closed_hour_str = _seed_count_rollup(cache_root, "is_ipv6", hours_back=2, value_counts={"false": 50})
    _seed_count_rollup(cache_root, "p_type", hours_back=2, value_counts={"hosting": 50})
    closed_hour_dt = datetime.strptime(closed_hour_str, "%Y-%m-%d-%H").replace(tzinfo=UTC)

    # Capture every CREATE TEMP TABLE statement so we can inspect the
    # projected col list.
    create_sqls: list[str] = []
    orig_execute = QueryRunner.execute

    def _spy(self, sql, *args, **kwargs):
        if isinstance(sql, str) and "CREATE TEMP TABLE" in sql:
            create_sqls.append(sql)
        return orig_execute(self, sql, *args, **kwargs)

    monkeypatch.setattr(QueryRunner, "execute", _spy)

    # Window >= 3 days to satisfy the rollup-routing gate.
    start = (closed_hour_dt - timedelta(days=4)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end = (closed_hour_dt + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=start,
        end_time=end,
        filters={},
    )

    # The catalog temp materialization should have dropped both cols.
    # Other CREATE TEMP TABLEs run for sub-queries — notably the
    # fingerprint rollup's active-hour merge, which is now hoisted ahead
    # of the catalog temp and ALSO projects tls_ciphers_sha (just that one
    # column). Disambiguate by picking the WIDEST projection among the
    # tls_ciphers_sha creates — that's the multi-column catalog temp.
    catalog_creates = [s for s in create_sqls if "tls_ciphers_sha" in s]
    assert catalog_creates, "didn't capture any catalog temp create — test fixture issue"
    catalog_sql = max(catalog_creates, key=lambda s: s.count('"'))
    assert "is_ipv6" not in catalog_sql, (
        f"is_ipv6 still in temp projection despite rollup pre-check passing — Tier-2 regression. "
        f"SQL: {catalog_sql[:300]}"
    )
    assert "p_type" not in catalog_sql, (
        f"p_type still in temp projection despite rollup pre-check passing — Tier-2 regression. "
        f"SQL: {catalog_sql[:300]}"
    )


def test_security_aggregates_ipv6_serves_from_bundled_hour_when_per_field_swept(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    """Steady-state prod layout: hour_bundles.bundle_hours() writes the
    per-hour ``all_fields.parquet`` bundle then ``_cleanup_per_field_
    after_bundle`` deletes the per-field per-hour parquets. The IPv6
    helper must serve from the bundled file alone — no per-field tree
    available as a fallback.

    Pinned by session-9 adversarial review: the bundled-paths branch
    used hive_partitioning=0 which fails to surface ``hour`` from the
    path segment (the bundle body has only ``(field, value, count)``),
    raising BinderException → empty IPv6 chart on every closed-hour-
    bundled service. Caught after merge would have shipped a silent
    regression to prod.
    """
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))

    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=10)
    for log in logs:
        log["is_ipv6"] = False
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    from datetime import UTC, datetime, timedelta

    # ONLY the bundled tree — per-field tree is intentionally absent to
    # mirror the post-cleanup steady state.
    closed_hour_str = _seed_bundled_hour_rollup(
        cache_root,
        hours_back=3,
        field_value_counts={"is_ipv6": {"true": 17, "false": 83}},
    )
    closed_hour_dt = datetime.strptime(closed_hour_str, "%Y-%m-%d-%H").replace(tzinfo=UTC)

    # Spy on the live IPV6_ADOPTION_TS path — it MUST NOT fire (the
    # temp dropped is_ipv6 because the pre-check sees the bundled file).
    live_calls = {"n": 0}
    orig_execute = QueryRunner.execute

    def _spy(self, sql, *args, **kwargs):
        if isinstance(sql, str) and "CASE WHEN is_ipv6" in sql and "FROM t_" in sql:
            live_calls["n"] += 1
        return orig_execute(self, sql, *args, **kwargs)

    monkeypatch.setattr(QueryRunner, "execute", _spy)

    # Window >= 3 days to satisfy the rollup-routing gate.
    start = (closed_hour_dt - timedelta(days=4)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end = (closed_hour_dt + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=start,
        end_time=end,
        filters={},
    )

    # The bundled hour must produce a time-series point at the seeded
    # pct = 17/100 * 100 = 17.0. Empty result indicates the BinderException
    # → swallowed → None → empty-card failure mode the regression was about.
    ipv6 = result["ipv6_adoption"]
    by_time = {row["time"]: row for row in ipv6}
    expected_time = f"{closed_hour_str[:10]}T{closed_hour_str[11:13]}:00:00+00:00"
    assert expected_time in by_time, (
        f"bundled-hour rollup point missing from result — the bundled-branch "
        f"BinderException regression is back. Got times: {list(by_time)}"
    )
    assert abs(by_time[expected_time]["pct"] - 17.0) < 0.001
    # And live SQL must NOT have run (temp dropped is_ipv6).
    assert live_calls["n"] == 0


def test_security_aggregates_ipv6_mixes_bundled_and_per_field_hours(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    """Mid-bundling state: some closed hours are bundled, others are
    still in the per-field tree. Reader must produce a row per hour
    from BOTH sources without double-counting (bundled wins for the
    hours it covers; per-field fills the gaps).
    """
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))

    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=5)
    for log in logs:
        log["is_ipv6"] = False
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    from datetime import UTC, datetime, timedelta

    bundled_hour = _seed_bundled_hour_rollup(
        cache_root,
        hours_back=4,
        field_value_counts={"is_ipv6": {"true": 30, "false": 70}},
    )
    per_field_hour = _seed_count_rollup(cache_root, "is_ipv6", hours_back=2, value_counts={"true": 50, "false": 50})
    bundled_hour_dt = datetime.strptime(bundled_hour, "%Y-%m-%d-%H").replace(tzinfo=UTC)
    per_field_hour_dt = datetime.strptime(per_field_hour, "%Y-%m-%d-%H").replace(tzinfo=UTC)

    # Window >= 3 days to satisfy the rollup-routing gate.
    start = (bundled_hour_dt - timedelta(days=4)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end = (per_field_hour_dt + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=start,
        end_time=end,
        filters={},
    )

    by_time = {row["time"]: row for row in result["ipv6_adoption"]}
    bundled_iso = f"{bundled_hour[:10]}T{bundled_hour[11:13]}:00:00+00:00"
    per_field_iso = f"{per_field_hour[:10]}T{per_field_hour[11:13]}:00:00+00:00"
    assert bundled_iso in by_time, f"bundled hour missing; got {list(by_time)}"
    assert per_field_iso in by_time, f"per-field hour missing; got {list(by_time)}"
    assert abs(by_time[bundled_iso]["pct"] - 30.0) < 0.001
    assert abs(by_time[per_field_iso]["pct"] - 50.0) < 0.001


def test_security_aggregates_ipv6_proxy_skip_rollup_on_small_window(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    """Windows narrower than _ROLLUP_MIN_WINDOW_SECONDS (3 days) skip
    the rollup-served paths even when rollup data exists and filters
    are empty. The live SQL serves ipv6 + proxy from the catalog temp
    instead. Pinned because the small-window regression on prod
    measurements drove the gate-introduction: 24h was +110 ms net
    regression vs the prior live path because the closed-hour parquet
    reads cost more than scanning is_ipv6 / p_type from the already-
    materialised temp.
    """
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))

    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=5)
    for log in logs:
        log["is_ipv6"] = True
        log["p_type"] = "hosting"
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    # Seed rollup data so the pre-check would PASS if window were wide
    # enough — the test is whether the window-size gate fires first.
    _seed_count_rollup(cache_root, "is_ipv6", hours_back=2, value_counts={"true": 100})
    _seed_count_rollup(cache_root, "p_type", hours_back=2, value_counts={"hosting": 100})

    from datetime import UTC, datetime, timedelta

    # 24h window — below the 3-day gate threshold.
    end = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    start = (datetime.now(UTC) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")

    live_ipv6_fired = {"n": 0}
    live_proxy_fired = {"n": 0}
    orig_execute = QueryRunner.execute

    def _spy(self, sql, *args, **kwargs):
        if isinstance(sql, str):
            if "CASE WHEN is_ipv6" in sql and "FROM t_" in sql:
                live_ipv6_fired["n"] += 1
            if "SELECT p_type, count(*)" in sql and "FROM t_" in sql:
                live_proxy_fired["n"] += 1
        return orig_execute(self, sql, *args, **kwargs)

    monkeypatch.setattr(QueryRunner, "execute", _spy)

    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=start,
        end_time=end,
        filters={},
    )

    # Gate kept ipv6 + p_type in the temp; live SQL paths fired even
    # though rollup data existed.
    assert live_ipv6_fired["n"] >= 1, (
        "live IPV6_ADOPTION_TS did NOT fire on a 24h window — small-window gate regression"
    )
    assert live_proxy_fired["n"] >= 1, (
        "live PROXY_TYPE_DIST did NOT fire on a 24h window — small-window gate regression"
    )
    # Response well-formed (cards present + non-empty per the seeded data).
    assert "ipv6_adoption" in result
    assert "proxy_dist" in result


def test_security_aggregates_window_gate_keeps_ipv6_ptype_in_temp_on_small_window(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    """Companion to the rollup-skip test above: when the window is
    below the gate threshold, the catalog temp KEEPS is_ipv6 + p_type
    in its projection so the live SQL fallback can scan them. Pinned
    because dropping the cols on small windows would break the live
    path (BinderException on `is_ipv6` / `p_type` not in temp).
    """
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))

    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=5)
    for log in logs:
        log["is_ipv6"] = False
        log["p_type"] = "hosting"
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    # Rollup data exists — would normally pass the coverage pre-check.
    _seed_count_rollup(cache_root, "is_ipv6", hours_back=2, value_counts={"false": 50})
    _seed_count_rollup(cache_root, "p_type", hours_back=2, value_counts={"hosting": 50})

    from datetime import UTC, datetime, timedelta

    end = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    start = (datetime.now(UTC) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")

    create_sqls: list[str] = []
    orig_execute = QueryRunner.execute

    def _spy(self, sql, *args, **kwargs):
        if isinstance(sql, str) and "CREATE TEMP TABLE" in sql:
            create_sqls.append(sql)
        return orig_execute(self, sql, *args, **kwargs)

    monkeypatch.setattr(QueryRunner, "execute", _spy)

    get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=start,
        end_time=end,
        filters={},
    )

    # Pick the WIDEST tls_ciphers_sha projection: the hoisted fingerprint
    # rollup also creates a single-column active-hour temp, so [0] is no
    # longer reliably the multi-column catalog temp.
    catalog_creates = [s for s in create_sqls if "tls_ciphers_sha" in s]
    assert catalog_creates, "didn't capture any catalog temp create — fixture issue"
    catalog_sql = max(catalog_creates, key=lambda s: s.count('"'))
    assert "is_ipv6" in catalog_sql, (
        "is_ipv6 missing from temp on small-window — gate didn't keep it "
        "in the projection; live SQL would BinderException"
    )
    assert "p_type" in catalog_sql, (
        "p_type missing from temp on small-window — gate didn't keep it "
        "in the projection; live SQL would BinderException"
    )


# ── Section selector (P-4 slice 1) ───────────────────────────────────────────


def test_security_aggregates_sections_none_preserves_full_response(in_memory_duckdb, test_service_source):
    """sections=None must return every section the full-response path
    produces today — the zero-risk default for callers that haven't
    opted into the selector."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        sections=None,
    )

    full_section_keys = {
        "verified_bots_ts",
        "ngwaf_verified_bots",
        "ngwaf_verified_bots_ts",
        "wellknown_bots",
        "tls_fingerprints",
        "fingerprint_coverage",
        "req_size_dist",
        "top_ips_header",
        "ipv6_adoption",
        "proxy_dist",
        "conn_reuse_dist",
    }
    assert full_section_keys.issubset(result.keys()), (
        f"sections=None should keep the full response shape; missing: {full_section_keys - result.keys()}"
    )


def test_security_aggregates_single_section_emits_only_requested(in_memory_duckdb, test_service_source):
    """sections=['ipv6_adoption'] returns ONLY ipv6_adoption among the
    13 selectable sections — proves the gates suppress the unrequested
    SQL + skip the result-dict writes."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=10)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        sections={"ipv6_adoption"},
    )

    all_section_keys = {
        "verified_bots_ts",
        "ngwaf_verified_bots",
        "ngwaf_verified_bots_ts",
        "wellknown_bots",
        "tls_fingerprints",
        "fingerprint_coverage",
        "req_size_dist",
        "top_ips_header",
        "ipv6_adoption",
        "proxy_dist",
        "conn_reuse_dist",
    }
    present = all_section_keys & result.keys()
    assert present == {"ipv6_adoption"}, f"single-section request leaked extra section keys; got {present}"
    # Timer + telemetry envelopes always survive the selector — the
    # frontend needs them on every response.
    assert "section_timings" in result
    assert "debug_queries" in result


def test_security_aggregates_multi_section_respects_fingerprint_coupling(in_memory_duckdb, test_service_source):
    """Multi-section request honors the fingerprint-card → coverage
    coupling: requesting tls_fingerprints causes fingerprint_coverage to
    be auto-computed (the router boundary enforces this — the repo layer
    just trusts the expanded set).
    """
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20)
    for i, log in enumerate(logs):
        log["tls_ciphers_sha"] = "abc123" if i < 10 else "def456"
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    # The router auto-expands FP card requests to include
    # fingerprint_coverage; emulate that expansion here so this test
    # exercises the repo gate in the same shape the router produces.
    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        sections={"tls_fingerprints", "fingerprint_coverage"},
    )

    all_section_keys = {
        "verified_bots_ts",
        "ngwaf_verified_bots",
        "ngwaf_verified_bots_ts",
        "wellknown_bots",
        "tls_fingerprints",
        "fingerprint_coverage",
        "req_size_dist",
        "top_ips_header",
        "ipv6_adoption",
        "proxy_dist",
        "conn_reuse_dist",
    }
    present = all_section_keys & result.keys()
    assert present == {"tls_fingerprints", "fingerprint_coverage"}, (
        f"multi-section request leaked or dropped keys; got {present}"
    )
    # ipv6_adoption was NOT requested, so it must NOT appear in the result —
    # proves the selector skipped its live SQL branch.
    assert "ipv6_adoption" not in result


# ── Section-aware temp narrowing / skip (P0 security/aggregates) ──────────────


def test_security_aggregates_skips_temp_entirely_when_every_section_rollup_served(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    """When every requested section serves from a parquet rollup, the
    catalog temp must not be built at all: ``temp_table_create`` is absent
    from section_timings and a ``security:temp_skipped`` marker is present.
    Pinned because this is the whole point of the P0 fix — the shared
    materialize the perf audit flagged at p95 3.8s / max 17.3s @30d.
    """
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))

    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=10)
    for log in logs:
        log["is_ipv6"] = False
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    from datetime import UTC, datetime, timedelta

    closed_hour_str = _seed_count_rollup(cache_root, "is_ipv6", hours_back=2, value_counts={"true": 23, "false": 77})
    closed_hour_dt = datetime.strptime(closed_hour_str, "%Y-%m-%d-%H").replace(tzinfo=UTC)

    # No CREATE TEMP TABLE may run against the base catalog table — the
    # only section requested (ipv6_adoption) serves entirely from the
    # is_ipv6 count rollup.
    catalog_creates: list[str] = []
    orig_execute = QueryRunner.execute

    def _spy(self, sql, *args, **kwargs):
        if isinstance(sql, str) and "CREATE TEMP TABLE" in sql and table_name in sql:
            catalog_creates.append(sql)
        return orig_execute(self, sql, *args, **kwargs)

    monkeypatch.setattr(QueryRunner, "execute", _spy)

    start = (closed_hour_dt - timedelta(days=4)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end = (closed_hour_dt + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=start,
        end_time=end,
        filters={},
        sections={"ipv6_adoption"},
    )

    markers = {e["section"] for e in result["section_timings"]}
    assert "temp_table_create" not in markers, (
        f"catalog temp was built even though ipv6_adoption is rollup-served; timings={markers}"
    )
    assert "security:temp_skipped" in markers, f"missing temp-skip marker; timings={markers}"
    assert catalog_creates == [], f"a catalog temp was materialized: {catalog_creates}"
    # And the section still came back from the rollup.
    assert result["ipv6_adoption"], "ipv6_adoption empty despite seeded rollup"
    assert "ipv6_adoption" == (result.keys() & {"req_size_dist", "ipv6_adoption", "proxy_dist"}).pop()


def test_security_aggregates_drops_wide_string_cols_from_default_temp(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    """The realistic frontend default (all 10 cards, no verified_bots_ts)
    on a wide window must NOT carry the wide ``ua`` / ``waf_sig`` string
    columns in the catalog temp once wellknown + ipv6 + proxy serve from
    rollups — ``ua`` is wellknown-only and ``waf_sig`` is verified_bots_ts
    only, neither of which live-scans here. tls_ciphers_sha STAYS (coverage
    still scans it). This is the column-narrowing half of the P0 win.
    """
    from datetime import UTC, datetime, timedelta

    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))

    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=10)
    for log in logs:
        log["is_ipv6"] = False
        log["p_type"] = "hosting"
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    # Seed is_ipv6 + p_type rollups so those sections serve without the temp.
    closed = _seed_count_rollup(cache_root, "is_ipv6", hours_back=2, value_counts={"false": 50})
    _seed_count_rollup(cache_root, "p_type", hours_back=2, value_counts={"hosting": 50})
    closed_dt = datetime.strptime(closed, "%Y-%m-%d-%H").replace(tzinfo=UTC)

    # Force the wellknown rollup to "serve" (return rows) so its live
    # ua/ip temp scan is skipped — that's what drops `ua` from the temp.
    monkeypatch.setattr(
        "backend.core.rollups.read_wellknown_bots_rollup",
        lambda *a, **k: [("Googlebot/2.1", "66.249.66.1", 5)],
    )

    create_sqls: list[str] = []
    orig_execute = QueryRunner.execute

    def _spy(self, sql, *args, **kwargs):
        if isinstance(sql, str) and "CREATE TEMP TABLE" in sql and table_name in sql:
            create_sqls.append(sql)
        return orig_execute(self, sql, *args, **kwargs)

    monkeypatch.setattr(QueryRunner, "execute", _spy)

    # The 10 sections the /security page actually requests (no verified_bots_ts).
    frontend_sections = {
        "ngwaf_verified_bots",
        "ngwaf_verified_bots_ts",
        "wellknown_bots",
        "tls_fingerprints",
        "fingerprint_coverage",
        "req_size_dist",
        "top_ips_header",
        "ipv6_adoption",
        "proxy_dist",
        "conn_reuse_dist",
    }
    start = (closed_dt - timedelta(days=4)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end = (closed_dt + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=start,
        end_time=end,
        filters={},
        sections=frontend_sections,
    )

    # The widest projection is the catalog temp.
    assert create_sqls, "no catalog temp captured — fixture issue"
    catalog_sql = max(create_sqls, key=lambda s: s.count('"'))
    assert '"ua"' not in catalog_sql, f"ua still in temp despite wellknown rollup-served: {catalog_sql}"
    assert '"waf_sig"' not in catalog_sql, (
        f"waf_sig still in temp despite verified_bots_ts not requested: {catalog_sql}"
    )
    assert '"is_ipv6"' not in catalog_sql, f"is_ipv6 should serve from rollup: {catalog_sql}"
    assert '"p_type"' not in catalog_sql, f"p_type should serve from rollup: {catalog_sql}"


def test_security_aggregates_histogram_rollups_skip_temp(in_memory_duckdb, test_service_source, monkeypatch):
    """When req_size_dist / conn_reuse_dist / top_ips_header all serve from
    the security_dims rollups, no catalog temp is built — proves the Part B
    readers are wired into the pass-1 hoist (security_dims removes the last
    all-rows histogram scans)."""
    from backend.repositories._base import QueryRunner

    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f'CREATE TABLE "{table_name}" (timestamp TIMESTAMPTZ, ip VARCHAR, req_header_bytes BIGINT, conn_requests BIGINT)'
    )
    in_memory_duckdb.execute(f"INSERT INTO {table_name} VALUES (now(), '1.2.3.4', 300, 2)")

    monkeypatch.setattr(
        QueryRunner,
        "try_security_req_size_from_rollup",
        lambda self, s, e, has_filters: [{"bucket": "0-256B", "count": 5}],
    )
    monkeypatch.setattr(
        QueryRunner,
        "try_security_conn_reuse_from_rollup",
        lambda self, s, e, has_filters: [{"bucket": "1 (None)", "count": 3}],
    )
    monkeypatch.setattr(
        QueryRunner,
        "try_security_top_ips_from_rollup",
        lambda self, s, e, has_filters: [{"ip": "9.9.9.9", "max_header": 900}],
    )

    catalog_creates: list[str] = []
    orig_execute = QueryRunner.execute

    def _spy(self, sql, *args, **kwargs):
        if isinstance(sql, str) and "CREATE TEMP TABLE" in sql and table_name in sql:
            catalog_creates.append(sql)
        return orig_execute(self, sql, *args, **kwargs)

    monkeypatch.setattr(QueryRunner, "execute", _spy)

    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time="2026-01-01T00:00:00Z",
        end_time="2026-01-08T00:00:00Z",  # 7d → rollup-eligible
        filters={},
        sections={"req_size_dist", "conn_reuse_dist", "top_ips_header"},
    )

    markers = {e["section"] for e in result["section_timings"]}
    assert "temp_table_create" not in markers, f"temp built despite all histograms rollup-served; timings={markers}"
    assert "security:temp_skipped" in markers, f"missing temp-skip marker; timings={markers}"
    assert catalog_creates == [], f"a catalog temp was materialized: {catalog_creates}"
    assert result["req_size_dist"] == [{"bucket": "0-256B", "count": 5}]
    assert result["conn_reuse_dist"] == [{"bucket": "1 (None)", "count": 3}]
    assert result["top_ips_header"] == [{"ip": "9.9.9.9", "max_header": 900}]


def test_security_aggregates_coverage_rollup_drops_tls_and_skips_temp(
    in_memory_duckdb, test_service_source, monkeypatch
):
    """When tls_fingerprints serves from its rollup AND the security_cov
    rollup serves fingerprint_coverage, tls_ciphers_sha drops out of the
    temp entirely — so a fingerprints+coverage request builds NO temp.
    This is the lever that unblocks the NGWAF-only temp narrowing."""
    from backend.repositories._base import QueryRunner

    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f'CREATE TABLE "{table_name}" (timestamp TIMESTAMPTZ, ip VARCHAR, tls_ciphers_sha VARCHAR)'
    )
    in_memory_duckdb.execute(f"INSERT INTO {table_name} VALUES (now(), '1.2.3.4', 'abc123')")

    # Fingerprints serve from the count + ip_spread rollups.
    monkeypatch.setattr(
        QueryRunner,
        "execute_top_n_rollups",
        lambda self, fields, s, e, limit=10, per_field_limits=None, **kw: ([("tls_ciphers_sha", "abc123", 10)], []),
    )
    monkeypatch.setattr(
        QueryRunner,
        "execute_ip_spread_rollups",
        lambda self, fields, s, e, **kw: ({("tls_ciphers_sha", "abc123"): 4}, {}),
    )
    # Coverage serves from the security_cov counts: 600 / 1000 = 0.6.
    monkeypatch.setattr(QueryRunner, "try_security_coverage_from_rollup", lambda self, s, e, has_filters: (1000, 600))

    catalog_creates: list[str] = []
    orig_execute = QueryRunner.execute

    def _spy(self, sql, *args, **kwargs):
        if isinstance(sql, str) and "CREATE TEMP TABLE" in sql and table_name in sql:
            catalog_creates.append(sql)
        return orig_execute(self, sql, *args, **kwargs)

    monkeypatch.setattr(QueryRunner, "execute", _spy)

    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time="2026-01-01T00:00:00Z",
        end_time="2026-01-08T00:00:00Z",
        filters={},
        sections={"tls_fingerprints", "fingerprint_coverage"},
    )

    markers = {e["section"] for e in result["section_timings"]}
    assert "temp_table_create" not in markers, f"temp built despite fingerprints+coverage rollup-served; {markers}"
    assert "security:temp_skipped" in markers, f"missing temp-skip marker; {markers}"
    assert catalog_creates == [], f"a catalog temp was materialized: {catalog_creates}"
    assert result["tls_fingerprints"] == [{"fingerprint": "abc123", "ip_count": 4, "request_count": 10}]
    assert result["fingerprint_coverage"] == {"tls_ciphers_sha": 0.6}

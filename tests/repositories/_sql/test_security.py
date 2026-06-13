"""Template-render tests for `backend.repositories._sql.security`.

Phase 5a — string-level renders only (no DuckDB needed). For each
template constant we assert the rendered output contains the expected
fragments and pin the exact set of format placeholders.
"""

from __future__ import annotations

from backend.repositories._sql import security as SQL


def _placeholders(template: str) -> list[str]:
    """Return the sorted unique list of ``{name}`` placeholders in ``template``."""
    names = {p.split("}")[0] for p in template.split("{")[1:] if "}" in p}
    return sorted(names)


# ── TOP_UAS_BY_COUNT ──────────────────────────────────────────────────────────


def test_top_uas_by_count_renders_with_temp_table():
    rendered = SQL.TOP_UAS_BY_COUNT.format(temp_table="t_filtered_xyz")
    assert "SELECT ua, count(*) AS cnt" in rendered
    assert "FROM t_filtered_xyz" in rendered
    assert "WHERE ua IS NOT NULL" in rendered
    assert "GROUP BY ua" in rendered
    assert "ORDER BY cnt DESC" in rendered
    assert "LIMIT 50000" in rendered


def test_top_uas_by_count_pins_placeholders():
    assert _placeholders(SQL.TOP_UAS_BY_COUNT) == ["temp_table"]


# ── NGWAF_TOP_BOTS_JOIN ───────────────────────────────────────────────────────


def test_ngwaf_top_bots_join_renders_with_temp_table_and_n():
    rendered = SQL.NGWAF_TOP_BOTS_JOIN.format(temp_table="t_filtered_xyz", n=15)
    assert "SELECT nb.bot_name, nb.category, count(*) AS cnt" in rendered
    assert "FROM t_filtered_xyz t" in rendered
    assert "INNER JOIN ngwaf_top.ngwaf_bots nb USING (waf_req_id)" in rendered
    assert "WHERE nb.bot_name IS NOT NULL" in rendered
    assert "LIMIT 15" in rendered


def test_ngwaf_top_bots_join_pins_placeholders():
    assert _placeholders(SQL.NGWAF_TOP_BOTS_JOIN) == ["n", "temp_table"]


# ── VERIFIED_BOTS_TS ──────────────────────────────────────────────────────────


def test_verified_bots_ts_renders_with_bucket_and_temp_table():
    rendered = SQL.VERIFIED_BOTS_TS.format(bucket_seconds=300, temp_table="t_filtered_xyz")
    assert "time_bucket(INTERVAL '300 seconds', timestamp)" in rendered
    assert "replace(tag, 'VERIFIED-BOT.', '')" in rendered
    assert "FROM t_filtered_xyz" in rendered
    assert "WHERE waf_sig IS NOT NULL AND waf_sig ILIKE '%VERIFIED-BOT.%'" in rendered
    assert "GROUP BY 1, 2" in rendered
    assert "ORDER BY 1, 2" in rendered


def test_verified_bots_ts_pins_placeholders():
    assert _placeholders(SQL.VERIFIED_BOTS_TS) == ["bucket_seconds", "temp_table"]


# ── NGWAF_VERIFIED_BOTS ───────────────────────────────────────────────────────


def test_ngwaf_verified_bots_renders_with_temp_table():
    rendered = SQL.NGWAF_VERIFIED_BOTS.format(temp_table="t_filtered_xyz")
    assert "nb.bot_name" in rendered
    assert "nb.wellknown_bot_name" in rendered
    assert "nb.category" in rendered
    assert "count(*) AS request_count" in rendered
    assert "FROM t_filtered_xyz t" in rendered
    assert "INNER JOIN ngwaf_cache.ngwaf_bots nb USING (waf_req_id)" in rendered
    assert "GROUP BY 1, 2, 3" in rendered
    assert "ORDER BY 4 DESC" in rendered


def test_ngwaf_verified_bots_pins_placeholders():
    assert _placeholders(SQL.NGWAF_VERIFIED_BOTS) == ["temp_table"]


# ── NGWAF_VERIFIED_BOTS_TS ────────────────────────────────────────────────────


def test_ngwaf_verified_bots_ts_renders_with_bucket_and_temp_table():
    rendered = SQL.NGWAF_VERIFIED_BOTS_TS.format(bucket_seconds=60, temp_table="t_filtered_xyz")
    assert "time_bucket(INTERVAL '60 seconds', t.timestamp)" in rendered
    assert "FROM t_filtered_xyz t" in rendered
    assert "INNER JOIN ngwaf_cache.ngwaf_bots nb USING (waf_req_id)" in rendered
    assert "WHERE nb.bot_name IS NOT NULL" in rendered
    assert "GROUP BY 1, 2" in rendered


def test_ngwaf_verified_bots_ts_pins_placeholders():
    assert _placeholders(SQL.NGWAF_VERIFIED_BOTS_TS) == ["bucket_seconds", "temp_table"]


# ── TLS_FINGERPRINTS ──────────────────────────────────────────────────────────


def test_tls_fingerprints_renders_with_temp_table():
    rendered = SQL.TLS_FINGERPRINTS.format(temp_table="t_filtered_xyz")
    assert "SELECT tls_ciphers_sha" in rendered
    assert "count(DISTINCT ip) as ip_count" in rendered
    assert "count(*) as req_count" in rendered
    assert "FROM t_filtered_xyz" in rendered
    assert "WHERE tls_ciphers_sha IS NOT NULL" in rendered
    assert "GROUP BY 1 ORDER BY 3 DESC LIMIT 20" in rendered


def test_tls_fingerprints_pins_placeholders():
    assert _placeholders(SQL.TLS_FINGERPRINTS) == ["temp_table"]


# ── REQ_HEADER_SIZE_DIST ──────────────────────────────────────────────────────


def test_req_header_size_dist_renders_with_temp_table():
    rendered = SQL.REQ_HEADER_SIZE_DIST.format(temp_table="t_filtered_xyz")
    assert "WHEN req_header_bytes <= 256 THEN '0-256B'" in rendered
    assert "WHEN req_header_bytes <= 32768 THEN '24-32KB'" in rendered
    assert "ELSE '>32KB'" in rendered
    assert "MIN(req_header_bytes) as min_val" in rendered
    assert "FROM t_filtered_xyz" in rendered
    assert "WHERE req_header_bytes IS NOT NULL" in rendered
    assert "GROUP BY 1 ORDER BY min_val" in rendered


def test_req_header_size_dist_pins_placeholders():
    assert _placeholders(SQL.REQ_HEADER_SIZE_DIST) == ["temp_table"]


# ── TOP_IPS_BY_MAX_HEADER ─────────────────────────────────────────────────────


def test_top_ips_by_max_header_renders_with_temp_table():
    rendered = SQL.TOP_IPS_BY_MAX_HEADER.format(temp_table="t_filtered_xyz")
    assert "SELECT ip, MAX(req_header_bytes) as max_header" in rendered
    assert "FROM t_filtered_xyz" in rendered
    assert "WHERE ip IS NOT NULL AND req_header_bytes IS NOT NULL" in rendered
    assert "GROUP BY 1 ORDER BY 2 DESC LIMIT 10" in rendered


def test_top_ips_by_max_header_pins_placeholders():
    assert _placeholders(SQL.TOP_IPS_BY_MAX_HEADER) == ["temp_table"]


# ── IPV6_ADOPTION_TS ──────────────────────────────────────────────────────────


def test_ipv6_adoption_ts_renders_with_time_bucket_and_temp_table():
    rendered = SQL.IPV6_ADOPTION_TS.format(
        time_bucket_select="time_bucket(INTERVAL '1 hour', timestamp) AS bucket",
        temp_table="t_filtered_xyz",
    )
    assert "time_bucket(INTERVAL '1 hour', timestamp) AS bucket" in rendered
    assert "SUM(CASE WHEN is_ipv6 THEN 1 ELSE 0 END) * 100.0 / count(*) as ipv6_pct" in rendered
    assert "FROM t_filtered_xyz" in rendered
    assert "GROUP BY 1 ORDER BY 1" in rendered


def test_ipv6_adoption_ts_pins_placeholders():
    assert _placeholders(SQL.IPV6_ADOPTION_TS) == ["temp_table", "time_bucket_select"]


# ── PROXY_TYPE_DIST ───────────────────────────────────────────────────────────


def test_proxy_type_dist_renders_with_temp_table():
    rendered = SQL.PROXY_TYPE_DIST.format(temp_table="t_filtered_xyz")
    assert "SELECT p_type, count(*) as count" in rendered
    assert "FROM t_filtered_xyz" in rendered
    assert "WHERE p_type IS NOT NULL AND p_type != ''" in rendered
    assert "GROUP BY 1 ORDER BY 2 DESC" in rendered


def test_proxy_type_dist_pins_placeholders():
    assert _placeholders(SQL.PROXY_TYPE_DIST) == ["temp_table"]


# ── CONN_REUSE_DIST ───────────────────────────────────────────────────────────


def test_conn_reuse_dist_renders_with_temp_table():
    rendered = SQL.CONN_REUSE_DIST.format(temp_table="t_filtered_xyz")
    assert "WHEN conn_requests = 1 THEN '1 (None)'" in rendered
    assert "WHEN conn_requests <= 5 THEN '2-5'" in rendered
    assert "WHEN conn_requests <= 100 THEN '21-100'" in rendered
    assert "ELSE '>100'" in rendered
    assert "MIN(conn_requests) as min_val" in rendered
    assert "FROM t_filtered_xyz" in rendered
    assert "WHERE conn_requests IS NOT NULL AND conn_requests > 0" in rendered
    assert "GROUP BY 1 ORDER BY min_val" in rendered


def test_conn_reuse_dist_pins_placeholders():
    assert _placeholders(SQL.CONN_REUSE_DIST) == ["temp_table"]


# ── WELLKNOWN_BOTS_UA_IP ──────────────────────────────────────────────────────


def test_wellknown_bots_ua_ip_renders_with_minimal_prefilter():
    rendered = SQL.WELLKNOWN_BOTS_UA_IP.format(
        temp_table="t_filtered_xyz",
        prefilter="WHERE ua IS NOT NULL AND ip IS NOT NULL",
    )
    assert "SELECT ua, ip, count(*) AS cnt" in rendered
    assert "FROM t_filtered_xyz" in rendered
    assert "WHERE ua IS NOT NULL AND ip IS NOT NULL" in rendered
    assert "GROUP BY ua, ip" in rendered
    assert "ORDER BY cnt DESC" in rendered
    assert "LIMIT 10000" in rendered


def test_wellknown_bots_ua_ip_renders_with_regex_prefilter():
    prefilter = "WHERE ua IS NOT NULL AND ip IS NOT NULL AND regexp_matches(ua, '(googlebot|bingbot)')"
    rendered = SQL.WELLKNOWN_BOTS_UA_IP.format(
        temp_table="t_filtered_xyz",
        prefilter=prefilter,
    )
    assert "regexp_matches(ua, '(googlebot|bingbot)')" in rendered
    assert "FROM t_filtered_xyz" in rendered


def test_wellknown_bots_ua_ip_pins_placeholders():
    assert _placeholders(SQL.WELLKNOWN_BOTS_UA_IP) == ["prefilter", "temp_table"]

"""SQL templates for `backend.repositories.security`.

Phase 5a extraction. Per-template inputs documented inline; non-trusted
values are bound via DuckDB ``?`` parameters (or already inlined upstream
into ``where_clause`` by ``build_where_clause(inline_params=True)``),
never interpolated from user input here.

See ``backend/repositories/_sql/__init__.py`` for the ownership policy.
"""

from __future__ import annotations

# ── Top bots (get_top_bots) ───────────────────────────────────────────────────

TOP_UAS_BY_COUNT = """
                    SELECT ua, count(*) AS cnt
                    FROM {temp_table}
                    WHERE ua IS NOT NULL
                    GROUP BY ua
                    ORDER BY cnt DESC
                    LIMIT 50000
                """
"""Top distinct UAs by request count over the filtered temp table.

Inputs (trusted-identifier substitutions only):
- ``{temp_table}`` — name of the filtered TEMP TABLE (built by
  ``QueryRunner.temp_table``).

Output rows: ``(ua: str, cnt: int)`` — fed to ``build_matcher()`` in
Python for arcjet bot classification.
"""

NGWAF_TOP_BOTS_JOIN = """
                    SELECT nb.bot_name, nb.category, count(*) AS cnt
                    FROM {temp_table} t
                    INNER JOIN ngwaf_top.ngwaf_bots nb USING (waf_req_id)
                    WHERE nb.bot_name IS NOT NULL
                    GROUP BY 1, 2
                    ORDER BY 3 DESC
                    LIMIT {n}
                """
"""NGWAF-resolved bot names joined against the filtered temp table.

Inputs (trusted-identifier substitutions only):
- ``{temp_table}`` — filtered TEMP TABLE built by ``QueryRunner.temp_table``.
- ``{n}`` — integer LIMIT (caller passes an in-range Python ``int``).

Output rows: ``(bot_name: str, category: str, cnt: int)``.

Requires the SQLite ``ngwaf_top`` database to be ATTACHed (done by the
caller before invoking).
"""

NGWAF_TOP_BOTS_JOIN_DIRECT = """
                    SELECT nb.bot_name, nb.category, count(*) AS cnt
                    FROM (
                        SELECT waf_req_id FROM {table_name}
                        WHERE {where_clause} AND waf_req_id IS NOT NULL
                    ) t
                    INNER JOIN ngwaf_top.ngwaf_bots nb USING (waf_req_id)
                    WHERE nb.bot_name IS NOT NULL
                    GROUP BY 1, 2
                    ORDER BY 3 DESC
                    LIMIT {n}
                """
"""``NGWAF_TOP_BOTS_JOIN`` joined straight against the base table.

Used when the join would be the temp's ONLY consumer (the UA branch was
rollup-served): materializing the window's waf_req_ids to probe them once
is pure overhead, so this variant scans the base table exactly once. The
``waf_req_id IS NOT NULL`` floor is semantically identical to the INNER
JOIN (NULLs can never match) and keeps the join's build input minimal.

Inputs (trusted-identifier / trusted-fragment substitutions only):
- ``{table_name}`` — quoted/safe base table identifier.
- ``{where_clause}`` — pre-built time/filters clause from
  ``build_where_clause`` (values inlined upstream).
- ``{n}`` — integer LIMIT (caller passes an in-range Python ``int``).

Output rows: ``(bot_name: str, category: str, cnt: int)``.

Requires the SQLite ``ngwaf_top`` database to be ATTACHed (done by the
caller before invoking).
"""

# ── Verified bots time series (get_security_aggregates) ───────────────────────

VERIFIED_BOTS_TS = """
            SELECT
                time_bucket(INTERVAL '{bucket_seconds} seconds', timestamp) AS bucket,
                replace(tag, 'VERIFIED-BOT.', '') AS bot_type,
                count(*) AS count
            FROM (
                SELECT timestamp, unnest(string_split(waf_sig, ',')) AS tag
                FROM {temp_table}
                WHERE waf_sig IS NOT NULL AND waf_sig ILIKE '%VERIFIED-BOT.%'
            ) sub
            WHERE tag LIKE 'VERIFIED-BOT.%'
            GROUP BY 1, 2
            ORDER BY 1, 2
        """
"""Category-level verified-bot time series from the ``waf_sig`` tag column.

Inputs (trusted-identifier substitutions only):
- ``{bucket_seconds}`` — integer seconds per bucket (caller-validated int).
- ``{temp_table}`` — filtered TEMP TABLE name.

Output rows: ``(bucket: timestamp, bot_type: str, count: int)``.
"""

# ── NGWAF verified bots (get_security_aggregates) ─────────────────────────────

NGWAF_VERIFIED_BOTS = """
                SELECT
                    nb.bot_name,
                    nb.wellknown_bot_name,
                    nb.category,
                    count(*) AS request_count
                FROM {temp_table} t
                INNER JOIN ngwaf_cache.ngwaf_bots nb USING (waf_req_id)
                WHERE nb.bot_name IS NOT NULL
                GROUP BY 1, 2, 3
                ORDER BY 4 DESC
            """
"""NGWAF-resolved verified bots aggregated by name + category.

Inputs (trusted-identifier substitutions only):
- ``{temp_table}`` — filtered TEMP TABLE name.

Output rows: ``(bot_name, wellknown_bot_name, category, request_count)``.

Requires the SQLite ``ngwaf_cache`` database to be ATTACHed.
"""

NGWAF_VERIFIED_BOTS_TS = """
                SELECT
                    time_bucket(INTERVAL '{bucket_seconds} seconds', t.timestamp) AS bucket,
                    nb.bot_name,
                    count(*) AS count
                FROM {temp_table} t
                INNER JOIN ngwaf_cache.ngwaf_bots nb USING (waf_req_id)
                WHERE nb.bot_name IS NOT NULL
                GROUP BY 1, 2
                ORDER BY 1, 2
            """
"""Bucketed NGWAF-resolved bot counts by bot name.

Inputs (trusted-identifier substitutions only):
- ``{bucket_seconds}`` — integer seconds per bucket.
- ``{temp_table}`` — filtered TEMP TABLE name.

Output rows: ``(bucket: timestamp, bot_name: str, count: int)``.

Requires the SQLite ``ngwaf_cache`` database to be ATTACHed.
"""

# ── TLS fingerprints ──────────────────────────────────────────────────────────

FINGERPRINT_TOP_N = """
            SELECT "{col}",
                   count(DISTINCT ip) as ip_count,
                   count(*) as req_count
            FROM {temp_table}
            WHERE "{col}" IS NOT NULL AND "{col}" != ''
            GROUP BY 1 ORDER BY 3 DESC LIMIT 20
        """
"""Top-20 fingerprints for a single column, with IP spread.

Used by the fingerprint-card endpoint(s); parameterised by column name so
sibling fingerprint columns can share one template without drifting (the
prior separately-named per-column templates were byte-identical except for
the column).

Inputs (trusted-identifier substitutions only):
- ``{col}`` — fingerprint column name (``tls_ciphers_sha``).
- ``{temp_table}`` — filtered TEMP TABLE name.

Output rows: ``(<fingerprint>: str, ip_count: int, req_count: int)``.

The empty-string filter (``!= ''``) is load-bearing: the VCL emits
``""`` (not NULL) for requests whose fingerprint isn't applicable
(e.g. ``tls.client.ciphers_list_sha`` returns empty when the request
is served from a shielded PoP rather than the true edge). Without this
filter the top-N's #1 row would be an
empty-string fingerprint with the bulk of request volume — useless
for analyst-facing leaderboards.
"""

# Coverage check used to drive the FE "low coverage" hint per fingerprint card.
# Returns total_rows plus one populated_<i> aggregate per requested column in a
# single scan of the temp table. The FE uses each ratio to decide whether to
# render a "<N% of requests have this fingerprint" banner when the leaderboard
# is sparse-by-design (e.g. TLS fingerprints on a service whose traffic is
# mostly served from shielded PoPs). Replaces an earlier per-column template
# that scanned the temp table once per fingerprint card.
FINGERPRINT_COVERAGE_BULK = """
            SELECT count(*) AS total_rows, {agg_cols}
            FROM {temp_table}
        """
"""Total + per-column populated counts for any number of fingerprint columns.

Inputs:
- ``{temp_table}`` — filtered TEMP TABLE name.
- ``{agg_cols}`` — caller-built comma-joined ``count(*) FILTER (...)`` aggregates,
  one per fingerprint column. Column names come from the
  ``(tls_ciphers_sha,)`` safelist in ``_build_security_response``; no
  untrusted input reaches this template.

Output: one row ``(total_rows, populated_0, populated_1, …)`` aligned with
the order the caller passed columns.
"""

# ── Request header size distribution ──────────────────────────────────────────

REQ_HEADER_SIZE_DIST = """
            SELECT
                CASE
                    WHEN req_header_bytes <= 256 THEN '0-256B'
                    WHEN req_header_bytes <= 512 THEN '256-512B'
                    WHEN req_header_bytes <= 768 THEN '512-768B'
                    WHEN req_header_bytes <= 1024 THEN '768B-1KB'
                    WHEN req_header_bytes <= 1536 THEN '1-1.5KB'
                    WHEN req_header_bytes <= 2048 THEN '1.5-2KB'
                    WHEN req_header_bytes <= 3072 THEN '2-3KB'
                    WHEN req_header_bytes <= 4096 THEN '3-4KB'
                    WHEN req_header_bytes <= 6144 THEN '4-6KB'
                    WHEN req_header_bytes <= 8192 THEN '6-8KB'
                    WHEN req_header_bytes <= 12288 THEN '8-12KB'
                    WHEN req_header_bytes <= 16384 THEN '12-16KB'
                    WHEN req_header_bytes <= 24576 THEN '16-24KB'
                    WHEN req_header_bytes <= 32768 THEN '24-32KB'
                    ELSE '>32KB'
                END as bucket,
                count(*) as count,
                MIN(req_header_bytes) as min_val
            FROM {temp_table}
            WHERE req_header_bytes IS NOT NULL
            GROUP BY 1 ORDER BY min_val
        """
"""Histogram of ``req_header_bytes`` over fixed size buckets.

Inputs (trusted-identifier substitutions only):
- ``{temp_table}`` — filtered TEMP TABLE name.

Output rows: ``(bucket: str, count: int, min_val: int)`` ordered by
bucket lower-bound.
"""

TOP_IPS_BY_MAX_HEADER = """
            SELECT ip, MAX(req_header_bytes) as max_header
            FROM {temp_table}
            WHERE ip IS NOT NULL AND req_header_bytes IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """
"""Top-10 client IPs by maximum request header size observed.

Inputs (trusted-identifier substitutions only):
- ``{temp_table}`` — filtered TEMP TABLE name.

Output rows: ``(ip: str, max_header: int)``.
"""

# ── IPv6 adoption ─────────────────────────────────────────────────────────────

IPV6_ADOPTION_TS = """
            SELECT {time_bucket_select},
                   SUM(CASE WHEN is_ipv6 THEN 1 ELSE 0 END) * 100.0 / count(*) as ipv6_pct
            FROM {temp_table}
            GROUP BY 1 ORDER BY 1
        """
"""IPv6 adoption percentage time series (hourly).

Inputs (trusted-identifier substitutions only):
- ``{time_bucket_select}`` — output of ``_base.time_bucket_select(interval)``.
- ``{temp_table}`` — filtered TEMP TABLE name.

Output rows: ``(bucket: timestamp, ipv6_pct: float)``.
"""

# ── Proxy / anonymizer ────────────────────────────────────────────────────────

PROXY_TYPE_DIST = """
            SELECT p_type, count(*) as count
            FROM {temp_table}
            WHERE p_type IS NOT NULL AND p_type != ''
            GROUP BY 1 ORDER BY 2 DESC
        """
"""Distribution of proxy/anonymizer ``p_type`` values.

Inputs (trusted-identifier substitutions only):
- ``{temp_table}`` — filtered TEMP TABLE name.

Output rows: ``(p_type: str, count: int)`` sorted by count desc.
"""

# ── Connection reuse ──────────────────────────────────────────────────────────

CONN_REUSE_DIST = """
            SELECT
                CASE
                    WHEN conn_requests = 1 THEN '1 (None)'
                    WHEN conn_requests <= 5 THEN '2-5'
                    WHEN conn_requests <= 20 THEN '6-20'
                    WHEN conn_requests <= 100 THEN '21-100'
                    ELSE '>100'
                END as bucket,
                count(*) as count,
                MIN(conn_requests) as min_val
            FROM {temp_table}
            WHERE conn_requests IS NOT NULL AND conn_requests > 0
            GROUP BY 1 ORDER BY min_val
        """
"""Distribution of per-connection request counts (connection reuse).

Inputs (trusted-identifier substitutions only):
- ``{temp_table}`` — filtered TEMP TABLE name.

Output rows: ``(bucket: str, count: int, min_val: int)`` ordered by
bucket lower-bound.
"""

# ── Well-known bots (UA + IP rollup) ──────────────────────────────────────────

WELLKNOWN_BOTS_UA_IP = """
                SELECT ua, ip, count(*) AS cnt
                FROM {temp_table}
                {prefilter}
                GROUP BY ua, ip
                ORDER BY cnt DESC
                LIMIT 10000
            """
"""Top (UA, IP) pairs by count for well-known bot classification.

Inputs (trusted-identifier / trusted-fragment substitutions only):
- ``{temp_table}`` — filtered TEMP TABLE name.
- ``{prefilter}`` — pre-built WHERE clause; either ``"WHERE ua IS NOT NULL
  AND ip IS NOT NULL"`` or that plus a ``regexp_matches`` predicate
  whose pattern comes from ``get_bot_regex_pattern`` (escaped at the
  call-site).

Output rows: ``(ua: str, ip: str, cnt: int)``.
"""

# ── VPN & Proxy Watchdog Templates ───────────────────────────────────────────

GET_PROXY_STATS = """
    WITH metrics AS (
        SELECT
            ip,
            pop,
            rtt_min / 1000.0 AS rtt_min_ms,
            lat AS client_lat,
            lon AS client_lon,
            -- Haversine formula distance calculation using a default coordinate for POP
            -- If pop is SJC, use SJC coords, else a reasonable US default or central pop fallback
            CASE
                WHEN pop = 'SJC' THEN 6371 * 2 * ASIN(SQRT(POWER(SIN(RADIANS(lat - 37.3382) / 2), 2) + COS(RADIANS(37.3382)) * COS(RADIANS(lat)) * POWER(SIN(RADIANS(lon - -121.8863) / 2), 2)))
                WHEN pop = 'IAD' THEN 6371 * 2 * ASIN(SQRT(POWER(SIN(RADIANS(lat - 38.9531) / 2), 2) + COS(RADIANS(38.9531)) * COS(RADIANS(lat)) * POWER(SIN(RADIANS(lon - -77.4565) / 2), 2)))
                WHEN pop = 'NRT' THEN 6371 * 2 * ASIN(SQRT(POWER(SIN(RADIANS(lat - 35.7720) / 2), 2) + COS(RADIANS(35.7720)) * COS(RADIANS(lat)) * POWER(SIN(RADIANS(lon - 140.3929) / 2), 2)))
                ELSE 6371 * 2 * ASIN(SQRT(POWER(SIN(RADIANS(lat - 38.0) / 2), 2) + COS(RADIANS(38.0)) * COS(RADIANS(lat)) * POWER(SIN(RADIANS(lon - -97.0) / 2), 2)))
            END AS distance_km,
            tcp_rtt
        FROM {temp_table}
    )
    SELECT
        COUNT(DISTINCT CASE
            WHEN distance_km > (rtt_min_ms * 100.0 + 150.0) THEN ip
            WHEN tcp_rtt > 0 AND (CAST(rtt_min_ms * 1000.0 AS DOUBLE) / tcp_rtt) < 0.1 THEN ip
            ELSE NULL
        END) AS active_proxies_count,
        COUNT(CASE
            WHEN distance_km > (rtt_min_ms * 100.0 + 150.0) THEN 1
            WHEN tcp_rtt > 0 AND (CAST(rtt_min_ms * 1000.0 AS DOUBLE) / tcp_rtt) < 0.1 THEN 1
            ELSE NULL
        END) AS total_requests_count,
        COUNT(DISTINCT CASE WHEN distance_km > (rtt_min_ms * 100.0 + 150.0) THEN ip ELSE NULL END) AS distance_mismatches_count
    FROM metrics
"""

GET_TRAFFIC_QUALITY = """
    SELECT
        CASE
            WHEN rtt_min IS NULL OR tcp_rtt IS NULL THEN 'Direct Connection'
            -- relativity violation or aroma score < 0.1
            WHEN tcp_rtt > 0 AND (CAST(rtt_min AS DOUBLE) / tcp_rtt) < 0.1 THEN 'Active Tunnel / Proxy'
            WHEN tcp_rtt > 0 AND (CAST(rtt_min AS DOUBLE) / tcp_rtt) < 0.7 THEN 'WiFi / Mobile'
            ELSE 'Direct Connection'
        END AS type,
        COUNT(*) AS count
    FROM {temp_table}
    GROUP BY 1
    ORDER BY count DESC
"""

GET_SUSPICIOUS_ISPS = """
    SELECT
        asn,
        COUNT(*) AS count
    FROM {temp_table}
    WHERE
        tcp_rtt > 0 AND (CAST(rtt_min AS DOUBLE) / tcp_rtt) < 0.1
    GROUP BY 1
    ORDER BY count DESC
    LIMIT 10
"""

GET_ACTIVE_PROXY_CLIENTS = """
    WITH metrics AS (
        SELECT
            ip,
            pop,
            rtt_min / 1000.0 AS rtt_min_ms,
            tcp_rtt / 1000.0 AS tcp_rtt_ms,
            lat AS client_lat,
            lon AS client_lon,
            asn,
            CASE
                WHEN pop = 'SJC' THEN 37.3382
                WHEN pop = 'IAD' THEN 38.9531
                WHEN pop = 'NRT' THEN 35.7720
                ELSE 38.0
            END AS pop_lat,
            CASE
                WHEN pop = 'SJC' THEN -121.8863
                WHEN pop = 'IAD' THEN -77.4565
                WHEN pop = 'NRT' THEN 140.3929
                ELSE -97.0
            END AS pop_lon,
            -- Haversine distance formula matching GET_PROXY_STATS
            CASE
                WHEN pop = 'SJC' THEN 6371 * 2 * ASIN(SQRT(POWER(SIN(RADIANS(lat - 37.3382) / 2), 2) + COS(RADIANS(37.3382)) * COS(RADIANS(lat)) * POWER(SIN(RADIANS(lon - -121.8863) / 2), 2)))
                WHEN pop = 'IAD' THEN 6371 * 2 * ASIN(SQRT(POWER(SIN(RADIANS(lat - 38.9531) / 2), 2) + COS(RADIANS(38.9531)) * COS(RADIANS(lat)) * POWER(SIN(RADIANS(lon - -77.4565) / 2), 2)))
                WHEN pop = 'NRT' THEN 6371 * 2 * ASIN(SQRT(POWER(SIN(RADIANS(lat - 35.7720) / 2), 2) + COS(RADIANS(35.7720)) * COS(RADIANS(lat)) * POWER(SIN(RADIANS(lon - 140.3929) / 2), 2)))
                ELSE 6371 * 2 * ASIN(SQRT(POWER(SIN(RADIANS(lat - 38.0) / 2), 2) + COS(RADIANS(38.0)) * COS(RADIANS(lat)) * POWER(SIN(RADIANS(lon - -97.0) / 2), 2)))
            END AS distance_km
            {select_country_city_inner}
        FROM {temp_table}
        WHERE rtt_min IS NOT NULL
    )
    SELECT
        ip,
        asn,
        ROUND(rtt_min_ms, 2) AS rtt_min_ms,
        ROUND(tcp_rtt_ms, 2) AS tcp_rtt_ms,
        ROUND(distance_km, 1) AS distance_km,
        pop,
        ROUND(client_lat, 4) AS client_lat,
        ROUND(client_lon, 4) AS client_lon,
        pop_lat,
        pop_lon,
        CASE
            WHEN distance_km > (rtt_min_ms * 100.0 + 150.0) THEN true
            ELSE false
        END AS impossible_distance,
        CASE
            WHEN distance_km > (rtt_min_ms * 100.0 + 150.0) THEN 'High'
            WHEN tcp_rtt_ms > 0 AND (rtt_min_ms / tcp_rtt_ms) < 0.1 THEN 'High'
            WHEN tcp_rtt_ms > 0 AND (rtt_min_ms / tcp_rtt_ms) < 0.2 THEN 'Medium'
            ELSE 'Low'
        END AS risk_level
        {select_country_city_outer}
    FROM metrics
    WHERE
        (distance_km > (rtt_min_ms * 100.0 + 150.0)) OR
        (tcp_rtt_ms > 0 AND (rtt_min_ms / tcp_rtt_ms) < 0.2)
    ORDER BY risk_level DESC, distance_km DESC
    LIMIT 100
"""

__all__ = [
    "TOP_UAS_BY_COUNT",
    "NGWAF_TOP_BOTS_JOIN",
    "NGWAF_TOP_BOTS_JOIN_DIRECT",
    "VERIFIED_BOTS_TS",
    "NGWAF_VERIFIED_BOTS",
    "NGWAF_VERIFIED_BOTS_TS",
    "FINGERPRINT_TOP_N",
    "FINGERPRINT_COVERAGE_BULK",
    "REQ_HEADER_SIZE_DIST",
    "TOP_IPS_BY_MAX_HEADER",
    "IPV6_ADOPTION_TS",
    "PROXY_TYPE_DIST",
    "CONN_REUSE_DIST",
    "WELLKNOWN_BOTS_UA_IP",
    "GET_PROXY_STATS",
    "GET_TRAFFIC_QUALITY",
    "GET_SUSPICIOUS_ISPS",
    "GET_ACTIVE_PROXY_CLIENTS",
]

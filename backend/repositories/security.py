"""Security repository — TLS analysis, bot detection, and request anomalies."""

from __future__ import annotations

import os

import duckdb

from backend.models.common import FiltersDict
from backend.repositories._base import QueryRunner, _safe_table, safe_iso, time_bucket_select
from backend.repositories.utils.filters import build_where_clause


def get_top_bots(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    n: int = 10,
) -> dict:
    """Return top N bots from UA matching and (if available) the NGWAF bot cache."""
    import logging

    source_name = src["name"]
    table_name = _safe_table(source_name)
    runner = QueryRunner(con, src)

    actual_cols = runner.get_schema_cols()
    if not actual_cols:
        from backend.repositories._base import empty_schema_response

        return empty_schema_response(bots=[], ngwaf_bots=[])

    params, where_clause = build_where_clause(start_time, end_time, filters, actual_cols, inline_params=True)

    # ── Arcjet UA-matched bots ────────────────────────────────────────────────
    arcjet_bots: list[dict] = []
    if "ua" in actual_cols:
        try:
            from backend.utils.bot_sources import build_matcher, get_bot_regex_pattern

            pattern = get_bot_regex_pattern(200)
            ua_filter = f"AND regexp_matches(ua, '{pattern.replace(chr(39), chr(39) * 2)}')" if pattern else ""

            q = f"""
                SELECT ua, count(*) AS cnt
                FROM {table_name}
                WHERE {where_clause} AND ua IS NOT NULL {ua_filter}
                GROUP BY ua
                ORDER BY cnt DESC
                LIMIT 2000
            """
            rows = runner.execute(q).fetchall()

            match_ua = build_matcher()
            bot_counts: dict[str, dict] = {}
            for ua_val, cnt in rows:
                for entry in match_ua(ua_val):
                    bot_id = entry.get("id", "unknown")
                    if bot_id not in bot_counts:
                        cats = entry.get("categories", [])
                        bot_counts[bot_id] = {
                            "id": bot_id,
                            "name": bot_id.replace("-", " ").title(),
                            "category": cats[0] if cats else "unknown",
                            "request_count": 0,
                        }
                    bot_counts[bot_id]["request_count"] += cnt

            arcjet_bots = sorted(bot_counts.values(), key=lambda x: x["request_count"], reverse=True)[:n]
        except Exception as e:
            logging.getLogger(__name__).error("[security] arcjet top bots failed: %s", e)

    # ── NGWAF cache bot names ─────────────────────────────────────────────────
    ngwaf_bots: list[dict] = []
    from backend.repositories._base import attach_ngwaf_cache

    with attach_ngwaf_cache(con, actual_cols, alias="ngwaf_top") as attached:
        if attached:
            try:
                q = f"""
                    SELECT nb.bot_name, nb.category, count(*) AS cnt
                    FROM {table_name}
                    INNER JOIN ngwaf_top.ngwaf_bots nb USING (waf_req_id)
                    WHERE {where_clause} AND nb.bot_name IS NOT NULL
                    GROUP BY 1, 2
                    ORDER BY 3 DESC
                    LIMIT {n}
                """
                res = runner.execute(q).fetchall()
                ngwaf_bots = [{"name": r[0], "category": r[1], "request_count": r[2]} for r in res]
            except Exception as e:
                logging.getLogger(__name__).error("[security] NGWAF top bots failed: %s", e)

    return {"bots": arcjet_bots, "ngwaf_bots": ngwaf_bots}


def get_security_aggregates(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    bucket_seconds: int = 300,
) -> dict:
    source_name = src["name"]
    table_name = _safe_table(source_name)
    runner = QueryRunner(con, src)

    actual_cols = runner.get_schema_cols()
    if not actual_cols:
        from backend.repositories._base import empty_schema_response

        return empty_schema_response(
            tls_fingerprints=[],
            req_size_dist=[],
            ipv6_adoption=[],
            proxy_dist=[],
            conn_reuse_dist=[],
            http_versions=[],
            **runner.telemetry(),
        )

    params, where_clause = build_where_clause(start_time, end_time, filters, actual_cols, inline_params=True)

    cols = [
        "timestamp",
        "ip",
        "asn",
        "tls_ciphers_sha",
        "req_header_bytes",
        "req_bytes",
        "is_ipv6",
        "p_type",
        "conn_requests",
        "ja3",
        "ja4",
        "waf_sig",
        "ua",
        "waf_req_id",
    ]
    temp_table = runner.create_filtered_temp_table(cols, actual_cols, table_name, where_clause, params)
    if temp_table is None:
        return {**runner.telemetry()}

    try:
        return _build_security_response(runner, src, con, actual_cols, temp_table, bucket_seconds)
    finally:
        try:
            runner.execute(f"DROP TABLE IF EXISTS {temp_table}")
        except Exception:
            pass


def _build_security_response(
    runner: QueryRunner,
    src: dict,
    con: duckdb.DuckDBPyConnection,
    actual_cols: list[str],
    temp_table: str,
    bucket_seconds: int,
) -> dict:
    results = {**runner.telemetry()}

    # Surface whether NGWAF is configured so the frontend can distinguish
    # "not configured" from "configured but no detections yet".
    try:
        from backend import config as svcconfig

        results["ngwaf_configured"] = bool(svcconfig.get_ngwaf_workspace_id(src.get("service_id", "")))
    except Exception:
        results["ngwaf_configured"] = False

    # Attach the NGWAF bot cache once per connection if it exists and waf_req_id is in schema.
    # The attach costs ~22ms so we guard on both conditions to avoid overhead when unused.
    _ngwaf_attached = False
    if "waf_req_id" in actual_cols:
        try:
            from backend import config as svcconfig

            ngwaf_db = svcconfig.ngwaf_db_path()
            if os.path.exists(ngwaf_db):
                ngwaf_db_escaped = ngwaf_db.replace("'", "''")
                con.execute(f"ATTACH '{ngwaf_db_escaped}' AS ngwaf_cache (TYPE SQLITE, READ_ONLY)")
                _ngwaf_attached = True
        except Exception:
            pass  # ATTACH failed (e.g. DuckDB SQLite extension not loaded) — fall back gracefully

    # 0. Verified Bots Time Series (waf_sig fallback — category-level, no bot names)
    if "waf_sig" in actual_cols:
        q = f"""
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
        res = runner.execute(q).fetchall()
        results["verified_bots_ts"] = [{"time": safe_iso(r[0]), "bot_type": r[1], "count": r[2]} for r in res]
    else:
        results["verified_bots_ts"] = []

    # 0b. NGWAF Verified Bots (name-resolved, requires ngwaf_bot_cache.db + waf_req_id column)
    if _ngwaf_attached:
        try:
            # Table: group by bot_name + wellknown_bot_name + category
            q = f"""
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
            res = runner.execute(q).fetchall()
            results["ngwaf_verified_bots"] = [
                {
                    "bot_name": r[0],
                    "wellknown_bot_name": r[1],
                    "category": r[2],
                    "request_count": r[3],
                }
                for r in res
            ]

            # Time series: bucketed counts by bot_name
            q = f"""
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
            res = runner.execute(q).fetchall()
            results["ngwaf_verified_bots_ts"] = [{"time": safe_iso(r[0]), "bot_name": r[1], "count": r[2]} for r in res]
        except Exception as e:
            import logging

            logging.getLogger(__name__).error("[security] NGWAF bot join failed: %s", e)
            results["ngwaf_verified_bots"] = []
            results["ngwaf_verified_bots_ts"] = []
    else:
        results["ngwaf_verified_bots"] = []
        results["ngwaf_verified_bots_ts"] = []

    # 1. TLS Fingerprints (Cipher SHA + IP Spread)
    if "tls_ciphers_sha" in actual_cols and "ip" in actual_cols:
        q = f"""
            SELECT tls_ciphers_sha,
                   count(DISTINCT ip) as ip_count,
                   count(*) as req_count
            FROM {temp_table}
            WHERE tls_ciphers_sha IS NOT NULL
            GROUP BY 1 ORDER BY 3 DESC LIMIT 20
        """
        res = runner.execute(q).fetchall()
        results["tls_fingerprints"] = [{"fingerprint": r[0], "ip_count": r[1], "request_count": r[2]} for r in res]
    else:
        results["tls_fingerprints"] = []

    # 3. Request Header Size Distribution
    if "req_header_bytes" in actual_cols:
        q = f"""
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
        res = runner.execute(q).fetchall()
        results["req_size_dist"] = [{"bucket": r[0], "count": r[1]} for r in res]

        # Top IPs by Max Header Size
        q = f"""
            SELECT ip, MAX(req_header_bytes) as max_header
            FROM {temp_table}
            WHERE ip IS NOT NULL AND req_header_bytes IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """
        res = runner.execute(q).fetchall()
        results["top_ips_header"] = [{"ip": r[0], "max_header": r[1]} for r in res]
    else:
        results["req_size_dist"] = []
        results["top_ips_header"] = []

    # 4. IPv6 Adoption over Time
    if "is_ipv6" in actual_cols:
        q = f"""
            SELECT {time_bucket_select("1 hour")},
                   SUM(CASE WHEN is_ipv6 THEN 1 ELSE 0 END) * 100.0 / count(*) as ipv6_pct
            FROM {temp_table}
            GROUP BY 1 ORDER BY 1
        """
        res = runner.execute(q).fetchall()
        results["ipv6_adoption"] = [{"time": safe_iso(r[0]), "pct": r[1]} for r in res]
    else:
        results["ipv6_adoption"] = []

    # 5. Proxy/Anonymizer Breakdown
    if "p_type" in actual_cols:
        q = f"""
            SELECT p_type, count(*) as count
            FROM {temp_table}
            WHERE p_type IS NOT NULL AND p_type != ''
            GROUP BY 1 ORDER BY 2 DESC
        """
        res = runner.execute(q).fetchall()
        results["proxy_dist"] = [{"type": r[0], "count": r[1]} for r in res]
    else:
        results["proxy_dist"] = []

    # 6. Connection Reuse Distribution
    if "conn_requests" in actual_cols:
        q = f"""
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
        res = runner.execute(q).fetchall()
        results["conn_reuse_dist"] = [{"bucket": r[0], "count": r[1]} for r in res]
    else:
        results["conn_reuse_dist"] = []

    # 7. Well-Known Bots (UA matching + FCrDNS verification)
    if "ua" in actual_cols and "ip" in actual_cols:
        try:
            from backend.utils.bot_sources import build_matcher, get_bot_regex_pattern
            from backend.utils.rdns_cache import classify, enqueue, get_hostnames

            # Build a dynamic regex pre-filter from actual bot pattern literals.
            # regexp_matches is O(N) via RE2, vs O(N*M) for long ILIKE OR chains.
            pattern = get_bot_regex_pattern(500)
            if pattern:
                pattern_sql = pattern.replace("'", "''")
                prefilter = f"WHERE ua IS NOT NULL AND ip IS NOT NULL AND regexp_matches(ua, '{pattern_sql}')"
            else:
                prefilter = "WHERE ua IS NOT NULL AND ip IS NOT NULL"

            q = f"""
                SELECT ua, ip, count(*) AS cnt
                FROM {temp_table}
                {prefilter}
                GROUP BY ua, ip
                ORDER BY cnt DESC
                LIMIT 10000
            """
            ua_ip_rows = runner.execute(q).fetchall()

            match_ua = build_matcher()
            bot_agg: dict[str, dict] = {}
            new_ips: list[str] = []

            # Batch-resolve every distinct IP in one SELECT instead of opening a
            # fresh SQLite connection per (ua, ip) row inside the loop.
            hostnames = get_hostnames([ip for _, ip, _ in ua_ip_rows if ip])

            for ua_val, ip_val, cnt in ua_ip_rows:
                matches = match_ua(ua_val)
                for entry in matches:
                    bot_id = entry.get("id", "unknown")
                    hostname, status, fcrdns_verified = hostnames.get(ip_val, (None, "pending", False))
                    if status == "pending":
                        new_ips.append(ip_val)
                    verification = entry.get("verification", {})
                    verification_domains = verification.get("domains", [])
                    verification_cidrs = verification.get("cidrs", [])
                    state = classify(
                        ip_val, hostname, status, fcrdns_verified, verification_domains, verification_cidrs
                    )

                    if bot_id not in bot_agg:
                        cats = entry.get("categories", [])
                        bot_agg[bot_id] = {
                            "id": bot_id,
                            "name": bot_id.replace("-", " ").title(),
                            "category": cats[0] if cats else "unknown",
                            "request_count": 0,
                            "verified_count": 0,
                            "impersonator_count": 0,
                            "unverified_count": 0,
                            "pending_count": 0,
                        }
                    agg = bot_agg[bot_id]
                    agg["request_count"] += cnt
                    if state == "verified":
                        agg["verified_count"] += cnt
                    elif state == "impersonator":
                        agg["impersonator_count"] += cnt
                    elif state == "unverified_pending":
                        agg["pending_count"] += cnt
                    else:
                        agg["unverified_count"] += cnt

            # Enqueue any pending IPs we encountered for future enrichment
            if new_ips:
                enqueue(list(set(new_ips)))

            # Sort by request count, return top 50, annotate coverage
            sorted_bots = sorted(bot_agg.values(), key=lambda x: x["request_count"], reverse=True)[:50]
            for b in sorted_bots:
                total = b["request_count"]
                covered = b["verified_count"] + b["impersonator_count"]
                b["verification_coverage"] = round(covered / total, 3) if total else 0.0

            results["wellknown_bots"] = sorted_bots
        except Exception as e:
            import logging

            logging.getLogger(__name__).error("[security] well-known bots query failed: %s", e)
            results["wellknown_bots"] = []
    else:
        results["wellknown_bots"] = []

    return results

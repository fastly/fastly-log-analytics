"""Security repository — TLS analysis, bot detection, and request anomalies."""

from __future__ import annotations

import os

import duckdb

from backend.models.common import FiltersDict
from backend.repositories._base import QueryRunner, _safe_table, safe_iso, time_bucket_select
from backend.repositories._sql import security as SQL
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

    arcjet_bots: list[dict] = []
    # ── Single filtered TEMP TABLE shared across arcjet UA + NGWAF JOIN ─────
    # Previously the function ran TWO independent scans over the same
    # filtered window: a UA TopN (LIMIT 2000) for arcjet classification
    # then a SECOND scan with an NGWAF JOIN for waf bot names. With the
    # dashboard's security panel mounted, both ran on every request.
    # Materializing one filtered temp table with the columns BOTH passes
    # need (ua + waf_req_id) collapses the scan to one Iceberg manifest
    # walk and keeps both downstream queries reading from memory.
    cols_needed: list[str] = []
    if "ua" in actual_cols:
        cols_needed.append("ua")
    if "waf_req_id" in actual_cols:
        cols_needed.append("waf_req_id")
    # If the schema has neither (very minimal log_fields preset), skip
    # both passes — there's nothing to classify.
    if not cols_needed:
        return {"bots": [], "ngwaf_bots": []}

    # Use QueryRunner.temp_table context manager so the DROP runs even
    # if an intermediate query raises (was a manual try/finally before).
    with runner.temp_table(cols_needed, actual_cols, table_name, where_clause, params) as temp_table:
        if temp_table is None:
            return {"bots": [], "ngwaf_bots": []}
        if "ua" in actual_cols:
            try:
                from backend.utils.bot_sources import build_matcher

                # Item 41 — the inline regexp_matches(ua, '<200-pattern OR-chain>')
                # cost ~353 ms on prod / week (per dashboard telemetry) because
                # DuckDB has to evaluate the alternation per row. The Python
                # matcher below is already what we use to classify each UA's
                # bot_id, so move the regex out of SQL: pull the top 50,000
                # distinct UAs by count (cheap GROUP BY + ORDER BY) then run
                # build_matcher() on them in Python where the per-UA result
                # is lru_cached and most lookups are sub-microsecond.
                q = SQL.TOP_UAS_BY_COUNT.format(temp_table=temp_table)
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

        # ── NGWAF cache bot names ─────────────────────────────────────────────
        # Memoize ATTACH per-connection the same way get_security_aggregates
        # does for `ngwaf_cache`. The previous attach_ngwaf_cache context
        # manager DETACHed on exit, so every /dashboard cold load paid the
        # ~22 ms ATTACH cost on /api/security/top-bots even when the file
        # was already attached. The duckdb_databases() catalog query is
        # ~90 us — fast enough to run unconditionally.
        ngwaf_bots: list[dict] = []
        ngwaf_attached = False
        if "waf_req_id" in actual_cols:
            try:
                from backend import config as svcconfig

                ngwaf_db = svcconfig.ngwaf_db_path()
                if ngwaf_db:
                    existing = con.execute(
                        "SELECT path FROM duckdb_databases() WHERE database_name='ngwaf_top' LIMIT 1"
                    ).fetchone()
                    already_path = existing[0] if existing else None
                    if already_path == ngwaf_db:
                        ngwaf_attached = True
                    elif os.path.exists(ngwaf_db):
                        if already_path is not None:
                            try:
                                con.execute("DETACH ngwaf_top")
                            except Exception:
                                pass
                        ngwaf_db_escaped = ngwaf_db.replace("'", "''")
                        con.execute(f"ATTACH '{ngwaf_db_escaped}' AS ngwaf_top (TYPE SQLITE, READ_ONLY)")
                        ngwaf_attached = True
            except Exception:
                pass  # ATTACH failed — fall back gracefully

        if ngwaf_attached:
            try:
                # Join against the temp table instead of re-scanning the
                # source view — same filter window, no second manifest walk.
                q = SQL.NGWAF_TOP_BOTS_JOIN.format(temp_table=temp_table, n=n)
                res = runner.execute(q).fetchall()
                ngwaf_bots = [{"name": r[0], "category": r[1], "request_count": r[2]} for r in res]
            except Exception as e:
                logging.getLogger(__name__).error("[security] NGWAF top bots failed: %s", e)

    return {"bots": arcjet_bots, "ngwaf_bots": ngwaf_bots, **runner.telemetry()}


def get_security_aggregates(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    bucket_seconds: int = 300,
) -> dict:
    import time as _time

    # Per-phase timings for /api/security/aggregates so the perf
    # harness can attribute wall time across the ~14 sub-queries
    # _build_security_response runs without ad-hoc instrumentation.
    section_timings: list[dict] = []

    def _phase(name: str, t0: float) -> None:
        section_timings.append({"section": name, "time_ms": round((_time.perf_counter() - t0) * 1000, 2)})

    source_name = src["name"]
    table_name = _safe_table(source_name)
    runner = QueryRunner(con, src)

    _t = _time.perf_counter()
    actual_cols = runner.get_schema_cols()
    _phase("get_schema_cols", _t)
    if not actual_cols:
        from backend.repositories._base import empty_schema_response

        return empty_schema_response(
            tls_fingerprints=[],
            req_size_dist=[],
            ipv6_adoption=[],
            proxy_dist=[],
            conn_reuse_dist=[],
            http_versions=[],
            section_timings=section_timings,
            **runner.telemetry(),
        )

    _t = _time.perf_counter()
    params, where_clause = build_where_clause(start_time, end_time, filters, actual_cols, inline_params=True)
    _phase("build_where_clause", _t)

    # Projection narrowed: asn / req_bytes / ja3 / ja4 are not consumed
    # by _build_security_response (audited 2026-06-05) so they're dropped
    # from the TEMP TABLE materialization. Each saves a column scan +
    # cast per parquet read.
    cols = [
        "timestamp",
        "ip",
        "tls_ciphers_sha",
        "h2_fingerprint",
        "oh_fingerprint",
        "req_header_bytes",
        "is_ipv6",
        "p_type",
        "conn_requests",
        "waf_sig",
        "ua",
        "waf_req_id",
    ]
    _t = _time.perf_counter()
    temp_table = runner.create_filtered_temp_table(cols, actual_cols, table_name, where_clause, params)
    _phase("temp_table_create", _t)
    if temp_table is None:
        return {"section_timings": section_timings, **runner.telemetry()}

    try:
        return _build_security_response(runner, src, con, actual_cols, temp_table, bucket_seconds, section_timings)
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
    section_timings: list[dict] | None = None,
) -> dict:
    import time as _time

    if section_timings is None:
        section_timings = []

    def _phase(name: str, t0: float) -> None:
        section_timings.append({"section": name, "time_ms": round((_time.perf_counter() - t0) * 1000, 2)})

    results = {**runner.telemetry()}

    # Surface whether NGWAF is configured so the frontend can distinguish
    # "not configured" from "configured but no detections yet".
    try:
        from backend import config as svcconfig

        results["ngwaf_configured"] = bool(svcconfig.get_ngwaf_workspace_id(src.get("service_id", "")))
    except Exception:
        results["ngwaf_configured"] = False

    # Attach the NGWAF bot cache once per connection if it exists and waf_req_id is in schema.
    # The attach costs ~22ms; check DuckDB's own duckdb_databases() catalog
    # (~90us) first and skip the ATTACH if this connection already has the
    # cache bound to the exact same path. The catalog query reflects live
    # state, so we don't need Python-side memoization (DuckDBPyConnection
    # has no __dict__ for arbitrary attrs anyway) and a config switch that
    # changes the path triggers a DETACH + re-ATTACH instead of silently
    # serving from a stale binding.
    _ngwaf_attached = False
    if "waf_req_id" in actual_cols:
        try:
            from backend import config as svcconfig

            ngwaf_db = svcconfig.ngwaf_db_path()
            if ngwaf_db:
                existing = con.execute(
                    "SELECT path FROM duckdb_databases() WHERE database_name='ngwaf_cache' LIMIT 1"
                ).fetchone()
                already_path = existing[0] if existing else None
                if already_path == ngwaf_db:
                    _ngwaf_attached = True
                elif os.path.exists(ngwaf_db):
                    if already_path is not None:
                        try:
                            con.execute("DETACH ngwaf_cache")
                        except Exception:
                            pass
                    ngwaf_db_escaped = ngwaf_db.replace("'", "''")
                    con.execute(f"ATTACH '{ngwaf_db_escaped}' AS ngwaf_cache (TYPE SQLITE, READ_ONLY)")
                    _ngwaf_attached = True
        except Exception:
            pass  # ATTACH failed (e.g. DuckDB SQLite extension not loaded) — fall back gracefully

    # 0. Verified Bots Time Series (waf_sig fallback — category-level, no bot names)
    if "waf_sig" in actual_cols:
        q = SQL.VERIFIED_BOTS_TS.format(bucket_seconds=bucket_seconds, temp_table=temp_table)
        _t = _time.perf_counter()
        res = runner.execute(q).fetchall()
        _phase("verified_bots_ts", _t)
        results["verified_bots_ts"] = [{"time": safe_iso(r[0]), "bot_type": r[1], "count": r[2]} for r in res]
    else:
        results["verified_bots_ts"] = []

    # 0b. NGWAF Verified Bots (name-resolved, requires ngwaf_bot_cache.db + waf_req_id column)
    if _ngwaf_attached:
        try:
            # Table: group by bot_name + wellknown_bot_name + category
            q = SQL.NGWAF_VERIFIED_BOTS.format(temp_table=temp_table)
            _t = _time.perf_counter()
            res = runner.execute(q).fetchall()
            _phase("ngwaf_verified_bots", _t)
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
            q = SQL.NGWAF_VERIFIED_BOTS_TS.format(bucket_seconds=bucket_seconds, temp_table=temp_table)
            _t = _time.perf_counter()
            res = runner.execute(q).fetchall()
            _phase("ngwaf_verified_bots_ts", _t)
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
        q = SQL.TLS_FINGERPRINTS.format(temp_table=temp_table)
        _t = _time.perf_counter()
        res = runner.execute(q).fetchall()
        _phase("tls_fingerprints", _t)
        results["tls_fingerprints"] = [{"fingerprint": r[0], "ip_count": r[1], "request_count": r[2]} for r in res]
    else:
        results["tls_fingerprints"] = []

    # 1.1 H2 Fingerprints
    if "h2_fingerprint" in actual_cols and "ip" in actual_cols:
        q = SQL.H2_FINGERPRINTS.format(temp_table=temp_table)
        _t = _time.perf_counter()
        res = runner.execute(q).fetchall()
        _phase("h2_fingerprints", _t)
        results["h2_fingerprints"] = [{"fingerprint": r[0], "ip_count": r[1], "request_count": r[2]} for r in res]
    else:
        results["h2_fingerprints"] = []

    # 1.2 OH Fingerprints
    if "oh_fingerprint" in actual_cols and "ip" in actual_cols:
        q = SQL.OH_FINGERPRINTS.format(temp_table=temp_table)
        _t = _time.perf_counter()
        res = runner.execute(q).fetchall()
        _phase("oh_fingerprints", _t)
        results["oh_fingerprints"] = [{"fingerprint": r[0], "ip_count": r[1], "request_count": r[2]} for r in res]
    else:
        results["oh_fingerprints"] = []

    # 3. Request Header Size Distribution
    if "req_header_bytes" in actual_cols:
        q = SQL.REQ_HEADER_SIZE_DIST.format(temp_table=temp_table)
        _t = _time.perf_counter()
        res = runner.execute(q).fetchall()
        _phase("req_size_dist", _t)
        results["req_size_dist"] = [{"bucket": r[0], "count": r[1]} for r in res]

        # Top IPs by Max Header Size
        q = SQL.TOP_IPS_BY_MAX_HEADER.format(temp_table=temp_table)
        _t = _time.perf_counter()
        res = runner.execute(q).fetchall()
        _phase("top_ips_by_header", _t)
        results["top_ips_header"] = [{"ip": r[0], "max_header": r[1]} for r in res]
    else:
        results["req_size_dist"] = []
        results["top_ips_header"] = []

    # 4. IPv6 Adoption over Time
    if "is_ipv6" in actual_cols:
        q = SQL.IPV6_ADOPTION_TS.format(
            time_bucket_select=time_bucket_select("1 hour"),
            temp_table=temp_table,
        )
        _t = _time.perf_counter()
        res = runner.execute(q).fetchall()
        _phase("ipv6_adoption", _t)
        results["ipv6_adoption"] = [{"time": safe_iso(r[0]), "pct": r[1]} for r in res]
    else:
        results["ipv6_adoption"] = []

    # 5. Proxy/Anonymizer Breakdown
    if "p_type" in actual_cols:
        q = SQL.PROXY_TYPE_DIST.format(temp_table=temp_table)
        _t = _time.perf_counter()
        res = runner.execute(q).fetchall()
        _phase("proxy_dist", _t)
        results["proxy_dist"] = [{"type": r[0], "count": r[1]} for r in res]
    else:
        results["proxy_dist"] = []

    # 6. Connection Reuse Distribution
    if "conn_requests" in actual_cols:
        q = SQL.CONN_REUSE_DIST.format(temp_table=temp_table)
        _t = _time.perf_counter()
        res = runner.execute(q).fetchall()
        _phase("conn_reuse_dist", _t)
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

            q = SQL.WELLKNOWN_BOTS_UA_IP.format(temp_table=temp_table, prefilter=prefilter)
            _t = _time.perf_counter()
            ua_ip_rows = runner.execute(q).fetchall()
            _phase("wellknown_bots_query", _t)

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

    results["section_timings"] = section_timings
    return results

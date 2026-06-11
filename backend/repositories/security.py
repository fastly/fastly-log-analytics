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
    import time as _time

    section_timings: list[dict] = []

    def _phase(name: str, t0: float) -> None:
        section_timings.append({"section": name, "time_ms": round((_time.perf_counter() - t0) * 1000, 2)})

    source_name = src["name"]
    table_name = _safe_table(source_name)
    runner = QueryRunner(con, src)

    _t = _time.perf_counter()
    actual_cols = runner.get_schema_cols()
    _phase("top_bots:get_schema_cols", _t)
    if not actual_cols:
        from backend.repositories._base import empty_schema_response

        return empty_schema_response(bots=[], ngwaf_bots=[])

    _t = _time.perf_counter()
    params, where_clause = build_where_clause(start_time, end_time, filters, actual_cols, inline_params=True)
    _phase("top_bots:build_where_clause", _t)

    arcjet_bots: list[dict] = []
    ngwaf_bots: list[dict] = []

    use_rollups = not filters

    # ── Arcjet UA matching ──────────────────────────────────────────
    # Rollup-served when no filters apply. The hour bundles already
    # carry top-500 UAs per hour; UNION + GROUP-BY across the window
    # is sub-second even on 30d (vs ~1.1s for the ua column scan via
    # the iceberg view on prod). Real bots send enough traffic that
    # their UAs almost always land in top-500 for at least some hours,
    # so the rollup gives equivalent arcjet matches to the raw scan.
    # Filtered requests bypass this path (rollup is filter-free) and
    # fall through to the temp-scan branch below.
    ua_rollup_rows: list[tuple[str, int]] | None = None
    if use_rollups and "ua" in actual_cols:
        try:
            _t = _time.perf_counter()
            rolled, _ = runner.execute_top_n_rollups(
                ["ua"],
                start_time,
                end_time,
                limit=50000,
                per_field_limits={"ua": 50000},
            )
            _phase("top_bots:ua_rollup_query", _t)
            ua_rollup_rows = [(v, int(c)) for _f, v, c in rolled if v and v != "__other__"]
            if not ua_rollup_rows:
                # Rollup is empty (cold service, no backfill yet) —
                # fall back to the raw temp scan so we still produce
                # bot matches on first dashboard load.
                ua_rollup_rows = None
        except Exception as e:
            logging.getLogger(__name__).warning("[security] UA rollup read failed, falling back: %s", e)
            ua_rollup_rows = None

    def _classify(rows: list[tuple[str, int]]) -> list[dict]:
        from backend.utils.bot_sources import build_matcher

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
        return sorted(bot_counts.values(), key=lambda x: x["request_count"], reverse=True)[:n]

    if ua_rollup_rows is not None:
        _t = _time.perf_counter()
        try:
            arcjet_bots = _classify(ua_rollup_rows)
        except Exception as e:
            logging.getLogger(__name__).error("[security] arcjet rollup match failed: %s", e)
        _phase("top_bots:arcjet_match", _t)

    # ── NGWAF cache bot names + filtered-UA fallback ────────────────
    # NGWAF JOIN needs raw waf_req_id (high-cardinality, no rollup),
    # so it still builds a temp. When the rollup-served UA path
    # didn't run (filters present, or "ua" not in schema), the temp
    # also carries `ua` so the filtered-UA branch can scan it.
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

    needs_filtered_ua_scan = ua_rollup_rows is None and "ua" in actual_cols
    cols_needed: list[str] = []
    if needs_filtered_ua_scan:
        cols_needed.append("ua")
    if ngwaf_attached and "waf_req_id" in actual_cols:
        cols_needed.append("waf_req_id")

    if cols_needed:
        _t = _time.perf_counter()
        with runner.temp_table(cols_needed, actual_cols, table_name, where_clause, params) as temp_table:
            _phase("top_bots:temp_table_create", _t)
            if temp_table is None:
                return {
                    "bots": arcjet_bots,
                    "ngwaf_bots": ngwaf_bots,
                    "section_timings": section_timings,
                    **runner.telemetry(),
                }
            if needs_filtered_ua_scan:
                try:
                    _t = _time.perf_counter()
                    q = SQL.TOP_UAS_BY_COUNT.format(temp_table=temp_table)
                    rows = runner.execute(q).fetchall()
                    _phase("top_bots:top_uas_query", _t)
                    _t = _time.perf_counter()
                    arcjet_bots = _classify(rows)
                    _phase("top_bots:arcjet_match", _t)
                except Exception as e:
                    logging.getLogger(__name__).error("[security] arcjet top bots failed: %s", e)

            if ngwaf_attached:
                try:
                    _t = _time.perf_counter()
                    q = SQL.NGWAF_TOP_BOTS_JOIN.format(temp_table=temp_table, n=n)
                    res = runner.execute(q).fetchall()
                    ngwaf_bots = [{"name": r[0], "category": r[1], "request_count": r[2]} for r in res]
                    _phase("top_bots:ngwaf_join", _t)
                except Exception as e:
                    logging.getLogger(__name__).error("[security] NGWAF top bots failed: %s", e)

    return {
        "bots": arcjet_bots,
        "ngwaf_bots": ngwaf_bots,
        "section_timings": section_timings,
        **runner.telemetry(),
    }


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

    # Fingerprint cards: TLS + H2 + OH. Each card returns top-20 + a coverage
    # fraction (populated rows / total rows) so the FE can render a low-
    # coverage hint when a leaderboard is legitimately sparse for the current
    # traffic mix (e.g. h2 fingerprints on a ~99.99% HTTP/1.1 service).
    fingerprint_coverage: dict[str, float] = {}

    def _coverage_for(col: str) -> float:
        # Returns 0.0 on any error or empty temp_table; the FE treats 0.0 as
        # "no signal, show the existing emptyMessage" rather than the hint.
        try:
            q = SQL.FINGERPRINT_COVERAGE.format(col=col, temp_table=temp_table)
            total, populated = runner.execute(q).fetchone() or (0, 0)
            return float(populated) / float(total) if total else 0.0
        except Exception:
            return 0.0

    # 1. TLS Fingerprints (Cipher SHA + IP Spread)
    if "tls_ciphers_sha" in actual_cols and "ip" in actual_cols:
        q = SQL.TLS_FINGERPRINTS.format(temp_table=temp_table)
        _t = _time.perf_counter()
        res = runner.execute(q).fetchall()
        _phase("tls_fingerprints", _t)
        results["tls_fingerprints"] = [{"fingerprint": r[0], "ip_count": r[1], "request_count": r[2]} for r in res]
        fingerprint_coverage["tls_ciphers_sha"] = _coverage_for("tls_ciphers_sha")
    else:
        results["tls_fingerprints"] = []

    # 1.1 H2 Fingerprints
    if "h2_fingerprint" in actual_cols and "ip" in actual_cols:
        q = SQL.H2_FINGERPRINTS.format(temp_table=temp_table)
        _t = _time.perf_counter()
        res = runner.execute(q).fetchall()
        _phase("h2_fingerprints", _t)
        results["h2_fingerprints"] = [{"fingerprint": r[0], "ip_count": r[1], "request_count": r[2]} for r in res]
        fingerprint_coverage["h2_fingerprint"] = _coverage_for("h2_fingerprint")
    else:
        results["h2_fingerprints"] = []

    # 1.2 OH Fingerprints
    if "oh_fingerprint" in actual_cols and "ip" in actual_cols:
        q = SQL.OH_FINGERPRINTS.format(temp_table=temp_table)
        _t = _time.perf_counter()
        res = runner.execute(q).fetchall()
        _phase("oh_fingerprints", _t)
        results["oh_fingerprints"] = [{"fingerprint": r[0], "ip_count": r[1], "request_count": r[2]} for r in res]
        fingerprint_coverage["oh_fingerprint"] = _coverage_for("oh_fingerprint")
    else:
        results["oh_fingerprints"] = []

    results["fingerprint_coverage"] = fingerprint_coverage

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

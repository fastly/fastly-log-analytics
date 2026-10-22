"""Per-hour rollups feeding /api/security/aggregates' ``proxies`` and
``bot_efficiency`` sections.

Both sections are all-rows live scans of the catalog temp (``bot_efficiency``
over ua/ip/referer/resp_bytes, ``proxies`` over ip/pop/rtt/lat/lon/asn). They
were the last sections forcing the ~1.2 s ``temp_table_create``; once both are
rollup-served the temp is skipped.

Files per closed hour (``rollups/hour_bundled/hour=H/``):

``security_proxy_quality.parquet`` (traffic-quality classification, all rows):
  type   VARCHAR  -- 'Active Tunnel / Proxy' | 'WiFi / Mobile' | 'Direct Connection'
  count  BIGINT

``security_proxy_flagged.parquet`` (flagged rows only; ~0.5 % of traffic):
  ip, asn, pop, rtt_min, tcp_rtt, lat, lon, country, city, cnt
Flagged = ``rtt_min`` present AND (distance mismatch OR ``rtt_min/tcp_rtt < 0.2``)
-- the union every proxies sub-query filters on, so the reader recomputes the
stats / ISP / client panels from it exactly. The haversine CASE is evaluated at
write time only to apply the flag; it is not persisted.

``security_bot_eff.parquet`` (BOT_EFFICIENCY row shape):
  kind VARCHAR, key VARCHAR, cnt BIGINT, resp_bytes BIGINT

Math across hours: proxies is EXACT (counts SUM, distinct IPs are counted over
the stored flagged rows). bot_efficiency is exact for the site totals and
per-UA bytes/counts (up to the per-hour top-K cut) but its ``ref`` count is the
SUM of per-hour distinct IPs, an overcount for IPs seen in several hours. That
count only feeds ``referrals`` / ``requests_per_referral``; the section has no
``_approx`` field to carry the caveat.

The writers must not import ``repositories`` (import-linter): the SQL bodies are
replicated from ``_sql/security.py`` and a parity test pins them to it.
"""

from __future__ import annotations

import logging

from backend.utils.pop_utils import build_pop_haversine_case_sql

from ._common import (
    SECURITY_BOT_EFF_BUNDLE_FILENAME,
    SECURITY_BOT_EFF_UA_TOP_K,
    SECURITY_PROXY_FLAGGED_BUNDLE_FILENAME,
    SECURITY_PROXY_QUALITY_BUNDLE_FILENAME,
    MultiBundleSpec,
    backfill_multi_spec_bundles,
    build_multi_spec_bundles,
    build_per_hour_bundles,
    parquet_zstd_target,
)

logger = logging.getLogger(__name__)


def _window(start_iso: str, end_iso: str) -> str:
    return f"timestamp >= TIMESTAMPTZ '{start_iso}' AND timestamp < TIMESTAMPTZ '{end_iso}'"


def _build_quality_copy_sql(ctx: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
    return (
        f"COPY ("
        f"  SELECT "
        f"    CASE "
        f"      WHEN rtt_min IS NULL OR tcp_rtt IS NULL THEN 'Direct Connection' "
        f"      WHEN tcp_rtt > 0 AND (CAST(rtt_min AS DOUBLE) / tcp_rtt) < 0.1 THEN 'Active Tunnel / Proxy' "
        f"      WHEN tcp_rtt > 0 AND (CAST(rtt_min AS DOUBLE) / tcp_rtt) < 0.7 THEN 'WiFi / Mobile' "
        f"      ELSE 'Direct Connection' "
        f"    END AS type, "
        f"    CAST(COUNT(*) AS BIGINT) AS count "
        f"  FROM {table_ident} "
        f"  WHERE {_window(start_iso, end_iso)} "
        f"  GROUP BY 1"
        f") {parquet_zstd_target(tmp_path)}"
    )


def _build_flagged_copy_sql(ctx: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
    cols = ctx if isinstance(ctx, (set, frozenset)) else set()
    country = "country" if "country" in cols else "CAST(NULL AS VARCHAR)"
    city = "city" if "city" in cols else "CAST(NULL AS VARCHAR)"
    window = _window(start_iso, end_iso)
    return (
        f"COPY ("
        f"  WITH geo AS ("
        f"    SELECT pop AS g_pop, lat AS g_lat, lon AS g_lon, {build_pop_haversine_case_sql()} AS distance_km "
        f"    FROM (SELECT DISTINCT pop, lat, lon FROM {table_ident} "
        f"          WHERE {window} AND lat IS NOT NULL AND lon IS NOT NULL)"
        f"  ), "
        f"  m AS ("
        f"    SELECT ip, asn, pop, rtt_min, tcp_rtt, lat, lon, {country} AS country, {city} AS city, "
        f"           rtt_min / 1000.0 AS rtt_min_ms, geo.distance_km AS distance_km "
        f"    FROM {table_ident} "
        f"    LEFT JOIN geo ON pop IS NOT DISTINCT FROM geo.g_pop "
        f"                 AND lat IS NOT DISTINCT FROM geo.g_lat "
        f"                 AND lon IS NOT DISTINCT FROM geo.g_lon "
        f"    WHERE {window} AND rtt_min IS NOT NULL"
        f"  ) "
        f"  SELECT ip, CAST(asn AS BIGINT) AS asn, pop, CAST(rtt_min AS DOUBLE) AS rtt_min, "
        f"         CAST(tcp_rtt AS DOUBLE) AS tcp_rtt, CAST(lat AS DOUBLE) AS lat, CAST(lon AS DOUBLE) AS lon, "
        f"         country, city, CAST(COUNT(*) AS BIGINT) AS cnt "
        f"  FROM m "
        f"  WHERE (distance_km > (rtt_min_ms * 100.0 + 150.0)) "
        f"     OR (tcp_rtt > 0 AND (CAST(rtt_min AS DOUBLE) / tcp_rtt) < 0.2) "
        f"  GROUP BY ALL"
        f") {parquet_zstd_target(tmp_path)}"
    )


def _build_bot_eff_copy_sql(ctx: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
    cols = ctx if isinstance(ctx, (set, frozenset)) else set()
    resp_bytes = "COALESCE(t.resp_bytes, 0)" if "resp_bytes" in cols else "0"
    referer = "t.referer" if "referer" in cols else "CAST(NULL AS VARCHAR)"
    select_referer = "referer" if "referer" in cols else "CAST(NULL AS VARCHAR) AS referer"
    select_bytes = "resp_bytes" if "resp_bytes" in cols else "CAST(0 AS BIGINT) AS resp_bytes"
    return (
        f"COPY ("
        f"  WITH t AS ("
        f"    SELECT ua, ip, {select_referer}, {select_bytes} FROM {table_ident} "
        f"    WHERE {_window(start_iso, end_iso)}"
        f"  ), "
        f"  site_totals AS ("
        f"    SELECT 'site' AS kind, '' AS key, COUNT(*) AS cnt, "
        f"           CAST(COALESCE(SUM({resp_bytes}), 0) AS BIGINT) AS resp_bytes FROM t"
        f"  ), "
        f"  ua_agg AS ("
        f"    SELECT 'ua' AS kind, t.ua AS key, COUNT(*) AS cnt, "
        f"           CAST(COALESCE(SUM({resp_bytes}), 0) AS BIGINT) AS resp_bytes "
        f"    FROM t WHERE t.ua IS NOT NULL AND t.ua != '' GROUP BY t.ua "
        f"    ORDER BY resp_bytes DESC, cnt DESC LIMIT {SECURITY_BOT_EFF_UA_TOP_K}"
        f"  ), "
        f"  ref_extracted AS ("
        f"    SELECT t.ip, COALESCE("
        f"      NULLIF(regexp_extract(lower(COALESCE(t.ua, '')), '([a-z0-9]+)-(user|externalfetcher)', 1), ''), "
        f"      NULLIF(regexp_extract(lower(COALESCE({referer}, '')), 'http[s]*://(www\\.)*([^/.:;\\s]+)', 2), '')"
        f"    ) AS ref_stem "
        f"    FROM t WHERE t.ip IS NOT NULL AND t.ip != '' "
        f"      AND (({referer} IS NOT NULL AND {referer} != '' AND {referer} != '-') "
        f"           OR lower(COALESCE(t.ua, '')) LIKE '%-user%' "
        f"           OR lower(COALESCE(t.ua, '')) LIKE '%-externalfetcher%') "
        f"      AND NOT regexp_matches(lower(COALESCE(t.ua, '')), "
        f"          '(bot|crawler|spider|slurp|archiver|scraper|externalagent|extended|headless)')"
        f"  ), "
        f"  ref_agg AS ("
        f"    SELECT 'ref' AS kind, ref_stem AS key, COUNT(DISTINCT ip) AS cnt, CAST(0 AS BIGINT) AS resp_bytes "
        f"    FROM ref_extracted WHERE ref_stem IS NOT NULL AND ref_stem != '' GROUP BY ref_stem"
        f"  ) "
        f"  SELECT kind, key, CAST(cnt AS BIGINT) AS cnt, resp_bytes FROM site_totals "
        f"  UNION ALL SELECT kind, key, CAST(cnt AS BIGINT), resp_bytes FROM ua_agg "
        f"  UNION ALL SELECT kind, key, CAST(cnt AS BIGINT), resp_bytes FROM ref_agg"
        f") {parquet_zstd_target(tmp_path)}"
    )


_SECURITY_PROXIES: tuple[MultiBundleSpec, ...] = (
    ("proxy_quality", SECURITY_PROXY_QUALITY_BUNDLE_FILENAME, ("rtt_min", "tcp_rtt"), _build_quality_copy_sql),
    (
        "proxy_flagged",
        SECURITY_PROXY_FLAGGED_BUNDLE_FILENAME,
        ("ip", "pop", "rtt_min", "tcp_rtt", "lat", "lon", "asn"),
        _build_flagged_copy_sql,
    ),
    ("bot_eff", SECURITY_BOT_EFF_BUNDLE_FILENAME, ("ua", "ip"), _build_bot_eff_copy_sql),
)


def _columns_ctx(cols: set[str], table_ident: str) -> frozenset[str]:
    return frozenset(cols)


def build_security_proxies_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write the security_proxy_quality + security_proxy_flagged + security_bot_eff
    rollups for each closed hour in ``hours``."""
    return build_multi_spec_bundles(
        service_id,
        source,
        hours,
        specs=_SECURITY_PROXIES,
        group_label="security_proxies",
        tmp_prefix=".tmp_sp_",
        logger=logger,
        context_fn=_columns_ctx,
        driver=build_per_hour_bundles,
    )


def backfill_security_proxies_bundles(service_id: str, source: dict) -> int:
    """Self-heal pass: build the security_proxies rollups for every closed hour
    that has ``all_fields.parquet`` but is missing one of the three files."""
    return backfill_multi_spec_bundles(
        service_id,
        source,
        specs=_SECURITY_PROXIES,
        group_label="security_proxies",
        tmp_prefix=".tmp_sp_",
        logger=logger,
        context_fn=_columns_ctx,
        driver=build_per_hour_bundles,
    )

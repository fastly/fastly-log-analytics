"""ASN parsing, label resolution, row enrichment, and ASN helper functions."""

from __future__ import annotations

import math
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any


def float_or_zero(val: Any, ndigits: int | None = None) -> float:
    """Convert ``val`` to ``float`` (optionally rounded), returning ``0.0`` when ``None`` or non-finite."""
    if val is None:
        return 0.0
    try:
        f = float(val)
    except (ValueError, TypeError):
        return 0.0
    if not math.isfinite(f):
        return 0.0
    return round(f, ndigits) if ndigits is not None else f


def parse_asn_int(val: Any) -> int | None:
    """Parse an integer ASN from arbitrary input, returning ``None`` on failure or bool."""
    if val is None or isinstance(val, bool):
        return None
    if isinstance(val, int):
        return val
    s = str(val).strip()
    if not s:
        return None
    if s.isdigit():
        return int(s)
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def resolve_asn_names_map(service_name: str, asns: Iterable[Any]) -> dict[int, str]:
    """Look up ASN organization names from the metadata store for ``asns``."""
    from backend.core import fos_client as _db

    asn_ints = sorted({asn_int for a in asns if (asn_int := parse_asn_int(a)) is not None})
    if not asn_ints:
        return {}
    return _db.get_asn_names(service_name, asn_ints)


def format_asn_display_label(
    asn_val: Any,
    asn_names: Mapping[int, str],
    *,
    non_int_fallback: str | None = None,
) -> str:
    """Format a human-readable ASN display label ``Name (ASN)`` or ``AS{asn}``."""
    from backend.core import fos_client as _db

    asn_int = parse_asn_int(asn_val)
    if asn_int is not None:
        return _db.format_asn_label(asn_int, asn_names.get(asn_int, ""))
    if non_int_fallback is not None:
        return non_int_fallback
    return str(asn_val) if asn_val is not None else ""


def build_asn_label_map(service_name: str, asns: Iterable[Any]) -> dict[int, str]:
    """Return ``{asn_int: formatted_display_label}`` for all valid integer ASNs in ``asns``."""
    from backend.core import fos_client as _db

    asn_ints = [asn_int for a in asns if (asn_int := parse_asn_int(a)) is not None]
    if not asn_ints:
        return {}
    unique_asns = list(dict.fromkeys(asn_ints))
    names = _db.get_asn_names(service_name, unique_asns)
    return {asn: _db.format_asn_label(asn, names.get(asn, "")) for asn in unique_asns}


def enrich_asn_rows(
    rows: list[dict[str, Any]],
    service_name: str,
    *,
    asn_key: str = "asn",
    label_key: str = "label",
    name_lookup: Callable[[str, list[int]], Mapping[int, str]] | None = None,
) -> list[dict[str, Any]]:
    """Enrich a list of row dicts in-place with formatted ASN labels under ``label_key``."""
    if not rows:
        return rows
    if name_lookup is not None:
        asn_ints = [asn_int for r in rows if (asn_int := parse_asn_int(r.get(asn_key))) is not None]
        asn_names: Mapping[int, str] = name_lookup(service_name, asn_ints) if asn_ints else {}
    else:
        asn_names = resolve_asn_names_map(service_name, (r.get(asn_key) for r in rows))
    for row in rows:
        raw_asn = row.get(asn_key)
        asn_int = parse_asn_int(raw_asn)
        if asn_int is not None:
            row[label_key] = format_asn_display_label(asn_int, asn_names)
        elif label_key not in row or not row[label_key]:
            row[label_key] = str(raw_asn) if raw_asn is not None else ""
    return rows


def shape_performance_asn_rows(
    service_name: str | Iterable[Any],
    rows: Iterable[Any] | Mapping[Any, str] | None = None,
    *,
    asn_lookup: Mapping[Any, str] | None = None,
) -> list[dict[str, Any]]:
    """Shape and ASN-enrich performance ``top_asns`` rows from either tuple or dict rows."""
    if not isinstance(service_name, str):
        row_list = list(service_name)
        lookup: Mapping[Any, str] = (
            asn_lookup
            if asn_lookup is not None
            else (rows if isinstance(rows, Mapping) else {})
        )
        if not row_list:
            return []
        out_legacy: list[dict[str, Any]] = []
        for r in row_list:
            if isinstance(r, Mapping):
                raw_val = r.get("asn", r.get("value"))
                asn_int = parse_asn_int(raw_val)
                requests = int(r["req_count"]) if "req_count" in r else int(r.get("requests", 0) or 0)
                avg_v = float_or_zero(r["avg_ms"]) if "avg_ms" in r else float_or_zero(r.get("avg"))
                p50_v = float_or_zero(r["p50_ms"]) if "p50_ms" in r else float_or_zero(r.get("p50"))
                p95_v = float_or_zero(r["p95_ms"]) if "p95_ms" in r else float_or_zero(r.get("p95"))
                p99_v = float_or_zero(r["p99_ms"]) if "p99_ms" in r else float_or_zero(r.get("p99"))
            else:
                raw_val = r[0]
                asn_int = parse_asn_int(raw_val)
                requests, avg_v, p50_v, p95_v, p99_v = (
                    int(r[1] or 0),
                    float_or_zero(r[2]),
                    float_or_zero(r[3]),
                    float_or_zero(r[4]),
                    float_or_zero(r[5]),
                )
            org_name = (
                (lookup.get(asn_int) or lookup.get(str(asn_int)) or "")
                if (lookup and asn_int is not None)
                else ""
            )
            asn_str = f"AS{asn_int}" if asn_int is not None else str(raw_val if raw_val is not None else "")
            label = f"{org_name} ({asn_int})" if (org_name and asn_int is not None) else asn_str
            out_legacy.append(
                {
                    "asn": asn_str,
                    "label": label,
                    "requests": requests,
                    "avg": avg_v,
                    "p50": p50_v,
                    "p95": p95_v,
                    "p99": p99_v,
                }
            )
        return out_legacy

    row_list = list(rows or ())
    if not row_list:
        return []
    raw_asns = [r["value"] if isinstance(r, Mapping) else r[0] for r in row_list]
    asn_names = resolve_asn_names_map(service_name, [int(v) for v in raw_asns if str(v).isdigit()])
    out: list[dict[str, Any]] = []
    for r in row_list:
        if isinstance(r, Mapping):
            raw_val = r["value"]
            asn_val: int | str = int(raw_val) if str(raw_val).isdigit() else raw_val
            requests = int(r["req_count"]) if "req_count" in r else r["requests"]
            avg_v = float_or_zero(r["avg_ms"]) if "avg_ms" in r else r["avg"]
            p50_v = float_or_zero(r["p50_ms"]) if "p50_ms" in r else r["p50"]
            p95_v = float_or_zero(r["p95_ms"]) if "p95_ms" in r else r["p95"]
            p99_v = float_or_zero(r["p99_ms"]) if "p99_ms" in r else r["p99"]
        else:
            raw_val = r[0]
            asn_val = int(raw_val) if str(raw_val).isdigit() else raw_val
            requests, avg_v, p50_v, p95_v, p99_v = r[1], r[2], r[3], r[4], r[5]
        label = format_asn_display_label(asn_val, asn_names) if isinstance(asn_val, int) else str(asn_val)
        out.append(
            {
                "asn": asn_val,
                "label": label,
                "requests": requests,
                "avg": avg_v,
                "p50": p50_v,
                "p95": p95_v,
                "p99": p99_v,
            }
        )
    return out


def finish_security_proxies(
    source_name: str | Iterable[Any] | None = None,
    stats: Mapping[str, Any] | Iterable[Mapping[str, Any]] | None = None,
    quality_rows: Iterable[Any] | bool | None = None,
    raw_isps: Iterable[Mapping[str, Any]] | None = None,
    raw_clients: Iterable[Mapping[str, Any]] | None = None,
    mask_ips: bool = False,
    *,
    flagged_rows: Iterable[Mapping[str, Any]] | None = None,
    asn_lookup: Mapping[Any, str] | None = None,
) -> dict[str, Any]:
    """Shape the security proxies payload (traffic quality, suspicious ISPs, active clients) across engines."""
    if not isinstance(source_name, str) or flagged_rows is not None or raw_isps is None:
        q_iter: Iterable[Any] = (
            source_name
            if (source_name is not None and not isinstance(source_name, str))
            else (quality_rows if isinstance(quality_rows, Iterable) else ())
        )
        f_iter: Iterable[Mapping[str, Any]] = (
            flagged_rows
            if flagged_rows is not None
            else (stats if (stats is not None and not isinstance(stats, Mapping)) else ())
        )
        effective_mask = bool(quality_rows) if isinstance(quality_rows, bool) else bool(mask_ips)

        q_list_legacy = list(q_iter)
        f_list = [dict(r) for r in f_iter if isinstance(r, Mapping)]

        traffic_quality_legacy: list[dict[str, Any]] = []
        if q_list_legacy:
            pairs_legacy: list[tuple[Any, int]] = []
            for item in q_list_legacy:
                if isinstance(item, Mapping):
                    row_lbl = item.get("type") or item.get("category") or item.get("label")
                    pairs_legacy.append((row_lbl, int(item.get("count") or 0)))
                else:
                    pairs_legacy.append((item[0], int(item[1] or 0)))
            total_cnt = sum(cnt for _, cnt in pairs_legacy)
            if total_cnt > 0:
                for row_type, row_count in pairs_legacy:
                    label = str(row_type if row_type is not None else "Unknown")
                    value = round((float(row_count) / total_cnt) * 100, 1)
                    traffic_quality_legacy.append({"label": label, "value": value})

        lookup_map = dict(asn_lookup or {})
        if not lookup_map:
            asns_needed = [
                asn_int for r in f_list if (asn_int := parse_asn_int(r.get("asn"))) is not None
            ]
            if asns_needed:
                try:
                    from backend.core.fos_client import get_asn_names

                    lookup_map = get_asn_names("default", list(set(asns_needed)))
                except Exception:
                    lookup_map = {}

        def _legacy_asn_display(asn_int: int | None) -> str:
            if asn_int is not None:
                found = lookup_map.get(asn_int) or lookup_map.get(str(asn_int))
                if found:
                    return str(found)
                return f"AS{asn_int}"
            return "Unknown ISP"

        isp_counts: dict[int | None, int] = {}
        active_clients_legacy: list[dict[str, Any]] = []
        dist_mismatch_ips: set[str] = set()
        proxy_ips: set[str] = set()
        total_tunnel_reqs = 0

        if effective_mask:
            from backend.core.share_db import mask_ip

        for row in f_list:
            raw_ip = str(row.get("ip") or "")
            if raw_ip:
                proxy_ips.add(raw_ip)
            reqs = int(row.get("requests") or row.get("count") or 1)
            total_tunnel_reqs += reqs
            asn_int = parse_asn_int(row.get("asn"))
            isp_counts[asn_int] = isp_counts.get(asn_int, 0) + reqs

            impossible = bool(row.get("impossible_distance") or row.get("impossible_dist"))
            if not impossible:
                rtt = float_or_zero(row.get("rtt_min_ms", row.get("rtt_ms")))
                dist = float_or_zero(row.get("distance_km"))
                if rtt > 0 and dist > (rtt * 100.0 + 150.0):
                    impossible = True
            if impossible and raw_ip:
                dist_mismatch_ips.add(raw_ip)

            risk_level = str(row.get("risk_level") or ("High" if impossible else "Medium"))
            client_item = {**row}
            if effective_mask and client_item.get("ip"):
                client_item["ip"] = mask_ip(str(client_item["ip"]))
            client_item["impossible_distance"] = impossible
            client_item["risk_level"] = risk_level
            client_item["asn_name"] = _legacy_asn_display(asn_int)
            client_item.pop("asn", None)
            active_clients_legacy.append(client_item)

        suspicious_isps_legacy = [
            {
                "isp": _legacy_asn_display(asn_val),
                "asn": asn_val,
                "count": cnt,
            }
            for asn_val, cnt in sorted(isp_counts.items(), key=lambda kv: kv[1], reverse=True)[:10]
        ]

        return {
            "active_proxies_count": len(proxy_ips),
            "tunnel_requests_count": total_tunnel_reqs,
            "distance_mismatches_count": len(dist_mismatch_ips),
            "traffic_quality": traffic_quality_legacy,
            "suspicious_isps": suspicious_isps_legacy,
            "active_clients": active_clients_legacy[:50],
        }

    stats_map: Mapping[str, Any] = stats if isinstance(stats, Mapping) else {}
    q_list = list(quality_rows if isinstance(quality_rows, Iterable) else ())
    isp_list = list(raw_isps or ())
    client_list = list(raw_clients or ())

    traffic_quality: list[dict[str, Any]] = []
    if q_list:
        pairs: list[tuple[Any, int]] = []
        for item in q_list:
            if isinstance(item, Mapping):
                pairs.append((item.get("type"), int(item.get("count") or 0)))
            else:
                pairs.append((item[0], int(item[1] or 0)))
        total_count = sum(cnt for _, cnt in pairs)
        if total_count > 0:
            for row_type, row_count in pairs:
                label = str(row_type if row_type is not None else "Unknown")
                value = round((float(row_count) / total_count) * 100, 1)
                traffic_quality.append({"label": label, "value": value})

    asns_to_resolve = {
        asn_int for row in (*isp_list, *client_list) if (asn_int := parse_asn_int(row.get("asn"))) is not None
    }
    from backend.core.fos_client import get_asn_names

    asn_names = get_asn_names(source_name, list(asns_to_resolve))

    def _asn_display(asn_int: int | None) -> str:
        return (asn_names.get(asn_int) if asn_int is not None else None) or (
            f"AS{asn_int}" if asn_int else "Unknown ISP"
        )

    suspicious_isps = [
        {
            "isp": _asn_display(asn_int := parse_asn_int(row.get("asn"))),
            "asn": asn_int,
            "count": int(row.get("count", 0)),
        }
        for row in isp_list
    ]

    active_clients: list[dict[str, Any]] = []
    if mask_ips:
        from backend.core.share_db import mask_ip
    for row in client_list:
        asn_int = parse_asn_int(row.get("asn"))
        client_item = {**row}
        if mask_ips and client_item.get("ip"):
            client_item["ip"] = mask_ip(str(client_item["ip"]))
        client_item["asn_name"] = _asn_display(asn_int)
        client_item.pop("asn", None)
        active_clients.append(client_item)

    return {
        "active_proxies_count": int(stats_map.get("active_proxies_count", 0)),
        "tunnel_requests_count": int(stats_map.get("total_requests_count", 0)),
        "distance_mismatches_count": int(stats_map.get("distance_mismatches_count", 0)),
        "traffic_quality": traffic_quality,
        "suspicious_isps": suspicious_isps,
        "active_clients": active_clients,
    }


def build_insight_asn_context(
    service_id: str,
    rows: Iterable[Any],
    insight_id: str,
    *,
    include_dma: bool | None = None,
) -> dict[str, Any]:
    """Build ``{"asn_names": ..., "dma_map": ...}`` context for ASN/metro insights across ClickHouse."""
    if not any(marker in insight_id for marker in ("asn", "metro")):
        return {}
    row_list = list(rows)
    if not row_list:
        return {}
    from backend.core import fos_client as _db

    asn_vals = [r["asn"] if isinstance(r, Mapping) else r[0] for r in row_list if r]
    context: dict[str, Any] = {
        "asn_names": _db.get_asn_names(service_id, asn_vals),
    }
    want_dma = ("metro" in insight_id) if include_dma is None else include_dma
    if want_dma:
        context["dma_map"] = _db._get_dma_map()
    return context


def build_asn_speed_mix(
    speed_rows: Iterable[Any],
    *,
    top_k: int = 5,
) -> dict[int, dict[str, float]]:
    """Build per-ASN connection speed share maps (top ``top_k`` speed classes per ASN)."""
    asn_speed_rows: dict[int, list[tuple[str, int]]] = {}
    for r in speed_rows:
        if isinstance(r, Mapping):
            raw_asn, raw_speed, raw_cnt = r.get("asn"), r.get("c_speed"), r.get("cnt")
        else:
            raw_asn, raw_speed, raw_cnt = r[0], r[1], r[2]
        if raw_asn is None:
            continue
        asn_v = int(raw_asn)
        bucket = asn_speed_rows.setdefault(asn_v, [])
        if len(bucket) < top_k:
            bucket.append((str(raw_speed), int(raw_cnt or 0)))
    return {
        asn_v: {cs: round(cnt / total, 3) for cs, cnt in rows}
        for asn_v, rows in asn_speed_rows.items()
        if (total := sum(cnt for _, cnt in rows)) > 0
    }


def avg_bucket_health_score(
    buckets_data: Mapping[str, Mapping[str, Any]],
    keys: Iterable[str],
) -> float | None:
    """Average ``health_score`` over a sequence of bucket keys, or ``None`` if no bucket has a score."""
    scores = [hs for b in keys if b in buckets_data and (hs := buckets_data[b].get("health_score")) is not None]
    return round(sum(scores) / len(scores), 1) if scores else None


def build_network_asn_leaderboard(
    top_asns: Sequence[int],
    asn_labels: Mapping[int, str],
    asn_bucket_data: Mapping[int, Mapping[str, Mapping[str, Any]]],
    all_buckets: Sequence[str],
    bucket_seconds: int,
    all_asns_seen: Mapping[int, int],
    asn_speed_mix: Mapping[int, Mapping[str, float]],
    asn_rtt_pct: Mapping[int, Mapping[str, float | None]],
) -> list[dict[str, Any]]:
    """Build the ASN health leaderboard with 1h/1w trend analysis."""
    leaderboard: list[dict[str, Any]] = []
    for asn in top_asns:
        buckets_data = asn_bucket_data.get(asn, {})
        if not buckets_data:
            continue
        label = asn_labels.get(asn, f"AS{asn}")
        latest_bucket = max(buckets_data)
        hs_now = buckets_data[latest_bucket].get("health_score")
        hs_1h = hs_now
        hs_1w = hs_now
        if len(all_buckets) > 1:
            n_buckets_1h = max(1, 3600 // bucket_seconds)
            sorted_buckets = sorted(buckets_data)
            early = sorted_buckets[: max(1, len(sorted_buckets) // 4)]
            hs_1w = avg_bucket_health_score(buckets_data, early)
            mid = sorted_buckets[max(0, len(sorted_buckets) - n_buckets_1h) :]
            hs_1h = avg_bucket_health_score(buckets_data, mid)

        trend = "stable"
        if hs_now is not None and hs_1h is not None:
            delta = hs_now - hs_1h
            if delta < -5:
                trend = "degrading"
            elif delta > 5:
                trend = "improving"

        rtt_pct = asn_rtt_pct.get(asn, {})
        leaderboard.append(
            {
                "asn": asn,
                "label": label,
                "health_score_now": hs_now,
                "health_score_1h_ago": hs_1h,
                "health_score_1w_ago": hs_1w,
                "trend": trend,
                "total_reqs": all_asns_seen.get(asn, 0),
                "c_speed_mix": dict(asn_speed_mix.get(asn, {})),
                "p95_rtt_us": rtt_pct.get("p95_rtt_us"),
                "p99_rtt_us": rtt_pct.get("p99_rtt_us"),
            }
        )
    return leaderboard

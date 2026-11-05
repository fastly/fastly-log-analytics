from __future__ import annotations

import os
import re
from typing import Any

from backend.repositories.utils.response_shaping import (
    empty_security_proxies,
    finish_security_proxies,
    parse_asn_int,
)

_BOT_UA_CLASSIFY_LIMIT = int(os.getenv("BOT_UA_CLASSIFY_LIMIT", "2000"))
_RDNS_LOOKUP_LIMIT = int(os.getenv("SECURITY_RDNS_LOOKUP_LIMIT", "1000"))
_RDNS_ENQUEUE_LIMIT = int(os.getenv("SECURITY_RDNS_ENQUEUE_LIMIT", "500"))

_empty_proxies_payload = empty_security_proxies
_parse_asn_int = parse_asn_int
_finish_proxies = finish_security_proxies


def classify_ua_counts(rows: list[tuple[str, int]], n: int) -> list[dict]:
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


def summarize_wellknown_bot_rows(rows: list) -> list[dict]:
    if not rows:
        return []
    from backend.utils.bot_sources import build_matcher, classify, enqueue, get_hostnames

    match_ua = build_matcher()
    bot_agg: dict[str, dict] = {}
    new_ips: set[str] = set()
    ua_matches: dict[str, list[dict]] = {}
    matched_rows: list[tuple[list[dict], str, int]] = []
    ip_totals: dict[str, int] = {}
    for ua_val, ip_val, cnt in rows:
        if not ua_val or not ip_val:
            continue
        matches = ua_matches.get(ua_val)
        if matches is None:
            matches = list(match_ua(ua_val))
            ua_matches[ua_val] = matches
        if not matches:
            continue
        cnt_int = int(cnt or 0)
        matched_rows.append((matches, ip_val, cnt_int))
        ip_totals[ip_val] = ip_totals.get(ip_val, 0) + cnt_int
    if not matched_rows:
        return []
    if len(ip_totals) > _RDNS_LOOKUP_LIMIT:
        top_ips = [ip for ip, _ in sorted(ip_totals.items(), key=lambda kv: kv[1], reverse=True)[:_RDNS_LOOKUP_LIMIT]]
    else:
        top_ips = list(ip_totals)
    hostnames = get_hostnames(top_ips)
    top_ips_set = set(top_ips)
    classify_cache: dict[tuple[str, str], str] = {}
    for matches, ip_val, cnt in matched_rows:
        hostname, status, fcrdns_verified = hostnames.get(ip_val, (None, "pending", False))
        if ip_val not in top_ips_set and ip_val not in hostnames and len(new_ips) < _RDNS_ENQUEUE_LIMIT:
            new_ips.add(ip_val)
        for entry in matches:
            bot_id = entry.get("id", "unknown")
            cache_key = (bot_id, ip_val)
            state = classify_cache.get(cache_key)
            if state is None:
                verification = entry.get("verification", {})
                verification_domains = verification.get("domains", [])
                verification_cidrs = tuple(verification.get("cidrs") or ())
                state = classify(ip_val, hostname, status, fcrdns_verified, verification_domains, verification_cidrs)
                classify_cache[cache_key] = state
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
    if new_ips:
        enqueue(list(new_ips))
    sorted_bots = sorted(bot_agg.values(), key=lambda x: x["request_count"], reverse=True)[:50]
    for b in sorted_bots:
        total = b["request_count"]
        covered = b["verified_count"] + b["impersonator_count"]
        b["verification_coverage"] = round(covered / total, 3) if total else 0.0
    return sorted_bots


_VOWEL_TABLE = str.maketrans("", "", "aeiou")


def _stems_match(bot_stem: str, ref_stem: str) -> bool:
    if not bot_stem or not ref_stem:
        return False
    if bot_stem == ref_stem:
        return True
    b_cons = bot_stem.translate(_VOWEL_TABLE)
    r_cons = ref_stem.translate(_VOWEL_TABLE)
    if len(b_cons) >= 3 and b_cons == r_cons:
        return True
    return len(bot_stem) >= 3 and len(ref_stem) >= 3 and (bot_stem in ref_stem or ref_stem in bot_stem)


_GENERIC_BOT_RE = re.compile(
    r"([a-z0-9_.-]*(?:bot|crawler|spider|slurp|archiver|scraper|externalagent|extended)[a-z0-9_.-]*)", re.IGNORECASE
)


def summarize_bot_efficiency_rows(rows: list[tuple[str, str, int, int]]) -> list[dict[str, Any]]:
    from backend.utils.bot_sources import (
        _BOT_TOKEN_SUFFIX_RE,
        build_well_known_bots_matcher,
        extract_entry_referral_stems,
        load_well_known_bots_catalog,
        resolve_bot_display_name,
    )

    matcher = build_well_known_bots_matcher()
    catalog = load_well_known_bots_catalog() or []
    vendor_stems: dict[str, set[str]] = {}
    for entry in catalog:
        if not isinstance(entry, dict):
            continue
        vid = (entry.get("id") or "").split("-")[0].lower()
        if vid:
            vendor_stems.setdefault(vid, set()).update(extract_entry_referral_stems(entry))
    total_site_bytes = 0
    ref_counts: dict[str, int] = {}
    ua_rows: list[tuple[str, int, int]] = []
    for kind, key, cnt, r_bytes in rows:
        cnt_i = int(cnt or 0)
        bytes_i = int(r_bytes or 0)
        if kind == "site":
            total_site_bytes += bytes_i
        elif kind == "ref" and key:
            ref_counts[key.lower()] = ref_counts.get(key.lower(), 0) + cnt_i
        elif kind == "ua" and key:
            ua_rows.append((key, cnt_i, bytes_i))
    bot_agg: dict[str, dict[str, Any]] = {}
    for ua, cnt_i, bytes_i in ua_rows:
        matches = matcher(ua)
        matched_entry = matches[0] if matches else None
        if isinstance(matched_entry, dict):
            bot_id = matched_entry.get("id", "unknown")
            bot_name = bot_id.replace("-", " ").title()
            cats = matched_entry.get("categories") or []
            category = cats[0].replace("-", " ").title() if cats else "Uncategorized"
            vid = bot_id.split("-")[0].lower()
            stems = set(extract_entry_referral_stems(matched_entry)) | vendor_stems.get(vid, set())
        else:
            m = _GENERIC_BOT_RE.search(ua.lower())
            if not m:
                continue
            raw_tok = m.group(1)
            bot_id = raw_tok
            bot_name = resolve_bot_display_name(raw_tok, default="Other Bot")
            category = "General Bot"
            stem = _BOT_TOKEN_SUFFIX_RE.sub("", raw_tok).strip("-_.")
            stems = {stem} if len(stem) >= 3 else set()
        bucket = bot_agg.get(bot_id)
        if bucket is None:
            bucket = {"bot_name": bot_name, "category": category, "request_count": 0, "resp_bytes": 0, "stems": set()}
            bot_agg[bot_id] = bucket
        bucket["request_count"] += cnt_i
        bucket["resp_bytes"] += bytes_i
        bucket["stems"].update(stems)
    denom_bytes = max(total_site_bytes, 1)
    results: list[dict[str, Any]] = []
    for b in bot_agg.values():
        b_stems: set[str] = b["stems"]
        referrals = sum(rcnt for rstem, rcnt in ref_counts.items() if any(_stems_match(bs, rstem) for bs in b_stems))
        req_cnt = int(b["request_count"])
        resp_b = int(b["resp_bytes"])
        ref_denom = max(referrals, 1)
        results.append(
            {
                "bot_name": b["bot_name"],
                "category": b["category"],
                "request_count": req_cnt,
                "resp_bytes": resp_b,
                "bandwidth_pct": round(resp_b * 100.0 / denom_bytes, 2),
                "referrals": referrals,
                "requests_per_referral": round(req_cnt * 1.0 / ref_denom, 1),
                "bytes_per_referral": round(resp_b * 1.0 / ref_denom, 1),
            }
        )
    results.sort(key=lambda x: (x["resp_bytes"], x["request_count"]), reverse=True)
    return results[:25]


for _fn in (
    classify_ua_counts,
    summarize_wellknown_bot_rows,
    _stems_match,
    summarize_bot_efficiency_rows,
):
    _fn.__module__ = "backend.repositories.security"

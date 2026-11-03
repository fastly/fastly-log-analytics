"""Canonical synthetic request-log factory (`generate_synthetic_traffic.py` and `scale_harness.py`)."""

from __future__ import annotations

import json
import math
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, quote, urlparse

if TYPE_CHECKING:
    import numpy as np
    import pyarrow as pa

# fmt: off
COUNTRIES = ["US", "GB", "DE", "JP", "BR", "IN", "AU", "FR", "CA", "NL", "SG", "ES", "IT", "SE", "KR", "MX", "ZA", "CH", "PL", "IE"]
_CW = [0.35, 0.08, 0.07, 0.06, 0.05, 0.05, 0.04, 0.04, 0.04, 0.03, 0.03, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.01, 0.01, 0.01]
COUNTRY_WEIGHTS = [w / sum(_CW) for w in _CW]

COUNTRY_GEO_MAP = {
    "US": {"city": "New York", "metro": "501", "lat": 40.7128, "lon": -74.0060, "region": "NY"},
    "GB": {"city": "London", "metro": "826", "lat": 51.5074, "lon": -0.1278, "region": "ENG"},
    "DE": {"city": "Frankfurt", "metro": "276", "lat": 50.1109, "lon": 8.6821, "region": "HE"},
    "JP": {"city": "Tokyo", "metro": "392", "lat": 35.6762, "lon": 139.6503, "region": "13"},
    "BR": {"city": "Sao Paulo", "metro": "076", "lat": -23.5505, "lon": -46.6333, "region": "SP"},
    "IN": {"city": "Mumbai", "metro": "356", "lat": 19.0760, "lon": 72.8777, "region": "MH"},
    "AU": {"city": "Sydney", "metro": "036", "lat": -33.8688, "lon": 151.2093, "region": "NSW"},
    "FR": {"city": "Paris", "metro": "250", "lat": 48.8566, "lon": 2.3522, "region": "IDF"},
    "CA": {"city": "Toronto", "metro": "124", "lat": 43.6532, "lon": -79.3832, "region": "ON"},
    "NL": {"city": "Amsterdam", "metro": "528", "lat": 52.3676, "lon": 4.9041, "region": "NH"},
    "SG": {"city": "Singapore", "metro": "702", "lat": 1.3521, "lon": 103.8198, "region": "SG"},
    "ES": {"city": "Madrid", "metro": "724", "lat": 40.4168, "lon": -3.7038, "region": "MD"},
    "IT": {"city": "Milan", "metro": "380", "lat": 45.4642, "lon": 9.1900, "region": "25"},
    "SE": {"city": "Stockholm", "metro": "752", "lat": 59.3293, "lon": 18.0686, "region": "AB"},
    "KR": {"city": "Seoul", "metro": "410", "lat": 37.5665, "lon": 126.9780, "region": "11"},
    "MX": {"city": "Mexico City", "metro": "484", "lat": 19.4326, "lon": -99.1332, "region": "DIF"},
    "ZA": {"city": "Johannesburg", "metro": "710", "lat": -26.2041, "lon": 28.0473, "region": "GP"},
    "CH": {"city": "Zurich", "metro": "756", "lat": 47.3769, "lon": 8.5417, "region": "ZH"},
    "PL": {"city": "Warsaw", "metro": "616", "lat": 52.2297, "lon": 21.0122, "region": "MZ"},
    "IE": {"city": "Dublin", "metro": "372", "lat": 53.3498, "lon": -6.2603, "region": "L"},
}

POP_COORDS = {
    "IAD": (38.95, -77.45), "LHR": (51.47, -0.45), "NRT": (35.77, 140.39), "SYD": (-33.94, 151.18),
    "SFO": (37.62, -122.38), "FRA": (50.03, 8.57), "CDG": (49.01, 2.55), "SIN": (1.36, 103.99),
    "ORD": (41.98, -87.90), "DFW": (32.90, -97.04), "AMS": (52.31, 4.76), "HKG": (22.31, 113.91),
    "JFK": (40.64, -73.78), "LAX": (33.94, -118.41), "MIA": (25.79, -80.29), "SEA": (47.45, -122.31),
    "DEN": (39.86, -104.67), "ATL": (33.64, -84.43), "BOS": (42.36, -71.01), "YYZ": (43.68, -79.62),
}
POPS = ["IAD", "LHR", "NRT", "SYD", "SFO", "FRA", "CDG", "SIN", "ORD", "DFW", "AMS", "HKG", "JFK", "LAX", "MIA", "SEA", "DEN", "ATL", "BOS", "YYZ"]
POP_WAVE_POPS = [
    "IAD", "LHR", "NRT", "SYD", "SFO", "FRA", "CDG", "SIN", "ORD", "DFW",
    "AMS", "HKG", "JFK", "LAX", "MIA", "SEA", "DEN", "ATL", "BOS", "YYZ",
    "MAD", "MXP", "ARN", "ICN", "GRU", "BOM", "JNB", "ZRH", "WAW", "DUB",
    "SJC", "EWR",
]
_IP_POOL_SIZE = 254 * 254
ASN_POOL_SIZE = 3000
GEO_CELLS_PER_COUNTRY = 24
_VECTOR_POOLS: dict[str, Any] = {}


def build_pop_wave_weights(pop_count: int) -> tuple[list[str], list[float]]:
    """Return ``(pops, weights)`` with Zipf-like edge traffic distribution across ``pop_count`` POPs."""
    if pop_count <= 0:
        raise ValueError(f"pop_count must be > 0 (got {pop_count})")
    pops = [
        POP_WAVE_POPS[i] if i < len(POP_WAVE_POPS) else f"P{i:02d}"
        for i in range(pop_count)
    ]
    raw_weights = [1.0 / ((i + 1) ** 0.75) for i in range(pop_count)]
    total = sum(raw_weights)
    return pops, [w / total for w in raw_weights]


def _ensure_vector_pools() -> dict[str, Any]:
    if not _VECTOR_POOLS:
        import numpy as np

        _VECTOR_POOLS["ip"] = np.array(
            [f"198.51.{(i // 254) & 0xFF}.{i % 254 + 1}" for i in range(_IP_POOL_SIZE)],
            dtype=object,
        )
        _VECTOR_POOLS["ja3"] = np.array([f"ja3-{i:04x}" for i in range(250)], dtype=object)
        _VECTOR_POOLS["ja4"] = np.array([f"ja4-{i:04x}" for i in range(250)], dtype=object)
        _VECTOR_POOLS["sess"] = np.array([f"sess_{i:08x}" for i in range(500)], dtype=object)
        _VECTOR_POOLS["countries"] = np.array(COUNTRIES, dtype=object)
        _VECTOR_POOLS["cities"] = np.array([COUNTRY_GEO_MAP[c]["city"] for c in COUNTRIES], dtype=object)
        _VECTOR_POOLS["metros"] = np.array([COUNTRY_GEO_MAP[c]["metro"] for c in COUNTRIES], dtype=object)
        _VECTOR_POOLS["lats"] = np.array([COUNTRY_GEO_MAP[c]["lat"] for c in COUNTRIES], dtype=np.float64)
        _VECTOR_POOLS["lons"] = np.array([COUNTRY_GEO_MAP[c]["lon"] for c in COUNTRIES], dtype=np.float64)
        _VECTOR_POOLS["regions"] = np.array([COUNTRY_GEO_MAP[c]["region"] for c in COUNTRIES], dtype=object)
        _VECTOR_POOLS["asns"] = np.arange(1000, 1000 + ASN_POOL_SIZE, dtype=np.int32)
        zipf = 1.0 / np.arange(1, ASN_POOL_SIZE + 1) ** 1.1
        _VECTOR_POOLS["asn_weights"] = zipf / zipf.sum()
        _VECTOR_POOLS["urls"] = np.array(URL_PATHS, dtype=object)
        _VECTOR_POOLS["uas"] = np.array(USER_AGENTS, dtype=object)
    return _VECTOR_POOLS
HOSTS = ["www.example.com", "api.example.com", "assets.example.com"]
HOST_WEIGHTS = [0.75, 0.20, 0.05]
METHODS = ["GET", "POST", "HEAD", "OPTIONS", "PUT", "DELETE"]
METHOD_WEIGHTS = [0.85, 0.10, 0.02, 0.01, 0.01, 0.01]
PROTOCOLS = ["HTTP/2", "HTTP/1.1", "HTTP/3"]
PROTO_WEIGHTS = [0.70, 0.20, 0.10]
NORMAL_STATUSES = [200, 204, 301, 302, 304, 400, 401, 403, 404, 500, 502, 503]
_NSW = [0.72, 0.05, 0.02, 0.01, 0.10, 0.01, 0.005, 0.01, 0.05, 0.01, 0.005, 0.01]
NORMAL_STATUS_WEIGHTS = [w / sum(_NSW) for w in _NSW]
CACHE_NORMAL = ["HIT", "HIT-CLUSTER", "MISS", "PASS", "ERROR"]
CACHE_NORMAL_WEIGHTS = [0.70, 0.12, 0.12, 0.05, 0.01]
ORIGIN_FETCH_STATES = ["MISS", "PASS", "ERROR"]
ORIGIN_IPS = [f"203.0.113.{octet}" for octet in range(10, 18)]

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/127.0.0.0 Safari/537.36",
]
BOT_USER_AGENTS = [
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
    "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)",
    "AhrefsBot/7.0; +http://ahrefs.com/robot/",
    "SemrushBot/7~bl; +http://www.semrush.com/bot.html",
    "python-requests/2.31.0",
    "Scrapy/2.11.0 (+https://scrapy.org)",
]
BOT_CATEGORIES = ["Search Engine", "SEO / Analytics", "Social Media", "AI Crawler", "Automated Scraper"]
NGWAF_SIGNALS = ["DATACENTER", "SUSPICIOUS-UA", "SCRAPER", "SQLI-PROBE", "XSS-PROBE", "RATE-LIMIT"]
URL_PATHS = [
    "/", "/index.html", "/products", "/products/search", "/api/v1/catalog",
    "/api/v2/checkout", "/api/v1/auth/login", "/api/v1/user/profile", "/assets/app.js",
    "/assets/styles.css", "/images/hero.webp", "/images/logo.png", "/docs/getting-started", "/blog/announcements",
]


def _physical_rtts(rtt_min: Any, tcp_rtt: Any, pop: Any, lat: Any, lon: Any) -> tuple[Any, Any]:
    """Raise RTTs to the light-in-fibre floor (100 km/ms) so only genuinely odd rows look like proxies."""
    import numpy as np

    labels, inverse = np.unique(np.asarray(pop, dtype=object).astype(str), return_inverse=True)
    pop_lat = np.array([POP_COORDS.get(label, (38.0, -97.0))[0] for label in labels])[inverse]
    pop_lon = np.array([POP_COORDS.get(label, (38.0, -97.0))[1] for label in labels])[inverse]
    p1, p2 = np.radians(lat), np.radians(pop_lat)
    a = np.sin((p2 - p1) / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(np.radians(pop_lon - lon) / 2) ** 2
    distance_km = 6371 * 2 * np.arcsin(np.sqrt(a))
    floor_us = (distance_km * 10).astype(np.int64)
    rtt_min = np.maximum(rtt_min, floor_us).astype(np.int32)
    return rtt_min, np.maximum(tcp_rtt, rtt_min).astype(np.int32)


def generate_columns(
    n: int,
    start_ms: int,
    end_ms: int,
    scenario: str,
    rng: np.random.Generator,
    *,
    pop_override: str | None = None,
) -> dict[str, Any]:
    """Generate realistic columnar data for n log events."""
    import numpy as np

    pools = _ensure_vector_pools()
    span_ms = max(end_ms - start_ms, 1)
    u = rng.uniform(0, 1, size=n)
    if scenario == "diurnal":
        phase = 2 * math.pi * ((start_ms / 1000 / 3600) % 24 - 6) / 24
        ts_ms = start_ms + (((u + 0.2 * np.sin(2 * math.pi * u + phase)) % 1.0) * span_ms).astype(np.int64)
    elif scenario == "ddos-spike":
        spike_center, spike_radius = start_ms + span_ms // 2, min(span_ms // 8, 15 * 60 * 1000)
        is_spike = rng.random(n) < 0.40
        spike_ts = rng.integers(spike_center - spike_radius, spike_center + spike_radius, size=n, dtype=np.int64)
        ts_ms = np.where(is_spike, spike_ts, (start_ms + u * span_ms).astype(np.int64))
    elif scenario == "origin-5xx-outage":
        outage_start = start_ms + span_ms // 2
        outage_end = outage_start + min(span_ms // 4, 30 * 60 * 1000)
        ts_ms = (start_ms + u * span_ms).astype(np.int64)
    else:
        ts_ms = (start_ms + u * span_ms).astype(np.int64)

    ts_us = ts_ms * 1000
    host = rng.choice(HOSTS, size=n, p=HOST_WEIGHTS)
    method = rng.choice(METHODS, size=n, p=METHOD_WEIGHTS)
    proto = rng.choice(PROTOCOLS, size=n, p=PROTO_WEIGHTS)
    country_idx = rng.choice(len(COUNTRIES), size=n, p=COUNTRY_WEIGHTS)
    country = pools["countries"][country_idx]
    pop = np.full(n, pop_override, dtype=object) if pop_override else rng.choice(POPS, size=n)

    city = pools["cities"][country_idx]
    metro = pools["metros"][country_idx]
    geo_cell = rng.integers(0, GEO_CELLS_PER_COUNTRY, size=n)
    lat = pools["lats"][country_idx] + (geo_cell % 6 - 2.5) * 0.4
    lon = pools["lons"][country_idx] + (geo_cell // 6 - 1.5) * 0.4
    region = pools["regions"][country_idx]

    url_idx = rng.integers(0, len(URL_PATHS), size=n)
    url = pools["urls"][url_idx]
    ip_int = rng.integers(0, _IP_POOL_SIZE, size=n)
    ip = pools["ip"][ip_int]
    ua_idx = rng.integers(0, len(USER_AGENTS), size=n)
    ua = pools["uas"][ua_idx]
    asn = rng.choice(pools["asns"], size=n, p=pools["asn_weights"])

    if scenario == "origin-5xx-outage":
        in_outage = (ts_ms >= outage_start) & (ts_ms <= outage_end)
        status = rng.choice(NORMAL_STATUSES, size=n, p=NORMAL_STATUS_WEIGHTS).astype(np.int32)
        outage_statuses = rng.choice([502, 503, 504], size=n, p=[0.25, 0.65, 0.10]).astype(np.int32)
        status = np.where(in_outage, outage_statuses, status)
        cache = rng.choice(CACHE_NORMAL, size=n, p=CACHE_NORMAL_WEIGHTS)
        outage_cache = rng.choice(["MISS", "PASS", "ERROR"], size=n, p=[0.75, 0.20, 0.05])
        cache = np.where(in_outage, outage_cache, cache)
    elif scenario == "ddos-spike":
        status = rng.choice(NORMAL_STATUSES, size=n, p=NORMAL_STATUS_WEIGHTS).astype(np.int32)
        spike_statuses = rng.choice([429, 503, 200], size=n, p=[0.60, 0.25, 0.15]).astype(np.int32)
        status = np.where(is_spike, spike_statuses, status)
        cache = rng.choice(CACHE_NORMAL, size=n, p=CACHE_NORMAL_WEIGHTS)
        spike_cache = rng.choice(["MISS", "PASS"], size=n, p=[0.85, 0.15])
        cache = np.where(is_spike, spike_cache, cache)
        attack_ip_choice = rng.choice(["203.0.113.55", "203.0.113.88", "198.51.100.99"], size=n)
        attack_url_choice = rng.choice(["/api/v1/auth/login", "/api/v2/checkout"], size=n)
        ip = np.where(is_spike, attack_ip_choice, ip)
        url = np.where(is_spike, attack_url_choice, url)
        asn = np.where(is_spike, 13335, asn)
    elif scenario == "bot-scrape":
        status = rng.choice([200, 301, 304, 403, 404, 429], size=n, p=[0.65, 0.05, 0.15, 0.05, 0.08, 0.02]).astype(np.int32)
        cache = rng.choice(CACHE_NORMAL, size=n, p=CACHE_NORMAL_WEIGHTS)
        ua = rng.choice(BOT_USER_AGENTS, size=n)
    else:
        status = rng.choice(NORMAL_STATUSES, size=n, p=NORMAL_STATUS_WEIGHTS).astype(np.int32)
        cache = rng.choice(CACHE_NORMAL, size=n, p=CACHE_NORMAL_WEIGHTS)

    if scenario == "slow-network":
        elapsed = np.clip(rng.lognormal(mean=np.log(800), sigma=0.8, size=n).astype(np.int32), 100, 30_000)
        tls_time = np.clip(rng.lognormal(mean=np.log(250), sigma=0.6, size=n), 50, 2000).astype(np.int32)
        ottfb = (elapsed * rng.uniform(0.3, 0.7, size=n)).astype(np.int32)
        ottlb = (ottfb + rng.lognormal(mean=np.log(50), sigma=0.8, size=n)).astype(np.int32)
        ttfb = (elapsed * rng.uniform(0.7, 0.95, size=n)).astype(np.int32)
        waf_ms = np.clip(rng.lognormal(mean=np.log(8), sigma=0.4, size=n), 1, 100).astype(np.int32)
        tcp_rtt = np.clip(rng.lognormal(mean=np.log(180000), sigma=0.6, size=n).astype(np.int32), 10000, 2000000)
        rtt_min = np.clip((tcp_rtt * rng.uniform(0.5, 0.8, size=n)).astype(np.int32), 5000, None)
        rtt_var = np.clip((tcp_rtt * rng.uniform(0.05, 0.25, size=n)).astype(np.int32), 1000, None)
        ploss = rng.choice([0.0, 0.005, 0.01, 0.02, 0.05, 0.10], size=n, p=[0.60, 0.12, 0.10, 0.08, 0.06, 0.04])
    elif scenario == "origin-5xx-outage":
        elapsed = rng.lognormal(mean=np.log(25), sigma=1.0, size=n).astype(np.int32)
        ottfb = (elapsed * rng.uniform(0.1, 0.7, size=n)).astype(np.int32)
        ottlb = (ottfb + 10).astype(np.int32)
        outage_elapsed = rng.integers(2000, 8000, size=n, dtype=np.int32)
        elapsed = np.where(in_outage, outage_elapsed, elapsed)
        ottfb = np.where(in_outage, (outage_elapsed * 0.9).astype(np.int32), ottfb)
        ottlb = np.where(in_outage, outage_elapsed, ottlb)
        ttfb = (elapsed * 0.85).astype(np.int32)
        tls_time = np.clip(rng.lognormal(mean=np.log(35), sigma=0.4, size=n), 10, 500).astype(np.int32)
        waf_ms = np.clip(rng.lognormal(mean=np.log(4), sigma=0.3, size=n), 1, 50).astype(np.int32)
        tcp_rtt = np.clip(rng.lognormal(mean=np.log(25000), sigma=0.5, size=n).astype(np.int32), 1000, 500000)
        rtt_min = np.clip((tcp_rtt * rng.uniform(0.7, 0.9, size=n)).astype(np.int32), 500, None)
        rtt_var = np.clip((tcp_rtt * rng.uniform(0.01, 0.12, size=n)).astype(np.int32), 100, None)
        ploss = rng.choice([0.0, 0.001, 0.005, 0.01, 0.02], size=n, p=[0.92, 0.04, 0.02, 0.01, 0.01])
    else:
        elapsed = np.clip(rng.lognormal(mean=np.log(25), sigma=1.0, size=n).astype(np.int32), 1, 10_000)
        ttfb = (elapsed * rng.uniform(0.2, 0.8, size=n)).astype(np.int32)
        ottfb = np.where(cache == "HIT", 0, (elapsed * rng.uniform(0.3, 0.8, size=n)).astype(np.int32))
        ottlb = np.where(cache == "HIT", 0, (ottfb + rng.lognormal(mean=np.log(10), sigma=0.5, size=n)).astype(np.int32))
        tls_time = np.clip(rng.lognormal(mean=np.log(35), sigma=0.4, size=n), 5, 500).astype(np.int32)
        waf_ms = np.clip(rng.lognormal(mean=np.log(3), sigma=0.4, size=n), 1, 50).astype(np.int32)
        tcp_rtt = np.clip(rng.lognormal(mean=np.log(25000), sigma=0.5, size=n).astype(np.int32), 1000, 500000)
        rtt_min = np.clip((tcp_rtt * rng.uniform(0.7, 0.9, size=n)).astype(np.int32), 500, None)
        rtt_var = np.clip((tcp_rtt * rng.uniform(0.01, 0.12, size=n)).astype(np.int32), 100, None)
        ploss = rng.choice([0.0, 0.001, 0.005, 0.01, 0.02], size=n, p=[0.95, 0.02, 0.01, 0.01, 0.01])

    rtt_min, tcp_rtt = _physical_rtts(rtt_min, tcp_rtt, pop, lat, lon)

    resp_bytes = np.clip(rng.lognormal(mean=np.log(8_000), sigma=1.5, size=n).astype(np.int64), 100, 25_000_000)
    req_bytes = np.clip(rng.lognormal(mean=np.log(1_000), sigma=0.8, size=n).astype(np.int64), 40, 500_000)
    req_header_bytes = np.clip(rng.lognormal(mean=np.log(700), sigma=0.5, size=n).astype(np.int64), 120, 16_000)
    conn_requests = np.clip(rng.geometric(0.12, size=n), 1, 400).astype(np.int32)
    ttl = rng.choice([0, 300, 3600, 86400], size=n, p=[0.25, 0.35, 0.30, 0.10]).astype(np.int32)
    ja3 = pools["ja3"][ip_int % 250]
    ja4 = pools["ja4"][ip_int % 250]
    cookie_session = pools["sess"][ip_int % 500]

    bot_cat = rng.choice(BOT_CATEGORIES, size=n)
    is_bot = rng.random(n) < (0.80 if scenario == "bot-scrape" else 0.12)
    bot_category = np.where(is_bot, bot_cat, None)
    waf_sig = rng.choice(NGWAF_SIGNALS, size=n)
    has_waf_sig = rng.random(n) < (0.50 if scenario in ["bot-scrape", "ddos-spike"] else 0.05)
    waf_signal = np.where(has_waf_sig, waf_sig, None)

    origin_fetch = np.isin(cache, ORIGIN_FETCH_STATES)
    ost = status.astype(object)
    ost[~origin_fetch] = None
    oip = rng.choice(np.array(ORIGIN_IPS, dtype=object), size=n)
    oip[~origin_fetch] = None

    return {
        "timestamp": ts_us, "ip": ip, "status": status, "elapsed": elapsed, "cache": cache,
        "resp_bytes": resp_bytes, "host": host, "url": url, "method": method, "proto": proto,
        "ua": ua, "req_bytes": req_bytes, "pop": pop, "ttfb": ttfb, "ottfb": ottfb, "ottlb": ottlb,
        "tls_time": tls_time, "waf_ms": waf_ms, "ttl": ttl, "country": country, "asn": asn,
        "city": city, "metro": metro, "lat": lat, "lon": lon, "region": region,
        "tcp_rtt": tcp_rtt, "rtt_min": rtt_min, "rtt_var": rtt_var, "ploss": ploss,
        "ja3": ja3, "ja4": ja4, "cookie_session": cookie_session, "bot_category": bot_category,
        "waf_signal": waf_signal, "ost": ost, "oip": oip,
        "req_header_bytes": req_header_bytes, "conn_requests": conn_requests, "_source_file": np.full(n, f"synthetic://gen/{int(time.time())}", dtype=object),
    }


def cols_to_arrow_table(cols: dict[str, Any], schema: pa.Schema) -> pa.Table:
    """Build pa.Table matching schema, gracefully defaulting missing catalog columns."""
    import pyarrow as pa

    n_rows = len(next(iter(cols.values())))
    arrays = []
    for field in schema:
        if field.name in cols:
            arr = pa.array(cols[field.name], type=None)
            arrays.append(arr.cast(field.type, safe=False) if arr.type != field.type else arr)
        elif pa.types.is_string(field.type):
            arrays.append(pa.array([f"dummy_{field.name}"] * n_rows, type=field.type))
        elif pa.types.is_integer(field.type) or pa.types.is_timestamp(field.type):
            arrays.append(pa.array([0] * n_rows, type=field.type))
        elif pa.types.is_floating(field.type):
            arrays.append(pa.array([0.0] * n_rows, type=field.type))
        elif pa.types.is_boolean(field.type):
            arrays.append(pa.array([False] * n_rows, type=field.type))
        else:
            arrays.append(pa.nulls(n_rows, type=field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


# ---------------------------------------------------------------------------
# RUM (Faro) synthetic beacons and records — stdlib random, injectable rng
# ---------------------------------------------------------------------------

_BEACON_TYPES, _BEACON_WEIGHTS = ["vitals", "interaction", "exception"], [75, 15, 10]
_RUM_METRICS = ["LCP", "CLS", "INP", "FID", "TTFB", "FCP"]
_RUM_PATHS = ["/", "/dashboard", "/origin", "/security", "/performance"]
_RESERVED_FIRST_OCTETS = frozenset({10, 100, 127, 169, 172, 192, 198, 203})
_PUBLIC_FIRST_OCTETS = tuple(o for o in range(12, 224) if o not in _RESERVED_FIRST_OCTETS)


def rum_hex_id(rng: Any, chars: int = 16) -> str:
    return f"{rng.getrandbits(chars * 4):0{chars}x}"


def random_source_ip(rng: Any = random) -> str:
    first = rng.choice(_PUBLIC_FIRST_OCTETS)
    return f"{first}.{rng.randint(1, 254)}.{rng.randint(1, 254)}.{rng.randint(1, 254)}"


def _draw_file_metric(rng: Any) -> tuple[str, float]:
    metric = rng.choice(_RUM_METRICS)
    if metric == "LCP":
        return metric, rng.choice([800, 1200, 2200, 3100, 4200])
    if metric == "CLS":
        return metric, round(rng.uniform(0.01, 0.35), 3)
    if metric == "INP":
        return metric, rng.choice([50, 120, 180, 240, 320])
    if metric == "TTFB":
        return metric, rng.choice([150, 250, 650, 950])
    return metric, rng.choice([10, 25, 80, 150])


def _draw_harness_metric(rng: Any) -> tuple[str, float]:
    metric = rng.choice(_RUM_METRICS)
    return (metric, round(rng.uniform(0.01, 0.45), 3)) if metric == "CLS" else (metric, rng.choice([150, 250, 1200, 2500, 3500]))


@dataclass(frozen=True)
class RumProfile:
    browsers: tuple[str, ...]
    oses: tuple[str, ...]
    events: tuple[str, ...]
    errors: tuple[str, ...]
    other_good_max: float
    hashed_chunk: bool
    mobile_browser_hint: bool
    draw_metric: Callable[[Any], tuple[str, float]]


FILE_RUM_PROFILE = RumProfile(
    browsers=("Chrome", "Firefox", "Safari", "Edge", "Mobile Safari"),
    oses=("Windows", "macOS", "iOS", "Android", "Linux"),
    events=("click_search_button", "form_submit", "tab_switch_performance", "modal_close_settings"),
    errors=(
        "TypeError: Cannot read properties of null (reading 'style')",
        "ReferenceError: x_analytics is not defined",
        "Error: Failed to fetch npm registry assets",
        "DOMException: Playwright auto-navigation aborted",
    ),
    other_good_max=100, hashed_chunk=True, mobile_browser_hint=True, draw_metric=_draw_file_metric,
)

HARNESS_RUM_PROFILE = RumProfile(
    browsers=("Chrome", "Firefox", "Safari"),
    oses=("Windows", "macOS", "iOS"),
    events=("click_search_button", "form_submit", "tab_switch_performance"),
    errors=(
        "TypeError: Cannot read properties of undefined (reading 'split')",
        "ReferenceError: analytics_tracker is not defined",
        "DOMException: Failed to execute 'appendChild' on 'Node'",
    ),
    other_good_max=800, hashed_chunk=False, mobile_browser_hint=False, draw_metric=_draw_harness_metric,
)


def pick_beacon_type(rng: Any = random) -> str:
    return str(rng.choices(_BEACON_TYPES, weights=_BEACON_WEIGHTS)[0])


def rum_rating(metric: str, value: float, profile: RumProfile) -> str:
    if metric == "LCP":
        return "good" if value <= 2500 else ("needs_improvement" if value <= 4000 else "poor")
    if metric == "CLS":
        return "good" if value <= 0.1 else ("needs_improvement" if value <= 0.25 else "poor")
    if metric == "INP":
        return "good" if value <= 200 else ("needs_improvement" if value <= 500 else "poor")
    if metric == "TTFB":
        return "good" if value <= 800 else "poor"
    return "good" if value <= profile.other_good_max else "poor"


def rum_device(browser: str, os_name: str, profile: RumProfile) -> str:
    if profile.mobile_browser_hint:
        return "Mobile" if "Mobile" in browser or os_name in ("iOS", "Android") else "Desktop"
    return "Mobile" if os_name == "iOS" else "Desktop"


def generate_faro_payload(
    rng: Any, profile: RumProfile, *, beacon_type: str, browser: str, os_name: str,
    page_url: str, record_dt: datetime, metric: str | None = None, value: float | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "meta": {
            "browser": {"name": browser, "mobile": rum_device(browser, os_name, profile) == "Mobile"},
            "os": {"name": os_name},
            "page": {"url": page_url},
        }
    }
    if beacon_type == "vitals":
        if metric is None or value is None:
            metric, value = profile.draw_metric(rng)
        payload["measurements"] = [{"type": "web-vitals", "values": {metric: value}, "context": {"rating": rum_rating(metric, value, profile)}}]
    elif beacon_type == "interaction":
        payload["events"] = [{"name": rng.choice(profile.events), "timestamp": record_dt.isoformat(), "attributes": {"element_id": f"btn_{rng.randint(100, 999)}"}}]
    else:
        message = rng.choice(profile.errors)
        filename = f"static/chunks/main-{rum_hex_id(rng, 8)}.js" if profile.hashed_chunk else "static/chunks/main.js"
        payload["exceptions"] = [{"type": "error", "value": message, "stacktrace": {"frames": [{"filename": filename, "lineno": rng.randint(10, 500), "colno": rng.randint(1, 120)}]}}]
    return payload


def generate_rum_record(service_id: str, record_dt: datetime, rng: Any = random) -> dict[str, Any]:
    profile = FILE_RUM_PROFILE
    beacon_type = pick_beacon_type(rng)
    browser, os_name = rng.choice(profile.browsers), rng.choice(profile.oses)
    device = rum_device(browser, os_name, profile)
    city = rng.choice(["Denver", "New York", "San Francisco", "London", "Tokyo", "Paris"])
    region = rng.choice(["CO", "NY", "CA", "ENG", "TKY", "IDF"])
    country = rng.choice(["US", "US", "US", "GB", "JP", "FR"])
    pop = rng.choice(["DEN", "EWR", "SJC", "LCY", "TYO", "CDG"])
    path = rng.choice(["/", "/dashboard", "/origin", "/security", "/performance", "/rum"])
    cid, req_id = f"cid-{rng.randint(10000, 99999)}", f"req-{rum_hex_id(rng, 16)}"
    faro_payload = generate_faro_payload(
        rng, profile, beacon_type=beacon_type, browser=browser, os_name=os_name,
        page_url=f"http://localhost{path}", record_dt=record_dt,
    )
    return {
        "service_id": service_id, "timestamp": record_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "geo_city": city, "geo_region": region, "geo_country_code": country, "server_pop": pop,
        "browser": browser, "os": os_name, "device": device, "rum_cid": cid,
        "req_id": req_id, "fastly_req_id": req_id, "request_event_id": req_id,
        "url": f"http://localhost{path}", "rum_body": json.dumps(faro_payload),
    }


def generate_rum_records(service_id: str, n: int, now: datetime, rng: Any = random, step_ms: int = 50) -> list[dict[str, Any]]:
    return [generate_rum_record(service_id, now - timedelta(milliseconds=i * step_ms), rng) for i in range(n)]


def build_rum_beacon_url(base_url: str, rng: Any = random) -> str:
    metric, value = HARNESS_RUM_PROFILE.draw_metric(rng)
    beacon_id = rum_hex_id(rng, 32)
    cid, req_id, path = f"cid-{beacon_id}", f"req-{beacon_id}", rng.choice(_RUM_PATHS)
    return (
        f"{base_url.rstrip('/')}/rum-beacon?rum_metric_name={metric}"
        f"&rum_metric_value={value}&rum_cid={quote(cid)}&cid={quote(cid)}"
        f"&req={quote(req_id)}&rum_pathname={quote(path)}"
    )


def build_rum_beacon_body(url: str, rng: Any = random, now: datetime | None = None) -> dict[str, Any]:
    profile = HARNESS_RUM_PROFILE
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    metric = qs.get("rum_metric_name", ["LCP"])[0]
    try:
        value = float(qs.get("rum_metric_value", ["1200"])[0])
    except ValueError:
        value = 1200.0
    path = qs.get("rum_pathname", ["/"])[0]
    browser, os_name = rng.choice(profile.browsers), rng.choice(profile.oses)
    return generate_faro_payload(
        rng, profile, beacon_type=pick_beacon_type(rng), browser=browser, os_name=os_name,
        page_url=f"{parsed.scheme}://{parsed.netloc}{path}", record_dt=now or datetime.now(UTC),
        metric=metric, value=value,
    )


# ---------------------------------------------------------------------------
# CMCD (Common Media Client Data) edge-request generator
# ---------------------------------------------------------------------------

_CMCD_BITRATES_KBPS = [800, 1500, 3000, 6000]
_CMCD_TOP_BITRATE_KBPS = _CMCD_BITRATES_KBPS[-1]
_CMCD_TITLES = ["news-live", "sports-live", "movie-001", "movie-002", "series-s1e1", "series-s1e2", "kids-001"]
_CMCD_OBJECT_TYPES = ["v", "v", "v", "v", "a", "m", "i"]
_CMCD_REBUFFER_RATE, _CMCD_V2_RATE, _CMCD_SESSION_POOL_SIZE, _BOT_UA_RATE = 0.04, 0.1, 300, 0.15


@dataclass
class CmcdSession:
    sid: str
    cid: str
    stream_format: str
    version: int
    started: bool = False


def build_cmcd_sessions(rng: Any, size: int = _CMCD_SESSION_POOL_SIZE) -> list[CmcdSession]:
    sessions = []
    for _ in range(size):
        sid = f"{rum_hex_id(rng, 8)}-{rum_hex_id(rng, 4)}-{rum_hex_id(rng, 4)}-{rum_hex_id(rng, 12)}"
        sessions.append(
            CmcdSession(
                sid=sid, cid=rng.choice(_CMCD_TITLES),
                stream_format=rng.choice(["h", "h", "d"]),
                version=2 if rng.random() < _CMCD_V2_RATE else 1,
            )
        )
    return sessions


def _cmcd_bool(session: CmcdSession, key: str) -> str:
    return f"{key}=?1" if session.version == 2 else key


def build_cmcd_request(session: CmcdSession, rng: Any = random) -> tuple[str, dict[str, str], str]:
    object_type, bitrate = rng.choice(_CMCD_OBJECT_TYPES), rng.choice(_CMCD_BITRATES_KBPS)
    object_fields = [f"br={bitrate}", f"d={rng.choice([2000, 4000, 6000])}", f"ot={object_type}", f"tb={_CMCD_TOP_BITRATE_KBPS}"]
    buffer_length = rng.randrange(0, 30000, 100)
    request_fields = [f"bl={buffer_length}", f"dl={rng.randrange(0, 30000, 100)}", f"mtp={rng.randrange(2000, 50000, 100)}"]
    if not session.started:
        request_fields.append(_cmcd_bool(session, "su"))
        session.started = True
    session_fields = [f'cid="{session.cid}"', f"sf={session.stream_format}", f'sid="{session.sid}"', "st=v"]
    status_fields = [f"rtp={rng.randrange(1000, 40000, 100)}"]
    if rng.random() < _CMCD_REBUFFER_RATE:
        status_fields.insert(0, _cmcd_bool(session, "bs"))
    headers = {
        "CMCD-Object": ",".join(object_fields), "CMCD-Request": ",".join(request_fields),
        "CMCD-Session": ",".join(session_fields), "CMCD-Status": ",".join(status_fields),
    }
    query_value = ",".join(sorted(object_fields + request_fields + session_fields + status_fields))
    if object_type == "m":
        path = f"/video/{session.cid}/manifest.{'m3u8' if session.stream_format == 'h' else 'mpd'}"
    elif object_type == "a":
        path = f"/video/{session.cid}/audio/seg-{rng.randint(1, 900)}.m4a"
    elif object_type == "i":
        path = f"/video/{session.cid}/thumb-{rng.randint(1, 60)}.jpg"
    else:
        path = f"/video/{session.cid}/{bitrate}/seg-{rng.randint(1, 900)}.m4s"
    return path, headers, query_value


def pick_user_agent(rng: Any = random) -> str:
    return rng.choice(BOT_USER_AGENTS if rng.random() < _BOT_UA_RATE else USER_AGENTS)
# fmt: on

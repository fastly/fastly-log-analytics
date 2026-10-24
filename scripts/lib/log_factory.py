"""Canonical synthetic request-log factory.

Single owner of the synthetic request-log record schema, field dimension pools,
and distributions used across the repo:

* ``scripts/load_test/generate_synthetic_traffic.py`` imports the dimension
  pools plus :func:`generate_columns` / :func:`cols_to_arrow_table` to build
  bulk Arrow/Parquet/NDJSON batches.
* ``scripts/dev/scale_harness.py`` builds RUM (Faro) records/beacons through the
  ``generate_rum_*`` / ``build_rum_*`` helpers at the bottom of this module.

Both the columnar generator (NumPy ``Generator``) and the per-record generator
(stdlib ``random``) take an injectable RNG for determinism. NumPy / PyArrow are
imported lazily inside the columnar helpers so importing this module from the
backend test suite stays stdlib-only (no heavy array deps pulled into test
collection).
"""

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

# ---------------------------------------------------------------------------
# Dimension Pools & Weights (bulk columnar generator)
# ---------------------------------------------------------------------------

COUNTRIES = [
    "US",
    "GB",
    "DE",
    "JP",
    "BR",
    "IN",
    "AU",
    "FR",
    "CA",
    "NL",
    "SG",
    "ES",
    "IT",
    "SE",
    "KR",
    "MX",
    "ZA",
    "CH",
    "PL",
    "IE",
]
_CW = [
    0.35,
    0.08,
    0.07,
    0.06,
    0.05,
    0.05,
    0.04,
    0.04,
    0.04,
    0.03,
    0.03,
    0.02,
    0.02,
    0.02,
    0.02,
    0.02,
    0.02,
    0.01,
    0.01,
    0.01,
]
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

POPS = [
    "IAD",
    "LHR",
    "NRT",
    "SYD",
    "SFO",
    "FRA",
    "CDG",
    "SIN",
    "ORD",
    "DFW",
    "AMS",
    "HKG",
    "JFK",
    "LAX",
    "MIA",
    "SEA",
    "DEN",
    "ATL",
    "BOS",
    "YYZ",
]

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

BOT_CATEGORIES = [
    "Search Engine",
    "SEO / Analytics",
    "Social Media",
    "AI Crawler",
    "Automated Scraper",
]

NGWAF_SIGNALS = [
    "DATACENTER",
    "SUSPICIOUS-UA",
    "SCRAPER",
    "SQLI-PROBE",
    "XSS-PROBE",
    "RATE-LIMIT",
]

URL_PATHS = [
    "/",
    "/index.html",
    "/products",
    "/products/search",
    "/api/v1/catalog",
    "/api/v2/checkout",
    "/api/v1/auth/login",
    "/api/v1/user/profile",
    "/assets/app.js",
    "/assets/styles.css",
    "/images/hero.webp",
    "/images/logo.png",
    "/docs/getting-started",
    "/blog/announcements",
]


# ---------------------------------------------------------------------------
# Bulk Columnar Generator (NumPy)
# ---------------------------------------------------------------------------


def generate_columns(
    n: int,
    start_ms: int,
    end_ms: int,
    scenario: str,
    rng: np.random.Generator,
) -> dict[str, Any]:
    """Generate realistic columnar data for n log events."""
    import numpy as np

    span_ms = max(end_ms - start_ms, 1)

    # 1. Timestamps with diurnal curve
    # Generate linear uniform distribution, modulated by sinusoidal curve
    u = rng.uniform(0, 1, size=n)
    if scenario == "diurnal":
        # Diurnal peak at 15:00 UTC (phase shift)
        # Rejection/weight sampling via sinusoidal probability
        # 1.0 + 0.5 * sin(2*pi * (t - 6h) / 24h)
        phase = 2 * math.pi * ((start_ms / 1000 / 3600) % 24 - 6) / 24
        ts_rel = (u + 0.2 * np.sin(2 * math.pi * u + phase)) % 1.0
        ts_ms = start_ms + (ts_rel * span_ms).astype(np.int64)
    elif scenario == "ddos-spike":
        # 40% of traffic concentrated in a 15-minute spike window in the middle
        spike_center = start_ms + span_ms // 2
        spike_radius = min(span_ms // 8, 15 * 60 * 1000)
        is_spike = rng.random(n) < 0.40
        spike_ts = rng.integers(spike_center - spike_radius, spike_center + spike_radius, size=n, dtype=np.int64)
        normal_ts = (start_ms + u * span_ms).astype(np.int64)
        ts_ms = np.where(is_spike, spike_ts, normal_ts)
    elif scenario == "origin-5xx-outage":
        # Outage window in the middle 30 minutes
        outage_start = start_ms + span_ms // 2
        outage_end = outage_start + min(span_ms // 4, 30 * 60 * 1000)
        ts_ms = (start_ms + u * span_ms).astype(np.int64)
    else:
        ts_ms = (start_ms + u * span_ms).astype(np.int64)

    ts_us = ts_ms * 1000

    # 2. Host, Method, Protocol, Country, POP
    host = rng.choice(HOSTS, size=n, p=HOST_WEIGHTS)
    method = rng.choice(METHODS, size=n, p=METHOD_WEIGHTS)
    proto = rng.choice(PROTOCOLS, size=n, p=PROTO_WEIGHTS)
    country = rng.choice(COUNTRIES, size=n, p=COUNTRY_WEIGHTS)
    pop = rng.choice(POPS, size=n)

    # Generate realistic City, Metro, Region, and Coordinates based on chosen Country code
    city_list = []
    metro_list = []
    lat_list = []
    lon_list = []
    region_list = []
    for c in country:
        geo = COUNTRY_GEO_MAP.get(c, {"city": "Unknown", "metro": "000", "lat": 0.0, "lon": 0.0, "region": "XX"})
        jitter_lat = rng.uniform(-0.15, 0.15)
        jitter_lon = rng.uniform(-0.15, 0.15)
        city_list.append(geo["city"])
        metro_list.append(geo["metro"])
        lat_list.append(geo["lat"] + jitter_lat)
        lon_list.append(geo["lon"] + jitter_lon)
        region_list.append(geo["region"])

    city = np.array(city_list, dtype=object)
    metro = np.array(metro_list, dtype=object)
    lat = np.array(lat_list, dtype=np.float64)
    lon = np.array(lon_list, dtype=np.float64)
    region = np.array(region_list, dtype=object)

    # 3. URLs, IPs, User Agents, ASNs
    url_idx = rng.integers(0, len(URL_PATHS), size=n)
    url = np.array([URL_PATHS[i] for i in url_idx], dtype=object)

    ip_int = rng.integers(1, 254 * 254 * 254, size=n)
    ip = np.array(
        [f"198.51.{(i // 254) & 0xFF}.{i % 254 + 1}" for i in ip_int],
        dtype=object,
    )

    ua_idx = rng.integers(0, len(USER_AGENTS), size=n)
    ua = np.array([USER_AGENTS[i] for i in ua_idx], dtype=object)

    asn_base = rng.integers(1000, 65000, size=n, dtype=np.int32)
    asn = asn_base

    # 4. Status Codes & Cache States based on scenario
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

        # Concentrated attacker IPs and URLs during DDoS
        attacker_ips = ["203.0.113.55", "203.0.113.88", "198.51.100.99"]
        attacker_urls = ["/api/v1/auth/login", "/api/v2/checkout"]
        attack_ip_choice = rng.choice(attacker_ips, size=n)
        attack_url_choice = rng.choice(attacker_urls, size=n)
        ip = np.where(is_spike, attack_ip_choice, ip)
        url = np.where(is_spike, attack_url_choice, url)
        asn = np.where(is_spike, 13335, asn)
    elif scenario == "bot-scrape":
        status = rng.choice([200, 301, 304, 403, 404, 429], size=n, p=[0.65, 0.05, 0.15, 0.05, 0.08, 0.02]).astype(
            np.int32
        )
        cache = rng.choice(CACHE_NORMAL, size=n, p=CACHE_NORMAL_WEIGHTS)
        bot_uas = rng.choice(BOT_USER_AGENTS, size=n)
        ua = bot_uas
    else:  # diurnal or slow-network
        status = rng.choice(NORMAL_STATUSES, size=n, p=NORMAL_STATUS_WEIGHTS).astype(np.int32)
        cache = rng.choice(CACHE_NORMAL, size=n, p=CACHE_NORMAL_WEIGHTS)

    # 5. Latency & Timing Breakdown
    if scenario == "slow-network":
        elapsed = rng.lognormal(mean=np.log(800), sigma=0.8, size=n).astype(np.int32)
        elapsed = np.clip(elapsed, 100, 30_000)
        tls_time = np.clip(rng.lognormal(mean=np.log(250), sigma=0.6, size=n), 50, 2000).astype(np.int32)
        ottfb = (elapsed * rng.uniform(0.3, 0.7, size=n)).astype(np.int32)
        ottlb = (ottfb + rng.lognormal(mean=np.log(50), sigma=0.8, size=n)).astype(np.int32)
        ttfb = (elapsed * rng.uniform(0.7, 0.95, size=n)).astype(np.int32)
        waf_ms = np.clip(rng.lognormal(mean=np.log(8), sigma=0.4, size=n), 1, 100).astype(np.int32)

        # Slow network network quality metrics (elevated RTT in us and ploss)
        tcp_rtt = rng.lognormal(mean=np.log(180000), sigma=0.6, size=n).astype(np.int32)
        tcp_rtt = np.clip(tcp_rtt, 10000, 2000000)
        rtt_min = np.clip((tcp_rtt * rng.uniform(0.5, 0.8, size=n)).astype(np.int32), 5000, None)
        rtt_var = np.clip((tcp_rtt * rng.uniform(0.05, 0.25, size=n)).astype(np.int32), 1000, None)
        ploss = rng.choice([0.0, 0.005, 0.01, 0.02, 0.05, 0.10], size=n, p=[0.60, 0.12, 0.10, 0.08, 0.06, 0.04])
    elif scenario == "origin-5xx-outage":
        elapsed = rng.lognormal(mean=np.log(25), sigma=1.0, size=n).astype(np.int32)
        ottfb = (elapsed * rng.uniform(0.1, 0.7, size=n)).astype(np.int32)
        ottlb = (ottfb + 10).astype(np.int32)
        # Spike origin wait during outage
        outage_elapsed = rng.integers(2000, 8000, size=n, dtype=np.int32)
        elapsed = np.where(in_outage, outage_elapsed, elapsed)
        ottfb = np.where(in_outage, (outage_elapsed * 0.9).astype(np.int32), ottfb)
        ottlb = np.where(in_outage, outage_elapsed, ottlb)
        ttfb = (elapsed * 0.85).astype(np.int32)
        tls_time = np.clip(rng.lognormal(mean=np.log(35), sigma=0.4, size=n), 10, 500).astype(np.int32)
        waf_ms = np.clip(rng.lognormal(mean=np.log(4), sigma=0.3, size=n), 1, 50).astype(np.int32)

        # Normal network health with minor loss during outage
        tcp_rtt = rng.lognormal(mean=np.log(25000), sigma=0.5, size=n).astype(np.int32)
        tcp_rtt = np.clip(tcp_rtt, 1000, 500000)
        rtt_min = np.clip((tcp_rtt * rng.uniform(0.7, 0.9, size=n)).astype(np.int32), 500, None)
        rtt_var = np.clip((tcp_rtt * rng.uniform(0.01, 0.12, size=n)).astype(np.int32), 100, None)
        ploss = rng.choice([0.0, 0.001, 0.005, 0.01, 0.02], size=n, p=[0.92, 0.04, 0.02, 0.01, 0.01])
    else:
        elapsed = rng.lognormal(mean=np.log(25), sigma=1.0, size=n).astype(np.int32)
        elapsed = np.clip(elapsed, 1, 10_000)
        ttfb = (elapsed * rng.uniform(0.2, 0.8, size=n)).astype(np.int32)
        ottfb = np.where(cache == "HIT", 0, (elapsed * rng.uniform(0.3, 0.8, size=n)).astype(np.int32))
        ottlb = np.where(
            cache == "HIT", 0, (ottfb + rng.lognormal(mean=np.log(10), sigma=0.5, size=n)).astype(np.int32)
        )
        tls_time = np.clip(rng.lognormal(mean=np.log(35), sigma=0.4, size=n), 5, 500).astype(np.int32)
        waf_ms = np.clip(rng.lognormal(mean=np.log(3), sigma=0.4, size=n), 1, 50).astype(np.int32)

        # Healthy networks (standard diurnal, bots, spikes)
        tcp_rtt = rng.lognormal(mean=np.log(25000), sigma=0.5, size=n).astype(np.int32)
        tcp_rtt = np.clip(tcp_rtt, 1000, 500000)
        rtt_min = np.clip((tcp_rtt * rng.uniform(0.7, 0.9, size=n)).astype(np.int32), 500, None)
        rtt_var = np.clip((tcp_rtt * rng.uniform(0.01, 0.12, size=n)).astype(np.int32), 100, None)
        ploss = rng.choice([0.0, 0.001, 0.005, 0.01, 0.02], size=n, p=[0.95, 0.02, 0.01, 0.01, 0.01])

    # 6. Bytes, TTL, Security & Bot Signals
    resp_bytes = np.clip(
        rng.lognormal(mean=np.log(8_000), sigma=1.5, size=n).astype(np.int64),
        100,
        25_000_000,
    )
    req_bytes = np.clip(
        rng.lognormal(mean=np.log(1_000), sigma=0.8, size=n).astype(np.int64),
        40,
        500_000,
    )

    ttl_choices = [0, 300, 3600, 86400]
    ttl = rng.choice(ttl_choices, size=n, p=[0.25, 0.35, 0.30, 0.10]).astype(np.int32)

    ja3 = np.array([f"ja3-{i % 250:04x}" for i in ip_int], dtype=object)
    ja4 = np.array([f"ja4-{i % 250:04x}" for i in ip_int], dtype=object)
    cookie_session = np.array([f"sess_{i % 500:08x}" for i in ip_int], dtype=object)

    # Bot & WAF signals
    bot_cat = rng.choice(BOT_CATEGORIES, size=n)
    is_bot = rng.random(n) < (0.80 if scenario == "bot-scrape" else 0.12)
    bot_category = np.where(is_bot, bot_cat, None)

    waf_sig = rng.choice(NGWAF_SIGNALS, size=n)
    has_waf_sig = rng.random(n) < (0.50 if scenario in ["bot-scrape", "ddos-spike"] else 0.05)
    waf_signal = np.where(has_waf_sig, waf_sig, None)

    return {
        "timestamp": ts_us,
        "ip": ip,
        "status": status,
        "elapsed": elapsed,
        "cache": cache,
        "resp_bytes": resp_bytes,
        "host": host,
        "url": url,
        "method": method,
        "proto": proto,
        "ua": ua,
        "req_bytes": req_bytes,
        "pop": pop,
        "ttfb": ttfb,
        "ottfb": ottfb,
        "ottlb": ottlb,
        "tls_time": tls_time,
        "waf_ms": waf_ms,
        "ttl": ttl,
        "country": country,
        "asn": asn,
        "city": city,
        "metro": metro,
        "lat": lat,
        "lon": lon,
        "region": region,
        "tcp_rtt": tcp_rtt,
        "rtt_min": rtt_min,
        "rtt_var": rtt_var,
        "ploss": ploss,
        "ja3": ja3,
        "ja4": ja4,
        "cookie_session": cookie_session,
        "bot_category": bot_category,
        "waf_signal": waf_signal,
        "_source_file": np.array([f"synthetic://gen/{int(time.time())}"] * n, dtype=object),
    }


def cols_to_arrow_table(cols: dict[str, Any], schema: pa.Schema) -> pa.Table:
    """Build pa.Table matching schema, gracefully defaulting missing catalog columns."""
    import pyarrow as pa

    n_rows = len(next(iter(cols.values())))
    arrays = []
    for field in schema:
        name = field.name
        if name in cols:
            arr = pa.array(cols[name], type=None)
            if arr.type != field.type:
                arr = arr.cast(field.type, safe=False)
            arrays.append(arr)
        else:
            if pa.types.is_string(field.type):
                arr = pa.array([f"dummy_{name}"] * n_rows, type=field.type)
            elif pa.types.is_integer(field.type):
                arr = pa.array([0] * n_rows, type=field.type)
            elif pa.types.is_floating(field.type):
                arr = pa.array([0.0] * n_rows, type=field.type)
            elif pa.types.is_boolean(field.type):
                arr = pa.array([False] * n_rows, type=field.type)
            elif pa.types.is_timestamp(field.type):
                arr = pa.array([0] * n_rows, type=field.type)
            else:
                arr = pa.nulls(n_rows, type=field.type)
            arrays.append(arr)
    return pa.Table.from_arrays(arrays, schema=schema)


# ---------------------------------------------------------------------------
# RUM (Faro) synthetic beacons and records — stdlib random, injectable rng
# ---------------------------------------------------------------------------
#
# Two historical generators fed this section: the FOS seeding script (full
# log-line records with a wide dimension pool) and the scale harness (beacon
# POST bodies derived from the request URL, narrower pools). Both now draw from
# one rng so a seeded ``random.Random`` reproduces a run; the pool/threshold
# differences between them live in the two :class:`RumProfile` constants.

_BEACON_TYPES = ["vitals", "interaction", "exception"]
_BEACON_WEIGHTS = [75, 15, 10]
_RUM_METRICS = ["LCP", "CLS", "INP", "FID", "TTFB", "FCP"]
_RUM_PATHS = ["/", "/dashboard", "/origin", "/security", "/performance"]


def rum_hex_id(rng: Any, chars: int = 16) -> str:
    """Seedable stand-in for ``uuid.uuid4().hex[:chars]``."""
    return f"{rng.getrandbits(chars * 4):0{chars}x}"


_RESERVED_FIRST_OCTETS = frozenset({10, 100, 127, 169, 172, 192, 198, 203})
_PUBLIC_FIRST_OCTETS = tuple(o for o in range(12, 224) if o not in _RESERVED_FIRST_OCTETS)


def random_source_ip(rng: Any = random) -> str:
    """Public-looking IPv4 used to spoof geography/ASN via ``X-Source-Ip``."""
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
    if metric == "CLS":
        return metric, round(rng.uniform(0.01, 0.45), 3)
    return metric, rng.choice([150, 250, 1200, 2500, 3500])


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
    other_good_max=100,
    hashed_chunk=True,
    mobile_browser_hint=True,
    draw_metric=_draw_file_metric,
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
    other_good_max=800,
    hashed_chunk=False,
    mobile_browser_hint=False,
    draw_metric=_draw_harness_metric,
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
    rng: Any,
    profile: RumProfile,
    *,
    beacon_type: str,
    browser: str,
    os_name: str,
    page_url: str,
    record_dt: datetime,
    metric: str | None = None,
    value: float | None = None,
) -> dict[str, Any]:
    """Faro payload (``meta`` plus one of measurements/events/exceptions).

    ``metric``/``value`` are drawn from the profile's pools when omitted
    (vitals beacons only).
    """
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
        payload["measurements"] = [
            {
                "type": "web-vitals",
                "values": {metric: value},
                "context": {"rating": rum_rating(metric, value, profile)},
            }
        ]
    elif beacon_type == "interaction":
        payload["events"] = [
            {
                "name": rng.choice(profile.events),
                "timestamp": record_dt.isoformat(),
                "attributes": {"element_id": f"btn_{rng.randint(100, 999)}"},
            }
        ]
    else:
        message = rng.choice(profile.errors)
        filename = f"static/chunks/main-{rum_hex_id(rng, 8)}.js" if profile.hashed_chunk else "static/chunks/main.js"
        payload["exceptions"] = [
            {
                "type": "error",
                "value": message,
                "stacktrace": {
                    "frames": [{"filename": filename, "lineno": rng.randint(10, 500), "colno": rng.randint(1, 120)}]
                },
            }
        ]
    return payload


def generate_rum_record(service_id: str, record_dt: datetime, rng: Any = random) -> dict[str, Any]:
    """One raw RUM log line as the edge writes it (FOS ``raw/rum/`` layout)."""
    profile = FILE_RUM_PROFILE
    beacon_type = pick_beacon_type(rng)
    browser = rng.choice(profile.browsers)
    os_name = rng.choice(profile.oses)
    device = rum_device(browser, os_name, profile)

    city = rng.choice(["Denver", "New York", "San Francisco", "London", "Tokyo", "Paris"])
    region = rng.choice(["CO", "NY", "CA", "ENG", "TKY", "IDF"])
    country = rng.choice(["US", "US", "US", "GB", "JP", "FR"])
    pop = rng.choice(["DEN", "EWR", "SJC", "LCY", "TYO", "CDG"])

    path = rng.choice(["/", "/dashboard", "/origin", "/security", "/performance", "/rum"])
    cid = f"cid-{rng.randint(10000, 99999)}"
    req_id = f"req-{rum_hex_id(rng, 16)}"

    faro_payload = generate_faro_payload(
        rng,
        profile,
        beacon_type=beacon_type,
        browser=browser,
        os_name=os_name,
        page_url=f"http://localhost{path}",
        record_dt=record_dt,
    )

    return {
        "service_id": service_id,
        "timestamp": record_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "geo_city": city,
        "geo_region": region,
        "geo_country_code": country,
        "server_pop": pop,
        "browser": browser,
        "os": os_name,
        "device": device,
        "rum_cid": cid,
        "req_id": req_id,
        # Standard-mode ingest (backend/core/rum_ingest.py) reads
        # `fastly_req_id` specifically — the same field the real edge VCL
        # stamps. Without it the request-id join to the CDN log resolves to "".
        "fastly_req_id": req_id,
        "request_event_id": req_id,  # Aligns ClickHouse high-scale schema
        "url": f"http://localhost{path}",
        "rum_body": json.dumps(faro_payload),
    }


def generate_rum_records(
    service_id: str, n: int, now: datetime, rng: Any = random, step_ms: int = 50
) -> list[dict[str, Any]]:
    """``n`` records, newest first, spaced ``step_ms`` apart so timestamps are unique."""
    return [generate_rum_record(service_id, now - timedelta(milliseconds=i * step_ms), rng) for i in range(n)]


def build_rum_beacon_url(base_url: str, rng: Any = random) -> str:
    """``/rum-beacon`` URL carrying the identity/metric query the edge VCL reads."""
    metric, value = HARNESS_RUM_PROFILE.draw_metric(rng)
    beacon_id = rum_hex_id(rng, 32)
    cid = f"cid-{beacon_id}"
    req_id = f"req-{beacon_id}"
    path = rng.choice(_RUM_PATHS)
    return (
        f"{base_url.rstrip('/')}/rum-beacon?rum_metric_name={metric}"
        f"&rum_metric_value={value}&rum_cid={quote(cid)}&cid={quote(cid)}"
        f"&req={quote(req_id)}&rum_pathname={quote(path)}"
    )


def build_rum_beacon_body(url: str, rng: Any = random, now: datetime | None = None) -> dict[str, Any]:
    """Faro POST body for a ``/rum-beacon`` URL built by :func:`build_rum_beacon_url`."""
    profile = HARNESS_RUM_PROFILE
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    metric = qs.get("rum_metric_name", ["LCP"])[0]
    try:
        value = float(qs.get("rum_metric_value", ["1200"])[0])
    except ValueError:
        value = 1200.0
    path = qs.get("rum_pathname", ["/"])[0]
    browser = rng.choice(profile.browsers)
    os_name = rng.choice(profile.oses)
    return generate_faro_payload(
        rng,
        profile,
        beacon_type=pick_beacon_type(rng),
        browser=browser,
        os_name=os_name,
        page_url=f"{parsed.scheme}://{parsed.netloc}{path}",
        record_dt=now or datetime.now(UTC),
        metric=metric,
        value=value,
    )


# ---------------------------------------------------------------------------
# CMCD (Common Media Client Data) edge-request generator
# ---------------------------------------------------------------------------

_CMCD_BITRATES_KBPS = [800, 1500, 3000, 6000]
_CMCD_TOP_BITRATE_KBPS = _CMCD_BITRATES_KBPS[-1]
_CMCD_TITLES = ["news-live", "sports-live", "movie-001", "movie-002", "series-s1e1", "series-s1e2", "kids-001"]
_CMCD_OBJECT_TYPES = ["v", "v", "v", "v", "a", "m", "i"]
_CMCD_REBUFFER_RATE = 0.04
_CMCD_V2_RATE = 0.1
_CMCD_SESSION_POOL_SIZE = 300
_BOT_UA_RATE = 0.15


@dataclass
class CmcdSession:
    """One synthetic player session; ``started`` flips after the first (startup) request."""

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
                sid=sid,
                cid=rng.choice(_CMCD_TITLES),
                stream_format=rng.choice(["h", "h", "d"]),
                version=2 if rng.random() < _CMCD_V2_RATE else 1,
            )
        )
    return sessions


def _cmcd_bool(session: CmcdSession, key: str) -> str:
    # CMCD v1 encodes true as a bare key; v2 uses RFC 8941 ``key=?1``.
    return f"{key}=?1" if session.version == 2 else key


def build_cmcd_request(session: CmcdSession, rng: Any = random) -> tuple[str, dict[str, str], str]:
    """Return ``(path, headers, query_value)`` carrying the same CMCD data both ways.

    The edge VCL reads either the four ``CMCD-*`` headers or ``?CMCD=...``
    depending on the transport the service was provisioned with; sending both
    makes the traffic valid for either mode.
    """
    object_type = rng.choice(_CMCD_OBJECT_TYPES)
    bitrate = rng.choice(_CMCD_BITRATES_KBPS)
    object_fields = [f"br={bitrate}", f"d={rng.choice([2000, 4000, 6000])}", f"ot={object_type}"]
    object_fields.append(f"tb={_CMCD_TOP_BITRATE_KBPS}")
    buffer_length = rng.randrange(0, 30000, 100)
    request_fields = [
        f"bl={buffer_length}",
        f"dl={rng.randrange(0, 30000, 100)}",
        f"mtp={rng.randrange(2000, 50000, 100)}",
    ]
    if not session.started:
        request_fields.append(_cmcd_bool(session, "su"))
        session.started = True
    session_fields = [
        f'cid="{session.cid}"',
        f"sf={session.stream_format}",
        f'sid="{session.sid}"',
        "st=v",
    ]
    status_fields = [f"rtp={rng.randrange(1000, 40000, 100)}"]
    if rng.random() < _CMCD_REBUFFER_RATE:
        status_fields.insert(0, _cmcd_bool(session, "bs"))
    headers = {
        "CMCD-Object": ",".join(object_fields),
        "CMCD-Request": ",".join(request_fields),
        "CMCD-Session": ",".join(session_fields),
        "CMCD-Status": ",".join(status_fields),
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
    """Mostly browsers with a bot minority so bot/security panels have data."""
    pool = BOT_USER_AGENTS if rng.random() < _BOT_UA_RATE else USER_AGENTS
    return rng.choice(pool)

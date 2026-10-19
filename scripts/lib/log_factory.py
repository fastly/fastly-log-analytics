"""Canonical synthetic request-log factory.

Single owner of the synthetic request-log record schema, field dimension pools,
and distributions used across the repo:

* ``scripts/load_test/generate_synthetic_traffic.py`` imports the dimension
  pools plus :func:`generate_columns` / :func:`cols_to_arrow_table` to build
  bulk Arrow/Parquet/NDJSON batches.
* ``tests/utils/mock_data.py`` imports :func:`generate_mock_records` for small
  deterministic per-record fixtures.

Both the columnar generator (NumPy ``Generator``) and the per-record generator
(stdlib ``random``) take an injectable RNG for determinism. NumPy / PyArrow are
imported lazily inside the columnar helpers so importing this module from the
backend test suite stays stdlib-only (no heavy array deps pulled into test
collection).
"""

from __future__ import annotations

import math
import random
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

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
# Per-Record Generator (stdlib random) — small deterministic test fixtures
# ---------------------------------------------------------------------------


def generate_mock_records(
    config: dict,
    num_logs: int = 100,
    hours_ago: int = 1,
    rng: random.Random | Any = random,
) -> list[dict]:
    """Generate synthetic per-record log entries based on the log_fields schema.

    ``rng`` defaults to the stdlib ``random`` module so output is byte-identical
    to the historical ``mock_data.generate_mock_logs`` behaviour; pass a seeded
    ``random.Random`` for deterministic fixtures.
    """
    from backend.core.log_fields import resolve_enabled_fields

    enabled_fields = resolve_enabled_fields(config.get("log_fields"))
    logs = []

    now = datetime.now(UTC)
    start_time = now - timedelta(hours=hours_ago)

    # Common IPs and User Agents for realistic aggregations
    ips = ["192.168.1.1", "10.0.0.1", "203.0.113.5", "198.51.100.10"]
    uas = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15",
        "curl/7.68.0",
        "python-requests/2.25.1",
    ]
    urls = ["/", "/api/data", "/images/logo.png", "/about", "/login"]

    for _ in range(num_logs):
        log_entry = {}

        # Advance time slightly for each log to spread them out
        log_time = start_time + timedelta(seconds=rng.randint(0, hours_ago * 3600))

        # Always-on fields
        if "timestamp" in enabled_fields:
            log_entry["timestamp"] = log_time.strftime("%Y-%m-%dT%H:%M:%S%z")
            # DuckDB expects a valid format, python's %z leaves off the colon, let's fix it
            if log_entry["timestamp"].endswith("0000"):
                log_entry["timestamp"] = log_entry["timestamp"][:-4] + "00:00"

        if "ip" in enabled_fields:
            log_entry["ip"] = rng.choice(ips)

        if "status" in enabled_fields:
            # 80% 200s, 10% 404s, 10% 500s
            log_entry["status"] = rng.choices([200, 404, 500], weights=[0.8, 0.1, 0.1])[0]

        if "elapsed" in enabled_fields:
            log_entry["elapsed"] = rng.randint(1000, 500000)  # 1ms to 500ms

        if "cache" in enabled_fields:
            log_entry["cache"] = rng.choices(["HIT", "MISS", "PASS"], weights=[0.6, 0.3, 0.1])[0]

        if "resp_bytes" in enabled_fields:
            log_entry["resp_bytes"] = rng.randint(500, 50000)

        # Group A
        if "url" in enabled_fields:
            log_entry["url"] = rng.choice(urls)
        if "ua" in enabled_fields:
            log_entry["ua"] = rng.choice(uas)
        if "method" in enabled_fields:
            log_entry["method"] = "GET"

        # Group C (Infrastructure)
        if "pop" in enabled_fields:
            log_entry["pop"] = rng.choice(["JFK", "LHR", "SYD"])
        if "ttfb" in enabled_fields:
            # TTFB usually smaller than elapsed. Convert back to seconds for the log format
            ttfb_ms = log_entry.get("elapsed", 20000) * 0.8
            log_entry["ttfb"] = f"{ttfb_ms / 1000000:.6f}"

        # Group D (Geo)
        if "country" in enabled_fields:
            log_entry["country"] = rng.choice(["US", "GB", "AU", "DE"])

        # Group F (Network)
        if "asn" in enabled_fields:
            log_entry["asn"] = rng.choice([7922, 3320, 15169])

        logs.append(log_entry)

    return logs

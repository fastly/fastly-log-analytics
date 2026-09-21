#!/usr/bin/env python3
"""Unified Multi-Tier Synthetic Traffic Generator.

Supports both:
  1. --target local: Writes synthetic Parquet batches directly into
     ``cache/{bucket}/buffer/`` and optionally calls ``commit_buffer``
     to materialize DuckLake snapshots. Instant, zero S3 API cost,
     and 100% reproducible for local testing and CI.
  2. --target fos: Generates gzipped NDJSON chunks conforming to
     ``backend/provision/log_paths.py`` and uploads them to FOS object
     storage to test the full discovery, ledger, and commit crons.
  3. --target clickhouse / --with-clickhouse: Directly seeds ClickHouse
     fact tables (``request_facts``) and registers publication batches
     for high-scale serving verification.

Scenario Profiles:
  - diurnal: Standard 24h day/night curve with normal CDN performance (~82% CHR).
  - ddos-spike: 15-minute attack spike with concentrated IPs, JA3s, and 429/503 errors.
  - origin-5xx-outage: 30-minute backend database outage (502/503/504 MISSes, high OTTFB).
  - bot-scrape: High-volume bot crawls across search engines, SEO tools, and scrapers.
  - slow-network: High RTT, elevated TLS handshakes, and slow mobile connections.

Usage:
  # Local fast fixture (100k rows, 24 hours):
  uv run python scripts/load_test/generate_synthetic_traffic.py \\
      --target local --scenario diurnal --rows 100000 --commit

  # Direct ClickHouse seeding for high-scale:
  uv run python scripts/load_test/generate_synthetic_traffic.py \\
      --target clickhouse --scenario diurnal --rows 50000

  # FOS upload + direct ClickHouse seeding:
  uv run python scripts/load_test/generate_synthetic_traffic.py \\
      --target fos --with-clickhouse --scenario diurnal --rows 50000

  # High-throughput rate pacing (50k sustained, 100k burst to FOS):
  uv run python scripts/load_test/generate_synthetic_traffic.py \\
      --target fos --rows 200000 --rate-rps 50000 --burst-rps 100000 --upload-workers 8
"""

from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import gzip
import io
import json
import math
import os
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# Add repo root to sys.path
for p in [Path.cwd(), Path("/app"), Path(__file__).resolve().parent.parent.parent]:
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from backend.config import list_configs, load_config  # noqa: E402
from backend.core.iceberg import (  # noqa: E402
    _buffer_dir,
    commit_buffer,
    get_arrow_schema,
)

# ---------------------------------------------------------------------------
# Dimension Pools & Weights
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
# Data Generation Core
# ---------------------------------------------------------------------------


def _generate_batch_data(
    n: int,
    start_ms: int,
    end_ms: int,
    scenario: str,
    rng: np.random.Generator,
) -> dict[str, Any]:
    """Generate realistic columnar data for n log events."""
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
        "ja3": ja3,
        "ja4": ja4,
        "cookie_session": cookie_session,
        "bot_category": bot_category,
        "waf_signal": waf_signal,
        "_source_file": np.array([f"synthetic://gen/{int(time.time())}"] * n, dtype=object),
    }


def _cols_to_arrow_table(cols: dict[str, Any], schema: pa.Schema) -> pa.Table:
    """Build pa.Table matching schema, gracefully defaulting missing catalog columns."""
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
# Target Runners: Local Parquet vs FOS S3 Upload
# ---------------------------------------------------------------------------


def run_target_local(
    src: dict[str, Any],
    scenario: str,
    rows: int,
    start_dt: datetime,
    end_dt: datetime,
    batch_size: int,
    file_rows: int,
    seed: int,
    commit: bool,
    clean: bool,
    dry_run: bool,
    rate_rps: int = 0,
    burst_rps: int = 0,
    burst_duration: int = 30,
    burst_interval: int = 120,
) -> int:
    """Writes Parquet directly to local buffer and optionally commits to DuckLake."""
    buf_dir = _buffer_dir(src)
    os.makedirs(buf_dir, exist_ok=True)

    if clean:
        print(f"Cleaning existing buffer files in {buf_dir}...")
        for f in Path(buf_dir).glob("*.parquet"):
            try:
                f.unlink()
            except Exception as e:
                print(f"  warning: failed to delete {f.name}: {e}")

    schema = get_arrow_schema(src.get("log_fields", {}))
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    rng = np.random.default_rng(seed)

    t0 = time.monotonic()
    rows_remaining = rows
    file_idx = 0
    total_rows = 0

    pacing_desc = (
        f"{rate_rps:,} rps (burst: {burst_rps:,} rps for {burst_duration}s every {burst_interval}s)"
        if rate_rps > 0
        else "Unthrottled (maximum throughput)"
    )

    print(
        f"Generating {rows:,} rows [{scenario}] for service '{src.get('service_id')}'\n"
        f"  Window: {start_dt.isoformat()} -> {end_dt.isoformat()}\n"
        f"  Target: Local buffer ({buf_dir})\n"
        f"  Pacing: {pacing_desc}"
    )

    if dry_run:
        print("[dry-run] Sample batch generated successfully. Exiting without writing.")
        return 0

    while rows_remaining > 0:
        rows_this_file = min(file_rows, rows_remaining)
        fname = f"synth_{scenario}_{int(time.time())}_{file_idx:04d}.parquet"
        fpath = os.path.join(buf_dir, fname)
        writer = pq.ParquetWriter(fpath, schema, compression="zstd", compression_level=1)

        rows_in_this_file = 0
        while rows_in_this_file < rows_this_file:
            b_t0 = time.monotonic()
            elapsed_total = b_t0 - t0
            if burst_rps > 0 and (elapsed_total % burst_interval) < burst_duration:
                target_rps = burst_rps
            elif rate_rps > 0:
                target_rps = rate_rps
            else:
                target_rps = 0

            n = min(batch_size, rows_this_file - rows_in_this_file)
            cols = _generate_batch_data(n, start_ms, end_ms, scenario, rng)
            tbl = _cols_to_arrow_table(cols, schema)
            tbl = tbl.sort_by([("timestamp", "ascending"), ("ip", "ascending")])
            writer.write_table(tbl)
            rows_in_this_file += n

            if target_rps > 0:
                target_batch_sec = n / target_rps
                b_dur = time.monotonic() - b_t0
                if target_batch_sec > b_dur:
                    time.sleep(target_batch_sec - b_dur)

        writer.close()
        rows_remaining -= rows_this_file
        total_rows += rows_this_file
        file_idx += 1
        elapsed = time.monotonic() - t0
        rate = total_rows / max(elapsed, 0.001)
        size_mb = os.path.getsize(fpath) / (1024 * 1024)
        print(
            f"  wrote {fname}: {rows_this_file:,} rows ({size_mb:.2f} MB) | "
            f"total: {total_rows:,}/{rows:,} ({100 * total_rows / rows:.1f}%) | "
            f"{rate:,.0f} rows/s",
            flush=True,
        )

    total_elapsed = time.monotonic() - t0
    print(
        f"\nSUCCESS: Generated {total_rows:,} rows in {total_elapsed:.2f}s ({total_rows / total_elapsed:,.0f} rows/s)."
    )

    if commit:
        print("\nCommitting buffer to DuckLake table...", flush=True)
        t_commit = time.monotonic()
        res = commit_buffer(src)
        print(
            f"COMMITTED: {res.get('rows_committed', 0):,} rows in "
            f"{res.get('files_committed', 0)} files in "
            f"{time.monotonic() - t_commit:.2f}s."
        )

    return 0


def run_target_fos(
    src: dict[str, Any],
    scenario: str,
    rows: int,
    start_dt: datetime,
    end_dt: datetime,
    batch_size: int,
    seed: int,
    dry_run: bool,
    rate_rps: int = 0,
    burst_rps: int = 0,
    burst_duration: int = 30,
    burst_interval: int = 120,
    upload_workers: int = 8,
) -> int:
    """Generates gzipped NDJSON and uploads directly to FOS object storage with rate pacing & background uploads."""
    from backend.provision.log_paths import minute_list_prefix

    bucket = src.get("s3_bucket") or src.get("fos_bucket")
    if not bucket:
        print("ERROR: Service config does not specify an s3_bucket / fos_bucket", file=sys.stderr)
        return 1

    import boto3
    from botocore.config import Config

    endpoint = src.get("s3_endpoint") or src.get("fos_endpoint")
    if endpoint and not endpoint.startswith("http://") and not endpoint.startswith("https://"):
        endpoint = f"https://{endpoint}"
    key_id = src.get("s3_access_key") or src.get("fos_access_key_id")
    secret_key = src.get("s3_secret_key") or src.get("fos_secret_access_key")

    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=key_id,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4", max_pool_connections=max(25, upload_workers * 2)),
    )

    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    rng = np.random.default_rng(seed)

    pacing_desc = (
        f"{rate_rps:,} rps (burst: {burst_rps:,} rps for {burst_duration}s every {burst_interval}s)"
        if rate_rps > 0
        else "Unthrottled (maximum throughput)"
    )

    print(
        f"Generating and uploading {rows:,} raw log records [{scenario}] to FOS\n"
        f"  Bucket: {bucket}\n"
        f"  Service: {src.get('service_id')}\n"
        f"  Window: {start_dt.isoformat()} -> {end_dt.isoformat()}\n"
        f"  Upload Workers: {upload_workers}\n"
        f"  Pacing: {pacing_desc}"
    )

    rows_remaining = rows
    file_idx = 0
    t0 = time.monotonic()
    total_bytes_uploaded = 0
    total_rows_emitted = 0

    upload_pool = ThreadPoolExecutor(max_workers=upload_workers) if not dry_run else None
    pending_uploads = set()

    def _upload_task(bucket_name: str, key_name: str, payload: bytes) -> int:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key_name,
            Body=payload,
            ContentType="application/gzip",
        )
        return len(payload)

    try:
        while rows_remaining > 0:
            b_t0 = time.monotonic()
            elapsed_total = b_t0 - t0

            if burst_rps > 0 and (elapsed_total % burst_interval) < burst_duration:
                target_rps = burst_rps
                in_burst = True
            elif rate_rps > 0:
                target_rps = rate_rps
                in_burst = False
            else:
                target_rps = 0
                in_burst = False

            n = min(batch_size, rows_remaining)
            cols = _generate_batch_data(n, start_ms, end_ms, scenario, rng)

            # Fast NDJSON formatting with pre-converted lists
            cols_py = {k: cols[k].tolist() if hasattr(cols[k], "tolist") else cols[k] for k in cols}
            lines = []
            keys = [k for k in cols_py if k != "_source_file"]
            ts_list = cols_py["timestamp"]
            for i in range(n):
                record = {k: cols_py[k][i] for k in keys}
                record["timestamp"] = datetime.fromtimestamp(ts_list[i] / 1_000_000, tz=UTC).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
                lines.append(json.dumps(record, default=str))

            ndjson_bytes = "\n".join(lines).encode("utf-8")
            gz_buf = io.BytesIO()
            with gzip.GzipFile(fileobj=gz_buf, mode="wb", compresslevel=1) as gz:
                gz.write(ndjson_bytes)
            gz_bytes = gz_buf.getvalue()

            now_utc = datetime.now(UTC)
            prefix = (src.get("s3_prefix") or src.get("fos_prefix") or "").strip("/")
            min_prefix = minute_list_prefix(now_utc)
            base_dir = f"{prefix}/{min_prefix}" if prefix else min_prefix
            s3_key = f"{base_dir}{src.get('service_id')}_{now_utc.strftime('%Y%m%dT%H%M%SZ')}_{file_idx:04d}.log.gz"

            mode_str = f"BURST {burst_rps:,} rps" if in_burst else (f"NORMAL {rate_rps:,} rps" if rate_rps > 0 else "MAX")

            if dry_run:
                total_bytes_uploaded += len(gz_bytes)
                print(
                    f"  [dry-run] [{mode_str}] File {file_idx:04d}: {n:,} rows -> "
                    f"s3://{bucket}/{s3_key} ({len(gz_bytes):,} bytes)"
                )
            else:
                assert upload_pool is not None
                if len(pending_uploads) >= upload_workers * 2:
                    done, pending_uploads = wait(pending_uploads, return_when=FIRST_COMPLETED)
                    for fut in done:
                        total_bytes_uploaded += fut.result()

                fut = upload_pool.submit(_upload_task, bucket, s3_key, gz_bytes)
                pending_uploads.add(fut)
                print(
                    f"  queued [{mode_str}] s3://{bucket}/{s3_key} ({len(gz_bytes):,} bytes, {n:,} rows)",
                    flush=True,
                )

            total_rows_emitted += n
            rows_remaining -= n
            file_idx += 1

            if target_rps > 0:
                target_batch_sec = n / target_rps
                b_dur = time.monotonic() - b_t0
                if target_batch_sec > b_dur:
                    time.sleep(target_batch_sec - b_dur)

        if upload_pool and pending_uploads:
            print(f"Waiting for {len(pending_uploads)} pending S3 uploads to complete...")
            done, _ = wait(pending_uploads)
            for fut in done:
                total_bytes_uploaded += fut.result()

    finally:
        if upload_pool:
            upload_pool.shutdown(wait=True)

    elapsed = time.monotonic() - t0
    rate = total_rows_emitted / max(elapsed, 0.001)
    print(
        f"\nSUCCESS: Finished FOS upload of {total_rows_emitted:,} rows ({total_bytes_uploaded / (1024 * 1024):.2f} MB compressed) "
        f"in {elapsed:.2f}s ({rate:,.0f} rows/s)."
    )
    return 0


def run_target_clickhouse(
    src: dict[str, Any],
    scenario: str,
    rows: int,
    start_dt: datetime,
    end_dt: datetime,
    batch_size: int,
    seed: int,
    dry_run: bool,
    clickhouse_host: str | None = None,
    clickhouse_port: int | None = None,
    clickhouse_database: str | None = None,
    clickhouse_user: str | None = None,
    clickhouse_password: str | None = None,
    rate_rps: int = 0,
    burst_rps: int = 0,
    burst_duration: int = 30,
    burst_interval: int = 120,
) -> int:
    """Generates synthetic log records and inserts them directly into ClickHouse."""
    from uuid import uuid4

    from backend.core.clickhouse_client import (
        ClickHouseClient,
        get_clickhouse_client,
    )
    from backend.high_scale.publication import (
        ClickHouseBatchAdapter,
        HighScaleBatch,
    )
    from backend.high_scale.schema_install import install_clickhouse_schema

    # Apply configuration overrides to environment
    if clickhouse_host:
        os.environ["CLICKHOUSE_HOST"] = clickhouse_host
    if clickhouse_port:
        os.environ["CLICKHOUSE_PORT"] = str(clickhouse_port)
    if clickhouse_database:
        os.environ["CLICKHOUSE_DATABASE"] = clickhouse_database
    if clickhouse_user:
        os.environ["CLICKHOUSE_USER"] = clickhouse_user
    if clickhouse_password is not None:
        os.environ["CLICKHOUSE_PASSWORD"] = clickhouse_password

    os.environ.setdefault("CLICKHOUSE_ENABLED", "true")
    os.environ.setdefault("CLICKHOUSE_HOST", "localhost")
    os.environ.setdefault("CLICKHOUSE_DATABASE", "default")
    os.environ.setdefault("CLICKHOUSE_USER", "default")
    os.environ.setdefault("CLICKHOUSE_SECURE", "false")

    client: ClickHouseClient | None = None
    if not dry_run:
        try:
            client = get_clickhouse_client()
            if client is None:
                from backend.config import load_clickhouse_config

                cfg = load_clickhouse_config()
                if cfg:
                    client = ClickHouseClient(cfg)
            if client is None:
                print("ERROR: Could not initialize ClickHouse client. Check CLICKHOUSE_* settings.", file=sys.stderr)
                return 1
            client.health()
            install_clickhouse_schema()
        except Exception as exc:
            print(f"ERROR: ClickHouse connection/schema initialization failed: {exc}", file=sys.stderr)
            return 1

    service_id = src.get("service_id", "default_service")
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    rng = np.random.default_rng(seed)

    pacing_desc = (
        f"{rate_rps:,} rps (burst: {burst_rps:,} rps for {burst_duration}s every {burst_interval}s)"
        if rate_rps > 0
        else "Unthrottled (maximum throughput)"
    )

    print(
        f"Generating and inserting {rows:,} log records [{scenario}] into ClickHouse\n"
        f"  Service: {service_id}\n"
        f"  Window: {start_dt.isoformat()} -> {end_dt.isoformat()}\n"
        f"  Pacing: {pacing_desc}"
    )

    rows_remaining = rows
    batch_idx = 0
    t0 = time.monotonic()
    adapter = ClickHouseBatchAdapter(client) if client else None
    total_rows_emitted = 0

    while rows_remaining > 0:
        b_t0 = time.monotonic()
        elapsed_total = b_t0 - t0

        if burst_rps > 0 and (elapsed_total % burst_interval) < burst_duration:
            target_rps = burst_rps
            in_burst = True
        elif rate_rps > 0:
            target_rps = rate_rps
            in_burst = False
        else:
            target_rps = 0
            in_burst = False

        n = min(batch_size, rows_remaining)
        cols = _generate_batch_data(n, start_ms, end_ms, scenario, rng)
        cols_py = {k: cols[k].tolist() if hasattr(cols[k], "tolist") else cols[k] for k in cols}

        batch_rows = []
        batch_uid = uuid4().hex[:8]
        ts_list = cols_py["timestamp"]
        for i in range(n):
            row_dict = {k: cols_py[k][i] for k in cols_py if k != "_source_file"}
            ts_us = ts_list[i]
            row_dt = datetime.fromtimestamp(ts_us / 1_000_000, tz=UTC)
            row_dict["timestamp"] = row_dt.isoformat()
            row_dict["event_id"] = f"syn_{batch_idx}_{i}_{batch_uid}"
            row_dict["source_object_key"] = f"synthetic://{service_id}/{batch_idx}"
            row_dict["source_object_version"] = "1"
            row_dict["line_ordinal"] = i
            row_dict["transform_version"] = "v1"
            row_dict["client_ip"] = str(row_dict.get("ip", ""))
            batch_rows.append(row_dict)

        batch_id = f"batch_{service_id}_{batch_idx}_{batch_uid}"
        batch = HighScaleBatch(
            batch_id=batch_id,
            service_id=service_id,
            domain="request",
            generation="1",
            rows=tuple(batch_rows),
        )

        mode_str = f"BURST {burst_rps:,} rps" if in_burst else (f"NORMAL {rate_rps:,} rps" if rate_rps > 0 else "MAX")

        if dry_run:
            print(f"  [dry-run] [{mode_str}] Would insert ClickHouse batch {batch_id}: {n:,} rows ({batch.digest[:16]}...)")
        else:
            assert adapter is not None
            receipt = adapter.insert(batch)
            print(
                f"  inserted [{mode_str}] ClickHouse batch {receipt.batch_id}: "
                f"{receipt.rows_inserted:,} rows ({receipt.digest[:16]}...)"
            )

        total_rows_emitted += n
        rows_remaining -= n
        batch_idx += 1

        if target_rps > 0:
            target_batch_sec = n / target_rps
            b_dur = time.monotonic() - b_t0
            if target_batch_sec > b_dur:
                time.sleep(target_batch_sec - b_dur)

    elapsed = time.monotonic() - t0
    rate = total_rows_emitted / max(elapsed, 0.001)
    print(f"\nSUCCESS: Finished ClickHouse insertion of {total_rows_emitted:,} rows in {elapsed:.2f}s ({rate:,.0f} rows/s).")
    return 0


# ---------------------------------------------------------------------------
# CLI Entrypoint
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--service-id",
        help="Service ID to populate. Defaults to first available config.",
    )
    parser.add_argument(
        "--config",
        help="Path to service config JSON file.",
    )
    parser.add_argument(
        "--target",
        choices=["local", "fos", "clickhouse"],
        default="local",
        help="Target data tier: 'local' (direct DuckLake Parquet), 'fos' (raw gzipped S3 upload), or 'clickhouse'.",
    )
    parser.add_argument(
        "--with-clickhouse",
        action="store_true",
        help="Also seed generated batches directly to ClickHouse (works alongside --target local or --target fos).",
    )
    parser.add_argument(
        "--clickhouse-host",
        help="ClickHouse host (defaults to CLICKHOUSE_HOST env or 'localhost').",
    )
    parser.add_argument(
        "--clickhouse-port",
        type=int,
        help="ClickHouse HTTP port (defaults to CLICKHOUSE_PORT env or 8123).",
    )
    parser.add_argument(
        "--clickhouse-database",
        help="ClickHouse database (defaults to CLICKHOUSE_DATABASE env or 'default').",
    )
    parser.add_argument(
        "--clickhouse-user",
        help="ClickHouse user (defaults to CLICKHOUSE_USER env or 'default').",
    )
    parser.add_argument(
        "--clickhouse-password",
        help="ClickHouse password (defaults to CLICKHOUSE_PASSWORD env or '').",
    )
    parser.add_argument(
        "--scenario",
        "--profile",
        dest="scenario",
        choices=["diurnal", "ddos-spike", "origin-5xx-outage", "bot-scrape", "slow-network"],
        default="diurnal",
        help="Incident/traffic pattern scenario.",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=100_000,
        help="Total rows to generate (default: 100,000).",
    )
    parser.add_argument(
        "--span-hours",
        type=int,
        default=24,
        help="Time window span in hours (default: 24).",
    )
    parser.add_argument(
        "--span-days",
        type=int,
        help="Time window span in days (overrides --span-hours).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=25_000,
        help="Rows per generation batch.",
    )
    parser.add_argument(
        "--file-rows",
        type=int,
        default=50_000,
        help="Rows per output Parquet file.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="RNG seed for reproducibility.",
    )
    parser.add_argument(
        "--commit",
        dest="commit",
        action="store_true",
        default=True,
        help="Commit buffer to DuckLake table after writing (default: True).",
    )
    parser.add_argument(
        "--no-commit",
        dest="commit",
        action="store_false",
        help="Skip committing buffer to DuckLake table.",
    )
    parser.add_argument(
        "--rate-rps",
        type=int,
        default=0,
        help="Sustained rate pacing in requests per second (e.g. 50000). 0 disables rate pacing (max throughput).",
    )
    parser.add_argument(
        "--burst-rps",
        type=int,
        default=0,
        help="Burst rate pacing in requests per second (e.g. 100000). 0 disables burst modulation.",
    )
    parser.add_argument(
        "--burst-duration",
        type=int,
        default=30,
        help="Duration of each burst in seconds (default: 30s).",
    )
    parser.add_argument(
        "--burst-interval",
        type=int,
        default=120,
        help="Interval between bursts in seconds (default: 120s).",
    )
    parser.add_argument(
        "--upload-workers",
        type=int,
        default=8,
        help="Concurrent S3 upload worker threads for --target fos (default: 8).",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Purge existing buffer files before generating.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview generation without writing files or uploading.",
    )

    args = parser.parse_args()

    # Resolve service configuration
    src = None
    if args.config:
        with open(args.config) as f:
            src = json.load(f)
    elif args.service_id:
        src = load_config(args.service_id)
    else:
        configs = list_configs()
        if configs:
            src = configs[0]
            print(f"Using default active service: {src.get('service_id')}")
        else:
            print("ERROR: No configured services found in configs/", file=sys.stderr)
            return 2

    if not src:
        print(f"ERROR: Could not load configuration for service {args.service_id!r}", file=sys.stderr)
        return 2

    span_hours = args.span_days * 24 if args.span_days else args.span_hours
    end_dt = datetime.now(UTC)
    start_dt = end_dt - timedelta(hours=span_hours)

    res = 0
    if args.target == "local":
        res = run_target_local(
            src=src,
            scenario=args.scenario,
            rows=args.rows,
            start_dt=start_dt,
            end_dt=end_dt,
            batch_size=args.batch_size,
            file_rows=args.file_rows,
            seed=args.seed,
            commit=args.commit,
            clean=args.clean,
            dry_run=args.dry_run,
            rate_rps=args.rate_rps,
            burst_rps=args.burst_rps,
            burst_duration=args.burst_duration,
            burst_interval=args.burst_interval,
        )
    elif args.target == "fos":
        res = run_target_fos(
            src=src,
            scenario=args.scenario,
            rows=args.rows,
            start_dt=start_dt,
            end_dt=end_dt,
            batch_size=args.batch_size,
            seed=args.seed,
            dry_run=args.dry_run,
            rate_rps=args.rate_rps,
            burst_rps=args.burst_rps,
            burst_duration=args.burst_duration,
            burst_interval=args.burst_interval,
            upload_workers=args.upload_workers,
        )
    elif args.target == "clickhouse":
        res = run_target_clickhouse(
            src=src,
            scenario=args.scenario,
            rows=args.rows,
            start_dt=start_dt,
            end_dt=end_dt,
            batch_size=args.batch_size,
            seed=args.seed,
            dry_run=args.dry_run,
            clickhouse_host=args.clickhouse_host,
            clickhouse_port=args.clickhouse_port,
            clickhouse_database=args.clickhouse_database,
            clickhouse_user=args.clickhouse_user,
            clickhouse_password=args.clickhouse_password,
            rate_rps=args.rate_rps,
            burst_rps=args.burst_rps,
            burst_duration=args.burst_duration,
            burst_interval=args.burst_interval,
        )

    if res == 0 and args.with_clickhouse and args.target != "clickhouse":
        res = run_target_clickhouse(
            src=src,
            scenario=args.scenario,
            rows=args.rows,
            start_dt=start_dt,
            end_dt=end_dt,
            batch_size=args.batch_size,
            seed=args.seed,
            dry_run=args.dry_run,
            clickhouse_host=args.clickhouse_host,
            clickhouse_port=args.clickhouse_port,
            clickhouse_database=args.clickhouse_database,
            clickhouse_user=args.clickhouse_user,
            clickhouse_password=args.clickhouse_password,
            rate_rps=args.rate_rps,
            burst_rps=args.burst_rps,
            burst_duration=args.burst_duration,
            burst_interval=args.burst_interval,
        )

    return res


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Synthetic RUM Traffic Generator for Fastly Log Analytics.

Generates mock Faro-compliant browser Web Vitals JSON lines, gzips them,
and uploads them directly to the configured service FOS bucket under the
correct 'raw/rum/' prefix to support local offline standard RUM testing.
"""

import argparse
import gzip
import json
import os
import random
import sys
import tempfile
import time
from datetime import datetime, UTC

# Insert backend directory to import libraries
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from backend.core.duckdb import get_source_for_service
from backend.core.ingest import _get_fos_client


def generate_rum_record(service_id: str, timestamp_str: str) -> dict:
    metric = random.choice(["LCP", "CLS", "INP", "FID", "TTFB", "FCP"])
    
    # Generate realistic values and ratings
    if metric == "LCP":
        value = random.choice([800, 1200, 2200, 3100, 4200])
        rating = "good" if value <= 2500 else ("needs_improvement" if value <= 4000 else "poor")
    elif metric == "CLS":
        value = round(random.uniform(0.01, 0.35), 3)
        rating = "good" if value <= 0.1 else ("needs_improvement" if value <= 0.25 else "poor")
    elif metric == "INP":
        value = random.choice([50, 120, 180, 240, 320])
        rating = "good" if value <= 200 else ("needs_improvement" if value <= 500 else "poor")
    elif metric == "TTFB":
        value = random.choice([150, 250, 650, 950])
        rating = "good" if value <= 800 else "poor"
    else: # FID or FCP
        value = random.choice([10, 25, 80, 150])
        rating = "good" if value <= 100 else "poor"

    browser = random.choice(["Chrome", "Firefox", "Safari", "Edge", "Mobile Safari"])
    os_name = random.choice(["Windows", "macOS", "iOS", "Android", "Linux"])
    device = "Mobile" if "Mobile" in browser or os_name in ("iOS", "Android") else "Desktop"
    
    city = random.choice(["Denver", "New York", "San Francisco", "London", "Tokyo", "Paris"])
    region = random.choice(["CO", "NY", "CA", "ENG", "TKY", "IDF"])
    country = random.choice(["US", "US", "US", "GB", "JP", "FR"])
    pop = random.choice(["DEN", "EWR", "SJC", "LCY", "TYO", "CDG"])

    path = random.choice(["/", "/dashboard", "/origin", "/security", "/performance", "/rum"])
    cid = f"cid-{random.randint(10000, 99999)}"

    # Faro payload structure
    return {
        "service_id": service_id,
        "timestamp": timestamp_str,
        "geo_city": city,
        "geo_region": region,
        "geo_country_code": country,
        "server_pop": pop,
        "browser": browser,
        "os": os_name,
        "device": device,
        "rum_cid": cid,
        "url": f"http://localhost{path}",
        "faro": {
            "meta": {
                "browser": {
                    "name": browser,
                    "mobile": device == "Mobile"
                },
                "os": {
                    "name": os_name
                },
                "page": {
                    "url": f"http://localhost{path}"
                }
            },
            "measurements": [
                {
                    "type": "web-vitals",
                    "values": {
                        metric: value
                    },
                    "context": {
                        "rating": rating
                    }
                }
            ]
        }
    }


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic RUM traffic and upload to FOS")
    parser.add_argument("--service-id", default="ZU15BvY2LX7WcEp43T9VwU", help="Service ID to populate")
    parser.add_argument("--rows", type=int, default=1000, help="Number of RUM log records to generate")
    args = parser.parse_args()

    print(f"🚀 SEEDING SYNTHETIC RUM TRAFFIC FOR SERVICE: {args.service_id}")
    
    # Load FOS credentials
    src = get_source_for_service(args.service_id)
    if not src:
        print(f"❌ Error: Service ID {args.service_id} config not found!")
        sys.exit(1)

    bucket = src.get("bucket")
    if not bucket:
        print("❌ Error: FOS bucket not configured for this service!")
        sys.exit(1)

    # Initialize FOS client
    s3 = _get_fos_client(src)
    
    # Resolve the correct time-based folder layout for raw/rum prefix
    now = datetime.now(UTC)
    prefix = now.strftime("raw/rum/year=%Y/month=%m/day=%d/hour=%H/minute=%M/")
    filename = f"{now.strftime('%Y-%m-%dT%H:%M:%S.000')}-mock-rum-seeding-{random.randint(1000, 9999)}.log.gz"
    key = f"{prefix}{filename}"

    print(f"   - Bucket: {bucket}")
    print(f"   - Target Key: {key}")

    # Generate records
    print(f"   - Generating {args.rows:,} mock Faro vitals records...")
    timestamp_str = now.isoformat()
    records = [generate_rum_record(args.service_id, timestamp_str) for _ in range(args.rows)]

    # Write gzipped log to temp file
    with tempfile.NamedTemporaryFile(suffix=".log.gz", delete=False) as tmp:
        temp_path = tmp.name
        
    try:
        with gzip.open(temp_path, "wt", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r) + "\n")

        # Upload to FOS
        print("   - Uploading gzipped log file to FOS S3 bucket...")
        s3.upload_file(temp_path, bucket, key)
        print(f"✅ Successful! Uploaded mock RUM logs as {key} ({os.path.getsize(temp_path)} bytes)")
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


if __name__ == "__main__":
    main()

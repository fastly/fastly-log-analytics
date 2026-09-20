#!/usr/bin/env python3
# scripts/dev/seed_local_rum.py
# Generates realistic RUM beacon CDN logs and uploads them directly to local Standard and High-Scale FOS buckets.

import argparse
import gzip
import io
import json
import os
import random
import sys
import uuid
from datetime import datetime, UTC

# Add project root to python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend import config as svcconfig
from backend.core.duckdb import get_source_for_service, is_configured


def generate_mock_rum_log(service_id: str) -> str:
    """Generate a single JSON log line mimicking Fastly CDN edge-logged RUM transactions."""
    browsers = ["Chrome", "Safari", "Firefox", "Edge", "Opera"]
    os_list = ["macOS", "Windows", "iOS", "Android", "Linux"]
    devices = ["Desktop", "Mobile", "Tablet"]
    paths = ["/", "/dashboard", "/origin", "/security", "/performance", "/alerts", "/control-room"]

    metric = random.choice(["LCP", "CLS", "INP", "FID", "TTFB", "FCP"])
    value = random.choice([150, 250, 1200, 2500, 3500]) if metric != "CLS" else round(random.uniform(0.01, 0.45), 3)

    browser = random.choice(browsers)
    os_name = random.choice(os_list)
    device = random.choice(devices)
    path = random.choice(paths)
    cid = f"cid-{random.randint(1000, 99999)}"

    # Construct the raw URL with query-string parameters parsed by backend/core/ingest.py
    raw_query_url = (
        f"https://example.invalid/rum-beacon"
        f"?rum_metric_name={metric}"
        f"&rum_metric_value={value}"
        f"&rum_cid={cid}"
        f"&rum_pathname={path}"
    )

    log_entry = {
        "service_id": service_id,
        "timestamp": datetime.now(UTC).isoformat(),
        "url": raw_query_url,
        "browser": browser,
        "os": os_name,
        "device": device,
        "fastly_req_id": str(uuid.uuid4()).replace("-", "")[:16],
        "geo_city": random.choice(["Denver", "New York", "San Francisco", "Austin", "London", "Tokyo"]),
        "geo_region": random.choice(["CO", "NY", "CA", "TX", "ENG", "13"]),
        "geo_country_code": random.choice(["US", "US", "US", "US", "GB", "JP"]),
        "server_pop": random.choice(["DEN", "JFK", "SJC", "DFW", "LHR", "TYO"]),
        "tls_version": random.choice(["TLSv1.3", "TLSv1.2"]),
        "time_to_first_byte": round(random.uniform(0.02, 0.45), 3),
    }
    return json.dumps(log_entry) + "\n"


def seed_service_rum(service_id: str, num_files: int = 3, rows_per_file: int = 15) -> None:
    print(f"\nSeeding RUM logs for service {service_id}...")

    src = get_source_for_service(service_id)
    if not src or not is_configured(src):
        print(f"❌ Service {service_id} is not fully configured locally. Skipping.")
        return

    # Dynamically load the S3/FOS storage client configured for this service
    try:
        from backend.core.s3_client import get_s3_client
        s3 = get_s3_client(src)
    except Exception as e:
        print(f"❌ Failed to initialize S3 client: {e}. Skipping.")
        return

    bucket = src.get("bucket", "fos-zu15bvy2lx7wcep43t9vwu-logs")
    uploaded_count = 0

    for i in range(num_files):
        # Generate raw gzipped log data
        log_lines = [generate_mock_rum_log(service_id) for _ in range(rows_per_file)]
        raw_content = "".join(log_lines).encode("utf-8")

        # Compress to gzip
        gzip_buf = io.BytesIO()
        with gzip.GzipFile(fileobj=gzip_buf, mode="wb") as f:
            f.write(raw_content)
        gzip_data = gzip_buf.getvalue()

        # Upload to FOS raw/rum/ prefix
        file_uuid = str(uuid.uuid4())[:8]
        timestamp_str = datetime.now(UTC).strftime("%Y-%m-%dT%H%M%S")
        object_key = f"raw/rum/year=2026/month=09/day=20/hour=22/minute=00/{timestamp_str}-{file_uuid}.log.gz"

        try:
            s3.put_object(
                Bucket=bucket,
                Key=object_key,
                Body=gzip_data,
                ContentType="application/x-gzip",
            )
            print(f"   [+] Uploaded s3://{bucket}/{object_key} ({len(log_lines)} rows)")
            uploaded_count += 1
        except Exception as e:
            print(f"   [x] Failed to upload {object_key}: {e}")

    print(f"✅ Successfully uploaded {uploaded_count} RUM log files to FOS!")


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed Local Standard/High-Scale services with mock RUM log files in FOS.")
    parser.add_argument("--files", type=int, default=3, help="Number of files to generate per service")
    parser.add_argument("--rows", type=int, default=20, help="Number of RUM rows per file")
    args = parser.parse_args()

    # Active Local Services
    local_standard_id = "ZU15BvY2LX7WcEp43T9VwU"
    local_high_scale_id = "qI4D8yXXFYOIpZEMrkJy65"

    seed_service_rum(local_standard_id, args.files, args.rows)
    seed_service_rum(local_high_scale_id, args.files, args.rows)

    print("\n🎉 Seeding process complete! Ingestion crons will discover and process these files shortly.")


if __name__ == "__main__":
    main()

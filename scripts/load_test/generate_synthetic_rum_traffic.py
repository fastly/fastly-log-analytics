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
from datetime import UTC, datetime

# Insert backend directory to import libraries
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from backend.core.duckdb import get_source_for_service
from backend.core.ingest import _get_fos_client
from scripts.lib.log_factory import generate_rum_records


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic RUM traffic and upload to FOS")
    parser.add_argument("--service-id", required=True, help="Service ID to populate")
    parser.add_argument("--rows", type=int, default=1000, help="Number of RUM log records to generate")
    parser.add_argument("--seed", type=int, default=None, help="Seed the generator for reproducible records")
    args = parser.parse_args()
    rng = random.Random(args.seed)

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

    # Resolve correct time-based folder layout for raw/rum prefix
    now = datetime.now(UTC)
    prefix = now.strftime("raw/rum/year=%Y/month=%m/day=%d/hour=%H/minute=%M/")
    filename = f"{now.strftime('%Y-%m-%dT%H:%M:%S.000')}-mock-rum-seeding-{rng.randint(1000, 9999)}.log.gz"
    key = f"{prefix}{filename}"

    print(f"   - Bucket: {bucket}")
    print(f"   - Target Key: {key}")

    # Generate records with varied sequential timestamps
    print(f"   - Generating {args.rows:,} mock Faro vitals, events, and exceptions...")
    # Sequential 50ms steps keep every record's timestamp unique.
    records = generate_rum_records(args.service_id, args.rows, now, rng)

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

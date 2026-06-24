#!/usr/bin/env python3
"""
Cleanup orphaned raw log files in Fastly Object Storage (FOS).
Deletes any .gz files in the FOS bucket that have already been recorded as
ingested in the service's SQLite metadata database.
"""

import logging
import os
import sys

# Add project root to python path to ensure backend imports work correctly when run from the root directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from backend import config as svcconfig
from backend.core import metadata as metadata_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("cleanup")


def cleanup_orphans(service_id: str):
    cfg = svcconfig.load_config(service_id)
    if not cfg:
        logger.error(f"Config for service {service_id} not found.")
        sys.exit(1)

    src = svcconfig.config_to_source(cfg)
    if not src.get("bucket"):
        logger.error(f"Service {service_id} is not configured to use Fastly Object Storage (missing bucket config).")
        sys.exit(1)

    bucket = src["bucket"]
    prefix = src.get("prefix", "").strip("/")
    if prefix:
        prefix = f"{prefix}/raw/"
    else:
        prefix = "raw/"

    # 1. Connect to the SQLite metadata DB
    logger.info(f"Connecting to metadata database for service '{service_id}'...")
    con = metadata_db.get_con(service_id)

    # 2. Initialize S3 client via backend's proxy-enabled helper
    from backend.core.duckdb import _get_fos_client

    s3_client = _get_fos_client(src)

    logger.info(f"Listing raw files in bucket '{bucket}' with prefix '{prefix}'...")

    # 3. Retrieve all files currently marked as ingested in SQLite
    try:
        rows = con.execute("SELECT file_name FROM ingested_files").fetchall()
        ingested_set = {row[0] for row in rows}
    finally:
        con.close()

    logger.info(f"Found {len(ingested_set)} ingested files in local metadata database.")

    # 4. List all raw files in FOS and find orphans to delete
    paginator = s3_client.get_paginator("list_objects_v2")
    orphan_keys = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".gz"):
                continue

            # Reconstruct the absolute path format used in SQL
            abs_path = f"s3://{bucket}/{key}"

            # If the file has been successfully ingested, it's an orphan in FOS and safe to delete
            if abs_path in ingested_set:
                orphan_keys.append({"Key": key})

    if not orphan_keys:
        logger.info("No orphaned raw log files found in FOS. Bucket is clean!")
        return

    logger.info(f"Found {len(orphan_keys)} orphaned raw log files in FOS. Starting deletion...")

    # 5. Delete orphans in batches of 500
    batch_size = 500
    deleted_count = 0
    for i in range(0, len(orphan_keys), batch_size):
        batch = orphan_keys[i : i + batch_size]
        try:
            response = s3_client.delete_objects(Bucket=bucket, Delete={"Objects": batch, "Quiet": True})
            deleted_count += len(batch)
            logger.info(f"Deleted batch {i // batch_size + 1}: {len(batch)} files (Total deleted: {deleted_count})")
        except Exception as e:
            logger.error(f"Failed to delete batch starting at index {i}: {e}")

    logger.info(f"Successfully pruned {deleted_count} orphaned raw files from FOS.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Usage: python scripts/cleanup_orphan_raw_logs.py <service_id>")
        sys.exit(1)
    cleanup_orphans(sys.argv[1])

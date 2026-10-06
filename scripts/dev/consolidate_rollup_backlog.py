#!/usr/bin/env python3
"""One-time migration and ad-hoc consolidation script for historical rollups.

Sweeps fragmented per-hour rollup files (such as hour_ip_spread, hour_bundled, and
per-field day/hour parquets) and consolidates them into compact per-day bundles
(rollups/day_bundled/day=YYYY-MM-DD/all_fields.parquet), pruning obsolete hourly
and per-field intermediate files older than 14 days.

Usage:
    uv run python scripts/dev/consolidate_rollup_backlog.py [--service-id <id>] [--dry-run]
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from backend.config import list_configs
from backend.core.duckdb import _cache_dir, get_source_for_service
from backend.core.rollups import (
    _rollups_root,
    backfill_day_bundles,
    compact_closed_days_to_daily,
)
from backend.core.rollups._common import _ip_spread_root
from backend.core.rollups.hour_bundles import bundle_hours, bundle_hours_ip_spread
from backend.core.rollups.recompute import cleanup_old_rollups


def count_files_and_size(root_path: str) -> tuple[int, int]:
    """Count total files and total bytes recursively under root_path."""
    if not os.path.isdir(root_path):
        return 0, 0
    total_files = 0
    total_size = 0
    try:
        for root, _, fnames in os.walk(root_path):
            for f in fnames:
                total_files += 1
                try:
                    total_size += os.path.getsize(os.path.join(root, f))
                except OSError:
                    pass
    except OSError:
        pass
    return total_files, total_size


def consolidate_service_rollups(source: dict, dry_run: bool = False) -> dict:
    service_id = source.get("service_id") or source.get("name") or "default"
    display_name = source.get("name", service_id)
    cache_root = _cache_dir(source)
    rollups_dir = os.path.join(cache_root, "rollups")

    if not os.path.isdir(rollups_dir):
        print(f"[{service_id}] No rollups directory found at {rollups_dir}. Skipping.")
        return {}

    before_files, before_bytes = count_files_and_size(rollups_dir)
    print("\n=======================================================")
    print(f"Consolidating rollups for {display_name} ({service_id})")
    print(f"Rollups path: {rollups_dir}")
    print(f"Initial files: {before_files:,} ({before_bytes / (1024 * 1024):.2f} MB)")
    print("=======================================================")

    t0 = time.time()

    # Step 1: Enumerate and bundle all hour_ip_spread hours
    ip_spread_root = _ip_spread_root(source)
    hours_to_bundle: set[str] = set()
    if os.path.isdir(ip_spread_root):
        try:
            for fe in os.listdir(ip_spread_root):
                if not fe.startswith("field="):
                    continue
                fpath = os.path.join(ip_spread_root, fe)
                try:
                    for he in os.listdir(fpath):
                        if he.startswith("hour="):
                            hours_to_bundle.add(he[5:])
                except OSError:
                    continue
        except OSError:
            pass

    print(f"Found {len(hours_to_bundle):,} hour tokens in hour_ip_spread.")
    if not dry_run and hours_to_bundle:
        rebuilt_ip = bundle_hours_ip_spread(service_id, source, sorted(hours_to_bundle))
        print(f"Bundled {rebuilt_ip:,} hour_ip_spread hours into all_fields_ip.parquet.")
    elif dry_run:
        print(f"[dry-run] Would bundle {len(hours_to_bundle):,} hour_ip_spread hours.")

    # Step 2: Enumerate and bundle all count hours
    hour_root = _rollups_root(source)
    count_hours_to_bundle: set[str] = set()
    if os.path.isdir(hour_root):
        try:
            for fe in os.listdir(hour_root):
                if not fe.startswith("field="):
                    continue
                fpath = os.path.join(hour_root, fe)
                try:
                    for he in os.listdir(fpath):
                        if he.startswith("hour="):
                            count_hours_to_bundle.add(he[5:])
                except OSError:
                    continue
        except OSError:
            pass

    print(f"Found {len(count_hours_to_bundle):,} hour tokens in per-field rollups/hour.")
    if not dry_run and count_hours_to_bundle:
        rebuilt_count = bundle_hours(service_id, source, sorted(count_hours_to_bundle))
        print(f"Bundled {rebuilt_count:,} hours into all_fields.parquet.")
    elif dry_run:
        print(f"[dry-run] Would bundle {len(count_hours_to_bundle):,} hours.")

    # Step 3: Compact closed days to daily per-field and then bundle into day_bundled
    if not dry_run:
        days_compacted = compact_closed_days_to_daily(service_id, source)
        print(f"Compacted {days_compacted:,} (field, day) files to rollups/day.")
        days_bundled = backfill_day_bundles(service_id, source)
        print(f"Bundled {days_bundled:,} days into rollups/day_bundled.")
    else:
        print("[dry-run] Would run compact_closed_days_to_daily and backfill_day_bundles.")

    # Step 4: Retention and historical prune
    retention_months = int(source.get("rollup_retention_months", 12))
    max_age_days = int(source.get("rollups_days", retention_months * 30))
    if not dry_run:
        cleaned_dirs = cleanup_old_rollups(
            service_id,
            source,
            max_age_days=max_age_days,
            hour_bundle_max_age_days=14,
        )
        print(f"Pruned {cleaned_dirs:,} obsolete hourly and day directories.")
    else:
        print(f"[dry-run] Would run cleanup_old_rollups with max_age_days={max_age_days}, hour_bundle_max_age_days=14.")

    after_files, after_bytes = count_files_and_size(rollups_dir)
    elapsed = time.time() - t0

    print(f"\n--- Results for {service_id} ---")
    print(f"Duration: {elapsed:.2f}s")
    print(
        f"Files before: {before_files:,} -> Files after: {after_files:,} (Removed {before_files - after_files:,} files)"
    )
    print(f"Size before: {before_bytes / (1024 * 1024):.2f} MB -> Size after: {after_bytes / (1024 * 1024):.2f} MB")
    print("---------------------------------\n")

    return {
        "service_id": service_id,
        "before_files": before_files,
        "after_files": after_files,
        "deleted_files": max(0, before_files - after_files),
        "duration_s": round(elapsed, 2),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Consolidate fragmented rollup files into daily bundles.")
    parser.add_argument("--service-id", help="Fastly service ID (default: all configured services)")
    parser.add_argument("--dry-run", action="store_true", help="Print planned actions without modifying disk")
    args = parser.parse_args()

    services: list[dict] = []
    if args.service_id:
        src = get_source_for_service(args.service_id)
        if not src:
            print(f"Error: service {args.service_id} not found in configurations.")
            return 1
        services.append(src)
    else:
        configs = list_configs()
        for cfg in configs:
            sid = cfg.get("service_id") or cfg.get("name")
            if sid:
                src = get_source_for_service(sid)
                if src:
                    services.append(src)

    if not services:
        print("No services found to consolidate.")
        return 1

    for src in services:
        consolidate_service_rollups(src, dry_run=args.dry_run)

    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
Utility script to backfill pre-computed rollups for an existing service.
"""

import argparse
import logging
import sys

from backend.config import config_to_source, load_config
from backend.core.rollups import backfill_rollups

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Backfill dashboard rollups for a service")
    parser.add_argument("service_id", help="The Fastly Service ID")
    args = parser.parse_args()

    service_id = args.service_id
    cfg = load_config(service_id)
    if not cfg:
        logger.error("Configuration not found for service: %s", service_id)
        sys.exit(1)

    # rollups.* helpers expect the normalized source dict (where `name` is the
    # SQL-safe slug, not the human-readable display name). load_config returns
    # the raw on-disk config; without the conversion, _safe_table_for rejects
    # any service whose `name` field contains spaces or other non-identifier
    # characters.
    source = config_to_source(cfg)

    logger.info("Starting rollup backfill for service: %s", service_id)
    backfill_rollups(service_id, source)
    logger.info("Backfill complete.")


if __name__ == "__main__":
    main()

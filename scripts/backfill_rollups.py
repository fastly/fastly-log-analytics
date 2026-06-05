#!/usr/bin/env python3
"""
Utility script to backfill pre-computed rollups for an existing service.
"""

import argparse
import logging
import sys

from backend.config import load_config
from backend.core.rollups import backfill_rollups

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Backfill dashboard rollups for a service")
    parser.add_argument("service_id", help="The Fastly Service ID")
    args = parser.parse_args()

    service_id = args.service_id
    source = load_config(service_id)
    if not source:
        logger.error("Configuration not found for service: %s", service_id)
        sys.exit(1)

    logger.info("Starting rollup backfill for service: %s", service_id)
    backfill_rollups(service_id, source)
    logger.info("Backfill complete.")


if __name__ == "__main__":
    main()

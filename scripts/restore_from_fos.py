#!/usr/bin/env python3
"""Standalone CLI for Elevation zero-DB cold-start restore from FOS."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from dataclasses import asdict

from backend.high_scale.cold_start_restore import restore_from_fos, scrub_secrets


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="restore_from_fos",
        description=(
            "Restore a brand-new High-Scale Elevation installation (empty Postgres & ClickHouse) "
            "from Fastly Object Storage (FOS) using a Fastly API key or explicit FOS credentials."
        ),
    )
    parser.add_argument("--service-id", required=True, help="Fastly service ID to restore")
    parser.add_argument("--bucket", required=True, help="FOS bucket name")
    parser.add_argument("--region", required=True, help="FOS region (e.g. us-east-1)")
    parser.add_argument("--prefix", default="", help="Optional FOS base prefix for meta/dr_state.json")
    parser.add_argument(
        "--archive-prefix",
        default="high-scale/archive",
        help="FOS prefix for High-Scale archives (default: high-scale/archive)",
    )
    parser.add_argument(
        "--fastly-api-key",
        default=None,
        help="Fastly API key used to mint a bucket-scoped FOS key (env fallback: FASTLY_API_KEY)",
    )
    parser.add_argument(
        "--fos-access-key",
        default=None,
        help="Explicit FOS access key ID (env fallback: FOS_ACCESS_KEY_ID or FOS_ACCESS_KEY)",
    )
    parser.add_argument(
        "--fos-secret-key",
        default=None,
        help="Explicit FOS secret access key (env fallback: FOS_SECRET_ACCESS_KEY or FOS_SECRET_KEY)",
    )
    parser.add_argument(
        "--skip-serving-rebuild",
        action="store_true",
        help="Restore config, Postgres metadata, and archive manifests without rebuilding ClickHouse serving tables",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="json_output",
        help="Emit compact JSON summary to stdout",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(list(argv) if argv is not None else None)
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) else 2
        return code

    fastly_api_key = (args.fastly_api_key or os.getenv("FASTLY_API_KEY") or "").strip() or None
    fos_access_key = (
        args.fos_access_key or os.getenv("FOS_ACCESS_KEY_ID") or os.getenv("FOS_ACCESS_KEY") or ""
    ).strip() or None
    fos_secret_key = (
        args.fos_secret_key or os.getenv("FOS_SECRET_ACCESS_KEY") or os.getenv("FOS_SECRET_KEY") or ""
    ).strip() or None

    if bool(fos_access_key) != bool(fos_secret_key):
        print(
            "Error: --fos-access-key and --fos-secret-key must both be provided together.",
            file=sys.stderr,
        )
        return 2
    if not fastly_api_key and not (fos_access_key and fos_secret_key):
        print(
            "Error: provide --fastly-api-key (or FASTLY_API_KEY) or both --fos-access-key and --fos-secret-key.",
            file=sys.stderr,
        )
        return 2

    secrets_to_hide = (fastly_api_key, fos_access_key, fos_secret_key)
    restore_serving_state = not args.skip_serving_rebuild

    try:
        result = restore_from_fos(
            service_id=args.service_id,
            bucket=args.bucket,
            region=args.region,
            prefix=args.prefix,
            archive_prefix=args.archive_prefix,
            fastly_api_key=fastly_api_key,
            fos_access_key=fos_access_key,
            fos_secret_key=fos_secret_key,
            restore_serving_state=restore_serving_state,
        )
    except ValueError as exc:
        safe_msg = scrub_secrets(str(exc), secrets_to_hide)
        print(f"Error: {safe_msg}", file=sys.stderr)
        return 2
    except Exception as exc:
        safe_msg = scrub_secrets(str(exc), secrets_to_hide)
        print(f"Error ({type(exc).__name__}): {safe_msg}", file=sys.stderr)
        return 1

    payload = asdict(result)
    if args.json_output:
        print(json.dumps(payload, sort_keys=True))
    else:
        print(
            f"Restored service {result.service_id}: "
            f"dr_state={result.dr_state_restored}, "
            f"views={result.views_restored}, "
            f"alerts={result.alerts_restored}, "
            f"scoring_labels={result.scoring_labels_restored}, "
            f"audit_logs={result.audit_logs_restored}, "
            f"manifests={result.manifests_registered}"
        )
        for domain, info in result.domains_rebuilt.items():
            print(
                f"  domain={domain}: complete={info.get('full_rebuild_complete')}, "
                f"manifests={info.get('manifests_replayed')}, "
                f"rows={info.get('rows_replayed')}, "
                f"errors={len(info.get('errors') or ())}"
            )

    if restore_serving_state:
        incomplete = [
            domain for domain, info in result.domains_rebuilt.items() if not info.get("full_rebuild_complete", False)
        ]
        if incomplete:
            print(
                f"Error: serving rebuild incomplete for domain(s): {', '.join(sorted(incomplete))}",
                file=sys.stderr,
            )
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

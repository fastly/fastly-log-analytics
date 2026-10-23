"""Unified CLI entrypoint for rebuilding high-scale projections and fact tables from immutable archives."""

from __future__ import annotations

import argparse
import sys

from scripts import (
    backfill_high_scale_origin,
    backfill_high_scale_performance,
    backfill_high_scale_security,
)


def main(argv: list[str] | None = None) -> int:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    probe = argparse.ArgumentParser(add_help=False)
    probe.add_argument(
        "--projection",
        choices=("origin", "cmcd", "rum_vitals", "rum_errors", "performance", "security"),
        default="origin",
    )
    known, remaining = probe.parse_known_args(raw_argv)

    if known.projection == "performance":
        sys.argv = [sys.argv[0], *remaining]
        return backfill_high_scale_performance.main()
    if known.projection == "security":
        sys.argv = [sys.argv[0], *remaining]
        return backfill_high_scale_security.main()

    sys.argv = [sys.argv[0], "--projection", known.projection, *remaining]
    return backfill_high_scale_origin.main()


if __name__ == "__main__":
    raise SystemExit(main())

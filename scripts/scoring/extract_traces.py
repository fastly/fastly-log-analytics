#!/usr/bin/env -S uv run python
"""CLI: dump sessionized request traces from a local DuckDB to JSONL.

Used to generate fixture data for the session-scoring training pipeline and
scorer unit tests. Requires the local backend to be stopped (DuckDB file lock).

Usage:

    ./scripts/scoring/extract_traces.py \\
        --service-id <your-fastly-service-id> \\
        --start 2026-05-15T00:00:00 \\
        --end   2026-05-16T00:00:00 \\
        --limit 100000 \\
        --out tests/fixtures/scoring/traces_2026-05-15.jsonl

Defaults to the single service in configs/ and the most recent 24 hours
covered by local data.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))


def _load_dotenv() -> None:
    env_file = ROOT / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val


_load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("extract_traces")


def _discover_service_id() -> str:
    configs = list((ROOT / "configs").glob("*.json"))
    if len(configs) != 1:
        raise SystemExit(f"expected exactly 1 config in configs/, found {len(configs)}. Pass --service-id explicitly.")
    return configs[0].stem


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--service-id", default=None)
    parser.add_argument("--start", help="ISO timestamp (UTC) for lower bound (inclusive).")
    parser.add_argument("--end", help="ISO timestamp (UTC) for upper bound (exclusive).")
    parser.add_argument("--limit", type=int, default=None, help="Cap on rows fetched (debug aid).")
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output JSONL path. Defaults to tests/fixtures/scoring/traces.jsonl",
    )
    parser.add_argument(
        "--gap-seconds",
        type=int,
        default=None,
        help="Session boundary gap (default 1800).",
    )
    args = parser.parse_args()

    service_id = args.service_id or _discover_service_id()
    out = args.out or (ROOT / "tests" / "fixtures" / "scoring" / f"traces_{service_id}.jsonl")

    start = datetime.fromisoformat(args.start).replace(tzinfo=UTC) if args.start else None
    end = datetime.fromisoformat(args.end).replace(tzinfo=UTC) if args.end else None

    from backend.core.duckdb import get_connection, get_source_for_service
    from backend.scoring import fixtures

    src = get_source_for_service(service_id)
    if src is None:
        log.error("service config not found for %s", service_id)
        return 1

    log.info("opening read-only DuckDB connection for %s …", service_id)
    con = get_connection(source=src, read_only=True)

    kwargs: dict = {}
    if args.gap_seconds is not None:
        kwargs["gap_seconds"] = args.gap_seconds

    sessions = fixtures.extract_traces(
        con,
        service_id=service_id,
        start=start,
        end=end,
        limit=args.limit,
        **kwargs,
    )

    log.info("writing JSONL → %s", out)
    n = fixtures.write_jsonl_path(sessions, out)
    log.info("done: wrote %d sessions to %s", n, out)
    return 0


if __name__ == "__main__":
    sys.exit(main())

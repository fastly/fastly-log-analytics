#!/usr/bin/env python3
"""Refresh the Fastly edge CIDR list inside the repo-root Caddyfile.

Fastly periodically adds new edge POPs. The Caddyfile's ``@from_fastly_v4``
matcher gates the ``X-Forwarded-For`` rewrite on the TCP peer falling inside
Fastly's published v4 ranges, so a stale list silently classifies traffic
from new POPs as direct (untrusted) until somebody refreshes the CIDRs and
reloads Caddy.

Usage:

* **Manual one-shot:** ``uv run python scripts/refresh_fastly_cidrs.py``
  fetches the current list, rewrites the matcher block in-place, and writes
  the file. Run from the repo root.
* **CI check:** ``uv run python scripts/refresh_fastly_cidrs.py --check``
  exits 1 if the Caddyfile would change. Wire into a weekly cron / GitHub
  Action so a stale list shows up as a failed job instead of a silent
  security gap.
* **Preview:** ``--dry-run`` prints a unified diff and exits 0 without
  touching the file.

Only the v4 list is rewritten today — the Caddyfile matcher is v4-only.
Adding a v6 sibling block would be a follow-up.

This script is intentionally stdlib + httpx; it does not pull in the
backend package so it can run in a thin tooling venv.
"""

from __future__ import annotations

import argparse
import difflib
import re
import sys
from pathlib import Path

import httpx

FASTLY_PUBLIC_IP_LIST = "https://api.fastly.com/public-ip-list"

# Matches the entire ``@from_fastly_v4 { remote_ip ... }`` block. The
# remote_ip line is what we rewrite; the surrounding braces + matcher name
# are preserved verbatim so the rest of the Caddyfile stays byte-for-byte
# identical.
#
# Group 1 captures the leading indentation + ``remote_ip`` token so we
# preserve tabs vs spaces exactly as authored.
MATCHER_BLOCK_RE = re.compile(
    r"(@from_fastly_v4\s*\{\s*\n)"  # opening line (kept verbatim)
    r"(\s*remote_ip)[^\n]*\n"  # the line we rewrite (indent captured)
    r"(\s*\}\s*\n)",  # closing brace line (kept verbatim)
)


def fetch_fastly_cidrs(client: httpx.Client | None = None) -> list[str]:
    """Fetch and return the current Fastly v4 edge CIDR list, sorted.

    Sorting is by (first-octet, network-size) so the output is stable across
    runs — Fastly's API returns the list in insertion order, which would
    cause spurious diffs on every refresh.
    """
    owns_client = client is None
    if owns_client:
        client = httpx.Client(timeout=10.0)
    try:
        resp = client.get(FASTLY_PUBLIC_IP_LIST)
        resp.raise_for_status()
        payload = resp.json()
    finally:
        if owns_client:
            client.close()

    addresses = payload.get("addresses") or []
    if not addresses:
        raise RuntimeError(
            "Fastly public-ip-list returned no v4 addresses — refusing to "
            "overwrite the Caddyfile with an empty allow-list."
        )
    return sort_cidrs(addresses)


def sort_cidrs(cidrs: list[str]) -> list[str]:
    """Stable sort that mirrors how a human would read the list."""

    def key(cidr: str) -> tuple[tuple[int, ...], int]:
        addr, _, prefix = cidr.partition("/")
        octets = tuple(int(o) for o in addr.split("."))
        return (octets, int(prefix) if prefix else 32)

    return sorted(cidrs, key=key)


def rewrite_caddyfile(original: str, cidrs: list[str]) -> str:
    """Return ``original`` with the ``@from_fastly_v4`` remote_ip line refreshed.

    Raises ``RuntimeError`` if the matcher block isn't present — failing
    loud is better than silently no-op'ing if somebody renames the matcher.
    """
    match = MATCHER_BLOCK_RE.search(original)
    if not match:
        raise RuntimeError(
            "Could not locate the @from_fastly_v4 { remote_ip ... } block in "
            "the Caddyfile. Did the matcher name change?"
        )

    opening = match.group(1)  # "@from_fastly_v4 {\n"
    indent_prefix = match.group(2)  # e.g. "\t\tremote_ip" — preserves tabs
    closing = match.group(3)  # "\t}\n"
    replacement = f"{opening}{indent_prefix} {' '.join(cidrs)}\n{closing}"
    return original[: match.start()] + replacement + original[match.end() :]


def _diff(before: str, after: str, path: str) -> str:
    return "".join(
        difflib.unified_diff(
            before.splitlines(keepends=True),
            after.splitlines(keepends=True),
            fromfile=f"{path} (current)",
            tofile=f"{path} (refreshed)",
        )
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument(
        "--caddyfile",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "Caddyfile",
        help="Path to the Caddyfile (defaults to repo-root Caddyfile).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print a unified diff and exit 0 without writing.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit 1 if the file would change. For CI use.",
    )
    args = parser.parse_args(argv)

    caddyfile_path: Path = args.caddyfile
    original = caddyfile_path.read_text()

    cidrs = fetch_fastly_cidrs()
    updated = rewrite_caddyfile(original, cidrs)

    if updated == original:
        print(f"No changes — Caddyfile already lists {len(cidrs)} current Fastly v4 CIDRs.")
        return 0

    diff = _diff(original, updated, str(caddyfile_path))
    if args.dry_run:
        print(diff)
        return 0

    if args.check:
        print(diff)
        print("Caddyfile is stale — run without --check to refresh.", file=sys.stderr)
        return 1

    caddyfile_path.write_text(updated)
    print(f"Refreshed Caddyfile with {len(cidrs)} Fastly v4 CIDRs.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

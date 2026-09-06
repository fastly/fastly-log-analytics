"""Browser/OS fingerprint hashing for analyst session cookies.

Section #18 (security): hash a narrowed signature of browser family +
major version + OS family, NOT the full User-Agent. Chrome UA-Reduction
updates every ~4 weeks would otherwise boot every analyst, swamping the
audit log with false positives. The narrowed signature still detects a
cross-browser / cross-OS cookie theft.
"""

from __future__ import annotations

import hashlib
import re

_UA_RE = re.compile(r"(Chrome|Firefox|Safari|Edge|OPR)/(\d+)")
_OS_RE = re.compile(r"(Macintosh|Mac OS X|Windows|Linux|X11|iPhone|iPad|Android)")


def compute_fingerprint(headers: dict[str, str]) -> str:
    """Narrowed SHA-256 over browser family + major version + OS family."""
    ua = headers.get("user-agent", "") or headers.get("User-Agent", "") or ""
    browser_match = _UA_RE.search(ua)
    os_match = _OS_RE.search(ua)
    parts = [
        browser_match.group(1) if browser_match else "unknown-browser",
        browser_match.group(2) if browser_match else "0",
        os_match.group(1) if os_match else "unknown-os",
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()

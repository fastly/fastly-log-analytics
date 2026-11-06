# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""Thin wrapper delegating Fastly API contract recording to `tests.support.recording_transport`."""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tests.support.recording_transport import (  # noqa: E402
    RecordingTransport,
    _redact,
    _token,
    fetch_contract,
    install_identifiers,
    main,
    record,
    record_install,
)

__all__ = [
    "RecordingTransport",
    "_redact",
    "_token",
    "fetch_contract",
    "install_identifiers",
    "main",
    "record",
    "record_install",
]

if __name__ == "__main__":
    main()

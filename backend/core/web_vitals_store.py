"""Opt-in JSONL sink for client Web Vitals samples.

Disabled by default. When ``WEB_VITALS_COLLECT`` is truthy the
``/api/web-vitals`` endpoint appends one JSON line per metric to
``data/system/web_vitals.jsonl``; ``scripts/analyze_web_vitals.py`` reads
that file to produce a per-route / per-metric performance report a human
or coding agent can act on.

Why JSONL instead of a SQLite table (cf. ``metric_snapshots.py``): this is
an opt-in, short-lived dev artifact. The loop is gather → analyze →
delete: turn collection on, let real-user samples accumulate, run the
analyzer (which can purge the file when done), turn collection off. No
schema, no migration, no retention cron — the file is trivially
deletable, and ``data/`` is gitignored so it never lands in a commit.

Size safety: even though the file is meant to be purged after each
analysis, an operator who leaves collection on indefinitely shouldn't be
able to fill the disk. When the active file reaches ``WEB_VITALS_MAX_MB``
(default 200) it's rotated to a single ``.1`` backup (replacing any prior
backup) and a fresh file starts — the same size-cap-plus-rotation model as
the docker ``max-size``/``max-file`` logging in docker-compose.prod.yml.
At most two segments are kept, so peak on-disk is ~2× the cap; the
analyzer reads both and ``--purge`` removes both.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path
from typing import Any

logger = logging.getLogger("backend.web_vitals")

# Single source of truth for the sink location. Relative to the process
# CWD (repo root in dev, ``/app`` in the container) — the same convention
# ``metric_snapshots.py`` uses for ``data/system``. The analyzer script
# mirrors this path (see scripts/analyze_web_vitals.py).
LOG_PATH = Path("data") / "system" / "web_vitals.jsonl"

# Default active-file size cap in MB, overridable via WEB_VITALS_MAX_MB.
DEFAULT_MAX_MB = 200

# Serializes the append (and the rotation it may trigger) so concurrent
# requests can't interleave a partial line or race the rename.
_write_lock = threading.Lock()


def collection_enabled() -> bool:
    """True when ``WEB_VITALS_COLLECT`` is set to a truthy value.

    Default off: collection is opt-in, so neither dev nor prod writes
    anything (and the frontend, which mirrors this flag via
    ``/api/bootstrap``, sends nothing) until it's explicitly enabled.
    """
    return os.environ.get("WEB_VITALS_COLLECT", "0").strip().lower() in ("1", "true", "yes", "on")


def rotated_path(path: Path | None = None) -> Path:
    """The single rotated-backup path for ``path`` (``…jsonl`` -> ``…jsonl.1``)."""
    p = path or LOG_PATH
    return p.with_name(p.name + ".1")


def _max_bytes() -> int:
    """Active-file size cap in bytes from ``WEB_VITALS_MAX_MB``.

    Defaults to ``DEFAULT_MAX_MB``. A value <= 0 (or unparseable) disables
    the cap entirely (unbounded growth — only for deliberate use).
    """
    raw = os.environ.get("WEB_VITALS_MAX_MB")
    if raw is None or raw.strip() == "":
        mb: float = DEFAULT_MAX_MB
    else:
        try:
            mb = float(raw)
        except ValueError:
            mb = DEFAULT_MAX_MB
    return int(mb * 1024 * 1024) if mb > 0 else 0


def _rotate_if_needed() -> None:
    """Rotate the active file to a single ``.1`` backup once it hits the cap.

    Caller must hold ``_write_lock``. Keeps at most one rotated segment so
    the most recent window is always retained (rather than freezing on the
    oldest data, which a plain drop-when-full would do).
    """
    cap = _max_bytes()
    if cap <= 0:
        return
    try:
        size = LOG_PATH.stat().st_size
    except FileNotFoundError:
        return
    if size < cap:
        return
    backup = rotated_path()
    try:
        backup.unlink(missing_ok=True)
        LOG_PATH.replace(backup)  # atomic rename within the same dir
    except OSError as exc:  # pragma: no cover - defensive
        logger.warning("[web_vitals] log rotation failed: %s", exc)


def append_sample(sample: dict[str, Any]) -> None:
    """Append one Web Vitals sample as a single JSON line. Best-effort.

    Telemetry must never break the request that produced it, so any IO
    error is logged and swallowed rather than propagated.
    """
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(sample, separators=(",", ":")) + "\n"
        with _write_lock:
            _rotate_if_needed()
            with LOG_PATH.open("a", encoding="utf-8") as fh:
                fh.write(line)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("[web_vitals] sample append failed: %s", exc)

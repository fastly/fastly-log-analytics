"""Periodic serving-volume sampling for the high-scale disk guard.

Reads the serving volume's real capacity from ClickHouse (``system.disks``),
runs :func:`backend.high_scale.disk_guard.evaluate_disk_state`, and keeps the
latest result for the health snapshot. It only evaluates and surfaces the
mode: no ingest, compaction, replay or deletion is suspended (see
docs/runbooks/high-scale-helper-wiring.md).
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Literal, Protocol

import structlog

logger = structlog.get_logger(__name__)

DiskMode = Literal["normal", "warning", "protective", "emergency"]

_RESERVE_FRACTION = 0.20
_PROTECTIVE_FREE_FRACTION = 0.15
_EMERGENCY_FREE_FRACTION = 0.10


@dataclass(frozen=True)
class VolumeStats:
    total_bytes: int
    free_bytes: int
    # Size of one full compaction/replay batch — the reserve floor is the
    # GREATER of the 20% fraction or this, so a large batch on a small
    # volume can never be scheduled into a reserve too thin to hold it.
    one_batch_bytes: int = 0

    def __post_init__(self) -> None:
        if self.total_bytes <= 0:
            raise ValueError("total_bytes must be positive")
        if self.free_bytes < 0:
            raise ValueError("free_bytes must be non-negative")
        if self.free_bytes > self.total_bytes:
            raise ValueError("free_bytes cannot exceed total_bytes")
        if self.one_batch_bytes < 0:
            raise ValueError("one_batch_bytes must be non-negative")


@dataclass(frozen=True)
class DiskState:
    mode: DiskMode
    free_fraction: float
    reserve_bytes: int
    reasoning: str


def evaluate_disk_state(volume: VolumeStats) -> DiskState:
    free_fraction = volume.free_bytes / volume.total_bytes
    reserve_bytes = max(int(volume.total_bytes * _RESERVE_FRACTION), volume.one_batch_bytes)

    if free_fraction <= _EMERGENCY_FREE_FRACTION:
        mode: DiskMode = "emergency"
        reasoning = f"free space {free_fraction:.1%} at or below the {_EMERGENCY_FREE_FRACTION:.0%} emergency floor"
    elif free_fraction <= _PROTECTIVE_FREE_FRACTION:
        mode = "protective"
        reasoning = f"free space {free_fraction:.1%} at or below the {_PROTECTIVE_FREE_FRACTION:.0%} protective floor"
    elif volume.free_bytes <= reserve_bytes:
        mode = "warning"
        reasoning = f"free space {volume.free_bytes} bytes at or below the {reserve_bytes}-byte protected reserve"
    else:
        mode = "normal"
        reasoning = f"free space {free_fraction:.1%} clears the {reserve_bytes}-byte protected reserve"

    return DiskState(mode=mode, free_fraction=free_fraction, reserve_bytes=reserve_bytes, reasoning=reasoning)


MODE_LEVELS: dict[DiskMode, int] = {"normal": 0, "warning": 1, "protective": 2, "emergency": 3}


class StorageProbe(Protocol):
    def storage_health(self) -> dict: ...


@dataclass(frozen=True)
class DiskSnapshot:
    state: DiskState
    total_bytes: int
    free_bytes: int
    sampled_at: float


_lock = threading.Lock()
_latest: DiskSnapshot | None = None


def _one_batch_bytes() -> int:
    raw = os.getenv("HIGH_SCALE_DISK_ONE_BATCH_BYTES", "").strip()
    if not raw:
        return 0
    try:
        return max(0, int(raw))
    except ValueError:
        logger.warning("high_scale.disk_batch_bytes_invalid", value=raw)
        return 0


def sample_disk_state(probe: StorageProbe, *, now: float | None = None) -> DiskSnapshot:
    """Sample the serving volume once and record the evaluated mode.

    Raises whatever the probe raises; callers decide how to degrade.
    """

    health = probe.storage_health()
    volume = VolumeStats(
        total_bytes=int(health["disk_total"]),
        free_bytes=int(health["disk_free"]),
        one_batch_bytes=_one_batch_bytes(),
    )
    snapshot = DiskSnapshot(
        state=evaluate_disk_state(volume),
        total_bytes=volume.total_bytes,
        free_bytes=volume.free_bytes,
        sampled_at=time.time() if now is None else now,
    )
    _record(snapshot)
    return snapshot


def _record(snapshot: DiskSnapshot) -> None:
    global _latest
    with _lock:
        previous, _latest = _latest, snapshot
    if previous is None or previous.state.mode != snapshot.state.mode:
        log = logger.warning if MODE_LEVELS[snapshot.state.mode] >= MODE_LEVELS["protective"] else logger.info
        log(
            "high_scale.disk_mode_changed",
            previous_mode=previous.state.mode if previous else None,
            mode=snapshot.state.mode,
            free_fraction=round(snapshot.state.free_fraction, 4),
            reserve_bytes=snapshot.state.reserve_bytes,
            reasoning=snapshot.state.reasoning,
        )


def latest_disk_snapshot() -> DiskSnapshot | None:
    with _lock:
        return _latest


def disk_snapshot_payload(now: float | None = None) -> dict[str, Any] | None:
    """Health-snapshot projection of the latest sample, or None before the first."""

    snapshot = latest_disk_snapshot()
    if snapshot is None:
        return None
    current = time.time() if now is None else now
    return {
        "mode": snapshot.state.mode,
        "free_fraction": snapshot.state.free_fraction,
        "reserve_bytes": snapshot.state.reserve_bytes,
        "total_bytes": snapshot.total_bytes,
        "free_bytes": snapshot.free_bytes,
        "reasoning": snapshot.state.reasoning,
        "sample_age_s": max(0.0, current - snapshot.sampled_at),
    }


def reset_for_tests() -> None:
    global _latest
    with _lock:
        _latest = None

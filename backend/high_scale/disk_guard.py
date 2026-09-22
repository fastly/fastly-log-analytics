"""Disk-pressure state machine for the high-scale local serving volume.

Pure evaluation only. The caller (a periodic sampler, mirroring the
standard-mode SystemHealthCard sampler) is responsible for reading real
volume stats and for actually disabling optional work at each mode —
this module only decides which mode applies.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

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

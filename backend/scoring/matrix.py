"""Transition-matrix builder.

Reads sessionized JSONL traces (output of ``backend.scoring.fixtures``),
normalizes each URL to its canonical route, counts pairwise (prev →
current) transitions across all sessions, and emits a ``matrix.json``
that ``backend.scoring.scorer.score_layer2`` consumes directly.

Output schema (matches what the scorer expects):

    {
      "version": "2026-06-01-a",       # YYYY-MM-DD-<letter> per training run
      "built_at": "2026-06-01T18:30:00+00:00",
      "vocab_size": 250,                # |V| for Laplace smoothing denom
      "session_count": 12483,
      "transition_count": 137501,
      "counts":       {prev: {curr: n}},     # raw transition counts
      "row_totals":   {prev: total_out_count},
      "categories":   {route: "category"},   # for L2 category backoff
      "anchors":      []                     # populated by pagerank module
    }

Training-time bot filter (research doc §1.3 + §9.2): we drop the obvious
"naive scraper" cases (single-event sessions, sessions whose mean dwell
falls below the human-floor) BEFORE building the matrix, so they don't
poison the learned distribution. We do NOT use confirmed-bad labels here
— that path is explicitly avoided in the doc to block label-poisoning
attacks; labels are only used by the ROC-AUC evaluator on the
already-trained matrix."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any

from backend.scoring.normalize import Route, normalize

logger = logging.getLogger(__name__)

# Tuning knobs for the training-time bot filter. Conservative defaults — we
# err on the side of letting borderline sessions in (Laplace smoothing
# absorbs the noise), only filtering the truly degenerate cases.
MIN_EVENTS_PER_SESSION = 2  # < this is just a 1-shot probe, not a session
MIN_MEAN_DWELL_S = 0.2  # mirrors L1's impossibly-fast threshold


@dataclass
class MatrixStats:
    """Counters surfaced for the training CLI's summary log."""

    sessions_in: int = 0
    sessions_dropped_short: int = 0
    sessions_dropped_fast: int = 0
    sessions_kept: int = 0
    transitions: int = 0
    routes_seen: int = 0


@dataclass
class TransitionMatrix:
    counts: dict[str, dict[str, int]] = field(default_factory=dict)
    row_totals: dict[str, int] = field(default_factory=dict)
    categories: dict[str, str] = field(default_factory=dict)
    vocab: set[str] = field(default_factory=set)
    session_count: int = 0
    transition_count: int = 0
    anchors: list[str] = field(default_factory=list)

    def add_transition(self, prev: Route, curr: Route) -> None:
        self.counts.setdefault(prev.path, {})
        self.counts[prev.path][curr.path] = self.counts[prev.path].get(curr.path, 0) + 1
        self.row_totals[prev.path] = self.row_totals.get(prev.path, 0) + 1
        self.transition_count += 1
        # Categories: first sighting wins. Routes are deterministic →
        # category, so later overrides would be no-ops anyway.
        if prev.path not in self.categories:
            self.categories[prev.path] = prev.category
        if curr.path not in self.categories:
            self.categories[curr.path] = curr.category
        self.vocab.add(prev.path)
        self.vocab.add(curr.path)

    def to_json_dict(self, version: str) -> dict[str, Any]:
        return {
            "version": version,
            "built_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "vocab_size": len(self.vocab),
            "session_count": self.session_count,
            "transition_count": self.transition_count,
            "counts": self.counts,
            "row_totals": self.row_totals,
            "categories": self.categories,
            "anchors": self.anchors,
        }


def _read_jsonl(path: Path) -> Iterator[dict]:
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _session_mean_dwell_seconds(events: list[dict]) -> float:
    """Mean of inter-event time deltas, in seconds.

    Reads timestamps from the JSONL trace format. Returns 0.0 for sessions
    with < 2 events (no delta to measure)."""
    if len(events) < 2:
        return 0.0
    first = datetime.fromisoformat(events[0]["ts"])
    last = datetime.fromisoformat(events[-1]["ts"])
    span = (last - first).total_seconds()
    # n-1 deltas across n events.
    return span / (len(events) - 1)


def build_matrix(
    sessions: Iterable[dict],
    *,
    min_events: int = MIN_EVENTS_PER_SESSION,
    min_mean_dwell_s: float = MIN_MEAN_DWELL_S,
) -> tuple[TransitionMatrix, MatrixStats]:
    """Build a transition matrix from sessionized traces.

    ``sessions`` is an iterable of JSONL-shaped dicts (as emitted by
    ``backend.scoring.fixtures.write_jsonl``). Streams through the input
    in one pass — no buffering.

    Returns the matrix plus a MatrixStats summary for logging."""
    matrix = TransitionMatrix()
    stats = MatrixStats()

    for session in sessions:
        stats.sessions_in += 1
        events = session.get("events", [])

        if len(events) < min_events:
            stats.sessions_dropped_short += 1
            continue

        mean_dwell = _session_mean_dwell_seconds(events)
        if mean_dwell < min_mean_dwell_s:
            stats.sessions_dropped_fast += 1
            continue

        # Walk consecutive pairs (sliding window of 2).
        prev_route: Route | None = None
        for ev in events:
            curr = normalize(ev.get("url", "/"))
            if prev_route is not None:
                matrix.add_transition(prev_route, curr)
            prev_route = curr
        stats.sessions_kept += 1
        matrix.session_count += 1

    stats.transitions = matrix.transition_count
    stats.routes_seen = len(matrix.vocab)
    return matrix, stats


def build_matrix_from_jsonl(path: Path, **kwargs) -> tuple[TransitionMatrix, MatrixStats]:
    return build_matrix(_read_jsonl(path), **kwargs)


# ── FSM1 binary encoding (KV-store payload) ──────────────────────────────────
#
# The Rust scorer (compute/scorer/src/matrix.rs) reads the matrix from the KV
# Store. JSON (1.85 MB) is slow to fetch + parse into nested HashMaps on a COLD
# Wasm instance — and Fastly Compute is instance-per-request, so every L2
# request is cold. We encode a compact, sparse-CSR binary instead: ~5.6× smaller
# and decoded in one pass (no per-entry allocation). The on-disk / FOS matrix
# stays JSON (Python eval + diffing); only the KV payload is FSM1.
#
# Only the fields the Rust scorer actually reads are encoded: version,
# vocab_size, counts, row_totals. `categories` / `anchors` are NOT read by the
# scorer and are dropped from the KV payload (they remain in the JSON).
#
# Format (little-endian). MUST stay byte-for-byte in lockstep with the Rust
# decoder `parse_fsm1` in compute/scorer/src/matrix.rs — the cross-language
# fixture in tests/scoring/test_matrix.py + matrix.rs pins the exact bytes.
#
#   magic[4]     = b"FSM1"
#   fmt_ver  u8  = 1
#   vocab_size   u32   (Laplace |V| from the trained matrix)
#   n_routes     u32   (size of the string table)
#   ver_len  u16 + version[ver_len]   UTF-8
#   str_off[n_routes+1]  u32 each (cumulative byte offsets into str_blob)
#   str_blob             concatenated UTF-8 routes, SORTED ascending by raw
#                        bytes → route id == index → binary-searchable
#   row_total[n_routes]  uvarint u64 (indexed by route id; 0 if not a prev)
#   row_off[n_routes+1]  uvarint u32 (cumulative pair index; CSR row pointers)
#   pairs[total_pairs]   per row, col_id ASCENDING: uvarint(col_id) uvarint(count)

FSM1_MAGIC = b"FSM1"
FSM1_VERSION = 1


def _uvarint(value: int) -> bytes:
    """LEB128 unsigned varint. Mirrors the Rust reader's `uvarint`."""
    if value < 0:
        raise ValueError(f"uvarint cannot encode negative value {value}")
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def serialize_kv(matrix: dict[str, Any]) -> bytes:
    """Encode a matrix JSON dict into the FSM1 binary KV payload.

    Pure function of the dict's ``version`` / ``vocab_size`` / ``counts`` /
    ``row_totals`` — so it works identically for train output, FOS-fetched, and
    restored matrices. Routes are the union of every prev, every dest, and every
    row_total key, sorted by raw UTF-8 bytes (== code-point order == the Rust
    `<[u8]>` comparison, since UTF-8 preserves ordering).
    """
    version = str(matrix.get("version", ""))
    vocab_size = int(matrix.get("vocab_size", 0))
    counts: dict[str, dict[str, int]] = matrix.get("counts", {}) or {}
    row_totals: dict[str, int] = matrix.get("row_totals", {}) or {}

    routes: set[str] = set(counts.keys()) | set(row_totals.keys())
    for row in counts.values():
        routes.update(row.keys())
    sorted_routes = sorted(routes, key=lambda s: s.encode())
    rid = {r: i for i, r in enumerate(sorted_routes)}
    n_routes = len(sorted_routes)

    out = bytearray()
    out += FSM1_MAGIC
    out.append(FSM1_VERSION)
    out += vocab_size.to_bytes(4, "little")
    out += n_routes.to_bytes(4, "little")
    ver_bytes = version.encode()
    out += len(ver_bytes).to_bytes(2, "little")
    out += ver_bytes

    # String table: fixed-width u32 offsets (binary-searchable) + blob.
    blob = bytearray()
    offsets = [0]
    for r in sorted_routes:
        blob += r.encode()
        offsets.append(len(blob))
    for off in offsets:  # n_routes + 1 entries
        out += off.to_bytes(4, "little")
    out += blob

    # Row totals, indexed by route id (uvarint u64; 0 when route is curr-only).
    for r in sorted_routes:
        out += _uvarint(int(row_totals.get(r, 0)))

    # CSR: row offsets (uvarint, cumulative) then ascending (col_id, count) pairs.
    row_off = [0]
    pair_bytes = bytearray()
    for r in sorted_routes:
        row = counts.get(r, {})
        cols = sorted((rid[c], int(cnt)) for c, cnt in row.items())
        for col_id, cnt in cols:
            pair_bytes += _uvarint(col_id)
            pair_bytes += _uvarint(cnt)
        row_off.append(row_off[-1] + len(cols))
    for off in row_off:  # n_routes + 1 entries
        out += _uvarint(off)
    out += pair_bytes

    return bytes(out)


def write_matrix(matrix: TransitionMatrix, version: str, out: IO[str]) -> None:
    """Serialize the matrix as canonical JSON. Use sort_keys so two runs
    on the same input produce byte-identical output — important for
    diffing matrix versions in CI and for the ``fastly compute deploy``
    package hash."""
    json.dump(matrix.to_json_dict(version), out, sort_keys=True, separators=(",", ":"))


def write_matrix_path(matrix: TransitionMatrix, version: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        write_matrix(matrix, version, f)


def default_version() -> str:
    """Auto-assigned matrix version, ``YYYY-MM-DD-HHMMSS`` (UTC, second
    resolution).

    EC-06: the old ``YYYY-MM-DD-a`` form had a constant trailing ``a`` and no
    caller ever bumped it, so 3+ same-day retrains all minted the *same* version
    string — the history archive (keyed on the version string) overwrote prior
    same-day snapshots and two distinct matrices reported an identical
    ``X-Edge-Matrix-Version``. A sub-day time component makes the auto version
    strictly monotonic within a day (operator retrains are minutes-to-hours
    apart, so second resolution is ample). Lexicographically sortable and fixed
    width, so it orders correctly; legacy ``-a`` versions remain valid (the
    date prefix dominates cross-day ordering). Operators can still pass an
    explicit ``--version`` to override entirely."""
    return datetime.now(UTC).strftime("%Y-%m-%d-%H%M%S")

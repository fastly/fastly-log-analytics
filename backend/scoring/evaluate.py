"""ROC-AUC evaluation of a trained matrix against labeled negatives, plus
transition-matrix builder and FSM1 binary KV serializer.

Per research doc §9.2 we use labels for EVALUATION ONLY — never to
auto-zero transitions in the trained matrix itself, because letting a
compromised verifier account submit "bad sessions" would create a
matrix-poisoning vector. The evaluator answers: "if I apply the L2
scorer to these known-malicious sessions and these known-good sessions,
does the AUC clear the quality bar?"
"""

from __future__ import annotations

import logging
import sys as _sys
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Final

from backend.scoring.features import Route, normalize, score_layer2

logger = logging.getLogger(__name__)

# ── Transition-Matrix Builder & FSM1 Encoding (folded from backend.scoring.matrix) ──

MIN_EVENTS_PER_SESSION = 2
MIN_MEAN_DWELL_S = 0.2


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


def _session_mean_dwell_seconds(events: list[dict]) -> float:
    """Mean of inter-event time deltas, in seconds."""
    if len(events) < 2:
        return 0.0
    first = datetime.fromisoformat(events[0]["ts"])
    last = datetime.fromisoformat(events[-1]["ts"])
    return (last - first).total_seconds() / (len(events) - 1)


def build_matrix(
    sessions: Iterable[dict],
    *,
    min_events: int = MIN_EVENTS_PER_SESSION,
    min_mean_dwell_s: float = MIN_MEAN_DWELL_S,
) -> tuple[TransitionMatrix, MatrixStats]:
    """Build a transition matrix from sessionized traces."""
    matrix = TransitionMatrix()
    stats = MatrixStats()

    for session in sessions:
        stats.sessions_in += 1
        events = session.get("events", [])

        if len(events) < min_events:
            stats.sessions_dropped_short += 1
            continue

        if _session_mean_dwell_seconds(events) < min_mean_dwell_s:
            stats.sessions_dropped_fast += 1
            continue

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
    """Encode a matrix JSON dict into the FSM1 binary KV payload."""
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

    blob = bytearray()
    offsets = [0]
    for r in sorted_routes:
        blob += r.encode()
        offsets.append(len(blob))
    for off in offsets:
        out += off.to_bytes(4, "little")
    out += blob

    for r in sorted_routes:
        out += _uvarint(int(row_totals.get(r, 0)))

    row_off = [0]
    pair_bytes = bytearray()
    for r in sorted_routes:
        row = counts.get(r, {})
        cols = sorted((rid[c], int(cnt)) for c, cnt in row.items())
        for col_id, cnt in cols:
            pair_bytes += _uvarint(col_id)
            pair_bytes += _uvarint(cnt)
        row_off.append(row_off[-1] + len(cols))
    for off in row_off:
        out += _uvarint(off)
    out += pair_bytes

    return bytes(out)


def default_version() -> str:
    """Auto-assigned matrix version, ``YYYY-MM-DD-HHMMSS`` (UTC, second resolution)."""
    return datetime.now(UTC).strftime("%Y-%m-%d-%H%M%S")


# ── ROC-AUC Evaluation ────────────────────────────────────────────────────────

DEFAULT_MIN_AUC: Final[float] = 0.85


@dataclass
class EvaluatedSession:
    session_id: str
    label: str  # "good" | "bad"
    l2_score: int  # 0-100 from the L2 scorer
    transition_count: int


@dataclass
class EvaluationResult:
    auc: float
    pass_threshold: float
    passed: bool
    n_good: int
    n_bad: int
    per_session: list[EvaluatedSession] = field(default_factory=list)

    def summary(self) -> str:
        return (
            f"AUC={self.auc:.3f} (threshold {self.pass_threshold:.2f}) — "
            f"{'PASS' if self.passed else 'FAIL'} "
            f"(n_good={self.n_good}, n_bad={self.n_bad})"
        )


def _session_l2_score(session: dict, matrix: dict) -> tuple[int, int]:
    """Compute the maximum L2 score across all transitions in a session."""
    events = session.get("events", [])
    if len(events) < 2:
        return 0, 0
    prev = normalize(events[0].get("url", "/"))
    max_score = 0
    n_trans = 0
    for ev in events[1:]:
        curr = normalize(ev.get("url", "/"))
        score, _, _ = score_layer2(matrix, prev, None, curr)
        max_score = max(max_score, score)
        prev = curr
        n_trans += 1
    return max_score, n_trans


def _compute_auc(scores: list[tuple[int, str]]) -> float:
    """Area under the ROC curve via the Mann-Whitney U formulation."""
    good_scores = [s for s, lbl in scores if lbl == "good"]
    bad_scores = [s for s, lbl in scores if lbl == "bad"]
    n_good = len(good_scores)
    n_bad = len(bad_scores)
    if n_good == 0 or n_bad == 0:
        return 0.5

    wins = 0.0
    for g in good_scores:
        for b in bad_scores:
            if g < b:
                wins += 1.0
            elif g == b:
                wins += 0.5
    return wins / (n_good * n_bad)


def _evaluate_sessions_with_extractor(
    labeled_sessions: Iterable[tuple[dict, str]],
    score_extractor: Any,
    *,
    min_auc: float = DEFAULT_MIN_AUC,
) -> EvaluationResult:
    per_session: list[EvaluatedSession] = []
    scores_for_auc: list[tuple[int, str]] = []
    n_good = n_bad = 0

    for session, label in labeled_sessions:
        if label not in ("good", "bad", "neutral"):
            raise ValueError(f"unexpected label {label!r}; want 'good', 'bad', or 'neutral'")
        extracted = score_extractor(session)
        if extracted is None:
            continue
        score, n_trans = extracted
        per_session.append(
            EvaluatedSession(
                session_id=session.get("session_id", "?"),
                label=label,
                l2_score=score,
                transition_count=n_trans,
            )
        )
        if label == "neutral":
            continue
        scores_for_auc.append((score, label))
        if label == "good":
            n_good += 1
        else:
            n_bad += 1

    auc = _compute_auc(scores_for_auc)
    return EvaluationResult(
        auc=auc,
        pass_threshold=min_auc,
        passed=auc >= min_auc,
        n_good=n_good,
        n_bad=n_bad,
        per_session=per_session,
    )


def evaluate(
    matrix: dict,
    labeled_sessions: Iterable[tuple[dict, str]],
    *,
    min_auc: float = DEFAULT_MIN_AUC,
) -> EvaluationResult:
    """Score every labeled session against ``matrix`` and report AUC."""
    return _evaluate_sessions_with_extractor(
        labeled_sessions,
        lambda session: _session_l2_score(session, matrix),
        min_auc=min_auc,
    )


def evaluate_from_persisted_scores(
    labeled_sessions: Iterable[tuple[dict, str]],
    *,
    min_auc: float = DEFAULT_MIN_AUC,
) -> EvaluationResult:
    """AUC using the score that was ALREADY persisted in DuckDB."""

    def _extract_persisted(session: dict) -> tuple[int, int] | None:
        max_score = session.get("max_edge_score")
        if max_score is None:
            return None
        n_events = len(session.get("events", []))
        return int(max_score), max(0, n_events - 1)

    return _evaluate_sessions_with_extractor(
        labeled_sessions,
        _extract_persisted,
        min_auc=min_auc,
    )


_KNOWN_REASON_ATOMS = (
    "cookie-missing",
    "cookie-tampered",
    "cookie-expired",
    "cookie-replayed",
    "impossibly-fast",
    "robotic-consistency",
    "low-transition-prob",
)


def evaluate_per_reason(
    labeled_sessions: Iterable[tuple[dict, str]],
    *,
    min_auc: float = DEFAULT_MIN_AUC,
    min_per_class: int = 3,
) -> dict:
    """AUC broken down by which L1/L2 rule fired in each session."""
    sessions_list = list(labeled_sessions)

    buckets: list[dict] = []
    for reason in _KNOWN_REASON_ATOMS:
        filtered: list[tuple[dict, str]] = []
        for session, label in sessions_list:
            if label not in ("good", "bad", "neutral"):
                continue
            events = session.get("events") or []
            if any(
                reason in {atom.strip() for atom in (ev.get("edge_score_reason") or "").split(",") if atom.strip()}
                for ev in events
            ):
                filtered.append((session, label))

        n_good = sum(1 for _, lbl in filtered if lbl == "good")
        n_bad = sum(1 for _, lbl in filtered if lbl == "bad")
        bucket: dict = {
            "reason": reason,
            "n_good": n_good,
            "n_bad": n_bad,
            "min_per_class": min_per_class,
            "has_min_samples": n_good >= min_per_class and n_bad >= min_per_class,
        }
        if bucket["has_min_samples"]:
            result = evaluate_from_persisted_scores(filtered, min_auc=min_auc)
            bucket["auc"] = round(float(result.auc), 4)
            bucket["passed"] = bool(result.passed)
            bucket["threshold"] = float(result.pass_threshold)
        buckets.append(bucket)
    return {"buckets": buckets, "min_per_class": min_per_class, "known_reasons": list(_KNOWN_REASON_ATOMS)}


_self = _sys.modules[__name__]
_sys.modules["backend.scoring.matrix"] = _self
if "backend.scoring" in _sys.modules:
    setattr(_sys.modules["backend.scoring"], "matrix", _self)

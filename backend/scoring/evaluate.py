"""ROC-AUC evaluation of a trained matrix against labeled negatives.

Per research doc §9.2 we use labels for EVALUATION ONLY — never to
auto-zero transitions in the trained matrix itself, because letting a
compromised verifier account submit "bad sessions" would create a
matrix-poisoning vector. The evaluator answers: "if I apply the L2
scorer to these known-malicious sessions and these known-good sessions,
does the AUC clear the quality bar?"

Inputs:
  - matrix (already trained; from backend.scoring.matrix)
  - labeled sessions: each is one of the JSONL trace dicts plus a label
    field ("good" | "bad"). Labels are sourced from a human verifier
    interface (admin UI) or from explicit allow/block lists.

Outputs:
  - AUC (area under the ROC curve)
  - per-session L2 scores
  - pass/fail vs configurable threshold
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Final

from backend.scoring.normalize import normalize
from backend.scoring.scorer import score_layer2

logger = logging.getLogger(__name__)

# A trained matrix that can't separate the labeled good from labeled bad
# at AUC > this is not deployment-quality. 0.85 is the doc's implicit
# bar (referenced as "deployment-quality" in §9.2 discussion). Override
# per-call via the ``min_auc`` kwarg on ``evaluate()`` if a particular
# matrix needs a stricter or looser bar; the CLI script and the test
# suite are the current callers.
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
    """Compute the maximum L2 score across all transitions in a session.

    "Maximum" because in production VCL would block on any single high-
    score request — a session whose highest-score transition exceeds the
    threshold is operationally caught regardless of where in the session
    it happened. Also returns the transition count so empty-session edge
    cases can be filtered."""
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
    """Area under the ROC curve via the Mann-Whitney U formulation.

    AUC = (#{good_score < bad_score} + 0.5 * #{good_score == bad_score})
          / (n_good * n_bad)

    O(n²) on the input, which is fine for typical evaluation set sizes
    (~10-10000 sessions). No SciPy/sklearn dependency."""
    good_scores = [s for s, lbl in scores if lbl == "good"]
    bad_scores = [s for s, lbl in scores if lbl == "bad"]
    n_good = len(good_scores)
    n_bad = len(bad_scores)
    if n_good == 0 or n_bad == 0:
        # Degenerate: can't compute AUC without one of each.
        return 0.5

    wins = 0.0
    for g in good_scores:
        for b in bad_scores:
            if g < b:
                wins += 1.0
            elif g == b:
                wins += 0.5
    return wins / (n_good * n_bad)


def evaluate(
    matrix: dict,
    labeled_sessions: Iterable[tuple[dict, str]],
    *,
    min_auc: float = DEFAULT_MIN_AUC,
) -> EvaluationResult:
    """Score every labeled session against ``matrix`` and report AUC.

    ``labeled_sessions`` is an iterable of (session_dict, label) where
    label is ``"good"`` or ``"bad"``. Any other label string is rejected
    upfront so a typo doesn't silently degrade AUC."""
    per_session: list[EvaluatedSession] = []
    scores_for_auc: list[tuple[int, str]] = []
    n_good = n_bad = 0

    for session, label in labeled_sessions:
        if label not in ("good", "bad", "neutral"):
            raise ValueError(f"unexpected label {label!r}; want 'good', 'bad', or 'neutral'")
        score, n_trans = _session_l2_score(session, matrix)
        per_session.append(
            EvaluatedSession(
                session_id=session.get("session_id", "?"),
                label=label,
                l2_score=score,
                transition_count=n_trans,
            )
        )
        if label == "neutral":
            # Intentionally uncertain: don't bias the AUC in either
            # direction. The row stays in per_session for display, but
            # is excluded from the scores_for_auc list.
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


def evaluate_from_persisted_scores(
    labeled_sessions: Iterable[tuple[dict, str]],
    *,
    min_auc: float = DEFAULT_MIN_AUC,
) -> EvaluationResult:
    """AUC using the score that was ALREADY persisted in DuckDB (i.e.
    what the live scorer actually returned at the edge — L1 + L2 +
    cookie-compliance combined), instead of recomputing L2 from the
    event list.

    Why this exists alongside ``evaluate()``: the L2-only evaluator
    can't see cookie compliance or L1 timing signals, so single-URL bot
    probes (the most common "bad" label class) score 0 because they
    have 0 transitions — even though the LIVE scorer correctly
    flagged them at 75 via the cookie-missing rule. The result is
    AUC=0 even when the matrix is perfectly tracking labels.

    The offline trainer keeps using ``evaluate()`` because at training
    time we only have raw event JSONL, no persisted edge scores. The
    /scoring/evaluation API uses THIS function because it does have
    them.

    Each session dict must carry a ``max_edge_score`` field (int 0-100).
    Sessions without one are dropped — there's nothing to evaluate.
    """
    per_session: list[EvaluatedSession] = []
    scores_for_auc: list[tuple[int, str]] = []
    n_good = n_bad = 0

    for session, label in labeled_sessions:
        if label not in ("good", "bad", "neutral"):
            raise ValueError(f"unexpected label {label!r}; want 'good', 'bad', or 'neutral'")
        max_score = session.get("max_edge_score")
        if max_score is None:
            continue  # no persisted score — can't evaluate
        n_events = len(session.get("events", []))
        per_session.append(
            EvaluatedSession(
                session_id=session.get("session_id", "?"),
                label=label,
                l2_score=int(max_score),  # "l2_score" name kept for back-compat with EvaluatedSession dataclass
                transition_count=max(0, n_events - 1),
            )
        )
        if label == "neutral":
            continue
        scores_for_auc.append((int(max_score), label))
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


# Known L1/L2 reason atoms emitted by the live scorer. Sourced from
# compute/scorer/src/scorer.rs (mirrored exactly by the Python reference in
# backend/scoring/scorer.py) — any new atom added there needs to be mirrored
# here (or, longer-term, derived from /scoring/health's top_reasons list
# dynamically). The compute-side failure atoms are excluded — they indicate
# scorer outages, not detection signals, and would skew per-rule AUC. Those are
# the compute-unavailable-* family (the scorer's 401-unauthorized lands here as
# compute-unavailable-401 once VCL rewrites the non-200 — there is no bare
# "unauthorized" log atom; EC-05) and the 200 "internal-error-keys" reason.
#
# The cookie atoms come from scorer.rs's `format!("cookie-{}", compliance)`
# over {missing, expired, replayed} plus the literal "cookie-tampered". The
# three non-missing cookie atoms were absent here, so their per-rule AUC
# never surfaced; "rare-transition" was listed but is emitted by neither
# scorer (the L2 rule emits "low-transition-prob"), so it produced a
# perpetually-empty bucket.
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
    """AUC broken down by which L1/L2 rule fired in each session.

    For each known reason atom (cookie-missing, impossibly-fast, etc.):
      - Filter to sessions whose events contain that atom in any
        edge_score_reason CSV
      - Compute AUC against ``max_edge_score`` over those sessions
      - Gate display when n_good < min_per_class OR n_bad < min_per_class
        (per-reason populations are strictly smaller than combined,
        so this gate fires more often)

    Returns ``{"buckets": [{"reason": ..., "auc": ..., "passed": ...,
    "n_good": ..., "n_bad": ..., "has_min_samples": bool}, ...],
    "min_per_class": int}`` — the headline /scoring/evaluation gives
    the combined AUC, this gives the per-rule breakdown.
    """
    sessions_list = list(labeled_sessions)

    buckets: list[dict] = []
    for reason in _KNOWN_REASON_ATOMS:
        filtered: list[tuple[dict, str]] = []
        for session, label in sessions_list:
            if label not in ("good", "bad", "neutral"):
                continue
            events = session.get("events") or []
            tripped = False
            for ev in events:
                ev_reason = ev.get("edge_score_reason") or ""
                if reason in {atom.strip() for atom in ev_reason.split(",") if atom.strip()}:
                    tripped = True
                    break
            if not tripped:
                continue
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

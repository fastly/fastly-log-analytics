"""Edge scoring engine: Layer 2 (route transition) evaluation.

**Reference implementation, not the runtime path.** This module exists to
serve as the Python-side ground truth in cross-language parity tests and
ROC-AUC evaluation; the production scoring on every customer request runs in
the Rust/Wasm port under ``compute/scorer/`` at the edge.
"""

from __future__ import annotations

import math
from typing import Final

from backend.scoring.normalize import Route

# ── Layer 2 tuning constants (research doc §4.2) ──────────────────────────────

# Laplace (additive) smoothing factor. Larger = more conservative (every
# unseen transition gets more probability mass). 0.5 per the doc.
L2_LAPLACE_ALPHA: Final[float] = 0.5

# Skip-gram discount: a high-probability anchor→current transition counts
# slightly less than a direct prev→current high-probability transition.
L2_SKIPGRAM_BETA: Final[float] = 0.7


# Maps TransScore in [0, 1] → contribution in [0, 100]. We use a log-shaped
# transform so the score climbs sharply as transition probability drops
# below 1e-3 (the "almost certainly never happens in human traffic" floor).
# At P = 1.0 → 0. At P = 1e-3 → ~50. At P = 1e-6 → ~100.
def _l2_score_from_trans_prob(p: float) -> int:
    """Map a TransScore probability to a Layer 2 anomaly score [0, 100]."""
    if p >= 1.0:
        return 0
    if p <= 1e-12:
        return 100
    # -log10(p) maps p=1e-3 → 3, p=1e-6 → 6. Scale by 100/6 so p=1e-6 ≈ 100.
    raw = -math.log10(p) * (100.0 / 6.0)
    if raw < 0:
        return 0
    if raw > 100:
        return 100
    return int(round(raw))


# ── Layer 2: Route Transition Shield ──────────────────────────────────────────


def _transition_prob(matrix: dict, prev_route: str, current_route: str, vocab_size: int) -> float:
    """Laplace-smoothed P(current | prev) lookup from a serialized matrix.

    Matrix shape (matches what backend.scoring.matrix emits):
        {
          "counts": {prev_route: {current_route: count, ...}},
          "row_totals": {prev_route: total_out_count},
          "vocab_size": int,
          ...
        }

    Returns the smoothed conditional probability. Always > 0 (Laplace
    floor). An unseen prev route in a POPULATED matrix is penalized to the
    probability floor (→ max L2 score) rather than getting the lenient
    uniform prior — see the fail-closed note below."""
    counts = matrix.get("counts", {})
    row_totals = matrix.get("row_totals", {})
    # Fail closed on an unseen prev route. In a populated matrix a prev
    # route that was never a transition SOURCE isn't "neutral" — it's a
    # transition the model has never observed, which is the exact shape of
    # evasion (prepend an untracked route to dodge the shield). The old
    # uniform Laplace prior (1/vocab) mapped to a sub-threshold score, so
    # unseen-prev sequences slipped past enforcement. Return the floor so it
    # maps to the max L2 score. Only fires when the matrix HAS data; a truly
    # empty matrix still falls through to the uniform prior below (L2 self-disables).
    #
    # EC-04: "unseen" is decided on the row_total VALUE (<= 0), not mere key
    # presence — a prev route PRESENT in row_totals with total 0 (a curr-only
    # route: seen only as a destination, never a source) is still unobserved as a
    # transition source. This mirrors the Rust scorer (scorer.rs::transition_prob
    # gates on ``row_total(id) > 0``), so the train-side reference and the edge
    # agree on a key-present-zero prev. Unreachable from prod matrices (the
    # trainer always +1's a source's row_total), so this is behaviour-preserving.
    if row_totals and row_totals.get(prev_route, 0) <= 0:
        return 1e-12
    prev_row = counts.get(prev_route, {})
    numerator = prev_row.get(current_route, 0) + L2_LAPLACE_ALPHA
    denominator = row_totals.get(prev_route, 0) + L2_LAPLACE_ALPHA * vocab_size
    if denominator <= 0:
        # Truly empty matrix — return the uniform prior.
        return 1.0 / max(vocab_size, 1)
    return numerator / denominator


def score_layer2(
    matrix: dict | None,
    prev_route: Route | None,
    prev_anchor_route: Route | None,
    current_route: Route,
) -> tuple[int, list[str], float]:
    """Apply the L2 rules. Returns (score_contribution, reasons, trans_prob).

    - ``matrix`` is the serialized transition matrix; None → L2 disabled
      (matrix not yet trained).
    - ``prev_route`` is the immediate previous route this session visited
      (None on first request — L2 returns 0 with no reasons).
    - ``prev_anchor_route`` is the most-recent ANCHOR route (skipping
      auxiliary pages like /about-us) — used for the skip-gram lookback.
      Pass None to disable skip-gram; pass the same as prev_route to
      collapse skip-gram down to "look one step back" semantics.
    - ``current_route`` is the request being scored.
    """
    if matrix is None or prev_route is None:
        return 0, [], 1.0

    vocab_size = int(matrix.get("vocab_size", 0))
    if vocab_size <= 0:
        return 0, [], 1.0

    direct_p = _transition_prob(matrix, prev_route.path, current_route.path, vocab_size)
    if (
        prev_anchor_route is not None
        and prev_anchor_route.path != prev_route.path
        and prev_anchor_route.path in matrix.get("counts", {})
    ):
        anchor_p = _transition_prob(matrix, prev_anchor_route.path, current_route.path, vocab_size) * L2_SKIPGRAM_BETA
        trans_prob = max(direct_p, anchor_p)
    else:
        trans_prob = direct_p

    score = _l2_score_from_trans_prob(trans_prob)
    reasons = ["low-transition-prob"] if score >= 50 else []
    return score, reasons, trans_prob

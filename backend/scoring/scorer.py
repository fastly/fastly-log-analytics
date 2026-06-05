"""Edge scoring engine: Layer 1 (universal behavioral) + Layer 2 (route
transition) + combined output.

**Reference implementation, not the runtime path.** This module exists to
serve as the Python-side ground truth in cross-language parity tests; the
production scoring on every customer request runs in the Rust/Wasm port
under ``compute/scorer/`` at the edge. The wire-format fixtures in
``tests/scoring/fixtures/`` are byte-pinned and exercised by both
implementations — any drift between Python and Rust fails the build.
Production application code should NOT import ``score_combined``,
``score_layer1``, or ``ScoreResult.to_headers`` from this module; the
data path is VCL → Compute scorer → response headers, with no Python
in the loop.

The math here is deliberately straightforward — no NumPy, no library
calls inside hot paths, integers + small dicts only — so it's easy to
keep in lock-step with Rust.

This module is pure: no I/O, no clock reads, no environment lookups. The
caller is responsible for decoding the cookie (``backend.scoring.cookie``),
loading the matrix (``backend.scoring.matrix``), and normalizing the
incoming request URL (``backend.scoring.normalize``). The scorer just
takes those values and emits a verdict.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Final

from backend.scoring.cookie import SessionState, quantize_score
from backend.scoring.normalize import Route

# ── Layer 1 tuning constants (research doc §4.1) ──────────────────────────────

# "Warmup gate" — timing rules suppressed until the session has a meaningful
# sample for mean/variance. seq=3 is the minimum for any variance estimate,
# the doc bumps that to "> 3" so we score from the 4th request onward.
L1_TIMING_WARMUP_SEQ: Final[int] = 3

# Impossibly Fast: mean dwell < this → fire. 200ms is faster than any human
# can read+click; lower than a typical TLS RTT to the nearest POP.
L1_FAST_DWELL_THRESHOLD_S: Final[float] = 0.20

# Robotic Consistency: variance must be below this AND mean dwell in the
# suspicious band. Variance threshold = 0.05s² captures `sleep(1)` style
# loops (variance ≈ 0).
#
# Security: was 0.5 — there was a "robotic safe zone" between
# L1_FAST_DWELL_THRESHOLD_S (0.20) and L1_ROBOTIC_DWELL_LOW_S (0.50)
# where a low-variance bot averaging 0.30s/page scored zero. Lower
# the threshold to 0.20 so the robotic detector covers the
# previously-uncovered band.
L1_ROBOTIC_VARIANCE_THRESHOLD: Final[float] = 0.05
L1_ROBOTIC_DWELL_LOW_S: Final[float] = 0.20
L1_ROBOTIC_DWELL_HIGH_S: Final[float] = 3.0

# Score contributions per fired rule. Sum of all L1 rules is capped at 100
# in the combined output. Cookie compliance dominates because it's the
# strongest "this is definitely a bot" signal among L1's three rules.
L1_SCORE_FAST: Final[int] = 50
L1_SCORE_ROBOTIC: Final[int] = 40
L1_SCORE_COOKIE_MISSING: Final[int] = 75
# Security: tampered cookies are a strictly stronger anomaly signal
# than missing/expired (missing might be a fresh visitor, tampered is
# intentional). The threshold-matrix admin UI uses score==100 to enable
# hard-block enforcement, so capping tampered at 75 let attackers stay
# below the enforcement bar while exhibiting active anomalous behavior.
L1_SCORE_COOKIE_TAMPERED: Final[int] = 100

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


# ── Combined output ───────────────────────────────────────────────────────────


@dataclass
class ScoreResult:
    """Output of a single scoring evaluation. Maps 1:1 onto the
    ``X-Edge-*`` response headers in research doc §6."""

    score: int = 0  # quantized 0-100, the X-Edge-Score header
    l1_score: int = 0
    l2_score: int = 0
    reasons: list[str] = field(default_factory=list)
    cookie_compliance: str = "ok"  # ok | missing | expired | rotated | tampered
    # Diagnostics (not in the header set; useful in tests and analyst UI).
    mean_dwell_s: float = 0.0
    variance_s2: float = 0.0
    trans_prob: float = 1.0
    matrix_version: str = ""

    def to_headers(self) -> dict[str, str]:
        """Materialize the ``X-Edge-*`` header set per research doc §6."""
        return {
            "X-Edge-Score": str(self.score),
            "X-Edge-Score-L1": str(self.l1_score),
            "X-Edge-Score-L2": str(self.l2_score),
            "X-Edge-Cookie-Compliance": self.cookie_compliance,
            "X-Edge-Score-Reason": ",".join(self.reasons),
            "X-Edge-Matrix-Version": self.matrix_version,
        }


# ── Layer 1: Universal Behavioral Shield ──────────────────────────────────────


def _running_mean_variance(state: SessionState) -> tuple[float, float]:
    """Compute mean dwell and timing variance from the cookie's running sums.

    Uses the algebraic identity Var(X) = E[X²] − E[X]² so we never need to
    store the history array. seq=0 case (no transitions yet) returns
    (0, 0) — the timing rules check the warmup gate separately."""
    if state.seq <= 0:
        return 0.0, 0.0
    mean = state.sum_dt / state.seq
    second_moment = state.sum_dt_sq / state.seq
    # Floating-point can push the second moment slightly below mean² when
    # they're nearly equal (all dwells identical). Clamp to 0.
    var = max(0.0, second_moment - mean * mean)
    return mean, var


def score_layer1(state: SessionState) -> tuple[int, list[str], float, float]:
    """Apply the L1 rules to a decoded session state. Returns
    (score_contribution, reasons, mean_dwell_s, variance_s2)."""
    mean, var = _running_mean_variance(state)
    score = 0
    reasons: list[str] = []

    # Impossibly Fast and Robotic Consistency both share the seq>3 warmup
    # gate. Below that, only cookie compliance fires (which is handled by
    # the caller — see score_combined — because it needs to know whether
    # the cookie was missing/tampered, info that's lost by the time we
    # have a SessionState in hand).
    if state.seq > L1_TIMING_WARMUP_SEQ:
        if mean < L1_FAST_DWELL_THRESHOLD_S:
            score += L1_SCORE_FAST
            reasons.append("impossibly-fast")
        if var < L1_ROBOTIC_VARIANCE_THRESHOLD and L1_ROBOTIC_DWELL_LOW_S <= mean <= L1_ROBOTIC_DWELL_HIGH_S:
            score += L1_SCORE_ROBOTIC
            reasons.append("robotic-consistency")

    return min(score, 100), reasons, mean, var


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
    floor). Unseen prev rows get the all-uniform smoothed prior."""
    counts = matrix.get("counts", {})
    row_totals = matrix.get("row_totals", {})
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
      (matrix not yet trained, first 7 days of deployment per §4.3).
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
    if prev_anchor_route is not None and prev_anchor_route.path != prev_route.path:
        anchor_p = _transition_prob(matrix, prev_anchor_route.path, current_route.path, vocab_size) * L2_SKIPGRAM_BETA
        trans_prob = max(direct_p, anchor_p)
    else:
        trans_prob = direct_p

    score = _l2_score_from_trans_prob(trans_prob)
    reasons = ["low-transition-prob"] if score >= 50 else []
    return score, reasons, trans_prob


# ── Combined evaluation ───────────────────────────────────────────────────────


def _blend_weight(matrix_age_days: float) -> float:
    """Layer 2 weight ramps from 0 → 1 over the 3 days following Day 7.

    Per §4.3: avoids a step-function score change the moment training
    becomes available. Day 7 → 0.0, Day 8 → 0.333, Day 10 → 1.0."""
    if matrix_age_days < 7.0:
        return 0.0
    if matrix_age_days >= 10.0:
        return 1.0
    return (matrix_age_days - 7.0) / 3.0


def score_combined(
    *,
    state: SessionState | None,
    cookie_compliance: str = "ok",
    current_route: Route,
    prev_route: Route | None,
    prev_anchor_route: Route | None = None,
    matrix: dict | None = None,
    matrix_age_days: float = 0.0,
) -> ScoreResult:
    """One-stop scorer. The caller assembles the inputs from cookie decode
    + route history + loaded matrix; this function applies all rules,
    blends per §4.3, quantizes per §3.3, and returns the headers.

    ``state=None, cookie_compliance != 'ok'`` is the "no cookie" path —
    cookie compliance rule fires when seq>1 (i.e. this is NOT the first
    request from this client) and the cookie is missing/tampered."""

    result = ScoreResult(cookie_compliance=cookie_compliance)
    result.matrix_version = str(matrix.get("version", "")) if matrix else ""

    # Cookie-compliance rule (§4.1). Note: we don't have seq if state is
    # None, so the caller must hint "is this multi-request?" via cookie
    # compliance status. ``missing`` and ``expired`` get the historical
    # 75; ``tampered`` gets the full 100 (security) because it's a
    # deliberate evasion signal. ``rotated`` is benign (fresh cookie
    # post-rotation).
    l1_from_cookie = 0
    if cookie_compliance == "tampered":
        l1_from_cookie = L1_SCORE_COOKIE_TAMPERED
        result.reasons.append("cookie-tampered")
    elif cookie_compliance in ("missing", "expired"):
        l1_from_cookie = L1_SCORE_COOKIE_MISSING
        result.reasons.append(f"cookie-{cookie_compliance}")

    # Layer 1 timing rules (only meaningful with a valid decoded state).
    if state is not None:
        l1_timing, l1_reasons, mean, var = score_layer1(state)
        result.mean_dwell_s = mean
        result.variance_s2 = var
        result.reasons.extend(l1_reasons)
        result.l1_score = min(100, l1_from_cookie + l1_timing)
    else:
        result.l1_score = l1_from_cookie

    # Layer 2 transition rule (gated on matrix availability).
    l2_score, l2_reasons, trans_prob = score_layer2(matrix, prev_route, prev_anchor_route, current_route)
    result.l2_score = l2_score
    result.trans_prob = trans_prob
    result.reasons.extend(l2_reasons)

    # Combined per §4.3, quantized per §3.3.
    w_l2 = _blend_weight(matrix_age_days)
    raw_combined = result.l1_score + result.l2_score * w_l2
    result.score = quantize_score(raw_combined)

    return result

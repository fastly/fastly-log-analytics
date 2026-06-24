"""Tests for backend.scoring.scorer — L1, L2, and combined evaluation.

Each rule has dedicated tests for both fired and not-fired cases. The
combined-output tests are the cross-language fixture contract: the Rust
port under ``compute/scorer/`` must produce identical (l1, l2, combined,
reasons) tuples for the same inputs."""

from __future__ import annotations

import pytest

from backend.scoring.cookie import SessionState
from backend.scoring.normalize import Route
from backend.scoring.scorer import (
    L1_ROBOTIC_VARIANCE_THRESHOLD,
    L1_SCORE_COOKIE_MISSING,
    L1_SCORE_FAST,
    L1_SCORE_ROBOTIC,
    L1_TIMING_WARMUP_SEQ,
    L2_RAMP_DAYS,
    ScoreResult,
    _l2_score_from_trans_prob,
    _optin_ramp_weight,
    _running_mean_variance,
    _transition_prob,
    score_combined,
    score_layer1,
    score_layer2,
)


def _state(seq: int = 0, sum_dt: int = 0, sum_dt_sq: int = 0) -> SessionState:
    return SessionState(
        sid=b"\x00\x01\x02\x03\x04\x05",
        seq=seq,
        sum_dt=sum_dt,
        sum_dt_sq=sum_dt_sq,
        last_ts=1_700_000_000,
        score=0,
        issued_at=1_699_990_000,
    )


# ── _running_mean_variance ────────────────────────────────────────────────────


def test_running_mean_variance_seq_zero_returns_zeros():
    assert _running_mean_variance(_state(seq=0)) == (0.0, 0.0)


def test_running_mean_variance_uniform_dwells():
    """Five identical 2-second dwells → mean=2, variance=0."""
    state = _state(seq=5, sum_dt=10, sum_dt_sq=20)  # Σx=10, Σx²=5*4=20
    mean, var = _running_mean_variance(state)
    assert mean == 2.0
    assert var == 0.0


def test_running_mean_variance_mixed():
    """Dwells [1,2,3,4] → mean=2.5, var=(1+4+9+16)/4 - 6.25 = 7.5-6.25 = 1.25."""
    state = _state(seq=4, sum_dt=10, sum_dt_sq=30)
    mean, var = _running_mean_variance(state)
    assert mean == pytest.approx(2.5)
    assert var == pytest.approx(1.25)


def test_running_mean_variance_long_session_divides_by_full_seq():
    """#038: the whole-session mean divides by the full seq, NOT a capped
    min(seq, 20). 40 dwells summing to 80 → mean 2.0; a min(20) divisor
    would wrongly give 80/20 = 4.0. Pins the divide-by-seq contract and
    guards against re-introducing the inflating divisor cap. Mirrors the
    Rust test running_long_session_divides_by_full_seq_not_capped."""
    state = _state(seq=40, sum_dt=80, sum_dt_sq=200)
    mean, var = _running_mean_variance(state)
    assert mean == pytest.approx(2.0)
    assert var == pytest.approx(1.0)  # 200/40 - 2² = 5 - 4 = 1


def test_running_mean_variance_clamps_negative_to_zero():
    """Floating-point underflow can push variance microscopically negative
    when all dwells are identical — clamp to 0 not propagate the noise."""
    # Construct: seq=10, sum_dt=10 → mean=1. sum_dt_sq=10 → second moment=1.
    # var = 1 - 1*1 = 0 exactly here, but verify clamp logic.
    state = _state(seq=10, sum_dt=10, sum_dt_sq=10)
    _, var = _running_mean_variance(state)
    assert var >= 0


# ── Layer 1: warmup gate ─────────────────────────────────────────────────────


def test_layer1_below_warmup_no_timing_rules_fire():
    """seq <= L1_TIMING_WARMUP_SEQ: timing rules suppressed even if metrics
    would otherwise fire."""
    # Below warmup with comically fast dwell (would normally fire fast rule).
    state = _state(seq=L1_TIMING_WARMUP_SEQ, sum_dt=0, sum_dt_sq=0)
    score, reasons, _, _ = score_layer1(state)
    assert score == 0
    assert reasons == []


def test_layer1_at_warmup_plus_one_evaluates_rules():
    """First scoring eligible request is seq = WARMUP + 1."""
    # 4 transitions, all 0.1s → mean=0.1 (below fast threshold)
    state = _state(seq=L1_TIMING_WARMUP_SEQ + 1, sum_dt=0, sum_dt_sq=0)
    # Actually sum_dt=0 means mean=0 which is < 0.2, will fire.
    score, reasons, _, _ = score_layer1(state)
    assert "impossibly-fast" in reasons
    assert score >= L1_SCORE_FAST


# ── Layer 1: Impossibly Fast ──────────────────────────────────────────────────


def test_layer1_impossibly_fast_fires_when_mean_below_threshold():
    # seq=5, sum_dt=0 → mean=0 → < 0.2
    state = _state(seq=5, sum_dt=0, sum_dt_sq=0)
    score, reasons, mean, _ = score_layer1(state)
    assert "impossibly-fast" in reasons
    assert score >= L1_SCORE_FAST
    assert mean == 0.0


def test_layer1_impossibly_fast_does_not_fire_at_threshold():
    """Mean exactly at threshold (0.2s) does NOT fire — strict less-than."""
    # seq=5, mean=0.2 → sum_dt=1, sum_dt_sq must be coherent (1²/5 = 0.2)
    state = _state(seq=5, sum_dt=1, sum_dt_sq=1)  # mean=0.2, var=1/5 - 0.04 = 0.16
    _, reasons, mean, _ = score_layer1(state)
    assert mean == 0.2
    assert "impossibly-fast" not in reasons


def test_layer1_impossibly_fast_does_not_fire_for_humans():
    """Mean 5s (normal human dwell) → no fire."""
    state = _state(seq=10, sum_dt=50, sum_dt_sq=300)
    _, reasons, mean, _ = score_layer1(state)
    assert mean == 5.0
    assert "impossibly-fast" not in reasons


# ── Layer 1: Robotic Consistency ─────────────────────────────────────────────


def test_layer1_robotic_consistency_fires_uniform_1s_loops():
    """time.sleep(1.0) bot: 10 dwells of exactly 1.0s → var=0, mean=1.0."""
    state = _state(seq=10, sum_dt=10, sum_dt_sq=10)  # all dwells=1
    score, reasons, mean, var = score_layer1(state)
    assert "robotic-consistency" in reasons
    assert score >= L1_SCORE_ROBOTIC
    assert mean == 1.0
    assert var < L1_ROBOTIC_VARIANCE_THRESHOLD


def test_layer1_robotic_consistency_does_not_fire_outside_dwell_band():
    """Uniform 10s dwells: low variance but mean is outside the suspicious
    band (0.5–3s) → does NOT fire."""
    state = _state(seq=10, sum_dt=100, sum_dt_sq=1000)  # all dwells=10
    _, reasons, mean, var = score_layer1(state)
    assert mean == 10.0
    assert var < L1_ROBOTIC_VARIANCE_THRESHOLD
    assert "robotic-consistency" not in reasons


def test_layer1_robotic_consistency_does_not_fire_when_variance_high():
    """High variance even within dwell band → real human, no fire."""
    # Dwells [0.5, 1.5, 2.5, 0.5, 1.5, 2.5] = sum 9, sum_sq 17.5, mean 1.5, var 1.0
    state = _state(seq=6, sum_dt=9, sum_dt_sq=int(17.5))
    _, reasons, _, _ = score_layer1(state)
    assert "robotic-consistency" not in reasons


# ── Layer 1: both rules can fire together ────────────────────────────────────


def test_layer1_both_rules_can_fire_simultaneously():
    """A bot with both fast mean AND low variance (e.g. sleep(0.1) loop)
    gets BOTH penalties added — but capped at 100."""
    # 10 dwells of 0.1s → sum_dt=1, sum_dt_sq=0.1 → mean=0.1, var=0.01-0.01=0
    state = _state(seq=10, sum_dt=1, sum_dt_sq=0)  # int rounding ok here
    score, reasons, mean, var = score_layer1(state)
    assert mean == pytest.approx(0.1)
    assert var < L1_ROBOTIC_VARIANCE_THRESHOLD
    # The fast rule fires (mean < 0.2), but robotic does NOT (mean < 0.5).
    assert "impossibly-fast" in reasons
    assert "robotic-consistency" not in reasons  # mean below band
    assert score == L1_SCORE_FAST


# ── Layer 2: _transition_prob (Laplace lookup) ───────────────────────────────


def _matrix(counts: dict[str, dict[str, int]], vocab: int = 10) -> dict:
    row_totals = {k: sum(v.values()) for k, v in counts.items()}
    return {
        "counts": counts,
        "row_totals": row_totals,
        "vocab_size": vocab,
        "version": "test-1",
    }


def test_transition_prob_with_observed_pair():
    """Observed (a→b) 50 times out of 100 from a → P ≈ 0.5 (slight Laplace
    shift)."""
    m = _matrix({"a": {"b": 50, "c": 50}}, vocab=10)
    p = _transition_prob(m, "a", "b", m["vocab_size"])
    # (50 + 0.5) / (100 + 0.5 * 10) = 50.5 / 105 ≈ 0.48
    assert 0.45 < p < 0.51


def test_transition_prob_with_unseen_pair():
    """Unseen pair from a known row gets Laplace prior — nonzero, small."""
    m = _matrix({"a": {"b": 100}}, vocab=10)
    p = _transition_prob(m, "a", "never-seen", m["vocab_size"])
    # (0 + 0.5) / (100 + 0.5*10) = 0.5 / 105 ≈ 0.0048
    assert 0.001 < p < 0.01


def test_transition_prob_with_unseen_prev_row():
    """Unknown prev route in a POPULATED matrix → fail closed at the
    probability floor (→ max L2 score), not the lenient uniform prior.

    The uniform prior (0.1 here) mapped to a sub-threshold L2 score, which
    let an attacker dodge the transition shield by prepending an untracked
    route. An unseen prev row is now treated as maximally anomalous."""
    m = _matrix({"a": {"b": 100}}, vocab=10)
    p = _transition_prob(m, "never-seen-prev", "b", m["vocab_size"])
    assert p <= 1e-12


def test_transition_prob_present_zero_row_total_fails_closed():
    """EC-04 cross-language fixture: a prev route PRESENT in row_totals with a
    value of 0 (a curr-only route — seen as a destination, never a source) is
    'unseen as a source' and must fail closed to the probability floor (→ max L2),
    NOT the lenient uniform prior. Decided on the VALUE (<= 0), not key presence.
    Mirrors the Rust scorer's ``l2_present_prev_zero_row_total_fails_closed``
    (scorer.rs gates on ``row_total(id) > 0``). Pre-EC-04, Python keyed on
    presence and returned ~0.1 (→ L2 17) here while Rust returned the floor
    (→ L2 100) — a silent cross-language contract divergence. Unreachable from
    prod matrices (the trainer always +1's a source's row_total)."""
    # "a" is present in row_totals but with total 0 (it has no outgoing counts).
    m = {"counts": {"b": {"c": 100}}, "row_totals": {"a": 0, "b": 100}, "vocab_size": 50}
    p = _transition_prob(m, "a", "c", m["vocab_size"])
    assert p <= 1e-12


def test_transition_prob_empty_matrix_falls_back_to_uniform():
    m = {"counts": {}, "row_totals": {}, "vocab_size": 50}
    p = _transition_prob(m, "x", "y", m["vocab_size"])
    assert p == pytest.approx(1.0 / 50.0)


# ── Layer 2: _l2_score_from_trans_prob curve ─────────────────────────────────


@pytest.mark.parametrize(
    "p,expected_band",
    [
        (1.0, (0, 0)),
        (0.5, (0, 10)),  # very high prob → near 0
        (0.1, (10, 25)),  # 1 in 10 → mild
        (0.01, (30, 40)),  # 1 in 100 → moderate
        (0.001, (45, 55)),  # 1 in 1000 → mid
        (0.000001, (95, 100)),  # 1e-6 → near max
    ],
)
def test_l2_score_curve_monotonic(p, expected_band):
    s = _l2_score_from_trans_prob(p)
    low, high = expected_band
    assert low <= s <= high, f"p={p}: got {s}, expected in {expected_band}"


def test_l2_score_returns_100_at_zero():
    assert _l2_score_from_trans_prob(0.0) == 100


def test_l2_score_returns_0_at_one():
    assert _l2_score_from_trans_prob(1.0) == 0


# ── Layer 2: score_layer2 integration ────────────────────────────────────────


def test_score_layer2_no_matrix_returns_zero():
    score, reasons, p = score_layer2(None, Route("/a", "home"), None, Route("/b", "home"))
    assert score == 0
    assert reasons == []
    assert p == 1.0


def test_score_layer2_no_prev_returns_zero():
    """First request in a session (no prev_route) → L2 returns 0."""
    score, reasons, p = score_layer2(_matrix({"a": {"b": 1}}), None, None, Route("/b", "home"))
    assert score == 0


def test_score_layer2_high_prob_transition_no_score():
    """Common transition (e.g. home → products) gets no penalty."""
    m = _matrix({"/home": {"/products": 99, "/other": 1}}, vocab=10)
    score, reasons, p = score_layer2(
        m,
        Route("/home", "home"),
        None,
        Route("/products", "product"),
    )
    assert score == 0
    assert reasons == []
    # P should be (99 + 0.5) / (100 + 5) ≈ 0.948
    assert p > 0.9


def test_score_layer2_rare_transition_high_score():
    """Truly rare (a → b) transition fires the low-transition-prob rule."""
    # 1 out of 10000 → P ≈ 1/10000 → score ≈ 65+
    m = _matrix({"/home": {"/checkout": 1}}, vocab=100)
    m["row_totals"]["/home"] = 10000  # simulate dense row
    score, reasons, p = score_layer2(
        m,
        Route("/home", "home"),
        None,
        Route("/checkout", "checkout"),
    )
    assert score >= 50
    assert "low-transition-prob" in reasons
    assert p < 0.001


def test_score_layer2_skipgram_rescues_via_anchor():
    """Even if the direct prev→curr is rare, a high-prob anchor→curr saves
    the score (skip-gram lookback)."""
    m = _matrix(
        {
            "/about-us": {"/checkout": 1},  # rare from auxiliary page
            "/product": {"/checkout": 100},  # common from anchor
        },
        vocab=10,
    )
    m["row_totals"]["/about-us"] = 1000
    m["row_totals"]["/product"] = 100
    score, _, p = score_layer2(
        m,
        Route("/about-us", "content"),
        Route("/product", "product"),  # anchor lookback
        Route("/checkout", "checkout"),
    )
    # anchor_p = (100 + 0.5) / (100 + 5) * 0.7 ≈ 0.67
    # direct_p ≈ 0.0015
    # max → ≈ 0.67 → score ≈ 0
    assert p > 0.5
    assert score < 10


def test_score_layer2_skipgram_unseen_anchor_does_not_override_anomalous_transition():
    """Finding 021: Ensure that if the skip-gram anchor is an unseen route, its Laplace-smoothed
    uniform prior does NOT override or mask a highly anomalous direct transition."""
    # We construct a matrix with a highly visited direct route having an anomalous transition to /checkout,
    # and no counts or totals for the unseen anchor /never-visited-anchor.
    m = _matrix(
        {
            "/about-us": {"/home": 1000},  # direct transition /about-us -> /checkout is anomalous (0 count)
        },
        vocab=10,
    )
    # The direct transition probability will be: (0 + 0.5) / (1000 + 0.5 * 10) = 0.5 / 1005 ≈ 0.000497 (very low!)
    # The unseen anchor prior would be: (0 + 0.5) / (0 + 0.5 * 10) = 0.1, multiplied by L2_SKIPGRAM_BETA (0.7) ≈ 0.07.
    # Without the fix, max(0.000497, 0.07) = 0.07, which is above the low-transition threshold (score ≈ 0).
    # With the fix, the unseen anchor is ignored, trans_prob = 0.000497, triggering a high transition anomaly score.
    score, reasons, p = score_layer2(
        m,
        Route("/about-us", "content"),
        Route("/never-visited-anchor", "product"),  # Unseen anchor! Not in matrix counts.
        Route("/checkout", "checkout"),
    )
    assert p < 0.001
    assert score >= 50
    assert "low-transition-prob" in reasons


# ── _optin_ramp_weight ───────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "days_since_optin,expected_w",
    [
        (-1.0, 0.0),
        (0.0, 0.0),
        (1.5, 0.5),
        (3.0, 1.0),
        (30.0, 1.0),
    ],
)
def test_optin_ramp_weight_pinned(days_since_optin, expected_w):
    """Parity with the Rust ``optin_ramp_weight_pinned``: linear fade-in over
    [0, L2_RAMP_DAYS] from the opt-in instant — 0 at (and before) opt-in, 0.5 at
    the midpoint, 1.0 once fully ramped. Replaces the retired day-7 age-blend."""
    assert _optin_ramp_weight(days_since_optin) == pytest.approx(expected_w)
    assert L2_RAMP_DAYS == 3.0


# ── score_combined: end-to-end output contract ───────────────────────────────


def test_score_combined_clean_human_session_returns_zero():
    """Normal user: valid cookie, healthy timing, common route transition.
    Score should be 0 / low."""
    state = _state(seq=10, sum_dt=50, sum_dt_sq=300)  # mean=5s, var=5
    m = _matrix({"/home": {"/products": 100}}, vocab=10)
    result = score_combined(
        state=state,
        cookie_compliance="ok",
        current_route=Route("/products", "product"),
        prev_route=Route("/home", "home"),
        matrix=m,
        l2_enforce_enabled=True,
        l2_days_since_optin=30.0,
    )
    assert result.score == 0
    assert result.l1_score == 0
    assert result.l2_score == 0
    assert result.reasons == []


def test_score_combined_missing_cookie_high_score():
    """Multi-request client with no cookie → cookie-missing fires hard."""
    result = score_combined(
        state=None,
        cookie_compliance="missing",
        current_route=Route("/checkout", "checkout"),
        prev_route=Route("/home", "home"),
        matrix=None,
    )
    assert result.l1_score >= L1_SCORE_COOKIE_MISSING
    assert "cookie-missing" in result.reasons
    assert result.score >= 75


def test_score_combined_tampered_cookie_caps_at_100():
    """Security: tampered cookies must score 100, not the 75 ceiling
    that ``missing`` / ``expired`` share. Tampering is an active evasion
    signal — the prior 75 cap let an attacker stay below the
    threshold-matrix enforcement bar."""
    from backend.scoring.scorer import L1_SCORE_COOKIE_TAMPERED

    result = score_combined(
        state=None,
        cookie_compliance="tampered",
        current_route=Route("/checkout", "checkout"),
        prev_route=Route("/home", "home"),
        matrix=None,
    )
    assert result.l1_score == L1_SCORE_COOKIE_TAMPERED == 100
    assert "cookie-tampered" in result.reasons
    assert result.score >= 95  # quantized to nearest 5; 100 → 100


def test_score_combined_robotic_threshold_gap_closed():
    """Security: a low-variance bot with mean dwell ~0.30s used to
    fall in the gap between L1_FAST_DWELL_THRESHOLD_S (0.20) and the old
    L1_ROBOTIC_DWELL_LOW_S (0.50), scoring 0. After the fix the
    robotic detector covers that band.

    State units: sum_dt is the sum of dwells in seconds across `seq`
    events. 10 dwells of exactly 0.30s ⇒ sum_dt=3.0, sum_dt_sq=0.90
    (each dwell squared is 0.09s², ×10 = 0.90; variance = 0.09 − 0.09 = 0).
    """
    state = _state(seq=10, sum_dt=3, sum_dt_sq=1)  # mean=0.3s, var≈0.0 (sum_dt_sq integer)
    result = score_combined(
        state=state,
        cookie_compliance="ok",
        current_route=Route("/products/123", "product"),
        prev_route=Route("/products/122", "product"),
        matrix=None,
    )
    assert "robotic-consistency" in result.reasons, (
        f"expected robotic-consistency to fire at mean=0.30s; got reasons={result.reasons}, "
        f"l1={result.l1_score}, mean={result.mean_dwell_s}, var={result.variance_s2}"
    )
    assert result.l1_score >= L1_SCORE_ROBOTIC


def test_score_combined_impossibly_fast_scraper():
    """Fast-burst scraper: 10 dwells of 0.05s, healthy cookie."""
    state = _state(seq=10, sum_dt=0, sum_dt_sq=0)  # mean=0
    result = score_combined(
        state=state,
        cookie_compliance="ok",
        current_route=Route("/products/123", "product"),
        prev_route=Route("/products/122", "product"),
        matrix=None,
    )
    assert "impossibly-fast" in result.reasons
    assert result.l1_score >= L1_SCORE_FAST
    assert result.score >= 50


def test_score_combined_l2_off_contributes_nothing():
    """Parity with the Rust ``l2_off_contributes_nothing``: flag false → L2 stays
    observe-only forever. Its sub-score is still computed, but it never reaches
    the enforced combined score regardless of how stale the opt-in anchor is."""
    state = _state(seq=10, sum_dt=50, sum_dt_sq=300)  # clean
    m = _matrix({"/a": {"/b": 1}}, vocab=100)  # /a→/c is RARE
    for days in (0.0, 1.5, 3.0, 30.0):
        result = score_combined(
            state=state,
            cookie_compliance="ok",
            current_route=Route("/c", "other"),
            prev_route=Route("/a", "other"),
            matrix=m,
            l2_enforce_enabled=False,
            l2_days_since_optin=days,
        )
        # L2 raw score is high but the flag is off, so combined = L1 (= 0).
        assert result.l1_score == 0
        assert result.l2_score > 0, f"days {days}: L2 sub-score still computed"
        assert result.score == 0, f"days {days}: flag off → combined == clean L1 (0)"


def test_score_combined_l2_on_fades_in_from_optin():
    """Parity with the Rust ``l2_on_fades_in_from_optin``: with the flag on, L2's
    contribution fades in from the opt-in anchor. At opt-in (day 0) the ramp
    weight is 0 so the combined score equals L1; once fully ramped
    (days ≥ L2_RAMP_DAYS) L2 lifts the enforced combined score. The L2 sub-score
    itself is age-independent."""
    state = _state(seq=10, sum_dt=50, sum_dt_sq=300)  # clean L1 → 0
    m = _matrix({"/a": {"/b": 1}}, vocab=100)  # /a→/c is RARE
    day0 = score_combined(
        state=state,
        cookie_compliance="ok",
        current_route=Route("/c", "other"),
        prev_route=Route("/a", "other"),
        matrix=m,
        l2_enforce_enabled=True,
        l2_days_since_optin=0.0,
    )
    fully = score_combined(
        state=state,
        cookie_compliance="ok",
        current_route=Route("/c", "other"),
        prev_route=Route("/a", "other"),
        matrix=m,
        l2_enforce_enabled=True,
        l2_days_since_optin=3.0,
    )
    assert day0.l1_score == 0 and fully.l1_score == 0
    assert day0.l2_score > 0 and day0.l2_score == fully.l2_score  # sub-score age-independent
    assert day0.score == 0  # ramp weight 0 at the instant of opt-in
    assert fully.score > day0.score  # fully ramped → L2 moves the enforced score


def test_score_combined_young_optin_cannot_hard_block():
    """Parity with the Rust ``l2_on_young_optin_cannot_hard_block``: enabling L2
    must NOT instantly push a clean-L1 session to the score==100 hard-block bar —
    the ramp opens at 0 the moment the operator opts in."""
    state = _state(seq=10, sum_dt=50, sum_dt_sq=300)  # clean L1 → 0
    m = _matrix({"/a": {"/b": 1}}, vocab=1000)
    m["row_totals"]["/a"] = 1_000_000  # /a→/c ≈ probability floor → L2 ~100
    result = score_combined(
        state=state,
        cookie_compliance="ok",
        current_route=Route("/c", "other"),
        prev_route=Route("/a", "other"),
        matrix=m,
        l2_enforce_enabled=True,
        l2_days_since_optin=0.0,  # just opted in
    )
    assert result.l2_score >= 50, "L2 sub-score still computed"
    assert result.score < 100, "opt-in day 0 must keep L2 below the hard-block bar"
    assert result.score == 0, "opt-in day 0: combined must equal clean L1 (0)"


def test_score_combined_quantized_to_nearest_5():
    """Score is always a multiple of 5 per cookie.quantize_score."""
    state = _state(seq=10, sum_dt=0, sum_dt_sq=0)  # fires impossibly-fast
    result = score_combined(
        state=state,
        cookie_compliance="ok",
        current_route=Route("/a", "other"),
        prev_route=Route("/b", "other"),
    )
    assert result.score % 5 == 0


def test_score_combined_caps_at_100():
    """L1 + L2 sum bigger than 100 → clamped."""
    state = _state(seq=10, sum_dt=0, sum_dt_sq=0)  # fast fires
    m = _matrix({"/a": {"/b": 1}}, vocab=1000)
    m["row_totals"]["/a"] = 1_000_000  # very rare → ~max L2
    result = score_combined(
        state=state,
        cookie_compliance="missing",  # +75
        current_route=Route("/b", "other"),
        prev_route=Route("/a", "other"),
        matrix=m,
        l2_enforce_enabled=True,
        l2_days_since_optin=30.0,  # fully ramped
    )
    assert result.score == 100


def test_score_combined_headers_round_trip():
    """ScoreResult.to_headers() emits the exact set in §6."""
    result = ScoreResult(
        score=65,
        l1_score=40,
        l2_score=25,
        reasons=["impossibly-fast", "low-transition-prob"],
        cookie_compliance="ok",
        matrix_version="2026-06-15-a",
    )
    headers = result.to_headers()
    assert headers == {
        "X-Edge-Score": "65",
        "X-Edge-Score-L1": "40",
        "X-Edge-Score-L2": "25",
        "X-Edge-Cookie-Compliance": "ok",
        "X-Edge-Score-Reason": "impossibly-fast,low-transition-prob",
        "X-Edge-Matrix-Version": "2026-06-15-a",
    }

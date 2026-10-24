"""Tests for backend.scoring.scorer — Layer 2 transition evaluation."""

from __future__ import annotations

import pytest

from backend.scoring.normalize import Route
from backend.scoring.scorer import (
    _l2_score_from_trans_prob,
    _transition_prob,
    score_layer2,
)

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

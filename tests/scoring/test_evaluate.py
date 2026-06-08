"""Tests for backend.scoring.evaluate — ROC-AUC quality gate."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from backend.scoring.evaluate import (
    _compute_auc,
    _session_l2_score,
    evaluate,
)
from backend.scoring.matrix import build_matrix

UTC = UTC


def _session(urls: list[str], dwell_s: float = 5.0) -> dict:
    base = datetime(2026, 5, 15, 10, 0, 0, tzinfo=UTC)
    return {
        "session_id": "test",
        "events": [
            {
                "ts": (base + timedelta(seconds=int(i * dwell_s))).isoformat(timespec="seconds"),
                "url": url,
                "method": "GET",
                "status": 200,
                "referer": "",
                "ttfb_ms": 50.0,
                "country": "US",
                "asn": 1,
            }
            for i, url in enumerate(urls)
        ],
    }


def _matrix_from(funnels: list[tuple[list[str], int]]) -> dict:
    """Build a matrix by repeating each funnel path N times. Returns the
    serialized dict form the scorer consumes."""
    import io
    import json

    from backend.scoring.matrix import write_matrix

    sessions = []
    for path, repeats in funnels:
        for _ in range(repeats):
            sessions.append(_session(path))
    matrix, _ = build_matrix(sessions)
    buf = io.StringIO()
    write_matrix(matrix, "test-v1", buf)
    return json.loads(buf.getvalue())


# ── _compute_auc ─────────────────────────────────────────────────────────────


def test_auc_perfect_separation_returns_one():
    scores = [(0, "good"), (10, "good"), (90, "bad"), (100, "bad")]
    assert _compute_auc(scores) == 1.0


def test_auc_inverted_returns_zero():
    """If bads score lower than goods, AUC = 0 — model is anti-correlated."""
    scores = [(90, "good"), (100, "good"), (0, "bad"), (10, "bad")]
    assert _compute_auc(scores) == 0.0


def test_auc_random_returns_half():
    scores = [(50, "good"), (50, "good"), (50, "bad"), (50, "bad")]
    assert _compute_auc(scores) == 0.5


def test_auc_partial_overlap():
    """2 goods at [10,20], 2 bads at [15,30]:
    comparisons (good vs bad):
      10 vs 15 → win (10 < 15)
      10 vs 30 → win
      20 vs 15 → loss
      20 vs 30 → win
    3/4 = 0.75"""
    scores = [(10, "good"), (20, "good"), (15, "bad"), (30, "bad")]
    assert _compute_auc(scores) == 0.75


def test_auc_no_negatives_returns_half():
    scores = [(10, "good"), (50, "good")]
    assert _compute_auc(scores) == 0.5


def test_auc_no_positives_returns_half():
    scores = [(10, "bad"), (50, "bad")]
    assert _compute_auc(scores) == 0.5


# ── _session_l2_score: max-over-transitions semantics ────────────────────────


def test_session_l2_score_takes_maximum_transition():
    """Two clean transitions plus one rare one — the max wins, not the
    average. Trained matrix sees /a only ever going to /b (99 times,
    /a→/c only once) so /a→/c is rare relative to its row total."""
    matrix = _matrix_from(
        [
            (["/a", "/b"], 99),  # /a→/b dominates
            (["/a", "/c"], 1),  # /a→/c is rare
        ]
    )
    # Within one session: /a→/b (common) followed by /a→/c (rare).
    # We rely on max-takes-all to surface the rare one.
    s = _session(["/a", "/b", "/a", "/c"])
    score, n = _session_l2_score(s, matrix)
    assert n == 3
    # /a→/c with count 1 out of 100 → P_smoothed ≈ 0.014 → score in the 30-40 band.
    assert score >= 25, f"expected the rare /a→/c to surface, got max score {score}"


def test_session_l2_score_single_event_zero():
    matrix = _matrix_from([(["/a", "/b"], 50)])
    score, n = _session_l2_score(_session(["/only"]), matrix)
    assert n == 0
    assert score == 0


# ── evaluate: end-to-end ─────────────────────────────────────────────────────


def test_evaluate_returns_high_auc_on_clear_separation():
    """Goods follow the trained funnel; bads jump directly to /admin."""
    matrix = _matrix_from(
        [
            (["/home", "/products", "/cart", "/checkout"], 200),
        ]
    )
    good = _session(["/home", "/products", "/cart", "/checkout"])
    bad = _session(["/home", "/admin/secrets"])
    result = evaluate(matrix, [(good, "good"), (bad, "bad")])
    assert result.n_good == 1
    assert result.n_bad == 1
    assert result.auc == 1.0
    assert result.passed


def test_evaluate_below_threshold_fails():
    matrix = _matrix_from([(["/a", "/b"], 10)])
    # Both score similarly → AUC = 0.5
    s1 = _session(["/a", "/b"])
    s2 = _session(["/a", "/b"])
    result = evaluate(matrix, [(s1, "good"), (s2, "bad")], min_auc=0.85)
    assert result.auc == 0.5
    assert not result.passed


def test_evaluate_rejects_unknown_label():
    matrix = _matrix_from([(["/a", "/b"], 10)])
    # 'neutral' is now allowed (admin can mark uncertain sessions); only
    # truly bogus labels should raise.
    with pytest.raises(ValueError, match="unexpected label"):
        evaluate(matrix, [(_session(["/a", "/b"]), "maybe")])


def test_evaluate_accepts_neutral_excluded_from_auc():
    """Neutral rows must appear in per_session but NOT count toward AUC —
    they're intentionally uncertain and shouldn't bias precision/recall."""
    matrix = _matrix_from([(["/a", "/b"], 10)])
    s_good = _session(["/a", "/b"])
    s_bad = _session(["/x", "/y"])
    s_neut = _session(["/q", "/r"])
    result = evaluate(matrix, [(s_good, "good"), (s_bad, "bad"), (s_neut, "neutral")])
    assert result.n_good == 1
    assert result.n_bad == 1
    # All three sessions are returned for display.
    assert len(result.per_session) == 3
    assert {r.label for r in result.per_session} == {"good", "bad", "neutral"}


def test_evaluate_per_session_records_complete():
    matrix = _matrix_from([(["/a", "/b"], 10)])
    s = _session(["/a", "/b", "/c"])
    result = evaluate(matrix, [(s, "good")])
    assert len(result.per_session) == 1
    rec = result.per_session[0]
    assert rec.label == "good"
    assert rec.transition_count == 2  # /a→/b, /b→/c
    assert isinstance(rec.l2_score, int)


def test_evaluate_summary_string_contains_pass_fail():
    matrix = _matrix_from([(["/a", "/b"], 10)])
    good = _session(["/a", "/b"])
    bad = _session(["/home", "/admin"])
    result = evaluate(matrix, [(good, "good"), (bad, "bad")])
    summary = result.summary()
    assert "AUC=" in summary
    assert "PASS" in summary or "FAIL" in summary
    assert "n_good=1" in summary
    assert "n_bad=1" in summary


# ── evaluate_from_persisted_scores ───────────────────────────────────────────


def test_evaluate_from_persisted_scores_uses_max_edge_score_not_l2_recompute():
    """The persisted-score evaluator ignores the matrix entirely and just
    uses the max_edge_score the live scorer already wrote into DuckDB.
    A single-URL bot session with 0 events but max_edge_score=75 (the
    cookie-missing flag fired at the edge) gets counted properly — the
    L2-only evaluator would have scored it 0 and broken AUC."""
    from backend.scoring.evaluate import evaluate_from_persisted_scores

    bot_single_hit = {"session_id": "bot1", "events": [], "max_edge_score": 75}
    real_browser = {"session_id": "good1", "events": [], "max_edge_score": 0}

    result = evaluate_from_persisted_scores([(bot_single_hit, "bad"), (real_browser, "good")])
    # good_score < bad_score → AUC = 1.0
    assert result.auc == 1.0
    assert result.n_good == 1
    assert result.n_bad == 1
    assert result.passed is True


def test_evaluate_from_persisted_scores_drops_sessions_without_score():
    """Sessions where DuckDB doesn't have a persisted edge_score (sid
    never ingested, rotated away) get dropped from the AUC instead of
    silently being scored 0."""
    from backend.scoring.evaluate import evaluate_from_persisted_scores

    scored = {"session_id": "s1", "events": [], "max_edge_score": 80}
    not_scored = {"session_id": "s2", "events": [], "max_edge_score": None}

    result = evaluate_from_persisted_scores([(scored, "bad"), (not_scored, "good")])
    # Only one session usable, so degenerate AUC (0.5)
    assert result.n_bad == 1
    assert result.n_good == 0
    assert result.auc == 0.5


def test_evaluate_per_reason_splits_buckets_by_atom():
    from backend.scoring.evaluate import evaluate_per_reason

    sessions = [
        ({"session_id": "b1", "max_edge_score": 80, "events": [{"edge_score_reason": "cookie-missing"}]}, "bad"),
        (
            {
                "session_id": "b2",
                "max_edge_score": 75,
                "events": [{"edge_score_reason": "cookie-missing,impossibly-fast"}],
            },
            "bad",
        ),
        ({"session_id": "b3", "max_edge_score": 75, "events": [{"edge_score_reason": "cookie-missing"}]}, "bad"),
        ({"session_id": "g1", "max_edge_score": 0, "events": [{"edge_score_reason": "cookie-missing"}]}, "good"),
        ({"session_id": "g2", "max_edge_score": 5, "events": [{"edge_score_reason": "cookie-missing"}]}, "good"),
        ({"session_id": "g3", "max_edge_score": 10, "events": [{"edge_score_reason": "cookie-missing"}]}, "good"),
    ]
    result = evaluate_per_reason(sessions)
    by_reason = {b["reason"]: b for b in result["buckets"]}

    cm = by_reason["cookie-missing"]
    assert cm["has_min_samples"] is True
    assert cm["n_good"] == 3 and cm["n_bad"] == 3
    assert cm["auc"] == 1.0

    # impossibly-fast only tripped in b2 → 0 good + 1 bad → under min
    fast = by_reason["impossibly-fast"]
    assert fast["has_min_samples"] is False
    assert "auc" not in fast


def test_evaluate_per_reason_returns_buckets_for_every_known_atom():
    from backend.scoring.evaluate import _KNOWN_REASON_ATOMS, evaluate_per_reason

    result = evaluate_per_reason([])
    assert len(result["buckets"]) == len(_KNOWN_REASON_ATOMS)
    assert all(b["has_min_samples"] is False for b in result["buckets"])


def test_evaluate_from_persisted_scores_excludes_neutral_from_auc():
    from backend.scoring.evaluate import evaluate_from_persisted_scores

    result = evaluate_from_persisted_scores(
        [
            ({"session_id": "g", "max_edge_score": 10}, "good"),
            ({"session_id": "b", "max_edge_score": 80}, "bad"),
            ({"session_id": "n", "max_edge_score": 50}, "neutral"),
        ]
    )
    assert result.n_good == 1
    assert result.n_bad == 1
    # Neutral DOES appear in per_session for display, but not in AUC
    assert len(result.per_session) == 3
    assert result.auc == 1.0

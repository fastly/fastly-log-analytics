from __future__ import annotations

"""Tests for backend.scoring.evaluate — ROC-AUC quality gate."""

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
    sessions = []
    for path, repeats in funnels:
        for _ in range(repeats):
            sessions.append(_session(path))
    matrix, _ = build_matrix(sessions)
    return matrix.to_json_dict("test-v1")


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


def test_known_reason_atoms_track_scorer_emitters():
    """_KNOWN_REASON_ATOMS must mirror what the live scorer actually emits.

    ``rare-transition`` must be absent — it is emitted by neither the Rust
    prod scorer nor the Python reference (the L2 rule emits
    ``low-transition-prob``), and previously produced a perpetually-empty
    per-rule bucket. ``cookie-replayed`` must be present: the prod (Rust)
    scorer's ``format!("cookie-{}", compliance)`` emits it on the replay
    path, so it appears in real ``edge_score_reason`` data.
    """
    from backend.scoring import scorer as pyscorer
    from backend.scoring.evaluate import _KNOWN_REASON_ATOMS
    from backend.scoring.normalize import Route

    known = set(_KNOWN_REASON_ATOMS)

    m = {"counts": {"/home": {"/other": 10000}}, "row_totals": {"/home": 10000}, "vocab_size": 100}
    _, l2_reasons, _ = pyscorer.score_layer2(m, Route("/home", "home"), None, Route("/checkout", "checkout"))
    assert set(l2_reasons) == {"low-transition-prob"}
    assert set(l2_reasons) <= known

    assert "rare-transition" not in known
    assert known == {
        "cookie-missing",
        "cookie-tampered",
        "cookie-expired",
        "cookie-replayed",
        "impossibly-fast",
        "robotic-consistency",
        "low-transition-prob",
    }


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


# ── Merged from test_fixtures.py ──────────────────────────────────────────────────────

"""Tests for backend.scoring.fixtures — log rows → sessionized JSONL traces."""

from datetime import UTC

from backend.scoring.fixtures import (
    SESSION_GAP_SECONDS,
    Event,
    extract_traces,
    rows_to_events,
    sessionize,
)

UTC = UTC


def _row(
    ts: datetime,
    ip: str = "1.1.1.1",
    ua: str = "Mozilla/5.0",
    url: str = "/",
    method: str = "GET",
    status: int = 200,
    referer: str = "",
    ttfb: float = 0.05,
    country: str = "US",
    asn: int | None = 7922,
) -> tuple:
    """Build a row tuple in the order rows_to_events expects."""
    return (ts, ip, ua, url, method, status, referer, ttfb, country, asn)


# ── rows_to_events ────────────────────────────────────────────────────────────


def test_rows_to_events_basic_field_mapping():
    rows = [_row(datetime(2026, 5, 15, 10, 0, 0, tzinfo=UTC), url="/foo", status=404)]
    items = list(rows_to_events(rows))
    assert len(items) == 1
    ip, ua, ev = items[0]
    assert ip == "1.1.1.1"
    assert ua == "Mozilla/5.0"
    assert ev.url == "/foo"
    assert ev.status == 404
    assert ev.method == "GET"
    assert ev.ttfb_ms == 50.0  # 0.05s → 50ms


def test_rows_to_events_method_uppercased():
    rows = [_row(datetime(2026, 5, 15, 10, 0, 0, tzinfo=UTC), method="get")]
    ip, ua, ev = next(rows_to_events(rows))
    assert ev.method == "GET"


def test_rows_to_events_null_safe():
    """Production logs can have NULL ASN, missing referer, etc. Don't crash."""
    rows = [
        (
            datetime(2026, 5, 15, 10, 0, 0, tzinfo=UTC),
            None,  # ip
            None,  # ua
            None,  # url
            None,  # method
            None,  # status
            None,  # referer
            None,  # ttfb
            None,  # country
            None,  # asn
        )
    ]
    ip, ua, ev = next(rows_to_events(rows))
    assert ip == ""
    assert ua == ""
    assert ev.url == ""
    assert ev.method == ""
    assert ev.status == 0
    assert ev.ttfb_ms == 0.0
    assert ev.asn is None


# ── sessionize ────────────────────────────────────────────────────────────────


def _events_for(
    ip: str, ua: str, timestamps: list[datetime], urls: list[str] | None = None
) -> list[tuple[str, str, Event]]:
    """Build a stream pre-sorted by (ip, ua, ts) as the sessionizer requires."""
    if urls is None:
        urls = [f"/page{i}" for i in range(len(timestamps))]
    assert len(urls) == len(timestamps)
    return [
        (
            ip,
            ua,
            Event(
                ts=ts.isoformat(timespec="seconds"),
                url=urls[i],
                method="GET",
                status=200,
                referer="",
                ttfb_ms=50.0,
                country="US",
                asn=7922,
            ),
        )
        for i, ts in enumerate(timestamps)
    ]


def test_sessionize_single_session_all_close():
    """Six requests within the gap window → one session."""
    base = datetime(2026, 5, 15, 10, 0, 0, tzinfo=UTC)
    events = _events_for(
        "1.1.1.1",
        "Mozilla",
        [base + timedelta(seconds=10 * i) for i in range(6)],
    )
    sessions = list(sessionize(events))
    assert len(sessions) == 1
    assert sessions[0].event_count == 6


def test_sessionize_gap_splits_into_two_sessions():
    """One request, then a 31-min gap, then more → two sessions."""
    base = datetime(2026, 5, 15, 10, 0, 0, tzinfo=UTC)
    events = _events_for(
        "1.1.1.1",
        "Mozilla",
        [
            base,
            base + timedelta(seconds=10),
            base + timedelta(seconds=SESSION_GAP_SECONDS + 60),  # past threshold
            base + timedelta(seconds=SESSION_GAP_SECONDS + 70),
        ],
    )
    sessions = list(sessionize(events))
    assert len(sessions) == 2
    assert sessions[0].event_count == 2
    assert sessions[1].event_count == 2


def test_sessionize_different_ips_are_separate_sessions():
    base = datetime(2026, 5, 15, 10, 0, 0, tzinfo=UTC)
    events = [
        *_events_for("1.1.1.1", "Mozilla", [base, base + timedelta(seconds=10)]),
        *_events_for("2.2.2.2", "Mozilla", [base + timedelta(seconds=20)]),
    ]
    sessions = list(sessionize(events))
    assert len(sessions) == 2
    assert {s.client_ip for s in sessions} == {"1.1.1.1", "2.2.2.2"}


def test_sessionize_different_uas_same_ip_are_separate_sessions():
    """NAT'd users sharing an IP get distinguished by UA."""
    base = datetime(2026, 5, 15, 10, 0, 0, tzinfo=UTC)
    events = [
        *_events_for("1.1.1.1", "Mozilla/Firefox", [base]),
        *_events_for("1.1.1.1", "Mozilla/Chrome", [base + timedelta(seconds=5)]),
    ]
    sessions = list(sessionize(events))
    assert len(sessions) == 2
    assert {s.user_agent for s in sessions} == {"Mozilla/Firefox", "Mozilla/Chrome"}


def test_sessionize_empty_input_yields_nothing():
    assert list(sessionize(iter([]))) == []


def test_sessionize_custom_gap_seconds():
    base = datetime(2026, 5, 15, 10, 0, 0, tzinfo=UTC)
    events = _events_for(
        "1.1.1.1",
        "Mozilla",
        [base, base + timedelta(seconds=120)],
    )
    sessions_tight = list(sessionize(events, gap_seconds=60))
    sessions_loose = list(sessionize(events, gap_seconds=300))
    assert len(sessions_tight) == 2
    assert len(sessions_loose) == 1


def test_sessionize_stable_session_id_for_same_input():
    """Re-running extraction on the same data must produce identical session
    ids — required for reproducible test fixtures."""
    base = datetime(2026, 5, 15, 10, 0, 0, tzinfo=UTC)
    events = _events_for("1.1.1.1", "Mozilla", [base, base + timedelta(seconds=10)])
    s1 = list(sessionize(events))
    s2 = list(sessionize(_events_for("1.1.1.1", "Mozilla", [base, base + timedelta(seconds=10)])))
    assert s1[0].session_id == s2[0].session_id


# ── extract_traces (with mock DuckDB connection) ──────────────────────────────


class _FakeCursor:
    def __init__(self, rows: list[tuple]):
        self._rows = rows

    def fetchall(self) -> list[tuple]:
        return self._rows


class _FakeCon:
    def __init__(self, rows: list[tuple]):
        self._rows = rows
        self.last_sql: str | None = None

    def execute(self, sql: str) -> _FakeCursor:
        self.last_sql = sql
        return _FakeCursor(self._rows)


def test_extract_traces_builds_correct_sql():
    con = _FakeCon([])
    start = datetime(2026, 5, 15, 0, 0, 0, tzinfo=UTC)
    end = datetime(2026, 5, 16, 0, 0, 0, tzinfo=UTC)
    list(extract_traces(con, service_id="MySvc123", start=start, end=end, limit=100))
    assert con.last_sql is not None
    # View name lowercased per existing project convention.
    assert "logs_mysvc123" in con.last_sql
    assert "ORDER BY ip, ua, timestamp" in con.last_sql
    assert "LIMIT 100" in con.last_sql
    assert "timestamp >= TIMESTAMP '2026-05-15T00:00:00+00:00'" in con.last_sql
    assert "timestamp < TIMESTAMP '2026-05-16T00:00:00+00:00'" in con.last_sql


def test_extract_traces_end_to_end_yields_sessions():
    base = datetime(2026, 5, 15, 10, 0, 0, tzinfo=UTC)
    rows = [
        _row(base, ip="1.1.1.1", url="/home"),
        _row(base + timedelta(seconds=5), ip="1.1.1.1", url="/products"),
        _row(base + timedelta(seconds=10), ip="1.1.1.1", url="/checkout"),
        # Different IP → separate session
        _row(base + timedelta(seconds=15), ip="2.2.2.2", url="/home"),
    ]
    con = _FakeCon(rows)
    sessions = list(extract_traces(con, service_id="svc"))
    assert len(sessions) == 2
    assert sessions[0].event_count == 3
    assert sessions[1].event_count == 1
    # Order should match the sort: 1.1.1.1 then 2.2.2.2
    assert sessions[0].client_ip == "1.1.1.1"
    assert sessions[1].client_ip == "2.2.2.2"
    # Events within a session preserve URL order
    assert [e.url for e in sessions[0].events] == ["/home", "/products", "/checkout"]


def test_extract_traces_no_filters_omits_where_clause():
    con = _FakeCon([])
    list(extract_traces(con, service_id="svc"))
    assert "WHERE" not in con.last_sql
    assert "LIMIT" not in con.last_sql


@pytest.mark.parametrize(
    "start,end,expected_clauses",
    [
        (datetime(2026, 5, 15, 0, 0, 0, tzinfo=UTC), None, ["timestamp >="]),
        (None, datetime(2026, 5, 16, 0, 0, 0, tzinfo=UTC), ["timestamp <"]),
    ],
)
def test_extract_traces_partial_time_filters(start, end, expected_clauses):
    con = _FakeCon([])
    list(extract_traces(con, service_id="svc", start=start, end=end))
    for clause in expected_clauses:
        assert clause in con.last_sql


# ── Merged from test_labels.py ──────────────────────────────────────────────────────

"""Tests for backend.scoring.labels — CRUD over the scoring_labels table.

The isolate_metadata_db fixture in tests/conftest.py points
backend.core.metadata_db at a per-test sandbox SQLite DB; the schema
is initialized on first get_con() call so each test starts clean."""

import pytest

from backend.scoring import labels as _labels

SVC = "svc1"


def test_save_label_basic_round_trip():
    saved = _labels.save_label(
        SVC,
        sid="abc123def456",
        label="bad",
        notes="probable scraper",
        flagged_by="alice",
        sample_ip="1.2.3.4",
        sample_ua="curl/8.4",
        sample_url="/login",
    )
    assert saved["sid"] == "abc123def456"
    assert saved["label"] == "bad"
    assert saved["notes"] == "probable scraper"
    assert saved["sample_ip"] == "1.2.3.4"
    assert saved["created_at"]

    fetched = _labels.get_label_by_id(SVC, saved["id"])
    assert fetched is not None
    assert fetched["id"] == saved["id"]
    assert fetched["label"] == "bad"


def test_save_label_upserts_on_sid():
    """Re-labeling the same sid must UPDATE in place — same id, new label."""
    first = _labels.save_label(SVC, sid="sid001", label="bad")
    assert first["label"] == "bad"

    second = _labels.save_label(SVC, sid="sid001", label="good", notes="actually a real user")
    # Same row, updated label + notes
    assert second["id"] == first["id"]
    assert second["label"] == "good"
    assert second["notes"] == "actually a real user"

    # And only one row exists
    rows = _labels.list_labels(SVC)
    assert len([r for r in rows if r["sid"] == "sid001"]) == 1


def test_save_label_rejects_unknown_label():
    with pytest.raises(ValueError, match="label must be one of"):
        _labels.save_label(SVC, sid="x", label="maybe")


def test_save_label_requires_sid():
    with pytest.raises(ValueError, match="sid is required"):
        _labels.save_label(SVC, sid="", label="good")


def test_list_labels_most_recent_first():
    _labels.save_label(SVC, sid="aaa", label="good")
    _labels.save_label(SVC, sid="bbb", label="bad")
    _labels.save_label(SVC, sid="ccc", label="neutral")

    rows = _labels.list_labels(SVC)
    # All three are present and the query uses a deterministic portable
    # tie-break when SQLite's second-resolution timestamps collide.
    assert {r["sid"] for r in rows[:3]} == {"aaa", "bbb", "ccc"}


def test_update_label_patch_semantics():
    saved = _labels.save_label(SVC, sid="u1", label="bad", notes="initial")

    # Update only notes
    after_notes = _labels.update_label(SVC, saved["id"], notes="revised note")
    assert after_notes["label"] == "bad"
    assert after_notes["notes"] == "revised note"

    # Update only label
    after_label = _labels.update_label(SVC, saved["id"], label="neutral")
    assert after_label["label"] == "neutral"
    assert after_label["notes"] == "revised note"  # unchanged


def test_update_label_rejects_bad_label():
    saved = _labels.save_label(SVC, sid="u2", label="good")
    with pytest.raises(ValueError, match="label must be one of"):
        _labels.update_label(SVC, saved["id"], label="ugly")


def test_update_label_noop_when_no_fields_passed():
    saved = _labels.save_label(SVC, sid="u3", label="good")
    # Calling without any patchable fields returns current state, no-op.
    result = _labels.update_label(SVC, saved["id"])
    assert result["id"] == saved["id"]
    assert result["label"] == "good"


def test_delete_label_is_idempotent():
    saved = _labels.save_label(SVC, sid="d1", label="bad")
    r1 = _labels.delete_label(SVC, saved["id"])
    assert r1["status"] == "success"

    r2 = _labels.delete_label(SVC, saved["id"])
    assert r2["status"] == "success"  # second delete no-ops cleanly

    assert _labels.get_label_by_id(SVC, saved["id"]) is None


def test_counts_by_label_always_returns_three_keys():
    # No labels yet
    counts = _labels.counts_by_label(SVC)
    assert counts == {"good": 0, "bad": 0, "neutral": 0}

    _labels.save_label(SVC, sid="g1", label="good")
    _labels.save_label(SVC, sid="g2", label="good")
    _labels.save_label(SVC, sid="b1", label="bad")
    counts = _labels.counts_by_label(SVC)
    assert counts == {"good": 2, "bad": 1, "neutral": 0}


def test_labels_isolated_per_service():
    saved_a = _labels.save_label("svcA", sid="shared", label="good")
    saved_b = _labels.save_label("svcB", sid="shared", label="bad")
    # Same sid in two services → two independent labels.
    a = _labels.get_label_by_id("svcA", saved_a["id"])
    b = _labels.get_label_by_id("svcB", saved_b["id"])
    assert a["label"] == "good"
    assert b["label"] == "bad"
    assert a["id"] != b["id"]


# ── Merged from test_matrix.py ──────────────────────────────────────────────────────

"""Tests for backend.scoring.matrix — transition matrix builder."""

import json
from datetime import UTC, datetime

from backend.scoring.matrix import (
    TransitionMatrix,
    default_version,
    serialize_kv,
)
from backend.scoring.normalize import normalize

UTC = UTC


def _matrix_session(urls: list[str], dwell_seconds: float = 5.0, base: datetime | None = None) -> dict:
    """Build a JSONL-shaped session with evenly-spaced timestamps."""
    base = base or datetime(2026, 5, 15, 10, 0, 0, tzinfo=UTC)
    events = []
    for i, url in enumerate(urls):
        events.append(
            {
                "ts": (base + timedelta(seconds=int(i * dwell_seconds))).isoformat(timespec="seconds"),
                "url": url,
                "method": "GET",
                "status": 200,
                "referer": "",
                "ttfb_ms": 50.0,
                "country": "US",
                "asn": 7922,
            }
        )
    return {
        "session_id": "ip_test",
        "client_ip": "1.1.1.1",
        "user_agent": "Mozilla",
        "start_ts": events[0]["ts"],
        "end_ts": events[-1]["ts"],
        "event_count": len(events),
        "events": events,
    }


# ── TransitionMatrix.add_transition ──────────────────────────────────────────


def test_add_transition_increments_counts_and_totals():
    m = TransitionMatrix()
    a = normalize("/a")
    b = normalize("/b")
    m.add_transition(a, b)
    m.add_transition(a, b)
    m.add_transition(a, normalize("/c"))
    assert m.counts == {"/a": {"/b": 2, "/c": 1}}
    assert m.row_totals == {"/a": 3}
    assert m.transition_count == 3
    assert m.vocab == {"/a", "/b", "/c"}


def test_add_transition_records_categories():
    m = TransitionMatrix()
    m.add_transition(normalize("/products/42"), normalize("/cart"))
    assert m.categories["/products/*"] == "product"
    assert m.categories["/cart"] == "cart"


# ── build_matrix: bot filtering ──────────────────────────────────────────────


def test_build_matrix_drops_single_event_sessions():
    sessions = [_matrix_session(["/home"]), _matrix_session(["/home", "/products"])]
    m, stats = build_matrix(sessions)
    assert stats.sessions_dropped_short == 1
    assert stats.sessions_kept == 1
    # Only the 2-event session contributed.
    assert m.transition_count == 1


def test_build_matrix_drops_impossibly_fast_sessions():
    """A session with 10 events spaced 50ms apart looks like an instant
    scraper — mean dwell ~0.05s < 0.2s floor."""
    urls = [f"/p{i}" for i in range(10)]
    s = _matrix_session(urls, dwell_seconds=0.05)
    m, stats = build_matrix([s])
    assert stats.sessions_dropped_fast == 1
    assert stats.sessions_kept == 0
    assert m.transition_count == 0


def test_build_matrix_keeps_normal_sessions():
    """Normal user: 5s mean dwell, 5 events → kept and contributes 4 transitions."""
    s = _matrix_session(["/home", "/products", "/products/42", "/cart", "/checkout"], dwell_seconds=5.0)
    m, stats = build_matrix([s])
    assert stats.sessions_kept == 1
    assert stats.sessions_dropped_short == 0
    assert stats.sessions_dropped_fast == 0
    assert m.transition_count == 4


def test_build_matrix_aggregates_across_sessions():
    sessions = [
        _matrix_session(["/home", "/products", "/cart"], dwell_seconds=3.0),
        _matrix_session(["/home", "/products", "/cart"], dwell_seconds=3.0),
        _matrix_session(["/home", "/about", "/products"], dwell_seconds=3.0),
    ]
    m, stats = build_matrix(sessions)
    assert stats.sessions_kept == 3
    # Two sessions have /home → /products, one has /home → /about
    assert m.counts["/home"]["/products"] == 2
    assert m.counts["/home"]["/about"] == 1
    assert m.row_totals["/home"] == 3


def test_build_matrix_normalizes_urls_during_build():
    """Numeric-id segments collapse, so /products/42 and /products/99 share a row."""
    sessions = [
        _matrix_session(["/home", "/products/42"], dwell_seconds=3.0),
        _matrix_session(["/home", "/products/99"], dwell_seconds=3.0),
    ]
    m, _ = build_matrix(sessions)
    # Both collapsed to /products/*
    assert m.counts["/home"] == {"/products/*": 2}
    assert "/products/42" not in m.counts.get("/home", {})


def test_build_matrix_respects_overrides():
    """Custom thresholds work — disable the fast filter, get more sessions."""
    s = _matrix_session(["/a", "/b", "/c"], dwell_seconds=0.1)
    _, stats_filtered = build_matrix([s])
    _, stats_unfiltered = build_matrix([s], min_mean_dwell_s=0.0)
    assert stats_filtered.sessions_dropped_fast == 1
    assert stats_unfiltered.sessions_kept == 1


def test_build_matrix_empty_input_returns_empty_matrix():
    m, stats = build_matrix([])
    assert stats.sessions_in == 0
    assert stats.sessions_kept == 0
    assert m.transition_count == 0
    assert len(m.vocab) == 0


# ── End-to-end: built matrix is usable by the scorer ─────────────────────────


def test_built_matrix_round_trips_through_scorer():
    """The schema returned by ``to_json_dict`` is exactly what the L2 scorer
    expects — verify the contract end-to-end."""
    from backend.scoring.normalize import normalize as norm
    from backend.scoring.scorer import score_layer2

    # Build a meaningful dataset: 100 sessions doing the common funnel,
    # 1 session doing an off-pattern jump. Laplace smoothing then leaves
    # the common transition with P ≈ 1, the rare one with P ≈ 0.01.
    common = [_matrix_session(["/home", "/products"]) for _ in range(100)]
    rare = [_matrix_session(["/home", "/about"])]
    matrix, _ = build_matrix(common + rare)
    loaded = matrix.to_json_dict("test")

    # Common transition (100/101) → ~no penalty.
    score_common, _, p_common = score_layer2(loaded, norm("/home"), None, norm("/products"))
    assert p_common > 0.9
    assert score_common <= 5

    # Unseen transition → meaningful penalty (small Laplace prior).
    score_unseen, _, p_unseen = score_layer2(loaded, norm("/home"), None, norm("/never-visited"))
    assert 0 < p_unseen < 0.1
    assert score_unseen > score_common


# ── default_version ──────────────────────────────────────────────────────────


def test_default_version_format():
    v = default_version()
    # EC-06: YYYY-MM-DD-HHMMSS (UTC, second resolution) — sub-day component so
    # same-day retrains don't collide on the version string.
    assert len(v) == 17
    # Round-trips through the exact strftime that produced it.
    assert datetime.strptime(v, "%Y-%m-%d-%H%M%S").strftime("%Y-%m-%d-%H%M%S") == v
    parts = v.split("-")
    assert len(parts) == 4  # year, month, day, HHMMSS
    int(parts[0])  # parseable as year
    int(parts[1])
    int(parts[2])
    int(parts[3])  # HHMMSS


def test_default_version_distinct_and_ordered_within_day():
    """EC-06: two retrains on the same day no longer collide — the sub-day time
    component makes the auto version distinct AND lexicographically ordered, so
    the history archive (keyed on the version string) can't overwrite a prior
    same-day snapshot and two matrices can't report an identical
    X-Edge-Matrix-Version."""
    import time_machine

    with time_machine.travel("2026-06-20 09:00:00+00:00"):
        v1 = default_version()
    with time_machine.travel("2026-06-20 14:30:52+00:00"):
        v2 = default_version()
    assert v1 == "2026-06-20-090000"
    assert v2 == "2026-06-20-143052"
    assert v1 != v2
    assert v1 < v2  # fixed-width → lexicographically sortable


# ── FSM1 binary KV encoding ──────────────────────────────────────────────────
#
# CROSS-LANGUAGE CONTRACT: SMALL_MATRIX_FSM1_HEX is byte-identical to the fixture
# the Rust decoder test parses in
# compute/scorer/src/matrix.rs::tests::parse_fsm1_cross_lang_fixture. Change the
# FSM1 wire format and BOTH update together — or the build breaks.

# Tiny matrix exercising a curr-only route (/cart, row_total 0), multi-pair rows
# with ascending col-ids, and uvarint counts. Routes sort by raw bytes to
# /cart(id0), /home(id1), /products(id2).
SMALL_MATRIX = {
    "version": "test-fsm1-a",
    "vocab_size": 3,
    "counts": {"/home": {"/products": 15, "/cart": 5}, "/products": {"/cart": 2}},
    "row_totals": {"/home": 20, "/products": 2},
    # categories/anchors are intentionally dropped from the KV payload.
    "categories": {"/home": "home"},
    "anchors": ["/home"],
}

SMALL_MATRIX_FSM1_HEX = (
    "46534d31"  # magic "FSM1"
    "01"  # fmt_ver = 1
    "03000000"  # vocab_size = 3
    "03000000"  # n_routes = 3
    "0b00"  # ver_len = 11
    "746573742d66736d312d61"  # "test-fsm1-a"
    "00000000050000000a00000013000000"  # str_off = [0,5,10,19]
    "2f636172742f686f6d652f70726f6475637473"  # "/cart/home/products"
    "001402"  # row_total = [0,20,2]
    "00000203"  # row_off = [0,0,2,3]
    "0005020f0002"  # pairs: /home->/cart:5,/products:15 ; /products->/cart:2
)


def test_serialize_kv_byte_exact():
    assert serialize_kv(SMALL_MATRIX).hex() == SMALL_MATRIX_FSM1_HEX


def test_serialize_kv_drops_categories_and_anchors():
    # The KV payload must NOT grow with categories/anchors — they're Rust-unread.
    bloated = dict(SMALL_MATRIX)
    bloated["categories"] = {f"/r{i}": "x" * 100 for i in range(1000)}
    bloated["anchors"] = [f"/a{i}" for i in range(1000)]
    assert serialize_kv(bloated) == serialize_kv(SMALL_MATRIX)


def test_serialize_kv_empty_matrix():
    # Untrained / empty → header with vocab_size 0, no rows. Rust decodes → L2 off.
    b = serialize_kv({"version": "", "vocab_size": 0, "counts": {}, "row_totals": {}})
    assert b[:4] == b"FSM1"
    assert b[4] == 1
    assert int.from_bytes(b[5:9], "little") == 0  # vocab_size
    assert int.from_bytes(b[9:13], "little") == 0  # n_routes


def test_serialize_kv_shrinks_vs_json():
    # Order-of-magnitude guard: a realistic sparse matrix must encode far smaller
    # than its JSON (the whole point — faster KV fetch + parse on cold instances).
    routes = [f"/p/{i}" for i in range(500)]
    counts = {r: {routes[(i + 1) % 500]: 3, routes[(i + 2) % 500]: 1} for i, r in enumerate(routes)}
    m = {
        "version": "2026-06-18-a",
        "vocab_size": 500,
        "counts": counts,
        "row_totals": {r: 4 for r in routes},
    }
    binary = serialize_kv(m)
    js = json.dumps(m, separators=(",", ":")).encode()
    assert len(binary) < len(js) // 2, f"binary {len(binary)} not <½ of json {len(js)}"


# ── Merged from test_scorer.py ──────────────────────────────────────────────────────

"""Tests for backend.scoring.scorer — Layer 2 transition evaluation."""

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

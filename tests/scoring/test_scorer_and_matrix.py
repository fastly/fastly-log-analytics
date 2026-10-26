"""Tests for backend.scoring.evaluate — ROC-AUC quality gate."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, call, patch

import pytest
from backend.provision.session_scoring_setup import (
    COMPUTE_MANAGE_URL,
    CONFIG_RESOURCE_LINK_NAME,
    CONFIG_STORE_MANAGE_URL,
    CURRENT_KEY_HEX,
    DEBUG_LOG_DEFAULT,
    DEBUG_LOG_KEY,
    KEYS_RESOURCE_LINK_NAME,
    KV_STORE_MANAGE_URL,
    MATRIX_RESOURCE_LINK_NAME,
    PREVIOUS_KEY_HEX,
    SCORING_ENABLED_AT_KEY,
    EntitlementError,
    delete_scoring_service,
    ensure_scoring_service,
    rotate_aes_key,
)
from backend.provision.session_scoring_vcl import (
    DEFAULT_ENFORCE_STATUS_CODE,
    SCORING_BACKEND_API_NAME,
    SCORING_BACKEND_VCL_NAME,
    SCORING_DELIVER_NAME,
    SCORING_ENFORCE_NAME,
    SCORING_FETCH_NAME,
    SCORING_MISS_NAME,
    SCORING_PASS_NAME,
    SCORING_RECV_NAME,
    SCORING_SNIPPET_PRIORITY,
    deliver_snippet,
    enforce_reason_phrase,
    enforce_snippet,
    fetch_snippet,
    generate_scoring_vcl,
    miss_snippet,
    pass_snippet,
    recv_snippet,
    resolve_enforce_status_code,
    scoring_snippet_names,
)
from backend.scoring.evaluate import (
    _compute_auc,
    _session_l2_score,
    evaluate,
)
from backend.scoring.matrix import build_matrix

from backend.provision import session_scoring_setup as sss

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
    from backend.scoring.evaluate import _KNOWN_REASON_ATOMS
    from backend.scoring.normalize import Route

    from backend.scoring import scorer as pyscorer

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


# ── Merged from tests/scoring/test_normalize.py ──

"""Tests for backend.scoring.normalize — URL → canonical (route, category).

Each ID-pattern collapse case is parametrized so adding new patterns later
is one-line additive. Category-mapping tests are split out so renames /
additions don't churn the unrelated route-collapse cases."""

import pytest

# ── Route normalization ──────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "url,expected_path",
    [
        # Doc §5.3 examples we CAN catch heuristically (id-like segments).
        ("/items/10243", "/items/*"),
        ("/api/v2/orders/00000abc-1234-5678-9abc-deadbeef0000", "/api/v2/orders/*"),
        # Trivial paths.
        ("/", "/"),
        ("", "/"),
        ("/home", "/home"),
        # Query strings always stripped.
        ("/search?q=red+shoes", "/search"),
        ("/search?q=x&page=2", "/search"),
        ("/items/42?ref=email", "/items/*"),
        # Numeric ids in middle and at end.
        ("/blog/12345", "/blog/*"),
        ("/orders/789/items/42", "/orders/*/items/*"),
        # UUID v4.
        ("/sessions/123e4567-e89b-12d3-a456-426614174000", "/sessions/*"),
        # 24-char hex hash (Mongo ObjectId).
        ("/jobs/64bc89ff1a2b3c4d5e6f7081", "/jobs/*"),
        # Prefixed ID like SKU-12345.
        ("/inventory/SKU-12345", "/inventory/*"),
        ("/orders/ORD-789-ABC", "/orders/*"),
        # Long opaque token (>= 20 chars alphanumeric).
        (
            "/oauth/callback/abcdef0123456789xyzwAA",
            "/oauth/callback/*",
        ),
        # Absolute URL.
        (
            "https://www.example.com/api/v1/users/777?token=abc",
            "/api/v1/users/*",
        ),
        # Trailing slash preserved (or rather: NOT preserved — split drops empty segs).
        ("/products/", "/products"),
        # Mixed case in non-id segments → lowercased for stable matrix keys.
        ("/Products/Foo", "/products/foo"),
    ],
)
def test_normalize_paths(url, expected_path):
    assert normalize(url).path == expected_path


def test_normalize_handles_double_slashes():
    """Multiple consecutive slashes collapse to a single boundary."""
    assert normalize("/foo//bar").path == "/foo/bar"


def test_normalize_does_not_collapse_short_alphanumeric():
    """Real route names that are short alphanumeric strings (e.g. /faq, /cart)
    must NOT be collapsed to '*'."""
    assert normalize("/faq").path == "/faq"
    assert normalize("/cart").path == "/cart"
    assert normalize("/api/v2").path == "/api/v2"  # 'v2' is too short for LONG_OPAQUE


def test_normalize_does_not_collapse_words_with_dashes():
    """Hyphenated route slugs ('/about-us', '/privacy-policy') stay intact —
    they're not key-like even though they have dashes."""
    assert normalize("/about-us").path == "/about-us"
    assert normalize("/privacy-policy").path == "/privacy-policy"


def test_normalize_known_limitation_word_like_user_ids():
    """KNOWN LIMITATION: ``/users/drew/profile`` (doc §5.3 example) is NOT
    collapsed because 'drew' is alphanumeric+short and indistinguishable
    from a real route name without per-site context. Per-service route
    templates are tracked under Phase B as a follow-up; for now the
    matrix carries one row per real user, and Laplace smoothing absorbs
    the cardinality."""
    r = normalize("/users/drew/profile")
    assert r.path == "/users/drew/profile"  # documents current behavior
    assert r.category == "account"  # but the category lookup still works


# ── Category mapping ─────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "url,expected_category",
    [
        ("/", "home"),
        ("/products/42", "product"),
        ("/items/99", "product"),
        ("/cart", "cart"),
        ("/checkout/step-1", "checkout"),
        ("/orders/789", "checkout"),
        ("/account/settings", "account"),
        ("/users/drew", "account"),
        ("/api/v2/orders/123", "api"),
        ("/graphql", "api"),
        ("/auth/login", "auth"),
        ("/login", "auth"),
        ("/admin/dashboard", "admin"),
        ("/blog/launch-post", "content"),
        ("/about-us", "other"),  # Not in the prefix map; bucketed as 'other'.
        ("/totally-unknown-section/foo", "other"),
    ],
)
def test_category_mapping(url, expected_category):
    assert normalize(url).category == expected_category


def test_category_derived_from_original_first_segment_not_star():
    """If the first segment were collapsed to '*' we'd lose category signal.
    Verify the category lookup happens before the collapse."""
    # A URL whose FIRST segment looks numeric would collapse to /*/foo,
    # and we have to decide what category that gets. Currently we lookup
    # against the original first segment text, so '12345' → 'other'.
    r = normalize("/12345/foo")
    assert r.path == "/*/foo"
    assert r.category == "other"


# ── Route dataclass invariants ───────────────────────────────────────────────


def test_route_is_hashable_for_use_as_dict_key():
    r1 = normalize("/items/42")
    r2 = normalize("/items/99")
    assert r1 == r2  # Same canonical path + category
    # Frozen dataclass → hashable → usable as matrix key.
    {r1: 1, r2: 2}  # noqa: B015  — just verifying no exception


def test_route_immutable():
    r = normalize("/foo")
    with pytest.raises(Exception):
        r.path = "/bar"  # type: ignore[misc]


def test_normalize_canonicalizes_percent_encoding_and_dot_segments():
    """Verify that percent-encoded characters and dot segments are canonicalized."""
    assert normalize("/%61pi/foo").path == "/api/foo"
    assert normalize("/%61pi/foo").category == "api"

    assert normalize("/./api/foo").path == "/api/foo"
    assert normalize("/./api/foo").category == "api"

    assert normalize("/api/../auth/login").path == "/auth/login"
    assert normalize("/api/../auth/login").category == "auth"


def test_normalize_encoded_dot_segments_do_not_traverse():
    """Unquote before normpath to evaluate percent-encoded traversals
    like ``%2e%2e`` and prevent category bypasses."""
    r = normalize("/admin/%2e%2e/items/foo")
    assert r.path == "/items/foo"
    assert r.category == "product"
    r = normalize("/admin/%2e%2e/%2e%2e/etc/passwd")
    assert r.path == "/etc/passwd"
    assert r.category == "other"


def test_normalize_double_slash_path_is_not_authority():
    """Regression for audit finding 018: ``urlsplit('//foo/bar')`` parses
    ``foo`` as a network location and returns ``/bar`` as the path, which
    let an attacker drop the first segment by prefixing the URL with a
    double-slash. _strip_query now flattens leading double-slashes first."""
    assert normalize("//admin/secret").path.startswith("/admin")
    assert normalize("//admin/secret").category == "admin"
    # Triple+ slashes get flattened too.
    assert normalize("///admin/secret").path.startswith("/admin")


def test_normalize_finding_012_encoded_query_does_not_truncate():
    """Verify that encoded query delimiters (%3F) are NOT treated as query
    separators before normalization — finding 012 demonstrated that the
    prior pre-split %3F → ? replacement let an attacker hide path-traversal
    payloads (e.g. ``/search%3F/../../etc/passwd``) behind a benign-looking
    prefix. The path now keeps the encoded character literally so downstream
    scoring sees the whole payload (unquoted at the per-segment unquote pass
    inside normalize)."""
    # Encoded ? becomes a literal ? in the segment after unquote (the
    # full string ends up as a single first-segment, hence the 'other'
    # category fallback).
    assert normalize("/search%3fq=red+shoes&page=2").path == "/search?q=red+shoes&page=2"
    assert normalize("/search%3Fq=red+shoes&page=2").category == "other"


def test_normalize_finding_014_encoded_slash_traversal_bypass():
    """Verify that encoded slashes (%2F) do not act as structural separators,
    and thus do not allow path-traversal bypasses (Finding 014)."""
    r = normalize("/auth/login%2F..%2F..%2Fproduct")
    assert r.path == "/auth/login/../../product"
    assert r.category == "auth"


def test_normalize_urlsplit_value_error_handling():
    """Verify that malformed URLs causing ValueError in urlsplit are gracefully
    handled and fallback to '/' (Finding 008-val)."""
    assert normalize("http://[example.com").path == "/"


def test_normalize_finding_011_double_encoded_traversal_resolves():
    """Finding 011 (2026-06-15): a single ``unquote_except_slash`` pass only
    peeled one encoding layer, so a doubly-encoded traversal like
    ``/admin/%252e%252e/items`` was decoded to ``/admin/%2e%2e/items``
    before posixpath.normpath ran — normpath saw it as a normal segment
    and didn't resolve. The delayed per-segment unquote at the end of
    normalize then decoded ``%2e%2e`` to ``..`` AFTER normpath, leaving
    the traversal embedded in the canonical path and tagging the route
    with the wrong category (``admin`` instead of the resolved target's
    category).

    Iterating until fixed-point unwinds the multi-level encoding *before*
    normpath, so ``/admin/%252e%252e/items`` correctly resolves to
    ``/items`` with category ``product``. The 014 ``%2F``-stays-as-data
    contract is preserved (each iteration runs the same except-slash
    decoder)."""
    r = normalize("/admin/%252e%252e/items")
    assert r.path == "/items", f"double-encoded traversal must resolve; got {r.path!r}"
    assert r.category == "product", (
        f"category must reflect the resolved target, not the pre-traversal path; got {r.category!r}"
    )

    # Triple-encoded variant — paranoid coverage that the loop converges
    # past two layers as well.
    r3 = normalize("/admin/%25252e%25252e/items")
    assert r3.path == "/items"
    assert r3.category == "product"

    # The 014 ``%2F``-as-data contract still holds — encoded slashes
    # decode but don't drive traversal resolution.
    r014 = normalize("/auth/login%2F..%2F..%2Fproduct")
    assert r014.path == "/auth/login/../../product"
    assert r014.category == "auth"


def test_normalize_finding_010_encoded_fragment_does_not_truncate():
    """Finding 010: ``_strip_query`` used to convert ``%23`` to ``#`` before
    ``urlsplit`` so an attacker could write ``/search%23/../../api/admin`` and
    have the scorer categorize it as a benign ``/search`` browse — the
    downstream origin still sees the encoded ``%23`` as a literal character
    and processes the full traversal. After the fix, the encoded fragment
    delimiter is preserved through ``urlsplit``, normpath collapses the
    traversal, and the final categorisation is whatever the resolved path
    is (``api`` in this case)."""
    r = normalize("/search%23/../../api/admin")
    assert r.path == "/api/admin", (
        f"encoded #-fragment must not truncate the path; normpath must collapse the traversal — got {r.path!r}"
    )
    assert r.category == "api"


# ── Merged from test_normalize_runtime_parity.py ──────────────────────────────────────────────────────

"""Runtime Py↔Rust normalize differential parity (toolchain-gated).

Complements :mod:`tests.scoring.test_normalize_parity`, which compares the two
GOLDEN *tables* statically (no toolchain) but never executes the Rust normalizer
on arbitrary input. This test compiles ``compute/scorer/src/normalize.rs``
natively (``rustc``; the module is pure-``std``) and runs BOTH normalizers over a
shared corpus, asserting byte-identical ``(path, category)`` where they MUST
agree and PINNING the known, documented divergence classes where they don't.

Why this exists (Edge/Compute pre-release audit): the matrix is TRAINED with the
Python normalizer and SCORED with the Rust one, so any URL that normalizes
differently is a silent train/score *key* mismatch. A runtime differential over
a fuzz corpus surfaced four divergence classes the table-only test cannot catch:

  1. Unicode lowercasing — Python ``str.lower()`` (full Unicode) vs Rust
     ``to_ascii_lowercase()`` (ASCII only). ``/CAF%C3%89`` → ``/café`` (Py) vs
     ``/cafÉ`` (Rust). DOCUMENTED divergence under the ASCII-only normalization
     contract (EC-02; see both normalizers' module docstrings).
  2. Unicode decimal digits — Python ``\\d`` matches Unicode Nd, Rust
     ``is_ascii_digit`` does not. ``/items/%D9%A1%D9%A2%D9%A3`` → ``/items/*``
     (Py) vs ``/items/١٢٣`` (Rust). Same ASCII-only contract.
  3. ``scheme:opaque`` (``mailto:``/``tel:``) — Python ``urlsplit`` drops the
     scheme, Rust keeps it. ``mailto:foo`` → ``/foo`` (Py) vs ``/mailto:foo``
     (Rust). Unreachable at the edge (request targets start with ``/``).

RESOLVED (EC-02): raw C0 control chars (``\\t \\n \\r``) used to be the only
*category* flip — Python ``urlsplit`` stripped them, Rust kept them
(``/adm\\tin`` → ``admin`` Py vs ``other`` Rust). The Rust ``normalize`` now
strips them too, so the two AGREE; ``test_raw_control_chars_now_agree`` pins the
fix (it trips if the strip is removed on either side).

The agreement block enforces parity on the ASCII-printable, non-control,
non-``scheme:`` subset (where the two genuinely must agree); a NEW pure-ASCII
divergence there would be a real regression. The divergence block pins the
remaining DOCUMENTED non-ASCII / scheme:opaque classes: if a one-sided change
lands (a real fix OR a brand-new divergence), the pin trips and forces both
normalizers + the parity doc to be updated together.

Skipped when ``rustc`` is unavailable so the default toolchain-free suite stays
green; runs locally and in the Scorer CI job where Rust is present.
"""

import shutil
import subprocess
from pathlib import Path

import pytest

_RUSTC = shutil.which("rustc")
_NORMALIZE_RS = Path(__file__).resolve().parents[2] / "compute" / "scorer" / "src" / "normalize.rs"

# A thin wrapper that pulls in the REAL edge normalizer (pure std; #[cfg(test)]
# is excluded without --test). I/O is HEX-framed so URLs/outputs containing
# control chars (\t \n \r) or non-ASCII bytes survive the line protocol intact —
# a raw newline-delimited protocol silently mangles exactly the inputs this test
# needs to probe. One hex line in per url → one "hex(path)\thex(category)" out.
_WRAPPER_SRC = """
#[path = "{rs}"]
mod normalize;
use std::io::{{self, BufRead, Write}};
fn unhex(s: &str) -> Vec<u8> {{
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len() / 2);
    let mut i = 0;
    while i + 1 < b.len() {{
        let hi = (b[i] as char).to_digit(16).unwrap();
        let lo = (b[i + 1] as char).to_digit(16).unwrap();
        out.push(((hi << 4) | lo) as u8);
        i += 2;
    }}
    out
}}
fn hex(b: &[u8]) -> String {{
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {{
        s.push_str(&format!("{{:02x}}", x));
    }}
    s
}}
fn main() {{
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut out = stdout.lock();
    for line in stdin.lock().lines() {{
        let line = match line {{ Ok(l) => l, Err(_) => break }};
        let url_bytes = unhex(line.trim());
        let url = String::from_utf8_lossy(&url_bytes);
        let r = normalize::normalize(&url);
        writeln!(out, "{{}}\\t{{}}", hex(r.path.as_bytes()), hex(r.category.as_bytes())).unwrap();
    }}
}}
"""

# ── Agreement corpus: ASCII-printable, no raw C0 control bytes, no scheme:opaque.
# This is the subset where the two normalizers MUST produce identical output.
# Picks hit the id-collapse boundaries, traversal/encoding, %2F-as-data, and the
# embedded-scheme (finding-023) protection — the surface most likely to drift.
_AGREEMENT_URLS = [
    "/",
    "/home",
    "/Products/Foo",
    "/items/10243",
    "/items/42?ref=email",
    "/api/v2/orders/00000abc-1234-5678-9abc-deadbeef0000",
    "/api/v2/orders/00000ABC-1234-5678-9ABC-DEADBEEF0000",  # uppercase-hex uuid
    "/jobs/64bc89ff1a2b3c4d5e6f7081",  # 24-hex hash
    "/inventory/SKU-12345",
    "/orders/ORD-789-ABC",
    "/oauth/callback/abcdef0123456789xyzwAA",  # long-opaque >= 20
    "/api/v2",  # too short for long-opaque
    "/about-us",
    "/privacy-policy",
    "/static/../admin",
    "/a/./b/../c",
    "/a/b/%2e%2e%2f%2e%2e%2fc",
    "/admin/%2e%2e/items/foo",
    "/admin/%252e%252e/items",  # double-encoded traversal
    "/auth/login%2F..%2F..%2Fproduct",  # %2F-as-data (finding 014)
    "/items/%2e%2e%2fsecret",
    "/a%2Fb",
    "/%61dmin",
    "/foo//bar",
    "//api/x",
    "///a//b",
    "/admin/delete/http://x/",  # embedded-scheme finding-023
    "/api/v2/orders/file://x/",
    "https://www.example.com/api/v1/users/777?token=abc",
    "ftp://h/a/b",
    "/search?q=red+shoes&page=2",
    "/AB-",  # prefixed-id boundary: empty suffix → not an id
    "/ABCDEF-123",  # 6-char prefix → not prefixed-id
    "/A1-bc",  # digit in prefix → not prefixed-id
    "/AB_CD-EF",  # first separator is '_'
    "/users/drew/profile",  # known v1 limitation (no auto-collapse)
    "/checkout/step-1",
    "/blog/12345",
    "/orders/789/items/42",
    "/cart",
    "/faq",
    "/zzz-unknown",
    "/foo%00bar",  # NUL is not a C0 strip char for urlsplit; both keep it
    "/a b/c",  # space inside a segment (not leading)
]

# ── Divergence corpus: (url, why) — pinned as CURRENTLY divergent. If a fix
# lands (Rust aligned to Python) OR a new divergence appears, the pin trips.
_KNOWN_DIVERGENT = [
    ("/CAF%C3%89", "unicode-lowercase: É lowercased by Python only"),
    ("/%C3%89/%C3%89", "unicode-lowercase"),
    ("/items/%D9%A1%D9%A2%D9%A3", "unicode-digit: Arabic-Indic ١٢٣ collapse Py-only"),
    ("/%D9%A1", "unicode-digit"),
    ("mailto:foo", "scheme:opaque stripped by Python urlsplit only"),
    ("tel:+1234", "scheme:opaque"),
]

# ── Control-char corpus: raw C0 (\t \n \r). EC-02 made BOTH sides strip these
# (Python via urlsplit, Rust in normalize), so they now AGREE. Kept as a pin so
# removing the strip on either side re-opens the category flip and trips
# test_raw_control_chars_now_agree.
_CONTROL_CHARS = [
    "/adm\tin",
    "/admin\t",
    "/admin\n",
    "/admin\r",
    "/che\tckout",
]


def _run_rust(urls: list[str], tmp_path: Path) -> list[tuple[str, str]]:
    """Compile normalize.rs natively and return [(path, category)] per url."""
    assert _NORMALIZE_RS.is_file(), f"normalizer not found: {_NORMALIZE_RS}"
    src = tmp_path / "rust_norm.rs"
    # Escape backslashes for the Rust string literal path (Windows-safe; on
    # posix the path has none, but be defensive).
    src.write_text(_WRAPPER_SRC.format(rs=str(_NORMALIZE_RS).replace("\\", "\\\\")))
    binary = tmp_path / "rust_norm"
    compile_proc = subprocess.run(
        [_RUSTC, "--edition", "2021", "-O", str(src), "-o", str(binary)],
        capture_output=True,
        text=True,
    )
    if compile_proc.returncode != 0:
        pytest.skip(f"rustc could not build normalize.rs wrapper:\n{compile_proc.stderr}")
    # Hex-frame each url (one per line); decode hex(path)\thex(category) back out.
    stdin = "\n".join(u.encode("utf-8", "surrogatepass").hex() for u in urls) + "\n"
    run = subprocess.run([str(binary)], input=stdin, capture_output=True, text=True)
    assert run.returncode == 0, f"rust normalizer crashed: {run.stderr}"
    out = [ln for ln in run.stdout.split("\n") if ln]
    assert len(out) == len(urls), f"expected {len(urls)} rows, got {len(out)}"
    rows: list[tuple[str, str]] = []
    for line in out:
        hpath, _, hcat = line.partition("\t")
        path = bytes.fromhex(hpath).decode("utf-8", "replace")
        cat = bytes.fromhex(hcat).decode("utf-8", "replace")
        rows.append((path, cat))
    return rows


@pytest.mark.skipif(_RUSTC is None, reason="rustc not available — runtime Rust differential skipped")
def test_python_rust_agree_on_ascii_subset(tmp_path):
    """On the ASCII-printable, non-control, non-scheme:opaque subset the two
    normalizers MUST be byte-identical (train key == score key). A failure here
    is a real, security-relevant divergence (the edge would key a route the
    trainer keyed differently) — NOT an expected i18n/control-char case."""
    rust = _run_rust(_AGREEMENT_URLS, tmp_path)
    mismatches = []
    for url, (rp, rc) in zip(_AGREEMENT_URLS, rust):
        pr = normalize(url)
        if (pr.path, pr.category) != (rp, rc):
            mismatches.append((url, (pr.path, pr.category), (rp, rc)))
    assert not mismatches, (
        "Py↔Rust normalize divergence on the ASCII subset (each row is "
        "url, python(path,cat), rust(path,cat)):\n" + "\n".join(f"  {m}" for m in mismatches)
    )


@pytest.mark.skipif(_RUSTC is None, reason="rustc not available — runtime Rust differential skipped")
def test_known_divergence_classes_still_diverge(tmp_path):
    """Pin the documented non-ASCII / scheme:opaque divergences as CURRENTLY
    real. If a one-sided change aligns Rust to Python (a fix) or introduces a
    new shape, this trips — update both normalizers + test_normalize_parity.py
    GOLDEN + the parity doc together (never re-baseline one side)."""
    urls = [u for u, _why in _KNOWN_DIVERGENT]
    rust = _run_rust(urls, tmp_path)
    still_same = []
    for (url, why), (rp, rc) in zip(_KNOWN_DIVERGENT, rust):
        pr = normalize(url)
        if (pr.path, pr.category) == (rp, rc):
            still_same.append((url, why, (pr.path, pr.category)))
    assert not still_same, (
        "A known Py↔Rust divergence has CLOSED (Rust now matches Python). "
        "Good — but the pin is stale: remove these rows here and reflect the "
        "fix in test_normalize_parity.py + the parity doc:\n" + "\n".join(f"  {s}" for s in still_same)
    )


@pytest.mark.skipif(_RUSTC is None, reason="rustc not available — runtime Rust differential skipped")
def test_raw_control_chars_now_agree(tmp_path):
    """EC-02: raw \\t/\\n/\\r are stripped on BOTH sides (Python urlsplit; Rust
    normalize), so the previous category flip (admin → other at the edge) is
    closed. Pin the AGREEMENT — both produce the control-char-free (path,
    category) — so removing the strip on either side trips this test."""
    rust = _run_rust(_CONTROL_CHARS, tmp_path)
    expected = {
        "/adm\tin": ("/admin", "admin"),
        "/admin\t": ("/admin", "admin"),
        "/admin\n": ("/admin", "admin"),
        "/admin\r": ("/admin", "admin"),
        "/che\tckout": ("/checkout", "checkout"),
    }
    for url, (rp, rc) in zip(_CONTROL_CHARS, rust):
        pr = normalize(url)
        want_path, want_cat = expected[url]
        # Python strips the control char (urlsplit).
        assert (pr.path, pr.category) == (want_path, want_cat), (
            f"{url!r}: Python normalize drifted → {(pr.path, pr.category)!r}"
        )
        # Rust now strips it too → byte-identical to Python (no more flip).
        assert (rp, rc) == (want_path, want_cat), (
            f"{url!r}: Rust no longer strips the control char (got {(rp, rc)!r}); the EC-02 fix regressed."
        )


# ── Merged from tests/scoring/test_normalize_parity.py ──

import re

from backend.scoring.normalize import _CATEGORY_MAP

_RUST_NORMALIZE = _NORMALIZE_RS

GOLDEN: list[tuple[str, str, str, str, str]] = [
    ("/", "/", "home", "/", "home"),
    ("/items/10243", "/items/*", "product", "/items/*", "product"),
    (
        "/api/v2/orders/00000abc-1234-5678-9abc-deadbeef0000",
        "/api/v2/orders/*",
        "api",
        "/api/v2/orders/*",
        "api",
    ),
    ("/%61dmin", "/admin", "admin", "/admin", "admin"),
    ("/static/../admin", "/admin", "admin", "/admin", "admin"),
    ("/admin/%2e%2e/items/foo", "/items/foo", "product", "/items/foo", "product"),
    ("/search?q=red+shoes&page=2", "/search", "browse", "/search", "browse"),
    ("/auth/login%2F..%2F..%2Fproduct", "/auth/login/../../product", "auth", "/auth/login/../../product", "auth"),
    ("/items/%2e%2e%2fsecret", "/items/../secret", "product", "/items/../secret", "product"),
    ("/a/b/%2e%2e%2f%2e%2e%2fc", "/a/b/../../c", "other", "/a/b/../../c", "other"),
]

KNOWN_DIVERGENCES: set[str] = set()

CATEGORY_GOLDEN: list[tuple[str, str]] = [
    ("/", "home"),
    ("/api/status", "api"),
    ("/products/widget", "product"),
    ("/search", "browse"),
    ("/cart", "cart"),
    ("/checkout/step-1", "checkout"),
    ("/account/settings", "account"),
    ("/login", "auth"),
    ("/admin/users", "admin"),
    ("/static/app.js", "asset"),
    ("/blog/post", "content"),
    ("/zzz-unknown", "other"),
]


@pytest.mark.parametrize("url,py_path,py_cat,_rust_path,_rust_cat", GOLDEN)
def test_python_normalize_matches_golden(url, py_path, py_cat, _rust_path, _rust_cat):
    """Pin the Python normalizer's output for every golden URL."""
    r = normalize(url)
    assert r.path == py_path, f"{url!r}: path drifted {r.path!r} != {py_path!r}"
    assert r.category == py_cat, f"{url!r}: category drifted {r.category!r} != {py_cat!r}"


@pytest.mark.parametrize("url,category", CATEGORY_GOLDEN)
def test_python_category_golden(url, category):
    """Pin the Python category bucket for one URL per CATEGORY_MAP value."""
    assert normalize(url).category == category, f"{url!r}: category drifted {normalize(url).category!r} != {category!r}"


def test_divergence_classification_is_honest():
    """The AGREE/DIVERGE split must match reality."""
    for url, py_path, py_cat, rust_path, rust_cat in GOLDEN:
        same = (py_path, py_cat) == (rust_path, rust_cat)
        if url in KNOWN_DIVERGENCES:
            assert not same, (
                f"{url!r} is marked divergent but Python==Rust expected — gap closed? update KNOWN_DIVERGENCES."
            )
        else:
            assert same, (
                f"{url!r} is treated as parity but expected values differ — new divergence? add it to KNOWN_DIVERGENCES."
            )


def test_finding_014_no_category_evasion():
    """Finding 014: an encoded slash (`%2F`) must not let a traversal relabel a route's category."""
    evasion_targets = {
        "/auth/login%2F..%2F..%2Fproduct": "product",
        "/items/%2e%2e%2fsecret": "other",
    }
    for url, evaded_cat in evasion_targets.items():
        cat = normalize(url).category
        assert cat != evaded_cat, f"{url!r}: category-evasion not prevented (got {cat!r})"
    assert ".." in normalize("/auth/login%2F..%2F..%2Fproduct").path


def _read_rust_source() -> str:
    assert _RUST_NORMALIZE.is_file(), f"Rust normalizer not found at {_RUST_NORMALIZE}"
    return _RUST_NORMALIZE.read_text(encoding="utf-8")


def _extract_slice_body(src: str, const_name: str) -> str:
    m = re.search(rf"const\s+{re.escape(const_name)}\s*:[^=]*=\s*&\[", src)
    assert m, f"could not locate `const {const_name}` slice in {_RUST_NORMALIZE.name}"
    start = m.end()
    end = src.index("]", start)
    return src[start:end]


def _parse_string_tuples(block: str, arity: int) -> list[tuple[str, ...]]:
    block = re.sub(r"//[^\n]*", "", block)
    raw = re.findall(r'"((?:[^"\\]|\\.)*)"', block)
    literals = [s.replace('\\"', '"').replace("\\\\", "\\") for s in raw]
    assert literals, f"no string literals parsed from slice body: {block!r}"
    assert len(literals) % arity == 0, (
        f"parsed {len(literals)} literals, not a multiple of arity {arity} — "
        "the Rust table layout drifted from what the parser expects"
    )
    return [tuple(literals[i : i + arity]) for i in range(0, len(literals), arity)]


def test_rust_category_map_matches_python():
    rust_pairs = _parse_string_tuples(_extract_slice_body(_read_rust_source(), "CATEGORY_MAP"), 2)
    py_pairs = list(_CATEGORY_MAP.items())
    if rust_pairs != py_pairs:
        only_rust = set(rust_pairs) - set(py_pairs)
        only_py = set(py_pairs) - set(rust_pairs)
        raise AssertionError(
            "CATEGORY_MAP desynced between normalize.rs and normalize.py.\n"
            f"  only in Rust: {sorted(only_rust)}\n"
            f"  only in Python: {sorted(only_py)}\n"
            + ("  (same entries, different order)" if not only_rust and not only_py else "")
        )


def test_rust_parity_golden_matches_python_golden():
    rust_golden = _parse_string_tuples(_extract_slice_body(_read_rust_source(), "PARITY_GOLDEN"), 3)
    py_rust_view = [(url, rust_path, rust_cat) for (url, _pp, _pc, rust_path, rust_cat) in GOLDEN]
    assert rust_golden == py_rust_view, (
        "PARITY_GOLDEN (normalize.rs) and GOLDEN (test_normalize.py) diverged.\n"
        f"  Rust:   {rust_golden}\n"
        f"  Python: {py_rust_view}"
    )


def test_rust_category_golden_matches_python():
    rust_cat_golden = _parse_string_tuples(_extract_slice_body(_read_rust_source(), "CATEGORY_GOLDEN"), 2)
    py_cat_golden = [tuple(row) for row in CATEGORY_GOLDEN]
    assert rust_cat_golden == py_cat_golden, (
        "CATEGORY_GOLDEN diverged between normalize.rs and test_normalize.py.\n"
        f"  Rust:   {rust_cat_golden}\n"
        f"  Python: {py_cat_golden}"
    )


# ── Merged from tests/scoring/test_session_scoring_vcl.py ──

LOG_SVC = "TestLogSvcABC123"
TOKEN = "FAKE_TOKEN"
KEYS_STORE_ID = "KEYS_STORE_XYZ"
CURRENT_ITEM_PATH = f"/resources/stores/config/{KEYS_STORE_ID}/item/{CURRENT_KEY_HEX}"
PREVIOUS_ITEM_PATH = f"/resources/stores/config/{KEYS_STORE_ID}/item/{PREVIOUS_KEY_HEX}"
ITEM_COLLECTION_PATH = f"/resources/stores/config/{KEYS_STORE_ID}/item"


def _hex64(byte: int) -> str:
    return f"{byte:02x}" * 32


# ── 1. Snippet Constants & Recv Snippet ──────────────────────────────────────


def test_snippet_constants_and_naming_templates():
    assert scoring_snippet_names() == [
        SCORING_RECV_NAME,
        SCORING_PASS_NAME,
        SCORING_FETCH_NAME,
        SCORING_DELIVER_NAME,
        SCORING_MISS_NAME,
        SCORING_ENFORCE_NAME,
    ]
    assert SCORING_SNIPPET_PRIORITY == 100
    assert SCORING_BACKEND_API_NAME == "session_scorer" and SCORING_BACKEND_VCL_NAME == "F_session_scorer"
    assert sss._scoring_service_name("ABC123") == "Session Scoring Service for ABC123"
    d = sss._scoring_domain("MixedCaseSvcId001")
    assert d == "fos-mixedcasesvcid001-session-scorer.edgecompute.app" == d.lower()


def test_recv_snippet_routing_scrub_and_guards():
    vcl = recv_snippet("MyServiceId")
    assert f"set req.backend = {SCORING_BACKEND_VCL_NAME};" in vcl
    assert "fastly.ff.visits_this_service == 0" in vcl
    assert "req.restarts == 0" in vcl and 'req.http.X-Edge-Scoring-Pass != "1"' in vcl
    assert "X-Edge-Shield-Auth" not in vcl
    assert "X-Edge-Scorer-Auth" not in vcl and "X-Edge-Service-Id" not in vcl
    assert "set req.http.X-Edge-Prev-Route" not in vcl and "set req.http.X-Edge-Last-Route" not in vcl

    # Client-edge scrub unsets must appear after the boundary guard
    boundary_idx = vcl.find("req.restarts == 0 && fastly.ff.visits_this_service == 0")
    assert boundary_idx != -1
    for unset in (
        "unset req.http.X-Edge-Scoring-Pass",
        "unset req.http.X-Edge-Score",
        "unset req.http.X-Edge-Sid",
        "unset req.http.X-Edge-Score-Enforce",
        "unset req.http.X-Edge-Score-Reason",
        "unset req.http.X-Edge-Score-Set-Cookie",
        "unset req.http.X-Edge-Prev-Anchor",
        "unset req.http.x-sigsci-skip-inspection-once;",
    ):
        assert vcl.find(unset) > boundary_idx, f"missing or misplaced scrub: {unset!r}"

    # Post-scoring restart re-enables shielding and unsets skip-inspection
    restart_idx = vcl.find("req.restarts == 1 && req.http.x-edge-score")
    assert restart_idx != -1 and "set var.fastly_req_do_shield = true;" in vcl
    assert vcl.count("unset req.http.x-sigsci-skip-inspection-once;") == 2
    assert vcl.find("unset req.http.x-sigsci-skip-inspection-once;", restart_idx) > restart_idx

    # DDoS guard, RTT stamp, NGWAF skip-inspection, and scoring pass marker order
    assert "!fastly.ddos_detected" in vcl
    assert (
        vcl.find('req.http.X-Edge-Scoring-Pass != "1"')
        < vcl.find("!fastly.ddos_detected")
        < vcl.find("std.tolower(req.url) !~")
    )
    assert "set req.http.x-edge-score-t0 = time.elapsed.usec;" in vcl
    assert 'set req.http.X-Edge-Scoring-Pass = "1";' in vcl
    backend_idx = vcl.find(f"set req.backend = {SCORING_BACKEND_VCL_NAME};")
    skip_idx = vcl.find('set req.http.x-sigsci-skip-inspection-once = "true";')
    t0_idx = vcl.find("set req.http.x-edge-score-t0")
    pass_idx = vcl.find("return(pass);")
    assert backend_idx < skip_idx < pass_idx and t0_idx < pass_idx


# ── 2. Pass, Fetch, Deliver, Miss & Enforce Snippets ─────────────────────────


def test_pass_fetch_and_miss_snippets():
    vcl = pass_snippet("MyServiceId", "deadbeef1234567890")
    assert f"if (req.backend == {SCORING_BACKEND_VCL_NAME})" in vcl
    assert 'set bereq.method = "GET";' in vcl
    assert 'set bereq.http.X-Edge-Service-Id = "MyServiceId";' in vcl
    assert 'set bereq.http.X-Edge-Scorer-Auth = "deadbeef1234567890";' in vcl
    assert "unset bereq.http.X-Edge-Scoring-Pass;" in vcl and "unset bereq.http.x-edge-score;" in vcl

    a, b = pass_snippet("Svc", "secret_a"), pass_snippet("Svc", "secret_b")
    assert a != b and "secret_a" in a and "secret_a" not in b and "secret_b" in b

    fetch_vcl = fetch_snippet()
    assert f"if (req.backend == {SCORING_BACKEND_VCL_NAME})" in fetch_vcl and "return(deliver);" in fetch_vcl

    miss_vcl = miss_snippet()
    assert "unset bereq.http.x-edge-score;" in miss_vcl and "unset bereq.http.X-Edge-Scoring-Pass;" in miss_vcl


def test_deliver_snippet_subfields_rtt_and_anti_leak():
    vcl = deliver_snippet()
    assert 'if (req.http.X-Edge-Scoring-Pass == "1")' in vcl and "unset req.http.X-Edge-Scoring-Pass;" in vcl
    assert "\n  restart;\n" in vcl and "return(restart)" not in vcl

    # Subfield captures
    for sub, src in (
        ("score", "x-edge-score"),
        ("l1", "x-edge-score-l1"),
        ("l2", "x-edge-score-l2"),
        ("compliance", "X-Edge-Cookie-Compliance"),
        ("reason", "x-edge-score-reason"),
        ("sid", "x-edge-sid"),
        ("enforce", "x-edge-score-enforce"),
        ("exec", "x-edge-score-exec-us"),
        ("matrix", "X-Edge-Matrix-Version"),
        ("set-cookie", "Set-Cookie"),
    ):
        assert f"set req.http.x-edge-score:{sub} = resp.http.{src};" in vcl

    # Matrix capture must happen BEFORE anti-leak unset
    assert vcl.index("set req.http.x-edge-score:matrix = resp.http.X-Edge-Matrix-Version;") < vcl.index(
        "unset resp.http.X-Edge-Matrix-Version;"
    )

    # Anti-leak unsets
    for header in (
        "x-edge-score",
        "x-edge-score-l1",
        "x-edge-score-l2",
        "x-edge-score-reason",
        "x-edge-sid",
        "x-edge-score-enforce",
        "x-edge-score-exec-us",
        "X-Edge-Cookie-Compliance",
        "X-Edge-Matrix-Version",
    ):
        assert f"unset resp.http.{header};" in vcl

    # Fail-open branch
    assert "} else {" in vcl
    assert 'set req.http.x-edge-score:score = "0";' in vcl
    assert 'set req.http.x-edge-score:compliance = "unknown";' in vcl
    assert 'set req.http.x-edge-score:reason = "compute-unavailable-" + resp.status;' in vcl

    # RTT calculation before resp.status branch
    for stmt in (
        "declare local var.rtt INTEGER;",
        "set var.rtt = std.atoi(time.elapsed.usec);",
        "set var.rtt -= std.atoi(req.http.x-edge-score-t0);",
        'set req.http.x-edge-score:rtt = "" + var.rtt;',
    ):
        assert stmt in vcl
    assert vcl.index("set req.http.x-edge-score:rtt") < vcl.index("if (resp.status == 200)")

    # Pass 2 additive Set-Cookie at edge only
    assert 'if (fastly.ff.visits_this_service == 0 && req.http.x-edge-score:set-cookie != "") {' in vcl
    assert "add resp.http.Set-Cookie = req.http.x-edge-score:set-cookie;" in vcl
    assert "set resp.http.Set-Cookie =" not in vcl and "X-Edge-Shield-Auth" not in vcl


def test_enforce_snippet_guards_and_parameterized_status_codes():
    vcl = enforce_snippet()
    if_idx = vcl.find("if (")
    assert if_idx != -1
    tail = vcl[if_idx:]
    edge_idx = tail.find("fastly.ff.visits_this_service == 0")
    restart_idx = tail.find("req.restarts == 1")
    enforce_idx = tail.find('req.http.x-edge-score:enforce == "1"')
    assert 0 <= edge_idx < restart_idx < enforce_idx
    assert "&&" in tail[edge_idx:restart_idx] and "&&" in tail[restart_idx:enforce_idx]
    assert 'error 429 "Too Many Requests"' in vcl and "X-Edge-Shield-Auth" not in vcl

    assert DEFAULT_ENFORCE_STATUS_CODE == 429
    for raw, expected in [(None, 429), (403, 403), (99, 429), (600, 429), ("not-an-int", 429)]:
        assert resolve_enforce_status_code(raw) == expected
    for code, phrase in [
        (403, "Forbidden"),
        (429, "Too Many Requests"),
        (451, "Unavailable For Legal Reasons"),
        (503, "Service Unavailable"),
        (511, "Network Authentication Required"),
        (599, "Blocked"),
        (444, "Blocked"),
    ]:
        assert enforce_reason_phrase(code) == phrase

    for code, phrase in [
        (403, "Forbidden"),
        (451, "Unavailable For Legal Reasons"),
        (503, "Service Unavailable"),
        (418, "I'm a Teapot"),
        (599, "Blocked"),
        (99, "Too Many Requests"),
        (600, "Too Many Requests"),
    ]:
        effective = 429 if code in (99, 600) else code
        snippet = enforce_snippet(code)
        assert f'error {effective} "{phrase}"' in snippet
        assert "fastly.ff.visits_this_service == 0" in snippet and "req.restarts == 1" in snippet


def test_generate_scoring_vcl_integration_and_injection_guards():
    snippets = generate_scoring_vcl("UniqueServiceXYZ", "deadbeef1234", enforce_status_code=451)
    assert set(snippets.keys()) == set(scoring_snippet_names())
    assert 'error 451 "Unavailable For Legal Reasons"' in snippets[SCORING_ENFORCE_NAME]
    assert (
        'error 429 "Too Many Requests"'
        in generate_scoring_vcl("Svc", "sec", enforce_status_code=None)[SCORING_ENFORCE_NAME]
    )

    # Service ID and secret baked ONLY into pass_snippet
    assert "UniqueServiceXYZ" in snippets[SCORING_PASS_NAME] and "deadbeef1234" in snippets[SCORING_PASS_NAME]
    for other in (SCORING_RECV_NAME, SCORING_FETCH_NAME, SCORING_DELIVER_NAME, SCORING_MISS_NAME, SCORING_ENFORCE_NAME):
        assert "UniqueServiceXYZ" not in snippets[other] and "deadbeef1234" not in snippets[other]

    # Pure determinism & no spoofable Matrix-Age-Days header
    assert generate_scoring_vcl("Svc", "a" * 64) == generate_scoring_vcl("Svc", "a" * 64)
    assert not [n for n, b in snippets.items() if "matrix-age-days" in b.lower()]

    # EC-07 injection guards
    bad_secret = 'abc" || req.http.x-evil == "1'
    for fn in (lambda: pass_snippet("Svc", bad_secret), lambda: generate_scoring_vcl("Svc", bad_secret)):
        with pytest.raises(ValueError, match="request_secret"):
            fn()
    with pytest.raises(ValueError, match="exclude_url_regex"):
        recv_snippet("Svc", exclude_url_regex=r'\.(png)"; bad')
    assert r"\.(png|jpg)" in recv_snippet("Svc", exclude_url_regex=r"^/skip/.*\.(png|jpg)$")


# ── 3. ensure_scoring_service: Create, Entitlements, Rollback & Reuse ────────


def _create_side_effect(*, fail_on=None, fail_exc=None):
    config_store_responses = iter(
        [
            {"id": "KEYS_STORE_ID", "name": "scoring_keys_SCORING_SVC_ID"},
            {"id": "CFG_STORE_ID", "name": "scoring_config_SCORING_SVC_ID"},
        ]
    )
    base = {
        ("GET", "/service"): [],
        ("POST", "/service"): {"id": "SCORING_SVC_ID", "name": "x", "type": "wasm"},
        ("GET", "/resources/stores/kv"): [],
        ("POST", "/resources/stores/kv"): {"id": "MATRIX_STORE_ID", "name": "scoring_matrix_SCORING_SVC_ID"},
        ("GET", "/service/SCORING_SVC_ID/version"): [],
    }

    def side_effect(method, path, body=None, token=None, **kwargs):
        if fail_on and (method, path) == fail_on:
            raise fail_exc
        if (method, path) == ("POST", "/resources/stores/config"):
            return next(config_store_responses)
        return base.get((method, path), {})

    return MagicMock(side_effect=side_effect)


def test_ensure_creates_full_stack_and_emits_status_callbacks():
    fastly_mock = _create_side_effect()
    status_cb = MagicMock()
    with (
        patch("backend.provision.session_scoring_setup.fastly", fastly_mock),
        patch("backend.provision.session_scoring_setup.product_enabled", return_value=True),
    ):
        result = ensure_scoring_service(LOG_SVC, TOKEN, status_cb=status_cb)

    assert result["scoring_service_id"] == "SCORING_SVC_ID"
    assert result["scoring_service_name"] == f"Session Scoring Service for {LOG_SVC}"
    assert result["scoring_domain"] == f"fos-{LOG_SVC.lower()}-session-scorer.edgecompute.app"
    assert result["scoring_keys_store_id"] == "KEYS_STORE_ID"
    assert result["scoring_config_store_id"] == "CFG_STORE_ID"
    assert result["scoring_matrix_store_id"] == "MATRIX_STORE_ID"
    assert len(result["aes_key_hex"]) == 64 and int(result["aes_key_hex"], 16) >= 0

    calls = fastly_mock.call_args_list
    assert call("GET", "/service", token=TOKEN) in calls
    assert next(c for c in calls if c.args[:2] == ("POST", "/service")).args[2] == {
        "name": f"Session Scoring Service for {LOG_SVC}",
        "type": "wasm",
    }
    assert (
        next(c for c in calls if c.args[1] == "/service/SCORING_SVC_ID/version/1/domain")
        .args[2]["name"]
        .endswith("session-scorer.edgecompute.app")
    )
    assert (
        next(c for c in calls if c.args[1] == "/service/SCORING_SVC_ID/version/1/backend").args[2]["name"]
        == "placeholder_origin"
    )

    store_creates = [c for c in calls if c.args[:2] == ("POST", "/resources/stores/config")]
    assert len(store_creates) == 2
    key_upload = next(c for c in calls if c.args[1].endswith("/KEYS_STORE_ID/item"))
    assert key_upload.args[2]["item_key"] == CURRENT_KEY_HEX and len(key_upload.args[2]["item_value"]) == 64

    cfg_posts = [c.args[2] for c in calls if c.args[:2] == ("POST", "/resources/stores/config/CFG_STORE_ID/item")]
    assert {"item_key": DEBUG_LOG_KEY, "item_value": DEBUG_LOG_DEFAULT} in cfg_posts
    anchor_val = next(p["item_value"] for p in cfg_posts if p["item_key"] == SCORING_ENABLED_AT_KEY)
    assert anchor_val.isdigit() and int(anchor_val) > 0

    link_names = {c.args[2]["name"] for c in calls if c.args[1].endswith("/version/1/resource")}
    assert link_names == {KEYS_RESOURCE_LINK_NAME, CONFIG_RESOURCE_LINK_NAME, MATRIX_RESOURCE_LINK_NAME}

    assert status_cb.call_count >= 4
    msgs = " ".join(str(c.args[0]) for c in status_cb.call_args_list).lower()
    assert "scoring service" in msgs and "domain" in msgs and "config stores" in msgs


@pytest.mark.parametrize(
    ("kv_enabled", "fail_on", "fail_exc", "expected_code", "expected_link", "expected_deletes"),
    [
        (False, None, None, "kv_store_not_enabled", KV_STORE_MANAGE_URL, set()),
        (
            True,
            ("POST", "/service"),
            RuntimeError("HTTP 403 Forbidden"),
            "compute_not_enabled",
            COMPUTE_MANAGE_URL,
            set(),
        ),
        (
            True,
            ("POST", "/resources/stores/config"),
            RuntimeError("HTTP 400 Bad Request"),
            "config_store_not_enabled",
            CONFIG_STORE_MANAGE_URL,
            {("DELETE", "/service/SCORING_SVC_ID")},
        ),
        (
            True,
            ("POST", "/service/SCORING_SVC_ID/version/1/resource"),
            RuntimeError("HTTP 500 Internal Server Error"),
            None,
            None,
            {
                ("DELETE", "/service/SCORING_SVC_ID"),
                ("DELETE", "/resources/stores/config/KEYS_STORE_ID"),
                ("DELETE", "/resources/stores/config/CFG_STORE_ID"),
                ("DELETE", "/resources/stores/kv/MATRIX_STORE_ID"),
            },
        ),
    ],
)
def test_ensure_entitlement_mapping_and_rollback(
    kv_enabled, fail_on, fail_exc, expected_code, expected_link, expected_deletes
):
    fastly_mock = _create_side_effect(fail_on=fail_on, fail_exc=fail_exc)
    exc_cls = EntitlementError if expected_code else RuntimeError
    with (
        patch("backend.provision.session_scoring_setup.fastly", fastly_mock),
        patch("backend.provision.session_scoring_setup.product_enabled", return_value=kv_enabled),
        pytest.raises(exc_cls) as ei,
    ):
        ensure_scoring_service(LOG_SVC, TOKEN)

    if expected_code:
        assert ei.value.code == expected_code and ei.value.link == expected_link
    else:
        assert not isinstance(ei.value, EntitlementError) and "HTTP 5" in str(ei.value)

    deletes = {c.args[:2] for c in fastly_mock.call_args_list if c.args[0] == "DELETE"}
    assert expected_deletes <= deletes
    if not expected_deletes:
        assert ("DELETE", "/service/SCORING_SVC_ID") not in deletes


@pytest.mark.parametrize("anchor_missing", [False, True])
def test_ensure_reuses_existing_service_and_heals_missing_anchor(anchor_missing):
    existing = {"id": "EXISTING_SVC_ID", "name": f"Session Scoring Service for {LOG_SVC}"}
    stored_secret = "stored_secret_" + "z" * 50

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", "/service"):
            return [existing]
        if (method, path) == ("GET", "/resources/stores/config"):
            return [
                {"id": "EXISTING_KEYS", "name": "scoring_keys_EXISTING_SVC_ID"},
                {"id": "EXISTING_CFG", "name": "scoring_config_EXISTING_SVC_ID"},
            ]
        if (method, path) == ("GET", "/resources/stores/kv"):
            return [{"id": "EXISTING_MATRIX", "name": "scoring_matrix_EXISTING_SVC_ID"}]
        if (method, path) == ("GET", "/resources/stores/config/EXISTING_KEYS/item/request_secret"):
            return {"item_key": "request_secret", "item_value": stored_secret}
        if (method, path) == ("GET", f"/resources/stores/config/EXISTING_CFG/item/{SCORING_ENABLED_AT_KEY}"):
            if anchor_missing:
                raise RuntimeError("404 not found")
            return {"item_key": SCORING_ENABLED_AT_KEY, "item_value": "1700000000"}
        if anchor_missing and (method, path) == ("POST", "/resources/stores/config/EXISTING_CFG/item"):
            return {}
        raise AssertionError(f"unexpected API call: {method} {path}")

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        result = ensure_scoring_service(LOG_SVC, TOKEN)

    assert result["scoring_service_id"] == "EXISTING_SVC_ID"
    assert result["scoring_keys_store_id"] == "EXISTING_KEYS"
    assert result["scoring_config_store_id"] == "EXISTING_CFG"
    assert result["scoring_matrix_store_id"] == "EXISTING_MATRIX"
    assert result["request_secret"] == stored_secret and result["aes_key_hex"] == ""
    if anchor_missing:
        anchor_post = next(
            c
            for c in fastly_mock.call_args_list
            if c.args[:2] == ("POST", "/resources/stores/config/EXISTING_CFG/item")
        )
        assert anchor_post.args[2]["item_key"] == SCORING_ENABLED_AT_KEY and anchor_post.args[2]["item_value"].isdigit()


# ── 4. delete_scoring_service: Teardown Ordering & Error Handling ────────────


def test_delete_runs_full_teardown_in_correct_order():
    versions = [{"number": 1, "active": True}, {"number": 2, "active": False}]
    calls_in_order: list = []

    def side_effect(method, path, body=None, token=None, **kwargs):
        calls_in_order.append((method, path))
        return versions if (method, path) == ("GET", "/service/SVC_X/version") else {}

    with patch("backend.provision.session_scoring_setup.fastly", MagicMock(side_effect=side_effect)):
        failed = delete_scoring_service(
            "SVC_X",
            scoring_keys_store_id="KEYS_X",
            scoring_config_store_id="CFG_X",
            scoring_matrix_store_id="MATRIX_X",
            token=TOKEN,
        )

    assert failed == []
    deactivate_idx = calls_in_order.index(("PUT", "/service/SVC_X/version/1/deactivate"))
    svc_delete_idx = calls_in_order.index(("DELETE", "/service/SVC_X"))
    keys_delete_idx = calls_in_order.index(("DELETE", "/resources/stores/config/KEYS_X"))
    cfg_delete_idx = calls_in_order.index(("DELETE", "/resources/stores/config/CFG_X"))
    kv_key_idx = calls_in_order.index(("DELETE", "/resources/stores/kv/MATRIX_X/keys/matrix"))
    kv_store_idx = calls_in_order.index(("DELETE", "/resources/stores/kv/MATRIX_X"))
    assert deactivate_idx < svc_delete_idx < keys_delete_idx and svc_delete_idx < cfg_delete_idx
    assert svc_delete_idx < kv_key_idx < kv_store_idx
    assert ("PUT", "/service/SVC_X/version/2/deactivate") not in calls_in_order


def test_delete_edge_cases_and_store_failure_reporting():
    # Empty service_id is a no-op
    noop_mock = MagicMock()
    with patch("backend.provision.session_scoring_setup.fastly", noop_mock):
        delete_scoring_service("", token=TOKEN)
    noop_mock.assert_not_called()

    # 404 on version list or store delete is tolerated; 500 on store delete is surfaced
    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", "/service/MISSING/version"):
            raise RuntimeError("404 not found")
        if (method, path) == ("GET", "/service/SVC/version"):
            return []
        if path.startswith("/resources/stores/config/MISSING_STORE"):
            raise RuntimeError("404 not found")
        if method == "DELETE" and path.startswith("/resources/stores/config/STUCK"):
            raise RuntimeError("500 internal error")
        return {}

    msgs: list[str] = []
    with patch("backend.provision.session_scoring_setup.fastly", MagicMock(side_effect=side_effect)):
        delete_scoring_service("MISSING", token=TOKEN)
        delete_scoring_service(
            "SVC", scoring_keys_store_id="MISSING_STORE", scoring_config_store_id="FINE", token=TOKEN
        )
        failed = delete_scoring_service(
            "SVC", scoring_keys_store_id="STUCK", scoring_config_store_id="FINE", token=TOKEN, status_cb=msgs.append
        )

    assert ("scoring_keys", "STUCK") in failed and all(sid != "FINE" for _, sid in failed)
    assert any("STUCK" in m and "manual" in m.lower() for m in msgs)


# ── 5. rotate_aes_key: Cold Start, Steady State, Upsert & Validation ─────────


def test_first_and_second_rotation_and_upsert_fallback():
    # 1. Cold start: PATCH previous_key_hex 404s -> POST upsert fallback
    old_current = _hex64(0xAB)
    calls_1: list = []

    def side_effect_1(method, path, body=None, token=None, **kwargs):
        calls_1.append((method, path, body))
        if (method, path) == ("GET", CURRENT_ITEM_PATH):
            return {"item_key": CURRENT_KEY_HEX, "item_value": old_current}
        if method == "PATCH" and path == PREVIOUS_ITEM_PATH:
            raise RuntimeError("HTTP 404 PATCH /.../previous_key_hex\n    not found")
        return {}

    with patch("backend.provision.session_scoring_setup.fastly", MagicMock(side_effect=side_effect_1)):
        res1 = rotate_aes_key(KEYS_STORE_ID, token=TOKEN)

    assert len(res1["current_key_hex"]) == 64 and int(res1["current_key_hex"], 16) >= 0
    assert res1["previous_key_hex"] == old_current and res1["current_key_hex"] != old_current
    assert "T" in res1["rotated_at"]
    prev_values = [
        (c[2] or {}).get("item_value")
        for c in calls_1
        if (c[1] == PREVIOUS_ITEM_PATH and c[0] == "PATCH")
        or (c[1] == ITEM_COLLECTION_PATH and c[0] == "POST" and (c[2] or {}).get("item_key") == PREVIOUS_KEY_HEX)
    ]
    assert old_current in prev_values

    # 2. Steady state: overwrites previous with old_current, drops old_previous
    old_cur2, old_prev2 = _hex64(0xCD), _hex64(0xEF)
    calls_2: list = []

    def side_effect_2(method, path, body=None, token=None, **kwargs):
        calls_2.append((method, path, body))
        return (
            {"item_key": CURRENT_KEY_HEX, "item_value": old_cur2}
            if (method, path) == ("GET", CURRENT_ITEM_PATH)
            else {}
        )

    with patch("backend.provision.session_scoring_setup.fastly", MagicMock(side_effect=side_effect_2)):
        res2 = rotate_aes_key(KEYS_STORE_ID, token=TOKEN)

    assert res2["previous_key_hex"] == old_cur2 and res2["current_key_hex"] not in (old_cur2, old_prev2)
    assert [c[2] for c in calls_2 if c[0] == "PATCH" and c[1] == PREVIOUS_ITEM_PATH] == [{"item_value": old_cur2}]
    assert [c[2] for c in calls_2 if c[0] == "PATCH" and c[1] == CURRENT_ITEM_PATH] == [
        {"item_value": res2["current_key_hex"]}
    ]
    written_values = [(c[2] or {}).get("item_value") for c in calls_2 if c[0] in ("PATCH", "POST") and c[2]]
    assert old_prev2 not in written_values

    # 3. Missing current_key_hex: GET & PATCH 404 -> POST fallback creates current_key_hex
    calls_3: list = []

    def side_effect_3(method, path, body=None, token=None, **kwargs):
        calls_3.append((method, path, body))
        if (method, path) == ("GET", CURRENT_ITEM_PATH) or method == "PATCH":
            raise RuntimeError("HTTP 404 not found")
        return {}

    with patch("backend.provision.session_scoring_setup.fastly", MagicMock(side_effect=side_effect_3)):
        res3 = rotate_aes_key(KEYS_STORE_ID, token=TOKEN)

    assert res3["previous_key_hex"] == ""
    assert any(
        c[0] == "POST"
        and c[1] == ITEM_COLLECTION_PATH
        and c[2] == {"item_key": CURRENT_KEY_HEX, "item_value": res3["current_key_hex"]}
        for c in calls_3
    )
    assert len([c for c in calls_3 if c[0] == "PATCH" and c[1] == CURRENT_ITEM_PATH]) == 1


def test_rotate_validation_auth_errors_and_entropy():
    # Empty store id raises ValueError before any API call
    fastly_mock = MagicMock()
    with (
        patch("backend.provision.session_scoring_setup.fastly", fastly_mock),
        pytest.raises(ValueError, match="scoring_keys_store_id is required"),
    ):
        rotate_aes_key("", token=TOKEN)
    fastly_mock.assert_not_called()

    # 401 on all calls or on writes propagates as RuntimeError
    for get_ok, tok in [(False, ""), (True, "bad-token")]:

        def _auth_fail(method, path, body=None, token=None, _ok=get_ok, **kwargs):
            if _ok and method == "GET":
                return {"item_key": CURRENT_KEY_HEX, "item_value": _hex64(0x11)}
            raise RuntimeError("HTTP 401 unauthorized\n    bad token")

        with (
            patch("backend.provision.session_scoring_setup.fastly", MagicMock(side_effect=_auth_fail)),
            pytest.raises(RuntimeError, match="401"),
        ):
            rotate_aes_key(KEYS_STORE_ID, token=tok)

    # Two successive rotations chain properly and produce distinct keys
    state = {"current": _hex64(0x01)}

    def _stateful(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", CURRENT_ITEM_PATH):
            return {"item_key": CURRENT_KEY_HEX, "item_value": state["current"]}
        if method == "PATCH" and path == CURRENT_ITEM_PATH and body:
            state["current"] = body["item_value"]
        return {}

    with patch("backend.provision.session_scoring_setup.fastly", MagicMock(side_effect=_stateful)):
        first = rotate_aes_key(KEYS_STORE_ID, token=TOKEN)
        second = rotate_aes_key(KEYS_STORE_ID, token=TOKEN)
    assert (
        first["current_key_hex"] != second["current_key_hex"] and second["previous_key_hex"] == first["current_key_hex"]
    )

    # Uses secrets.token_hex(32)
    sentinel = "ff" * 32
    with (
        patch("backend.provision.session_scoring_setup.fastly", MagicMock(side_effect=_stateful)),
        patch("secrets.token_hex", return_value=sentinel) as token_hex_mock,
    ):
        res = rotate_aes_key(KEYS_STORE_ID, token=TOKEN)
    assert res["current_key_hex"] == sentinel and sss.CURRENT_KEY_HEX == "current_key_hex"
    token_hex_mock.assert_called_with(32)

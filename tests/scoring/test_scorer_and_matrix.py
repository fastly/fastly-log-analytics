"""Tests for backend.scoring (evaluate, fixtures, labels, matrix, scorer, normalize) and session scoring VCL/setup."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
import time_machine
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
    _KNOWN_REASON_ATOMS,
    _compute_auc,
    _session_l2_score,
    evaluate,
    evaluate_from_persisted_scores,
    evaluate_per_reason,
)
from backend.scoring.fixtures import (
    SESSION_GAP_SECONDS,
    Event,
    extract_traces,
    rows_to_events,
    sessionize,
)
from backend.scoring.matrix import (
    TransitionMatrix,
    build_matrix,
    default_version,
    serialize_kv,
)
from backend.scoring.normalize import _CATEGORY_MAP, Route, normalize
from backend.scoring.scorer import (
    _l2_score_from_trans_prob,
    _transition_prob,
    score_layer2,
)

from backend.provision import session_scoring_setup as sss
from backend.scoring import labels as _labels
from backend.scoring import scorer as pyscorer

BASE_TS = datetime(2026, 5, 15, 10, 0, 0, tzinfo=UTC)
SVC = "svc1"


def _session(urls: list[str], dwell_s: float = 5.0, base: datetime | None = None) -> dict:
    base = base or BASE_TS
    events = [
        {
            "ts": (base + timedelta(seconds=int(i * dwell_s))).isoformat(timespec="seconds"),
            "url": url,
            "method": "GET",
            "status": 200,
            "referer": "",
            "ttfb_ms": 50.0,
            "country": "US",
            "asn": 7922,
        }
        for i, url in enumerate(urls)
    ]
    return {
        "session_id": "ip_test",
        "client_ip": "1.1.1.1",
        "user_agent": "Mozilla",
        "start_ts": events[0]["ts"] if events else "",
        "end_ts": events[-1]["ts"] if events else "",
        "event_count": len(events),
        "events": events,
    }


def _matrix_from(funnels: list[tuple[list[str], int]]) -> dict:
    sessions = [_session(path) for path, repeats in funnels for _ in range(repeats)]
    matrix, _ = build_matrix(sessions)
    return matrix.to_json_dict("test-v1")


def _matrix(counts: dict[str, dict[str, int]], vocab: int = 10) -> dict:
    return {
        "counts": counts,
        "row_totals": {k: sum(v.values()) for k, v in counts.items()},
        "vocab_size": vocab,
        "version": "test-1",
    }


# ── evaluate: _compute_auc, _session_l2_score, evaluate, persisted & per-reason ──


@pytest.mark.parametrize(
    ("scores", "expected"),
    [
        ([(0, "good"), (10, "good"), (90, "bad"), (100, "bad")], 1.0),
        ([(90, "good"), (100, "good"), (0, "bad"), (10, "bad")], 0.0),
        ([(50, "good"), (50, "good"), (50, "bad"), (50, "bad")], 0.5),
        ([(10, "good"), (20, "good"), (15, "bad"), (30, "bad")], 0.75),
        ([(10, "good"), (50, "good")], 0.5),
        ([(10, "bad"), (50, "bad")], 0.5),
    ],
)
def test_compute_auc(scores, expected):
    assert _compute_auc(scores) == expected


def test_session_l2_score_max_and_single_event():
    matrix = _matrix_from([(["/a", "/b"], 99), (["/a", "/c"], 1)])
    score, n = _session_l2_score(_session(["/a", "/b", "/a", "/c"]), matrix)
    assert n == 3 and score >= 25, f"expected rare /a→/c to surface, got {score}"

    score_single, n_single = _session_l2_score(_session(["/only"]), _matrix_from([(["/a", "/b"], 50)]))
    assert (n_single, score_single) == (0, 0)


def test_evaluate_end_to_end_and_summary():
    # High AUC on clear separation + summary formatting
    matrix = _matrix_from([(["/home", "/products", "/cart", "/checkout"], 200)])
    good = _session(["/home", "/products", "/cart", "/checkout"])
    bad = _session(["/home", "/admin/secrets"])
    result = evaluate(matrix, [(good, "good"), (bad, "bad")])
    assert (result.n_good, result.n_bad, result.auc, result.passed) == (1, 1, 1.0, True)
    summary = result.summary()
    assert "AUC=" in summary and ("PASS" in summary or "FAIL" in summary)
    assert "n_good=1" in summary and "n_bad=1" in summary

    # Below threshold fails
    m_small = _matrix_from([(["/a", "/b"], 10)])
    res_low = evaluate(m_small, [(_session(["/a", "/b"]), "good"), (_session(["/a", "/b"]), "bad")], min_auc=0.85)
    assert res_low.auc == 0.5 and not res_low.passed

    # Unknown label rejected
    with pytest.raises(ValueError, match="unexpected label"):
        evaluate(m_small, [(_session(["/a", "/b"]), "maybe")])

    # Neutral accepted for display but excluded from AUC
    res_neut = evaluate(
        m_small,
        [(_session(["/a", "/b"]), "good"), (_session(["/x", "/y"]), "bad"), (_session(["/q", "/r"]), "neutral")],
    )
    assert (res_neut.n_good, res_neut.n_bad, len(res_neut.per_session)) == (1, 1, 3)
    assert {r.label for r in res_neut.per_session} == {"good", "bad", "neutral"}

    # Per-session record fields
    res_rec = evaluate(m_small, [(_session(["/a", "/b", "/c"]), "good")])
    rec = res_rec.per_session[0]
    assert len(res_rec.per_session) == 1 and rec.label == "good"
    assert rec.transition_count == 2 and isinstance(rec.l2_score, int)


def test_evaluate_from_persisted_scores():
    # Uses max_edge_score rather than L2 recompute
    r1 = evaluate_from_persisted_scores(
        [
            ({"session_id": "bot1", "events": [], "max_edge_score": 75}, "bad"),
            ({"session_id": "good1", "events": [], "max_edge_score": 0}, "good"),
        ]
    )
    assert (r1.auc, r1.n_good, r1.n_bad, r1.passed) == (1.0, 1, 1, True)

    # Drops sessions without score
    r2 = evaluate_from_persisted_scores(
        [
            ({"session_id": "s1", "events": [], "max_edge_score": 80}, "bad"),
            ({"session_id": "s2", "events": [], "max_edge_score": None}, "good"),
        ]
    )
    assert (r2.n_bad, r2.n_good, r2.auc) == (1, 0, 0.5)

    # Excludes neutral from AUC while keeping in per_session
    r3 = evaluate_from_persisted_scores(
        [
            ({"session_id": "g", "max_edge_score": 10}, "good"),
            ({"session_id": "b", "max_edge_score": 80}, "bad"),
            ({"session_id": "n", "max_edge_score": 50}, "neutral"),
        ]
    )
    assert (r3.n_good, r3.n_bad, len(r3.per_session), r3.auc) == (1, 1, 3, 1.0)


def test_evaluate_per_reason_and_known_atoms():
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
    by_reason = {b["reason"]: b for b in evaluate_per_reason(sessions)["buckets"]}
    cm = by_reason["cookie-missing"]
    assert cm["has_min_samples"] is True and cm["n_good"] == 3 and cm["n_bad"] == 3 and cm["auc"] == 1.0
    assert by_reason["impossibly-fast"]["has_min_samples"] is False and "auc" not in by_reason["impossibly-fast"]

    empty_res = evaluate_per_reason([])
    assert len(empty_res["buckets"]) == len(_KNOWN_REASON_ATOMS)
    assert all(b["has_min_samples"] is False for b in empty_res["buckets"])

    known = set(_KNOWN_REASON_ATOMS)
    m = {"counts": {"/home": {"/other": 10000}}, "row_totals": {"/home": 10000}, "vocab_size": 100}
    _, l2_reasons, _ = pyscorer.score_layer2(m, Route("/home", "home"), None, Route("/checkout", "checkout"))
    assert set(l2_reasons) == {"low-transition-prob"} and set(l2_reasons) <= known
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


# ── fixtures: rows_to_events, sessionize, extract_traces ─────────────────────


def _row(
    ts: datetime = BASE_TS,
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
    return (ts, ip, ua, url, method, status, referer, ttfb, country, asn)


def _events_for(
    ip: str, ua: str, timestamps: list[datetime], urls: list[str] | None = None
) -> list[tuple[str, str, Event]]:
    urls = urls or [f"/page{i}" for i in range(len(timestamps))]
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


def test_rows_to_events_mapping_uppercase_and_null_safety():
    items = list(rows_to_events([_row(url="/foo", status=404)]))
    assert len(items) == 1
    ip, ua, ev = items[0]
    assert (ip, ua, ev.url, ev.status, ev.method, ev.ttfb_ms) == ("1.1.1.1", "Mozilla/5.0", "/foo", 404, "GET", 50.0)

    _, _, ev_upper = next(rows_to_events([_row(method="get")]))
    assert ev_upper.method == "GET"

    ip_n, ua_n, ev_n = next(rows_to_events([(BASE_TS, *(None,) * 9)]))
    assert (ip_n, ua_n, ev_n.url, ev_n.method, ev_n.status, ev_n.ttfb_ms, ev_n.asn) == ("", "", "", "", 0, 0.0, None)


def test_sessionize_grouping_gaps_and_stability():
    # Single session within gap
    s_close = list(
        sessionize(_events_for("1.1.1.1", "Mozilla", [BASE_TS + timedelta(seconds=10 * i) for i in range(6)]))
    )
    assert len(s_close) == 1 and s_close[0].event_count == 6

    # Gap > SESSION_GAP_SECONDS splits into two sessions
    s_gap = list(
        sessionize(
            _events_for(
                "1.1.1.1",
                "Mozilla",
                [
                    BASE_TS,
                    BASE_TS + timedelta(seconds=10),
                    BASE_TS + timedelta(seconds=SESSION_GAP_SECONDS + 60),
                    BASE_TS + timedelta(seconds=SESSION_GAP_SECONDS + 70),
                ],
            )
        )
    )
    assert [s.event_count for s in s_gap] == [2, 2]

    # Different IPs -> separate sessions
    s_ips = list(
        sessionize(
            [
                *_events_for("1.1.1.1", "Mozilla", [BASE_TS, BASE_TS + timedelta(seconds=10)]),
                *_events_for("2.2.2.2", "Mozilla", [BASE_TS + timedelta(seconds=20)]),
            ]
        )
    )
    assert {s.client_ip for s in s_ips} == {"1.1.1.1", "2.2.2.2"}

    # Different UAs on same IP -> separate sessions
    s_uas = list(
        sessionize(
            [
                *_events_for("1.1.1.1", "Mozilla/Firefox", [BASE_TS]),
                *_events_for("1.1.1.1", "Mozilla/Chrome", [BASE_TS + timedelta(seconds=5)]),
            ]
        )
    )
    assert {s.user_agent for s in s_uas} == {"Mozilla/Firefox", "Mozilla/Chrome"}

    # Empty input, custom gap_seconds, and deterministic session_id
    assert list(sessionize(iter([]))) == []
    ev_120 = _events_for("1.1.1.1", "Mozilla", [BASE_TS, BASE_TS + timedelta(seconds=120)])
    assert len(list(sessionize(ev_120, gap_seconds=60))) == 2
    assert len(list(sessionize(ev_120, gap_seconds=300))) == 1

    ev_det = [BASE_TS, BASE_TS + timedelta(seconds=10)]
    assert (
        list(sessionize(_events_for("1.1.1.1", "Mozilla", ev_det)))[0].session_id
        == list(sessionize(_events_for("1.1.1.1", "Mozilla", ev_det)))[0].session_id
    )


class _FakeCon:
    def __init__(self, rows: list[tuple]):
        self._rows = rows
        self.last_sql: str | None = None

    def execute(self, sql: str):
        self.last_sql = sql
        return MagicMock(fetchall=lambda: self._rows)


def test_extract_traces_sql_and_sessions():
    con = _FakeCon([])
    start = datetime(2026, 5, 15, 0, 0, 0, tzinfo=UTC)
    end = datetime(2026, 5, 16, 0, 0, 0, tzinfo=UTC)
    list(extract_traces(con, service_id="MySvc123", start=start, end=end, limit=100))
    assert con.last_sql is not None
    for needle in (
        "logs_mysvc123",
        "ORDER BY ip, ua, timestamp",
        "LIMIT 100",
        "timestamp >= TIMESTAMP '2026-05-15T00:00:00+00:00'",
        "timestamp < TIMESTAMP '2026-05-16T00:00:00+00:00'",
    ):
        assert needle in con.last_sql

    # No filters omits WHERE and LIMIT; partial filters include only one bound
    list(extract_traces(con, service_id="svc"))
    assert "WHERE" not in con.last_sql and "LIMIT" not in con.last_sql
    list(extract_traces(con, service_id="svc", start=start, end=None))
    assert "timestamp >=" in con.last_sql and "timestamp <" not in con.last_sql
    list(extract_traces(con, service_id="svc", start=None, end=end))
    assert "timestamp <" in con.last_sql and "timestamp >=" not in con.last_sql

    # End-to-end session extraction
    rows = [
        _row(BASE_TS, ip="1.1.1.1", url="/home"),
        _row(BASE_TS + timedelta(seconds=5), ip="1.1.1.1", url="/products"),
        _row(BASE_TS + timedelta(seconds=10), ip="1.1.1.1", url="/checkout"),
        _row(BASE_TS + timedelta(seconds=15), ip="2.2.2.2", url="/home"),
    ]
    sessions = list(extract_traces(_FakeCon(rows), service_id="svc"))
    assert [(s.client_ip, s.event_count) for s in sessions] == [("1.1.1.1", 3), ("2.2.2.2", 1)]
    assert [e.url for e in sessions[0].events] == ["/home", "/products", "/checkout"]


# ── labels: CRUD over scoring_labels ─────────────────────────────────────────


def test_labels_crud_upsert_and_service_isolation():
    # Basic round-trip
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
    assert (saved["sid"], saved["label"], saved["notes"], saved["sample_ip"]) == (
        "abc123def456",
        "bad",
        "probable scraper",
        "1.2.3.4",
    )
    assert saved["created_at"]
    fetched = _labels.get_label_by_id(SVC, saved["id"])
    assert fetched is not None and (fetched["id"], fetched["label"]) == (saved["id"], "bad")

    # Upsert on same sid updates in-place
    first = _labels.save_label(SVC, sid="sid001", label="bad")
    second = _labels.save_label(SVC, sid="sid001", label="good", notes="actually a real user")
    assert (second["id"], second["label"], second["notes"]) == (first["id"], "good", "actually a real user")
    assert len([r for r in _labels.list_labels(SVC) if r["sid"] == "sid001"]) == 1

    # Validation errors
    with pytest.raises(ValueError, match="label must be one of"):
        _labels.save_label(SVC, sid="x", label="maybe")
    with pytest.raises(ValueError, match="sid is required"):
        _labels.save_label(SVC, sid="", label="good")

    # Patch semantics, noop, and bad label on update
    u = _labels.save_label(SVC, sid="u1", label="bad", notes="initial")
    after_notes = _labels.update_label(SVC, u["id"], notes="revised note")
    assert (after_notes["label"], after_notes["notes"]) == ("bad", "revised note")
    after_label = _labels.update_label(SVC, u["id"], label="neutral")
    assert (after_label["label"], after_label["notes"]) == ("neutral", "revised note")
    noop = _labels.update_label(SVC, u["id"])
    assert (noop["id"], noop["label"]) == (u["id"], "neutral")
    with pytest.raises(ValueError, match="label must be one of"):
        _labels.update_label(SVC, u["id"], label="ugly")

    # Idempotent delete
    assert _labels.delete_label(SVC, u["id"])["status"] == "success"
    assert _labels.delete_label(SVC, u["id"])["status"] == "success"
    assert _labels.get_label_by_id(SVC, u["id"]) is None

    # List order, counts_by_label, and service isolation
    assert _labels.counts_by_label("svc_empty") == {"good": 0, "bad": 0, "neutral": 0}
    for sid, lbl in [("aaa", "good"), ("bbb", "bad"), ("ccc", "neutral")]:
        _labels.save_label("svc_list", sid=sid, label=lbl)
    assert {r["sid"] for r in _labels.list_labels("svc_list")[:3]} == {"aaa", "bbb", "ccc"}
    assert _labels.counts_by_label("svc_list") == {"good": 1, "bad": 1, "neutral": 1}

    sa = _labels.save_label("svcA", sid="shared", label="good")
    sb = _labels.save_label("svcB", sid="shared", label="bad")
    assert sa["id"] != sb["id"]
    assert _labels.get_label_by_id("svcA", sa["id"])["label"] == "good"
    assert _labels.get_label_by_id("svcB", sb["id"])["label"] == "bad"


# ── matrix: TransitionMatrix, build_matrix, default_version, serialize_kv ────


def test_transition_matrix_and_build_matrix():
    m = TransitionMatrix()
    m.add_transition(normalize("/a"), normalize("/b"))
    m.add_transition(normalize("/a"), normalize("/b"))
    m.add_transition(normalize("/a"), normalize("/c"))
    assert m.counts == {"/a": {"/b": 2, "/c": 1}} and m.row_totals == {"/a": 3}
    assert m.transition_count == 3 and m.vocab == {"/a", "/b", "/c"}

    m_cat = TransitionMatrix()
    m_cat.add_transition(normalize("/products/42"), normalize("/cart"))
    assert m_cat.categories["/products/*"] == "product" and m_cat.categories["/cart"] == "cart"

    # Drops short, drops fast, keeps normal, respects override, handles empty
    m_short, st_short = build_matrix([_session(["/home"]), _session(["/home", "/products"])])
    assert (st_short.sessions_dropped_short, st_short.sessions_kept, m_short.transition_count) == (1, 1, 1)

    m_fast, st_fast = build_matrix([_session([f"/p{i}" for i in range(10)], dwell_s=0.05)])
    assert (st_fast.sessions_dropped_fast, st_fast.sessions_kept, m_fast.transition_count) == (1, 0, 0)

    m_norm, st_norm = build_matrix(
        [_session(["/home", "/products", "/products/42", "/cart", "/checkout"], dwell_s=5.0)]
    )
    assert (
        st_norm.sessions_kept,
        st_norm.sessions_dropped_short,
        st_norm.sessions_dropped_fast,
        m_norm.transition_count,
    ) == (
        1,
        0,
        0,
        4,
    )

    s_override = _session(["/a", "/b", "/c"], dwell_s=0.1)
    assert build_matrix([s_override])[1].sessions_dropped_fast == 1
    assert build_matrix([s_override], min_mean_dwell_s=0.0)[1].sessions_kept == 1

    m_empty, st_empty = build_matrix([])
    assert (st_empty.sessions_in, st_empty.sessions_kept, m_empty.transition_count, len(m_empty.vocab)) == (0, 0, 0, 0)

    # Aggregation & URL normalization during build
    m_agg, st_agg = build_matrix(
        [
            _session(["/home", "/products/42", "/cart"], dwell_s=3.0),
            _session(["/home", "/products/99", "/cart"], dwell_s=3.0),
            _session(["/home", "/about", "/products/42"], dwell_s=3.0),
        ]
    )
    assert st_agg.sessions_kept == 3
    assert m_agg.counts["/home"] == {"/products/*": 2, "/about": 1} and m_agg.row_totals["/home"] == 3
    assert "/products/42" not in m_agg.counts["/home"]

    # Built matrix round-trips through score_layer2
    matrix_rt, _ = build_matrix(
        [_session(["/home", "/products"]) for _ in range(100)] + [_session(["/home", "/about"])]
    )
    loaded = matrix_rt.to_json_dict("test")
    score_common, _, p_common = score_layer2(loaded, normalize("/home"), None, normalize("/products"))
    score_unseen, _, p_unseen = score_layer2(loaded, normalize("/home"), None, normalize("/never-visited"))
    assert p_common > 0.9 and score_common <= 5
    assert 0 < p_unseen < 0.1 and score_unseen > score_common


def test_default_version_format_and_ordering():
    v = default_version()
    assert len(v) == 17 and datetime.strptime(v, "%Y-%m-%d-%H%M%S").strftime("%Y-%m-%d-%H%M%S") == v
    parts = v.split("-")
    assert len(parts) == 4 and all(p.isdigit() for p in parts)

    with time_machine.travel("2026-06-20 09:00:00+00:00"):
        v1 = default_version()
    with time_machine.travel("2026-06-20 14:30:52+00:00"):
        v2 = default_version()
    assert (v1, v2) == ("2026-06-20-090000", "2026-06-20-143052") and v1 < v2


SMALL_MATRIX = {
    "version": "test-fsm1-a",
    "vocab_size": 3,
    "counts": {"/home": {"/products": 15, "/cart": 5}, "/products": {"/cart": 2}},
    "row_totals": {"/home": 20, "/products": 2},
    "categories": {"/home": "home"},
    "anchors": ["/home"],
}

SMALL_MATRIX_FSM1_HEX = (
    "46534d310103000000030000000b00746573742d66736d312d61"
    "00000000050000000a000000130000002f636172742f686f6d652f70726f6475637473"
    "001402000002030005020f0002"
)


def test_serialize_kv_format_and_compaction():
    assert serialize_kv(SMALL_MATRIX).hex() == SMALL_MATRIX_FSM1_HEX

    bloated = {
        **SMALL_MATRIX,
        "categories": {f"/r{i}": "x" * 100 for i in range(1000)},
        "anchors": [f"/a{i}" for i in range(1000)],
    }
    assert serialize_kv(bloated) == serialize_kv(SMALL_MATRIX)

    b = serialize_kv({"version": "", "vocab_size": 0, "counts": {}, "row_totals": {}})
    assert b[:4] == b"FSM1" and b[4] == 1
    assert int.from_bytes(b[5:9], "little") == 0 and int.from_bytes(b[9:13], "little") == 0

    routes = [f"/p/{i}" for i in range(500)]
    m = {
        "version": "2026-06-18-a",
        "vocab_size": 500,
        "counts": {r: {routes[(i + 1) % 500]: 3, routes[(i + 2) % 500]: 1} for i, r in enumerate(routes)},
        "row_totals": {r: 4 for r in routes},
    }
    binary = serialize_kv(m)
    js = json.dumps(m, separators=(",", ":")).encode()
    assert len(binary) < len(js) // 2


# ── scorer: _transition_prob, _l2_score_from_trans_prob, score_layer2 ────────


@pytest.mark.parametrize(
    ("m", "prev", "curr", "low", "high"),
    [
        (_matrix({"a": {"b": 50, "c": 50}}, vocab=10), "a", "b", 0.45, 0.51),
        (_matrix({"a": {"b": 100}}, vocab=10), "a", "never-seen", 0.001, 0.01),
        (_matrix({"a": {"b": 100}}, vocab=10), "never-seen-prev", "b", 0.0, 1e-12),
        ({"counts": {"b": {"c": 100}}, "row_totals": {"a": 0, "b": 100}, "vocab_size": 50}, "a", "c", 0.0, 1e-12),
        ({"counts": {}, "row_totals": {}, "vocab_size": 50}, "x", "y", 0.0199, 0.0201),
    ],
)
def test_transition_prob(m, prev, curr, low, high):
    p = _transition_prob(m, prev, curr, m["vocab_size"])
    assert low <= p <= high


@pytest.mark.parametrize(
    ("p", "expected_band"),
    [
        (0.0, (100, 100)),
        (1.0, (0, 0)),
        (0.5, (0, 10)),
        (0.1, (10, 25)),
        (0.01, (30, 40)),
        (0.001, (45, 55)),
        (0.000001, (95, 100)),
    ],
)
def test_l2_score_curve(p, expected_band):
    s = _l2_score_from_trans_prob(p)
    assert expected_band[0] <= s <= expected_band[1]


def test_score_layer2_scenarios():
    # No matrix or no prev route -> 0 score
    assert score_layer2(None, Route("/a", "home"), None, Route("/b", "home")) == (0, [], 1.0)
    assert score_layer2(_matrix({"a": {"b": 1}}), None, None, Route("/b", "home"))[0] == 0

    # High prob transition -> no score
    m_hi = _matrix({"/home": {"/products": 99, "/other": 1}}, vocab=10)
    score, reasons, p = score_layer2(m_hi, Route("/home", "home"), None, Route("/products", "product"))
    assert score == 0 and reasons == [] and p > 0.9

    # Rare transition -> high score + low-transition-prob reason
    m_rare = _matrix({"/home": {"/checkout": 1}}, vocab=100)
    m_rare["row_totals"]["/home"] = 10000
    score, reasons, p = score_layer2(m_rare, Route("/home", "home"), None, Route("/checkout", "checkout"))
    assert score >= 50 and "low-transition-prob" in reasons and p < 0.001

    # Skip-gram rescues via seen anchor
    m_skip = _matrix({"/about-us": {"/checkout": 1}, "/product": {"/checkout": 100}}, vocab=10)
    m_skip["row_totals"]["/about-us"] = 1000
    m_skip["row_totals"]["/product"] = 100
    score, _, p = score_layer2(
        m_skip, Route("/about-us", "content"), Route("/product", "product"), Route("/checkout", "checkout")
    )
    assert p > 0.5 and score < 10

    # Finding 021: unseen anchor does NOT override anomalous direct transition
    m_f21 = _matrix({"/about-us": {"/home": 1000}}, vocab=10)
    score, reasons, p = score_layer2(
        m_f21,
        Route("/about-us", "content"),
        Route("/never-visited-anchor", "product"),
        Route("/checkout", "checkout"),
    )
    assert p < 0.001 and score >= 50 and "low-transition-prob" in reasons


# ── normalize: route collapse, category mapping, security & parity ───────────


@pytest.mark.parametrize(
    ("url", "expected_path"),
    [
        ("/items/10243", "/items/*"),
        ("/api/v2/orders/00000abc-1234-5678-9abc-deadbeef0000", "/api/v2/orders/*"),
        ("/", "/"),
        ("", "/"),
        ("/home", "/home"),
        ("/search?q=red+shoes", "/search"),
        ("/search?q=x&page=2", "/search"),
        ("/items/42?ref=email", "/items/*"),
        ("/blog/12345", "/blog/*"),
        ("/orders/789/items/42", "/orders/*/items/*"),
        ("/sessions/123e4567-e89b-12d3-a456-426614174000", "/sessions/*"),
        ("/jobs/64bc89ff1a2b3c4d5e6f7081", "/jobs/*"),
        ("/inventory/SKU-12345", "/inventory/*"),
        ("/orders/ORD-789-ABC", "/orders/*"),
        ("/oauth/callback/abcdef0123456789xyzwAA", "/oauth/callback/*"),
        ("https://www.example.com/api/v1/users/777?token=abc", "/api/v1/users/*"),
        ("/products/", "/products"),
        ("/Products/Foo", "/products/foo"),
        ("/foo//bar", "/foo/bar"),
        ("/faq", "/faq"),
        ("/cart", "/cart"),
        ("/api/v2", "/api/v2"),
        ("/about-us", "/about-us"),
        ("/privacy-policy", "/privacy-policy"),
        ("http://[example.com", "/"),
    ],
)
def test_normalize_paths(url, expected_path):
    assert normalize(url).path == expected_path


@pytest.mark.parametrize(
    ("url", "expected_category"),
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
        ("/about-us", "other"),
        ("/totally-unknown-section/foo", "other"),
    ],
)
def test_category_mapping(url, expected_category):
    assert normalize(url).category == expected_category


@pytest.mark.parametrize(
    ("url", "expected_path", "expected_cat"),
    [
        ("/users/drew/profile", "/users/drew/profile", "account"),
        ("/12345/foo", "/*/foo", "other"),
        ("/%61pi/foo", "/api/foo", "api"),
        ("/./api/foo", "/api/foo", "api"),
        ("/api/../auth/login", "/auth/login", "auth"),
        ("/admin/%2e%2e/items/foo", "/items/foo", "product"),
        ("/admin/%2e%2e/%2e%2e/etc/passwd", "/etc/passwd", "other"),
        ("//admin/secret", "/admin/secret", "admin"),
        ("///admin/secret", "/admin/secret", "admin"),
        ("/search%3fq=red+shoes&page=2", "/search?q=red+shoes&page=2", "other"),
        ("/search%3Fq=red+shoes&page=2", "/search?q=red+shoes&page=2", "other"),
        ("/auth/login%2F..%2F..%2Fproduct", "/auth/login/../../product", "auth"),
        ("/admin/%252e%252e/items", "/items", "product"),
        ("/admin/%25252e%25252e/items", "/items", "product"),
        ("/search%23/../../api/admin", "/api/admin", "api"),
    ],
)
def test_normalize_canonicalization_and_security_findings(url, expected_path, expected_cat):
    r = normalize(url)
    assert (r.path, r.category) == (expected_path, expected_cat)


def test_route_hashable_and_immutable():
    r1, r2 = normalize("/items/42"), normalize("/items/99")
    assert r1 == r2 and len({r1: 1, r2: 2}) == 1
    with pytest.raises(Exception):
        r1.path = "/bar"  # type: ignore[misc]


# ── Runtime & Static Py↔Rust Normalize Parity ────────────────────────────────

_RUSTC = shutil.which("rustc")
_NORMALIZE_RS = Path(__file__).resolve().parents[2] / "compute" / "scorer" / "src" / "normalize.rs"
_RUST_NORMALIZE = _NORMALIZE_RS

_WRAPPER_SRC = """
#[path = "{rs}"]
mod normalize;
use std::io::{{self, BufRead, Write}};
fn unhex(s: &str) -> Vec<u8> {{
    (0..s.len() / 2).map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap()).collect()
}}
fn hex(b: &[u8]) -> String {{
    b.iter().map(|x| format!("{{:02x}}", x)).collect()
}}
fn main() {{
    let (stdin, stdout) = (io::stdin(), io::stdout());
    let mut out = stdout.lock();
    for line in stdin.lock().lines().map_while(Result::ok) {{
        let url_bytes = unhex(line.trim());
        let url = String::from_utf8_lossy(&url_bytes);
        let r = normalize::normalize(&url);
        writeln!(out, "{{}}\\t{{}}", hex(r.path.as_bytes()), hex(r.category.as_bytes())).unwrap();
    }}
}}
"""

_AGREEMENT_URLS = [
    "/", "/home", "/Products/Foo", "/items/10243", "/items/42?ref=email",
    "/api/v2/orders/00000abc-1234-5678-9abc-deadbeef0000",
    "/api/v2/orders/00000ABC-1234-5678-9ABC-DEADBEEF0000",
    "/jobs/64bc89ff1a2b3c4d5e6f7081", "/inventory/SKU-12345", "/orders/ORD-789-ABC",
    "/oauth/callback/abcdef0123456789xyzwAA", "/api/v2", "/about-us", "/privacy-policy",
    "/static/../admin", "/a/./b/../c", "/a/b/%2e%2e%2f%2e%2e%2fc",
    "/admin/%2e%2e/items/foo", "/admin/%252e%252e/items",
    "/auth/login%2F..%2F..%2Fproduct", "/items/%2e%2e%2fsecret", "/a%2Fb", "/%61dmin",
    "/foo//bar", "//api/x", "///a//b",
    "/admin/delete/http://x/", "/api/v2/orders/file://x/",
    "https://www.example.com/api/v1/users/777?token=abc", "ftp://h/a/b",
    "/search?q=red+shoes&page=2", "/AB-", "/ABCDEF-123", "/A1-bc", "/AB_CD-EF",
    "/users/drew/profile", "/checkout/step-1", "/blog/12345", "/orders/789/items/42",
    "/cart", "/faq", "/zzz-unknown", "/foo%00bar", "/a b/c",
]  # fmt: skip

_KNOWN_DIVERGENT = [
    ("/CAF%C3%89", "unicode-lowercase: É lowercased by Python only"),
    ("/%C3%89/%C3%89", "unicode-lowercase"),
    ("/items/%D9%A1%D9%A2%D9%A3", "unicode-digit: Arabic-Indic ١٢٣ collapse Py-only"),
    ("/%D9%A1", "unicode-digit"),
    ("mailto:foo", "scheme:opaque stripped by Python urlsplit only"),
    ("tel:+1234", "scheme:opaque"),
]

_CONTROL_CHARS = ["/adm\tin", "/admin\t", "/admin\n", "/admin\r", "/che\tckout"]


def _run_rust(urls: list[str], tmp_path: Path) -> list[tuple[str, str]]:
    assert _NORMALIZE_RS.is_file(), f"normalizer not found: {_NORMALIZE_RS}"
    src = tmp_path / "rust_norm.rs"
    src.write_text(_WRAPPER_SRC.format(rs=str(_NORMALIZE_RS).replace("\\", "\\\\")))
    binary = tmp_path / "rust_norm"
    compile_proc = subprocess.run(
        [_RUSTC, "--edition", "2021", "-O", str(src), "-o", str(binary)],
        capture_output=True,
        text=True,
    )
    if compile_proc.returncode != 0:
        pytest.skip(f"rustc could not build normalize.rs wrapper:\n{compile_proc.stderr}")
    stdin = "\n".join(u.encode("utf-8", "surrogatepass").hex() for u in urls) + "\n"
    run = subprocess.run([str(binary)], input=stdin, capture_output=True, text=True)
    assert run.returncode == 0, f"rust normalizer crashed: {run.stderr}"
    out = [ln for ln in run.stdout.split("\n") if ln]
    assert len(out) == len(urls)
    return [
        (
            bytes.fromhex(line.partition("\t")[0]).decode("utf-8", "replace"),
            bytes.fromhex(line.partition("\t")[2]).decode("utf-8", "replace"),
        )
        for line in out
    ]


@pytest.mark.skipif(_RUSTC is None, reason="rustc not available — runtime Rust differential skipped")
def test_python_rust_agree_on_ascii_subset(tmp_path):
    rust = _run_rust(_AGREEMENT_URLS, tmp_path)
    mismatches = [
        (url, (normalize(url).path, normalize(url).category), (rp, rc))
        for url, (rp, rc) in zip(_AGREEMENT_URLS, rust)
        if (normalize(url).path, normalize(url).category) != (rp, rc)
    ]
    assert not mismatches, f"Py↔Rust normalize divergence:\n{mismatches}"


@pytest.mark.skipif(_RUSTC is None, reason="rustc not available — runtime Rust differential skipped")
def test_known_divergence_classes_still_diverge(tmp_path):
    rust = _run_rust([u for u, _ in _KNOWN_DIVERGENT], tmp_path)
    still_same = [
        (url, why, (normalize(url).path, normalize(url).category))
        for (url, why), (rp, rc) in zip(_KNOWN_DIVERGENT, rust)
        if (normalize(url).path, normalize(url).category) == (rp, rc)
    ]
    assert not still_same, f"A known Py↔Rust divergence has CLOSED:\n{still_same}"


@pytest.mark.skipif(_RUSTC is None, reason="rustc not available — runtime Rust differential skipped")
def test_raw_control_chars_now_agree(tmp_path):
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
        assert (pr.path, pr.category) == expected[url] == (rp, rc)


GOLDEN: list[tuple[str, str, str, str, str]] = [
    ("/", "/", "home", "/", "home"),
    ("/items/10243", "/items/*", "product", "/items/*", "product"),
    ("/api/v2/orders/00000abc-1234-5678-9abc-deadbeef0000", "/api/v2/orders/*", "api", "/api/v2/orders/*", "api"),
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


@pytest.mark.parametrize(("url", "py_path", "py_cat", "_rust_path", "_rust_cat"), GOLDEN)
def test_python_normalize_matches_golden(url, py_path, py_cat, _rust_path, _rust_cat):
    r = normalize(url)
    assert (r.path, r.category) == (py_path, py_cat)


@pytest.mark.parametrize(("url", "category"), CATEGORY_GOLDEN)
def test_python_category_golden(url, category):
    assert normalize(url).category == category


def test_divergence_classification_and_finding_014_no_evasion():
    for url, py_path, py_cat, rust_path, rust_cat in GOLDEN:
        same = (py_path, py_cat) == (rust_path, rust_cat)
        assert (not same) if url in KNOWN_DIVERGENCES else same

    for url, evaded_cat in {"/auth/login%2F..%2F..%2Fproduct": "product", "/items/%2e%2e%2fsecret": "other"}.items():
        assert normalize(url).category != evaded_cat
    assert ".." in normalize("/auth/login%2F..%2F..%2Fproduct").path


def _parse_rust_slice(const_name: str, arity: int) -> list[tuple[str, ...]]:
    assert _RUST_NORMALIZE.is_file(), f"Rust normalizer not found at {_RUST_NORMALIZE}"
    src = _RUST_NORMALIZE.read_text(encoding="utf-8")
    m = re.search(rf"const\s+{re.escape(const_name)}\s*:[^=]*=\s*&\[", src)
    assert m, f"could not locate `const {const_name}` slice"
    block = re.sub(r"//[^\n]*", "", src[m.end() : src.index("]", m.end())])
    literals = [s.replace('\\"', '"').replace("\\\\", "\\") for s in re.findall(r'"((?:[^"\\]|\\.)*)"', block)]
    assert literals and len(literals) % arity == 0
    return [tuple(literals[i : i + arity]) for i in range(0, len(literals), arity)]


def test_rust_static_tables_match_python():
    assert _parse_rust_slice("CATEGORY_MAP", 2) == list(_CATEGORY_MAP.items())
    assert _parse_rust_slice("PARITY_GOLDEN", 3) == [(u, rp, rc) for (u, _pp, _pc, rp, rc) in GOLDEN]
    assert _parse_rust_slice("CATEGORY_GOLDEN", 2) == [tuple(row) for row in CATEGORY_GOLDEN]


# ── Session Scoring VCL & Provisioning Setup ─────────────────────────────────

LOG_SVC = "TestLogSvcABC123"
TOKEN = "FAKE_TOKEN"
KEYS_STORE_ID = "KEYS_STORE_XYZ"
CURRENT_ITEM_PATH = f"/resources/stores/config/{KEYS_STORE_ID}/item/{CURRENT_KEY_HEX}"
PREVIOUS_ITEM_PATH = f"/resources/stores/config/{KEYS_STORE_ID}/item/{PREVIOUS_KEY_HEX}"
ITEM_COLLECTION_PATH = f"/resources/stores/config/{KEYS_STORE_ID}/item"


def _hex64(byte: int) -> str:
    return f"{byte:02x}" * 32


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
    for forbidden in (
        "X-Edge-Shield-Auth",
        "X-Edge-Scorer-Auth",
        "X-Edge-Service-Id",
        "set req.http.X-Edge-Prev-Route",
        "set req.http.X-Edge-Last-Route",
    ):
        assert forbidden not in vcl

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
        assert vcl.find(unset) > boundary_idx

    restart_idx = vcl.find("req.restarts == 1 && req.http.x-edge-score")
    assert restart_idx != -1 and "set var.fastly_req_do_shield = true;" in vcl
    assert vcl.count("unset req.http.x-sigsci-skip-inspection-once;") == 2
    assert vcl.find("unset req.http.x-sigsci-skip-inspection-once;", restart_idx) > restart_idx

    assert "!fastly.ddos_detected" in vcl
    assert (
        vcl.find('req.http.X-Edge-Scoring-Pass != "1"')
        < vcl.find("!fastly.ddos_detected")
        < vcl.find("std.tolower(req.url) !~")
    )
    assert "set req.http.x-edge-score-t0 = time.elapsed.usec;" in vcl
    assert 'set req.http.X-Edge-Scoring-Pass = "1";' in vcl
    assert (
        vcl.find(f"set req.backend = {SCORING_BACKEND_VCL_NAME};")
        < vcl.find('set req.http.x-sigsci-skip-inspection-once = "true";')
        < vcl.find("return(pass);")
    )
    assert vcl.find("set req.http.x-edge-score-t0") < vcl.find("return(pass);")


def test_pass_fetch_and_miss_snippets():
    vcl = pass_snippet("MyServiceId", "deadbeef1234567890")
    for expected in (
        f"if (req.backend == {SCORING_BACKEND_VCL_NAME})",
        'set bereq.method = "GET";',
        'set bereq.http.X-Edge-Service-Id = "MyServiceId";',
        'set bereq.http.X-Edge-Scorer-Auth = "deadbeef1234567890";',
        "unset bereq.http.X-Edge-Scoring-Pass;",
        "unset bereq.http.x-edge-score;",
    ):
        assert expected in vcl

    a, b = pass_snippet("Svc", "secret_a"), pass_snippet("Svc", "secret_b")
    assert a != b and "secret_a" in a and "secret_a" not in b and "secret_b" in b

    fetch_vcl, miss_vcl = fetch_snippet(), miss_snippet()
    assert f"if (req.backend == {SCORING_BACKEND_VCL_NAME})" in fetch_vcl and "return(deliver);" in fetch_vcl
    assert "unset bereq.http.x-edge-score;" in miss_vcl and "unset bereq.http.X-Edge-Scoring-Pass;" in miss_vcl


def test_deliver_snippet_subfields_rtt_and_anti_leak():
    vcl = deliver_snippet()
    assert 'if (req.http.X-Edge-Scoring-Pass == "1")' in vcl and "unset req.http.X-Edge-Scoring-Pass;" in vcl
    assert "\n  restart;\n" in vcl and "return(restart)" not in vcl

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

    assert vcl.index("set req.http.x-edge-score:matrix = resp.http.X-Edge-Matrix-Version;") < vcl.index(
        "unset resp.http.X-Edge-Matrix-Version;"
    )

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

    for stmt in (
        "} else {",
        'set req.http.x-edge-score:score = "0";',
        'set req.http.x-edge-score:compliance = "unknown";',
        'set req.http.x-edge-score:reason = "compute-unavailable-" + resp.status;',
        "declare local var.rtt INTEGER;",
        "set var.rtt = std.atoi(time.elapsed.usec);",
        "set var.rtt -= std.atoi(req.http.x-edge-score-t0);",
        'set req.http.x-edge-score:rtt = "" + var.rtt;',
        'if (fastly.ff.visits_this_service == 0 && req.http.x-edge-score:set-cookie != "") {',
        "add resp.http.Set-Cookie = req.http.x-edge-score:set-cookie;",
    ):
        assert stmt in vcl
    assert vcl.index("set req.http.x-edge-score:rtt") < vcl.index("if (resp.status == 200)")
    assert "set resp.http.Set-Cookie =" not in vcl and "X-Edge-Shield-Auth" not in vcl


def test_enforce_snippet_guards_and_parameterized_status_codes():
    vcl = enforce_snippet()
    tail = vcl[vcl.index("if (") :]
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
    assert "UniqueServiceXYZ" in snippets[SCORING_PASS_NAME] and "deadbeef1234" in snippets[SCORING_PASS_NAME]
    for other in (SCORING_RECV_NAME, SCORING_FETCH_NAME, SCORING_DELIVER_NAME, SCORING_MISS_NAME, SCORING_ENFORCE_NAME):
        assert "UniqueServiceXYZ" not in snippets[other] and "deadbeef1234" not in snippets[other]

    assert generate_scoring_vcl("Svc", "a" * 64) == generate_scoring_vcl("Svc", "a" * 64)
    assert not [n for n, b in snippets.items() if "matrix-age-days" in b.lower()]

    bad_secret = 'abc" || req.http.x-evil == "1'
    for fn in (lambda: pass_snippet("Svc", bad_secret), lambda: generate_scoring_vcl("Svc", bad_secret)):
        with pytest.raises(ValueError, match="request_secret"):
            fn()
    with pytest.raises(ValueError, match="exclude_url_regex"):
        recv_snippet("Svc", exclude_url_regex=r'\.(png)"; bad')
    assert r"\.(png|jpg)" in recv_snippet("Svc", exclude_url_regex=r"^/skip/.*\.(png|jpg)$")


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

    assert (
        result["scoring_service_id"],
        result["scoring_service_name"],
        result["scoring_domain"],
        result["scoring_keys_store_id"],
        result["scoring_config_store_id"],
        result["scoring_matrix_store_id"],
    ) == (
        "SCORING_SVC_ID",
        f"Session Scoring Service for {LOG_SVC}",
        f"fos-{LOG_SVC.lower()}-session-scorer.edgecompute.app",
        "KEYS_STORE_ID",
        "CFG_STORE_ID",
        "MATRIX_STORE_ID",
    )
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
    assert len([c for c in calls if c.args[:2] == ("POST", "/resources/stores/config")]) == 2

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

    assert (
        result["scoring_service_id"],
        result["scoring_keys_store_id"],
        result["scoring_config_store_id"],
        result["scoring_matrix_store_id"],
        result["request_secret"],
        result["aes_key_hex"],
    ) == ("EXISTING_SVC_ID", "EXISTING_KEYS", "EXISTING_CFG", "EXISTING_MATRIX", stored_secret, "")
    if anchor_missing:
        anchor_post = next(
            c
            for c in fastly_mock.call_args_list
            if c.args[:2] == ("POST", "/resources/stores/config/EXISTING_CFG/item")
        )
        assert anchor_post.args[2]["item_key"] == SCORING_ENABLED_AT_KEY and anchor_post.args[2]["item_value"].isdigit()


def test_delete_runs_full_teardown_and_handles_edge_cases():
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
    idx = calls_in_order.index
    assert idx(("PUT", "/service/SVC_X/version/1/deactivate")) < idx(("DELETE", "/service/SVC_X"))
    assert idx(("DELETE", "/service/SVC_X")) < idx(("DELETE", "/resources/stores/config/KEYS_X"))
    assert idx(("DELETE", "/service/SVC_X")) < idx(("DELETE", "/resources/stores/config/CFG_X"))
    assert (
        idx(("DELETE", "/service/SVC_X"))
        < idx(("DELETE", "/resources/stores/kv/MATRIX_X/keys/matrix"))
        < idx(("DELETE", "/resources/stores/kv/MATRIX_X"))
    )
    assert ("PUT", "/service/SVC_X/version/2/deactivate") not in calls_in_order

    # Empty service_id is a no-op
    noop_mock = MagicMock()
    with patch("backend.provision.session_scoring_setup.fastly", noop_mock):
        delete_scoring_service("", token=TOKEN)
    noop_mock.assert_not_called()

    # 404 tolerated; 500 on store delete reported
    def err_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", "/service/MISSING/version") or path.startswith(
            "/resources/stores/config/MISSING_STORE"
        ):
            raise RuntimeError("404 not found")
        if (method, path) == ("GET", "/service/SVC/version"):
            return []
        if method == "DELETE" and path.startswith("/resources/stores/config/STUCK"):
            raise RuntimeError("500 internal error")
        return {}

    msgs: list[str] = []
    with patch("backend.provision.session_scoring_setup.fastly", MagicMock(side_effect=err_effect)):
        delete_scoring_service("MISSING", token=TOKEN)
        delete_scoring_service(
            "SVC", scoring_keys_store_id="MISSING_STORE", scoring_config_store_id="FINE", token=TOKEN
        )
        failed = delete_scoring_service(
            "SVC", scoring_keys_store_id="STUCK", scoring_config_store_id="FINE", token=TOKEN, status_cb=msgs.append
        )

    assert ("scoring_keys", "STUCK") in failed and all(sid != "FINE" for _, sid in failed)
    assert any("STUCK" in m and "manual" in m.lower() for m in msgs)


def test_rotate_aes_key_scenarios():
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
    assert any(
        (c[2] or {}).get("item_value") == old_current
        for c in calls_1
        if (c[1] == PREVIOUS_ITEM_PATH and c[0] == "PATCH")
        or (c[1] == ITEM_COLLECTION_PATH and c[0] == "POST" and (c[2] or {}).get("item_key") == PREVIOUS_KEY_HEX)
    )

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
    assert [c[2] for c in calls_2 if c[:2] == ("PATCH", PREVIOUS_ITEM_PATH)] == [{"item_value": old_cur2}]
    assert [c[2] for c in calls_2 if c[:2] == ("PATCH", CURRENT_ITEM_PATH)] == [{"item_value": res2["current_key_hex"]}]
    assert old_prev2 not in [(c[2] or {}).get("item_value") for c in calls_2 if c[0] in ("PATCH", "POST") and c[2]]

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
    assert (
        "POST",
        ITEM_COLLECTION_PATH,
        {"item_key": CURRENT_KEY_HEX, "item_value": res3["current_key_hex"]},
    ) in calls_3
    assert len([c for c in calls_3 if c[:2] == ("PATCH", CURRENT_ITEM_PATH)]) == 1

    # 4. Validation, auth errors, chaining, and secrets.token_hex(32)
    fastly_mock = MagicMock()
    with (
        patch("backend.provision.session_scoring_setup.fastly", fastly_mock),
        pytest.raises(ValueError, match="scoring_keys_store_id is required"),
    ):
        rotate_aes_key("", token=TOKEN)
    fastly_mock.assert_not_called()

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

    sentinel = "ff" * 32
    with (
        patch("backend.provision.session_scoring_setup.fastly", MagicMock(side_effect=_stateful)),
        patch("secrets.token_hex", return_value=sentinel) as token_hex_mock,
    ):
        res = rotate_aes_key(KEYS_STORE_ID, token=TOKEN)
    assert res["current_key_hex"] == sentinel and sss.CURRENT_KEY_HEX == "current_key_hex"
    token_hex_mock.assert_called_with(32)

"""Tests for backend.scoring.matrix — transition matrix builder."""

from __future__ import annotations

import io
import json
from datetime import UTC, datetime, timedelta

from backend.scoring.matrix import (
    TransitionMatrix,
    build_matrix,
    default_version,
    write_matrix,
)
from backend.scoring.normalize import normalize

UTC = UTC


def _session(urls: list[str], dwell_seconds: float = 5.0, base: datetime | None = None) -> dict:
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
    sessions = [_session(["/home"]), _session(["/home", "/products"])]
    m, stats = build_matrix(sessions)
    assert stats.sessions_dropped_short == 1
    assert stats.sessions_kept == 1
    # Only the 2-event session contributed.
    assert m.transition_count == 1


def test_build_matrix_drops_impossibly_fast_sessions():
    """A session with 10 events spaced 50ms apart looks like an instant
    scraper — mean dwell ~0.05s < 0.2s floor."""
    urls = [f"/p{i}" for i in range(10)]
    s = _session(urls, dwell_seconds=0.05)
    m, stats = build_matrix([s])
    assert stats.sessions_dropped_fast == 1
    assert stats.sessions_kept == 0
    assert m.transition_count == 0


def test_build_matrix_keeps_normal_sessions():
    """Normal user: 5s mean dwell, 5 events → kept and contributes 4 transitions."""
    s = _session(["/home", "/products", "/products/42", "/cart", "/checkout"], dwell_seconds=5.0)
    m, stats = build_matrix([s])
    assert stats.sessions_kept == 1
    assert stats.sessions_dropped_short == 0
    assert stats.sessions_dropped_fast == 0
    assert m.transition_count == 4


def test_build_matrix_aggregates_across_sessions():
    sessions = [
        _session(["/home", "/products", "/cart"], dwell_seconds=3.0),
        _session(["/home", "/products", "/cart"], dwell_seconds=3.0),
        _session(["/home", "/about", "/products"], dwell_seconds=3.0),
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
        _session(["/home", "/products/42"], dwell_seconds=3.0),
        _session(["/home", "/products/99"], dwell_seconds=3.0),
    ]
    m, _ = build_matrix(sessions)
    # Both collapsed to /products/*
    assert m.counts["/home"] == {"/products/*": 2}
    assert "/products/42" not in m.counts.get("/home", {})


def test_build_matrix_respects_overrides():
    """Custom thresholds work — disable the fast filter, get more sessions."""
    s = _session(["/a", "/b", "/c"], dwell_seconds=0.1)
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


# ── write_matrix: serialization ──────────────────────────────────────────────


def test_write_matrix_is_canonical_json():
    """Output must be deterministic across runs on the same input —
    needed for matrix-version diffs and Wasm-package hash stability."""
    m = TransitionMatrix()
    m.add_transition(normalize("/home"), normalize("/products"))
    m.session_count = 1

    buf1, buf2 = io.StringIO(), io.StringIO()
    write_matrix(m, "test-v1", buf1)
    write_matrix(m, "test-v1", buf2)

    # built_at differs across calls — strip it to compare the rest.
    j1 = json.loads(buf1.getvalue())
    j2 = json.loads(buf2.getvalue())
    j1.pop("built_at")
    j2.pop("built_at")
    assert j1 == j2


def test_write_matrix_includes_all_required_keys():
    m = TransitionMatrix()
    m.add_transition(normalize("/home"), normalize("/products"))
    buf = io.StringIO()
    write_matrix(m, "test-v1", buf)
    j = json.loads(buf.getvalue())
    expected_keys = {
        "version",
        "built_at",
        "vocab_size",
        "session_count",
        "transition_count",
        "counts",
        "row_totals",
        "categories",
        "anchors",
    }
    assert set(j.keys()) == expected_keys
    assert j["version"] == "test-v1"
    assert j["vocab_size"] == 2


# ── End-to-end: built matrix is usable by the scorer ─────────────────────────


def test_built_matrix_round_trips_through_scorer():
    """The schema written by ``write_matrix`` is exactly what the L2 scorer
    expects — verify the contract end-to-end."""
    from backend.scoring.normalize import normalize as norm
    from backend.scoring.scorer import score_layer2

    # Build a meaningful dataset: 100 sessions doing the common funnel,
    # 1 session doing an off-pattern jump. Laplace smoothing then leaves
    # the common transition with P ≈ 1, the rare one with P ≈ 0.01.
    common = [_session(["/home", "/products"]) for _ in range(100)]
    rare = [_session(["/home", "/about"])]
    matrix, _ = build_matrix(common + rare)
    buf = io.StringIO()
    write_matrix(matrix, "test", buf)
    loaded = json.loads(buf.getvalue())

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
    # YYYY-MM-DD-a
    assert len(v) == 12
    assert v[-2:] == "-a"
    parts = v.split("-")
    assert len(parts) == 4
    int(parts[0])  # parseable as year
    int(parts[1])
    int(parts[2])

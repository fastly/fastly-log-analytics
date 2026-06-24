"""Tests for backend.scoring.matrix — transition matrix builder."""

from __future__ import annotations

import io
import json
from datetime import UTC, datetime, timedelta

from backend.scoring.matrix import (
    TransitionMatrix,
    build_matrix,
    default_version,
    serialize_kv,
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

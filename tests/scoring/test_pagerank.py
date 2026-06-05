"""Tests for backend.scoring.pagerank — funnel-anchor identification."""

from __future__ import annotations

from backend.scoring.matrix import TransitionMatrix
from backend.scoring.normalize import normalize
from backend.scoring.pagerank import (
    compute_anchors,
    pagerank,
    select_anchors,
)


def _make_matrix(edges: list[tuple[str, str, int]]) -> TransitionMatrix:
    m = TransitionMatrix()
    for src, dst, n in edges:
        for _ in range(n):
            m.add_transition(normalize(src), normalize(dst))
    return m


# ── pagerank: math sanity ────────────────────────────────────────────────────


def test_pagerank_empty_matrix_returns_empty_dict():
    assert pagerank(TransitionMatrix()) == {}


def test_pagerank_sums_to_one_within_tolerance():
    """All standard PageRank impls preserve unit total mass."""
    m = _make_matrix([("/a", "/b", 10), ("/b", "/c", 8), ("/c", "/a", 5), ("/a", "/c", 3)])
    rank = pagerank(m)
    assert abs(sum(rank.values()) - 1.0) < 1e-6


def test_pagerank_uniform_on_symmetric_cycle():
    """Symmetric 3-cycle a→b→c→a: stationary distribution is uniform."""
    m = _make_matrix([("/a", "/b", 1), ("/b", "/c", 1), ("/c", "/a", 1)])
    rank = pagerank(m)
    assert abs(rank["/a"] - rank["/b"]) < 1e-4
    assert abs(rank["/b"] - rank["/c"]) < 1e-4
    assert abs(rank["/a"] - 1 / 3) < 1e-3


def test_pagerank_hub_outranks_periphery():
    """Star graph (4 periphery nodes all pointing to /hub) → /hub wins."""
    m = _make_matrix(
        [
            ("/p1", "/hub", 1),
            ("/p2", "/hub", 1),
            ("/p3", "/hub", 1),
            ("/p4", "/hub", 1),
            # Plus self-loop on hub so it's not a dangling node.
            ("/hub", "/hub", 1),
        ]
    )
    rank = pagerank(m)
    for p in ("/p1", "/p2", "/p3", "/p4"):
        assert rank["/hub"] > rank[p], f"hub should outrank {p}"


def test_pagerank_dangling_node_handled():
    """A node with no outlinks (/exit) shouldn't crash the iteration —
    its mass redistributes uniformly each step."""
    m = _make_matrix([("/start", "/exit", 5)])
    # /exit has no entry in matrix.counts (it's never a source).
    rank = pagerank(m)
    # Both routes have positive mass.
    assert rank["/start"] > 0
    assert rank["/exit"] > 0
    # Mass still totals to 1.
    assert abs(sum(rank.values()) - 1.0) < 1e-6


# ── select_anchors ───────────────────────────────────────────────────────────


def test_select_anchors_returns_top_k_by_rank():
    rank = {"/a": 0.5, "/b": 0.3, "/c": 0.15, "/d": 0.05}
    anchors = select_anchors(rank, fraction=0.5, min_anchors=1, max_anchors=10)
    # 4 routes * 0.5 = 2 anchors.
    assert anchors == ["/a", "/b"]


def test_select_anchors_respects_min():
    rank = {"/only-one": 1.0}
    anchors = select_anchors(rank, fraction=0.05, min_anchors=5)
    # Asks for 0 anchors by fraction, but min is 5 → return all available.
    assert anchors == ["/only-one"]  # can't go above what exists


def test_select_anchors_respects_max():
    rank = {f"/r{i}": 1.0 - i * 0.0001 for i in range(1000)}
    anchors = select_anchors(rank, fraction=0.5, max_anchors=10)
    assert len(anchors) == 10
    assert anchors[0] == "/r0"  # highest rank


def test_select_anchors_deterministic_tie_breaking():
    """When ranks tie, sort by route name ascending — guarantees the same
    anchors across runs even if dict iteration order varies."""
    rank = {"/c": 0.5, "/a": 0.5, "/b": 0.5}
    anchors = select_anchors(rank, fraction=1.0, min_anchors=1, max_anchors=3)
    # All tied at 0.5 → name-sorted: /a, /b, /c
    assert anchors == ["/a", "/b", "/c"]


def test_select_anchors_empty_input():
    assert select_anchors({}) == []


# ── compute_anchors: integration ─────────────────────────────────────────────


def test_compute_anchors_mutates_matrix_in_place():
    m = _make_matrix(
        [
            ("/home", "/products", 100),
            ("/products", "/cart", 80),
            ("/cart", "/checkout", 50),
            ("/checkout", "/home", 10),
            ("/about", "/home", 5),
        ]
    )
    assert m.anchors == []
    result = compute_anchors(m, fraction=0.4, min_anchors=2, max_anchors=10)
    assert m.anchors == result
    assert len(result) >= 2
    # Hub-like routes (/home, /products) should dominate the anchor list.
    assert "/home" in result or "/products" in result

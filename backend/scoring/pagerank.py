"""PageRank-based funnel-anchor identification (research doc §4.2 / §7).

Computes the stationary distribution of a Markov walker over the
transition matrix, then declares the top-K routes as "anchors". The
Layer 2 scorer uses anchors for skip-gram lookback: a transition
``prev_anchor → current`` rescues an otherwise-rare ``prev → current``
when the intervening pages are non-anchor auxiliaries (e.g. /about-us,
/privacy-policy, blog posts).

Pure standard-library implementation — power-iteration with a damping
factor (canonical PageRank). No NumPy dependency so the same algorithm
is trivial to port to Rust.
"""

from __future__ import annotations

import logging
from typing import Final

from backend.scoring.matrix import TransitionMatrix

logger = logging.getLogger(__name__)

PAGERANK_DAMPING: Final[float] = 0.85
PAGERANK_TOLERANCE: Final[float] = 1e-7
PAGERANK_MAX_ITER: Final[int] = 200
DEFAULT_ANCHOR_FRACTION: Final[float] = 0.20  # top 20% of routes by PR


def _row_normalized_outlinks(matrix: TransitionMatrix) -> dict[str, dict[str, float]]:
    """Convert raw counts → row-stochastic transition probabilities.

    Pages with no outlinks are handled in pagerank() as dangling nodes
    that redistribute their mass uniformly (standard PageRank treatment).
    """
    out: dict[str, dict[str, float]] = {}
    for src, dests in matrix.counts.items():
        total = matrix.row_totals.get(src, 0)
        if total <= 0:
            continue
        out[src] = {dst: cnt / total for dst, cnt in dests.items()}
    return out


def pagerank(
    matrix: TransitionMatrix,
    *,
    damping: float = PAGERANK_DAMPING,
    tol: float = PAGERANK_TOLERANCE,
    max_iter: int = PAGERANK_MAX_ITER,
) -> dict[str, float]:
    """Compute PageRank scores for every route in the vocab.

    Implementation: standard power iteration with damping.
        PR(p) = (1-d)/N + d * Σ_{q→p} PR(q) / outdeg(q) + dangling_mass/N
    where dangling_mass = Σ_{q has no outlinks} PR(q) — the "stuck"
    probability mass that gets redistributed uniformly each iteration.
    """
    vocab = sorted(matrix.vocab)  # sorted → deterministic
    n = len(vocab)
    if n == 0:
        return {}

    outlinks = _row_normalized_outlinks(matrix)
    inverted: dict[str, list[tuple[str, float]]] = {p: [] for p in vocab}
    for src, dests in outlinks.items():
        for dst, prob in dests.items():
            inverted.setdefault(dst, []).append((src, prob))

    rank = {p: 1.0 / n for p in vocab}
    teleport = (1.0 - damping) / n
    dangling_nodes = [p for p in vocab if p not in outlinks]

    for it in range(max_iter):
        dangling_mass = sum(rank[p] for p in dangling_nodes)
        dangling_contribution = damping * dangling_mass / n
        new_rank: dict[str, float] = {}
        for p in vocab:
            incoming = sum(rank[q] * prob for q, prob in inverted.get(p, []))
            new_rank[p] = teleport + dangling_contribution + damping * incoming

        delta = sum(abs(new_rank[p] - rank[p]) for p in vocab)
        rank = new_rank
        if delta < tol:
            logger.debug("[pagerank] converged after %d iterations (delta=%.2e)", it + 1, delta)
            break
    else:
        logger.warning("[pagerank] hit max_iter=%d without converging (final delta=%.2e)", max_iter, delta)

    return rank


def select_anchors(
    rank: dict[str, float],
    *,
    fraction: float = DEFAULT_ANCHOR_FRACTION,
    min_anchors: int = 5,
    max_anchors: int = 50,
) -> list[str]:
    """Pick the top-K routes by PageRank as anchors.

    K = clamp(round(n * fraction), min_anchors, max_anchors). The clamp
    handles both tiny sites (where 20% would be 1 anchor — too few for
    skip-gram to help) and giant sites (where 20% would be hundreds —
    too many; the L2 lookback only walks back a few steps anyway)."""
    if not rank:
        return []
    target = round(len(rank) * fraction)
    k = max(min_anchors, min(max_anchors, target))
    # Sort by (-rank, route) so ties break deterministically by route name.
    sorted_routes = sorted(rank.items(), key=lambda kv: (-kv[1], kv[0]))
    return [route for route, _ in sorted_routes[:k]]


def compute_anchors(matrix: TransitionMatrix, **kwargs) -> list[str]:
    """Convenience wrapper: pagerank + select_anchors in one call,
    mutating ``matrix.anchors`` in place."""
    rank = pagerank(matrix)
    anchors = select_anchors(rank, **kwargs)
    matrix.anchors = anchors
    return anchors

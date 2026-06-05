#!/usr/bin/env -S uv run python
"""CLI: train a session-scoring matrix from sessionized JSONL traces.

Pipeline (per research doc §7):
    1. Read sessions (output of scripts/scoring/extract_traces.py).
    2. Build transition matrix (drops bot-like sessions per §1.3 filter).
    3. Compute PageRank anchors for the L2 skip-gram lookback.
    4. (Optional) Run ROC-AUC eval against a labeled-negatives file.
    5. Write matrix.json.

Usage:
    ./scripts/scoring/train.py \\
        --in tests/fixtures/scoring/traces_<service-id>.jsonl \\
        --out compute/scorer/matrix.json
    ./scripts/scoring/train.py --in traces.jsonl --out matrix.json --version 2026-06-15-a
    ./scripts/scoring/train.py --in traces.jsonl --out matrix.json --labels labels.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("train")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--in", dest="input", type=Path, required=True, help="JSONL traces file")
    parser.add_argument("--out", type=Path, required=True, help="Output matrix.json path")
    parser.add_argument(
        "--version",
        default=None,
        help="Matrix version string. Defaults to YYYY-MM-DD-a.",
    )
    parser.add_argument(
        "--labels",
        type=Path,
        default=None,
        help="Optional JSONL file with {session: {...}, label: 'good'|'bad'} "
        "rows for ROC-AUC evaluation against the trained matrix.",
    )
    parser.add_argument(
        "--min-auc",
        type=float,
        default=None,
        help="Pass-fail AUC threshold when --labels is provided. Default 0.85.",
    )
    parser.add_argument(
        "--anchor-fraction",
        type=float,
        default=None,
        help="Top-K-by-PageRank fraction for anchors. Default 0.20.",
    )
    parser.add_argument(
        "--min-events",
        type=int,
        default=None,
        help="Drop sessions shorter than this during training. Default 2.",
    )
    parser.add_argument(
        "--min-dwell-s",
        type=float,
        default=None,
        help="Drop sessions whose mean dwell is below this. Default 0.2.",
    )
    args = parser.parse_args()

    if not args.input.exists():
        log.error("input not found: %s", args.input)
        return 1

    from backend.scoring import evaluate as ev_mod
    from backend.scoring.matrix import (
        build_matrix_from_jsonl,
        default_version,
        write_matrix_path,
    )
    from backend.scoring.pagerank import compute_anchors

    version = args.version or default_version()

    build_kwargs: dict = {}
    if args.min_events is not None:
        build_kwargs["min_events"] = args.min_events
    if args.min_dwell_s is not None:
        build_kwargs["min_mean_dwell_s"] = args.min_dwell_s

    log.info("reading sessions from %s …", args.input)
    matrix, stats = build_matrix_from_jsonl(args.input, **build_kwargs)
    log.info(
        "sessions: in=%d kept=%d dropped(short=%d, fast=%d) | transitions=%d, routes=%d",
        stats.sessions_in,
        stats.sessions_kept,
        stats.sessions_dropped_short,
        stats.sessions_dropped_fast,
        stats.transitions,
        stats.routes_seen,
    )

    anchor_kwargs: dict = {}
    if args.anchor_fraction is not None:
        anchor_kwargs["fraction"] = args.anchor_fraction
    anchors = compute_anchors(matrix, **anchor_kwargs)
    log.info("anchors: %d (top: %s)", len(anchors), ", ".join(anchors[:5]))

    write_matrix_path(matrix, version, args.out)
    log.info("wrote matrix → %s (version=%s)", args.out, version)

    if args.labels:
        if not args.labels.exists():
            log.error("--labels file not found: %s", args.labels)
            return 1
        eval_kwargs: dict = {}
        if args.min_auc is not None:
            eval_kwargs["min_auc"] = args.min_auc

        labeled = []
        with args.labels.open() as f:
            for line in f:
                row = json.loads(line)
                labeled.append((row["session"], row["label"]))

        # Load the matrix we just wrote so the evaluator sees the same
        # serialized form as the runtime scorer will.
        with args.out.open() as f:
            loaded = json.load(f)
        result = ev_mod.evaluate(loaded, labeled, **eval_kwargs)
        log.info("evaluation: %s", result.summary())
        if not result.passed:
            log.error("matrix DID NOT pass quality bar — refusing to mark as good")
            return 2

    log.info("training complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

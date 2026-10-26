"""Edge session-scoring system.

Hybrid Fastly Compute (Wasm) + VCL session-anomaly scoring. This Python
package contains:

- The offline training pipeline (sessionize prod logs → transition matrix +
  PageRank anchors) used to compile matrix.json for the edge scorer.
- A reference implementation of the scoring logic (Layer 1 universal
  behavioral + Layer 2 route-transition) in pure Python. The Rust/Wasm
  port under ``compute/scorer/`` must produce byte-identical scores against
  the shared fixture set.
- The AES-GCM-with-AAD cookie codec, also paired 1:1 with the Rust port.

See the session-scoring runbook (``docs/session_scoring_runbook.md``)
for operational guidance.
"""

from __future__ import annotations

import sys as _sys

from backend.scoring import features

normalize = features
scorer = features
labels = features
fixtures = features
evaluate = features
matrix = features
_sys.modules.setdefault("backend.scoring.normalize", features)
_sys.modules.setdefault("backend.scoring.scorer", features)
_sys.modules.setdefault("backend.scoring.labels", features)
_sys.modules.setdefault("backend.scoring.fixtures", features)
_sys.modules.setdefault("backend.scoring.evaluate", features)
_sys.modules.setdefault("backend.scoring.matrix", features)
if "backend" in _sys.modules and hasattr(_sys.modules["backend"], "_MODULE_ALIASES"):
    _sys.modules["backend"]._MODULE_ALIASES["backend.scoring.evaluate"] = "backend.scoring.features"
    _sys.modules["backend"]._MODULE_ALIASES["backend.scoring.matrix"] = "backend.scoring.features"

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

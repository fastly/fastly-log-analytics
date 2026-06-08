//! Embedded transition-matrix loader.
//!
//! `matrix.json` is produced by `scripts/scoring/train.py` and embedded at
//! compile time via [`include_bytes!`]. We parse it lazily into a
//! [`TransitionMatrix`] on first use and cache the parsed shape for the
//! lifetime of the Wasm instance — a single parse + many lookups per
//! request, no allocations on the hot path.

use serde::Deserialize;
use std::collections::HashMap;
use std::sync::OnceLock;

/// Embedded at compile time. The workspace tracks a no-op placeholder at
/// `matrix.default.json` (vocab_size=0, no counts) so anyone can `cargo
/// build` and `cargo test` on a fresh checkout. The deploy pipeline
/// (`scripts/scoring/build_wasm.sh`, written in Phase D) copies the real
/// trained `matrix.json` over this path before invoking `fastly compute
/// build`, embedding the customer-specific matrix into the published Wasm.
///
/// When the embedded blob is the empty default (vocab_size == 0), the
/// scorer's L2 layer disables itself — matching the doc's pre-Day-7
/// behavior (§4.3 blend weight is 0).
const EMBEDDED_MATRIX_BYTES: &[u8] = include_bytes!("../matrix.default.json");

#[derive(Debug, Clone, Default, Deserialize)]
pub struct TransitionMatrix {
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub vocab_size: u32,
    #[serde(default)]
    pub session_count: u64,
    #[serde(default)]
    pub transition_count: u64,
    #[serde(default)]
    pub counts: HashMap<String, HashMap<String, u64>>,
    #[serde(default)]
    pub row_totals: HashMap<String, u64>,
    #[serde(default)]
    pub categories: HashMap<String, String>,
    #[serde(default)]
    pub anchors: Vec<String>,
}

static MATRIX_CACHE: OnceLock<TransitionMatrix> = OnceLock::new();

/// Lazily parse and return a static-lifetime reference to the embedded
/// matrix. Returns `None` if the embedded blob is empty (build artifact
/// missing) — the request handler treats that as "L2 disabled, fall back
/// to L1 only" which matches the doc's pre-Day-7 behavior (§4.3).
pub fn load_embedded() -> Option<&'static TransitionMatrix> {
    if EMBEDDED_MATRIX_BYTES.is_empty() {
        return None;
    }
    Some(MATRIX_CACHE.get_or_init(|| {
        serde_json::from_slice(EMBEDDED_MATRIX_BYTES).expect("embedded matrix.json malformed")
    }))
}

/// Convenience for tests: parse from an arbitrary JSON byte slice.
#[cfg(test)]
pub fn parse(bytes: &[u8]) -> serde_json::Result<TransitionMatrix> {
    serde_json::from_slice(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
    {
      "version": "test-2026-06-01-a",
      "built_at": "2026-06-01T00:00:00+00:00",
      "vocab_size": 3,
      "session_count": 10,
      "transition_count": 20,
      "counts": {"/home": {"/products": 15, "/cart": 5}},
      "row_totals": {"/home": 20},
      "categories": {"/home": "home", "/products": "product", "/cart": "cart"},
      "anchors": ["/home", "/products"]
    }
    "#;

    #[test]
    fn parse_sample_round_trip() {
        let m = parse(SAMPLE.as_bytes()).unwrap();
        assert_eq!(m.version, "test-2026-06-01-a");
        assert_eq!(m.vocab_size, 3);
        assert_eq!(m.counts.get("/home").unwrap().get("/products"), Some(&15));
        assert_eq!(m.row_totals.get("/home"), Some(&20));
        assert_eq!(m.anchors, vec!["/home", "/products"]);
    }

    #[test]
    fn parse_handles_missing_optional_fields() {
        let minimal = r#"{"vocab_size": 1}"#;
        let m = parse(minimal.as_bytes()).unwrap();
        assert_eq!(m.vocab_size, 1);
        assert!(m.counts.is_empty());
        assert!(m.anchors.is_empty());
    }
}

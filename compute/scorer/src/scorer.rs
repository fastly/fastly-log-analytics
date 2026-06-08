//! Layer 1 (universal behavioral) + Layer 2 (route transition) + combined.
//!
//! Mirrors `backend/scoring/scorer.py`. Constants are pinned to the same
//! values; the math is the same algebraic identity for variance and the
//! same `-log10(p) * 100/6` curve for the L2 score. Every Python test in
//! `tests/scoring/test_scorer.py` has a paired Rust test here so behavioural
//! drift between the two impls is caught at build time.

use crate::cookie::{quantize_score, SessionState};
use crate::matrix::TransitionMatrix;
use crate::normalize::Route;

// ── Layer 1 tuning (research doc §4.1) ──────────────────────────────────────

pub const L1_TIMING_WARMUP_SEQ: u16 = 3;
pub const L1_FAST_DWELL_THRESHOLD_S: f64 = 0.20;
pub const L1_ROBOTIC_VARIANCE_THRESHOLD: f64 = 0.05;
// Security #037: was 0.5 — there was a "robotic safe zone" between
// L1_FAST_DWELL_THRESHOLD_S (0.20) and L1_ROBOTIC_DWELL_LOW_S (0.50)
// where a low-variance bot averaging 0.30s/page scored zero. The
// audit verified the gap was exploitable. Lower the threshold so the
// robotic detector covers the previously-uncovered band; the only
// behavior change is that bots in that gap now get the ROBOTIC score
// instead of zero.
pub const L1_ROBOTIC_DWELL_LOW_S: f64 = 0.20;
pub const L1_ROBOTIC_DWELL_HIGH_S: f64 = 3.0;
pub const L1_SCORE_FAST: u8 = 50;
pub const L1_SCORE_ROBOTIC: u8 = 40;
pub const L1_SCORE_COOKIE_MISSING: u8 = 75;
// Security #036: cookie tampering is a strictly stronger anomaly signal
// than missing — missing might be a fresh visitor or a privacy-mode
// browser, tampering is intentional. Cap tampered sessions at 100
// rather than the 75 ceiling missing/expired share.
pub const L1_SCORE_COOKIE_TAMPERED: u8 = 100;

// ── Layer 2 tuning (research doc §4.2) ──────────────────────────────────────

pub const L2_LAPLACE_ALPHA: f64 = 0.5;
pub const L2_SKIPGRAM_BETA: f64 = 0.7;

/// Map a transition probability ∈ [0, 1] to an L2 anomaly score [0, 100].
/// Pinned to mirror Python's `_l2_score_from_trans_prob`.
pub fn l2_score_from_trans_prob(p: f64) -> u8 {
    if p >= 1.0 {
        return 0;
    }
    if p <= 1e-12 {
        return 100;
    }
    let raw = -p.log10() * (100.0 / 6.0);
    let clamped = raw.clamp(0.0, 100.0);
    clamped.round() as u8
}

// ── Combined output ─────────────────────────────────────────────────────────

#[derive(Debug, Default, Clone)]
pub struct ScoreResult {
    pub score: u8,
    pub l1_score: u8,
    pub l2_score: u8,
    pub reasons: Vec<String>,
    pub cookie_compliance: String,
    pub mean_dwell_s: f64,
    pub variance_s2: f64,
    pub trans_prob: f64,
    pub matrix_version: String,
}

impl ScoreResult {
    pub fn headers(&self) -> Vec<(&'static str, String)> {
        vec![
            ("X-Edge-Score", self.score.to_string()),
            ("X-Edge-Score-L1", self.l1_score.to_string()),
            ("X-Edge-Score-L2", self.l2_score.to_string()),
            ("X-Edge-Cookie-Compliance", self.cookie_compliance.clone()),
            ("X-Edge-Score-Reason", self.reasons.join(",")),
            ("X-Edge-Matrix-Version", self.matrix_version.clone()),
        ]
    }
}

// ── Layer 1 ─────────────────────────────────────────────────────────────────

fn running_mean_variance(state: &SessionState) -> (f64, f64) {
    if state.seq == 0 {
        return (0.0, 0.0);
    }
    let n = state.seq as f64;
    let mean = state.sum_dt as f64 / n;
    let second_moment = state.sum_dt_sq as f64 / n;
    let var = (second_moment - mean * mean).max(0.0);
    (mean, var)
}

// FOLLOW-UP for security #038 (Unwindowed mean allows amortized delays).
//
// The current implementation accumulates sum_dt + sum_dt_sq over the
// entire session. An attacker who's fast at the start of a session
// (triggering impossibly-fast → robotic-consistency) can deliberately
// slow down later to drag the mean back into "normal" territory,
// effectively rolling the L1 score off. The audit confirmed this is
// exploitable in principle.
//
// A real fix would replace the cumulative sum with a sliding window of
// the last N (~20) dwells. That requires:
//   1. Cookie schema v3: add a fixed-size ring buffer of u16 dwells
//      (40 bytes for 20 entries) to SessionState.
//   2. Backward-compat: v2 cookies treated as "missing" (rotate).
//   3. Both Rust + Python implementations + cross-language fixture tests.
//   4. update_state pushes the new dwell into the buffer (eviction = FIFO).
//   5. score_layer1 computes mean/variance over the buffer only.
//
// Partial mitigation already in place:
//   - 30min idle expire (cookie::SESSION_IDLE_EXPIRE_S) rotates the
//     session and clears the timing accumulator, bounding the
//     amortization window.
//   - 24h hard cap (cookie::SESSION_HARD_CAP_S) caps total session
//     lifetime.
//   - The threshold-matrix admin UI applies the highest score the
//     session has ever held when blocking decisions are made, so
//     dragging the mean down can't UN-block a session that was
//     previously flagged.
//
// Tracking: see security_remediation_final_v6.md §5/#038 for the
// audit re-scoping requirement.
pub fn score_layer1(state: &SessionState) -> (u8, Vec<String>, f64, f64) {
    let (mean, var) = running_mean_variance(state);
    let mut score: u8 = 0;
    let mut reasons: Vec<String> = vec![];

    if state.seq > L1_TIMING_WARMUP_SEQ {
        if mean < L1_FAST_DWELL_THRESHOLD_S {
            score = score.saturating_add(L1_SCORE_FAST);
            reasons.push("impossibly-fast".into());
        }
        if var < L1_ROBOTIC_VARIANCE_THRESHOLD
            && (L1_ROBOTIC_DWELL_LOW_S..=L1_ROBOTIC_DWELL_HIGH_S).contains(&mean)
        {
            score = score.saturating_add(L1_SCORE_ROBOTIC);
            reasons.push("robotic-consistency".into());
        }
    }

    (score.min(100), reasons, mean, var)
}

// ── Layer 2 ─────────────────────────────────────────────────────────────────

fn transition_prob(
    matrix: &TransitionMatrix,
    prev: &str,
    current: &str,
    vocab_size: u32,
) -> f64 {
    let v = vocab_size as f64;
    let prev_row = matrix.counts.get(prev);
    let numerator = prev_row
        .and_then(|row| row.get(current).copied())
        .unwrap_or(0) as f64
        + L2_LAPLACE_ALPHA;
    let row_total = matrix.row_totals.get(prev).copied().unwrap_or(0) as f64;
    let denominator = row_total + L2_LAPLACE_ALPHA * v;
    if denominator <= 0.0 {
        return 1.0 / v.max(1.0);
    }
    numerator / denominator
}

pub fn score_layer2(
    matrix: Option<&TransitionMatrix>,
    prev_route: Option<&Route>,
    prev_anchor_route: Option<&Route>,
    current_route: &Route,
) -> (u8, Vec<String>, f64) {
    let matrix = match matrix {
        Some(m) => m,
        None => return (0, vec![], 1.0),
    };
    let prev = match prev_route {
        Some(r) => r,
        None => return (0, vec![], 1.0),
    };
    let vocab_size = matrix.vocab_size;
    if vocab_size == 0 {
        return (0, vec![], 1.0);
    }

    let direct_p = transition_prob(matrix, &prev.path, &current_route.path, vocab_size);
    let trans_prob = match prev_anchor_route {
        Some(anchor) if anchor.path != prev.path => {
            let anchor_p = transition_prob(matrix, &anchor.path, &current_route.path, vocab_size)
                * L2_SKIPGRAM_BETA;
            direct_p.max(anchor_p)
        }
        _ => direct_p,
    };

    let score = l2_score_from_trans_prob(trans_prob);
    let reasons = if score >= 50 {
        vec!["low-transition-prob".into()]
    } else {
        vec![]
    };
    (score, reasons, trans_prob)
}

// ── Combined ────────────────────────────────────────────────────────────────

pub fn blend_weight(matrix_age_days: f64) -> f64 {
    if matrix_age_days < 7.0 {
        return 0.0;
    }
    if matrix_age_days >= 10.0 {
        return 1.0;
    }
    (matrix_age_days - 7.0) / 3.0
}

pub struct ScoreInputs<'a> {
    pub state: Option<&'a SessionState>,
    pub cookie_compliance: &'a str,
    pub current_route: &'a Route,
    pub prev_route: Option<&'a Route>,
    pub prev_anchor_route: Option<&'a Route>,
    pub matrix: Option<&'a TransitionMatrix>,
    pub matrix_age_days: f64,
}

pub fn score_combined(inp: ScoreInputs<'_>) -> ScoreResult {
    let mut result = ScoreResult {
        cookie_compliance: inp.cookie_compliance.to_string(),
        matrix_version: inp
            .matrix
            .map(|m| m.version.clone())
            .unwrap_or_default(),
        ..Default::default()
    };

    // Security #036: tampered cookies score 100 (the audit's required
    // ceiling); missing/expired keep the historical 75. This matters
    // because the threshold-matrix admin UI uses score==100 to enable
    // hard-block enforcement — capping tampered at 75 meant an attacker
    // editing the cookie's payload to evade an L2 anomaly could keep
    // their session below the enforcement bar.
    let mut l1_from_cookie: u8 = 0;
    match inp.cookie_compliance {
        "tampered" => {
            l1_from_cookie = L1_SCORE_COOKIE_TAMPERED;
            result.reasons.push("cookie-tampered".into());
        }
        "missing" | "expired" => {
            l1_from_cookie = L1_SCORE_COOKIE_MISSING;
            result.reasons.push(format!("cookie-{}", inp.cookie_compliance));
        }
        _ => {}
    }

    if let Some(state) = inp.state {
        let (l1_timing, l1_reasons, mean, var) = score_layer1(state);
        result.mean_dwell_s = mean;
        result.variance_s2 = var;
        result.reasons.extend(l1_reasons);
        result.l1_score = l1_from_cookie.saturating_add(l1_timing).min(100);
    } else {
        result.l1_score = l1_from_cookie;
    }

    let (l2_score, l2_reasons, trans_prob) = score_layer2(
        inp.matrix,
        inp.prev_route,
        inp.prev_anchor_route,
        inp.current_route,
    );
    result.l2_score = l2_score;
    result.trans_prob = trans_prob;
    result.reasons.extend(l2_reasons);

    let w_l2 = blend_weight(inp.matrix_age_days);
    let raw = result.l1_score as f64 + result.l2_score as f64 * w_l2;
    result.score = quantize_score(raw);

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matrix::TransitionMatrix;
    use std::collections::HashMap;

    fn state(seq: u16, sum_dt: u32, sum_dt_sq: u64) -> SessionState {
        SessionState {
            v: crate::cookie::SCHEMA_VERSION,
            sid: [0, 1, 2, 3, 4, 5],
            seq,
            sum_dt,
            sum_dt_sq,
            last_ts: 1_700_000_000,
            score: 0,
            issued_at: 1_699_990_000,
            prev_route_path: String::new(),
        }
    }

    fn r(path: &str, category: &str) -> Route {
        Route {
            path: path.to_string(),
            category: category.to_string(),
        }
    }

    fn matrix(counts: &[(&str, &[(&str, u64)])], vocab_size: u32) -> TransitionMatrix {
        let mut m = TransitionMatrix {
            version: "test-v1".into(),
            vocab_size,
            ..Default::default()
        };
        for (src, dests) in counts {
            let mut row = HashMap::new();
            let mut total: u64 = 0;
            for (dst, n) in *dests {
                row.insert(dst.to_string(), *n);
                total += n;
            }
            m.counts.insert(src.to_string(), row);
            m.row_totals.insert(src.to_string(), total);
        }
        m
    }

    // ── running_mean_variance ────────────────────────────────────────────────

    #[test]
    fn running_seq_zero_returns_zeros() {
        let (m, v) = running_mean_variance(&state(0, 0, 0));
        assert_eq!(m, 0.0);
        assert_eq!(v, 0.0);
    }

    #[test]
    fn running_uniform_dwells() {
        // 5 identical 2s dwells → mean=2, var=0
        let (m, v) = running_mean_variance(&state(5, 10, 20));
        assert_eq!(m, 2.0);
        assert_eq!(v, 0.0);
    }

    #[test]
    fn running_mixed() {
        // Dwells [1,2,3,4] → mean=2.5, var=1.25
        let (m, v) = running_mean_variance(&state(4, 10, 30));
        assert!((m - 2.5).abs() < 1e-9);
        assert!((v - 1.25).abs() < 1e-9);
    }

    // ── Layer 1 warmup gate ──────────────────────────────────────────────────

    #[test]
    fn l1_below_warmup_no_rules_fire() {
        let (score, reasons, _, _) = score_layer1(&state(L1_TIMING_WARMUP_SEQ, 0, 0));
        assert_eq!(score, 0);
        assert!(reasons.is_empty());
    }

    #[test]
    fn l1_impossibly_fast_fires() {
        let (score, reasons, mean, _) = score_layer1(&state(5, 0, 0));
        assert!(reasons.contains(&"impossibly-fast".to_string()));
        assert!(score >= L1_SCORE_FAST);
        assert_eq!(mean, 0.0);
    }

    #[test]
    fn l1_robotic_consistency_fires_uniform_1s_loops() {
        // 10 dwells of 1s → mean=1, var=0
        let (score, reasons, _, var) = score_layer1(&state(10, 10, 10));
        assert!(reasons.contains(&"robotic-consistency".to_string()));
        assert!(score >= L1_SCORE_ROBOTIC);
        assert!(var < L1_ROBOTIC_VARIANCE_THRESHOLD);
    }

    #[test]
    fn l1_robotic_does_not_fire_outside_band() {
        // 10s mean — outside the bot-suspicious band.
        let (_, reasons, _, _) = score_layer1(&state(10, 100, 1000));
        assert!(!reasons.contains(&"robotic-consistency".to_string()));
    }

    // ── Layer 2 ──────────────────────────────────────────────────────────────

    #[test]
    fn l2_score_curve_pinned() {
        // Same band assertions as the Python parametrized test.
        assert_eq!(l2_score_from_trans_prob(1.0), 0);
        assert!(matches!(l2_score_from_trans_prob(0.5), 0..=10));
        assert!(matches!(l2_score_from_trans_prob(0.1), 10..=25));
        assert!(matches!(l2_score_from_trans_prob(0.01), 30..=40));
        assert!(matches!(l2_score_from_trans_prob(0.001), 45..=55));
        assert!(matches!(l2_score_from_trans_prob(1e-6), 95..=100));
        assert_eq!(l2_score_from_trans_prob(0.0), 100);
    }

    #[test]
    fn l2_no_matrix_returns_zero() {
        let (score, reasons, p) = score_layer2(None, Some(&r("/a", "home")), None, &r("/b", "home"));
        assert_eq!(score, 0);
        assert!(reasons.is_empty());
        assert_eq!(p, 1.0);
    }

    #[test]
    fn l2_high_prob_no_score() {
        let m = matrix(&[("/home", &[("/products", 99), ("/other", 1)])], 10);
        let (score, _, p) = score_layer2(Some(&m), Some(&r("/home", "home")), None, &r("/products", "product"));
        assert_eq!(score, 0);
        assert!(p > 0.9);
    }

    #[test]
    fn l2_rare_pair_fires() {
        let mut m = matrix(&[("/home", &[("/checkout", 1)])], 100);
        m.row_totals.insert("/home".into(), 10_000);
        let (score, reasons, p) = score_layer2(
            Some(&m),
            Some(&r("/home", "home")),
            None,
            &r("/checkout", "checkout"),
        );
        assert!(score >= 50);
        assert!(reasons.contains(&"low-transition-prob".to_string()));
        assert!(p < 0.001);
    }

    #[test]
    fn l2_skipgram_rescues_via_anchor() {
        let mut m = matrix(
            &[
                ("/about-us", &[("/checkout", 1)]),
                ("/product", &[("/checkout", 100)]),
            ],
            10,
        );
        m.row_totals.insert("/about-us".into(), 1000);
        m.row_totals.insert("/product".into(), 100);
        let (score, _, p) = score_layer2(
            Some(&m),
            Some(&r("/about-us", "content")),
            Some(&r("/product", "product")),
            &r("/checkout", "checkout"),
        );
        assert!(p > 0.5);
        assert!(score < 10);
    }

    // ── Blend weight ─────────────────────────────────────────────────────────

    #[test]
    fn blend_weight_pinned() {
        assert_eq!(blend_weight(0.0), 0.0);
        assert_eq!(blend_weight(6.99), 0.0);
        assert_eq!(blend_weight(7.0), 0.0);
        assert!((blend_weight(8.5) - 0.5).abs() < 1e-9);
        assert_eq!(blend_weight(10.0), 1.0);
        assert_eq!(blend_weight(30.0), 1.0);
    }

    // ── Combined output ──────────────────────────────────────────────────────

    #[test]
    fn combined_clean_session_zero() {
        let s = state(10, 50, 300);
        let m = matrix(&[("/home", &[("/products", 100)])], 10);
        let r1 = r("/home", "home");
        let r2 = r("/products", "product");
        let result = score_combined(ScoreInputs {
            state: Some(&s),
            cookie_compliance: "ok",
            current_route: &r2,
            prev_route: Some(&r1),
            prev_anchor_route: None,
            matrix: Some(&m),
            matrix_age_days: 30.0,
        });
        assert_eq!(result.score, 0);
        assert_eq!(result.l1_score, 0);
        assert_eq!(result.l2_score, 0);
        assert!(result.reasons.is_empty());
    }

    #[test]
    fn combined_missing_cookie_high_score() {
        let r1 = r("/home", "home");
        let r2 = r("/checkout", "checkout");
        let result = score_combined(ScoreInputs {
            state: None,
            cookie_compliance: "missing",
            current_route: &r2,
            prev_route: Some(&r1),
            prev_anchor_route: None,
            matrix: None,
            matrix_age_days: 0.0,
        });
        assert!(result.l1_score >= L1_SCORE_COOKIE_MISSING);
        assert!(result.reasons.iter().any(|r| r == "cookie-missing"));
        assert!(result.score >= 75);
    }

    #[test]
    fn combined_caps_at_100() {
        let s = state(10, 0, 0); // fast
        let mut m = matrix(&[("/a", &[("/b", 1)])], 1000);
        m.row_totals.insert("/a".into(), 1_000_000);
        let r1 = r("/a", "other");
        let r2 = r("/b", "other");
        let result = score_combined(ScoreInputs {
            state: Some(&s),
            cookie_compliance: "missing", // +75
            current_route: &r2,
            prev_route: Some(&r1),
            prev_anchor_route: None,
            matrix: Some(&m),
            matrix_age_days: 30.0,
        });
        assert_eq!(result.score, 100);
    }

    #[test]
    fn combined_score_quantized_to_nearest_5() {
        let s = state(10, 0, 0);
        let r1 = r("/a", "other");
        let r2 = r("/b", "other");
        let result = score_combined(ScoreInputs {
            state: Some(&s),
            cookie_compliance: "ok",
            current_route: &r2,
            prev_route: Some(&r1),
            prev_anchor_route: None,
            matrix: None,
            matrix_age_days: 0.0,
        });
        assert_eq!(result.score % 5, 0);
    }
}

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
    // Python's `round()` is banker's rounding (round-half-to-even); Rust's
    // `f64::round` is round-half-away-from-zero. At an exact .5 the two
    // diverge by one point, breaking the Python/Rust parity this function
    // is pinned to mirror. Match Python with `round_ties_even` (same fix
    // as cookie.rs's bucket rounding).
    clamped.round_ties_even() as u8
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
    // Mean/variance over the WHOLE session via the algebraic identity
    // Var(X) = E[X²] − E[X]². Divide by the full `seq` — see the #038
    // note below for why a naive `min(seq, 20)` divisor cap was tried
    // and reverted (it inflates the mean instead of windowing it).
    let mean = state.sum_dt as f64 / state.seq as f64;
    let second_moment = state.sum_dt_sq as f64 / state.seq as f64;
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
// A naive shortcut — dividing the still-cumulative `sum_dt` by a capped
// `min(seq, 20)` — does NOT work and was reverted: once seq > 20 the
// denominator freezes at 20 while the numerator keeps growing, so the
// mean INFLATES over a long session, pushing it further above
// L1_FAST_DWELL_THRESHOLD_S and making "impossibly-fast" detection
// fire LESS, not more. A real fix must window the SUM, not just the
// divisor — replace the cumulative sum with a sliding window of the
// last N (~20) dwells. That requires:
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

fn transition_prob(matrix: &TransitionMatrix, prev: &str, current: &str, vocab_size: u32) -> f64 {
    let v = vocab_size as f64;
    let has_rows = matrix.has_rows();
    let prev_id = matrix.route_id(prev);
    // Fail closed on an unseen prev route in a POPULATED matrix — mirrors
    // backend/scoring/scorer.py::_transition_prob. An unseen prev row isn't
    // neutral; it's a transition the model never observed (the shape of
    // evasion: prepend an untracked route to dodge the shield). Return the
    // floor so l2_score_from_trans_prob maps it to the max anomaly score.
    // "Known prev" = in the vocab AND seen as a transition SOURCE (row_total > 0).
    // A curr-only route (present in the matrix but row_total 0 — seen only as a
    // destination) is treated as UNSEEN: we fail closed on the VALUE, not mere
    // key presence. (Python's _transition_prob mirrors this exactly:
    // `row_totals.get(prev) <= 0` → floor; EC-04 aligned the two for a
    // key-present-zero prev.) A truly empty matrix still falls through to the
    // uniform prior below so L2 self-disables until trained.
    let prev_is_known = matches!(prev_id, Some(id) if matrix.row_total(id) > 0);
    if has_rows && !prev_is_known {
        return 1e-12;
    }
    let count = match (prev_id, matrix.route_id(current)) {
        (Some(p), Some(c)) => matrix.transition_count(p, c),
        _ => 0,
    };
    let numerator = count as f64 + L2_LAPLACE_ALPHA;
    let row_total = prev_id.map(|id| matrix.row_total(id)).unwrap_or(0) as f64;
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
    let vocab_size = matrix.vocab_size();
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

/// Length of the L2 opt-in fade-in, in days. On operator opt-in, L2's weight in
/// the enforced combined score ramps linearly 0 → 1 over this window from the
/// `l2_enabled_at` anchor — a smooth, operator-initiated rollout, never an auto
/// step-change keyed off deployment age.
pub const L2_RAMP_DAYS: f64 = 3.0;

/// Weight L2 contributes to the combined score, as a function of days since the
/// operator opted L2 into enforcement. Linear ramp over `[0, L2_RAMP_DAYS]`:
/// `<= 0 → 0.0` (the instant of opt-in), `>= L2_RAMP_DAYS → 1.0` (fade-in
/// complete), else `days / L2_RAMP_DAYS`. Whether L2 contributes at all is gated
/// upstream by the operator's explicit opt-in — `score_combined` forces the
/// weight to 0 when `l2_enforce_enabled` is false — so this function only shapes
/// the post-opt-in ramp, never the on/off decision.
pub fn optin_ramp_weight(days_since_optin: f64) -> f64 {
    if days_since_optin <= 0.0 {
        return 0.0;
    }
    if days_since_optin >= L2_RAMP_DAYS {
        return 1.0;
    }
    days_since_optin / L2_RAMP_DAYS
}

pub struct ScoreInputs<'a> {
    pub state: Option<&'a SessionState>,
    pub cookie_compliance: &'a str,
    pub current_route: &'a Route,
    pub prev_route: Option<&'a Route>,
    pub prev_anchor_route: Option<&'a Route>,
    pub matrix: Option<&'a TransitionMatrix>,
    /// Operator's explicit opt-in. When false, L2 contributes 0 to the enforced
    /// combined score — its sub-score is still computed + emitted for
    /// observability, only its weight in the combined score is gated.
    pub l2_enforce_enabled: bool,
    /// Days since the opt-in anchor (`l2_enabled_at`); shapes `optin_ramp_weight`.
    pub l2_days_since_optin: f64,
}

pub fn score_combined(inp: ScoreInputs<'_>) -> ScoreResult {
    let mut result = ScoreResult {
        cookie_compliance: inp.cookie_compliance.to_string(),
        matrix_version: inp.matrix.map(|m| m.version().to_string()).unwrap_or_default(),
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
        "missing" | "expired" | "replayed" => {
            // "replayed" (main.rs is_sustained_replay) is treated like
            // missing/expired: the cultivated low-seq state is dropped, so a
            // replayer gets the no-state baseline instead of dodging warmup.
            // Penalized at the missing tier (not the tampered 100) — a
            // replay isn't payload forgery, and keeping it below the tamper
            // ceiling avoids over-penalizing the rare benign repeat.
            l1_from_cookie = L1_SCORE_COOKIE_MISSING;
            result
                .reasons
                .push(format!("cookie-{}", inp.cookie_compliance));
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

    // L2 joins the *enforced* combined score only on explicit operator opt-in
    // (l2_enforce_enabled); on opt-in it fades in over L2_RAMP_DAYS from the
    // opt-in anchor. Flag off → weight 0 → L2 stays observe-only forever (its
    // sub-score above is still always computed/emitted).
    let w_l2 = if inp.l2_enforce_enabled {
        optin_ramp_weight(inp.l2_days_since_optin)
    } else {
        0.0
    };
    let raw = result.l1_score as f64 + result.l2_score as f64 * w_l2;
    result.score = quantize_score(raw);

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matrix::TransitionMatrix;

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
        // Builds via the real FSM1 encode→decode path (see matrix.rs). row_total
        // defaults to each row's count-sum; tests that need a different total
        // call `set_row_total` after.
        TransitionMatrix::from_counts("test-v1", vocab_size, counts)
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

    #[test]
    fn running_long_session_divides_by_full_seq_not_capped() {
        // #038: the mean must divide by the full session length, NOT a
        // capped min(seq, 20). 40 dwells summing to 80 → mean 2.0. A
        // min(20) divisor would (wrongly) give 80/20 = 4.0. Pinning 2.0
        // guards against re-introducing the inflating divisor cap.
        let (m, v) = running_mean_variance(&state(40, 80, 200));
        assert!((m - 2.0).abs() < 1e-9);
        assert!((v - 1.0).abs() < 1e-9); // 200/40 - 2² = 5 - 4 = 1
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
        let (score, reasons, p) =
            score_layer2(None, Some(&r("/a", "home")), None, &r("/b", "home"));
        assert_eq!(score, 0);
        assert!(reasons.is_empty());
        assert_eq!(p, 1.0);
    }

    #[test]
    fn l2_high_prob_no_score() {
        let m = matrix(&[("/home", &[("/products", 99), ("/other", 1)])], 10);
        let (score, _, p) = score_layer2(
            Some(&m),
            Some(&r("/home", "home")),
            None,
            &r("/products", "product"),
        );
        assert_eq!(score, 0);
        assert!(p > 0.9);
    }

    #[test]
    fn l2_rare_pair_fires() {
        let mut m = matrix(&[("/home", &[("/checkout", 1)])], 100);
        m.set_row_total("/home", 10_000);
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
        m.set_row_total("/about-us", 1000);
        m.set_row_total("/product", 100);
        let (score, _, p) = score_layer2(
            Some(&m),
            Some(&r("/about-us", "content")),
            Some(&r("/product", "product")),
            &r("/checkout", "checkout"),
        );
        assert!(p > 0.5);
        assert!(score < 10);
    }

    #[test]
    fn l2_unseen_prev_row_fails_closed() {
        // Parity with Python test_transition_prob_with_unseen_prev_row.
        // An unseen prev route in a populated matrix is maximally anomalous,
        // not the lenient uniform prior — closes the shield-evasion gap
        // where an attacker prepends an untracked route to dodge L2.
        let m = matrix(&[("/home", &[("/products", 100)])], 10);
        let (score, reasons, p) = score_layer2(
            Some(&m),
            Some(&r("/never-seen-prev", "other")),
            None,
            &r("/products", "product"),
        );
        assert!(p <= 1e-12);
        assert_eq!(score, 100);
        assert!(reasons.contains(&"low-transition-prob".to_string()));
    }

    #[test]
    fn l2_present_prev_zero_row_total_fails_closed() {
        // EC-04 parity with Python's test_transition_prob_present_zero_row_total_
        // fails_closed: a prev route PRESENT in the matrix but never a transition
        // SOURCE (row_total 0 — a curr-only/destination-only route) is treated as
        // UNSEEN and fails closed to the floor, same as a fully-absent prev. Here
        // /products is only ever a destination, so its row_total is 0; using it as
        // prev must map to the max L2 score, not the lenient uniform prior.
        let m = matrix(&[("/home", &[("/products", 100)])], 10);
        assert_eq!(m.row_total(m.route_id("/products").unwrap()), 0, "fixture: /products is curr-only");
        let (score, reasons, p) = score_layer2(
            Some(&m),
            Some(&r("/products", "product")), // present in matrix, row_total 0
            None,
            &r("/home", "home"),
        );
        assert!(p <= 1e-12);
        assert_eq!(score, 100);
        assert!(reasons.contains(&"low-transition-prob".to_string()));
    }

    #[test]
    fn l2_unseen_anchor_does_not_rescue_anomalous_transition() {
        // Parity with the Python finding-021 guard: an unseen skip-gram
        // anchor must not mask an anomalous direct transition. Python
        // filters unseen anchors via `in counts`; on the Rust side the
        // unseen-prev fail-closed makes the anchor contribute ~0 so max()
        // can't rescue the score.
        let mut m = matrix(&[("/about-us", &[("/home", 1000)])], 10);
        m.set_row_total("/about-us", 1000);
        let (score, reasons, p) = score_layer2(
            Some(&m),
            Some(&r("/about-us", "content")),
            Some(&r("/never-visited-anchor", "product")), // unseen anchor
            &r("/checkout", "checkout"),
        );
        assert!(p < 0.001);
        assert!(score >= 50);
        assert!(reasons.contains(&"low-transition-prob".to_string()));
    }

    // ── Opt-in ramp weight ───────────────────────────────────────────────────

    #[test]
    fn optin_ramp_weight_pinned() {
        // Linear fade-in over [0, L2_RAMP_DAYS=3]: 0 at (and before) the instant
        // of opt-in, 0.5 at the midpoint, 1.0 once fully ramped and beyond.
        assert_eq!(optin_ramp_weight(0.0), 0.0);
        assert_eq!(optin_ramp_weight(-1.0), 0.0);
        assert!((optin_ramp_weight(1.5) - 0.5).abs() < 1e-9);
        assert_eq!(optin_ramp_weight(3.0), 1.0);
        assert_eq!(optin_ramp_weight(30.0), 1.0);
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
            l2_enforce_enabled: true,
            l2_days_since_optin: 30.0,
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
            l2_enforce_enabled: false,
            l2_days_since_optin: 0.0,
        });
        assert!(result.l1_score >= L1_SCORE_COOKIE_MISSING);
        assert!(result.reasons.iter().any(|r| r == "cookie-missing"));
        assert!(result.score >= 75);
    }

    #[test]
    fn combined_caps_at_100() {
        let s = state(10, 0, 0); // fast
        let mut m = matrix(&[("/a", &[("/b", 1)])], 1000);
        m.set_row_total("/a", 1_000_000);
        let r1 = r("/a", "other");
        let r2 = r("/b", "other");
        let result = score_combined(ScoreInputs {
            state: Some(&s),
            cookie_compliance: "missing", // +75
            current_route: &r2,
            prev_route: Some(&r1),
            prev_anchor_route: None,
            matrix: Some(&m),
            l2_enforce_enabled: true,
            l2_days_since_optin: 30.0, // fully ramped
        });
        assert_eq!(result.score, 100);
    }

    #[test]
    fn l2_off_contributes_nothing() {
        // Flag false → L2 stays observe-only forever: its sub-score is still
        // computed/emitted, but it never reaches the *enforced* combined score,
        // no matter how stale the opt-in anchor is. A maximally-anomalous
        // transition (unseen prev in a populated matrix → L2 100) must leave the
        // combined score at quantize(L1) for every age.
        let s = state(10, 50, 300); // clean L1: mean 5s, var 5 → no timing flags
        let m = matrix(&[("/home", &[("/products", 100)])], 10);
        let prev = r("/never-seen-prev", "other"); // unseen prev → fail-closed → L2 100
        let cur = r("/products", "product");
        for days in [0.0_f64, 1.5, 3.0, 30.0] {
            let result = score_combined(ScoreInputs {
                state: Some(&s),
                cookie_compliance: "ok",
                current_route: &cur,
                prev_route: Some(&prev),
                prev_anchor_route: None,
                matrix: Some(&m),
                l2_enforce_enabled: false,
                l2_days_since_optin: days,
            });
            assert_eq!(result.l2_score, 100, "days {days}: L2 sub-score still computed");
            assert_eq!(result.l1_score, 0, "L1 must be clean so L2 is the only mover");
            assert_eq!(
                result.score,
                quantize_score(result.l1_score as f64),
                "days {days}: flag off → combined must equal quantize(L1)"
            );
        }
    }

    #[test]
    fn l2_on_fades_in_from_optin() {
        // Flag true: at the instant of opt-in (days 0) the ramp weight is 0 so
        // the combined score still equals quantize(L1); once the fade-in
        // completes (days ≥ L2_RAMP_DAYS) L2 lifts the enforced combined score.
        // The L2 sub-score itself is age-independent (always computed).
        let s = state(10, 50, 300); // clean L1: mean 5s, var 5 → no timing flags
        let m = matrix(&[("/home", &[("/products", 100)])], 10);
        let prev = r("/never-seen-prev", "other"); // unseen prev → fail-closed → L2 100
        let cur = r("/products", "product");
        let mk = |days: f64| {
            score_combined(ScoreInputs {
                state: Some(&s),
                cookie_compliance: "ok",
                current_route: &cur,
                prev_route: Some(&prev),
                prev_anchor_route: None,
                matrix: Some(&m),
                l2_enforce_enabled: true,
                l2_days_since_optin: days,
            })
        };
        let day0 = mk(0.0);
        let day3 = mk(3.0);
        assert_eq!(day0.l2_score, 100);
        assert_eq!(day3.l2_score, 100);
        assert_eq!(day0.l1_score, 0, "L1 must be clean so L2 is the only mover");
        // Day 0: ramp weight 0 → combined == quantize(L1).
        assert_eq!(day0.score, quantize_score(day0.l1_score as f64));
        // Fully ramped: L2 now moves the enforced combined score.
        assert!(
            day3.score > day0.score,
            "L2 must lift the combined score once the opt-in fade-in completes"
        );
    }

    #[test]
    fn l2_on_young_optin_cannot_hard_block() {
        // Enabling L2 must NOT instantly push a clean-L1 session to the
        // score==100 hard-block bar — the admin enforcement UI keys hard blocks
        // on 100, and the ramp opens at 0 the moment you opt in. Guards against
        // a step-change block the instant the operator flips the switch.
        let s = state(10, 50, 300); // clean L1
        let m = matrix(&[("/home", &[("/products", 100)])], 10);
        let prev = r("/never-seen-prev", "other");
        let cur = r("/products", "product");
        let result = score_combined(ScoreInputs {
            state: Some(&s),
            cookie_compliance: "ok",
            current_route: &cur,
            prev_route: Some(&prev),
            prev_anchor_route: None,
            matrix: Some(&m),
            l2_enforce_enabled: true,
            l2_days_since_optin: 0.0, // just opted in
        });
        assert_eq!(result.l2_score, 100, "L2 sub-score still computed");
        assert!(
            result.score < 100,
            "opt-in day 0 must keep L2 below the hard-block bar"
        );
        assert_eq!(
            result.score,
            quantize_score(result.l1_score as f64),
            "opt-in day 0: combined must equal quantize(L1)"
        );
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
            l2_enforce_enabled: false,
            l2_days_since_optin: 0.0,
        });
        assert_eq!(result.score % 5, 0);
    }
}

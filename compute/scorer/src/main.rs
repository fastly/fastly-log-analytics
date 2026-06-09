//! Fastly Compute entrypoint for the edge session scorer.
//!
//! Wire-up:
//!   1. Read `X-Session-State` cookie from inbound request.
//!   2. Look up the AES-GCM key(s) from the `scoring_keys` Edge Dictionary.
//!   3. Decode + verify the cookie. On any failure, mark compliance accordingly.
//!   4. Pull the previous route from the cookie's seq context (carried in
//!      separate header by VCL; see vcl/snippet.fetch.vcl in Phase D).
//!   5. Normalize the current request URL → Route.
//!   6. Score (L1 + L2 + combined) against the embedded matrix.
//!   7. Re-encode the updated state into a fresh cookie.
//!   8. Return X-Edge-* headers + Set-Cookie. VCL strips X-Edge-* before
//!      client delivery (research doc §1.3).
//!
//! Fail-open: any internal error path returns score=0 + reason=internal-error
//! so a Compute-side bug never blocks real users (§6).

mod cookie;
mod matrix;
mod normalize;
mod scorer;

use fastly::{ConfigStore, Error, Request, Response};
use std::sync::atomic::{AtomicU64, Ordering};

// Lightweight in-process counters. Emitted via dbg_log every
// METRICS_EMIT_EVERY requests so the operator can grep `metrics:` in
// `fastly log-tail` for a rough sense of how often the cookie is being
// tampered with, how often we're hard-blocking, and whether the
// embedded matrix ever fails to load (it shouldn't — it's compiled in).
// Counters are process-wide on the Wasm instance and reset whenever
// Fastly recycles the instance; we accept that imprecision in exchange
// for zero-coordination atomics on the hot path.
static TAMPERED_COOKIE_COUNT: AtomicU64 = AtomicU64::new(0);
static ENFORCE_BLOCK_COUNT: AtomicU64 = AtomicU64::new(0);
static MATRIX_LOAD_FAIL_COUNT: AtomicU64 = AtomicU64::new(0);
static REQUEST_COUNT: AtomicU64 = AtomicU64::new(0);
const METRICS_EMIT_EVERY: u64 = 1000;

const SERVICE_ID_HEADER: &str = "X-Edge-Service-Id";
const PREV_ROUTE_HEADER: &str = "X-Edge-Prev-Route";
const PREV_ANCHOR_HEADER: &str = "X-Edge-Prev-Anchor";
const MATRIX_AGE_HEADER: &str = "X-Edge-Matrix-Age-Days";
const SCORER_AUTH_HEADER: &str = "X-Edge-Scorer-Auth";
const COOKIE_NAME: &str = "X-Session-State";
const KEYS_STORE: &str = "scoring_keys";
const REQUEST_SECRET_KEY: &str = "request_secret";
// Separate config store from the keys so the dev/debug toggle can be flipped
// without ever touching the cookie key. We default-load both — missing
// config store reads degrade to "no debug logging" so a fresh service that
// hasn't been fully configured still serves real requests.
const CONFIG_STORE: &str = "scoring_config";
const DEBUG_LOG_KEY: &str = "debug_logging_enabled";
const ENFORCE_THRESHOLD_KEY: &str = "enforce_threshold";

#[fastly::main]
fn main(req: Request) -> Result<Response, Error> {
    // The score function never panics; the worst case returns a
    // diagnostic-only response with X-Edge-Score=0 so VCL fails open.
    Ok(score_request(&req))
}

fn score_request(req: &Request) -> Response {
    // ── Auth: reject any request that doesn't bear the right shared
    //         secret from our VCL service. The secret is written into
    //         the scoring_keys ConfigStore at provision time and
    //         embedded into the VCL recv snippet. Stops the scorer
    //         domain (which is reachable from anywhere on the public
    //         internet) from being scored on by random people who
    //         find the hostname.
    if !request_auth_ok(req) {
        return Response::from_status(401)
            .with_header("X-Edge-Score-Reason", "unauthorized")
            .with_body("unauthorized");
    }

    // ── Header inputs (VCL fills these before Compute is invoked). ───────────
    let service_id = req.get_header_str(SERVICE_ID_HEADER).unwrap_or("default");
    let prev_route_raw = req.get_header_str(PREV_ROUTE_HEADER);
    let prev_anchor_raw = req.get_header_str(PREV_ANCHOR_HEADER);
    let matrix_age_days: f64 = req
        .get_header_str(MATRIX_AGE_HEADER)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);

    // Cheap toggle: flip the `debug_logging_enabled` key in the
    // `scoring_config` ConfigStore to true (any truthy string), tail the
    // service with `fastly log-tail`, then flip back to off. The store
    // lookup is a constant-time hash hit so leaving the check in costs
    // ~nothing on the hot path.
    let debug = debug_logging_enabled();
    // Wall-clock since the Wasm instance booted, in nanoseconds. We
    // diff start vs end to get the time spent scoring this request,
    // which goes into the debug log so the operator can see real
    // edge-side latency without leaving Fastly's tools.
    let t0 = if debug {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    } else {
        0
    };
    if debug {
        dbg_log(&format!(
            "incoming: url={} service={} prev_route={:?} prev_anchor={:?} matrix_age_days={}",
            req.get_url_str(),
            service_id,
            prev_route_raw,
            prev_anchor_raw,
            matrix_age_days,
        ));
    }

    // ── Resolve current route from the request URL. ──────────────────────────
    let current_route = normalize::normalize(req.get_url_str());

    // ── Load AES-GCM keys from the Edge Dictionary. ──────────────────────────
    let (key, prev_key) = match load_keys() {
        Ok(pair) => pair,
        Err(_) => {
            // Misconfigured dictionary is operationally critical but should
            // fail open in the request path. Emit a diagnostic header so the
            // outage is visible in VCL logs.
            return fail_open_response("internal-error-keys");
        }
    };

    // ── Decode the inbound cookie (if any). ──────────────────────────────────
    let inbound_cookie = req
        .get_header_str("cookie")
        .and_then(|h| extract_cookie_value(h, COOKIE_NAME));

    let (state, compliance) = match inbound_cookie {
        None => (None, "missing"),
        Some(value) => match cookie::decode(
            value,
            &key,
            prev_key.as_deref(),
            service_id,
            cookie::SCHEMA_VERSION,
        ) {
            Ok(s) => (Some(s), "ok"),
            Err(_) => {
                TAMPERED_COOKIE_COUNT.fetch_add(1, Ordering::Relaxed);
                (None, "tampered")
            }
        },
    };

    if debug {
        match &state {
            Some(s) => {
                dbg_log(&format!(
                    "inbound_cookie: status={} sid={} seq={} sum_dt={} sum_dt_sq={} last_ts={} issued_at={} prev_route_path={:?} last_score={}",
                    compliance,
                    hex::encode(s.sid),
                    s.seq,
                    s.sum_dt,
                    s.sum_dt_sq,
                    s.last_ts,
                    s.issued_at,
                    s.prev_route_path,
                    s.score,
                ));
            }
            None => {
                dbg_log(&format!("inbound_cookie: status={}", compliance));
            }
        }
    }

    // ── Resolve previous route(s) for L2. ────────────────────────────────────
    // Prefer the prev_route stored in the cookie state (carried forward
    // from the last scored request in this session) — req.http doesn't
    // persist across separate client requests, so the X-Edge-Prev-Route
    // header path was always empty. The header is still consulted as a
    // fallback for one-off testing scenarios where the cookie is missing.
    let prev_route_from_state = state
        .as_ref()
        .filter(|s| !s.prev_route_path.is_empty())
        .map(|s| normalize::Route {
            path: s.prev_route_path.clone(),
            // L2 transition lookup only uses path; category is unused.
            // Leaving empty avoids re-running the full normalize() pass.
            category: String::new(),
        });
    let prev_route = prev_route_from_state
        .or_else(|| prev_route_raw.map(|s| normalize::normalize(s)));
    let prev_anchor = prev_anchor_raw.map(|s| normalize::normalize(s));

    // ── Score. ───────────────────────────────────────────────────────────────
    let matrix = matrix::load_embedded();
    if matrix.is_none() {
        // The matrix is compiled into the Wasm binary, so a None here
        // means the embedded JSON failed to parse at first access —
        // operationally this would only happen after a bad deploy.
        // Bump the counter so the periodic metrics line surfaces it.
        MATRIX_LOAD_FAIL_COUNT.fetch_add(1, Ordering::Relaxed);
    }
    let result = scorer::score_combined(scorer::ScoreInputs {
        state: state.as_ref(),
        cookie_compliance: compliance,
        current_route: &current_route,
        prev_route: prev_route.as_ref(),
        prev_anchor_route: prev_anchor.as_ref(),
        matrix,
        matrix_age_days,
    });

    // ── Re-encode the updated cookie. ────────────────────────────────────────
    // We rotate the cookie on every request so the seq/sum_dt fields stay
    // fresh and the encryption nonce never repeats. The just-scored
    // current_route becomes the next request's prev_route.
    let now_secs: u32 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as u32)
        .unwrap_or(0);
    let updated = update_state(state.clone(), &result, &current_route.path, now_secs);
    let set_cookie = match cookie::encode(
        &updated,
        &key,
        &random_nonce(),
        service_id,
        cookie::SCHEMA_VERSION,
    ) {
        Ok(c) => Some(c),
        Err(_) => None,
    };

    // ── Build response. ──────────────────────────────────────────────────────
    let mut resp = Response::from_status(200);
    for (k, v) in result.headers() {
        resp.set_header(k, v);
    }
    // Emit the session id as a hex-encoded response header so VCL can
    // capture it into the log line. Used by the admin to label specific
    // sessions (good / bad / neutral) for ROC-AUC training. The VCL
    // deliver snippet strips this header from the client-facing response
    // (same hardening as the other X-Edge-* fields, per research doc
    // §1.3).
    resp.set_header("X-Edge-Sid", hex::encode(updated.sid));

    // ENFORCEMENT signal: when the operator has committed an
    // enforce_threshold value via the admin UI (writes to the
    // scoring_config ConfigStore), set X-Edge-Score-Enforce=1 when the
    // request's score meets or exceeds it. VCL reads this in a recv-
    // restart-2 snippet and `error 429`s the request. Missing key or
    // unparseable value → no enforcement (fail-open).
    if let Some(t) = load_enforce_threshold() {
        if u32::from(result.score) >= t {
            ENFORCE_BLOCK_COUNT.fetch_add(1, Ordering::Relaxed);
            resp.set_header("X-Edge-Score-Enforce", "1");
        }
    }

    if let Some(cookie_value) = set_cookie {
        resp.set_header(
            "Set-Cookie",
            format!(
                "{}={}; Path=/; HttpOnly; Secure; SameSite=Lax",
                COOKIE_NAME, cookie_value
            ),
        );
    }

    if debug {
        let t1 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let elapsed_us = (t1.saturating_sub(t0)) / 1_000;
        
        let current_dt_secs = state.as_ref()
            .map(|s| now_secs.saturating_sub(s.last_ts).min(3600))
            .unwrap_or(0);

        dbg_log(&format!(
            "scored: score={} l1={} l2={} compliance={} reasons=[{}] mean_dwell_s={:.3} variance_s2={:.3} trans_prob={:.6} matrix_version={} elapsed_us={}",
            result.score,
            result.l1_score,
            result.l2_score,
            result.cookie_compliance,
            result.reasons.join(","),
            result.mean_dwell_s,
            result.variance_s2,
            result.trans_prob,
            result.matrix_version,
            elapsed_us,
        ));

        dbg_log(&format!(
            "outbound_cookie: sid={} seq={} current_dt={} sum_dt={} sum_dt_sq={} last_ts={} prev_route_path={:?}",
            hex::encode(updated.sid),
            updated.seq,
            current_dt_secs,
            updated.sum_dt,
            updated.sum_dt_sq,
            updated.last_ts,
            updated.prev_route_path,
        ));
    }

    maybe_emit_metrics();

    resp
}

/// Read the debug toggle from the `scoring_config` ConfigStore. Any truthy
/// string ("1", "true", "yes", any non-empty value other than "0"/"false")
/// enables verbose log emission. Missing config store / missing key → off.
///
/// Always returns a bool — never panics — because this is on the request
/// hot path and a misconfigured store must not 5xx real traffic.
fn debug_logging_enabled() -> bool {
    // ConfigStore::open panics if the store doesn't exist. catch_unwind is
    // a no-op under wasm32 + panic=abort, so we use try_open to actually
    // achieve the "missing store → silent fallback" semantic. A missing
    // store on this fast-path code = debug off, which is the right default
    // for a fresh service that hasn't been fully configured yet.
    let dict = match ConfigStore::try_open(CONFIG_STORE) {
        Ok(d) => d,
        Err(_) => return false,
    };
    match dict.get(DEBUG_LOG_KEY) {
        Some(v) => {
            let trimmed = v.trim().to_ascii_lowercase();
            !trimmed.is_empty() && trimmed != "0" && trimmed != "false" && trimmed != "no"
        }
        None => false,
    }
}

/// Read the operator's committed enforce_threshold from scoring_config.
///
/// Returns ``Some(0..=100)`` when set + parseable; ``None`` otherwise.
/// The fail-open posture matches debug_logging_enabled: missing store,
/// missing key, or unparseable value → None → no enforcement happens.
/// Operator clears enforcement by deleting the key or writing a non-
/// numeric value (e.g. "off"). Values outside 0..100 are clamped.
fn load_enforce_threshold() -> Option<u32> {
    let dict = ConfigStore::try_open(CONFIG_STORE).ok()?;
    let raw = dict.get(ENFORCE_THRESHOLD_KEY)?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let n: u32 = trimmed.parse().ok()?;
    Some(n.min(100))
}

/// Write a structured log line to stderr via `eprintln!`. On Wasm,
/// Fastly's runtime captures stderr (alongside stdout) and surfaces it
/// via `fastly log-tail`, so eprintln is the right destination here —
/// it keeps these diagnostic lines visually distinct from any future
/// real stdout output the binary might emit. Native test builds also
/// just print so the dbg_log call sites compile and can be exercised
/// without a Wasm runtime.
fn dbg_log(msg: &str) {
    eprintln!("[scoring/dbg] {}", msg);
}

/// Bump the per-instance request counter and, every `METRICS_EMIT_EVERY`
/// requests, emit a one-line metrics summary via dbg_log. Runs
/// unconditionally (independent of the debug toggle) because the
/// counters themselves are always incremented and the emission cost is
/// amortized to ~one log line per 1000 requests. Reads counters with
/// Relaxed ordering — exact values aren't required, only rough
/// magnitudes for operator visibility.
fn maybe_emit_metrics() {
    let count = REQUEST_COUNT.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
    if count % METRICS_EMIT_EVERY == 0 {
        dbg_log(&format!(
            "metrics: tampered={} enforce_block={} matrix_fail={} requests={}",
            TAMPERED_COOKIE_COUNT.load(Ordering::Relaxed),
            ENFORCE_BLOCK_COUNT.load(Ordering::Relaxed),
            MATRIX_LOAD_FAIL_COUNT.load(Ordering::Relaxed),
            count,
        ));
    }
}

fn fail_open_response(reason: &str) -> Response {
    let mut resp = Response::from_status(200);
    resp.set_header("X-Edge-Score", "0");
    resp.set_header("X-Edge-Score-L1", "0");
    resp.set_header("X-Edge-Score-L2", "0");
    resp.set_header("X-Edge-Cookie-Compliance", "unknown");
    resp.set_header("X-Edge-Score-Reason", reason);
    resp
}

fn request_auth_ok(req: &Request) -> bool {
    let provided = req.get_header_str(SCORER_AUTH_HEADER).unwrap_or("");
    if provided.is_empty() {
        return false;
    }
    // Use try_open so a missing scoring_keys store fails-closed gracefully
    // instead of panicking; load_request_secret also returns None when
    // the key is missing or empty. Either way: reject the request — better
    // than letting unauthenticated traffic through on misconfiguration.
    let expected = match load_request_secret() {
        Some(v) => v,
        None => return false,
    };
    // Constant-time compare to avoid timing-leak side channels. The
    // comparison is over short strings (32 hex chars) so the gain is
    // minor in practice but free to add.
    constant_time_eq(provided.as_bytes(), expected.as_bytes())
}

fn load_request_secret() -> Option<String> {
    let dict = ConfigStore::try_open(KEYS_STORE).ok()?;
    let v = dict.get(REQUEST_SECRET_KEY)?;
    if v.is_empty() {
        None
    } else {
        Some(v)
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn load_keys() -> Result<(Vec<u8>, Option<Vec<u8>>), Error> {
    let dict = ConfigStore::open(KEYS_STORE);
    let key_hex = dict
        .get("current_key_hex")
        .ok_or_else(|| Error::msg("scoring_keys.current_key_hex missing"))?;
    let prev_hex = dict.get("previous_key_hex");

    let key = hex_decode(&key_hex)?;
    let prev = match prev_hex {
        Some(h) if !h.is_empty() => Some(hex_decode(&h)?),
        _ => None,
    };
    Ok((key, prev))
}

fn hex_decode(s: &str) -> Result<Vec<u8>, Error> {
    if s.len() % 2 != 0 {
        return Err(Error::msg("hex key has odd length"));
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
        .collect::<Result<Vec<u8>, _>>()
        .map_err(|_| Error::msg("invalid hex in key"))
}

/// Pull a single named cookie value out of a `Cookie:` header. Minimal —
/// doesn't handle quoted values or escapes (we only consume cookies we
/// ourselves emitted, so the value space is base64url alphabet only).
fn extract_cookie_value<'a>(cookie_header: &'a str, name: &str) -> Option<&'a str> {
    for part in cookie_header.split(';') {
        let trimmed = part.trim();
        if let Some(eq_idx) = trimmed.find('=') {
            let (k, v) = trimmed.split_at(eq_idx);
            if k == name {
                return Some(&v[1..]);
            }
        }
    }
    None
}

fn random_nonce() -> [u8; cookie::NONCE_BYTES] {
    // CSPRNG from the WASI runtime. AES-GCM nonce REUSE is catastrophic
    // for confidentiality (an attacker who sees two ciphertexts under
    // the same key+nonce recovers the plaintext XOR and can forge),
    // so if getrandom fails we PANIC rather than fall back to a weak
    // source. On wasm32-wasip1 (Fastly Compute) this branch is
    // unreachable in practice — random_get is a host function backed
    // by Fastly's runtime entropy and has never been observed to fail.
    let mut buf = [0u8; cookie::NONCE_BYTES];
    getrandom::getrandom(&mut buf)
        .expect("WASI getrandom must not fail — AES-GCM nonce reuse is catastrophic");
    buf
}

/// Apply the just-computed score back into a new SessionState for the
/// next request's cookie. If the inbound state was missing/tampered we
/// start a fresh session here.
///
/// TIMING (used by L1 rules):
///   sum_dt    = Σ (now − last_ts) across the session
///   sum_dt_sq = Σ (now − last_ts)²
///   mean_dwell = sum_dt / seq        — L1 "impossibly fast" check
///   variance   = sum_dt_sq/seq − mean_dwell²  — L1 "robotic consistency" check
///
/// Source of `now`: `SystemTime::now()` on wasm32-wasip1 reads the host
/// clock that Fastly's Compute runtime exposes — wall-clock second
/// precision, same source we already use for debug-log timestamps at
/// main.rs:81. The previous `now_secs = 0` placeholder collapsed both
/// L1 rules to identity functions; this lights them up for real.
/// Session-lifetime bounds. SESSION_IDLE_EXPIRE_S caps the gap between
/// adjacent requests in one session (default 30 min — covers typical
/// user idle on a tab); SESSION_HARD_CAP_S caps the total session
/// duration regardless of activity (default 24h — bounds long-lived
/// sessions so a stolen cookie can't be replayed indefinitely). When
/// either threshold is exceeded, update_state mints a fresh sid +
/// resets all timing accumulators — the cookie remains valid (AES
/// decrypt + AAD check still succeed) but acts as a new session.
const SESSION_IDLE_EXPIRE_S: u32 = 30 * 60; // 30 minutes
const SESSION_HARD_CAP_S: u32 = 24 * 60 * 60; // 24 hours

fn update_state(
    prev: Option<cookie::SessionState>,
    result: &scorer::ScoreResult,
    current_route_path: &str,
    now_secs: u32,
) -> cookie::SessionState {
    let prev_route_path = current_route_path.to_string();
    match prev {
        Some(s) => {
            let idle = now_secs.saturating_sub(s.last_ts);
            let age = now_secs.saturating_sub(s.issued_at);
            // SESSION ROTATION: idle-expire OR hard-cap → mint a fresh
            // sid and reset timing. Bounded session lifetime is a
            // security feature (stolen cookies can't be replayed after
            // their window) and a data-hygiene feature (long-running
            // sessions stop biasing the variance estimator).
            if idle > SESSION_IDLE_EXPIRE_S || age > SESSION_HARD_CAP_S {
                return cookie::SessionState {
                    v: cookie::SCHEMA_VERSION,
                    sid: new_random_sid(),
                    seq: 1,
                    sum_dt: 0,
                    sum_dt_sq: 0,
                    last_ts: now_secs,
                    score: result.score,
                    issued_at: now_secs,
                    prev_route_path,
                };
            }
            let new_seq = s.seq.saturating_add(1);
            // Δt since the previous request in this session. Clamp at a
            // 1-hour ceiling to bound the impact of long-idle sessions
            // on the variance estimator (the L2 transition matrix
            // already discounts inactivity differently). saturating_sub
            // protects against clock skew where last_ts > now_secs.
            let dt_secs: u32 = idle.min(3600);
            let dt64 = u64::from(dt_secs);
            let new_sum_dt = s.sum_dt.saturating_add(dt_secs);
            let new_sum_dt_sq = s.sum_dt_sq.saturating_add(dt64.saturating_mul(dt64));
            cookie::SessionState {
                v: cookie::SCHEMA_VERSION,
                sid: s.sid,
                seq: new_seq,
                sum_dt: new_sum_dt,
                sum_dt_sq: new_sum_dt_sq,
                last_ts: now_secs,
                score: result.score,
                issued_at: s.issued_at,
                prev_route_path,
            }
        }
        None => cookie::SessionState {
            v: cookie::SCHEMA_VERSION,
            sid: new_random_sid(),
            seq: 1,
            sum_dt: 0,
            sum_dt_sq: 0,
            last_ts: now_secs,
            score: result.score,
            issued_at: now_secs,
            prev_route_path,
        },
    }
}

fn new_random_sid() -> [u8; cookie::SID_BYTES] {
    // Same CSPRNG-or-panic policy as random_nonce: a collision in sids
    // doesn't break confidentiality but it does collapse distinct
    // sessions into the same row in the labels table. Better to abort
    // the request and fail-open than to return a deterministic sid.
    let mut buf = [0u8; cookie::SID_BYTES];
    getrandom::getrandom(&mut buf).expect("WASI getrandom must not fail when generating session sid");
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_cookie_value_basic() {
        let h = "X-Session-State=ABC; other=foo";
        assert_eq!(extract_cookie_value(h, "X-Session-State"), Some("ABC"));
        assert_eq!(extract_cookie_value(h, "other"), Some("foo"));
        assert_eq!(extract_cookie_value(h, "missing"), None);
    }

    #[test]
    fn extract_cookie_value_handles_spacing() {
        // Each `;`-delimited segment is .trim()'d, so leading/trailing
        // whitespace around the segment is dropped. Real cookies are
        // base64url so this is mostly cosmetic, but verify the trim works.
        let h = " a=1 ; X-Session-State=XYZ ; b=2";
        assert_eq!(extract_cookie_value(h, "X-Session-State"), Some("XYZ"));
    }

    #[test]
    fn hex_decode_round_trip() {
        let bytes: Vec<u8> = (0..32).collect();
        let hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
        assert_eq!(hex_decode(&hex).unwrap(), bytes);
    }

    #[test]
    fn hex_decode_rejects_odd_length() {
        assert!(hex_decode("abc").is_err());
    }

    #[test]
    fn hex_decode_rejects_non_hex() {
        assert!(hex_decode("zzzz").is_err());
    }

    // ── update_state lifecycle tests ────────────────────────────────────────
    //
    // These exercise the session-rotation rules in update_state without
    // touching the WASI runtime. The refactor that hoisted `now_secs` to
    // be a parameter (rather than reading SystemTime inside the function)
    // exists specifically so these tests can drive the clock forward in a
    // controlled way — see the comment block above SESSION_IDLE_EXPIRE_S
    // for why bounded session lifetime is a security feature.

    fn mk_score_result(score: u8) -> scorer::ScoreResult {
        scorer::ScoreResult {
            score,
            ..Default::default()
        }
    }

    #[test]
    fn test_update_state_increments_event_count() {
        // First call (prev = None) → fresh session with seq = 1.
        let result = mk_score_result(10);
        let s1 = update_state(None, &result, "/home", 1_000);
        assert_eq!(s1.seq, 1, "first call should mint seq = 1");

        // Second call (prev = Some(s1)) → seq grows by 1.
        let s2 = update_state(Some(s1.clone()), &result, "/about", 1_001);
        assert_eq!(s2.seq, 2, "second call should bump seq to 2");
        assert_eq!(s2.sid, s1.sid, "sid should be preserved inside the window");
    }

    #[test]
    fn test_update_state_idle_expire_rotates_sid() {
        // Establish a session at t = 1000.
        let result = mk_score_result(20);
        let s1 = update_state(None, &result, "/home", 1_000);
        assert_eq!(s1.seq, 1);
        let original_sid = s1.sid;

        // Advance "now" past SESSION_IDLE_EXPIRE_S (30 min). idle =
        // now - last_ts > 30*60 → fresh sid + zeroed accumulators.
        let now = 1_000 + SESSION_IDLE_EXPIRE_S + 1;
        let s2 = update_state(Some(s1), &result, "/home", now);

        assert_ne!(s2.sid, original_sid, "idle-expire must mint a new sid");
        assert_eq!(s2.seq, 1, "accumulators should reset to fresh-session state");
        assert_eq!(s2.sum_dt, 0);
        assert_eq!(s2.sum_dt_sq, 0);
        assert_eq!(s2.issued_at, now);
        assert_eq!(s2.last_ts, now);
    }

    #[test]
    fn test_update_state_hard_cap_rotates_sid() {
        // Establish a session at t = 1000.
        let result = mk_score_result(20);
        let s1 = update_state(None, &result, "/home", 1_000);
        let original_sid = s1.sid;

        // Walk the session forward in small idle steps so idle stays
        // small but the cumulative age exceeds SESSION_HARD_CAP_S. We
        // can't just jump now forward in one step because that would
        // also blow past SESSION_IDLE_EXPIRE_S and trigger the idle
        // branch instead of the hard-cap branch. Simulate a long-lived
        // session by manually constructing prev with an old issued_at.
        let aged = cookie::SessionState {
            issued_at: 1_000,
            last_ts: 1_000 + SESSION_HARD_CAP_S, // recent activity
            ..s1
        };
        let now = 1_000 + SESSION_HARD_CAP_S + 1; // 1s past the cap
        let s2 = update_state(Some(aged), &result, "/home", now);

        assert_ne!(s2.sid, original_sid, "hard-cap must mint a new sid");
        assert_eq!(s2.seq, 1, "accumulators should reset on hard-cap rotation");
        assert_eq!(s2.issued_at, now);
    }

    #[test]
    fn test_update_state_normal_idle_keeps_sid() {
        // Establish a session and advance by 5 min — well under the
        // 30-min idle ceiling.
        let result = mk_score_result(15);
        let s1 = update_state(None, &result, "/home", 1_000);
        let original_sid = s1.sid;
        let original_issued_at = s1.issued_at;

        let now = 1_000 + 5 * 60;
        let s2 = update_state(Some(s1), &result, "/about", now);

        assert_eq!(s2.sid, original_sid, "sid must persist inside the window");
        assert_eq!(s2.seq, 2, "seq should grow on normal continuation");
        assert_eq!(
            s2.issued_at, original_issued_at,
            "issued_at is anchored to session start, not reset on continuation"
        );
        assert_eq!(s2.last_ts, now);
    }

    #[test]
    fn test_update_state_clock_skew_saturates_to_zero() {
        // Establish a session at now = 100, then call again at now = 50
        // (clock went backward — e.g. NTP correction, or two Compute
        // pops with skewed local clocks). idle = saturating_sub(50, 100)
        // = 0, which is well inside the idle window → no rotation.
        let result = mk_score_result(0);
        let s1 = update_state(None, &result, "/home", 100);
        let original_sid = s1.sid;

        let s2 = update_state(Some(s1), &result, "/home", 50);

        assert_eq!(s2.sid, original_sid, "clock skew must not rotate sid");
        assert_eq!(s2.seq, 2, "session continues across clock skew");
        // dt = saturating_sub(50, 100) = 0, so sum_dt stays at 0.
        assert_eq!(s2.sum_dt, 0, "sum_dt should not go backward");
        assert_eq!(s2.sum_dt_sq, 0);
        assert_eq!(s2.last_ts, 50, "last_ts tracks the most recent observation");
    }

    #[test]
    fn test_update_state_sum_dt_clamp() {
        // In-window dt should accumulate to the actual elapsed seconds.
        // After two calls 7s apart, sum_dt should be 7 and sum_dt_sq
        // should be 49 — verifies neither underflow (saturating_sub)
        // nor the 1-hour ceiling clamp fires for ordinary spacings.
        let result = mk_score_result(0);
        let s1 = update_state(None, &result, "/home", 1_000);
        assert_eq!(s1.sum_dt, 0, "fresh session starts with sum_dt = 0");

        let s2 = update_state(Some(s1), &result, "/about", 1_007);
        assert_eq!(s2.sum_dt, 7, "sum_dt should equal elapsed seconds");
        assert_eq!(s2.sum_dt_sq, 49, "sum_dt_sq should equal dt²");

        // The clamp branch: a 2-hour idle is clamped to 1h = 3600s
        // before being folded in. Verifies the ceiling fires.
        let s3 = update_state(Some(s2), &result, "/x", 1_007 + 7_200);
        // 30-min idle limit isn't tripped here? 7200 > 1800 → rotates.
        // Confirm rotation rather than asserting the clamp under this
        // path; the clamp itself fires for idle ≤ 30min but > 1h is
        // unreachable without first crossing the idle cap. Asserting
        // rotation here documents the precedence: idle-expire wins
        // over the dt clamp.
        assert_eq!(s3.seq, 1, "idle-expire takes precedence over dt clamp");
    }

    #[test]
    fn test_session_idle_expire_s_constant() {
        // 30 minutes, in seconds. Pinned so a refactor that
        // accidentally drops a zero is caught at test time rather
        // than discovered in production via mass session resets.
        assert_eq!(SESSION_IDLE_EXPIRE_S, 30 * 60);
    }

    #[test]
    fn test_session_hard_cap_s_constant() {
        // 24 hours, in seconds. Same rationale as the idle-expire
        // constant test above — a bounded session lifetime is a
        // security guarantee against indefinite cookie replay.
        assert_eq!(SESSION_HARD_CAP_S, 24 * 60 * 60);
    }
}

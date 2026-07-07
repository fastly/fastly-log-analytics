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
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

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
static REPLAY_FLAGGED_COUNT: AtomicU64 = AtomicU64::new(0);
// EC-05: fail-open observability. keys_fail counts key-load fail-opens (which
// early-return before the bottom-of-function metrics flush) so they land in the
// in-process denominator; cookie_encode_fail counts the silent drop of a rotated
// Set-Cookie when re-encode fails. Both were previously invisible to `metrics:`.
static KEYS_FAIL_COUNT: AtomicU64 = AtomicU64::new(0);
static COOKIE_ENCODE_FAIL_COUNT: AtomicU64 = AtomicU64::new(0);
const METRICS_EMIT_EVERY: u64 = 1000;

// ── Sustained-replay detector (per-instance, lock-free) ──────────────────────
// The session cookie is rotated on every response, so a legit client sends a
// DISTINCT sealed value each request. The only benign reason to see the SAME
// value twice is concurrency — a burst of in-flight requests carrying the
// cookie the client most recently held, before they process the new
// Set-Cookie. Those settle within a second or two. An attacker REPLAYS one
// low-score cookie indefinitely (ignoring Set-Cookie) to keep seq=1 and dodge
// the L1 warmup/velocity rules inside the idle window. We remember when each
// value was FIRST seen and flag a repeat only once it's still being presented
// past REPLAY_WINDOW_S — long after any legit burst. Keyed on first-seen (not
// the cookie's last_ts) so an idle-then-burst client is never falsely flagged.
const REPLAY_CACHE_SIZE: usize = 8192; // power of two → mask indexing
const REPLAY_PROBES: usize = 8;
const REPLAY_WINDOW_S: u32 = 30;
#[allow(clippy::declare_interior_mutable_const)]
const ZERO_SLOT: AtomicU64 = AtomicU64::new(0);
static REPLAY_HASH: [AtomicU64; REPLAY_CACHE_SIZE] = [ZERO_SLOT; REPLAY_CACHE_SIZE];
static REPLAY_FIRST_SEEN: [AtomicU64; REPLAY_CACHE_SIZE] = [ZERO_SLOT; REPLAY_CACHE_SIZE];

const SERVICE_ID_HEADER: &str = "X-Edge-Service-Id";
const PREV_ROUTE_HEADER: &str = "X-Edge-Prev-Route";
const PREV_ANCHOR_HEADER: &str = "X-Edge-Prev-Anchor";
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
// Layer-2 enforcement opt-in (scoring_config). `l2_enforce_enabled` is the
// explicit operator switch: L2 joins the *enforced* combined score only when it
// trims to "1" — never on a deployment-age clock. `l2_enabled_at` is the
// UNIX-epoch (seconds) anchor the backend stamps on off→on; the scorer derives
// the opt-in fade-in age from it live (see load_l2_days_since_optin), so it
// can't be spoofed by a client and doesn't reset on retrain. The 7-day
// deployment-age "readiness" signal is now an advisory gauge computed
// backend-side — the scorer no longer reads scoring_enabled_at.
const L2_ENFORCE_ENABLED_KEY: &str = "l2_enforce_enabled";
const L2_ENABLED_AT_KEY: &str = "l2_enabled_at";

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
    // Open the scoring_keys ConfigStore ONCE for the whole request: auth reads
    // the shared secret from it; load_keys reads the AES-GCM key(s) from the
    // same handle below. Opening per-helper crossed the host boundary twice for
    // the same store on every request — one reused handle halves that.
    let keys_store = ConfigStore::try_open(KEYS_STORE).ok();
    if !request_auth_ok(req, keys_store.as_ref()) {
        dbg_log("[ERROR] Unauthorized request: X-Edge-Scorer-Auth header missing or invalid");
        return Response::from_status(401)
            .with_header("X-Edge-Score-Reason", "unauthorized")
            .with_body("unauthorized");
    }

    // ── Header inputs (VCL fills these before Compute is invoked). ───────────
    let service_id = req.get_header_str(SERVICE_ID_HEADER).unwrap_or("default");
    let prev_route_raw = req.get_header_str(PREV_ROUTE_HEADER);
    let prev_anchor_raw = req.get_header_str(PREV_ANCHOR_HEADER);
    // Layer-2 enforcement gate. L2 joins the *enforced* combined score only when
    // an operator has explicitly opted in (l2_enforce_enabled="1"); on opt-in it
    // fades in over L2_RAMP_DAYS from the l2_enabled_at anchor. Both are
    // self-derived server-side from scoring_config, so a client can't spoof them
    // (the F009 evasion class closed for prev_route/anchor). Flag absent/off →
    // L2 observe-only forever: its sub-score is still computed + logged, just
    // weighted 0 in the combined score — no auto monitoring→blocking transition.
    // Open the scoring_config ConfigStore ONCE and read every toggle from the
    // single handle: the L2 opt-in flag + anchor here, the debug switch below,
    // and the enforce threshold near the end. `None` = store unavailable → each
    // reader falls back to its safe default, exactly as the per-helper try_open
    // calls did before (3 fewer host-boundary opens of the same store).
    let config_store = ConfigStore::try_open(CONFIG_STORE).ok();
    let l2_enforce_enabled: bool = config_store
        .as_ref()
        .map(load_l2_enforce_enabled)
        .unwrap_or(false);
    let l2_days_since_optin: f64 = config_store
        .as_ref()
        .map(load_l2_days_since_optin)
        .unwrap_or(0.0);

    // Cheap toggle: flip the `debug_logging_enabled` key in the
    // `scoring_config` ConfigStore to true (any truthy string), tail the
    // service with `fastly log-tail`, then flip back to off. The store
    // lookup is a constant-time hash hit so leaving the check in costs
    // ~nothing on the hot path.
    let debug = config_store
        .as_ref()
        .map(debug_logging_enabled)
        .unwrap_or(false);
    // Wall-clock at scorer entry, in nanoseconds. Diffed against a read
    // taken just before we return to get this request's Wasm execution
    // time. Captured UNCONDITIONALLY now (was debug-gated) because it is
    // emitted on every 200 as the X-Edge-Score-Exec-Us header → VCL →
    // edge_score_exec_us, giving operators compute-only latency distinct
    // from the edge-observed round-trip. SystemTime::now() is already on
    // the hot path (now_secs below), so this adds nothing measurable.
    let t0 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    if debug {
        dbg_log(&format!(
            "incoming: url={} service={} prev_route={:?} prev_anchor={:?} l2_enforce_enabled={} l2_days_since_optin={}",
            req.get_url_str(),
            service_id,
            prev_route_raw,
            prev_anchor_raw,
            l2_enforce_enabled,
            l2_days_since_optin,
        ));
    }

    // ── Resolve current route from the request URL. ──────────────────────────
    let current_route = normalize::normalize(req.get_url_str());

    // ── Load AES-GCM keys from the Edge Dictionary. ──────────────────────────
    let (key, prev_key) = match load_keys(keys_store.as_ref()) {
        Ok(pair) => pair,
        Err(e) => {
            // Misconfigured dictionary is operationally critical but should
            // fail open in the request path. Emit a diagnostic header so the
            // outage is visible in VCL logs.
            dbg_log(&format!("[ERROR] Failed to load keys from ConfigStore: {:?}", e));
            // EC-05: bump the keys fail-open counter AND flush metrics BEFORE the
            // early return — otherwise this path skips maybe_emit_metrics() at the
            // bottom, leaving key-load outages out of the in-process `metrics:`
            // line and its request denominator entirely.
            KEYS_FAIL_COUNT.fetch_add(1, Ordering::Relaxed);
            maybe_emit_metrics();
            return fail_open_response("internal-error-keys");
        }
    };

    // ── Decode the inbound cookie (if any). ──────────────────────────────────
    let inbound_cookie = req
        .get_header_str("cookie")
        .and_then(|h| extract_cookie_value(h, COOKIE_NAME));

    // Compute "now" UP FRONT so we can reject expired cookies BEFORE we hand
    // their state into the scorer. Pre-fix (audit finding 009), expiration
    // was only evaluated at the bottom of this function when minting the
    // replacement cookie — meaning an attacker who replayed an expired
    // low-score cookie got scored against the trusted historical state and
    // bypassed enforcement thresholds.
    // Fail CLOSED on a clock read failure. The pre-fix `.unwrap_or(0)` made
    // now_secs = 0, which silently disabled BOTH the cookie expiration check
    // (`idle`/`age` saturating-sub to 0 → never expired) and replay detection
    // (`now_secs - first_seen` → 0, never over the window), letting a replayed
    // expired low-score cookie be scored against trusted historical state.
    // We panic instead (panic=abort → request fails closed), mirroring the
    // getrandom decision in `random_nonce` below: on wasm32-wasip1 SystemTime
    // is a reliable host function and this branch is unreachable in practice.
    let now_secs: u32 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as u32)
        .expect("WASI clock must not fail — now_secs=0 disables cookie expiry + replay detection");

    let (state, compliance) = match inbound_cookie {
        None => (None, "missing"),
        Some(value) => match cookie::decode(
            value,
            &key,
            prev_key.as_deref(),
            service_id,
            cookie::SCHEMA_VERSION,
        ) {
            Ok(s) => {
                if session_expired(&s, now_secs) {
                    (None, "expired")
                } else if is_sustained_replay(value, now_secs) {
                    // Same sealed cookie still presented long after first
                    // sight → the client is ignoring Set-Cookie rotation
                    // (replay). Drop the cultivated low-seq state so the
                    // replayer can't sit at seq=1 dodging L1 warmup; scored
                    // as a fresh no-state session (penalized like missing).
                    REPLAY_FLAGGED_COUNT.fetch_add(1, Ordering::Relaxed);
                    (None, "replayed")
                } else {
                    (Some(s), "ok")
                }
            }
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
    // Use ONLY the prev_route stored in the cookie state (carried forward
    // from the last scored request in this session). The session cookie is
    // AEAD-sealed; an attacker can't forge it without the AES-GCM key.
    //
    // The prior implementation fell back to the unauthenticated
    // X-Edge-Prev-Route header when the cookie was missing — an attacker
    // could omit the session cookie and supply a benign route in the
    // header, deterministically depressing the L2 sequence anomaly score
    // and bypassing enforcement thresholds (audit F009, run 7ba15352).
    // The header is dropped here; missing-cookie sessions get a None
    // prev_route, matching the safe-by-default L2 fallback. The
    // ``prev_route_raw`` variable is still bound up-top for the debug log
    // so operators can see what the client tried to send.
    let prev_route = state
        .as_ref()
        .filter(|s| !s.prev_route_path.is_empty())
        .map(|s| normalize::Route {
            path: s.prev_route_path.clone(),
            // L2 transition lookup only uses path; category is unused.
            // Leaving empty avoids re-running the full normalize() pass.
            category: String::new(),
        });
    // The skip-gram anchor is treated exactly like prev_route above: it is
    // NOT taken from the unauthenticated X-Edge-Prev-Anchor header. A client
    // could otherwise supply a SEEN high-probability anchor route; because the
    // L2 transition prob is `direct_p.max(anchor_p * BETA)` (scorer.rs), a
    // client-chosen anchor can only RAISE trans_prob and thus DEPRESS the L2
    // sequence-anomaly score — the same evasion class closed for prev_route in
    // audit F009. There is no trusted server-side source for the anchor today
    // (it is not carried in the sealed cookie state), so it stays None and the
    // skip-gram self-disables, matching the safe-by-default L2 fallback. The
    // `prev_anchor_raw` binding survives only for the debug log so operators
    // can see what the client tried to send.
    let prev_anchor: Option<normalize::Route> = None;

    // ── Score. ───────────────────────────────────────────────────────────────
    // Lazy matrix load: L2 only fires when there's a trusted previous-route
    // signal (the sealed-cookie prev_route; prev_anchor is forced None above).
    // Cookie-missing requests — the large majority — never use the matrix, so
    // skip the ~1.8MB KV fetch entirely for them. The fetch (and its cold-start
    // cost) was pushing the scorer round-trip past the timeout budget and
    // causing fail-opens; keeping it off the common path fixes that while
    // still serving the matrix (cached per-instance) to L2-eligible requests.
    // Behaviour is unchanged: with prev_route None, L2 has no transition to
    // score regardless of whether the matrix is loaded.
    let needs_matrix = prev_route.is_some() || prev_anchor.is_some();
    let matrix = if needs_matrix { matrix::load_matrix() } else { None };
    if needs_matrix && matrix.is_none() {
        // We needed the matrix but it's unlinked/empty/unparseable. L2
        // self-disables; bump the counter so the periodic metrics line
        // surfaces a service not yet seeded with a trained matrix.
        MATRIX_LOAD_FAIL_COUNT.fetch_add(1, Ordering::Relaxed);
    }
    let result = scorer::score_combined(scorer::ScoreInputs {
        state: state.as_ref(),
        cookie_compliance: compliance,
        current_route: &current_route,
        prev_route: prev_route.as_ref(),
        prev_anchor_route: prev_anchor.as_ref(),
        matrix,
        l2_enforce_enabled,
        l2_days_since_optin,
    });

    // ── Re-encode the updated cookie. ────────────────────────────────────────
    // We rotate the cookie on every request so the seq/sum_dt fields stay
    // fresh and the encryption nonce never repeats. The just-scored
    // current_route becomes the next request's prev_route. `now_secs` was
    // computed near the top of the function so the expiration check could
    // run before scoring (see audit finding 009).
    let updated = update_state(state.clone(), &result, &current_route.path, now_secs);
    let set_cookie = match cookie::encode(
        &updated,
        &key,
        &random_nonce(),
        service_id,
        cookie::SCHEMA_VERSION,
    ) {
        Ok(c) => Some(c),
        Err(e) => {
            dbg_log(&format!("[ERROR] Failed to encode rotated cookie: {:?}", e));
            // EC-05: count the silent Set-Cookie drop so a re-encode regression is
            // visible in `metrics:` (this request still completes — no rotated
            // cookie means the next request is cookie-missing).
            COOKIE_ENCODE_FAIL_COUNT.fetch_add(1, Ordering::Relaxed);
            None
        }
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
    if let Some(t) = config_store.as_ref().and_then(load_enforce_threshold) {
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

    // Wasm execution time for this request (µs). Emitted on every 200 so
    // VCL can stash it into edge_score_exec_us — compute-only latency,
    // separate from the edge-observed round-trip (edge_score_rtt_us, which
    // also includes network + cold-start and is measured in VCL). Computed
    // unconditionally here and reused by the debug log below.
    let t1 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let elapsed_us = exec_us(t0, t1);
    resp.set_header("X-Edge-Score-Exec-Us", elapsed_us.to_string());

    if debug {
        let current_dt_secs = state
            .as_ref()
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
/// enables verbose log emission. Missing key → off. The store handle is opened
/// once at the top of `score_request` and shared with the other config readers;
/// a missing store is handled there (the reader is skipped → debug off).
///
/// Always returns a bool — never panics — because this is on the request
/// hot path and a misconfigured store must not 5xx real traffic.
fn debug_logging_enabled(dict: &ConfigStore) -> bool {
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
fn load_enforce_threshold(dict: &ConfigStore) -> Option<u32> {
    let raw = dict.get(ENFORCE_THRESHOLD_KEY)?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let n: u32 = trimmed.parse().ok()?;
    Some(n.min(100))
}

/// Read the operator's explicit Layer-2 enforcement opt-in flag from
/// scoring_config. Returns ``true`` only when ``l2_enforce_enabled`` is present
/// AND trims to exactly "1".
///
/// Fail-CLOSED-to-observe-only on EVERY other path (missing store, missing/empty/
/// any-other value) → ``false`` → L2 contributes nothing to the enforced
/// combined score. This is the safe default: L2's sub-score is still always
/// computed + logged, but it never joins blocking until an operator explicitly
/// opts in via the admin UI. There is deliberately no deployment-age clock that
/// flips this on automatically.
fn load_l2_enforce_enabled(dict: &ConfigStore) -> bool {
    match dict.get(L2_ENFORCE_ENABLED_KEY) {
        Some(v) => v.trim() == "1",
        None => false,
    }
}

/// Resolve the L2 opt-in fade-in age: days (with fractional part) since the
/// operator opted L2 into enforcement, read from the ``l2_enabled_at`` anchor
/// (UNIX epoch seconds) in the scoring_config ConfigStore. The backend stamps
/// this key on the off→on transition; the scorer derives the age live so the
/// fade-in (optin_ramp_weight: day 0 → 0 … day L2_RAMP_DAYS → 1, scorer.rs)
/// advances continuously. Deriving it here — rather than from a request header
/// or the matrix's build date — means it cannot be spoofed by a client AND does
/// not reset every time the matrix is retrained (a build-date clock would).
///
/// Fail-open to 0.0 on EVERY error path (missing store, missing/empty/unparseable
/// key, clock read failure, future anchor). age 0.0 → optin_ramp_weight 0 → L2
/// contributes nothing until the fade-in advances — so a service whose anchor is
/// missing (flag never set) or just-stamped scores L2 as observe-only.
fn load_l2_days_since_optin(dict: &ConfigStore) -> f64 {
    let raw = match dict.get(L2_ENABLED_AT_KEY) {
        Some(v) => v,
        None => return 0.0,
    };
    let enabled_at: u64 = match raw.trim().parse() {
        Ok(n) => n,
        Err(_) => return 0.0,
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    if now <= enabled_at {
        // Anchor in the future (clock skew / mis-seed) → treat as just-opted-in
        // → fade-in keeps L2 weight at 0 rather than over-trusting it.
        return 0.0;
    }
    (now - enabled_at) as f64 / 86_400.0
}

/// Wasm execution time in microseconds from two `SystemTime`-derived
/// nanosecond stamps. `saturating_sub` so a non-monotonic clock (t1 < t0)
/// yields 0 rather than a wrapped enormous value — this feeds the
/// X-Edge-Score-Exec-Us header → edge_score_exec_us, and a bogus spike
/// there would skew the latency percentiles operators tune timeouts on.
fn exec_us(t0_nanos: u128, t1_nanos: u128) -> u128 {
    t1_nanos.saturating_sub(t0_nanos) / 1_000
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
    let count = REQUEST_COUNT
        .fetch_add(1, Ordering::Relaxed)
        .wrapping_add(1);
    if count.is_multiple_of(METRICS_EMIT_EVERY) {
        dbg_log(&format!(
            "metrics: tampered={} replayed={} enforce_block={} matrix_fail={} keys_fail={} cookie_encode_fail={} requests={}",
            TAMPERED_COOKIE_COUNT.load(Ordering::Relaxed),
            REPLAY_FLAGGED_COUNT.load(Ordering::Relaxed),
            ENFORCE_BLOCK_COUNT.load(Ordering::Relaxed),
            MATRIX_LOAD_FAIL_COUNT.load(Ordering::Relaxed),
            KEYS_FAIL_COUNT.load(Ordering::Relaxed),
            COOKIE_ENCODE_FAIL_COUNT.load(Ordering::Relaxed),
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

fn request_auth_ok(req: &Request, keys: Option<&ConfigStore>) -> bool {
    let provided = req.get_header_str(SCORER_AUTH_HEADER).unwrap_or("");
    if provided.is_empty() {
        return false;
    }
    // A missing scoring_keys store (keys=None) or a missing/empty secret rejects
    // the request — better than letting unauthenticated traffic through on
    // misconfiguration. The store handle is opened once by the caller.
    let expected = match keys.and_then(load_request_secret) {
        Some(v) => v,
        None => return false,
    };
    // Constant-time compare to avoid timing-leak side channels. The
    // comparison is over short strings (32 hex chars) so the gain is
    // minor in practice but free to add.
    constant_time_eq(provided.as_bytes(), expected.as_bytes())
}

fn load_request_secret(dict: &ConfigStore) -> Option<String> {
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

fn load_keys(keys: Option<&ConfigStore>) -> Result<(Vec<u8>, Option<Vec<u8>>), Error> {
    // EC-05: a missing / unlinked scoring_keys store (keys=None) must fail OPEN
    // gracefully via the Err → fail_open_response("internal-error-keys") path,
    // not panic=abort the request. The handle is opened once at the top of
    // score_request and shared with the auth check (request_auth_ok).
    let dict = keys.ok_or_else(|| Error::msg("scoring_keys store unavailable"))?;
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
    if !s.len().is_multiple_of(2) {
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

/// Returns true when this sealed cookie value looks like a SUSTAINED replay:
/// the same value is still being presented more than `REPLAY_WINDOW_S` after
/// this instance first saw it. See the cache doc above for why first-seen
/// (not the cookie's `last_ts`) is the right key — it tolerates legit
/// concurrent bursts (including idle-then-burst) while catching a client
/// that ignores Set-Cookie and replays one cookie indefinitely.
///
/// Lock-free open-addressing over a fixed ring. On a fresh sighting the
/// timestamp is published BEFORE the hash so any reader that observes the
/// hash is guaranteed to read a valid first-seen (no torn "seen but ts=0"
/// false positive). Probe-chain exhaustion overwrites the home slot and
/// treats the value as not-replay (eviction is a documented residual risk —
/// it can only MISS a replay, never manufacture a false positive).
fn is_sustained_replay(cookie_value: &str, now_secs: u32) -> bool {
    static REPLAY_HASHER: OnceLock<RandomState> = OnceLock::new();
    let mut hasher = REPLAY_HASHER.get_or_init(RandomState::new).build_hasher();
    cookie_value.hash(&mut hasher);
    let h = hasher.finish();
    let h = if h == 0 { 1 } else { h }; // 0 marks an empty slot
    let start = (h as usize) & (REPLAY_CACHE_SIZE - 1);
    for probe in 0..REPLAY_PROBES {
        let idx = (start + probe) & (REPLAY_CACHE_SIZE - 1);
        let slot = REPLAY_HASH[idx].load(Ordering::Acquire);
        if slot == h {
            let first = REPLAY_FIRST_SEEN[idx].load(Ordering::Acquire) as u32;
            return now_secs.saturating_sub(first) > REPLAY_WINDOW_S;
        }
        if slot == 0 {
            REPLAY_FIRST_SEEN[idx].store(u64::from(now_secs), Ordering::Release);
            REPLAY_HASH[idx].store(h, Ordering::Release);
            return false;
        }
    }
    REPLAY_FIRST_SEEN[start].store(u64::from(now_secs), Ordering::Release);
    REPLAY_HASH[start].store(h, Ordering::Release);
    false
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

/// True when a session has exceeded either lifetime bound — the idle gap
/// since the last request or the total age since issue. Shared by the
/// decode-time "expired" verdict (score_request) and the update_state
/// sid-rotation so the two predicates stay in lockstep. saturating_sub
/// protects against clock skew where last_ts/issued_at > now_secs.
fn session_expired(s: &cookie::SessionState, now_secs: u32) -> bool {
    let idle = now_secs.saturating_sub(s.last_ts);
    let age = now_secs.saturating_sub(s.issued_at);
    idle > SESSION_IDLE_EXPIRE_S || age > SESSION_HARD_CAP_S
}

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
            // SESSION ROTATION: idle-expire OR hard-cap → mint a fresh
            // sid and reset timing. Bounded session lifetime is a
            // security feature (stolen cookies can't be replayed after
            // their window) and a data-hygiene feature (long-running
            // sessions stop biasing the variance estimator).
            if session_expired(&s, now_secs) {
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
            let mut new_sum_dt = s.sum_dt;
            let mut new_sum_dt_sq = s.sum_dt_sq;
            if s.seq >= 20 {
                new_sum_dt -= new_sum_dt / 20;
                new_sum_dt_sq -= new_sum_dt_sq / 20;
            }
            let new_sum_dt = new_sum_dt.saturating_add(dt_secs);
            let new_sum_dt_sq = new_sum_dt_sq.saturating_add(dt64.saturating_mul(dt64));
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
    getrandom::getrandom(&mut buf)
        .expect("WASI getrandom must not fail when generating session sid");
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
    fn exec_us_converts_ns_to_us_and_saturates() {
        // 5000ns = 5µs.
        assert_eq!(exec_us(1_000, 6_000), 5);
        // Zero elapsed.
        assert_eq!(exec_us(10_000, 10_000), 0);
        // Non-monotonic clock (t1 < t0) → 0, never a wrapped huge value
        // that would corrupt the latency percentiles.
        assert_eq!(exec_us(9_000, 1_000), 0);
    }

    #[test]
    fn hex_decode_rejects_odd_length() {
        assert!(hex_decode("abc").is_err());
    }

    #[test]
    fn hex_decode_rejects_non_hex() {
        assert!(hex_decode("zzzz").is_err());
    }

    // ── is_sustained_replay ──────────────────────────────────────────────────
    //
    // The replay cache is a process-wide static shared across the whole test
    // binary, so each test uses a UNIQUE cookie string. Slot collisions only
    // ever cause probing (the `slot == h` check compares the full 64-bit
    // hash, never just the index), so distinct strings can't false-match.

    #[test]
    fn replay_first_sighting_not_flagged() {
        assert!(!is_sustained_replay("replaytest-first-sighting", 1_000));
    }

    #[test]
    fn replay_concurrent_burst_within_window_not_flagged() {
        // A concurrent in-flight burst re-presents the SAME freshly-rotated
        // cookie a few seconds apart — must be tolerated, up to the window.
        assert!(!is_sustained_replay("replaytest-burst", 1_000));
        assert!(!is_sustained_replay("replaytest-burst", 1_003));
        assert!(!is_sustained_replay(
            "replaytest-burst",
            1_000 + REPLAY_WINDOW_S
        ));
    }

    #[test]
    fn replay_sustained_beyond_window_flagged() {
        assert!(!is_sustained_replay("replaytest-sustained", 5_000));
        // Still presenting the same sealed cookie well past first sight.
        assert!(is_sustained_replay(
            "replaytest-sustained",
            5_000 + REPLAY_WINDOW_S + 1
        ));
    }

    #[test]
    fn replay_rotating_client_never_flagged() {
        // A protocol-following client sends a distinct value each request.
        for i in 0..64u32 {
            let v = format!("replaytest-rotating-{i}");
            assert!(!is_sustained_replay(&v, 6_000 + i));
        }
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
        assert_eq!(
            s2.seq, 1,
            "accumulators should reset to fresh-session state"
        );
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

    #[test]
    fn test_session_expired_boundaries() {
        // Pin the shared predicate at both bounds: exactly == cap is NOT
        // expired (the comparison is strictly `>`); one second past IS.
        let base = update_state(None, &mk_score_result(0), "/home", 1_000);

        // Idle bound: age stays tiny, idle drives the verdict.
        assert!(
            !session_expired(&base, 1_000 + SESSION_IDLE_EXPIRE_S),
            "idle == cap must not expire"
        );
        assert!(
            session_expired(&base, 1_000 + SESSION_IDLE_EXPIRE_S + 1),
            "idle one second past cap must expire"
        );

        // Hard-cap bound: keep last_ts recent so only total age drives it.
        let aged = cookie::SessionState {
            issued_at: 1_000,
            last_ts: 1_000 + SESSION_HARD_CAP_S,
            ..base
        };
        assert!(
            !session_expired(&aged, 1_000 + SESSION_HARD_CAP_S),
            "age == cap must not expire"
        );
        assert!(
            session_expired(&aged, 1_000 + SESSION_HARD_CAP_S + 1),
            "age one second past cap must expire"
        );
    }
}

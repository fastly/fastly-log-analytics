//! AES-GCM-with-AAD session cookie codec.
//!
//! Wire-format contract with the Python reference (`backend/scoring/cookie.py`).
//! The packed-binary layout, AAD format, dual-key trial-decrypt order, and
//! base64url encoding all match byte-for-byte. The cross-language fixture
//! tests in [`tests::wire_format`] are the canonical source of truth — if a
//! Python change drifts from Rust (or vice versa), one of those tests fails
//! immediately.
//!
//! Layout (variable, little-endian throughout):
//!
//! ```text
//!   v                u8         schema version (first byte, for dispatch)
//!   sid              [u8;6]     raw session id
//!   seq              u16        sequence count
//!   sum_dt           u32        Σ Δt seconds
//!   sum_dt_sq        u64        Σ Δt² seconds²
//!   last_ts          u32        last-request unix epoch
//!   score            u8         quantized 0-100
//!   issued_at        u32        cookie creation unix epoch     ← end of v1 (30 B)
//!   prev_route_len   u8         length of prev_route_path  (v2 only, 0-255)
//!   prev_route_path  [u8;N]     normalized path of last-scored URL (UTF-8)
//! ```
//!
//! v1 cookies still decode (with prev_route_path = ""). Encoder always emits v2.
//!
//! aad: ASCII `{service_id}|v{schema_version}`

use aes_gcm::{
    aead::{Aead, KeyInit, Payload},
    Aes256Gcm, Key, Nonce,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

pub const SCHEMA_VERSION: u8 = 2;
pub const SID_BYTES: usize = 6;
pub const NONCE_BYTES: usize = 12;
pub const KEY_BYTES: usize = 32;
pub const SCORE_BUCKET: u8 = 5;
/// v1 plaintext size (fixed 30 bytes). Kept exposed because the decoder
/// uses it to dispatch v1 vs v2 layout.
pub const V1_PLAINTEXT_BYTES: usize = 30;
/// Maximum bytes we'll encode for prev_route_path. Long paths get
/// truncated at encode time — the matrix transition lookup tolerates an
/// unknown prev_route by returning the uniform-prior probability, so the
/// failure mode of truncation is "L2 = uniform" not "crash".
pub const PREV_ROUTE_MAX_BYTES: usize = 255;
pub const GCM_TAG_BYTES: usize = 16;
/// Minimum total envelope size (v1 plaintext + nonce + tag). Used by
/// decode() to reject obviously-malformed cookies before attempting AEAD.
pub const ENVELOPE_BYTES: usize = NONCE_BYTES + V1_PLAINTEXT_BYTES + GCM_TAG_BYTES;

#[derive(Debug, PartialEq, Eq)]
pub enum CookieError {
    /// Cookie too short, wrong nonce length, plaintext length mismatch.
    BadFraming(&'static str),
    /// Base64URL decode failed.
    BadBase64,
    /// AEAD verification failed (tampered, wrong key, wrong AAD).
    BadAuth,
    /// Decoded payload has a schema version this codec doesn't support.
    BadSchemaVersion(u8),
    /// SessionState bounds violation when building before encode.
    OutOfRange(&'static str),
    /// Wrong AES key length passed to codec constructor.
    BadKeyLength,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SessionState {
    pub v: u8,
    pub sid: [u8; SID_BYTES],
    pub seq: u16,
    pub sum_dt: u32,
    pub sum_dt_sq: u64,
    pub last_ts: u32,
    pub score: u8,
    pub issued_at: u32,
    /// Normalized path of the most-recently-scored URL for this session.
    /// Carried in the cookie so the scorer can compute the L2 transition
    /// probability without VCL having to pass prev_route via a header
    /// (req.http doesn't persist across separate client requests, so a
    /// header-based mechanism was always broken). Empty when the cookie
    /// was a v1 decode or when this is the first request in a session.
    pub prev_route_path: String,
}

impl SessionState {
    /// Validate score/version bounds before serialization. The other fields
    /// are width-typed (u16, u32, u64) so out-of-range is structurally
    /// impossible at the Rust level.
    pub fn validate(&self) -> Result<(), CookieError> {
        if self.score > 100 {
            return Err(CookieError::OutOfRange("score"));
        }
        // prev_route_path is silently truncated at PREV_ROUTE_MAX_BYTES
        // during pack_payload — we don't reject on long paths since the
        // fail-mode of truncation is L2 falls back to uniform-prior for
        // the unrecognized prefix, which is correct.
        Ok(())
    }
}

/// Quantize a score to the nearest [`SCORE_BUCKET`], clamped to [0, 100].
///
/// Matches Python's `quantize_score`: uses bankers-rounding (round-half-to-
/// even) so 12.5 → 10 and 17.5 → 20. The cross-lang round-trip tests pin
/// the expected values, but we get there honestly via Rust's f64::round_ties_even.
pub fn quantize_score(raw: f64) -> u8 {
    let clamped = raw.clamp(0.0, 100.0);
    let bucket = SCORE_BUCKET as f64;
    let rounded = (clamped / bucket).round_ties_even() * bucket;
    rounded as u8
}

fn pack_payload(state: &SessionState) -> Vec<u8> {
    // v2 layout: 30-byte fixed header + 1-byte length prefix + N bytes
    // of UTF-8 path. We always emit the v2 length prefix even when the
    // path is empty so the decoder can dispatch unambiguously on
    // plaintext length (== 30 → v1 legacy, > 30 → v2).
    let path_len = state.prev_route_path.floor_char_boundary(PREV_ROUTE_MAX_BYTES);
    let path_bytes = state.prev_route_path.as_bytes();
    let mut out = Vec::with_capacity(V1_PLAINTEXT_BYTES + 1 + path_len);
    out.push(state.v);
    out.extend_from_slice(&state.sid);
    out.extend_from_slice(&state.seq.to_le_bytes());
    out.extend_from_slice(&state.sum_dt.to_le_bytes());
    out.extend_from_slice(&state.sum_dt_sq.to_le_bytes());
    out.extend_from_slice(&state.last_ts.to_le_bytes());
    out.push(state.score);
    out.extend_from_slice(&state.issued_at.to_le_bytes());
    out.push(path_len as u8);
    out.extend_from_slice(&path_bytes[..path_len]);
    out
}

fn unpack_payload(buf: &[u8]) -> Result<SessionState, CookieError> {
    if buf.len() < V1_PLAINTEXT_BYTES {
        return Err(CookieError::BadFraming("plaintext too short"));
    }
    let mut sid = [0u8; SID_BYTES];
    sid.copy_from_slice(&buf[1..7]);
    let mut state = SessionState {
        v: buf[0],
        sid,
        seq: u16::from_le_bytes(buf[7..9].try_into().unwrap()),
        sum_dt: u32::from_le_bytes(buf[9..13].try_into().unwrap()),
        sum_dt_sq: u64::from_le_bytes(buf[13..21].try_into().unwrap()),
        last_ts: u32::from_le_bytes(buf[21..25].try_into().unwrap()),
        score: buf[25],
        issued_at: u32::from_le_bytes(buf[26..30].try_into().unwrap()),
        prev_route_path: String::new(),
    };
    // v1 cookies stop here (30 bytes total). v2 has a length-prefixed
    // UTF-8 path suffix.
    if buf.len() > V1_PLAINTEXT_BYTES {
        let path_len = buf[V1_PLAINTEXT_BYTES] as usize;
        let path_end = V1_PLAINTEXT_BYTES + 1 + path_len;
        if buf.len() != path_end {
            return Err(CookieError::BadFraming("prev_route_path length"));
        }
        state.prev_route_path = std::str::from_utf8(&buf[V1_PLAINTEXT_BYTES + 1..path_end])
            .map_err(|_| CookieError::BadFraming("prev_route_path utf-8"))?
            .to_string();
    }
    Ok(state)
}

fn aad(service_id: &str, schema_version: u8) -> Vec<u8> {
    format!("{}|v{}", service_id, schema_version).into_bytes()
}

/// Encrypt + base64url. `nonce` MUST be unique-per-encrypt under any key
/// (reused nonces under the same key destroy GCM's confidentiality + auth
/// guarantees simultaneously). The caller passes a fresh 96-bit random
/// nonce in production; tests use a fixed nonce to pin wire-format bytes.
pub fn encode(
    state: &SessionState,
    key: &[u8],
    nonce: &[u8],
    service_id: &str,
    schema_version: u8,
) -> Result<String, CookieError> {
    state.validate()?;
    if state.v != schema_version {
        return Err(CookieError::BadSchemaVersion(state.v));
    }
    if key.len() != KEY_BYTES {
        return Err(CookieError::BadKeyLength);
    }
    if nonce.len() != NONCE_BYTES {
        return Err(CookieError::BadFraming("nonce length"));
    }

    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let plaintext = pack_payload(state);
    let aad_bytes = aad(service_id, schema_version);
    let ciphertext = cipher
        .encrypt(
            Nonce::from_slice(nonce),
            Payload {
                msg: plaintext.as_slice(),
                aad: &aad_bytes,
            },
        )
        .map_err(|_| CookieError::BadAuth)?;

    let mut envelope = Vec::with_capacity(NONCE_BYTES + ciphertext.len());
    envelope.extend_from_slice(nonce);
    envelope.extend_from_slice(&ciphertext);
    Ok(URL_SAFE_NO_PAD.encode(&envelope))
}

/// Decrypt + verify. Trial-decrypts with `key`, then `previous_key` if
/// present (24h post-rotation grace window). All three failure modes
/// (bad framing, bad base64, AEAD failure) surface as distinct
/// [`CookieError`] variants so the caller can categorize for the
/// X-Edge-Cookie-Compliance header.
pub fn decode(
    cookie: &str,
    key: &[u8],
    previous_key: Option<&[u8]>,
    service_id: &str,
    schema_version: u8,
) -> Result<SessionState, CookieError> {
    if key.len() != KEY_BYTES {
        return Err(CookieError::BadKeyLength);
    }
    if let Some(p) = previous_key {
        if p.len() != KEY_BYTES {
            return Err(CookieError::BadKeyLength);
        }
    }

    let raw = URL_SAFE_NO_PAD
        .decode(cookie)
        .map_err(|_| CookieError::BadBase64)?;
    if raw.len() < ENVELOPE_BYTES {
        return Err(CookieError::BadFraming("envelope too short"));
    }
    let nonce = &raw[..NONCE_BYTES];
    let ciphertext = &raw[NONCE_BYTES..];
    let aad_bytes = aad(service_id, schema_version);

    let try_decrypt = |k: &[u8]| -> Option<Vec<u8>> {
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(k));
        cipher
            .decrypt(
                Nonce::from_slice(nonce),
                Payload { msg: ciphertext, aad: &aad_bytes },
            )
            .ok()
    };

    let plaintext = match try_decrypt(key) {
        Some(p) => p,
        None => previous_key
            .and_then(try_decrypt)
            .ok_or(CookieError::BadAuth)?,
    };

    let state = unpack_payload(&plaintext)?;
    // Accept v1 cookies during the migration window — they have no
    // prev_route_path, which means L2 transition lookup falls back to
    // uniform probability for that one request, but the request still
    // serves. The decoder is the only place we accept old schemas; the
    // encoder always emits the current SCHEMA_VERSION.
    if state.v != schema_version && state.v != 1 {
        return Err(CookieError::BadSchemaVersion(state.v));
    }
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY_A: [u8; 32] = [
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
        25, 26, 27, 28, 29, 30, 31,
    ];
    const KEY_B: [u8; 32] = [
        31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9,
        8, 7, 6, 5, 4, 3, 2, 1, 0,
    ];
    const NONCE_FIXED: [u8; 12] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
    const SVC: &str = "TestSvc123";

    fn state() -> SessionState {
        SessionState {
            v: SCHEMA_VERSION,
            sid: [1, 2, 3, 4, 5, 6],
            seq: 10,
            sum_dt: 100,
            sum_dt_sq: 1500,
            last_ts: 1_700_000_000,
            score: 25,
            issued_at: 1_699_990_000,
            prev_route_path: String::new(),
        }
    }

    // ── quantize_score ──────────────────────────────────────────────────────

    #[test]
    fn quantize_buckets() {
        // Pinned values from the Python parametrized test set.
        assert_eq!(quantize_score(0.0), 0);
        assert_eq!(quantize_score(1.0), 0);
        assert_eq!(quantize_score(2.0), 0);
        assert_eq!(quantize_score(3.0), 5);
        assert_eq!(quantize_score(7.0), 5);
        assert_eq!(quantize_score(8.0), 10);
        assert_eq!(quantize_score(12.0), 10);
        assert_eq!(quantize_score(13.0), 15);
        assert_eq!(quantize_score(47.0), 45);
        assert_eq!(quantize_score(48.0), 50);
        assert_eq!(quantize_score(97.0), 95);
        assert_eq!(quantize_score(98.0), 100);
        assert_eq!(quantize_score(100.0), 100);
        assert_eq!(quantize_score(-5.0), 0);
        assert_eq!(quantize_score(150.0), 100);
        // Banker's rounding: 50.5 → 50 (matches Python's round())
        assert_eq!(quantize_score(50.5), 50);
        // 52.5/5 = 10.5 → bankers-rounds to 10 → *5 = 50
        assert_eq!(quantize_score(52.5), 50);
    }

    // ── pack / unpack ───────────────────────────────────────────────────────

    /// CROSS-LANGUAGE CONTRACT: this hex string is byte-identical to the one
    /// pinned in `tests/scoring/test_cookie.py::test_pack_layout_byte_exact`.
    /// If either side changes wire format, both tests update together — or
    /// the build breaks. v2 adds a length-prefixed UTF-8 path suffix; the
    /// fixture exercises a non-empty path so the byte layout is verified
    /// end-to-end including the length prefix.
    #[test]
    fn pack_layout_byte_exact() {
        let s = SessionState {
            v: 2,
            sid: [0x11, 0x22, 0x33, 0x44, 0x55, 0x66],
            seq: 0x1234,
            sum_dt: 0x10203040,
            sum_dt_sq: 0x0102030405060708,
            last_ts: 0x65000000,
            score: 80,
            issued_at: 0x64000000,
            // "/home" → 5 UTF-8 bytes: 2f 68 6f 6d 65
            prev_route_path: "/home".to_string(),
        };
        let packed = pack_payload(&s);
        let expected = hex::decode(
            "02\
             112233445566\
             3412\
             40302010\
             0807060504030201\
             00000065\
             50\
             00000064\
             05\
             2f686f6d65",
        )
        .unwrap();
        assert_eq!(&packed[..], &expected[..]);
    }

    /// v2 with empty prev_route_path still emits the length-prefix byte
    /// (= 0). The wire layout is always v2 from the encoder; v1 is only a
    /// decode-side back-compat.
    #[test]
    fn pack_layout_empty_prev_route() {
        let mut s = state();
        s.prev_route_path = String::new();
        let packed = pack_payload(&s);
        assert_eq!(packed.len(), V1_PLAINTEXT_BYTES + 1);
        assert_eq!(packed[V1_PLAINTEXT_BYTES], 0);
    }

    /// v1 decoder back-compat: a 30-byte plaintext should round-trip into
    /// a SessionState with prev_route_path = empty (legacy cookies issued
    /// before the schema bump).
    #[test]
    fn unpack_accepts_v1_30_byte_plaintext() {
        let v1 = hex::decode(
            "01\
             112233445566\
             3412\
             40302010\
             0807060504030201\
             00000065\
             50\
             00000064",
        )
        .unwrap();
        let s = unpack_payload(&v1).unwrap();
        assert_eq!(s.v, 1);
        assert_eq!(s.prev_route_path, "");
        assert_eq!(s.score, 80);
    }

    #[test]
    fn pack_unpack_round_trip_with_path() {
        let mut s = state();
        s.prev_route_path = "/checkout".to_string();
        let packed = pack_payload(&s);
        // v1 header (30) + 1 length byte + 9 path bytes = 40 bytes
        assert_eq!(packed.len(), V1_PLAINTEXT_BYTES + 1 + 9);
        let recovered = unpack_payload(&packed).unwrap();
        assert_eq!(recovered, s);
    }

    #[test]
    fn unpack_rejects_too_short() {
        assert!(matches!(
            unpack_payload(&[0u8; 29]),
            Err(CookieError::BadFraming(_))
        ));
    }

    // ── encode / decode round-trip ──────────────────────────────────────────

    #[test]
    fn encode_decode_round_trip() {
        let s = state();
        let cookie = encode(&s, &KEY_A, &NONCE_FIXED, SVC, SCHEMA_VERSION).unwrap();
        let decoded = decode(&cookie, &KEY_A, None, SVC, SCHEMA_VERSION).unwrap();
        assert_eq!(decoded, s);
    }

    #[test]
    fn encode_envelope_size_with_empty_path() {
        // v2 always emits a length-prefix byte even when path is empty,
        // so envelope is one byte larger than the v1 baseline.
        let cookie = encode(&state(), &KEY_A, &NONCE_FIXED, SVC, SCHEMA_VERSION).unwrap();
        let raw = URL_SAFE_NO_PAD.decode(&cookie).unwrap();
        assert_eq!(raw.len(), NONCE_BYTES + V1_PLAINTEXT_BYTES + 1 + GCM_TAG_BYTES);
    }

    #[test]
    fn encode_decode_round_trip_preserves_prev_route_path() {
        let mut s = state();
        s.prev_route_path = "/users/{int}/profile".to_string();
        let cookie = encode(&s, &KEY_A, &NONCE_FIXED, SVC, SCHEMA_VERSION).unwrap();
        let decoded = decode(&cookie, &KEY_A, None, SVC, SCHEMA_VERSION).unwrap();
        assert_eq!(decoded.prev_route_path, "/users/{int}/profile");
        assert_eq!(decoded.score, s.score);
        assert_eq!(decoded.sid, s.sid);
    }

    #[test]
    fn encode_truncates_path_over_max() {
        let mut s = state();
        s.prev_route_path = "a".repeat(PREV_ROUTE_MAX_BYTES + 50);
        let cookie = encode(&s, &KEY_A, &NONCE_FIXED, SVC, SCHEMA_VERSION).unwrap();
        let decoded = decode(&cookie, &KEY_A, None, SVC, SCHEMA_VERSION).unwrap();
        // Truncated to the cap; decoder reads exactly what was encoded.
        assert_eq!(decoded.prev_route_path.len(), PREV_ROUTE_MAX_BYTES);
    }

    #[test]
    fn encode_truncates_path_safely_on_utf8_char_boundary() {
        let mut s = state();
        // A multi-byte character (🦀 is 4 bytes). Place it right at the boundary.
        let mut path = "a".repeat(PREV_ROUTE_MAX_BYTES - 1);
        path.push_str("🦀"); // Total: 254 + 4 = 258 bytes
        s.prev_route_path = path;

        let cookie = encode(&s, &KEY_A, &NONCE_FIXED, SVC, SCHEMA_VERSION).unwrap();
        let decoded = decode(&cookie, &KEY_A, None, SVC, SCHEMA_VERSION).unwrap();

        // Assert that the decoded path has exactly PREV_ROUTE_MAX_BYTES - 1 bytes,
        // dropping the whole straddling emoji cleanly instead of splitting its raw bytes.
        assert_eq!(decoded.prev_route_path.len(), PREV_ROUTE_MAX_BYTES - 1);
        assert_eq!(decoded.prev_route_path, "a".repeat(PREV_ROUTE_MAX_BYTES - 1));
    }


    #[test]
    fn decode_rejects_tampered_ciphertext() {
        let cookie = encode(&state(), &KEY_A, &NONCE_FIXED, SVC, SCHEMA_VERSION).unwrap();
        let mut raw = URL_SAFE_NO_PAD.decode(&cookie).unwrap();
        raw[NONCE_BYTES + 5] ^= 0x01;
        let tampered = URL_SAFE_NO_PAD.encode(&raw);
        assert_eq!(
            decode(&tampered, &KEY_A, None, SVC, SCHEMA_VERSION),
            Err(CookieError::BadAuth)
        );
    }

    #[test]
    fn decode_rejects_wrong_key() {
        let cookie = encode(&state(), &KEY_A, &NONCE_FIXED, SVC, SCHEMA_VERSION).unwrap();
        assert_eq!(
            decode(&cookie, &KEY_B, None, SVC, SCHEMA_VERSION),
            Err(CookieError::BadAuth)
        );
    }

    #[test]
    fn decode_rejects_wrong_service_id() {
        let cookie = encode(&state(), &KEY_A, &NONCE_FIXED, "Foo", SCHEMA_VERSION).unwrap();
        assert_eq!(
            decode(&cookie, &KEY_A, None, "Bar", SCHEMA_VERSION),
            Err(CookieError::BadAuth)
        );
    }

    #[test]
    fn decode_rejects_wrong_schema_version() {
        // Encode at AAD "svc|v1", decode at AAD "svc|v99" → AAD mismatch
        // → BadAuth. (v1 is intentionally accepted by the decoder for
        // back-compat, so we exercise a version the encoder won't ever
        // emit and the decoder won't accept.)
        let mut s = state();
        s.v = 1;
        let cookie = encode(&s, &KEY_A, &NONCE_FIXED, SVC, 1).unwrap();
        assert_eq!(
            decode(&cookie, &KEY_A, None, SVC, 99),
            Err(CookieError::BadAuth) // AAD mismatch surfaces first
        );
    }

    #[test]
    fn decode_rejects_garbage_base64() {
        assert_eq!(
            decode("A", &KEY_A, None, SVC, SCHEMA_VERSION),
            Err(CookieError::BadBase64)
        );
    }

    #[test]
    fn decode_rejects_too_short_envelope() {
        let cookie = URL_SAFE_NO_PAD.encode([0u8; 20]);
        assert!(matches!(
            decode(&cookie, &KEY_A, None, SVC, SCHEMA_VERSION),
            Err(CookieError::BadFraming(_))
        ));
    }

    #[test]
    fn dual_key_decrypts_via_previous_during_grace() {
        let cookie_under_old = encode(&state(), &KEY_A, &NONCE_FIXED, SVC, SCHEMA_VERSION).unwrap();
        // Encode under A, decode via codec that holds B as current + A as previous.
        let decoded =
            decode(&cookie_under_old, &KEY_B, Some(&KEY_A), SVC, SCHEMA_VERSION).unwrap();
        assert_eq!(decoded, state());
    }

    #[test]
    fn no_previous_key_strict_mode_rejects_old_cookies() {
        let cookie = encode(&state(), &KEY_A, &NONCE_FIXED, SVC, SCHEMA_VERSION).unwrap();
        assert_eq!(
            decode(&cookie, &KEY_B, None, SVC, SCHEMA_VERSION),
            Err(CookieError::BadAuth)
        );
    }
}

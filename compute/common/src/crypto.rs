use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use hmac::{Hmac, KeyInit, Mac};
use serde::{de::DeserializeOwned, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

/// Constant-time byte-slice equality check backed by `subtle::ConstantTimeEq`.
#[inline]
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && bool::from(a.ct_eq(b))
}

/// Constant-time equality check over two secrets by comparing their 32-byte SHA-256 digests so
/// secret length does not leak.
#[inline]
pub fn constant_time_secret_eq(a: &str, b: &str) -> bool {
    let da = Sha256::digest(a.as_bytes());
    let db = Sha256::digest(b.as_bytes());
    bool::from(da.ct_eq(&db))
}

/// Computes a 32-byte HMAC-SHA256 digest over `data` using `key`.
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> Result<[u8; 32], &'static str> {
    let mut mac = HmacSha256::new_from_slice(key).map_err(|_| "invalid_hmac_key")?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().into())
}

/// Computes a lowercase hex-encoded HMAC-SHA256 digest over `data` using `key`.
pub fn hmac_sha256_hex(key: &[u8], data: &[u8]) -> Result<String, &'static str> {
    Ok(hex::encode(hmac_sha256(key, data)?))
}

/// Computes a lowercase hex-encoded SHA-256 digest of `data`.
pub fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// Serializes `value` as JSON and returns `<b64url_payload>.<b64url_hmac_sha256>`.
pub fn seal_json<T: Serialize>(secret: &[u8], value: &T) -> Result<String, &'static str> {
    let bytes = serde_json::to_vec(value).map_err(|_| "seal_serialize_failed")?;
    let payload = URL_SAFE_NO_PAD.encode(bytes);
    let tag = hmac_sha256(secret, payload.as_bytes())?;
    let mac = URL_SAFE_NO_PAD.encode(tag);
    Ok(format!("{payload}.{mac}"))
}

/// Verifies `<b64url_payload>.<mac>` (accepting base64url or hex MAC) and deserializes JSON.
pub fn unseal_json<T: DeserializeOwned>(secret: &[u8], token: &str) -> Result<T, &'static str> {
    let (payload, mac_str) = token.split_once('.').ok_or("invalid_token_format")?;
    let expected = hmac_sha256(secret, payload.as_bytes())?;
    let valid = if let Ok(decoded_mac) = URL_SAFE_NO_PAD.decode(mac_str) {
        constant_time_eq(&decoded_mac, &expected)
    } else if let Ok(decoded_hex) = hex::decode(mac_str) {
        constant_time_eq(&decoded_hex, &expected)
    } else {
        false
    };
    if !valid {
        return Err("invalid_token_signature");
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| "invalid_token_payload")?;
    serde_json::from_slice(&bytes).map_err(|_| "invalid_token_json")
}

/// 32-bit FNV-1a hash over a sequence of bytes.
#[inline]
pub fn fnv1a32(bytes: impl IntoIterator<Item = u8>) -> u32 {
    bytes.into_iter().fold(0x811c_9dc5_u32, |hash, byte| {
        (hash ^ u32::from(byte)).wrapping_mul(0x0100_0193)
    })
}

/// 64-bit FNV-1a hash over a sequence of bytes.
#[inline]
pub fn fnv1a64(bytes: impl IntoIterator<Item = u8>) -> u64 {
    bytes
        .into_iter()
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    #[test]
    fn seal_and_unseal_round_trip_and_reject_tampering() {
        let secret = b"0123456789abcdef0123456789abcdef";
        let claims = json!({"sub": "u1", "exp": 1234});
        let sealed = seal_json(secret, &claims).unwrap();
        let unsealed: Value = unseal_json(secret, &sealed).unwrap();
        assert_eq!(unsealed, claims);

        assert!(unseal_json::<Value>(b"wrong-key", &sealed).is_err());
        assert!(unseal_json::<Value>(secret, &format!("{sealed}x")).is_err());
    }

    #[test]
    fn fnv1a_matches_reference_vectors() {
        assert_eq!(fnv1a32(b"".iter().copied()), 0x811c_9dc5);
        assert_eq!(fnv1a64(b"".iter().copied()), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a32(b"foobar".iter().copied()), 0xbf9c_f968);
        assert_eq!(fnv1a64(b"foobar".iter().copied()), 0x8594_4171_f739_67e8);
    }
}

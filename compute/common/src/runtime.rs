use std::time::{SystemTime, UNIX_EPOCH};

/// Current UTC Unix timestamp in whole seconds.
#[inline]
pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Current UTC Unix timestamp in milliseconds.
#[inline]
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Formats a `u64` Unix timestamp (seconds) as an RFC 3339 UTC timestamp string.
pub fn rfc3339_from_secs(seconds: u64) -> String {
    time::OffsetDateTime::from_unix_timestamp(i64::try_from(seconds).unwrap_or(0))
        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH)
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".into())
}

/// Formats an `i64` Unix timestamp (seconds) as an RFC 3339 UTC timestamp string.
#[inline]
pub fn rfc3339(seconds: i64) -> String {
    rfc3339_from_secs(u64::try_from(seconds).unwrap_or(0))
}

/// Fills `buf` with OS / WASI cryptographic random bytes.
#[inline]
pub fn fill_random(buf: &mut [u8]) -> Result<(), &'static str> {
    getrandom::getrandom(buf).map_err(|_| "rng_failed")
}

/// Returns a fixed-size array of cryptographic random bytes.
#[inline]
pub fn random_bytes<const N: usize>() -> Result<[u8; N], &'static str> {
    let mut bytes = [0u8; N];
    fill_random(&mut bytes)?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_rfc3339_timestamps() {
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339_from_secs(1_700_000_000), "2023-11-14T22:13:20Z");
    }

    #[test]
    fn generates_random_bytes() {
        let a: [u8; 32] = random_bytes().unwrap();
        let b: [u8; 32] = random_bytes().unwrap();
        assert_ne!(a, b);
    }
}

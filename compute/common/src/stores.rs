use fastly::{
    kv_store::{InsertMode, KVStoreError},
    ConfigStore, KVStore, SecretStore,
};
use std::time::Duration;

/// Delay in milliseconds between KV write attempts: 1,100 ms on `wasm32` (for Fastly KV's 1 req/s
/// per-key write rate limit) and configurable via `FLAKY_KV_WRITE_DELAY_MS` on native tests.
pub fn kv_write_delay_ms() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        1_100
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        std::env::var("FLAKY_KV_WRITE_DELAY_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    }
}

/// Runs `op(attempt)` up to `attempts` times, sleeping `kv_write_delay_ms()` before each retry.
pub fn kv_retry_with_delay<T, E, F>(attempts: usize, mut op: F) -> Result<T, E>
where
    F: FnMut(usize) -> Result<T, E>,
{
    let total = attempts.max(1);
    let mut attempt = 0;
    loop {
        if attempt > 0 {
            let delay_ms = kv_write_delay_ms();
            if delay_ms > 0 {
                std::thread::sleep(Duration::from_millis(delay_ms));
            }
        }
        match op(attempt) {
            Ok(val) => return Ok(val),
            Err(err) => {
                attempt += 1;
                if attempt >= total {
                    return Err(err);
                }
            }
        }
    }
}

/// Reads an optional non-empty value from a Fastly `ConfigStore`.
pub fn config_lookup(store_name: &str, key: &str) -> Result<Option<String>, String> {
    let store = ConfigStore::try_open(store_name).map_err(|e| e.to_string())?;
    Ok(store.get(key).filter(|v| !v.is_empty()))
}

/// Reads an optional UTF-8 secret from a Fastly `SecretStore`.
pub fn secret_lookup(store_name: &str, key: &str) -> Result<Option<String>, String> {
    let store = SecretStore::open(store_name).map_err(|e| e.to_string())?;
    let Some(secret) = store.get(key) else {
        return Ok(None);
    };
    Ok(String::from_utf8(secret.plaintext().to_vec()).ok())
}

/// Looks up a key in a Fastly `KVStore`, returning `(body_bytes, generation)` when present.
pub fn kv_lookup_bytes(store_name: &str, key: &str) -> Result<Option<(Vec<u8>, u64)>, String> {
    let store = KVStore::open(store_name)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("KV store {store_name} is not linked"))?;
    match store.lookup(key) {
        Ok(mut response) => {
            let generation = response.current_generation();
            let bytes = response.take_body_bytes();
            Ok(Some((bytes, generation)))
        }
        Err(KVStoreError::ItemNotFound) => Ok(None),
        Err(err) => Err(err.to_string()),
    }
}

/// Inserts or CAS-updates a key in a Fastly `KVStore`. Returns `Ok(false)` on generation mismatch.
pub fn kv_insert_bytes(
    store_name: &str,
    key: &str,
    value: &[u8],
    expected_generation: Option<u64>,
    ttl: Option<Duration>,
) -> Result<bool, String> {
    let store = KVStore::open(store_name)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("KV store {store_name} is not linked"))?;
    let mut builder = store.build_insert();
    match expected_generation {
        Some(generation) => {
            builder = builder
                .mode(InsertMode::Overwrite)
                .if_generation_match(generation);
        }
        None => {
            builder = builder.mode(InsertMode::Add);
        }
    }
    if let Some(ttl) = ttl {
        builder = builder.time_to_live(ttl);
    }
    match builder.execute(key, value.to_vec()) {
        Ok(()) => Ok(true),
        Err(KVStoreError::ItemPreconditionFailed) => Ok(false),
        Err(err) => Err(err.to_string()),
    }
}

/// Deletes a key from a Fastly `KVStore` (treating `ItemNotFound` as success).
pub fn kv_delete_key(store_name: &str, key: &str) -> Result<(), String> {
    let store = KVStore::open(store_name)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("KV store {store_name} is not linked"))?;
    match store.delete(key) {
        Ok(()) | Err(KVStoreError::ItemNotFound) => Ok(()),
        Err(err) => Err(err.to_string()),
    }
}

/// Fastly KV rejects prefix queries containing these characters with HTTP 400.
pub const KV_UNSEARCHABLE: &[char] = &[
    '!', '"', '$', '%', '&', '(', ')', '*', '+', ',', '/', '\\', ':', '<', '=', '>', '@', '[', ']',
    '`', '{', '}',
];

/// The longest searchable leading part of `prefix`; the caller filters the rest client-side.
#[inline]
pub fn kv_list_stem(prefix: &str) -> &str {
    prefix
        .find(KV_UNSEARCHABLE)
        .map_or(prefix, |end| &prefix[..end])
}

/// Filters a stem-listed KV page down to keys matching `prefix` while preserving the cursor until
/// the sorted listing has passed `prefix`.
pub fn kv_filter_stem_page(
    keys: Vec<String>,
    next_cursor: Option<String>,
    prefix: &str,
) -> (Vec<String>, Option<String>) {
    let past = keys
        .iter()
        .any(|k| !k.starts_with(prefix) && k.as_str() > prefix);
    let filtered = keys.into_iter().filter(|k| k.starts_with(prefix)).collect();
    let next = if past { None } else { next_cursor };
    (filtered, next)
}

/// In-memory KV list pagination helper used across native runtime and test fixtures.
pub fn kv_page_from_keys(
    all_keys: impl IntoIterator<Item = String>,
    prefix: &str,
    cursor: Option<&str>,
    limit: u32,
) -> (Vec<String>, Option<String>) {
    let limit = limit as usize;
    let mut matching: Vec<String> = all_keys
        .into_iter()
        .filter(|k| k.starts_with(prefix) && cursor.is_none_or(|c| k.as_str() > c))
        .collect();
    let more = matching.len() > limit;
    matching.truncate(limit);
    let next_cursor = more.then(|| matching.last().cloned()).flatten();
    (matching, next_cursor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kv_stem_and_page_helpers_preserve_pagination() {
        assert_eq!(kv_list_stem("logs:cursor:a"), "logs");
        let (keys, next) = kv_filter_stem_page(
            vec!["logs:a".into(), "logs:cursor:b".into()],
            Some("c1".into()),
            "logs:cursor:",
        );
        assert_eq!(keys, vec!["logs:cursor:b"]);
        assert_eq!(next.as_deref(), Some("c1"));

        let (page1, cur1) = kv_page_from_keys(
            vec!["p:1".into(), "p:2".into(), "p:3".into()],
            "p:",
            None,
            2,
        );
        assert_eq!(page1, vec!["p:1", "p:2"]);
        assert_eq!(cur1.as_deref(), Some("p:2"));
        let (page2, cur2) = kv_page_from_keys(
            vec!["p:1".into(), "p:2".into(), "p:3".into()],
            "p:",
            cur1.as_deref(),
            2,
        );
        assert_eq!(page2, vec!["p:3"]);
        assert_eq!(cur2, None);
    }
}

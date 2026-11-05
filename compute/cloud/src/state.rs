use crate::auth::{AuthState, User};
use std::sync::Mutex;
#[cfg(target_arch = "wasm32")]
pub mod fastly;
pub mod projection;
#[cfg(target_arch = "wasm32")]
pub use fastly::FastlyState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateError(pub String);
impl std::fmt::Display for StateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for StateError {}

#[derive(Clone, Default)]
pub struct VersionedState {
    pub generation: Option<String>,
    pub state: AuthState,
}

/// Implement with a generation-match write on `auth:state`; absence requires insert-only.
/// A successful CAS commits both domain mutations and their audit outbox atomically.
/// Materialize users, invites, challenges, setup failures and audit entries before
/// acknowledging the write. A subsequent load must repair interrupted materialization.
/// Archived audit keys are `audit:<audit_offset + index>`. After durable archival,
/// a load may clear the outbox and advance `audit_offset` in its returned snapshot;
/// the next generation-matched domain write commits this compaction.
///
/// Auth visibility assumes foreground writes (`background_fetch=false`, the SDK
/// default) are reflected by subsequent entry lookups. This is not a claim of
/// linearizable item reads. A model returning pre-acknowledgement user and
/// coordinator snapshots after a completed foreground write is an unconfirmed
/// platform hypothesis; stronger guarantees would require platform confirmation.
pub trait StateStore {
    fn load(&self) -> Result<VersionedState, StateError>;
    /// Read `user:<id>` independently. Return only a committed materialization.
    fn load_user(&self, id: &str) -> Result<Option<User>, StateError>;
    fn compare_and_swap(
        &self,
        expected: Option<&str>,
        next: &AuthState,
    ) -> Result<bool, StateError>;
}

#[derive(Default)]
pub struct MemoryStore {
    inner: Mutex<(u64, AuthState)>,
}

impl StateStore for MemoryStore {
    fn load_user(&self, id: &str) -> Result<Option<User>, StateError> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| StateError("lock".into()))?
            .1
            .users
            .get(id)
            .cloned())
    }
    fn load(&self) -> Result<VersionedState, StateError> {
        let inner = self.inner.lock().map_err(|_| StateError("lock".into()))?;
        Ok(VersionedState {
            generation: (inner.0 != 0).then(|| inner.0.to_string()),
            state: inner.1.clone(),
        })
    }

    fn compare_and_swap(
        &self,
        expected: Option<&str>,
        next: &AuthState,
    ) -> Result<bool, StateError> {
        let mut inner = self.inner.lock().map_err(|_| StateError("lock".into()))?;
        let generation = (inner.0 != 0).then(|| inner.0.to_string());
        if generation.as_deref() != expected {
            return Ok(false);
        }
        inner.0 += 1;
        inner.1 = next.clone();
        Ok(true)
    }
}

pub fn parse_kv_meta(meta: &[u8]) -> (Option<u64>, Option<u64>) {
    let Ok(s) = std::str::from_utf8(meta) else {
        return (None, None);
    };
    if let Some((rev_part, ts_part)) = s.split_once('@') {
        let rev = (!rev_part.is_empty())
            .then(|| rev_part.parse().ok())
            .flatten();
        let ts = (!ts_part.is_empty())
            .then(|| ts_part.parse().ok())
            .flatten();
        (rev, ts)
    } else {
        (s.parse().ok(), None)
    }
}

#[cfg(test)]
mod tests {
    use super::parse_kv_meta;

    #[test]
    fn parse_kv_meta_supports_legacy_and_timestamped_formats() {
        assert_eq!(parse_kv_meta(b""), (None, None));
        assert_eq!(parse_kv_meta(b"42"), (Some(42), None));
        assert_eq!(
            parse_kv_meta(b"42@1700000000123"),
            (Some(42), Some(1700000000123))
        );
        assert_eq!(
            parse_kv_meta(b"@1700000000123"),
            (None, Some(1700000000123))
        );
    }
}

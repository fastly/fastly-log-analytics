use super::{StateError, StateStore, VersionedState};
use crate::auth::{AuditEntry, AuthState, Role, User};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;

const ROOT: &str = "auth:state";
const CALL_LIMIT: u32 = 24;

#[derive(Clone, Debug)]
pub struct KvRecord {
    pub generation: String,
    pub value: Vec<u8>,
    pub revision: Option<u64>,
}

/// `None` requires insert-only; `Some` requires an exact generation-match write.
pub trait Kv {
    fn get(&self, key: &str) -> Result<Option<KvRecord>, StateError>;
    fn put(
        &self,
        key: &str,
        expected: Option<&str>,
        value: &[u8],
        revision: Option<u64>,
    ) -> Result<bool, StateError>;
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct Envelope {
    state: AuthState,
    revision: u64,
    user_revisions: BTreeMap<String, u64>,
    pending: Vec<Change>,
}
#[derive(Clone, Serialize, Deserialize)]
struct Change {
    key: String,
    value: Value,
    previous: Option<Value>,
    #[serde(default)]
    previous_revision: Option<u64>,
}
#[derive(Clone)]
struct Snapshot {
    generation: Option<String>,
    envelope: Envelope,
}

/// Create once per request: budgets and snapshots must not survive request boundaries.
pub struct ProjectedStore<K: Kv> {
    kv: K,
    now: u64,
    calls: Cell<u32>,
    conflicts: Cell<u32>,
    snapshot: RefCell<Option<Snapshot>>,
}

impl<K: Kv> ProjectedStore<K> {
    pub fn new(kv: K, now: u64) -> Self {
        Self {
            kv,
            now,
            calls: Cell::new(0),
            conflicts: Cell::new(0),
            snapshot: RefCell::new(None),
        }
    }
    pub fn calls(&self) -> u32 {
        self.calls.get()
    }
    pub fn conflicts(&self) -> u32 {
        self.conflicts.get()
    }
    fn charge(&self) -> Result<(), StateError> {
        if self.calls.get() >= CALL_LIMIT {
            return Err(StateError("KV request budget exhausted".into()));
        }
        self.calls.set(self.calls.get() + 1);
        Ok(())
    }
    fn get(&self, key: &str) -> Result<Option<KvRecord>, StateError> {
        self.charge()?;
        self.kv.get(key)
    }
    fn put(
        &self,
        key: &str,
        expected: Option<&str>,
        value: &[u8],
        revision: Option<u64>,
    ) -> Result<bool, StateError> {
        self.charge()?;
        let success = self.kv.put(key, expected, value, revision)?;
        if !success {
            self.conflicts.set(self.conflicts.get() + 1);
        }
        Ok(success)
    }
    fn marker(revision: u64) -> String {
        format!("auth:projection:{revision:020}")
    }
    fn read_snapshot(&self) -> Result<Snapshot, StateError> {
        let snapshot = match self.get(ROOT)? {
            Some(record) => Snapshot {
                generation: Some(record.generation),
                envelope: serde_json::from_slice(&record.value)
                    .map_err(|_| StateError("invalid auth coordinator".into()))?,
            },
            None => Snapshot {
                generation: None,
                envelope: Envelope::default(),
            },
        };
        if snapshot.envelope.revision > 0
            && self
                .get(&Self::marker(snapshot.envelope.revision))?
                .is_none()
        {
            self.materialize(&snapshot.envelope)?;
        }
        *self.snapshot.borrow_mut() = Some(snapshot.clone());
        Ok(snapshot)
    }
    fn materialize(&self, envelope: &Envelope) -> Result<(), StateError> {
        for change in &envelope.pending {
            self.project(change, envelope.revision)?;
        }
        let key = Self::marker(envelope.revision);
        if !self.put(&key, None, b"true", Some(envelope.revision))? && self.get(&key)?.is_none() {
            return Err(StateError("projection acknowledgement conflict".into()));
        }
        Ok(())
    }
    fn project(&self, change: &Change, revision: u64) -> Result<(), StateError> {
        let expected = serde_json::to_vec(&change.value)
            .map_err(|_| StateError("invalid projection".into()))?;
        for _ in 0..2 {
            let current = self.get(&change.key)?;
            if let Some(record) = &current {
                if record.revision.is_some_and(|r| r > revision) {
                    return Err(StateError("projection superseded".into()));
                }
                let value: Value = serde_json::from_slice(&record.value)
                    .map_err(|_| StateError("invalid materialized record".into()))?;
                if record.revision == Some(revision) && value == change.value {
                    return Ok(());
                }
                if change.key.starts_with("user:")
                    && value != change.value
                    && change
                        .previous
                        .as_ref()
                        .is_some_and(|previous| previous != &value)
                    && (record.revision.is_none()
                        || record.revision == Some(revision)
                        || record.revision == change.previous_revision)
                {
                    // Preserve account-owner edits; normalize metadata so load_user imports them.
                    if record.revision.is_none() {
                        return Ok(());
                    }
                    if self.put(&change.key, Some(&record.generation), &record.value, None)? {
                        return Ok(());
                    }
                    continue;
                }
            } else if change.key.starts_with("user:") && change.previous.is_some() {
                return Ok(());
            }
            if self.put(
                &change.key,
                current.as_ref().map(|r| r.generation.as_str()),
                &expected,
                Some(revision),
            )? {
                return Ok(());
            }
        }
        Err(StateError("projection generation conflict".into()))
    }
    fn commit(
        &self,
        expected: Option<&str>,
        next: &AuthState,
        force_user: Option<&str>,
    ) -> Result<bool, StateError> {
        let cached = self.snapshot.borrow().clone();
        let old = match cached {
            Some(snapshot) if snapshot.generation.as_deref() == expected => snapshot,
            _ => self.read_snapshot()?,
        };
        if old.generation.as_deref() != expected {
            return Ok(false);
        }
        let revision = old
            .envelope
            .revision
            .checked_add(1)
            .ok_or_else(|| StateError("revision exhausted".into()))?;
        let mut envelope = Envelope {
            state: next.clone(),
            revision,
            user_revisions: old.envelope.user_revisions.clone(),
            pending: Vec::new(),
        };
        let old_records = records(&old.envelope.state, self.now)?;
        let next_records = records(next, self.now)?;
        for (key, value) in &next_records {
            let previous = old_records.get(key);
            if previous != Some(value) || force_user.is_some_and(|id| key == &format!("user:{id}"))
            {
                envelope.pending.push(Change {
                    key: key.clone(),
                    value: value.clone(),
                    previous: previous.cloned(),
                    previous_revision: key
                        .strip_prefix("user:")
                        .and_then(|id| old.envelope.user_revisions.get(id).copied()),
                });
                if let Some(id) = key.strip_prefix("user:") {
                    envelope.user_revisions.insert(id.into(), revision);
                }
            }
        }
        for (key, value) in &old_records {
            if !next_records.contains_key(key) && !key.starts_with("audit:") {
                envelope.pending.push(Change {
                    key: key.clone(),
                    value: Value::Null,
                    previous: Some(value.clone()),
                    previous_revision: key
                        .strip_prefix("user:")
                        .and_then(|id| old.envelope.user_revisions.get(id).copied()),
                });
                if let Some(id) = key.strip_prefix("user:") {
                    envelope.user_revisions.insert(id.into(), revision);
                }
            }
        }
        let needed = 2 * envelope.pending.len() as u32 + 2;
        if self.calls.get().saturating_add(needed) > CALL_LIMIT {
            return Err(StateError("mutation exceeds remaining KV budget".into()));
        }
        let value =
            serde_json::to_vec(&envelope).map_err(|_| StateError("invalid coordinator".into()))?;
        if !self.put(ROOT, expected, &value, Some(revision))? {
            *self.snapshot.borrow_mut() = None;
            return Ok(false);
        }
        *self.snapshot.borrow_mut() = None;
        self.materialize(&envelope)?;
        Ok(true)
    }
}

impl<K: Kv> StateStore for ProjectedStore<K> {
    fn load(&self) -> Result<VersionedState, StateError> {
        let snapshot = self.read_snapshot()?;
        let mut state = snapshot.envelope.state;
        state.audit_offset = state
            .audit_offset
            .checked_add(state.audit.len() as u64)
            .ok_or_else(|| StateError("audit sequence exhausted".into()))?;
        state.recent_audit.append(&mut state.audit);
        if state.recent_audit.len() > 50 {
            let drain_count = state.recent_audit.len() - 50;
            state.recent_audit.drain(..drain_count);
        }
        Ok(VersionedState {
            generation: snapshot.generation,
            state,
        })
    }
    fn load_user(&self, id: &str) -> Result<Option<User>, StateError> {
        let cached = self.snapshot.borrow().clone();
        let snapshot = match cached {
            Some(snapshot) => snapshot,
            None => self.read_snapshot()?,
        };
        let Some(canonical) = snapshot.envelope.state.users.get(id) else {
            return Ok(None);
        };
        let Some(record) = self.get(&format!("user:{id}"))? else {
            return Ok(None);
        };
        let user: User = match serde_json::from_slice(&record.value) {
            Ok(user) => user,
            Err(_) if record.value == b"null" => return Ok(None),
            Err(_) => return Err(StateError("invalid user record".into())),
        };
        let expected = snapshot.envelope.user_revisions.get(id).copied();
        if record.revision.is_some() && record.revision != expected {
            return Err(StateError("user projection revision mismatch".into()));
        }
        if &user == canonical && record.revision == expected {
            return Ok(Some(user));
        }
        if user.id != id || user.display_name.trim().is_empty() || user.display_name.len() > 200 {
            return Err(StateError("invalid CLI user edit".into()));
        }
        // A lookup can be stale. Prove this is still the primary's CLI edit
        // before importing it into the authoritative coordination record.
        if !self.put(
            &format!("user:{id}"),
            Some(&record.generation),
            &record.value,
            record.revision,
        )? {
            return Err(StateError("CLI user edit was superseded".into()));
        }
        let mut state = snapshot.envelope.state;
        state.audit_offset = state
            .audit_offset
            .checked_add(state.audit.len() as u64)
            .ok_or_else(|| StateError("audit sequence exhausted".into()))?;
        state.recent_audit.append(&mut state.audit);
        if state.recent_audit.len() > 50 {
            let drain_count = state.recent_audit.len() - 50;
            state.recent_audit.drain(..drain_count);
        }
        state.users.insert(id.into(), user.clone());
        if !state.users.values().any(|u| {
            u.role == Role::Admin && !u.revoked && u.credentials.iter().any(|c| !c.revoked)
        }) {
            return Err(StateError("CLI edit would remove last admin".into()));
        }
        state.audit.push(AuditEntry {
            actor: "cli".into(),
            action: "user_edited".into(),
            target: id.into(),
            time: self.now,
            ip: String::new(),
        });
        if !self.commit(snapshot.generation.as_deref(), &state, Some(id))? {
            return Err(StateError("CLI edit generation conflict".into()));
        }
        Ok(Some(user))
    }
    fn compare_and_swap(
        &self,
        expected: Option<&str>,
        next: &AuthState,
    ) -> Result<bool, StateError> {
        self.commit(expected, next, None)
    }
}

fn records(state: &AuthState, now: u64) -> Result<BTreeMap<String, Value>, StateError> {
    let mut result = BTreeMap::new();
    for (id, user) in &state.users {
        result.insert(
            format!("user:{id}"),
            serde_json::to_value(user).map_err(serialization)?,
        );
    }
    for (id, invite) in &state.invites {
        if invite.expires_at > now && !invite.revoked {
            result.insert(
                format!("invite:{id}"),
                serde_json::to_value(invite).map_err(serialization)?,
            );
        }
    }
    for (id, challenge) in &state.challenges {
        if challenge.expires_at > now {
            result.insert(
                format!("webauthn_challenge:{id}"),
                serde_json::to_value(challenge).map_err(serialization)?,
            );
        }
    }
    result.insert(
        "auth:setup_failures".into(),
        serde_json::to_value(&state.setup).map_err(serialization)?,
    );
    for (index, entry) in state.audit.iter().enumerate() {
        let sequence = state
            .audit_offset
            .checked_add(index as u64)
            .ok_or_else(|| StateError("audit sequence exhausted".into()))?;
        result.insert(
            format!("audit:{sequence:020}"),
            serde_json::to_value(entry).map_err(serialization)?,
        );
    }
    Ok(result)
}
fn serialization(_: serde_json::Error) -> StateError {
    StateError("record serialization failed".into())
}

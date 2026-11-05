use fastly_log_analytics::auth::{AuthState, Credential, Role, User};
use fastly_log_analytics::state::projection::{Kv, KvRecord, ProjectedStore};
use fastly_log_analytics::state::{StateError, StateStore};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
struct KvMemory {
    records: Arc<Mutex<(u64, BTreeMap<String, KvRecord>)>>,
    fail_key: Arc<Mutex<Option<String>>>,
}
impl Kv for KvMemory {
    fn get(&self, key: &str) -> Result<Option<KvRecord>, StateError> {
        Ok(self.records.lock().unwrap().1.get(key).cloned())
    }
    fn put(
        &self,
        key: &str,
        expected: Option<&str>,
        value: &[u8],
        revision: Option<u64>,
    ) -> Result<bool, StateError> {
        if self.fail_key.lock().unwrap().as_deref() == Some(key) {
            return Err(StateError("injected boundary write failure".into()));
        }
        let mut inner = self.records.lock().unwrap();
        if inner.1.get(key).map(|r| r.generation.as_str()) != expected {
            return Ok(false);
        }
        inner.0 += 1;
        let generation = inner.0.to_string();
        inner.1.insert(
            key.into(),
            KvRecord {
                generation,
                value: value.to_vec(),
                revision,
            },
        );
        Ok(true)
    }
}
fn user() -> User {
    User {
        id: "one".into(),
        display_name: "Owner".into(),
        role: Role::Admin,
        credentials: vec![Credential {
            id: "passkey".into(),
            public_key: vec![],
            sign_count: 0,
            revoked: false,
        }],
        revoked: false,
        created_by: "setup".into(),
        created_at: 1,
    }
}
fn initial(kv: KvMemory) -> ProjectedStore<KvMemory> {
    let adapter = ProjectedStore::new(kv, 100);
    let mut state = AuthState::default();
    state.users.insert("one".into(), user());
    assert!(adapter.compare_and_swap(None, &state).unwrap());
    adapter
}

#[test]
fn committed_state_materializes_named_records_and_reads_user_changes() {
    let kv = KvMemory::default();
    let store = initial(kv.clone());
    assert_eq!(kv.get("user:one").unwrap().unwrap().revision, Some(1));
    let mut edited = user();
    edited.display_name = "CLI update".into();
    let generation = kv.get("user:one").unwrap().unwrap().generation;
    assert!(kv
        .put(
            "user:one",
            Some(&generation),
            &serde_json::to_vec(&edited).unwrap(),
            None
        )
        .unwrap());
    assert_eq!(
        store.load_user("one").unwrap().unwrap().display_name,
        "CLI update"
    );
    let fresh = ProjectedStore::new(kv.clone(), 101);
    assert_eq!(
        fresh.load().unwrap().state.users["one"].display_name,
        "CLI update"
    );
    assert!(kv.get("audit:00000000000000000000").unwrap().is_some());
}

#[test]
fn usage_settings_and_audit_survive_a_fresh_request_after_projection_repair() {
    use fastly_log_analytics::auth::{AuthConfig, AuthService, Error};
    use serde_json::json;

    let kv = KvMemory::default();
    let _initial = initial(kv.clone());
    let cfg = AuthConfig::new("https://logs.example.com", vec![42; 32], None).unwrap();
    *kv.fail_key.lock().unwrap() = Some("audit:00000000000000000000".into());
    let writer = ProjectedStore::new(kv.clone(), 101);
    assert_eq!(
        AuthService::new(&writer, &cfg).set_usage_logging(
            "one",
            json!({"enabled":true,"retention_days":60}),
            "192.0.2.1",
            101,
        ),
        Err(Error::Storage)
    );
    *kv.fail_key.lock().unwrap() = None;
    let fresh = ProjectedStore::new(kv.clone(), 102);
    let saved = AuthService::new(&fresh, &cfg).usage_logging("one").unwrap();
    assert!(saved.enabled);
    assert_eq!(saved.retention_days, 60.0);
    let archived = kv.get("audit:00000000000000000000").unwrap().unwrap();
    let audit: serde_json::Value = serde_json::from_slice(&archived.value).unwrap();
    assert_eq!(audit["action"], "usage_logging_updated");
    assert_eq!(audit["target"], "usage_logging");
}

#[test]
fn completed_projection_does_not_write_during_unchanged_authorization() {
    let kv = KvMemory::default();
    let store = initial(kv.clone());
    let before = kv.records.lock().unwrap().0;
    for _ in 0..3 {
        store.load().unwrap();
        store.load_user("one").unwrap();
    }
    assert_eq!(kv.records.lock().unwrap().0, before);
    assert!(store.calls() <= 24);
}

#[test]
fn interrupted_projection_is_repaired_before_any_authorization_read() {
    let kv = KvMemory::default();
    *kv.fail_key.lock().unwrap() = Some("user:one".into());
    let store = ProjectedStore::new(kv.clone(), 100);
    let mut state = AuthState::default();
    state.users.insert("one".into(), user());
    assert!(store.compare_and_swap(None, &state).is_err());
    assert!(kv.get("auth:state").unwrap().is_some());
    assert!(kv.get("user:one").unwrap().is_none());
    *kv.fail_key.lock().unwrap() = None;
    let next = ProjectedStore::new(kv.clone(), 101);
    assert_eq!(next.load_user("one").unwrap().unwrap().role, Role::Admin);
    assert!(kv
        .get("auth:projection:00000000000000000001")
        .unwrap()
        .is_some());
}

#[test]
fn old_writer_cannot_overwrite_a_newer_materialized_revision() {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Barrier,
    };
    #[derive(Clone)]
    struct Delayed {
        inner: KvMemory,
        entered: Arc<Barrier>,
        resume: Arc<Barrier>,
        armed: Arc<AtomicBool>,
    }
    impl Kv for Delayed {
        fn get(&self, key: &str) -> Result<Option<KvRecord>, StateError> {
            self.inner.get(key)
        }
        fn put(
            &self,
            key: &str,
            generation: Option<&str>,
            value: &[u8],
            revision: Option<u64>,
        ) -> Result<bool, StateError> {
            if key == "user:one" && revision == Some(2) && self.armed.swap(false, Ordering::SeqCst)
            {
                self.entered.wait();
                self.resume.wait();
            }
            self.inner.put(key, generation, value, revision)
        }
    }
    let kv = KvMemory::default();
    initial(kv.clone());
    let delayed = Delayed {
        inner: kv.clone(),
        entered: Arc::new(Barrier::new(2)),
        resume: Arc::new(Barrier::new(2)),
        armed: Arc::new(AtomicBool::new(true)),
    };
    let writer = delayed.clone();
    let old = std::thread::spawn(move || {
        let store = ProjectedStore::new(writer, 101);
        let mut current = store.load().unwrap();
        current.state.users.get_mut("one").unwrap().display_name = "old writer".into();
        store.compare_and_swap(current.generation.as_deref(), &current.state)
    });
    delayed.entered.wait();
    let newest = ProjectedStore::new(delayed.clone(), 102);
    let mut current = newest.load().unwrap();
    current.state.users.get_mut("one").unwrap().display_name = "new writer".into();
    assert!(newest
        .compare_and_swap(current.generation.as_deref(), &current.state)
        .unwrap());
    delayed.resume.wait();
    assert!(old.join().unwrap().is_err());
    let record = kv.get("user:one").unwrap().unwrap();
    assert_eq!(record.revision, Some(3));
    let user: User = serde_json::from_slice(&record.value).unwrap();
    assert_eq!(user.display_name, "new writer");
}

#[test]
fn call_limit_is_hard_and_oversized_mutations_never_commit_partially() {
    let kv = KvMemory::default();
    let store = initial(kv.clone());
    while store.load().is_ok() {}
    assert_eq!(store.calls(), 24);
    let empty = KvMemory::default();
    let store = ProjectedStore::new(empty.clone(), 100);
    let mut state = AuthState::default();
    for i in 0..20 {
        let mut user = user();
        user.id = i.to_string();
        state.users.insert(user.id.clone(), user);
    }
    assert!(store.compare_and_swap(None, &state).is_err());
    assert!(empty.get("auth:state").unwrap().is_none());
    assert!(store.calls() < 24);
}

#[test]
fn cli_role_changes_and_revocation_apply_on_the_next_authenticated_request() {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
    use fastly_log_analytics::auth::{AuthConfig, AuthService, Error};
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;
    let kv = KvMemory::default();
    let store = initial(kv.clone());
    let mut current = store.load().unwrap();
    let mut guest = user();
    guest.id = "guest".into();
    guest.role = Role::AnalystNoPii;
    current.state.users.insert("guest".into(), guest.clone());
    assert!(store
        .compare_and_swap(current.generation.as_deref(), &current.state)
        .unwrap());
    let cfg = AuthConfig::new("https://logs.example.com", vec![42; 32], None).unwrap();
    let body = URL_SAFE_NO_PAD
        .encode(br#"{"sub":"guest","role":"analyst-no-pii","policy_version":0,"exp":200}"#);
    let mut mac = Hmac::<Sha256>::new_from_slice(&cfg.session_key).unwrap();
    mac.update(body.as_bytes());
    let cookie = format!(
        "{body}.{}",
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    );
    let put_guest = |guest: &User| {
        let previous = kv.get("user:guest").unwrap().unwrap();
        assert!(kv
            .put(
                "user:guest",
                Some(&previous.generation),
                &serde_json::to_vec(guest).unwrap(),
                None
            )
            .unwrap());
    };
    guest.role = Role::Analyst;
    put_guest(&guest);
    let request = ProjectedStore::new(kv.clone(), 101);
    assert_eq!(
        AuthService::new(&request, &cfg)
            .authenticate(&cookie, 101)
            .unwrap()
            .role,
        Role::Analyst
    );
    guest.revoked = true;
    put_guest(&guest);
    let request = ProjectedStore::new(kv.clone(), 102);
    assert_eq!(
        AuthService::new(&request, &cfg)
            .authenticate(&cookie, 102)
            .unwrap_err(),
        Error::Unauthorized
    );
    assert!(kv.get("audit:00000000000000000000").unwrap().is_some());
    assert!(kv.get("audit:00000000000000000001").unwrap().is_some());
}

#[test]
fn stale_cli_projection_cannot_resurrect_a_role_overwritten_by_a_newer_commit() {
    #[derive(Clone)]
    struct Stale {
        inner: KvMemory,
        record: KvRecord,
    }
    impl Kv for Stale {
        fn get(&self, key: &str) -> Result<Option<KvRecord>, StateError> {
            if key == "user:one" {
                Ok(Some(self.record.clone()))
            } else {
                self.inner.get(key)
            }
        }
        fn put(
            &self,
            key: &str,
            generation: Option<&str>,
            value: &[u8],
            revision: Option<u64>,
        ) -> Result<bool, StateError> {
            self.inner.put(key, generation, value, revision)
        }
    }
    let kv = KvMemory::default();
    initial(kv.clone());
    let record = kv.get("user:one").unwrap().unwrap();
    let mut edited = user();
    edited.display_name = "obsolete CLI value".into();
    assert!(kv
        .put(
            "user:one",
            Some(&record.generation),
            &serde_json::to_vec(&edited).unwrap(),
            None
        )
        .unwrap());
    let old_cli = kv.get("user:one").unwrap().unwrap();
    let imported = ProjectedStore::new(kv.clone(), 101);
    imported.load_user("one").unwrap();
    let active = ProjectedStore::new(kv.clone(), 102);
    let mut current = active.load().unwrap();
    current.state.users.get_mut("one").unwrap().display_name = "current value".into();
    assert!(active
        .compare_and_swap(current.generation.as_deref(), &current.state)
        .unwrap());
    let stale = ProjectedStore::new(
        Stale {
            inner: kv.clone(),
            record: old_cli,
        },
        103,
    );
    assert!(stale.load_user("one").is_err());
    let latest = ProjectedStore::new(kv, 104);
    assert_eq!(
        latest.load().unwrap().state.users["one"].display_name,
        "current value"
    );
}

#[test]
fn cli_revocation_of_other_admin_cannot_allow_the_app_to_remove_the_last_admin() {
    use fastly_log_analytics::auth::{AuthConfig, AuthService, Error};
    let kv = KvMemory::default();
    let store = initial(kv.clone());
    let mut current = store.load().unwrap();
    let mut second = user();
    second.id = "two".into();
    current.state.users.insert("two".into(), second);
    assert!(store
        .compare_and_swap(current.generation.as_deref(), &current.state)
        .unwrap());
    let mut external = user();
    external.revoked = true;
    let previous = kv.get("user:one").unwrap().unwrap();
    assert!(kv
        .put(
            "user:one",
            Some(&previous.generation),
            &serde_json::to_vec(&external).unwrap(),
            None
        )
        .unwrap());
    let request = ProjectedStore::new(kv.clone(), 101);
    let cfg = AuthConfig::new("https://logs.example.com", vec![42; 32], None).unwrap();
    assert_eq!(
        AuthService::new(&request, &cfg).revoke_user("two", "two", "admin", 101),
        Err(Error::Forbidden)
    );
    let latest = ProjectedStore::new(kv, 102);
    assert!(!latest.load_user("two").unwrap().unwrap().revoked);
}

#[test]
fn fresh_foreground_user_and_coordinator_reads_deny_acknowledged_revocation() {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
    use fastly_log_analytics::auth::{AuthConfig, AuthService, Error};
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;
    for primary_user_changed in [true, false] {
        let kv = KvMemory::default();
        initial(kv.clone());
        let root = kv.get("auth:state").unwrap().unwrap();
        let cached_user = kv.get("user:one").unwrap().unwrap();
        if primary_user_changed {
            let mut revoked = user();
            revoked.revoked = true;
            assert!(kv
                .put(
                    "user:one",
                    Some(&cached_user.generation),
                    &serde_json::to_vec(&revoked).unwrap(),
                    None
                )
                .unwrap());
        } else {
            let mut changed: serde_json::Value = serde_json::from_slice(&root.value).unwrap();
            changed["state"]["policy_version"] = serde_json::json!(1);
            assert!(kv
                .put(
                    "auth:state",
                    Some(&root.generation),
                    &serde_json::to_vec(&changed).unwrap(),
                    Some(2)
                )
                .unwrap());
        }
        let cfg = AuthConfig::new("https://logs.example.com", vec![42; 32], None).unwrap();
        let body =
            URL_SAFE_NO_PAD.encode(br#"{"sub":"one","role":"admin","policy_version":0,"exp":200}"#);
        let mut mac = Hmac::<Sha256>::new_from_slice(&cfg.session_key).unwrap();
        mac.update(body.as_bytes());
        let cookie = format!(
            "{body}.{}",
            URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
        );
        let current = ProjectedStore::new(kv.clone(), 101);
        assert_eq!(
            AuthService::new(&current, &cfg)
                .authenticate(&cookie, 101)
                .unwrap_err(),
            if primary_user_changed {
                Error::Storage
            } else {
                Error::Unauthorized
            }
        );
        let actual: User =
            serde_json::from_slice(&kv.get("user:one").unwrap().unwrap().value).unwrap();
        assert_eq!(actual.revoked, primary_user_changed);
    }
}

#[test]
fn expired_invites_prune_without_burning_projection_kv_budget() {
    use fastly_log_analytics::auth::{AuthConfig, AuthService, Invite};

    let kv = KvMemory::default();
    let mut state = AuthState::default();
    state.users.insert("one".into(), user());
    for batch in [0..8, 8..15] {
        let store = ProjectedStore::new(kv.clone(), 100);
        let generation = store.load().unwrap().generation;
        for i in batch {
            let id = format!("expired-{i}");
            state.invites.insert(
                id.clone(),
                Invite {
                    id,
                    display_name: format!("Expired {i}"),
                    role: Role::Analyst,
                    expires_at: 200,
                    revoked: false,
                    used: false,
                    created_by: "one".into(),
                },
            );
        }
        assert!(store
            .compare_and_swap(generation.as_deref(), &state)
            .unwrap());
    }
    let store = ProjectedStore::new(kv.clone(), 100);
    let generation = store.load().unwrap().generation;
    state.invites.insert(
        "active-used".into(),
        Invite {
            id: "active-used".into(),
            display_name: "Active Used".into(),
            role: Role::Analyst,
            expires_at: 500,
            revoked: false,
            used: true,
            created_by: "one".into(),
        },
    );
    assert!(store
        .compare_and_swap(generation.as_deref(), &state)
        .unwrap());

    let cfg = AuthConfig::new("https://logs.example.com", vec![42; 32], None).unwrap();
    let later = ProjectedStore::new(kv.clone(), 300);
    let service = AuthService::new(&later, &cfg);
    assert!(service.login_options("192.0.2.1", 300, [7; 32]).is_ok());

    let after = later.load().unwrap().state;
    assert_eq!(after.invites.len(), 1);
    assert!(after.invites.contains_key("active-used"));
}

use fastly_log_analytics::auth::{AuthConfig, AuthService, Error};
use fastly_log_analytics::state::MemoryStore;

fn timed_config(code: &str, expires_at: u64) -> AuthConfig {
    AuthConfig::from_hash(
        "https://logs.example.com",
        vec![7; 32],
        Some(
            serde_json::json!({
                "sha256": fastly_log_analytics::auth::hash(code),
                "expires_at": expires_at,
            })
            .to_string(),
        ),
    )
    .unwrap()
}

#[test]
fn setup_code_and_grant_stop_at_the_code_expiration_boundary() {
    let store = MemoryStore::default();
    let cfg = timed_config("setup-code", 900);
    let service = AuthService::new(&store, &cfg);
    let grant = service
        .accept_setup("setup-code", "owner", 899, [1; 32])
        .unwrap();
    assert_eq!(service.check_setup_grant(&grant, 899), Ok(()));
    assert_eq!(
        service.accept_setup("setup-code", "owner", 900, [2; 32]),
        Err(Error::SetupClosed)
    );
    assert_eq!(
        service.check_setup_grant(&grant, 900),
        Err(Error::Unauthorized)
    );
    assert!(matches!(
        service.registration_options(Some(&grant), None, "Owner", "owner", 900, [3; 32]),
        Err(Error::Unauthorized)
    ));
}

#[test]
fn untimed_legacy_setup_codes_require_reset_without_invalidating_auth_config() {
    let store = MemoryStore::default();
    let cfg = AuthConfig::from_hash(
        "https://logs.example.com",
        vec![7; 32],
        Some(fastly_log_analytics::auth::hash("legacy-code")),
    )
    .unwrap();
    assert_eq!(
        AuthService::new(&store, &cfg).accept_setup("legacy-code", "owner", 1, [1; 32]),
        Err(Error::SetupClosed)
    );
}

#[test]
fn malformed_timed_setup_secrets_cannot_open_setup() {
    let digest = fastly_log_analytics::auth::hash("setup-code");
    for secret in [
        serde_json::json!({"sha256":digest}),
        serde_json::json!({"sha256":digest,"expires_at":null}),
        serde_json::json!({"sha256":digest,"expires_at":-1}),
        serde_json::json!({"sha256":digest,"expires_at":"900"}),
        serde_json::json!({"sha256":"not-a-digest","expires_at":900}),
        serde_json::json!({"sha256":digest,"expires_at":900,"unexpected":true}),
    ] {
        assert!(matches!(
            AuthConfig::from_hash(
                "https://logs.example.com",
                vec![7; 32],
                Some(secret.to_string())
            ),
            Err(Error::InvalidInput)
        ));
    }
}

fn config() -> AuthConfig {
    AuthConfig::new(
        "https://logs.example.com",
        vec![7; 32],
        Some(("setup-code", 900)),
    )
    .unwrap()
}

#[test]
fn global_setup_failures_burn_generation_and_reset_does_not_restore_old_grants() {
    let store = MemoryStore::default();
    let cfg = config();
    let service = AuthService::new(&store, &cfg);
    let grant = service
        .accept_setup("setup-code", "first", 1, [1; 32])
        .unwrap();
    for i in 0..20 {
        assert_eq!(
            service.accept_setup("wrong", &format!("client-{i}"), 2, [2; 32]),
            Err(Error::Unauthorized)
        );
    }
    assert_eq!(
        service.accept_setup("setup-code", "right", 3, [3; 32]),
        Err(Error::SetupClosed)
    );
    let rotated = AuthConfig::new(
        "https://logs.example.com",
        vec![7; 32],
        Some(("new-code", 900)),
    )
    .unwrap();
    let reset = AuthService::new(&store, &rotated);
    assert_eq!(reset.check_setup_grant(&grant, 4), Err(Error::Unauthorized));
    assert!(reset.accept_setup("new-code", "right", 4, [4; 32]).is_ok());
    assert_eq!(
        service.accept_setup("setup-code", "stale-secret-pop", 5, [5; 32]),
        Err(Error::SetupClosed),
    );
    assert!(reset
        .check_setup_grant(
            &reset
                .accept_setup("new-code", "fresh-pop", 6, [6; 32])
                .unwrap(),
            7
        )
        .is_ok());
}

fn sign_test_session(
    key: &[u8],
    user_id: &str,
    role: fastly_log_analytics::auth::Role,
    exp: u64,
) -> String {
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;
    let claims = fastly_log_analytics::auth::encode(
        serde_json::json!({
            "sub": user_id,
            "role": role,
            "policy_version": 0,
            "exp": exp,
        })
        .to_string()
        .as_bytes(),
    );
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(claims.as_bytes());
    format!(
        "{claims}.{}",
        fastly_log_analytics::auth::encode(&mac.finalize().into_bytes())
    )
}

#[test]
fn sec20e_audit_entries_requires_active_admin_actor() {
    use fastly_log_analytics::auth::{webauthn::Credential, AuditEntry, AuthState, Role, User};
    use fastly_log_analytics::state::StateStore;

    let store = MemoryStore::default();
    let cfg = config();
    let mut state = AuthState::default();
    for (id, role, revoked) in [
        ("u-admin", Role::Admin, false),
        ("u-analyst", Role::Analyst, false),
        ("u-nopii", Role::AnalystNoPii, false),
        ("u-revoked-admin", Role::Admin, true),
    ] {
        state.users.insert(
            id.into(),
            User {
                id: id.into(),
                display_name: id.into(),
                role,
                credentials: vec![Credential {
                    id: format!("{id}-cred"),
                    public_key: vec![1],
                    sign_count: 0,
                    revoked: false,
                }],
                revoked,
                created_by: "test".into(),
                created_at: 10,
                last_login_at: None,
            },
        );
    }
    state.recent_audit.push(AuditEntry {
        time: 50,
        actor: "u-admin".into(),
        action: "invite_created".into(),
        target: "i-1".into(),
        ip: "192.0.2.1".into(),
    });
    assert!(store.compare_and_swap(None, &state).unwrap());

    let service = AuthService::new(&store, &cfg);
    let entries = service
        .audit_entries("u-admin")
        .expect("admin can read audit");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].action, "invite_created");

    for denied in [
        "u-analyst",
        "u-nopii",
        "u-revoked-admin",
        "unknown-user",
        "",
    ] {
        assert!(
            matches!(service.audit_entries(denied), Err(Error::Forbidden)),
            "actor={denied} must be forbidden"
        );
    }
}

#[test]
fn sec18_logout_revokes_session_server_side_without_kv_writes_on_forged_or_expired_cookies() {
    use fastly_log_analytics::auth::{webauthn::Credential, AuthState, Role, User};
    use fastly_log_analytics::state::{StateError, StateStore, VersionedState};
    use std::cell::Cell;

    struct CountingStore {
        inner: MemoryStore,
        cas_writes: Cell<u32>,
    }
    impl StateStore for CountingStore {
        fn load(&self) -> Result<VersionedState, StateError> {
            self.inner.load()
        }
        fn load_user(&self, id: &str) -> Result<Option<User>, StateError> {
            self.inner.load_user(id)
        }
        fn compare_and_swap(
            &self,
            generation: Option<&str>,
            next: &AuthState,
        ) -> Result<bool, StateError> {
            self.cas_writes.set(self.cas_writes.get() + 1);
            self.inner.compare_and_swap(generation, next)
        }
    }

    let key = vec![7u8; 32];
    let cfg = AuthConfig::new(
        "https://logs.example.com",
        key.clone(),
        Some(("setup-code", 900)),
    )
    .unwrap();
    let store = CountingStore {
        inner: MemoryStore::default(),
        cas_writes: Cell::new(0),
    };
    let mut initial = AuthState::default();
    for (id, role) in [("u-admin", Role::Admin), ("u-analyst", Role::Analyst)] {
        initial.users.insert(
            id.into(),
            User {
                id: id.into(),
                display_name: id.into(),
                role,
                credentials: vec![Credential {
                    id: format!("{id}-cred"),
                    public_key: vec![1],
                    sign_count: 0,
                    revoked: false,
                }],
                revoked: false,
                created_by: "test".into(),
                created_at: 10,
                last_login_at: None,
            },
        );
    }
    assert!(store.inner.compare_and_swap(None, &initial).unwrap());
    let service = AuthService::new(&store, &cfg);

    let expired_token = sign_test_session(&key, "u-admin", Role::Admin, 99);
    let ghost_token = sign_test_session(&key, "u-nonexistent", Role::Admin, 500);
    service.logout("forged.garbage", "192.0.2.1", 100).unwrap();
    service.logout(&expired_token, "192.0.2.1", 100).unwrap();
    service.logout(&ghost_token, "192.0.2.1", 100).unwrap();
    assert_eq!(
        store.cas_writes.get(),
        0,
        "forged/expired/unknown-user logout must do zero KV writes"
    );
    assert!(store
        .inner
        .load()
        .unwrap()
        .state
        .user_revoked_sessions
        .is_empty());

    let admin_token = sign_test_session(&key, "u-admin", Role::Admin, 500);
    let analyst_token = sign_test_session(&key, "u-analyst", Role::Analyst, 500);
    assert!(service.authenticate(&admin_token, 100).is_ok());
    assert!(service.authenticate_session(&analyst_token, 100).is_ok());

    service.logout(&admin_token, "192.0.2.1", 100).unwrap();
    service.logout(&analyst_token, "192.0.2.2", 100).unwrap();
    assert_eq!(store.cas_writes.get(), 2);

    assert_eq!(
        service.authenticate(&admin_token, 101),
        Err(Error::Unauthorized)
    );
    assert_eq!(
        service.authenticate_session(&analyst_token, 101),
        Err(Error::Unauthorized)
    );
    assert_eq!(
        service.authenticate_with_settings(&admin_token, 101),
        Err(Error::Unauthorized)
    );

    // Duplicate logout does not append a duplicate entry or write KV
    service.logout(&admin_token, "192.0.2.1", 102).unwrap();
    assert_eq!(store.cas_writes.get(), 2);
    assert_eq!(
        store.inner.load().unwrap().state.user_revoked_sessions["u-admin"].len(),
        1
    );
}

#[test]
fn sec18_per_user_logout_isolation_blocks_cross_user_eviction_self_eviction_and_resurrection() {
    use fastly_log_analytics::auth::{
        webauthn::Credential, AuthState, Role, User, MAX_USER_REVOKED_SESSIONS, SESSION_TTL,
    };
    use fastly_log_analytics::state::StateStore;

    let key = vec![7u8; 32];
    let cfg = AuthConfig::new(
        "https://logs.example.com",
        key.clone(),
        Some(("setup-code", 900)),
    )
    .unwrap();
    let store = MemoryStore::default();
    let mut state = AuthState::default();
    for (id, role) in [("u-admin", Role::Admin), ("u-nopii", Role::AnalystNoPii)] {
        state.users.insert(
            id.into(),
            User {
                id: id.into(),
                display_name: id.into(),
                role,
                credentials: vec![Credential {
                    id: format!("{id}-cred"),
                    public_key: vec![1],
                    sign_count: 0,
                    revoked: false,
                }],
                revoked: false,
                created_by: "test".into(),
                created_at: 10,
                last_login_at: None,
            },
        );
    }
    assert!(store.compare_and_swap(None, &state).unwrap());
    let service = AuthService::new(&store, &cfg);

    // Attack A: Admin logs in at t=100, logs out at t=500; 300 AnalystNoPii sessions minted at t=101..=400 log out.
    let admin_a = sign_test_session(&key, "u-admin", Role::Admin, 100 + SESSION_TTL);
    service.logout(&admin_a, "192.0.2.1", 500).unwrap();
    for t in 101..=400u64 {
        let nopii_tok = sign_test_session(&key, "u-nopii", Role::AnalystNoPii, t + SESSION_TTL);
        service.logout(&nopii_tok, "192.0.2.99", 500 + t).unwrap();
    }
    assert_eq!(
        service.authenticate(&admin_a, 950),
        Err(Error::Unauthorized),
        "300 AnalystNoPii logouts must never evict Admin's revoked session"
    );

    // Attack B: Admin minted earlier (t=105) logs out AFTER 300 newer logouts — must NOT self-evict!
    let admin_older = sign_test_session(&key, "u-admin", Role::Admin, 105 + SESSION_TTL);
    service.logout(&admin_older, "192.0.2.1", 1000).unwrap();
    assert_eq!(
        service.authenticate(&admin_older, 1001),
        Err(Error::Unauthorized),
        "older-minted Admin session logging out later must never self-evict"
    );

    // Same-user cap + eviction floor: if u-admin logs out > MAX_USER_REVOKED_SESSIONS sessions,
    // oldest revoked_at is evicted AND user_min_session_exp prevents resurrection of evicted token!
    let mut admin_logged_out = vec![admin_a, admin_older];
    for idx in 0..(MAX_USER_REVOKED_SESSIONS + 5) {
        let exp = 1100 + idx as u64 + SESSION_TTL;
        let tok = sign_test_session(&key, "u-admin", Role::Admin, exp);
        service
            .logout(&tok, "192.0.2.1", 1100 + idx as u64)
            .unwrap();
        admin_logged_out.push(tok);
    }
    let updated = store.load().unwrap().state;
    assert_eq!(
        updated.user_revoked_sessions["u-admin"].len(),
        MAX_USER_REVOKED_SESSIONS
    );
    for tok in &admin_logged_out {
        assert_eq!(
            service.authenticate(tok, 1200),
            Err(Error::Unauthorized),
            "even evicted same-user sessions must stay invalid via user_min_session_exp"
        );
    }
    // Fresh login minted after eviction floor still works
    let fresh_admin = sign_test_session(&key, "u-admin", Role::Admin, 2000 + SESSION_TTL);
    assert!(service.authenticate(&fresh_admin, 2000).is_ok());
}

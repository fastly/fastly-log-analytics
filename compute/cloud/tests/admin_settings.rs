use fastly_log_analytics::{
    auth::{AuthConfig, AuthService, AuthState, Credential, Error, Role, User},
    state::{MemoryStore, StateError, StateStore, VersionedState},
};
use serde_json::json;
use std::cell::Cell;

fn state() -> MemoryStore {
    let store = MemoryStore::default();
    let mut state = AuthState::default();
    for (id, role) in [("admin", Role::Admin), ("analyst", Role::Analyst)] {
        state.users.insert(
            id.into(),
            User {
                id: id.into(),
                display_name: id.into(),
                role,
                credentials: vec![Credential {
                    id: format!("{id}-passkey"),
                    public_key: vec![],
                    sign_count: 0,
                    revoked: false,
                }],
                revoked: false,
                created_by: "setup".into(),
                created_at: 1,
            },
        );
    }
    assert!(store.compare_and_swap(None, &state).unwrap());
    store
}

fn config() -> AuthConfig {
    AuthConfig::new("https://logs.example.com", vec![42; 32], None).unwrap()
}

#[test]
fn usage_settings_merge_persist_and_audit_without_changing_users() {
    let store = state();
    let config = config();
    let auth = AuthService::new(&store, &config);
    let users = store.load().unwrap().state.users;
    let original = auth.usage_logging("admin").unwrap();
    assert!(original.enabled);
    assert_eq!(original.retention_days, 30.0);
    let updated = auth
        .set_usage_logging(
            "admin",
            json!({"enabled":false,"class_a_rate_per_1k":0.007}),
            "192.0.2.1",
            100,
        )
        .unwrap();
    assert!(!updated.enabled);
    assert_eq!(updated.class_a_rate_per_1k, 0.007);
    assert_eq!(
        updated.storage_rate_per_gb_month,
        original.storage_rate_per_gb_month
    );
    let saved = store.load().unwrap().state;
    assert_eq!(saved.users, users);
    assert_eq!(saved.audit.last().unwrap().action, "usage_logging_updated");
    let restored = MemoryStore::default();
    let decoded = serde_json::from_slice(&serde_json::to_vec(&saved).unwrap()).unwrap();
    assert!(restored.compare_and_swap(None, &decoded).unwrap());
    assert_eq!(
        AuthService::new(&restored, &config)
            .usage_logging("admin")
            .unwrap(),
        updated
    );
}

#[test]
fn settings_authorization_and_invalid_patches_never_mutate_state() {
    let store = state();
    let config = config();
    let auth = AuthService::new(&store, &config);
    let before = store.load().unwrap().state;
    assert_eq!(auth.usage_logging("analyst"), Err(Error::Forbidden));
    assert_eq!(
        auth.set_usage_logging("analyst", json!({"enabled":true}), "192.0.2.1", 100),
        Err(Error::Forbidden)
    );
    for invalid in [
        json!({"enabled":"true"}),
        json!({"enabled":null}),
        json!({"retention_days":0}),
        json!({"class_b_rate_per_10k":-1}),
        json!({"cdn_egress_rate_per_gb":"0.12"}),
        json!({"storage_rate_per_gb_month":null}),
        json!({"min_billed_days":0}),
        json!({"track_duckdb_httpfs":true}),
        json!([]),
    ] {
        assert_eq!(
            auth.set_usage_logging("admin", invalid.clone(), "192.0.2.1", 100),
            Err(Error::InvalidInput),
            "{invalid}"
        );
    }
    assert!(store.load().unwrap().state == before);
}

#[test]
fn missing_legacy_settings_receive_defaults_but_corrupt_settings_are_rejected() {
    let store = state();
    let mut legacy = serde_json::to_value(store.load().unwrap().state).unwrap();
    legacy.as_object_mut().unwrap().remove("usage_logging");
    let decoded: AuthState = serde_json::from_value(legacy.clone()).unwrap();
    assert_eq!(decoded.usage_logging.retention_days, 30.0);
    legacy["usage_logging"] = json!({"enabled":"invalid"});
    assert!(serde_json::from_value::<AuthState>(legacy).is_err());
    let mut invalid = store.load().unwrap();
    invalid.state.usage_logging.retention_days = -1.0;
    assert!(store
        .compare_and_swap(invalid.generation.as_deref(), &invalid.state)
        .unwrap());
    assert_eq!(
        AuthService::new(&store, &config()).usage_logging("admin"),
        Err(Error::Storage)
    );
}

#[test]
fn conflicting_partial_updates_preserve_the_other_admins_changes() {
    struct Concurrent {
        store: MemoryStore,
        conflict: Cell<bool>,
    }
    impl StateStore for Concurrent {
        fn load(&self) -> Result<VersionedState, StateError> {
            self.store.load()
        }
        fn load_user(&self, id: &str) -> Result<Option<User>, StateError> {
            self.store.load_user(id)
        }
        fn compare_and_swap(
            &self,
            expected: Option<&str>,
            next: &AuthState,
        ) -> Result<bool, StateError> {
            if self.conflict.replace(false) {
                let mut other = self.store.load()?;
                other.state.usage_logging.cdn_egress_rate_per_gb = 0.21;
                assert!(self
                    .store
                    .compare_and_swap(other.generation.as_deref(), &other.state)?);
                return Ok(false);
            }
            self.store.compare_and_swap(expected, next)
        }
    }
    let store = Concurrent {
        store: state(),
        conflict: Cell::new(true),
    };
    let config = config();
    let result = AuthService::new(&store, &config)
        .set_usage_logging("admin", json!({"retention_days":60}), "192.0.2.1", 100)
        .unwrap();
    assert_eq!(result.retention_days, 60.0);
    assert_eq!(result.cdn_egress_rate_per_gb, 0.21);
    assert_eq!(store.load().unwrap().state.audit.len(), 1);
}

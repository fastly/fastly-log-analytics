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

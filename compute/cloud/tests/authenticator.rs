use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use ciborium::value::Value;
use fastly_log_analytics::auth::webauthn::BrowserResponse;
use fastly_log_analytics::auth::{
    webauthn, AuthConfig, AuthService, BrowserCredential, Error, Role, SignedIn,
};
use fastly_log_analytics::state::{MemoryStore, StateStore};
use p256::ecdsa::signature::Signer;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::{Arc, Barrier};

const ORIGIN: &str = "https://logs.example.com";
const RP: &str = "logs.example.com";

fn b64(value: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(value)
}
fn cbor(value: &Value) -> Vec<u8> {
    let mut bytes = Vec::new();
    ciborium::into_writer(value, &mut bytes).unwrap();
    bytes
}
fn integer(value: i64) -> Value {
    Value::Integer(value.into())
}

enum Authenticator {
    Es(p256::ecdsa::SigningKey),
    Ed(ed25519_dalek::SigningKey),
}
impl Authenticator {
    fn new(ed: bool, seed: u8) -> Self {
        if ed {
            Self::Ed(ed25519_dalek::SigningKey::from_bytes(&[seed; 32]))
        } else {
            Self::Es(p256::ecdsa::SigningKey::from_slice(&[seed; 32]).unwrap())
        }
    }
    fn id(&self) -> String {
        b64(&Sha256::digest(self.key()))
    }
    fn key(&self) -> Vec<u8> {
        let fields = match self {
            Self::Es(key) => {
                let point = key.verifying_key().to_sec1_point(false);
                vec![
                    (integer(1), integer(2)),
                    (integer(3), integer(-7)),
                    (integer(-1), integer(1)),
                    (integer(-2), Value::Bytes(point.x().unwrap().to_vec())),
                    (integer(-3), Value::Bytes(point.y().unwrap().to_vec())),
                ]
            }
            Self::Ed(key) => vec![
                (integer(1), integer(1)),
                (integer(3), integer(-8)),
                (integer(-1), integer(6)),
                (
                    integer(-2),
                    Value::Bytes(key.verifying_key().to_bytes().to_vec()),
                ),
            ],
        };
        cbor(&Value::Map(fields))
    }
    fn sign(&self, message: &[u8]) -> Vec<u8> {
        match self {
            Self::Es(key) => {
                let sig: p256::ecdsa::Signature = key.sign(message);
                sig.to_der().as_bytes().to_vec()
            }
            Self::Ed(key) => key.sign(message).to_bytes().to_vec(),
        }
    }
    fn client(challenge: &str, origin: &str, kind: &str) -> Vec<u8> {
        serde_json::to_vec(
            &json!({"type":kind,"challenge":challenge,"origin":origin,"crossOrigin":false}),
        )
        .unwrap()
    }
    fn auth(rp: &str, count: u32, registration: bool) -> Vec<u8> {
        let mut bytes = Sha256::digest(rp.as_bytes()).to_vec();
        bytes.push(if registration { 0x45 } else { 0x05 });
        bytes.extend(count.to_be_bytes());
        bytes
    }
    fn registration(
        &self,
        challenge: &str,
        origin: &str,
        rp: &str,
        packed: bool,
    ) -> BrowserCredential {
        let client = Self::client(challenge, origin, "webauthn.create");
        let mut auth = Self::auth(rp, 0, true);
        auth.extend([0; 16]);
        let id = URL_SAFE_NO_PAD.decode(self.id()).unwrap();
        auth.extend((id.len() as u16).to_be_bytes());
        auth.extend(id);
        auth.extend(self.key());
        let stmt = if packed {
            let mut message = auth.clone();
            message.extend(Sha256::digest(&client));
            Value::Map(vec![
                (
                    Value::Text("alg".into()),
                    integer(if matches!(self, Self::Ed(_)) { -8 } else { -7 }),
                ),
                (Value::Text("sig".into()), Value::Bytes(self.sign(&message))),
            ])
        } else {
            Value::Map(vec![])
        };
        let attestation = cbor(&Value::Map(vec![
            (
                Value::Text("fmt".into()),
                Value::Text(if packed { "packed" } else { "none" }.into()),
            ),
            (Value::Text("authData".into()), Value::Bytes(auth)),
            (Value::Text("attStmt".into()), stmt),
        ]));
        BrowserCredential {
            id: self.id(),
            raw_id: self.id(),
            kind: "public-key".into(),
            response: BrowserResponse {
                client_data_json: b64(&client),
                attestation_object: Some(b64(&attestation)),
                authenticator_data: None,
                signature: None,
                user_handle: None,
            },
        }
    }
    fn assertion(&self, challenge: &str, count: u32) -> BrowserCredential {
        let client = Self::client(challenge, ORIGIN, "webauthn.get");
        let auth = Self::auth(RP, count, false);
        let mut message = auth.clone();
        message.extend(Sha256::digest(&client));
        BrowserCredential {
            id: self.id(),
            raw_id: self.id(),
            kind: "public-key".into(),
            response: BrowserResponse {
                client_data_json: b64(&client),
                attestation_object: None,
                authenticator_data: Some(b64(&auth)),
                signature: Some(b64(&self.sign(&message))),
                user_handle: None,
            },
        }
    }
}
fn config() -> AuthConfig {
    AuthConfig::new(ORIGIN, vec![42; 32], Some(("owner-code", 900))).unwrap()
}
fn signup(store: &MemoryStore, config: &AuthConfig, auth: &Authenticator, seed: u8) -> SignedIn {
    let service = AuthService::new(store, config);
    let grant = service
        .accept_setup("owner-code", "owner", 100, [seed; 32])
        .unwrap();
    let options = service
        .registration_options(Some(&grant), None, "Owner", "owner", 101, [seed + 1; 32])
        .unwrap();
    let credential = auth.registration(
        options.public_key["challenge"].as_str().unwrap(),
        ORIGIN,
        RP,
        true,
    );
    service
        .finish_registration(&options.challenge_id, &credential, "owner", 102)
        .unwrap()
}

#[test]
fn both_software_authenticators_register_and_assert_with_real_signatures() {
    for ed in [false, true] {
        let auth = Authenticator::new(ed, 3);
        for packed in [false, true] {
            let credential = webauthn::register(
                &auth.registration("nonce", ORIGIN, RP, packed),
                "nonce",
                ORIGIN,
                RP,
            )
            .unwrap();
            assert_eq!(
                webauthn::assert(
                    &auth.assertion("nonce", 1),
                    &credential,
                    "nonce",
                    ORIGIN,
                    RP
                ),
                Ok(1)
            );
        }
    }
}

#[test]
fn registration_rejects_origin_rp_challenge_flags_and_packed_signature_corruption() {
    for ed in [false, true] {
        let auth = Authenticator::new(ed, 4);
        for (origin, rp, challenge) in [
            ("https://evil.example", RP, "nonce"),
            (ORIGIN, "evil.example", "nonce"),
            (ORIGIN, RP, "wrong"),
        ] {
            assert_eq!(
                webauthn::register(
                    &auth.registration(challenge, origin, rp, true),
                    "nonce",
                    ORIGIN,
                    RP
                )
                .unwrap_err(),
                Error::InvalidCredential
            );
        }
        let mut input = auth.registration("nonce", ORIGIN, RP, true);
        let bytes = URL_SAFE_NO_PAD
            .decode(input.response.attestation_object.take().unwrap())
            .unwrap();
        let mut value: Value = ciborium::from_reader(bytes.as_slice()).unwrap();
        let entries = value.as_map_mut().unwrap();
        let stmt = &mut entries
            .iter_mut()
            .find(|(k, _)| k.as_text() == Some("attStmt"))
            .unwrap()
            .1;
        let sig = &mut stmt
            .as_map_mut()
            .unwrap()
            .iter_mut()
            .find(|(k, _)| k.as_text() == Some("sig"))
            .unwrap()
            .1;
        sig.as_bytes_mut().unwrap()[0] ^= 1;
        input.response.attestation_object = Some(b64(&cbor(&value)));
        assert_eq!(
            webauthn::register(&input, "nonce", ORIGIN, RP).unwrap_err(),
            Error::InvalidCredential
        );
    }
}

#[test]
fn assertions_reject_wrong_origin_rp_challenge_signature_counter_and_presence() {
    for ed in [false, true] {
        let auth = Authenticator::new(ed, 5);
        let mut stored = webauthn::register(
            &auth.registration("nonce", ORIGIN, RP, false),
            "nonce",
            ORIGIN,
            RP,
        )
        .unwrap();
        let valid = auth.assertion("nonce", 1);
        assert_eq!(
            webauthn::assert(&valid, &stored, "other", ORIGIN, RP),
            Err(Error::InvalidCredential)
        );
        assert_eq!(
            webauthn::assert(&valid, &stored, "nonce", "https://evil.example", RP),
            Err(Error::InvalidCredential)
        );
        assert_eq!(
            webauthn::assert(&valid, &stored, "nonce", ORIGIN, "evil.example"),
            Err(Error::InvalidCredential)
        );
        let mut corrupt = valid.clone();
        let mut signature = URL_SAFE_NO_PAD
            .decode(corrupt.response.signature.take().unwrap())
            .unwrap();
        let middle = signature.len() / 2;
        signature[middle] ^= 1;
        corrupt.response.signature = Some(b64(&signature));
        assert_eq!(
            webauthn::assert(&corrupt, &stored, "nonce", ORIGIN, RP),
            Err(Error::InvalidCredential)
        );
        let mut missing_uv = valid.clone();
        let mut auth_data = URL_SAFE_NO_PAD
            .decode(missing_uv.response.authenticator_data.take().unwrap())
            .unwrap();
        auth_data[32] = 1;
        missing_uv.response.authenticator_data = Some(b64(&auth_data));
        assert_eq!(
            webauthn::assert(&missing_uv, &stored, "nonce", ORIGIN, RP),
            Err(Error::InvalidCredential)
        );
        stored.sign_count = 1;
        assert_eq!(
            webauthn::assert(&valid, &stored, "nonce", ORIGIN, RP),
            Err(Error::CounterRegression)
        );
        assert_eq!(
            webauthn::assert(&auth.assertion("nonce", 0), &stored, "nonce", ORIGIN, RP),
            Err(Error::CounterRegression)
        );
    }
}

#[test]
fn setup_registration_cannot_finish_after_the_code_expires() {
    let store = MemoryStore::default();
    let cfg = AuthConfig::from_hash(
        ORIGIN,
        vec![42; 32],
        Some(
            serde_json::json!({
                "sha256": fastly_log_analytics::auth::hash("owner-code"),
                "expires_at": 900,
            })
            .to_string(),
        ),
    )
    .unwrap();
    let service = AuthService::new(&store, &cfg);
    let grant = service
        .accept_setup("owner-code", "owner", 898, [1; 32])
        .unwrap();
    let options = service
        .registration_options(Some(&grant), None, "Owner", "owner", 899, [2; 32])
        .unwrap();
    let credential = Authenticator::new(false, 6).registration(
        options.public_key["challenge"].as_str().unwrap(),
        ORIGIN,
        RP,
        true,
    );
    assert!(matches!(
        service.finish_registration(&options.challenge_id, &credential, "owner", 900),
        Err(Error::Unauthorized)
    ));
    assert!(store.load().unwrap().state.users.is_empty());
}

#[test]
fn expired_setup_code_does_not_disable_existing_passkeys_or_invites() {
    let store = MemoryStore::default();
    let cfg = config();
    let auth = Authenticator::new(false, 6);
    let signed = signup(&store, &cfg, &auth, 1);
    let service = AuthService::new(&store, &cfg);
    let (_, invite) = service
        .create_invite(
            &signed.user.id,
            "Analyst",
            Role::Analyst,
            "owner",
            899,
            [3; 32],
        )
        .unwrap();
    assert_eq!(
        service.authenticate(&signed.cookie, 901).unwrap().id,
        signed.user.id
    );
    let legacy = AuthConfig::from_hash(ORIGIN, vec![42; 32], cfg.setup_hash.clone()).unwrap();
    assert_eq!(
        AuthService::new(&store, &legacy)
            .authenticate(&signed.cookie, 901)
            .unwrap()
            .id,
        signed.user.id
    );
    let login = service.login_options("owner", 901, [4; 32]).unwrap();
    let assertion = auth.assertion(login.public_key["challenge"].as_str().unwrap(), 1);
    assert_eq!(
        service
            .finish_login(&login.challenge_id, &assertion, "owner", 902)
            .unwrap()
            .user
            .id,
        signed.user.id
    );
    let options = service
        .registration_options(None, Some(&invite), "Analyst", "analyst", 901, [5; 32])
        .unwrap();
    let credential = Authenticator::new(true, 7).registration(
        options.public_key["challenge"].as_str().unwrap(),
        ORIGIN,
        RP,
        true,
    );
    assert_eq!(
        service
            .finish_registration(&options.challenge_id, &credential, "analyst", 902)
            .unwrap()
            .user
            .role,
        Role::Analyst
    );
}

#[test]
fn setup_finishes_once_and_new_secret_generation_opens_recovery() {
    let store = MemoryStore::default();
    let cfg = config();
    let auth = Authenticator::new(false, 6);
    let signed = signup(&store, &cfg, &auth, 1);
    let service = AuthService::new(&store, &cfg);
    assert_eq!(service.setup_complete(), Ok(true));
    assert_eq!(
        service.accept_setup("owner-code", "x", 103, [7; 32]),
        Err(Error::SetupClosed)
    );
    assert_eq!(
        service.revoke_user(&signed.user.id, &signed.user.id, "x", 103),
        Err(Error::Forbidden)
    );
    assert_eq!(
        service.set_role(&signed.user.id, &signed.user.id, Role::Analyst, "x", 103),
        Err(Error::Forbidden)
    );
    let reset = AuthConfig::new(ORIGIN, vec![42; 32], Some(("reset-code", 1000))).unwrap();
    let recovery = AuthService::new(&store, &reset);
    assert_eq!(recovery.setup_complete(), Ok(false));
    assert!(recovery
        .accept_setup("reset-code", "owner", 104, [8; 32])
        .is_ok());
}

#[test]
fn challenge_is_single_use_including_failed_and_expired_assertions() {
    let store = MemoryStore::default();
    let cfg = config();
    let auth = Authenticator::new(true, 7);
    signup(&store, &cfg, &auth, 1);
    let service = AuthService::new(&store, &cfg);
    let options = service.login_options("x", 110, [10; 32]).unwrap();
    let response = auth.assertion(options.public_key["challenge"].as_str().unwrap(), 1);
    assert!(service
        .finish_login(&options.challenge_id, &response, "x", 111)
        .is_ok());
    assert_eq!(
        service
            .finish_login(&options.challenge_id, &response, "x", 112)
            .unwrap_err(),
        Error::Unauthorized
    );
    let options = service.login_options("x", 120, [11; 32]).unwrap();
    let wrong = auth.assertion("wrong", 2);
    assert_eq!(
        service
            .finish_login(&options.challenge_id, &wrong, "x", 121)
            .unwrap_err(),
        Error::InvalidCredential
    );
    let valid = auth.assertion(options.public_key["challenge"].as_str().unwrap(), 2);
    assert_eq!(
        service
            .finish_login(&options.challenge_id, &valid, "x", 122)
            .unwrap_err(),
        Error::Unauthorized
    );
    let options = service.login_options("x", 130, [12; 32]).unwrap();
    let response = auth.assertion(options.public_key["challenge"].as_str().unwrap(), 2);
    assert_eq!(
        service
            .finish_login(&options.challenge_id, &response, "x", 430)
            .unwrap_err(),
        Error::ChallengeExpired
    );
}

#[test]
fn invite_role_expiry_replay_revocation_and_live_session_authorization() {
    let store = MemoryStore::default();
    let cfg = config();
    let owner_auth = Authenticator::new(false, 8);
    let owner = signup(&store, &cfg, &owner_auth, 1);
    let service = AuthService::new(&store, &cfg);
    let (invite, token) = service
        .create_invite(
            &owner.user.id,
            "Masked",
            Role::AnalystNoPii,
            "x",
            110,
            [20; 32],
        )
        .unwrap();
    let guest_auth = Authenticator::new(true, 9);
    let options = service
        .registration_options(None, Some(&token), "Ignored", "guest", 111, [21; 32])
        .unwrap();
    let response = guest_auth.registration(
        options.public_key["challenge"].as_str().unwrap(),
        ORIGIN,
        RP,
        false,
    );
    let guest = service
        .finish_registration(&options.challenge_id, &response, "guest", 112)
        .unwrap();
    assert_eq!(guest.user.role, Role::AnalystNoPii);
    assert_eq!(guest.user.display_name, "Masked");
    assert_eq!(service.users(&guest.user.id).unwrap_err(), Error::Forbidden);
    assert_eq!(
        service
            .registration_options(None, Some(&token), "Replay", "x", 113, [22; 32])
            .unwrap_err(),
        Error::Unauthorized
    );
    service
        .set_role(&owner.user.id, &guest.user.id, Role::Analyst, "x", 114)
        .unwrap();
    assert_eq!(
        service.authenticate(&guest.cookie, 115).unwrap().role,
        Role::Analyst
    );
    assert_eq!(
        service.authenticate("", 115).unwrap_err(),
        Error::Unauthorized
    );
    assert_eq!(
        service
            .authenticate(&(guest.cookie.clone() + "x"), 115)
            .unwrap_err(),
        Error::Unauthorized
    );
    assert_eq!(
        service
            .authenticate(&guest.cookie, 112 + 12 * 3600)
            .unwrap_err(),
        Error::Unauthorized
    );
    service
        .revoke_user(&owner.user.id, &guest.user.id, "x", 116)
        .unwrap();
    assert_eq!(
        service.authenticate(&guest.cookie, 117).unwrap_err(),
        Error::Unauthorized
    );
    let (revoked, token) = service
        .create_invite(&owner.user.id, "Nope", Role::Admin, "x", 118, [23; 32])
        .unwrap();
    service
        .revoke_invite(&owner.user.id, &revoked.id, "x", 119)
        .unwrap();
    assert_eq!(
        service
            .registration_options(None, Some(&token), "Nope", "x", 120, [24; 32])
            .unwrap_err(),
        Error::Unauthorized
    );
    let (_, expired) = service
        .create_invite(&owner.user.id, "Expired", Role::Admin, "x", 118, [25; 32])
        .unwrap();
    assert_eq!(
        service
            .registration_options(
                None,
                Some(&expired),
                "Expired",
                "x",
                118 + 7 * 86400,
                [26; 32]
            )
            .unwrap_err(),
        Error::Unauthorized
    );
    let state = store.load().unwrap().state;
    assert!(state.invites.get(&invite.id).unwrap().used);
    assert!(!state.invites.contains_key(&revoked.id));
    assert!(!service
        .invites(&owner.user.id)
        .unwrap()
        .iter()
        .any(|entry| entry.id == revoked.id));
    let audit = serde_json::to_string(&state.audit).unwrap();
    assert!(!audit.contains(&token));
    assert!(!audit.contains("owner-code"));
    for action in [
        "invite_created",
        "invite_revoked",
        "role_changed",
        "user_revoked",
        "user_registered",
    ] {
        assert!(
            state.audit.iter().any(|entry| entry.action == action),
            "{action}"
        );
    }
}

#[test]
fn rate_limits_expire_and_challenge_counter_is_separate_from_failures() {
    let store = MemoryStore::default();
    let cfg = config();
    let service = AuthService::new(&store, &cfg);
    for _ in 0..10 {
        service.check_rate_limit("x", false, 100).unwrap();
        service
            .record_failure("x", "login", "bad_signature", 100)
            .unwrap();
    }
    assert_eq!(
        service.check_rate_limit("x", false, 101),
        Err(Error::RateLimited { retry_after: 899 })
    );
    assert_eq!(
        service.check_rate_limit("x", false, 999),
        Err(Error::RateLimited { retry_after: 1 })
    );
    assert_eq!(service.check_rate_limit("x", false, 1000), Ok(()));
    for _ in 0..30 {
        service.check_rate_limit("other", true, 1000).unwrap();
    }
    assert_eq!(
        service.check_rate_limit("other", true, 1000),
        Err(Error::RateLimited { retry_after: 20 })
    );
    assert_eq!(service.check_rate_limit("other", true, 1020), Ok(()));
}

#[test]
fn simultaneous_global_setup_failures_and_grants_are_generation_cas_safe() {
    let store = Arc::new(MemoryStore::default());
    let cfg = Arc::new(config());
    let barrier = Arc::new(Barrier::new(20));
    let results = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..20)
            .map(|id| {
                let store = store.clone();
                let cfg = cfg.clone();
                let barrier = barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    AuthService::new(&*store, &cfg).accept_setup(
                        "wrong",
                        &id.to_string(),
                        100,
                        [id; 32],
                    )
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert!(results
        .iter()
        .all(|r| matches!(r, Err(Error::Unauthorized | Error::Conflict))));
    let failures = results
        .iter()
        .filter(|r| **r == Err(Error::Unauthorized))
        .count() as u32;
    assert_eq!(store.load().unwrap().state.setup.failures, failures);
    for _ in failures..20 {
        assert_eq!(
            AuthService::new(&*store, &cfg).accept_setup("wrong", "serial", 100, [90; 32]),
            Err(Error::Unauthorized)
        );
    }
    assert_eq!(store.load().unwrap().state.setup.failures, 20);
    assert_eq!(
        AuthService::new(&*store, &cfg).accept_setup("owner-code", "x", 101, [40; 32]),
        Err(Error::SetupClosed)
    );
}

#[test]
fn concurrent_invite_redemption_can_create_only_one_user() {
    let store = MemoryStore::default();
    let cfg = config();
    let owner = signup(&store, &cfg, &Authenticator::new(false, 10), 1);
    let service = AuthService::new(&store, &cfg);
    let (_, token) = service
        .create_invite(
            &owner.user.id,
            "One invite",
            Role::AnalystNoPii,
            "owner",
            110,
            [40; 32],
        )
        .unwrap();
    let mut submissions = Vec::new();
    for seed in 41..49 {
        let auth = Authenticator::new(seed % 2 == 0, seed);
        let options = service
            .registration_options(None, Some(&token), "One invite", "guest", 111, [seed; 32])
            .unwrap();
        let response = auth.registration(
            options.public_key["challenge"].as_str().unwrap(),
            ORIGIN,
            RP,
            false,
        );
        submissions.push((options.challenge_id, response));
    }
    let barrier = Barrier::new(submissions.len());
    let results = std::thread::scope(|scope| {
        let handles: Vec<_> = submissions
            .into_iter()
            .map(|(id, response)| {
                let barrier = &barrier;
                let service = &service;
                scope.spawn(move || {
                    barrier.wait();
                    service.finish_registration(&id, &response, "guest", 112)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert!(results
        .iter()
        .filter_map(|r| r.as_ref().err())
        .all(|e| matches!(e, Error::Unauthorized | Error::Conflict)));
    assert_eq!(service.users(&owner.user.id).unwrap().len(), 2);
}

#[test]
fn concurrent_counter_replay_is_rejected_after_generation_conflict() {
    let store = MemoryStore::default();
    let cfg = config();
    let auth = Authenticator::new(true, 12);
    signup(&store, &cfg, &auth, 1);
    let service = AuthService::new(&store, &cfg);
    let mut submissions = Vec::new();
    for seed in [50, 51] {
        let options = service.login_options("guest", 110, [seed; 32]).unwrap();
        let response = auth.assertion(options.public_key["challenge"].as_str().unwrap(), 1);
        submissions.push((options.challenge_id, response));
    }
    let barrier = Barrier::new(2);
    let results = std::thread::scope(|scope| {
        let handles: Vec<_> = submissions
            .into_iter()
            .map(|(id, response)| {
                let barrier = &barrier;
                let service = &service;
                scope.spawn(move || {
                    barrier.wait();
                    service.finish_login(&id, &response, "guest", 111)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results.iter().filter_map(|r| r.as_ref().err()).next(),
        Some(&Error::CounterRegression)
    );
}

#[test]
fn inconsistent_per_user_materialization_never_grants_stale_permissions() {
    use fastly_log_analytics::{
        auth::{AuthState, User},
        state::{StateError, VersionedState},
    };
    struct Stale {
        inner: MemoryStore,
        user: User,
    }

    impl StateStore for Stale {
        fn load(&self) -> Result<VersionedState, StateError> {
            self.inner.load()
        }
        fn load_user(&self, _: &str) -> Result<Option<User>, StateError> {
            Ok(Some(self.user.clone()))
        }
        fn compare_and_swap(
            &self,
            generation: Option<&str>,
            next: &AuthState,
        ) -> Result<bool, StateError> {
            self.inner.compare_and_swap(generation, next)
        }
    }
    let store = MemoryStore::default();
    let cfg = config();
    let signed = signup(&store, &cfg, &Authenticator::new(false, 13), 1);
    let mut user = signed.user.clone();
    user.role = Role::AnalystNoPii;
    let stale = Stale { inner: store, user };
    assert_eq!(
        AuthService::new(&stale, &cfg)
            .authenticate(&signed.cookie, 103)
            .unwrap_err(),
        Error::Unauthorized
    );
}

#[test]
fn standard_browser_json_preserves_the_client_data_json_acronym() {
    let auth = Authenticator::new(false, 14);
    let registration = auth.registration("nonce", ORIGIN, RP, false);
    let browser_json = json!({
        "id":registration.id,
        "rawId":registration.raw_id,
        "type":"public-key",
        "response":{
            "clientDataJSON":registration.response.client_data_json,
            "attestationObject":registration.response.attestation_object
        }
    });
    let decoded: BrowserCredential = serde_json::from_value(browser_json).unwrap();
    assert_eq!(
        webauthn::register(&decoded, "nonce", ORIGIN, RP)
            .unwrap()
            .id,
        auth.id()
    );
    assert!(serde_json::to_value(decoded).unwrap()["response"]["clientDataJSON"].is_string());
}

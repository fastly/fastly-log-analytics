use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use fastly_log_analytics::auth::{AuthConfig, AuthService, AuthState, Credential, Role, User};
use fastly_log_analytics::state::projection::{Kv, KvRecord, ProjectedStore};
use fastly_log_analytics::state::{StateError, StateStore};
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use std::collections::BTreeMap;
use std::sync::{Arc, Barrier, Mutex};

#[derive(Clone, Default)]
struct EdgeKv {
    primary: Arc<Mutex<(u64, BTreeMap<String, KvRecord>)>>,
    cached: Option<Arc<BTreeMap<String, KvRecord>>>,
    readers: Option<Arc<Barrier>>,
}
impl Kv for EdgeKv {
    fn get(&self, key: &str) -> Result<Option<KvRecord>, StateError> {
        if key == "user:owner" {
            if let Some(readers) = &self.readers {
                readers.wait();
            }
        }
        if let Some(cached) = &self.cached {
            if key == "auth:state" || key == "user:owner" {
                return Ok(cached.get(key).cloned());
            }
        }
        Ok(self.primary.lock().unwrap().1.get(key).cloned())
    }
    fn put(
        &self,
        key: &str,
        expected: Option<&str>,
        value: &[u8],
        revision: Option<u64>,
    ) -> Result<bool, StateError> {
        let mut primary = self.primary.lock().unwrap();
        if primary.1.get(key).map(|v| v.generation.as_str()) != expected {
            return Ok(false);
        }
        primary.0 += 1;
        let generation = primary.0.to_string();
        primary.1.insert(
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

#[test]
fn concurrent_unchanged_user_authorization_does_not_invalidate_other_readers() {
    const READERS: usize = 8;
    let kv = EdgeKv::default();
    let writer = ProjectedStore::new(kv.clone(), 100);
    let mut state = AuthState::default();
    state.users.insert(
        "owner".into(),
        User {
            id: "owner".into(),
            display_name: "Owner".into(),
            role: Role::Admin,
            credentials: vec![Credential {
                id: "credential".into(),
                public_key: Vec::new(),
                sign_count: 0,
                revoked: false,
            }],
            revoked: false,
            created_by: "setup".into(),
            created_at: 1,
        },
    );
    assert!(writer.compare_and_swap(None, &state).unwrap());
    let cfg = AuthConfig::new("https://logs.example.com", vec![42; 32], None).unwrap();
    let body =
        URL_SAFE_NO_PAD.encode(br#"{"sub":"owner","role":"admin","policy_version":0,"exp":200}"#);
    let mut mac = Hmac::<Sha256>::new_from_slice(&cfg.session_key).unwrap();
    mac.update(body.as_bytes());
    let cookie = format!(
        "{body}.{}",
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    );
    let before = kv.primary.lock().unwrap().0;
    let cached = Arc::new(kv.primary.lock().unwrap().1.clone());
    let readers = Arc::new(Barrier::new(READERS));
    let outcomes = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..READERS)
            .map(|_| {
                let reader = EdgeKv {
                    primary: kv.primary.clone(),
                    cached: Some(cached.clone()),
                    readers: Some(readers.clone()),
                };
                let cfg = &cfg;
                let cookie = &cookie;
                scope.spawn(move || {
                    let store = ProjectedStore::new(reader, 101);
                    AuthService::new(&store, cfg)
                        .authenticate(cookie, 101)
                        .map(|user| user.role)
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|w| w.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(outcomes, vec![Ok(Role::Admin); READERS]);
    assert_eq!(kv.primary.lock().unwrap().0, before);
}

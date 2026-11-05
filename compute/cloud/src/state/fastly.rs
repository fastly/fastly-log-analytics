use super::projection::{Kv, KvRecord, ProjectedStore};
use super::{parse_kv_meta, StateError};
use ::fastly::kv_store::{InsertMode, KVStore, KVStoreError};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct SdkKv {
    store: Option<KVStore>,
    budget: crate::admin::BackendBudget,
    writes: RefCell<BTreeMap<String, u64>>,
}
pub type FastlyState = ProjectedStore<SdkKv>;

fn now_ms() -> u64 {
    compute_common::runtime::now_ms()
}

impl FastlyState {
    pub fn open() -> Result<Self, StateError> {
        Self::open_with_budget(crate::admin::BackendBudget::default())
    }
    pub fn open_with_budget(budget: crate::admin::BackendBudget) -> Result<Self, StateError> {
        let store =
            KVStore::open("flp_state").map_err(|_| StateError("KV store link failed".into()))?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| StateError("clock unavailable".into()))?
            .as_secs();
        Ok(Self::new(
            SdkKv {
                store,
                budget,
                writes: RefCell::new(BTreeMap::new()),
            },
            now,
        ))
    }
}
impl Kv for SdkKv {
    fn get(&self, key: &str) -> Result<Option<KvRecord>, StateError> {
        self.budget.charge().map_err(StateError)?;
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| StateError("flp_state is not linked".into()))?;
        match store.lookup(key) {
            Ok(mut response) => {
                let generation = response.current_generation().to_string();
                let (revision, written_ms) = response
                    .metadata()
                    .map(|b| parse_kv_meta(&b))
                    .unwrap_or((None, None));
                if let Some(ts) = written_ms {
                    self.writes.borrow_mut().insert(key.into(), ts);
                }
                Ok(Some(KvRecord {
                    generation,
                    revision,
                    value: response.take_body_bytes(),
                }))
            }
            Err(KVStoreError::ItemNotFound) => Ok(None),
            Err(_) => Err(StateError("KV lookup failed".into())),
        }
    }
    fn put(
        &self,
        key: &str,
        expected: Option<&str>,
        value: &[u8],
        revision: Option<u64>,
    ) -> Result<bool, StateError> {
        if let Some(last) = self.writes.borrow().get(key).copied() {
            let delay = crate::admin::kv_write_delay_ms(last, now_ms()).map_err(StateError)?;
            if delay > 0 {
                std::thread::sleep(std::time::Duration::from_millis(delay));
            }
        }
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| StateError("flp_state is not linked".into()))?;
        let parsed_gen = match expected {
            Some(generation) => Some(
                generation
                    .parse::<u64>()
                    .map_err(|_| StateError("invalid KV generation".into()))?,
            ),
            None => None,
        };
        let ttl = if key.starts_with("webauthn_challenge:") {
            serde_json::from_slice::<serde_json::Value>(value)
                .ok()
                .and_then(|v| v["expires_at"].as_u64())
                .map(|expires| {
                    let now = now_ms() / 1000;
                    std::time::Duration::from_secs(expires.saturating_sub(now).max(1))
                })
        } else {
            None
        };
        let rev_prefix = revision.map(|v| v.to_string()).unwrap_or_default();
        for attempt in 0..2 {
            if attempt > 0 {
                std::thread::sleep(std::time::Duration::from_millis(1_100));
            }
            self.budget.charge().map_err(StateError)?;
            let timestamp = now_ms();
            let meta = format!("{rev_prefix}@{timestamp}");
            let mut builder = store.build_insert().metadata(&meta);
            builder = match parsed_gen {
                Some(g) => builder.if_generation_match(g),
                None => builder.mode(InsertMode::Add),
            };
            if let Some(ttl) = ttl {
                builder = builder.time_to_live(ttl);
            }
            match builder.execute(key, value.to_vec()) {
                Ok(()) => {
                    self.writes.borrow_mut().insert(key.into(), timestamp);
                    return Ok(true);
                }
                Err(KVStoreError::ItemPreconditionFailed) => return Ok(false),
                Err(_) if attempt == 0 => continue,
                Err(_) => return Err(StateError("KV generation write failed".into())),
            }
        }
        Err(StateError("KV generation write failed".into()))
    }
}

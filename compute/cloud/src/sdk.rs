use crate::{
    admin::transport::{
        FASTLY_API_TIMEOUT, FOS_TIMEOUT, PEER_TIMEOUT, REQUEST_PROBE_TIMEOUT,
        STATE_CONFIRMATION_TIMEOUT,
    },
    checks::{self, CheckApi, CheckProbe, Environment},
    setup::SetupStores,
    shell::{CoreCacheEntry, CoreCacheError, KvPage, Runtime},
};
use base64::Engine as _;
use fastly::{ConfigStore, KVStore, Request, SecretStore};
use serde_json::{json, Value};
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    io::Read,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

const SECRET_BUNDLE_KEY: &str = "flp_bundle";
const CONFIG_BUNDLE_KEY: &str = "flp_config_bundle";
const MAX_RAW_SECRET_READS: u32 = 24;

pub struct FastlyRuntime {
    config: Option<ConfigStore>,
    secrets: SecretStore,
    kv: Option<KVStore>,
    pub(crate) budget: crate::admin::BackendBudget,
    config_cache: RefCell<BTreeMap<String, Option<String>>>,
    config_bundle_loaded: std::cell::Cell<bool>,
    secret_cache: RefCell<BTreeMap<String, Option<String>>>,
    secret_presence: RefCell<Option<BTreeSet<String>>>,
    secret_reads: std::cell::Cell<u32>,
    bundle_loaded: std::cell::Cell<bool>,
    bundle_from_edge: std::cell::Cell<bool>,
    bundle_map: RefCell<BTreeMap<String, String>>,
    kv_cache: RefCell<BTreeMap<String, Option<crate::admin::Record>>>,
    admin_writes: RefCell<BTreeMap<String, u64>>,
    usage: crate::telemetry::usage::UsageAgg,
    api_log: crate::telemetry::api::ApiLog,
    cache_ops: std::cell::Cell<u32>,
    kv_ops: std::cell::Cell<u32>,
    cache_lookups: std::cell::Cell<u32>,
    cache_replaces: std::cell::Cell<u32>,
    purges: std::cell::Cell<u32>,
    worker_sent: std::cell::Cell<u32>,
    /// Speculative CDN reads sent by [`Runtime::admin_cdn_get_many`] whose answers are not read
    /// yet, by `<service>/<key>`, with their send time and the response limit reserved.
    unread: RefCell<BTreeMap<String, (Instant, fastly::http::request::PendingRequest, usize)>>,
}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
struct KvCoordState {
    #[serde(default)]
    last_write_ms: u64,
    #[serde(default)]
    dead_generation: Option<String>,
    #[serde(default)]
    dead_since_ms: u64,
    #[serde(default)]
    last_written_value: Option<Value>,
}

impl FastlyRuntime {
    pub fn backend_budget(&self) -> crate::admin::BackendBudget {
        self.budget.clone()
    }
    pub fn open() -> Result<Self, String> {
        let secrets = SecretStore::open("flp_secrets").map_err(|_| "secret_store_unavailable")?;
        let config = match ConfigStore::try_open("flp_config") {
            Ok(store) => Some(store),
            Err(_) => {
                println!(
                    "{}",
                    json!({"event":"store.open.error","store":"Config","level":"error"})
                );
                None
            }
        };
        let kv = KVStore::open("flp_state").map_err(|_| "kv_store_unavailable")?;
        Ok(Self {
            config,
            secrets,
            kv,
            budget: crate::admin::BackendBudget::default(),
            config_cache: RefCell::new(BTreeMap::new()),
            config_bundle_loaded: Default::default(),
            secret_cache: RefCell::new(BTreeMap::new()),
            secret_presence: RefCell::new(None),
            secret_reads: Default::default(),
            bundle_loaded: Default::default(),
            bundle_from_edge: Default::default(),
            bundle_map: RefCell::new(BTreeMap::new()),
            kv_cache: RefCell::new(BTreeMap::new()),
            admin_writes: RefCell::new(BTreeMap::new()),
            usage: Default::default(),
            api_log: Default::default(),
            cache_ops: Default::default(),
            kv_ops: Default::default(),
            cache_lookups: Default::default(),
            cache_replaces: Default::default(),
            purges: Default::default(),
            worker_sent: Default::default(),
            unread: Default::default(),
        })
    }

    fn ensure_config_bundle_loaded(&self) -> Result<(), String> {
        if self.config_bundle_loaded.get() {
            return Ok(());
        }
        self.config_bundle_loaded.set(true);
        let Some(store) = &self.config else {
            return Ok(());
        };
        if let Some(raw) = store
            .try_get(CONFIG_BUNDLE_KEY)
            .map_err(|_| String::from("config_lookup_failed"))?
        {
            if let Ok(map) = serde_json::from_str::<BTreeMap<String, String>>(&raw) {
                let mut cache = self.config_cache.borrow_mut();
                for (k, v) in map {
                    cache.entry(k).or_insert(Some(v));
                }
            }
        }
        Ok(())
    }

    fn read_raw_secret(&self, name: &str) -> Result<Option<String>, String> {
        let used = self.secret_reads.get();
        if used >= MAX_RAW_SECRET_READS {
            return Err("secret_read_budget_exhausted".into());
        }
        self.secret_reads.set(used + 1);
        self.secrets
            .try_get(name)
            .map_err(|_| "secret_lookup_failed")?
            .map(|value| {
                String::from_utf8(value.plaintext().to_vec())
                    .map_err(|_| String::from("secret_encoding_invalid"))
            })
            .transpose()
    }

    fn ensure_secret_bundle_loaded(&self) -> Result<(), String> {
        if self.bundle_loaded.get() {
            return Ok(());
        }
        self.bundle_loaded.set(true);
        if let Some(raw) = self.read_raw_secret(SECRET_BUNDLE_KEY)? {
            if let Ok(map) = serde_json::from_str::<BTreeMap<String, String>>(&raw) {
                self.bundle_from_edge.set(true);
                *self.bundle_map.borrow_mut() = map;
            }
        }
        Ok(())
    }

    fn charge_cache_op(&self) -> Result<(), CoreCacheError> {
        let used = self.cache_ops.get();
        if used >= crate::shell::CACHE_OP_BUDGET {
            return Err(CoreCacheError::Failed("cache_op_budget_exhausted".into()));
        }
        self.cache_ops.set(used + 1);
        Ok(())
    }

    fn is_cacheable_kv_key(key: &str) -> bool {
        crate::shell::is_cacheable_kv_key(key)
    }

    fn is_coordinated_kv_key(key: &str) -> bool {
        !Self::is_cacheable_kv_key(key)
            && !key.starts_with("ingest:deleteq:")
            && !key.starts_with("ingest:quarantine:")
    }

    fn kv_coord_cache_key(key: &str) -> String {
        format!("flp:kvcoord:v1:{key}")
    }

    fn kv_coord_get(&self, key: &str) -> Option<KvCoordState> {
        if !Self::is_coordinated_kv_key(key)
            || self.cache_ops.get() + 8 >= crate::shell::CACHE_OP_BUDGET
        {
            return None;
        }
        let cache_key = Self::kv_coord_cache_key(key);
        let Ok(Some((body, meta))) = self.core_cache_lookup(&cache_key) else {
            return None;
        };
        if meta != b"coord" {
            return None;
        }
        serde_json::from_slice(&body).ok()
    }

    fn kv_coord_put(&self, key: &str, state: &KvCoordState) {
        if !Self::is_coordinated_kv_key(key)
            || self.cache_ops.get() + 8 >= crate::shell::CACHE_OP_BUDGET
        {
            return;
        }
        let Ok(body) = serde_json::to_vec(state) else {
            return;
        };
        let cache_key = Self::kv_coord_cache_key(key);
        let _ = self.core_cache_replace(
            &cache_key,
            &["flp-kvcoord"],
            std::time::Duration::from_secs(300),
            &|_| (body.clone(), b"coord".to_vec()),
        );
    }

    fn kv_core_cache_key(key: &str) -> String {
        format!("flp:kvcache:v1:{key}")
    }

    fn kv_core_cache_get(&self, key: &str) -> Option<Option<crate::admin::Record>> {
        if self.cache_ops.get() + 8 >= crate::shell::CACHE_OP_BUDGET {
            return None;
        }
        let cache_key = Self::kv_core_cache_key(key);
        let Ok(Some((body, meta))) = self.core_cache_lookup(&cache_key) else {
            return None;
        };
        if meta == b"miss" {
            return Some(None);
        }
        if meta != b"hit" {
            return None;
        }
        serde_json::from_slice(&body).ok().map(Some)
    }

    fn kv_core_cache_put(&self, key: &str, record: Option<&crate::admin::Record>) {
        if self.cache_ops.get() + 8 >= crate::shell::CACHE_OP_BUDGET {
            return;
        }
        let (body, meta) = match record {
            Some(record) => {
                let Ok(body) = serde_json::to_vec(record) else {
                    return;
                };
                (body, b"hit".to_vec())
            }
            None => (Vec::new(), b"miss".to_vec()),
        };
        let cache_key = Self::kv_core_cache_key(key);
        let _ = self.core_cache_replace(
            &cache_key,
            &["flp-kvcache"],
            std::time::Duration::from_secs(crate::shell::KV_READ_CACHE_TTL_S),
            &|_| (body.clone(), meta.clone()),
        );
    }

    fn kv_core_cache_invalidate(&self, key: &str) {
        self.kv_cache.borrow_mut().remove(key);
        if !Self::is_cacheable_kv_key(key)
            || self.cache_ops.get() + 8 >= crate::shell::CACHE_OP_BUDGET
        {
            return;
        }
        let cache_key = Self::kv_core_cache_key(key);
        let _ = self.core_cache_replace(
            &cache_key,
            &["flp-kvcache"],
            std::time::Duration::from_secs(5),
            &|_| (Vec::new(), b"gone".to_vec()),
        );
    }

    fn required(&self, key: &str) -> Result<String, String> {
        self.config(key)?
            .filter(|value| !value.is_empty())
            .ok_or_else(|| format!("missing_config:{key}"))
    }

    /// The clock's last tick: the IAD schedule record's effective seen timestamp on the shield,
    /// else the durable last run, and whether it came from the record.
    fn read_tick(&self) -> (Option<u64>, bool) {
        if let Some(seen) = crate::admin::schedule::check_seen(self) {
            return (Some(seen), true);
        }
        let durable = self
            .admin_get("cron:state")
            .ok()
            .flatten()
            .and_then(|record| record.value["last_tick"].as_u64());
        (durable, false)
    }

    fn raw_admin_api(
        &self,
        input: crate::admin::ApiRequest<'_>,
    ) -> Result<crate::admin::ApiResponse, String> {
        let started = Instant::now();
        let timeout = input.timeout.min(FASTLY_API_TIMEOUT);
        let mut stage = "budget";
        let mut status = None;
        let result: Result<_, String> = (|| {
            self.budget.charge()?;
            if input.method == "POST" && input.path.contains("/purge/") {
                self.purges.set(self.purges.get().saturating_add(1));
            }
            stage = "request";
            if !input.path.starts_with('/')
                || input.path.contains("..")
                || input.path.contains(['\r', '\n', '?'])
                || input.query.contains(['\r', '\n', '#', ' ', '?'])
            {
                return Err("api_path_invalid".into());
            }
            let url = if input.query.is_empty() {
                format!("https://api.fastly.com{}", input.path)
            } else {
                format!("https://api.fastly.com{}?{}", input.path, input.query)
            };
            let mut request = Request::new(input.method, url)
                .with_header("fastly-key", input.token)
                .with_header("accept", "application/json")
                .with_header("cache-control", "no-cache");
            if let Some(body) = input.body {
                if input.path.starts_with("/resources/")
                    && !input.path.starts_with("/resources/stores/config/")
                {
                    request = request
                        .with_header("content-type", "application/json")
                        .with_body(body.to_string());
                } else {
                    let mut form = url::form_urlencoded::Serializer::new(String::new());
                    for (key, value) in body.as_object().ok_or("invalid_api_body")? {
                        if value.is_null() {
                            continue;
                        }
                        form.append_pair(
                            key,
                            value
                                .as_str()
                                .map(str::to_owned)
                                .unwrap_or_else(|| value.to_string())
                                .as_str(),
                        );
                    }
                    request = request
                        .with_header("content-type", "application/x-www-form-urlencoded")
                        .with_body(form.finish());
                }
            }
            stage = "headers";
            let mut response = bounded_send_with_timeout(
                request,
                "api_fastly",
                timeout.saturating_sub(started.elapsed()),
            )?;
            let response_status = response.get_status().as_u16();
            status = Some(response_status);
            let retry_after =
                crate::admin::transport::parse_retry_after(response.get_header_str("retry-after"));
            stage = "body";
            let value = if (200..300).contains(&response_status) {
                bounded_json(response.take_body())?
            } else {
                Value::Null
            };
            if started.elapsed() >= timeout {
                return Err("backend_deadline".into());
            }
            Ok(crate::admin::ApiResponse {
                status: response_status,
                body: value,
                retry_after,
            })
        })();
        self.api_log.record(
            input.method,
            input.path,
            status,
            started.elapsed().as_millis() as u64,
            result.as_ref().err().map(String::as_str),
            input.token,
        );
        println!(
            "{}",
            json!({"event":"admin.external.call","level":if result.is_err() {"error"} else {"info"},"method":input.method,"resource":api_resource(input.path),"status":status,"stage":stage,"error":result.as_ref().err(),"timeout_ms":timeout.as_millis(),"duration_ms":started.elapsed().as_millis()})
        );
        result
    }

    fn reconcile_secret_bundle(&self, store: &str, token: &str) -> Result<usize, String> {
        if let Some(cfg_store) = &self.config {
            if cfg_store
                .try_get(CONFIG_BUNDLE_KEY)
                .ok()
                .flatten()
                .is_none()
                && self.budget.remaining() > 2
            {
                if let Ok(cfg_id) = self.required("config_store_id") {
                    let mut cfg_map = BTreeMap::new();
                    for &k in crate::setup::CONFIG_KEYS {
                        if let Ok(Some(v)) = self.config(k) {
                            cfg_map.insert(k.to_owned(), v);
                        }
                    }
                    if let Ok(serialized) = serde_json::to_string(&cfg_map) {
                        let cfg_enc = crate::admin::transport::encode(&cfg_id);
                        let _ = self.raw_admin_api(crate::admin::ApiRequest {
                            method: "PUT",
                            path: &format!(
                                "/resources/stores/config/{cfg_enc}/item/{CONFIG_BUNDLE_KEY}"
                            ),
                            query: "",
                            token,
                            body: Some(json!({"item_value": serialized})),
                            timeout: FASTLY_API_TIMEOUT,
                        });
                    }
                }
            }
        }
        self.ensure_secret_bundle_loaded()?;
        let store_enc = crate::admin::transport::encode(store);
        let response = self.raw_admin_api(crate::admin::ApiRequest {
            method: "GET",
            path: &format!("/resources/stores/secret/{store_enc}/secrets"),
            query: "limit=200",
            token,
            body: None,
            timeout: FASTLY_API_TIMEOUT,
        })?;
        if response.status != 200 {
            return Err(format!("secret_store_status_{}", response.status));
        }
        let items = response.body["data"]
            .as_array()
            .ok_or("secret_store_shape")?;
        let names: Vec<String> = items
            .iter()
            .filter_map(|v| v["name"].as_str().map(str::to_owned))
            .collect();
        let standalone: Vec<String> = names
            .iter()
            .filter(|n| n.as_str() != SECRET_BUNDLE_KEY)
            .cloned()
            .collect();
        if standalone.is_empty() && names.iter().any(|n| n == SECRET_BUNDLE_KEY) {
            return Ok(1);
        }
        let edge_bundle = self.bundle_map.borrow().clone();
        let mut desired = edge_bundle.clone();
        for name in &standalone {
            if name.starts_with("fos_recovery.") || name.starts_with("fos_tmp_") {
                continue;
            }
            if (!edge_bundle.contains_key(name) || crate::setup::rotatable_secret(name))
                && self.secret_reads.get() < MAX_RAW_SECRET_READS
            {
                if let Some(val) = self.read_raw_secret(name)? {
                    desired.insert(name.clone(), val);
                }
            }
        }
        if desired.is_empty() {
            return Ok(names.len());
        }
        if !self.bundle_from_edge.get() || edge_bundle != desired {
            if self.budget.remaining() > 1 {
                let serialized =
                    serde_json::to_string(&desired).map_err(|_| "secret_bundle_encode_failed")?;
                let b64 = base64::engine::general_purpose::STANDARD.encode(serialized.as_bytes());
                let put_resp = self.raw_admin_api(crate::admin::ApiRequest {
                    method: "PUT",
                    path: &format!("/resources/stores/secret/{store_enc}/secrets"),
                    query: "",
                    token,
                    body: Some(json!({"name": SECRET_BUNDLE_KEY, "secret": b64})),
                    timeout: FASTLY_API_TIMEOUT,
                })?;
                if (200..300).contains(&put_resp.status) {
                    *self.bundle_map.borrow_mut() = desired;
                    if !names.iter().any(|n| n == SECRET_BUNDLE_KEY) {
                        return Ok(names.len() + 1);
                    }
                }
            }
            return Ok(names.len());
        }
        let mut remaining_count = names.len();
        for name in &standalone {
            if self.budget.remaining() <= 1 {
                break;
            }
            let del = self.raw_admin_api(crate::admin::ApiRequest {
                method: "DELETE",
                path: &format!(
                    "/resources/stores/secret/{store_enc}/secrets/{}",
                    crate::admin::transport::encode(name)
                ),
                query: "",
                token,
                body: None,
                timeout: FASTLY_API_TIMEOUT,
            })?;
            if (200..300).contains(&del.status) || del.status == 404 {
                remaining_count = remaining_count.saturating_sub(1);
            }
        }
        Ok(remaining_count)
    }
}

impl SetupStores for FastlyRuntime {
    fn config_present(&self, name: &str) -> Result<bool, String> {
        Ok(self.config(name)?.is_some_and(|value| !value.is_empty()))
    }
    fn secret_present(&self, name: &str) -> Result<bool, String> {
        self.ensure_secret_bundle_loaded()?;
        if let Some(value) = self.secret_cache.borrow().get(name) {
            return Ok(value.is_some());
        }
        if self.bundle_map.borrow().contains_key(name) {
            return Ok(true);
        }
        if self.secret_presence.borrow().is_none() {
            let token = self.secret("fastly_token")?;
            let Some(token) = token else {
                return Ok(false);
            };
            let store = self.required("secret_store_id")?;
            let response = self.raw_admin_api(crate::admin::ApiRequest {
                method: "GET",
                path: &format!("/resources/stores/secret/{store}/secrets"),
                query: "",
                token: &token,
                body: None,
                timeout: REQUEST_PROBE_TIMEOUT,
            })?;
            let value = response.body;
            let values = value
                .as_array()
                .or_else(|| value["data"].as_array())
                .ok_or("secret_metadata_invalid")?;
            let names = values
                .iter()
                .filter_map(|v| v["name"].as_str().map(str::to_owned))
                .collect();
            *self.secret_presence.borrow_mut() = Some(names);
        }
        Ok(self
            .secret_presence
            .borrow()
            .as_ref()
            .is_some_and(|names| names.contains(name)))
    }
    fn state_linked(&self) -> Result<bool, String> {
        Ok(self.kv.is_some())
    }
}

impl Runtime for FastlyRuntime {
    fn config(&self, name: &str) -> Result<Option<String>, String> {
        self.ensure_config_bundle_loaded()?;
        if let Some(value) = self.config_cache.borrow().get(name) {
            return Ok(value.clone());
        }
        let value = match &self.config {
            Some(store) => store
                .try_get(name)
                .map_err(|_| String::from("config_lookup_failed")),
            None => Ok(None),
        }?;
        self.config_cache
            .borrow_mut()
            .insert(name.into(), value.clone());
        Ok(value)
    }
    fn secret(&self, name: &str) -> Result<Option<String>, String> {
        self.ensure_secret_bundle_loaded()?;
        if let Some(value) = self.secret_cache.borrow().get(name) {
            return Ok(value.clone());
        }
        if crate::setup::rotatable_secret(name) {
            if let Some(standalone) = self.read_raw_secret(name)? {
                self.secret_cache
                    .borrow_mut()
                    .insert(name.into(), Some(standalone.clone()));
                return Ok(Some(standalone));
            }
        }
        if let Some(value) = self.bundle_map.borrow().get(name).cloned() {
            self.secret_cache
                .borrow_mut()
                .insert(name.into(), Some(value.clone()));
            return Ok(Some(value));
        }
        let value = self.read_raw_secret(name)?;
        self.secret_cache
            .borrow_mut()
            .insert(name.into(), value.clone());
        Ok(value)
    }
    fn random(&self) -> Result<[u8; 32], String> {
        let mut bytes = [0u8; 32];
        getrandom::getrandom(&mut bytes).map_err(|_| "random_unavailable")?;
        Ok(bytes)
    }
    fn now(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |time| time.as_secs())
    }
    fn checks(&self, front_rl: bool) -> Result<Value, String> {
        let (last_tick, last_tick_cached) = self.read_tick();
        let environment = Environment {
            now: self.now(),
            front_rl,
            front_verified: true,
            front_service_id: self.required("front_service_id")?,
            flp_service_id: self.required("flp_service_id")?,
            worker_service_id: self.required("worker_service_id")?,
            config_store_id: self.required("config_store_id")?,
            secret_store_id: self.required("secret_store_id")?,
            kv_store_id: self.required("kv_store_id")?,
            buckets_dictionary_id: self.required("buckets_dictionary_id")?,
            credentials_dictionary_id: self.required("credentials_dictionary_id")?,
            compute_host: self.required("compute_host")?,
            install_name: self.required("install_name")?,
            last_tick,
            last_tick_cached,
            clock_interval_s: self
                .required("clock_interval_s")?
                .parse()
                .map_err(|_| "clock_interval_invalid")?,
            fos_regions: vec!["us-east-1".into()],
        };
        let mut api = FastlyApi {
            runtime: self,
            token: self.secret("fastly_token")?.ok_or("missing_api_token")?,
        };
        let mut probe = Probe { runtime: self };
        serde_json::to_value(checks::run_checks(&mut api, &mut probe, &environment))
            .map_err(|_| "checks_serialize".into())
    }
    fn versions(&self) -> Result<Value, String> {
        let mut api = FastlyApi {
            runtime: self,
            token: self.secret("fastly_token")?.ok_or("missing_api_token")?,
        };
        let mut rows = Vec::new();
        let name = self.required("install_name")?;
        for (resource, key) in [
            (name.clone(), "flp_service_id"),
            (format!("{name}-worker"), "worker_service_id"),
        ] {
            let id = self.required(key)?;
            let (status, versions) = api.get(&format!("/service/{id}/version"))?;
            if status != 200 {
                return Err("version_api_failed".into());
            }
            let active = versions
                .as_array()
                .ok_or("version_api_shape")?
                .iter()
                .find(|version| version["active"] == true)
                .ok_or("active_version_missing")?;
            let version = active["number"].as_u64().ok_or("active_version_invalid")?;
            let sha = if key == "flp_service_id" {
                env!("FLP_BUILD_SHA").to_owned()
            } else {
                let host = self.required("worker_host")?;
                let worker_key = self.secret("worker_secret")?.ok_or("missing_worker_key")?;
                let mut response = bounded_send_with_timeout(
                    Request::get(format!("https://{host}/version"))
                        .with_header("x-flp-worker-key", worker_key),
                    "flp_worker",
                    PEER_TIMEOUT,
                )
                .map_err(|_| "worker_version_unavailable")?;
                if response.get_status().as_u16() != 200 {
                    return Err("worker_version_denied".into());
                }
                let data = bounded_json(response.take_body())?;
                data["build_sha"]
                    .as_str()
                    .ok_or("worker_version_invalid")?
                    .to_owned()
            };
            rows.push(json!({"name":resource,"version":version.to_string(),"source_sha":sha}));
        }
        Ok(json!({"versions":rows}))
    }
    fn compute_service_id(&self) -> Option<String> {
        std::env::var("FASTLY_SERVICE_ID").ok()
    }
    fn compute_service_version(&self) -> Option<String> {
        std::env::var("FASTLY_SERVICE_VERSION").ok()
    }
    fn setup_missing(
        &self,
        control_plane: bool,
    ) -> Result<Vec<crate::setup::MissingEntry>, String> {
        if !control_plane {
            return crate::setup::missing_entries(self);
        }
        // Signed-in control-plane routes read their own credentials below. Re-reading
        // bootstrap secret metadata here would consume the sixth secret read.
        let mut missing = Vec::new();
        for &name in crate::setup::CONFIG_KEYS {
            if !self.config_present(name)? {
                missing.push(crate::setup::MissingEntry{store:"Config",name,command:format!("fastly config-store-entry create --store-id=<config-store-id> --key={name} --stdin")});
            }
        }
        if !self.state_linked()? {
            return Err("kv_store_unavailable".into());
        }
        Ok(missing)
    }
    fn admin_get(&self, key: &str) -> Result<Option<crate::admin::Record>, String> {
        let cacheable = Self::is_cacheable_kv_key(key);
        if cacheable {
            if let Some(cached) = self.kv_cache.borrow().get(key) {
                return Ok(cached.clone());
            }
            if let Some(cached) = self.kv_core_cache_get(key) {
                self.kv_cache
                    .borrow_mut()
                    .insert(key.into(), cached.clone());
                return Ok(cached);
            }
        }
        self.budget.charge()?;
        self.kv_ops.set(self.kv_ops.get().saturating_add(1));
        let kv = self.kv.as_ref().ok_or("kv_store_unavailable")?;
        match kv.lookup(key) {
            Ok(mut response) => {
                let generation = response.current_generation().to_string();
                let mut value = bounded_json(response.take_body())?;
                if Self::is_coordinated_kv_key(key) {
                    if let Some(mut coord) = self.kv_coord_get(key) {
                        let kv_ts = value["_kv_written_ms"].as_u64().unwrap_or(0);
                        if let Some(ref cached_val) = coord.last_written_value {
                            let cached_ts = cached_val["_kv_written_ms"].as_u64().unwrap_or(0);
                            if cached_ts > kv_ts {
                                if coord.dead_generation.as_deref() != Some(generation.as_str()) {
                                    coord.dead_generation = Some(generation.clone());
                                    coord.dead_since_ms = coord.last_write_ms.max(cached_ts);
                                    self.kv_coord_put(key, &coord);
                                }
                                value = cached_val.clone();
                            }
                        }
                        if coord.dead_generation.as_deref() == Some(generation.as_str())
                            && self.budget.remaining() > 4
                        {
                            if let Ok(Some(token)) = self
                                .secret("fastly_token")
                                .map(|opt| opt.filter(|t| !t.is_empty()))
                            {
                                if let Ok(Some(fresh)) = self.admin_get_fresh(key, &token) {
                                    let cur_ts = value["_kv_written_ms"].as_u64().unwrap_or(0);
                                    let fresh_ts =
                                        fresh.value["_kv_written_ms"].as_u64().unwrap_or(0);
                                    if fresh_ts > cur_ts || (cur_ts == 0 && fresh.value != value) {
                                        return Ok(Some(fresh));
                                    }
                                }
                            }
                        }
                    }
                }
                let record = crate::admin::Record { generation, value };
                if cacheable {
                    self.kv_cache
                        .borrow_mut()
                        .insert(key.into(), Some(record.clone()));
                    self.kv_core_cache_put(key, Some(&record));
                }
                Ok(Some(record))
            }
            Err(fastly::kv_store::KVStoreError::ItemNotFound) => {
                if cacheable {
                    self.kv_cache.borrow_mut().insert(key.into(), None);
                    self.kv_core_cache_put(key, None);
                }
                Ok(None)
            }
            Err(_) => Err("admin_state_lookup_failed".into()),
        }
    }
    fn admin_cas(
        &self,
        key: &str,
        generation: Option<&str>,
        value: &Value,
    ) -> Result<bool, String> {
        use fastly::kv_store::{InsertMode, KVStoreError};
        let now = || {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0)
        };
        let coordinated = Self::is_coordinated_kv_key(key);
        let coord = if coordinated {
            self.kv_coord_get(key)
        } else {
            None
        };
        let last_write = self
            .admin_writes
            .borrow()
            .get(key)
            .copied()
            .into_iter()
            .chain(coord.as_ref().map(|c| c.last_write_ms).filter(|&ts| ts > 0))
            .max();
        if let Some(last) = last_write {
            let delay = crate::admin::kv_write_delay_ms(last, now())?;
            if delay > 0 {
                std::thread::sleep(std::time::Duration::from_millis(delay));
            }
        }
        let kv = self.kv.as_ref().ok_or("kv_store_unavailable")?;
        let parsed_gen = match generation {
            Some(g) => Some(g.parse::<u64>().map_err(|_| "invalid_generation")?),
            None => None,
        };
        for attempt in 0..2 {
            if attempt > 0 {
                std::thread::sleep(std::time::Duration::from_millis(1_100));
            }
            self.budget.charge()?;
            self.kv_ops.set(self.kv_ops.get().saturating_add(1));
            let mut builder = kv.build_insert();
            builder = match parsed_gen {
                Some(g) => builder.if_generation_match(g),
                None => builder.mode(InsertMode::Add),
            };
            let timestamp = now();
            let mut value = value.clone();
            value["_kv_written_ms"] = timestamp.into();
            match builder.execute(key, value.to_string()) {
                Ok(()) => {
                    self.admin_writes.borrow_mut().insert(key.into(), timestamp);
                    self.kv_core_cache_invalidate(key);
                    if coordinated {
                        self.kv_coord_put(
                            key,
                            &KvCoordState {
                                last_write_ms: timestamp,
                                dead_generation: None,
                                dead_since_ms: 0,
                                last_written_value: Some(value),
                            },
                        );
                    }
                    return Ok(true);
                }
                Err(KVStoreError::ItemPreconditionFailed) => {
                    self.kv_core_cache_invalidate(key);
                    if coordinated {
                        if let Some(g_str) = generation {
                            let now_ms = now();
                            let dead_since = coord
                                .as_ref()
                                .filter(|c| c.dead_generation.as_deref() == Some(g_str))
                                .map(|c| c.dead_since_ms)
                                .unwrap_or(0);
                            if dead_since > 0
                                && now_ms.saturating_sub(dead_since) >= 2_000
                                && self.budget.remaining() > 0
                            {
                                std::thread::sleep(std::time::Duration::from_millis(1_100));
                                self.budget.charge()?;
                                self.kv_ops.set(self.kv_ops.get().saturating_add(1));
                                let heal_ts = now();
                                let mut heal_val = value.clone();
                                heal_val["_kv_written_ms"] = heal_ts.into();
                                if kv
                                    .build_insert()
                                    .mode(InsertMode::Overwrite)
                                    .execute(key, heal_val.to_string())
                                    .is_ok()
                                {
                                    self.admin_writes.borrow_mut().insert(key.into(), heal_ts);
                                    self.kv_coord_put(
                                        key,
                                        &KvCoordState {
                                            last_write_ms: heal_ts,
                                            dead_generation: None,
                                            dead_since_ms: 0,
                                            last_written_value: Some(heal_val),
                                        },
                                    );
                                    self.admin_event(json!({
                                        "event": "admin.state.healed_stale_pop_cache",
                                        "level": "warn",
                                        "coordination_key": key,
                                        "dead_generation": g_str,
                                        "dead_age_ms": heal_ts.saturating_sub(dead_since),
                                        "runtime_service_id": std::env::var("FASTLY_SERVICE_ID").ok(),
                                        "runtime_service_version": std::env::var("FASTLY_SERVICE_VERSION").ok(),
                                    }));
                                    return Ok(true);
                                }
                            }
                            let mut state = coord.unwrap_or_default();
                            if state.dead_generation.as_deref() != Some(g_str)
                                || state.dead_since_ms == 0
                            {
                                state.dead_generation = Some(g_str.to_owned());
                                state.dead_since_ms = now_ms;
                                self.kv_coord_put(key, &state);
                            }
                        }
                    }
                    return Ok(false);
                }
                Err(_) if attempt == 0 => continue,
                Err(_) => return Err("admin_state_write_failed".into()),
            }
        }
        Err("admin_state_write_failed".into())
    }
    fn admin_get_fresh(
        &self,
        key: &str,
        token: &str,
    ) -> Result<Option<crate::admin::Record>, String> {
        self.budget.charge()?;
        self.kv_ops.set(self.kv_ops.get().saturating_add(1));
        let store = self.required("kv_store_id")?;
        let path = format!(
            "/resources/stores/kv/{store}/keys/{}",
            crate::admin::transport::encode(key)
        );
        let started = Instant::now();
        let mut response = bounded_send_with_timeout(
            Request::get(format!("https://api.fastly.com{path}"))
                .with_header("fastly-key", token)
                .with_header("cache-control", "no-cache"),
            "api_fastly",
            STATE_CONFIRMATION_TIMEOUT,
        )?;
        let status = response.get_status().as_u16();
        let expected_ts = self.admin_writes.borrow().get(key).copied();
        self.admin_event(json!({
            "event": "admin.state.confirm",
            "level": if status == 200 { "info" } else { "error" },
            "status": status,
            "duration_ms": started.elapsed().as_millis(),
            "coordination_key": match key {
                crate::ingest::PLAN_KEY
                | crate::ingest::WATERMARK_KEY
                | crate::ingest::compaction::STATE_KEY => key,
                _ => "other",
            },
            "generation": response.get_header_str("generation"),
            "expected_written_ms": expected_ts,
            "api_cache": response.get_header_str("x-cache"),
            "api_pop": response.get_header_str("x-served-by"),
            "runtime_service_id": std::env::var("FASTLY_SERVICE_ID").ok(),
            "runtime_service_version": std::env::var("FASTLY_SERVICE_VERSION").ok(),
        }));
        let fallback_local_kv = || -> Option<crate::admin::Record> {
            let expected = expected_ts?;
            let kv = self.kv.as_ref()?;
            let mut local = kv.lookup(key).ok()?;
            let generation = local.current_generation().to_string();
            let value = bounded_json(local.take_body()).ok()?;
            value["_kv_written_ms"]
                .as_u64()
                .is_some_and(|ts| ts >= expected)
                .then_some(crate::admin::Record { generation, value })
        };
        if status == 404 {
            if let Some(record) = fallback_local_kv() {
                if Self::is_cacheable_kv_key(key) {
                    self.kv_cache
                        .borrow_mut()
                        .insert(key.into(), Some(record.clone()));
                    self.kv_core_cache_put(key, Some(&record));
                }
                return Ok(Some(record));
            }
            if expected_ts.is_none() && Self::is_cacheable_kv_key(key) {
                self.kv_cache.borrow_mut().insert(key.into(), None);
                self.kv_core_cache_put(key, None);
            }
            return Ok(None);
        }
        if status != 200 {
            return Err("admin_state_confirmation_rejected".into());
        }
        let generation = response
            .get_header_str("generation")
            .ok_or("admin_state_generation_missing")?
            .parse::<u64>()
            .map_err(|_| "admin_state_generation_invalid")?
            .to_string();
        let value = bounded_json(response.take_body())?;
        if expected_ts.is_some_and(|expected| {
            value["_kv_written_ms"]
                .as_u64()
                .is_none_or(|ts| ts < expected)
        }) {
            if let Some(record) = fallback_local_kv() {
                if Self::is_cacheable_kv_key(key) {
                    self.kv_cache
                        .borrow_mut()
                        .insert(key.into(), Some(record.clone()));
                    self.kv_core_cache_put(key, Some(&record));
                }
                return Ok(Some(record));
            }
            return Ok(Some(crate::admin::Record { generation, value }));
        }
        let record = crate::admin::Record { generation, value };
        if Self::is_cacheable_kv_key(key) {
            self.kv_cache
                .borrow_mut()
                .insert(key.into(), Some(record.clone()));
            self.kv_core_cache_put(key, Some(&record));
        }
        Ok(Some(record))
    }
    fn admin_api(
        &self,
        input: crate::admin::ApiRequest<'_>,
    ) -> Result<crate::admin::ApiResponse, String> {
        if let Some(rest) = input.path.strip_prefix("/resources/stores/secret/") {
            let _ = self.ensure_secret_bundle_loaded();
            let bundle_active = self.bundle_from_edge.get() || !self.bundle_map.borrow().is_empty();
            if bundle_active {
                if input.method == "PUT" {
                    if let Some((_store, "secrets")) = rest.split_once('/') {
                        if let Some(ref body) = input.body {
                            if let (Some(name), Some(b64)) =
                                (body["name"].as_str(), body["secret"].as_str())
                            {
                                if name != SECRET_BUNDLE_KEY {
                                    let decoded = base64::engine::general_purpose::STANDARD
                                        .decode(b64)
                                        .map_err(|_| String::from("secret_encoding_invalid"))?;
                                    let plaintext = String::from_utf8(decoded)
                                        .map_err(|_| String::from("secret_encoding_invalid"))?;
                                    let mut map = self.bundle_map.borrow_mut();
                                    map.insert(name.to_owned(), plaintext.clone());
                                    self.secret_cache
                                        .borrow_mut()
                                        .insert(name.to_owned(), Some(plaintext));
                                    let serialized = serde_json::to_string(&*map)
                                        .map_err(|_| String::from("secret_bundle_encode_failed"))?;
                                    drop(map);
                                    let bundle_b64 = base64::engine::general_purpose::STANDARD
                                        .encode(serialized.as_bytes());
                                    let new_body =
                                        json!({"name": SECRET_BUNDLE_KEY, "secret": bundle_b64});
                                    return self.raw_admin_api(crate::admin::ApiRequest {
                                        method: "PUT",
                                        path: input.path,
                                        query: input.query,
                                        token: input.token,
                                        body: Some(new_body),
                                        timeout: input.timeout,
                                    });
                                }
                            }
                        }
                    }
                } else if input.method == "DELETE" {
                    if let Some((store, tail)) = rest.split_once("/secrets/") {
                        let name = percent_encoding::percent_decode_str(tail).decode_utf8_lossy();
                        if name != SECRET_BUNDLE_KEY {
                            self.secret_cache
                                .borrow_mut()
                                .insert(name.clone().into_owned(), None);
                            let removed =
                                self.bundle_map.borrow_mut().remove(name.as_ref()).is_some();
                            if removed {
                                let serialized = serde_json::to_string(&*self.bundle_map.borrow())
                                    .map_err(|_| String::from("secret_bundle_encode_failed"))?;
                                let bundle_b64 = base64::engine::general_purpose::STANDARD
                                    .encode(serialized.as_bytes());
                                let new_body =
                                    json!({"name": SECRET_BUNDLE_KEY, "secret": bundle_b64});
                                let put_path = format!("/resources/stores/secret/{store}/secrets");
                                let resp = self.raw_admin_api(crate::admin::ApiRequest {
                                    method: "PUT",
                                    path: &put_path,
                                    query: "",
                                    token: input.token,
                                    body: Some(new_body),
                                    timeout: input.timeout,
                                })?;
                                return Ok(crate::admin::ApiResponse {
                                    status: if (200..300).contains(&resp.status) {
                                        204
                                    } else {
                                        resp.status
                                    },
                                    body: Value::Null,
                                    retry_after: resp.retry_after,
                                });
                            }
                            return Ok(crate::admin::ApiResponse {
                                status: 204,
                                body: Value::Null,
                                retry_after: None,
                            });
                        }
                    }
                } else if input.method == "GET" {
                    if let Some((_store, tail)) = rest.split_once("/secrets/") {
                        if !tail.is_empty() && !tail.contains('/') {
                            let name =
                                percent_encoding::percent_decode_str(tail).decode_utf8_lossy();
                            if name != SECRET_BUNDLE_KEY
                                && self.bundle_map.borrow().contains_key(name.as_ref())
                            {
                                return Ok(crate::admin::ApiResponse {
                                    status: 200,
                                    body: json!({"name": name}),
                                    retry_after: None,
                                });
                            }
                        }
                    }
                }
            }
        }
        self.raw_admin_api(input)
    }
    fn admin_fos(
        &self,
        input: crate::admin::FosRequest<'_>,
    ) -> Result<crate::admin::FosResponse, String> {
        self.fos_call(input).map(|(response, _)| response)
    }
    fn admin_fos_etag(
        &self,
        input: crate::admin::FosRequest<'_>,
    ) -> Result<(crate::admin::FosResponse, Option<String>), String> {
        self.fos_call(input)
    }
    fn admin_cdn_get(
        &self,
        service_id: &str,
        key: &str,
        cdn_key: &str,
        max_bytes: usize,
        timeout: std::time::Duration,
    ) -> Result<crate::admin::FosResponse, String> {
        self.admin_cdn_read(service_id, key, cdn_key, max_bytes, timeout, None)
    }
    fn admin_cdn_read(
        &self,
        service_id: &str,
        key: &str,
        cdn_key: &str,
        max_bytes: usize,
        timeout: std::time::Duration,
        range: Option<(u64, u64)>,
    ) -> Result<crate::admin::FosResponse, String> {
        let unread = range
            .is_none()
            .then(|| {
                self.unread
                    .borrow_mut()
                    .remove(&format!("{service_id}/{key}"))
            })
            .flatten();
        let started = Instant::now();
        let (sent, result) = match unread {
            // A speculative read already sent and charged: wait for it and read its body exactly
            // as a fresh read would.
            Some((sent, pending, _)) => (
                sent,
                if timeout.is_zero() {
                    Err("backend_deadline".into())
                } else {
                    wait_pending(pending, started, timeout)
                        .and_then(|response| cdn_response(response, max_bytes))
                },
            ),
            None => (
                started,
                (|| {
                    if timeout.is_zero() {
                        return Err("backend_deadline".into());
                    }
                    let request = self.cdn_request(service_id, key, cdn_key, range)?;
                    let response = send_with_timeout(request, "flp_front", timeout, false)?;
                    cdn_response(response, max_bytes)
                })(),
            ),
        };
        self.record_cdn_get(key, sent.elapsed(), &result);
        result
    }
    fn admin_cdn_get_many(
        &self,
        service_id: &str,
        keys: &[(String, usize)],
        cdn_key: &str,
        timeout: std::time::Duration,
    ) -> Vec<Result<crate::admin::FosResponse, String>> {
        let Some(((first, max_bytes), rest)) = keys.split_first() else {
            return Vec::new();
        };
        let mut results = Vec::with_capacity(keys.len());
        if timeout.is_zero() {
            results.push(Err("backend_deadline".to_owned()));
            self.record_cdn_get(first, std::time::Duration::ZERO, &results[0]);
        } else if self
            .unread
            .borrow()
            .contains_key(&format!("{service_id}/{first}"))
        {
            self.send_unread(service_id, rest, cdn_key);
            results
                .push(self.admin_cdn_read(service_id, first, cdn_key, *max_bytes, timeout, None));
        } else {
            let started = Instant::now();
            let sent = self
                .cdn_request(service_id, first, cdn_key, None)
                .and_then(|request| {
                    request
                        .send_async("flp_front")
                        .map_err(|error| send_error_category(&error, "backend_send_failed").into())
                });
            match sent {
                Ok(pending) => {
                    self.send_unread(service_id, rest, cdn_key);
                    results.push(
                        wait_pending(pending, started, timeout)
                            .and_then(|response| cdn_response(response, *max_bytes)),
                    );
                }
                Err(error) => results.push(Err(error)),
            }
            self.record_cdn_get(first, started.elapsed(), &results[0]);
        }
        results.extend(
            rest.iter()
                .map(|_| Err(crate::shell::PREFETCH_UNREAD.to_owned())),
        );
        results
    }
    fn release_unread(&self) {
        for (key, (sent, _, _)) in std::mem::take(&mut *self.unread.borrow_mut()) {
            let key = key.split_once('/').map_or(key.as_str(), |(_, key)| key);
            let duration_ms = sent.elapsed().as_millis();
            self.usage
                .record_cdn(key, None, None, duration_ms as u64, 0);
            // An unused speculative read is expected, not a failed read.
            println!(
                "{}",
                json!({"event":"admin.external.call","level":"info","service":"cdn","method":"GET","outcome":"prefetch_unused","duration_ms":duration_ms})
            );
        }
    }
    fn admin_unread(&self, service_id: &str) -> BTreeMap<String, usize> {
        let prefix = format!("{service_id}/");
        self.unread
            .borrow()
            .iter()
            .filter_map(|(id, (_, _, limit))| Some((id.strip_prefix(&prefix)?.to_owned(), *limit)))
            .collect()
    }
    fn admin_remaining_calls(&self) -> u32 {
        self.budget.remaining()
    }
    fn execution_backend_calls(&self) -> Option<u32> {
        Some(self.budget.used())
    }
    fn admin_charge_call(&self) -> Result<(), String> {
        self.budget.charge()
    }
    fn cache_ops(&self) -> u32 {
        self.cache_ops.get()
    }
    fn kv_ops(&self) -> u32 {
        self.kv_ops.get()
    }
    fn cache_lookups(&self) -> u32 {
        self.cache_lookups.get()
    }
    fn cache_replaces(&self) -> u32 {
        self.cache_replaces.get()
    }
    fn purges(&self) -> u32 {
        self.purges.get()
    }
    fn worker_requests(&self) -> u32 {
        self.worker_sent.get()
    }
    fn compute_billing(&self, from: u64, to: u64) -> Result<Value, String> {
        let token = self.secret("fastly_token")?.ok_or("missing_api_token")?;
        let query = format!("from={from}&to={to}&by=hour");
        let mut services = Vec::new();
        for (role, key) in [("app", "flp_service_id"), ("worker", "worker_service_id")] {
            let id = self.required(key)?;
            let mut service = json!({"role": role, "service_id": id});
            for field in crate::telemetry::usage::BILLING_FIELDS {
                let response = self.admin_api(crate::admin::ApiRequest {
                    method: "GET",
                    path: &format!(
                        "/stats/service/{}/field/{field}",
                        crate::admin::transport::encode(&id)
                    ),
                    query: &query,
                    token: &token,
                    body: None,
                    timeout: FASTLY_API_TIMEOUT,
                })?;
                if response.status != 200 {
                    return Err(format!("stats_api_status_{}", response.status));
                }
                if matches!(
                    field,
                    "kv_store_class_a_operations" | "kv_store_class_b_operations"
                ) {
                    let (total, first, last, buckets) =
                        crate::telemetry::usage::stats_field_buckets(&response.body, field)?;
                    let bucket_key = if field == "kv_store_class_a_operations" {
                        "kv_class_a_buckets"
                    } else {
                        "kv_class_b_buckets"
                    };
                    service[field] = total.into();
                    service[bucket_key] = buckets.into();
                    service["covered_from"] = first.into();
                    service["covered_to"] = last.map(|start| start + 3600).into();
                } else {
                    let (total, first, last) =
                        crate::telemetry::usage::sum_stats_field(&response.body, field)?;
                    service[field] = total.into();
                    service["covered_from"] = first.into();
                    service["covered_to"] = last.map(|start| start + 3600).into();
                }
            }
            services.push(service);
        }
        if let Some(front_id) = self.config("front_service_id")?.filter(|s| !s.is_empty()) {
            let response = self.admin_api(crate::admin::ApiRequest {
                method: "GET",
                path: &format!(
                    "/stats/service/{}/field/bandwidth",
                    crate::admin::transport::encode(&front_id)
                ),
                query: &query,
                token: &token,
                body: None,
                timeout: FASTLY_API_TIMEOUT,
            })?;
            if response.status != 200 {
                return Err(format!("stats_api_status_{}", response.status));
            }
            let (total, first, last, buckets) =
                crate::telemetry::usage::stats_field_buckets(&response.body, "bandwidth")?;
            let mut service = json!({
                "role": "front",
                "service_id": front_id,
                "bandwidth": total,
                "bandwidth_buckets": buckets,
                "covered_from": first,
                "covered_to": last.map(|start| start + 3600),
            });
            for field in crate::telemetry::usage::FRONT_BILLING_FIELDS {
                let response = self.admin_api(crate::admin::ApiRequest {
                    method: "GET",
                    path: &format!(
                        "/stats/service/{}/field/{field}",
                        crate::admin::transport::encode(&front_id)
                    ),
                    query: &query,
                    token: &token,
                    body: None,
                    timeout: FASTLY_API_TIMEOUT,
                })?;
                if response.status != 200 {
                    return Err(format!("stats_api_status_{}", response.status));
                }
                if matches!(
                    field,
                    "kv_store_class_a_operations" | "kv_store_class_b_operations"
                ) {
                    let (total, _, _, buckets) =
                        crate::telemetry::usage::stats_field_buckets(&response.body, field)?;
                    let bucket_key = if field == "kv_store_class_a_operations" {
                        "kv_class_a_buckets"
                    } else {
                        "kv_class_b_buckets"
                    };
                    service[field] = total.into();
                    service[bucket_key] = buckets.into();
                } else {
                    service[field] =
                        crate::telemetry::usage::sum_stats_field(&response.body, field)?
                            .0
                            .into();
                }
            }
            services.push(service);
        }
        let store = self.required("secret_store_id")?;
        let secrets = self.reconcile_secret_bundle(&store, &token)?;
        Ok(json!({
            "from": from, "to": to, "by": "hour", "services": services,
            "secret_store": {"store_id": store, "secrets": secrets},
        }))
    }
    fn logged_traffic(&self, service_id: &str, from: u64, to: u64) -> Result<Value, String> {
        let token = self.secret("fastly_token")?.ok_or("missing_api_token")?;
        let query = format!("from={from}&to={to}&by=hour");
        let mut days = std::collections::BTreeMap::<u64, serde_json::Map<String, Value>>::new();
        for field in crate::telemetry::usage::TRAFFIC_FIELDS {
            let response = self.admin_api(crate::admin::ApiRequest {
                method: "GET",
                path: &format!(
                    "/stats/service/{}/field/{field}",
                    crate::admin::transport::encode(service_id)
                ),
                query: &query,
                token: &token,
                body: None,
                timeout: FASTLY_API_TIMEOUT,
            })?;
            if response.status != 200 {
                return Err(format!("stats_api_status_{}", response.status));
            }
            let (_, _, _, buckets) =
                crate::telemetry::usage::stats_field_buckets(&response.body, field)?;
            for (day, total) in crate::telemetry::usage::daily_totals(&buckets, field) {
                let entry = days.entry(day).or_default();
                entry.insert("day".into(), day.into());
                entry.insert(field.into(), total.into());
            }
        }
        Ok(json!({
            "service_id": service_id, "from": from, "to": to,
            "days": days.into_values().map(Value::Object).collect::<Vec<_>>(),
        }))
    }
    fn usage_agg(&self) -> Option<&crate::telemetry::usage::UsageAgg> {
        Some(&self.usage)
    }
    fn set_telemetry_budget(&self, on: bool) {
        self.budget.set_telemetry(on);
    }
    fn api_log(&self) -> Option<&crate::telemetry::api::ApiLog> {
        Some(&self.api_log)
    }
    fn admin_event(&self, event: Value) {
        println!("{event}");
        if event["event"] != "cron.execution"
            && event["event"] != "admin.state.confirm"
            && event["event"] != "admin.state.healed_stale_pop_cache"
            && !(event["event"] == "cron.finalize" && event["stage"] == "dispatch_start")
        {
            return;
        }
        let result = (|| -> Result<(), &str> {
            let Some(name) = self
                .config("execution_log_endpoint")
                .map_err(|_| "config_lookup_failed")?
            else {
                return Ok(());
            };
            let timestamp_us = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| "clock_before_epoch")?
                .as_micros()
                .try_into()
                .map_err(|_| "clock_out_of_range")?;
            let mut endpoint =
                fastly::log::Endpoint::try_from_name(&name).map_err(|_| "endpoint_unavailable")?;
            crate::telemetry::execution::write_resource_event(&mut endpoint, &event, timestamp_us)
                .map_err(|_| "event_write_failed")
        })();
        if let Err(reason) = result {
            eprintln!(
                "{}",
                json!({
                    "event": "cron.execution_log_error",
                    "root_run_id": event["root_run_id"],
                    "reason": reason,
                })
            );
        }
    }
    fn core_cache_replace(
        &self,
        key: &str,
        surrogate_keys: &[&str],
        ttl: std::time::Duration,
        update: &dyn Fn(Option<CoreCacheEntry>) -> CoreCacheEntry,
    ) -> Result<(), CoreCacheError> {
        use fastly::cache::core::{self, CacheKey, ReplaceStrategy};
        self.charge_cache_op()?;
        self.cache_replaces
            .set(self.cache_replaces.get().saturating_add(1));
        let replace = core::replace(CacheKey::copy_from_slice(key.as_bytes()))
            .replace_strategy(ReplaceStrategy::Wait)
            .begin()
            .map_err(|_| CoreCacheError::ReplaceFailed)?;
        let existing = match replace.existing_object() {
            Some(found) => {
                let mut body = Vec::new();
                found
                    .to_stream()
                    .and_then(|mut stream| {
                        stream
                            .read_to_end(&mut body)
                            .map_err(|_| core::CacheError::InvalidOperation)
                    })
                    .map_err(|_| CoreCacheError::ReplaceFailed)?;
                Some((body, found.user_metadata().to_vec()))
            }
            None => None,
        };
        let (body, meta) = update(existing);
        let mut stream = replace
            .known_length(body.len() as u64)
            .user_metadata(bytes::Bytes::from(meta))
            .surrogate_keys(surrogate_keys.iter().copied())
            .execute(ttl)
            .map_err(|_| CoreCacheError::ReplaceFailed)?;
        std::io::Write::write_all(&mut stream, &body).map_err(|_| CoreCacheError::ReplaceFailed)?;
        stream.finish().map_err(|_| CoreCacheError::ReplaceFailed)
    }
    fn core_cache_lookup(&self, key: &str) -> Result<Option<CoreCacheEntry>, CoreCacheError> {
        use fastly::cache::core::{self, CacheKey};
        self.charge_cache_op()?;
        self.cache_lookups
            .set(self.cache_lookups.get().saturating_add(1));
        let found = core::lookup(CacheKey::copy_from_slice(key.as_bytes()))
            .execute()
            .map_err(|e| CoreCacheError::Failed(e.to_string()))?;
        let Some(found) = found else {
            return Ok(None);
        };
        let mut body = Vec::new();
        found
            .to_stream()
            .map_err(|e| CoreCacheError::Failed(e.to_string()))?
            .read_to_end(&mut body)
            .map_err(|e| CoreCacheError::Failed(e.to_string()))?;
        Ok(Some((body, found.user_metadata().to_vec())))
    }
    fn core_cache_purge(&self, surrogate: &str) -> Result<(), CoreCacheError> {
        self.charge_cache_op()?;
        self.purges.set(self.purges.get().saturating_add(1));
        fastly::http::purge::purge_surrogate_key(surrogate).map_err(|e| {
            let message = e.to_string();
            let upper = message.to_ascii_uppercase();
            if ["LIMIT", "RATE", "429"].iter().any(|t| upper.contains(t)) {
                CoreCacheError::PurgeRateLimited
            } else {
                CoreCacheError::Failed(message)
            }
        })
    }
    fn kv_list(&self, prefix: &str, cursor: Option<&str>, limit: u32) -> Result<KvPage, String> {
        self.budget.charge()?;
        self.kv_ops.set(self.kv_ops.get().saturating_add(1));
        let kv = self.kv.as_ref().ok_or("kv_store_unavailable")?;
        let stem = crate::shell::kv_list_stem(prefix);
        let mut builder = kv.build_list().prefix(stem).limit(limit);
        if let Some(cursor) = cursor {
            builder = builder.cursor(cursor);
        }
        let page = builder.execute().map_err(|_| "kv_list_failed")?;
        let next = page.next_cursor();
        Ok(crate::shell::kv_filter_stem_page(
            prefix,
            page.into_keys(),
            next,
        ))
    }
    fn kv_delete(&self, key: &str) -> Result<(), String> {
        let now = || {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0)
        };
        let coord = self.kv_coord_get(key);
        let last_write = self
            .admin_writes
            .borrow()
            .get(key)
            .copied()
            .into_iter()
            .chain(coord.as_ref().map(|c| c.last_write_ms).filter(|&t| t > 0))
            .max();
        if let Some(last) = last_write {
            let delay = crate::admin::kv_write_delay_ms(last, now())?;
            if delay > 0 {
                std::thread::sleep(std::time::Duration::from_millis(delay));
            }
        }
        self.budget.charge()?;
        self.kv_ops.set(self.kv_ops.get().saturating_add(1));
        let kv = self.kv.as_ref().ok_or("kv_store_unavailable")?;
        let res = match kv.delete(key) {
            Ok(()) | Err(fastly::kv_store::KVStoreError::ItemNotFound) => Ok(()),
            Err(_) => Err("kv_delete_failed".into()),
        };
        if res.is_ok() {
            let ts = now();
            self.admin_writes.borrow_mut().insert(key.into(), ts);
            self.kv_coord_put(
                key,
                &KvCoordState {
                    last_write_ms: ts,
                    dead_generation: None,
                    dead_since_ms: 0,
                    last_written_value: None,
                },
            );
            self.kv_core_cache_invalidate(key);
            if Self::is_cacheable_kv_key(key) {
                self.kv_cache.borrow_mut().insert(key.into(), None);
            }
        }
        res
    }
    fn worker_delete(&self, body: &Value) -> Result<Value, String> {
        self.budget.charge()?;
        let host = self.required("worker_host")?;
        let key = self.secret("worker_secret")?.ok_or("missing_worker_key")?;
        self.worker_sent.set(self.worker_sent.get() + 1);
        let mut response = bounded_send_with_timeout(
            Request::post(format!("https://{host}/_internal/delete"))
                .with_header("x-flp-worker-key", key)
                .with_header("content-type", "application/json")
                .with_body(body.to_string()),
            "flp_worker",
            PEER_TIMEOUT,
        )
        .map_err(|_| "worker_delete_unavailable")?;
        let status = response.get_status().as_u16();
        if status != 200 {
            return Err(format!("worker_delete_http_{status}"));
        }
        bounded_json(response.take_body())
    }
    fn worker_dispatch(
        &self,
        requests: &[(String, Value)],
        deadline: std::time::Duration,
    ) -> Vec<Result<Value, String>> {
        let started = Instant::now();
        let mut results: Vec<Result<Value, String>> = requests
            .iter()
            .map(|_| Err("worker_deadline".into()))
            .collect();
        let worker_id = self.config("worker_service_id").ok().flatten();
        if worker_id.is_none() {
            return requests
                .iter()
                .map(|_| Err("worker_dispatch_forbidden".into()))
                .collect();
        }
        // A service can't call its own domain: the worker service's relay reaches the main one.
        let relaying = std::env::var("FASTLY_SERVICE_ID").ok() == worker_id;
        if relaying
            && requests.iter().any(|(path, _)| {
                ![
                    crate::ingest::worker::PATH,
                    crate::ingest::worker::RANGE_PATH,
                ]
                .contains(&path.as_str())
            })
        {
            return requests
                .iter()
                .map(|_| Err("worker_dispatch_forbidden".into()))
                .collect();
        }
        let (host_name, backend) = if relaying {
            ("compute_host", "flp_compute")
        } else {
            ("worker_host", "flp_worker")
        };
        let target = self.required(host_name).and_then(|host| {
            let key = self.secret("worker_secret")?.ok_or("missing_worker_key")?;
            Ok((host, key))
        });
        let (host, key) = match target {
            Ok(target) => target,
            Err(error) => return requests.iter().map(|_| Err(error.clone())).collect(),
        };
        let mut pending = Vec::new();
        for (index, (path, body)) in requests.iter().enumerate() {
            if let Err(error) = self.budget.charge() {
                results[index] = Err(error);
                continue;
            }
            let sent = Request::post(format!("https://{host}{path}"))
                .with_header("x-flp-worker-key", &key)
                .with_header("content-type", "application/json")
                .with_body(body.to_string())
                .with_pass(true)
                .send_async(backend);
            match sent {
                Ok(request) => {
                    self.worker_sent.set(self.worker_sent.get() + 1);
                    pending.push((index, request));
                }
                Err(error) => {
                    results[index] = Err(send_error_category(&error, "worker_send_failed").into())
                }
            }
        }
        while !pending.is_empty() && started.elapsed() < deadline {
            let mut waiting = Vec::new();
            for (index, request) in pending {
                match request.poll() {
                    fastly::http::request::PollResult::Done(Ok(mut response)) => {
                        let status = response.get_status().as_u16();
                        results[index] = if status == 200 {
                            bounded_json(response.take_body())
                        } else {
                            Err(format!("worker_http_{status}"))
                        };
                    }
                    fastly::http::request::PollResult::Done(Err(error)) => {
                        results[index] =
                            Err(send_error_category(&error, "worker_poll_failed").into());
                    }
                    fastly::http::request::PollResult::Pending(request) => {
                        waiting.push((index, request))
                    }
                }
            }
            pending = waiting;
            if !pending.is_empty() {
                std::thread::sleep(
                    std::time::Duration::from_millis(10)
                        .min(deadline.saturating_sub(started.elapsed())),
                );
            }
        }
        println!(
            "{}",
            json!({"event":"admin.external.call","service":"worker","requests":requests.len(),"unfinished":pending.len(),"duration_ms":started.elapsed().as_millis()})
        );
        results
    }
    fn vcpu_ms(&self) -> u64 {
        fastly::compute_runtime::elapsed_vcpu_ms().unwrap_or(0)
    }
    fn execution_vcpu_ms(&self) -> Option<u64> {
        fastly::compute_runtime::elapsed_vcpu_ms().ok()
    }
    fn heap_mib(&self) -> Option<u32> {
        fastly::compute_runtime::heap_memory_snapshot_mib().ok()
    }
    fn linear_memory_high_water_bytes(&self) -> Option<u64> {
        Some(core::arch::wasm32::memory_size::<0>() as u64 * 65_536)
    }
    fn pop(&self) -> Option<String> {
        Some(fastly::compute_runtime::pop().to_owned())
    }
    fn on_log_shield(&self) -> bool {
        fastly::shielding::Shield::new("iad-va-us")
            .map(|shield| shield.running_on())
            .unwrap_or(false)
    }
}

impl FastlyRuntime {
    /// Sends the GET of a streamed body to the front's `/fos/<service>/<key>` path, passing the
    /// client's `Range` on, and returns once the answer's headers arrive; its body streams.
    pub fn upstream(&self, upstream: &crate::http::Upstream) -> Result<fastly::Response, String> {
        let base = self.required("public_base_url")?;
        let host = url::Url::parse(&base)
            .ok()
            .and_then(|u| u.host_str().map(str::to_owned))
            .ok_or("public_base_url_invalid")?;
        self.budget.charge()?;
        let path = upstream
            .key
            .split('/')
            .map(crate::admin::transport::encode)
            .collect::<Vec<_>>()
            .join("/");
        let url = format!("https://{host}/fos/{}/{path}", upstream.service_id);
        let mut request = if upstream.head_only {
            Request::head(url)
        } else {
            Request::get(url)
        }
        .with_header("x-fastly-key", &upstream.cdn_key);
        if let Some(range) = &upstream.range {
            request.set_header("range", range);
        }
        let mut response = send_with_timeout(
            request,
            "flp_front",
            std::time::Duration::from_secs(30),
            false,
        )?;
        if upstream.head_only
            && upstream.range.as_deref() == Some("bytes=0-")
            && response.get_status().as_u16() == 200
        {
            if let Some(length) = response
                .get_header_str("content-length")
                .and_then(|length| length.parse::<u64>().ok())
                .filter(|length| *length > 0)
            {
                response.set_status(206);
                response.set_header("accept-ranges", "bytes");
                response.set_header("content-range", format!("bytes 0-{}/{length}", length - 1));
            }
        }
        Ok(response)
    }
}

impl FastlyRuntime {
    /// A GET of `key` through the Front VCL CDN; charges one backend call.
    fn cdn_request(
        &self,
        service_id: &str,
        key: &str,
        cdn_key: &str,
        range: Option<(u64, u64)>,
    ) -> Result<Request, String> {
        if cdn_key.is_empty() {
            return Err("backend_deadline".into());
        }
        let base = self.required("public_base_url")?;
        let host = url::Url::parse(&base)
            .ok()
            .and_then(|u| u.host_str().map(str::to_owned))
            .ok_or("public_base_url_invalid")?;
        self.budget.charge()?;
        let path = key
            .split('/')
            .map(crate::admin::transport::encode)
            .collect::<Vec<_>>()
            .join("/");
        let mut request = Request::get(format!("https://{host}/fos/{service_id}/{path}"))
            .with_header("x-fastly-key", cdn_key);
        if let Some((start, end)) = range {
            request.set_header("range", format!("bytes={start}-{end}"));
        }
        Ok(request.with_pass(false))
    }

    /// Sends speculative CDN reads of `keys` not already sent and leaves their answers unread,
    /// so a stalled header or body never holds the batch's required read. A later
    /// `admin_cdn_read` of the key waits for and reads it under that read's own deadline.
    fn send_unread(&self, service_id: &str, keys: &[(String, usize)], cdn_key: &str) {
        for (key, limit) in keys {
            let id = format!("{service_id}/{key}");
            if self.unread.borrow().contains_key(&id) {
                continue;
            }
            let Ok(request) = self.cdn_request(service_id, key, cdn_key, None) else {
                break;
            };
            match request.send_async("flp_front") {
                Ok(pending) => {
                    self.unread
                        .borrow_mut()
                        .insert(id, (Instant::now(), pending, *limit));
                }
                Err(error) => self.record_cdn_get(
                    key,
                    std::time::Duration::ZERO,
                    &Err(send_error_category(&error, "backend_send_failed").into()),
                ),
            }
        }
    }

    fn record_cdn_get(
        &self,
        key: &str,
        duration: std::time::Duration,
        result: &Result<crate::admin::FosResponse, String>,
    ) {
        if let Ok(response) = result {
            self.usage.record_cdn(
                key,
                Some(response.status),
                response.cdn_cache.as_deref(),
                duration.as_millis() as u64,
                response.body.len() as u64,
            );
        } else {
            self.usage
                .record_cdn(key, None, None, duration.as_millis() as u64, 0);
        }
        println!(
            "{}",
            json!({"event":"admin.external.call","level":if result.is_err() {"error"} else {"info"},"service":"cdn","method":"GET","status":result.as_ref().ok().map(|r| r.status),"error":result.as_ref().err(),"duration_ms":duration.as_millis()})
        );
    }
}

fn cdn_response(
    mut response: fastly::Response,
    max_bytes: usize,
) -> Result<crate::admin::FosResponse, String> {
    let status = response.get_status().as_u16();
    let cdn_cache = response.get_header_str("x-flp-fos-cache").map(|val| {
        if val == "MISS"
            && (response
                .get_header_str("age")
                .and_then(|a| a.parse::<u64>().ok())
                .is_some_and(|a| a > 0)
                || response
                    .get_header_str("x-cache")
                    .is_some_and(|c| c.to_ascii_uppercase().contains("HIT")))
        {
            "HIT".to_owned()
        } else {
            val.to_owned()
        }
    });
    let retry_after =
        crate::admin::transport::parse_retry_after(response.get_header_str("retry-after"));
    let mut bytes = Vec::new();
    response
        .take_body()
        .take(max_bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "cdn_body_unavailable")?;
    if bytes.len() > max_bytes {
        return Err("cdn_body_too_large".into());
    }
    Ok(crate::admin::FosResponse {
        retry_after,
        status,
        body: if status == 200 || status == 206 {
            bytes
        } else {
            Vec::new()
        },
        cdn_cache,
    })
}

impl FastlyRuntime {
    fn fos_call(
        &self,
        input: crate::admin::FosRequest<'_>,
    ) -> Result<(crate::admin::FosResponse, Option<String>), String> {
        let method = input.method;
        let started = Instant::now();
        let timeout = input.timeout.min(FOS_TIMEOUT);
        let mut stage = "budget";
        let mut status = None;
        let mut bytes_read = 0;
        let bytes_in = input.body.len() as u64;
        let (usage_key, usage_query) = (input.key, input.query);
        let result: Result<_, String> = (|| {
            if timeout.is_zero() {
                return Err("backend_deadline".into());
            }
            self.budget.charge()?;
            stage = "request";
            if !crate::admin::REGIONS.contains(&input.region)
                || !crate::admin::valid_bucket(input.bucket)
            {
                return Err("invalid_fos_target".into());
            }
            let host = format!("{}.object.fastlystorage.app", input.region);
            let path = crate::admin::fos_path(input.bucket, input.key);
            let query = if input.query.is_empty() {
                String::new()
            } else {
                format!("?{}", input.query)
            };
            let mut request = Request::new(input.method, format!("https://{host}{path}{query}"));
            for (name, value) in crate::admin::fos_headers(&input, self.now())? {
                request.set_header(name, value);
            }
            if !input.body.is_empty() {
                // POST bodies (bulk delete, multipart complete) are XML whatever the key.
                let content_type = if input.method == "POST" {
                    "application/xml"
                } else if input.key.ends_with(".json") {
                    "application/json"
                } else if input.key.ends_with(".js") {
                    "application/javascript; charset=utf-8"
                } else if input.key.ends_with(".parquet") {
                    "application/vnd.apache.parquet"
                } else {
                    "application/xml"
                };
                request = request
                    .with_header("content-type", content_type)
                    .with_body(input.body);
            }
            stage = "headers";
            let mut response = bounded_send_with_timeout(
                request,
                &format!("fos_{}", input.region.replace('-', "_")),
                timeout.saturating_sub(started.elapsed()),
            )?;
            let response_status = response.get_status().as_u16();
            status = Some(response_status);
            let etag = response.get_header_str("etag").map(str::to_owned);
            let retry_after =
                crate::admin::transport::parse_retry_after(response.get_header_str("retry-after"));
            stage = "body";
            let bytes = crate::admin::transport::read_fos_body(
                response.take_body(),
                response_status,
                input.response_limit,
                &mut bytes_read,
            )?;
            if started.elapsed() >= timeout {
                return Err("backend_deadline".into());
            }
            Ok((
                crate::admin::FosResponse {
                    retry_after,
                    status: response_status,
                    body: bytes,
                    cdn_cache: None,
                },
                etag,
            ))
        })();
        self.usage.record_fos(
            method,
            usage_key,
            usage_query,
            status,
            started.elapsed().as_millis() as u64,
            bytes_in,
            bytes_read as u64,
        );
        println!(
            "{}",
            json!({"event":"admin.external.call","level":if result.is_err() {"error"} else {"info"},"service":"fos","method":method,"status":status,"stage":stage,"error":result.as_ref().err(),"timeout_ms":timeout.as_millis(),"duration_ms":started.elapsed().as_millis(),"bytes":bytes_read})
        );
        result
    }
}

fn bounded_send_with_timeout(
    request: Request,
    backend: &str,
    timeout: std::time::Duration,
) -> Result<fastly::Response, String> {
    send_with_timeout(request, backend, timeout, true)
}

fn send_with_timeout(
    request: Request,
    backend: &str,
    timeout: std::time::Duration,
    pass: bool,
) -> Result<fastly::Response, String> {
    if timeout.is_zero() {
        return Err("backend_deadline".into());
    }
    let started = Instant::now();
    let pending = request
        .with_pass(pass)
        .send_async(backend)
        .map_err(|error| send_error_category(&error, "backend_send_failed"))?;
    wait_pending(pending, started, timeout)
}

/// Polls `pending` until its headers arrive or `timeout` after `started` passes.
fn wait_pending(
    mut pending: fastly::http::request::PendingRequest,
    started: Instant,
    timeout: std::time::Duration,
) -> Result<fastly::Response, String> {
    loop {
        if started.elapsed() >= timeout {
            return Err("backend_deadline".into());
        }
        pending = match pending.poll() {
            fastly::http::request::PollResult::Done(result) => {
                return result
                    .map_err(|error| send_error_category(&error, "backend_poll_failed").into())
            }
            fastly::http::request::PollResult::Pending(pending) => pending,
        };
        std::thread::sleep(
            std::time::Duration::from_millis(10).min(timeout.saturating_sub(started.elapsed())),
        );
    }
}

fn send_error_category(
    error: &fastly::http::request::SendError,
    fallback: &'static str,
) -> &'static str {
    use fastly::http::request::SendErrorCause;
    match error.root_cause() {
        SendErrorCause::DnsTimeout => "backend_dns_timeout",
        SendErrorCause::DnsError { .. } => "backend_dns_error",
        SendErrorCause::DestinationNotFound => "backend_not_found",
        SendErrorCause::DestinationUnavailable => "backend_unavailable",
        SendErrorCause::DestinationIpUnroutable => "backend_unroutable",
        SendErrorCause::ConnectionRefused => "backend_connection_refused",
        SendErrorCause::ConnectionTerminated => "backend_connection_terminated",
        SendErrorCause::ConnectionTimeout => "backend_connection_timeout",
        SendErrorCause::ConnectionLimitReached => "backend_connection_limit",
        SendErrorCause::TlsProtocolError => "backend_tls_protocol_error",
        SendErrorCause::TlsCertificateError => "backend_tls_certificate_error",
        SendErrorCause::TlsAlertReceived { .. } => "backend_tls_alert",
        SendErrorCause::TlsConfigurationError => "backend_tls_configuration_error",
        SendErrorCause::HttpResponseTimeout => "backend_response_timeout",
        SendErrorCause::HttpIncompleteResponse => "backend_incomplete_response",
        SendErrorCause::HttpResponseHeaderSectionTooLarge
        | SendErrorCause::HttpResponseBodyTooLarge => "backend_response_limit",
        SendErrorCause::HttpProtocolError | SendErrorCause::HttpResponseStatusInvalid => {
            "backend_http_protocol_error"
        }
        SendErrorCause::IoError(_) => "backend_io_error",
        SendErrorCause::InternalError(detail) => {
            eprintln!(
                "flp backend_internal_error backend={} detail={detail:?}",
                error.backend_name()
            );
            "backend_internal_error"
        }
        _ => fallback,
    }
}

fn api_resource(path: &str) -> &'static str {
    if path.contains("/secrets") {
        "secret"
    } else if path.contains("/dictionary/") {
        "dictionary"
    } else if path.contains("/access-keys") {
        "fos_key"
    } else if path.contains("/version/") {
        "version"
    } else {
        "service"
    }
}

struct FastlyApi<'a> {
    runtime: &'a FastlyRuntime,
    token: String,
}
impl CheckApi for FastlyApi<'_> {
    fn get(&mut self, path: &str) -> Result<(u16, Value), String> {
        let response = self.runtime.admin_api(crate::admin::ApiRequest {
            method: "GET",
            path,
            query: "",
            token: &self.token,
            body: None,
            timeout: FASTLY_API_TIMEOUT,
        })?;
        Ok((response.status, response.body))
    }
}

fn bounded_json(body: fastly::Body) -> Result<Value, String> {
    let mut bytes = Vec::new();
    body.take(2_097_153)
        .read_to_end(&mut bytes)
        .map_err(|_| "backend_body_unavailable")?;
    if bytes.len() > 2_097_152 {
        return Err("backend_response_too_large".into());
    }
    if bytes.is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_slice(&bytes).map_err(|_| "backend_json_invalid".into())
}

struct Probe<'a> {
    runtime: &'a FastlyRuntime,
}
impl CheckProbe for Probe<'_> {
    fn probe(&mut self, host: &str, path: &str) -> Result<u16, String> {
        if host == self.runtime.required("compute_host")? && path == "/" {
            let worker = self.runtime.required("worker_host")?;
            return crate::shell::unauthenticated_probe_status(self.runtime, || {
                bounded_send_with_timeout(
                    Request::get(format!("https://{worker}/")),
                    "flp_worker",
                    PEER_TIMEOUT,
                )
                .map(|response| response.get_status().as_u16())
                .map_err(|_| "peer_probe_unavailable".into())
            });
        }
        if host != "us-east-1.object.fastlystorage.app" || path != "/" {
            return Err("probe_target_invalid".into());
        }
        bounded_send_with_timeout(
            Request::get(format!("https://{host}/")),
            "fos_us_east_1",
            FOS_TIMEOUT,
        )
        .map(|response| response.get_status().as_u16())
        .map_err(|_| "fos_probe_unavailable".into())
    }
}

/// Shard requests relayed to the main service by the worker service's `RELAY_PATH`.
pub fn is_relayed_shard(request: &crate::http::Request) -> bool {
    request.method == "POST"
        && request.header("x-flp-worker-key").is_some()
        && [
            crate::ingest::worker::PATH,
            crate::ingest::worker::RANGE_PATH,
        ]
        .contains(&request.path.as_str())
}

pub fn worker(request: &crate::http::Request, runtime: &FastlyRuntime) -> crate::http::Response {
    if !crate::ingest::worker::authorized(runtime, request) {
        return crate::http::Response::new(403, "Forbidden").private();
    }
    if (request.method.as_str(), request.path.as_str()) == ("POST", "/_internal/delete") {
        return crate::ingest::retention::handle_worker_delete(runtime, request);
    }
    if request.method == "POST"
        && [
            crate::ingest::worker::PATH,
            crate::ingest::worker::RANGE_PATH,
        ]
        .contains(&request.path.as_str())
    {
        return crate::ingest::worker::handle_worker_ingest(runtime, request);
    }
    if (request.method.as_str(), request.path.as_str())
        == ("POST", crate::ingest::worker::RELAY_PATH)
    {
        return crate::ingest::worker::handle_worker_relay(runtime, request);
    }
    if (request.method.as_str(), request.path.as_str()) == ("POST", crate::admin::stage::PATH) {
        return crate::admin::stage::handle_worker_stage(runtime, request);
    }
    let data = match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/healthz") => json!({"healthy":true}),
        ("GET", "/version") => json!({"build_sha":env!("FLP_BUILD_SHA")}),
        ("GET", "/_internal/probe") => {
            let result = runtime.required("compute_host").and_then(|host| {
                bounded_send_with_timeout(
                    Request::get(format!("https://{host}/")),
                    "flp_compute",
                    PEER_TIMEOUT,
                )
                .map(|response| response.get_status().as_u16())
                .map_err(|_| "direct_probe_unavailable".into())
            });
            match result {
                Ok(status) => json!({"status":status}),
                Err(_) => return crate::http::Response::new(503, "Probe unavailable").private(),
            }
        }
        _ => return crate::http::Response::new(404, "Not found").private(),
    };
    crate::http::Response::new(200, data.to_string())
        .header("content-type", "application/json")
        .private()
}

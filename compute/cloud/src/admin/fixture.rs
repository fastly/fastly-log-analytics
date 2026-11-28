use super::{ApiRawRequest, ApiRequest, FosRequest, FosResponse, Record};
use base64::Engine;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Mutex,
};

#[derive(Clone)]
struct Version {
    number: u64,
    active: bool,
    locked: bool,
    comment: String,
    resources: BTreeMap<String, BTreeMap<String, Value>>,
}
#[derive(Default)]
struct State {
    generation: u64,
    records: BTreeMap<String, Record>,
    secrets: BTreeMap<String, String>,
    versions: BTreeMap<String, Vec<Version>>,
    services: BTreeMap<String, Value>,
    keys: Vec<Value>,
    buckets: BTreeMap<String, BTreeMap<String, Vec<u8>>>,
    dictionaries: BTreeMap<String, BTreeMap<String, String>>,
    sequence: u64,
    calls: Vec<(String, String)>,
    fail: Option<(String, String, bool, usize)>,
    invalid_draft: bool,
    secret_reads: BTreeMap<String, Result<Option<String>, String>>,
    kv_clock_ms: Option<u64>,
    kv_waits: usize,
    kv_visibility_delay: u32,
    stale_records: BTreeMap<String, (u32, Option<Record>)>,
    confirmation_delay: u32,
    stale_confirmations: BTreeMap<String, (u32, Option<Record>)>,
    kv_cache: BTreeMap<String, Option<Record>>,
    config_stores: BTreeMap<String, (String, BTreeMap<String, String>)>,
    kv_stores: BTreeMap<String, (String, BTreeMap<String, Vec<u8>>)>,
    packages: BTreeMap<(String, u64), Vec<u8>>,
    disabled_products: BTreeSet<String>,
    pragmas_missing: BTreeSet<String>,
}

/// Deterministic native-only boundary. It has no network client or real credential path.
#[derive(Default)]
pub struct FixtureAdapter {
    state: Mutex<State>,
}

impl FixtureAdapter {
    pub fn new() -> Self {
        let mut state = State::default();
        for (sid, name, kind) in [
            ("fixtureApp", "Fixture app", "wasm"),
            ("fixtureWorker", "Fixture worker", "wasm"),
            ("fixtureFront", "Fixture front", "vcl"),
            ("fixtureService", "Fixture delivery service", "vcl"),
            ("fixtureOther", "Other delivery service", "vcl"),
        ] {
            state.services.insert(
                sid.into(),
                json!({"id":sid,"name":name,"type":kind,"customer_id":"fixtureCustomer"}),
            );
            state.versions.insert(
                sid.into(),
                vec![Version {
                    number: 1,
                    active: true,
                    locked: false,
                    comment: String::new(),
                    resources: BTreeMap::new(),
                }],
            );
        }
        Self {
            state: Mutex::new(state),
        }
    }
    pub fn cached_get(&self, key: &str) -> Option<Option<Record>> {
        if !crate::shell::is_cacheable_kv_key(key) {
            return None;
        }
        let state = self.state.lock().ok()?;
        if state
            .fail
            .as_ref()
            .is_some_and(|(m, p, _, _)| m == "KV_GET" && key.starts_with(p))
        {
            return None;
        }
        if state
            .stale_records
            .get(key)
            .is_some_and(|(remaining, _)| *remaining > 0)
        {
            return None;
        }
        state.kv_cache.get(key).cloned()
    }
    pub fn cache_invalidate(&self, key: &str) {
        if let Ok(mut state) = self.state.lock() {
            state.kv_cache.remove(key);
        }
    }
    pub fn get(&self, key: &str) -> Result<Option<Record>, String> {
        let mut state = self.state.lock().map_err(|_| "fixture_lock")?;
        state.calls.push(("KV_GET".into(), key.into()));
        if matches_failure(&mut state, "KV_GET", key, false) {
            return Err("injected_failure".into());
        }
        if let Some((remaining, record)) = state.stale_records.get_mut(key) {
            if *remaining > 0 {
                *remaining -= 1;
                return Ok(record.clone());
            }
        }
        let record = state.records.get(key).cloned();
        if crate::shell::is_cacheable_kv_key(key) {
            state.kv_cache.insert(key.into(), record.clone());
        }
        Ok(record)
    }
    pub fn cas(&self, key: &str, generation: Option<&str>, value: &Value) -> Result<bool, String> {
        let mut state = self.state.lock().map_err(|_| "fixture_lock")?;
        state.kv_cache.remove(key);
        state.calls.push(("KV_CAS".into(), key.into()));
        if matches_failure(&mut state, "KV_CAS", key, false) {
            return Err("injected_failure".into());
        }
        if state.records.get(key).map(|r| r.generation.as_str()) != generation {
            return Ok(false);
        }
        let mut value = value.clone();
        if let Some(now) = state.kv_clock_ms {
            if let Some(last) = state
                .records
                .get(key)
                .and_then(|r| r.value["_kv_written_ms"].as_u64())
            {
                if now.saturating_sub(last) < 1000 {
                    return Err("kv_write_rate_exceeded".into());
                }
            }
            value["_kv_written_ms"] = now.into();
        }
        state.generation += 1;
        let generation = state.generation.to_string();
        let previous = state.records.get(key).cloned();
        let delay = state.kv_visibility_delay;
        if delay > 0 {
            state
                .stale_records
                .insert(key.into(), (delay, previous.clone()));
        }
        let delay = state.confirmation_delay;
        if delay > 0 {
            state
                .stale_confirmations
                .insert(key.into(), (delay, previous));
        }
        state
            .records
            .insert(key.into(), Record { generation, value });
        if matches_failure(&mut state, "KV_CAS", key, true) {
            return Err("injected_lost_ack".into());
        }
        Ok(true)
    }
    pub fn delete(&self, key: &str) -> Result<(), String> {
        let mut state = self.state.lock().map_err(|_| "fixture_lock")?;
        state.kv_cache.remove(key);
        state.calls.push(("KV_DELETE".into(), key.into()));
        if matches_failure(&mut state, "KV_DELETE", key, false) {
            return Err("injected_failure".into());
        }
        state.records.remove(key);
        Ok(())
    }
    pub fn confirm(&self, key: &str) -> Result<Option<Record>, String> {
        let mut state = self.state.lock().map_err(|_| "fixture_lock")?;
        state.calls.push(("KV_CONFIRM".into(), key.into()));
        if matches_failure(&mut state, "KV_CONFIRM", key, false) {
            return Err("injected_failure".into());
        }
        if let Some((remaining, record)) = state.stale_confirmations.get_mut(key) {
            if *remaining > 0 {
                *remaining -= 1;
                return Ok(record.clone());
            }
        }
        let record = state.records.get(key).cloned();
        if crate::shell::is_cacheable_kv_key(key) {
            state.kv_cache.insert(key.into(), record.clone());
        }
        Ok(record)
    }
    pub fn secret(&self, name: &str) -> Result<Option<String>, String> {
        let state = self.state.lock().map_err(|_| "fixture_lock")?;
        if let Some((_, value)) = state
            .secret_reads
            .iter()
            .find(|(prefix, _)| name.starts_with(prefix.as_str()))
        {
            return value.clone();
        }
        Ok(state.secrets.get(name).cloned())
    }
    pub fn enforce_kv_write_rate(&self) {
        self.state.lock().expect("fixture lock").kv_clock_ms = Some(1_800_000_000_000);
    }
    pub fn delay_kv_visibility(&self, reads: u32) {
        self.state.lock().expect("fixture lock").kv_visibility_delay = reads;
    }
    pub fn delay_confirmation_visibility(&self, reads: u32) {
        self.state.lock().expect("fixture lock").confirmation_delay = reads;
    }
    pub fn stale_kv_read(&self, key: &str, reads: u32, record: Option<Record>) {
        self.state
            .lock()
            .expect("fixture lock")
            .stale_confirmations
            .insert(key.into(), (reads, record));
    }
    /// Plants `record` in the read cache, as an edge cache can after a read lands inside KV's
    /// propagation window.
    pub fn cache_stale(&self, key: &str, record: Option<Record>) {
        self.state
            .lock()
            .expect("fixture lock")
            .kv_cache
            .insert(key.into(), record);
    }
    pub fn advance_kv_clock(&self, millis: u64) {
        let mut state = self.state.lock().expect("fixture lock");
        if let Some(now) = state.kv_clock_ms.as_mut() {
            *now += millis;
        }
    }
    pub fn kv_waits(&self) -> usize {
        self.state.lock().expect("fixture lock").kv_waits
    }
    pub fn paced_cas(
        &self,
        key: &str,
        generation: Option<&str>,
        value: &Value,
    ) -> Result<bool, String> {
        {
            let mut state = self.state.lock().map_err(|_| "fixture_lock")?;
            if let (Some(now), Some(last)) = (
                state.kv_clock_ms,
                state
                    .records
                    .get(key)
                    .and_then(|r| r.value["_kv_written_ms"].as_u64()),
            ) {
                let delay = super::kv_write_delay_ms(last, now)?;
                if delay > 0 {
                    state.kv_clock_ms = Some(now + delay);
                    state.kv_waits += 1;
                }
            }
        }
        self.cas(key, generation, value)
    }
    pub fn override_secret_reads(&self, prefix: &str, value: Result<Option<String>, String>) {
        self.state
            .lock()
            .expect("fixture lock")
            .secret_reads
            .insert(prefix.into(), value);
    }
    pub fn clear_secret_reads(&self) {
        self.state
            .lock()
            .expect("fixture lock")
            .secret_reads
            .clear();
    }
    pub fn api(&self, request: ApiRequest<'_>) -> Result<(u16, Value), String> {
        let mut state = self.state.lock().map_err(|_| "fixture_lock")?;
        state
            .calls
            .push((request.method.into(), request.path.into()));
        if matches_failure(&mut state, request.method, request.path, false) {
            return Err("injected_failure".into());
        }
        if !matches!(
            request.token,
            "fixture-token" | "fixture-read-only" | "fixture-wrong-account" | "fixture-limited"
        ) {
            return Ok((401, json!({"error":"invalid_token"})));
        }
        let response = api(&mut state, &request)?;
        if matches_failure(&mut state, request.method, request.path, true) {
            return Err("injected_lost_ack".into());
        }
        Ok(response)
    }
    pub fn api_raw(&self, request: ApiRawRequest<'_>) -> Result<(u16, Value), String> {
        let mut state = self.state.lock().map_err(|_| "fixture_lock")?;
        state
            .calls
            .push((request.method.into(), request.path.into()));
        if matches_failure(&mut state, request.method, request.path, false) {
            return Err("injected_failure".into());
        }
        if !matches!(
            request.token,
            "fixture-token" | "fixture-read-only" | "fixture-wrong-account" | "fixture-limited"
        ) {
            return Ok((401, json!({"error":"invalid_token"})));
        }
        let parts: Vec<&str> = request.path.trim_matches('/').split('/').collect();
        match (request.method, parts.as_slice()) {
            ("PUT", ["service", sid, "version", num, "package"]) => {
                let ver_num: u64 = num.parse().map_err(|_| "fixture_invalid_version")?;
                let v = version_mut(&mut state, sid, num)?;
                if v.locked || v.active {
                    return Ok((400, json!({"error":"version_locked"})));
                }
                state
                    .packages
                    .insert(((*sid).to_owned(), ver_num), request.body.to_vec());
                Ok((200, json!({"id": format!("pkg-{sid}-{ver_num}")})))
            }
            ("PUT", ["resources", "stores", "kv", store_id, "keys", key]) => {
                let Some((_, entries)) = state.kv_stores.get_mut(*store_id) else {
                    return Ok((404, json!({})));
                };
                entries.insert((*key).to_owned(), request.body.to_vec());
                Ok((200, json!({"status":"ok"})))
            }
            _ => Err(format!(
                "fixture_raw_unhandled:{}:{}",
                request.method, request.path
            )),
        }
    }
    pub fn fos(&self, request: FosRequest<'_>) -> Result<FosResponse, String> {
        let mut state = self.state.lock().map_err(|_| "fixture_lock")?;
        let path = format!("/{}/{}", request.bucket, request.key);
        state.calls.push((request.method.into(), path.clone()));
        if matches_failure(&mut state, request.method, &path, false) {
            return Err("injected_failure".into());
        }
        let key = state.keys.iter().find(|k| {
            k["access_key"] == request.access_key && k["secret_key"] == request.secret_key
        });
        let authorized = key.is_some_and(|k| {
            k["permission"] == "read-write-admin"
                || k["buckets"]
                    .as_array()
                    .is_some_and(|b| b.iter().any(|b| b == request.bucket))
        });
        if !authorized {
            return Ok(FosResponse {
                cdn_cache: None,
                status: 403,
                body: vec![],
                retry_after: None,
            });
        }
        let result = match request.method {
            "HEAD" => FosResponse {
                cdn_cache: None,
                status: if state.buckets.contains_key(request.bucket) {
                    200
                } else {
                    404
                },
                body: vec![],
                retry_after: None,
            },
            "PUT" => {
                if request.key.is_empty() {
                    if state.buckets.contains_key(request.bucket) {
                        FosResponse {
                            cdn_cache: None,
                            status: 409,
                            body: vec![],
                            retry_after: None,
                        }
                    } else {
                        state.buckets.insert(request.bucket.into(), BTreeMap::new());
                        FosResponse {
                            cdn_cache: None,
                            status: 200,
                            body: vec![],
                            retry_after: None,
                        }
                    }
                } else if let Some(bucket) = state.buckets.get_mut(request.bucket) {
                    bucket.insert(request.key.into(), request.body);
                    FosResponse {
                        cdn_cache: None,
                        status: 200,
                        body: vec![],
                        retry_after: None,
                    }
                } else {
                    FosResponse {
                        cdn_cache: None,
                        status: 404,
                        body: vec![],
                        retry_after: None,
                    }
                }
            }
            "GET" => {
                if let Some(bucket) = state.buckets.get(request.bucket) {
                    if !request.key.is_empty() {
                        if let Some(object) = bucket.get(request.key) {
                            FosResponse {
                                cdn_cache: None,
                                status: 200,
                                body: object.clone(),
                                retry_after: None,
                            }
                        } else {
                            FosResponse {
                                cdn_cache: None,
                                status: 404,
                                body: vec![],
                                retry_after: None,
                            }
                        }
                    } else {
                        let query_pairs: BTreeMap<String, String> =
                            url::form_urlencoded::parse(request.query.as_bytes())
                                .into_owned()
                                .collect();
                        let prefix = query_pairs.get("prefix").cloned().unwrap_or_default();
                        let delim = query_pairs
                            .get("delimiter")
                            .cloned()
                            .filter(|d| !d.is_empty());
                        let start_after = query_pairs
                            .get("start-after")
                            .cloned()
                            .filter(|s| !s.is_empty());
                        let mut contents = String::new();
                        let mut prefixes = BTreeSet::new();
                        for (k, v) in bucket.iter() {
                            if !k.starts_with(&prefix) {
                                continue;
                            }
                            if let Some(ref after) = start_after {
                                if k.as_str() <= after.as_str() {
                                    continue;
                                }
                            }
                            let rest = &k[prefix.len()..];
                            if let Some(ref d) = delim {
                                if let Some(idx) = rest.find(d.as_str()) {
                                    prefixes.insert(format!("{prefix}{}", &rest[..idx + d.len()]));
                                    continue;
                                }
                            }
                            contents.push_str(&format!(
                                "<Contents><Key>{}</Key><Size>{}</Size></Contents>",
                                quick_xml::escape::escape(k),
                                v.len()
                            ));
                        }
                        let mut pxml = String::new();
                        for p in prefixes {
                            pxml.push_str(&format!(
                                "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                                quick_xml::escape::escape(&p)
                            ));
                        }
                        FosResponse {
                            cdn_cache: None,
                            status: 200,
                            body: format!(
                                "<ListBucketResult><IsTruncated>false</IsTruncated>{contents}{pxml}</ListBucketResult>"
                            )
                            .into_bytes(),
                            retry_after: None,
                        }
                    }
                } else {
                    FosResponse {
                        cdn_cache: None,
                        status: 404,
                        body: vec![],
                        retry_after: None,
                    }
                }
            }
            "DELETE" => {
                let status = if request.key.is_empty() {
                    if state
                        .buckets
                        .get(request.bucket)
                        .is_some_and(|b| !b.is_empty())
                    {
                        409
                    } else {
                        state.buckets.remove(request.bucket);
                        204
                    }
                } else {
                    if let Some(bucket) = state.buckets.get_mut(request.bucket) {
                        bucket.remove(request.key);
                    }
                    204
                };
                FosResponse {
                    cdn_cache: None,
                    status,
                    body: vec![],
                    retry_after: None,
                }
            }
            "POST" if request.query == "delete" => {
                let xml = String::from_utf8_lossy(&request.body);
                let mut out = String::from("<DeleteResult>");
                if let Some(bucket) = state.buckets.get_mut(request.bucket) {
                    for part in xml.split("<Key>").skip(1) {
                        if let Some(k) = part.split("</Key>").next() {
                            let unescaped = k
                                .replace("&amp;", "&")
                                .replace("&lt;", "<")
                                .replace("&gt;", ">");
                            bucket.remove(&unescaped);
                            out.push_str(&format!(
                                "<Deleted><Key>{}</Key></Deleted>",
                                quick_xml::escape::escape(&unescaped)
                            ));
                        }
                    }
                }
                out.push_str("</DeleteResult>");
                FosResponse {
                    cdn_cache: None,
                    status: 200,
                    body: out.into_bytes(),
                    retry_after: None,
                }
            }
            _ => return Err("fixture_invalid_fos_method".into()),
        };
        if matches_failure(&mut state, request.method, &path, true) {
            return Err("injected_lost_ack".into());
        }
        Ok(result)
    }
    pub fn fail_next(&self, method: &str, path: &str, after_effect: bool) {
        self.fail_on_nth(method, path, after_effect, 1);
    }
    pub fn fail_on_nth(&self, method: &str, path: &str, after_effect: bool, nth: usize) {
        self.state.lock().expect("fixture lock").fail =
            Some((method.into(), path.into(), after_effect, nth));
    }
    pub fn calls(&self) -> Vec<(String, String)> {
        self.state.lock().expect("fixture lock").calls.clone()
    }
    pub fn clear_calls(&self) {
        self.state.lock().expect("fixture lock").calls.clear();
    }
    pub fn records(&self) -> BTreeMap<String, Record> {
        self.state.lock().expect("fixture lock").records.clone()
    }
    pub fn bucket_objects(&self, bucket: &str) -> BTreeMap<String, Vec<u8>> {
        self.state
            .lock()
            .expect("fixture lock")
            .buckets
            .get(bucket)
            .cloned()
            .unwrap_or_default()
    }
    pub fn resources(&self, sid: &str, resource: &str) -> Vec<Value> {
        self.state
            .lock()
            .expect("fixture lock")
            .versions
            .get(sid)
            .and_then(|v| v.iter().find(|v| v.active))
            .and_then(|v| v.resources.get(resource))
            .map(|v| v.values().cloned().collect())
            .unwrap_or_default()
    }
    pub fn key_count(&self) -> usize {
        self.state.lock().expect("fixture lock").keys.len()
    }
    pub fn seed_resource(&self, sid: &str, resource: &str, mut value: Value) {
        let mut state = self.state.lock().expect("fixture lock");
        let minted_rl_seq = if resource == "resource" && value["id"].as_str().is_none() {
            state.sequence += 1;
            Some(state.sequence)
        } else {
            None
        };
        let version = state
            .versions
            .get_mut(sid)
            .expect("fixture service")
            .iter_mut()
            .find(|v| v.active)
            .expect("fixture version");
        let name = value["name"]
            .as_str()
            .expect("fixture resource name")
            .to_owned();
        if resource == "dictionary" && value["id"].as_str().is_none() {
            value["id"] = format!("dict-{sid}-{name}").into();
        }
        if let Some(seq) = minted_rl_seq {
            value["id"] = format!("rl-{}-{seq}", version.number).into();
        }
        version
            .resources
            .entry(resource.into())
            .or_default()
            .insert(name, value);
    }
    pub fn remove_resource(&self, sid: &str, resource: &str, name: &str) -> Option<Value> {
        self.state
            .lock()
            .expect("fixture lock")
            .versions
            .get_mut(sid)?
            .iter_mut()
            .find(|v| v.active)?
            .resources
            .get_mut(resource)?
            .remove(name)
    }
    pub fn seed_object(&self, bucket: &str, key: &str) {
        self.seed_object_bytes(bucket, key, vec![1]);
    }
    pub fn seed_object_bytes(&self, bucket: &str, key: &str, body: Vec<u8>) {
        self.state
            .lock()
            .expect("fixture lock")
            .buckets
            .entry(bucket.into())
            .or_default()
            .insert(key.into(), body);
    }
    pub fn get_object_bytes(&self, bucket: &str, key: &str) -> Option<Vec<u8>> {
        self.state
            .lock()
            .expect("fixture lock")
            .buckets
            .get(bucket)
            .and_then(|b| b.get(key).cloned())
    }
    pub fn seed_fos_key(&self, access_key: &str, secret_key: &str, bucket: &str) {
        self.state.lock().expect("fixture lock").keys.push(json!({
            "access_key": access_key,
            "secret_key": secret_key,
            "permission": "read-write-objects",
            "description": format!("seeded-{bucket}"),
            "buckets": [bucket],
        }));
    }
    pub fn object_keys(&self, bucket: &str) -> Vec<String> {
        self.state
            .lock()
            .expect("fixture lock")
            .buckets
            .get(bucket)
            .map(|b| b.keys().cloned().collect())
            .unwrap_or_default()
    }
    pub fn dictionary_items(&self, dictionary: &str) -> BTreeMap<String, String> {
        self.state
            .lock()
            .expect("fixture lock")
            .dictionaries
            .get(dictionary)
            .cloned()
            .unwrap_or_default()
    }
    pub fn invalid_draft(&self, value: bool) {
        self.state.lock().expect("fixture lock").invalid_draft = value;
    }
    pub fn change_customer(&self, sid: &str, customer: &str) {
        self.state
            .lock()
            .expect("fixture lock")
            .services
            .get_mut(sid)
            .expect("fixture service")["customer_id"] = customer.into();
    }
    pub fn disable_product(&self, product: &str) {
        self.state
            .lock()
            .expect("fixture lock")
            .disabled_products
            .insert(product.into());
    }
    pub fn set_pragmas_missing(&self, sid: &str, missing: bool) {
        let mut state = self.state.lock().expect("fixture lock");
        if missing {
            state.pragmas_missing.insert(sid.into());
        } else {
            state.pragmas_missing.remove(sid);
        }
    }
    pub fn kv_store_value(&self, store_id: &str, key: &str) -> Option<Vec<u8>> {
        self.state
            .lock()
            .expect("fixture lock")
            .kv_stores
            .get(store_id)
            .and_then(|(_, m)| m.get(key))
            .cloned()
    }
    pub fn config_store_value(&self, store_id: &str, key: &str) -> Option<String> {
        self.state
            .lock()
            .expect("fixture lock")
            .config_stores
            .get(store_id)
            .and_then(|(_, m)| m.get(key))
            .cloned()
    }
    pub fn record_snapshot(&self, key: &str) -> Option<Record> {
        self.state
            .lock()
            .expect("fixture lock")
            .records
            .get(key)
            .cloned()
    }
    pub fn has_service(&self, sid: &str) -> bool {
        self.state
            .lock()
            .expect("fixture lock")
            .services
            .contains_key(sid)
    }
}
fn matches_failure(state: &mut State, method: &str, path: &str, after: bool) -> bool {
    let matches = state
        .fail
        .as_ref()
        .is_some_and(|(m, p, a, _)| m == method && path.starts_with(p) && *a == after);
    if matches {
        if let Some((_, _, _, remaining)) = state.fail.as_mut() {
            if *remaining > 1 {
                *remaining -= 1;
                return false;
            }
        }
    }
    if matches {
        state.fail = None;
    }
    matches
}
fn api(state: &mut State, request: &ApiRequest<'_>) -> Result<(u16, Value), String> {
    let method = request.method;
    let path = request.path;
    let body = request.body.clone().unwrap_or(json!({}));
    let parts: Vec<String> = path
        .trim_matches('/')
        .split('/')
        .map(|p| {
            percent_encoding::percent_decode_str(p)
                .decode_utf8_lossy()
                .into_owned()
        })
        .collect();
    let p: Vec<&str> = parts.iter().map(String::as_str).collect();
    if path == "/tokens/self" {
        return Ok((
            200,
            json!({"user_id":"fixtureUser","customer_id":if request.token=="fixture-wrong-account" {"otherCustomer"} else {"fixtureCustomer"},"scope":if request.token=="fixture-read-only" {"global:read"} else {"global"},"services":[]}),
        ));
    }
    if path == "/current_user" {
        return Ok((
            200,
            json!({"id":"fixtureUser","role":if request.token=="fixture-limited" {"user"} else {"superuser"},"limit_services":false}),
        ));
    }
    if let Some(product) = path.strip_prefix("/enabled-products/v1/") {
        if state.disabled_products.contains(product) {
            return Ok((200, json!({"enabled":false})));
        }
        return Ok((200, json!({"enabled":true})));
    }
    if path == "/service" && method == "GET" {
        return Ok((
            200,
            Value::Array(state.services.values().cloned().collect()),
        ));
    }
    if path == "/service" && method == "POST" {
        if state.disabled_products.contains("compute") {
            return Ok((403, json!({"error":"compute_not_enabled"})));
        }
        state.sequence += 1;
        let sid = format!("fixtureScorer{}", state.sequence);
        let svc = json!({
            "id": sid,
            "name": body["name"],
            "type": body["type"].as_str().unwrap_or("wasm"),
            "customer_id": "fixtureCustomer",
        });
        state.services.insert(sid.clone(), svc.clone());
        state.versions.insert(
            sid,
            vec![Version {
                number: 1,
                active: false,
                locked: false,
                comment: String::new(),
                resources: BTreeMap::new(),
            }],
        );
        return Ok((200, svc));
    }
    match (method,p.as_slice()) {
        ("GET",["service",sid])=>Ok(state.services.get(*sid).map(|v|(200,v.clone())).unwrap_or((404,json!({})))),
        ("DELETE",["service",sid])=>{
            state.services.remove(*sid);
            state.versions.remove(*sid);
            Ok((200,json!({"status":"ok"})))
        }
        ("GET",["service",sid,"version"])=>Ok((200,Value::Array(state.versions.get(*sid).into_iter().flatten().map(|v|json!({"number":v.number,"active":v.active,"locked":v.locked,"comment":v.comment})).collect()))),
        ("PUT",["service",sid,"version",number,"deactivate"])=>{
            let versions=state.versions.get_mut(*sid).ok_or("fixture_service_missing")?;
            for version in versions {if version.number.to_string()==*number {version.active=false;}}
            Ok((200,json!({"number":number})))
        }
        ("GET",["service",sid,"version",_,"generated_vcl"])=>{
            let content=if state.pragmas_missing.contains(*sid) {
                "sub vcl_recv {}"
            } else {
                "pragma fix_unsent_body_drain true;\npragma no_body_if_bereq_is_get_or_head true;"
            };
            Ok((200,json!({"content":content})))
        }
        ("GET",["service",sid,"version",number,"package"])=>{
            let ver_num:u64=number.parse().unwrap_or(1);
            if let Some(bytes)=state.packages.get(&((*sid).to_owned(),ver_num)) {
                Ok((200,json!({"metadata":{"files_hash":format!("fh-{}",bytes.len())}})))
            } else {
                Ok((404,json!({})))
            }
        }
        ("GET",["resources","stores","config"])=>{
            let items:Vec<_>=state.config_stores.iter().map(|(id,(name,_))|json!({"id":id,"name":name})).collect();
            Ok((200,Value::Array(items)))
        }
        ("GET",["resources","stores","config",id])=>{
            Ok(state.config_stores.get(*id).map(|(name,_)|(200,json!({"id":id,"name":name}))).unwrap_or((404,json!({}))))
        }
        ("POST",["resources","stores","config"])=>{
            if state.disabled_products.contains("config_store") {return Ok((403,json!({"error":"config_store_not_enabled"})));}
            state.sequence+=1;
            let id=format!("cfg-store-{}",state.sequence);
            let name=body["name"].as_str().unwrap_or("").to_owned();
            state.config_stores.insert(id.clone(),(name.clone(),BTreeMap::new()));
            Ok((200,json!({"id":id,"name":name})))
        }
        ("DELETE",["resources","stores","config",id])=>{
            if state.config_stores.remove(*id).is_some() {Ok((200,json!({"status":"ok"})))} else {Ok((404,json!({})))}
        }
        ("GET",["resources","stores","config",id,"item",key])=>{
            let Some((_,items))=state.config_stores.get(*id) else {return Ok((404,json!({})));};
            Ok(items.get(*key).map(|v|(200,json!({"item_key":key,"item_value":v}))).unwrap_or((404,json!({}))))
        }
        ("POST",["resources","stores","config",id,"item"])=>{
            let Some((_,items))=state.config_stores.get_mut(*id) else {return Ok((404,json!({})));};
            let k=body["item_key"].as_str().ok_or("fixture_cfg_key")?.to_owned();
            let v=body["item_value"].as_str().unwrap_or("").to_owned();
            items.insert(k.clone(),v.clone());
            Ok((200,json!({"item_key":k,"item_value":v})))
        }
        ("PUT",["resources","stores","config",id,"item",key])=>{
            let Some((_,items))=state.config_stores.get_mut(*id) else {return Ok((404,json!({})));};
            if !items.contains_key(*key) {return Ok((404,json!({})));}
            let v=body["item_value"].as_str().unwrap_or("").to_owned();
            items.insert((*key).to_owned(),v.clone());
            Ok((200,json!({"item_key":key,"item_value":v})))
        }
        ("GET",["resources","stores","kv"])=>{
            let items:Vec<_>=state.kv_stores.iter().map(|(id,(name,_))|json!({"id":id,"name":name})).collect();
            Ok((200,json!({"data":items})))
        }
        ("GET",["resources","stores","kv",id])=>{
            Ok(state.kv_stores.get(*id).map(|(name,_)|(200,json!({"id":id,"name":name}))).unwrap_or((404,json!({}))))
        }
        ("POST",["resources","stores","kv"])=>{
            if state.disabled_products.contains("kv_store") {return Ok((403,json!({"error":"kv_store_not_enabled"})));}
            state.sequence+=1;
            let id=format!("kv-store-{}",state.sequence);
            let name=body["name"].as_str().unwrap_or("").to_owned();
            state.kv_stores.insert(id.clone(),(name.clone(),BTreeMap::new()));
            Ok((200,json!({"id":id,"name":name})))
        }
        ("DELETE",["resources","stores","kv",id])=>{
            if state.kv_stores.remove(*id).is_some() {Ok((200,json!({"status":"ok"})))} else {Ok((404,json!({})))}
        }
        ("DELETE",["resources","stores","kv",id,"keys",key])=>{
            if let Some((_,entries))=state.kv_stores.get_mut(*id) {entries.remove(*key);}
            Ok((204,json!({})))
        }
        ("PUT",["service",sid,"version",source,"clone"])=>{
            let mut next_seq=state.sequence;
            let versions=state.versions.get_mut(*sid).ok_or("fixture_service_missing")?;
            let mut version=versions.iter().find(|v|v.number.to_string()==*source).ok_or("fixture_source_missing")?.clone();
            version.number=versions.iter().map(|v|v.number).max().unwrap_or(0)+1;
            version.active=false;version.locked=false;
            let number=version.number;
            if let Some(rl_entries)=version.resources.get_mut("resource") {
                for val in rl_entries.values_mut() {
                    next_seq+=1;
                    val["id"]=format!("rl-{number}-{next_seq}").into();
                }
            }
            versions.push(version);
            state.sequence=next_seq;
            Ok((200,json!({"number":number})))
        }
        ("PUT",["service",sid,"version",number])=>{
            let version=version_mut(state,sid,number)?;
            version.comment=body["comment"].as_str().unwrap_or("").into();
            Ok((200,json!({"number":version.number})))
        }
        ("GET",["service",sid,"version",number,"validate"])=>{
            if state.invalid_draft {
                return Ok((200,json!({"status":"error","errors":"fixture validation error"})));
            }
            if state.services.get(*sid).and_then(|s|s["type"].as_str())==Some("vcl") {
                let version=version_mut(state,sid,number)?;
                let backends=version.resources.get("backend");
                let conditions=version.resources.get("condition");
                if let Some(snippets)=version.resources.get("snippet") {
                    for snippet in snippets.values() {
                        let body=snippet["content"].as_str().or_else(||snippet["body"].as_str()).unwrap_or("");
                        if body.contains("F_session_scorer") && backends.is_none_or(|b|!b.contains_key("session_scorer")) {
                            return Ok((200,json!({"status":"error","errors":"undefined variable \"F_session_scorer\""})));
                        }
                        if body.contains("F_fos_origin") && backends.is_none_or(|b|!b.contains_key("fos_origin")) {
                            return Ok((200,json!({"status":"error","errors":"undefined variable \"F_fos_origin\""})));
                        }
                    }
                }
                if let Some(bmap)=backends {
                    for b in bmap.values() {
                        if let Some(cond)=b["request_condition"].as_str().filter(|s|!s.is_empty()) {
                            if conditions.is_none_or(|c|!c.contains_key(cond)) {
                                return Ok((200,json!({"status":"error","errors":format!("undefined request_condition \"{cond}\"")})));
                            }
                        }
                    }
                }
            }
            Ok((200,json!({"status":"ok","errors":""})))
        }
        ("PUT",["service",sid,"version",number,"activate"])=>{
            if state.invalid_draft {return Ok((400,json!({})));}
            let versions=state.versions.get_mut(*sid).ok_or("fixture_service_missing")?;
            if !versions.iter().any(|v|v.number.to_string()==*number) {return Ok((404,json!({})));}
            for version in versions {if version.active {version.locked=true;}version.active=version.number.to_string()==*number;}
            Ok((200,json!({"number":number})))
        }
        ("PUT",["resources","stores","secret",_,"secrets"])=>{
            let name=body["name"].as_str().ok_or("fixture_invalid_secret")?;
            let value=base64::engine::general_purpose::STANDARD.decode(body["secret"].as_str().ok_or("fixture_invalid_secret")?).map_err(|_|"fixture_invalid_secret")?;
            state.secrets.insert(name.into(),String::from_utf8(value).map_err(|_|"fixture_invalid_secret")?);
            Ok((200,json!({"name":name})))
        }
        ("DELETE",["resources","stores","secret",_,"secrets",name])=>{state.secrets.remove(*name);Ok((204,json!({})))}
        ("GET",["resources","stores","secret",_,"secrets",name])=>{Ok(if state.secrets.contains_key(*name) {(200,json!({"name":name}))} else {(404,json!({}))})}
        ("GET",["resources","object-storage","access-keys"])=>{
            let keys:Vec<_>=state.keys.iter().map(|k|{let mut k=k.clone();k.as_object_mut().expect("key").remove("secret_key");k}).collect();
            Ok((200,json!({"data":keys})))
        }
        ("POST",["resources","object-storage","access-keys"])=>{
            state.sequence+=1;
            let mut key=body;
            key["access_key"]=format!("fixture-ak-{}",state.sequence).into();
            key["secret_key"]=format!("fixture-sk-{}",state.sequence).into();
            state.keys.push(key.clone());
            Ok((201,key))
        }
        ("DELETE",["resources","object-storage","access-keys",key])=>{state.keys.retain(|v|v["access_key"]!=*key);Ok((204,json!({})))}
        ("PUT",["service",_,"dictionary",dictionary,"item",key])=>{
            state.dictionaries.entry((*dictionary).into()).or_default().insert((*key).into(),body["item_value"].as_str().unwrap_or("").into());
            Ok((200,json!({})))
        }
        ("DELETE",["service",_,"dictionary",dictionary,"item",key])=>{
            state.dictionaries.entry((*dictionary).into()).or_default().remove(*key);Ok((204,json!({})))
        }
        ("POST",["service",_,"purge",_])=>Ok((200,json!({"status":"ok"}))),
        (_,["service",sid,"version",number,tail @ ..])=> {
            let logging=tail.first()==Some(&"logging");
            let resource=if logging {format!("logging/{}",tail.get(1).copied().unwrap_or(""))} else {tail.first().copied().unwrap_or("").to_owned()};
            let resource=resource.as_str();
            let offset=if logging {2} else {1};
            let name=tail.get(offset).copied();
            if resource=="logging/fos" && matches!(method,"POST"|"PUT") {
                // Like Fastly: only these fields are writable; the bucket-scoped key is minted server-side.
                const WRITABLE:&[&str]=&["name","bucket_name","region","path","period","format","format_version","message_type","compression_codec","gzip_level","file_max_bytes","placement","response_condition","log_processing_region","public_key","timestamp_format"];
                if body.as_object().is_none_or(|o|o.keys().any(|k|!WRITABLE.contains(&k.as_str()))) {
                    return Ok((400,json!({"msg":"Bad request"})));
                }
            }
            let minted=if resource=="logging/fos" && method=="POST" {state.sequence+=1;Some(format!("fixture-fos-endpoint-key-{}",state.sequence))} else {None};
            let minted_rl_id=if resource=="resource" && method=="POST" {state.sequence+=1;Some(format!("rl-{number}-{}",state.sequence))} else {None};
            let version=version_mut(state,sid,number)?;
            if !version.locked && !version.active && matches!(method,"POST"|"PUT") {
                if resource=="backend" {
                    if let Some(cond)=body["request_condition"].as_str().filter(|s|!s.is_empty()) {
                        if version.resources.get("condition").is_none_or(|c|!c.contains_key(cond)) {
                            return Ok((400,json!({"msg":"Bad request","detail":format!("Condition '{cond}' does not exist")})));
                        }
                    }
                } else if resource == "logging/fos" {
                    if let Some(cond)=body["response_condition"].as_str().filter(|s|!s.is_empty()) {
                        if version.resources.get("condition").is_none_or(|c|!c.contains_key(cond)) {
                            return Ok((400,json!({"msg":"Bad request","detail":format!("Condition '{cond}' does not exist")})));
                        }
                    }
                }
            }
            if !version.locked && !version.active && method=="DELETE" && resource=="condition" {
                if let Some(cond_name)=name {
                    let referenced_by_backend=version.resources.get("backend").is_some_and(|bmap|{
                        bmap.values().any(|b|b["request_condition"].as_str()==Some(cond_name))
                    });
                    let referenced_by_logging=version.resources.get("logging/fos").is_some_and(|lmap|{
                        lmap.values().any(|ep|ep["response_condition"].as_str()==Some(cond_name))
                    });
                    if referenced_by_backend || referenced_by_logging {
                        return Ok((409,json!({"msg":"Conflict","detail":format!("Condition '{cond_name}' is still referenced")})));
                    }
                }
            }
            let values=version.resources.entry(resource.into()).or_default();
            if method=="GET" {
                if resource=="resource" {
                    return Ok(if let Some(item_id)=name {
                        values.values().find(|v|v["id"].as_str()==Some(item_id)).cloned().map(|v|(200,v)).unwrap_or((404,json!({})))
                    } else {
                        (200,Value::Array(values.values().cloned().collect()))
                    });
                }
                return Ok(if let Some(name)=name {values.get(name).map(|v|(200,v.clone())).unwrap_or((404,json!({})))} else {(200,Value::Array(values.values().cloned().collect()))});
            }
            if version.locked || version.active {return Ok((400,json!({"error":"version_locked"})));}
            match method {
                "POST"|"PUT"=>{
                    let name=name.or_else(||body["name"].as_str()).ok_or("fixture_resource_name")?.to_owned();
                    let mut body=body;
                    if resource=="resource" {
                        if method=="POST" && values.contains_key(&name) {
                            return Ok((409,json!({"error":"duplicate_resource_link"})));
                        }
                        if body["id"].as_str().is_none() {
                            if let Some(id)=minted_rl_id {
                                body["id"]=id.into();
                            }
                        }
                    }
                    if resource=="dictionary" && body["id"].as_str().is_none() {
                        body["id"]=format!("dict-{sid}-{name}").into();
                    }
                    // Like Fastly: a PUT to a health check updates only the fields it sends.
                    if resource=="healthcheck" && method=="PUT" {
                        if let (Some(existing),Some(sent))=(values.get(&name).and_then(Value::as_object),body.as_object()) {
                            let mut merged=existing.clone();
                            merged.extend(sent.clone());
                            body=Value::Object(merged);
                        }
                    }
                    // Like Fastly: a condition keeps its priority (a numeric string, "0" when new) unless one is sent.
                    if resource=="condition" {
                        let priority=match &body["priority"] {
                            Value::Null=>values.get(&name).and_then(|v|v["priority"].as_str()).unwrap_or("0").to_owned(),
                            Value::String(s)=>s.clone(),
                            other=>other.to_string(),
                        };
                        body["priority"]=priority.into();
                    }
                    // Like Fastly: the endpoint keeps its minted key, and numbers come back as strings.
                    if resource=="logging/fos" {
                        let existing=values.get(&name).cloned().unwrap_or_else(||json!({"timestamp_format":"%Y-%m-%dT%H:%M:%S.000"}));
                        let object=body.as_object_mut().ok_or("fixture_invalid_body")?;
                        object.retain(|_,v|!v.is_null());
                        for key in ["period","format_version","gzip_level"] {
                            if let Some(n)=object.get(key).and_then(Value::as_u64) {object.insert(key.into(),n.to_string().into());}
                        }
                        object.insert("access_key".into(),minted.map(Value::from).unwrap_or_else(||existing["access_key"].clone()));
                        if !object.contains_key("timestamp_format") {object.insert("timestamp_format".into(),existing["timestamp_format"].clone());}
                    }
                    values.insert(name,body.clone());Ok((200,body))
                }
                "DELETE"=>{
                    if resource=="resource" {
                        let Some(item_id)=name else {return Ok((404,json!({})));};
                        let found_key=values.iter().find_map(|(k,v)|(v["id"].as_str()==Some(item_id)).then(||k.clone()));
                        return Ok(if let Some(k)=found_key {
                            values.remove(&k);
                            (204,json!({}))
                        } else {
                            (404,json!({"error":"resource_link_not_found"}))
                        });
                    }
                    if let Some(name)=name {values.remove(name);}
                    Ok((204,json!({})))
                }
                _=>Ok((405,json!({}))),
            }
        }
        _=>Err(format!("fixture_unhandled:{method}:{path}")),
    }
}
fn version_mut<'a>(
    state: &'a mut State,
    sid: &str,
    number: &str,
) -> Result<&'a mut Version, String> {
    state
        .versions
        .get_mut(sid)
        .and_then(|versions| versions.iter_mut().find(|v| v.number.to_string() == number))
        .ok_or_else(|| "fixture_version_missing".into())
}

use super::{ApiRequest, FosRequest, FosResponse, Record};
use base64::Engine;
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Mutex};

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
                        let prefix = url::form_urlencoded::parse(request.query.as_bytes())
                            .find(|(k, _)| k == "prefix")
                            .map(|(_, v)| v.into_owned())
                            .unwrap_or_default();
                        let contents = bucket
                            .keys()
                            .find(|k| k.starts_with(&prefix))
                            .map(|k| {
                                format!(
                                    "<Contents><Key>{}</Key></Contents>",
                                    quick_xml::escape::escape(k)
                                )
                            })
                            .unwrap_or_default();
                        FosResponse {
                            cdn_cache: None,
                            status: 200,
                            body: format!("<ListBucketResult>{contents}</ListBucketResult>")
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
        version
            .resources
            .entry(resource.into())
            .or_default()
            .insert(name, value);
    }
    pub fn seed_object(&self, bucket: &str, key: &str) {
        self.state
            .lock()
            .expect("fixture lock")
            .buckets
            .entry(bucket.into())
            .or_default()
            .insert(key.into(), vec![1]);
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
    if path == "/enabled-products/v1/object_storage" {
        return Ok((200, json!({"enabled":true})));
    }
    if path == "/service" && method == "GET" {
        return Ok((
            200,
            Value::Array(state.services.values().cloned().collect()),
        ));
    }
    match (method,p.as_slice()) {
        ("GET",["service",sid])=>Ok(state.services.get(*sid).map(|v|(200,v.clone())).unwrap_or((404,json!({})))),
        ("GET",["service",sid,"version"])=>Ok((200,Value::Array(state.versions.get(*sid).into_iter().flatten().map(|v|json!({"number":v.number,"active":v.active,"locked":v.locked,"comment":v.comment})).collect()))),
        ("PUT",["service",sid,"version",source,"clone"])=>{
            let versions=state.versions.get_mut(*sid).ok_or("fixture_service_missing")?;
            let mut version=versions.iter().find(|v|v.number.to_string()==*source).ok_or("fixture_source_missing")?.clone();
            version.number=versions.iter().map(|v|v.number).max().unwrap_or(0)+1;
            version.active=false;version.locked=false;
            let number=version.number;
            versions.push(version);
            Ok((200,json!({"number":number})))
        }
        ("PUT",["service",sid,"version",number])=>{
            let version=version_mut(state,sid,number)?;
            version.comment=body["comment"].as_str().unwrap_or("").into();
            Ok((200,json!({"number":version.number})))
        }
        ("GET",["service",_,"version",_,"validate"])=>Ok((200,json!({"status":if state.invalid_draft {"error"} else {"ok"},"errors":if state.invalid_draft {"fixture validation error"} else {""}}))),
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
            let resource=if tail.first()==Some(&"logging") {"logging/s3"} else {tail.first().copied().unwrap_or("")};
            let offset=if resource=="logging/s3" {2} else {1};
            let name=tail.get(offset).copied();
            let version=version_mut(state,sid,number)?;
            let values=version.resources.entry(resource.into()).or_default();
            if method=="GET" {
                return Ok(if let Some(name)=name {values.get(name).map(|v|(200,v.clone())).unwrap_or((404,json!({})))} else {(200,Value::Array(values.values().cloned().collect()))});
            }
            if version.locked || version.active {return Ok((400,json!({"error":"version_locked"})));}
            match method {
                "POST"|"PUT"=>{
                    let name=name.or_else(||body["name"].as_str()).ok_or("fixture_resource_name")?.to_owned();
                    let mut body=body;
                    if resource=="dictionary" && body["id"].as_str().is_none() {
                        body["id"]=format!("dict-{sid}-{name}").into();
                    }
                    values.insert(name,body.clone());Ok((200,body))
                }
                "DELETE"=>{if let Some(name)=name {values.remove(name);}Ok((204,json!({})))}
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

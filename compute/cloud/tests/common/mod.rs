#![allow(dead_code)]

use arrow_array::{
    Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use bytes::Bytes;
use fastly_log_analytics::{
    admin::{fixture::FixtureAdapter, ApiRequest, FosRequest, FosResponse, Record, Service},
    auth::{self, Role},
    http::Request,
    ingest::storage::Store,
    setup::SetupStores,
    shell::{CoreCacheEntry, CoreCacheError, KvPage, Runtime},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Value};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    io::Write,
    time::{Duration, Instant},
};

pub const TEST_SESSION_KEY: &[u8] = b"fixture-session-signing-material-32-bytes";
pub const TEST_BASE_TS: u64 = 1_800_000_000;
pub const TEST_SERVICE_ID: &str = "AbCdEfGhIjKlMnOpQrSt12";

/// Shared deterministic Runtime backed by the production admin API/KV/FOS fixture.
pub struct FixtureRuntime {
    pub adapter: FixtureAdapter,
    pub now: Cell<u64>,
    config: RefCell<BTreeMap<String, String>>,
    secrets: RefCell<BTreeMap<String, String>>,
    cache: RefCell<BTreeMap<String, (CoreCacheEntry, Vec<String>)>>,
}

impl FixtureRuntime {
    pub fn new() -> Self {
        let config = BTreeMap::from([
            ("flp_service_id".into(), "fixtureApp".into()),
            ("worker_service_id".into(), "fixtureWorker".into()),
            ("front_service_id".into(), "fixtureFront".into()),
            ("install_name".into(), "fixture".into()),
            ("secret_store_id".into(), "fixtureSecrets".into()),
            ("buckets_dictionary_id".into(), "fixtureBuckets".into()),
            (
                "credentials_dictionary_id".into(),
                "fixtureCredentials".into(),
            ),
            ("front_auth_dictionary_id".into(), "fixtureAuth".into()),
            ("public_base_url".into(), "https://logs.example.com".into()),
        ]);
        let secrets = BTreeMap::from([
            ("cdn_key".into(), "fixture-cdn-key".into()),
            ("front_key".into(), "fixture-front-key".into()),
            (
                "session_key".into(),
                String::from_utf8(TEST_SESSION_KEY.to_vec()).unwrap(),
            ),
            ("fastly_token".into(), "fixture-token".into()),
        ]);
        Self {
            adapter: FixtureAdapter::new(),
            now: Cell::new(TEST_BASE_TS),
            config: RefCell::new(config),
            secrets: RefCell::new(secrets),
            cache: RefCell::new(BTreeMap::new()),
        }
    }

    pub fn set_config(&self, name: &str, value: impl Into<String>) {
        self.config.borrow_mut().insert(name.into(), value.into());
    }

    pub fn set_secret(&self, name: &str, value: impl Into<String>) {
        self.secrets.borrow_mut().insert(name.into(), value.into());
    }
}

impl Default for FixtureRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl SetupStores for FixtureRuntime {
    fn config_present(&self, name: &str) -> Result<bool, String> {
        Ok(self.config.borrow().contains_key(name))
    }

    fn secret_present(&self, name: &str) -> Result<bool, String> {
        Ok(self.secrets.borrow().contains_key(name) || self.adapter.secret(name)?.is_some())
    }

    fn state_linked(&self) -> Result<bool, String> {
        Ok(true)
    }
}

impl Runtime for FixtureRuntime {
    fn config(&self, name: &str) -> Result<Option<String>, String> {
        Ok(self.config.borrow().get(name).cloned())
    }

    fn secret(&self, name: &str) -> Result<Option<String>, String> {
        if let Some(value) = self.secrets.borrow().get(name) {
            return Ok(Some(value.clone()));
        }
        self.adapter.secret(name)
    }

    fn random(&self) -> Result<[u8; 32], String> {
        Ok([7; 32])
    }

    fn now(&self) -> u64 {
        self.now.get()
    }

    fn checks(&self, _: bool) -> Result<Value, String> {
        Ok(json!({}))
    }

    fn versions(&self) -> Result<Value, String> {
        Ok(json!({}))
    }

    fn compute_service_id(&self) -> Option<String> {
        Some("fixtureApp".into())
    }

    fn compute_service_version(&self) -> Option<String> {
        Some("1".into())
    }

    fn admin_get(&self, key: &str) -> Result<Option<Record>, String> {
        self.adapter.get(key)
    }

    fn admin_get_fresh(&self, key: &str, _: &str) -> Result<Option<Record>, String> {
        self.adapter.confirm(key)
    }

    fn admin_cas(
        &self,
        key: &str,
        generation: Option<&str>,
        value: &Value,
    ) -> Result<bool, String> {
        self.adapter.cas(key, generation, value)
    }

    fn admin_api(
        &self,
        request: ApiRequest<'_>,
    ) -> Result<fastly_log_analytics::admin::ApiResponse, String> {
        self.adapter.api(request).map(Into::into)
    }

    fn admin_fos(&self, request: FosRequest<'_>) -> Result<FosResponse, String> {
        self.adapter.fos(request)
    }

    fn kv_list(&self, prefix: &str, cursor: Option<&str>, limit: u32) -> Result<KvPage, String> {
        Ok(KvPage::from_sorted_keys(
            self.adapter.records().into_keys(),
            prefix,
            cursor,
            limit,
        ))
    }

    fn kv_delete(&self, key: &str) -> Result<(), String> {
        self.adapter.delete(key)
    }

    fn core_cache_replace(
        &self,
        key: &str,
        surrogate_keys: &[&str],
        _: Duration,
        update: &dyn Fn(Option<CoreCacheEntry>) -> CoreCacheEntry,
    ) -> Result<(), CoreCacheError> {
        let mut cache = self.cache.borrow_mut();
        let next = update(cache.get(key).map(|(entry, _)| entry.clone()));
        cache.insert(
            key.into(),
            (
                next,
                surrogate_keys.iter().map(|key| key.to_string()).collect(),
            ),
        );
        Ok(())
    }

    fn core_cache_lookup(&self, key: &str) -> Result<Option<CoreCacheEntry>, CoreCacheError> {
        Ok(self.cache.borrow().get(key).map(|(entry, _)| entry.clone()))
    }

    fn core_cache_purge(&self, surrogate: &str) -> Result<(), CoreCacheError> {
        self.cache
            .borrow_mut()
            .retain(|_, (_, keys)| !keys.iter().any(|key| key == surrogate));
        Ok(())
    }
}

pub fn session_cookie(user_id: &str, role: Role, key: &[u8], now: u64) -> String {
    let token = compute_common::crypto::seal_json(
        key,
        &json!({
            "sub": user_id,
            "role": role,
            "policy_version": 0,
            "exp": now + auth::SESSION_TTL,
        }),
    )
    .unwrap();
    format!("flp_session={token}")
}

pub fn authed_get(path: &str, cookie: &str) -> Request {
    let mut headers = BTreeMap::new();
    headers.insert("cookie".into(), cookie.into());
    Request {
        method: "GET".into(),
        path: path.into(),
        headers,
        body: Vec::new(),
    }
}

pub fn authed_post(path: &str, cookie: &str, body: Value) -> Request {
    let mut headers = BTreeMap::new();
    headers.insert("cookie".into(), cookie.into());
    headers.insert("content-type".into(), "application/json".into());
    Request {
        method: "POST".into(),
        path: path.into(),
        headers,
        body: body.to_string().into_bytes(),
    }
}

pub fn authed_patch(path: &str, cookie: &str, body: Value) -> Request {
    let mut req = authed_post(path, cookie, body);
    req.method = "PATCH".into();
    req
}

pub fn authed_delete(path: &str, cookie: &str, body: Value) -> Request {
    let mut req = authed_post(path, cookie, body);
    req.method = "DELETE".into();
    req
}

pub fn service_json(service_id: &str, fos_prefix: &str) -> Value {
    json!({
        "service_id": service_id,
        "name": "Test Service",
        "service_type": "vcl",
        "logging_endpoint_name": "flp_s3_logs",
        "fos_bucket": "fos-logs",
        "fos_region": "us-east-1",
        "fos_prefix": fos_prefix,
        "fos_key_ref": format!("fos:{service_id}"),
        "fastly_token_ref": format!("fastly:{service_id}"),
        "log_fields": {
            "groups": ["A", "C"],
            "custom_fields": [],
            "field_overrides": {},
            "field_limits": {},
        },
        "log_retention_days": 30,
        "rum_retention_days": 30,
        "delete_after": true,
        "rum": {"enabled": false},
        "status": "healthy",
        "created_at": TEST_BASE_TS,
        "updated_at": TEST_BASE_TS,
    })
}

pub fn sample_service(service_id: &str, fos_prefix: &str) -> Service {
    serde_json::from_value(service_json(service_id, fos_prefix)).unwrap()
}

pub fn test_store<'a, R: Runtime>(runtime: &'a R, service: &'a Service) -> Store<'a, R> {
    Store {
        runtime,
        service,
        access_key: "AKIAIOSFODNN7EXAMPLE".into(),
        secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
        started: Instant::now(),
        vcpu_start: 0,
    }
}

pub fn zstd_ndjson(rows: &[Value]) -> Vec<u8> {
    let mut buf = Vec::new();
    for row in rows {
        writeln!(buf, "{row}").unwrap();
    }
    zstd::stream::encode_all(buf.as_slice(), 1).unwrap()
}

pub fn parse_parquet_rows(bytes: &[u8]) -> Vec<RecordBatch> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))
        .unwrap()
        .build()
        .unwrap();
    reader.map(Result::unwrap).collect()
}

pub fn col_str(batch: &RecordBatch, name: &str, row: usize) -> Option<String> {
    let col = batch.column_by_name(name)?;
    if col.is_null(row) {
        return None;
    }
    col.as_any()
        .downcast_ref::<StringArray>()
        .map(|a| a.value(row).to_owned())
}

pub fn col_i64(batch: &RecordBatch, name: &str, row: usize) -> Option<i64> {
    let col = batch.column_by_name(name)?;
    if col.is_null(row) {
        return None;
    }
    let any = col.as_any();
    any.downcast_ref::<Int64Array>()
        .map(|a| a.value(row))
        .or_else(|| {
            any.downcast_ref::<TimestampMillisecondArray>()
                .map(|a| a.value(row))
        })
}

pub fn col_f64(batch: &RecordBatch, name: &str, row: usize) -> Option<f64> {
    let col = batch.column_by_name(name)?;
    if col.is_null(row) {
        return None;
    }
    col.as_any()
        .downcast_ref::<Float64Array>()
        .map(|a| a.value(row))
}

pub fn col_bool(batch: &RecordBatch, name: &str, row: usize) -> Option<bool> {
    let col = batch.column_by_name(name)?;
    if col.is_null(row) {
        return None;
    }
    col.as_any()
        .downcast_ref::<BooleanArray>()
        .map(|a| a.value(row))
}

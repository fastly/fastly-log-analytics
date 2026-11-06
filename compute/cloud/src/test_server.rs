#![cfg_attr(target_arch = "wasm32", allow(dead_code, unused_imports))]

use fastly_log_analytics::{
    auth,
    http::{Request, Response},
    setup::{SetupStores, CONFIG_KEYS, SECRET_KEYS},
    shell::{Runtime, Shell},
    state::MemoryStore,
};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    io::Read,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

struct NativeRuntime {
    config: BTreeMap<String, String>,
    secrets: BTreeMap<String, String>,
    admin: fastly_log_analytics::admin::fixture::FixtureAdapter,
    csp_reports: Mutex<Vec<Value>>,
    extension_requests: Mutex<Vec<String>>,
}

impl NativeRuntime {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let origin = std::env::var("FLP_TEST_ORIGIN")?;
        let code = std::env::var("FLP_TEST_SETUP_CODE")?;
        let front = std::env::var("FLP_TEST_FRONT_KEY")?;
        let mut config: BTreeMap<String, String> = CONFIG_KEYS
            .iter()
            .map(|name| ((*name).into(), "native-test".into()))
            .collect();
        config.insert("public_base_url".into(), origin);
        for (key, value) in [
            ("flp_service_id", "fixtureApp"),
            ("worker_service_id", "fixtureWorker"),
            ("front_service_id", "fixtureFront"),
            ("secret_store_id", "fixtureSecrets"),
            ("config_store_id", "fixtureConfig"),
            ("kv_store_id", "fixtureState"),
            ("buckets_dictionary_id", "fixtureBuckets"),
            ("credentials_dictionary_id", "fixtureCredentials"),
            ("front_auth_dictionary_id", "fixtureAuth"),
        ] {
            config.insert(key.into(), value.into());
        }
        let mut secrets: BTreeMap<String, String> = SECRET_KEYS
            .iter()
            .map(|name| {
                (
                    (*name).into(),
                    "native-test-material-not-a-real-secret".into(),
                )
            })
            .collect();
        secrets.insert(
            "admin_setup_hash".into(),
            serde_json::json!({
                "sha256":auth::hash(&code),
                "expires_at":SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() + 900,
            })
            .to_string(),
        );
        secrets.insert("front_key".into(), front);
        let mut random = [0u8; 48];
        getrandom::getrandom(&mut random).map_err(|_| "native random unavailable")?;
        secrets.insert("session_key".into(), auth::encode(&random));
        if let Ok(name) = std::env::var("FLP_TEST_MISSING") {
            let trimmed = name
                .strip_prefix("secret:")
                .or_else(|| name.strip_prefix("config:"))
                .unwrap_or(&name);
            config.remove(trimmed);
            secrets.remove(trimmed);
        }
        if let Ok(dev) = std::env::var("FLP_DEV_TELEMETRY") {
            config.insert("dev_telemetry".into(), dev);
        }
        Ok(Self {
            config,
            secrets,
            admin: fastly_log_analytics::admin::fixture::FixtureAdapter::new(),
            csp_reports: Mutex::new(Vec::new()),
            extension_requests: Mutex::new(Vec::new()),
        })
    }
}

impl SetupStores for NativeRuntime {
    fn config_present(&self, name: &str) -> Result<bool, String> {
        Ok(self.config.contains_key(name))
    }
    fn secret_present(&self, name: &str) -> Result<bool, String> {
        Ok(self.secrets.contains_key(name))
    }
    fn state_linked(&self) -> Result<bool, String> {
        Ok(true)
    }
}
impl Runtime for NativeRuntime {
    fn config(&self, name: &str) -> Result<Option<String>, String> {
        Ok(self.config.get(name).cloned())
    }
    fn secret(&self, name: &str) -> Result<Option<String>, String> {
        Ok(self
            .admin
            .secret(name)?
            .or_else(|| self.secrets.get(name).cloned()))
    }
    fn random(&self) -> Result<[u8; 32], String> {
        let mut random = [0u8; 32];
        getrandom::getrandom(&mut random).map_err(|_| "native_random_unavailable")?;
        Ok(random)
    }
    fn now(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |time| time.as_secs())
    }
    fn checks(&self, _: bool) -> Result<Value, String> {
        Err("Native harness cannot verify a Fastly account; run live checks".into())
    }
    fn versions(&self) -> Result<Value, String> {
        Err("Native harness has no deployed service versions".into())
    }
    fn compute_service_id(&self) -> Option<String> {
        None
    }
    fn compute_service_version(&self) -> Option<String> {
        None
    }
    fn admin_get(&self, key: &str) -> Result<Option<fastly_log_analytics::admin::Record>, String> {
        self.admin.get(key)
    }
    fn admin_cas(
        &self,
        key: &str,
        generation: Option<&str>,
        value: &Value,
    ) -> Result<bool, String> {
        self.admin.cas(key, generation, value)
    }
    fn admin_api(
        &self,
        request: fastly_log_analytics::admin::ApiRequest<'_>,
    ) -> Result<fastly_log_analytics::admin::ApiResponse, String> {
        self.admin.api(request).map(Into::into)
    }
    fn admin_fos(
        &self,
        request: fastly_log_analytics::admin::FosRequest<'_>,
    ) -> Result<fastly_log_analytics::admin::FosResponse, String> {
        self.admin.fos(request)
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn read_request(request: &mut tiny_http::Request) -> Result<Request, Box<dyn std::error::Error>> {
    let method = request.method().as_str().to_owned();
    let path = request
        .url()
        .split('?')
        .next()
        .ok_or("missing path")?
        .to_owned();
    let headers = request
        .headers()
        .iter()
        .map(|h| {
            (
                h.field.as_str().as_str().to_ascii_lowercase(),
                h.value.as_str().to_owned(),
            )
        })
        .collect();
    let mut body = Vec::new();
    request.as_reader().take(65_537).read_to_end(&mut body)?;
    if body.len() > 65_536 {
        return Err("HTTP body too large".into());
    }
    Ok(Request {
        method,
        path,
        headers,
        body,
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn handle_test_endpoint(
    runtime: &mut NativeRuntime,
    state: &mut MemoryStore,
    request: &Request,
) -> Option<Response> {
    let path = request.path_only();
    if path.ends_with(".duckdb_extension.wasm") {
        if let Ok(mut paths) = runtime.extension_requests.lock() {
            paths.push(path.to_owned());
        }
    }
    match (request.method.as_str(), path) {
        ("POST", "/__test/csp-reports") => {
            let payload = serde_json::from_slice::<Value>(&request.body)
                .unwrap_or_else(|_| serde_json::json!({}));
            if let Ok(mut reports) = runtime.csp_reports.lock() {
                reports.push(payload);
            }
            Some(Response::new(204, Vec::new()).private())
        }
        ("DELETE", "/__test/csp-reports") => {
            if let Ok(mut reports) = runtime.csp_reports.lock() {
                reports.clear();
            }
            Some(Response::json(200, &serde_json::json!({ "reports": [] })))
        }
        ("GET", "/__test/csp-reports") => {
            let reports = runtime
                .csp_reports
                .lock()
                .map(|r| r.clone())
                .unwrap_or_default();
            Some(Response::json(
                200,
                &serde_json::json!({ "reports": reports }),
            ))
        }
        ("DELETE", "/__test/extension-requests") => {
            if let Ok(mut paths) = runtime.extension_requests.lock() {
                paths.clear();
            }
            Some(Response::json(200, &serde_json::json!({ "paths": [] })))
        }
        ("GET", "/__test/extension-requests") => {
            let paths = runtime
                .extension_requests
                .lock()
                .map(|p| p.clone())
                .unwrap_or_default();
            Some(Response::json(200, &serde_json::json!({ "paths": paths })))
        }
        ("POST", "/__test/reset") => {
            if let Ok(mut reports) = runtime.csp_reports.lock() {
                reports.clear();
            }
            if let Ok(mut paths) = runtime.extension_requests.lock() {
                paths.clear();
            }
            runtime.admin = fastly_log_analytics::admin::fixture::FixtureAdapter::new();
            *state = MemoryStore::default();
            Some(Response::json(200, &serde_json::json!({ "ok": true })))
        }
        _ => None,
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn to_response(response: Response) -> tiny_http::ResponseBox {
    let mut response = response.secured();
    if let Some(csp) = response.headers.get_mut("content-security-policy") {
        if !csp.contains("report-uri /__test/csp-reports") {
            csp.push_str("; report-uri /__test/csp-reports");
        }
    }
    let mut headers = Vec::with_capacity(response.headers.len());
    for (name, value) in response.headers {
        if let Ok(header) = tiny_http::Header::from_bytes(name.as_bytes(), value.as_bytes()) {
            headers.push(header);
        }
    }
    let length = response.body.len();
    tiny_http::Response::new(
        tiny_http::StatusCode(response.status),
        headers,
        std::io::Cursor::new(response.body),
        Some(length),
        None,
    )
    .boxed()
}

#[cfg(not(target_arch = "wasm32"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut runtime = NativeRuntime::new()?;
    let mut state = MemoryStore::default();
    let address = std::env::var("FLP_TEST_LISTEN").unwrap_or_else(|_| "127.0.0.1:8787".into());
    let server =
        tiny_http::Server::http(&address).map_err(|err| std::io::Error::other(err.to_string()))?;
    println!("Native acceptance backend listening on {address}");
    for mut incoming in server.incoming_requests() {
        let response = match read_request(&mut incoming) {
            Ok(request) => {
                if let Some(response) = handle_test_endpoint(&mut runtime, &mut state, &request) {
                    response
                } else {
                    let response =
                        Shell::new(&runtime, &state, fastly_log_analytics::ASSETS).handle(&request);
                    fastly_log_analytics::shell::resolve_upstream(&runtime, response)
                }
            }
            Err(error) => {
                eprintln!("Native request rejected: {error}");
                Response::new(400, "Bad request").private()
            }
        };
        let _ = incoming.respond(to_response(response));
    }
    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn main() {}

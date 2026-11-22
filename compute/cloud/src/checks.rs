use serde::Serialize;
use serde_json::Value;
use time::OffsetDateTime;

pub trait CheckApi {
    fn get(&mut self, path: &str) -> Result<(u16, Value), String>;

    fn record_failure(&mut self, event: Value) {
        eprintln!("{event}");
    }
}

pub trait CheckProbe {
    fn probe(&mut self, host: &str, path: &str) -> Result<u16, String>;

    fn record_failure(&mut self, event: Value) {
        eprintln!("{event}");
    }
}

#[derive(Clone, Debug, Default)]
pub struct Environment {
    pub now: u64,
    pub front_rl: bool,
    pub front_verified: bool,
    pub front_service_id: String,
    pub flp_service_id: String,
    pub worker_service_id: String,
    pub config_store_id: String,
    pub secret_store_id: String,
    pub kv_store_id: String,
    pub buckets_dictionary_id: String,
    pub credentials_dictionary_id: String,
    pub compute_host: String,
    pub install_name: String,
    pub last_tick: Option<u64>,
    /// `last_tick` is the IAD schedule record's `last_seen`, not the durable last run.
    pub last_tick_cached: bool,
    pub clock_interval_s: u64,
    pub fos_regions: Vec<String>,
}

/// How old the durable last run may be: a configured install runs maintenance every
/// `MAINT_INTERVAL_S`, plus the lease and margin.
const DURABLE_CLOCK_LIMIT_S: u64 = 300;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckStatus {
    Pass,
    Warn,
    Fail,
}

#[derive(Debug, Serialize)]
pub struct Check {
    pub id: String,
    pub name: String,
    pub status: CheckStatus,
    pub message: String,
    pub fix: Option<String>,
}

#[derive(Debug, Default, Serialize)]
pub struct CheckReport {
    pub checks: Vec<Check>,
    pub api_calls: u32,
    pub probe_calls: u32,
}

impl CheckReport {
    fn add(&mut self, id: &str, name: &str, status: CheckStatus, message: &str, fix: &str) {
        self.checks.push(Check {
            id: id.into(),
            name: name.into(),
            status,
            message: message.into(),
            fix: (!fix.is_empty()).then(|| fix.into()),
        });
    }

    fn get(&mut self, api: &mut impl CheckApi, path: &str) -> Option<(u16, Value)> {
        let endpoint = api_endpoint(path);
        if self.api_calls + self.probe_calls >= 24 {
            api.record_failure(failure_event(endpoint, "call_budget_exhausted", None));
            return None;
        }
        self.api_calls += 1;
        match api.get(path) {
            Ok((status, body)) => {
                if status != 200 {
                    api.record_failure(failure_event(endpoint, "unexpected_status", Some(status)));
                }
                Some((status, body))
            }
            Err(_) => {
                api.record_failure(failure_event(endpoint, "transport_error", None));
                None
            }
        }
    }

    fn json(&mut self, api: &mut impl CheckApi, path: &str) -> Option<Value> {
        self.get(api, path)
            .filter(|(status, _)| *status == 200)
            .map(|(_, body)| body)
    }

    fn probe(&mut self, probe: &mut impl CheckProbe, host: &str) -> Option<u16> {
        let endpoint = if host.ends_with(".edgecompute.app") {
            "compute:/"
        } else {
            "fos_region:/"
        };
        if self.api_calls + self.probe_calls >= 24 {
            probe.record_failure(failure_event(endpoint, "call_budget_exhausted", None));
            return None;
        }
        self.probe_calls += 1;
        match probe.probe(host, "/") {
            Ok(status) => {
                if status != 403 && !(endpoint == "fos_region:/" && status == 200) {
                    probe.record_failure(failure_event(
                        endpoint,
                        "unexpected_status",
                        Some(status),
                    ));
                }
                Some(status)
            }
            Err(_) => {
                probe.record_failure(failure_event(endpoint, "transport_error", None));
                None
            }
        }
    }

    fn active(&mut self, api: &mut impl CheckApi, service_id: &str) -> Option<u64> {
        if !identifier(service_id) {
            return None;
        }
        let versions = self.json(api, &format!("/service/{service_id}/version"))?;
        let active: Vec<_> = versions
            .as_array()?
            .iter()
            .filter(|v| v.get("active").and_then(Value::as_bool) == Some(true))
            .collect();
        if active.len() != 1 {
            return None;
        }
        active[0].get("number")?.as_u64().filter(|v| *v > 0)
    }
}

fn identifier(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && value.bytes().all(|c| c.is_ascii_alphanumeric())
}

fn api_endpoint(path: &str) -> &'static str {
    match path.split('/').collect::<Vec<_>>().as_slice() {
        ["", "tokens", "self"] => "/tokens/self",
        ["", "enabled-products", "v1", "object_storage"] => "/enabled-products/v1/object_storage",
        ["", "service", _, "version"] => "/service/{service_id}/version",
        ["", "service", _, "version", _, "generated_vcl"] => {
            "/service/{service_id}/version/{version_id}/generated_vcl"
        }
        ["", "service", _, "version", _, "dictionary", _, "info"] => {
            "/service/{service_id}/version/{version_id}/dictionary/{dictionary_id}/info"
        }
        ["", "service", _, "version", _, "resource"] => {
            "/service/{service_id}/version/{version_id}/resource"
        }
        _ => "[redacted]",
    }
}

fn failure_event(endpoint: &'static str, code: &'static str, status: Option<u16>) -> Value {
    serde_json::json!({
        "event": "environment.check.request_failed",
        "level": "error",
        "timestamp_s": OffsetDateTime::now_utc().unix_timestamp(),
        "method": "GET",
        "endpoint": endpoint,
        "code": code,
        "status": status
    })
}

pub fn run_checks(
    api: &mut impl CheckApi,
    probe: &mut impl CheckProbe,
    env: &Environment,
) -> CheckReport {
    use CheckStatus::{Fail, Pass, Warn};
    let mut report = CheckReport::default();
    let front_version = report.active(api, &env.front_service_id);
    if env.front_verified && env.front_rl {
        report.add(
            "edge_rate_limiting",
            "Edge Rate Limiting",
            Pass,
            "The authenticated front reports Edge Rate Limiting enabled.",
            "",
        );
    } else {
        let generated = front_version.and_then(|version| {
            report.json(
                api,
                &format!(
                    "/service/{}/version/{version}/generated_vcl",
                    env.front_service_id
                ),
            )
        });
        let content = generated
            .as_ref()
            .and_then(|v| v.get("content"))
            .and_then(Value::as_str);
        match content.filter(|v| !v.is_empty()) {
            Some(content) if content.split_whitespace().zip(content.split_whitespace().skip(1)).any(|(key, value)| key == "ratelimit_opt_in" && value.trim_end_matches(';') == "true") => {
                report.add("edge_rate_limiting", "Edge Rate Limiting", Warn, "Edge Rate Limiting is available but the authenticated front does not report it deployed.", "Redeploy flp-front with Edge Rate Limiting enabled: make -C app deploy.");
            }
            Some(_) => report.add("edge_rate_limiting", "Edge Rate Limiting", Warn, "Edge Rate Limiting is unavailable; sign-in limits use KV in Compute.", "KV fallback remains required. Enable Edge Rate Limiting on the account before redeploying flp-front."),
            None => report.add("edge_rate_limiting", "Edge Rate Limiting", Warn, "Could not verify Edge Rate Limiting availability; KV fallback remains required.", "Check fastly_token access and flp-front's active generated VCL."),
        }
    }

    for (id, name, dictionary_id) in [
        (
            "dictionary_buckets",
            "flp_fos_buckets",
            &env.buckets_dictionary_id,
        ),
        (
            "dictionary_credentials",
            "flp_fos_credentials",
            &env.credentials_dictionary_id,
        ),
    ] {
        let count = front_version
            .filter(|_| identifier(dictionary_id))
            .and_then(|version| {
                report.json(
                    api,
                    &format!(
                        "/service/{}/version/{version}/dictionary/{dictionary_id}/info",
                        env.front_service_id
                    ),
                )
            })
            .and_then(|body| body.get("item_count").and_then(Value::as_u64));
        match count {
            Some(count) if count < 1000 => report.add(id, name, Pass, &format!("{count} items against the documented default 1,000-item limit; one logged service uses 1 bucket item and 2 credential items across both dictionaries. Raised account limits are not reported by the API."), ""),
            Some(count) => report.add(id, name, Warn, &format!("{count} items meets or exceeds the documented default 1,000-item limit. A raised account limit cannot be verified through this API."), "Remove unused dictionary items or confirm a raised limit with Fastly; provisioning reports any actual write rejection."),
            None => report.add(id, name, Fail, "Dictionary item count could not be verified.", "Check the dictionary ID, flp-front's active version and fastly_token access."),
        }
    }

    report.add(
        "front_path",
        "Authenticated front path",
        if env.front_verified { Pass } else { Fail },
        if env.front_verified {
            "This request carried a verified front key."
        } else {
            "This request did not carry a verified front key."
        },
        if env.front_verified {
            ""
        } else {
            "Access the app through flp-front and configure matching front_key values."
        },
    );
    let valid_host = env.compute_host.ends_with(".edgecompute.app")
        && env
            .compute_host
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'.' || c == b'-');
    let direct = valid_host
        .then(|| report.probe(probe, &env.compute_host))
        .flatten();
    report.add(
        "direct_compute",
        "Direct Compute access",
        if direct == Some(403) { Pass } else { Fail },
        if direct == Some(403) {
            "A direct unauthenticated Compute probe was denied with HTTP 403."
        } else {
            "A direct Compute probe did not confirm HTTP 403 denial."
        },
        if direct == Some(403) {
            ""
        } else {
            "Verify compute_host and require the front key on direct public Compute requests."
        },
    );

    for (id, name, service_id) in [
        ("flp_stores", "flp store links", &env.flp_service_id),
        (
            "worker_stores",
            "flp-worker store links",
            &env.worker_service_id,
        ),
    ] {
        let links = report.active(api, service_id).and_then(|version| {
            report.json(
                api,
                &format!("/service/{service_id}/version/{version}/resource"),
            )
        });
        let required = [
            ("flp_config", &env.config_store_id),
            ("flp_secrets", &env.secret_store_id),
            ("flp_state", &env.kv_store_id),
        ];
        let missing: Vec<_> = required
            .iter()
            .filter(|(alias, store_id)| {
                !identifier(store_id)
                    || !links
                        .as_ref()
                        .and_then(Value::as_array)
                        .is_some_and(|links| {
                            links.iter().any(|link| {
                                link.get("name").and_then(Value::as_str) == Some(alias)
                                    && link.get("resource_id").and_then(Value::as_str)
                                        == Some(store_id.as_str())
                            })
                        })
            })
            .map(|(alias, _)| *alias)
            .collect();
        report.add(id, name, if missing.is_empty() { Pass } else { Fail }, if missing.is_empty() { "The active service links the required Config, Secret and KV Stores with the expected aliases and IDs." } else { "Required active-version store links are missing, mismatched or could not be verified." }, &if missing.is_empty() { String::new() } else { format!("Reconcile these resource links through app/infra/provision.py: {}.", missing.join(", ")) });
    }

    let enabled = report.get(api, "/enabled-products/v1/object_storage");
    let enabled_ok = enabled.as_ref().is_some_and(|(status, _)| *status == 200);
    report.add(
        "object_storage",
        "Object Storage account enablement",
        if enabled_ok { Pass } else { Fail },
        if enabled_ok {
            "The account Object Storage enablement endpoint returned HTTP 200."
        } else {
            "Object Storage account enablement could not be confirmed."
        },
        if enabled_ok {
            ""
        } else {
            "Enable Object Storage on the account and verify fastly_token access."
        },
    );
    if env.fos_regions.is_empty() {
        report.add(
            "fos_regions",
            "FOS regional backends",
            Fail,
            "No FOS region backend is configured.",
            "Configure the static us-east-1 FOS backend.",
        );
    }
    for (index, region) in env.fos_regions.iter().take(24).enumerate() {
        let valid = !region.is_empty()
            && region.len() <= 32
            && region
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-');
        let status = valid
            .then(|| report.probe(probe, &format!("{region}.object.fastlystorage.app")))
            .flatten();
        let reachable = matches!(status, Some(200 | 403));
        report.add(&format!("fos_region_{index}"), "FOS regional backend", if reachable { Pass } else { Fail }, if reachable { "The configured FOS regional backend responded. This unsigned probe does not verify object credentials or bucket access." } else { "The configured FOS regional backend did not return an expected unsigned response." }, if reachable { "" } else { "Verify the static FOS region backend, TLS hostname and region configuration." });
    }
    if env.fos_regions.len() > 24 {
        report.add(
            "fos_regions_budget",
            "FOS regional backend coverage",
            Fail,
            "The regional backend list exceeds this bounded check run.",
            "Reduce configured regional backends or check the remaining backends separately.",
        );
    }
    // The IAD record sees every 1 s probe; off the shield only the durable last run is
    // readable, which a configured install refreshes at least every maintenance interval.
    let limit = if env.last_tick_cached {
        env.clock_interval_s.saturating_mul(2).max(10)
    } else {
        DURABLE_CLOCK_LIMIT_S
    };
    let recent = env
        .last_tick
        .and_then(|tick| env.now.checked_sub(tick))
        .is_some_and(|age| age < limit);
    let (status, detail) = match (recent, env.last_tick_cached) {
        (true, true) => (Pass, "The clock schedule saw a tick within two probe intervals."),
        (true, false) => (Warn, "Schedule unavailable (off-shield): a scheduled job ran recently, but this check cannot see individual ticks."),
        (false, _) => (Fail, "No recent tick or scheduled run was recorded."),
    };
    report.add("clock", "Ingestion clock", status, detail, if status == Pass { "" } else { "Verify flp-front's health checks, the IAD shield and clock_interval_s; inspect /_tick delivery." });
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;

    // Synthetic boundary responses; recorded account contracts are tested separately.
    #[derive(Default)]
    struct Api(
        BTreeMap<String, Result<(u16, Value), String>>,
        Vec<Value>,
        Vec<String>,
    );

    impl CheckApi for Api {
        fn get(&mut self, path: &str) -> Result<(u16, Value), String> {
            self.2.push(path.into());
            self.0
                .get(path)
                .cloned()
                .unwrap_or_else(|| Err("synthetic transport failure: SECRET".into()))
        }

        fn record_failure(&mut self, event: Value) {
            self.1.push(event);
        }
    }

    struct Probe(u16);

    impl CheckProbe for Probe {
        fn probe(&mut self, _: &str, _: &str) -> Result<u16, String> {
            Ok(self.0)
        }
    }

    fn environment() -> Environment {
        Environment {
            now: 1_700_000_000,
            front_rl: false,
            front_verified: true,
            front_service_id: "front".into(),
            flp_service_id: "app".into(),
            worker_service_id: "worker".into(),
            config_store_id: "config".into(),
            secret_store_id: "secret".into(),
            kv_store_id: "state".into(),
            buckets_dictionary_id: "buckets".into(),
            credentials_dictionary_id: "credentials".into(),
            compute_host: "example.edgecompute.app".into(),
            install_name: "test".into(),
            last_tick: Some(1_699_999_999),
            last_tick_cached: true,
            clock_interval_s: 1,
            fos_regions: vec!["us-east-1".into()],
        }
    }

    fn api() -> Api {
        let mut api = Api::default();
        for id in ["front", "app", "worker"] {
            api.0.insert(
                format!("/service/{id}/version"),
                Ok((200, json!([{"number":4,"active":true}]))),
            );
        }
        api.0.insert(
            "/service/front/version/4/generated_vcl".into(),
            Ok((
                200,
                json!({"content":"pragma optional_param ratelimit_opt_in true;"}),
            )),
        );
        for (id, count) in [("buckets", 1), ("credentials", 2)] {
            api.0.insert(
                format!("/service/front/version/4/dictionary/{id}/info"),
                Ok((200, json!({"item_count":count}))),
            );
        }
        for id in ["app", "worker"] {
            api.0.insert(
                format!("/service/{id}/version/4/resource"),
                Ok((200, json!([
                    {"name":"flp_config","resource_id":"config","resource_type":"config_store"},
                    {"name":"flp_secrets","resource_id":"secret","resource_type":"secret_store"},
                    {"name":"flp_state","resource_id":"state","resource_type":"kv_store"}
                ]))),
            );
        }
        api.0.insert(
            "/enabled-products/v1/object_storage".into(),
            Ok((200, json!({}))),
        );
        api
    }

    fn status(report: &CheckReport, id: &str) -> CheckStatus {
        report.checks.iter().find(|c| c.id == id).unwrap().status
    }

    #[test]
    fn goal1_checks_do_not_require_or_validate_a_management_token() {
        let mut api = api();
        api.0.remove("/tokens/self");
        let report = run_checks(&mut api, &mut Probe(403), &environment());
        assert!(!api.2.iter().any(|path| path == "/tokens/self"));
        assert!(!report.checks.iter().any(|check| check.id == "admin_token"));
        assert_eq!(status(&report, "flp_stores"), CheckStatus::Pass);
        assert_eq!(status(&report, "object_storage"), CheckStatus::Pass);
    }

    #[test]
    fn detects_unavailable_and_undeployed_erl_without_assuming_success() {
        let env = environment();
        let mut api = api();
        let report = run_checks(&mut api, &mut Probe(403), &env);
        assert_eq!(status(&report, "edge_rate_limiting"), CheckStatus::Warn);
        assert!(report.checks[0].fix.as_ref().unwrap().contains("front"));
        api.0.insert(
            "/service/front/version/4/generated_vcl".into(),
            Ok((200, json!({"content":"pragma other true;"}))),
        );
        let report = run_checks(&mut api, &mut Probe(403), &env);
        assert!(report.checks[0].message.contains("KV"));
        api.0.insert(
            "/service/front/version/4/generated_vcl".into(),
            Ok((200, json!({}))),
        );
        let report = run_checks(&mut api, &mut Probe(403), &env);
        assert_eq!(status(&report, "edge_rate_limiting"), CheckStatus::Warn);
        assert!(report.checks[0].message.contains("verify"));
    }

    #[test]
    fn refuses_wrong_store_ids_and_capacity_at_default_limit() {
        let mut api = api();
        api.0.insert(
            "/service/worker/version/4/resource".into(),
            Ok((
                200,
                json!([
                    {"name":"flp_config","resource_id":"wrong","resource_type":"config_store"},
                    {"name":"flp_secrets","resource_id":"secret","resource_type":"secret_store"},
                    {"name":"flp_state","resource_id":"state","resource_type":"kv_store"}
                ]),
            )),
        );
        api.0.insert(
            "/service/front/version/4/dictionary/credentials/info".into(),
            Ok((200, json!({"item_count":1000}))),
        );
        let report = run_checks(&mut api, &mut Probe(403), &environment());
        assert_eq!(status(&report, "worker_stores"), CheckStatus::Fail);
        assert_eq!(status(&report, "dictionary_credentials"), CheckStatus::Warn);
        assert_eq!(status(&report, "dictionary_buckets"), CheckStatus::Pass);
        api.0.insert(
            "/service/front/version/4/dictionary/credentials/info".into(),
            Ok((403, json!({"item_count":0,"detail":"SECRET"}))),
        );
        let report = run_checks(&mut api, &mut Probe(403), &environment());
        assert_eq!(status(&report, "dictionary_credentials"), CheckStatus::Fail);
        assert!(!serde_json::to_string(&report).unwrap().contains("SECRET"));
    }

    #[test]
    fn clock_rejects_missing_stale_and_future_ticks() {
        let mut env = environment();
        for (tick, cached, expected) in [
            (None, true, CheckStatus::Fail),
            (Some(env.now - 10), true, CheckStatus::Fail),
            (Some(env.now + 1), true, CheckStatus::Fail),
            (Some(env.now - 9), true, CheckStatus::Pass),
            (Some(env.now - 9), false, CheckStatus::Warn),
            (Some(env.now - 299), false, CheckStatus::Warn),
            (Some(env.now - 300), false, CheckStatus::Fail),
        ] {
            env.last_tick = tick;
            env.last_tick_cached = cached;
            assert_eq!(
                status(&run_checks(&mut api(), &mut Probe(403), &env), "clock"),
                expected,
                "{tick:?} {cached}"
            );
        }
    }

    #[test]
    fn failures_are_redacted_and_requests_are_bounded() {
        let mut env = environment();
        env.fos_regions = vec!["us-east-1".into(); 50];
        let mut api = Api::default();
        api.0.insert(
            "/tokens/self".into(),
            Ok((429, json!({"access_token":"SECRET","msg":"SECRET"}))),
        );
        let report = run_checks(&mut api, &mut Probe(503), &env);
        assert!(report.api_calls + report.probe_calls < 32);
        assert!(!serde_json::to_string(&report).unwrap().contains("SECRET"));
        assert_eq!(status(&report, "direct_compute"), CheckStatus::Fail);
        assert_eq!(status(&report, "object_storage"), CheckStatus::Fail);
        assert!(!report
            .checks
            .iter()
            .any(|c| c.status == CheckStatus::Pass && c.id != "front_path" && c.id != "clock"));
    }

    #[test]
    fn requires_authenticated_front_and_direct_compute_denial() {
        let mut env = environment();
        env.front_verified = false;
        env.front_rl = true;
        let report = run_checks(&mut api(), &mut Probe(200), &env);
        assert_eq!(status(&report, "front_path"), CheckStatus::Fail);
        assert_eq!(status(&report, "direct_compute"), CheckStatus::Fail);
        assert_ne!(status(&report, "edge_rate_limiting"), CheckStatus::Pass);
    }

    #[test]
    fn account_enablement_and_region_reachability_are_independent() {
        let mut api = api();
        let mut env = environment();
        env.front_rl = true;
        let report = run_checks(&mut api, &mut Probe(403), &env);
        for id in [
            "edge_rate_limiting",
            "flp_stores",
            "worker_stores",
            "object_storage",
            "fos_region_0",
        ] {
            assert_eq!(status(&report, id), CheckStatus::Pass, "{id}");
        }
        for status_code in [404, 500] {
            api.0.insert(
                "/enabled-products/v1/object_storage".into(),
                Ok((status_code, json!({"error":"SECRET"}))),
            );
            let report = run_checks(&mut api, &mut Probe(403), &env);
            assert_eq!(status(&report, "object_storage"), CheckStatus::Fail);
            assert_eq!(status(&report, "fos_region_0"), CheckStatus::Pass);
            assert!(!serde_json::to_string(&report).unwrap().contains("SECRET"));
        }
        let report = run_checks(&mut api, &mut Probe(503), &env);
        assert_eq!(status(&report, "fos_region_0"), CheckStatus::Fail);
    }

    #[test]
    fn recorded_account_responses_report_erl_redeployment_and_missing_links() {
        let mut api = Api::default();
        for fixture in [
            include_str!("../tests/fixtures/fastly-api/object-storage.json"),
            include_str!("../tests/fixtures/fastly-api/test-service-versions.json"),
            include_str!("../tests/fixtures/fastly-api/test-service-generated-vcl.json"),
            include_str!("../tests/fixtures/fastly-api/missing-service.json"),
        ] {
            let fixture: Value = serde_json::from_str(fixture).unwrap();
            let path = fixture["request"]["path"]
                .as_str()
                .unwrap()
                .replace("TEST_SERVICE", "front")
                .replace("flp-contract-nonexistent-service", "missing");
            api.0.insert(
                path,
                Ok((
                    fixture["status"].as_u64().unwrap() as u16,
                    fixture["response"].clone(),
                )),
            );
        }
        let mut env = environment();
        env.now = 1_793_145_600;
        env.flp_service_id = "missing".into();
        let report = run_checks(&mut api, &mut Probe(403), &env);
        assert_eq!(status(&report, "object_storage"), CheckStatus::Pass);
        assert_eq!(status(&report, "edge_rate_limiting"), CheckStatus::Warn);
        assert!(report.checks[0]
            .fix
            .as_ref()
            .unwrap()
            .contains("make -C app deploy"));
        assert_eq!(status(&report, "flp_stores"), CheckStatus::Fail);
        let output = serde_json::to_string(&report).unwrap();
        assert!(!output.contains("REDACTED_ID"));
        assert!(!output.contains("Record not found"));
    }

    #[test]
    fn io_failures_emit_only_sanitized_endpoint_and_generic_error_codes() {
        #[derive(Default)]
        struct FailedProbe(Vec<Value>);
        impl CheckProbe for FailedProbe {
            fn probe(&mut self, _: &str, _: &str) -> Result<u16, String> {
                Err("SECRET transport details".into())
            }
            fn record_failure(&mut self, event: Value) {
                self.0.push(event);
            }
        }
        let mut api = Api::default();
        let mut report = CheckReport::default();
        assert!(report.get(&mut api, "/service/SECRET/version").is_none());
        api.0
            .insert("/tokens/self".into(), Ok((429, json!({"msg":"SECRET"}))));
        assert_eq!(report.get(&mut api, "/tokens/self").unwrap().0, 429);
        assert_eq!(api.1.len(), 2);
        assert_eq!(api.1[0]["endpoint"], "/service/{service_id}/version");
        assert_eq!(api.1[0]["code"], "transport_error");
        assert_eq!(api.1[1]["status"], 429);
        assert!(!serde_json::to_string(&api.1).unwrap().contains("SECRET"));
        let mut probe = FailedProbe::default();
        assert!(report.probe(&mut probe, "SECRET.edgecompute.app").is_none());
        assert_eq!(probe.0.len(), 1);
        assert_eq!(probe.0[0]["endpoint"], "compute:/");
        assert_eq!(probe.0[0]["code"], "transport_error");
        assert!(!serde_json::to_string(&probe.0).unwrap().contains("SECRET"));
    }

    #[test]
    fn recorded_install_metadata_requires_active_versions_before_passing() {
        let mut api = api();
        for fixture in [
            include_str!("../tests/fixtures/fastly-api/install-front-versions.json"),
            include_str!("../tests/fixtures/fastly-api/install-compute-versions.json"),
            include_str!("../tests/fixtures/fastly-api/install-worker-versions.json"),
            include_str!("../tests/fixtures/fastly-api/install-compute-resource-links.json"),
            include_str!("../tests/fixtures/fastly-api/install-worker-resource-links.json"),
            include_str!("../tests/fixtures/fastly-api/install-front-buckets-dictionary-info.json"),
            include_str!(
                "../tests/fixtures/fastly-api/install-front-credentials-dictionary-info.json"
            ),
        ] {
            let mut normalized = fixture.to_string();
            for (recorded, synthetic) in [
                ("INSTALL_FRONT_ID", "front"),
                ("INSTALL_COMPUTE_ID", "app"),
                ("INSTALL_WORKER_ID", "worker"),
                ("STORE_CONFIG_ID", "config"),
                ("STORE_SECRET_ID", "secret"),
                ("STORE_KV_ID", "state"),
                ("DICTIONARY_BUCKETS_ID", "buckets"),
                ("DICTIONARY_CREDENTIALS_ID", "credentials"),
            ] {
                normalized = normalized.replace(recorded, synthetic);
            }
            let fixture: Value = serde_json::from_str(&normalized).unwrap();
            api.0.insert(
                fixture["request"]["path"].as_str().unwrap().into(),
                Ok((
                    fixture["status"].as_u64().unwrap() as u16,
                    fixture["response"].clone(),
                )),
            );
        }
        let env = environment();
        let draft_report = run_checks(&mut api, &mut Probe(403), &env);
        for id in [
            "flp_stores",
            "worker_stores",
            "dictionary_buckets",
            "dictionary_credentials",
        ] {
            assert_eq!(status(&draft_report, id), CheckStatus::Fail, "{id}");
        }
        // Activation is synthetic; the recorded install was still draft-only.
        for id in ["front", "app", "worker"] {
            api.0.insert(
                format!("/service/{id}/version"),
                Ok((200, json!([{"number":1,"active":true}]))),
            );
        }
        let report = run_checks(&mut api, &mut Probe(403), &env);
        for id in [
            "flp_stores",
            "worker_stores",
            "dictionary_buckets",
            "dictionary_credentials",
        ] {
            assert_eq!(status(&report, id), CheckStatus::Pass, "{id}");
        }
        let credentials = report
            .checks
            .iter()
            .find(|c| c.id == "dictionary_credentials")
            .unwrap();
        assert!(credentials.message.starts_with("0 items"));
        let output = serde_json::to_string(&report).unwrap();
        assert!(!output.contains("STORE_"));
        assert!(!output.contains("href"));
    }
}

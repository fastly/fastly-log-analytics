use arrow_array::{Array, StringArray};
use ciborium::value::Value;
use fastly_log_analytics::{
    admin::{fixture::FixtureAdapter, Record},
    auth::{
        self,
        webauthn::{self, BrowserCredential, BrowserResponse, Credential},
        AuthState, Error as AuthError, Role, User,
    },
    http::{self, Request, INTERNAL_BODY_LIMIT, PUBLIC_BODY_LIMIT},
    ingest,
    setup::SetupStores,
    shell::{Runtime, Shell},
    state::{MemoryStore, StateStore},
    telemetry::{api::ApiLog, usage::UsageAgg},
    views::{self, filters_reference_pii, SavedView, ViewStore, PII_FILTER_KEYS},
};
use hmac::{Hmac, KeyInit, Mac};
use p256::ecdsa::{signature::Signer, Signature, SigningKey};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

#[test]
fn views_pii_canonicalization_parity_blocks_obfuscated_identifiers() {
    assert_eq!(PII_FILTER_KEYS.len(), 18);

    let bypass_keys = [
        // Percent-encoding (single, double, and mixed single+double UTF-8)
        "client%5Fip",
        "client%255Fip",
        "client_%C4%25B0p",
        "client_%25C4%B0p",
        "%69%70",
        "x%2dforwarded%2dfor",
        "session%255Fid",
        // Turkish dotted-I (U+0130) and combining dot (U+0307) before camelCase
        "client\u{0130}p",
        "client_\u{0130}p",
        "c\u{0130}d",
        "client_i\u{0307}p",
        // CamelCase & IPv4/IPv6 normalization
        "clientIp",
        "xForwardedFor",
        "trueClientIP",
        "clientIPv4",
        "client_ipv6",
        // Iterative bracket stripping
        "client[0]_ip",
        "cli[[a]b]ent_ip",
        "x[foo]-forwarded-for",
        // Zero-width / invisible / C0 control chars
        "c\u{200b}lient_ip",
        "client\u{2060}_ip",
        "ip\u{180e}",
        "client_ip\u{200c}",
        "client\u{200d}_ip",
        "\u{feff}client_ip",
        "\x07client_ip",
        // Affixes & suffixes
        "filter_client_ip",
        "xfilter_session_id",
        "_cf_client_ip",
        "req_http_x_forwarded_for",
        "client_ip_hash",
        "client_ip_v2",
        "client_ip2",
        // Non-word separator normalization
        "clientip",
        "xforwardedfor",
        "sessionid",
    ];

    for key in bypass_keys {
        let payload = json!({
            "filters": [{
                "column": key,
                "mode": "include",
                "value": "1.2.3.4"
            }]
        })
        .to_string();
        assert!(
            filters_reference_pii(&payload),
            "obfuscated PII key {key:?} must be blocked"
        );
    }

    // Legitimate non-PII columns must remain allowed.
    for safe_key in [
        "status",
        "request_method",
        "url_path",
        "cache_status",
        "service_id",
        "req_header_count",
        "is_ipv6",
    ] {
        let payload = json!({
            "filters": [{
                "column": safe_key,
                "mode": "include",
                "value": "ok"
            }]
        })
        .to_string();
        assert!(
            !filters_reference_pii(&payload),
            "non-PII key {safe_key:?} must remain allowed"
        );
    }
}

fn cbor_bytes(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    ciborium::into_writer(value, &mut out).expect("cbor encode");
    out
}

fn es256_cose_key(signing: &SigningKey) -> Vec<u8> {
    let point = signing.verifying_key().to_sec1_point(false);
    let x = point.x().expect("x").as_slice().to_vec();
    let y = point.y().expect("y").as_slice().to_vec();
    cbor_bytes(&Value::Map(vec![
        (Value::Integer(1.into()), Value::Integer(2.into())), // kty: EC2
        (Value::Integer(3.into()), Value::Integer((-7).into())), // alg: ES256
        (Value::Integer((-1).into()), Value::Integer(1.into())), // crv: P-256
        (Value::Integer((-2).into()), Value::Bytes(x)),
        (Value::Integer((-3).into()), Value::Bytes(y)),
    ]))
}

#[test]
fn webauthn_rejects_deeply_nested_cbor_extensions_on_register_and_assert() {
    let origin = "https://logs.example";
    let rp_id = "logs.example";
    let challenge = "challenge-nonce";
    let cred_id_bytes = b"cred-id-1234";
    let cred_id = auth::encode(cred_id_bytes);
    let signing = SigningKey::from_bytes(&[7u8; 32].into()).expect("key");
    let cose_key = es256_cose_key(&signing);

    // Build a 40-deep nested definite CBOR array (0x81 * 40 + 0x00):
    // shallow enough not to overflow native test stack on unhardened code,
    // deep enough to exceed MAX_CBOR_DEPTH (32).
    let mut deep_def = vec![0x81u8; 40];
    deep_def.push(0x00);

    // Build a 40-deep nested indefinite CBOR array (0x9f * 40 + 0xff * 40).
    let mut deep_indef = vec![0x9fu8; 40];
    deep_indef.extend(std::iter::repeat_n(0xffu8, 40));

    for deep_ext in [&deep_def, &deep_indef] {
        // 1) Registration with ED flag (0x80) and deeply nested CBOR extensions
        let mut reg_auth = Vec::new();
        reg_auth.extend_from_slice(&Sha256::digest(rp_id.as_bytes()));
        reg_auth.push(0x01 | 0x04 | 0x40 | 0x80); // UP | UV | AT | ED
        reg_auth.extend_from_slice(&1u32.to_be_bytes());
        reg_auth.extend_from_slice(&[0u8; 16]); // AAGUID
        reg_auth.extend_from_slice(&(cred_id_bytes.len() as u16).to_be_bytes());
        reg_auth.extend_from_slice(cred_id_bytes);
        reg_auth.extend_from_slice(&cose_key);
        reg_auth.extend_from_slice(deep_ext);

        let attestation = cbor_bytes(&Value::Map(vec![
            (Value::Text("fmt".into()), Value::Text("none".into())),
            (Value::Text("attStmt".into()), Value::Map(Vec::new())),
            (Value::Text("authData".into()), Value::Bytes(reg_auth)),
        ]));
        let create_client = json!({
            "type": "webauthn.create",
            "challenge": challenge,
            "origin": origin,
            "crossOrigin": false,
        })
        .to_string();
        let reg_input = BrowserCredential {
            id: cred_id.clone(),
            raw_id: cred_id.clone(),
            kind: "public-key".into(),
            response: BrowserResponse {
                client_data_json: auth::encode(create_client.as_bytes()),
                attestation_object: Some(auth::encode(&attestation)),
                authenticator_data: None,
                signature: None,
                user_handle: None,
            },
        };
        assert_eq!(
            webauthn::register(&reg_input, challenge, origin, rp_id),
            Err(AuthError::InvalidCredential)
        );

        // 2) Assertion with ED flag (0x80) and deeply nested CBOR extensions
        let credential = webauthn::Credential {
            id: cred_id.clone(),
            public_key: cose_key.clone(),
            sign_count: 1,
            revoked: false,
        };
        let mut get_auth = Vec::new();
        get_auth.extend_from_slice(&Sha256::digest(rp_id.as_bytes()));
        get_auth.push(0x01 | 0x04 | 0x80); // UP | UV | ED
        get_auth.extend_from_slice(&2u32.to_be_bytes());
        get_auth.extend_from_slice(deep_ext);

        let get_client = json!({
            "type": "webauthn.get",
            "challenge": challenge,
            "origin": origin,
            "crossOrigin": false,
        })
        .to_string();
        let mut signed_msg = get_auth.clone();
        signed_msg.extend_from_slice(&Sha256::digest(get_client.as_bytes()));
        let sig: Signature = signing.sign(&signed_msg);

        let assert_input = BrowserCredential {
            id: cred_id.clone(),
            raw_id: cred_id.clone(),
            kind: "public-key".into(),
            response: BrowserResponse {
                client_data_json: auth::encode(get_client.as_bytes()),
                attestation_object: None,
                authenticator_data: Some(auth::encode(&get_auth)),
                signature: Some(auth::encode(sig.to_der().as_bytes())),
                user_handle: None,
            },
        };
        assert_eq!(
            webauthn::assert(&assert_input, &credential, challenge, origin, rp_id),
            Err(AuthError::InvalidCredential)
        );
    }
}

#[test]
fn bogus_worker_key_header_presence_cannot_amplify_body_limit() {
    for path in [ingest::worker::PATH, ingest::worker::RANGE_PATH] {
        let req = Request {
            method: "POST".into(),
            path: path.into(),
            headers: [("x-flp-worker-key".into(), "bogus-unauthenticated".into())].into(),
            body: Vec::new(),
        };
        assert_eq!(http::body_limit(&req), PUBLIC_BODY_LIMIT);
    }
}

struct WorkerSecretRuntime {
    worker_secret: Option<String>,
}

impl SetupStores for WorkerSecretRuntime {
    fn config_present(&self, _: &str) -> Result<bool, String> {
        Ok(true)
    }
    fn secret_present(&self, name: &str) -> Result<bool, String> {
        Ok(name == "worker_secret" && self.worker_secret.is_some())
    }
    fn state_linked(&self) -> Result<bool, String> {
        Ok(true)
    }
}

impl Runtime for WorkerSecretRuntime {
    fn config(&self, _: &str) -> Result<Option<String>, String> {
        Ok(None)
    }
    fn secret(&self, name: &str) -> Result<Option<String>, String> {
        if name == "worker_secret" {
            Ok(self.worker_secret.clone())
        } else {
            Ok(None)
        }
    }
    fn random(&self) -> Result<[u8; 32], String> {
        Ok([1u8; 32])
    }
    fn now(&self) -> u64 {
        1_700_000_000
    }
    fn checks(&self, _: bool) -> Result<serde_json::Value, String> {
        Ok(json!({}))
    }
    fn versions(&self) -> Result<serde_json::Value, String> {
        Ok(json!({}))
    }
    fn compute_service_id(&self) -> Option<String> {
        Some("testWorkerSvc".into())
    }
    fn compute_service_version(&self) -> Option<String> {
        Some("1".into())
    }
}

#[test]
fn empty_worker_secret_fails_closed_for_body_limit_and_ingest() {
    let empty_secret_rt = WorkerSecretRuntime {
        worker_secret: Some(String::new()),
    };

    for path in [ingest::worker::PATH, ingest::worker::RANGE_PATH] {
        for headers in [
            std::collections::BTreeMap::new(),
            [("x-flp-worker-key".into(), String::new())].into(),
            [("x-flp-worker-key".into(), "anything".into())].into(),
        ] {
            let req = Request {
                method: "POST".into(),
                path: path.into(),
                headers,
                body: Vec::new(),
            };
            assert_eq!(
                http::body_limit_for_runtime(&empty_secret_rt, &req),
                PUBLIC_BODY_LIMIT,
                "empty worker_secret must never grant INTERNAL_BODY_LIMIT on {path}"
            );
            let resp = ingest::worker::handle_worker_ingest(&empty_secret_rt, &req);
            assert_eq!(
                resp.status, 403,
                "empty worker_secret must never authorize {path}"
            );
        }
    }

    // Positive control: a non-empty matching secret still grants INTERNAL_BODY_LIMIT.
    let valid_rt = WorkerSecretRuntime {
        worker_secret: Some("real-worker-secret".into()),
    };
    let valid_req = Request {
        method: "POST".into(),
        path: ingest::worker::PATH.into(),
        headers: [("x-flp-worker-key".into(), "real-worker-secret".into())].into(),
        body: Vec::new(),
    };
    assert_eq!(
        http::body_limit_for_runtime(&valid_rt, &valid_req),
        INTERNAL_BODY_LIMIT
    );
}

#[test]
fn views_column_projection_and_unicode_ws_subtree_keys_block_pii() {
    let bypass_payloads = [
        json!({"columns": {"include": ["client_ip"]}}),
        json!({"columns": {"mode": "include", "values": ["client_ip"]}}),
        json!({"fields": {"include": ["client_ip"]}}),
        json!({"dimensions": {"mode": "include", "values": ["oip"]}}),
        json!({"sort_by": {"include": ["client_ip"]}}),
        json!({"col": "columns", "values": ["client_ip"]}),
        json!({"cols": {"include": ["client_ip"]}}),
        json!({"visible_columns": {"include": ["client_ip"]}}),
        json!({"selected_columns": {"include": ["client_ip"]}}),
        json!({"hidden_columns": {"include": ["client_ip"]}}),
        json!({"display_columns": {"include": ["client_ip"]}}),
        json!({"group_columns": {"include": ["client_ip"]}}),
        json!({"sort_columns": {"include": ["client_ip"]}}),
        json!({"order_columns": {"include": ["client_ip"]}}),
        json!({"projections": {"include": ["client_ip"]}}),
        // Zero-width / Unicode SQL whitespace around reserved subtree keys
        json!({"\u{200b}columns": {"include": ["client_ip"]}}),
        json!({"\u{feff}sort_by": {"include": ["client_ip"]}}),
        // Mixed single+double percent-encoding of U+0130 (Turkish dotted I)
        json!({"filters": [{"column": "client_%C4%25B0p", "mode": "include", "value": "1.2.3.4"}]}),
    ];

    for payload in bypass_payloads {
        let raw = payload.to_string();
        assert!(
            filters_reference_pii(&raw),
            "PII payload must be blocked: {raw}"
        );
    }
}

struct StaticViewStore {
    views: Vec<SavedView>,
}

impl ViewStore for StaticViewStore {
    fn get(&self, _key: &str) -> Result<Option<Record>, String> {
        Ok(Some(Record {
            generation: "1".into(),
            value: json!({"views": self.views}),
        }))
    }
    fn cas(
        &self,
        _key: &str,
        _generation: Option<&str>,
        _value: &serde_json::Value,
    ) -> Result<bool, String> {
        Ok(true)
    }
    fn random(&self) -> Result<[u8; 32], String> {
        Ok([2u8; 32])
    }
    fn now(&self) -> u64 {
        1_700_000_000
    }
}

#[test]
fn list_for_role_hides_views_with_pii_in_page_query_string() {
    let make_view = |id: &str, page: Option<&str>| SavedView {
        id: id.into(),
        service_id: "svc1".into(),
        name: format!("view-{id}"),
        filters_json: "{}".into(),
        time_range_type: None,
        start_time: None,
        end_time: None,
        page: page.map(str::to_owned),
        created_at: None,
    };

    let store = StaticViewStore {
        views: vec![
            make_view("safe-none", None),
            make_view("safe-path", Some("/logs")),
            make_view("safe-query", Some("/logs?status=200&method=GET")),
            make_view("pii-direct", Some("/logs?client_ip=198.51.100.7")),
            make_view(
                "pii-encoded",
                Some("/logs?status=200&client_%C4%25B0p=198.51.100.7"),
            ),
            make_view("pii-fragment", Some("/logs?oip=1.2.3.4#top")),
            make_view(
                "safe-json-filters",
                Some("/logs?filters=%7B%22status%22%3A%7B%22mode%22%3A%22include%22%2C%22values%22%3A%5B%22200%22%5D%7D%7D"),
            ),
            make_view(
                "pii-json-filters-param",
                Some("/logs?filters=%7B%22client_ip%22%3A%7B%22mode%22%3A%22include%22%2C%22values%22%3A%5B%22198.51.100.7%22%5D%7D%7D"),
            ),
            make_view("pii-query-value-col", Some("/logs?col=client_ip&val=198.51.100.7")),
            make_view(
                "pii-query-value-sql",
                Some("/query?sql=SELECT+client_ip+FROM+logs"),
            ),
            make_view(
                "pii-semicolon-sep",
                Some("/logs?status=200;client_ip=198.51.100.7"),
            ),
            make_view(
                "pii-fragment-kv",
                Some("/logs?status=200#client_ip=198.51.100.7"),
            ),
            make_view("pii-fragment-only", Some("/logs#client_ip=198.51.100.7")),
            make_view("pii-extra-query", Some("/logs?_extra=198.51.100.7")),
            make_view(
                "pii-extra-dotted",
                Some("/logs?_extra.custom_ip=198.51.100.7"),
            ),
            make_view("pii-extra-encoded", Some("/logs?%5Fextra=198.51.100.7")),
            make_view("pii-extra-fragment", Some("/logs#_extra=198.51.100.7")),
            make_view(
                "pii-double-hash-fragment",
                Some("/logs#top#client_ip=198.51.100.7"),
            ),
            make_view(
                "pii-query-double-hash",
                Some("/logs?status=200#section#client_ip=198.51.100.7"),
            ),
            make_view("pii-hash-router-extra", Some("/#/logs?_extra=198.51.100.7")),
            make_view(
                "pii-hash-router-extra-dotted",
                Some("/#/logs?_extra.custom_ip=198.51.100.7"),
            ),
            make_view(
                "pii-frag-query-extra",
                Some("/logs?status=200#section?_extra=198.51.100.7"),
            ),
            make_view(
                "pii-frag-query-extra-dotted",
                Some("/logs?status=200#section?_extra.custom_ip=198.51.100.7"),
            ),
            make_view(
                "pii-encoded-delim-extra",
                Some("/logs?status%3D200%26_extra=198.51.100.7"),
            ),
            make_view(
                "pii-encoded-hash-extra",
                Some("/logs?status%3D200%23_extra=198.51.100.7"),
            ),
        ],
    };

    let analyst = views::list_for_role(&store, "svc1", Role::AnalystNoPii, 50).expect("list");
    let analyst_ids: Vec<&str> = analyst.iter().map(|v| v.id.as_str()).collect();
    assert_eq!(
        analyst_ids,
        vec!["safe-none", "safe-path", "safe-query", "safe-json-filters"]
    );

    let admin = views::list_for_role(&store, "svc1", Role::Admin, 50).expect("list");
    assert_eq!(admin.len(), 25);
}

const DEBUG_TEST_SESSION_KEY: &[u8] = b"debug-gate-session-signing-key-32b";

struct DebugGateRuntime {
    adapter: FixtureAdapter,
    usage: UsageAgg,
    api_log: ApiLog,
}

impl DebugGateRuntime {
    fn new() -> Self {
        Self {
            adapter: FixtureAdapter::new(),
            usage: UsageAgg::default(),
            api_log: ApiLog::default(),
        }
    }
}

impl SetupStores for DebugGateRuntime {
    fn config_present(&self, _: &str) -> Result<bool, String> {
        Ok(true)
    }
    fn secret_present(&self, _: &str) -> Result<bool, String> {
        Ok(true)
    }
    fn state_linked(&self) -> Result<bool, String> {
        Ok(true)
    }
}

impl Runtime for DebugGateRuntime {
    fn config(&self, key: &str) -> Result<Option<String>, String> {
        Ok(Some(
            match key {
                "public_base_url" => "https://logs.example.com",
                _ => "fixtureValue",
            }
            .into(),
        ))
    }
    fn secret(&self, key: &str) -> Result<Option<String>, String> {
        match key {
            "front_key" => Ok(Some("debug-front-key".into())),
            "session_key" => Ok(Some(
                String::from_utf8(DEBUG_TEST_SESSION_KEY.to_vec()).unwrap(),
            )),
            "cdn_key" => Ok(Some("debug-cdn-key".into())),
            _ => self.adapter.secret(key),
        }
    }
    fn random(&self) -> Result<[u8; 32], String> {
        Ok([5u8; 32])
    }
    fn now(&self) -> u64 {
        1_800_000_000
    }
    fn checks(&self, _: bool) -> Result<serde_json::Value, String> {
        Ok(json!({}))
    }
    fn versions(&self) -> Result<serde_json::Value, String> {
        Ok(json!({}))
    }
    fn compute_service_id(&self) -> Option<String> {
        Some("debugTestApp".into())
    }
    fn compute_service_version(&self) -> Option<String> {
        Some("1".into())
    }
    fn admin_get(&self, key: &str) -> Result<Option<Record>, String> {
        self.adapter.get(key)
    }
    fn admin_cas(
        &self,
        key: &str,
        generation: Option<&str>,
        value: &serde_json::Value,
    ) -> Result<bool, String> {
        self.adapter.cas(key, generation, value)
    }
    fn usage_agg(&self) -> Option<&UsageAgg> {
        Some(&self.usage)
    }
    fn api_log(&self) -> Option<&ApiLog> {
        Some(&self.api_log)
    }
}

fn make_test_user(id: &str, role: Role, now: u64) -> User {
    User {
        id: id.into(),
        display_name: id.into(),
        role,
        credentials: vec![Credential {
            id: format!("{id}-cred"),
            public_key: vec![1],
            sign_count: 0,
            revoked: false,
        }],
        revoked: false,
        created_by: "test".into(),
        created_at: now,
        last_login_at: None,
    }
}

fn sign_session_cookie(user: &User, exp: u64) -> String {
    let claims = auth::encode(
        json!({"sub": user.id, "role": user.role, "policy_version": 0, "exp": exp})
            .to_string()
            .as_bytes(),
    );
    let mut mac = Hmac::<Sha256>::new_from_slice(DEBUG_TEST_SESSION_KEY).unwrap();
    mac.update(claims.as_bytes());
    format!(
        "flp_session={claims}.{}",
        auth::encode(&mac.finalize().into_bytes())
    )
}

#[test]
fn x_debug_responses_gated_by_api_call_debug_visibility_and_role() {
    let now = 1_800_000_000u64;
    let admin = make_test_user("u-admin", Role::Admin, now);
    let analyst = make_test_user("u-analyst", Role::Analyst, now);
    let no_pii = make_test_user("u-nopii", Role::AnalystNoPii, now);

    let admin_cookie = sign_session_cookie(&admin, now + 3600);
    let analyst_cookie = sign_session_cookie(&analyst, now + 3600);
    let no_pii_cookie = sign_session_cookie(&no_pii, now + 3600);

    let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let expected_trace_id = "4bf92f3577b34da6a3ce929d0e0e4736";

    let cases: &[(&str, bool, bool, bool)] = &[
        ("disabled", false, false, false),
        ("admins", true, false, false),
        ("analysts", false, true, true),
        ("both", true, true, true),
        ("unknown-invalid", false, false, false),
    ];

    for &(visibility, expect_admin, expect_analyst, expect_no_pii) in cases {
        let mut state = AuthState::default();
        state.usage_logging.api_call_debug_visibility = visibility.into();
        for u in [&admin, &analyst, &no_pii] {
            state.users.insert(u.id.clone(), u.clone());
        }
        let store = MemoryStore::default();
        assert!(store.compare_and_swap(None, &state).unwrap());

        for (label, cookie, expected) in [
            ("admin", Some(admin_cookie.as_str()), expect_admin),
            ("analyst", Some(analyst_cookie.as_str()), expect_analyst),
            ("no_pii", Some(no_pii_cookie.as_str()), expect_no_pii),
            ("anonymous", None, false),
        ] {
            let rt = DebugGateRuntime::new();
            rt.api_log
                .record("GET", "/service/fixtureService", Some(200), 4, None, "");
            rt.usage.record_fos(
                "GET",
                "processed/request/secret-key.parquet",
                "",
                Some(200),
                128,
                0,
                3,
            );
            let shell = Shell::new(&rt, &store, &[]);
            let mut headers = BTreeMap::from([
                ("x-flp-front-key".into(), "debug-front-key".into()),
                ("fastly-client-ip".into(), "192.0.2.1".into()),
                ("x-flp-front-rl".into(), "1".into()),
                ("x-debug-responses".into(), "1".into()),
                ("traceparent".into(), traceparent.into()),
            ]);
            if let Some(c) = cookie {
                headers.insert("cookie".into(), c.into());
            }
            let res = shell.handle(&Request {
                method: "GET".into(),
                path: "/api/rum/versions".into(),
                headers,
                body: Vec::new(),
            });
            if cookie.is_some() {
                assert_eq!(res.status, 200, "visibility={visibility} role={label}");
            } else {
                assert_eq!(res.status, 401, "visibility={visibility} role={label}");
            }
            if expected {
                assert_eq!(
                    res.headers.get("x-flp-trace-id").map(String::as_str),
                    Some(expected_trace_id),
                    "expected x-flp-trace-id for visibility={visibility} role={label}"
                );
                assert!(
                    res.headers.contains_key("x-flp-debug-calls"),
                    "expected x-flp-debug-calls for visibility={visibility} role={label}"
                );
            } else {
                assert!(
                    !res.headers.contains_key("x-flp-trace-id"),
                    "x-flp-trace-id must be suppressed for visibility={visibility} role={label}"
                );
                assert!(
                    !res.headers.contains_key("x-flp-debug-calls"),
                    "x-flp-debug-calls must be suppressed for visibility={visibility} role={label}"
                );
            }
        }
    }
}

fn zstd_lines(lines: &[String]) -> Vec<u8> {
    let mut raw = lines.join("\n").into_bytes();
    raw.push(b'\n');
    zstd::stream::encode_all(raw.as_slice(), 1).expect("zstd encode")
}

fn assert_col_max_bytes(batch: &arrow_array::RecordBatch, col_name: &str, max_bytes: usize) {
    let idx = batch
        .schema()
        .index_of(col_name)
        .unwrap_or_else(|_| panic!("missing column {col_name}"));
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("column {col_name} is not Utf8"));
    for i in 0..arr.len() {
        if !arr.is_null(i) {
            let val = arr.value(i);
            assert!(
                val.len() <= max_bytes,
                "column {col_name} row {i} length {} exceeds {max_bytes}",
                val.len()
            );
        }
    }
}

#[test]
fn rum_beacon_caps_counts_and_field_lengths_without_panicking() {
    const MAX_METRICS: usize = 64;
    const MAX_RESOURCES: usize = 64;
    const MAX_ERRORS: usize = 16;
    const MAX_PATH_BYTES: usize = 512;
    const MAX_FIELD_BYTES: usize = 512;
    const MAX_ERROR_BYTES: usize = 1024;
    let sid = "AbCdEfGhIjKlMnOpQrSt12";
    let base_s = 1_800_000_000i64;
    let ts_ms = base_s * 1000;

    // 1a) Adversarial ~8 KB batched query-string beacon:
    //     ~4 KB multibyte UTF-8 pathname + 1,000 short `m=a:1` metrics + oversized `cid`.
    let long_utf8_path = format!("/{}", "é🔥".repeat(700)); // ~4.2 KB, 2- & 4-byte UTF-8 chars
    let long_utf8_cid = "ç".repeat(400); // 800 bytes > 512
    let long_utf8_err = "💥error".repeat(200); // 1.8 KB > 1024
    let many_metrics = std::iter::repeat_n("a:1", 1000)
        .collect::<Vec<_>>()
        .join(",");
    let metrics_line = json!({
        "ts_ms": ts_ms,
        "service_id": sid,
        "url": format!("https://example.com/_rum?p={long_utf8_path}&cid={long_utf8_cid}&m={many_metrics}"),
    })
    .to_string();

    let metrics_out =
        ingest::rum::convert_object(&zstd_lines(&[metrics_line]), 60, base_s, 1 << 20, sid)
            .expect("convert batch metrics beacon");
    assert_eq!(
        metrics_out.bad_lines, 0,
        "metrics beacon must stay under MAX_LINE_BYTES"
    );
    let metrics_vitals: usize = metrics_out.windows.values().map(|b| b.num_rows()).sum();
    assert_eq!(
        metrics_vitals, MAX_METRICS,
        "batch beacon must cap metrics at {MAX_METRICS}"
    );
    for b in metrics_out.windows.values() {
        assert_col_max_bytes(b, "pathname", MAX_PATH_BYTES);
        assert_col_max_bytes(b, "cid", MAX_FIELD_BYTES);
        assert_col_max_bytes(b, "metric_name", MAX_FIELD_BYTES);
        assert_col_max_bytes(b, "metric_rating", MAX_FIELD_BYTES);
    }

    // 1b) Batched resources (>64 `r=` entries) + errors (>16 `e=` entries) with oversized UTF-8 fields:
    let many_resources = (0..100)
        .map(|i| {
            if i == 0 {
                format!("rid0_{}:/{}:12.5", "x".repeat(600), "é".repeat(300))
            } else {
                format!("rid{i}:/r{i}:12.5")
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    let many_errors = (0..30)
        .map(|i| {
            if i == 0 {
                format!("e={long_utf8_err}|/{}.js|10|2", "é".repeat(300))
            } else {
                format!("e=err{i}|/app.js|10|2")
            }
        })
        .collect::<Vec<_>>()
        .join("&");
    let res_err_line = json!({
        "ts_ms": ts_ms,
        "service_id": sid,
        "url": format!("https://example.com/_rum?p={long_utf8_path}&r={many_resources}&{many_errors}"),
    })
    .to_string();

    let res_err_out =
        ingest::rum::convert_object(&zstd_lines(&[res_err_line]), 60, base_s, 1 << 20, sid)
            .expect("convert batch resource+error beacon");
    assert_eq!(
        res_err_out.bad_lines, 0,
        "resource+error beacon must stay under MAX_LINE_BYTES"
    );
    let res_vitals: usize = res_err_out.windows.values().map(|b| b.num_rows()).sum();
    let err_rows: usize = res_err_out.errors.values().map(|b| b.num_rows()).sum();
    assert_eq!(
        res_vitals, MAX_RESOURCES,
        "batch beacon must cap resources at {MAX_RESOURCES}"
    );
    assert_eq!(
        err_rows, MAX_ERRORS,
        "batch beacon must cap errors at {MAX_ERRORS}"
    );
    for b in res_err_out.windows.values() {
        assert_col_max_bytes(b, "pathname", MAX_PATH_BYTES);
        assert_col_max_bytes(b, "resource_url", MAX_PATH_BYTES);
        assert_col_max_bytes(b, "req_id", MAX_FIELD_BYTES);
    }
    for b in res_err_out.errors.values() {
        assert_col_max_bytes(b, "pathname", MAX_PATH_BYTES);
        assert_col_max_bytes(b, "error_message", MAX_ERROR_BYTES);
        assert_col_max_bytes(b, "error_file", MAX_PATH_BYTES);
    }

    // 2) VCL multi-param query-string beacon (`vcl_query_metrics`):
    let mut vcl_parts = vec![format!("rum_pathname={long_utf8_path}")];
    for i in 0..120 {
        vcl_parts.push(format!("rum_metric_name=m{i}&rum_metric_value=1"));
    }
    vcl_parts.push(format!("rum_error_message={long_utf8_err}"));
    for i in 1..30 {
        vcl_parts.push(format!("rum_error_message=err_{i}"));
    }
    let vcl_line = json!({
        "ts_ms": ts_ms,
        "service_id": sid,
        "url": format!("https://example.com/_rum?{}", vcl_parts.join("&")),
    })
    .to_string();
    let vcl_out = ingest::rum::convert_object(&zstd_lines(&[vcl_line]), 60, base_s, 1 << 20, sid)
        .expect("convert vcl query beacon");
    assert_eq!(
        vcl_out.bad_lines, 0,
        "vcl beacon must stay under MAX_LINE_BYTES"
    );
    let vcl_vitals: usize = vcl_out.windows.values().map(|b| b.num_rows()).sum();
    let vcl_errors: usize = vcl_out.errors.values().map(|b| b.num_rows()).sum();
    assert_eq!(vcl_vitals, MAX_METRICS);
    assert_eq!(vcl_errors, MAX_ERRORS);
    for b in vcl_out.windows.values() {
        assert_col_max_bytes(b, "pathname", MAX_PATH_BYTES);
    }
    for b in vcl_out.errors.values() {
        assert_col_max_bytes(b, "error_message", MAX_ERROR_BYTES);
    }

    // 3) JSON Faro object beacon (`faro_metrics`):
    let measurements: Vec<serde_json::Value> = (0..100)
        .map(|i| {
            json!({
                "type": "web-vitals",
                "values": { format!("lcp_{i}"): 123.0 },
                "context": { "rating": "good" }
            })
        })
        .collect();
    let exceptions: Vec<serde_json::Value> = (0..30)
        .map(|i| {
            let msg = if i == 0 {
                long_utf8_err.clone()
            } else {
                format!("err_{i}")
            };
            let file = if i == 0 {
                format!("/{}.js", "é".repeat(300))
            } else {
                "/app.js".into()
            };
            json!({
                "value": msg,
                "stacktrace": {
                    "frames": [{ "filename": file, "lineno": 1, "colno": 2 }]
                }
            })
        })
        .collect();
    let faro_body = json!({
        "meta": {
            "browser": { "name": "é".repeat(300) },
            "page": { "url": format!("https://example.com{long_utf8_path}") }
        },
        "measurements": measurements,
        "exceptions": exceptions,
    })
    .to_string();
    let faro_line = json!({
        "ts_ms": ts_ms,
        "service_id": sid,
        "rum_body": faro_body,
    })
    .to_string();
    let faro_out = ingest::rum::convert_object(&zstd_lines(&[faro_line]), 60, base_s, 1 << 20, sid)
        .expect("convert faro json beacon");
    assert_eq!(
        faro_out.bad_lines, 0,
        "faro beacon must stay under MAX_LINE_BYTES"
    );
    let faro_vitals: usize = faro_out.windows.values().map(|b| b.num_rows()).sum();
    let faro_errors: usize = faro_out.errors.values().map(|b| b.num_rows()).sum();
    assert_eq!(faro_vitals, MAX_METRICS);
    assert_eq!(faro_errors, MAX_ERRORS);
    for b in faro_out.windows.values() {
        assert_col_max_bytes(b, "pathname", MAX_PATH_BYTES);
        assert_col_max_bytes(b, "browser", MAX_FIELD_BYTES);
    }
    for b in faro_out.errors.values() {
        assert_col_max_bytes(b, "error_message", MAX_ERROR_BYTES);
        assert_col_max_bytes(b, "error_file", MAX_PATH_BYTES);
    }

    // 4) Object-level amplification cap: 12 amplified ~1.8 KiB beacons (~22 KiB decompressed,
    //    well below max_decompressed=1 MiB) would expand to >1.7 MiB of Arrow string buffers
    //    and must be rejected with ConvertError::TooLarge before OOMing the 128 MiB Wasm heap.
    let amp_path = format!("/{}", "p".repeat(511));
    let amp_cid = "c".repeat(512);
    let m_items: Vec<&str> = (0..64).map(|_| "a:1").collect();
    let r_items: Vec<&str> = (0..64).map(|_| "1:/:1").collect();
    let e_items: Vec<&str> = (0..16).map(|_| "e=x").collect();
    let amp_line = json!({
        "ts_ms": ts_ms,
        "service_id": sid,
        "url": format!(
            "https://example.com/_rum?p={amp_path}&cid={amp_cid}&m={}&r={}&{}",
            m_items.join(","),
            r_items.join(","),
            e_items.join("&")
        ),
    })
    .to_string();
    let amp_lines: Vec<String> = (0..12).map(|_| amp_line.clone()).collect();
    let total_decompressed: usize = amp_lines.iter().map(|l| l.len() + 1).sum();
    assert!(
        total_decompressed < (1 << 20) / 10,
        "amplification fixture must be tiny decompressed ({total_decompressed} B)"
    );
    let amp_res = ingest::rum::convert_object(&zstd_lines(&amp_lines), 60, base_s, 1 << 20, sid);
    assert_eq!(
        amp_res.err(),
        Some(ingest::convert::ConvertError::TooLarge),
        "amplified RUM object exceeding output byte budget must return ConvertError::TooLarge"
    );
}

use ciborium::value::Value;
use fastly_log_analytics::{
    admin::Record,
    auth::{
        self,
        webauthn::{self, BrowserCredential, BrowserResponse},
        Error as AuthError, Role,
    },
    http::{self, Request, INTERNAL_BODY_LIMIT, PUBLIC_BODY_LIMIT},
    ingest,
    setup::SetupStores,
    shell::Runtime,
    views::{self, filters_reference_pii, SavedView, ViewStore, PII_FILTER_KEYS},
};
use p256::ecdsa::{signature::Signer, Signature, SigningKey};
use serde_json::json;
use sha2::{Digest, Sha256};

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
        ],
    };

    let analyst = views::list_for_role(&store, "svc1", Role::AnalystNoPii, 50).expect("list");
    let analyst_ids: Vec<&str> = analyst.iter().map(|v| v.id.as_str()).collect();
    assert_eq!(
        analyst_ids,
        vec!["safe-none", "safe-path", "safe-query", "safe-json-filters"]
    );

    let admin = views::list_for_role(&store, "svc1", Role::Admin, 50).expect("list");
    assert_eq!(admin.len(), 13);
}

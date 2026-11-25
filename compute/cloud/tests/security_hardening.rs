use ciborium::value::Value;
use fastly_log_analytics::{
    auth::{
        self,
        webauthn::{self, BrowserCredential, BrowserResponse},
        Error as AuthError,
    },
    http::{self, Request, PUBLIC_BODY_LIMIT},
    ingest,
    views::{filters_reference_pii, PII_FILTER_KEYS},
};
use p256::ecdsa::{signature::Signer, Signature, SigningKey};
use serde_json::json;
use sha2::{Digest, Sha256};

#[test]
fn views_pii_canonicalization_parity_blocks_obfuscated_identifiers() {
    assert_eq!(PII_FILTER_KEYS.len(), 18);

    let bypass_keys = [
        // Percent-encoding (single & double)
        "client%5Fip",
        "client%255Fip",
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

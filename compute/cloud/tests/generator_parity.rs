use fastly_log_analytics::{catalog, logging};
use serde_json::{json, Value};

fn fixtures() -> Value {
    serde_json::from_str(include_str!("generator_fixtures.json")).unwrap()
}

#[test]
fn all_cmcd_modes_and_versions_match_independently_executed_pinned_generators() {
    let data = fixtures();
    let cases = data["cmcd_cases"].as_array().unwrap();
    assert_eq!(cases.len(), 4);
    for case in cases {
        let cmcd: logging::cmcd::Config = serde_json::from_value(json!({
            "enabled":true, "mode":case["mode"],"version":case["version"]
        }))
        .unwrap();
        assert_eq!(
            logging::cmcd::generate_cmcd_vcl(&cmcd).unwrap(),
            case["vcl"]
        );
        assert_eq!(
            logging::generate_log_format(&case["config"]).unwrap(),
            case["format"]
        );
        assert_eq!(
            logging::generate_capture_vcl_with_cmcd(&case["config"], &cmcd).unwrap(),
            case["capture"]
        );
        let state = json!({"logging_enabled":true,"log_fields":case["config"],"cmcd":cmcd});
        assert_eq!(logging::desired_snippets(&state).unwrap(), case["snippets"]);
        let endpoints = logging::desired_logging_endpoints(&state).unwrap();
        assert_eq!(endpoints, case["endpoints"]);
        assert_endpoint_body_bytes(
            &endpoints,
            &case["endpoint_bodies"],
            &case["endpoint_bodies_canonical"],
        );
        let mut managed = case["config"].clone();
        managed["custom_fields"] = json!([]);
        logging::cmcd::reconcile_fields(&mut managed, &cmcd, None, false).unwrap();
        for field in managed["custom_fields"].as_array().unwrap() {
            let value = &case["sample_json"][field["name"].as_str().unwrap()];
            assert_eq!(
                field["parquet_type"],
                if value.is_string() { "Utf8" } else { "Int64" }
            );
            assert!(value.is_string() || value.is_i64());
        }
        assert_eq!(
            logging::generate_log_format(&managed).unwrap(),
            case["format"]
        );
    }
}

#[test]
fn pure_generators_match_full_inputs_from_pinned_python() {
    for case in fixtures()["cases"].as_array().unwrap() {
        let config = &case["config"];
        let expected = case["format"].as_str().unwrap();
        if expected.len() <= 12_288 {
            assert_eq!(
                logging::generate_log_format(config).unwrap(),
                expected,
                "{}",
                case["name"]
            );
            assert_eq!(logging::format_hash(config).unwrap(), case["hash"]);
        } else {
            assert!(logging::generate_log_format(config)
                .unwrap_err()
                .contains("12288"));
        }
        assert_eq!(
            logging::generate_capture_vcl(config).unwrap(),
            case["capture"],
            "{}",
            case["name"]
        );
        assert_eq!(
            serde_json::to_value(catalog::resolve_enabled_fields(config)).unwrap(),
            case["enabled"]
        );
        assert_eq!(
            catalog::estimate_log_line_bytes(config),
            case["estimate_bytes"].as_i64().unwrap()
        );
        assert_eq!(
            logging::load_log_format(config).unwrap(),
            case["loaded_format"],
            "{}",
            case["name"]
        );
        assert_eq!(logging::desired_conditions(&json!({
            "sample_rate":37,"edge_only":true,"custom_condition":"  req.url.path !~ \"^/health\"  "
        })).unwrap(), case["conditions"]);
    }
}

#[test]
fn routed_adapter_resources_match_pinned_python_for_every_case() {
    for case in fixtures()["cases"].as_array().unwrap() {
        let config = &case["config"];
        let state = json!({"log_fields": {
            "groups": config.get("groups").cloned().unwrap_or(json!([])),
            "custom_fields": config.get("custom_fields").cloned().unwrap_or(json!([])),
            "field_overrides": config.get("field_overrides").cloned().unwrap_or(json!({})),
        }});
        assert_eq!(
            logging::desired_snippets(&state).unwrap(),
            case["snippets"],
            "{}",
            case["name"]
        );
        let endpoints = logging::desired_logging_endpoints(&state).unwrap();
        assert_eq!(endpoints, case["endpoints"], "{}", case["name"]);
        assert_endpoint_body_bytes(
            &endpoints,
            &case["endpoint_bodies"],
            &case["endpoint_bodies_canonical"],
        );
    }
}

fn assert_endpoint_body_bytes(endpoints: &Value, bodies: &Value, canonical: &Value) {
    for (index, endpoint) in endpoints.as_array().unwrap().iter().enumerate() {
        let actual = logging::logging_endpoint_body(
            endpoint,
            &json!({
                "bucket_name":"fixture-bucket","domain":"us-east-1.object.fastlystorage.app",
                "access_key":"fixture-access","secret_key":"fixture-secret"
            }),
        );
        assert_eq!(actual, bodies[index]);
        assert_eq!(
            serde_json::to_string(&actual).unwrap().as_bytes(),
            canonical[index].as_str().unwrap().as_bytes()
        );
    }
}

#[test]
fn full_input_endpoint_specs_and_body_bytes_match_pure_python_boundaries() {
    for case in fixtures()["cases"].as_array().unwrap() {
        let endpoints =
            logging::desired_logging_endpoints(&json!({"log_fields":case["config"]})).unwrap();
        assert_eq!(endpoints, case["direct_endpoints"], "{}", case["name"]);
        assert_endpoint_body_bytes(
            &endpoints,
            &case["direct_endpoint_bodies"],
            &case["direct_endpoint_bodies_canonical"],
        );
    }
}

#[test]
fn endpoint_names_periods_prefix_boundary_and_conditions_match_pinned_python() {
    let data = fixtures();
    let cases = data["endpoint_settings_cases"].as_array().unwrap();
    assert_eq!(cases.len(), 2);
    for case in cases {
        let endpoints = logging::desired_logging_endpoints(&case["state"]).unwrap();
        assert_eq!(endpoints, case["endpoints"], "{}", case["name"]);
        assert_endpoint_body_bytes(
            &endpoints,
            &case["endpoint_bodies"],
            &case["endpoint_bodies_canonical"],
        );
        assert_eq!(
            logging::desired_conditions(&case["state"]).unwrap(),
            case["conditions"],
            "{}",
            case["name"]
        );
    }
}

#[test]
fn custom_validation_matches_pinned_python() {
    for case in fixtures()["validations"].as_array().unwrap() {
        assert_eq!(
            json!(catalog::validate_custom_field(&case["field"], &[])),
            case["errors"],
            "{}",
            case["field"]
        );
    }
}

#[test]
fn logging_disabled_produces_no_resources() {
    assert_eq!(
        logging::desired_logging_endpoints(&json!({"logging_enabled": false})).unwrap(),
        json!([])
    );
    for snippet in logging::desired_snippets(&json!({"logging_enabled": false}))
        .unwrap()
        .as_array()
        .unwrap()
    {
        assert_eq!(snippet["body"], "");
    }
}

#[test]
fn rejects_format_over_reconcile_cap_without_truncating_it() {
    let fields: Vec<Value> = (0..100)
        .map(|i| {
            json!({
                "name": format!("field_{i}"), "vcl_log_expression": "req.http.X-Test",
                "bytes_estimate": 200, "value_type": "numeric",
            })
        })
        .collect();
    assert!(
        logging::generate_log_format(&json!({"custom_fields": fields}))
            .unwrap_err()
            .contains("12288")
    );
}

#[test]
fn endpoint_body_retains_null_placement_and_omits_empty_optional_values() {
    let endpoints =
        logging::desired_logging_endpoints(&json!({"log_period": 60, "log_fields": {}})).unwrap();
    let body = logging::logging_endpoint_body(
        &endpoints[0],
        &json!({
            "bucket_name": "example-bucket", "domain": "us-east-1.object.fastlystorage.app",
            "access_key": "fixture-access", "secret_key": "fixture-secret"
        }),
    );
    assert_eq!(body["compression_codec"], "zstd");
    assert!(body.get("gzip_level").is_none());
    assert_eq!(body["format_version"], 2);
    assert_eq!(body["placement"], Value::Null);
    assert_eq!(
        body["path"],
        "raw/request/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"
    );
    assert_eq!(body["secret_key"], "fixture-secret");
    assert!(logging::logging_endpoint_body(&endpoints[0], &json!({}))
        .get("secret_key")
        .is_none());
}

#[test]
fn canonical_custom_field_types_use_v3_generation() {
    let canonical = json!({
        "name":"a_edge","vcl":"req.http.X-Count","parquet_type":"Int64",
        "bytes_estimate":30
    });
    assert_eq!(
        catalog::validate_custom_field(&canonical, &[]),
        Vec::<String>::new()
    );
    let original = json!({
        "name":"a_edge","vcl_log_expression":"req.http.X-Count","duckdb_type":"BIGINT",
        "value_type":"numeric","bytes_estimate":30
    });
    assert_eq!(
        logging::generate_log_format(&json!({"custom_fields":[canonical]})).unwrap(),
        logging::generate_log_format(&json!({"custom_fields":[original]})).unwrap()
    );
    assert!(!catalog::validate_custom_field(
        &json!({
            "name":"a_edge","vcl":"req.http.X-Count","parquet_type":"unknown"
        }),
        &[]
    )
    .is_empty());
}

#[test]
fn format_cap_counts_bytes_and_accepts_exact_boundary() {
    let mut config = json!({"custom_fields":[{
        "name":"text","vcl_log_expression":"x","collection_stage":"deliver"
    }]});
    let base_length = logging::generate_log_format(&config).unwrap().len();
    config["custom_fields"][0]["vcl_log_expression"] = json!("x".repeat(12_288 - base_length + 1));
    assert_eq!(logging::generate_log_format(&config).unwrap().len(), 12_288);
    config["custom_fields"][0]["vcl_log_expression"] = json!("x".repeat(12_288 - base_length + 2));
    assert!(logging::generate_log_format(&config).is_err());
    config["custom_fields"][0]["vcl_log_expression"] = json!("é".repeat(6_144));
    assert!(logging::generate_log_format(&config).is_err());
}

#[test]
fn field_limits_reach_endpoint_format_and_capture_without_v3_adapter_loss() {
    let config = json!({"groups":["A"],"field_limits":{"url":12,"ua":6,"referer":0}});
    let endpoints = logging::desired_logging_endpoints(&json!({"log_fields":config})).unwrap();
    let format = endpoints[0]["format"].as_str().unwrap();
    assert!(format.contains("substr(req.url, 0, 2)"));
    assert!(format.contains("req.http.User-Agent), 0, 1)"));
    assert!(logging::generate_capture_vcl(&config).unwrap()["recv"]
        .as_str()
        .unwrap()
        .contains("substr(req.http.User-Agent, 0, 6)"));
    let api = catalog::get_catalog_for_api(Some(&json!({"url":12})));
    assert_eq!(
        api.as_array()
            .unwrap()
            .iter()
            .find(|f| f["id"] == "url")
            .unwrap()["limit"],
        12
    );
}

#[test]
fn client_ip_reset_remains_early_and_future_features_are_rejected() {
    let snippets = logging::logging_service_snippets(&json!({"log_fields":{}})).unwrap();
    let reset = snippets
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == "Fastly Log Analysis Reset Client IP")
        .unwrap();
    assert_eq!(reset["priority"], -100);
    assert_eq!(reset["subroutine"], "vcl_recv");
    let feature = "scoring";
    let mut state = json!({});
    state[feature] = json!({"enabled":true});
    assert_eq!(
        logging::desired_snippets(&state).unwrap_err(),
        format!("{feature} is outside Goal 2")
    );
    assert!(!catalog::resolve_enabled_fields(
        &json!({"groups":["H"],"field_overrides":{"cookie_session":true}})
    )
    .contains("cookie_session"));
}

#[test]
fn invalid_limits_periods_and_sample_rates_fail_instead_of_generating_resources() {
    assert!(
        logging::generate_log_format(&json!({"groups":["A"],"field_limits":{"url":"oops"}}))
            .is_err()
    );
    assert!(logging::desired_logging_endpoints(&json!({"log_period":0})).is_err());
    assert!(logging::desired_conditions(&json!({"sample_rate":101})).is_err());
    assert!(logging::generate_log_format(&json!({"custom_fields":[{
        "name":"invalid_limit","vcl_log_expression":"req.http.X-Test","byte_limit":"oops"
    }]}))
    .is_err());
    assert_eq!(
        logging::desired_conditions(&json!({"logging_enabled":false})).unwrap(),
        json!([])
    );
    assert_eq!(
        logging::logging_service_snippets(&json!({"logging_enabled":false})).unwrap(),
        json!([])
    );
}

#[test]
fn rum_generator_parity_covers_shared_and_rum_only_vcl_endpoints_and_tracker_js() {
    let shared_state = json!({
        "logging_enabled": true,
        "logging_endpoint_name": "Fastly Object Storage Logs",
        "log_period": 60,
        "sample_rate": 100,
        "edge_only": false,
        "custom_condition": "",
        "log_fields": {"groups":["A","B","C","D","E","F","G","H","I","J","K","L","M"],"custom_fields":[],"field_overrides":{},"field_limits":{}},
        "rum": {
            "enabled": true,
            "log_period": 15,
            "custom_condition": "req.http.Host == \"example.com\"",
            "faro_version": "2.10.0",
            "endpoint_name": "Fastly RUM Logs",
            "capture_vitals": true,
            "capture_performance": true,
            "capture_errors": false,
            "capture_events": true
        }
    });
    let snippets = logging::desired_snippets(&shared_state).unwrap();
    let snippet_names: Vec<_> = snippets
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        snippet_names,
        vec![
            "Fastly Log Analytics - vcl_recv",
            "Fastly Log Analytics - vcl_miss",
            "Fastly Log Analytics - vcl_pass",
            "Fastly Log Analytics - vcl_fetch",
            "Fastly Log Analytics - vcl_deliver",
            "Fastly Log Analytics - vcl_error"
        ]
    );
    let recv = snippets[0]["body"].as_str().unwrap();
    assert!(recv.contains("Section 5: RUM field extraction (vcl_recv)"));
    assert!(recv.contains("Section 5: RUM (vcl_recv)"));
    assert!(recv.contains("Section 6: Log Field Capture (vcl_recv)"));
    assert!(recv.contains("/js/faro-sdk.js"));
    assert!(recv.contains("fastly.try_select_shield(ssl_shield_iad_va_us, F_fos_origin)"));
    let miss = snippets[1]["body"].as_str().unwrap();
    assert!(miss.contains("/rum/faro-web-sdk-v2.10.0.iife.js"));

    let conditions = logging::desired_conditions(&shared_state).unwrap();
    assert_eq!(conditions.as_array().unwrap().len(), 3);
    assert_eq!(conditions[0]["name"], "log_analytics_condition");
    assert_eq!(
        conditions[0]["statement"],
        "!segmented_caching.is_inner_req && req.url.path != \"/rum-beacon\""
    );
    assert_eq!(conditions[1]["name"], "rum_log_condition");
    assert_eq!(
        conditions[1]["statement"],
        "req.url.path == \"/rum-beacon\" && req.method == \"POST\" && fastly.ff.visits_this_service == 0 && req.restarts == 0 && (req.http.Host == \"example.com\")"
    );
    assert_eq!(conditions[2]["name"], "fastly_log_analytics_false");

    let endpoints = logging::desired_logging_endpoints(&shared_state).unwrap();
    assert_eq!(endpoints.as_array().unwrap().len(), 2);
    assert_eq!(endpoints[0]["name"], "Fastly Object Storage Logs");
    assert_eq!(endpoints[1]["name"], "Fastly RUM Logs");
    assert_eq!(endpoints[1]["period"], 15);
    assert_eq!(
        endpoints[1]["path"],
        "raw/rum/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"
    );
    assert_eq!(endpoints[1]["response_condition"], "rum_log_condition");
    let rum_format = endpoints[1]["format"].as_str().unwrap();
    let keys: Vec<&str> = rum_format
        .split('"')
        .collect::<Vec<_>>()
        .windows(2)
        .filter(|w| w[1].starts_with(':') && !w[0].contains(['%', '(', ' ']))
        .map(|w| w[0])
        .collect();
    assert_eq!(
        keys,
        [
            "ts_ms",
            "service_id",
            "fastly_req_id",
            "url",
            "city",
            "region",
            "country",
            "pop",
            "tls",
            "ttfb"
        ],
        "only what the RUM parser reads: {rum_format}"
    );
    assert_eq!(
        rum_format.matches("req.url").count(),
        1,
        "the beacon query string is logged once"
    );
    assert!(rum_format.len() < 1_000, "{}", rum_format.len());
    assert!(!recv.contains("rum_raw_query") && !recv.contains("rum_metric_name"));
    assert!(!recv.contains("req.body"));
    assert!(recv.contains(
        "set req.url = querystring.filter(req.url, \"service_id\" + querystring.filtersep() + \"req\");"
    ));

    let rum_only_state = json!({
        "logging_enabled": false,
        "rum": {
            "enabled": true,
            "log_period": 60,
            "custom_condition": "",
            "faro_version": "2.10.0",
            "endpoint_name": "Fastly RUM Logs",
            "capture_vitals": true,
            "capture_performance": false,
            "capture_errors": true,
            "capture_events": false
        }
    });
    let rum_only_snippets = logging::desired_snippets(&rum_only_state).unwrap();
    assert_eq!(rum_only_snippets.as_array().unwrap().len(), 6);
    assert!(!rum_only_snippets[0]["body"]
        .as_str()
        .unwrap()
        .contains("Section 6: Log Field Capture (vcl_recv)"));
    let rum_only_endpoints = logging::desired_logging_endpoints(&rum_only_state).unwrap();
    assert_eq!(rum_only_endpoints.as_array().unwrap().len(), 1);
    assert_eq!(rum_only_endpoints[0]["name"], "Fastly RUM Logs");

    let cfg: logging::rum::Config = serde_json::from_value(rum_only_state["rum"].clone()).unwrap();
    let tracker = logging::rum::generate_rum_tracker_js("fixtureService", &cfg);
    assert!(tracker.contains("var CAPTURE_VITALS = true;"));
    assert!(tracker.contains("var CAPTURE_PERFORMANCE = false;"));
    assert_eq!(
        logging::rum::faro_bundle_bytes("2.10.0").unwrap().len(),
        103553
    );

    let mut compact_state = shared_state.clone();
    compact_state["compression_codec"] = json!("zstd");
    compact_state["wire_format"] = json!("array");
    let compact_endpoints = logging::desired_logging_endpoints(&compact_state).unwrap();
    for ep in compact_endpoints.as_array().unwrap() {
        assert_eq!(ep["compression_codec"], "zstd");
        assert!(ep["format"].as_str().unwrap().starts_with('['));
        let body = logging::logging_endpoint_body(ep, &json!({}));
        assert_eq!(body["compression_codec"], "zstd");
        assert!(body.get("gzip_level").is_none());
    }
}

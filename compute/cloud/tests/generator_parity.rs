use fastly_log_analytics::{catalog, logging};
use serde_json::{json, Value};

fn fixtures() -> Value {
    serde_json::from_str(include_str!("generator_fixtures.json")).unwrap()
}

fn endpoint_from_template(template: &Value, format: &Value) -> Value {
    let mut ep = template.as_object().unwrap().clone();
    ep.insert("format".into(), format.clone());
    json!([Value::Object(ep)])
}

fn expected_endpoint_body(endpoint: &Value) -> Value {
    let mut body = endpoint.as_object().unwrap().clone();
    body.remove("type");
    body.insert("format_version".into(), json!(2));
    body.insert("bucket_name".into(), json!("fixture-bucket"));
    body.insert("domain".into(), json!("us-east-1.object.fastlystorage.app"));
    body.insert("access_key".into(), json!("fixture-access"));
    body.insert("secret_key".into(), json!("fixture-secret"));
    Value::Object(body)
}

#[test]
fn all_cmcd_modes_and_versions_match_independently_executed_pinned_generators() {
    let data = fixtures();
    let template = &data["default_endpoint_template"];
    let config = &data["cmcd_default_config"];
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
            logging::generate_log_format(config).unwrap(),
            case["format"]
        );
        assert_eq!(
            logging::generate_capture_vcl_with_cmcd(config, &cmcd).unwrap(),
            case["capture"]
        );
        // The fixtures pin the original generator's period of 60 s; the default is shorter now.
        let state = json!({"logging_enabled":true,"log_period":60,"log_fields":config,"cmcd":cmcd});
        assert_eq!(logging::desired_snippets(&state).unwrap(), case["snippets"]);
        let endpoints = logging::desired_logging_endpoints(&state).unwrap();
        let expected_endpoints = endpoint_from_template(template, &case["format"]);
        assert_eq!(endpoints, expected_endpoints);
        assert_endpoint_body_bytes(&endpoints);
        let mut managed = config.clone();
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
    let data = fixtures();
    let default_conditions = &data["default_conditions"];
    for case in data["cases"].as_array().unwrap() {
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
        })).unwrap(), *default_conditions);
    }
}

#[test]
fn routed_adapter_resources_match_pinned_python_for_every_case() {
    let data = fixtures();
    let template = &data["default_endpoint_template"];
    for case in data["cases"].as_array().unwrap() {
        let config = &case["config"];
        let state = json!({"log_period": 60, "log_fields": {
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
        let expected_format = case.get("routed_format").unwrap_or(&case["loaded_format"]);
        assert_eq!(
            endpoints,
            endpoint_from_template(template, expected_format),
            "{}",
            case["name"]
        );
        assert_endpoint_body_bytes(&endpoints);
    }
}

fn assert_endpoint_body_bytes(endpoints: &Value) {
    for endpoint in endpoints.as_array().unwrap() {
        let actual = logging::logging_endpoint_body(
            endpoint,
            &json!({
                "bucket_name":"fixture-bucket","domain":"us-east-1.object.fastlystorage.app",
                "access_key":"fixture-access","secret_key":"fixture-secret"
            }),
        );
        let expected = expected_endpoint_body(endpoint);
        assert_eq!(actual, expected);
        assert_eq!(
            serde_json::to_string(&actual).unwrap().as_bytes(),
            serde_json::to_string(&expected).unwrap().as_bytes()
        );
    }
}

#[test]
fn full_input_endpoint_specs_and_body_bytes_match_pure_python_boundaries() {
    let data = fixtures();
    let template = &data["default_endpoint_template"];
    for case in data["cases"].as_array().unwrap() {
        let endpoints = logging::desired_logging_endpoints(
            &json!({"log_period":60,"log_fields":case["config"]}),
        )
        .unwrap();
        assert_eq!(
            endpoints,
            endpoint_from_template(template, &case["loaded_format"]),
            "{}",
            case["name"]
        );
        assert_endpoint_body_bytes(&endpoints);
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
        assert_endpoint_body_bytes(&endpoints);
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
fn fos_endpoint_body_matches_the_s3_body_without_credentials_or_domain() {
    let endpoints = logging::desired_logging_endpoints(
        &json!({"log_period": 60, "log_fields": {}, "rum": {"enabled": true}}),
    )
    .unwrap();
    assert_eq!(endpoints.as_array().unwrap().len(), 2);
    for endpoint in endpoints.as_array().unwrap() {
        let s3 = logging::logging_endpoint_body(
            endpoint,
            &json!({
                "bucket_name": "example-bucket", "domain": "us-east-1.object.fastlystorage.app",
                "access_key": "fixture-access", "secret_key": "fixture-secret"
            }),
        );
        let fos = logging::fos_logging_endpoint_body(endpoint, "example-bucket", "us-east-1");
        let mut expected = s3.as_object().unwrap().clone();
        for key in ["domain", "access_key", "secret_key", "response_object_name"] {
            expected.remove(key);
        }
        expected.insert("region".into(), "us-east-1".into());
        assert_eq!(fos, Value::Object(expected), "{}", endpoint["name"]);
    }
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
            "log_period": 120,
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
    assert_eq!(endpoints[1]["period"], 120);
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

#[test]
fn catalog_json_exports_enum_decode_maps_and_fos_regions() {
    use fastly_log_analytics::ingest::convert;

    let data = catalog::data();
    assert!(data.get("golden_vectors").is_none());
    let encodings = data["enum_encodings"].as_object().unwrap();
    for (table, field_id) in [
        ("CONN_SPEED_ENCODE", "c_speed"),
        ("PROXY_TYPE_ENCODE", "p_type"),
        ("PROXY_DESC_ENCODE", "p_desc"),
    ] {
        let map = encodings[table].as_object().unwrap();
        assert!(!map.is_empty());
        for (full_val, code_val) in map {
            let code = code_val.as_str().unwrap();
            assert_eq!(convert::decode_enum_value(field_id, code), full_val);
        }
    }
    let regions = data["fos_regions"].as_object().unwrap();
    assert!(!regions.is_empty());
    for (region, shield) in regions {
        assert_eq!(
            logging::rum::shield_for_region(region),
            shield.as_str().unwrap()
        );
    }
    assert_eq!(
        logging::rum::shield_for_region("unknown-region"),
        "iad-va-us"
    );
}

#[test]
fn templates_json_exports_rum_and_sigv4_vcl_snippets() {
    let tpl: Value = serde_json::from_str(include_str!("../vcl/capture/templates.json")).unwrap();
    assert_eq!(
        tpl["M"]["deliver"], tpl["group_capture_deliver"]["M"],
        "Group M deliver templates must be identical without fake comments"
    );
    let m_deliver = tpl["M"]["deliver"].as_str().unwrap();
    assert!(m_deliver.contains("!req.http.x-fos-edge-data:io"));
    assert!(m_deliver.contains("dc_fmt=([a-z0-9]+)"));
    assert!(m_deliver.contains("Fastly-Io-Info"));
    assert_eq!(
        logging::rum::beacon_field_extraction_vcl(),
        tpl["rum_beacon_extraction"].as_str().unwrap()
    );
    assert_eq!(
        logging::rum::beacon_response_vcl(),
        tpl["rum_beacon_response"].as_str().unwrap()
    );
    assert_eq!(
        logging::rum::error_beacon_vcl(),
        tpl["rum_error"].as_str().unwrap()
    );
    assert_eq!(
        logging::rum::asset_fetch_vcl("iad-va-us", Some("2.13.0")).unwrap(),
        tpl["rum_asset_fetch"].as_str().unwrap()
    );
    assert_eq!(
        logging::rum::sigv4_sign_vcl(Some("2.13.0")).unwrap(),
        tpl["rum_sigv4_sign"].as_str().unwrap()
    );
}

#[test]
fn rum_ingest_matches_python_golden_vectors() {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Float64Type, Int32Type};
    use fastly_log_analytics::ingest::rum;

    let gv = &fixtures()["golden_vectors"]["rum_ingest"];
    let service_id = gv["service_id"].as_str().unwrap();
    let window_s = gv["window_s"].as_i64().unwrap();
    let object_time_s = gv["object_time_s"].as_i64().unwrap();
    let line_bytes = format!("{}\n", serde_json::to_string(&gv["faro_line"]).unwrap());
    let compressed = zstd::stream::encode_all(line_bytes.as_bytes(), 1).unwrap();
    let converted = rum::convert_object(
        &compressed,
        window_s,
        object_time_s,
        1024 * 1024,
        service_id,
    )
    .unwrap();

    let expected = gv["expected_extracted"].as_array().unwrap();
    let expected_vitals: Vec<&Value> = expected
        .iter()
        .filter(|m| m["metric_name"] != "exception")
        .collect();
    let expected_errors: Vec<&Value> = expected
        .iter()
        .filter(|m| m["metric_name"] == "exception")
        .collect();

    let vitals_batch = converted.windows.values().next().unwrap();
    assert_eq!(vitals_batch.num_rows(), expected_vitals.len());
    let names = vitals_batch
        .column_by_name("metric_name")
        .unwrap()
        .as_string::<i32>();
    let values = vitals_batch
        .column_by_name("metric_value")
        .unwrap()
        .as_primitive::<Float64Type>();
    let ratings = vitals_batch
        .column_by_name("metric_rating")
        .unwrap()
        .as_string::<i32>();
    let pathnames = vitals_batch
        .column_by_name("pathname")
        .unwrap()
        .as_string::<i32>();
    let browsers = vitals_batch
        .column_by_name("browser")
        .unwrap()
        .as_string::<i32>();
    let devices = vitals_batch
        .column_by_name("device")
        .unwrap()
        .as_string::<i32>();
    for (idx, exp) in expected_vitals.iter().enumerate() {
        assert_eq!(names.value(idx), exp["metric_name"].as_str().unwrap());
        assert_eq!(values.value(idx), exp["metric_value"].as_f64().unwrap());
        assert_eq!(ratings.value(idx), exp["metric_rating"].as_str().unwrap());
        assert_eq!(pathnames.value(idx), exp["pathname"].as_str().unwrap());
        assert_eq!(browsers.value(idx), exp["browser"].as_str().unwrap());
        assert_eq!(devices.value(idx), exp["device"].as_str().unwrap());
    }

    let errors_batch = converted.errors.values().next().unwrap();
    assert_eq!(errors_batch.num_rows(), expected_errors.len());
    let err_msgs = errors_batch
        .column_by_name("error_message")
        .unwrap()
        .as_string::<i32>();
    let err_files = errors_batch
        .column_by_name("error_file")
        .unwrap()
        .as_string::<i32>();
    let err_lines = errors_batch
        .column_by_name("error_line")
        .unwrap()
        .as_primitive::<Int32Type>();
    let err_cols = errors_batch
        .column_by_name("error_col")
        .unwrap()
        .as_primitive::<Int32Type>();
    for (idx, exp) in expected_errors.iter().enumerate() {
        assert_eq!(err_msgs.value(idx), exp["error_message"].as_str().unwrap());
        assert_eq!(err_files.value(idx), exp["error_file"].as_str().unwrap());
        assert_eq!(
            i64::from(err_lines.value(idx)),
            exp["error_line"].as_i64().unwrap()
        );
        assert_eq!(
            i64::from(err_cols.value(idx)),
            exp["error_col"].as_i64().unwrap()
        );
    }
}

#[test]
fn rum_validators_match_python_golden_vectors() {
    let gv = &fixtures()["golden_vectors"]["rum_validators"];
    for entry in gv["faro_versions"].as_array().unwrap() {
        let ver = entry["version"].as_str().unwrap();
        let valid = entry["valid"].as_bool().unwrap();
        assert_eq!(
            logging::rum::validate_faro_version(ver).is_ok(),
            valid,
            "faro_version={ver}"
        );
    }
    for entry in gv["custom_conditions"].as_array().unwrap() {
        let cond = entry["condition"].as_str().unwrap();
        let valid = entry["valid"].as_bool().unwrap();
        let res = logging::rum::build_rum_log_condition(cond);
        assert_eq!(res.is_ok(), valid, "condition={cond}");
        if valid {
            assert_eq!(res.unwrap(), entry["statement"].as_str().unwrap());
        }
    }
}

/// fla's hash and its exact small counts; the sketch bytes diverge on purpose (Cloud summary v3
/// keeps sparse HLL++ at p=12, fla dense p=8 registers).
#[test]
fn hll_hash_and_count_match_python_golden_vectors() {
    use fastly_log_analytics::ingest::summary::{hll_hash, Hll};

    let cases = fixtures()["golden_vectors"]["hll"]
        .as_array()
        .unwrap()
        .clone();
    assert!(!cases.is_empty());
    for case in &cases {
        let mut hll = Hll::default();
        for item_entry in case["item_hashes"].as_array().unwrap() {
            let item = item_entry["item"].as_str().unwrap();
            let expected_hash =
                u64::from_str_radix(item_entry["hash_hex"].as_str().unwrap(), 16).unwrap();
            let actual_hash = hll_hash(item);
            assert_eq!(actual_hash, expected_hash, "item={item}");
            hll.add_hash(actual_hash);
        }
        assert_eq!(
            hll.count(),
            case["count"].as_u64().unwrap(),
            "case={}",
            case["name"]
        );
    }
}

#[test]
fn fingerprint_matches_python_golden_vectors() {
    use compute_common::crypto::{fnv1a32, fnv1a64};
    use fastly_log_analytics::ingest::bundle::fingerprint;

    let gv = &fixtures()["golden_vectors"]["fingerprint"];
    for entry in gv["fnv1a"].as_array().unwrap() {
        let input = entry["input"].as_str().unwrap();
        let expected32 = u32::from_str_radix(entry["fnv1a32_hex"].as_str().unwrap(), 16).unwrap();
        let expected64 = u64::from_str_radix(entry["fnv1a64_hex"].as_str().unwrap(), 16).unwrap();
        assert_eq!(fnv1a32(input.bytes()), expected32, "input={input}");
        assert_eq!(fnv1a64(input.bytes()), expected64, "input={input}");
    }
    let bundle_names: Vec<&str> = gv["bundle_names"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    let expected_fp =
        u64::from_str_radix(gv["bundle_fingerprint_hex"].as_str().unwrap(), 16).unwrap();
    assert_eq!(fingerprint(bundle_names), expected_fp);
}

#[test]
fn cloud_endpoints_never_use_a_period_below_one_minute() {
    let endpoints = logging::desired_logging_endpoints(&json!({})).unwrap();
    assert_eq!(endpoints[0]["period"], 60);
    let legacy = json!({
        "log_period": 10,
        "rum": {"enabled": true, "log_period": 10, "endpoint_name": "Fastly RUM Logs"}
    });
    let endpoints = logging::desired_logging_endpoints(&legacy).unwrap();
    assert_eq!(endpoints.as_array().unwrap().len(), 2);
    assert!(endpoints
        .as_array()
        .unwrap()
        .iter()
        .all(|e| e["period"] == 60));
}

#[test]
fn sec16_rum_fos_request_scrubbing_and_sigv4_backend_path_guard() {
    let templates: Value =
        serde_json::from_str(include_str!("../vcl/capture/templates.json")).unwrap();
    assert!(templates["scrub_headers"]
        .as_array()
        .unwrap()
        .contains(&json!("X-FOS-Request")));
    assert!(templates["rum_recv"]
        .as_str()
        .unwrap()
        .contains("unset req.http.X-FOS-Request;"));
    assert!(templates["rum_deliver"]
        .as_str()
        .unwrap()
        .contains("; Path=/; SameSite=Lax; Secure\""));

    let with_faro = logging::rum::sigv4_sign_vcl(Some("2.13.0")).unwrap();
    assert!(with_faro.contains(
        r#"if (req.http.X-FOS-Request == "1" && req.backend == F_fos_origin && !req.backend.is_shield && (req.url.path == "/js/rum.js" || req.url.path == "/js/faro-sdk.js")) {"#
    ));
    let no_faro = logging::rum::sigv4_sign_vcl(None).unwrap();
    assert!(no_faro.contains(
        r#"if (req.http.X-FOS-Request == "1" && req.backend == F_fos_origin && !req.backend.is_shield && req.url.path == "/js/rum.js") {"#
    ));
    assert!(!no_faro.contains("faro-sdk.js"));

    for (logging_enabled, rum_enabled) in [(true, false), (true, true), (false, true)] {
        let state = json!({
            "logging_enabled": logging_enabled,
            "log_fields": {"groups": ["A"], "custom_fields": [], "field_overrides": {}, "field_limits": {}},
            "rum": {"enabled": rum_enabled, "faro_version": "2.13.0"}
        });
        let snippets = logging::desired_snippets(&state).unwrap();
        let recv = snippets
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["subroutine"] == "vcl_recv")
            .expect("vcl_recv snippet")["body"]
            .as_str()
            .unwrap();
        let scrub_pos = recv
            .find("unset req.http.X-FOS-Request;")
            .expect("recv must scrub X-FOS-Request");
        assert!(
            recv.contains("unset req.http.x-fos-edge-data;"),
            "recv must scrub x-fos-edge-data when logging={logging_enabled}, rum={rum_enabled}"
        );
        for sub in ["vcl_miss", "vcl_pass"] {
            let body = snippets
                .as_array()
                .unwrap()
                .iter()
                .find(|s| s["subroutine"] == sub)
                .unwrap()["body"]
                .as_str()
                .unwrap();
            assert!(
                body.contains("unset bereq.http.x-fos-edge-data;"),
                "{sub} must strip bereq.http.x-fos-edge-data when logging={logging_enabled}, rum={rum_enabled}"
            );
        }
        assert!(recv.contains("if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {"));
        if rum_enabled {
            let set_pos = recv
                .find("set req.http.X-FOS-Request = \"1\";")
                .expect("rum asset fetch sets X-FOS-Request");
            assert!(
                scrub_pos < set_pos,
                "X-FOS-Request scrub must run strictly before asset_fetch_vcl"
            );

            // Lint wrapped snippets in falco to prove valid VCL with F_fos_origin
            let mut vcl = String::from(
                "backend F_customer_origin { .host = \"origin.example.com\"; }\n\
                 backend F_fos_origin { .host = \"us-east-1.object.fastlystorage.app\"; }\n\
                 backend ssl_shield_iad_va_us { .host = \"shield.example.com\"; }\n\
                 table fos_credentials {\n  \"access_key\": \"test\",\n  \"secret_key\": \"test\",\n  \"bucket\": \"b\",\n  \"region\": \"us-east-1\",\n}\n",
            );
            for snip in snippets.as_array().unwrap() {
                let sub_name = snip["subroutine"].as_str().unwrap();
                let short = sub_name.trim_start_matches("vcl_");
                let body = snip["body"].as_str().unwrap();
                vcl.push_str(&format!(
                    "sub {sub_name} {{\n  #FASTLY {short}\n{body}\n}}\n"
                ));
            }
            let tmp = std::env::temp_dir().join(format!(
                "flp-sec16-{logging_enabled}-{rum_enabled}-{}.vcl",
                std::process::id()
            ));
            std::fs::write(&tmp, &vcl).unwrap();
            let out = std::process::Command::new("falco")
                .arg("lint")
                .arg(&tmp)
                .output();
            let _ = std::fs::remove_file(&tmp);
            let out = out.expect("falco binary must run");
            assert!(
                out.status.success(),
                "falco lint failed for logging={logging_enabled}, rum={rum_enabled}:\n{}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
    }
}

#[test]
fn sec20ab_vcl_condition_and_log_expression_validators_block_breakouts_and_exfiltration() {
    let valid_conditions = [
        "",
        "req.method == \"GET\"",
        "req.url.path !~ \"^/health\"",
        "(req.restarts == 0 && req.http.Set-Cookie == \"\")",
        "req.http.x-error-code != \"500\"",
    ];
    for cond in valid_conditions {
        assert!(
            logging::validate_vcl_condition(cond).is_ok(),
            "expected valid condition: {cond}"
        );
    }

    let malicious_conditions = [
        ") || true || (",
        "(req.method == \"GET\"",
        "req.http.X == \"unterminated",
        r#"req.http.X == "\") || true || (req.http.Y == "\\\""#,
        "req.method == \"GET\"; unset req.url",
        "req.method == \"GET\"\n&& true",
        "req.method == \"GET\"\r&& true",
        "req.method == \"GET\" { }",
        "req.method == 'GET'",
        "req.method == \"GET\" // comment",
        "req.method == \"GET\" /* comment */",
        "req.method == \"GET\" */",
        "req.method == \"GET\" # comment",
        "table.lookup(fos_credentials, \"secret_key\") != \"\"",
        "TABLE . LOOKUP(fos_credentials, \"secret_key\") != \"\"",
        "table.contains(flp_front_auth, \"front_key\")",
        "header.get(req, \"Auth\" + \"orization\") != \"\"",
        "HEADER . GET(req, \"x-fastly-key\") != \"\"",
        "header.filter(req, \"Cookie\")",
        "header.filter_except(req, \"Host\")",
        "req.http.X == \"%22) || true || (\"",
        "req.http.x-fastly-key != \"\"",
        "req.http.Fastly-Key != \"\"",
        "req . http . Authorization != \"\"",
        "bereq.http.Proxy-Authorization != \"\"",
        "req.http.x-edge-scorer-auth != \"\"",
        "req.http.x-flp-front-key != \"\"",
        "req.http.x-flp-worker-key != \"\"",
        "req.http.x-flp-internal != \"\"",
        "req.http.Cookie != \"\"",
        "req.http.Cookie:flp_session != \"\"",
        "resp.http.Set-Cookie != \"\"",
        "beresp.http.Set-Cookie != \"\"",
        "fos_credentials != \"\"",
        "session_scoring_secrets != \"\"",
        "cdn_auth != \"\"",
        "flp_fos_credentials != \"\"",
        "flp_front_auth != \"\"",
        "flp_fos_buckets != \"\"",
        "synthetic \"x\"",
        "call my_sub",
        "return(pass)",
        "error 600",
        "restart",
        "set req.url = \"/\"",
        "unset req.http.Host",
        "add req.http.X = \"1\"",
        "esi",
        "include \"foo\"",
        "import std",
        "declare local var.x STRING",
    ];
    for bad in malicious_conditions {
        assert!(
            logging::validate_vcl_condition(bad).is_err(),
            "expected validate_vcl_condition to reject: {bad}"
        );
        assert!(
            logging::rum::validate_custom_condition(bad).is_err(),
            "expected rum::validate_custom_condition to reject: {bad}"
        );
        assert!(
            logging::desired_conditions(&json!({"custom_condition": bad})).is_err(),
            "expected desired_conditions to reject: {bad}"
        );
    }
    for rum_url_bypass in [
        "req.url == \"/x\"",
        "REQ.URL.path ~ \"^/secret\"",
        "Req . Url != \"\"",
    ] {
        assert!(
            logging::rum::validate_custom_condition(rum_url_bypass).is_err(),
            "expected rum::validate_custom_condition to reject case-insensitive req.url: {rum_url_bypass}"
        );
    }
    assert!(logging::rum::validate_custom_condition("req.url.qs != \"\"").is_ok());

    let valid_exprs = [
        "req.http.X-Custom-Header",
        "resp.http.X-Cache-Status",
        "beresp.http.Cache-Control",
        "if(req.http.X-A != \"\", req.http.X-A, \"none\")",
    ];
    for expr in valid_exprs {
        assert!(
            logging::validate_vcl_log_expression(expr).is_empty(),
            "expected valid expression: {expr}"
        );
        let good_cf = json!({
            "name": "good_field",
            "vcl_log_expression": expr,
            "parquet_type": "Utf8",
            "collection_stage": "edge"
        });
        assert!(
            catalog::validate_custom_field(&good_cf, &[]).is_empty(),
            "expected catalog::validate_custom_field to accept: {expr}"
        );
    }

    let malicious_exprs = [
        "x;\nunset req.url;",
        "req.http.X\r",
        "req.http.X; set req.url = \"/\"",
        "req.http.X { }",
        "%{req.http.X}V",
        "req.http.X }V",
        "'single'",
        "req.http.X // comment",
        "req.http.X /* comment */",
        "req.http.X # comment",
        "if(req.http.X != \"\"",
        "req.http.X)",
        r#"if(req.http.X == "\"", "a", "b")"#,
        "table.lookup(fos_credentials, \"secret_key\")",
        "TABLE . LOOKUP(flp_front_auth, \"front_key\")",
        "return(pass)",
        "req.http.X-Edge-Scorer-Auth",
        "req.http.Authorization",
        "req.http.authorization:Bearer",
        "bereq.http.X-Fastly-Key",
        "req.http.X-FLP-Front-Key",
        "req.http.X-FLP-Worker-Key",
        "req.http.X-FLP-Clock-Secret",
        "req.http.x-flp-any-internal",
        "req.http.Fastly-Key",
        "bereq.http.Proxy-Authorization",
        "req.http.Cookie",
        "req.http.Cookie:flp_session",
        "req.http.Set-Cookie",
        "resp.http.Set-Cookie",
        "header.get(req, \"Auth\" + \"orization\")",
        "table.contains(fos_credentials, \"secret_key\")",
        "if(req.http.X == \"%22\", \"a\", \"b\")",
        "req.http.X */",
        "session_scoring_secrets",
        "declare local var.x STRING",
    ];
    for bad in malicious_exprs {
        assert!(
            !logging::validate_vcl_log_expression(bad).is_empty(),
            "expected validate_vcl_log_expression to reject: {bad}"
        );
        let cf = json!({
            "name": "bad_field",
            "vcl_log_expression": bad,
            "parquet_type": "Utf8",
            "collection_stage": "edge"
        });
        assert!(
            !catalog::validate_custom_field(&cf, &[]).is_empty(),
            "expected catalog::validate_custom_field to reject: {bad}"
        );
        let cfg = json!({
            "groups": ["A"],
            "custom_fields": [cf],
            "field_overrides": {},
            "field_limits": {}
        });
        assert!(
            logging::generate_log_format(&cfg).is_err(),
            "expected generate_log_format to reject: {bad}"
        );
        assert!(
            logging::generate_compact_log_format(&cfg).is_err(),
            "expected generate_compact_log_format to reject: {bad}"
        );
        for stage in ["edge", "origin", "deliver"] {
            let mut staged = cfg.clone();
            staged["custom_fields"][0]["collection_stage"] = json!(stage);
            assert!(
                logging::generate_capture_vcl(&staged).is_err(),
                "expected generate_capture_vcl({stage}) to reject: {bad}"
            );
        }
    }
}

#[test]
fn sec21_vcl_validators_block_setcookie_rtime_adjacency_unary_table_and_unicode_percent_null() {
    let parity_bad = [
        "setcookie.get_value_by_name(resp, \"sid\")",
        "setcookie.delete_by_name(resp, \"rum_cid\")",
        "setcookie.get_by_name(resp, \"sid\")",
        "SETCOOKIE . GET_VALUE_BY_NAME(resp, \"sid\")",
        "now + 0msheader.get(req, \"Authorization\")",
        "now + 0.msheader.get(req, \"Authorization\")",
        "now + 0.5wsetcookie.delete_by_name(resp, \"sid\")",
        "now + 0mtable.lookup(fos_credentials, \"secret_key\")",
        "now + 0msfos_credentials",
        "now + 0.5wauth_fail_rc",
        "now + 0.5wauth_fail_pb",
        "now + 0msx-fastly-key",
        "-table.lookup_integer(solution_config, \"k\")",
        "+table.lookup_float(solution_config, \"k\")",
        "!table.lookup_bool(solution_config, \"k\")",
        "table /*x*/ . lookup(fos_credentials, \"secret_key\")",
        "\"foo%u0000bar\"",
        "\"foo%U0000bar\"",
        "\"foo%u{00}bar\"",
        "\"foo%u0041bar\"",
        "req.http.X \\",
        "req.url == \"/\"\n",
        "req.url == \"/\"\u{2028}",
        "req.url == \"/\"\u{2029}",
    ];
    for bad in parity_bad {
        assert!(
            logging::validate_vcl_condition(bad).is_err(),
            "expected validate_vcl_condition to reject: {bad:?}"
        );
        assert!(
            logging::rum::validate_custom_condition(bad).is_err(),
            "expected rum::validate_custom_condition to reject: {bad:?}"
        );
        assert!(
            !logging::validate_vcl_log_expression(bad).is_empty(),
            "expected validate_vcl_log_expression to reject: {bad:?}"
        );
    }

    assert!(logging::rum::validate_custom_condition("(now + 0msreq.url.path) != \"\"").is_err());
    assert!(logging::rum::validate_custom_condition("(now + 0.5wreq.url) != \"\"").is_err());
    assert!(
        logging::rum::validate_custom_condition("(now +\u{00a0}0msreq . url . path) != \"\"")
            .is_err()
    );

    let poisoned_shield = "iad-va-us); set req.http.X-Pwn = \"1";
    assert!(logging::rum::asset_fetch_vcl(poisoned_shield, Some("2.13.0")).is_err());
    assert!(logging::desired_snippets(&json!({
        "logging_enabled": true,
        "rum_enabled": true,
        "cdn_shield": poisoned_shield
    }))
    .is_err());
    assert!(logging::desired_snippets(&json!({
        "logging_enabled": true,
        "rum_enabled": false,
        "cdn_shield": poisoned_shield
    }))
    .is_err());
    assert!(logging::rum::asset_fetch_vcl("  iad-va-us  ", Some("2.13.0")).is_ok());

    for benign in [
        "req.http.X-Header.Get",
        "req.http.X-Table.Lookup",
        "resp.http.X-Setcookie",
        "req.http.X-LT-Run-Id",
        "req.http.X-LT-Request-Id",
    ] {
        assert!(
            logging::validate_vcl_log_expression(benign).is_empty(),
            "expected benign log expression to pass: {benign}"
        );
        assert!(
            logging::validate_vcl_condition(&format!("{benign} != \"\"")).is_ok(),
            "expected benign condition to pass: {benign}"
        );
    }
    assert!(
        logging::validate_vcl_condition("(req.restarts == 0 && req.http.Set-Cookie == \"\")")
            .is_ok()
    );
    assert!(logging::validate_vcl_condition(
        "req.http.Host == \"www.example.com\" && req.url.ext != \"ico\""
    )
    .is_ok());
}

use super::support::*;
use fastly_log_analytics::http::Response;
use serde_json::{json, Value};

fn other_install_resources(
    drop_capture_line: Option<&str>,
    rum: bool,
) -> Vec<(&'static str, Value)> {
    let other = Platform::new();
    let started = other.json("POST","/api/admin/provision/execute",json!({
        "token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-fixture-logs","fos_region":"us-east-1",
        "rum_enabled":rum,
        "log_fields":{"groups":["A","C"],"custom_fields":[],"field_overrides":{},"field_limits":{}}
    }));
    other.drain(started["job"].clone());
    let mut seeded = Vec::new();
    for resource in [
        "snippet",
        "logging/s3",
        "logging/fos",
        "condition",
        "backend",
        "dictionary",
    ] {
        for mut value in other.adapter.resources("fixtureService", resource) {
            if value["name"]
                .as_str()
                .is_some_and(|n| n.starts_with("flp-owner-"))
            {
                continue;
            }
            if let (Some(drop), Some(content)) = (drop_capture_line, value["content"].as_str()) {
                value["content"] = content
                    .lines()
                    .filter(|line| !line.contains(drop))
                    .collect::<Vec<_>>()
                    .join("\n")
                    .into();
            }
            seeded.push((resource, value));
        }
    }
    seeded
}

fn seed_other_install(
    platform: &Platform,
    drop_capture_line: Option<&str>,
    rum: bool,
) -> Vec<(&'static str, Value)> {
    let seeded = other_install_resources(drop_capture_line, rum);
    for (resource, value) in &seeded {
        platform
            .adapter
            .seed_resource("fixtureService", resource, value.clone());
    }
    seeded
}

fn attach_body(extra: Value) -> Value {
    let mut body = json!({
        "token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-attach-logs","fos_region":"us-east-1",
        "capture":"attached",
        "log_fields":{"groups":["A","C"],"custom_fields":[],"field_overrides":{},"field_limits":{}}
    });
    for (key, value) in extra.as_object().unwrap() {
        body[key] = value.clone();
    }
    body
}

fn error_code(response: &Response) -> String {
    serde_json::from_slice::<Value>(&response.body).unwrap()["error"]
        .as_str()
        .unwrap_or("")
        .to_owned()
}

#[test]
fn attach_mode_manages_only_its_marker_endpoint_and_condition() {
    attach_alongside(false);
}

#[test]
fn attach_mode_leaves_the_other_install_rum_backend_and_dictionary_alone() {
    attach_alongside(true);
}

fn attach_alongside(other_has_rum: bool) {
    let platform = Platform::new();
    let seeded = seed_other_install(&platform, None, other_has_rum);
    assert!(seeded
        .iter()
        .any(|(_, v)| v["name"] == "Fastly Object Storage Logs"));
    assert_eq!(
        seeded.iter().any(|(_, v)| v["name"] == "fos_origin"),
        other_has_rum
    );
    let managed = platform.call(
        "POST",
        "/api/admin/provision/execute",
        json!({"token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-attach-logs","fos_region":"us-east-1"}),
    );
    assert_eq!(managed.status, 409);
    assert_eq!(error_code(&managed), "service_managed_by_other_install");

    let validated = platform.json(
        "POST",
        "/api/admin/provision/validate",
        json!({"token":"fixture-token","service_id":"fixtureService","capture":"attached",
            "log_fields":{"groups":["A","C"],"custom_fields":[],"field_overrides":{},"field_limits":{}}}),
    );
    assert_eq!(
        validated["capture"],
        json!({"mode":"attached","missing_fields":[],"rum_present":other_has_rum,"cmcd_present":false})
    );
    let default_bucket = validated["defaults"]["fos_bucket_name"]
        .as_str()
        .expect("default fos_bucket_name should be a string");
    assert!(
        default_bucket.starts_with("fla-")
            && default_bucket.len() == 28
            && default_bucket[4..]
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            && !default_bucket.contains("fixtureservice"),
        "expected opaque CSPRNG bucket name without service_id, got {default_bucket}"
    );
    assert_eq!(validated["defaults"]["fos_region"], "us-east-1");

    let job = platform.json(
        "POST",
        "/api/admin/provision/execute",
        attach_body(json!({})),
    )["job"]
        .clone();
    assert_eq!(platform.drain(job)["status"], "complete");
    let service = platform.service();
    assert_eq!(service["service"]["capture"], "attached");
    assert_eq!(
        service["service"]["logging_endpoint_name"],
        "FLP Cloud Logs"
    );

    for (resource, value) in &seeded {
        let now = platform.adapter.resources("fixtureService", resource);
        assert!(now.contains(value), "{resource} {} changed", value["name"]);
    }

    let names = |resource: &str| -> Vec<String> {
        let mut names: Vec<String> = platform
            .adapter
            .resources("fixtureService", resource)
            .iter()
            .map(|v| v["name"].as_str().unwrap().to_owned())
            .filter(|name| {
                !seeded
                    .iter()
                    .any(|(r, v)| r == &resource && v["name"] == name.as_str())
            })
            .collect();
        names.sort();
        names
    };
    assert_eq!(names("snippet"), ["flp-owner-fixtureApp"]);
    assert_eq!(names("logging/fos"), ["FLP Cloud Logs"]);
    assert_eq!(names("condition"), ["flp_cloud_log_condition"]);
    let endpoint = platform
        .adapter
        .resources("fixtureService", "logging/fos")
        .into_iter()
        .find(|v| v["name"] == "FLP Cloud Logs")
        .unwrap();
    assert_eq!(endpoint["response_condition"], "flp_cloud_log_condition");
    assert_eq!(endpoint["bucket_name"], "fos-attach-logs");
    let conditions = platform.adapter.resources("fixtureService", "condition");
    let statement =
        |name: &str| conditions.iter().find(|v| v["name"] == name).unwrap()["statement"].clone();
    assert_eq!(
        statement("flp_cloud_log_condition"),
        statement("log_analytics_condition")
    );
    assert_eq!(
        statement("flp_cloud_log_condition")
            .as_str()
            .unwrap()
            .contains("req.url.path != \"/rum-beacon\""),
        other_has_rum
    );
    let touched: Vec<_> = platform
        .adapter
        .calls()
        .into_iter()
        .filter(|(_, path)| {
            path.starts_with("/service/fixtureService/")
                && (path.contains("/backend") || path.contains("/dictionary"))
        })
        .collect();
    assert!(touched.is_empty(), "{touched:?}");

    let service = platform.service();
    let again = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation":service["generation"]}),
    );
    assert_eq!(platform.drain(again["job"].clone())["changes"], json!([]));

    let service = platform.service();
    let result = platform.json("POST","/api/admin/provision/teardown",json!({
        "service_id":"fixtureService","confirm_service_id":"fixtureService","token":"fixture-token",
        "generation":service["generation"]
    }));
    let job = platform.drain(result["job"].clone());
    let mut removed: Vec<_> = job["changes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            (
                c["action"].as_str().unwrap().to_owned(),
                c["name"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    removed.sort();
    assert_eq!(
        removed,
        [
            ("delete".to_owned(), "FLP Cloud Logs".to_owned()),
            ("delete".to_owned(), "flp-owner-fixtureApp".to_owned()),
            ("delete".to_owned(), "flp_cloud_log_condition".to_owned()),
        ]
    );
    for resource in [
        "snippet",
        "logging/s3",
        "logging/fos",
        "condition",
        "backend",
        "dictionary",
    ] {
        let mut now = platform.adapter.resources("fixtureService", resource);
        let mut before: Vec<Value> = seeded
            .iter()
            .filter(|(r, _)| *r == resource)
            .map(|(_, v)| v.clone())
            .collect();
        now.sort_by_key(|v| v["name"].to_string());
        before.sort_by_key(|v| v["name"].to_string());
        assert_eq!(now, before, "{resource}");
    }
}

#[test]
fn attach_detects_cmcd_from_existing_logging_endpoint_format() {
    let platform = Platform::new();
    let seeded = seed_other_install(&platform, None, false);
    let endpoint = seeded
        .iter()
        .find(|(resource, value)| {
            ["logging/s3", "logging/fos"].contains(resource)
                && value["name"] == "Fastly Object Storage Logs"
        })
        .unwrap()
        .1
        .clone();
    let mut with_cmcd = endpoint;
    with_cmcd["format"] = json!(format!(
        "{}{}",
        with_cmcd["format"].as_str().unwrap(),
        r#","cmcd_sid":"%{json.escape(req.http.x-cmcd-sid)}V""#
    ));
    platform
        .adapter
        .seed_resource("fixtureService", "logging/fos", with_cmcd);

    let validated = platform.json(
        "POST",
        "/api/admin/provision/validate",
        json!({"token":"fixture-token","service_id":"fixtureService","capture":"attached",
            "log_fields":{"groups":["A","C"],"custom_fields":[],"field_overrides":{},"field_limits":{}}}),
    );
    assert_eq!(validated["capture"]["cmcd_present"], true);
}

#[test]
fn attach_mode_requires_the_other_install_capture_vcl() {
    let platform = Platform::new();
    for path in [
        "/api/admin/provision/validate",
        "/api/admin/provision/execute",
    ] {
        let response = platform.call("POST", path, attach_body(json!({})));
        assert_eq!(response.status, 409);
        assert_eq!(error_code(&response), "capture_vcl_missing");
    }
    assert_eq!(platform.adapter.key_count(), 0);
}

#[test]
fn attach_mode_rejects_reserved_endpoint_names() {
    let platform = Platform::new();
    seed_other_install(&platform, None, false);
    for (extra, code) in [
        (
            json!({"logging_endpoint_name":"Fastly RUM Logs"}),
            "attach_endpoint_name_reserved",
        ),
        (
            json!({"logging_endpoint_name":"My Fastly Log Analytics copy"}),
            "attach_endpoint_name_reserved",
        ),
    ] {
        let response = platform.call(
            "POST",
            "/api/admin/provision/execute",
            attach_body(extra.clone()),
        );
        assert_eq!(response.status, 400, "{extra}");
        assert_eq!(error_code(&response), code, "{extra}");
    }
    assert_eq!(platform.adapter.key_count(), 0);
}

#[test]
fn attach_mode_supports_cmcd_and_rum_without_touching_vcl_backend_or_dictionary() {
    let platform = Platform::new();
    let seeded = seed_other_install(&platform, None, true);
    let job = platform.json(
        "POST",
        "/api/admin/provision/execute",
        attach_body(json!({
            "rum_enabled": true,
            "cmcd": {"enabled": true, "mode": "query_string", "version": 1},
        })),
    )["job"]
        .clone();
    assert_eq!(platform.drain(job)["status"], "complete");

    let service = platform.service();
    assert_eq!(service["service"]["capture"], "attached");
    assert_eq!(service["service"]["rum"]["enabled"], true);
    assert_eq!(service["service"]["rum"]["endpoint_name"], "FLP Cloud RUM");
    assert_eq!(service["service"]["cmcd"]["enabled"], true);

    for (resource, value) in &seeded {
        let now = platform.adapter.resources("fixtureService", resource);
        assert!(now.contains(value), "{resource} {} changed", value["name"]);
    }

    let names = |resource: &str| -> Vec<String> {
        let mut names: Vec<String> = platform
            .adapter
            .resources("fixtureService", resource)
            .iter()
            .map(|v| v["name"].as_str().unwrap().to_owned())
            .filter(|name| {
                !seeded
                    .iter()
                    .any(|(r, v)| r == &resource && v["name"] == name.as_str())
            })
            .collect();
        names.sort();
        names
    };
    assert_eq!(names("snippet"), ["flp-owner-fixtureApp"]);
    assert_eq!(names("logging/fos"), ["FLP Cloud Logs", "FLP Cloud RUM"]);
    assert_eq!(
        names("condition"),
        ["flp_cloud_log_condition", "flp_cloud_rum_condition"]
    );

    let logs_ep = platform
        .adapter
        .resources("fixtureService", "logging/fos")
        .into_iter()
        .find(|v| v["name"] == "FLP Cloud Logs")
        .unwrap();
    assert_eq!(logs_ep["response_condition"], "flp_cloud_log_condition");
    assert!(logs_ep["format"].as_str().unwrap().contains("cmcd"));

    let rum_ep = platform
        .adapter
        .resources("fixtureService", "logging/fos")
        .into_iter()
        .find(|v| v["name"] == "FLP Cloud RUM")
        .unwrap();
    assert_eq!(rum_ep["response_condition"], "flp_cloud_rum_condition");
    assert_eq!(rum_ep["bucket_name"], "fos-attach-logs");

    let conditions = platform.adapter.resources("fixtureService", "condition");
    let statement =
        |name: &str| conditions.iter().find(|v| v["name"] == name).unwrap()["statement"].clone();
    assert_eq!(
        statement("flp_cloud_rum_condition"),
        statement("rum_log_condition")
    );

    let touched: Vec<_> = platform
        .adapter
        .calls()
        .into_iter()
        .filter(|(_, path)| {
            path.starts_with("/service/fixtureService/")
                && (path.contains("/backend") || path.contains("/dictionary"))
        })
        .collect();
    assert!(
        touched.is_empty(),
        "touched backend/dictionary: {touched:?}"
    );

    let changed_rum = "req.url.path == \"/rum-beacon\" && req.http.host == \"rum.example\"";
    platform.adapter.seed_resource(
        "fixtureService",
        "condition",
        json!({"name":"rum_log_condition","statement":changed_rum,"type":"RESPONSE","priority":10}),
    );
    let again = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation":service["generation"]}),
    );
    assert_eq!(platform.drain(again["job"].clone())["status"], "complete");
    let updated_conditions = platform.adapter.resources("fixtureService", "condition");
    assert_eq!(
        updated_conditions
            .iter()
            .find(|v| v["name"] == "flp_cloud_rum_condition")
            .unwrap()["statement"],
        changed_rum
    );
}

#[test]
fn attach_validation_reports_fields_the_existing_capture_vcl_never_sets() {
    let platform = Platform::new();
    seed_other_install(&platform, Some("x-fos-edge-data:srv_region"), false);
    let validated = platform.json(
        "POST",
        "/api/admin/provision/validate",
        json!({"token":"fixture-token","service_id":"fixtureService","capture":"attached",
            "log_fields":{"groups":["A","C"],"custom_fields":[],"field_overrides":{},"field_limits":{}}}),
    );
    assert_eq!(
        validated["capture"]["missing_fields"],
        json!(["server_region"])
    );
}

fn seed_other_install_condition(platform: &Platform, condition: Option<Value>) {
    for (resource, value) in other_install_resources(None, false) {
        if resource != "condition" || value["name"] != "log_analytics_condition" {
            platform
                .adapter
                .seed_resource("fixtureService", resource, value);
        }
    }
    if let Some(condition) = condition {
        platform
            .adapter
            .seed_resource("fixtureService", "condition", condition);
    }
}

fn attached_statement(platform: &Platform) -> Value {
    platform
        .adapter
        .resources("fixtureService", "condition")
        .into_iter()
        .find(|v| v["name"] == "flp_cloud_log_condition")
        .unwrap()["statement"]
        .clone()
}

#[test]
fn attach_mode_copies_the_other_install_condition_verbatim_and_follows_its_changes() {
    let platform = Platform::new();
    let theirs = "!segmented_caching.is_inner_req && req.url.path != \"/rum-beacon\" && (req.http.host == \"shop.example\")";
    seed_other_install_condition(
        &platform,
        Some(
            json!({"name":"log_analytics_condition","statement":theirs,"type":"RESPONSE","priority":10}),
        ),
    );
    let job = platform.json(
        "POST",
        "/api/admin/provision/execute",
        attach_body(json!({"sample_rate":50,"custom_condition":"req.http.x-other == \"1\""})),
    )["job"]
        .clone();
    assert_eq!(platform.drain(job)["status"], "complete");
    assert_eq!(attached_statement(&platform), theirs);

    let changed = "!segmented_caching.is_inner_req && randombool(25, 100)";
    platform.adapter.seed_resource(
        "fixtureService",
        "condition",
        json!({"name":"log_analytics_condition","statement":changed,"type":"RESPONSE","priority":10}),
    );
    let service = platform.service();
    let again = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation":service["generation"]}),
    );
    let job = platform.drain(again["job"].clone());
    assert_eq!(job["status"], "complete");
    assert_eq!(attached_statement(&platform), changed);
}

#[test]
fn attach_mode_without_the_other_condition_excludes_the_rum_beacon() {
    let platform = Platform::new();
    seed_other_install_condition(&platform, None);
    let job = platform.json(
        "POST",
        "/api/admin/provision/execute",
        attach_body(json!({"edge_only":true})),
    )["job"]
        .clone();
    assert_eq!(platform.drain(job)["status"], "complete");
    assert_eq!(
        attached_statement(&platform),
        "!segmented_caching.is_inner_req && req.url.path != \"/rum-beacon\" && fastly.ff.visits_this_service == 0"
    );
}

#[test]
fn attach_mode_rejects_an_other_install_condition_it_cannot_read() {
    for condition in [
        json!({"name":"log_analytics_condition","statement":"  ","type":"RESPONSE"}),
        json!({"name":"log_analytics_condition","type":"RESPONSE"}),
        json!({"name":"log_analytics_condition","statement":"req.url.path != \"/x\"","type":"REQUEST"}),
    ] {
        let platform = Platform::new();
        seed_other_install_condition(&platform, Some(condition.clone()));
        for path in [
            "/api/admin/provision/validate",
            "/api/admin/provision/execute",
        ] {
            let response = platform.call("POST", path, attach_body(json!({})));
            assert_eq!(response.status, 409, "{path} {condition}");
            assert_eq!(
                error_code(&response),
                "capture_condition_unreadable",
                "{path} {condition}"
            );
        }
        assert_eq!(platform.adapter.key_count(), 0);
    }
}

#[test]
fn attach_mode_accepts_partial_log_fields_with_cmcd_rum_and_teardown_preserves_other_install() {
    let platform = Platform::new();
    let mut seeded = seed_other_install(&platform, None, true);
    let cmcd_snippet = json!({"name":"CMCD Extraction","subroutine":"vcl_recv","type":"recv","content":"declare local var.cmcd STRING;"});
    platform
        .adapter
        .seed_resource("fixtureService", "snippet", cmcd_snippet.clone());
    seeded.push(("snippet", cmcd_snippet));
    let rum_s3 = json!({"name":"Fastly RUM Logs","bucket_name":"other-logs","response_condition":"rum_log_condition"});
    platform
        .adapter
        .seed_resource("fixtureService", "logging/s3", rum_s3.clone());
    seeded.push(("logging/s3", rum_s3));

    // Wizard's applyAttach sends log_fields as a JSON string without custom_fields.
    let job = platform.json(
        "POST",
        "/api/provision/execute",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-attach-logs",
            "fos_region": "us-east-1",
            "capture": "attached",
            "edge_only": false,
            "sample_rate": "100",
            "logging_enabled": true,
            "rum_enabled": true,
            "cmcd": {"enabled": true, "mode": "query_string", "version": 1},
            "log_fields": "{\"groups\":[\"A\",\"B\",\"C\",\"D\",\"E\",\"F\",\"G\",\"H\",\"I\",\"J\",\"K\",\"L\",\"M\"],\"field_overrides\":{}}",
        }),
    )["job"]
        .clone();
    assert_eq!(platform.drain(job)["status"], "complete");

    let service = platform.service();
    let teardown = platform.json(
        "POST",
        "/api/admin/provision/teardown",
        json!({
            "service_id": "fixtureService",
            "confirm_service_id": "fixtureService",
            "token": "fixture-token",
            "generation": service["generation"],
        }),
    );
    let finished = platform.drain(teardown["job"].clone());
    assert_eq!(finished["status"], "complete");
    let mut removed: Vec<_> = finished["changes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            (
                c["action"].as_str().unwrap().to_owned(),
                c["name"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    removed.sort();
    assert_eq!(
        removed,
        [
            ("delete".to_owned(), "FLP Cloud Logs".to_owned()),
            ("delete".to_owned(), "FLP Cloud RUM".to_owned()),
            ("delete".to_owned(), "flp-owner-fixtureApp".to_owned()),
            ("delete".to_owned(), "flp_cloud_log_condition".to_owned()),
            ("delete".to_owned(), "flp_cloud_rum_condition".to_owned()),
        ]
    );
    for resource in [
        "snippet",
        "logging/s3",
        "logging/fos",
        "condition",
        "backend",
        "dictionary",
    ] {
        let mut now = platform.adapter.resources("fixtureService", resource);
        let mut before: Vec<Value> = seeded
            .iter()
            .filter(|(r, _)| *r == resource)
            .map(|(_, v)| v.clone())
            .collect();
        now.sort_by_key(|v| v["name"].to_string());
        before.sort_by_key(|v| v["name"].to_string());
        assert_eq!(now, before, "{resource}");
    }
}

use super::support::*;
use fastly_log_analytics::{
    admin,
    auth::{self, Role},
    shell::{Runtime, ANALYTICS_SITE_ROUTES},
    state::StateStore,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

#[test]
fn anonymous_expired_and_tampered_sessions_get_401_everywhere() {
    let harness = Harness::new();
    for (method, path, class) in ROUTES {
        for session in ["anonymous", "expired", "tampered"] {
            let status = harness.send(method, path, session).status;
            assert_eq!(status, 401, "{method} {path} ({class:?}) as {session}");
        }
    }
}

#[test]
fn admin_invites_report_acceptance_and_the_linked_users_last_sign_in() {
    let h = Harness::new();
    let now = h.platform.now();
    let mut current = h.store.load().unwrap();
    current
        .state
        .users
        .get_mut("u-analyst")
        .unwrap()
        .last_login_at = Some(now - 60);
    current.state.invites.insert(
        "i".into(),
        auth::Invite {
            id: "i".into(),
            display_name: "u-analyst".into(),
            role: Role::Analyst,
            expires_at: now + 3600,
            revoked: false,
            used: true,
            created_by: "u-admin".into(),
            accepted_at: Some(now - 120),
            user_id: Some("u-analyst".into()),
        },
    );
    assert!(h
        .store
        .compare_and_swap(current.generation.as_deref(), &current.state)
        .unwrap());

    let body: Value =
        serde_json::from_slice(&h.send("GET", "/api/admin/invites", "admin").body).unwrap();
    let invite = &body["invites"][0];
    assert_eq!(invite["user_id"], "u-analyst");
    assert_eq!(invite["accepted_at"], admin::rfc3339_from_secs(now - 120));
    assert_eq!(invite["last_login_at"], admin::rfc3339_from_secs(now - 60));

    let body: Value =
        serde_json::from_slice(&h.send("GET", "/api/admin/users", "admin").body).unwrap();
    let users = body["users"].as_array().unwrap();
    let analyst = users.iter().find(|u| u["id"] == "u-analyst").unwrap();
    assert_eq!(analyst["created_at"], admin::rfc3339_from_secs(now));
    assert_eq!(analyst["last_login_at"], admin::rfc3339_from_secs(now - 60));
    let admin_user = users.iter().find(|u| u["id"] == "u-admin").unwrap();
    assert!(admin_user["last_login_at"].is_null());
}

#[test]
fn admin_routes_reject_analysts_and_admin_is_not_blocked() {
    let harness = Harness::new();
    for (method, path, class) in ROUTES {
        let admin = harness.send(method, path, "admin").status;
        assert!(
            status_ok_for_admin(admin),
            "admin blocked on {method} {path}: {admin}"
        );
        for session in ["analyst", "no_pii"] {
            let status = harness.send(method, path, session).status;
            match class {
                Class::Admin => assert_eq!(status, 403, "{method} {path} as {session}"),
                Class::NoPiiDenied if session == "no_pii" => {
                    assert_eq!(status, 403, "{method} {path} as {session}")
                }
                _ => assert!(
                    status_ok_for_admin(status),
                    "{method} {path} as {session}: {status}"
                ),
            }
        }
    }
}

#[test]
fn analyst_attachment_is_read_only_and_revocation_invalidates_the_session() {
    let harness = Harness::new();

    assert_eq!(
        harness.send("GET", "/api/header-badge", "admin").status,
        200
    );
    assert_eq!(
        harness.send("GET", "/api/header-badge", "analyst").status,
        200
    );
    assert_eq!(
        harness
            .send("POST", "/api/admin/users/u-victim/revoke", "analyst")
            .status,
        403
    );

    let mut current = harness.store.load().unwrap();
    current.state.users.get_mut("u-analyst").unwrap().revoked = true;
    assert!(harness
        .store
        .compare_and_swap(current.generation.as_deref(), &current.state)
        .unwrap());

    assert_eq!(
        harness.send("GET", "/api/header-badge", "analyst").status,
        401
    );
}

#[test]
fn admin_pages_redirect_anonymous_and_reject_analysts() {
    let harness = Harness::new();
    for page in [
        "/admin",
        "/admin/share",
        "/admin/usage-log",
        "/admin/rum",
        "/admin/session-scoring",
        "/logs",
    ] {
        for path in [
            page.to_string(),
            format!("{page}.html"),
            format!("{page}.txt"),
        ] {
            let anonymous = harness.send("GET", &path, "anonymous");
            assert_eq!(anonymous.status, 303, "{path} as anonymous");
            for session in ["analyst", "no_pii"] {
                let status = harness.send("GET", &path, session).status;
                assert_eq!(status, 403, "{path} as {session}");
            }
            let admin = harness.send("GET", &path, "admin").status;
            assert!(
                status_ok_for_admin(admin) && admin != 303,
                "{path} as admin: {admin}"
            );
        }
    }
}

#[test]
fn analytics_site_routes_allowlist_covers_all_exported_pages() {
    for route in [
        "/dashboard",
        "/origin",
        "/performance",
        "/security",
        "/network",
        "/fastly-value",
        "/assets-shield",
        "/rum",
        "/streaming",
        "/sessions",
        "/sessions/detail",
        "/sessions/stream",
        "/query",
        "/insights",
        "/usage",
        "/how-it-works",
        "/share-login",
        "/share-login/acknowledge",
        "/logs",
        "/admin",
    ] {
        assert!(
            ANALYTICS_SITE_ROUTES.contains(&route),
            "expected {route} in ANALYTICS_SITE_ROUTES"
        );
    }
}

#[test]
fn routes_without_an_exported_page_are_not_site_or_admin_pages() {
    for route in ["/admin/tick"] {
        assert!(
            !ANALYTICS_SITE_ROUTES.contains(&route),
            "{route} has no page but is in ANALYTICS_SITE_ROUTES"
        );
        for path in [route.to_string(), format!("{route}.html")] {
            assert!(
                !fastly_log_analytics::shell::is_admin_page_path(&path),
                "{path} has no page but is an admin page path"
            );
        }
    }
}

#[test]
fn operational_datasets_and_files_are_admin_only() {
    let harness = Harness::new();
    for path in [
        "/api/files?dataset=ops:cron_ticks",
        "/api/files?dataset=ops",
        "/api/file?key=processed/ops/cron_ticks/x.parquet",
    ] {
        for session in ["analyst", "no_pii"] {
            let status = harness.send("GET", path, session).status;
            assert_eq!(status, 403, "{path} as {session}");
        }
        assert_eq!(harness.send("GET", path, "anonymous").status, 401, "{path}");
        let admin = harness.send("GET", path, "admin").status;
        assert!(status_ok_for_admin(admin), "{path} as admin: {admin}");
    }
    for session in ["analyst", "no_pii"] {
        let status = harness
            .send("GET", "/api/files?dataset=request", session)
            .status;
        assert!(status_ok_for_admin(status), "request dataset as {session}");
    }
}

#[test]
fn operational_settings_changes_are_audited() {
    let harness = Harness::new();
    let patch = harness.send_with(
        "PATCH",
        "/api/admin/operational-logging",
        "admin",
        Some("https://logs.example.com"),
        br#"{"flush_max_rows": 750}"#,
    );
    assert_eq!(
        patch.status,
        200,
        "{}",
        String::from_utf8_lossy(&patch.body)
    );
    let rejected = harness.send_with(
        "PATCH",
        "/api/admin/operational-logging",
        "admin",
        Some("https://logs.example.com"),
        br#"{"flush_max_rows": 1}"#,
    );
    assert_eq!(rejected.status, 400);
    let audit = harness.send("GET", "/api/admin/audit-logs", "admin");
    let body: Value = serde_json::from_slice(&audit.body).unwrap();
    assert!(
        !body.to_string().contains("operational_logging_updated"),
        "settings changes belong to the audit log, not service history"
    );
    let state = harness.store.load().unwrap().state;
    let text = serde_json::to_string(&[&state.audit, &state.recent_audit]).unwrap();
    assert_eq!(
        text.matches("operational_logging_updated").count(),
        1,
        "only the applied patch is audited: {text}"
    );
    assert!(text.contains("flush_max_rows"));
}

#[test]
fn quarantine_purge_needs_a_confirm_token_and_audits_only_confirmed_steps() {
    let harness = Harness::new();
    let post = |body: Value| {
        let response = harness.send_with(
            "POST",
            "/api/admin/quarantine/purge",
            "admin",
            Some("https://logs.example.com"),
            body.to_string().as_bytes(),
        );
        let status = response.status;
        (
            status,
            serde_json::from_slice::<Value>(&response.body).unwrap(),
        )
    };
    let audited = || {
        let state = harness.store.load().unwrap().state;
        serde_json::to_string(&[&state.audit, &state.recent_audit])
            .unwrap()
            .matches("quarantine_purged")
            .count()
    };
    let (status, issued) = post(json!({"target": "all"}));
    assert_eq!(status, 200, "{issued}");
    assert_eq!(issued["status"], "confirm_required");
    assert_eq!(audited(), 0, "asking for a token is not a purge");
    let (status, _) = post(json!({"target": "all", "confirm": "forged.token"}));
    assert_eq!(status, 400);
    assert_eq!(audited(), 0);
    let (status, done) = post(json!({"target": "all", "confirm": issued["confirm_token"]}));
    assert_eq!(status, 200, "{done}");
    assert_eq!(
        (done["purged"].as_u64(), done["complete"].as_bool()),
        (Some(0), Some(true))
    );
    assert_eq!(audited(), 1);
}

#[test]
fn services_listing_is_redacted_for_analysts_only() {
    let harness = Harness::new();
    let read = |session| -> Vec<Value> {
        let response = harness.send("GET", "/api/services", session);
        assert_eq!(response.status, 200);
        serde_json::from_slice::<Value>(&response.body).unwrap()["services"]
            .as_array()
            .unwrap()
            .clone()
    };
    let allowed: BTreeSet<&str> = [
        "service_id",
        "name",
        "status",
        "logging_enabled",
        "created_at",
        "updated_at",
        "log_fields",
        "rum_enabled",
        "rum",
        "faro_version",
        "cmcd",
        "scoring",
        "active_log_field_ids",
    ]
    .into();
    let full = read("admin");
    assert!(!full.is_empty());
    let logged = full[0]["active_log_field_ids"]
        .as_array()
        .expect("services list the enabled log field ids");
    for id in ["status", "host", "pop"] {
        assert!(logged.contains(&json!(id)), "{id} missing from {logged:?}");
    }
    for id in ["edge_score_reason_ind", "_bot_name", "ja3", "ploss"] {
        assert!(!logged.contains(&json!(id)), "{id} listed in {logged:?}");
    }
    let full_keys: BTreeSet<String> = full[0].as_object().unwrap().keys().cloned().collect();
    assert!(
        full_keys.iter().any(|key| key.ends_with("_ref")
            || key == "bucket"
            || key == "prefix"
            || key == "owner"
            || key == "log_fields"),
        "admin record unexpectedly lacks sensitive fields: {full_keys:?}"
    );
    for session in ["analyst", "no_pii"] {
        let services = read(session);
        assert_eq!(services.len(), full.len());
        for (service, original) in services.iter().zip(&full) {
            for key in service.as_object().unwrap().keys() {
                assert!(
                    allowed.contains(key.as_str()),
                    "{session} sees {key} in /api/services"
                );
            }
            assert_eq!(service["rum_enabled"], original["rum_enabled"]);
            assert_eq!(
                service["active_log_field_ids"],
                original["active_log_field_ids"]
            );
            assert_eq!(service["faro_version"], original["faro_version"]);
            assert_eq!(
                service["rum"],
                json!({
                    "enabled": original["rum_enabled"],
                    "faro_version": original["faro_version"],
                })
            );
            for feature in ["cmcd", "scoring"] {
                assert_eq!(service[feature], json!({"enabled": false}));
            }
            assert_eq!(
                service["log_fields"],
                json!({
                    "groups": ["A", "C"],
                    "field_overrides": {},
                    "custom_fields": [],
                })
            );
        }
    }
}

#[test]
fn tick_requires_exact_path() {
    let harness = Harness::new();
    assert_eq!(harness.send("GET", "/_tick/", "anonymous").status, 403);
    assert_eq!(harness.send("GET", "/_tick/?x=1", "anonymous").status, 403);
    assert_eq!(harness.send("GET", "/_tick", "anonymous").status, 200);
}

#[test]
fn every_api_literal_in_the_shell_declares_an_auth_class() {
    let source = include_str!("../../src/shell.rs");
    let declared: BTreeMap<&str, Class> = DECLARED.iter().copied().collect();
    let mut found = BTreeSet::new();
    let mut rest = source;
    while let Some(start) = rest.find("\"/api/") {
        let after = &rest[start + 1..];
        let end = after.find('"').unwrap();
        found.insert(&after[..end]);
        rest = &after[end + 1..];
    }
    assert!(found.len() > 20, "literal scan found too little: {found:?}");
    let undeclared: Vec<_> = found
        .iter()
        .filter(|literal| !declared.contains_key(**literal))
        .collect();
    assert!(
        undeclared.is_empty(),
        "declare an auth class for new shell routes in DECLARED and ROUTES: {undeclared:?}"
    );
    for (literal, class) in DECLARED {
        if *class != Class::Public {
            assert!(
                ROUTES.iter().any(|(_, path, _)| path.starts_with(literal)),
                "{literal} is declared but no ROUTES entry exercises it"
            );
        }
    }
    for (_, path, class) in ROUTES {
        let covering = DECLARED
            .iter()
            .filter(|(literal, _)| path.starts_with(literal) && *literal != "/api/")
            .map(|(_, class)| *class)
            .collect::<Vec<_>>();
        assert!(
            covering.contains(class),
            "{path} has no matching DECLARED class {class:?}"
        );
    }
}

#[test]
fn array_style_routes_are_not_missed_by_the_literal_scan() {
    let source = include_str!("../../src/shell.rs");
    let mut rest = source;
    while let Some(start) = rest.find("[\"api\"") {
        let after = &rest[start..];
        let end = after.find(']').unwrap();
        let segments: Vec<&str> = after[1..end]
            .split(',')
            .map(str::trim)
            .map(|part| {
                if part.starts_with('"') {
                    part.trim_matches('"')
                } else {
                    "*"
                }
            })
            .collect();
        let path = format!("/{}", segments.join("/"));
        assert!(
            ROUTES.iter().any(|(_, route, _)| {
                let route_parts: Vec<_> = route.split('/').skip(1).collect();
                route_parts.len() >= segments.len()
                    && route_parts
                        .iter()
                        .zip(&segments)
                        .all(|(a, b)| *b == "*" || a == b)
            }),
            "segment route {path} is not exercised in ROUTES"
        );
        rest = &after[end..];
    }
}

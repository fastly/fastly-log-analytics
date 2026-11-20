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
            let res = harness.send(method, path, session);
            assert_eq!(res.status, 401, "{method} {path} ({class:?}) as {session}");
            if session == "anonymous" {
                assert!(
                    !res.headers.contains_key("x-flp-auth-fail"),
                    "{method} {path} as anonymous must not set x-flp-auth-fail"
                );
            } else {
                assert_eq!(
                    res.headers.get("x-flp-auth-fail").map(String::as_str),
                    Some("1"),
                    "{method} {path} as {session} must set x-flp-auth-fail"
                );
                assert!(
                    res.cookies().iter().any(|c| c.starts_with("flp_session=;")),
                    "{method} {path} as {session} must clear flp_session"
                );
            }
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
    let harness = Harness::new();
    for route in ["/admin/tick"] {
        assert!(
            !ANALYTICS_SITE_ROUTES.contains(&route),
            "{route} has no page but is in ANALYTICS_SITE_ROUTES"
        );
        for path in [route.to_string(), format!("{route}.html")] {
            assert!(
                fastly_log_analytics::shell::is_admin_page_path(&path),
                "{path} under /admin/ must enforce the admin page gate"
            );
            assert_eq!(harness.send("GET", &path, "anonymous").status, 303);
            assert_eq!(harness.send("GET", &path, "analyst").status, 403);
            let admin = harness.send("GET", &path, "admin");
            assert_eq!(admin.status, 404);
            assert_eq!(
                admin.headers.get("cache-control").map(String::as_str),
                Some("private, no-store")
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

#[test]
fn nested_admin_and_logs_rsc_subpaths_enforce_admin_gate_without_affecting_api_logs_live() {
    use fastly_log_analytics::http::Request;
    use fastly_log_analytics::shell::Shell;
    use fastly_log_analytics::site::Asset;

    static ASSETS: &[Asset] = &[
        Asset {
            path: "/admin/__next._tree.txt",
            content_type: "text/x-component",
            identity: Some(b"admin-tree"),
            gzip: None,
            br: None,
        },
        Asset {
            path: "/admin/share/__next._full.txt",
            content_type: "text/x-component",
            identity: Some(b"admin-share-full"),
            gzip: None,
            br: None,
        },
        Asset {
            path: "/logs/__next._tree.txt",
            content_type: "text/x-component",
            identity: Some(b"logs-tree"),
            gzip: None,
            br: None,
        },
        Asset {
            path: "/logs/subpath.html",
            content_type: "text/html; charset=utf-8",
            identity: Some(b"<html>logs-sub</html>"),
            gzip: None,
            br: None,
        },
        Asset {
            path: "/provision/__next._tree.txt",
            content_type: "text/x-component",
            identity: Some(b"provision-tree"),
            gzip: None,
            br: None,
        },
        Asset {
            path: "/log-fields/__next._tree.txt",
            content_type: "text/x-component",
            identity: Some(b"log-fields-tree"),
            gzip: None,
            br: None,
        },
        Asset {
            path: "/services/__next._tree.txt",
            content_type: "text/x-component",
            identity: Some(b"services-tree"),
            gzip: None,
            br: None,
        },
    ];

    let h = Harness::new();
    let send_asset = |path: &str, session: &str| {
        let mut headers = BTreeMap::from([
            (
                "x-flp-front-key".to_string(),
                "fixture-front-key".to_string(),
            ),
            ("fastly-client-ip".into(), "192.0.2.1".into()),
            ("x-flp-front-rl".into(), "1".into()),
        ]);
        if let Some(cookie) = &h.sessions[session] {
            headers.insert("cookie".into(), cookie.clone());
        }
        Shell::new(&h.platform, &h.store, ASSETS).handle(&Request {
            method: "GET".into(),
            path: path.into(),
            headers,
            body: Vec::new(),
        })
    };

    for path in [
        "/admin/__next._tree.txt",
        "/admin/share/__next._full.txt",
        "/logs/__next._tree.txt",
        "/logs/subpath",
        "/provision/__next._tree.txt",
        "/log-fields/__next._tree.txt",
        "/services/__next._tree.txt",
    ] {
        assert_eq!(
            send_asset(path, "anonymous").status,
            303,
            "{path} anonymous"
        );
        assert_eq!(send_asset(path, "analyst").status, 403, "{path} analyst");
        assert_eq!(send_asset(path, "no_pii").status, 403, "{path} no_pii");
        let admin = send_asset(path, "admin");
        assert_eq!(admin.status, 200, "{path} admin");
        assert_eq!(
            admin.headers.get("cache-control").map(String::as_str),
            Some("private, no-store"),
            "{path} cache-control"
        );
    }

    assert_eq!(
        h.send("GET", "/api/logs/cron_ticks/live", "anonymous")
            .status,
        401
    );
    assert_eq!(
        h.send("GET", "/api/logs/cron_ticks/live", "analyst").status,
        403
    );
    assert_eq!(
        h.send("GET", "/api/logs/cron_ticks/live", "admin").status,
        200
    );
}

#[test]
fn logout_clears_all_three_cookies_and_revokes_session_across_protected_routes() {
    let h = Harness::new();
    assert_eq!(h.send("GET", "/api/services", "analyst").status, 200);

    let logout = h.send("POST", "/api/auth/logout", "analyst");
    assert_eq!(logout.status, 200);
    let cookies = logout.cookies();
    assert!(
        cookies
            .iter()
            .any(|c| c.starts_with("flp_session=;") && c.contains("Max-Age=0")),
        "expected flp_session clear cookie: {cookies:?}"
    );
    assert!(
        cookies
            .iter()
            .any(|c| c.starts_with("flp_cdn=;") && c.contains("Max-Age=0")),
        "expected flp_cdn clear cookie: {cookies:?}"
    );
    assert!(
        cookies
            .iter()
            .any(|c| c.starts_with("__Host-flp_cdn=;") && c.contains("Max-Age=0")),
        "expected __Host-flp_cdn clear cookie: {cookies:?}"
    );

    let replay = h.send("GET", "/api/services", "analyst");
    assert_eq!(replay.status, 401);
    assert_eq!(
        replay.headers.get("x-flp-auth-fail").map(String::as_str),
        Some("1")
    );

    let session_probe = h.send("GET", "/api/auth/session", "analyst");
    assert_eq!(session_probe.status, 200);
    let body: Value = serde_json::from_slice(&session_probe.body).unwrap();
    assert_eq!(body["user"], Value::Null);
    assert_eq!(session_probe.cookies().len(), 3);
}

#[test]
fn admin_user_revoke_and_role_change_advance_front_auth_epoch_and_reissue_v2_cdn_cookies() {
    let h = Harness::new();
    let sid22 = "AAAAAAAAAAAAAAAAAAAAAA";
    let mut svc_val = h
        .platform
        .adapter
        .get("svc:fixtureService")
        .unwrap()
        .unwrap()
        .value;
    svc_val["service_id"] = json!(sid22);
    assert!(h
        .platform
        .adapter
        .cas(&format!("svc:{sid22}"), None, &svc_val)
        .unwrap());
    let install_rec = h.platform.adapter.get("admin:install").unwrap().unwrap();
    let mut install_val = install_rec.value;
    install_val["service_id"] = json!(sid22);
    assert!(h
        .platform
        .adapter
        .cas("admin:install", Some(&install_rec.generation), &install_val)
        .unwrap());

    let files0 = h.send("GET", "/api/log-extents", "analyst");
    assert_eq!(files0.status, 200);
    let v1_cookie = files0
        .cookies()
        .iter()
        .find(|c| c.starts_with("flp_cdn="))
        .expect("initial flp_cdn cookie")
        .clone();
    let v1_kv = v1_cookie.split(';').next().unwrap().to_owned();
    let v1_val = v1_kv.strip_prefix("flp_cdn=").unwrap();
    assert_eq!(
        v1_val.split('~').count(),
        6,
        "epoch 0 cookie must use 5-field payload + sig v1 format: {v1_val}"
    );

    let revoke = h.send("POST", "/api/admin/users/u-victim/revoke", "admin");
    assert_eq!(revoke.status, 200);
    assert_eq!(
        h.platform
            .adapter
            .dictionary_items("fixtureAuth")
            .get("auth_epoch")
            .map(String::as_str),
        Some("1")
    );
    assert_eq!(h.store.load().unwrap().state.auth_epoch, 1);

    let combined_cookie = format!("{}; {v1_kv}", h.sessions["analyst"].as_ref().unwrap());
    let files1 = h.send_with_headers(
        "GET",
        "/api/log-extents",
        "anonymous",
        None,
        &[],
        &[("cookie", &combined_cookie)],
    );
    assert_eq!(files1.status, 200);
    let v2_cookie = files1
        .cookies()
        .iter()
        .find(|c| c.starts_with("flp_cdn="))
        .expect("epoch-bumped request must reissue v2 flp_cdn cookie");
    let v2_val = v2_cookie
        .split(';')
        .next()
        .unwrap()
        .strip_prefix("flp_cdn=")
        .unwrap();
    let v2_fields: Vec<&str> = v2_val.split('~').collect();
    assert_eq!(
        v2_fields.len(),
        7,
        "epoch > 0 cookie must use 6-field payload + sig v2 format: {v2_val}"
    );
    assert_eq!(v2_fields[1], "analyst");
    assert_eq!(v2_fields[4], "1");

    let downgrade = h.send_with(
        "PATCH",
        "/api/admin/users/u-analyst",
        "admin",
        Some("https://logs.example.com"),
        br#"{"role":"analyst-no-pii"}"#,
    );
    assert_eq!(downgrade.status, 200);
    assert_eq!(
        h.platform
            .adapter
            .dictionary_items("fixtureAuth")
            .get("auth_epoch")
            .map(String::as_str),
        Some("2")
    );
    assert_eq!(h.store.load().unwrap().state.auth_epoch, 2);

    let file_denied = h.send("GET", "/api/file?key=processed/logs/x.parquet", "analyst");
    assert_eq!(file_denied.status, 403);
}

#[test]
fn host_flp_cdn_cookie_issued_with_precedence_and_malformed_or_cookieless_auth_has_no_fail_header()
{
    let h = Harness::new();
    let sid22 = "AAAAAAAAAAAAAAAAAAAAAA";
    let mut svc_val = h
        .platform
        .adapter
        .get("svc:fixtureService")
        .unwrap()
        .unwrap()
        .value;
    svc_val["service_id"] = json!(sid22);
    assert!(h
        .platform
        .adapter
        .cas(&format!("svc:{sid22}"), None, &svc_val)
        .unwrap());
    let install_rec = h.platform.adapter.get("admin:install").unwrap().unwrap();
    let mut install_val = install_rec.value;
    install_val["service_id"] = json!(sid22);
    assert!(h
        .platform
        .adapter
        .cas("admin:install", Some(&install_rec.generation), &install_val)
        .unwrap());

    let res = h.send("GET", "/api/log-extents", "analyst");
    assert_eq!(res.status, 200);
    let flp_cdn = res
        .cookies()
        .iter()
        .find(|c| c.starts_with("flp_cdn="))
        .expect("flp_cdn Set-Cookie")
        .clone();
    let host_cdn = res
        .cookies()
        .iter()
        .find(|c| c.starts_with("__Host-flp_cdn="))
        .expect("__Host-flp_cdn Set-Cookie on HTTPS origin")
        .clone();
    let host_kv = host_cdn.split(';').next().unwrap();
    assert_eq!(
        flp_cdn.split(';').next().unwrap().strip_prefix("flp_cdn="),
        host_kv.strip_prefix("__Host-flp_cdn=")
    );

    // Tossed bad flp_cdn alongside valid __Host-flp_cdn is ignored because __Host-flp_cdn wins
    let tossed = format!(
        "{}; flp_cdn=forged-subdomain-tossed; {host_kv}",
        h.sessions["analyst"].as_ref().unwrap()
    );
    let res2 = h.send_with_headers(
        "GET",
        "/api/log-extents",
        "anonymous",
        None,
        &[],
        &[("cookie", &tossed)],
    );
    assert_eq!(res2.status, 200);
    assert!(
        res2.cookies().is_empty(),
        "valid __Host-flp_cdn must take precedence over tossed flp_cdn"
    );

    // 422 malformed JSON on auth route must NOT emit x-flp-auth-fail
    let bad_json = h.send_with(
        "POST",
        "/api/auth/login/verify",
        "anonymous",
        Some("https://logs.example.com"),
        b"{not-json",
    );
    assert_eq!(bad_json.status, 422);
    assert!(!bad_json.headers.contains_key("x-flp-auth-fail"));

    // Cookie-less and invite-less register/options must NOT emit x-flp-auth-fail
    let cookieless_reg = h.send_with(
        "POST",
        "/api/auth/register/options",
        "anonymous",
        Some("https://logs.example.com"),
        br#"{"display_name":"Probe"}"#,
    );
    assert_eq!(cookieless_reg.status, 401);
    assert!(!cookieless_reg.headers.contains_key("x-flp-auth-fail"));

    // Invalid invite_token on register/options MUST still emit x-flp-auth-fail
    let bad_invite_reg = h.send_with(
        "POST",
        "/api/auth/register/options",
        "anonymous",
        Some("https://logs.example.com"),
        br#"{"display_name":"Probe","invite_token":"invalid-invite-token"}"#,
    );
    assert_eq!(bad_invite_reg.status, 401);
    assert_eq!(
        bad_invite_reg
            .headers
            .get("x-flp-auth-fail")
            .map(String::as_str),
        Some("1")
    );
}

#[test]
fn sec19_route_level_saved_views_blocks_all_nine_bypasses_and_allows_non_pii_pills() {
    let h = Harness::new();
    let pii_bypasses = [
        r#"{"clauses":[{"col":"c_ip","op":"=","value":"198.51.100.7"}]}"#,
        r#"{"filters":[{"name":"client_ip","op":"=","value":"198.51.100.7"}]}"#,
        r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE (c_ip) = '198.51.100.7'"}}"#,
        r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE '198.51.100.7' = client_ip"}}"#,
        r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE lower(x_forwarded_for) = '198.51.100.7'"}}"#,
        "{\"_view_extras\":{\"sql\":\"SELECT * FROM logs WHERE \\\"c_ip\\\" IN\\n('198.51.100.7')\"}}",
        r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE [oip] = '198.51.100.7'"}}"#,
        r#"{"c-ip":"198.51.100.7"}"#,
        r#"{"session-id":"sess_secret_99"}"#,
    ];
    for (idx, filters_json) in pii_bypasses.iter().enumerate() {
        let name = format!("pii-{idx}");
        let body = serde_json::to_vec(&json!({
            "service_id": "fixtureService",
            "name": name,
            "filters_json": *filters_json,
        }))
        .unwrap();
        let post = h.send_with(
            "POST",
            "/api/views",
            "admin",
            Some("https://logs.example.com"),
            &body,
        );
        assert_eq!(post.status, 201);

        let admin_res = h.send("GET", "/api/views/fixtureService", "admin");
        assert_eq!(admin_res.status, 200);
        let admin_list: Value = serde_json::from_slice(&admin_res.body).unwrap();
        assert_eq!(admin_list[0]["name"], name);

        let nopii_res = h.send("GET", "/api/views/fixtureService", "no_pii");
        assert_eq!(nopii_res.status, 200);
        let nopii_list: Value = serde_json::from_slice(&nopii_res.body).unwrap();
        assert!(
            nopii_list.as_array().unwrap().is_empty(),
            "expected AnalystNoPii to hide PII bypass #{idx}: {filters_json}"
        );
    }
    let benign_views = [
        r#"{"url":{"mode":"include","values":["/api/ip:status"]}}"#,
        r#"{"clauses":[{"col":"path","op":"=","value":"/ip=1"}]}"#,
        r#"{"_view_extras":{"sql":"SELECT status FROM logs WHERE path = '/api/ip:status'"}}"#,
    ];
    for (idx, filters_json) in benign_views.iter().enumerate() {
        let name = format!("safe-{idx}");
        let body = serde_json::to_vec(&json!({
            "service_id": "fixtureService",
            "name": name,
            "filters_json": *filters_json,
        }))
        .unwrap();
        let post = h.send_with(
            "POST",
            "/api/views",
            "admin",
            Some("https://logs.example.com"),
            &body,
        );
        assert_eq!(post.status, 201);

        let nopii_res = h.send("GET", "/api/views/fixtureService", "no_pii");
        assert_eq!(nopii_res.status, 200);
        let nopii_list: Value = serde_json::from_slice(&nopii_res.body).unwrap();
        assert_eq!(
            nopii_list.as_array().unwrap().len(),
            1,
            "expected AnalystNoPii to see benign FilterPill/SQL #{idx}: {filters_json}"
        );
        assert_eq!(nopii_list[0]["name"], name);
    }
}

#[test]
fn sec19_route_level_wave2_redteam_and_structural_bypasses_blocked_while_analyst_sees_all() {
    let h = Harness::new();
    let pii_vectors = [
        (
            "redteam-1",
            r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE /*x*/client_ip = '198.51.100.7'"}}"#,
        ),
        (
            "redteam-2",
            "{\"_view_extras\":{\"sql\":\"-- Admin's investigation\\nSELECT * FROM logs WHERE client_ip = '198.51.100.7'\"}}",
        ),
        (
            "redteam-3",
            r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE /* ' */ client_ip = '198.51.100.7' -- '"}}"#,
        ),
        (
            "redteam-4",
            r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE list_contains([client_ip, oip], '198.51.100.7')"}}"#,
        ),
        (
            "redteam-5",
            r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE [c_ip = '198.51.100.7'][1]"}}"#,
        ),
        (
            "redteam-6",
            r#"{"where":{"value":"client_ip = '198.51.100.7'"}}"#,
        ),
        (
            "redteam-7",
            r#"{"_view_extras":{"sql":{"value":"SELECT * FROM logs WHERE sid = 'sess_secret_99'"}}}"#,
        ),
        (
            "redteam-8",
            r#"{"_view_extras":{"sql":"SELECT 1/sid FROM logs WHERE 1=1"}}"#,
        ),
        (
            "redteam-9",
            r#"{"_view_extras":{"sql":"SELECT status AS \"Admin's status\" FROM logs WHERE client_ip = '198.51.100.7'"}}"#,
        ),
        (
            "redteam-10",
            r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE path = $$Admin's path$$ AND client_ip = '198.51.100.7'"}}"#,
        ),
        (
            "redteam-11",
            r#"{"filters":[{"name":"suspicious_traffic","value":"SELECT * FROM logs WHERE client_ip = '198.51.100.7'"}]}"#,
        ),
        (
            "struct-s1",
            r#"{"filters":[{"column":"status","op":"= 200 OR client_ip = '198.51.100.7'","value":"200"}]}"#,
        ),
        (
            "struct-s2",
            r#"{"filters":[{"column":"status","sql":"SELECT 1","value":"client_ip = '198.51.100.7'"}]}"#,
        ),
        (
            "struct-s3",
            r#"{"filters":[{"column":"where","value":"client_ip = '198.51.100.7'"}]}"#,
        ),
        (
            "struct-s4",
            r#"{"column":"status","values":[{"value":"SELECT * FROM logs WHERE client_ip = '198.51.100.7'"}]}"#,
        ),
        (
            "struct-s5",
            r#"{"filters":[{"field":"custom_fields['oip']","value":"198.51.100.7"}]}"#,
        ),
        (
            "struct-s6",
            r#"{"_view_extras":{"sql":"\"SELECT * FROM logs WHERE \\u0063lient_ip = '198.51.100.7'\""}}"#,
        ),
        (
            "struct-s7",
            r#"{"where_1":{"value":"client_ip = '198.51.100.7'"}}"#,
        ),
    ];

    let benign_vectors = [
        (
            "benign-ui-pill",
            r#"{"filters":[{"id":"url-ip-0","column":"url","mode":"include","value":"Don't /ip"}],"_view_extras":{"edgeOnly":true,"relativeRange":"24h"}}"#,
        ),
        (
            "benign-4xx-catalog",
            r#"{"4xx":{"mode":"include","values":["1"]},"5xx":{"mode":"exclude","values":["0"]}}"#,
        ),
        (
            "benign-sql-quotes-comments",
            "{\"_view_extras\":{\"sql\":\"-- Today's run\\nSELECT arr[1], status FROM logs WHERE path = 'C:\\\\ip\\\\sid\\\\' AND pop = $$user's$$\"}}",
        ),
    ];

    for (id, filters_json) in pii_vectors.iter().chain(benign_vectors.iter()) {
        let body = serde_json::to_vec(&json!({
            "id": *id,
            "service_id": "fixtureService",
            "name": *id,
            "filters_json": *filters_json,
        }))
        .unwrap();
        let post = h.send_with(
            "POST",
            "/api/views",
            "admin",
            Some("https://logs.example.com"),
            &body,
        );
        assert_eq!(post.status, 201, "Admin POST failed for {id}");
    }

    let total_expected = pii_vectors.len() + benign_vectors.len();

    for full_role in ["admin", "analyst"] {
        let res = h.send("GET", "/api/views/fixtureService", full_role);
        assert_eq!(res.status, 200);
        let list: Value = serde_json::from_slice(&res.body).unwrap();
        assert_eq!(
            list.as_array().unwrap().len(),
            total_expected,
            "{full_role} must see all {total_expected} saved views"
        );
    }

    let nopii_res = h.send("GET", "/api/views/fixtureService", "no_pii");
    assert_eq!(nopii_res.status, 200);
    let nopii_raw = String::from_utf8_lossy(&nopii_res.body);
    assert!(
        !nopii_raw.contains("198.51.100.7") && !nopii_raw.contains("sess_secret_99"),
        "AnalystNoPii leaked PII sentinel literal: {nopii_raw}"
    );
    let nopii_list: Value = serde_json::from_slice(&nopii_res.body).unwrap();
    let nopii_names: Vec<&str> = nopii_list
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["name"].as_str().unwrap())
        .collect();
    let expected_benign: Vec<&str> = benign_vectors.iter().map(|(id, _)| *id).collect();
    assert_eq!(
        nopii_names, expected_benign,
        "AnalystNoPii must see strictly the benign views and zero PII views"
    );
}

#[test]
fn sec19_route_level_wave3_dupkeys_sticky_slots_columns_and_json_paths_blocked() {
    let h = Harness::new();
    let pii_vectors = [
        (
            "rt3-dupkey-view-extras",
            r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE client_ip = '198.51.100.7'"},"_view_extras":{"edgeOnly":true}}"#,
        ),
        (
            "rt3-dupkey-pill-column",
            r#"{"filters":[{"column":"client_ip","mode":"include","value":"198.51.100.7","column":"url"}]}"#,
        ),
        (
            "rt3-dupkey-case-colliding-column",
            r#"{"filters":[{"column":"client_ip","mode":"include","value":"198.51.100.7","Column":"url"}]}"#,
        ),
        (
            "rt3-dupkey-nested-json-str",
            r#"{"filters":"[{\"column\":\"client_ip\",\"mode\":\"include\",\"value\":\"198.51.100.7\",\"column\":\"url\"}]"}"#,
        ),
        (
            "rt3-sticky-escape-name-map",
            r#"{"_view_extras":{"name":{"url":{"mode":"include","values":["SELECT * FROM logs WHERE client_ip = '198.51.100.7'"]}}}}"#,
        ),
        (
            "rt3-sticky-escape-key-pill",
            r#"{"_view_extras":{"key":{"column":"url","mode":"include","value":"SELECT * FROM logs WHERE sid = 'sess_secret_99'"}}}"#,
        ),
        (
            "rt3-sticky-escape-where-lhs",
            r#"{"where":{"lhs":{"status":{"mode":"include","values":["client_ip = '198.51.100.7'"]}}}}"#,
        ),
        (
            "rt3-sticky-escape-left-json-str",
            r#"{"_view_extras":{"left":"[{\"column\":\"status\",\"mode\":\"include\",\"value\":\"SELECT * FROM logs WHERE client_ip = '198.51.100.7'\"}]"}}"#,
        ),
        (
            "rt3-selector-nonscalar-root",
            r#"{"lhs":{"url":{"mode":"include","values":["SELECT * FROM logs WHERE client_ip = '198.51.100.7'"]}}}"#,
        ),
        (
            "rt3-duckdb-columns-exact",
            r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE COLUMNS('client_ip') = '198.51.100.7'"}}"#,
        ),
        (
            "rt3-duckdb-columns-regex",
            r#"{"_view_extras":{"sql":"SELECT COLUMNS('^(c_ip|oip|sid)$') FROM logs"}}"#,
        ),
        (
            "rt3-duckdb-columns-wildcard",
            r#"{"_view_extras":{"sql":"SELECT COLUMNS('.*') FROM logs"}}"#,
        ),
        (
            "rt3-duckdb-columns-lambda",
            r#"{"_view_extras":{"sql":"SELECT COLUMNS(c -> c = 'client_ip') FROM logs"}}"#,
        ),
        (
            "rt3-duckdb-json-pointer-arrow",
            r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE _extra->>'/client_ip' = '198.51.100.7'"}}"#,
        ),
        (
            "rt3-duckdb-json-pointer-fn",
            r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_extract_string(_extra, '/headers/x-forwarded-for') = '198.51.100.7'"}}"#,
        ),
        (
            "rt3-duckdb-json-arrow-path-list",
            r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE _extra->>['headers', 'client_ip'] = '198.51.100.7'"}}"#,
        ),
        (
            "rt3-duckdb-paren-subscript-key",
            r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE _extra[('client_ip')] = '198.51.100.7'"}}"#,
        ),
        (
            "rt3-duckdb-paren-arrow-key",
            r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE _extra->>('sid') = 'sess_secret_99'"}}"#,
        ),
    ];

    let benign_vectors = [
        (
            "benign-sql-columns-nonpii",
            r#"{"_view_extras":{"sql":"SELECT COLUMNS('^(status|url|pop)$') FROM logs"}}"#,
        ),
        (
            "benign-sql-json-pointer-nonpii",
            r#"{"_view_extras":{"sql":"SELECT _extra->>'/request/url' FROM logs WHERE json_extract_string(_extra, '/meta/pop') = '/ip/sid'"}}"#,
        ),
        (
            "benign-sql-in-list-literals",
            r#"{"_view_extras":{"sql":"SELECT status FROM logs WHERE status IN ('ip', 'sid')"}}"#,
        ),
        (
            "benign-ui-pill-4xx-5xx",
            r#"{"4xx":{"mode":"include","values":["1"]},"5xx":{"mode":"exclude","values":["0"]},"filters":[{"id":"url-ip-0","column":"url","mode":"include","value":"[ERROR] /api/ip/sid"}]}"#,
        ),
    ];

    for (id, filters_json) in pii_vectors.iter().chain(benign_vectors.iter()) {
        let body = serde_json::to_vec(&json!({
            "id": *id,
            "service_id": "fixtureService",
            "name": *id,
            "filters_json": *filters_json,
        }))
        .unwrap();
        let post = h.send_with(
            "POST",
            "/api/views",
            "admin",
            Some("https://logs.example.com"),
            &body,
        );
        assert_eq!(post.status, 201, "Admin POST failed for {id}");
    }

    let total_expected = pii_vectors.len() + benign_vectors.len();

    for full_role in ["admin", "analyst"] {
        let res = h.send("GET", "/api/views/fixtureService", full_role);
        assert_eq!(res.status, 200);
        let list: Value = serde_json::from_slice(&res.body).unwrap();
        assert_eq!(
            list.as_array().unwrap().len(),
            total_expected,
            "{full_role} must see all {total_expected} saved views"
        );
    }

    let nopii_res = h.send("GET", "/api/views/fixtureService", "no_pii");
    assert_eq!(nopii_res.status, 200);
    let nopii_raw = String::from_utf8_lossy(&nopii_res.body);
    assert!(
        !nopii_raw.contains("198.51.100.7")
            && !nopii_raw.contains("sess_secret_99")
            && !nopii_raw.contains("client_ip")
            && !nopii_raw.contains("x-forwarded-for"),
        "AnalystNoPii leaked PII sentinel or column literal: {nopii_raw}"
    );
    let nopii_list: Value = serde_json::from_slice(&nopii_res.body).unwrap();
    let nopii_names: Vec<&str> = nopii_list
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["name"].as_str().unwrap())
        .collect();
    let expected_benign: Vec<&str> = benign_vectors.iter().map(|(id, _)| *id).collect();
    assert_eq!(
        nopii_names, expected_benign,
        "AnalystNoPii must see strictly the benign views and zero PII views"
    );
}

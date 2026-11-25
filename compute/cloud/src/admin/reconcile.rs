use super::*;
use std::collections::{BTreeMap, BTreeSet};

pub fn managed_name(name: &str) -> bool {
    name.starts_with("Fastly Log Analytics")
        || name.starts_with("Fastly Log Analysis")
        || super::scoring::is_scoring_snippet_name(name)
        || matches!(
            name,
            "Fastly Object Storage Logs"
                | "Fastly RUM Logs"
                | "Fastly RUM Object Storage Logs"
                | "log_analytics_condition"
                | "rum_log_condition"
                | "fastly_log_analytics_false"
                | "fos_origin"
                | "fos_credentials"
                | "session_scorer"
        )
        || name.starts_with("flp-owner-")
}
/// Whether the active version holds another install's capture VCL for attach mode to log.
pub fn capture_present(snippets: &[Value]) -> bool {
    let has = |name: &str| snippets.iter().any(|v| v["name"] == name);
    has("Fastly Log Analytics - vcl_recv") && has("Fastly Log Analytics - vcl_deliver")
        || has("Fastly Log Analysis Capture")
}

pub(super) fn endpoint_path_matches_scope(path: &str, scope: &str) -> bool {
    if path.contains("..") || path.contains('\\') || path.chars().any(char::is_control) {
        return false;
    }
    if scope.is_empty() {
        path.is_empty() || path.starts_with("raw/")
    } else {
        path.starts_with(&format!("{scope}raw/"))
    }
}

pub(super) fn has_attach_endpoint_marker(fos_items: &[Value]) -> bool {
    fos_items.iter().any(|v| {
        matches!(
            v["name"].as_str(),
            Some(ATTACH_ENDPOINT | ATTACH_RUM_ENDPOINT)
        )
    })
}

pub(super) fn has_attach_condition_marker(conditions: &[Value]) -> bool {
    conditions.iter().any(|v| {
        matches!(
            v["name"].as_str(),
            Some(ATTACH_CONDITION | ATTACH_RUM_CONDITION)
        )
    })
}

fn primary_endpoint_expected_condition(name: &str, cond: &str) -> Option<&'static str> {
    match name {
        "Fastly Object Storage Logs" | "Fastly Log Analytics" => Some("log_analytics_condition"),
        "Fastly RUM Logs" | "Fastly RUM Object Storage Logs" => Some("rum_log_condition"),
        _ => match cond {
            "log_analytics_condition" => Some("log_analytics_condition"),
            "rum_log_condition" => Some("rum_log_condition"),
            _ => None,
        },
    }
}

pub(super) fn has_foreign_primary_endpoint(
    service: &Service,
    scope: &str,
    fos_items: &[Value],
) -> bool {
    for item in fos_items {
        let name = item["name"].as_str().unwrap_or("");
        let cond = item["response_condition"].as_str().unwrap_or("");
        let Some(expected_cond) = primary_endpoint_expected_condition(name, cond) else {
            continue;
        };
        let path = item["path"].as_str().unwrap_or("");
        if item["bucket_name"].as_str() != Some(service.fos_bucket.as_str())
            || !endpoint_path_matches_scope(path, scope)
            || cond != expected_cond
        {
            return true;
        }
    }
    false
}

pub(super) fn verify_managed_customer_vcl_ownership(
    context: &Context<'_, impl Runtime>,
    service: &Service,
    token: &str,
) -> Result<u64, Error> {
    if service.capture == Capture::Attached {
        return Err(Error::new(409, "managed_resources_not_owned"));
    }
    let version = active_version(context, &service.service_id, token)?;
    let mut current = BTreeMap::new();
    for resource in ["snippet", "logging/fos", "condition"] {
        let value = context.api(
            "GET",
            &format!(
                "/service/{}/version/{version}/{resource}",
                service.service_id
            ),
            token,
            None,
        )?;
        current.insert(resource.to_owned(), list(&value)?.clone());
    }
    let snippets = &current["snippet"];
    if snippets.iter().any(|v| {
        v["name"]
            .as_str()
            .is_some_and(|s| s.starts_with("flp-owner-") && s != marker(service))
    }) {
        return Err(Error::new(409, "managed_resources_not_owned"));
    }
    let own_marker = snippets.iter().any(|v| {
        v["name"] == marker(service)
            && v["content"].as_str().or_else(|| v["body"].as_str())
                == Some(&format!("# {}", resource_owner(service)))
    });
    let scope = crate::ingest::scope(service);
    if !own_marker
        || has_attach_condition_marker(&current["condition"])
        || has_attach_endpoint_marker(&current["logging/fos"])
        || (capture_present(snippets)
            && has_foreign_primary_endpoint(service, &scope, &current["logging/fos"]))
    {
        return Err(Error::new(409, "managed_resources_not_owned"));
    }
    Ok(version)
}

fn verified_cloud_fos_endpoint(
    found: &Value,
    expected_cond: &str,
    service: &Service,
    scope: &str,
) -> bool {
    !expected_cond.is_empty()
        && found["bucket_name"].as_str() == Some(service.fos_bucket.as_str())
        && endpoint_path_matches_scope(found["path"].as_str().unwrap_or(""), scope)
        && found["response_condition"].as_str() == Some(expected_cond)
}
/// Attach mode logs exactly the requests the other install's request-log endpoint
/// logs, so both installs' data can be compared: its `log_analytics_condition`
/// verbatim, or `fallback` (Cloud's own statement) minus RUM beacons when absent.
pub fn attach_condition_statement(conditions: &[Value], fallback: &str) -> Result<String, Error> {
    let Some(existing) = conditions
        .iter()
        .find(|v| v["name"] == "log_analytics_condition")
    else {
        const BEACON: &str = "req.url.path != \"/rum-beacon\"";
        return Ok(match fallback.split_once(" && ") {
            Some((first, rest)) => format!("{first} && {BEACON} && {rest}"),
            None => format!("{fallback} && {BEACON}"),
        });
    };
    match (existing["statement"].as_str(), existing["type"].as_str()) {
        (Some(statement), Some(kind))
            if !statement.trim().is_empty() && kind.eq_ignore_ascii_case("RESPONSE") =>
        {
            Ok(statement.to_owned())
        }
        _ => Err(Error::new(409, "capture_condition_unreadable")),
    }
}
pub fn attach_rum_condition_statement(
    conditions: &[Value],
    fallback: &str,
) -> Result<String, Error> {
    let Some(existing) = conditions.iter().find(|v| v["name"] == "rum_log_condition") else {
        return Ok(fallback.to_owned());
    };
    match (existing["statement"].as_str(), existing["type"].as_str()) {
        (Some(statement), Some(kind))
            if !statement.trim().is_empty() && kind.eq_ignore_ascii_case("RESPONSE") =>
        {
            Ok(statement.to_owned())
        }
        _ => Err(Error::new(409, "capture_condition_unreadable")),
    }
}
pub fn active_version(
    context: &Context<'_, impl Runtime>,
    sid: &str,
    token: &str,
) -> Result<u64, Error> {
    let versions = context.api("GET", &format!("/service/{sid}/version"), token, None)?;
    list(&versions)?
        .iter()
        .find(|v| v["active"].as_bool() == Some(true))
        .and_then(|v| v["number"].as_u64())
        .ok_or_else(|| Error::new(409, "active_version_required"))
}
fn marker(service: &Service) -> String {
    format!("flp-owner-{}", service.owner)
}

fn desired(
    _context: &Context<'_, impl Runtime>,
    service: &Service,
) -> Result<BTreeMap<String, Vec<Value>>, Error> {
    let state = serialize(service)?;
    let mut snippets = list(
        &crate::logging::desired_snippets(&state)
            .map_err(|_| Error::new(400, "invalid_snippets"))?,
    )?
    .clone();
    if service.cmcd.enabled {
        for snippet in &mut snippets {
            if snippet["subroutine"] == "vcl_recv" {
                let body = snippet["body"].as_str().unwrap_or("");
                snippet["body"] = body
                    .replace(
                        "  # Section 3: CMCD Extraction",
                        "  unset req.http.x-cmcd;\n  # Section 3: CMCD Extraction",
                    )
                    .into();
            }
        }
    }
    let capture = crate::logging::logging_service_snippets(&state)
        .map_err(|_| Error::new(400, "invalid_snippets"))?;
    for snippet in list(&capture)?
        .iter()
        .filter(|v| v["name"] == "Fastly Log Analysis Reset Client IP")
    {
        snippets.push(snippet.clone());
    }
    snippets.push(json!({"name":marker(service),"subroutine":"vcl_recv","priority":0,"body":format!("# {}",resource_owner(service))}));
    let mut endpoints = list(
        &crate::logging::desired_logging_endpoints(&state)
            .map_err(|_| Error::new(400, "invalid_log_format"))?,
    )?
    .clone();
    if !service.fos_prefix.is_empty() {
        for endpoint in &mut endpoints {
            endpoint["path"] = format!(
                "{}/{}",
                service.fos_prefix.trim_end_matches('/'),
                endpoint["path"].as_str().unwrap_or("")
            )
            .into();
        }
    }
    let mut conditions = list(
        &crate::logging::desired_conditions(&state)
            .map_err(|_| Error::new(400, "invalid_logging_condition"))?,
    )?
    .clone();
    let host = &service.fos_endpoint;
    let trimmed_shield = service.cdn_shield.trim();
    let shield = if trimmed_shield.is_empty() {
        crate::logging::rum::shield_for_region(&service.fos_region)
    } else if trimmed_shield.eq_ignore_ascii_case("none") {
        ""
    } else {
        crate::logging::rum::validate_shield_pop(trimmed_shield)
            .map_err(|_| Error::new(400, "invalid_snippets"))?
    };
    let mut backends = if service.rum.enabled && service.capture != Capture::Attached {
        vec![json!({
            "name": "fos_origin",
            "address": host,
            "override_host": host,
            "ssl_sni_hostname": host,
            "ssl_cert_hostname": host,
            "port": 443,
            "use_ssl": true,
            "ssl_check_cert": true,
            "prefer_ipv6": false,
            "shield": shield,
            "request_condition": "fastly_log_analytics_false",
        })]
    } else {
        vec![]
    };
    let mut dictionaries = if service.rum.enabled && service.capture != Capture::Attached {
        vec![json!({
            "name": "fos_credentials",
            "write_only": true,
        })]
    } else {
        vec![]
    };
    if let Some(sc) = service.scoring.as_ref().filter(|s| s.enabled) {
        if service.capture != Capture::Attached {
            let scoring_snippets = super::scoring::desired_scoring_snippets(
                &service.service_id,
                super::scoring::REQUEST_SECRET_PLACEHOLDER,
                Some(&sc.exclude_url_regex),
                sc.enforce_status_code,
            )?;
            snippets.extend(scoring_snippets);
            backends.push(super::scoring::checked_scoring_backend(&sc.scoring_domain)?);
            if !conditions
                .iter()
                .any(|c| c["name"] == "fastly_log_analytics_false")
            {
                conditions.push(json!({
                    "name": "fastly_log_analytics_false",
                    "type": "REQUEST",
                    "priority": 10,
                    "statement": "false",
                }));
            }
        }
    }
    if service.capture == Capture::Attached {
        snippets.retain(|snippet| snippet["name"] == marker(service));
        backends.clear();
        dictionaries.clear();
        for endpoint in &mut endpoints {
            if endpoint["response_condition"] == "rum_log_condition" {
                endpoint["name"] = ATTACH_RUM_ENDPOINT.into();
                endpoint["response_condition"] = ATTACH_RUM_CONDITION.into();
            } else {
                endpoint["response_condition"] = ATTACH_CONDITION.into();
            }
        }
        for condition in &mut conditions {
            if condition["name"] == "log_analytics_condition" {
                condition["name"] = ATTACH_CONDITION.into();
            } else if condition["name"] == "rum_log_condition" {
                condition["name"] = ATTACH_RUM_CONDITION.into();
            }
        }
        conditions.retain(|condition| {
            matches!(
                condition["name"].as_str(),
                Some(ATTACH_CONDITION | ATTACH_RUM_CONDITION)
            )
        });
    }
    Ok(BTreeMap::from([
        ("snippet".into(), snippets),
        ("logging/fos".into(), endpoints),
        ("condition".into(), conditions),
        ("backend".into(), backends),
        ("dictionary".into(), dictionaries),
    ]))
}

fn normalized(resource: &str, value: &Value) -> Value {
    match resource {
        "snippet" => json!({
            "name":value["name"],
            "type":value["type"].as_str().unwrap_or_else(||value["subroutine"].as_str().unwrap_or("").trim_start_matches("vcl_")),
            "priority":value["priority"].as_i64().or_else(||value["priority"].as_str().and_then(|v|v.parse().ok())).unwrap_or(10),
            "content":value["content"].as_str().or_else(||value["body"].as_str()).unwrap_or(""),
            "dynamic":number(&value["dynamic"],0),
        }),
        "logging/fos" => {
            let codec = value["compression_codec"]
                .as_str()
                .filter(|s| !s.is_empty())
                .unwrap_or("zstd");
            json!({
                "name":value["name"],"path":value["path"],"period":number(&value["period"],60),"format":value["format"],
                "response_condition":value["response_condition"],"format_version":number(&value["format_version"],2),
                "bucket_name":value["bucket_name"],"region":value["region"],"message_type":value["message_type"],
                "compression_codec":codec,
            })
        }
        "condition" => {
            json!({"name":value["name"],"type":value["type"],"priority":number(&value["priority"],10),"statement":value["statement"]})
        }
        "backend" => json!({
            "name":value["name"],
            "address":value["address"],
            "override_host":value["override_host"],
            "ssl_sni_hostname":value["ssl_sni_hostname"],
            "ssl_cert_hostname":value["ssl_cert_hostname"],
            "port":number(&value["port"],443),
            "use_ssl":boolean(&value["use_ssl"],true),
            "ssl_check_cert":boolean(&value["ssl_check_cert"],true),
            "prefer_ipv6":boolean(&value["prefer_ipv6"],false),
            "shield":value["shield"],
            "request_condition":value["request_condition"],
        }),
        "dictionary" => json!({
            "name":value["name"],
            "write_only":boolean(&value["write_only"],true),
        }),
        _ => value.clone(),
    }
}
fn number(value: &Value, default: u64) -> u64 {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or(default)
}
fn boolean(value: &Value, default: bool) -> bool {
    value
        .as_bool()
        .or_else(|| value.as_u64().map(|n| n != 0))
        .or_else(|| {
            value
                .as_str()
                .map(|s| s == "1" || s.eq_ignore_ascii_case("true"))
        })
        .unwrap_or(default)
}

fn resource_body(resource: &str, value: &Value, service: &Service) -> Value {
    match resource {
        "snippet" => normalized(resource, value),
        "logging/fos" => {
            let mut body = crate::logging::fos_logging_endpoint_body(
                value,
                &service.fos_bucket,
                &service.fos_region,
            );
            body["message_type"] = "blank".into();
            body
        }
        // Fastly stores "0" when no priority is sent, which would never match the
        // compared default of 10 and re-plan every condition on each reconcile.
        "condition" => {
            let mut body = value.clone();
            if body["priority"].is_null() {
                body["priority"] = "10".into();
            }
            body
        }
        _ => value.clone(),
    }
}

pub fn run(
    context: &Context<'_, impl Runtime>,
    job: &mut Job,
    service: &Service,
    token: &str,
) -> Result<(), Error> {
    match job.step.as_str() {
        "discover" => discover(context, job, service, token),
        "clone" => clone(context, job, service, token),
        "apply" => apply(context, job, service, token),
        "validate" => {
            let draft = job
                .draft_version
                .ok_or_else(|| Error::new(409, "draft_missing"))?;
            let result = context.api(
                "GET",
                &format!("/service/{}/version/{draft}/validate", service.service_id),
                token,
                None,
            )?;
            if result["status"] != "ok" {
                return Err(Error::new(400, "draft_validation_failed"));
            }
            advance(job, if job.activate { "activate" } else { "finalize" });
            Ok(())
        }
        "activate" => {
            let draft = job
                .draft_version
                .ok_or_else(|| Error::new(409, "draft_missing"))?;
            let active = active_version(context, &service.service_id, token)?;
            if active != draft {
                if Some(active) != job.source_version {
                    return Err(Error::new(409, "active_version_changed"));
                }
                activation_generation(context, job)?;
                fence(context, job)?;
                context.api(
                    "PUT",
                    &format!("/service/{}/version/{draft}/activate", service.service_id),
                    token,
                    None,
                )?;
            }
            let needs_rum_sync = job.changes.iter().any(|c| c.resource == "fos_object")
                || (service.rum.enabled
                    && service.capture != Capture::Attached
                    && job.kind != "teardown");
            advance(
                job,
                if needs_rum_sync {
                    "sync_rum"
                } else if job.kind == "teardown" {
                    "remove_front"
                } else {
                    "finalize"
                },
            );
            Ok(())
        }
        "sync_rum" => {
            let delete = job
                .changes
                .iter()
                .find(|c| c.resource == "fos_object")
                .is_some_and(|c| c.action == "delete")
                || !service.rum.enabled
                || (job.kind == "teardown" && job.options.remove_snippets);
            sync_rum_fos_objects(context, service, delete)?;
            if !delete {
                let purge_path = format!("/service/{}/purge/rum-js", service.service_id);
                context.discard_uncertainty("POST", &purge_path, None);
                let _ = context.api_status("POST", &purge_path, token, None)?;
            }
            advance(
                job,
                if job.kind == "teardown" {
                    "remove_front"
                } else {
                    "finalize"
                },
            );
            Ok(())
        }
        _ => Err(Error::new(409, "invalid_reconcile_step")),
    }
}

fn discover(
    context: &Context<'_, impl Runtime>,
    job: &mut Job,
    service: &Service,
    token: &str,
) -> Result<(), Error> {
    if job.kind == "teardown" && !job.options.remove_logging && !job.options.remove_snippets {
        job.changes.clear();
        job.change_cursor = 0;
        advance(job, "remove_front");
        return Ok(());
    }
    if context.remaining_calls() < 9 {
        return Err(Error::new(503, "backend_budget"));
    }
    let version = active_version(context, &service.service_id, token)?;
    let attached = service.capture == Capture::Attached;
    let mut effective_service = service.clone();
    let mut origins_changed = false;
    if !attached && service.rum.enabled && job.kind != "teardown" && context.remaining_calls() >= 10
    {
        match context.api_status(
            "GET",
            &format!("/service/{}/version/{version}/domain", service.service_id),
            token,
            None,
        ) {
            Ok((200, val)) => {
                if let Ok(domains) = list(&val) {
                    let origins = crate::logging::rum::service_timing_origins(domains);
                    if !origins.is_empty() && origins != effective_service.rum.timing_allow_origins
                    {
                        effective_service.rum.timing_allow_origins = origins;
                        origins_changed = true;
                    }
                }
            }
            Ok(_) => {}
            Err(err) if err.code == "backend_budget" => return Err(err),
            Err(_) => {}
        }
    }
    let mut target = desired(context, &effective_service)?;
    let desired_fos_endpoints = target.get("logging/fos").cloned().unwrap_or_default();
    if job.kind == "teardown" {
        if job.options.remove_logging {
            target.insert("logging/fos".into(), vec![]);
            if job.options.remove_snippets {
                target.insert("condition".into(), vec![]);
            } else if let Some(conds) = target.get_mut("condition") {
                conds.retain(|c| c["name"] == "fastly_log_analytics_false");
            }
        }
        if job.options.remove_snippets {
            target.insert("snippet".into(), vec![]);
            target.insert("backend".into(), vec![]);
            target.insert("dictionary".into(), vec![]);
            if !job.options.remove_logging {
                if let Some(conds) = target.get_mut("condition") {
                    conds.retain(|c| c["name"] != "fastly_log_analytics_false");
                }
            }
        }
    }
    let mut current = BTreeMap::new();
    for resource in ["snippet", "logging/fos", "condition"] {
        let value = context.api(
            "GET",
            &format!(
                "/service/{}/version/{version}/{resource}",
                service.service_id
            ),
            token,
            None,
        )?;
        current.insert(resource.to_owned(), list(&value)?.clone());
    }
    // Attach mode never reads or writes the other install's RUM/scoring backends and dictionaries.
    let rum_involved = !attached
        && (service.rum.enabled
            || service
                .owned_endpoint_names
                .contains(&service.rum.endpoint_name)
            || job
                .previous_endpoint_names
                .contains(&service.rum.endpoint_name)
            || current["condition"]
                .iter()
                .any(|v| v["name"].as_str() == Some("rum_log_condition"))
            || current["snippet"].iter().any(|v| {
                v["content"]
                    .as_str()
                    .or_else(|| v["body"].as_str())
                    .unwrap_or("")
                    .contains("Section 1: RUM")
            }));
    let scoring_involved = !attached
        && (service.scoring.is_some()
            || job.kind == "reconcile"
            || (job.kind == "teardown" && job.options.remove_snippets)
            || current["condition"]
                .iter()
                .any(|v| v["name"].as_str() == Some("fastly_log_analytics_false"))
            || current["snippet"].iter().any(|v| {
                v["name"]
                    .as_str()
                    .is_some_and(super::scoring::is_scoring_snippet_name)
            }));
    if rum_involved {
        for resource in ["backend", "dictionary"] {
            let value = context.api(
                "GET",
                &format!(
                    "/service/{}/version/{version}/{resource}",
                    service.service_id
                ),
                token,
                None,
            )?;
            current.insert(resource.to_owned(), list(&value)?.clone());
        }
    } else if scoring_involved {
        let value = context.api(
            "GET",
            &format!("/service/{}/version/{version}/backend", service.service_id),
            token,
            None,
        )?;
        current.insert("backend".to_owned(), list(&value)?.clone());
    }
    let snippets = &current["snippet"];
    if snippets.iter().any(|v| {
        v["name"]
            .as_str()
            .is_some_and(|s| s.starts_with("flp-owner-") && s != marker(service))
    }) {
        return Err(Error::new(409, "service_managed_by_other_install"));
    }
    let own_marker = snippets.iter().any(|v| {
        v["name"] == marker(service)
            && v["content"].as_str().or_else(|| v["body"].as_str())
                == Some(&format!("# {}", resource_owner(service)))
    });
    if attached {
        if job.kind != "teardown" && !capture_present(snippets) {
            return Err(Error::new(409, "capture_vcl_missing"));
        }
        let conditions = &current["condition"];
        if let Some(ours) = target
            .get_mut("condition")
            .and_then(|c| c.iter_mut().find(|v| v["name"] == ATTACH_CONDITION))
        {
            let statement = match attach_condition_statement(
                conditions,
                ours["statement"].as_str().unwrap_or(""),
            ) {
                Ok(statement) => statement,
                // Teardown keeps a condition it leaves in place as it is.
                Err(error) if job.kind == "teardown" => conditions
                    .iter()
                    .find(|v| v["name"] == ATTACH_CONDITION)
                    .and_then(|v| v["statement"].as_str())
                    .map(str::to_owned)
                    .ok_or(error)?,
                Err(error) => return Err(error),
            };
            ours["statement"] = statement.into();
        }
        if service.rum.enabled {
            if let Some(ours) = target
                .get_mut("condition")
                .and_then(|c| c.iter_mut().find(|v| v["name"] == ATTACH_RUM_CONDITION))
            {
                let statement = match attach_rum_condition_statement(
                    conditions,
                    ours["statement"].as_str().unwrap_or(""),
                ) {
                    Ok(statement) => statement,
                    Err(error) if job.kind == "teardown" => conditions
                        .iter()
                        .find(|v| v["name"] == ATTACH_RUM_CONDITION)
                        .and_then(|v| v["statement"].as_str())
                        .map(str::to_owned)
                        .ok_or(error)?,
                    Err(error) => return Err(error),
                };
                ours["statement"] = statement.into();
            }
        }
    } else if !own_marker
        && snippets
            .iter()
            .any(|v| v["name"].as_str().is_some_and(managed_name))
    {
        return Err(Error::new(409, "managed_resources_not_owned"));
    }
    let scope = crate::ingest::scope(service);
    if !attached
        && (has_attach_condition_marker(&current["condition"])
            || has_attach_endpoint_marker(&current["logging/fos"])
            || (capture_present(snippets)
                && has_foreign_primary_endpoint(service, &scope, &current["logging/fos"])))
    {
        return Err(Error::new(409, "managed_resources_not_owned"));
    }
    if !attached
        && !own_marker
        && current["logging/fos"]
            .iter()
            .any(|v| v["name"].as_str().is_some_and(managed_name))
    {
        return Err(Error::new(409, "managed_resources_not_owned"));
    }
    let managed_names: BTreeSet<_> = target["snippet"]
        .iter()
        .filter_map(|s| s["name"].as_str().map(str::to_owned))
        .collect();
    if !attached && service.cmcd.enabled && service.logging_enabled && job.kind != "teardown" {
        for snippet in snippets {
            let name = snippet["name"].as_str().unwrap_or("");
            if name == "CMCD Extraction" {
                return Err(Error::new(409, "resource_name_collision"));
            }
            if !managed_names.contains(name)
                && (snippet["type"] == "recv" || snippet["subroutine"] == "vcl_recv")
                && snippet["content"]
                    .as_str()
                    .or_else(|| snippet["body"].as_str())
                    .unwrap_or("")
                    .contains("declare local var.cmcd")
            {
                return Err(Error::new(409, "cmcd_vcl_variable_collision"));
            }
        }
    }
    if let Some(desired) = target.get_mut("snippet") {
        for snippet in desired {
            let sub = snippet["subroutine"].as_str().unwrap_or("");
            let mut content = snippet["body"].as_str().unwrap_or("").to_owned();
            for variable in [
                "fosAccessKey",
                "fosSecretKey",
                "fosBucket",
                "fosRegion",
                "fosHost",
                "canonicalHeaders",
                "signedHeaders",
                "canonicalRequest",
                "canonicalQuery",
                "stringToSign",
                "dateStamp",
                "signature",
                "scope",
            ] {
                if snippets.iter().any(|other| {
                    !managed_names.contains(other["name"].as_str().unwrap_or(""))
                        && (other["subroutine"] == sub
                            || other["type"] == sub.trim_start_matches("vcl_"))
                        && other["content"]
                            .as_str()
                            .or_else(|| other["body"].as_str())
                            .unwrap_or("")
                            .contains(&format!("declare local var.{variable}"))
                }) {
                    content=content.replace(&format!("declare local var.{variable} STRING;"),
                        &format!("# declare local var.{variable} STRING; (omitted to prevent collision with unmanaged snippet)"));
                }
            }
            snippet["body"] = content.into();
        }
    }
    let mut changes = Vec::new();
    let resources: &[&str] = if job.kind == "teardown" && !job.options.remove_snippets {
        &["logging/fos", "condition"]
    } else if rum_involved {
        &[
            "snippet",
            "logging/fos",
            "condition",
            "backend",
            "dictionary",
        ]
    } else if scoring_involved {
        &["snippet", "logging/fos", "condition", "backend"]
    } else {
        &["snippet", "logging/fos", "condition"]
    };
    for &resource in resources {
        let wanted = &target[resource];
        for value in wanted {
            let name = required(value, "name")?;
            let desired = resource_body(resource, value, service);
            let found = current[resource].iter().find(|v| v["name"] == name);
            if let Some(existing) = found {
                if !own_marker && !name.starts_with("flp-owner-") {
                    return Err(Error::new(409, "resource_name_collision"));
                }
                if resource.starts_with("logging/") {
                    if existing["bucket_name"].as_str() != Some(service.fos_bucket.as_str())
                        || !endpoint_path_matches_scope(
                            existing["path"].as_str().unwrap_or(""),
                            &scope,
                        )
                    {
                        return Err(Error::new(409, "resource_name_collision"));
                    }
                    let already_owned = service.owned_endpoint_names.iter().any(|v| v == name);
                    let self_healed = resource == "logging/fos"
                        && own_marker
                        && verified_cloud_fos_endpoint(
                            existing,
                            value["response_condition"].as_str().unwrap_or(""),
                            service,
                            &scope,
                        );
                    if !already_owned && !self_healed {
                        return Err(Error::new(409, "resource_name_collision"));
                    }
                }
            }
            if found.is_none_or(|v| normalized(resource, v) != normalized(resource, &desired)) {
                changes.push(Change {
                    resource: resource.into(),
                    name: name.into(),
                    action: "upsert".into(),
                    body: desired,
                });
            }
        }
        for value in &current[resource] {
            let name = required(value, "name")?;
            let owned = own_marker
                && if resource.starts_with("logging/") {
                    let scope_ok = value["bucket_name"].as_str()
                        == Some(service.fos_bucket.as_str())
                        && endpoint_path_matches_scope(
                            value["path"].as_str().unwrap_or(""),
                            &scope,
                        )
                        && !value["response_condition"]
                            .as_str()
                            .unwrap_or("")
                            .is_empty();
                    scope_ok
                        && (service.owned_endpoint_names.iter().any(|v| v == name)
                            || job.previous_endpoint_names.iter().any(|v| v == name)
                            || (resource == "logging/fos"
                                && desired_fos_endpoints.iter().any(|d| {
                                    d["name"].as_str() == Some(name)
                                        && verified_cloud_fos_endpoint(
                                            value,
                                            d["response_condition"].as_str().unwrap_or(""),
                                            service,
                                            &scope,
                                        )
                                })))
                } else if resource == "condition" {
                    if attached {
                        name == ATTACH_CONDITION || name == ATTACH_RUM_CONDITION
                    } else if job.kind == "teardown" && !job.options.remove_snippets {
                        matches!(name, "log_analytics_condition" | "rum_log_condition")
                    } else if job.kind == "teardown" && !job.options.remove_logging {
                        name == "fastly_log_analytics_false"
                    } else {
                        matches!(
                            name,
                            "log_analytics_condition"
                                | "rum_log_condition"
                                | "fastly_log_analytics_false"
                        )
                    }
                } else if resource == "backend" {
                    matches!(
                        name,
                        "fos_origin" | super::scoring::SCORING_BACKEND_API_NAME
                    )
                } else if resource == "dictionary" {
                    name == "fos_credentials"
                } else if attached {
                    name == marker(service)
                } else {
                    managed_name(name)
                };
            if owned && !wanted.iter().any(|v| v["name"] == name) {
                changes.push(Change {
                    resource: resource.into(),
                    name: name.into(),
                    action: "delete".into(),
                    body: Value::Null,
                });
            }
        }
    }
    if rum_involved && !changes.is_empty() {
        if service.rum.enabled && job.kind != "teardown" {
            for item_key in ["access_key", "secret_key", "bucket", "region"] {
                changes.push(Change {
                    resource: "dictionary_item".into(),
                    name: item_key.into(),
                    action: "upsert".into(),
                    body: Value::Null,
                });
            }
            changes.push(Change {
                resource: "fos_object".into(),
                name: "rum_assets".into(),
                action: "upsert".into(),
                body: Value::Null,
            });
        } else if !service.rum.enabled || (job.kind == "teardown" && job.options.remove_snippets) {
            changes.push(Change {
                resource: "fos_object".into(),
                name: "rum_assets".into(),
                action: "delete".into(),
                body: Value::Null,
            });
        }
    }
    // Fastly rejects an endpoint or backend whose condition does not exist yet,
    // a dictionary item whose dictionary does not exist yet, and a condition still
    // referenced by a backend or endpoint; VCL snippet symbols are checked at validate.
    changes.sort_by_key(
        |change| match (change.action.as_str(), change.resource.as_str()) {
            (_, "snippet") if change.action != "delete" => 0,
            (_, "condition") if change.action != "delete" => 1,
            (_, "dictionary") if change.action != "delete" => 2,
            (_, "dictionary_item") if change.action != "delete" => 3,
            (_, "backend") if change.action != "delete" => 4,
            (_, "logging/fos") if change.action != "delete" => 5,
            (_, "fos_object") if change.action != "delete" => 6,
            ("delete", "snippet") => 7,
            ("delete", "logging/fos") => 8,
            ("delete", "backend") => 9,
            ("delete", "dictionary") => 10,
            ("delete", "condition") => 11,
            ("delete", "fos_object") => 12,
            _ => 13,
        },
    );
    job.source_version = Some(version);
    job.changes = changes;
    job.change_cursor = 0;
    let mut missing_endpoints = Vec::new();
    if service.logging_enabled
        && !service
            .owned_endpoint_names
            .contains(&service.logging_endpoint_name)
    {
        missing_endpoints.push(service.logging_endpoint_name.clone());
    }
    if service.rum.enabled {
        let rum_ep = if attached {
            ATTACH_RUM_ENDPOINT
        } else {
            service.rum.endpoint_name.as_str()
        };
        if !rum_ep.is_empty() && !service.owned_endpoint_names.iter().any(|v| v == rum_ep) {
            missing_endpoints.push(rum_ep.to_owned());
        }
    }
    if job.kind != "teardown" && !job.dry_run && (!missing_endpoints.is_empty() || origins_changed)
    {
        if context.remaining_calls() < 5 {
            return Err(Error::new(503, "backend_budget"));
        }
        let record = context
            .get(&format!("svc:{}", service.service_id))?
            .ok_or_else(|| Error::new(503, "state_unavailable"))?;
        if record.generation != job.expected_generation {
            return Err(Error::new(409, "service_changed_rerun"));
        }
        let mut claimed = service.clone();
        for ep in missing_endpoints {
            if !claimed.owned_endpoint_names.contains(&ep) {
                claimed.owned_endpoint_names.push(ep);
            }
        }
        if origins_changed {
            claimed.rum.timing_allow_origins = effective_service.rum.timing_allow_origins.clone();
        }
        save_job_service(
            context,
            &record,
            &claimed,
            job,
            "endpoint.claimed",
            "discover",
        )?;
        job.expected_generation = context
            .get(&format!("svc:{}", service.service_id))?
            .ok_or_else(|| Error::new(503, "state_unavailable"))?
            .generation;
    }
    advance(
        job,
        if job.changes.is_empty() || job.dry_run {
            if job.kind == "teardown" {
                "remove_front"
            } else if !job.dry_run && service.rum.enabled && service.capture != Capture::Attached {
                "sync_rum"
            } else {
                "finalize"
            }
        } else {
            "clone"
        },
    );
    Ok(())
}

fn clone(
    context: &Context<'_, impl Runtime>,
    job: &mut Job,
    service: &Service,
    token: &str,
) -> Result<(), Error> {
    let source = job
        .source_version
        .ok_or_else(|| Error::new(409, "source_version_missing"))?;
    let versions = context.api(
        "GET",
        &format!("/service/{}/version", service.service_id),
        token,
        None,
    )?;
    let versions = list(&versions)?;
    let tag = format!("flp-job:{}", job.id);
    if let Some(draft) = versions
        .iter()
        .filter(|v| v["comment"] == tag && v["active"] != true && v["locked"] != true)
        .filter_map(|v| v["number"].as_u64())
        .filter(|&n| n > source)
        .max()
    {
        job.draft_version = Some(draft);
        advance(job, "apply");
        return Ok(());
    }
    if let Some(intent) = job.clone_intent {
        if versions
            .iter()
            .any(|v| v["number"].as_u64().is_some_and(|n| n > intent))
        {
            return Err(Error::new(409, "uncheckpointed_clone_requires_review"));
        }
        if active_version(context, &service.service_id, token)? != source {
            return Err(Error::new(409, "active_version_changed"));
        }
        if context.remaining_calls() < 8 {
            return Err(Error::new(503, "backend_budget"));
        }
        let draft = context.api(
            "PUT",
            &format!("/service/{}/version/{source}/clone", service.service_id),
            token,
            None,
        )?;
        let number = draft["number"]
            .as_u64()
            .ok_or_else(|| Error::new(502, "invalid_draft_response"))?;
        context.api(
            "PUT",
            &format!("/service/{}/version/{number}", service.service_id),
            token,
            Some(json!({"comment":tag})),
        )?;
        job.draft_version = Some(number);
        advance(job, "apply");
    } else {
        job.clone_intent = Some(
            versions
                .iter()
                .filter_map(|v| v["number"].as_u64())
                .max()
                .unwrap_or(source),
        );
    }
    Ok(())
}

fn apply(
    context: &Context<'_, impl Runtime>,
    job: &mut Job,
    service: &Service,
    token: &str,
) -> Result<(), Error> {
    let Some(change) = job.changes.get(job.change_cursor) else {
        advance(job, "validate");
        return Ok(());
    };
    let draft = job
        .draft_version
        .ok_or_else(|| Error::new(409, "draft_missing"))?;
    if change.resource == "dictionary_item" {
        if change.action != "delete" {
            upsert_rum_dictionary_item(context, service, draft, token, &change.name)?;
        }
    } else if change.resource == "fos_object" {
        // Deferred until post-validate/activate (`sync_rum`) so validation failure never overwrites live FOS assets.
    } else {
        let mut body = change.body.clone();
        if change.resource == "snippet"
            && change.name == super::scoring::SCORING_PASS_NAME
            && change.action != "delete"
        {
            let Some(req_secret) =
                super::scoring::load_or_recover_request_secret(context, service, token)?
            else {
                return Ok(());
            };
            super::scoring::validate_vcl_literal(&req_secret, false)?;
            for field in ["content", "body"] {
                if let Some(text) = body[field].as_str() {
                    body[field] = text
                        .replace(super::scoring::REQUEST_SECRET_PLACEHOLDER, &req_secret)
                        .into();
                }
            }
        }
        upsert(context, &service.service_id, draft, token, change, &body)?;
    }
    job.change_cursor += 1;
    if job.change_cursor == job.changes.len() {
        advance(job, "validate");
    }
    Ok(())
}

fn upsert_rum_dictionary_item(
    context: &Context<'_, impl Runtime>,
    service: &Service,
    version: u64,
    token: &str,
    key: &str,
) -> Result<(), Error> {
    let dict = context.api(
        "GET",
        &format!(
            "/service/{}/version/{version}/dictionary/fos_credentials",
            service.service_id
        ),
        token,
        None,
    )?;
    let dict_id = dict["id"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::new(502, "dictionary_id_missing"))?;
    let value = match key {
        "access_key" => fos::credentials(context, service)?.0,
        "secret_key" => fos::credentials(context, service)?.1,
        "bucket" | "bucket_name" => service.fos_bucket.clone(),
        "region" => service.fos_region.clone(),
        _ => return Err(Error::new(400, "invalid_dictionary_item")),
    };
    let path = format!(
        "/service/{}/dictionary/{dict_id}/item/{}",
        service.service_id,
        encode(key)
    );
    let body = json!({"item_key": key, "item_value": value});
    context.discard_uncertainty("PUT", &path, Some(&body));
    context.api("PUT", &path, token, Some(body))?;
    Ok(())
}

fn sync_rum_fos_objects(
    context: &Context<'_, impl Runtime>,
    service: &Service,
    delete: bool,
) -> Result<(), Error> {
    crate::logging::rum::validate_faro_version(&service.rum.faro_version)
        .map_err(|_| Error::new(400, "invalid_rum_settings"))?;
    let faro_key = crate::logging::rum::faro_object_key(&service.rum.faro_version);
    let tracker_key = crate::logging::rum::TRACKER_OBJECT_KEY;
    let credentials = fos::credentials(context, service)?;
    if delete {
        fos::delete_object(context, service, &credentials, &faro_key)?;
        fos::delete_object(context, service, &credentials, tracker_key)?;
    } else {
        crate::logging::rum::validate_service_id(&service.service_id)
            .map_err(|_| Error::new(400, "invalid_service_id"))?;
        let faro_bytes = crate::logging::rum::faro_bundle_bytes(&service.rum.faro_version)
            .map_err(|_| Error::new(400, "unsupported_faro_version"))?;
        let tracker_js =
            crate::logging::rum::generate_rum_tracker_js(&service.service_id, &service.rum);
        fos::put_object(
            context,
            service,
            &credentials,
            &faro_key,
            faro_bytes.to_vec(),
        )?;
        fos::put_object(
            context,
            service,
            &credentials,
            tracker_key,
            tracker_js.into_bytes(),
        )?;
    }
    // After an upgrade the previous pinned bundle is unreferenced; teardown removes every version.
    for version in crate::logging::rum::SUPPORTED_FARO_VERSIONS {
        if *version != service.rum.faro_version {
            let key = crate::logging::rum::faro_object_key(version);
            fos::delete_object(context, service, &credentials, &key)?;
        }
    }
    Ok(())
}

fn upsert(
    context: &Context<'_, impl Runtime>,
    sid: &str,
    version: u64,
    token: &str,
    change: &Change,
    body: &Value,
) -> Result<(), Error> {
    let base = format!("/service/{sid}/version/{version}/{}", change.resource);
    let path = format!("{base}/{}", encode(&change.name));
    let (status, _) = context.api_status("GET", &path, token, None)?;
    // The readback above decides the next call, and replaying the same draft
    // mutation is idempotent, so an unrecorded earlier outcome is not blocking.
    match (change.action.as_str(), status) {
        ("delete", 404) => Ok(()),
        ("delete", 200) => {
            context.discard_uncertainty("DELETE", &path, None);
            context.api("DELETE", &path, token, None)?;
            Ok(())
        }
        ("upsert", 404) => {
            context.discard_uncertainty("POST", &base, Some(body));
            context.api("POST", &base, token, Some(body.clone()))?;
            Ok(())
        }
        ("upsert", 200) => {
            context.discard_uncertainty("PUT", &path, Some(body));
            context.api("PUT", &path, token, Some(body.clone()))?;
            Ok(())
        }
        _ => Err(Error::new(502, "resource_lookup_failed")),
    }
}

/// The front clock health checks' interval (`flp_clock` on `/_tick`, `flp_flush` on `/_flush`):
/// half the shortest log period the clock serves (request, and RUM when enabled, whose
/// ingest runs at least every 5 s), between 1 s and 30 s. Every Fastly POP sends both probes at
/// this interval and each is a billed front request, so a 1 s clock costs ~9M requests a day.
/// Half a period suffices because a turn that stops with work left continues itself
/// (`Shell::continue_clock`) instead of waiting for the next probe.
pub fn clock_interval_ms(service: &Service) -> u64 {
    clock_interval_ms_for(
        service.log_period,
        service.rum.enabled.then_some(service.rum.log_period),
    )
}

fn clock_interval_ms_for(log_period_s: u32, rum_log_period_s: Option<u32>) -> u64 {
    let mut period = u64::from(log_period_s.max(1));
    if let Some(rum) = rum_log_period_s {
        period = period.min(u64::from(rum.max(5)));
    }
    (period * 1000 / 2).clamp(1000, 30_000)
}

/// Clock health check changes that bring the front's existing checks to `clock_interval_ms`.
/// Missing checks are left to the provisioner, which creates them with their host and path.
fn clock_changes(
    context: &Context<'_, impl Runtime>,
    sid: &str,
    active: u64,
    token: &str,
    service: &Service,
) -> Result<Vec<Change>, Error> {
    let interval = clock_interval_ms(service);
    let checks = context.api(
        "GET",
        &format!("/service/{sid}/version/{active}/healthcheck"),
        token,
        None,
    )?;
    let checks = list(&checks)?;
    Ok(["flp_clock", "flp_flush"]
        .into_iter()
        .filter_map(|name| {
            let found = checks.iter().find(|v| v["name"] == name)?;
            let current = found["check_interval"]
                .as_u64()
                .or_else(|| found["check_interval"].as_str()?.parse().ok());
            (current != Some(interval)).then(|| Change {
                resource: "healthcheck".into(),
                name: name.into(),
                action: "upsert".into(),
                body: json!({"check_interval": interval}),
            })
        })
        .collect())
}

/// Brings the front service in line: the FOS region backend and dictionaries (provision), or with
/// `clock_only` just the clock health checks, which every reconcile (and so every deploy) checks.
pub fn front(
    context: &Context<'_, impl Runtime>,
    job: &mut Job,
    service: &Service,
    token: &str,
    remove: bool,
    clock_only: bool,
) -> Result<(), Error> {
    let sid = config(context.runtime, "front_service_id")?;
    // Past activation: the dictionaries (cursor 6..=9) are provision-only.
    let done = if clock_only { 10 } else { 6 };
    if remove {
        let (dictionary, key) = match job.front_cursor {
            0 => ("buckets_dictionary_id", service.service_id.clone()),
            1 => (
                "credentials_dictionary_id",
                format!("{}.ak", service.service_id),
            ),
            2 => (
                "credentials_dictionary_id",
                format!("{}.sk", service.service_id),
            ),
            _ => {
                advance(job, "delete_objects");
                return Ok(());
            }
        };
        let dictionary = config(context.runtime, dictionary)?;
        let (status, _) = context.api_status(
            "DELETE",
            &format!(
                "/service/{sid}/dictionary/{dictionary}/item/{}",
                encode(&key)
            ),
            token,
            None,
        )?;
        if !(200..300).contains(&status) && status != 404 {
            return Err(Error::new(502, "front_dictionary_failed"));
        }
        job.front_cursor += 1;
        return Ok(());
    }
    match job.front_cursor {
        0 => {
            let active = active_version(context, &sid, token)?;
            let name = format!("fos_{}", service.fos_region.replace('-', "_"));
            let condition = format!("flp_{name}");
            let host = &service.fos_endpoint;
            let desired = [
                Change {
                    resource: "condition".into(),
                    name: condition.clone(),
                    action: "upsert".into(),
                    body: json!({"name":condition,"type":"REQUEST","priority":5,"statement":format!("req.http.x-flp-fos-region == \"{}\"",service.fos_region)}),
                },
                Change {
                    resource: "backend".into(),
                    name: name.clone(),
                    action: "upsert".into(),
                    body: json!({"name":name,"address":host,"override_host":host,"ssl_sni_hostname":host,"ssl_cert_hostname":host,"port":443,"use_ssl":1,"ssl_check_cert":1,"prefer_ipv6":0,"shield":"iad-va-us","request_condition":condition}),
                },
            ];
            let mut changes = Vec::new();
            for change in desired.into_iter().filter(|_| !clock_only) {
                let values = context.api(
                    "GET",
                    &format!("/service/{sid}/version/{active}/{}", change.resource),
                    token,
                    None,
                )?;
                let found = list(&values)?.iter().find(|v| v["name"] == change.name);
                if found.is_none_or(|v| {
                    change.body.as_object().is_some_and(|m| {
                        m.iter().any(|(key, value)| {
                            v[key] != *value
                                && v[key].as_str()
                                    != value.as_u64().map(|n| n.to_string()).as_deref()
                                && !(value.as_u64().is_some_and(|n| n <= 1)
                                    && v[key].as_bool() == value.as_u64().map(|n| n == 1))
                        })
                    })
                }) {
                    changes.push(change);
                }
            }
            changes.extend(clock_changes(context, &sid, active, token, service)?);
            job.front_source_version = Some(active);
            job.front_changes = changes;
            // A dry run only reports what it would change.
            job.front_cursor = if job.front_changes.is_empty() || (clock_only && job.dry_run) {
                done
            } else {
                1
            };
        }
        1 => {
            let source = job
                .front_source_version
                .ok_or_else(|| Error::new(409, "front_source_missing"))?;
            let versions = context.api("GET", &format!("/service/{sid}/version"), token, None)?;
            let tag = format!("flp-front-job:{}", job.id);
            if let Some(number) = list(&versions)?
                .iter()
                .filter(|v| v["comment"] == tag && v["locked"] != true && v["active"] != true)
                .filter_map(|v| v["number"].as_u64())
                .filter(|&n| n > source)
                .max()
            {
                job.front_draft_version = Some(number);
                job.front_cursor = 2;
            } else if let Some(intent) = job.front_clone_intent {
                if list(&versions)?
                    .iter()
                    .any(|v| v["number"].as_u64().is_some_and(|n| n > intent))
                {
                    return Err(Error::new(
                        409,
                        "uncheckpointed_front_clone_requires_review",
                    ));
                }
                if active_version(context, &sid, token)? != source {
                    return Err(Error::new(409, "front_active_version_changed"));
                }
                if context.remaining_calls() < 8 {
                    return Err(Error::new(503, "backend_budget"));
                }
                let draft = context.api(
                    "PUT",
                    &format!("/service/{sid}/version/{source}/clone"),
                    token,
                    None,
                )?;
                let number = draft["number"]
                    .as_u64()
                    .ok_or_else(|| Error::new(502, "invalid_draft_response"))?;
                context.api(
                    "PUT",
                    &format!("/service/{sid}/version/{number}"),
                    token,
                    Some(json!({"comment":tag})),
                )?;
                job.front_draft_version = Some(number);
                job.front_cursor = 2;
            } else {
                job.front_clone_intent = Some(
                    list(&versions)?
                        .iter()
                        .filter_map(|v| v["number"].as_u64())
                        .max()
                        .unwrap_or(source),
                );
            }
        }
        2 | 3 => {
            // Cursor 2 applies the first change, cursor 3 the rest (two calls each).
            let draft = job
                .front_draft_version
                .ok_or_else(|| Error::new(409, "front_draft_missing"))?;
            let range = if job.front_cursor == 2 {
                0..1
            } else {
                1..job.front_changes.len()
            };
            for change in job.front_changes.get(range).unwrap_or_default() {
                upsert(context, &sid, draft, token, change, &change.body)?;
            }
            job.front_cursor += 1;
        }
        4 => {
            let draft = job
                .front_draft_version
                .ok_or_else(|| Error::new(409, "front_draft_missing"))?;
            let value = context.api(
                "GET",
                &format!("/service/{sid}/version/{draft}/validate"),
                token,
                None,
            )?;
            if value["status"] != "ok" {
                return Err(Error::new(400, "front_draft_validation_failed"));
            }
            job.front_cursor = 5;
        }
        5 => {
            let draft = job
                .front_draft_version
                .ok_or_else(|| Error::new(409, "front_draft_missing"))?;
            let active = active_version(context, &sid, token)?;
            if active != draft {
                if Some(active) != job.front_source_version {
                    return Err(Error::new(409, "front_active_version_changed"));
                }
                activation_generation(context, job)?;
                fence(context, job)?;
                context.api(
                    "PUT",
                    &format!("/service/{sid}/version/{draft}/activate"),
                    token,
                    None,
                )?;
            }
            job.front_cursor = done;
        }
        6..=9 if !clock_only => {
            let (dictionary, key, value) = match job.front_cursor {
                6 => (
                    "buckets_dictionary_id",
                    service.service_id.clone(),
                    format!("{}|{}", service.fos_bucket, service.fos_region),
                ),
                7 => (
                    "credentials_dictionary_id",
                    format!("{}.ak", service.service_id),
                    fos::credentials(context, service)?.0,
                ),
                8 => (
                    "credentials_dictionary_id",
                    format!("{}.sk", service.service_id),
                    fos::credentials(context, service)?.1,
                ),
                _ => (
                    "front_auth_dictionary_id",
                    "cdn_key".into(),
                    context.secret("cdn_key")?,
                ),
            };
            let dictionary = config(context.runtime, dictionary)?;
            context.api(
                "PUT",
                &format!(
                    "/service/{sid}/dictionary/{dictionary}/item/{}",
                    encode(&key)
                ),
                token,
                Some(json!({"item_key":key,"item_value":value})),
            )?;
            job.front_cursor += 1;
        }
        _ => {
            job.front_cursor = 0;
            advance(job, "discover");
        }
    }
    Ok(())
}

pub(super) fn activation_generation(
    context: &Context<'_, impl Runtime>,
    job: &Job,
) -> Result<(), Error> {
    let record = context
        .get(&format!("svc:{}", job.service_id))?
        .ok_or_else(|| Error::new(409, "service_changed_rerun"))?;
    if record.generation != job.expected_generation {
        return Err(Error::new(409, "service_changed_rerun"));
    }
    Ok(())
}

#[cfg(test)]
mod clock_tests {
    use super::clock_interval_ms_for;

    #[test]
    fn the_clock_runs_at_half_the_shortest_log_period_between_1_and_30_s() {
        assert_eq!(clock_interval_ms_for(60, None), 30_000);
        assert_eq!(clock_interval_ms_for(300, None), 30_000);
        assert_eq!(clock_interval_ms_for(40, None), 20_000);
        assert_eq!(clock_interval_ms_for(1, None), 1_000);
        assert_eq!(clock_interval_ms_for(60, Some(20)), 10_000);
        // RUM ingest runs at least every 5 s, so a shorter RUM period still clocks at 2.5 s.
        assert_eq!(clock_interval_ms_for(60, Some(1)), 2_500);
    }
}

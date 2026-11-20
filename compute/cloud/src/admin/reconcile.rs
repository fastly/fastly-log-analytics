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
            backends.push(super::scoring::desired_scoring_backend(&sc.scoring_domain));
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
        "logging/s3" => {
            let codec = value["compression_codec"]
                .as_str()
                .filter(|s| !s.is_empty())
                .unwrap_or("zstd");
            json!({
                "name":value["name"],"path":value["path"],"period":number(&value["period"],60),"format":value["format"],
                "response_condition":value["response_condition"],"format_version":number(&value["format_version"],2),
                "bucket_name":value["bucket_name"],"domain":value["domain"],"message_type":value["message_type"],
                "compression_codec":codec,
            })
        }
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
        "logging/s3" => {
            let connection =
                json!({"bucket_name":service.fos_bucket,"domain":service.fos_endpoint});
            let mut body = crate::logging::logging_endpoint_body(value, &connection);
            // NDJSON lines must reach FOS without a syslog-style prefix.
            body["message_type"] = "blank".into();
            body
        }
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
            if service.rum.enabled && service.capture != Capture::Attached && job.kind != "teardown"
            {
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
        "sync_rum" => {
            sync_rum_fos_objects(context, service, false)?;
            let purge_path = format!("/service/{}/purge/rum-js", service.service_id);
            context.discard_uncertainty("POST", &purge_path, None);
            let _ = context.api_status("POST", &purge_path, token, None)?;
            advance(job, "finalize");
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
    let version = active_version(context, &service.service_id, token)?;
    let mut target = desired(context, service)?;
    if job.kind == "teardown" {
        if job.options.remove_logging {
            target.insert("logging/fos".into(), vec![]);
            target.insert("condition".into(), vec![]);
        }
        if job.options.remove_snippets {
            target.insert("snippet".into(), vec![]);
            target.insert("backend".into(), vec![]);
            target.insert("dictionary".into(), vec![]);
        }
    }
    let mut current = BTreeMap::new();
    for resource in ["snippet", "logging/s3", "logging/fos", "condition"] {
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
    let attached = service.capture == Capture::Attached;
    // Attach mode never reads or writes the other install's RUM backend and dictionary.
    let rum_involved = !attached
        && (service.rum.enabled
            || service
                .owned_endpoint_names
                .contains(&service.rum.endpoint_name)
            || job
                .previous_endpoint_names
                .contains(&service.rum.endpoint_name)
            || current["condition"].iter().any(|v| {
                matches!(
                    v["name"].as_str(),
                    Some("fastly_log_analytics_false" | "rum_log_condition")
                )
            })
            || current["snippet"].iter().any(|v| {
                v["content"]
                    .as_str()
                    .or_else(|| v["body"].as_str())
                    .unwrap_or("")
                    .contains("Section 1: RUM")
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
    } else if service.scoring.as_ref().is_some_and(|s| s.enabled)
        || current["snippet"].iter().any(|v| {
            v["name"]
                .as_str()
                .is_some_and(super::scoring::is_scoring_snippet_name)
        })
    {
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
    if !attached
        && !own_marker
        && current["logging/s3"]
            .iter()
            .chain(&current["logging/fos"])
            .any(|v| v["name"].as_str().is_some_and(managed_name))
    {
        return Err(Error::new(409, "managed_resources_not_owned"));
    }
    // An endpoint that already exists as legacy S3 stays S3, so the bucket never
    // gets a second endpoint writing the same objects.
    let legacy: BTreeSet<String> = current["logging/s3"]
        .iter()
        .filter_map(|v| v["name"].as_str().map(str::to_owned))
        .collect();
    let (kept, native): (Vec<Value>, Vec<Value>) = target
        .remove("logging/fos")
        .unwrap_or_default()
        .into_iter()
        .partition(|v| v["name"].as_str().is_some_and(|n| legacy.contains(n)));
    target.insert("logging/s3".into(), kept);
    target.insert("logging/fos".into(), native);
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
    let resources: &[&str] = if rum_involved {
        &[
            "snippet",
            "logging/s3",
            "logging/fos",
            "condition",
            "backend",
            "dictionary",
        ]
    } else {
        &["snippet", "logging/s3", "logging/fos", "condition"]
    };
    for &resource in resources {
        let wanted = &target[resource];
        for value in wanted {
            let name = required(value, "name")?;
            let desired = resource_body(resource, value, service);
            let found = current[resource].iter().find(|v| v["name"] == name);
            if found.is_some()
                && (!own_marker && !name.starts_with("flp-owner-")
                    || resource.starts_with("logging/")
                        && !service.owned_endpoint_names.iter().any(|v| v == name))
            {
                return Err(Error::new(409, "resource_name_collision"));
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
                    service.owned_endpoint_names.iter().any(|v| v == name)
                        || job.previous_endpoint_names.iter().any(|v| v == name)
                } else if resource == "condition" {
                    if attached {
                        name == ATTACH_CONDITION || name == ATTACH_RUM_CONDITION
                    } else {
                        matches!(
                            name,
                            "log_analytics_condition"
                                | "rum_log_condition"
                                | "fastly_log_analytics_false"
                        )
                    }
                } else if resource == "backend" {
                    name == "fos_origin"
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
    // Fastly rejects an endpoint whose response condition does not exist yet,
    // and a condition still referenced by an endpoint.
    if rum_involved {
        changes.sort_by_key(
            |change| match (change.action.as_str(), change.resource.as_str()) {
                (_, "condition") if change.action != "delete" => 0,
                (_, "dictionary") if change.action != "delete" => 1,
                (_, "dictionary_item") if change.action != "delete" => 2,
                (_, "backend") if change.action != "delete" => 3,
                (_, "snippet") if change.action != "delete" => 4,
                (_, "logging/s3" | "logging/fos") if change.action != "delete" => 5,
                (_, "fos_object") if change.action != "delete" => 6,
                ("delete", "snippet") => 7,
                ("delete", "logging/s3" | "logging/fos") => 8,
                ("delete", "backend") => 9,
                ("delete", "dictionary") => 10,
                ("delete", "condition") => 11,
                ("delete", "fos_object") => 12,
                _ => 13,
            },
        );
    } else {
        changes.sort_by_key(
            |change| match (change.action.as_str(), change.resource.as_str()) {
                ("delete", "logging/s3" | "logging/fos") => 3,
                ("delete", "condition") => 4,
                ("delete", _) => 5,
                (_, "snippet") => 0,
                (_, "condition") => 1,
                _ => 2,
            },
        );
    }
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
    if service.rum.enabled
        && !service
            .owned_endpoint_names
            .contains(&service.rum.endpoint_name)
    {
        missing_endpoints.push(service.rum.endpoint_name.clone());
    }
    if job.kind != "teardown" && !job.dry_run && !missing_endpoints.is_empty() {
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
        .find(|v| v["comment"] == tag && v["active"] != true && v["locked"] != true)
        .and_then(|v| v["number"].as_u64())
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
        sync_rum_fos_objects(context, service, change.action == "delete")?;
    } else {
        let mut body = change.body.clone();
        if change.resource == "logging/s3" && change.action != "delete" {
            let (ak, sk) = fos::credentials(context, service)?;
            body["access_key"] = ak.into();
            body["secret_key"] = sk.into();
        } else if change.resource == "snippet"
            && change.name == super::scoring::SCORING_PASS_NAME
            && change.action != "delete"
        {
            let req_secret =
                context.secret(&super::scoring::request_secret_ref(&service.service_id))?;
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

pub fn front(
    context: &Context<'_, impl Runtime>,
    job: &mut Job,
    service: &Service,
    token: &str,
    remove: bool,
) -> Result<(), Error> {
    let sid = config(context.runtime, "front_service_id")?;
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
            for change in desired {
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
            job.front_source_version = Some(active);
            job.front_changes = changes;
            job.front_cursor = if job.front_changes.is_empty() { 6 } else { 1 };
        }
        1 => {
            let source = job
                .front_source_version
                .ok_or_else(|| Error::new(409, "front_source_missing"))?;
            let versions = context.api("GET", &format!("/service/{sid}/version"), token, None)?;
            let tag = format!("flp-front-job:{}", job.id);
            if let Some(number) = list(&versions)?
                .iter()
                .find(|v| v["comment"] == tag && v["locked"] != true && v["active"] != true)
                .and_then(|v| v["number"].as_u64())
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
            let index = job.front_cursor - 2;
            if let Some(change) = job.front_changes.get(index) {
                upsert(
                    context,
                    &sid,
                    job.front_draft_version
                        .ok_or_else(|| Error::new(409, "front_draft_missing"))?,
                    token,
                    change,
                    &change.body,
                )?;
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
            job.front_cursor = 6;
        }
        6..=9 => {
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

fn activation_generation(context: &Context<'_, impl Runtime>, job: &Job) -> Result<(), Error> {
    let record = context
        .get(&format!("svc:{}", job.service_id))?
        .ok_or_else(|| Error::new(409, "service_changed_rerun"))?;
    if record.generation != job.expected_generation {
        return Err(Error::new(409, "service_changed_rerun"));
    }
    Ok(())
}

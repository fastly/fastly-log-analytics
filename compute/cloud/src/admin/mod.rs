pub mod fixture;
mod fos;
pub mod logs;
mod model;
mod reconcile;
pub mod reset;
pub mod schedule;
pub mod stage;
pub mod transport;

use crate::{
    http::{Request, Response},
    shell::{Runtime, VcpuSpan},
    telemetry::{
        self,
        flush::{self as log_flush, FlushStatus, FlushTarget},
        write_log, LogBuffer, WritableLogKind,
    },
};
use base64::Engine;
pub use fos::{canonical_path as fos_path, signed_headers as fos_headers};
use model::*;
pub use model::{
    valid_bucket, valid_prefix, valid_service_id, CronRun, DataReset, Job, Record, ResetPhase,
    Service, REGIONS,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use transport::{encode, list, Context};
pub use transport::{
    kv_write_delay_ms, ApiRequest, ApiResponse, BackendBudget, FosRequest, FosResponse,
};

#[derive(Debug)]
pub struct Error {
    pub status: u16,
    pub code: &'static str,
    pub retry: Option<RetryState>,
}
impl Error {
    pub fn new(status: u16, code: &'static str) -> Self {
        Self {
            status,
            code,
            retry: None,
        }
    }
}

pub fn handle(runtime: &impl Runtime, actor: &str, request: &Request) -> Response {
    handle_audited(runtime, actor, request).0
}

/// The response plus the `(action, target)` the shell records in the admin audit log.
pub fn handle_audited(
    runtime: &impl Runtime,
    actor: &str,
    request: &Request,
) -> (Response, Option<(&'static str, String)>) {
    let context = Context::new(runtime);
    let result = route(&context, actor, request);
    let (status, body) = match result {
        Ok(body) => (200, body),
        Err(error) => (
            error.status,
            json!({"detail":error.code,"error":error.code}),
        ),
    };
    let audit = (status == 200).then(|| admin_audit(request)).flatten();
    let response = Response::new(status, body.to_string())
        .header("content-type", "application/json")
        .private();
    (response, audit)
}

fn admin_audit(request: &Request) -> Option<(&'static str, String)> {
    let path = request.path.split('?').next().unwrap_or_default();
    let parts: Vec<_> = path.split('/').filter(|part| !part.is_empty()).collect();
    match (request.method.as_str(), parts.as_slice()) {
        ("POST", ["api", "admin", "services", sid, "delete-data"]) => {
            Some(("delete_data_started", (*sid).to_owned()))
        }
        ("POST", ["api", "admin", "services", sid, "delete-data", "cancel"]) => {
            Some(("delete_data_cancelled", (*sid).to_owned()))
        }
        _ => None,
    }
}

fn decode<T: DeserializeOwned>(value: Value) -> Result<T, Error> {
    serde_json::from_value(value).map_err(|_| Error::new(400, "invalid_input"))
}
fn body<T: DeserializeOwned>(request: &Request) -> Result<T, Error> {
    if request.body.len() > 65_536 {
        return Err(Error::new(413, "body_too_large"));
    }
    serde_json::from_slice(&request.body).map_err(|_| Error::new(400, "invalid_input"))
}
fn serialize(value: &impl Serialize) -> Result<Value, Error> {
    serde_json::to_value(value).map_err(|_| Error::new(500, "state_encoding_failed"))
}
fn required<'a>(value: &'a Value, key: &str) -> Result<&'a str, Error> {
    value[key]
        .as_str()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| Error::new(400, "invalid_input"))
}
fn config(runtime: &impl Runtime, key: &str) -> Result<String, Error> {
    runtime
        .config(key)
        .map_err(|_| Error::new(503, "config_unavailable"))?
        .filter(|value| valid_service_id(value) || key == "install_name")
        .ok_or_else(|| Error::new(503, "install_configuration_missing"))
}
fn owner(runtime: &impl Runtime) -> Result<String, Error> {
    config(runtime, "flp_service_id")
}
fn resource_owner(service: &Service) -> String {
    format!("flp-owner:{}:{}", service.owner, service.service_id)
}

fn load_service(
    context: &Context<'_, impl Runtime>,
    sid: &str,
) -> Result<(Record, Service), Error> {
    if !valid_service_id(sid) {
        return Err(Error::new(400, "invalid_service_id"));
    }
    let record = context
        .get(&format!("svc:{sid}"))?
        .ok_or_else(|| Error::new(404, "service_not_found"))?;
    let mut service: Service = decode(record.value.clone())?;
    if service.compression_codec.is_empty() {
        service.compression_codec = "zstd".into();
    }
    if matches!(service.wire_format.as_str(), "" | "json") {
        service.wire_format = "array".into();
    }
    if service.owner != owner(context.runtime)? {
        return Err(Error::new(403, "service_not_owned"));
    }
    Ok((record, service))
}
fn install(context: &Context<'_, impl Runtime>) -> Result<(Option<Record>, Install), Error> {
    let record = context.get("admin:install")?;
    let value = record
        .as_ref()
        .map(|r| decode(r.value.clone()))
        .transpose()?
        .unwrap_or_default();
    Ok((record, value))
}

fn route(
    context: &Context<'_, impl Runtime>,
    actor: &str,
    request: &Request,
) -> Result<Value, Error> {
    let (path, query) = request.path.split_once('?').unwrap_or((&request.path, ""));
    let path = path.trim_end_matches('/');
    let parts: Vec<_> = path.split('/').filter(|part| !part.is_empty()).collect();
    match (request.method.as_str(), parts.as_slice()) {
        ("GET", ["api", "services"] | ["api", "admin", "services"]) => services(context),
        ("GET", ["api", "admin", "services", sid]) => {
            let (record, service) = load_service(context, sid)?;
            let job = service
                .pending_job_id
                .as_ref()
                .map(|id| load_job(context, id).map(|(_, j)| public_job(&j)))
                .transpose()?;
            Ok(
                json!({"service":service_value(&service,&record.generation),"generation":record.generation,"job":job}),
            )
        }
        ("PATCH", ["api", "admin", "services", sid]) => {
            patch_service(context, actor, sid, body(request)?)
        }
        ("POST", ["api", "admin", "services", sid, "reconcile"]) => {
            let input: Value = body(request)?;
            let (record, mut service) = load_service(context, sid)?;
            check_generation(&record, &input)?;
            if service.status == "torn_down" {
                return Err(Error::new(409, "service_torn_down"));
            }
            if input.get("cmcd").is_some() || input.get("rum").is_some() {
                let previous = service.clone();
                if let Some(cmcd) = input.get("cmcd") {
                    service.cmcd = decode(cmcd.clone())?;
                }
                if let Some(rum) = input.get("rum") {
                    service.rum = decode(rum.clone())?;
                }
                reconcile_system_fields(&mut service, Some(&previous))?;
                validate_service(
                    &service,
                    Some(&previous),
                    crate::ingest::window_seconds(context.runtime),
                )?;
                service.config_version += 1;
                service.updated_at = context.runtime.now();
            }
            let job = new_job(
                context,
                actor,
                &service,
                "reconcile",
                input["dry_run"].as_bool().unwrap_or(false),
                input["activate"].as_bool().unwrap_or(true),
            )?;
            start_job(context, &record, &mut service, job)
        }
        ("POST", ["api", "admin", "provision", "services"] | ["api", "provision", "services"]) => {
            let input: Value = body(request)?;
            let token = required(&input, "token")?;
            let customer = validate_token(context, token, None)?;
            let values = context.api("GET", "/service", token, None)?;
            let own = own_services(context.runtime)?;
            let services:Vec<_>=list(&values)?.iter()
                .filter(|s| s["type"]=="vcl" && s["customer_id"]==customer && s["id"].as_str().is_some_and(|id|!own.iter().any(|v|v==id)))
                .map(|s|json!({"id":s["id"],"name":s["name"],"type":"vcl","customer_id":s["customer_id"]})).collect();
            Ok(json!({"services":services,"customer_id":customer}))
        }
        ("POST", ["api", "admin", "provision", "validate"] | ["api", "provision", "validate"]) => {
            let input: Value = body(request)?;
            let sid = required(&input, "service_id")?;
            let token = required(&input, "token")?;
            let service = preflight(context, token, sid, input["logging_endpoint_name"].as_str())?;
            if let Some(bucket) = input["fos_bucket"].as_str() {
                if !valid_bucket(bucket) {
                    return Err(Error::new(400, "invalid_bucket"));
                }
            }
            if let Some(region) = input["fos_region"].as_str() {
                if !REGIONS.contains(&region) {
                    return Err(Error::new(400, "invalid_region"));
                }
            }
            if let Some(prefix) = input["fos_prefix"].as_str() {
                if !valid_prefix(prefix) {
                    return Err(Error::new(400, "invalid_prefix"));
                }
            }
            Ok(
                json!({"valid":true,"service":{"id":sid,"name":service["name"]},"customer_id":service["customer_id"]}),
            )
        }
        ("POST", ["api", "admin", "provision", "execute"] | ["api", "provision", "execute"]) => {
            provision(context, actor, body(request)?)
        }
        ("POST", ["api", "admin", "provision", "teardown"] | ["api", "provision", "teardown"]) => {
            teardown(context, actor, body(request)?)
        }
        ("POST", ["api", "admin", "services", sid, "delete-data"]) => {
            reset::start(context, actor, sid, body(request)?)
        }
        ("GET", ["api", "admin", "services", sid, "delete-data"]) => reset::status(context, sid),
        ("POST", ["api", "admin", "services", sid, "delete-data", "cancel"]) => {
            reset::cancel(context, actor, sid)
        }
        ("GET", ["api", "admin", "jobs", id]) => {
            Ok(json!({"job":public_job(&load_job(context,id)?.1)}))
        }
        ("POST", ["api", "admin", "jobs", id, "step"]) => {
            let _: serde_json::Map<String, Value> = body(request)?;
            step(context, id, false)
        }
        ("POST", ["api", "admin", "jobs", id, "retry"]) => {
            let _: serde_json::Map<String, Value> = body(request)?;
            retry_job(context, id)
        }
        ("GET", ["api", "admin", "log-fields", "catalog"] | ["api", "log-fields", "catalog"]) => {
            Ok(json!({
                "fields":crate::catalog::get_catalog_for_api(None),"groups":crate::catalog::get_groups_for_api(),
                "insights":crate::catalog::get_insights_for_api(),
                "default_config":default_fields(),"presets":crate::catalog::get_presets_for_api(),
            }))
        }
        (
            "GET",
            ["api", "admin", "log-fields", "config"] | ["api", "admin", "log-fields", "limits"],
        ) => {
            let sid = query_sid(query)?;
            let (record, service) = load_service(context, &sid)?;
            Ok(
                json!({"service_id":sid,"generation":record.generation,"log_fields":service.log_fields,"field_limits":service.field_limits}),
            )
        }
        (
            "PATCH",
            ["api", "admin", "log-fields", "config"] | ["api", "admin", "log-fields", "limits"],
        ) => {
            let mut input: Value = body(request)?;
            let sid = required(&input, "service_id")?.to_owned();
            input
                .as_object_mut()
                .ok_or_else(|| Error::new(400, "invalid_input"))?
                .remove("service_id");
            let result = patch_service(context, actor, &sid, input)?;
            Ok(
                json!({"service_id":sid,"generation":result["generation"],"log_fields":result["service"]["log_fields"],"field_limits":result["service"]["field_limits"]}),
            )
        }
        ("POST", ["api", "admin", "log-fields", "preview"]) => {
            let input: Value = body(request)?;
            let cmcd = input
                .get("cmcd")
                .cloned()
                .map(decode)
                .transpose()?
                .unwrap_or_default();
            let mut fields = input["log_fields"].clone();
            crate::logging::cmcd::reconcile_fields(&mut fields, &cmcd, None, true)
                .map_err(|_| Error::new(400, "system_field_locked"))?;
            validate_fields(&fields, None)?;
            let format = crate::logging::generate_log_format(&fields)
                .map_err(|_| Error::new(400, "invalid_log_format"))?;
            Ok(json!({"format_length":format.len(),"format":format,"max_length":12288}))
        }
        ("POST", ["api", "admin", "log-fields", "custom", "validate"]) => {
            let input: Value = body(request)?;
            let names: Vec<String> = input["existing_names"]
                .as_array()
                .map(|v| {
                    v.iter()
                        .filter_map(|s| s.as_str().map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default();
            let errors = crate::catalog::validate_custom_field(&input["field"], &names);
            Ok(json!({"valid":errors.is_empty(),"errors":errors}))
        }
        ("GET", ["api", "admin", "rum", "versions"] | ["api", "rum", "versions"]) => Ok(json!({
            "versions": crate::logging::rum::SUPPORTED_FARO_VERSIONS,
            "default": crate::logging::rum::DEFAULT_FARO_VERSION,
        })),
        _ => Err(Error::new(404, "not_found")),
    }
}

fn query_sid(query: &str) -> Result<String, Error> {
    url::form_urlencoded::parse(query.as_bytes())
        .find(|(k, _)| k == "service_id")
        .map(|(_, v)| v.into_owned())
        .filter(|v| valid_service_id(v))
        .ok_or_else(|| Error::new(400, "invalid_service_id"))
}
fn services(context: &Context<'_, impl Runtime>) -> Result<Value, Error> {
    let (_, install) = install(context)?;
    let mut result = Vec::new();
    if let Some(sid) = install.service_id {
        match load_service(context, &sid) {
            Ok((record, service)) => result.push(service_value(&service, &record.generation)),
            Err(error) if error.status == 404 => (),
            Err(error) => return Err(error),
        }
    }
    Ok(json!({"services":result}))
}
pub fn retry_job(context: &Context<'_, impl Runtime>, id: &str) -> Result<Value, Error> {
    step(context, id, true)
}

pub fn check_vcl_drift(deployed_vcl_sha: Option<&str>, current_vcl_sha: &str) -> bool {
    deployed_vcl_sha.is_some_and(|sha| sha != current_vcl_sha)
}

fn own_services(runtime: &impl Runtime) -> Result<Vec<String>, Error> {
    ["flp_service_id", "worker_service_id", "front_service_id"]
        .iter()
        .map(|key| config(runtime, key))
        .collect()
}

fn validate_token(
    context: &Context<'_, impl Runtime>,
    token: &str,
    sid: Option<&str>,
) -> Result<String, Error> {
    if token.is_empty() || token.len() > 1024 || !token.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(Error::new(400, "invalid_token"));
    }
    let info = context.api("GET", "/tokens/self", token, None)?;
    let scope = info["scope"].as_str().unwrap_or("");
    let global = scope
        .split(|c: char| c.is_ascii_whitespace() || c == ',')
        .any(|v| v == "global")
        || info["scope"]
            .as_array()
            .is_some_and(|v| v.iter().any(|s| s == "global"));
    if !global {
        return Err(Error::new(403, "global_scope_required"));
    }
    let customer = required(&info, "customer_id")
        .map_err(|_| Error::new(403, "token_customer_missing"))?
        .to_owned();
    if info["user_id"].as_str().is_some_and(|id| !id.is_empty()) {
        let user = context.api("GET", "/current_user", token, None)?;
        let role = user["role"].as_str().unwrap_or("");
        if role != "superuser" && !(role == "engineer" && user["limit_services"] == false) {
            return Err(Error::new(403, "admin_token_required"));
        }
    } else {
        let role = if let Some(role) = info["role"].as_str() {
            role.to_owned()
        } else {
            let id = info["id"]
                .as_str()
                .filter(|id| valid_service_id(id))
                .ok_or_else(|| Error::new(403, "token_permissions_unverified"))?;
            let details = context.api("GET", &format!("/automation-tokens/{id}"), token, None)?;
            details["role"].as_str().unwrap_or("").to_owned()
        };
        if !matches!(role.as_str(), "engineer" | "superuser") {
            return Err(Error::new(403, "admin_token_required"));
        }
    }
    let own = own_services(context.runtime)?;
    if let Some(allowed) = info["services"].as_array().filter(|v| !v.is_empty()) {
        for id in own.iter().chain(
            sid.into_iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .iter(),
        ) {
            if !allowed
                .iter()
                .any(|v| v.as_str() == Some(id.as_str()) || v["id"].as_str() == Some(id.as_str()))
            {
                return Err(Error::new(403, "service_permission_required"));
            }
        }
    }
    let app = context.api("GET", &format!("/service/{}", own[0]), token, None)?;
    if app["customer_id"] != customer {
        return Err(Error::new(403, "wrong_account"));
    }
    Ok(customer)
}

fn preflight(
    context: &Context<'_, impl Runtime>,
    token: &str,
    sid: &str,
    endpoint: Option<&str>,
) -> Result<Value, Error> {
    if !valid_service_id(sid) {
        return Err(Error::new(400, "invalid_service_id"));
    }
    if own_services(context.runtime)?.iter().any(|v| v == sid) {
        return Err(Error::new(400, "install_service_forbidden"));
    }
    let customer = validate_token(context, token, Some(sid))?;
    let service = context.api("GET", &format!("/service/{sid}"), token, None)?;
    if service["customer_id"] != customer {
        return Err(Error::new(403, "wrong_account"));
    }
    if service["type"] != "vcl" {
        return Err(Error::new(400, "vcl_service_required"));
    }
    context
        .api("GET", "/enabled-products/v1/object_storage", token, None)
        .map_err(|_| Error::new(400, "object_storage_required"))?;
    let (_, reservation) = install(context)?;
    if reservation.service_id.as_deref().is_some_and(|v| v != sid) {
        return Err(Error::new(409, "install_already_reserved"));
    }
    let local = context.get(&format!("svc:{sid}"))?;
    let owned = local
        .as_ref()
        .is_some_and(|r| r.value["owner"].as_str() == owner(context.runtime).ok().as_deref());
    let version = reconcile::active_version(context, sid, token)?;
    let own_marker = format!("flp-owner-{}", owner(context.runtime)?);
    let owned_endpoints = local
        .as_ref()
        .and_then(|r| r.value["owned_endpoint_names"].as_array());
    for resource in ["snippet", "logging/s3", "condition"] {
        let current = context.api(
            "GET",
            &format!("/service/{sid}/version/{version}/{resource}"),
            token,
            None,
        )?;
        for value in list(&current)? {
            let name = value["name"].as_str().unwrap_or("");
            if endpoint.is_some() && resource == "snippet" && name == "CMCD Extraction" {
                return Err(Error::new(409, "resource_name_collision"));
            }
            if name.starts_with("flp-owner-") && name != own_marker
                || !owned && reconcile::managed_name(name)
            {
                return Err(Error::new(409, "service_managed_by_other_install"));
            }
            if endpoint == Some(name)
                && !owned_endpoints.is_some_and(|names| names.iter().any(|v| v == name))
            {
                return Err(Error::new(409, "resource_name_collision"));
            }
        }
    }
    Ok(service)
}

fn check_generation(record: &Record, input: &Value) -> Result<(), Error> {
    if input["generation"].as_str() != Some(&record.generation) {
        Err(Error::new(409, "generation_conflict"))
    } else {
        Ok(())
    }
}
fn validate_fields(fields: &Value, previous: Option<&Value>) -> Result<(), Error> {
    if !fields.is_object() || fields["groups"].as_array().is_none_or(|v| v.len() > 32) {
        return Err(Error::new(400, "invalid_log_fields"));
    }
    let custom = fields["custom_fields"]
        .as_array()
        .ok_or_else(|| Error::new(400, "invalid_custom_fields"))?;
    let groups = crate::catalog::get_groups_for_api();
    if fields["groups"].as_array().is_some_and(|values| {
        values.iter().any(|id| {
            !id.is_string()
                || !groups
                    .as_array()
                    .into_iter()
                    .flatten()
                    .any(|g| g["id"] == *id)
        })
    }) {
        return Err(Error::new(400, "unknown_field_group"));
    }
    if fields["field_overrides"]
        .as_object()
        .is_none_or(|m| m.values().any(|v| !v.is_boolean()))
    {
        return Err(Error::new(400, "invalid_field_overrides"));
    }
    if custom.len() > 64 {
        return Err(Error::new(400, "too_many_custom_fields"));
    }
    let mut names = Vec::new();
    for field in custom {
        if !crate::logging::cmcd::managed_field(field)
            && !crate::catalog::validate_custom_field(field, &names).is_empty()
        {
            return Err(Error::new(400, "invalid_custom_field"));
        }
        names.push(required(field, "name")?.to_owned());
    }
    if let Some(previous) = previous {
        for old in previous["custom_fields"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|v| v["committed"] == true && !crate::logging::cmcd::managed_field(v))
        {
            let next = custom
                .iter()
                .find(|f| f["name"] == old["name"])
                .ok_or_else(|| Error::new(409, "committed_field_locked"))?;
            let old_normalized = crate::catalog::normalize_custom_field(old);
            let next_normalized = crate::catalog::normalize_custom_field(next);
            let old_type = &old_normalized["duckdb_type"];
            let next_type = &next_normalized["duckdb_type"];
            if old_type != next_type || next["committed"] != true {
                return Err(Error::new(409, "committed_field_locked"));
            }
        }
        for field in custom
            .iter()
            .filter(|v| v["committed"] == true && !crate::logging::cmcd::managed_field(v))
        {
            if !previous["custom_fields"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|v| v["name"] == field["name"] && v["committed"] == true)
            {
                return Err(Error::new(400, "committed_flag_reserved"));
            }
        }
    } else if custom
        .iter()
        .any(|v| v["committed"] == true && !crate::logging::cmcd::managed_field(v))
    {
        return Err(Error::new(400, "committed_flag_reserved"));
    }
    crate::logging::generate_log_format(fields)
        .map_err(|_| Error::new(400, "invalid_log_format"))?;
    Ok(())
}
fn reconcile_system_fields(service: &mut Service, previous: Option<&Service>) -> Result<(), Error> {
    crate::logging::cmcd::reconcile_fields(
        &mut service.log_fields,
        &service.cmcd,
        previous.map(|s| &s.log_fields),
        false,
    )
    .map_err(|_| Error::new(400, "system_field_locked"))
}
fn validate_service(
    service: &Service,
    previous: Option<&Service>,
    window_s: i64,
) -> Result<(), Error> {
    if !valid_bucket(&service.fos_bucket)
        || !valid_prefix(&service.fos_prefix)
        || !REGIONS.contains(&service.fos_region.as_str())
    {
        return Err(Error::new(400, "invalid_storage_configuration"));
    }
    if !(1..=3600).contains(&service.log_period)
        || !(1..=100).contains(&service.sample_rate)
        || service.logging_endpoint_name.is_empty()
        || service.logging_endpoint_name.len() > 80
        || service
            .logging_endpoint_name
            .chars()
            .any(|c| c.is_control())
        || service.custom_condition.len() > 2048
        || service
            .custom_condition
            .contains(['\n', '\r', ';', '{', '}'])
        || !matches!(service.compression_codec.as_str(), "" | "zstd")
        || !matches!(service.wire_format.as_str(), "" | "array")
    {
        return Err(Error::new(400, "invalid_logging_settings"));
    }
    if crate::ingest::object_windows(window_s, service.log_period)
        > crate::ingest::MAX_WINDOWS_PER_OBJECT as u64
    {
        return Err(Error::new(400, "log_period_exceeds_window_capacity"));
    }
    if service.rum.enabled {
        crate::logging::rum::validate_config(&service.rum)
            .map_err(|_| Error::new(400, "invalid_rum_settings"))?;
        if crate::ingest::object_windows(window_s, service.rum.log_period)
            > crate::ingest::MAX_WINDOWS_PER_OBJECT as u64
        {
            return Err(Error::new(400, "rum_log_period_exceeds_window_capacity"));
        }
        if service.rum.endpoint_name.is_empty()
            || service.rum.endpoint_name.len() > 80
            || service.rum.endpoint_name.chars().any(|c| c.is_control())
            || (service.logging_enabled
                && service.rum.endpoint_name == service.logging_endpoint_name)
        {
            return Err(Error::new(400, "invalid_rum_settings"));
        }
    }
    if service.log_retention_days > 3_650 || service.rum_retention_days > 3_650 {
        return Err(Error::new(400, "invalid_retention_days"));
    }
    if !service.compaction.valid() {
        return Err(Error::new(400, "invalid_compaction"));
    }
    if !service.field_limits.is_object()
        || service.field_limits.as_object().is_some_and(|m| {
            m.len() > 256 || m.values().any(|v| v.as_u64().is_none_or(|n| n > 16384))
        })
    {
        return Err(Error::new(400, "invalid_field_limits"));
    }
    validate_fields(&service.log_fields, previous.map(|s| &s.log_fields))
}

fn patch_service(
    context: &Context<'_, impl Runtime>,
    actor: &str,
    sid: &str,
    input: Value,
) -> Result<Value, Error> {
    let (record, previous) = load_service(context, sid)?;
    check_generation(&record, &input)?;
    if previous.pending_job_id.is_some() {
        return Err(Error::new(409, "job_in_progress"));
    }
    check_unlocked(context, sid)?;
    let mut value = serialize(&previous)?;
    let allowed = [
        "logging_enabled",
        "logging_endpoint_name",
        "log_period",
        "sample_rate",
        "edge_only",
        "custom_condition",
        "fos_prefix",
        "log_fields",
        "field_limits",
        "cmcd",
        "rum",
        "rum_enabled",
        "rum_log_period",
        "rum_custom_condition",
        "faro_version",
        "rum_endpoint_name",
        "capture_vitals",
        "capture_performance",
        "capture_errors",
        "capture_events",
        "delete_after",
        "log_retention_days",
        "rum_retention_days",
        "compaction",
        "compression_codec",
        "wire_format",
        "generation",
    ];
    let mut rum_obj = value["rum"].as_object().cloned().unwrap_or_default();
    for (key, next) in input
        .as_object()
        .ok_or_else(|| Error::new(400, "invalid_input"))?
    {
        if !allowed.contains(&key.as_str()) {
            return Err(Error::new(400, "unknown_setting"));
        }
        match key.as_str() {
            "generation" => (),
            "rum" => {
                let m = next
                    .as_object()
                    .ok_or_else(|| Error::new(400, "invalid_input"))?;
                for (k, val) in m {
                    rum_obj.insert(k.clone(), val.clone());
                }
            }
            "rum_enabled" => {
                rum_obj.insert("enabled".into(), next.clone());
            }
            "rum_log_period" => {
                rum_obj.insert("log_period".into(), next.clone());
            }
            "rum_custom_condition" => {
                rum_obj.insert("custom_condition".into(), next.clone());
            }
            "faro_version" => {
                rum_obj.insert("faro_version".into(), next.clone());
            }
            "rum_endpoint_name" => {
                rum_obj.insert("endpoint_name".into(), next.clone());
            }
            "capture_vitals" => {
                rum_obj.insert("capture_vitals".into(), next.clone());
            }
            "capture_performance" => {
                rum_obj.insert("capture_performance".into(), next.clone());
            }
            "capture_errors" => {
                rum_obj.insert("capture_errors".into(), next.clone());
            }
            "capture_events" => {
                rum_obj.insert("capture_events".into(), next.clone());
            }
            _ => {
                value[key] = next.clone();
            }
        }
    }
    value["rum"] = Value::Object(rum_obj);
    let mut service: Service = decode(value)?;
    if service.compression_codec.is_empty() {
        service.compression_codec = "zstd".into();
    }
    if service.wire_format.is_empty() {
        service.wire_format = "array".into();
    }
    if service.log_fields.get("field_limits").is_some() && input.get("log_fields").is_some() {
        service.field_limits = service.log_fields["field_limits"].clone();
    } else {
        service.log_fields["field_limits"] = service.field_limits.clone();
    }
    reconcile_system_fields(&mut service, Some(&previous))?;
    validate_service(
        &service,
        Some(&previous),
        crate::ingest::window_seconds(context.runtime),
    )?;
    service.config_version += 1;
    service.updated_at = context.runtime.now();
    save_service(context, &record, &service, actor, "config.updated")?;
    let (record, service) = load_service(context, sid)?;
    Ok(json!({"service":service_value(&service,&record.generation),"generation":record.generation}))
}

fn new_job(
    context: &Context<'_, impl Runtime>,
    actor: &str,
    service: &Service,
    kind: &str,
    dry_run: bool,
    activate: bool,
) -> Result<Job, Error> {
    let id = crate::auth::encode(
        &context
            .runtime
            .random()
            .map_err(|_| Error::new(503, "random_unavailable"))?,
    );
    let now = context.runtime.now();
    Ok(Job {
        id,
        service_id: service.service_id.clone(),
        kind: kind.into(),
        status: "pending".into(),
        step: if kind == "provision" {
            "bucket"
        } else {
            "discover"
        }
        .into(),
        completed_steps: vec![],
        error: None,
        draft_version: None,
        changes: vec![],
        created_at: now,
        updated_at: now,
        actor: actor.into(),
        expected_generation: String::new(),
        source_version: None,
        change_cursor: 0,
        lease_until: 0,
        fence: 0,
        dry_run,
        activate,
        options: Teardown::default(),
        front_source_version: None,
        front_draft_version: None,
        front_cursor: 0,
        clone_intent: None,
        front_clone_intent: None,
        front_changes: vec![],
        service_snapshot: None,
        previous_endpoint_names: service.owned_endpoint_names.clone(),
        retry: None,
        retry_attempts: Default::default(),
        uncertain_operations: Default::default(),
    })
}
fn start_job(
    context: &Context<'_, impl Runtime>,
    record: &Record,
    service: &mut Service,
    mut job: Job,
) -> Result<Value, Error> {
    context.confirm_with_secret(&service.fastly_token_ref);
    if let Some(id) = &service.pending_job_id {
        let (_, previous) = load_job(context, id)?;
        if previous.status != "complete" {
            return Err(Error::new(409, "job_in_progress"));
        }
    }
    check_unlocked(context, &service.service_id)?;
    service.pending_job_id = Some(job.id.clone());
    job.expected_generation = record.generation.clone();
    context.cas(&format!("job:{}", job.id), None, &serialize(&job)?)?;
    save_job_service(context, record, service, &job, "job.started", &job.step)?;
    let current = context
        .get(&format!("svc:{}", service.service_id))?
        .ok_or_else(|| Error::new(503, "state_unavailable"))?;
    job.expected_generation = current.generation.clone();
    Ok(
        json!({"job":public_job(&job),"service":service_value(service,&current.generation),"generation":current.generation}),
    )
}
fn save_service(
    context: &Context<'_, impl Runtime>,
    record: &Record,
    service: &Service,
    actor: &str,
    event: &str,
) -> Result<(), Error> {
    context.confirm_with_secret(&service.fastly_token_ref);
    let mut value = serialize(service)?;
    value["_audit"] = json!({"actor":actor,"at":context.runtime.now(),"event":event,"config_version":service.config_version});
    let mut audit = record.value["_audit_outbox"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    if audit.len() >= 4096 {
        return Err(Error::new(409, "audit_archive_required"));
    }
    audit.push(value["_audit"].clone());
    value["_audit_outbox"] = audit.into();
    value["_audit_outbox_base"] = record.value["_audit_outbox_base"].clone();
    context.cas(
        &format!("svc:{}", service.service_id),
        Some(&record.generation),
        &value,
    )
}
fn save_job_service(
    context: &Context<'_, impl Runtime>,
    record: &Record,
    service: &Service,
    job: &Job,
    event: &str,
    next: &str,
) -> Result<(), Error> {
    context.confirm_with_secret(&service.fastly_token_ref);
    let mut value = serialize(service)?;
    value["_audit"] = json!({"actor":job.actor,"at":context.runtime.now(),"event":event,"job_id":job.id,"config_version":service.config_version});
    let mut audit = record.value["_audit_outbox"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    if audit.len() >= 4096 {
        return Err(Error::new(409, "audit_archive_required"));
    }
    audit.push(value["_audit"].clone());
    value["_audit_outbox"] = audit.into();
    value["_audit_outbox_base"] = record.value["_audit_outbox_base"].clone();
    value["_job_checkpoint"] =
        json!({"job_id":job.id,"from_generation":record.generation,"next":next});
    context.cas(
        &format!("svc:{}", service.service_id),
        Some(&record.generation),
        &value,
    )
}

fn provision(
    context: &Context<'_, impl Runtime>,
    actor: &str,
    input: Provision,
) -> Result<Value, Error> {
    let rum = input.effective_rum();
    if !input.logging_enabled && !rum.enabled {
        return Err(Error::new(400, "invalid_logging_settings"));
    }
    let target = preflight(
        context,
        &input.token,
        &input.service_id,
        Some(&input.logging_endpoint_name),
    )?;
    context.confirm_with_token(&input.token);
    let now = context.runtime.now();
    let mut owned_endpoint_names = Vec::new();
    if input.logging_enabled {
        owned_endpoint_names.push(input.logging_endpoint_name.clone());
    }
    if rum.enabled && !owned_endpoint_names.contains(&rum.endpoint_name) {
        owned_endpoint_names.push(rum.endpoint_name.clone());
    }
    let mut service = Service {
        service_id: input.service_id.clone(),
        name: target["name"]
            .as_str()
            .unwrap_or(&input.service_id)
            .to_owned(),
        status: "provisioning".into(),
        created_at: now,
        updated_at: now,
        config_version: 1,
        logging_enabled: input.logging_enabled,
        logging_endpoint_name: input.logging_endpoint_name.clone(),
        log_period: input.log_period,
        sample_rate: input.sample_rate,
        edge_only: input.edge_only,
        custom_condition: input.custom_condition,
        fos_bucket: input.fos_bucket,
        fos_prefix: input.fos_prefix,
        fos_region: input.fos_region.clone(),
        fos_endpoint: format!("{}.object.fastlystorage.app", input.fos_region),
        field_limits: input
            .log_fields
            .get("field_limits")
            .cloned()
            .unwrap_or(json!({})),
        log_fields: input.log_fields,
        cmcd: input.cmcd,
        rum,
        fastly_token_ref: format!("fastly_token.{}", input.service_id),
        fos_access_key_ref: format!("fos_ak.{}", input.service_id),
        fos_secret_key_ref: format!("fos_sk.{}", input.service_id),
        owner: owner(context.runtime)?,
        bucket_adopted: false,
        bucket_checked: false,
        last_activated_version: None,
        last_reconcile: None,
        pending_job_id: None,
        owned_endpoint_names,
        delete_after: input.delete_after,
        log_retention_days: input.log_retention_days,
        rum_retention_days: input.rum_retention_days,
        compaction: input.compaction.clone(),
        data_reset: None,
        compression_codec: if input.compression_codec.is_empty() {
            "zstd".into()
        } else {
            input.compression_codec
        },
        wire_format: if input.wire_format.is_empty() {
            "array".into()
        } else {
            input.wire_format
        },
    };
    let previous: Option<Service> = context
        .get(&format!("svc:{}", input.service_id))?
        .map(|record| decode(record.value))
        .transpose()?;
    reconcile_system_fields(&mut service, previous.as_ref())?;
    validate_service(
        &service,
        None,
        crate::ingest::window_seconds(context.runtime),
    )?;
    let (reservation, mut install) = install(context)?;
    if install
        .service_id
        .as_deref()
        .is_some_and(|sid| sid != input.service_id)
    {
        return Err(Error::new(409, "install_already_reserved"));
    }
    if let Some(id) = &install.job_id {
        if let Some(record) = context.get(&format!("job:{id}"))? {
            let job: Job = decode(record.value)?;
            if job.kind == "provision" && job.status != "complete" {
                let record = context.get(&format!("svc:{}", input.service_id))?;
                let service = if let Some(record) = &record {
                    decode(record.value.clone())?
                } else {
                    let service = job
                        .service_snapshot
                        .clone()
                        .ok_or_else(|| Error::new(409, "provision_recovery_required"))?;
                    put_secret(
                        context,
                        &input.token,
                        &service.fastly_token_ref,
                        &input.token,
                    )?;
                    context.cas(
                        &format!("svc:{}", input.service_id),
                        None,
                        &serialize(&service)?,
                    )?;
                    service
                };
                let record = if let Some(record) = record {
                    record
                } else {
                    context
                        .get(&format!("svc:{}", input.service_id))?
                        .ok_or_else(|| Error::new(503, "state_unavailable"))?
                };
                return Ok(
                    json!({"job":public_job(&job),"service":service_value(&service,&record.generation),"generation":record.generation}),
                );
            }
        }
    }
    if let Some(record) = context.get(&format!("svc:{}", input.service_id))? {
        let existing: Service = decode(record.value)?;
        if existing.status != "torn_down" {
            return Err(Error::new(409, "service_already_configured"));
        }
        for name in existing.owned_endpoint_names {
            if !service.owned_endpoint_names.contains(&name) {
                service.owned_endpoint_names.push(name);
            }
        }
    }
    let mut job = new_job(context, actor, &service, "provision", false, true)?;
    if install.service_id.as_deref() == Some(&input.service_id) {
        if let Some(id) = &install.job_id {
            job.id = id.clone();
        }
    }
    install.service_id = Some(input.service_id.clone());
    install.job_id = Some(job.id.clone());
    install.fence += 1;
    context.cas(
        "admin:install",
        reservation.as_ref().map(|r| r.generation.as_str()),
        &serialize(&install)?,
    )?;
    put_secret(
        context,
        &input.token,
        &service.fastly_token_ref,
        &input.token,
    )?;
    service.pending_job_id = Some(job.id.clone());
    job.service_snapshot = Some(service.clone());
    context.cas(&format!("job:{}", job.id), None, &serialize(&job)?)?;
    let previous = context.get(&format!("svc:{}", input.service_id))?;
    context.cas(
        &format!("svc:{}", input.service_id),
        previous.as_ref().map(|r| r.generation.as_str()),
        &serialize(&service)?,
    )?;
    let record = context
        .get(&format!("svc:{}", input.service_id))?
        .ok_or_else(|| Error::new(503, "state_unavailable"))?;
    job.expected_generation = record.generation.clone();
    Ok(
        json!({"job":public_job(&job),"service":service_value(&service,&record.generation),"generation":record.generation}),
    )
}

fn teardown(
    context: &Context<'_, impl Runtime>,
    actor: &str,
    mut input: Value,
) -> Result<Value, Error> {
    let sid = required(&input, "service_id")?.to_owned();
    if input["confirm_service_id"] != sid {
        return Err(Error::new(400, "typed_service_id_required"));
    }
    let token = required(&input, "token")?.to_owned();
    preflight(context, &token, &sid, None)?;
    context.confirm_with_token(&token);
    let (record, mut service) = load_service(context, &sid)?;
    check_generation(&record, &input)?;
    if service.status == "torn_down" {
        return Err(Error::new(409, "service_torn_down"));
    }
    let object = input
        .as_object_mut()
        .ok_or_else(|| Error::new(400, "invalid_input"))?;
    for key in ["service_id", "confirm_service_id", "token", "generation"] {
        object.remove(key);
    }
    let options: Teardown = decode(input)?;
    if (!options.remove_logging || !options.remove_snippets)
        && (options.delete_fos_keys || options.delete_data || options.delete_fos_bucket)
    {
        return Err(Error::new(400, "remove_config_before_credentials"));
    }
    if service.bucket_adopted
        && (options.delete_fos_bucket || options.delete_data && service.fos_prefix.is_empty())
    {
        return Err(Error::new(409, "adopted_bucket_protected"));
    }
    if let Some(previous) = &service.pending_job_id {
        let (_, old) = load_job(context, previous)?;
        if old.status == "running" && old.lease_until > context.runtime.now() {
            return Err(Error::new(409, "job_in_progress"));
        }
        service.pending_job_id = None;
    }
    let mut job = new_job(context, actor, &service, "teardown", false, true)?;
    job.options = options;
    put_secret(context, &token, &service.fastly_token_ref, &token)?;
    start_job(context, &record, &mut service, job)
}

fn put_secret(
    context: &Context<'_, impl Runtime>,
    token: &str,
    name: &str,
    value: &str,
) -> Result<(), Error> {
    let store = config(context.runtime, "secret_store_id")?;
    let path = format!("/resources/stores/secret/{store}/secrets");
    let body = json!({"name":name,"secret":base64::engine::general_purpose::STANDARD.encode(value.as_bytes())});
    if context.api_uncertain("PUT", &path, Some(&body)) {
        if context.secret(name)? != value {
            return Err(Error::new(409, "mutation_outcome_unknown"));
        }
        context.confirm_api_effect("PUT", &path, Some(&body));
        return Ok(());
    }
    context.api("PUT", &path, token, Some(body))?;
    Ok(())
}
fn load_job(context: &Context<'_, impl Runtime>, id: &str) -> Result<(Record, Job), Error> {
    if id.is_empty()
        || id.len() > 64
        || !id
            .bytes()
            .all(|v| v.is_ascii_alphanumeric() || matches!(v, b'-' | b'_'))
    {
        return Err(Error::new(400, "invalid_job_id"));
    }
    let record = context
        .get(&format!("job:{id}"))?
        .ok_or_else(|| Error::new(404, "job_not_found"))?;
    let job: Job = decode(record.value.clone())?;
    Ok((record, job))
}
fn check_unlocked(context: &Context<'_, impl Runtime>, sid: &str) -> Result<(), Error> {
    if context
        .get(&format!("reconcile_lock:{sid}"))?
        .is_some_and(|r| r.value["expires_at"].as_u64().unwrap_or(0) > context.runtime.now())
    {
        return Err(Error::new(409, "service_locked"));
    }
    Ok(())
}

fn step(context: &Context<'_, impl Runtime>, id: &str, retry: bool) -> Result<Value, Error> {
    let (record, mut job) = load_job(context, id)?;
    if job.status == "complete" || job.status == "failed" && !retry {
        return Ok(json!({"job":public_job(&job)}));
    }
    if retry && job.status == "failed" {
        job.retry = None;
        job.retry_attempts.clear();
    }
    let now = context.runtime.now();
    if job.retry.as_ref().is_some_and(|retry| retry.at > now) {
        return Ok(json!({"job":public_job(&job)}));
    }
    if job.lease_until > now {
        return Err(Error::new(409, "job_leased"));
    }
    if context.runtime.admin_remaining_calls() < 14 {
        return Err(Error::new(503, "backend_budget"));
    }
    // Provision fixes this reference; settings cannot change it.
    let token_ref = format!("fastly_token.{}", job.service_id);
    context.confirm_with_secret(if job.step == "finalize" {
        "fastly_token"
    } else {
        &token_ref
    });
    let lease_key = format!("reconcile_lock:{}", job.service_id);
    let lease_record = context.get(&lease_key)?;
    let old: Lease = lease_record
        .as_ref()
        .map(|r| decode(r.value.clone()))
        .transpose()?
        .unwrap_or_default();
    if old.expires_at > now {
        return Err(Error::new(409, "service_locked"));
    }
    let lease = Lease {
        job_id: job.id.clone(),
        fence: old.fence + 1,
        expires_at: now + 60,
    };
    context.cas(
        &lease_key,
        lease_record.as_ref().map(|r| r.generation.as_str()),
        &serialize(&lease)?,
    )?;
    job.fence = lease.fence;
    job.lease_until = lease.expires_at;
    job.status = "running".into();
    job.error = None;
    let claim = context
        .cas(
            &format!("job:{id}"),
            Some(&record.generation),
            &serialize(&job)?,
        )
        .and_then(|()| {
            context
                .get(&format!("job:{id}"))?
                .ok_or_else(|| Error::new(503, "state_unavailable"))
        });
    let claimed = match claim {
        Ok(claimed) => claimed,
        Err(failure) => {
            // A stale job read must not strand the service lease for its full term.
            release_lease(context, &lease_key, &lease);
            return Err(failure);
        }
    };
    let previous_step = job.step.clone();
    let claimed_uncertain = job.uncertain_operations.clone();
    context.set_retry(job.retry.clone());
    context.set_retry_attempts(job.retry_attempts.clone());
    context.set_uncertain_operations(job.uncertain_operations.clone());
    context.journal_job(format!("job:{id}"), claimed.clone());
    let result = load_service(context, &job.service_id)
        .and_then(|current| run_step(context, &mut job, current));
    if result.is_ok() {
        context.settle_secret_propagation();
    }
    if let Err(mut failure) = result {
        if failure.code == "credentials_pending" && failure.retry.is_none() {
            failure.retry = Some(context.secret_propagation_retry());
        }
        job.retry = failure.retry;
        if job.retry.as_ref().is_some_and(|retry| retry.at > 0) {
            job.status = "pending".into();
            job.error = None;
        } else {
            job.status = "failed".into();
            job.error = Some(failure.code.into());
        }
    } else if job.status != "complete" {
        job.status = "pending".into();
        job.retry = if job.step == previous_step {
            context.retry()
        } else {
            None
        };
    } else {
        job.retry = None;
    }
    job.retry_attempts = if job.step == previous_step {
        context.retry_attempts()
    } else {
        Default::default()
    };
    job.uncertain_operations = context.uncertain_operations();
    job.updated_at = context.runtime.now();
    job.lease_until = 0;
    let checkpoint_gen = context.checkpoint_generation(&format!("job:{id}"), &claimed.generation);
    if let Err(failure) = context.cas(
        &format!("job:{id}"),
        Some(&checkpoint_gen),
        &serialize(&job)?,
    ) {
        if job.uncertain_operations == claimed_uncertain {
            release_lease(context, &lease_key, &lease);
        }
        return Err(failure);
    }
    let current = context
        .get(&lease_key)?
        .ok_or_else(|| Error::new(503, "lease_unavailable"))?;
    if current.value["fence"].as_u64() != Some(job.fence) {
        return Err(Error::new(409, "lease_fenced"));
    }
    context.cas(
        &lease_key,
        Some(&current.generation),
        &serialize(&Lease {
            expires_at: 0,
            ..lease
        })?,
    )?;
    context.event(json!({"event":"admin.job.step","level":if job.error.is_some() {"error"} else {"info"},"job_id":id,"kind":job.kind,"step":job.step,"status":job.status,"at":job.updated_at}));
    Ok(json!({"job":public_job(&job)}))
}

fn release_lease(context: &Context<'_, impl Runtime>, key: &str, lease: &Lease) {
    let Ok(Some(current)) = context.get(key) else {
        return;
    };
    if current.value["fence"].as_u64() != Some(lease.fence) {
        return;
    }
    if let Ok(value) = serialize(&Lease {
        expires_at: 0,
        ..lease.clone()
    }) {
        let _ = context.cas(key, Some(&current.generation), &value);
    }
}

fn fence(context: &Context<'_, impl Runtime>, job: &Job) -> Result<(), Error> {
    context.check()?;
    if context.runtime.now() + 10 >= job.lease_until {
        return Err(Error::new(409, "lease_expired"));
    }
    let lease = context
        .get(&format!("reconcile_lock:{}", job.service_id))?
        .ok_or_else(|| Error::new(409, "lease_fenced"))?;
    if lease.value["fence"].as_u64() != Some(job.fence) || lease.value["job_id"] != job.id {
        return Err(Error::new(409, "lease_fenced"));
    }
    Ok(())
}
fn advance(job: &mut Job, next: &str) {
    if !job.completed_steps.contains(&job.step) {
        job.completed_steps.push(job.step.clone());
    }
    job.step = next.into();
}

fn run_step(
    context: &Context<'_, impl Runtime>,
    job: &mut Job,
    (record, mut service): (Record, Service),
) -> Result<(), Error> {
    if record.generation != job.expected_generation {
        let checkpoint = &record.value["_job_checkpoint"];
        if checkpoint["job_id"] == job.id
            && checkpoint["from_generation"] == job.expected_generation
        {
            job.expected_generation = record.generation.clone();
            let next = checkpoint["next"]
                .as_str()
                .ok_or_else(|| Error::new(409, "invalid_job_checkpoint"))?;
            job.step = next.into();
        } else if job.expected_generation.is_empty()
            && job.kind == "provision"
            && job
                .service_snapshot
                .as_ref()
                .is_some_and(|snapshot| serialize(snapshot).ok() == serialize(&service).ok())
        {
            job.expected_generation = record.generation.clone();
        } else {
            return Err(Error::new(409, "service_changed_rerun"));
        }
    }
    if service.pending_job_id.as_deref() != Some(&job.id)
        && !(job.step == "finalize"
            && record.value["_job_checkpoint"]["job_id"] == job.id
            && service.pending_job_id.is_none())
    {
        return Err(Error::new(409, "service_changed_rerun"));
    }
    let token = if job.step == "finalize" {
        String::new()
    } else {
        context.secret(&service.fastly_token_ref)?
    };
    fence(context, job)?;
    match job.step.as_str() {
        "bucket" | "bucket_probe" | "bucket_create" => {
            fos::bucket(context, job, &mut service, &record, &token)
        }
        "key" => fos::permanent_key(context, job, &service, &token),
        "cleanup_temp" => {
            if fos::cleanup_temp(context, &service, &token)? {
                advance(job, "cleanup_temp_secrets");
            }
            Ok(())
        }
        "cleanup_temp_secrets" => {
            fos::cleanup_temp_secrets(context, &service, &token)?;
            advance(job, "front");
            Ok(())
        }
        "front" => reconcile::front(context, job, &service, &token, false),
        "discover" | "clone" | "apply" | "validate" | "activate" | "sync_rum" => {
            reconcile::run(context, job, &service, &token)
        }
        "remove_front" => reconcile::front(context, job, &service, &token, true),
        "delete_objects" | "delete_page" | "delete_bucket" => {
            fos::delete(context, job, &service, &token)
        }
        "revoke"
        | "revoke_temp"
        | "revoke_secret_ak"
        | "revoke_secret_sk"
        | "revoke_secret_tmp_ak"
        | "revoke_secret_tmp_sk"
        | "revoke_secret_token" => fos::revoke(context, job, &service, &token),
        "finalize" => {
            if job.kind == "teardown" {
                fos::cleanup_token(context, &service)?;
            }
            service.status = if job.kind == "teardown" {
                "torn_down"
            } else {
                "active"
            }
            .into();
            service.pending_job_id = None;
            service.updated_at = context.runtime.now();
            service.owned_endpoint_names = if !job.activate || job.dry_run {
                job.previous_endpoint_names.clone()
            } else if job.kind == "teardown" {
                if job.options.remove_logging {
                    vec![]
                } else {
                    service.owned_endpoint_names.clone()
                }
            } else {
                let mut names = Vec::new();
                if service.logging_enabled {
                    names.push(service.logging_endpoint_name.clone());
                }
                if service.rum.enabled && !names.contains(&service.rum.endpoint_name) {
                    names.push(service.rum.endpoint_name.clone());
                }
                names
            };
            if job.activate && !job.dry_run {
                service.last_activated_version =
                    job.draft_version.or(service.last_activated_version);
            }
            service.last_reconcile = Some(
                json!({"at":context.runtime.now(),"outcome":"complete","dry_run":job.dry_run,"activated":job.activate,"changes":public_job(job)["changes"]}),
            );
            save_job_service(context, &record, &service, job, "job.completed", "finalize")?;
            if job.kind == "teardown" {
                let (record, mut install) = install(context)?;
                if install.service_id.as_deref() == Some(&service.service_id) {
                    install.service_id = None;
                    install.job_id = None;
                    context.cas(
                        "admin:install",
                        record.as_ref().map(|r| r.generation.as_str()),
                        &serialize(&install)?,
                    )?;
                }
            }
            job.status = "complete".into();
            advance(job, "complete");
            Ok(())
        }
        _ => Err(Error::new(409, "invalid_job_step")),
    }
}

/// The front service's `/_tick` and `/_flush` health checks probe once a second.
pub const CLOCK_INTERVAL_S: u64 = 1;
/// The `bucket` label on cron_runs rows: two-minute buckets, as before the 1 s dispatcher.
pub const ROW_BUCKET_S: u64 = 120;
const MAX_HISTORY_ENTRIES: usize = 50;

pub fn rfc3339_from_secs(seconds: u64) -> String {
    compute_common::runtime::rfc3339_from_secs(seconds)
}

pub fn tick_timestamps(last_tick: Option<u64>) -> (Option<String>, Option<String>) {
    (
        last_tick.map(rfc3339_from_secs),
        last_tick.map(|ts| rfc3339_from_secs(ts.saturating_add(CLOCK_INTERVAL_S))),
    )
}

pub fn tick_timestamps_for(
    runtime: &impl Runtime,
    last_tick: Option<u64>,
) -> (Option<String>, Option<String>) {
    (
        last_tick.map(rfc3339_from_secs),
        schedule::next_tick_at(runtime, last_tick).map(rfc3339_from_secs),
    )
}

/// How many seconds the shield VCL may cache a `/_tick` or `/_flush` response before Compute
/// next wants a wake.
pub fn clock_ttl_s(runtime: &impl Runtime, path: &str) -> u64 {
    match path {
        "/_tick" => schedule::next_ttl_s(
            runtime,
            schedule::SCHED_KEY,
            schedule::tick_wanted,
            schedule::tick_wake_at,
        ),
        "/_flush" => schedule::next_ttl_s(
            runtime,
            schedule::FLUSH_SCHED_KEY,
            schedule::flush_wanted,
            schedule::flush_wake_at,
        ),
        _ => CLOCK_INTERVAL_S,
    }
}

fn tick_response(
    configured: bool,
    service_id: Option<String>,
    last_tick: Option<u64>,
    run: Option<CronRun>,
    runs: Vec<CronRun>,
) -> Value {
    let (last_tick_at, next_tick_at) = tick_timestamps(last_tick);
    json!({
        "ok": true,
        "configured": configured,
        "service_id": service_id,
        "last_tick_at": last_tick_at,
        "next_tick_at": next_tick_at,
        "clock_interval_s": CLOCK_INTERVAL_S,
        "run": run,
        "runs": runs,
    })
}

fn load_installed_service(
    runtime: &impl Runtime,
) -> Result<(Option<Record>, Option<Service>, u32), String> {
    let mut requests = 1u32;
    let install_val: Install = runtime
        .admin_get("admin:install")?
        .and_then(|r| serde_json::from_value(r.value).ok())
        .unwrap_or_default();
    let Some(sid) = install_val.service_id else {
        return Ok((None, None, requests));
    };
    requests += 1;
    let Some(svc_rec) = runtime.admin_get(&format!("svc:{sid}"))? else {
        return Ok((None, None, requests));
    };
    let active_service = serde_json::from_value::<Service>(svc_rec.value.clone())
        .ok()
        .filter(|svc| svc.status != "torn_down");
    Ok((Some(svc_rec), active_service, requests))
}

pub fn fos_credentials(runtime: &impl Runtime, svc: &Service) -> Option<(String, String)> {
    let raw_sk = runtime.secret(&svc.fos_secret_key_ref).ok()??;
    if raw_sk.is_empty() {
        return None;
    }
    if let Ok(env) = serde_json::from_str::<Value>(&raw_sk) {
        let ak = env["access_key"].as_str().filter(|s| !s.is_empty())?;
        let sk = env["secret_key"].as_str().filter(|s| !s.is_empty())?;
        return Some((ak.to_owned(), sk.to_owned()));
    }
    let ak = runtime.secret(&svc.fos_access_key_ref).ok()??;
    if ak.is_empty() {
        None
    } else {
        Some((ak, raw_sk))
    }
}

pub fn active_service(runtime: &impl Runtime) -> Result<Option<Service>, String> {
    load_installed_service(runtime).map(|(_, service, _)| service)
}

fn save_cron_state(
    runtime: &impl Runtime,
    cron_record: Option<&Record>,
    mut state: CronState,
    apply: impl Fn(&mut CronState),
) -> Result<CronState, String> {
    apply(&mut state);
    let state_value = serde_json::to_value(&state).map_err(|_| String::from("cron_encode"))?;
    let saved = runtime.admin_cas(
        "cron:state",
        cron_record.map(|r| r.generation.as_str()),
        &state_value,
    )?;
    if !saved {
        if let Some(latest) = runtime.admin_get("cron:state")? {
            let mut merged: CronState = serde_json::from_value(latest.value).unwrap_or_default();
            apply(&mut merged);
            let merged_val =
                serde_json::to_value(&merged).map_err(|_| String::from("cron_encode"))?;
            let saved = runtime.admin_cas("cron:state", Some(&latest.generation), &merged_val);
            if saved != Ok(true) {
                let error = saved.err().unwrap_or_else(|| "cron_state_conflict".into());
                runtime.admin_event(json!({"event": "cron.state_save_failed", "error": error}));
                return Err(error);
            }
            state = merged;
        }
    }
    Ok(state)
}

/// Stages the clock tick runs in its own execution, in budget-priority order. Maintenance
/// (`logs::MAINT_STAGES`) runs on the worker and the log flush on `/_flush`.
pub const TICK_STAGES: [&str; 3] = ["request_ingest", "rum_ingest", "raw_delete"];

/// A stage runs only when its full KV + FOS call requirement fits the remaining budget.
pub fn admit_stage(remaining: u32, needed: u32) -> bool {
    remaining >= needed
}

/// Calls the tick's outbox append leaves for `save_cron_state`: its CAS, and a re-read and CAS
/// after a conflict.
const CRON_STATE_CALLS: u32 = 3;
/// Calls a tick keeps after raw delete: the cron_runs outbox append (a GET and CAS joining its
/// bucket's key, and the overflow write it falls back to) and `CRON_STATE_CALLS`. Without the
/// GET and CAS every dispatch in a catch-up would add its own overflow key. The status purge
/// and a manual tick's manifest use what is left and are skipped when nothing is.
const TICK_TAIL: u32 = 3 + CRON_STATE_CALLS;
/// Calls ingest leaves for raw delete: the queue list, plan and watermark reads, one queue
/// record and its DeleteObjects (`raw_delete` needs 5 above its reserve to start).
const RAW_DELETE_CALLS: u32 = 5;
/// Ingest's reserve on top of the coordinator's own `TICK_RESERVE`, so raw delete still finds
/// `RAW_DELETE_CALLS` above `TICK_TAIL`.
const INGEST_RESERVE: u32 = RAW_DELETE_CALLS + TICK_TAIL - crate::ingest::TICK_RESERVE;

pub(crate) fn preserve_inline_deleteqs(
    runtime: &impl Runtime,
    queues: &[(String, Value)],
) -> Result<(), String> {
    if runtime.admin_remaining_calls() < CRON_STATE_CALLS + queues.len() as u32 {
        return Err("deleteq_handoff_budget_exhausted".into());
    }
    for (key, value) in queues {
        runtime.admin_cas(key, None, value)?;
    }
    Ok(())
}

/// Calls a second ingest step needs above its reserve to start: the plan and watermark reads and
/// a LIST. The coordinator dispatches only what fits after that and leaves the rest pending.
const SECOND_INGEST_CALLS: u32 = 3;
/// A tick that hears nothing back from its stage worker this many times in a row moves
/// `maint_next` on itself, so a stage whose worker always times out cannot block the others.
const MAINT_MISS_LIMIT: u32 = 2;
/// Calls the `/_flush` execution keeps for its cron_runs outbox write and the status purge.
/// Three lets the row join its bucket's key (GET and CAS) instead of adding an overflow key
/// per probe, which otherwise doubles the outbox inflow the flush has to drain.
const FLUSH_TAIL_CALLS: u32 = 3;

/// Runs the operational-log flush stage and returns its row.
fn run_flush_stage(
    runtime: &impl Runtime,
    svc: &Service,
    ak: &str,
    sk: &str,
    settings: &telemetry::OperationalLogSettings,
    started: std::time::Instant,
    flush_reserve: u32,
) -> StageRow {
    if let Some(usage) = runtime.usage_agg() {
        usage.set_caller("cron:log_flush");
    }
    let mut flush = StageRow::new("log_flush", runtime.now());
    let ops_prefix = format!("{}processed/ops/", crate::ingest::scope(svc));
    let target = FlushTarget {
        region: &svc.fos_region,
        bucket: &svc.fos_bucket,
        access_key: ak,
        secret_key: sk,
        ops_prefix: &ops_prefix,
    };
    let span = VcpuSpan::start(runtime);
    let stage_started = std::time::Instant::now();
    let before = runtime.admin_remaining_calls();
    let ops_before = runtime.cache_ops();
    match log_flush::log_flush_reserving(
        runtime,
        &svc.service_id,
        Some(&target),
        settings,
        false,
        started,
        flush_reserve,
    ) {
        Ok(report) => {
            flush.rows = report.rows;
            flush.files = report.files;
            flush.bytes = report.bytes_written;
            (flush.outcome, flush.more_pending) = match report.status {
                FlushStatus::Committed if report.behind => ("partial", true),
                FlushStatus::Committed => ("done", false),
                FlushStatus::Busy => ("busy", false),
                FlushStatus::Deferred => ("partial", true),
                FlushStatus::Stopped | FlushStatus::GcPending => ("partial", true),
                FlushStatus::NotDue | FlushStatus::Unprovisioned => ("idle", false),
            };
        }
        Err(reason) => {
            flush.outcome = "error";
            flush.error = Some(reason);
        }
    }
    flush.calls = before.saturating_sub(runtime.admin_remaining_calls());
    flush.cache_ops = runtime.cache_ops().saturating_sub(ops_before);
    flush.duration_ms = stage_started.elapsed().as_millis() as u64;
    flush.apply_span(runtime, &span);
    flush
}

/// The operational-log flush, in its own execution: the front service's `/_flush` health
/// check reaches flp about once a second. It never shares an execution with FOS maintenance,
/// whose sends fail once Core Cache transactions run out, and has the whole call and cache-op
/// budget to itself. Like the clock, a probe first claims a Core Cache record and starts a real
/// flush only when its wake time has passed (every `FLUSH_WAKE_S`, or the next probe after a
/// flush that left work); the KV flush lease stays the authority on who flushes. Errors are
/// recorded in cron_runs, not returned, so the probe stays healthy and is not retried early.
pub fn record_flush(runtime: &impl Runtime) -> Result<Value, String> {
    let started = std::time::Instant::now();
    if clock_forbidden(runtime) {
        return Err("worker_tick_forbidden".into());
    }
    if !runtime.on_log_shield() {
        return Ok(json!({"ok": true, "outcome": "off_shield"}));
    }
    let now = runtime.now();
    let suffix = hex::encode(&runtime.random().unwrap_or([0; 32])[..8]);
    let run_id = format!("flush-{now}-{suffix}");
    let key = schedule::FLUSH_SCHED_KEY;
    let idle = match schedule::claim(runtime, key, &run_id, false, schedule::flush_wanted) {
        Ok(schedule::Claim::Run {
            idle_ticks,
            idle_vcpu_ms,
        }) => (idle_ticks, idle_vcpu_ms),
        Ok(schedule::Claim::Idle) => return Ok(json!({"ok": true, "outcome": "idle"})),
        Ok(schedule::Claim::Busy) => return Ok(json!({"ok": true, "outcome": "busy"})),
        Err(error) => {
            runtime.admin_event(json!({"event": "cron.schedule_unavailable", "error": error}));
            (0, 0)
        }
    };
    let (result, carried, wake_at) = flush_once(runtime, started, now, &run_id, idle);
    let back = if carried { (0, 0) } else { idle };
    if let Err(error) = schedule::release(
        runtime,
        key,
        &run_id,
        &|record| record.wake_at = wake_at,
        back,
    ) {
        runtime.admin_event(json!({"event": "cron.schedule_unavailable", "error": error}));
    }
    Ok(result)
}

fn flush_once(
    runtime: &impl Runtime,
    started: std::time::Instant,
    now: u64,
    run_id: &str,
    idle: (u64, u64),
) -> (Value, bool, u64) {
    let bucket = now / ROW_BUCKET_S;
    let Ok((_, Some(svc), _)) = load_installed_service(runtime) else {
        return (
            json!({"ok": true, "configured": false}),
            false,
            now + schedule::FLUSH_WAKE_S,
        );
    };
    let Some((ak, sk)) = fos_credentials(runtime, &svc) else {
        return (
            json!({"ok": true, "configured": true, "outcome": "unprovisioned"}),
            false,
            now + schedule::FLUSH_WAKE_S,
        );
    };
    let settings = telemetry::api::operational_settings(runtime);
    let pre_flush_kv = runtime.kv_ops();
    let row = run_flush_stage(
        runtime,
        &svc,
        &ak,
        &sk,
        &settings,
        started,
        FLUSH_TAIL_CALLS,
    );
    let wake_at = match row.outcome {
        _ if row.more_pending => now,
        "busy" => now + schedule::BUSY_RETRY_S,
        "error" => now + schedule::RETRY_S,
        _ => now + schedule::FLUSH_WAKE_S,
    };
    let buffer = LogBuffer::new(&svc.service_id, run_id);
    let carried = row.outcome != "idle";
    if carried {
        let mut value = row.json(run_id, "", "clock", "clock", bucket);
        value["idle_ticks"] = idle.0.into();
        value["idle_vcpu_ms"] = idle.1.into();
        value["worker_executions"] = runtime.worker_requests().into();
        value["kv_ops"] = (u64::from(row.kv_ops) + u64::from(pre_flush_kv)).into();
        value["cache_replaces"] = (u64::from(row.cache_replaces) + idle.0 + 2).into();
        value["purges"] = (u64::from(row.purges) + 1).into();
        write_log(runtime, &buffer, WritableLogKind::CronRuns, value);
    }
    telemetry::api::drain_runtime_rows(runtime, &buffer);
    if let Some(error) = telemetry::flush_request_buffer(runtime, &buffer).outbox_error {
        runtime.admin_event(json!({"event": "log.outbox_error", "error": error}));
    }
    let _ = purge_status_cache(runtime, &svc.service_id);
    (
        json!({
            "ok": true, "configured": true, "outcome": row.outcome,
            "more_pending": row.more_pending, "rows": row.rows,
        }),
        carried,
        wake_at,
    )
}

struct StageRow {
    kind: &'static str,
    outcome: &'static str,
    started_at: u64,
    duration_ms: u64,
    vcpu_ms: u64,
    calls: u32,
    cache_ops: u32,
    cache_replaces: u32,
    kv_ops: u32,
    purges: u32,
    more_pending: bool,
    backlog: bool,
    listed: u64,
    converted: u64,
    lines_read: u64,
    rows: u64,
    bad_lines: u64,
    bytes: u64,
    files: u64,
    deleted: u64,
    details: Option<Value>,
    error: Option<String>,
    execution_metrics: Option<telemetry::execution::ExecutionMetrics>,
}

impl StageRow {
    fn new(kind: &'static str, started_at: u64) -> Self {
        Self {
            kind,
            outcome: "idle",
            started_at,
            duration_ms: 0,
            vcpu_ms: 0,
            calls: 0,
            cache_ops: 0,
            cache_replaces: 0,
            kv_ops: 0,
            purges: 0,
            more_pending: false,
            backlog: false,
            listed: 0,
            converted: 0,
            lines_read: 0,
            rows: 0,
            bad_lines: 0,
            bytes: 0,
            files: 0,
            deleted: 0,
            details: None,
            error: None,
            execution_metrics: None,
        }
    }

    fn apply_span(&mut self, runtime: &impl Runtime, span: &VcpuSpan) {
        self.vcpu_ms = span.elapsed(runtime);
        self.execution_metrics = Some(telemetry::execution::ExecutionMetrics {
            calls: Some(self.calls),
            vcpu_ms: Some(self.vcpu_ms),
            summary_vcpu_ms: None,
            duration_ms: Some(self.duration_ms),
            heap_mib: runtime.heap_mib(),
            linear_memory_high_water_bytes: runtime.linear_memory_high_water_bytes(),
        });
        self.kv_ops += span.kv_ops(runtime);
        self.cache_ops = self.cache_ops.max(span.cache_lookups(runtime));
        self.cache_replaces += span.cache_replaces(runtime);
        self.purges += span.purges(runtime);
    }

    fn ingest_metrics(&mut self, report: &crate::ingest::Report) {
        let details = self.details.get_or_insert_with(|| json!({}));
        details["shards_dispatched"] = report.shards_dispatched.into();
        details["shards_done"] = report.shards_done.into();
        details["worker_vcpu_ms"] = report.worker_vcpu_ms.into();
        details["summary_vcpu_ms"] = report.summary_vcpu_ms.into();
    }

    fn json(&self, run_id: &str, tick_id: &str, trigger: &str, actor: &str, bucket: u64) -> Value {
        let mut row = json!({
            "cron_kind": self.kind, "run_id": format!("{run_id}-{}", self.kind),
            "tick_event_id": (!tick_id.is_empty()).then_some(tick_id), "trigger": trigger, "actor": actor, "bucket": bucket,
            "outcome": self.outcome, "started_at": self.started_at * 1000,
            "duration_ms": self.duration_ms, "vcpu_ms": self.vcpu_ms,
            "listed_files": self.listed, "converted_files": self.converted,
            "deleted_files": self.deleted, "lines_read": self.lines_read, "rows_written": self.rows,
            "bad_lines": self.bad_lines, "bytes_read": Value::Null, "bytes_written": self.bytes,
            "parquet_files_written": self.files, "backend_requests_used": self.calls,
            "cache_lookups": self.cache_ops,
            "cache_replaces": self.cache_replaces,
            "kv_ops": self.kv_ops,
            "purges": self.purges,
            "more_pending": self.more_pending, "backlog": self.backlog, "error": self.error,
            "severity": if self.error.is_some() { "error" } else { "info" },
        });
        if self.outcome == "error" {
            for field in [
                "listed_files",
                "converted_files",
                "deleted_files",
                "lines_read",
                "rows_written",
                "bad_lines",
                "bytes_written",
                "parquet_files_written",
            ] {
                if row[field] == 0 {
                    row[field] = Value::Null;
                }
            }
        }
        let mut details = self.details.clone().unwrap_or_else(|| json!({}));
        details["root_run_id"] = run_id.into();
        if let Some(metrics) = &self.execution_metrics {
            details["stage_execution_metrics"] = json!(metrics);
            details["stage_execution_metrics"]["scope"] = "stage".into();
            details["heap_measurement"] = telemetry::execution::HEAP_MEASUREMENT.into();
            details["linear_memory_measurement"] =
                telemetry::execution::LINEAR_MEMORY_MEASUREMENT.into();
        }
        if details != json!({}) {
            row["details_json"] = details.to_string().into();
        }
        row
    }
}

/// Short fallback so a skipped or failed purge can only leave status stale briefly.
pub const STATUS_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(10);

pub fn status_surrogate(service_id: &str) -> String {
    format!("flp-status-{service_id}")
}

pub fn status_cache_key(name: &str, service_id: &str) -> String {
    format!("flp-status-{service_id}-{name}")
}

pub fn installed_service_id(runtime: &impl Runtime) -> Option<String> {
    let install: Install = runtime
        .admin_get("admin:install")
        .ok()
        .flatten()
        .and_then(|r| serde_json::from_value(r.value).ok())?;
    install.service_id
}

/// Purges cached status for a service. The purge is a backend call, so it is admitted
/// against the execution budget; when none is left the entries expire by TTL instead.
pub fn purge_status_cache(runtime: &impl Runtime, service_id: &str) -> Result<(), String> {
    if runtime.admin_remaining_calls() < 1 {
        return Err("backend_budget_exhausted".into());
    }
    runtime.admin_charge_call()?;
    runtime
        .core_cache_purge(&status_surrogate(service_id))
        .map_err(|error| format!("{error:?}"))
}

/// Whether this execution may not run the clock or flush: probes reach only flp, never the
/// worker service.
pub fn clock_forbidden(runtime: &impl Runtime) -> bool {
    let Some(compute_id) = runtime.compute_service_id().filter(|v| !v.is_empty()) else {
        return false;
    };
    let worker = runtime.config("worker_service_id").ok().flatten();
    let flp = runtime.config("flp_service_id").ok().flatten();
    worker.is_some_and(|id| !id.is_empty() && compute_id == id)
        || flp.is_some_and(|id| !id.is_empty() && compute_id != id)
}

/// The 1 s dispatcher. Off the IAD log shield a clock tick does nothing. On it, the tick claims
/// the Core Cache schedule ([`schedule::claim`]): an idle or busy tick ends there with one
/// cache op, no backend call and no KV write. The lease holder runs whichever of ingest, raw
/// delete and one maintenance stage are due, writes their cron_runs rows (carrying the idle
/// ticks counted since the last run) and one `cron:state`, then releases the lease. A manual
/// run forces every job; off the shield it runs ingest and raw delete only, which stay fenced
/// by their own KV state.
pub fn record_tick(runtime: &impl Runtime, trigger: &str, actor: &str) -> Result<Value, String> {
    let execution_started = std::time::Instant::now();
    if clock_forbidden(runtime) {
        return Err("worker_tick_forbidden".into());
    }
    let manual = trigger == "manual";
    let shield = runtime.on_log_shield();
    if !shield && !manual {
        return Ok(json!({"ok": true, "outcome": "off_shield"}));
    }
    let now = runtime.now();
    // Also the schedule's owner token, so it must differ between ticks in the same second.
    let suffix = hex::encode(&runtime.random().unwrap_or([0; 32])[..8]);
    let run_id = format!("cron-{now}-{suffix}");
    let mut idle = (0, 0);
    let mut cached_sched: Option<schedule::Sched> = None;
    if shield {
        match schedule::claim_with_record(
            runtime,
            schedule::SCHED_KEY,
            &run_id,
            manual,
            schedule::tick_wanted,
        ) {
            Ok((
                schedule::Claim::Run {
                    idle_ticks,
                    idle_vcpu_ms,
                },
                record,
            )) => {
                idle = (idle_ticks, idle_vcpu_ms);
                cached_sched = Some(record);
            }
            Ok((schedule::Claim::Idle, _)) => return Ok(json!({"ok": true, "outcome": "idle"})),
            Ok((schedule::Claim::Busy, _)) if manual => return Err("cron_busy".into()),
            Ok((schedule::Claim::Busy, _)) => return Ok(json!({"ok": true, "outcome": "busy"})),
            // Without the record every tick reads KV, but the jobs stay fenced, so run.
            Err(error) => {
                runtime.admin_event(json!({"event": "cron.schedule_unavailable", "error": error}))
            }
        }
    }
    let carried = std::cell::Cell::new(false);
    let phase = |stage: &str| {
        if telemetry::effective_dev_telemetry(runtime) {
            runtime.admin_event(json!({
                "event": "cron.finalize",
                "root_run_id": run_id,
                "stage": stage,
                "duration_ms": execution_started.elapsed().as_millis(),
            }));
        }
    };
    phase("dispatch_start");
    let result = dispatch(
        runtime,
        trigger,
        actor,
        &run_id,
        shield,
        idle,
        &carried,
        cached_sched.as_ref(),
        execution_started,
    );
    phase("dispatch_end");
    if shield {
        let mirror = result.as_ref().ok().and_then(|done| done.mirror.clone());
        let cron_generation = result
            .as_ref()
            .ok()
            .map(|done| done.cron_generation.clone());
        let deferred_idle = result.as_ref().ok().map(|done| done.deferred_idle.clone());
        // A failed dispatch leaves the mirror saying its jobs are due, so back off instead of
        // retrying every tick.
        let retry_after = if result.is_err() {
            now + schedule::RETRY_S
        } else {
            0
        };
        let back = if carried.get() { (0, 0) } else { idle };
        let update = |record: &mut schedule::Sched| {
            if let Some(mirror) = &mirror {
                record.mirror = mirror.clone();
            }
            if let Some(generation) = &cron_generation {
                record.cron_generation = generation.clone();
            }
            if let Some(deferred) = &deferred_idle {
                record.deferred_idle = deferred.clone();
            }
            record.retry_after = retry_after;
        };
        phase("schedule_release_start");
        if let Err(error) = schedule::release(runtime, schedule::SCHED_KEY, &run_id, &update, back)
        {
            runtime.admin_event(json!({"event": "cron.schedule_unavailable", "error": error}));
        }
        phase("schedule_release_end");
    }
    let mut done = match result {
        Ok(done) => done,
        Err(error) => {
            let mut metrics = json!(telemetry::execution::ExecutionMetrics::snapshot(
                runtime,
                execution_started
            ));
            metrics["scope"] = "tick_end".into();
            runtime.admin_event(json!({
                "event": "cron.execution", "run_id": run_id, "root_run_id": run_id,
                "status": "error", "execution_metrics": metrics,
            }));
            return Err(error);
        }
    };
    if done.ran {
        if let Some(sid) = done.value["service_id"].as_str() {
            phase("status_purge_start");
            let _ = purge_status_cache(runtime, sid);
            phase("status_purge_end");
        }
    }
    let mut metrics = json!(telemetry::execution::ExecutionMetrics::snapshot(
        runtime,
        execution_started,
    ));
    metrics["scope"] = "tick_end".into();
    metrics["heap_measurement"] = telemetry::execution::HEAP_MEASUREMENT.into();
    metrics["linear_memory_measurement"] = telemetry::execution::LINEAR_MEMORY_MEASUREMENT.into();
    let mut event = done.worker_telemetry.clone().unwrap_or_else(|| json!({}));
    event["event"] = "cron.execution".into();
    event["run_id"] = run_id.clone().into();
    event["root_run_id"] = run_id.into();
    event["execution_metrics"] = metrics.clone();
    runtime.admin_event(event);
    done.value["execution_metrics"] = metrics;
    done.value["worker_telemetry"] = done.worker_telemetry.into();
    Ok(done.value)
}

struct Dispatched {
    value: Value,
    mirror: Option<schedule::Mirror>,
    cron_generation: Option<String>,
    deferred_idle: Vec<schedule::DeferredIdleRun>,
    ran: bool,
    worker_telemetry: Option<Value>,
}

fn job_state(
    now: u64,
    outcome: &str,
    more_pending: bool,
    duration_ms: u64,
    previous: &schedule::JobState,
) -> schedule::JobState {
    schedule::JobState {
        service_id: previous.service_id.clone(),
        last_run: Some(now),
        last_success_period: previous.last_success_period,
        last_success_period_s: previous.last_success_period_s,
        retry_after: match outcome {
            "busy" => now + schedule::BUSY_RETRY_S,
            "error" => now + schedule::RETRY_S,
            _ => 0,
        },
        more_pending,
        last_status: outcome.into(),
        last_duration_ms: duration_ms,
        idle_streak: previous.idle_streak,
    }
}

#[allow(clippy::too_many_arguments)]
fn dispatch(
    runtime: &impl Runtime,
    trigger: &str,
    actor: &str,
    run_id: &str,
    shield: bool,
    idle: (u64, u64),
    carried: &std::cell::Cell<bool>,
    cached_sched: Option<&schedule::Sched>,
    execution_started: std::time::Instant,
) -> Result<Dispatched, String> {
    let started = std::time::Instant::now();
    let manual = trigger == "manual";
    let now = runtime.now();
    let bucket = now / ROW_BUCKET_S;
    let started_at = rfc3339_from_secs(now);
    let unconfigured = |last_tick| {
        if manual {
            return Err(String::from("service_not_configured"));
        }
        Ok(Dispatched {
            value: tick_response(false, None, last_tick, None, Vec::new()),
            mirror: Some(schedule::Mirror::default()),
            ran: false,
            cron_generation: None,
            deferred_idle: Vec::new(),
            worker_telemetry: None,
        })
    };

    let cron_record = match runtime.admin_get("cron:state") {
        Ok(record) => record,
        Err(err) if err == "control_plane_unavailable" => return unconfigured(None),
        Err(err) => return Err(err),
    };
    let record_gen = cron_record.as_ref().map(|r| r.generation.clone());
    let mut cron_state: CronState = cron_record
        .as_ref()
        .and_then(|r| serde_json::from_value(r.value.clone()).ok())
        .unwrap_or_default();
    let mut deferred_idle = cached_sched
        .map(|s| s.deferred_idle.clone())
        .unwrap_or_default();
    let external_gen_unchanged =
        cached_sched.is_some_and(|sched| sched.cron_generation == record_gen);
    if let Some(sched) =
        cached_sched.filter(|_| external_gen_unchanged && !deferred_idle.is_empty())
    {
        cron_state.jobs.merge(&sched.mirror.jobs);
        if let Some(last) = deferred_idle.last() {
            cron_state.last_tick = cron_state.last_tick.max(sched.mirror.jobs.ingest.last_run);
            cron_state.last_bucket = cron_state.last_bucket.max(Some(last.run.bucket));
        }
    }
    let (_, active_service, _) = load_installed_service(runtime)?;
    let Some(svc) = active_service else {
        return unconfigured(cron_state.last_tick);
    };
    let service_id = Some(svc.service_id.clone());
    let rum_period = svc.rum.enabled.then_some(svc.rum.log_period);
    let mirror =
        schedule::Mirror::from_state(&cron_state, Some(svc.log_period)).with_rum(rum_period);
    let mut due = schedule::due(&mirror, now);
    let ready = schedule::ready_period(now, mirror.log_period_s);
    if manual {
        due = schedule::Due {
            ingest: Some(ready),
            raw_delete: true,
            maint: shield,
            rum: rum_period.map(|_| schedule::ready_period(now, mirror.rum_log_period_s)),
        };
    }
    if reset::fencing(&svc) {
        due.ingest = None;
        due.rum = None;
        due.raw_delete = false;
    }
    if reset::rum_held(&svc) {
        due.rum = None;
    }
    if svc.data_reset.is_some() && shield {
        due.maint = true;
    }
    if !due.any() {
        return Ok(Dispatched {
            value: tick_response(true, service_id, cron_state.last_tick, None, Vec::new()),
            mirror: Some(mirror),
            ran: false,
            cron_generation: if external_gen_unchanged || deferred_idle.is_empty() {
                record_gen
            } else {
                cached_sched.and_then(|s| s.cron_generation.clone())
            },
            deferred_idle,
            worker_telemetry: None,
        });
    }

    let mut backend_requests_used = 2u32;
    let mut fos_manifest_key = None;
    let mut error = None;
    let outcome;
    let mut listed_raw_files = 0u64;
    let mut straggler_raw_files = 0u64;
    let mut converted_raw_files = 0u64;
    let mut deleted_raw_files = 0u64;
    let mut rows_written = 0u64;
    let mut bytes_written = 0u64;
    let buffer = LogBuffer::new(&svc.service_id, run_id);
    let settings = telemetry::api::operational_settings(runtime);
    let mut jobs = cron_state.jobs.clone();
    let mut stages: Vec<StageRow> = Vec::new();
    // `None`: no stage was dispatched; `Some(None)`: the worker did not answer.
    let mut maint_reply: Option<Option<usize>> = None;
    let maint_start = cron_state.maint_next % logs::MAINT_STAGES.len();
    let mut stage_rows = Vec::new();
    let stage_body = due
        .maint
        .then(|| {
            serde_json::to_value(stage::StageRequest {
                service_id: svc.service_id.clone(),
                run_id: run_id.to_owned(),
                tick_id: String::new(),
                trigger: trigger.into(),
                actor: actor.into(),
                bucket,
                start: maint_start,
            })
            .ok()
        })
        .flatten();
    let answered = |reply: Option<&Result<Value, String>>| {
        let reply = reply?.as_ref().ok()?;
        let pending = stage::reply_compact_pending(reply);
        Some((
            stage::reply_next(reply)?,
            (stage::reply_rows(reply), pending),
        ))
    };
    let mut maint_more_pending = false;
    let has_creds = fos_credentials(runtime, &svc);
    let pre_stages_kv = runtime.kv_ops();
    let mut ingest_error = None;
    // The RUM stream runs when its own log period is due. It shares the ingest budget: it runs
    // after requests, or first on the tick after it was starved or left work pending, so
    // neither stream can hold the budget for long.
    let rum_due = due.rum.filter(|_| svc.rum.enabled);
    let rum_enabled = rum_due.is_some();
    let rum_first = rum_enabled && cron_state.rum_first;
    let mut next_rum_first = None;
    let mut stage_body = stage_body;
    let relayed_workers = std::cell::Cell::new(0u32);
    let worker_metrics = std::cell::RefCell::new(Vec::new());
    let maintenance_children = std::cell::Cell::new(0u32);
    let observe_maintenance = |reply: Option<&Result<Value, String>>| {
        let reply = reply.and_then(|r| r.as_ref().ok());
        worker_metrics
            .borrow_mut()
            .push(telemetry::execution::WorkerExecutionMetrics::from_reply(
                "maintenance",
                None,
                reply,
            ));
        if let Some(reply) = reply {
            maintenance_children.set(
                maintenance_children.get()
                    + reply["worker_requests"].as_u64().unwrap_or(0).min(32) as u32,
            );
            if let Some(children) = reply["worker_executions_metrics"].as_array() {
                for child in children.iter().take(32) {
                    worker_metrics.borrow_mut().push(
                        telemetry::execution::WorkerExecutionMetrics::from_reply(
                            "request_compact",
                            None,
                            (child["answered"] == true).then_some(child),
                        ),
                    );
                }
            }
        }
    };
    if let Some((ak, sk)) = &has_creds {
        let mut inline_queues: Vec<(String, Value)> = Vec::new();
        let mut rum_converted_files = 0u64;
        let held_cleanup_pending = std::cell::Cell::new(false);
        let rum_step = |extra: Vec<(String, Value)>| {
            let maintenance_sent = !extra.is_empty();
            let options = crate::ingest::coordinator::Options {
                reserve: INGEST_RESERVE,
                extra,
                defer_deleteq: true,
                ..crate::ingest::coordinator::Options::rum(runtime)
            };
            let span = VcpuSpan::start(runtime);
            let stage_started = std::time::Instant::now();
            let before = runtime.admin_remaining_calls();
            let mut stage = StageRow::new(TICK_STAGES[1], runtime.now());
            let (mut report, result) = crate::ingest::coordinator::coordinate_reporting(
                runtime, &svc, ak, sk, started, &buffer, &options,
            );
            relayed_workers.set(relayed_workers.get() + report.relayed_worker_requests);
            worker_metrics
                .borrow_mut()
                .append(&mut report.worker_executions_metrics);
            if maintenance_sent {
                observe_maintenance(report.extra_results.first());
            }
            let pq = report.pending_deleteq.take();
            if result.is_ok() && report.failed > 0 {
                held_cleanup_pending.set(true);
            }
            let reply = answered(report.extra_results.first());
            stage.kv_ops += report.kv_ops;
            stage.cache_ops += report.cache_lookups;
            stage.cache_replaces += report.cache_replaces;
            stage.purges += report.purges;
            stage.more_pending = report.more_pending;
            stage.backlog = report.backlog;
            stage.listed = report.listed;
            stage.converted = report.converted;
            stage.lines_read = report.lines_read;
            stage.rows = report.rows;
            stage.bad_lines = report.bad_lines;
            stage.bytes = report.bytes;
            stage.files = report.files.len() as u64;
            let processed = if result.is_ok() {
                report.converted + report.failed
            } else {
                0
            };
            match result {
                Ok(()) => {
                    if report.busy {
                        stage.outcome = "busy";
                    } else if stage.listed > 0 || processed > 0 {
                        stage.outcome = if report.more_pending {
                            "partial"
                        } else {
                            "done"
                        };
                    }
                }
                Err(reason) => {
                    stage.outcome = "error";
                    stage.error = Some(reason);
                }
            }
            stage.calls = before.saturating_sub(runtime.admin_remaining_calls());
            stage.duration_ms = stage_started.elapsed().as_millis() as u64;
            stage.ingest_metrics(&report);
            stage.apply_span(runtime, &span);
            (stage, reply, pq, processed)
        };
        let second_fits = || {
            runtime.admin_elapsed(started) < crate::ingest::coordinator::DISPATCH_BY
                && runtime.admin_remaining_calls()
                    >= INGEST_RESERVE + crate::ingest::TICK_RESERVE + SECOND_INGEST_CALLS
        };
        let mut rum_stage = None;
        let ingest_at = stages.len();
        if rum_first {
            let extra = stage_body
                .take()
                .map(|body| vec![(stage::PATH.to_owned(), body)])
                .unwrap_or_default();
            if !extra.is_empty() {
                maint_reply = Some(None);
            }
            let (stage, reply, pq, processed) = rum_step(extra);
            if let Some(pq) = pq {
                inline_queues.push(pq);
            }
            rum_converted_files += processed;
            rum_stage = Some((stage, reply));
            next_rum_first = Some(false);
        }
        if let Some(period) = due.ingest.filter(|_| !rum_first || second_fits()) {
            let options = crate::ingest::coordinator::Options {
                reserve: INGEST_RESERVE,
                extra: stage_body
                    .clone()
                    .map(|body| vec![(stage::PATH.to_owned(), body)])
                    .unwrap_or_default(),
                allow_backfill: true,
                sweep: true,
                defer_deleteq: true,
                ..crate::ingest::coordinator::Options::request(runtime)
            };
            if !options.extra.is_empty() {
                maint_reply = Some(None);
            }
            let span = VcpuSpan::start(runtime);
            let stage_started = std::time::Instant::now();
            let before = runtime.admin_remaining_calls();
            let mut stage = StageRow::new(TICK_STAGES[0], runtime.now());
            let (mut report, result) = crate::ingest::coordinator::coordinate_reporting(
                runtime, &svc, ak, sk, started, &buffer, &options,
            );
            relayed_workers.set(relayed_workers.get() + report.relayed_worker_requests);
            worker_metrics
                .borrow_mut()
                .append(&mut report.worker_executions_metrics);
            if !options.extra.is_empty() {
                observe_maintenance(report.extra_results.first());
            }
            if let Some(pq) = report.pending_deleteq.take() {
                inline_queues.push(pq);
            }
            // The stage worker may have run even when the ingest step failed.
            if let Some((next, rows)) = answered(report.extra_results.first()) {
                maint_reply = Some(Some(next));
                (stage_rows, maint_more_pending) = rows;
            }
            stage.kv_ops += report.kv_ops;
            stage.cache_ops += report.cache_lookups;
            stage.cache_replaces += report.cache_replaces;
            stage.purges += report.purges;
            stage.listed = report.listed;
            stage.converted = report.converted;
            stage.lines_read = report.lines_read;
            stage.rows = report.rows;
            stage.bad_lines = report.bad_lines;
            stage.bytes = report.bytes;
            stage.files = report.files.len() as u64;
            match result {
                Ok(()) => {
                    listed_raw_files = report.listed;
                    straggler_raw_files = report.stragglers;
                    converted_raw_files = report.converted + report.failed;
                    held_cleanup_pending.set(held_cleanup_pending.get() || report.failed > 0);
                    rows_written = report.rows;
                    bytes_written = report.bytes;
                    stage.more_pending = report.more_pending;
                    let mut details = serde_json::Map::new();
                    if report.stragglers > 0 {
                        details.insert("stragglers".into(), report.stragglers.into());
                    }
                    if report.shards_done > 0 {
                        details.insert("worker_vcpu_ms".into(), report.worker_vcpu_ms.into());
                        details.insert("summary_vcpu_ms".into(), report.summary_vcpu_ms.into());
                    }
                    if !details.is_empty() {
                        stage.details = Some(Value::Object(details));
                    }
                    if report.busy {
                        stage.outcome = "busy";
                    } else if report.listed > 0 || converted_raw_files > 0 {
                        stage.outcome = if report.more_pending {
                            "partial"
                        } else {
                            "done"
                        };
                    }
                }
                Err(reason) => {
                    stage.outcome = "error";
                    stage.error = Some(reason.clone());
                    ingest_error = Some(reason);
                }
            }
            stage.calls = before.saturating_sub(runtime.admin_remaining_calls());
            stage.duration_ms = stage_started.elapsed().as_millis() as u64;
            stage.ingest_metrics(&report);
            stage.apply_span(runtime, &span);
            backend_requests_used += stage.calls;
            jobs.ingest = job_state(
                now,
                stage.outcome,
                stage.more_pending,
                stage.duration_ms,
                &jobs.ingest,
            );
            if !matches!(stage.outcome, "busy" | "error") && !stage.more_pending {
                jobs.ingest.record_success(period, mirror.log_period_s);
            }
            if stage.outcome == "idle" && !stage.more_pending {
                jobs.ingest.idle_streak = cron_state.jobs.ingest.idle_streak.saturating_add(1);
            } else if matches!(stage.outcome, "done" | "partial") || stage.more_pending {
                jobs.ingest.idle_streak = 0;
            }
            // A scheduled ingest is logged even when it found nothing: its row carries the
            // idle ticks and keeps the once-per-period cadence visible.
            stages.push(stage);
        } else if let Some(body) = stage_body {
            maint_reply = Some(None);
            let replies = runtime.worker_dispatch(
                &[(stage::PATH.to_owned(), body)],
                crate::ingest::coordinator::DRAIN_AT,
            );
            observe_maintenance(replies.first());
            backend_requests_used += 1;
            if let Some((next, rows)) = answered(replies.first()) {
                maint_reply = Some(Some(next));
                (stage_rows, maint_more_pending) = rows;
            }
        }

        if rum_enabled && !rum_first {
            if second_fits() {
                let (stage, _, pq, processed) = rum_step(Vec::new());
                if let Some(pq) = pq {
                    inline_queues.push(pq);
                }
                rum_converted_files += processed;
                next_rum_first = Some(stage.more_pending);
                rum_stage = Some((stage, None));
            } else {
                next_rum_first = Some(true);
            }
        }
        if let Some((stage, reply)) = rum_stage {
            if let Some((next, rows)) = reply {
                maint_reply = Some(Some(next));
                (stage_rows, maint_more_pending) = rows;
            }
            backend_requests_used += stage.calls;
            jobs.rum = job_state(
                now,
                stage.outcome,
                stage.more_pending,
                stage.duration_ms,
                &jobs.rum,
            );
            if let Some(period) = rum_due
                .filter(|_| !matches!(stage.outcome, "busy" | "error") && !stage.more_pending)
            {
                jobs.rum.record_success(period, mirror.rum_log_period_s);
            }
            // In run order: a RUM step that went first precedes the request row.
            stages.insert(if rum_first { ingest_at } else { stages.len() }, stage);
        }
        let check_kv_delete = jobs.raw_delete.more_pending
            || due.raw_delete
            || due.maint
            || held_cleanup_pending.get()
            || ((converted_raw_files + rum_converted_files) > 0 && inline_queues.is_empty());
        if !inline_queues.is_empty() || check_kv_delete {
            if let Some(usage) = runtime.usage_agg() {
                usage.set_caller("cron:raw_delete");
            }
            let store = crate::ingest::storage::Store {
                runtime,
                service: &svc,
                access_key: ak.clone(),
                secret_key: sk.clone(),
                started,
                vcpu_start: 0,
            };
            let mut stage = StageRow::new(TICK_STAGES[2], runtime.now());
            let span = VcpuSpan::start(runtime);
            let stage_started = std::time::Instant::now();
            let before = runtime.admin_remaining_calls();
            match crate::ingest::retention::raw_delete_with_inline(
                &store,
                &buffer,
                TICK_TAIL,
                &settings,
                &inline_queues,
                check_kv_delete,
            ) {
                Ok(report) => {
                    deleted_raw_files = report.deleted;
                    stage.deleted = report.deleted;
                    stage.bytes = report.bytes;
                    stage.more_pending = report.more_pending;
                    if report.deleted + report.failed > 0 || report.more_pending {
                        stage.outcome = if report.more_pending {
                            "partial"
                        } else {
                            "done"
                        };
                    }
                    if report.failed > 0 {
                        stage.error = Some(format!("raw_delete_failed_{}", report.failed));
                    }
                }
                Err(reason) => {
                    stage.outcome = "error";
                    stage.more_pending = true;
                    stage.error = Some(match preserve_inline_deleteqs(runtime, &inline_queues) {
                        Ok(()) => reason,
                        Err(error) => format!("{reason};{error}"),
                    });
                }
            }
            stage.calls = before.saturating_sub(runtime.admin_remaining_calls());
            stage.duration_ms = stage_started.elapsed().as_millis() as u64;
            stage.apply_span(runtime, &span);
            backend_requests_used += stage.calls;
            jobs.raw_delete = job_state(
                now,
                stage.outcome,
                stage.more_pending,
                stage.duration_ms,
                &jobs.raw_delete,
            );
            jobs.raw_delete.service_id = Some(svc.service_id.clone());
            if stage.outcome != "idle" {
                stages.push(stage);
            }
        }
    } else {
        // Nothing can run without FOS credentials; back off instead of re-checking every second.
        jobs.ingest = job_state(now, "error", false, 0, &jobs.ingest);
        jobs.ingest.last_status = "unprovisioned".into();
        jobs.maint = job_state(now, "error", false, 0, &jobs.maint);
        jobs.maint.last_status = "unprovisioned".into();
    }
    if maint_reply.is_some() {
        let answered = matches!(maint_reply, Some(Some(_)));
        let more_pending = answered && maint_more_pending;
        jobs.maint = job_state(
            now,
            if answered { "done" } else { "missed" },
            more_pending,
            0,
            &jobs.maint,
        );
        if more_pending {
            jobs.maint.retry_after = now + schedule::MAINT_CATCHUP_S;
        }
    }
    if let Some(reason) = ingest_error.clone() {
        error = Some(reason);
    }
    if manual {
        if let Some((ak, sk)) = has_creds {
            let key = format!("meta/cron-runs/dt={}/{}.json", &started_at[..10], run_id);
            let manifest = json!({
                "id": run_id,
                "started_at": started_at,
                "finished_at": started_at,
                "duration_ms": 0,
                "outcome": "done",
                "trigger": trigger,
                "actor": actor,
                "bucket": bucket,
                "service_id": svc.service_id,
                "fos_bucket": svc.fos_bucket,
                "listed_raw_files": listed_raw_files,
                "converted_raw_files": converted_raw_files,
                "deleted_raw_files": deleted_raw_files,
                "rows_written": rows_written,
                "bytes_written": bytes_written,
                "error": ingest_error,
            });
            backend_requests_used += 1;
            match runtime.admin_fos(FosRequest {
                method: "PUT",
                region: &svc.fos_region,
                bucket: &svc.fos_bucket,
                key: &key,
                query: "",
                access_key: &ak,
                secret_key: &sk,
                body: manifest.to_string().into_bytes(),
                timeout: transport::request_timeout(
                    transport::FOS_TIMEOUT,
                    runtime.admin_elapsed(started),
                ),
                response_limit: transport::FOS_RESPONSE_LIMIT,
                content_md5: None,
            }) {
                Ok(resp) if (200..300).contains(&resp.status) => {
                    fos_manifest_key = Some(key);
                    outcome = if ingest_error.is_some() {
                        "error"
                    } else {
                        "done"
                    }
                    .to_owned();
                }
                Ok(resp) => {
                    outcome = "error".to_owned();
                    error = Some(format!("fos_manifest_status_{}", resp.status));
                }
                Err(err) => {
                    outcome = "error".to_owned();
                    error = Some(err);
                }
            }
        } else {
            outcome = "done".to_owned();
        }
    } else if ingest_error.is_some() {
        outcome = "error".to_owned();
    } else if converted_raw_files > 0 {
        outcome = "done".to_owned();
    } else {
        outcome = "idle".to_owned();
    }

    let steady_state_idle = cached_sched.is_some()
        && !manual
        && cron_record.is_some()
        && (external_gen_unchanged || deferred_idle.is_empty())
        && cron_state.jobs.ingest.idle_streak >= 1
        && cron_state.jobs.ingest.last_success_period_s
            == Some(schedule::period_s(u64::from(svc.log_period)))
        && outcome == "idle"
        && maint_reply.is_none()
        && stage_rows.is_empty()
        && stages.len() == 1
        && stages[0].kind == TICK_STAGES[0]
        && stages[0].outcome == "idle"
        && !stages[0].more_pending;
    let defer_kv_writes =
        steady_state_idle && (deferred_idle.len() + 1) < schedule::IDLE_WRITE_BATCH as usize;

    let dispatched = runtime.worker_requests() + relayed_workers.get() + maintenance_children.get();
    let mut samples = worker_metrics.borrow_mut();
    let capacity = telemetry::execution::MAX_WORKER_METRICS;
    let truncated = samples.len() > capacity || dispatched as usize > capacity;
    samples.truncate(capacity);
    while samples.len() < (dispatched as usize).min(capacity) {
        samples.push(telemetry::execution::WorkerExecutionMetrics::from_reply(
            "unobserved_worker",
            None,
            None,
        ));
    }
    let worker_telemetry = json!({
        "root_run_id": run_id,
        "workers_dispatched": dispatched,
        "workers_answered": samples.iter().filter(|sample| sample.answered).count(),
        "worker_executions_metrics": *samples,
        "worker_metrics_truncated": truncated,
        "ingest_rows_written": stages.iter()
            .filter(|stage| matches!(stage.kind, "request_ingest" | "rum_ingest"))
            .map(|stage| stage.rows).sum::<u64>(),
        "ingest_bytes_written": stages.iter()
            .filter(|stage| matches!(stage.kind, "request_ingest" | "rum_ingest"))
            .map(|stage| stage.bytes).sum::<u64>(),
        "heap_measurement": telemetry::execution::HEAP_MEASUREMENT,
        "linear_memory_measurement": telemetry::execution::LINEAR_MEMORY_MEASUREMENT,
    });
    let mut rows: Vec<(WritableLogKind, Value)> = stages
        .iter()
        .map(|stage| {
            (
                WritableLogKind::CronRuns,
                stage.json(run_id, "", trigger, actor, bucket),
            )
        })
        .collect();
    rows.extend(stage_rows);
    if let Some((_, row)) = rows
        .iter_mut()
        .find(|(kind, _)| *kind == WritableLogKind::CronRuns)
    {
        row["idle_ticks"] = idle.0.into();
        row["idle_vcpu_ms"] = idle.1.into();
        row["worker_executions"] = (runtime.worker_requests() + relayed_workers.get()).into();
        let mut details: Value = row["details_json"]
            .as_str()
            .and_then(|text| serde_json::from_str(text).ok())
            .unwrap_or_else(|| json!({}));
        if let (Some(details), Some(worker_details)) =
            (details.as_object_mut(), worker_telemetry.as_object())
        {
            details.extend(worker_details.clone());
        }
        details["execution_metrics"] = json!(telemetry::execution::ExecutionMetrics::snapshot(
            runtime,
            execution_started,
        ));
        details["execution_metrics"]["scope"] = "tick_before_telemetry_flush".into();
        row["details_json"] = details.to_string().into();
        let base_kv = row["kv_ops"].as_u64().unwrap_or(0);
        let tail_kv = if defer_kv_writes {
            0
        } else {
            u64::from(CRON_STATE_CALLS)
        };
        row["kv_ops"] = (base_kv + u64::from(pre_stages_kv) + tail_kv).into();
        let base_replaces = row["cache_replaces"].as_u64().unwrap_or(0);
        row["cache_replaces"] = (base_replaces + idle.0 + if shield { 2 } else { 0 }).into();
        let base_purges = row["purges"].as_u64().unwrap_or(0);
        row["purges"] = (base_purges + u64::from(converted_raw_files > 0)).into();
        carried.set(true);
    }
    if !defer_kv_writes {
        for deferred in &deferred_idle {
            write_log(
                runtime,
                &buffer,
                WritableLogKind::CronRuns,
                deferred.row.clone(),
            );
        }
        for (kind, row) in &rows {
            write_log(runtime, &buffer, *kind, row.clone());
        }
        telemetry::api::drain_runtime_rows(runtime, &buffer);
        if let Some(error) =
            telemetry::flush_request_buffer_reserving(runtime, &buffer, CRON_STATE_CALLS)
                .outbox_error
        {
            runtime.admin_event(json!({"event": "log.outbox_error", "error": error}));
        }
        backend_requests_used += 1;
    }
    let finished_now = runtime.now();
    let duration_ms = finished_now.saturating_sub(now).saturating_mul(1000);
    let finished_at = rfc3339_from_secs(finished_now);

    let run = CronRun {
        id: run_id.to_owned(),
        started_at: started_at.clone(),
        finished_at,
        duration_ms,
        outcome,
        trigger: trigger.into(),
        actor: actor.into(),
        bucket,
        service_id: service_id.clone(),
        listed_raw_files,
        straggler_raw_files,
        converted_raw_files,
        deleted_raw_files,
        rows_written,
        bytes_written,
        backend_requests_used,
        fos_manifest_key,
        error,
    };

    if defer_kv_writes {
        if let Some((_, row)) = rows
            .into_iter()
            .find(|(kind, _)| *kind == WritableLogKind::CronRuns)
        {
            deferred_idle.push(schedule::DeferredIdleRun {
                run: run.clone(),
                row,
            });
        }
        cron_state
            .runs
            .retain(|r| r.service_id.as_deref() == Some(svc.service_id.as_str()));
        cron_state.last_tick = Some(now);
        cron_state.last_bucket = Some(bucket);
        for deferred in &deferred_idle {
            cron_state.runs.insert(0, deferred.run.clone());
        }
        cron_state.runs.truncate(MAX_HISTORY_ENTRIES);
        cron_state.jobs.merge(&jobs);
        runtime.admin_event(json!({
            "event": "cron.tick",
            "id": run.id,
            "trigger": run.trigger,
            "actor": run.actor,
            "outcome": run.outcome,
            "bucket": run.bucket,
            "service_id": run.service_id,
            "fos_manifest_key": run.fos_manifest_key,
        }));
        return Ok(Dispatched {
            mirror: Some(
                schedule::Mirror::from_state(&cron_state, Some(svc.log_period))
                    .with_rum(rum_period),
            ),
            value: tick_response(
                true,
                service_id,
                cron_state.last_tick,
                Some(run),
                cron_state.runs,
            ),
            ran: false,
            cron_generation: record_gen,
            deferred_idle,
            worker_telemetry: Some(worker_telemetry),
        });
    }

    let flushed_idle = std::mem::take(&mut deferred_idle);
    let cron_state = save_cron_state(runtime, cron_record.as_ref(), cron_state, |state| {
        state
            .runs
            .retain(|r| r.service_id.as_deref() == Some(svc.service_id.as_str()));
        state.last_tick = Some(now);
        state.last_bucket = Some(bucket);
        if let Some(rum_first) = next_rum_first {
            state.rum_first = rum_first;
        }
        if converted_raw_files > 0 {
            state.last_ingest_at = Some(now);
            state.last_ingest_service_id = Some(svc.service_id.clone());
        }
        for deferred in &flushed_idle {
            state.runs.insert(0, deferred.run.clone());
        }
        state.runs.insert(0, run.clone());
        state.runs.truncate(MAX_HISTORY_ENTRIES);
        state.jobs.merge(&jobs);
        // Relative to the start this tick dispatched: a concurrent writer that already moved
        // `maint_next` wins, so a stale reply never moves it backwards.
        match maint_reply.filter(|_| state.maint_next % logs::MAINT_STAGES.len() == maint_start) {
            Some(Some(next)) => {
                state.maint_next = next;
                state.maint_misses = 0;
            }
            Some(None) => {
                state.maint_misses += 1;
                if state.maint_misses >= MAINT_MISS_LIMIT {
                    state.maint_next = (maint_start + 1) % logs::MAINT_STAGES.len();
                    state.maint_misses = 0;
                }
            }
            None => {}
        }
    })?;

    runtime.admin_event(json!({
        "event": "cron.tick",
        "id": run.id,
        "trigger": run.trigger,
        "actor": run.actor,
        "outcome": run.outcome,
        "bucket": run.bucket,
        "service_id": run.service_id,
        "fos_manifest_key": run.fos_manifest_key,
    }));

    Ok(Dispatched {
        mirror: Some(
            schedule::Mirror::from_state(&cron_state, Some(svc.log_period)).with_rum(rum_period),
        ),
        value: tick_response(
            true,
            service_id,
            cron_state.last_tick,
            Some(run),
            cron_state.runs,
        ),
        ran: true,
        cron_generation: None,
        deferred_idle: Vec::new(),
        worker_telemetry: Some(worker_telemetry),
    })
}

pub fn cron_and_service_audit(
    runtime: &impl Runtime,
) -> (Option<String>, Option<u64>, Vec<CronRun>, Vec<Value>) {
    let snapshot = cron_audit_snapshot(runtime);
    (
        snapshot.service_id,
        snapshot.last_tick,
        snapshot.runs,
        snapshot.service_audit,
    )
}

pub struct CronAuditSnapshot {
    pub service_id: Option<String>,
    pub last_tick: Option<u64>,
    pub runs: Vec<CronRun>,
    pub service_audit: Vec<Value>,
    pub last_ingest_at: Option<String>,
    pub schedules: Vec<Value>,
}

pub fn cron_audit_snapshot(runtime: &impl Runtime) -> CronAuditSnapshot {
    let cron_state: CronState = runtime
        .admin_get("cron:state")
        .ok()
        .flatten()
        .and_then(|r| serde_json::from_value(r.value).ok())
        .unwrap_or_default();

    let (svc_rec, active_service) = load_installed_service(runtime)
        .map(|(rec, svc, _)| (rec, svc))
        .unwrap_or((None, None));
    let schedules = schedule::task_schedules(runtime, &cron_state, active_service.as_ref());
    let active_service_id = active_service.map(|svc| svc.service_id);
    let last_ingest_at = active_service_id
        .as_deref()
        .and_then(|sid| ingest_time(&cron_state, sid));
    let service_audit = project_service_audit(svc_rec);
    let active_runs = if let Some(ref sid) = active_service_id {
        cron_state
            .runs
            .into_iter()
            .filter(|r| r.service_id.as_deref() == Some(sid.as_str()))
            .collect()
    } else {
        Vec::new()
    };
    CronAuditSnapshot {
        service_id: active_service_id,
        last_tick: schedule::last_seen(runtime).or(cron_state.last_tick),
        runs: active_runs,
        service_audit,
        last_ingest_at,
        schedules,
    }
}

pub fn service_audit(runtime: &impl Runtime) -> Vec<Value> {
    let record = load_installed_service(runtime)
        .ok()
        .and_then(|(record, _, _)| record);
    project_service_audit(record)
}

fn project_service_audit(svc_rec: Option<Record>) -> Vec<Value> {
    let mut service_audit = Vec::new();

    if let Some(svc_rec) = svc_rec {
        let sid = svc_rec.value["service_id"].as_str().unwrap_or_default();
        if let Some(outbox) = svc_rec.value["_audit_outbox"].as_array() {
            for (idx, item) in outbox.iter().enumerate() {
                let at = item["at"].as_u64().unwrap_or(0);
                let actor = item["actor"].as_str().unwrap_or("system");
                let event = item["event"].as_str().unwrap_or("service.event");
                let cfg_ver = item["config_version"].as_u64();
                let job_id = item["job_id"].as_str();
                let details = match (job_id, cfg_ver) {
                    (Some(jid), Some(v)) => format!("Job {jid} · config v{v}"),
                    (None, Some(v)) => format!("Config v{v}"),
                    (Some(jid), None) => format!("Job {jid}"),
                    (None, None) => String::new(),
                };
                service_audit.push(json!({
                    "id": format!("svc-{sid}-{idx}"),
                    "time": rfc3339_from_secs(at),
                    "timestamp": at,
                    "category": "control_plane",
                    "actor": actor,
                    "action": event,
                    "target": sid,
                    "details": details,
                }));
            }
        }
    }

    service_audit
}

pub fn last_ingest_at(runtime: &impl Runtime, service_id: Option<&str>) -> Option<String> {
    let state: CronState = runtime
        .admin_get("cron:state")
        .ok()
        .flatten()
        .and_then(|r| serde_json::from_value(r.value).ok())?;
    ingest_time(&state, service_id?)
}

/// The durable marker only exists for ingests after it was introduced, and only for one service,
/// so fall back to the newest retained run of this service that converted files.
fn ingest_time(state: &CronState, service_id: &str) -> Option<String> {
    if state.last_ingest_service_id.as_deref() == Some(service_id) {
        if let Some(at) = state.last_ingest_at {
            return Some(rfc3339_from_secs(at));
        }
    }
    state
        .runs
        .iter()
        .filter(|run| run.service_id.as_deref() == Some(service_id) && run.converted_raw_files > 0)
        .map(|run| run.started_at.clone())
        .max()
}

/// Service history is the service outbox only; user and admin actions live in the audit log.
pub fn service_history(service_audit: Vec<Value>) -> Vec<Value> {
    let mut combined = service_audit;
    combined.sort_by(|a, b| {
        let ta = a["timestamp"].as_u64().unwrap_or(0);
        let tb = b["timestamp"].as_u64().unwrap_or(0);
        tb.cmp(&ta)
    });
    combined.truncate(MAX_HISTORY_ENTRIES);
    combined
}

#[cfg(test)]
mod last_ingest_tests {
    use super::*;

    fn run(service: &str, started_at: &str, converted: u64) -> CronRun {
        CronRun {
            id: started_at.into(),
            started_at: started_at.into(),
            finished_at: started_at.into(),
            duration_ms: 0,
            outcome: "done".into(),
            trigger: "clock".into(),
            actor: "clock".into(),
            bucket: 0,
            service_id: Some(service.into()),
            listed_raw_files: converted,
            straggler_raw_files: 0,
            converted_raw_files: converted,
            deleted_raw_files: 0,
            rows_written: 0,
            bytes_written: 0,
            backend_requests_used: 0,
            fos_manifest_key: None,
            error: None,
        }
    }

    #[test]
    fn falls_back_to_newest_converting_run_of_the_service() {
        let state = CronState {
            runs: vec![
                run("A", "2026-10-30T21:06:00Z", 0),
                run("B", "2026-10-30T20:50:00Z", 3),
                run("A", "2026-10-30T20:45:59Z", 4),
                run("A", "2026-10-30T20:42:00Z", 1),
            ],
            ..Default::default()
        };
        assert_eq!(
            ingest_time(&state, "A").as_deref(),
            Some("2026-10-30T20:45:59Z")
        );
        assert_eq!(ingest_time(&state, "C"), None);
    }

    #[test]
    fn durable_marker_wins_only_for_its_own_service() {
        let state = CronState {
            last_ingest_at: Some(1_793_394_000),
            last_ingest_service_id: Some("A".into()),
            runs: vec![run("B", "2026-10-30T20:50:00Z", 3)],
            ..Default::default()
        };
        assert_eq!(
            ingest_time(&state, "A").as_deref(),
            Some(rfc3339_from_secs(1_793_394_000).as_str())
        );
        assert_eq!(
            ingest_time(&state, "B").as_deref(),
            Some("2026-10-30T20:50:00Z")
        );
    }
}

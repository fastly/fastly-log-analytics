use super::*;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};

pub const SCHEMA_VERSION: u32 = 1;

const STRIPPED_SERVICE_FIELDS: &[&str] = &[
    "fastly_token_ref",
    "fos_access_key_ref",
    "fos_secret_key_ref",
    "pending_job_id",
    "data_reset",
    "generation",
    "_audit",
    "_audit_outbox",
    "_audit_outbox_base",
    "_job_checkpoint",
];

const SECRET_KEY_SUBSTRINGS: &[&str] = &[
    "token",
    "secret",
    "password",
    "passwd",
    "passcode",
    "api_key",
    "access_key",
    "secret_key",
    "private_key",
    "credential",
    "fos_access_key",
    "fos_secret_key",
    "fastly_api_key",
    "cdn_key",
    "front_key",
    "session_key",
];

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CloudDrState {
    pub schema_version: u32,
    pub updated_at: u64,
    pub service_id: String,
    pub service: Value,
    #[serde(default)]
    pub views: Vec<crate::views::SavedView>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_head: Option<crate::ingest::catalog::Head>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub floor_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rum_stats: Option<Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoverInput {
    pub token: String,
    pub service_id: String,
    pub fos_bucket: String,
    pub fos_region: String,
    #[serde(default)]
    pub fos_prefix: String,
    #[serde(default)]
    pub fos_access_key: Option<String>,
    #[serde(default)]
    pub fos_secret_key: Option<String>,
}

pub fn state_key(service: &Service) -> String {
    format!("{}meta/cloud_dr_state.json", crate::ingest::scope(service))
}

fn is_secret_key(key: &str) -> bool {
    let norm = key.to_ascii_lowercase().replace('-', "_");
    SECRET_KEY_SUBSTRINGS
        .iter()
        .any(|needle| norm.contains(needle))
}

pub fn scrub_secrets(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.retain(|k, _| !is_secret_key(k));
            for v in map.values_mut() {
                scrub_secrets(v);
            }
        }
        Value::Array(items) => {
            for item in items.iter_mut() {
                scrub_secrets(item);
            }
        }
        _ => {}
    }
}

pub fn sanitize_service(service: &Service) -> Result<Value, Error> {
    let mut value = serialize(service)?;
    sanitize_service_value(&mut value);
    Ok(value)
}

fn sanitize_service_value(value: &mut Value) {
    if let Some(map) = value.as_object_mut() {
        map.retain(|k, _| !k.starts_with('_') && !STRIPPED_SERVICE_FIELDS.contains(&k.as_str()));
    }
    scrub_secrets(value);
}

pub fn build_dr_state(
    service: &Service,
    views: Vec<crate::views::SavedView>,
    catalog_head: Option<crate::ingest::catalog::Head>,
    floor_key: Option<String>,
    rum_stats: Option<Value>,
    updated_at: u64,
) -> Result<Value, Error> {
    let sanitized_service = sanitize_service(service)?;
    let state = CloudDrState {
        schema_version: SCHEMA_VERSION,
        updated_at,
        service_id: service.service_id.clone(),
        service: sanitized_service,
        views,
        catalog_head,
        floor_key,
        rum_stats,
    };
    let mut value = serialize(&state)?;
    if let Some(obj) = value.as_object_mut() {
        for field in ["service", "views", "rum_stats"] {
            if let Some(sub) = obj.get_mut(field) {
                scrub_secrets(sub);
            }
        }
    }
    Ok(value)
}

/// Best-effort mirror of non-secret `svc:<sid>`, `views:<sid>`, and catalog head metadata to
/// `{scope}meta/cloud_dr_state.json`. Never fails the caller and never overflows the backend
/// call budget.
pub fn sync_best_effort(runtime: &impl Runtime, service: &Service) {
    if service.status != "active" {
        return;
    }
    let Some((ak, sk)) = crate::admin::fos_credentials(runtime, service) else {
        return;
    };
    if runtime.admin_remaining_calls() < 4 {
        return;
    }

    let Ok(views_rec) = runtime.admin_get(&crate::views::key(&service.service_id)) else {
        return;
    };
    let views: Vec<crate::views::SavedView> = views_rec
        .and_then(|rec| serde_json::from_value(rec.value["views"].clone()).ok())
        .unwrap_or_default();

    let Ok(wm_rec) = runtime.admin_get(crate::ingest::WATERMARK_KEY) else {
        return;
    };
    let (catalog_head, floor_key) = match wm_rec {
        Some(wm) => {
            let head = crate::ingest::catalog::head(&wm.value)
                .ok()
                .filter(|h| h.generation > 0 || h.manifest.is_some() || !h.days.is_empty());
            let floor = wm.value[crate::ingest::FLOOR_FIELD]
                .as_str()
                .map(str::to_owned);
            (head, floor)
        }
        None => (None, None),
    };

    let Ok(rum_rec) = runtime.admin_get(&crate::ingest::watermark_key(crate::ingest::rum::STREAM))
    else {
        return;
    };
    let rum_stats = rum_rec.and_then(|rec| {
        rec.value
            .get(crate::ingest::RUM_STATS_FIELD)
            .filter(|v| !v.is_null())
            .cloned()
    });

    if runtime.admin_remaining_calls() == 0 {
        return;
    }

    let Ok(state_value) = build_dr_state(
        service,
        views,
        catalog_head,
        floor_key,
        rum_stats,
        runtime.now(),
    ) else {
        return;
    };
    let Ok(body) = serde_json::to_vec(&state_value) else {
        return;
    };
    let key = state_key(service);
    if let Some(usage) = runtime.usage_agg() {
        usage.set_caller("admin:dr_state");
    }
    let result = runtime.admin_fos(FosRequest {
        method: "PUT",
        region: &service.fos_region,
        bucket: &service.fos_bucket,
        key: &key,
        query: "",
        access_key: &ak,
        secret_key: &sk,
        body,
        timeout: super::transport::FOS_TIMEOUT,
        response_limit: super::transport::FOS_RESPONSE_LIMIT,
        content_md5: None,
    });
    let failed = match result {
        Ok(resp) => !(200..300).contains(&resp.status),
        Err(_) => true,
    };
    if failed {
        runtime.admin_event(json!({
            "event": "admin.dr_state.sync_failed",
            "level": "warn",
            "service_id": service.service_id,
            "key": key,
        }));
    }
}

fn day_start(ts: i64) -> i64 {
    ts - ts.rem_euclid(86_400)
}

fn bootstrap_service(owner_id: String, input: &RecoverInput, now: u64) -> Service {
    Service {
        service_id: input.service_id.clone(),
        name: input.service_id.clone(),
        status: "active".into(),
        created_at: now,
        updated_at: now,
        config_version: 1,
        logging_enabled: true,
        logging_endpoint_name: "Fastly Object Storage Logs".into(),
        log_period: 60,
        sample_rate: 100,
        edge_only: true,
        custom_condition: String::new(),
        fos_bucket: input.fos_bucket.clone(),
        fos_prefix: input.fos_prefix.clone(),
        fos_region: input.fos_region.clone(),
        fos_endpoint: format!("{}.object.fastlystorage.app", input.fos_region),
        log_fields: default_fields(),
        cmcd: crate::logging::cmcd::Config::default(),
        rum: crate::logging::rum::Config::default(),
        field_limits: json!({}),
        fastly_token_ref: format!("fastly_token.{}", input.service_id),
        fos_access_key_ref: format!("fos_ak.{}", input.service_id),
        fos_secret_key_ref: format!("fos_sk.{}", input.service_id),
        owner: owner_id,
        bucket_adopted: true,
        bucket_checked: true,
        last_activated_version: None,
        last_reconcile: None,
        pending_job_id: None,
        owned_endpoint_names: Vec::new(),
        delete_after: true,
        log_retention_days: 30,
        rum_retention_days: 30,
        compaction: Compaction::default_provision(),
        compression_codec: "zstd".into(),
        wire_format: "array".into(),
        data_reset: None,
        capture: Capture::default(),
        scoring: None,
        cdn_shield: String::new(),
    }
}

fn safe_dr_object_key(key: &str, required_prefix: &str) -> bool {
    !key.is_empty()
        && key.len() <= 512
        && key.starts_with(required_prefix)
        && !key.contains("..")
        && !key.contains('\\')
        && !key.chars().any(char::is_control)
}

fn validate_dr_scoring(
    sc: &ScoringConfig,
    expected_service_id: &str,
    own_sids: &[String],
    own_store_ids: &[String],
) -> Result<(), Error> {
    if !super::valid_service_id(&sc.scoring_service_id)
        || sc.scoring_service_id == expected_service_id
        || own_sids.iter().any(|id| id == &sc.scoring_service_id)
        || !super::scoring::valid_scoring_domain(&sc.scoring_domain)
        || sc.scoring_service_name.chars().count() > 200
        || sc.scoring_service_name.chars().any(char::is_control)
        || super::scoring::validate_exclude_url_regex(&sc.exclude_url_regex).is_err()
        || super::scoring::recv_snippet(Some(&sc.exclude_url_regex)).is_err()
        || sc
            .enforce_status_code
            .is_some_and(|c| !(400..=599).contains(&c))
        || sc.threshold.is_some_and(|t| t > 100)
        || sc
            .matrix_version
            .as_deref()
            .is_some_and(|v| !super::scoring::valid_matrix_version(v))
    {
        return Err(Error::new(400, "invalid_dr_scoring_config"));
    }
    let mut seen_stores = BTreeSet::new();
    for store_id in [
        sc.scoring_keys_store_id.as_str(),
        sc.scoring_config_store_id.as_str(),
        sc.scoring_matrix_store_id.as_str(),
    ] {
        if (sc.enabled || !store_id.is_empty())
            && (!super::scoring::valid_store_id(store_id)
                || own_store_ids.iter().any(|id| id == store_id)
                || !seen_stores.insert(store_id))
        {
            return Err(Error::new(400, "invalid_dr_scoring_config"));
        }
    }
    for meta in [
        Some(sc.enabled_at.as_str()),
        Some(sc.deployed_package_sha.as_str()),
        Some(sc.deployed_package_files_hash.as_str()),
        Some(sc.deployed_vcl_sha.as_str()),
        sc.last_retrain_at.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if meta.chars().count() > 128 || meta.chars().any(char::is_control) {
            return Err(Error::new(400, "invalid_dr_scoring_config"));
        }
    }
    Ok(())
}

fn validate_dr_views(
    views: &mut [crate::views::SavedView],
    expected_service_id: &str,
) -> Result<(), Error> {
    if views.len() > crate::views::MAX_VIEWS {
        return Err(Error::new(400, "invalid_dr_views"));
    }
    let mut seen_ids = BTreeSet::new();
    for view in views {
        if view.service_id != expected_service_id {
            return Err(Error::new(400, "invalid_dr_views"));
        }
        if !crate::views::valid_view_id(&view.id) || !seen_ids.insert(view.id.clone()) {
            return Err(Error::new(400, "invalid_dr_views"));
        }
        let trimmed_name = view.name.trim().to_owned();
        let name_chars = trimmed_name.chars().count();
        if !(1..=120).contains(&name_chars) || trimmed_name.chars().any(char::is_control) {
            return Err(Error::new(400, "invalid_dr_views"));
        }
        view.name = trimmed_name;
        if view.filters_json.len() > 32 * 1024
            || view.filters_json.contains('\0')
            || serde_json::from_str::<Value>(&view.filters_json).is_err()
            || crate::views::json_has_duplicate_keys(&view.filters_json)
        {
            return Err(Error::new(400, "invalid_dr_views"));
        }
        for opt in [
            &view.time_range_type,
            &view.start_time,
            &view.end_time,
            &view.page,
            &view.created_at,
        ]
        .into_iter()
        .flatten()
        {
            if opt.chars().count() > 200 || opt.chars().any(char::is_control) {
                return Err(Error::new(400, "invalid_dr_views"));
            }
        }
    }
    Ok(())
}

fn validate_dr_catalog_head(head: &crate::ingest::catalog::Head, scope: &str) -> Result<(), Error> {
    let catalog_prefix = format!("{scope}processed/_catalog/");
    let processed_prefix = format!("{scope}processed/");
    if head
        .manifest
        .as_deref()
        .is_some_and(|key| !safe_dr_object_key(key, &catalog_prefix))
    {
        return Err(Error::new(400, "invalid_dr_catalog_head"));
    }
    for ev in &head.recent {
        if ev.cursor.chars().any(char::is_control)
            || ev
                .manifest
                .as_deref()
                .is_some_and(|key| !safe_dr_object_key(key, &catalog_prefix))
        {
            return Err(Error::new(400, "invalid_dr_catalog_head"));
        }
    }
    for key in head.days.values() {
        if !safe_dr_object_key(key, &catalog_prefix) {
            return Err(Error::new(400, "invalid_dr_catalog_head"));
        }
    }
    for map in [&head.hour_bundles, &head.open_bundles, &head.day_bundles] {
        for r in map.values() {
            if !safe_dr_object_key(&r.key, &catalog_prefix) {
                return Err(Error::new(400, "invalid_dr_catalog_head"));
            }
        }
    }
    for key in head.bundle_retired.keys() {
        if !safe_dr_object_key(key, &processed_prefix) {
            return Err(Error::new(400, "invalid_dr_catalog_head"));
        }
    }
    if let Some(ref commit) = head.last_compaction {
        for f in &commit.files {
            if !safe_dr_object_key(&f.key, &processed_prefix) {
                return Err(Error::new(400, "invalid_dr_catalog_head"));
            }
        }
        for key in &commit.replaces {
            if !safe_dr_object_key(key, &processed_prefix) {
                return Err(Error::new(400, "invalid_dr_catalog_head"));
            }
        }
    }
    Ok(())
}

fn hydrate_service(
    raw: Value,
    fallback: &Service,
    owner_id: &str,
    input: &RecoverInput,
    now: u64,
) -> Result<Service, Error> {
    let Some(raw_map) = raw.as_object() else {
        return Err(Error::new(400, "invalid_dr_service"));
    };
    let mut base = serialize(fallback)?;
    if let Some(base_map) = base.as_object_mut() {
        for (k, v) in raw_map {
            if !k.starts_with('_') && !STRIPPED_SERVICE_FIELDS.contains(&k.as_str()) {
                base_map.insert(k.clone(), v.clone());
            }
        }
    }
    let mut service: Service =
        serde_json::from_value(base).map_err(|_| Error::new(400, "invalid_dr_service"))?;
    let trimmed_name = service.name.trim().to_owned();
    let name_chars = trimmed_name.chars().count();
    if !(1..=200).contains(&name_chars) || trimmed_name.chars().any(char::is_control) {
        return Err(Error::new(400, "invalid_dr_service"));
    }
    service.name = trimmed_name;
    service.service_id = input.service_id.clone();
    service.owner = owner_id.to_owned();
    service.fastly_token_ref = format!("fastly_token.{}", input.service_id);
    service.fos_access_key_ref = format!("fos_ak.{}", input.service_id);
    service.fos_secret_key_ref = format!("fos_sk.{}", input.service_id);
    service.fos_bucket = input.fos_bucket.clone();
    service.fos_region = input.fos_region.clone();
    service.fos_prefix = input.fos_prefix.clone();
    service.fos_endpoint = format!("{}.object.fastlystorage.app", input.fos_region);
    service.status = "active".into();
    service.pending_job_id = None;
    service.data_reset = None;
    service.bucket_checked = true;
    service.bucket_adopted = true;
    service.updated_at = now;
    if service.compression_codec.is_empty() {
        service.compression_codec = "zstd".into();
    }
    if matches!(service.wire_format.as_str(), "" | "json") {
        service.wire_format = "array".into();
    }
    if service.capture == Capture::Attached && service.scoring.is_some() {
        return Err(Error::new(400, "invalid_dr_scoring_config"));
    }
    if service.capture == Capture::Attached
        && (!service.rum.enabled || service.rum.endpoint_name == "Fastly RUM Logs")
    {
        service.rum.endpoint_name = ATTACH_RUM_ENDPOINT.into();
    }
    Ok(service)
}

fn scan_catalog_head(
    context: &Context<'_, impl Runtime>,
    service: &Service,
    creds: &(String, String),
    reserve_calls: u32,
    foreign_dr_state: bool,
) -> Result<Option<crate::ingest::catalog::Head>, Error> {
    if context.remaining_calls() <= reserve_calls + 1 {
        return Ok(None);
    }
    let scope = crate::ingest::scope(service);
    let prefix = format!("{scope}processed/_catalog/");
    let page = match fos::list_objects(context, service, creds, &prefix, None) {
        Ok(page) => page,
        Err(_) => return Ok(None),
    };
    let mut objects: Vec<_> = page
        .objects
        .into_iter()
        .filter(|obj| obj.key.ends_with(".json") && safe_dr_object_key(&obj.key, &prefix))
        .collect();
    objects.sort_by(|a, b| b.last_modified.cmp(&a.last_modified));

    let mut best_manifest: Option<(String, crate::ingest::catalog::Manifest)> = None;
    let mut saw_foreign_manifest = false;
    let mut days: BTreeMap<i64, (u64, String, crate::ingest::catalog::Statistics)> =
        BTreeMap::new();

    for obj in objects {
        if context.remaining_calls() <= reserve_calls {
            break;
        }
        let Ok(Some(bytes)) = fos::get_object(context, service, creds, &obj.key) else {
            continue;
        };
        if let Ok(manifest) = serde_json::from_slice::<crate::ingest::catalog::Manifest>(&bytes) {
            if manifest.committed_at.chars().any(char::is_control) {
                saw_foreign_manifest = true;
                continue;
            }
            if manifest.service_id == service.service_id {
                if best_manifest
                    .as_ref()
                    .is_none_or(|(_, prev)| manifest.generation > prev.generation)
                {
                    best_manifest = Some((obj.key, manifest));
                }
            } else {
                saw_foreign_manifest = true;
            }
            continue;
        }
        if let Ok(day) = serde_json::from_slice::<crate::ingest::catalog::Day>(&bytes) {
            if let Some(day_ts) = day
                .hours
                .keys()
                .next()
                .map(|h| day_start(*h))
                .or_else(|| day.statistics.earliest.map(day_start))
            {
                let max_gen = day.hours.values().map(|r| r.generation).max().unwrap_or(0);
                let replace = days
                    .get(&day_ts)
                    .is_none_or(|(prev_gen, _, _)| max_gen >= *prev_gen);
                if replace {
                    days.insert(day_ts, (max_gen, obj.key, day.statistics));
                }
            }
        }
    }

    if best_manifest.is_none() && (days.is_empty() || foreign_dr_state || saw_foreign_manifest) {
        return Ok(None);
    }

    let max_day_gen = days.values().map(|(g, _, _)| *g).max().unwrap_or(0);
    let generation = best_manifest
        .as_ref()
        .map(|(_, m)| m.generation)
        .unwrap_or(max_day_gen)
        .max(1);
    let manifest_key = best_manifest.as_ref().map(|(k, _)| k.clone());
    let committed_at = best_manifest.as_ref().map(|(_, m)| m.committed_at.clone());

    let mut head = crate::ingest::catalog::Head {
        version: crate::ingest::catalog::VERSION,
        generation,
        manifest: manifest_key.clone(),
        recent: vec![crate::ingest::catalog::Event {
            cursor: format!("{}:{generation}", service.service_id),
            generation,
            committed_at,
            manifest: manifest_key,
        }],
        ..crate::ingest::catalog::Head::default()
    };
    for (day_ts, (_, key, stats)) in days {
        head.days.insert(day_ts, key);
        head.statistics.insert(day_ts, stats);
    }

    let encoded = serialize(&head)?;
    if crate::ingest::catalog::head(&json!({"catalog": encoded})).is_ok() {
        Ok(Some(head))
    } else {
        Ok(None)
    }
}

fn kv_payload_matches(key: &str, existing: &Value, desired: &Value) -> bool {
    if key.starts_with("views:") {
        return existing.get("views") == desired.get("views");
    }
    match (existing.as_object(), desired.as_object()) {
        (Some(ex_map), Some(des_map)) => {
            let ex_filtered = ex_map
                .iter()
                .filter(|(k, _)| !matches!(k.as_str(), "_kv_write_id" | "_kv_written_ms"));
            let des_filtered = des_map
                .iter()
                .filter(|(k, _)| !matches!(k.as_str(), "_kv_write_id" | "_kv_written_ms"));
            ex_filtered.eq(des_filtered)
        }
        _ => existing == desired,
    }
}

fn upsert_kv(
    context: &Context<'_, impl Runtime>,
    key: &str,
    value: &Value,
    allow_insert_first: bool,
    try_insert_first: &mut bool,
    remaining_upserts: u32,
) -> Result<(), Error> {
    if *try_insert_first {
        match context.cas(key, None, value) {
            Ok(()) => {
                *try_insert_first =
                    allow_insert_first && context.remaining_calls() > 2 + 2 * remaining_upserts;
                return Ok(());
            }
            Err(err) if err.status == 409 => {}
            Err(err) => return Err(err),
        }
    }
    let existing = context.get(key)?;
    let res = if existing
        .as_ref()
        .is_some_and(|r| kv_payload_matches(key, &r.value, value))
    {
        Ok(())
    } else {
        context.cas(key, existing.as_ref().map(|r| r.generation.as_str()), value)
    };
    *try_insert_first = allow_insert_first && context.remaining_calls() > 2 + 2 * remaining_upserts;
    res
}

fn verify_candidate_endpoints(
    service: &Service,
    scope: &str,
    candidate_names: &[String],
    s3_items: &[Value],
    fos_items: &[Value],
) -> Result<Vec<String>, Error> {
    let mut verified_owned = Vec::new();
    for ep_name in candidate_names {
        if s3_items
            .iter()
            .any(|v| v["name"].as_str() == Some(ep_name.as_str()))
        {
            return Err(Error::new(409, "resource_name_collision"));
        }
        if let Some(found) = fos_items
            .iter()
            .find(|v| v["name"].as_str() == Some(ep_name.as_str()))
        {
            let cond = found["response_condition"].as_str().unwrap_or("");
            let path = found["path"].as_str().unwrap_or("");
            let cond_ok = match service.capture {
                Capture::Attached => {
                    if ep_name == ATTACH_RUM_ENDPOINT {
                        cond == ATTACH_RUM_CONDITION
                    } else {
                        matches!(cond, ATTACH_CONDITION | ATTACH_RUM_CONDITION)
                    }
                }
                Capture::Managed => matches!(cond, "log_analytics_condition" | "rum_log_condition"),
            };
            if found["bucket_name"].as_str() != Some(service.fos_bucket.as_str())
                || !super::reconcile::endpoint_path_matches_scope(path, scope)
                || !cond_ok
            {
                return Err(Error::new(409, "resource_name_collision"));
            }
            verified_owned.push(ep_name.clone());
        }
    }
    Ok(verified_owned)
}

fn inspect_active_version_for_dr(
    context: &Context<'_, impl Runtime>,
    service: &mut Service,
    token: &str,
    candidate_names: &[String],
    minimal_fallback: bool,
    reserve_after: u32,
    mandatory_reserve: u32,
) -> Result<(), Error> {
    let scope = crate::ingest::scope(service);
    let ver = match super::reconcile::active_version(context, &service.service_id, token) {
        Ok(ver) => ver,
        Err(err) if err.status == 403 => {
            return Err(Error::new(403, "fastly_token_scope_insufficient"));
        }
        Err(_) => return Ok(()),
    };
    let encoded_sid = encode(&service.service_id);
    let (s3_st, s3_val) = context.api_status(
        "GET",
        &format!("/service/{encoded_sid}/version/{ver}/logging/s3"),
        token,
        None,
    )?;
    if s3_st == 401 || s3_st == 403 {
        return Err(Error::new(403, "fastly_token_scope_insufficient"));
    }
    let (fos_st, fos_val) = context.api_status(
        "GET",
        &format!("/service/{encoded_sid}/version/{ver}/logging/fos"),
        token,
        None,
    )?;
    if fos_st == 401 || fos_st == 403 {
        return Err(Error::new(403, "fastly_token_scope_insufficient"));
    }
    if !(200..300).contains(&s3_st) || !(200..300).contains(&fos_st) {
        return Ok(());
    }
    let (Ok(s3_items), Ok(fos_items)) = (list(&s3_val), list(&fos_val)) else {
        return Ok(());
    };

    let mut coexist = super::reconcile::has_attach_endpoint_marker(s3_items, fos_items);
    if !coexist
        && (minimal_fallback || service.capture == Capture::Managed)
        && context.remaining_calls() > reserve_after
    {
        let (cond_st, cond_val) = context.api_status(
            "GET",
            &format!("/service/{encoded_sid}/version/{ver}/condition"),
            token,
            None,
        )?;
        if cond_st == 401 || cond_st == 403 {
            return Err(Error::new(403, "fastly_token_scope_insufficient"));
        }
        if (200..300).contains(&cond_st) {
            if let Ok(cond_items) = list(&cond_val) {
                if super::reconcile::has_attach_condition_marker(cond_items) {
                    coexist = true;
                }
            }
        }
    }
    if !coexist
        && (minimal_fallback || service.capture == Capture::Managed)
        && super::reconcile::has_foreign_primary_endpoint(
            service, &scope, s3_items, fos_items, false,
        )
        && context.remaining_calls() > mandatory_reserve
    {
        let (snip_st, snip_val) = context.api_status(
            "GET",
            &format!("/service/{encoded_sid}/version/{ver}/snippet"),
            token,
            None,
        )?;
        if snip_st == 401 || snip_st == 403 {
            return Err(Error::new(403, "fastly_token_scope_insufficient"));
        }
        if (200..300).contains(&snip_st) {
            if let Ok(snip_items) = list(&snip_val) {
                if super::reconcile::capture_present(snip_items) {
                    coexist = true;
                }
            }
        }
    }

    if minimal_fallback {
        if coexist {
            service.capture = Capture::Attached;
            service.logging_endpoint_name = ATTACH_ENDPOINT.into();
            service.rum.endpoint_name = ATTACH_RUM_ENDPOINT.into();
            let attach_candidates =
                vec![ATTACH_ENDPOINT.to_owned(), ATTACH_RUM_ENDPOINT.to_owned()];
            service.owned_endpoint_names = verify_candidate_endpoints(
                service,
                &scope,
                &attach_candidates,
                s3_items,
                fos_items,
            )?;
        } else {
            let managed_candidates = vec![service.logging_endpoint_name.clone()];
            service.owned_endpoint_names = verify_candidate_endpoints(
                service,
                &scope,
                &managed_candidates,
                s3_items,
                fos_items,
            )?;
        }
        return Ok(());
    }

    if service.capture == Capture::Managed && coexist {
        return Err(Error::new(409, "resource_name_collision"));
    }
    service.owned_endpoint_names =
        verify_candidate_endpoints(service, &scope, candidate_names, s3_items, fos_items)?;
    Ok(())
}

fn sync_front_dictionaries(
    context: &Context<'_, impl Runtime>,
    service: &Service,
    token: &str,
    (ak, sk): &(String, String),
) -> bool {
    if context.remaining_calls() < 3 {
        return false;
    }
    let Ok(front_sid) = config(context.runtime, "front_service_id") else {
        return false;
    };
    let Ok(buckets_dict) = config(context.runtime, "buckets_dictionary_id") else {
        return false;
    };
    let Ok(creds_dict) = config(context.runtime, "credentials_dictionary_id") else {
        return false;
    };

    let sid = &service.service_id;
    let bucket_val = format!("{}|{}", service.fos_bucket, service.fos_region);
    let ak_key = format!("{sid}.ak");
    let sk_key = format!("{sid}.sk");
    let items: [(&str, &str, &str); 3] = [
        (&buckets_dict, sid, &bucket_val),
        (&creds_dict, &ak_key, ak),
        (&creds_dict, &sk_key, sk),
    ];

    for (dict_id, item_key, item_value) in items {
        let path = format!(
            "/service/{front_sid}/dictionary/{dict_id}/item/{}",
            encode(item_key)
        );
        let body = json!({
            "item_key": item_key,
            "item_value": item_value,
        });
        match context.api_status("PUT", &path, token, Some(body)) {
            Ok((status, _)) if (200..300).contains(&status) => {}
            _ => return false,
        }
    }
    true
}

/// Cold-start recovery from FOS (`POST /api/admin/provision/recover` and `/api/provision/recover`).
pub(super) fn recover(
    context: &Context<'_, impl Runtime>,
    actor: &str,
    input: RecoverInput,
) -> Result<Value, Error> {
    if !valid_service_id(&input.service_id) {
        return Err(Error::new(400, "invalid_service_id"));
    }
    let own_sids = own_services(context.runtime)?;
    if own_sids.iter().any(|id| id == &input.service_id) {
        return Err(Error::new(400, "install_service_forbidden"));
    }
    if !valid_bucket(&input.fos_bucket) {
        return Err(Error::new(400, "invalid_bucket"));
    }
    if !REGIONS.contains(&input.fos_region.as_str()) {
        return Err(Error::new(400, "invalid_region"));
    }
    if !valid_prefix(&input.fos_prefix) {
        return Err(Error::new(400, "invalid_prefix"));
    }
    let explicit_creds = match (&input.fos_access_key, &input.fos_secret_key) {
        (None, None) => None,
        (Some(ak), Some(sk)) if !ak.is_empty() && !sk.is_empty() => Some((ak.clone(), sk.clone())),
        _ => return Err(Error::new(400, "invalid_input")),
    };

    validate_token(context, &input.token, Some(&input.service_id))?;
    context.confirm_with_token(&input.token);

    let (install_rec, mut install_val) = install(context)?;
    if install_val
        .service_id
        .as_deref()
        .is_some_and(|sid| sid != input.service_id)
    {
        return Err(Error::new(409, "install_already_reserved"));
    }
    let svc_key = format!("svc:{}", input.service_id);
    let existing_svc = context.get(&svc_key)?;
    if let Some(ref rec) = existing_svc {
        let install_owns_sid = install_val.service_id.as_deref() == Some(input.service_id.as_str());
        let has_pending_job = rec.value["pending_job_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty());
        if (install_owns_sid || has_pending_job)
            && rec.value["status"].as_str() != Some("torn_down")
        {
            return Err(Error::new(409, "service_already_configured"));
        }
    }
    let cold_start = install_rec.is_none() && existing_svc.is_none();
    const PER_UPSERT: u32 = 2;

    let now = context.runtime.now();
    let owner_id = owner(context.runtime)?;
    let fallback_svc = bootstrap_service(owner_id.clone(), &input, now);

    put_secret(
        context,
        &input.token,
        &fallback_svc.fastly_token_ref,
        &input.token,
    )?;
    let creds = match explicit_creds {
        Some((ak, sk)) => {
            fos::persist_explicit_credentials(context, &fallback_svc, &input.token, &ak, &sk)?;
            (ak, sk)
        }
        None => fos::ensure_key_with_credentials(context, &fallback_svc, &input.token)?,
    };

    let dr_key = state_key(&fallback_svc);
    let dr_bytes = fos::get_object(context, &fallback_svc, &creds, &dr_key)?;
    let raw_dr = dr_bytes.and_then(|bytes| serde_json::from_slice::<CloudDrState>(&bytes).ok());
    let foreign_dr_state = raw_dr
        .as_ref()
        .is_some_and(|state| state.service_id != input.service_id);
    let parsed_dr = raw_dr.filter(|state| {
        state.schema_version == SCHEMA_VERSION && state.service_id == input.service_id
    });

    let (
        mut service,
        views,
        mut catalog_head,
        floor_key,
        rum_stats,
        mut source,
        candidate_names,
        mut inspected_active,
    ) = if let Some(dr_state) = parsed_dr {
        let own_store_ids = super::scoring::own_stores(context.runtime);
        let mut service = hydrate_service(dr_state.service, &fallback_svc, &owner_id, &input, now)?;
        if let Some(ref sc) = service.scoring {
            validate_dr_scoring(sc, &input.service_id, &own_sids, &own_store_ids)?;
            super::scoring::verify_scoring_resources(context, &input.service_id, sc, &input.token)
                .map_err(|e| {
                    if e.status == 400 {
                        Error::new(400, "invalid_dr_scoring_config")
                    } else {
                        e
                    }
                })?;
        }
        let prev_snapshot = service.clone();
        super::reconcile_system_fields(&mut service, Some(&prev_snapshot))?;
        super::validate_service(
            &service,
            Some(&prev_snapshot),
            crate::ingest::window_seconds(context.runtime),
        )?;
        let mut candidate_names = Vec::new();
        if service.logging_enabled && !service.logging_endpoint_name.is_empty() {
            candidate_names.push(service.logging_endpoint_name.clone());
        }
        if service.rum.enabled {
            let rum_ep = match service.capture {
                Capture::Attached => ATTACH_RUM_ENDPOINT.to_owned(),
                Capture::Managed => service.rum.endpoint_name.clone(),
            };
            if !rum_ep.is_empty() && !candidate_names.contains(&rum_ep) {
                candidate_names.push(rum_ep);
            }
        }
        let mut views = dr_state.views;
        validate_dr_views(&mut views, &input.service_id)?;
        let scope = crate::ingest::scope(&service);
        if let Some(ref fk) = dr_state.floor_key {
            if !safe_dr_object_key(fk, &format!("{scope}raw/")) {
                return Err(Error::new(400, "invalid_dr_floor_key"));
            }
        }
        if let Some(ref head) = dr_state.catalog_head {
            validate_dr_catalog_head(head, &scope)?;
        }
        let valid_head = dr_state.catalog_head.filter(|h| {
            (h.generation > 0 || h.manifest.is_some() || !h.days.is_empty())
                && serde_json::to_value(h)
                    .ok()
                    .is_some_and(|v| crate::ingest::catalog::head(&json!({"catalog": v})).is_ok())
        });
        let scan_needed = if valid_head.is_none() { 2u32 } else { 0u32 };
        let num_dr_upserts = u32::from(!views.is_empty())
            + u32::from(valid_head.is_some() || dr_state.floor_key.is_some() || scan_needed > 0)
            + u32::from(dr_state.rum_stats.is_some());
        let mandatory_post = 2 + if cold_start {
            num_dr_upserts
        } else {
            PER_UPSERT * num_dr_upserts
        };
        let conservative_post = 5 + PER_UPSERT * num_dr_upserts;
        service.owned_endpoint_names.clear();
        let should_inspect = !candidate_names.is_empty() || service.capture == Capture::Managed;
        let mut inspected = false;
        if should_inspect && context.remaining_calls() >= conservative_post + scan_needed + 3 {
            inspect_active_version_for_dr(
                context,
                &mut service,
                &input.token,
                &candidate_names,
                false,
                conservative_post + scan_needed,
                mandatory_post,
            )?;
            inspected = true;
        } else if !should_inspect || context.remaining_calls() < mandatory_post + scan_needed + 3 {
            inspected = true;
        }
        (
            service,
            views,
            valid_head,
            dr_state.floor_key,
            dr_state.rum_stats,
            "dr_state",
            candidate_names,
            inspected,
        )
    } else {
        (
            fallback_svc.clone(),
            Vec::new(),
            None,
            None,
            None,
            "minimal",
            Vec::new(),
            false,
        )
    };

    if catalog_head.is_none() {
        let mut reserve = 2u32; // svc:<sid> + admin:install
        if !views.is_empty() {
            reserve += PER_UPSERT;
        }
        reserve += PER_UPSERT; // ingest:watermark if found (or floor_key.is_some())
        if rum_stats.is_some() {
            reserve += PER_UPSERT;
        }
        reserve += 3; // inline Front dictionary sync (or post-upsert active-version inspection)
        if let Some(scanned) =
            scan_catalog_head(context, &service, &creds, reserve, foreign_dr_state)?
        {
            catalog_head = Some(scanned);
            if source != "dr_state" {
                source = "catalog_scan";
            }
        }
    }

    let mut remaining_upserts = u32::from(!views.is_empty())
        + u32::from(catalog_head.is_some() || floor_key.is_some())
        + u32::from(rum_stats.is_some());
    let initial_try_insert_first =
        cold_start && context.remaining_calls() < 5 + 2 * remaining_upserts;
    let mut try_insert_first = initial_try_insert_first
        && context.remaining_calls() > 2 + 2 * remaining_upserts.saturating_sub(1);

    let views_restored = views.len();
    if !views.is_empty() {
        remaining_upserts -= 1;
        upsert_kv(
            context,
            &crate::views::key(&input.service_id),
            &json!({"views": views, "updated_at": now}),
            initial_try_insert_first,
            &mut try_insert_first,
            remaining_upserts,
        )?;
    }

    if catalog_head.is_some() || floor_key.is_some() {
        remaining_upserts -= 1;
        let mut wm = serde_json::Map::new();
        if let Some(ref head) = catalog_head {
            wm.insert("catalog".into(), serialize(head)?);
        }
        if let Some(ref floor) = floor_key {
            wm.insert(crate::ingest::FLOOR_FIELD.into(), json!(floor));
        }
        upsert_kv(
            context,
            crate::ingest::WATERMARK_KEY,
            &Value::Object(wm),
            initial_try_insert_first,
            &mut try_insert_first,
            remaining_upserts,
        )?;
    }

    if let Some(ref stats) = rum_stats {
        remaining_upserts -= 1;
        upsert_kv(
            context,
            &crate::ingest::watermark_key(crate::ingest::rum::STREAM),
            &json!({ crate::ingest::RUM_STATS_FIELD: stats }),
            initial_try_insert_first,
            &mut try_insert_first,
            remaining_upserts,
        )?;
    }

    let post_upsert_floor = if initial_try_insert_first || source != "dr_state" {
        2 + 3
    } else {
        5 + 3
    };
    if !inspected_active && context.remaining_calls() >= post_upsert_floor {
        let reserve_after = if context.remaining_calls() >= 8 { 5 } else { 2 };
        inspect_active_version_for_dr(
            context,
            &mut service,
            &input.token,
            &candidate_names,
            source != "dr_state",
            reserve_after,
            2,
        )?;
        inspected_active = true;
    }
    let _ = inspected_active;

    let mut svc_value = serialize(&service)?;
    svc_value["_audit"] = json!({
        "actor": actor,
        "at": now,
        "event": "service.recovered",
        "source": source,
        "config_version": service.config_version,
    });
    if context.remaining_calls() < 2 {
        return Err(Error::new(503, "backend_budget"));
    }
    context.cas(
        &svc_key,
        existing_svc.as_ref().map(|r| r.generation.as_str()),
        &svc_value,
    )?;

    install_val.service_id = Some(input.service_id.clone());
    install_val.job_id = None;
    install_val.fence += 1;
    context.cas(
        "admin:install",
        install_rec.as_ref().map(|r| r.generation.as_str()),
        &serialize(&install_val)?,
    )?;

    let front_synced = sync_front_dictionaries(context, &service, &input.token, &creds);
    let catalog_generation = catalog_head.as_ref().map(|h| h.generation);

    Ok(json!({
        "ok": true,
        "service_id": input.service_id,
        "source": source,
        "views_restored": views_restored,
        "catalog_generation": catalog_generation,
        "front_synced": front_synced,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::catalog::{Event, Head, Reference};
    use crate::views::SavedView;

    fn sample_service() -> Service {
        Service {
            service_id: "svc123".into(),
            name: "Production CDN".into(),
            status: "active".into(),
            created_at: 1_700_000_000,
            updated_at: 1_700_000_500,
            config_version: 3,
            logging_enabled: true,
            logging_endpoint_name: "Fastly Object Storage Logs".into(),
            log_period: 60,
            sample_rate: 100,
            edge_only: true,
            custom_condition: String::new(),
            fos_bucket: "prod-logs".into(),
            fos_prefix: "tenant-a".into(),
            fos_region: "us-east-1".into(),
            fos_endpoint: "us-east-1.object.fastlystorage.app".into(),
            log_fields: json!({
                "groups": ["A", "C"],
                "cookie_session": true,
                "fastly_api_key": "SENTINEL_FASTLY_API_KEY",
                "fos_access_key": "SENTINEL_FOS_ACCESS_KEY",
                "fos_secret_key": "SENTINEL_FOS_SECRET_KEY",
                "nested": {
                    "password": "SENTINEL_PASSWORD",
                    "secret_token": "SENTINEL_SECRET_TOKEN",
                    "private_key": "SENTINEL_PRIVATE_KEY",
                    "credential": "SENTINEL_CREDENTIAL",
                    "cdn_key": "SENTINEL_CDN_KEY",
                    "front_key": "SENTINEL_FRONT_KEY",
                    "session_key": "SENTINEL_SESSION_KEY",
                    "safe_flag": "keep-me"
                }
            }),
            cmcd: crate::logging::cmcd::Config::default(),
            rum: crate::logging::rum::Config::default(),
            field_limits: json!({"api_key": "SENTINEL_LIMIT_API_KEY", "url_len": 512}),
            fastly_token_ref: "SENTINEL_FASTLY_TOKEN_REF".into(),
            fos_access_key_ref: "SENTINEL_FOS_AK_REF".into(),
            fos_secret_key_ref: "SENTINEL_FOS_SK_REF".into(),
            owner: "flpApp".into(),
            bucket_adopted: true,
            bucket_checked: true,
            last_activated_version: Some(4),
            last_reconcile: Some(json!({
                "outcome": "complete",
                "token": "SENTINEL_RECONCILE_TOKEN",
                "passwd": "SENTINEL_PASSWD"
            })),
            pending_job_id: Some("job-should-strip".into()),
            owned_endpoint_names: vec!["Fastly Object Storage Logs".into()],
            delete_after: true,
            log_retention_days: 30,
            rum_retention_days: 30,
            compaction: Compaction::default_provision(),
            compression_codec: "zstd".into(),
            wire_format: "array".into(),
            data_reset: Some(DataReset {
                job_id: "reset-1".into(),
                reset_at: 1_700_000_100,
                phase: ResetPhase::Fencing,
                started_at: 1_700_000_100,
                actor: "admin".into(),
                rum_held: false,
            }),
            capture: Capture::default(),
            scoring: Some(ScoringConfig {
                enabled: true,
                scoring_service_id: "scoreSvc".into(),
                scoring_service_name: "score-svc".into(),
                scoring_domain: "score.example.com".into(),
                scoring_keys_store_id: "store-keys-id".into(),
                scoring_config_store_id: "store-cfg-id".into(),
                scoring_matrix_store_id: "store-mat-id".into(),
                enabled_at: "2026-11-20T00:00:00Z".into(),
                deployed_package_sha: "abc".into(),
                deployed_package_files_hash: "def".into(),
                deployed_vcl_sha: "ghi".into(),
                exclude_url_regex: String::new(),
                enforce_status_code: None,
                threshold: Some(50),
                matrix_version: None,
                last_retrain_at: None,
            }),
            cdn_shield: String::new(),
        }
    }

    #[test]
    fn dr_state_strips_secret_refs_transient_fields_and_sentinel_secrets() {
        let svc = sample_service();
        let views = vec![SavedView {
            id: "v1".into(),
            service_id: "svc123".into(),
            name: "Errors".into(),
            filters_json: r#"{"status":500}"#.into(),
            time_range_type: Some("relative".into()),
            start_time: None,
            end_time: None,
            page: Some("overview".into()),
            created_at: Some("2026-11-20T00:00:00Z".into()),
        }];
        let mut head = Head {
            version: crate::ingest::catalog::VERSION,
            generation: 7,
            manifest: Some("tenant-a/processed/_catalog/m7.json".into()),
            recent: vec![Event {
                cursor: "svc123:7".into(),
                generation: 7,
                committed_at: Some("2026-11-20T00:00:00Z".into()),
                manifest: Some("tenant-a/processed/_catalog/m7.json".into()),
            }],
            ..Head::default()
        };
        head.hour_bundles.insert(
            1_700_000_000,
            Reference {
                key: "tenant-a/processed/_catalog/b1.json".into(),
                generation: 7,
            },
        );

        let mut dr_value = build_dr_state(
            &svc,
            views,
            Some(head),
            Some("tenant-a/raw/2026/floor.zst".into()),
            Some(json!({"latest_log": 1_700_000_400, "rows": 42, "at": 1_700_000_500})),
            1_700_000_500,
        )
        .expect("build_dr_state");

        // Also ensure top-level transient KV fields on raw service values are stripped.
        let mut raw_kv = serialize(&svc).unwrap();
        raw_kv["_audit"] = json!({"token": "SENTINEL_AUDIT"});
        raw_kv["_audit_outbox"] = json!([]);
        raw_kv["_job_checkpoint"] = json!({"next": "finalize"});
        raw_kv["generation"] = json!("g1");
        sanitize_service_value(&mut raw_kv);
        dr_value["service"] = raw_kv;

        let serialized = serde_json::to_string(&dr_value).unwrap();
        assert!(
            !serialized.contains("SENTINEL_"),
            "sentinel secret leaked into DR state: {serialized}"
        );
        let svc_obj = dr_value["service"].as_object().unwrap();
        for forbidden in [
            "fastly_token_ref",
            "fos_access_key_ref",
            "fos_secret_key_ref",
            "pending_job_id",
            "data_reset",
            "generation",
            "_audit",
            "_audit_outbox",
            "_job_checkpoint",
        ] {
            assert!(
                !svc_obj.contains_key(forbidden),
                "forbidden field {forbidden} present in DR state"
            );
        }

        // Legitimate fields must be preserved (including cookie_session, scoring_keys_store_id, floor_key, Reference.key).
        assert_eq!(dr_value["schema_version"], SCHEMA_VERSION);
        assert_eq!(dr_value["service_id"], "svc123");
        assert_eq!(dr_value["service"]["fos_bucket"], "prod-logs");
        assert_eq!(dr_value["service"]["log_fields"]["cookie_session"], true);
        assert_eq!(
            dr_value["service"]["log_fields"]["nested"]["safe_flag"],
            "keep-me"
        );
        assert_eq!(
            dr_value["service"]["scoring"]["scoring_keys_store_id"],
            "store-keys-id"
        );
        assert_eq!(dr_value["floor_key"], "tenant-a/raw/2026/floor.zst");
        assert_eq!(
            dr_value["catalog_head"]["hour_bundles"]["1700000000"]["key"],
            "tenant-a/processed/_catalog/b1.json"
        );
        assert_eq!(dr_value["views"][0]["name"], "Errors");
        assert_eq!(dr_value["rum_stats"]["rows"], 42);
        assert_eq!(state_key(&svc), "tenant-a/meta/cloud_dr_state.json");
    }
}

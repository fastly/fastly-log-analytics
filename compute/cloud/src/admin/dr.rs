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
    "active_log_field_ids",
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
    if service.status != "active" || service.data_reset.is_some() {
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
    let (rum_stats, rum_floor_key) = match rum_rec {
        Some(rec) => {
            let stats = rec
                .value
                .get(crate::ingest::RUM_STATS_FIELD)
                .filter(|v| !v.is_null())
                .cloned();
            let rfk = rec.value[crate::ingest::FLOOR_FIELD]
                .as_str()
                .map(str::to_owned);
            (stats, rfk)
        }
        None => (None, None),
    };

    if runtime.admin_remaining_calls() == 0 {
        return;
    }

    let Ok(mut state_value) = build_dr_state(
        service,
        views,
        catalog_head,
        floor_key,
        rum_stats,
        runtime.now(),
    ) else {
        return;
    };
    if let (Some(rfk), Some(obj)) = (rum_floor_key, state_value.as_object_mut()) {
        obj.insert("rum_floor_key".into(), json!(rfk));
    }
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
        stored_log_field_ids: None,
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

#[derive(Clone, Debug)]
struct ScannedReset {
    generation: u64,
    reset_at: Option<i64>,
}

struct ScannedDay {
    min_gen: u64,
    max_gen: u64,
    key: String,
    stats: crate::ingest::catalog::Statistics,
    verified_own: bool,
    block_keys: Vec<String>,
}

type ScannedHeadPair = (
    crate::ingest::catalog::Head,
    Option<crate::ingest::catalog::Manifest>,
    Option<ScannedReset>,
    bool,
);

fn manifest_unix_ts(m: &crate::ingest::catalog::Manifest) -> Option<u64> {
    time::OffsetDateTime::parse(
        &m.committed_at,
        &time::format_description::well_known::Rfc3339,
    )
    .ok()
    .and_then(|dt| u64::try_from(dt.unix_timestamp()).ok())
}

fn ts_floor_key(service: &Service, ts: i64) -> String {
    let minute_ts = ts - ts.rem_euclid(60);
    format!(
        "{}{}",
        crate::ingest::raw_prefix(service),
        crate::ingest::minute_dir(minute_ts)
    )
}

fn ts_rum_floor_key(service: &Service, ts: i64) -> String {
    let minute_ts = ts - ts.rem_euclid(60);
    format!(
        "{}raw/rum/{}",
        crate::ingest::scope(service),
        crate::ingest::minute_dir(minute_ts)
    )
}

fn record_scanned_reset(slot: &mut Option<ScannedReset>, generation: u64, reset_at: Option<i64>) {
    let replace = match slot {
        None => true,
        Some(prev) => {
            generation > prev.generation
                || (generation == prev.generation && prev.reset_at.is_none() && reset_at.is_some())
        }
    };
    if replace {
        *slot = Some(ScannedReset {
            generation,
            reset_at,
        });
    }
}

struct CatalogScanOpts<'a> {
    reserve_calls: u32,
    fallback_reserve_calls: u32,
    cursor_hold: u32,
    extra_if_catalog: (u32, u32),
    extra_if_reset: (u32, u32),
    optional_manifest_reserve: (u32, u32),
    foreign_dr_state: bool,
    from_dr_state: bool,
    must_verify_dr_state: bool,
    now: u64,
    expected_manifest: Option<&'a str>,
    dr_head_gen: Option<u64>,
}

fn scan_catalog_head(
    context: &Context<'_, impl Runtime>,
    service: &Service,
    creds: &(String, String),
    opts: CatalogScanOpts<'_>,
) -> Result<(Option<ScannedHeadPair>, bool), Error> {
    let CatalogScanOpts {
        reserve_calls,
        fallback_reserve_calls,
        cursor_hold,
        extra_if_catalog,
        extra_if_reset,
        optional_manifest_reserve,
        foreign_dr_state,
        from_dr_state,
        must_verify_dr_state,
        now,
        expected_manifest,
        dr_head_gen,
    } = opts;
    if must_verify_dr_state {
        if context.remaining_calls() <= fallback_reserve_calls {
            return Err(Error::new(503, "backend_budget"));
        }
    } else if context.remaining_calls() <= fallback_reserve_calls + 1 {
        return Ok((None, false));
    }
    let scope = crate::ingest::scope(service);
    let prefix = format!("{scope}processed/_catalog/");
    let svc_rev_tag = format!("manifest-s-{}-rev-", service.service_id);
    let svc_rev_prefix = format!("{prefix}{svc_rev_tag}");
    let mut tried_svc_rev = false;
    let (mut raw_objects, mut next_cat_tok, page_was_truncated) = if !from_dr_state
        && expected_manifest.is_none()
        && context.remaining_calls() >= fallback_reserve_calls + 7
    {
        tried_svc_rev = true;
        match fos::list_objects(context, service, creds, &svc_rev_prefix, None) {
            Ok(svc_page) if !svc_page.objects.is_empty() => (svc_page.objects, None, false),
            Err(err) if err.status == 503 => return Err(err),
            _ => {
                if context.remaining_calls() <= fallback_reserve_calls + 1 {
                    return Ok((None, false));
                }
                let page = match fos::list_objects(context, service, creds, &prefix, None) {
                    Ok(page) => page,
                    Err(err) if err.status == 503 => return Err(err),
                    Err(_) => return Ok((None, false)),
                };
                let trunc = page.next_token.is_some();
                (page.objects, page.next_token, trunc)
            }
        }
    } else {
        let page = match fos::list_objects(context, service, creds, &prefix, None) {
            Ok(page) => page,
            Err(err) if err.status == 503 || must_verify_dr_state => return Err(err),
            Err(_) => return Ok((None, false)),
        };
        let trunc = page.next_token.is_some();
        (page.objects, page.next_token, trunc)
    };
    let mut budget_stopped = false;
    let trunc_list_floor = if !must_verify_dr_state && optional_manifest_reserve.0 > 0 {
        (fallback_reserve_calls + 1).max(optional_manifest_reserve.0 + 1)
    } else {
        fallback_reserve_calls + 1
    };

    if page_was_truncated {
        let mut found_svc_rev = false;
        if !tried_svc_rev {
            if context.remaining_calls() > trunc_list_floor {
                match fos::list_objects(context, service, creds, &svc_rev_prefix, None) {
                    Ok(svc_page) => {
                        if !svc_page.objects.is_empty() {
                            found_svc_rev = true;
                            raw_objects.extend(svc_page.objects);
                        }
                    }
                    Err(err) if err.status == 503 || must_verify_dr_state => return Err(err),
                    Err(_) => {}
                }
            } else {
                budget_stopped = true;
            }
        }
        // Always scan `legacy_m_prefix` even when `found_svc_rev` is true so newer legacy reset
        // manifests (e.g. `manifest-0011-reset.json`) are never hidden behind older
        // `manifest-s-{sid}-rev-*` objects.
        if !budget_stopped {
            if context.remaining_calls() > trunc_list_floor {
                let legacy_m_prefix = format!("{prefix}manifest");
                let mut m_tok: Option<String> = next_cat_tok
                    .clone()
                    .filter(|t| t.starts_with(&legacy_m_prefix));
                let mut saw_any_manifest = m_tok.is_some();
                loop {
                    if context.remaining_calls() <= trunc_list_floor {
                        budget_stopped = true;
                        break;
                    }
                    let m_page = match fos::list_objects(
                        context,
                        service,
                        creds,
                        &legacy_m_prefix,
                        m_tok.as_deref(),
                    ) {
                        Ok(page) => page,
                        Err(err) if err.status == 503 || must_verify_dr_state => {
                            return Err(err);
                        }
                        Err(_) => break,
                    };
                    if !m_page.objects.is_empty() {
                        saw_any_manifest = true;
                        raw_objects.extend(m_page.objects);
                    }
                    m_tok = m_page.next_token;
                    if m_tok.is_none() {
                        break;
                    }
                }
                if !saw_any_manifest && !found_svc_rev {
                    while let Some(tok) = next_cat_tok.as_deref() {
                        if context.remaining_calls()
                            <= trunc_list_floor.max(fallback_reserve_calls + 3)
                        {
                            budget_stopped = true;
                            break;
                        }
                        let more_cat =
                            match fos::list_objects(context, service, creds, &prefix, Some(tok)) {
                                Ok(page) => page,
                                Err(err) if err.status == 503 || must_verify_dr_state => {
                                    return Err(err);
                                }
                                Err(_) => break,
                            };
                        raw_objects.extend(more_cat.objects);
                        next_cat_tok = more_cat.next_token;
                    }
                }
            } else {
                budget_stopped = true;
            }
        }
    }

    let expected_on_page =
        expected_manifest.is_some_and(|m_key| raw_objects.iter().any(|o| o.key == m_key));
    let has_newer_svc_manifest = dr_head_gen.is_some_and(|g| {
        raw_objects.iter().any(|o| {
            o.key
                .rsplit('/')
                .next()
                .and_then(|n| n.strip_prefix(&svc_rev_tag))
                .and_then(|rest| rest.split_once('-'))
                .and_then(|(rev_s, _)| {
                    (!rev_s.is_empty() && rev_s.bytes().all(|b| b.is_ascii_digit()))
                        .then(|| rev_s.parse::<u64>().ok())
                        .flatten()
                        .map(|rev| u64::MAX - rev)
                })
                .is_some_and(|gen| gen > g)
        })
    });
    let expected_missing = match expected_manifest {
        Some(m_key) if !expected_on_page => {
            if !page_was_truncated {
                true
            } else if has_newer_svc_manifest {
                false
            } else if context.remaining_calls() > fallback_reserve_calls + 1 {
                fos::get_object(context, service, creds, m_key)?.is_none()
            } else {
                budget_stopped = true;
                false
            }
        }
        _ => false,
    };

    let mut saw_foreign_manifest = false;
    let mut seen_keys: BTreeSet<String> = BTreeSet::new();
    let mut objects: Vec<_> = raw_objects
        .into_iter()
        .filter(|obj| {
            if !obj.key.ends_with(".json") || !safe_dr_object_key(&obj.key, &prefix) {
                return false;
            }
            if !seen_keys.insert(obj.key.clone()) {
                return false;
            }
            if let Some(name) = obj.key.rsplit('/').next() {
                if name.starts_with("manifest-s-") && !name.starts_with(&svc_rev_tag) {
                    saw_foreign_manifest = true;
                    return false;
                }
            }
            true
        })
        .collect();
    let manifest_sort_rank = |k: &str| -> (u8, Option<u64>) {
        let Some(name) = k.rsplit('/').next() else {
            return (0, None);
        };
        if let Some(rest_rev) = name.strip_prefix(&svc_rev_tag) {
            let gen_hint = rest_rev.split_once('-').and_then(|(rev_s, _)| {
                (!rev_s.is_empty() && rev_s.bytes().all(|b| b.is_ascii_digit()))
                    .then(|| rev_s.parse::<u64>().ok())
                    .flatten()
                    .map(|rev| u64::MAX - rev)
            });
            return (2, gen_hint);
        }
        if !name.starts_with("manifest") {
            return (0, None);
        }
        let gen_hint = name
            .strip_prefix("manifest-")
            .and_then(|rest| rest.split_once('-'))
            .and_then(|(g, _)| {
                (!g.is_empty() && g.bytes().all(|b| b.is_ascii_digit()))
                    .then(|| g.parse::<u64>().ok())
                    .flatten()
            });
        (1, gen_hint)
    };
    objects.sort_by(|a, b| {
        let (a_m, a_g) = manifest_sort_rank(&a.key);
        let (b_m, b_g) = manifest_sort_rank(&b.key);
        (b_m > 0)
            .cmp(&(a_m > 0))
            .then_with(|| b_g.cmp(&a_g))
            .then_with(|| b_m.cmp(&a_m))
            .then_with(|| b.last_modified.cmp(&a.last_modified))
            .then_with(|| b.key.cmp(&a.key))
    });
    let has_higher_or_unranked_manifest = objects.iter().any(|o| {
        let (t, g) = manifest_sort_rank(&o.key);
        t > 0 && (g.is_none() || dr_head_gen.is_some_and(|dg| g.is_some_and(|mg| mg > dg)))
    });

    let mut best_manifest: Option<(String, crate::ingest::catalog::Manifest)> = None;
    let mut best_manifest_days: Option<(
        BTreeMap<i64, String>,
        BTreeMap<i64, crate::ingest::catalog::Statistics>,
    )> = None;
    let mut effective_reset: Option<ScannedReset> = None;
    let mut verified_own_blocks: BTreeSet<String> = BTreeSet::new();
    let mut known_bad_blocks: BTreeSet<String> = BTreeSet::new();
    let mut day_candidates: BTreeMap<i64, Vec<ScannedDay>> = BTreeMap::new();
    let legacy_cutoff_day = (from_dr_state && service.log_retention_days > 0).then(|| {
        let cutoff =
            (now as i64 - i64::from(service.log_retention_days) * 86_400).div_euclid(3600) * 3600;
        day_start(cutoff)
    });

    for obj in objects {
        let (m_tier, m_gen_hint) = manifest_sort_rank(&obj.key);
        if m_tier > 0 {
            if expected_on_page
                && !page_was_truncated
                && !has_higher_or_unranked_manifest
                && best_manifest.is_none()
                && expected_manifest == Some(obj.key.as_str())
                && m_gen_hint.is_some()
                && m_gen_hint == dr_head_gen
            {
                // The highest-ranked manifest in the un-truncated listing is already the exact
                // dr_state catalog_head manifest; no newer/reset manifest exists.
                break;
            }
            if let (Some(gh), Some((_, prev_bm))) = (m_gen_hint, best_manifest.as_ref()) {
                if gh <= prev_bm.generation
                    && (best_manifest_days.is_some() || prev_bm.batch == "reset")
                {
                    continue;
                }
            }
        } else if best_manifest_days.is_some()
            || best_manifest
                .as_ref()
                .is_some_and(|(_, m)| m.batch == "reset")
        {
            break;
        }
        let has_verified_day = day_candidates
            .values()
            .any(|v| v.iter().any(|d| d.verified_own));
        let has_nonempty_manifest_days = best_manifest_days
            .as_ref()
            .is_some_and(|(d, _)| !d.is_empty());
        let use_primary_reserve = m_tier == 0
            && (best_manifest.is_some() || has_nonempty_manifest_days || has_verified_day);
        let cur_reserve = if use_primary_reserve {
            if !from_dr_state && day_candidates.is_empty() && !has_nonempty_manifest_days {
                reserve_calls
                    .saturating_sub(cursor_hold)
                    .max(fallback_reserve_calls)
            } else {
                reserve_calls
            }
        } else {
            fallback_reserve_calls
        };
        let extra_verify_hold = if !foreign_dr_state && best_manifest_days.is_none() {
            day_candidates
                .values()
                .flat_map(|v| v.iter())
                .filter(|d| !d.verified_own)
                .flat_map(|d| d.block_keys.iter())
                .filter(|bk| !verified_own_blocks.contains(*bk) && !known_bad_blocks.contains(*bk))
                .map(String::as_str)
                .collect::<BTreeSet<_>>()
                .len() as u32
        } else {
            0
        };
        let dynamic_reset_hold = if effective_reset.is_some() || m_tier > 0 {
            if use_primary_reserve {
                extra_if_reset.0
            } else {
                extra_if_reset.1
            }
        } else if use_primary_reserve {
            extra_if_catalog.0
        } else {
            extra_if_catalog.1
        };
        let optional_floor = if optional_manifest_reserve.0 > 0 {
            let need = if effective_reset.is_some() || (m_tier > 0 && obj.key.contains("reset")) {
                optional_manifest_reserve.1
            } else {
                optional_manifest_reserve.0
            };
            need + extra_verify_hold
        } else {
            0
        };
        if context.remaining_calls()
            <= (cur_reserve + extra_verify_hold + dynamic_reset_hold).max(optional_floor)
        {
            if m_tier > 0 || (expected_missing && best_manifest.is_none()) {
                budget_stopped = true;
            }
            break;
        }
        let bytes = match fos::get_object(context, service, creds, &obj.key) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => continue,
            Err(err) if (expected_missing || must_verify_dr_state) && m_tier > 0 => {
                return Err(err);
            }
            Err(_) => continue,
        };
        if let Ok(manifest) = serde_json::from_slice::<crate::ingest::catalog::Manifest>(&bytes) {
            if manifest.committed_at.chars().any(char::is_control) {
                saw_foreign_manifest = true;
                continue;
            }
            if manifest.service_id == service.service_id {
                let raw_val = serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null);
                if manifest.batch == "reset" {
                    let ts = raw_val
                        .get("reset_at")
                        .and_then(Value::as_i64)
                        .or_else(|| manifest_unix_ts(&manifest).map(|u| u as i64));
                    record_scanned_reset(&mut effective_reset, manifest.generation, ts);
                }
                if let Some(rg) = raw_val
                    .get("reset_generation")
                    .and_then(Value::as_u64)
                    .filter(|&rg| rg > 0 && rg <= manifest.generation)
                {
                    let ra = raw_val.get("reset_at").and_then(Value::as_i64);
                    record_scanned_reset(&mut effective_reset, rg, ra);
                }
                if best_manifest
                    .as_ref()
                    .is_none_or(|(_, prev)| manifest.generation > prev.generation)
                {
                    best_manifest_days = raw_val
                        .get("head_days")
                        .and_then(|v| {
                            serde_json::from_value::<BTreeMap<i64, String>>(v.clone()).ok()
                        })
                        .filter(|m| m.values().all(|dk| safe_dr_object_key(dk, &prefix)))
                        .map(|m| {
                            let st = raw_val
                                .get("head_statistics")
                                .and_then(|sv| {
                                    serde_json::from_value::<
                                        BTreeMap<i64, crate::ingest::catalog::Statistics>,
                                    >(sv.clone())
                                    .ok()
                                })
                                .unwrap_or_default();
                            (m, st)
                        });
                    best_manifest = Some((obj.key, manifest));
                }
            } else {
                saw_foreign_manifest = true;
            }
            continue;
        }
        if let Ok(block) = serde_json::from_slice::<crate::ingest::catalog::Block>(&bytes) {
            if block.service_id == service.service_id {
                verified_own_blocks.insert(obj.key);
            } else {
                known_bad_blocks.insert(obj.key);
                saw_foreign_manifest = true;
            }
            continue;
        }
        if let Ok(day) = serde_json::from_slice::<crate::ingest::catalog::Day>(&bytes) {
            if day.version != crate::ingest::catalog::VERSION || day.hours.is_empty() {
                continue;
            }
            if !day
                .hours
                .values()
                .all(|r| safe_dr_object_key(&r.key, &prefix))
            {
                saw_foreign_manifest = true;
                continue;
            }
            let day_sid = serde_json::from_slice::<Value>(&bytes).ok().and_then(|v| {
                v.get("service_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            });
            if day_sid
                .as_deref()
                .is_some_and(|sid| sid != service.service_id)
            {
                saw_foreign_manifest = true;
                continue;
            }
            let verified_own = day_sid.as_deref() == Some(service.service_id.as_str());
            let min_gen = day.hours.values().map(|r| r.generation).min().unwrap_or(0);
            let max_gen = day.hours.values().map(|r| r.generation).max().unwrap_or(0);
            let block_keys: Vec<String> = day
                .hours
                .values()
                .map(|r| r.key.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            if block_keys.is_empty() || min_gen == 0 || max_gen == 0 {
                continue;
            }
            if let Some(day_ts) = day
                .hours
                .keys()
                .next()
                .map(|h| day_start(*h))
                .or_else(|| day.statistics.earliest.map(day_start))
            {
                if legacy_cutoff_day.is_some_and(|cd| day_ts < cd) {
                    continue;
                }
                day_candidates.entry(day_ts).or_default().push(ScannedDay {
                    min_gen,
                    max_gen,
                    key: obj.key,
                    stats: day.statistics,
                    verified_own,
                    block_keys,
                });
            }
        }
    }

    if budget_stopped
        && (must_verify_dr_state
            || (expected_missing
                && (best_manifest.is_none()
                    || (effective_reset.is_none()
                        && best_manifest
                            .as_ref()
                            .is_none_or(|(_, m)| m.batch != "reset")))))
    {
        return Err(Error::new(503, "backend_budget"));
    }

    if let Some(ref rst) = effective_reset {
        for vec_d in day_candidates.values_mut() {
            vec_d.retain(|d| d.min_gen > rst.generation && d.max_gen > rst.generation);
        }
    }
    if expected_missing {
        if let Some(g) = dr_head_gen {
            for vec_d in day_candidates.values_mut() {
                vec_d.retain(|d| d.min_gen > g && d.max_gen > g);
            }
        }
    }

    let has_authoritative_days = best_manifest_days.is_some()
        && effective_reset.as_ref().is_none_or(|r| {
            best_manifest
                .as_ref()
                .is_some_and(|(_, bm)| r.generation <= bm.generation)
        });
    let mut days: BTreeMap<i64, ScannedDay> = BTreeMap::new();
    if !has_authoritative_days {
        if from_dr_state && service.log_retention_days > 0 {
            let cutoff = (now as i64 - i64::from(service.log_retention_days) * 86_400)
                .div_euclid(3600)
                * 3600;
            let cutoff_day = day_start(cutoff);
            day_candidates.retain(|day_ts, _| *day_ts >= cutoff_day);
        }
        if let Some((_, ref bm)) = best_manifest {
            for vec_d in day_candidates.values_mut() {
                vec_d.retain(|d| d.max_gen <= bm.generation);
            }
        }
        day_candidates.retain(|_, vec_d| !vec_d.is_empty());
        let reset_hold = if effective_reset.is_some() {
            extra_if_reset.1
        } else {
            extra_if_catalog.1
        };
        if !foreign_dr_state {
            for (day_ts, mut vec_d) in day_candidates {
                vec_d.sort_by(|a, b| {
                    b.verified_own
                        .cmp(&a.verified_own)
                        .then_with(|| b.max_gen.cmp(&a.max_gen))
                        .then_with(|| b.key.cmp(&a.key))
                });
                for cand in vec_d {
                    if cand.verified_own {
                        days.insert(day_ts, cand);
                        break;
                    }
                    let mut all_ok = true;
                    for bk in &cand.block_keys {
                        if verified_own_blocks.contains(bk) {
                            continue;
                        }
                        if known_bad_blocks.contains(bk) {
                            all_ok = false;
                            break;
                        }
                        let optional_blk_floor = if effective_reset.is_some() {
                            optional_manifest_reserve.1
                        } else {
                            optional_manifest_reserve.0
                        };
                        if context.remaining_calls()
                            <= (fallback_reserve_calls + reset_hold).max(optional_blk_floor)
                        {
                            all_ok = false;
                            break;
                        }
                        match fos::get_object(context, service, creds, bk) {
                            Ok(Some(blk_bytes)) => {
                                if let Ok(blk) = serde_json::from_slice::<
                                    crate::ingest::catalog::Block,
                                >(&blk_bytes)
                                {
                                    if blk.service_id == service.service_id {
                                        verified_own_blocks.insert(bk.clone());
                                    } else {
                                        known_bad_blocks.insert(bk.clone());
                                        saw_foreign_manifest = true;
                                        all_ok = false;
                                        break;
                                    }
                                } else {
                                    known_bad_blocks.insert(bk.clone());
                                    saw_foreign_manifest = true;
                                    all_ok = false;
                                    break;
                                }
                            }
                            _ => {
                                known_bad_blocks.insert(bk.clone());
                                all_ok = false;
                                break;
                            }
                        }
                    }
                    if all_ok {
                        days.insert(day_ts, cand);
                        break;
                    }
                }
            }
        }
        let _ = saw_foreign_manifest;
        if best_manifest.is_none() && days.is_empty() {
            return Ok((None, expected_missing));
        }
    }

    let max_day_gen = days.values().map(|d| d.max_gen).max().unwrap_or(0);
    let generation = best_manifest
        .as_ref()
        .map(|(_, m)| m.generation)
        .unwrap_or(max_day_gen)
        .max(1);
    let manifest_key = best_manifest.as_ref().map(|(k, _)| k.clone());
    let committed_at = best_manifest.as_ref().map(|(_, m)| m.committed_at.clone());
    let manifest_obj = best_manifest.map(|(_, m)| m);

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
    if has_authoritative_days {
        if let Some((bm_days, bm_stats)) = best_manifest_days {
            for (day_ts, day_key) in bm_days {
                if !safe_dr_object_key(&day_key, &prefix) {
                    continue;
                }
                if let Some(st) = bm_stats.get(&day_ts) {
                    head.statistics.insert(day_ts, st.clone());
                }
                head.days.insert(day_ts, day_key);
            }
        }
    } else {
        for (day_ts, d) in days {
            head.days.insert(day_ts, d.key);
            head.statistics.insert(day_ts, d.stats);
        }
    }

    let mut encoded = serialize(&head)?;
    if let (Some(ref rst), Some(obj)) = (&effective_reset, encoded.as_object_mut()) {
        obj.insert("reset_generation".into(), json!(rst.generation));
        if let Some(ra) = rst.reset_at {
            obj.insert("reset_at".into(), json!(ra));
        }
    }
    if let Ok(validated_head) = crate::ingest::catalog::head(&json!({"catalog": encoded})) {
        Ok((
            Some((
                validated_head,
                manifest_obj,
                effective_reset,
                has_authoritative_days,
            )),
            expected_missing,
        ))
    } else {
        Ok((None, expected_missing))
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

fn dr_svc_entry_anchor(entry: &Value) -> String {
    use sha2::{Digest, Sha256};
    let canonical = json!([
        entry.get("at"),
        entry.get("event"),
        entry.get("actor"),
        entry.get("job_id"),
        entry.get("config_version"),
        entry.get("source"),
        entry,
    ]);
    hex::encode(Sha256::digest(canonical.to_string().as_bytes()))
}

fn dr_svc_slice_anchor(entries: &[Value]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update((entries.len() as u64).to_be_bytes());
    for entry in entries {
        hasher.update(dr_svc_entry_anchor(entry).as_bytes());
        hasher.update(b"\n");
    }
    hex::encode(hasher.finalize())
}

fn dr_svc_archived_removable(svc_val: &Value, cursor_val: Option<&Value>) -> (u64, usize) {
    let raw_base = svc_val["_audit_outbox_base"].as_u64().unwrap_or(0);
    let Some(cv) = cursor_val else {
        return (raw_base, 0);
    };
    let archived = cv
        .get("svc_outbox_archived")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let entries = svc_val["_audit_outbox"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let len = entries.len() as u64;
    if len == 0 || archived == 0 {
        return (raw_base, 0);
    }
    let svc_last_base = cv.get("svc_last_base").and_then(Value::as_u64);
    let svc_kv_archived = cv.get("svc_kv_archived").and_then(Value::as_u64);
    let svc_rebased_base = cv.get("svc_rebased_base").and_then(Value::as_u64);
    let svc_archived_anchor = cv.get("svc_archived_anchor").and_then(Value::as_str);
    let svc_prefix_anchor = cv.get("svc_prefix_anchor").and_then(Value::as_str);
    let svc_trimmed_base = cv.get("svc_trimmed_base").and_then(Value::as_u64);
    let svc_trimmed_prefix_anchor = cv.get("svc_trimmed_prefix_anchor").and_then(Value::as_str);
    let svc_zero_anchor = cv.get("svc_zero_anchor").and_then(Value::as_str);
    let svc_zero_virtual_base = cv
        .get("svc_zero_virtual_base")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let last_flush_s = cv.get("last_flush_s").and_then(Value::as_u64).unwrap_or(0);
    let eff_last_svc_s = cv
        .get("last_svc_flush_s")
        .and_then(Value::as_u64)
        .unwrap_or(last_flush_s);
    let cur_head_anchor = entries.first().map(dr_svc_entry_anchor);

    // 1a. Un-rebased KV epoch from a prior reset (`svc_kv_archived < archived`).
    // Guard against rebased `raw_base >= svc_rebased_base` so Parquet-space coordinates are
    // never compared against pre-rebase `svc_kv_archived`.
    if let Some(kv_arch) = svc_kv_archived {
        if kv_arch < archived
            && svc_rebased_base.is_none_or(|rb| raw_base < rb)
            && svc_last_base == Some(raw_base)
            && raw_base < kv_arch
            && kv_arch <= raw_base + len
        {
            let removable = (kv_arch - raw_base) as usize;
            let slice_ok =
                svc_prefix_anchor == Some(dr_svc_slice_anchor(&entries[..removable]).as_str());
            if slice_ok {
                return (archived - removable as u64, removable);
            }
        }
    } else if raw_base == 0 && svc_zero_virtual_base > 0 {
        // 1b. Legacy cursor with `svc_zero_virtual_base > 0`.
        let vb = svc_zero_virtual_base;
        if svc_last_base.is_none_or(|lb| lb == 0)
            && svc_zero_anchor.is_some()
            && svc_zero_anchor == cur_head_anchor.as_deref()
            && archived >= vb
            && archived <= vb + len
        {
            let removable = (archived - vb) as usize;
            let boundary_ok = removable == 0
                || eff_last_svc_s == 0
                || entries[removable - 1]["at"].as_u64().unwrap_or(0) <= eff_last_svc_s;
            if boundary_ok {
                return (vb, removable);
            }
        }
    }

    // 2a. Normal or rebased space where `raw_base >= archived`: 0 entries are removable.
    if raw_base >= archived {
        return (raw_base, 0);
    }

    // 2b. Normal or rebased space where `raw_base < archived`: verify bounds, exact untrimmed or
    // planned-trimmed base, and full archived-slice SHA-256 anchor over `entries[0..removable]`.
    let min_expected_base = svc_rebased_base.or(svc_last_base);
    let bounds_ok = archived <= raw_base + len && min_expected_base.is_none_or(|lb| raw_base >= lb);
    if bounds_ok {
        let removable = (archived - raw_base) as usize;
        let slice = &entries[..removable];
        let verified = if svc_prefix_anchor.is_some() || svc_archived_anchor.is_some() {
            if min_expected_base == Some(raw_base) {
                svc_prefix_anchor == Some(dr_svc_slice_anchor(slice).as_str())
            } else if svc_trimmed_base == Some(raw_base) {
                svc_trimmed_prefix_anchor == Some(dr_svc_slice_anchor(slice).as_str())
            } else {
                false
            }
        } else if raw_base == 0 && svc_zero_anchor.is_some() {
            svc_zero_virtual_base == 0
                && svc_zero_anchor == cur_head_anchor.as_deref()
                && (eff_last_svc_s == 0
                    || entries[removable - 1]["at"].as_u64().unwrap_or(0) <= eff_last_svc_s)
        } else {
            // Legacy anchorless cursor: fail closed -> 409 audit_archive_required.
            false
        };
        if verified {
            return (raw_base, removable);
        }
    }

    // 3. Reset, replacement, or truncation: 0 entries are safely removable!
    (archived, 0)
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

fn dr_api_list(
    context: &Context<'_, impl Runtime>,
    path: &str,
    token: &str,
) -> Result<Vec<Value>, Error> {
    let (status, val) = context.api_status("GET", path, token, None)?;
    if status == 401 || status == 403 {
        return Err(Error::new(403, "fastly_token_scope_insufficient"));
    }
    if status == 503 {
        return Err(Error::new(503, "fastly_api_error"));
    }
    if !(200..300).contains(&status) {
        return Err(Error::new(502, "fastly_api_error"));
    }
    list(&val)
        .cloned()
        .map_err(|_| Error::new(502, "fastly_api_error"))
}

fn inspect_active_version_for_dr(
    context: &Context<'_, impl Runtime>,
    service: &mut Service,
    token: &str,
    candidate_names: &[String],
    minimal_fallback: bool,
) -> Result<(), Error> {
    let scope = crate::ingest::scope(service);
    let ver = match super::reconcile::active_version(context, &service.service_id, token) {
        Ok(ver) => ver,
        Err(err) if err.status == 409 && err.code == "active_version_required" => return Ok(()),
        Err(err) if err.status == 403 => {
            return Err(Error::new(403, "fastly_token_scope_insufficient"));
        }
        Err(err) => return Err(err),
    };
    let encoded_sid = encode(&service.service_id);
    let s3_items = dr_api_list(
        context,
        &format!("/service/{encoded_sid}/version/{ver}/logging/s3"),
        token,
    )?;
    let fos_items = dr_api_list(
        context,
        &format!("/service/{encoded_sid}/version/{ver}/logging/fos"),
        token,
    )?;

    let mut coexist = super::reconcile::has_attach_endpoint_marker(&s3_items, &fos_items);
    if !coexist
        && (minimal_fallback || service.capture == Capture::Managed)
        && super::reconcile::has_foreign_primary_endpoint(
            service, &scope, &s3_items, &fos_items, false,
        )
    {
        if context.remaining_calls() == 0 {
            return Err(Error::new(503, "backend_budget"));
        }
        let snip_items = dr_api_list(
            context,
            &format!("/service/{encoded_sid}/version/{ver}/snippet"),
            token,
        )?;
        if super::reconcile::capture_present(&snip_items) {
            coexist = true;
        }
    }
    if !coexist && (minimal_fallback || service.capture == Capture::Managed) {
        if context.remaining_calls() == 0 {
            return Err(Error::new(503, "backend_budget"));
        }
        let cond_items = dr_api_list(
            context,
            &format!("/service/{encoded_sid}/version/{ver}/condition"),
            token,
        )?;
        if super::reconcile::has_attach_condition_marker(&cond_items) {
            coexist = true;
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
                &s3_items,
                &fos_items,
            )?;
        } else {
            let managed_candidates = vec![service.logging_endpoint_name.clone()];
            service.owned_endpoint_names = verify_candidate_endpoints(
                service,
                &scope,
                &managed_candidates,
                &s3_items,
                &fos_items,
            )?;
        }
        return Ok(());
    }

    if service.capture == Capture::Managed && coexist {
        return Err(Error::new(409, "resource_name_collision"));
    }
    service.owned_endpoint_names =
        verify_candidate_endpoints(service, &scope, candidate_names, &s3_items, &fos_items)?;
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
    let mut cached_existing_outbox_trim: Option<(u64, usize)> = None;
    if let Some(ref rec) = existing_svc {
        if rec.value.get("data_reset").is_some_and(|v| !v.is_null()) {
            return Err(Error::new(409, "data_reset_in_progress"));
        }
        let install_owns_sid = install_val.service_id.as_deref() == Some(input.service_id.as_str());
        let has_pending_job = rec.value["pending_job_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty());
        if (install_owns_sid || has_pending_job)
            && rec.value["status"].as_str() != Some("torn_down")
        {
            return Err(Error::new(409, "service_already_configured"));
        }
        let prev_len = rec.value["_audit_outbox"].as_array().map_or(0, Vec::len);
        if prev_len >= super::AUDIT_OUTBOX_LIMIT {
            let cur_rec = context.get(&format!("logs:cursor:{}", input.service_id))?;
            let (eff_base, removable) =
                dr_svc_archived_removable(&rec.value, cur_rec.as_ref().map(|c| &c.value));
            let drop_n = ((prev_len + 1) - super::AUDIT_OUTBOX_LIMIT)
                .min(removable)
                .min(prev_len);
            if (prev_len + 1).saturating_sub(drop_n) > super::JOB_RECOVERY_OUTBOX_LIMIT {
                return Err(Error::new(409, "audit_archive_required"));
            }
            cached_existing_outbox_trim = Some((eff_base, removable));
        }
    }
    let cold_start = install_rec.is_none() && existing_svc.is_none();
    let cursor_hold = u32::from(existing_svc.is_none());
    let has_front_dicts = config(context.runtime, "front_service_id").is_ok()
        && config(context.runtime, "buckets_dictionary_id").is_ok()
        && config(context.runtime, "credentials_dictionary_id").is_ok();
    let front_calls = if has_front_dicts { 3u32 } else { 0u32 };
    const PER_UPSERT: u32 = 2;

    let now = context.runtime.now();
    let owner_id = owner(context.runtime)?;
    let fallback_svc = bootstrap_service(owner_id.clone(), &input, now);

    let (creds, persist_explicit, pending_secret_puts) = match explicit_creds {
        Some((ak, sk)) => {
            if context.remaining_calls() < 6 {
                return Err(Error::new(503, "backend_budget"));
            }
            ((ak, sk), true, 3u32)
        }
        None => {
            if context.remaining_calls() < 5 {
                return Err(Error::new(503, "backend_budget"));
            }
            let pair = fos::ensure_key_with_credentials(context, &fallback_svc, &input.token)?;
            (pair, false, 1u32)
        }
    };

    let clamp_floors_to_ts =
        |svc: &Service, ts: i64, fk: &mut Option<String>, rfk: &mut Option<String>| {
            let rf = ts_floor_key(svc, ts);
            if fk.as_deref().is_none_or(|cur| cur < rf.as_str()) {
                *fk = Some(rf);
            }
            let rrf = ts_rum_floor_key(svc, ts);
            if rfk.as_deref().is_none_or(|cur| cur < rrf.as_str()) {
                *rfk = Some(rrf);
            }
        };

    let dr_key = state_key(&fallback_svc);
    let dr_bytes = fos::get_object(context, &fallback_svc, &creds, &dr_key)?;
    let raw_dr_json = dr_bytes
        .as_deref()
        .and_then(|bytes| serde_json::from_slice::<Value>(bytes).ok());
    let raw_dr = dr_bytes.and_then(|bytes| serde_json::from_slice::<CloudDrState>(&bytes).ok());
    let foreign_dr_state = raw_dr
        .as_ref()
        .is_some_and(|state| state.service_id != input.service_id);
    let parsed_dr = raw_dr.filter(|state| {
        state.schema_version == SCHEMA_VERSION && state.service_id == input.service_id
    });

    let dr_updated_at = parsed_dr.as_ref().map(|d| d.updated_at).unwrap_or(0);
    let (
        service,
        views,
        mut catalog_head,
        mut floor_key,
        mut rum_floor_key,
        mut rum_stats,
        mut source,
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
        let parsed_rum_floor = match raw_dr_json
            .as_ref()
            .and_then(|v| v.get("rum_floor_key"))
            .filter(|v| !v.is_null())
        {
            Some(v) => {
                let s = v
                    .as_str()
                    .ok_or_else(|| Error::new(400, "invalid_dr_floor_key"))?;
                if !safe_dr_object_key(s, &format!("{scope}raw/rum/")) {
                    return Err(Error::new(400, "invalid_dr_floor_key"));
                }
                Some(s.to_owned())
            }
            None => None,
        };
        if let Some(ref head) = dr_state.catalog_head {
            validate_dr_catalog_head(head, &scope)?;
        }
        let valid_head = dr_state.catalog_head.filter(|h| {
            (h.generation > 0 || h.manifest.is_some() || !h.days.is_empty())
                && serde_json::to_value(h)
                    .ok()
                    .is_some_and(|v| crate::ingest::catalog::head(&json!({"catalog": v})).is_ok())
        });
        let mut floor_key = dr_state.floor_key;
        let mut rum_floor_key = parsed_rum_floor;
        let mut rum_stats = dr_state.rum_stats;
        if let Some(ts) = valid_head.as_ref().and_then(|h| h.reset_at) {
            clamp_floors_to_ts(&service, ts, &mut floor_key, &mut rum_floor_key);
            if rum_stats
                .as_ref()
                .and_then(|v| v["at"].as_i64())
                .is_some_and(|at| at < ts)
            {
                rum_stats = None;
            }
        }
        service.owned_endpoint_names.clear();
        let should_inspect = !candidate_names.is_empty() || service.capture == Capture::Managed;
        if should_inspect {
            if context.remaining_calls() < 3 {
                return Err(Error::new(503, "backend_budget"));
            }
            inspect_active_version_for_dr(
                context,
                &mut service,
                &input.token,
                &candidate_names,
                false,
            )?;
        }
        (
            service,
            views,
            valid_head,
            floor_key,
            rum_floor_key,
            rum_stats,
            "dr_state",
        )
    } else {
        let mut min_svc = fallback_svc.clone();
        min_svc.owned_endpoint_names.clear();
        if context.remaining_calls() < 3 {
            return Err(Error::new(503, "backend_budget"));
        }
        inspect_active_version_for_dr(context, &mut min_svc, &input.token, &[], true)?;
        (min_svc, Vec::new(), None, None, None, None, "minimal")
    };

    {
        let base_upserts = u32::from(!views.is_empty())
            + u32::from(catalog_head.is_some() || floor_key.is_some())
            + u32::from(rum_stats.is_some() || rum_floor_key.is_some());
        let plan_reserves = |u: u32| -> (u32, u32) {
            let rem = context
                .remaining_calls()
                .saturating_sub(pending_secret_puts);
            // Safe cold-start upsert cost: 1 for u=1, 2*u-1 for u>=2 (guarantees 0 partial writes
            // even if keys 2..u are pre-existing stale), and 2*u on non-cold-start.
            let safe_u = if !cold_start {
                PER_UPSERT * u
            } else if u == 0 {
                0
            } else {
                2 * u - 1
            };
            // On cold start with all-absent KV, optimistic inserts take only `u` calls (so front
            // dicts / cursor fit after `u` calls when enough room remains for `safe_u` too).
            let opt_u = if cold_start { u } else { PER_UPSERT * u };
            let full_no_f = 2 + safe_u;
            let worst_no_f = 2 + PER_UPSERT * u;
            let worst_with_front = worst_no_f + front_calls;
            let worst_with_front_cursor = worst_with_front + cursor_hold;
            let opt_with_front = (2 + opt_u + front_calls).max(full_no_f);
            let opt_with_front_cursor = (2 + opt_u + front_calls + cursor_hold).max(full_no_f);
            let base_r = if rem >= worst_no_f + 2 {
                worst_no_f
            } else if catalog_head.is_some()
                && !has_front_dicts
                && cursor_hold > 0
                && rem >= full_no_f + cursor_hold
            {
                full_no_f + cursor_hold
            } else {
                full_no_f
            };
            let scan_r = if has_front_dicts
                && (!cold_start || base_upserts > 0)
                && rem >= worst_with_front_cursor + 2
            {
                worst_with_front_cursor.max(base_r)
            } else if has_front_dicts
                && (!cold_start || base_upserts > 0)
                && rem >= worst_with_front + 2
            {
                worst_with_front.max(base_r)
            } else if rem >= opt_with_front_cursor + 2 {
                opt_with_front_cursor.max(base_r)
            } else if has_front_dicts && rem >= opt_with_front + 2 {
                opt_with_front.max(base_r)
            } else if cursor_hold > 0 && rem >= (2 + opt_u + cursor_hold).max(full_no_f) + 2 {
                (2 + opt_u + cursor_hold).max(base_r)
            } else {
                base_r
            };
            (scan_r + pending_secret_puts, base_r + pending_secret_puts)
        };
        let (scan_reserve, base_scan_reserve) = plan_reserves(base_upserts);
        let extra_if_catalog = if catalog_head.is_none() && floor_key.is_none() {
            let (sr_cat, br_cat) = plan_reserves(base_upserts + 1);
            (
                sr_cat.saturating_sub(scan_reserve),
                br_cat.saturating_sub(base_scan_reserve),
            )
        } else {
            (0u32, 0u32)
        };
        let extra_if_reset = if rum_stats.is_none() && rum_floor_key.is_none() {
            let extra_u = u32::from(catalog_head.is_none() && floor_key.is_none()) + 1;
            let (sr_rst, br_rst) = plan_reserves(base_upserts + extra_u);
            (
                sr_rst.saturating_sub(scan_reserve),
                br_rst.saturating_sub(base_scan_reserve),
            )
        } else {
            extra_if_catalog
        };

        let must_verify_dr_state = source == "dr_state"
            && (catalog_head.is_some()
                || floor_key.is_some()
                || rum_floor_key.is_some()
                || rum_stats.is_some());
        let commit_calls_est = pending_secret_puts + 2;
        let optional_manifest_reserve = if !must_verify_dr_state
            && base_upserts > 0
            && catalog_head.is_none()
            && floor_key.is_none()
        {
            let u_rst =
                base_upserts + 1 + u32::from(rum_stats.is_none() && rum_floor_key.is_none());
            let need_rst = commit_calls_est + PER_UPSERT * u_rst + front_calls + cursor_hold;
            (need_rst, need_rst)
        } else {
            (0u32, 0u32)
        };

        if let Some(ref dr_head) = catalog_head {
            let (scanned_opt, dr_manifest_missing) = scan_catalog_head(
                context,
                &service,
                &creds,
                CatalogScanOpts {
                    reserve_calls: scan_reserve,
                    fallback_reserve_calls: base_scan_reserve,
                    cursor_hold,
                    extra_if_catalog,
                    extra_if_reset,
                    optional_manifest_reserve,
                    foreign_dr_state,
                    from_dr_state: source == "dr_state",
                    must_verify_dr_state,
                    now,
                    expected_manifest: dr_head.manifest.as_deref(),
                    dr_head_gen: Some(dr_head.generation),
                },
            )?;
            if let Some((mut scanned, scanned_manifest, effective_reset, has_authoritative_days)) =
                scanned_opt
            {
                let reset_occurred = dr_manifest_missing
                    || effective_reset
                        .as_ref()
                        .is_some_and(|r| r.generation > dr_head.generation)
                    || scanned_manifest
                        .as_ref()
                        .is_some_and(|m| m.generation > dr_head.generation && m.batch == "reset");
                if let Some(ts) = effective_reset.as_ref().and_then(|r| r.reset_at) {
                    clamp_floors_to_ts(&service, ts, &mut floor_key, &mut rum_floor_key);
                }
                if reset_occurred {
                    rum_stats = None;
                    let reset_ts =
                        effective_reset
                            .as_ref()
                            .and_then(|r| r.reset_at)
                            .or_else(|| {
                                scanned_manifest
                                    .as_ref()
                                    .and_then(manifest_unix_ts)
                                    .map(|u| u as i64)
                            });
                    if let Some(ts) = reset_ts {
                        clamp_floors_to_ts(&service, ts, &mut floor_key, &mut rum_floor_key);
                    }
                } else if !dr_manifest_missing
                    && !has_authoritative_days
                    && scanned.days.is_empty()
                    && !dr_head.days.is_empty()
                    && scanned_manifest.as_ref().is_none_or(|m| m.batch != "reset")
                {
                    scanned.days = dr_head.days.clone();
                    scanned.statistics = dr_head.statistics.clone();
                }
                if dr_manifest_missing || scanned.generation > dr_head.generation {
                    catalog_head = Some(scanned);
                }
            } else if dr_manifest_missing {
                rum_stats = None;
                catalog_head = None;
            }
        } else if let (Some((scanned, scanned_manifest, effective_reset, _)), _) =
            scan_catalog_head(
                context,
                &service,
                &creds,
                CatalogScanOpts {
                    reserve_calls: scan_reserve,
                    fallback_reserve_calls: base_scan_reserve,
                    cursor_hold,
                    extra_if_catalog,
                    extra_if_reset,
                    optional_manifest_reserve,
                    foreign_dr_state,
                    from_dr_state: source == "dr_state",
                    must_verify_dr_state,
                    now,
                    expected_manifest: None,
                    dr_head_gen: None,
                },
            )?
        {
            let reset_occurred = effective_reset.as_ref().is_some_and(|r| {
                source != "dr_state"
                    || r.reset_at
                        .is_none_or(|ts| u64::try_from(ts).ok().is_none_or(|u| u >= dr_updated_at))
            }) || scanned_manifest.as_ref().is_some_and(|m| {
                m.batch == "reset"
                    && (source != "dr_state"
                        || manifest_unix_ts(m).is_none_or(|ts| ts >= dr_updated_at))
            });
            if let Some(ts) = effective_reset.as_ref().and_then(|r| r.reset_at) {
                clamp_floors_to_ts(&service, ts, &mut floor_key, &mut rum_floor_key);
            }
            if reset_occurred {
                rum_stats = None;
                let reset_ts = effective_reset
                    .as_ref()
                    .and_then(|r| r.reset_at)
                    .or_else(|| {
                        scanned_manifest
                            .as_ref()
                            .and_then(manifest_unix_ts)
                            .map(|u| u as i64)
                    });
                if let Some(ts) = reset_ts {
                    clamp_floors_to_ts(&service, ts, &mut floor_key, &mut rum_floor_key);
                }
            }
            catalog_head = Some(scanned);
            if source != "dr_state" {
                source = "catalog_scan";
            }
        }
    }

    if let Some(ts) = catalog_head.as_ref().and_then(|h| h.reset_at) {
        clamp_floors_to_ts(&service, ts, &mut floor_key, &mut rum_floor_key);
        if rum_stats
            .as_ref()
            .and_then(|v| v["at"].as_i64())
            .is_some_and(|at| at < ts)
        {
            rum_stats = None;
        }
    }

    let mut planned_upserts: Vec<(String, Value)> = Vec::new();
    let views_restored = views.len();
    if !views.is_empty() {
        planned_upserts.push((
            crate::views::key(&input.service_id),
            json!({"views": views, "updated_at": now}),
        ));
    }

    if rum_stats.is_some() || rum_floor_key.is_some() {
        let mut rum_wm = serde_json::Map::new();
        if let Some(ref stats) = rum_stats {
            rum_wm.insert(crate::ingest::RUM_STATS_FIELD.into(), stats.clone());
        }
        if let Some(ref rfk) = rum_floor_key {
            rum_wm.insert(crate::ingest::FLOOR_FIELD.into(), json!(rfk));
        }
        planned_upserts.push((
            crate::ingest::watermark_key(crate::ingest::rum::STREAM),
            Value::Object(rum_wm),
        ));
    }

    if catalog_head.is_some() || floor_key.is_some() {
        let mut wm = serde_json::Map::new();
        if let Some(ref head) = catalog_head {
            wm.insert("catalog".into(), serialize(head)?);
        }
        if let Some(ref floor) = floor_key {
            wm.insert(crate::ingest::FLOOR_FIELD.into(), json!(floor));
        }
        planned_upserts.push((crate::ingest::WATERMARK_KEY.to_owned(), Value::Object(wm)));
    }

    // Monotonic, zero-partial-write KV upserts:
    // - Whenever `rem >= commit + 2*u - 1`, Key 0 attempts `CAS(None)`:
    //   - If Key 0 was absent (written), remaining calls `>= commit + 2*(u-1)`, so every remaining
    //     key is mathematically guaranteed to complete without a mid-upsert 503. Optimistic
    //     `CAS(None)` continues only while `all_absent` holds and stops on the very first 409 so
    //     at most ONE key in the entire upsert phase can pay a +1 409 cost (preserving strict
    //     monotonicity of status, front_synced, and cursor seeding across all 3^u states).
    //   - If Key 0 existed (409 -> 0 KV writes so far), read-only pre-check ALL keys before
    //     issuing any CAS, guaranteeing 0 partial KV writes on 503 and at most one 409 total.
    // - When `rem < commit + 2*u - 1`, read-only pre-check of ALL keys runs before the first CAS,
    //   guaranteeing 0 partial KV writes on 503.
    let commit_calls = pending_secret_puts + 2;
    let u_count = planned_upserts.len() as u32;
    if context.remaining_calls() < commit_calls + u_count {
        return Err(Error::new(503, "backend_budget"));
    }
    let use_optimistic = u_count >= 1
        && context.remaining_calls() >= commit_calls + 2 * u_count - 1
        && context.remaining_calls() < commit_calls + 2 * u_count + front_calls + cursor_hold;
    if use_optimistic {
        let mut iter = planned_upserts.into_iter();
        let (k0, v0) = iter.next().unwrap();
        let rest: Vec<(String, Value)> = iter.collect();
        match context.cas(&k0, None, &v0) {
            Ok(()) => {
                let mut all_absent = true;
                let rest_n = rest.len() as u32;
                for (i, (key, value)) in rest.into_iter().enumerate() {
                    let after = rest_n - 1 - i as u32;
                    let mut done = false;
                    if all_absent && context.remaining_calls() >= commit_calls + 3 + 2 * after {
                        match context.cas(&key, None, &value) {
                            Ok(()) => done = true,
                            Err(err) if err.status == 409 => all_absent = false,
                            Err(err) => return Err(err),
                        }
                    }
                    if !done {
                        let existing = context.get(&key)?;
                        if !existing
                            .as_ref()
                            .is_some_and(|r| kv_payload_matches(&key, &r.value, &value))
                        {
                            context.cas(
                                &key,
                                existing.as_ref().map(|r| r.generation.as_str()),
                                &value,
                            )?;
                        }
                    }
                }
            }
            Err(err) if err.status == 409 => {
                let mut pending_writes: Vec<(String, Option<String>, Value)> = Vec::new();
                for (key, value) in std::iter::once((k0, v0)).chain(rest) {
                    let existing = context.get(&key)?;
                    if !existing
                        .as_ref()
                        .is_some_and(|r| kv_payload_matches(&key, &r.value, &value))
                    {
                        pending_writes.push((key, existing.map(|r| r.generation), value));
                    }
                }
                if context.remaining_calls() < commit_calls + pending_writes.len() as u32 {
                    return Err(Error::new(503, "backend_budget"));
                }
                for (key, generation, value) in pending_writes {
                    context.cas(&key, generation.as_deref(), &value)?;
                }
            }
            Err(err) => return Err(err),
        }
    } else {
        let mut pending_writes: Vec<(String, Option<String>, Value)> = Vec::new();
        for (key, value) in planned_upserts {
            let existing = context.get(&key)?;
            if !existing
                .as_ref()
                .is_some_and(|r| kv_payload_matches(&key, &r.value, &value))
            {
                pending_writes.push((key, existing.map(|r| r.generation), value));
            }
        }
        if context.remaining_calls() < commit_calls + pending_writes.len() as u32 {
            return Err(Error::new(503, "backend_budget"));
        }
        for (key, generation, value) in pending_writes {
            context.cas(&key, generation.as_deref(), &value)?;
        }
    }

    let mut svc_value = serialize_service(&service)?;
    let audit_entry = json!({
        "actor": actor,
        "at": now,
        "event": "service.recovered",
        "source": source,
        "config_version": service.config_version,
    });
    svc_value["_audit"] = audit_entry.clone();
    if let Some(ref prev_rec) = existing_svc {
        let mut outbox = prev_rec.value["_audit_outbox"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let mut base = prev_rec.value["_audit_outbox_base"].as_u64().unwrap_or(0);
        outbox.push(audit_entry);
        if let Some((eff_base, archived_removable)) = cached_existing_outbox_trim {
            if eff_base > base {
                base = eff_base;
            }
            let drop_n = (outbox.len() - super::AUDIT_OUTBOX_LIMIT)
                .min(archived_removable)
                .min(outbox.len().saturating_sub(1));
            if drop_n > 0 {
                outbox.drain(..drop_n);
                base += drop_n as u64;
            }
            if outbox.len() > super::JOB_RECOVERY_OUTBOX_LIMIT {
                return Err(Error::new(409, "audit_archive_required"));
            }
        }
        svc_value["_audit_outbox"] = Value::Array(outbox);
        if base > 0 || prev_rec.value.get("_audit_outbox_base").is_some() {
            svc_value["_audit_outbox_base"] = json!(base);
        }
    } else {
        svc_value["_audit_outbox"] = json!([audit_entry]);
        if context.remaining_calls() > pending_secret_puts + 2 + front_calls {
            if let Ok(Some(cur_rec)) = context.get(&format!("logs:cursor:{}", input.service_id)) {
                if let Some(archived) = cur_rec
                    .value
                    .get("svc_outbox_archived")
                    .and_then(Value::as_u64)
                    .filter(|&a| a > 0)
                {
                    svc_value["_audit_outbox_base"] = json!(archived);
                }
            }
        }
    }
    if context.remaining_calls() < pending_secret_puts + 2 {
        return Err(Error::new(503, "backend_budget"));
    }
    put_secret(
        context,
        &input.token,
        &service.fastly_token_ref,
        &input.token,
    )?;
    if persist_explicit {
        fos::persist_explicit_credentials(context, &service, &input.token, &creds.0, &creds.1)?;
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

    #[test]
    fn service_records_carry_the_log_field_ids_resolved_when_saved() {
        let service = sample_service();
        let resolved = crate::catalog::active_log_field_ids(&service.log_fields);
        let value = serialize_service(&service).unwrap();
        assert_eq!(value["active_log_field_ids"], json!(resolved));

        let mut deployed = value.clone();
        deployed["active_log_field_ids"] = json!(["as_deployed"]);
        let read: Service = serde_json::from_value(deployed).unwrap();
        assert_eq!(read.active_log_field_ids(), vec!["as_deployed"]);

        let mut older = value;
        older
            .as_object_mut()
            .unwrap()
            .remove("active_log_field_ids");
        let read: Service = serde_json::from_value(older).unwrap();
        assert_eq!(read.active_log_field_ids(), resolved);
    }

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
            stored_log_field_ids: None,
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

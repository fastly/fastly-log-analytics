use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

pub const REGIONS: &[&str] = &[
    "us-east-1",
    "us-west",
    "us-central-1",
    "eu-central-1",
    "eu-south-1",
    "eu-west-1",
    "uk-east-1",
    "jp-central-1",
    "au-east-1",
];

#[derive(Clone, Serialize, Deserialize)]
pub struct Record {
    pub generation: String,
    pub value: Value,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Service {
    pub service_id: String,
    pub name: String,
    pub status: String,
    pub created_at: u64,
    pub updated_at: u64,
    pub config_version: u64,
    pub logging_enabled: bool,
    pub logging_endpoint_name: String,
    pub log_period: u32,
    pub sample_rate: u32,
    pub edge_only: bool,
    pub custom_condition: String,
    pub fos_bucket: String,
    pub fos_prefix: String,
    pub fos_region: String,
    pub fos_endpoint: String,
    pub log_fields: Value,
    #[serde(default)]
    pub cmcd: crate::logging::cmcd::Config,
    #[serde(default)]
    pub rum: crate::logging::rum::Config,
    pub field_limits: Value,
    pub fastly_token_ref: String,
    pub fos_access_key_ref: String,
    pub fos_secret_key_ref: String,
    pub owner: String,
    pub bucket_adopted: bool,
    pub bucket_checked: bool,
    pub last_activated_version: Option<u64>,
    pub last_reconcile: Option<Value>,
    pub pending_job_id: Option<String>,
    #[serde(default)]
    pub owned_endpoint_names: Vec<String>,
    #[serde(default = "default_true")]
    pub delete_after: bool,
    #[serde(default = "default_retention_days")]
    pub log_retention_days: u32,
    #[serde(default = "default_retention_days")]
    pub rum_retention_days: u32,
    #[serde(default)]
    pub compaction: Compaction,
    #[serde(default = "default_compression_codec")]
    pub compression_codec: String,
    #[serde(default = "default_wire_format")]
    pub wire_format: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_reset: Option<DataReset>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResetPhase {
    Fencing,
    Cleanup,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataReset {
    pub job_id: String,
    pub reset_at: u64,
    pub phase: ResetPhase,
    pub started_at: u64,
    pub actor: String,
    /// Cleanup began before the RUM cursor reset landed: RUM ingest stays paused until it does.
    #[serde(default, skip_serializing_if = "is_false")]
    pub rum_held: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn default_compression_codec() -> String {
    "zstd".into()
}

fn default_wire_format() -> String {
    "array".into()
}

/// Size-aware request-log compaction: `off`, or merge eligible windows below
/// `size_target_mb` into `compact=` files of about that size.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Compaction {
    #[serde(default)]
    pub mode: CompactionMode,
    #[serde(default = "default_size_target_mb")]
    pub size_target_mb: u32,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionMode {
    #[default]
    Off,
    SizeTarget,
}

pub const MAX_SIZE_TARGET_MB: u32 = 256;

fn default_size_target_mb() -> u32 {
    128
}

impl Default for Compaction {
    fn default() -> Self {
        Self {
            mode: CompactionMode::Off,
            size_target_mb: default_size_target_mb(),
        }
    }
}

impl Compaction {
    pub fn valid(&self) -> bool {
        (1..=MAX_SIZE_TARGET_MB).contains(&self.size_target_mb)
    }

    /// The target in bytes while compaction is on.
    pub fn target_bytes(&self) -> Option<u64> {
        (self.mode == CompactionMode::SizeTarget).then(|| u64::from(self.size_target_mb) << 20)
    }
}

fn default_true() -> bool {
    true
}
fn default_retention_days() -> u32 {
    30
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub service_id: String,
    pub kind: String,
    pub status: String,
    pub step: String,
    pub completed_steps: Vec<String>,
    pub error: Option<String>,
    pub draft_version: Option<u64>,
    pub changes: Vec<Change>,
    pub created_at: u64,
    pub updated_at: u64,
    pub actor: String,
    pub expected_generation: String,
    pub source_version: Option<u64>,
    pub change_cursor: usize,
    pub lease_until: u64,
    pub fence: u64,
    pub dry_run: bool,
    pub activate: bool,
    pub options: Teardown,
    pub front_source_version: Option<u64>,
    pub front_draft_version: Option<u64>,
    pub front_cursor: usize,
    pub clone_intent: Option<u64>,
    pub front_clone_intent: Option<u64>,
    pub front_changes: Vec<Change>,
    pub service_snapshot: Option<Service>,
    #[serde(default)]
    pub previous_endpoint_names: Vec<String>,
    #[serde(default)]
    pub retry: Option<RetryState>,
    #[serde(default)]
    pub retry_attempts: BTreeMap<String, u8>,
    #[serde(default)]
    pub uncertain_operations: BTreeSet<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetryState {
    pub operation: String,
    pub attempt: u8,
    pub at: u64,
    pub reason: String,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Change {
    pub resource: String,
    pub name: String,
    pub action: String,
    pub body: Value,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Teardown {
    pub remove_logging: bool,
    pub remove_snippets: bool,
    pub delete_fos_bucket: bool,
    pub delete_fos_keys: bool,
    pub delete_data: bool,
}
impl Default for Teardown {
    fn default() -> Self {
        Self {
            remove_logging: true,
            remove_snippets: true,
            delete_fos_bucket: false,
            delete_fos_keys: true,
            delete_data: false,
        }
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Install {
    pub service_id: Option<String>,
    pub job_id: Option<String>,
    pub fence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CronRun {
    pub id: String,
    pub started_at: String,
    pub finished_at: String,
    pub duration_ms: u64,
    pub outcome: String,
    pub trigger: String,
    pub actor: String,
    pub bucket: u64,
    pub service_id: Option<String>,
    pub listed_raw_files: u64,
    /// Raw objects the straggler sweep found below the watermark and planned.
    #[serde(default)]
    pub straggler_raw_files: u64,
    pub converted_raw_files: u64,
    pub deleted_raw_files: u64,
    pub rows_written: u64,
    pub bytes_written: u64,
    pub backend_requests_used: u32,
    pub fos_manifest_key: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CronState {
    pub last_tick: Option<u64>,
    pub last_bucket: Option<u64>,
    #[serde(default)]
    pub last_ingest_at: Option<u64>,
    #[serde(default)]
    pub last_ingest_service_id: Option<String>,
    #[serde(default)]
    pub runs: Vec<CronRun>,
    /// Index into `logs::MAINT_STAGES` the next tick's stage worker starts from.
    #[serde(default)]
    pub maint_next: usize,
    /// Ticks in a row whose stage worker did not answer.
    #[serde(default)]
    pub maint_misses: u32,
    /// The dispatcher's per-job schedule; the Core Cache record mirrors it.
    #[serde(default)]
    pub jobs: super::schedule::Jobs,
    /// The next tick's ingest runs the RUM stream before requests (it was starved or left work
    /// pending last time).
    #[serde(default)]
    pub rum_first: bool,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Lease {
    pub job_id: String,
    pub fence: u64,
    pub expires_at: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Provision {
    pub token: String,
    pub service_id: String,
    #[serde(alias = "fos_bucket_name")]
    pub fos_bucket: String,
    pub fos_region: String,
    #[serde(default)]
    pub fos_prefix: String,
    #[serde(default = "yes")]
    pub logging_enabled: bool,
    #[serde(default = "period", deserialize_with = "deserialize_period")]
    pub log_period: u32,
    #[serde(default = "sample", deserialize_with = "deserialize_integer")]
    pub sample_rate: u32,
    #[serde(default = "yes")]
    pub edge_only: bool,
    #[serde(default, deserialize_with = "deserialize_condition")]
    pub custom_condition: String,
    #[serde(default = "endpoint", alias = "endpoint_name")]
    pub logging_endpoint_name: String,
    #[serde(default = "default_fields", deserialize_with = "deserialize_fields")]
    pub log_fields: Value,
    #[serde(default)]
    pub cmcd: crate::logging::cmcd::Config,
    #[serde(default, deserialize_with = "deserialize_new_rum")]
    pub rum: crate::logging::rum::Config,
    #[serde(default)]
    pub rum_enabled: Option<bool>,
    #[serde(default)]
    pub rum_log_period: Option<i64>,
    #[serde(default)]
    pub rum_custom_condition: Option<String>,
    #[serde(default)]
    pub faro_version: Option<String>,
    #[serde(default)]
    pub rum_endpoint_name: Option<String>,
    #[serde(default, alias = "rum_capture_vitals")]
    pub capture_vitals: Option<bool>,
    #[serde(default, alias = "rum_capture_performance")]
    pub capture_performance: Option<bool>,
    #[serde(default, alias = "rum_capture_errors")]
    pub capture_errors: Option<bool>,
    #[serde(default, alias = "rum_capture_events")]
    pub capture_events: Option<bool>,
    #[serde(default = "default_true")]
    pub delete_after: bool,
    #[serde(
        default = "default_retention_days",
        deserialize_with = "deserialize_retention"
    )]
    pub log_retention_days: u32,
    #[serde(
        default = "default_retention_days",
        deserialize_with = "deserialize_retention"
    )]
    pub rum_retention_days: u32,
    #[serde(default)]
    pub compaction: Compaction,
    #[serde(default = "default_compression_codec")]
    pub compression_codec: String,
    #[serde(default = "default_wire_format")]
    pub wire_format: String,
    #[serde(default, rename = "service_name")]
    pub _service_name: Option<String>,
}

impl Provision {
    pub fn effective_rum(&self) -> crate::logging::rum::Config {
        let mut rum = self.rum.clone();
        if let Some(enabled) = self.rum_enabled {
            rum.enabled = enabled;
        }
        if let Some(period) = self.rum_log_period {
            rum.log_period = period as u32;
        }
        if let Some(ref condition) = self.rum_custom_condition {
            rum.custom_condition = condition.trim().to_owned();
        }
        if let Some(ref version) = self.faro_version {
            rum.faro_version = version.clone();
        }
        if let Some(ref name) = self.rum_endpoint_name {
            if !name.trim().is_empty() {
                rum.endpoint_name = name.clone();
            }
        }
        if let Some(flag) = self.capture_vitals {
            rum.capture_vitals = flag;
        }
        if let Some(flag) = self.capture_performance {
            rum.capture_performance = flag;
        }
        if let Some(flag) = self.capture_errors {
            rum.capture_errors = flag;
        }
        if let Some(flag) = self.capture_events {
            rum.capture_events = flag;
        }
        rum
    }
}

fn deserialize_integer<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u32, D::Error> {
    let value = Value::deserialize(deserializer)?;
    value
        .as_u64()
        .and_then(|n| n.try_into().ok())
        .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
        .ok_or_else(|| serde::de::Error::custom("expected integer"))
}
pub const MAX_RETENTION_DAYS: u32 = 3_650;
fn deserialize_retention<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<u32, D::Error> {
    let days = deserialize_integer(deserializer)?;
    if days > MAX_RETENTION_DAYS {
        return Err(serde::de::Error::custom("retention days out of range"));
    }
    Ok(days)
}
fn deserialize_period<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u32, D::Error> {
    let value = Value::deserialize(deserializer)?;
    if let Some(n) = value.as_u64().and_then(|n| n.try_into().ok()) {
        return Ok(n);
    }
    let text = value
        .as_str()
        .ok_or_else(|| serde::de::Error::custom("expected period"))?;
    if let Ok(n) = text.parse() {
        return Ok(n);
    }
    let mut parts = text.split_whitespace();
    let count: u32 = parts
        .next()
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| serde::de::Error::custom("expected period"))?;
    let multiplier = match parts.next() {
        Some("second" | "seconds") => 1,
        Some("minute" | "minutes") => 60,
        Some("hour" | "hours") => 3600,
        _ => return Err(serde::de::Error::custom("expected period")),
    };
    if parts.next().is_some() {
        return Err(serde::de::Error::custom("expected period"));
    }
    count
        .checked_mul(multiplier)
        .ok_or_else(|| serde::de::Error::custom("period overflow"))
}
fn deserialize_condition<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<String, D::Error> {
    let value = Option::<String>::deserialize(deserializer)?;
    Ok(value.unwrap_or_default())
}
/// A stored RUM config without a version keeps the legacy Faro pin; a new service without
/// one starts on the newest vendored version.
fn deserialize_new_rum<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<crate::logging::rum::Config, D::Error> {
    let mut value = Value::deserialize(deserializer)?;
    if let Some(rum) = value.as_object_mut() {
        rum.entry("faro_version")
            .or_insert_with(|| crate::logging::rum::DEFAULT_FARO_VERSION.into());
    }
    serde_json::from_value(value).map_err(serde::de::Error::custom)
}
fn deserialize_fields<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Value, D::Error> {
    let value = Value::deserialize(deserializer)?;
    if value.is_null() {
        return Ok(default_fields());
    }
    if let Some(text) = value.as_str() {
        serde_json::from_str(text).map_err(serde::de::Error::custom)
    } else {
        Ok(value)
    }
}

fn period() -> u32 {
    60
}
fn sample() -> u32 {
    100
}
fn yes() -> bool {
    true
}
fn endpoint() -> String {
    "Fastly Object Storage Logs".into()
}
pub fn default_fields() -> Value {
    json!({"groups":["A","B","C","D","E","F","G","H","I","J","K","L","M"],"custom_fields":[],"field_overrides":{},"field_limits":{}})
}

pub fn valid_service_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 64 && value.bytes().all(|b| b.is_ascii_alphanumeric())
}
pub fn valid_bucket(value: &str) -> bool {
    (3..=63).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
        && !value.contains("..")
        && !value.contains(".-")
        && !value.contains("-.")
        && value.parse::<std::net::Ipv4Addr>().is_err()
}
pub fn valid_prefix(value: &str) -> bool {
    value.len() <= 256
        && !value.starts_with('/')
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'-' | b'_' | b'.'))
        && value.split('/').all(|part| part != "." && part != "..")
}

pub fn service_value(service: &Service, generation: &str) -> Value {
    let mut value = serde_json::to_value(service).unwrap_or(Value::Null);
    value["generation"] = generation.into();
    value["rum_enabled"] = json!(service.rum.enabled);
    value["rum_log_period"] = json!(service.rum.log_period);
    value["rum_custom_condition"] = json!(service.rum.custom_condition);
    value["faro_version"] = json!(service.rum.faro_version);
    value["rum_endpoint_name"] = json!(service.rum.endpoint_name);
    value["capture_vitals"] = json!(service.rum.capture_vitals);
    value["capture_performance"] = json!(service.rum.capture_performance);
    value["capture_errors"] = json!(service.rum.capture_errors);
    value["capture_events"] = json!(service.rum.capture_events);
    value
}

pub fn public_job(job: &Job) -> Value {
    json!({
        "id":job.id,"service_id":job.service_id,"kind":job.kind,"status":job.status,
        "step":job.step,"completed_steps":job.completed_steps,"error":job.error,
        "draft_version":job.draft_version,"changes":job.changes.iter().map(|change|
            json!({"resource":change.resource,"name":change.name,"action":change.action})).collect::<Vec<_>>(),
        "created_at":job.created_at,"updated_at":job.updated_at,
        "retry_at":job.retry.as_ref().and_then(|retry|(retry.at>0).then_some(retry.at)),
        "retry_attempt":job.retry.as_ref().map_or(0,|retry|retry.attempt),
        "retry_reason":job.retry.as_ref().map(|retry|&retry.reason),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provision(extra: Value) -> Result<Provision, serde_json::Error> {
        let mut input = json!({
            "token": "fixture-token", "service_id": "fixtureService",
            "fos_bucket_name": "fos-fixture-logs", "fos_region": "us-east-1",
        });
        input
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        serde_json::from_value(input)
    }

    #[test]
    fn provision_defaults_storage_settings_to_v3() {
        let input = provision(json!({})).unwrap();
        assert!(input.delete_after);
        assert_eq!(
            (input.log_retention_days, input.rum_retention_days),
            (30, 30)
        );
        assert_eq!(input.effective_rum().log_period, 300);
    }

    #[test]
    fn provision_accepts_admin_chosen_storage_settings() {
        let input = provision(json!({
            "delete_after": false, "log_retention_days": 0, "rum_retention_days": "3650",
        }))
        .unwrap();
        assert!(!input.delete_after);
        assert_eq!(
            (input.log_retention_days, input.rum_retention_days),
            (0, 3650)
        );
    }

    #[test]
    fn provision_rejects_out_of_range_retention() {
        for extra in [
            json!({"log_retention_days": 3651}),
            json!({"rum_retention_days": -1}),
            json!({"log_retention_days": 1.5}),
            json!({"delete_after": "yes"}),
        ] {
            assert!(provision(extra.clone()).is_err(), "{extra}");
        }
    }

    #[test]
    fn new_services_start_on_the_newest_faro_unless_one_is_chosen() {
        let newest = crate::logging::rum::DEFAULT_FARO_VERSION;
        for extra in [json!({}), json!({"rum": {"enabled": true}})] {
            let input = provision(extra.clone()).unwrap();
            assert_eq!(input.effective_rum().faro_version, newest, "{extra}");
        }
        let chosen = provision(json!({"rum": {"enabled": true, "faro_version": "2.10.0"}}));
        assert_eq!(chosen.unwrap().effective_rum().faro_version, "2.10.0");
        let top_level = provision(json!({"rum": {"enabled": true}, "faro_version": "2.10.0"}));
        assert_eq!(top_level.unwrap().effective_rum().faro_version, "2.10.0");
    }
}

use super::{admin, audit, AuthService, Error};
use crate::state::StateStore;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UsageLogging {
    pub enabled: bool,
    pub retention_days: f64,
    pub class_a_rate_per_1k: f64,
    pub class_b_rate_per_10k: f64,
    pub cdn_egress_rate_per_gb: f64,
    pub storage_rate_per_gb_month: f64,
    pub min_billed_days: f64,
    /// Compute requests, USD per million (app and worker services).
    #[serde(default = "rate_compute_request_rate_per_million")]
    pub compute_request_rate_per_million: f64,
    /// Compute vCPU time, USD per million ms.
    #[serde(default = "rate_compute_vcpu_rate_per_million_ms")]
    pub compute_vcpu_rate_per_million_ms: f64,
    /// Compute vCPU time included per Compute Request, ms.
    #[serde(default = "rate_compute_vcpu_free_ms")]
    pub compute_vcpu_free_ms_per_request: f64,
    /// Compute excess vCPU time above the free limit, USD per second.
    #[serde(default = "rate_compute_vcpu_excess")]
    pub compute_vcpu_excess_rate_per_second: f64,
    /// Compute excess vCPU time rounding increment per Compute Request, ms.
    #[serde(default = "rate_compute_vcpu_rounding_ms")]
    pub compute_vcpu_rounding_ms: f64,
    /// Front-service CDN requests, USD per 10,000.
    #[serde(default = "rate_cdn_request_rate_per_10k")]
    pub cdn_request_rate_per_10k: f64,
    /// KV Store Class A (writes, lists, deletes), USD per million.
    #[serde(default = "rate_kv_class_a_rate_per_million")]
    pub kv_class_a_rate_per_million: f64,
    /// KV Store Class B (reads), USD per million.
    #[serde(default = "rate_kv_class_b_rate_per_million")]
    pub kv_class_b_rate_per_million: f64,
    /// Secret Store, USD per secret per month.
    #[serde(default = "rate_secret_rate_per_month")]
    pub secret_rate_per_month: f64,
    /// Visibility of the Query Debugging Panel ("disabled" | "admins" | "analysts" | "both").
    #[serde(default = "default_debug_visibility")]
    pub query_debug_visibility: String,
    /// Visibility of the API Call Debug Panel ("disabled" | "admins" | "analysts" | "both").
    #[serde(default = "default_debug_visibility")]
    pub api_call_debug_visibility: String,
}

fn default_debug_visibility() -> String {
    "disabled".to_owned()
}

pub fn is_valid_debug_visibility(value: &str) -> bool {
    matches!(value, "disabled" | "admins" | "analysts" | "both")
}

pub fn allows_debug_role(visibility: &str, role: super::Role) -> bool {
    match visibility {
        "both" => true,
        "admins" => role == super::Role::Admin,
        "analysts" => matches!(role, super::Role::Analyst | super::Role::AnalystNoPii),
        _ => false,
    }
}

fn rate_compute_request_rate_per_million() -> f64 {
    0.5
}

fn rate_compute_vcpu_rate_per_million_ms() -> f64 {
    0.05
}

fn rate_compute_vcpu_free_ms() -> f64 {
    20.0
}

fn rate_compute_vcpu_excess() -> f64 {
    0.00005
}

fn rate_compute_vcpu_rounding_ms() -> f64 {
    10.0
}

fn rate_cdn_request_rate_per_10k() -> f64 {
    0.01
}

fn rate_kv_class_a_rate_per_million() -> f64 {
    6.5
}

fn rate_kv_class_b_rate_per_million() -> f64 {
    0.55
}

fn rate_secret_rate_per_month() -> f64 {
    5.0
}

impl Default for UsageLogging {
    fn default() -> Self {
        Self {
            enabled: true,
            retention_days: 30.0,
            class_a_rate_per_1k: 0.0025,
            class_b_rate_per_10k: 0.004,
            cdn_egress_rate_per_gb: 0.12,
            storage_rate_per_gb_month: 0.02,
            min_billed_days: 30.0,
            compute_request_rate_per_million: rate_compute_request_rate_per_million(),
            compute_vcpu_rate_per_million_ms: rate_compute_vcpu_rate_per_million_ms(),
            compute_vcpu_free_ms_per_request: rate_compute_vcpu_free_ms(),
            compute_vcpu_excess_rate_per_second: rate_compute_vcpu_excess(),
            compute_vcpu_rounding_ms: rate_compute_vcpu_rounding_ms(),
            cdn_request_rate_per_10k: rate_cdn_request_rate_per_10k(),
            kv_class_a_rate_per_million: rate_kv_class_a_rate_per_million(),
            kv_class_b_rate_per_million: rate_kv_class_b_rate_per_million(),
            secret_rate_per_month: rate_secret_rate_per_month(),
            query_debug_visibility: default_debug_visibility(),
            api_call_debug_visibility: default_debug_visibility(),
        }
    }
}

impl UsageLogging {
    fn valid(&self) -> bool {
        [
            self.retention_days,
            self.class_a_rate_per_1k,
            self.class_b_rate_per_10k,
            self.cdn_egress_rate_per_gb,
            self.storage_rate_per_gb_month,
            self.min_billed_days,
            self.compute_request_rate_per_million,
            self.compute_vcpu_rate_per_million_ms,
            self.compute_vcpu_excess_rate_per_second,
            self.cdn_request_rate_per_10k,
            self.kv_class_a_rate_per_million,
            self.kv_class_b_rate_per_million,
            self.secret_rate_per_month,
        ]
        .iter()
        .all(|value| value.is_finite() && *value > 0.0)
            && [
                self.compute_vcpu_free_ms_per_request,
                self.compute_vcpu_rounding_ms,
            ]
            .iter()
            .all(|value| value.is_finite() && *value >= 0.0)
            && is_valid_debug_visibility(&self.query_debug_visibility)
            && is_valid_debug_visibility(&self.api_call_debug_visibility)
    }
}

impl<S: StateStore> AuthService<'_, S> {
    pub fn usage_logging(&self, actor: &str) -> Result<UsageLogging, Error> {
        let state = self.store.load()?.state;
        admin(&state, actor)?;
        if !state.usage_logging.valid() {
            return Err(Error::Storage);
        }
        Ok(state.usage_logging)
    }

    /// Records an operational action an administrator already performed.
    pub fn record_admin_action(
        &self,
        actor: &str,
        action: &str,
        target: &str,
        ip: &str,
        now: u64,
    ) -> Result<(), Error> {
        self.update(|state| {
            admin(state, actor)?;
            audit(state, actor, action, target, ip, now);
            Ok(())
        })
    }

    pub fn set_usage_logging(
        &self,
        actor: &str,
        patch: Value,
        ip: &str,
        now: u64,
    ) -> Result<UsageLogging, Error> {
        self.update(|state| {
            admin(state, actor)?;
            if !state.usage_logging.valid() {
                return Err(Error::Storage);
            }
            let fields = patch.as_object().ok_or(Error::InvalidInput)?;
            let mut next = state.usage_logging.clone();
            let has_vcpu_million = fields.contains_key("compute_vcpu_rate_per_million_ms");
            let has_vcpu_excess = fields.contains_key("compute_vcpu_excess_rate_per_second");
            for (key, value) in fields {
                if key == "enabled" {
                    next.enabled = value.as_bool().ok_or(Error::InvalidInput)?;
                    continue;
                }
                if key == "query_debug_visibility" || key == "api_call_debug_visibility" {
                    let text = value
                        .as_str()
                        .filter(|v| is_valid_debug_visibility(v))
                        .ok_or(Error::InvalidInput)?
                        .to_owned();
                    if key == "query_debug_visibility" {
                        next.query_debug_visibility = text;
                    } else {
                        next.api_call_debug_visibility = text;
                    }
                    continue;
                }
                if key == "compute_vcpu_free_ms_per_request" || key == "compute_vcpu_rounding_ms" {
                    let non_neg = value
                        .as_f64()
                        .filter(|number| number.is_finite() && *number >= 0.0)
                        .ok_or(Error::InvalidInput)?;
                    if key == "compute_vcpu_free_ms_per_request" {
                        next.compute_vcpu_free_ms_per_request = non_neg;
                    } else {
                        next.compute_vcpu_rounding_ms = non_neg;
                    }
                    continue;
                }
                let target = match key.as_str() {
                    "retention_days" => &mut next.retention_days,
                    "class_a_rate_per_1k" => &mut next.class_a_rate_per_1k,
                    "class_b_rate_per_10k" => &mut next.class_b_rate_per_10k,
                    "cdn_egress_rate_per_gb" => &mut next.cdn_egress_rate_per_gb,
                    "storage_rate_per_gb_month" => &mut next.storage_rate_per_gb_month,
                    "min_billed_days" => &mut next.min_billed_days,
                    "compute_request_rate_per_million" => {
                        &mut next.compute_request_rate_per_million
                    }
                    "compute_vcpu_rate_per_million_ms" => {
                        &mut next.compute_vcpu_rate_per_million_ms
                    }
                    "compute_vcpu_excess_rate_per_second" => {
                        &mut next.compute_vcpu_excess_rate_per_second
                    }
                    "cdn_request_rate_per_10k" => &mut next.cdn_request_rate_per_10k,
                    "kv_class_a_rate_per_million" => &mut next.kv_class_a_rate_per_million,
                    "kv_class_b_rate_per_million" => &mut next.kv_class_b_rate_per_million,
                    "secret_rate_per_month" => &mut next.secret_rate_per_month,
                    _ => return Err(Error::InvalidInput),
                };
                *target = value
                    .as_f64()
                    .filter(|number| number.is_finite() && *number > 0.0)
                    .ok_or(Error::InvalidInput)?;
            }
            if has_vcpu_excess && !has_vcpu_million {
                next.compute_vcpu_rate_per_million_ms =
                    next.compute_vcpu_excess_rate_per_second * 1000.0;
            } else if has_vcpu_million && !has_vcpu_excess {
                next.compute_vcpu_excess_rate_per_second =
                    next.compute_vcpu_rate_per_million_ms / 1000.0;
            }
            if next != state.usage_logging {
                state.usage_logging = next.clone();
                audit(
                    state,
                    actor,
                    "usage_logging_updated",
                    "usage_logging",
                    ip,
                    now,
                );
            }
            Ok(next)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::UsageLogging;
    use crate::auth::{AuthConfig, AuthService, AuthState, Credential, Error, Role, User};
    use crate::state::{MemoryStore, StateStore};

    #[test]
    fn settings_saved_before_the_compute_kv_and_secret_rates_load_with_list_prices() {
        let stored = serde_json::json!({
            "enabled": true, "retention_days": 30.0, "class_a_rate_per_1k": 0.0025,
            "class_b_rate_per_10k": 0.004, "cdn_egress_rate_per_gb": 0.12,
            "storage_rate_per_gb_month": 0.02, "min_billed_days": 30.0,
        });
        let loaded: UsageLogging = serde_json::from_value(stored).unwrap();
        assert_eq!(loaded, UsageLogging::default());
        assert_eq!(loaded.compute_vcpu_free_ms_per_request, 20.0);
        assert_eq!(loaded.compute_vcpu_excess_rate_per_second, 0.00005);
        assert_eq!(loaded.compute_vcpu_rounding_ms, 10.0);
        assert!(loaded.valid());
    }

    #[test]
    fn patching_contract_cpu_terms_validates_bounds_and_syncs_rate() {
        let store = MemoryStore::default();
        let mut initial = AuthState::default();
        initial.users.insert(
            "admin".to_owned(),
            User {
                id: "admin".to_owned(),
                display_name: "Admin".to_owned(),
                role: Role::Admin,
                credentials: vec![Credential {
                    id: "c1".to_owned(),
                    public_key: vec![1, 2, 3],
                    sign_count: 0,
                    revoked: false,
                }],
                revoked: false,
                created_by: "setup".to_owned(),
                created_at: 1,
            },
        );
        assert!(store.compare_and_swap(None, &initial).unwrap());
        let config = AuthConfig::new("https://logs.example.com/", vec![7u8; 32], None).unwrap();
        let auth = AuthService::new(&store, &config);
        let updated = auth
            .set_usage_logging(
                "admin",
                serde_json::json!({
                    "compute_vcpu_free_ms_per_request": 0.0,
                    "compute_vcpu_excess_rate_per_second": 0.00008,
                    "compute_vcpu_rounding_ms": 0.0,
                }),
                "127.0.0.1",
                2,
            )
            .unwrap();
        assert_eq!(updated.compute_vcpu_free_ms_per_request, 0.0);
        assert_eq!(updated.compute_vcpu_excess_rate_per_second, 0.00008);
        assert_eq!(updated.compute_vcpu_rounding_ms, 0.0);
        assert!((updated.compute_vcpu_rate_per_million_ms - 0.08).abs() < 1e-12);

        assert_eq!(
            auth.set_usage_logging(
                "admin",
                serde_json::json!({ "compute_vcpu_free_ms_per_request": -1.0 }),
                "127.0.0.1",
                3,
            ),
            Err(Error::InvalidInput)
        );
        assert_eq!(
            auth.set_usage_logging(
                "admin",
                serde_json::json!({ "compute_vcpu_excess_rate_per_second": 0.0 }),
                "127.0.0.1",
                3,
            ),
            Err(Error::InvalidInput)
        );
    }
}

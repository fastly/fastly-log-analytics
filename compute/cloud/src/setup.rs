pub const CONFIG_KEYS: &[&str] = &[
    "public_base_url",
    "flp_service_id",
    "worker_service_id",
    "front_service_id",
    "worker_host",
    "compute_host",
    "install_name",
    "secret_store_id",
    "config_store_id",
    "kv_store_id",
    "front_auth_dictionary_id",
    "buckets_dictionary_id",
    "credentials_dictionary_id",
    "raw_prefix",
    "out_prefix",
    "window_s",
    "grace_s",
    "clock_interval_s",
    "run_interval_s",
];

pub const SECRET_KEYS: &[&str] = &[
    "front_key",
    "cdn_key",
    "worker_secret",
    "session_key",
    "admin_setup_hash",
    "fastly_token",
];

pub trait SetupStores {
    fn config_present(&self, name: &str) -> Result<bool, String>;
    fn secret_present(&self, name: &str) -> Result<bool, String>;
    fn state_linked(&self) -> Result<bool, String>;
}

pub struct MissingEntry {
    pub store: &'static str,
    pub name: &'static str,
    pub command: String,
}

pub fn missing_entries(stores: &impl SetupStores) -> Result<Vec<MissingEntry>, String> {
    let mut missing = Vec::new();
    for &name in CONFIG_KEYS {
        if !stores.config_present(name)? {
            missing.push(MissingEntry {
                store: "Config",
                name,
                command: format!(
                    "fastly config-store-entry create --store-id=<config-store-id> --key={name} --stdin"
                ),
            });
        }
    }
    for &name in SECRET_KEYS {
        if !stores.secret_present(name)? {
            let command = if name == "admin_setup_hash" {
                "make admin-reset".to_owned()
            } else {
                format!(
                    "fastly secret-store-entry create --store-id=<secret-store-id> --name={name} --stdin"
                )
            };
            missing.push(MissingEntry {
                store: "Secret",
                name,
                command,
            });
        }
    }
    if !stores.state_linked()? {
        missing.push(MissingEntry {
            store: "KV",
            name: "flp_state",
            command: "fastly service resource-link create --service-id=<compute-service-id> --version=latest --name=flp_state --resource-id=<kv-store-id>".to_owned(),
        });
    }
    Ok(missing)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    struct Stores {
        config: BTreeSet<String>,
        secrets: BTreeSet<String>,
        kv_linked: bool,
    }

    impl SetupStores for Stores {
        fn config_present(&self, name: &str) -> Result<bool, String> {
            Ok(self.config.contains(name))
        }
        fn secret_present(&self, name: &str) -> Result<bool, String> {
            Ok(self.secrets.contains(name))
        }
        fn state_linked(&self) -> Result<bool, String> {
            Ok(self.kv_linked)
        }
    }

    #[test]
    fn incomplete_install_only_exposes_names_and_commands() {
        let stores = Stores {
            config: BTreeSet::new(),
            secrets: BTreeSet::new(),
            kv_linked: false,
        };
        let missing = missing_entries(&stores).unwrap();
        assert!(missing.iter().any(|entry| entry.name == "front_key"));
        assert!(missing.iter().any(|entry| entry.name == "public_base_url"));
        assert!(missing
            .iter()
            .any(|entry| entry.store == "KV" && entry.name == "flp_state"));
        assert!(missing
            .iter()
            .all(|entry| !entry.command.contains("--value=")));
        assert_eq!(
            missing
                .iter()
                .find(|entry| entry.name == "admin_setup_hash")
                .unwrap()
                .command,
            "make admin-reset"
        );
    }

    #[test]
    fn goal1_setup_does_not_require_a_management_token() {
        let stores = Stores {
            config: CONFIG_KEYS.iter().map(|name| (*name).to_owned()).collect(),
            secrets: SECRET_KEYS
                .iter()
                .filter(|name| **name != "fastly_admin_token")
                .map(|name| (*name).to_owned())
                .collect(),
            kv_linked: true,
        };
        assert!(missing_entries(&stores).unwrap().is_empty());
    }

    #[test]
    fn provisioning_does_not_require_service_credentials_or_oidc() {
        let stores = Stores {
            config: CONFIG_KEYS.iter().map(|name| (*name).to_owned()).collect(),
            secrets: SECRET_KEYS.iter().map(|name| (*name).to_owned()).collect(),
            kv_linked: true,
        };
        assert!(missing_entries(&stores).unwrap().is_empty());
        assert!(!SECRET_KEYS.contains(&"fos_ak"));
        assert!(!SECRET_KEYS.contains(&"oidc_client_secret"));
    }
}

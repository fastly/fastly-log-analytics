use fastly_log_analytics::logging::{self, rum};
use serde_json::json;
use sha2::{Digest, Sha256};

#[test]
fn vendored_bundles_match_their_recorded_sha256() {
    for version in rum::SUPPORTED_FARO_VERSIONS {
        let bytes = rum::faro_bundle_bytes(version).unwrap();
        let recorded = rum::faro_bundle_sha256(version).unwrap();
        assert_eq!(hex::encode(Sha256::digest(bytes)), recorded, "{version}");
    }
    assert_eq!(
        rum::faro_bundle_sha256("2.13.0"),
        Some("85741c45fd1fd4875c5f25a95a7940e1171ea11e308a83d88fd5f3a5dcab8c1c")
    );
    assert_eq!(
        rum::faro_bundle_sha256("2.10.0"),
        Some("cd02768dda8e6dc88b558bc288e1ec2ec416f6ec8f7cce04782d52e70c478b69")
    );
    assert!(rum::faro_bundle_bytes("2.11.0").is_err());
}

#[test]
fn supported_versions_are_newest_first_and_new_services_default_to_the_newest() {
    assert_eq!(rum::SUPPORTED_FARO_VERSIONS, &["2.13.0", "2.10.0"]);
    assert_eq!(rum::DEFAULT_FARO_VERSION, "2.13.0");
    assert_eq!(rum::Config::default().faro_version, "2.13.0");
}

#[test]
fn a_stored_rum_config_without_a_version_stays_on_the_previous_default() {
    let stored: rum::Config = serde_json::from_value(json!({"enabled": true})).unwrap();
    assert_eq!(stored.faro_version, rum::LEGACY_FARO_VERSION);
    assert_eq!(rum::LEGACY_FARO_VERSION, "2.10.0");
    let state = json!({"log_period": 60, "rum": {"enabled": true}});
    assert_eq!(
        logging::rum_from_state(&state).unwrap().faro_version,
        "2.10.0"
    );
    let pinned: rum::Config =
        serde_json::from_value(json!({"enabled": true, "faro_version": "2.10.0"})).unwrap();
    assert_eq!(pinned.faro_version, "2.10.0");
}

#[test]
fn rum_sample_rates_default_to_one_and_must_stay_within_one_percent_and_one() {
    let config: rum::Config = serde_json::from_value(json!({"enabled": true})).unwrap();
    assert_eq!((config.sample_rate, config.error_sample_rate), (1.0, 1.0));
    for (sample_rate, error_sample_rate, ok) in [
        (0.01, 1.0, true),
        (0.5, 0.25, true),
        (0.0, 1.0, false),
        (1.5, 1.0, false),
        (1.0, 0.001, false),
    ] {
        let config = rum::Config {
            sample_rate,
            error_sample_rate,
            ..config.clone()
        };
        assert_eq!(rum::validate_config(&config).is_ok(), ok, "{config:?}");
    }
}

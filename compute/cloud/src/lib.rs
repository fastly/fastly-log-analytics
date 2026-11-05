pub mod admin;
pub mod auth;
pub mod catalog;
pub mod checks;
pub mod csp;
pub mod http;
pub mod ingest;
pub mod logging;
#[cfg(target_arch = "wasm32")]
pub mod sdk;
pub mod setup;
pub mod shell;
include!(concat!(env!("OUT_DIR"), "/assets.rs"));
pub mod site;
pub mod state;
pub mod telemetry;

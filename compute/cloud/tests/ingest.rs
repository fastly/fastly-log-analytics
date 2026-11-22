use arrow_array::{Array, StringArray};
use fastly_log_analytics::{admin::Service, ingest, telemetry::LogBuffer};
use serde_json::{json, Value};
use std::time::Instant;

#[path = "ingest/support.rs"]
mod support;
use support::*;

#[path = "ingest/catalog_and_retention.rs"]
mod catalog_and_retention;
#[path = "ingest/core_and_backfill.rs"]
mod core_and_backfill;
#[path = "ingest/freshness.rs"]
mod freshness;
#[path = "ingest/plans_and_workers.rs"]
mod plans_and_workers;
#[path = "ingest/rum_and_wire_formats.rs"]
mod rum_and_wire_formats;
#[path = "ingest/sweep_and_api.rs"]
mod sweep_and_api;

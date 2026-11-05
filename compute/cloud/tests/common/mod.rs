#![allow(dead_code)]

use arrow_array::{
    Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use bytes::Bytes;
use fastly_log_analytics::{
    admin::Service,
    auth::{self, Role},
    http::Request,
    ingest::storage::Store,
    shell::Runtime,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Value};
use std::{collections::BTreeMap, io::Write, time::Instant};

pub const TEST_SESSION_KEY: &[u8] = b"fixture-session-signing-material-32-bytes";
pub const TEST_BASE_TS: u64 = 1_800_000_000;
pub const TEST_SERVICE_ID: &str = "AbCdEfGhIjKlMnOpQrSt12";

pub fn session_cookie(user_id: &str, role: Role, key: &[u8], now: u64) -> String {
    let token = compute_common::crypto::seal_json(
        key,
        &json!({
            "sub": user_id,
            "role": role,
            "policy_version": 0,
            "exp": now + auth::SESSION_TTL,
        }),
    )
    .unwrap();
    format!("flp_session={token}")
}

pub fn authed_get(path: &str, cookie: &str) -> Request {
    let mut headers = BTreeMap::new();
    headers.insert("cookie".into(), cookie.into());
    Request {
        method: "GET".into(),
        path: path.into(),
        headers,
        body: Vec::new(),
    }
}

pub fn authed_post(path: &str, cookie: &str, body: Value) -> Request {
    let mut headers = BTreeMap::new();
    headers.insert("cookie".into(), cookie.into());
    headers.insert("content-type".into(), "application/json".into());
    Request {
        method: "POST".into(),
        path: path.into(),
        headers,
        body: body.to_string().into_bytes(),
    }
}

pub fn authed_patch(path: &str, cookie: &str, body: Value) -> Request {
    let mut req = authed_post(path, cookie, body);
    req.method = "PATCH".into();
    req
}

pub fn authed_delete(path: &str, cookie: &str, body: Value) -> Request {
    let mut req = authed_post(path, cookie, body);
    req.method = "DELETE".into();
    req
}

pub fn service_json(service_id: &str, fos_prefix: &str) -> Value {
    json!({
        "service_id": service_id,
        "name": "Test Service",
        "service_type": "vcl",
        "logging_endpoint_name": "flp_s3_logs",
        "fos_bucket": "fos-logs",
        "fos_region": "us-east-1",
        "fos_prefix": fos_prefix,
        "fos_key_ref": format!("fos:{service_id}"),
        "fastly_token_ref": format!("fastly:{service_id}"),
        "log_fields": {
            "groups": ["A", "C"],
            "custom_fields": [],
            "field_overrides": {},
            "field_limits": {},
        },
        "log_retention_days": 30,
        "rum_retention_days": 30,
        "delete_after": true,
        "rum": {"enabled": false},
        "status": "healthy",
        "created_at": TEST_BASE_TS,
        "updated_at": TEST_BASE_TS,
    })
}

pub fn sample_service(service_id: &str, fos_prefix: &str) -> Service {
    serde_json::from_value(service_json(service_id, fos_prefix)).unwrap()
}

pub fn test_store<'a, R: Runtime>(runtime: &'a R, service: &'a Service) -> Store<'a, R> {
    Store {
        runtime,
        service,
        access_key: "AKIAIOSFODNN7EXAMPLE".into(),
        secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
        started: Instant::now(),
    }
}

pub fn zstd_ndjson(rows: &[Value]) -> Vec<u8> {
    let mut buf = Vec::new();
    for row in rows {
        writeln!(buf, "{row}").unwrap();
    }
    zstd::stream::encode_all(buf.as_slice(), 1).unwrap()
}

pub fn parse_parquet_rows(bytes: &[u8]) -> Vec<RecordBatch> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))
        .unwrap()
        .build()
        .unwrap();
    reader.map(Result::unwrap).collect()
}

pub fn col_str(batch: &RecordBatch, name: &str, row: usize) -> Option<String> {
    let col = batch.column_by_name(name)?;
    if col.is_null(row) {
        return None;
    }
    col.as_any()
        .downcast_ref::<StringArray>()
        .map(|a| a.value(row).to_owned())
}

pub fn col_i64(batch: &RecordBatch, name: &str, row: usize) -> Option<i64> {
    let col = batch.column_by_name(name)?;
    if col.is_null(row) {
        return None;
    }
    let any = col.as_any();
    any.downcast_ref::<Int64Array>()
        .map(|a| a.value(row))
        .or_else(|| {
            any.downcast_ref::<TimestampMillisecondArray>()
                .map(|a| a.value(row))
        })
}

pub fn col_f64(batch: &RecordBatch, name: &str, row: usize) -> Option<f64> {
    let col = batch.column_by_name(name)?;
    if col.is_null(row) {
        return None;
    }
    col.as_any()
        .downcast_ref::<Float64Array>()
        .map(|a| a.value(row))
}

pub fn col_bool(batch: &RecordBatch, name: &str, row: usize) -> Option<bool> {
    let col = batch.column_by_name(name)?;
    if col.is_null(row) {
        return None;
    }
    col.as_any()
        .downcast_ref::<BooleanArray>()
        .map(|a| a.value(row))
}

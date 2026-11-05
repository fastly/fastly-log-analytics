use crate::shell::Runtime;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    io::{self, Write},
    time::Instant,
};

pub const HEAP_MEASUREMENT: &str = "allocated_heap_snapshot_mib_not_peak";
pub const LINEAR_MEMORY_MEASUREMENT: &str = "wasm_linear_memory_high_water_at_sample";
pub const MAX_WORKER_METRICS: usize = 512;

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionMetrics {
    pub calls: Option<u32>,
    pub vcpu_ms: Option<u64>,
    pub summary_vcpu_ms: Option<u64>,
    pub duration_ms: Option<u64>,
    pub heap_mib: Option<u32>,
    pub linear_memory_high_water_bytes: Option<u64>,
}

impl ExecutionMetrics {
    pub fn snapshot(runtime: &impl Runtime, started: Instant) -> Self {
        Self {
            calls: runtime.execution_backend_calls(),
            vcpu_ms: runtime.execution_vcpu_ms(),
            summary_vcpu_ms: None,
            duration_ms: Some(started.elapsed().as_millis() as u64),
            heap_mib: runtime.heap_mib(),
            linear_memory_high_water_bytes: runtime.linear_memory_high_water_bytes(),
        }
    }

    fn from_reply(reply: &Value) -> Self {
        let reply = reply.get("execution_metrics").unwrap_or(reply);
        Self {
            calls: reply["calls"].as_u64().and_then(|n| n.try_into().ok()),
            vcpu_ms: reply["vcpu_ms"].as_u64(),
            summary_vcpu_ms: reply["summary_vcpu_ms"].as_u64(),
            duration_ms: reply["duration_ms"].as_u64(),
            heap_mib: reply["heap_mib"].as_u64().and_then(|n| n.try_into().ok()),
            linear_memory_high_water_bytes: reply["linear_memory_high_water_bytes"].as_u64(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerExecutionMetrics {
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage: Option<String>,
    pub answered: bool,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub noop_reason: Option<String>,
    #[serde(flatten)]
    pub metrics: ExecutionMetrics,
}

impl WorkerExecutionMetrics {
    pub fn from_reply(kind: &str, shard: Option<&str>, reply: Option<&Value>) -> Self {
        Self {
            kind: kind.into(),
            shard: shard.map(str::to_owned),
            stage: reply
                .and_then(|r| r["ran"].as_str())
                .filter(|stage| {
                    stage.len() <= 32
                        && stage
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
                })
                .map(str::to_owned),
            answered: reply.is_some(),
            status: reply
                .and_then(|r| r["status"].as_str())
                .filter(|status| {
                    matches!(
                        *status,
                        "ok" | "error"
                            | "split"
                            | "segment"
                            | "schema_mismatch"
                            | "pending"
                            | "stale"
                            | "not_installed"
                    )
                })
                .unwrap_or(if reply.is_some() {
                    "invalid"
                } else {
                    "missing"
                })
                .into(),
            noop_reason: reply
                .filter(|r| {
                    kind == "catalog"
                        && r["status"] == "stale"
                        && r["noop_reason"] == "source_index_base_mismatch"
                })
                .map(|_| "source_index_base_mismatch".into()),
            metrics: reply.map(ExecutionMetrics::from_reply).unwrap_or_default(),
        }
    }
}

pub fn write_resource_event(
    writer: &mut impl Write,
    event: &Value,
    timestamp_us: u64,
) -> io::Result<()> {
    let mut event = event.as_object().cloned().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "execution event must be an object",
        )
    })?;
    event.insert("timestamp_us".into(), timestamp_us.into());
    let message = Value::Object(event).to_string();
    // Each named-endpoint write is a separate log line.
    if writer.write(message.as_bytes())? != message.len() {
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "partial execution log write",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::{self, Write};

    #[derive(Default)]
    struct Endpoint {
        messages: Vec<Vec<u8>>,
        partial: bool,
    }

    impl Write for Endpoint {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let count = if self.partial { 2 } else { bytes.len() };
            self.messages.push(bytes[..count].to_vec());
            Ok(count)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn named_resource_event_is_one_complete_json_message() {
        let event = json!({
            "event": "cron.execution",
            "root_run_id": "cron-1700000000-0123456789abcdef",
            "execution_metrics": {"scope": "tick_end", "vcpu_ms": 31, "calls": 18},
            "worker_executions_metrics": [
                {"kind": "catalog", "status": "stale",
                 "noop_reason": "source_index_base_mismatch", "vcpu_ms": 2},
                {"kind": "request_ingest", "status": "ok", "vcpu_ms": 610}
            ]
        });
        let mut endpoint = Endpoint::default();
        write_resource_event(&mut endpoint, &event, 1_700_000_000_123_456).unwrap();
        assert_eq!(endpoint.messages.len(), 1);
        let mut actual: Value = serde_json::from_slice(&endpoint.messages[0]).unwrap();
        assert_eq!(actual["timestamp_us"], 1_700_000_000_123_456u64);
        actual.as_object_mut().unwrap().remove("timestamp_us");
        assert_eq!(actual, event);
        assert!(!endpoint.messages[0].contains(&b'\n'));
    }

    #[test]
    fn named_resource_event_rejects_partial_writes_without_emitting_fragments() {
        let mut endpoint = Endpoint {
            partial: true,
            ..Endpoint::default()
        };
        let error = write_resource_event(
            &mut endpoint,
            &json!({"event": "cron.execution", "root_run_id": "root"}),
            1_700_000_000_000_000,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WriteZero);
        assert_eq!(endpoint.messages.len(), 1);
    }
}

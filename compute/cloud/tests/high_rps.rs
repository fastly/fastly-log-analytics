//! High-RPS ingest (Task 25): raw objects split into decompressed-byte segments converted by
//! separate workers, multipart window output and relayed fan-out past the coordinator's
//! direct dispatch width.

mod common;

#[path = "high_rps/support.rs"]
mod support;
#[path = "high_rps/segments.rs"]
mod segments;
#[path = "high_rps/multipart.rs"]
mod multipart;
#[path = "high_rps/relay.rs"]
mod relay;
#[path = "high_rps/replan.rs"]
mod replan;

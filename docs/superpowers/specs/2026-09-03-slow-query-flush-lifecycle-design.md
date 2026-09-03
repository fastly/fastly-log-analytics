# Slow-Query Flush Resource-Lifecycle Design

**Date:** 2026-09-03

## Goal

Prevent PostgreSQL metadata-pool exhaustion caused by short-lived slow-query
flush workers retaining a thread-local connection after the worker exits.
Preserve the existing best-effort nature of slow-query persistence and leave
normal request and long-lived cron connection semantics unchanged.

## Current failure mode

`backend/core/metadata/slow_queries.py::_flush_all()` is invoked by both a
`threading.Timer` and an ad-hoc batch-flush thread. It calls
`metadata.base.get_con(service_id)`, which uses a long-lived PostgreSQL
connection associated with the calling thread. Those worker threads terminate
after flushing, but `_flush_all()` currently has no lifecycle cleanup.

With `METADATA_DSN` configured, each terminated worker can therefore pin one
connection in the bounded PostgreSQL pool. Repeated flushes eventually consume
the pool, causing unrelated metadata operations to block until the pool
timeout and fail as if PostgreSQL were unavailable. The existing
`release_thread_connection()` helper is specifically designed to return only
the calling thread's connection and is already used by the cron heartbeat
worker.

## Design

### Flush ownership

`_flush_all()` remains responsible for batching and writing rows. For each
service, it will:

1. Obtain the metadata connection as it does today.
2. Execute the batch insert and commit.
3. Always call `release_thread_connection()` in a `finally` block.

The release call will cover successful writes, database errors, and any
unexpected exception after connection acquisition. If no connection was
acquired, the helper is a no-op. The existing exception handling remains
best-effort: flush errors are not raised into query completion or timer
threads.

### Compatibility

No changes will be made to `get_con()`, the thread-local connection contract,
or request/cron callers that intentionally retain their connection for the
life of a reused worker thread. SQLite behavior is unchanged because
`release_thread_connection()` already does nothing when PostgreSQL mode is
disabled.

### Scope

This change covers the slow-query flush path only. Other background workers
will not be migrated to a new abstraction as part of this fix. The audit will
still use repository searches and targeted tests to ensure this new release
seam does not duplicate or bypass existing pool behavior.

## Testing

Add regression coverage that:

- verifies a successful flush releases the calling thread's connection;
- verifies release still occurs when the batch write fails;
- exercises the flush from a short-lived worker thread so a connection cannot
  remain attached to a dead thread;
- verifies the no-PostgreSQL path does not attempt a release;
- preserves the current buffering and best-effort error behavior.

Run the smallest relevant slow-query and PostgreSQL metadata tests first,
followed by formatting and the repository's required `make ci` gate.

## Operational outcome

Short-lived slow-query flush workers will return their PostgreSQL connection
before exiting, preventing cumulative pool-slot leaks while retaining
asynchronous, non-blocking slow-query persistence.

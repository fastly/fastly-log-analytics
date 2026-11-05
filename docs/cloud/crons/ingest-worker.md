# ingest_worker

`POST /_internal/ingest` on the peer worker service (`flp-worker`). Converts one ingest shard (up to 22 raw objects) into Parquet window files within its own execution's budget. It has no `cron_kind` row: the coordinator ([request-ingest.md](request-ingest.md)) records the outcome.

## Purpose

One Compute execution has 32 backend calls and 60 s. A single tick cannot read, convert and write a minute's worth of raw files. Each worker execution brings its own budget, so the coordinator fans a plan out over up to 8 worker calls (`ingest_fanout`, bounded by the coordinator's own call budget). With `ingest_relay` each call can be a relay that reaches up to 16 shard workers.

## Why it exists

A Compute service cannot call its own domain, so the fan-out goes to a peer service running the same Wasm. The worker is stateless: it writes FOS objects only, and never KV, Core Cache or telemetry, so the coordinator stays the single owner of the plan and commit point.

## How it works

**Dispatch** (`FastlyRuntime::worker_dispatch`, `sdk.rs`)
- The coordinator sends a batch of `(path, body)` requests to backend `flp_worker` (host config `worker_host`) with `send_async`.
- 1 budget call is charged per request up front.
- All requests are polled until done or the deadline. A non-200 answer is `worker_http_<status>`; an unfinished request is `worker_deadline`. Bodies are read with the 2 MiB `bounded_json` cap.
- Dispatch is refused when `worker_service_id` is unset. On the worker service itself only shard requests (`/_internal/ingest`, `/_internal/ingest-range`) may be dispatched, and they go to backend `flp_compute` (host config `compute_host`): a service can't call its own domain, so a relay reaches the main service, never itself.
- The batch is generic: F4 can send `/_internal/stage` requests in it too.

**Relay** (`POST /_internal/relay`, `worker.rs` `handle_worker_relay`), opt-in with config `ingest_relay` = shards per relay (2..=16; 0 or 1 is off)
- Body `{deadline_ms, requests: [[path, ShardRequest]…]}`, at most 60,000 bytes (`RELAY_BODY_LIMIT`), 2..=16 requests (`MAX_RELAY_SHARDS`), each path a shard path and `deadline_ms` ≤ 32 s. Anything else is 400; a wrong worker key is 403.
- The relay runs on `flp-worker` and dispatches its shards to `flp`'s own `compute_host`, where `main.rs` routes a worker-keyed `POST /_internal/ingest(-range)` straight to `handle_worker_ingest` (the worker key is checked there) instead of the front-authenticated shell. Each shard therefore still runs in its own execution with its own 32 calls; the relay spends 1 secret read + 1 call per shard.
- It waits until every shard answered or `deadline_ms − 2 s` (`RELAY_MARGIN`), then answers `{status: "ok", replies: [{reply} | {error}], calls, vcpu_ms}` in request order. Relayed shards get a worker stop 2 s earlier than direct ones.
- Depth is bounded at two hops: the main service accepts only shard paths from the worker key, and a relay forwards only shard paths.

**Request** (`worker.rs` `ShardRequest`, JSON body ≤ 64 KiB)
`{service_id, region, bucket, prefix, plan_id, shard, attempt, stream, table, schema, window_s, keys, stop_ms, lease_until, segment?, segment_bytes?}`

A segment shard (`segment: {start, end?}`, exactly one key) is sent to `POST /_internal/ingest-range`, so a release without segments answers 404 before writing. It reads the object (at most 24 MiB compressed) and converts only lines whose first byte lies in `[start, end)`, decompressing and discarding the bytes before `start`.

**Steps** (`handle_worker_ingest`, `convert_shard`)
1. Authenticate `x-flp-worker-key` against secret `worker_secret` (constant time); 403 otherwise. Validate the body; 400 otherwise.
2. If `schema` differs from this build's `convert::schema_hash`, answer `{status: "schema_mismatch"}` and touch nothing.
3. Read secrets `fos_ak.<sid>`, `fos_sk.<sid>` and `cdn_key` (4 secret reads in total, within the limit of 5). `fos_sk.<sid>` holds the provisioning envelope `{access_key, secret_key}`; it is unwrapped with `admin::fos_credentials`, the same as the admin path. Missing secrets give 503.
4. GET every raw key through the CDN path, with a cumulative decompressed cap, and convert it.
   - If the batch outgrows the cap, answer `split`.
   - With `segment_bytes > 0`, a lone whole object or open segment whose content passes `2 × segment_bytes` is decompressed once more to measure it (up to 512 MiB), and the worker answers `{status: "segment", total}` before any write. Past 512 MiB it is quarantined as `decompressed_too_large`. A closed segment past the 48 MiB cap is quarantined too.
   - Past the stop time (`min(stop_ms, 28 s)`), answer error `worker_wall`.
5. Count the distinct windows. If they exceed `window_room = 32 − 4 − keys − 2`, answer `{status: "split"}` **before any PUT**.
6. PUT the intent manifest `processed/_inflight/<plan_id>/<shard>.json` = `{plan_id, shard, attempt, keys}`.
7. For each window, write Parquet and PUT `processed/<table>/<hour_dir>window=<s>-<e>-<plan_id>s<shard>-n<rows>.parquet`. Before the manifest and each window PUT, the worker checks the stop time and refuses to start within 30 s of `lease_until` (`WORKER_LEASE_MARGIN_S`, error `worker_lease_expired`).
   - Every PUT is `Store::put_verified`: the ETag returned by FOS must equal the body's MD5, or the shard fails with `etag_mismatch`.
   - A window file over 8 MiB (`compaction::PART_BYTES`) is uploaded as 8 MiB multipart parts when the remaining calls cover create + parts + complete and still leave every later window PUT, the summary PUT and one spare. Each part is ETag-checked, any failure aborts the upload, and `md5` is the whole body's MD5. Otherwise the window falls back to one PUT. A crash between create and complete leaves an open upload until FOS cleans it up on its platform-managed schedule (`docs/research/04-object-storage-writes.md`); the shard is retried under a new upload.
8. Answer `{status: "ok", result: {attempt, manifest, outputs: [{key, rows, bytes, md5}], objects: [...]}}`. The `objects` list gives per raw key: status, rows, lines, bad lines and a bad sample of at most 512 bytes. Every reply also carries `usage` (FOS usage rows), `calls` and `vcpu_ms`.

## Budgets

- Calls: 4 secrets + n GETs + 1 manifest + w window PUTs + 1 spare ≤ 32. The fake-runtime tests give each worker execution a fresh 32-call counter and assert that it stays within 32. The measured max is **26**.
- Wall: no new GET or PUT after `min(stop_ms, 28 s)`. The coordinator sets `stop_ms` 2 s before its drain deadline.
- Core Cache: none. No lookups, inserts or transactions, and no telemetry emission (`cache_transactions` is capped per execution).
- KV: none.

## Roles and permissions

Internal only. The route is served by the worker router in `sdk.rs` next to `/_internal/delete` and requires the shared worker secret.

## Telemetry

None is written by the worker. The coordinator buffers the returned usage rows as `fos_usage`. After its commit it writes `ingest_files` and `platform_errors` rows from `objects`.

## Failure modes and recovery

- **Timeout or lost reply.** The coordinator counts a failure and re-dispatches the shard on a later tick. The worker rewrites identical keys, so a retry never duplicates rows.
- **Late worker.** No PUT starts within 30 s of the plan lease. An abandon cleanup runs only after the lease expires, so it never races a write.
- **Partial writes.** The manifest, written first, names the shard's keys and therefore its outputs. If the plan is abandoned, the coordinator deletes every file the manifest names before replanning.
- **ETag mismatch.** The output is not recorded, and the shard fails and is retried.
- **Too many windows or bytes.** The worker answers `split` before writing anything, and the coordinator halves the shard.

## Tests

`app/tests/ingest_fanout_faults.rs`: `worker_signs_with_the_secret_key_unwrapped_from_its_envelope`, `a_worker_past_its_lease_writes_nothing`.

`app/tests/ingest.rs`: `worker_ingest_authenticates_validates_and_reports_schema_mismatch`, `a_shard_with_too_many_windows_for_its_budget_splits_before_writing`, `lost_and_failed_worker_calls_are_retried_without_duplicates`, `an_etag_that_is_not_the_body_md5_is_never_recorded`, and `sustained_load_of_sixty_files_a_minute_drains_without_backlog`.

## Code

- `app/src/ingest/worker.rs`: request and result types, `handle_worker_ingest`, `convert_shard`.
- `app/src/ingest/storage.rs`: `put_verified`, `normalize_etag`.
- `app/src/sdk.rs`: `worker_dispatch`, the worker router route.

# Streaming (`/streaming`)

Fleet-level video Quality-of-Experience analytics built from Common Media Client Data (CMCD v1/v2) that players attach to requests. The per-session companion is [sessions-stream.md](sessions-stream.md).

Source: [`frontend/app/streaming/`](../../frontend/app/streaming/): `page.tsx` is a server wrapper that prefetches and dehydrates the default aggregates (only when the response is `available` and `has_data`); `_sections/StreamingClient.tsx`, `StreamingChartsGrid.tsx`, `StreamingConstants.tsx`.

## Availability

Metrics come from the `cmcd_*` log fields the generated edge VCL extracts (`cmcd_sid`, `cmcd_br`, `cmcd_bl`, `cmcd_bs`, `cmcd_mtp`, `cmcd_ot`, ...). The sidebar entry is shown only when the active service's schema contains `cmcd_sid` (`requiresCmcd` in `AppLayout`). If the response says `available: false` the page shows "CMCD data not available" and points at Admin settings. CMCD is enabled per service with `POST /api/services/{service_id}/cmcd/enable` (token, `mode` = `query_string` or headers, `version` 1 or 2), `.../cmcd/disable`, and `GET .../cmcd/status` ([`backend/routers/performance.py`](../../backend/routers/performance.py), VCL in [`backend/provision/cmcd_fields.py`](../../backend/provision/cmcd_fields.py)). CMCD booleans are encoded as bare keys in v1 and `key=?1` in v2, so the service's version setting decides which form is parsed.

## Panels

- Headline cards: Total Sessions, Peak Viewers, Avg Duration, Rebuffer Rate, Avg Bitrate, Avg Buffer, Median Throughput.
- Charts and tables: Active Viewers, Session Starts, Buffer Health (buffer length plus starvation rate), Bitrate & Quality, Measured Throughput, Startup Requests, Top Content, Session Duration Distribution, Object Type Distribution, Rebuffer Rate by Country, Rebuffer Rate by ASN.
- Each card has its own loading and error state; a failed query does not blank the page. Zooming a time-series chart writes the global range. The query refetches every 30 seconds (`staleTime` 30 s) and cancels `cmcd` queries on unmount.

Global range and filters apply, as on the other analytics pages (`ReportLayout`).

## Roles

| Role | Access |
|---|---|
| Admin | All sections. |
| Analyst (remote share) | Read-only; `/api/cmcd/` is in the analyst POST allowlist; window clamped to the invite. CMCD enable/disable under `/api/services/...` is not an analyst-reachable write. |

## Backend

`POST /api/cmcd/aggregates` ([`backend/routers/performance.py`](../../backend/routers/performance.py), models in [`backend/models/analytics/cmcd.py`](../../backend/models/analytics/cmcd.py)): `CmcdRequest` with `filters`, `bucket_seconds` (page sends 300), `top_n` (page sends 30), range fields, and optional `sections` from `overview`, `sessions_ts`, `buffer_health_ts`, `bitrate_ts`, `throughput_ts`, `top_content`, `rebuffer_by_country`, `rebuffer_by_asn`, `object_type_dist`, `streaming_format_dist`, `startup_ts`, `session_duration_dist`. Returns `CmcdAggregatesResponse` (`available`, `has_data`, each section).

Standard mode computes the sections in one query runner over the request view ([`backend/repositories/cmcd.py`](../../backend/repositories/cmcd.py)). High-scale mode reads the `cmcd_projection_facts` table ([`backend/high_scale/cmcd.py`](../../backend/high_scale/cmcd.py)) with ClickHouse quantiles.

## Tests

- Frontend unit: [`frontend/__tests__/app/usage-sessions-streaming.test.tsx`](../../frontend/__tests__/app/usage-sessions-streaming.test.tsx)
- E2E: `Streaming (CMCD) Page Contract` in [`frontend/e2e/pages/dashboard.spec.ts`](../../frontend/e2e/pages/dashboard.spec.ts) (populated render, not-available state, accessibility and layout stability on a warm reload). The e2e dataset has no CMCD columns, so the isolated contract backend ([`scripts/run_contract_backend.py`](../../scripts/run_contract_backend.py)) serves a per-browser fixture when `CONTRACT_STREAMING_FIXTURES=1`.
- Traffic: `scripts/dev/scale_harness.py` can send CMCD playback traffic (see its `--help`).

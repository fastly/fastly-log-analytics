# Cloud provisioning and capture modes

Cloud provisions a Fastly VCL service through a job (`POST /api/provision/execute`, stepped by `POST /api/admin/jobs/{id}/step`). The job clones the active version, applies the difference between what Cloud wants and what it owns, validates and activates. Reconcile and teardown jobs use the same diff. The code is `compute/cloud/src/admin/reconcile.rs` (`desired`, `discover`), with the preflight in `admin/mod.rs`.

Cloud owns a resource when its name is in the service's owner marker snippet (`flp-owner-<id>`) or in Cloud's own service record. It never changes or deletes anything else.

## Logging endpoint type

New endpoints use Fastly's native Object Storage endpoint (`logging/fos`). Cloud sends the bucket name and region with the same path, period, format, `zstd` compression and `blank` message type as before. It sends no access key, secret key or domain: Fastly mints an access key limited to the bucket and keeps its secret. The output layout is unchanged, so ingest reads both endpoint types the same way.

An install provisioned before this change has a Fastly S3 endpoint (`logging/s3`) signed with Cloud's own Object Storage key. Reconcile keeps managing an endpoint that already exists as `logging/s3` as S3. It never converts it, and never adds a `logging/fos` endpoint beside it, which would write every log line twice. Renaming the endpoint deletes the old name and creates the new one as `logging/fos`.

Ownership checks, the preflight and teardown read both endpoint types and act only on Cloud's exact names. Teardown deletes the endpoint but not the key Fastly minted for it, since Fastly doesn't delete it either; revoke it under Object Storage access keys in the Fastly control panel if you no longer need it.

Cloud still creates its own bucket-scoped Object Storage key: ingest, the RUM assets and the CDN front service use it. The provisioning token needs the Engineer role with access to all services, or Superuser; Fastly requires one of those to create a `logging/fos` endpoint.

## Opaque resource naming (SEC-22)

Newly provisioned system-generated resources default to independent 96-bit CSPRNG opaque labels (`fla-<24 lowercase hex chars>`) rather than install-name or `service_id`-derived names:

- **FOS buckets** (`compute/cloud/src/admin/fos.rs`), **session scorer `.edgecompute.app` domains** (`compute/cloud/src/admin/scoring.rs`), and **internal Compute (`flp`) & Worker (`flp-worker`) `.edgecompute.app` domains** (plus default `.global.ssl.fastly.net` front domains when no custom public host is supplied, `compute/cloud/infra/provision.py`).
- Generated names are persisted to local inventory / KV before remote Fastly or FOS creation and retried up to 5 times on `409` (or bucket collision) responses.
- Existing legacy installs keep their persisted bucket and domain names unchanged across deploys and reconciles, and explicit operator-supplied public front or custom bucket names are preserved as given.

## Capture modes

`Provision.capture` picks how the service's request data is captured. The service record stores `capture` only when it is `attached`, and it can't be changed after provisioning; tear down and provision again to switch.

| Mode | Cloud manages | Capture VCL |
|---|---|---|
| `managed` (default) | its marker, the capture snippets (`Fastly Log Analytics - vcl_recv` / `vcl_deliver`), the logging endpoint (default `Fastly Object Storage Logs`), `log_analytics_condition`, and when RUM is on the RUM snippets, backend and dictionary | Cloud's own |
| `attached` | its marker, one logging endpoint (default `FLP Cloud Logs`) and its response condition `flp_cloud_log_condition` | the other install's |

In `attached` mode the endpoint's log format is the same one `managed` mode writes. It reads the `x-fos-edge-data:*` / `x-fos-origin-data:*` headers that the other install's capture VCL sets.

Cloud's condition logs exactly the requests the other install's request-log endpoint logs, so both installs hold the same request data (`reconcile::attach_condition_statement`):

- When the service has `log_analytics_condition`, `flp_cloud_log_condition` copies its statement verbatim, including the other install's sample rate, edge-only setting, custom condition and RUM-beacon exclusion. Cloud's own `sample_rate`, `edge_only` and `custom_condition` are then not used. Each reconcile re-reads it, so a change to the other install's condition reaches Cloud's on the next reconcile.
- When it is absent, Cloud uses its own statement with `req.url.path != "/rum-beacon"` added after the first clause.
- When it exists but has no non-empty statement or is not a `RESPONSE` condition, validate, provision and reconcile stop with `capture_condition_unreadable` rather than guess. Teardown keeps Cloud's condition as it is.

`attached` mode is request logs only. RUM and CMCD need VCL that Cloud would have to own, so they are rejected.

## Validate

`POST /api/provision/validate` takes `{token, service_id}` plus optional `capture`, `logging_endpoint_name`, `fos_bucket`, `fos_region`, `fos_prefix` and `log_fields`. With `"capture": "attached"`, the response adds:

```json
{"capture": {"mode": "attached", "missing_fields": ["server_region"]}}
```

`missing_fields` lists the chosen log fields (default fields when `log_fields` is absent) whose headers Cloud's own capture VCL would set but the existing snippets never do. Those columns will be blank. The check is a text match on `req.http.<name>` in the snippet bodies, so treat it as a guide.

## Errors

| Status | Code | When |
|---|---|---|
| 409 | `service_managed_by_other_install` | `managed` mode on a service with another install's managed snippets or endpoint and no Cloud marker |
| 409 | `managed_resources_not_owned` | a reconcile finds managed names Cloud doesn't own (`managed` mode only) |
| 409 | `resource_name_collision` | the chosen endpoint name already exists and Cloud doesn't own it |
| 409 | `capture_vcl_missing` | `attached` mode, but the service has neither both `Fastly Log Analytics - vcl_recv` and `- vcl_deliver` snippets nor `Fastly Log Analysis Capture` (validate, provision and reconcile; teardown still runs) |
| 409 | `capture_condition_unreadable` | `attached` mode, and the service's `log_analytics_condition` has no non-empty statement or is not a `RESPONSE` condition (validate, provision and reconcile) |
| 400 | `invalid_capture` | `capture` is not `managed` or `attached` |
| 400 | `rum_unavailable_in_attach_mode` | `attached` with RUM enabled |
| 400 | `cmcd_unavailable_in_attach_mode` | `attached` with CMCD enabled |
| 400 | `attach_endpoint_name_reserved` | `attached` with an endpoint name another install uses: a managed name, or one containing "Fastly Log Analytics", "Fastly Log Analysis", "Fastly Object Storage Logs" or "Fastly RUM" |

## Two installs on one service

A High-Scale install and a Cloud install can log the same service. The High-Scale install keeps its capture snippets, endpoint, condition and RUM resources. Cloud attaches alongside it:

1. In the provisioning wizard, pick the service. Cloud's managed-mode validate returns `service_managed_by_other_install`, and the wizard re-validates with `capture: "attached"`.
2. The wizard offers **Attach alongside the existing install**. It lists any fields the existing VCL never sets. If the capture VCL is missing it says so instead.
3. Choosing it sets logging only, endpoint `FLP Cloud Logs`, no RUM and no CMCD. The deploy sends `capture: "attached"`.

What to know:

- Both installs clone and activate versions of the same service. If both run at once, the later activation can drop the other's draft change. A Cloud job stops with `active_version_changed` when the active version moves under it; run one install's change at a time.
- Cloud depends on the other install's capture VCL. If that install is torn down or drops a header, the matching Cloud columns go blank, and Cloud's next reconcile fails with `capture_vcl_missing`.
- Cloud's condition follows the other install's `log_analytics_condition` only at reconcile. Between a change to it and Cloud's next reconcile, the two installs log different requests.
- A second Cloud install can't attach: Cloud's marker on the service makes it `service_managed_by_other_install` for any other install that checks markers.
- Cloud teardown in `attached` mode deletes only its marker, endpoint and condition.

# Cloud provisioning and capture modes

Cloud provisions a Fastly VCL service through a job (`POST /api/provision/execute`, stepped by `POST /api/admin/jobs/{id}/step`). The job clones the active version, applies the difference between what Cloud wants and what it owns, validates and activates. Reconcile and teardown jobs use the same diff. The code is `compute/cloud/src/admin/reconcile.rs` (`desired`, `discover`), with the preflight in `admin/mod.rs`.

Cloud owns a resource when its name is in the service's owner marker snippet (`flp-owner-<id>`) or in Cloud's own service record. It never changes or deletes anything else.

## Capture modes

`Provision.capture` picks how the service's request data is captured. The service record stores `capture` only when it is `attached`, and it can't be changed after provisioning; tear down and provision again to switch.

| Mode | Cloud manages | Capture VCL |
|---|---|---|
| `managed` (default) | its marker, the capture snippets (`Fastly Log Analytics - vcl_recv` / `vcl_deliver`), the logging endpoint (default `Fastly Object Storage Logs`), `log_analytics_condition`, and when RUM is on the RUM snippets, backend and dictionary | Cloud's own |
| `attached` | its marker, one `logging/s3` endpoint (default `FLP Cloud Logs`) and its response condition `flp_cloud_log_condition` | the other install's |

In `attached` mode the endpoint's log format is the same one `managed` mode writes. It reads the `x-fos-edge-data:*` / `x-fos-origin-data:*` headers that the other install's capture VCL sets. The condition has the same statement as `log_analytics_condition` (sample rate, edge only, custom condition).

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
- When the other install has RUM, its condition excludes `/rum-beacon` requests; Cloud's doesn't, so Cloud also logs those beacon requests.
- A second Cloud install can't attach: Cloud's marker on the service makes it `service_managed_by_other_install` for any other install that checks markers.
- Cloud teardown in `attached` mode deletes only its marker, endpoint and condition.

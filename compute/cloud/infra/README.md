# Install the app

Run from the repository root. Planning needs no token and makes no requests:

```sh
uv run app/infra/provision.py bootstrap --name flp
```

`--name` defaults to `flp`. It namespaces the three services and physical stores.
Both Compute services use fixed resource-link aliases: `flp_config`,
`flp_secrets` and `flp_state`.

## Create drafts

Only run this after approving resource creation in the target account:

```sh
mkdir -p app/pkg
uv run app/infra/provision.py bootstrap \
  --name flp \
  --erl-service-id <existing-active-vcl-service-id> \
  --inventory app/pkg/flp-resources.local.json \
  --apply
```

Provide one `FASTLY_API_TOKEN` (requested with `getpass` if unset; never put
credentials in command arguments). Bootstrap uses it to create services and
stores, and stores it in the Secret Store as `fastly_token` for the app's Fastly
API reads, following the log-analytics pattern. `admin-reset` also prompts for a
token and only writes the setup-code hash to the Secret Store, so it may use a
token scoped to Secret Store updates if Fastly allows it. To rotate the app's
token later, write a standalone `fastly_token` secret to the install's Secret
Store: the app reads it ahead of its secret bundle, folds it into the bundle on
its next admin pass and then removes the standalone entry. The CLI does not
create tokens. Bootstrap stores an expired placeholder and prints no setup code. The first
`make deploy` issues the one-time admin setup code, prints it once at the end
of activation and records `admin_code_issued` in the inventory; later deploys
never issue another. `admin-reset` is the only way to replace it. The admin
enters this code on `/setup` to prove ownership and enter admin mode. New
admin-reset codes can take a minute or longer to reach the
app; wait rather than resubmitting or regenerating.

The CLI falco-lints the selected VCL variant, refuses existing same-name services
or stores, checkpoints IDs without secrets, links stores to both Compute drafts,
and creates static TLS backends and the front's dictionaries, routes and clock.
It validates the front draft but **does not upload packages or activate anything**.
Deploy through `make -C app deploy`, which owns frozen-source provenance, paired
Compute activation and the `flp-html` purge.

The default public origin is `https://<name>.global.ssl.fastly.net`.
`--public-host`, `--compute-host` and `--worker-host` override the domains.
A custom public domain still needs Fastly TLS configured by its owner.

The one-time admin setup code is printed once. Only its SHA-256 hash is saved in
the Secret Store. The generated session, front, CDN and worker keys never enter
the inventory or console output. A failed bootstrap leaves its partial inventory
for explicit cleanup; it does not silently adopt resources or rotate live keys.

ERL detection reads an active service's generated VCL, as pinned v3 does.
`--no-erl` explicitly selects the Compute KV fallback. An unknown capability
without that override stops before creation.

## Recover admin access

Both names run the same operation:

```sh
uv run app/infra/provision.py admin-setup-code --name flp --apply
uv run app/infra/provision.py admin-reset --name flp --apply
```

Use `--secret-store-id` instead of name discovery when needed. Without `--apply`,
these commands only describe the operation. Rotation replaces `admin_setup_hash`;
it never resets users, sessions or other secrets.

## Update front VCL

```sh
uv run app/infra/provision.py front \
  --inventory app/pkg/flp-resources.local.json --apply
```

This prepares and validates an unactivated draft. The release wrapper can call
`prepare_front(api, inventory_path)` directly, then activate after its package
and provenance checks.

The Makefile calls `deploy --name <name> --package pkg/flp.tar.gz` from `app/`.
It reads `pkg/provenance.json` and defaults to `pkg/<name>-resources.local.json`;
`FLP_INVENTORY` or `--inventory` selects another inventory. It checks the package
digest, the embedded build SHA and the committed app tree before uploads and
again before every activation. Front VCL comes from that same build commit, not
the mutable working tree. Both remote package-file hashes must match.
Worker, Compute and front activate in that order. Before purging HTML, the CLI
waits up to 180 seconds for the public front's `/version` to report the exact
deployed build SHA. This covers first-activation propagation without mistaking
an older build's HTTP 200 for readiness. Authorization errors fail immediately.
Each step is checkpointed; a partial release is not reported as complete.

If activation succeeded but readiness or the final purge failed, finish only
that step without uploading packages or activating versions again:

```sh
uv run app/infra/provision.py purge-html \
  --inventory app/pkg/flp-resources.local.json
```

This verifies the front's identity and deployed SHA before using the documented
`POST /service/{service_id}/purge/flp-html` endpoint. A purge error remains an
error; it never becomes a successful release or an implicit full redeploy.

## Verify

```sh
uv run --with pytest pytest -q app/infra
uv run --with ruff ruff check app/infra
uv run --with ruff ruff format --check app/infra
falco lint -I app/vcl/front -I app/vcl/front/tests app/vcl/front/tests/main.vcl
falco test -I app/vcl/front -I app/vcl/front/tests app/vcl/front/tests/main.vcl
```

Pytest also runs real falco tests against the stripped no-ERL variant. Falco
2.0.1 hardcodes `fastly.ff.visits_this_service`; its tests cannot prove actual
edge-to-shield forwarding. That remains a live acceptance check.

## Shielding

`build_front_plan` gives the `flp` and `flp_tick` backends `shield = iad-va-us`,
so every `flp` execution runs at the shield and Core Cache log buckets are shared.
`running_on()` may report a nearby POP when Fastly relocates shield traffic, so
code does not require a literal IAD POP; it records the actual `pop` on telemetry
rows and warns only on a persistent unexpected POP. Shielding adds a Virginia
round trip to every `/api/*` call. `x-flp-auth-fail` must survive the shield hop
(`unset` only on `fastly.ff.visits_this_service == 0`), and falco cannot prove the
real edge-to-shield behavior: confirm it live (`/api/*` rate limiting still
triggers once shielded). `flp-worker` never writes operational logs.

Lint and test both VCL variants with the `falco lint` / `falco test` commands
above (the no-ERL `tests-no-erl` variant is exercised by the pytest run).

Recorded GET contracts are under `app/tests/fixtures/fastly-api/`. Each contains
its request, status, timestamp and redaction description. Token IDs/values and
full generated VCL are never recorded. The write-adapter tests are explicitly
synthetic: they are not recorded creation or upload responses. The `install-*`
fixtures are actual readbacks of the parent-owned bootstrap, including store
links, static TLS backends, write-only dictionaries and the clock. The recorded
error is a real 404; 409, 422 and 429 write-error contracts remain unverified,
not fabricated.

Run `FLP_LIVE=1 uv run --with pytest pytest -q app/infra/test_contracts.py` to
re-fetch the recorded GETs and fail on status or schema drift. This needs the
account token in the environment or the gitignored `.env`. It makes no writes.
Set `FLP_INVENTORY` to the corresponding install inventory to also re-fetch its
resource readbacks. Schema matching checks observed fields/types and list
variants; a recorded null does not establish a non-null primitive's type.

## Goal 2 infrastructure

Bootstrap prepares `flp_front` on both Compute services, pointing to the front's
public domain rather than the Compute service itself. It also prepares fixed
regional FOS backends for the nine regions in v3's `SHIELD_MAP`. Every backend
uses the domain as its address, override host, SNI and certificate hostname,
with IPv6 preference disabled. A normal, separately approved CLI deployment
adds missing backends to existing installs' drafts before validation. It refuses
to replace a backend whose domain identity differs.

The front has one registry item per service: `<sid>` -> `<bucket>|<region>` in
`flp_fos_buckets`. The write-only `flp_fos_credentials` dictionary holds
`<sid>.ak` and `<sid>.sk`; `flp_front_auth` holds the app-level `cdn_key`.
Bootstrap seeds the CDN key; provisioning synchronizes it for existing installs.
No key values belong in inventory or evidence.

Provisioning adds the selected region's backend and request condition to the
existing front, using a config-only draft when needed. The condition reads the
front's internally generated `x-flp-fos-region`; customer headers cannot select
it. The app backend condition excludes `/fos/`. No per-service CDN or Compute
package deployment is part of provisioning.

`/fos/<sid>/<key>` permits authenticated GET/HEAD only. It rejects missing keys
and unknown services before cache lookup, strips application credentials, signs
the storage request, and uses 20 MiB segmented caching. Listings and query-bearing
reads bypass cache. The cache identity includes the resolved bucket and region
so re-provisioning cannot reuse objects from an old bucket.

The v3 front's comma-separated query allowlist drops legitimate S3 parameters:
`querystring.filter_except` requires `querystring.filtersep()` between names.
This front uses the documented separator and tests that `prefix` and `max-keys`
survive while credentials and unexpected parameters do not. This is a D5 front
adaptation, not a change to the pinned logging generators.

These are local implementation and test results, not live provisioning evidence.
Deployment, activation, provision, teardown and re-provision all require fresh
approval. Raw logging ends Goal 2; ingestion and Parquet remain Goal 3.

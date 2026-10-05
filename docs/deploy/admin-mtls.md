# Certificate-authenticated admin access

## Access contract

Standard Docker Compose and Kubernetes deployments can expose a separate HTTPS
admin gateway requiring a certificate issued by a deployment-specific admin
client CA. This is an alternative to SSH and Kubernetes dashboard port
forwarding, not a replacement for host management or Kubernetes authorization.
The public analyst endpoint retains its existing login, tenancy and masking
rules.

The client private key stays on the administrator's machine. Do not distribute
one shared administrator certificate with the application. Enrollment and browser
certificate import are explicit operator actions, not unattended installation
steps. Server TLS certificates and administrator client certificates are
different credentials.

## Trust boundary

The gateway verifies the client certificate before proxying either frontend or
API traffic. It replaces client-supplied trust headers and injects a separate
server-to-server `X-Admin-Gateway-Token`. The gateway, frontend SSR runtime and
backend share `ADMIN_GATEWAY_SECRET`; it never belongs in browser code,
bootstrap JSON, logs or the repository.

The backend verifies this credential in constant time. Frontend SSR forwards it
only after independently verifying it. A hostname, private source IP, VPN
membership, or asserted certificate subject is not proof of admin access.
The public proxy strips this header rather than accepting it from visitors.

`ADMIN_GATEWAY_REQUIRED=1` disables implicit local/network administrator
classification for that deployment. Without a valid gateway credential,
requests retain the analyst restrictions, including requests arriving on
loopback or an old administrator-trusted subnet. The cheap liveness endpoint
remains available; deep health requires administrator authorization.
Legacy deployments without this setting retain their existing access behavior.

Kubernetes network policy must restrict direct application access to authorized
proxies and required workload traffic. The client CA signing key is not mounted
into application or gateway pods. The client-CA trust bundle, server TLS key and
gateway credential are separate externally provisioned secrets.

Client certificates can be presented automatically by browsers. Authenticated
gateway requests therefore reject cross-site fetch metadata and cross-origin
browser mutations. CLI clients using their own client certificate may omit
`Origin`; browser mutation origins must match the administrator HTTPS host.

## Portability and rollout

The gateway uses ordinary Kubernetes workloads and secrets. Exposure through an
ingress controller is a separate adapter: the standard Kubernetes Ingress API
does not define client-certificate authentication or TLS passthrough.
Controller-specific configuration must be documented explicitly and must not
terminate client authentication without preserving the enforced gateway boundary.

Use private network exposure where available. Network restrictions supplement
certificate authentication; they do not replace it.

Migrate both deployments before removing dashboard tunnel supervisors, proxy
failover and tunnel-specific verification code. Retain SSH used to deploy or
recover a VM. Do not remove existing test coverage just because its previous
transport has been superseded.

## Acceptance criteria

- Missing, wrong-CA, expired and no-longer-trusted client certificates cannot
  access the administrator gateway.
- An enrolled administrator can load bootstrap, analytics, admin APIs and SSE
  through the same authenticated HTTPS connection.
- A visitor cannot gain administrator access with forged gateway, forwarded-IP,
  analyst-marker or hostname headers, including through frontend SSR.
- Public anonymous bootstrap and authenticated analyst responses retain their
  existing restricted shapes and permissions.
- The gateway secret never appears in browser responses or diagnostics.
- Both deployment modes render real data and survive a sustained verification
  interval without dashboard port-forwarding.
- Certificate enrollment, expiry, rotation and removal of trust are documented
  with generic Kubernetes and Compose instructions and no private deployment
  identifiers.

## Installation and certificate lifecycle

The operator how-to is
[the admin gateway README](../../deploy/chart/admin-gateway/README.md). It
documents generic Compose and Kubernetes setup, explicit browser/client
installation, server trust, renewal and emergency removal of client-CA trust.

### Implemented interfaces

- `scripts/admin_certificates.py` provides `init`, `server`, `client-request`,
  `sign-client`, `export-client`, `bundle` and `check`. Output is owner-only and
  must live outside this repository. Signing keys are encrypted; client keys
  are generated on the client machine. Neither enrollment nor export modifies
  the OS/browser trust store.
- The gateway-only bundle contains `server.crt`, `server.key`, `client-ca.crt`
  and `ADMIN_GATEWAY_SECRET`. It excludes client keys and both CA signing keys.
- Standard Compose adds `docker-compose.admin-mtls.yml`; host-network
  production additionally adds `docker-compose.admin-mtls.prod.yml`, last in
  the overlay order. Both use the companion chart's `files/Caddyfile`.
  Default external TLS port is **9443**, separate from the legacy listener.
- `deploy/chart/admin-gateway` installs a separate companion release with
  `host`, `existingSecret`, `frontendUpstream` and `backendUpstream`. Its
  default Service is ClusterIP, **TCP 8443 only**. Credentials are referenced
  from an existing Secret, never supplied through Helm values.
- Companion `emptyDir` volumes are capped by `emptyDirSizeLimit` (`64Mi`)
  for bounded-storage admission policies. `image` (adapter `--image`,
  required outside `provision`) must be built from
  `deploy/admin-gateway/Dockerfile` and published to a registry the cluster
  admits, digest-pinned. Upstream Caddy cannot exec under `drop: ALL`: its
  `cap_net_bind_service` file capability exceeds the empty bounding set. The
  Dockerfile rewrites the binary because the Jenkins-published image retained
  the base layer's capability despite a guarded `setcap -r` step. Verify each
  published digest with the commands in the companion chart README. The
  harness reads the image from operator-local `ELEVATION_ADMIN_GATEWAY_IMAGE`
  (no default). Compose builds the same Dockerfile as `fla-admin-gateway`.
- The shared gateway Caddyfile site is hostless (`https://:8443`) so every TLS
  handshake, including no-SNI handshakes from IP origins such as
  `127.0.0.1` or a LoadBalancer IP, gets the single `require_and_verify` client
  policy. The Host check lives in a matcher: any other Host is aborted. See
  "Client installation is explicit" in the companion chart README.
- The experimental application chart accepts `adminGateway.enabled`,
  `adminGateway.existingSecret`, `adminGateway.host` and the required
  purpose-built `adminGateway.image`. Enabling it wires both
  application containers and inserts a credential-stripping public proxy.
- `scripts/deploy_admin_gateway.py --dry-run` validates an existing F5 NGINX
  VirtualServer adapter, gateway bundle and app Deployment patches without
  writing to the cluster. An approved rollout preserves existing application
  Helm ownership and patches only required environments and public headers.
- `bash scripts/validate_admin_gateway.sh` runs real TLS rejection/success
  tests, certificate lifecycle checks, chart renders and verifier identity
  isolation. It fails clearly when a required validation tool is absent;
  it does not install tools.

Set the exact gateway hostname in `LOCAL_HOSTS`. Gateway forwarding removes
`X-Proxied-By-Caddy`, `X-Remote-Analyst` and browser admin credentials before
injecting the server credential. Public proxies strip both admin credentials
and retain the analyst marker. Credential fields are filtered from Caddy
runtime/error logs as well as access logs.
Both backend **and frontend SSR** must receive `ADMIN_GATEWAY_REQUIRED=1`
alongside the same server credential when required mode is activated.
Frontend required mode must not infer administrator trust from an internal,
unmarked loopback Host; certificate-authenticated gateway evidence is required.

### Private exposure and migration status

A ClusterIP alone is not a workstation-reachable endpoint. A private
LoadBalancer or a controller-supported TLS passthrough adapter requires
verified provider/network configuration. The companion chart does not guess
provider annotations or expose an HTTP bypass.

Current inspection found namespace Service creation is permitted and the
existing platform internal ingress LoadBalancer is directly reachable from
the workstation. The GKE adapter uses its observed internal-only annotations,
never an external LoadBalancer by default. Generic Kubernetes retains
ClusterIP unless a reviewed provider-specific internal adapter is selected.
Direct certificate-authenticated gateways have been provisioned and exercised
on local Compose, single-VM Standard and Kubernetes High-Scale deployments.
Each deployment uses its own client CA and named administrator identity.
Native administrator browser/data access and public anonymous SSR restrictions
have been verified. The deployment harness no longer starts dashboard SSH or
Kubernetes forwards; host-management SSH and Kubernetes deployment/log access
remain. This does not certify every analytics panel: High-Scale RUM trend
population remains under investigation in the [RUM contract](../pages/rum.md).

Allocation is staged through `scripts/deploy_admin_gateway.py`:

1. `--phase provision --internal-gke --dry-run`: render a Service-only IP
   reservation, no Secret/pod/application changes. An approved canonical
   invocation without `--dry-run` allocates a private IPv4 address.
2. Issue a server leaf for that address using
   `admin_certificates.py server --host PRIVATE_IP`; the CLI emits an IP SAN.
   Build the gateway-only bundle in an operator-owned directory.
3. `--phase gateway --internal-gke --dry-run`: validate the bundle and
   gateway render. Approved gateway deployment leaves application credentials
   and `ADMIN_GATEWAY_REQUIRED` unchanged while direct TLS is verified and
   canonical callers are migrated.
4. `--phase activate --internal-gke --callers-ready`: only after readiness,
   install app credentials and required mode. Use `--dry-run` first.

For the canonical deployment, these stages must be invoked through
`scripts/dev/deploy_test_all.sh --admin-gateway-phase provision|gateway|activate`,
which retains the pushed-HEAD guard; direct script invocations above describe
portable operator tooling/dry-runs, not authorization to bypass the canonical
harness. The harness also applies standard Compose mTLS overlays through
`LOCAL_STANDARD_ADMIN_MTLS=1` or `GCE_ADMIN_MTLS=1`. GCE credentials and bundle
paths remain VM-local; no secret is placed in SSH command arguments.

The ignored canonical harness opt-in uses the **direct private HTTPS URL on
8443**, with no TLS kubectl forwarding. It requires an explicit
`ADMIN_GATEWAY_CALLERS_READY=1` acknowledgement before activation.
Audit, scale/metrics and freshness clients now use
`scripts.lib.admin_tls.admin_urlopen(request_or_url, timeout=...)`, with
per-environment origins from `ADMIN_GATEWAY_ENDPOINTS` (operator-local JSON
objects containing `origin`, `cert`, `key` and `ca`). Only exact configured
HTTPS origins receive client certificates; authenticated cross-origin
redirects fail. The canonical opt-in disables legacy Elevation forwarding and
uses direct URLs throughout checks and reports. The report-server bootstrap
proxy uses the same exact-origin certificate helper in required mode.
Do not remove legacy dashboard SSH
or Kubernetes forwarding scaffolding until both deployments satisfy the full
acceptance criteria; retain host-management SSH afterward.

### Expiry and revocation

For Node automation with a private server CA, set
`NODE_EXTRA_CA_CERTS="$ADMIN_GATEWAY_SERVER_CA"` before Node starts.
This extends Node's server-verification trust with the public CA PEM; it does
not import a client certificate and may be necessary even when macOS Keychain
already trusts the server CA. Chromium/browser trust and client PKCS#12
installation remain separate explicit steps. Production verifier TLS checks
must stay enabled.

Default leaf validity is **90 days**. Use `check --days 30` to detect impending
expiry and renew into a new output directory, with a fresh client key/CSR.
Restart/recreate gateways after replacing their server identity or trust
bundle; restart both app containers when rotating the shared server credential.

This implementation does not perform per-client CRL checks. Deleting a key,
exported identity or certificate file is not revocation. To revoke a
compromised identity, issue retained clients under a new dedicated admin
client CA, replace the gateway trust bundle, remove the old root and restart
all affected gateways to terminate existing TLS sessions. Keeping both roots
trusted keeps the compromised identity valid until its old root is removed.

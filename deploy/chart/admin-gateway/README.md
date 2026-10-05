# Optional admin mTLS gateway

This companion chart and the standard Compose overlays run **the same Caddy
configuration** (`files/Caddyfile`). The gateway authenticates a client
certificate against a dedicated, operator-owned **admin client CA**, then
overwrites `X-Admin-Gateway-Token` with a server-only credential. It routes
`/api/*`, `/js/*` and `/rum-beacon` to the backend; other requests go to the
frontend. Both app containers receive the same `ADMIN_GATEWAY_SECRET`.

The secret must contain at least 32 random characters. It is not a login token:
never give it to a browser, bootstrap response, client-certificate bundle, URL,
Helm values or log. Do not enable shell tracing while handling it.
`ADMIN_GATEWAY_REQUIRED=1` makes unauthenticated app traffic analyst traffic
even on loopback; cheap health probes remain available. Set that flag only on
deployments configured for mTLS. Include the exact gateway hostname in
`LOCAL_HOSTS`, never `*`. The public ingress must strip `X-Admin-Gateway-Token`
and `X-Admin-Token`, preserving the legitimate `X-Remote-Analyst` marker.

This is optional. The experimental application chart is **not** the current
Elevation application chart. Installing the companion does not take ownership
of an existing application's Deployments or change its ingest topology.

## Certificate lifecycle (operator machine)

Requirements: Python 3, OpenSSL with `req -addext` support, and Caddy >=2.8 for
local runtime validation. Helm/Kubernetes are only needed for Kubernetes
deployment. Scripts report missing tools; they do not install dependencies.

Keep all generated material **outside the repository**. Use an owner-only
password file (0600, containing a nonempty strong password) for the encrypted
CA signing keys. Set these operator-local paths:

```sh
export ADMIN_PKI="$HOME/.local/share/log-analytics-admin-pki"
export CA_PASSWORD_FILE="$HOME/.local/share/log-analytics-ca-password"
export ADMIN_CLIENT="$HOME/.local/share/log-analytics-admin-client"
export ADMIN_SERVER="$HOME/.local/share/log-analytics-admin-server"
export ADMIN_BUNDLE="$HOME/.local/share/log-analytics-admin-gateway"
export ADMIN_HOST="admin.example.com"
```

Create the password file using a password manager or an interactive editor,
with its parent directory 0700 and the file 0600. Do not put its value in shell
arguments/history. Then:

```sh
python3 scripts/admin_certificates.py init \
  --out "$ADMIN_PKI" --password-file "$CA_PASSWORD_FILE"
python3 scripts/admin_certificates.py server \
  --state "$ADMIN_PKI" --out "$ADMIN_SERVER" \
  --password-file "$CA_PASSWORD_FILE" --host "$ADMIN_HOST" --days 90
```

`init` creates **separate client and server CAs**, encrypted signing keys and a
random gateway credential. The server CA is for optional private server trust;
a publicly trusted server certificate can replace the generated server leaf.
The gateway's client trust is always `client-ca.crt`, never a platform-wide CA.
`server --host` also accepts IPv4 and emits an **IP SAN**, enabling a direct
private address URL without requiring DNS.

On each client machine generate its private key and CSR locally:

```sh
python3 scripts/admin_certificates.py client-request \
  --out "$ADMIN_CLIENT" --name operator
```

Transfer **only** `operator.csr` to the operator's CA machine. Sign it there:

```sh
python3 scripts/admin_certificates.py sign-client \
  --state "$ADMIN_PKI" --out "$ADMIN_CLIENT" \
  --password-file "$CA_PASSWORD_FILE" \
  --csr "$ADMIN_CLIENT/operator.csr" --days 90
```

Return `client.crt` and the public CA certificate to the client. On a separate
CA machine use a different owner-only output directory for `sign-client`;
neither the CLI nor the signer needs the client key. Signing ignores requested
CSR extensions and issues only `clientAuth`, never a CA or a server identity.
`client-request --password-file PATH` optionally encrypts the client key.
Headless verification uses an owner-only unencrypted key; keep it local.

Create the **gateway-only** bundle:

```sh
python3 scripts/admin_certificates.py bundle \
  --state "$ADMIN_PKI" --out "$ADMIN_BUNDLE" \
  --server-cert "$ADMIN_SERVER/server.crt" \
  --server-key "$ADMIN_SERVER/server.key"
```

It contains exactly `server.crt`, `server.key`, `client-ca.crt` and
`ADMIN_GATEWAY_SECRET`. Neither CA signing key nor any client private key is
copied. All output directories are 0700 and files 0600; commands refuse
overwrites. Certificate/key output inside this checkout is rejected.

## Client installation is explicit

The CLI does **not** modify a keychain, browser trust store, `/etc/hosts` or
system trust. Export an encrypted PKCS#12 file when a browser needs one:

```sh
python3 scripts/admin_certificates.py export-client \
  --out "$ADMIN_CLIENT" --key "$ADMIN_CLIENT/operator.key" \
  --cert "$ADMIN_CLIENT/client.crt" --ca "$ADMIN_PKI/client-ca.crt" \
  --password-file "$CA_PASSWORD_FILE"
```

If the client key is encrypted, also supply `--key-password-file`.
On macOS, open Keychain Access and import `client.p12` into the login keychain,
entering the export password interactively. Import/trust **only the server
CA's public certificate** for private server TLS, or use a publicly trusted
server leaf instead. Firefox may use its own certificate store. Follow the
browser/OS prompts deliberately; do not globally trust the client CA as a
general-purpose server issuer. The client PKCS#12 contains the client identity,
not the gateway credential or CA signing key.

Use a hostname resolving to the private listener. A TLS-preserving localhost
port-forward can use `admin.localhost` (issue the server leaf for that exact
name); no wildcard trust or disabled TLS verification is needed.

An IP literal also works (`--host 10.20.30.40` issues an IP SAN). Clients send
**no SNI** for an IP host, which is why the shared `files/Caddyfile` site is
hostless (`https://:8443`):

- A host-addressed site (`https://<host>:8443`) attaches client auth only to a
  TLS policy matching that SNI. A no-SNI handshake fell through to a catch-all
  policy without client auth: a TLS `internal error` when no certificate
  matched the listener's local address (containers, LoadBalancer IPs), or a
  handshake **without** a client certificate when one did.
- The hostless site produces a single TLS policy with no SNI matcher, so
  `require_and_verify` applies to every handshake whatever SNI is sent.
- `strict_sni_host insecure_off` is then safe and required: with one policy
  there is no SNI-selected site to cross, and strict mode would answer 421 to
  every IP-origin request (empty SNI never equals the Host header).
- The Host check moves into the `@admin_host` matcher: requests for any other
  Host are aborted without proxying or credential injection.

`tests/test_admin_certificates.py` pins this with real TLS against an IP origin
(no SNI, IP host, foreign SNI) and asserts the adapted config keeps exactly one
`require_and_verify` policy. Do not add a second site or `default_sni` here.

## Standard Compose

Set these in the **operator-local `.env`**, not in tracked configuration:

- `ADMIN_GATEWAY_HOST`: the server certificate's explicit DNS hostname or IP
  literal (IP SAN). Requests with any other Host header are aborted.
- `LOCAL_HOSTS`: existing allowed hosts plus that exact hostname.
- `ADMIN_GATEWAY_SECRET`: the generated value from the operator-owned
  `gateway-secret` file; keep it server-side.
- `ADMIN_GATEWAY_REQUIRED=1`.
- `ADMIN_GATEWAY_CERT_DIR`: the gateway-only bundle path.
- Optional `ADMIN_GATEWAY_LISTEN_IP`: a private, reachable interface address.
  The safe default is `127.0.0.1`; it is not reachable from another machine.
- Optional `ADMIN_GATEWAY_PORT`: external TLS port; default **9443**, distinct
  from the legacy SSH-only Caddy listener on 8443.
- `ADMIN_GATEWAY_UID` / `ADMIN_GATEWAY_GID`: match the owner of the mounted
  certificate files (default 1000:1000). On a laptop use your actual `id -u` /
  `id -g` values in the local `.env`; do not make the private key world-readable.

Local standard topology (base Compose shares the backend's network namespace):

```sh
docker compose -f docker-compose.yml -f docker-compose.admin-mtls.yml config --quiet
docker compose -f docker-compose.yml -f docker-compose.admin-mtls.yml up -d --build
```

Single-host production topology (retain its memory caps, public rate-limit
Caddy image and proxy-trust settings):

```sh
docker compose -f docker-compose.yml -f docker-compose.prod.yml \
  -f docker-compose.admin-mtls.yml -f docker-compose.admin-mtls.prod.yml config --quiet
docker compose -f docker-compose.yml -f docker-compose.prod.yml \
  -f docker-compose.admin-mtls.yml -f docker-compose.admin-mtls.prod.yml up -d --build
```

The new gateway has no HTTP port and uses a read-only certificate bind mount.
Private remote access still needs an independently verified route (VPN/private
network and firewall). Do not expose an unauthenticated frontend/backend socket
or assume a private IP is reachable. Public analyst Caddy remains separate.
Certificate replacement requires recreating the gateway to drop old TLS
sessions and reload trust.

## Portable Kubernetes

Bring an application with frontend/backend Services, and create the gateway
Secret from the gateway-only bundle. Feed the manifest directly through stdin;
do not save it or pass secret values via `--set`:

```sh
kubectl create secret generic admin-gateway -n "$NAMESPACE" \
  --from-file=server.crt="$ADMIN_BUNDLE/server.crt" \
  --from-file=server.key="$ADMIN_BUNDLE/server.key" \
  --from-file=client-ca.crt="$ADMIN_BUNDLE/client-ca.crt" \
  --from-file=ADMIN_GATEWAY_SECRET="$ADMIN_BUNDLE/ADMIN_GATEWAY_SECRET" \
  --dry-run=client -o json |
  kubectl apply --server-side --field-manager=admin-gateway -f -
helm upgrade --install admin-gateway deploy/chart/admin-gateway \
  --namespace "$NAMESPACE" --set-string host="$ADMIN_HOST" \
  --set-string existingSecret=admin-gateway \
  --set-string frontendUpstream=frontend-svc:3000 \
  --set-string backendUpstream=backend-svc:8000 --wait
```

### Admission policies and images

Both `emptyDir` volumes (Caddy data and the rendered runtime config) set
`sizeLimit` from `emptyDirSizeLimit` (default `64Mi`). Rendering fails if it
is empty, so clusters that require bounded ephemeral storage admit the pod.

`image` is required and must be built from
[`deploy/admin-gateway/Dockerfile`](../../admin-gateway/Dockerfile). The
upstream `caddy` image cannot run here: its binary carries the
`cap_net_bind_service` file capability, and the kernel refuses to exec a
binary whose file capabilities exceed the bounding set, so the pod
crash-loops with `exec /usr/bin/caddy: operation not permitted` once the
container drops `ALL` capabilities. The gateway listens on 8443 and needs no
capability; the Dockerfile rewrites the binary as a new file (dropping the
capability xattr) and runs as UID 1000.

The Jenkins-published image retained the base layer's capability despite a
guarded `setcap -r` step and a passing build-time check. Replacing the binary
avoids depending on capability-only changes being captured by the builder.
Verify the published digest before deploying it:

```bash
IMG=REGISTRY/PATH/fla-admin-gateway@sha256:DIGEST
docker run --rm --entrypoint /bin/sh --user 0:0 "$IMG" -c 'getcap /usr/bin/caddy'   # prints nothing
docker run --rm --cap-drop ALL --security-opt no-new-privileges --user 1000:1000 "$IMG" caddy version
```

Build and publish it to a registry the cluster admits, then pass a tag or,
preferably, a digest, e.g.
`--set-string image=REGISTRY/PATH/fla-admin-gateway:TAG@sha256:DIGEST`
(adapter: `--image`). Do not request a policy exception or add capabilities
to work around either admission rule.

Default exposure is **ClusterIP, TCP 8443 only**:

```sh
kubectl port-forward --address 127.0.0.1 -n "$NAMESPACE" svc/admin-gateway 19443:8443
```

This preserves end-to-end TLS and client authentication at Caddy. The
application must receive the credential through `secretKeyRef` in **both**
containers; **both** receive `ADMIN_GATEWAY_REQUIRED=1`, and backend receives the
exact `LOCAL_HOSTS` entry. Secret changes require restarting the app containers
because their environment is read at startup.

For the experimental app chart, enable `adminGateway.enabled=true`,
`adminGateway.existingSecret=admin-gateway`, and `adminGateway.host=ADMIN_HOST`.
This also inserts a controller-independent **public header-sanitizing Caddy**
between the public Ingress and app Services. Install the companion chart
separately with those app Services as upstreams. No ingress controller's
potentially disabled header-snippet feature is required.

For other applications, enforce header stripping at every public ingress and
configure equivalent app environment entries. A provider-neutral Ingress
cannot express TLS passthrough; **do not** point a TLS-terminating Ingress at
this mTLS service. A provider-supported TCP/TLS passthrough controller, or an
operator-configured **internal** `service.type=LoadBalancer` with the
reviewed `service.internalProvider=gke-internal`, can expose it privately.
LoadBalancer mode without this explicit internal provider fails rendering.
It pins `cloud.google.com/load-balancer-type: Internal` and
`networking.gke.io/internal-load-balancer-allow-global-access: "true"`, observed
on the platform's reachable internal ingress Service. These are **GKE-specific**,
not generic Kubernetes defaults. Other providers retain ClusterIP until an
equivalent internal-only adapter is reviewed; no external LB is supported.

### Native GKE internal allocation before certificate issuance

The adapter has three explicit phases so IP allocation does not enable
required mode prematurely:

```sh
# Render a Service-only reservation. No certs or app environment changes.
python3 scripts/deploy_admin_gateway.py --phase provision --internal-gke \
  --namespace "$NAMESPACE" --release admin-gateway --dry-run
```

After approval, the canonical workflow can run the same command without
`--dry-run`. It creates a **new companion release only**, with
`gateway.enabled=false`; no pods select the reserved Service. It waits up to
300 seconds for a private IPv4 address and prints it. Existing releases cannot
be downgraded to this reservation phase. Optionally supply an operator-reserved
`--load-balancer-ip`.

Set `ADMIN_HOST` to the returned private IPv4 address and issue the server
certificate using `server --host "$ADMIN_HOST"` into a fresh output directory.
Build the bundle, then render/deploy `--phase gateway --internal-gke` with the
application arguments shown below. This installs the gateway and sanitizes
public ingress, **without patching backend/frontend or enabling required
mode**. Verify direct server TLS/client-mTLS at
`https://ADMIN_HOST:8443`, browser server trust, and migrate every canonical
admin caller to that direct URL.

Only then use `--phase activate --internal-gke --callers-ready` with the same
application arguments. This installs app credentials, exact Host allowlisting
and required mode. The separate flag is an operator assertion, not an automatic
claim that diagnostics or browser validation have succeeded.

The chart includes a gateway NetworkPolicy: inbound TCP 8443 only, DNS egress,
and same-namespace upstream egress on 3000/8000. Restrict
`networkPolicy.ingressFrom` and `upstreamTo` to your actual topology.
NetworkPolicies require an enforcing CNI. The policy does not replace app
ingress isolation: ensure app Services cannot be reached through alternate
public paths. Kubernetes administrators who can read the Secret or exec into
app/gateway pods remain inside the administrative trust boundary.

## Existing F5 NGINX / Elevation adapter

`scripts/deploy_admin_gateway.py` supports an existing F5 NGINX VirtualServer
and app Deployments without taking over their Helm ownership. It validates
certificate expiry, server hostname/key match, bundle permissions, app
container names and supported public routes before changing anything:

```sh
python3 scripts/deploy_admin_gateway.py \
  --phase gateway --internal-gke \
  --namespace "$NAMESPACE" --host "$ADMIN_HOST" --cert-dir "$ADMIN_BUNDLE" \
  --frontend-upstream frontend-svc:3000 --backend-upstream backend-svc:8000 \
  --frontend-deployment frontend --backend-deployment backend \
  --public-virtualserver frontend-vs --dry-run
```

After reviewing the dry run, omit `--dry-run` **only through your approved
canonical deployment harness**. The adapter:

1. Applies only the gateway Secret.
2. Converts direct `pass` routes to equivalent `proxy` routes, setting empty
   `X-Admin-Gateway-Token`/`X-Admin-Token` upstream headers (NGINX suppresses
   empty header values), and sets `X-Proxied-By-Caddy=true`. Other headers,
   including the analyst marker, are preserved.
3. Installs/upgrades only the companion release.
4. In `activate` phase only, patches existing app Deployment environments,
   preserving unrelated env settings, then waits for their rollouts.
   Gateway-only phase leaves application authorization unchanged.

`--image` sets the companion image (validated reference with a mandatory tag
or digest; required for `gateway` and `activate`). Helm and rollout failures report the last 20 lines
of output with registered credentials, PEM blocks and long token-like runs
redacted; commands that send Secret or patch payloads never echo output.

Unsupported split/redirect routes fail before modification. `LOCAL_HOSTS`
backed by `valueFrom` must be updated explicitly at its source instead.
Existing Helm releases still own their Deployments: a future upgrade of the
application release must preserve these env entries and public route changes.

The local ignored canonical harness has an opt-in
`ELEVATION_ADMIN_MTLS=1` branch with externally configured gateway bundle,
hostname and client paths. It preserves the established image/Deployment patch
rollout and uses a **direct internal LoadBalancer URL**, never a gateway
port-forward. It refuses activation unless `ADMIN_GATEWAY_CALLERS_READY=1`.
It requires the operator's
server CA trust setup before Playwright, and the frontend verifier gives the
client certificate **only to the exact configured admin HTTPS origin**.
It never sets `ignoreHTTPSErrors`.

The canonical harness exposes gateway-only staged operations **after its
pushed-HEAD check**:

```sh
scripts/dev/deploy_test_all.sh --admin-gateway-phase provision
# Issue the server IP SAN/bundle explicitly on the operator machine.
scripts/dev/deploy_test_all.sh --admin-gateway-phase gateway
# After direct TLS/browser/server trust and all diagnostic callers are ready:
ADMIN_GATEWAY_CALLERS_READY=1 scripts/dev/deploy_test_all.sh --admin-gateway-phase activate
```

Gateway and activation phases consume operator-local
`ELEVATION_ADMIN_GATEWAY_HOST` and `ELEVATION_ADMIN_GATEWAY_CERT_DIR`.
Phase-only operations do not claim complete canonical live acceptance; follow
with the full approved canonical rollout/verification. Do not run ad-hoc
cluster mutations in place of this harness for the canonical deployment.

Canonical standard deployment opts in through
`LOCAL_STANDARD_ADMIN_MTLS=1` (local Compose overlays) or `GCE_ADMIN_MTLS=1`
(base + production + both mTLS overlays on the VM). Local values live in the
local environment/`.env`; GCE values and bundle paths must already be in the
**VM-local `.env`**, never transmitted as SSH secret arguments. Configure
the respective `local-standard`/`remote-standard` diagnostic identity before
activation. Host-management `restart.sh`/SSH remain intact. Remote production
gateway binding must be an actual private reachable interface rather than
the safe loopback default; firewall routing is a separately verified operator
step. No broad GCE ingress rule is created by this implementation.

For Node-based diagnostics using a private server CA, configure
`NODE_EXTRA_CA_CERTS="$ADMIN_GATEWAY_SERVER_CA"` **before starting Node**.
Node does not necessarily inherit macOS login-keychain server trust, and this
variable adds only the public server CA PEM to Node's server-verification
roots; it does not install a client identity. Playwright Chromium still needs
its own browser/OS server trust plus `clientCertificates` for the admin
origin. Importing the client PKCS#12 is separate from trusting the server CA.
Never replace either trust step with production `ignoreHTTPSErrors`, `-k`,
`NODE_TLS_REJECT_UNAUTHORIZED=0`, or a global Chromium certificate bypass.

**Current inspection evidence:** this operator can create Services and patch
the public F5 VirtualServer, but cannot list/create TransportServers or list
IngressClasses. Direct application ClusterIP access is unreachable; however
the documented internal ingress DNS resolves to an existing private GKE
LoadBalancer and TCP 443 is reachable from the workstation. Its observed
internal-only annotations are used by the explicit GKE adapter. A new
gateway's allocation and direct TLS reachability still require approved live
provisioning and verification; neither has been performed.

**Migration boundary:** canonical report-server/bootstrap and scale/metrics
callers must use the direct origin. Audit, scale/checkpoint, freshness and
canonical shell health/readiness now use the shared exact-origin stdlib client;
the report-server owner must integrate its bootstrap proxy separately.
The opt-in is not a claim of completed live verification. Do not remove the
legacy dashboard tunnel until admin SSR/API, public analyst access, and
canonical diagnostics pass. Keep host-management SSH.

### Per-environment diagnostic identities

`scripts.lib.admin_tls` reads `ADMIN_GATEWAY_ENDPOINTS`, an operator-local JSON
mapping from `local-standard`, `local-high-scale`, `remote-standard` or
`remote-high-scale` to objects with exactly `origin`, `cert`, `key`, and `ca`.
Each `origin` is an explicit HTTPS origin; paths point at operator-owned files.
Configure different identities for Compose/GCE and Kubernetes as needed.
Short aliases `local-std`, `local-hs`, `remote-std`, and `remote-hs` are accepted.
Never commit this JSON or keys.

The single-target `ADMIN_GATEWAY_CLIENT_ORIGIN`, `ADMIN_GATEWAY_CLIENT_CERT`,
`ADMIN_GATEWAY_CLIENT_KEY` and `ADMIN_GATEWAY_SERVER_CA` variables remain a
convenience for one target; `ADMIN_GATEWAY_ENVIRONMENT` selects its environment
(default `remote-high-scale`). Conflicting identities for one origin fail.

Only an exact configured HTTPS origin receives its client certificate.
Cross-origin redirects from authenticated requests are rejected; edge traffic
and CDN probes continue using ordinary TLS without an admin identity.
Invalid/incomplete configuration fails before canonical diagnostics begin.
The client uses `ssl.create_default_context`, loads the explicit server CA,
and never disables hostname/server verification or sends the server gateway
credential. Owner-only client files are required.

Invoke standalone diagnostics from the repository root as modules:

```sh
python3 -m scripts.dev.audit_environments --env remote-high-scale
python3 -m scripts.dev.scale_harness --help
python3 -m scripts.dev.measure_e2e_freshness --env remote-hs
```

Existing direct-path invocations such as
`uv run python scripts/dev/scale_harness.py --help` remain supported; each
standalone dev script explicitly bootstraps the repository root for imports.

The canonical harness now passes direct configured origins throughout commit
checks, readiness, browser verification, report links, seeding checkpoints and
health audits. Native Elevation mode does not launch the old forwarding/healer
branch. Disabled legacy modes remain available pending live replacement
verification; GCE SSH has not been removed.

## Expiry, renewal and emergency revocation

Check leaves before their last 30 days:

```sh
python3 scripts/admin_certificates.py check \
  --cert "$ADMIN_CLIENT/client.crt" --ca "$ADMIN_PKI/client-ca.crt" --days 30
python3 scripts/admin_certificates.py check --purpose sslserver \
  --cert "$ADMIN_SERVER/server.crt" --ca "$ADMIN_PKI/server-ca.crt" --days 30
```

The default leaf lifetime is 90 days; CA lifetime is 3650 days. Renewal uses a
**new output directory**, fresh key/CSR and freshly signed leaf. Distribute
client identities privately. Replace the gateway server bundle/Secret and
restart/recreate Caddy after server renewal.

Caddy's file trust pool does **not** implement per-client CRL revocation here.
Deleting a client key or renewing its certificate is **not revocation**.
For a compromised client, create a new dedicated client CA with `init` in a
new state directory, reissue all retained clients, replace `client-ca.crt` on
every affected gateway, and restart gateways to drop existing TLS sessions.
For an emergency, remove the old root **without overlap**, accepting a bounded
admin-access outage while retained clients are reissued. An overlap trusts
the compromised certificate until the old CA is removed.

CA rotation need not rotate the server CA/leaf or shared gateway credential:
bundle the new client CA with the existing server identity, and retain the
existing credential using your secret manager. If the server credential may
also be compromised, rotate it in gateway/backend/frontend together and restart
all three; never put the replacement value in rollout command arguments.
Keep CA backups encrypted and outside the cluster.

## Validation

```sh
bash scripts/validate_admin_gateway.sh
```

The explicit validation fails on missing tools and runs real Caddy/OpenSSL TLS
missing/untrusted/expired/wrong-purpose/success cases, credential overwrite and
analyst-marker removal on SSR and API paths, Helm renders, and verifier
client-identity isolation. Normal pytest runs skip only Caddy runtime cases
when Caddy is absent; they do not prove the mTLS runtime contract. No test PKI
or Caddy process persists after validation.

After an approved deployment, verify a valid client can reach admin bootstrap
and SSR, no/invalid/expired certificates fail at TLS, and the public analyst
entry never accepts admin credentials. Do not use `curl -k`, disabled browser
TLS verification or a gateway secret header as a substitute for client mTLS.

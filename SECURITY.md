# Security Policy

## Supported Versions

Security fixes are applied to the latest 3.x release. Older releases are not actively patched.

## Reporting a Vulnerability

If you believe you have found a security vulnerability in this project, please report it privately via [Fastly's security issue reporting process](https://www.fastly.com/security/report-security-issue). Do not open a public GitHub issue.

Please include, where possible:

- A description of the issue and its potential impact
- Steps to reproduce, or a proof-of-concept
- The affected version or commit SHA
- Any suggested mitigation

The project team will acknowledge receipt and work with you on coordinated disclosure.

## Security Advisories

Security advisories for this project are published via GitHub Security Advisories on this repository. These are distinct from [Fastly Security Advisories](https://www.fastly.com/security-advisories), which cover Fastly's products and services.

## Live Dashboard Sharing — Trust Model

The **Share Dashboard** feature exposes a read-only view of the admin's running instance to invited analysts. It runs in direct mode against a public HTTPS endpoint you control — there is no third-party relay in the path. Two connectivity options differ only in how analysts reach your host:

### Mode 1 — Your own hostname (HTTPS)

- Analysts connect directly to your machine; no third-party relay sees their traffic.
- Requires: a publicly resolvable hostname pointing at your host, a TLS certificate (Caddy / Cloudflare / Let's Encrypt all work), and the forward port reachable from the internet.
- Best for long-running or repeat collaboration.

### Mode 2 — Your public IP address (HTTPS)

- No DNS required. Useful when you already have a public IP but no domain.
- Still requires HTTPS: analyst session cookies are issued with `secure=true` and modern browsers refuse them over plain HTTP. Public CAs do not issue certificates for raw IPs, so you must either deploy a self-signed certificate (analysts will see a browser warning and must trust it manually) or terminate TLS in a reverse proxy that has a cert for *some* hostname.
- Most home/office networks sit behind NAT, CGNAT, or ISP-blocked inbound ports — verify the listen port is actually reachable from the public internet before relying on this mode.

### Defenses common to both modes

Regardless of which mode you pick, the share feature enforces:

- **Per-analyst passcodes** — minted by the admin, hashed at rest with argon2id, never transmitted in cleartext.
- **Rate limiting** — 5 failed logins in 60s triggers a 5-minute lockout per source IP.
- **Optional IP allowlist** — per-invite CIDR / IP restriction enforced at every request.
- **Optional expiry** — per-invite TTL.
- **Read-only enforcement** — admin endpoints are blocked from analyst sessions in middleware.
- **Instant revoke** — single-invite revoke or **Sever All Access** (immediate eviction of every analyst + tunnel teardown).
- **Audit log** — every login, heartbeat, query, and admin action is appended to the share audit log for forensic review.

For implementation details, see the **Live Dashboard Sharing** section of [CONTRIBUTOR_GUIDE.md](CONTRIBUTOR_GUIDE.md#live-dashboard-sharing).

## Admin Access — Trust Model

The admin surface (everything under `/api/*` that isn't an analyst-share or public endpoint) is gated by a **network trust boundary**: the backend treats a request as admin-privileged only when its peer address is loopback or inside a private network you list in `LOCAL_ADMIN_CIDRS` (private, non-link-local, no wider than a /16), and the request does not look like proxied analyst traffic. In a correct deployment the only things on loopback are the co-located reverse proxy and an operator who has already authenticated to the host (for example over SSH). Public ingress terminates at the reverse proxy, which never forwards the admin branch from the outside.

### Optional stronger boundary — certificate-authenticated admin gateway

For deployments where "from loopback" or "from a private subnet" is too weak a signal, an optional mTLS admin gateway admits administrators only with a client certificate from a deployment-specific CA. The gateway injects a server-to-server `X-Admin-Gateway-Token` (backed by `ADMIN_GATEWAY_SECRET`, at least 32 characters) that the backend and frontend SSR each verify in constant time. With `ADMIN_GATEWAY_REQUIRED=1`, implicit loopback and `LOCAL_ADMIN_CIDRS` admin classification is disabled: requests without a valid gateway credential keep analyst restrictions. Setup, certificate lifecycle and the revocation caveats are in [docs/deploy/admin-mtls.md](docs/deploy/admin-mtls.md).

**What it does NOT defend against:** anyone with shell or environment access to the host can read the gateway secret, so the host's own access controls (SSH, cloud IAM, file permissions on the env file) remain the primary admin authentication. Keep the admin port off the public internet regardless.

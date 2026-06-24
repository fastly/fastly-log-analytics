# Security Policy

## Supported Versions

Security fixes are applied to the latest released version on the `main` branch. Older releases are not actively patched.

## Reporting a Vulnerability

If you believe you have found a security vulnerability in this project, please report it privately via [Fastly's security issue reporting process](https://www.fastly.com/security/report-security-issue). Do not open a public GitHub issue.

Please include, where possible:

- A description of the issue and its potential impact
- Steps to reproduce, or a proof-of-concept
- The affected version or commit SHA
- Any suggested mitigation

The project team will acknowledge receipt and work with you on coordinated disclosure.

## Security Advisories

Security advisories for this project are published via [GitHub Security Advisories](../../security/advisories) on this repository. These are distinct from [Fastly Security Advisories](https://www.fastly.com/security-advisories), which cover Fastly's products and services.

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

> **Removed in v2.0:** an earlier SSH-reverse-tunnel option routed analyst traffic through a third-party relay ([localhost.run](https://localhost.run/)) that terminated TLS at the relay operator — who could then see request/response contents in cleartext. It was removed so analyst traffic stays strictly between the analyst and your host. If you previously relied on it for sharing data subject to GDPR / HIPAA / PCI-DSS, note that direct mode no longer exposes traffic to any relay operator.

### Defenses common to both modes

Regardless of which mode you pick, the share feature enforces:

- **Per-analyst passcodes** — minted by the admin, hashed at rest with argon2id (legacy scrypt hashes still verify and are transparently rehashed to argon2id on next login), never transmitted in cleartext.
- **Rate limiting** — 5 failed logins in 60s triggers a 5-minute lockout per source IP.
- **Optional IP allowlist** — per-invite CIDR / IP restriction enforced at every request.
- **Optional expiry** — per-invite TTL.
- **Read-only enforcement** — admin endpoints are blocked from analyst sessions in middleware.
- **Instant revoke** — single-invite revoke or **Sever All Access** (immediate eviction of every analyst + tunnel teardown).
- **Audit log** — every login, heartbeat, query, and admin action is appended to the share audit log for forensic review.

For implementation details, see the **Live Dashboard Sharing** section of [AGENTS.md](AGENTS.md).

## Admin Access — Trust Model

The admin surface (everything under `/api/*` that isn't an analyst-share or public endpoint) is gated by a **loopback trust boundary**: the backend only treats a request as admin-privileged when it arrives from `127.0.0.1`. In a correct deployment the only things on loopback are the co-located reverse proxy and an operator who has already authenticated to the host (e.g. over SSH). Public ingress terminates at the reverse proxy, which never forwards the admin branch from the outside.

### Optional second factor — `ADMIN_SHARED_SECRET`

For defense-in-depth on top of the loopback boundary, set the `ADMIN_SHARED_SECRET` environment variable in the backend's environment. When it is set:

- Every admin-branch request must carry an `X-Admin-Token` header matching the secret, or the backend returns `401` (`admin_token_required` / `admin_token_invalid`).
- The admin SPA fetches the token from its bootstrap response and injects it on every admin API call automatically; on a `401` it clears the cached token and re-fetches, so an operator sees at most a momentary error after a rotation.
- Two paths are exempt because they cannot present the token: `/api/health` (the container healthcheck) and `/api/bootstrap` (where the SPA *picks up* the token).
- When the variable is unset the gate is a **no-op** — the deployed-but-not-activated state is safe, and the public repo ships only the env passthrough wiring, never a value.

**What it defends against:** a future misconfiguration that weakens the loopback boundary — a proxy/Caddyfile change that accidentally forwards public traffic to the admin port, a container that binds a non-loopback interface, or a topology change (sidecars, multi-tenant orchestration) that makes "from loopback" a weaker signal. In those cases the secret is the remaining barrier.

**What it does NOT defend against:** anyone who already has shell or environment access to the host can read the secret (`printenv`, the env file), so this is **defense-in-depth, not primary authentication**. The host's own access controls — SSH, cloud IAM, file permissions on the env file — remain the primary admin auth. Treat `ADMIN_SHARED_SECRET` as a second factor, not a replacement for keeping the admin port off the public internet.

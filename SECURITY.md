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

The **Share Dashboard** feature exposes a read-only view of the admin's running instance to invited analysts. It offers three modes that differ in *who else can see analyst traffic*. Pick the one that matches your data sensitivity:

### Mode 1 — SSH reverse tunnel via [localhost.run](https://localhost.run/) (default)

- Easiest to set up: no DNS, no TLS, no port forwarding.
- Analyst traffic terminates TLS at the `localhost.run` operator before being relayed back to your machine over the SSH tunnel. **The relay operator can see request/response contents in cleartext at their edge.**
- Best for short-lived demos, ephemeral collaboration on non-sensitive data, or when you do not control infrastructure.
- **Review with your legal/compliance team before sharing logs subject to GDPR, HIPAA, PCI-DSS, or contractual data-handling restrictions.**

### Mode 2 — Your own hostname (HTTPS)

- No third-party relay. Analysts connect directly to your machine.
- Requires: a publicly resolvable hostname pointing at your host, a TLS certificate (Caddy / Cloudflare / Let's Encrypt all work), and the forward port reachable from the internet.
- Best for long-running or repeat collaboration, or when relay-operator visibility is unacceptable.

### Mode 3 — Your public IP address (HTTPS)

- No third-party relay and no DNS required. Useful when you already have a public IP but no domain.
- Still requires HTTPS: analyst session cookies are issued with `secure=true` and modern browsers refuse them over plain HTTP. Public CAs do not issue certificates for raw IPs, so you must either deploy a self-signed certificate (analysts will see a browser warning and must trust it manually) or terminate TLS in a reverse proxy that has a cert for *some* hostname.
- Most home/office networks sit behind NAT, CGNAT, or ISP-blocked inbound ports — verify the listen port is actually reachable from the public internet before relying on this mode.

### Defenses common to all three modes

Regardless of which mode you pick, the share feature enforces:

- **Per-analyst passcodes** — minted by the admin, scrypt-hashed at rest, never transmitted in cleartext.
- **Rate limiting** — 5 failed logins in 60s triggers a 5-minute lockout per source IP.
- **Optional IP allowlist** — per-invite CIDR / IP restriction enforced at every request.
- **Optional expiry** — per-invite TTL.
- **Read-only enforcement** — admin endpoints are blocked from analyst sessions in middleware.
- **Instant revoke** — single-invite revoke or **Sever All Access** (immediate eviction of every analyst + tunnel teardown).
- **Audit log** — every login, heartbeat, query, and admin action is appended to the share audit log for forensic review.

For implementation details, see the **Live Dashboard Sharing** section of [AGENTS.md](AGENTS.md).

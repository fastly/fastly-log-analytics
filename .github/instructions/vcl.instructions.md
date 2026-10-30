---
applyTo: "**/*.vcl"
---

# Fastly VCL

- Most VCL in this repo is generated from Python under `backend/provision/` (log format, scoring snippets, CDN-fronting VCL); change the generator, not a rendered copy.
- Classic Fastly VCL has **no loop or user-function constructs** — `falco lint` rejects `for`/`while` as syntax errors. To handle repeated/per-item data at the edge, capture it as one opaque field and parse it downstream (in Python), not in VCL.
- Validate every change with **`falco lint`** (and `falco test` for unit assertions) before considering it done.
- Verify backend/request field names before using them — they are easy to misremember and a wrong name causes a **silent 503**, not an error. Common ones: `override_host`, `ssl_cert_hostname`, `ssl_sni_hostname`, `use_ssl`.
- Reference the Fastly docs for API and VCL specifics rather than relying on memory.

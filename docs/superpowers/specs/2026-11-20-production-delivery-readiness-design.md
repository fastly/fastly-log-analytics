# Production Delivery Readiness Design

## Goal

Harden production delivery without changing application behavior:

- force TLS at the Cloud edge
- prevent public shell cache fragmentation
- stop treating unhashed static assets as immutable forever
- enforce a compressed dashboard bundle budget
- make High-Scale edge verification explicit instead of assuming its configured
  URL is the product

## Cloud TLS and shell cache

Cloud's Fastly front VCL will redirect every HTTP request to the same host and
request URL over HTTPS with a permanent redirect. The redirect happens before
static-file or API routing, so API, HTML, RSC, worker, and public asset requests
all use the same TLS policy. HSTS remains in responses but is not treated as a
substitute for the redirect.

Public static-export pages may share a small edge cache. Their cache key must
ignore query parameters that are irrelevant to the immutable shell:

- `/`
- `/usage`
- `/how-it-works`
- `/share-login` and acknowledgement
- their `.html`, `.txt`, trailing-slash, and `__next` variants

The VCL will keep the full request URL for authenticated/private API and
application routes. It must not strip query strings from `/api/*`, dynamic
analytics requests, file manifests, or saved-view URLs. Normalize only paths
known to be static shells.

Expected result:

- `http://<host>/usage?x=random` redirects to `https://<host>/usage?x=random`
- public static shell variants converge on the same cache key
- authenticated/private routes remain private and preserve their complete URL
- no query, filter, or service information changes a private response

## Static asset caching

Only content-hashed Next assets under `/_next/static/` receive:

```text
Cache-Control: public, max-age=31536000, immutable
```

Unhashed static files need a bounded, revalidating policy:

```text
Cache-Control: public, max-age=86400, must-revalidate
```

This applies to:

- `/geo/*`
- `/duckdb/*`
- `/maplibre-gl-*`

`Content-Type`, Brotli/gzip negotiation, edge caching, surrogate-key purging,
cookie stripping, and `Vary: Accept-Encoding` remain in place. The browser must
be allowed to refresh an unchanged URL after a deploy when that file's contents
change.

## Bundle budget

Add a build-time Node script that inspects the static Next export used by Cloud:

1. parse each exported app route's HTML
2. collect distinct `/_next/static/*.js` and `/_next/static/*.css` references
3. precompress each referenced file with Brotli quality 11
4. fail when the `/dashboard` initial references exceed:
   - 625 KiB combined Brotli JavaScript
   - 35 KiB combined Brotli CSS
   - 90 KiB for any single JavaScript chunk

The script reports raw and Brotli bytes by route and identifies the largest
chunks. It is deterministic and must not require a server, browser, credentials,
or installed global tooling. The budget runs in the Cloud release/static-export
path before the Rust package embeds the assets.

The thresholds are intentionally above the current release export
(604,214 bytes JS, 30,970 bytes CSS, and 84,203 bytes for the largest chunk) so
they catch meaningful regressions without making current normal chunking fail. The right response to a
future budget failure is code-splitting or dependency removal, not raising the
budget without review.

## High-Scale verification

The production audit is blocked for High-Scale because the configured
`front_url` currently returns an unrelated plain-text service. The implementation
must not guess or fabricate a replacement hostname.

Add a small read-only environment check that:

- expects an FLA HTML document or a known FLA route response at the configured
  `front_url`
- verifies HTTPS
- checks `private, no-cache` for authenticated app HTML
- checks the nonce CSP
- rejects an unrelated content type/body

The environment remains blocked until the operator supplies and verifies the
correct URL. GCE remains untouched.

## Tests

VCL/site tests must cover:

1. HTTP redirects to HTTPS while preserving path and query
2. random query strings share a static shell cache key
3. analytics/API query strings do not converge
4. `/_next/static` remains one-year immutable
5. `/geo`, `/duckdb`, and `/maplibre-gl-*` use bounded revalidation
6. missing hashed chunks get a small plain-text 404 rather than the HTML shell

Build tests must cover:

1. distinct asset deduplication
2. Brotli byte measurement
3. dashboard JS failure above 625 KiB
4. dashboard CSS failure above 35 KiB
5. largest-chunk failure above 90 KiB
6. current export passing

Environment validation must fail clearly when `front_url` is non-product rather
than reporting the edge as verified.

## Out of scope

- combining all Next chunks into one file
- changing client analytics to server-side Compute execution
- moving file manifests or Parquet processing into the shell
- changing deployed environments without explicit approval
- GCE changes
- guessing a High-Scale hostname
- replacing the existing strict CSP model

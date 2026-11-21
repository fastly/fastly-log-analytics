# Production Delivery Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden Cloud TLS/cache delivery and add build/readiness gates that prevent static-asset and High-Scale endpoint regressions.

**Architecture:** VCL owns edge redirects and public-shell cache canonicalization; Rust's static asset model owns browser cache policy; a Node script enforces Brotli bundle budgets after Next static export; and the deploy orchestrator rejects a High-Scale front URL that is not an FLA document instead of claiming readiness.

**Tech Stack:** Fastly VCL, `falco`, Rust, Node.js 24, Next.js static export, Python/pytest.

## Global Constraints

- Never cache authenticated API, dashboard, or analytics responses.
- Strip query strings only from explicitly allowlisted public static shells.
- Keep `Cache-Control: public, max-age=31536000, immutable` only for content-hashed `/_next/static/*`.
- Use `public, max-age=86400, must-revalidate` for unhashed `/geo/*`, `/duckdb/*`, and `/maplibre-gl-*`.
- Dashboard budget: 550 KiB Brotli JS, 35 KiB Brotli CSS, 75 KiB largest JS chunk.
- Do not guess a High-Scale hostname, mutate GCE, push, or deploy.

---

### Task 1: Force TLS and canonicalize public shell cache keys

**Files:**
- Modify: `compute/cloud/vcl/front/main.vcl`
- Test: `compute/cloud/vcl/front/tests/front.test.vcl`

**Interfaces:**
- Consumes: `fastly.ff.visits_this_service`, `req.is_ssl`, `req.url`, and the existing public/private route regex.
- Produces: HTTP 301 redirects to HTTPS and one cache key for equivalent public shell URLs while private routes retain their full URL.

- [ ] **Step 1: Add failing VCL tests**

Add at the start of `front.test.vcl`:

```vcl
// @scope: recv
sub http_requests_redirect_to_https {
  testing.inject_variable("req.is_ssl", false);
  set req.url = "/usage?service=fixture";
  set req.http.Host = "flp.example";
  testing.call_subroutine("vcl_recv");
  assert.error(801);
}

// @scope: recv
sub public_shell_queries_share_a_cache_key {
  testing.inject_variable("req.is_ssl", true);
  set req.url = "/usage?campaign=one";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.url, "/usage");
  assert.state(lookup);
}

// @scope: recv
sub private_api_queries_keep_the_complete_cache_key {
  testing.inject_variable("req.is_ssl", true);
  set req.url = "/api/files?from=a&to=b&service=fixture";
  testing.call_subroutine("vcl_recv");
  assert.equal(req.url, "/api/files?from=a&to=b&service=fixture");
  assert.state(pass);
}
```

- [ ] **Step 2: Run focused VCL tests**

```bash
falco test compute/cloud/vcl/front/main.vcl \
  --tests compute/cloud/vcl/front/tests/front.test.vcl
```

Expected: FAIL because HTTP is not redirected and public shell queries remain in `req.url`.

- [ ] **Step 3: Implement the HTTPS redirect**

Near the beginning of `vcl_recv`, before request classification, add:

```vcl
if (!req.is_ssl && req.url.path !~ "^/_tick$|^/_flush$") {
  error 801 "Force TLS";
}
```

Use the existing Fastly redirect error handler if one exists; otherwise add:

```vcl
if (obj.status == 801) {
  set obj.http.Location = "https://" req.http.Host req.url;
  set obj.status = 301;
  set obj.response = "Moved Permanently";
  synthetic {""};
  return(deliver);
}
```

Preserve the query string in `Location`.

- [ ] **Step 4: Canonicalize only public static-export shells**

In `vcl_recv`, before the method/cache classification:

```vcl
if (req.url.path ~ "^/(usage|how-it-works|share-login)(/acknowledge)?(\.html|\.txt)?/?$" || req.url.path == "/") {
  set req.url = req.url.path;
}
```

Do not apply this to `/api/`, `/dashboard`, `/streaming`, or any authenticated path. Add a test with `/usage?campaign=one` and `/api/files?...`.

- [ ] **Step 5: Run VCL tests**

```bash
falco test compute/cloud/vcl/front/main.vcl \
  --tests compute/cloud/vcl/front/tests/front.test.vcl
```

Expected: all VCL tests pass.

- [ ] **Step 6: Commit**

```bash
git add compute/cloud/vcl/front/main.vcl compute/cloud/vcl/front/tests/front.test.vcl
git commit -m "Harden Cloud TLS and shell cache keys"
```

### Task 2: Correct static asset cache policy

**Files:**
- Modify: `compute/cloud/src/site.rs:8-27`
- Modify: `compute/cloud/vcl/front/main.vcl`
- Test: `compute/cloud/src/site.rs:92-151`
- Test: `compute/cloud/vcl/front/tests/front.test.vcl:20-145`

**Interfaces:**
- Consumes: static asset path.
- Produces: long immutable caching only for `/_next/static/`; bounded revalidation for unhashed data/worker files.

- [ ] **Step 1: Add failing Rust and VCL tests**

Add to `site.rs` tests:

```rust
for path in [
    "/geo/world.topo.json",
    "/duckdb/1.33.1-dev57.0/duckdb-eh.wasm",
    "/maplibre-gl-worker.mjs",
] {
    let asset = Asset {
        path,
        content_type: "application/octet-stream",
        identity: Some(b"asset"),
        gzip: None,
        br: None,
    };
    assert_eq!(
        asset.cache_control(),
        "public, max-age=86400, must-revalidate"
    );
}
```

In VCL tests, replace `duckdb_has_year_ttl_and_surrogate_key` with:

```vcl
sub duckdb_has_day_ttl_and_revalidates {
  set req.url = "/duckdb/1.33.1-dev57.0/duckdb-eh.wasm";
  set beresp.status = 200;
  testing.call_subroutine("vcl_fetch");
  assert.equal(beresp.ttl, 86400s);
  assert.equal(
    beresp.http.Cache-Control,
    "public, max-age=86400, must-revalidate"
  );
  assert.equal(beresp.http.Surrogate-Key, "flp-static");
}
```

- [ ] **Step 2: Run focused tests to verify failure**

```bash
cargo test --manifest-path compute/cloud/Cargo.toml site::tests::
falco test compute/cloud/vcl/front/main.vcl \
  --tests compute/cloud/vcl/front/tests/front.test.vcl
```

Expected: FAIL because unhashed files still receive the one-year policy.

- [ ] **Step 3: Split hashed and bounded static helpers**

In `site.rs`, replace `is_static_bundle` semantics with:

```rust
fn is_hashed_bundle(&self) -> bool {
    self.path.starts_with("/_next/static/")
}

fn is_bounded_static(&self) -> bool {
    self.path.starts_with("/duckdb/")
        || self.path.starts_with("/geo/")
        || self.path.starts_with("/maplibre-gl-")
}

pub fn cache_control(&self) -> &'static str {
    if self.is_hashed_bundle() {
        "public, max-age=31536000, immutable"
    } else if self.is_bounded_static() {
        "public, max-age=86400, must-revalidate"
    } else {
        "public, max-age=60"
    }
}
```

Keep both categories under surrogate key `flp-static`.

- [ ] **Step 4: Match the policy in VCL**

Split the `vcl_fetch` branch:

```vcl
if (req.url.path ~ "^/_next/static/" && beresp.status == 200) {
  set beresp.ttl = 31536000s;
  set beresp.http.Cache-Control = "public, max-age=31536000, immutable";
}
if ((req.url.path ~ "^/(duckdb|geo)/" || req.url.path ~ "^/maplibre-gl-") && beresp.status == 200) {
  set beresp.ttl = 86400s;
  set beresp.http.Cache-Control = "public, max-age=86400, must-revalidate";
}
```

Both branches unset `Set-Cookie`, enable caching, assign `flp-static`, and deliver.

- [ ] **Step 5: Run focused tests**

```bash
cargo test --manifest-path compute/cloud/Cargo.toml site::tests::
falco test compute/cloud/vcl/front/main.vcl \
  --tests compute/cloud/vcl/front/tests/front.test.vcl
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add compute/cloud/src/site.rs compute/cloud/vcl/front/main.vcl compute/cloud/vcl/front/tests/front.test.vcl
git commit -m "Bound unhashed static asset caching"
```

### Task 3: Add compressed static-export budgets

**Files:**
- Create: `frontend/scripts/check-static-budget.mjs`
- Test: `frontend/scripts/check-static-budget.test.mjs`
- Modify: `scripts/release_compute.py:143-147, 260-270`

**Interfaces:**
- Consumes: `frontend/out/dashboard.html`, `frontend/out`, and optional synthetic fixture roots.
- Produces: a JSON summary and a non-zero exit above 550 KiB JS, 35 KiB CSS, or 75 KiB per JS chunk.

- [ ] **Step 1: Create failing unit tests for measurement**

Create `frontend/scripts/check-static-budget.test.mjs` with Node's test runner. Build a temporary export containing:

```text
dashboard.html -> /_next/static/shared.js and dashboard.css twice
_next/static/shared.js
_next/static/dashboard.js
_next/static/dashboard.css
```

Assert distinct URLs are counted once and that JSON includes:

```js
{
  route: '/dashboard',
  javascript_files: 2,
  css_files: 1,
  javascript_brotli_bytes: brotliCompressSync(shared + dashboard).byteLength,
  css_brotli_bytes: brotliCompressSync(css).byteLength,
}
```

Add `withBudgets` tests that assert failures for 550 KiB JS, 35 KiB CSS, and a 75 KiB largest chunk.

- [ ] **Step 2: Run and verify failure**

```bash
node --test frontend/scripts/check-static-budget.test.mjs
```

Expected: FAIL because `check-static-budget.mjs` does not exist.

- [ ] **Step 3: Implement deterministic budget checks**

Export:

```js
export const BUDGETS = {
  dashboardJs: 550 * 1024,
  dashboardCss: 35 * 1024,
  largestJs: 75 * 1024,
}
export function measureRoute(outDir, route = '/dashboard') { ... }
export function assertBudgets(measurement, budgets = BUDGETS) { ... }
```

Use `node:zlib` `brotliCompressSync`, quality 11, and `node:fs`. Resolve `/dashboard` to `out/dashboard.html`, strip query/hash, reject paths escaping `out`, and deduplicate asset paths with `Set`.

The CLI prints one JSON line with raw and Brotli bytes, file counts, and largest chunk. It throws an actionable error listing the exceeded budget and byte delta.

- [ ] **Step 4: Wire the budget into release export**

In `release_compute.py`, after `check_static_assets(web / "out")`:

```python
subprocess.run(
    ["node", "scripts/check-static-budget.mjs", "out/dashboard.html"],
    cwd=web,
    env=environment,
    check=True,
)
```

Run the budget for every `frontend` export, including restored `web_out_ready` output, immediately before Compute packaging.

- [ ] **Step 5: Run unit tests and the current export gate**

```bash
node --test frontend/scripts/check-static-budget.test.mjs
node frontend/scripts/check-static-budget.mjs frontend/out
```

Expected: unit tests pass; the real command reports the current payload and exits 0 if an export is present.

- [ ] **Step 6: Commit**

```bash
git add frontend/scripts/check-static-budget.mjs frontend/scripts/check-static-budget.test.mjs scripts/release_compute.py
git commit -m "Enforce Cloud dashboard asset budget"
```

### Task 4: Reject unrelated High-Scale front URLs

**Files:**
- Modify: `/Users/drew.michael/Projects/fastly-log-analytics-deploy-testing/fdt/envs/fla_elevation.py`
- Test: `/Users/drew.michael/Projects/fastly-log-analytics-deploy-testing/tests/test_env_fla_elevation.py`
- Test helper: `/Users/drew.michael/Projects/fastly-log-analytics-deploy-testing/tests/env_support.py`

**Interfaces:**
- Consumes: `front_url` HTTP response status, content type, and body prefix.
- Produces: a read-only readiness failure when the URL is not an FLA HTML document.

- [ ] **Step 1: Add a failing readiness test**

Extend `FrontServer` to accept response headers. Add:

```python
def test_ready_rejects_non_fla_front_url(tmp_path, registry, monkeypatch):
    server = FrontServer({
        "/": (200, {"Content-Type": "text/html", "Cache-Control": "private, no-cache"}, b"<html><title>Unrelated service</title></html>"),
        "/health": (200, {"Content-Type": "application/json"}, b'{"ok":true}'),
    })
    try:
        env = make_env(tmp_path, server.url.rstrip("/"))
        result = run_env_graph(env, "ready")
        assert result.success is False
        assert "did not return an FLA document" in result.steps["ready"].error
    finally:
        server.close()
```

Add a positive test with the FLA title or known bootstrap marker and
`private, no-cache` headers.

- [ ] **Step 2: Run the focused test and verify failure**

```bash
perl -e 'alarm 120; exec @ARGV' uv run pytest -q -x \
  tests/test_env_fla_elevation.py -k 'front_url or ready'
```

Expected: FAIL because readiness currently checks status only.

- [ ] **Step 3: Add a bounded product-document probe**

Refactor `_ready_status` to return status, error, content type, and an initial
bounded body sample. For `front_url` only, require:

```text
status == 200
content-type contains text/html
body contains "Fastly Log Analytics" or a bootstrap marker owned by FLA
cache-control contains private and no-cache
```

Do not parse arbitrary scripts or follow a successful redirect as product
proof. Preserve readiness support for `/health` JSON and other `ready_urls`.

- [ ] **Step 4: Run focused deploy-orchestrator tests**

```bash
perl -e 'alarm 120; exec @ARGV' nice -n 10 uv run pytest -q -x \
  tests/test_env_fla_elevation.py -k 'front_url or ready'
```

Expected: both unrelated-document rejection and valid FLA readiness pass.

- [ ] **Step 5: Commit in deploy-testing repository**

```bash
git add fdt/envs/fla_elevation.py tests/test_env_fla_elevation.py tests/env_support.py
git commit -m "Reject unrelated High-Scale front URLs"
```

### Task 5: Integrated verification

**Files:**
- No implementation files.

- [ ] **Step 1: Run Cloud edge and asset suites**

```bash
cargo test --manifest-path compute/cloud/Cargo.toml site::tests::
falco test compute/cloud/vcl/front/main.vcl \
  --tests compute/cloud/vcl/front/tests/front.test.vcl
```

Expected: all pass.

- [ ] **Step 2: Run static budget tests and full frontend format gate**

```bash
node --test frontend/scripts/check-static-budget.test.mjs
npx tsc --noEmit
npx eslint frontend/scripts/check-static-budget.mjs \
  frontend/scripts/check-static-budget.test.mjs
```

Run from the repository root, using `--prefix frontend` where required by local Node tooling.

- [ ] **Step 3: Run deploy-test readiness tests**

```bash
perl -e 'alarm 180; exec @ARGV' nice -n 10 uv run pytest -q -x \
  tests/test_env_fla_elevation.py -k 'front_url or ready'
```

Expected: all pass with no live network call.

- [ ] **Step 4: Check final worktrees and production todos**

```bash
git status --short
```

Leave `production-high-scale-url` blocked until a correct hostname is supplied.

## Plan Self-Review

- Spec coverage: Task 1 covers TLS/cache keys, Task 2 covers cache policy, Task 3 covers bundle budgets, Task 4 covers the High-Scale verification blocker, and Task 5 reruns all gates.
- Placeholder scan: every task contains concrete files, test code, commands, and expected behavior.
- Type consistency: all cache header strings, byte thresholds, paths, and test assertions match the approved spec.

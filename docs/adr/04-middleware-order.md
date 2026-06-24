# ADR-04 — Middleware Order

**Status:** Accepted (Phase 0)
**Decided by:** v2.0 cleanup planning
**Supersedes:** the paragraph-long prose comments in [backend/main.py:434-501](../../backend/main.py#L434-L501)

## Context

Middleware order is correctness-critical and has bitten us multiple times. The current state:

- `backend/main.py:434-501` carries a paragraph-long comment block documenting a 2026-06-09 audit decision about ordering
- No test catches a reorder
- The same audit had to be reconstructed each time someone touched the file
- Several middleware pieces have implicit ordering constraints that aren't visible at the call site

## Decision

The middleware order is declared as a tuple of expected entries and **asserted at boot**. The boot assertion crashes the process if the order diverges, before serving any traffic. A snapshot test in pytest mirrors the boot assertion.

### Declared order (outermost → innermost)

```
Compress                # outermost — must wrap response body before it ships
TelemetryResponseBody   # JSON-body backstop for debug panel (render-only)
RemoteAccessGate        # X-Proxied-By-Caddy + remote-IP gate; populates analyst_session
TelemetryDecorator      # span-builder (@app.middleware("http") → surfaces as BaseHTTPMiddleware); sits INSIDE RemoteAccess
CORS                    # innermost middleware; closest to FastAPI routing
─────────────────
FastAPI routes
```

Rationale per layer (one-liner each):

- **Compress outermost** — must see the final response body to compute encoding; reordering breaks `Content-Encoding`.
- **TelemetryResponseBody between Compress and RemoteAccess** — needs the raw JSON body before compression to back the debug panel.
- **RemoteAccessGate above the telemetry decorator** — admin-only routes must be rejected on origin before CORS pre-flight reveals their existence, AND it resolves `analyst_session` so the decorator's attribution sees it (audit finding 003). This inverts the decorator-vs-gate position from the original draft.
- **TelemetryDecorator inside RemoteAccess** — opens the per-request root span after `analyst_session` is populated, so attribution is correct; surfaces in `app.user_middleware` as `BaseHTTPMiddleware` (FastAPI's wrapper for `@app.middleware("http")`).
- **CORS innermost** — closest to the route allows route-specific overrides without re-entering outer layers.

## Implementation

```python
# backend/main.py (Phase 3)
MIDDLEWARE_ORDER = (
    "CompressMiddleware",
    "TelemetryResponseBodyMiddleware",
    "RemoteAccessMiddleware",
    "BaseHTTPMiddleware",  # the @app.middleware("http") telemetry-decorator span builder
    "CORSMiddleware",
)

def assert_middleware_order(app: FastAPI) -> None:
    actual = tuple(m.cls.__name__ for m in app.user_middleware)
    if actual != MIDDLEWARE_ORDER:
        raise RuntimeError(
            f"Middleware order violation (ADR-04). "
            f"expected={MIDDLEWARE_ORDER}, actual={actual}"
        )

assert_middleware_order(app)
```

The pytest snapshot (`tests/test_trust_topology.py`) imports the app and compares `app.user_middleware` against the same tuple.

## Trust topology — extended invariants

Phase 3.4 adds snapshot tests for the full trust chain, not just the FastAPI middleware:

- **`Caddyfile`** must contain the `@from_fastly` remote-IP matcher, the `X-Forwarded-For = {Fastly-Client-IP}` header rewrite, and the `/share-login` rate limit.
- **`docker-compose.prod.yml`** backend service must run with `--host 127.0.0.1`, `--proxy-headers`, `--forwarded-allow-ips=127.0.0.1`, and a memory cap.

These three together (Caddy → compose → FastAPI middleware) form the trust topology. Any one of them silently changing is a regression.

## Consequences

- The paragraph-long comments in `main.py` collapse to one-line `# INVARIANT: <X> (see ADR-04)` markers.
- A reorder that compiles is no longer enough to ship — boot will refuse.
- The existing `test_proxy_headers_regression.py` test (which already guards XFF spoof) stays; it's load-bearing for the same trust topology and predates this ADR.

## Out of scope

- ASGI lifespan hooks (lifecycle is its own concern, not middleware)
- Per-route middleware overrides

# Active Runtime Monkeypatches

This file tracks runtime monkeypatches applied to third-party libraries in the **Fastly Log Analytics** codebase, why each is required, and when it can be removed.

---

## 1. `fastapi.routing._IncludedRouter.path` (`backend/main.py`)

- **Location:** [`backend/main.py`](backend/main.py#L56-L68)
- **Target:** `fastapi.routing._IncludedRouter` (FastAPI `>= 0.135`)
- **Interacting library:** `opentelemetry-instrumentation-fastapi` (`0.61b0`)

### Patch

```python
try:
    import fastapi.routing

    if hasattr(fastapi.routing, "_IncludedRouter"):
        setattr(fastapi.routing._IncludedRouter, "path", property(lambda self: ""))
except Exception:
    pass
```

### Why it is needed

In newer FastAPI releases, `APIRouter.include_router(subrouter)` appends a lightweight `fastapi.routing._IncludedRouter` sentinel to `router.routes` (and therefore `app.routes`) in addition to copying the expanded child routes. `_IncludedRouter` inherits from `starlette.routing.BaseRoute` and stores `self.prefix`, not `self.path`:

- On normal matched requests (`Match.FULL`), `_IncludedRouter.matches(scope)` returns `(Match.NONE, {})`, and OpenTelemetry's `_get_route_details` stops at the concrete `APIRoute` that matched.
- On **`405 Method Not Allowed`** responses (or any request where a route returns `Match.PARTIAL`), no route returns `Match.FULL`, so `opentelemetry.instrumentation.fastapi._get_route_details` iterates through the remaining routes in `app.routes` and evaluates:
  ```python
  match, _ = route.matches(scope)
  if match == Match.PARTIAL:
      return getattr(route, "path")
  ```
- When a sub-router included via `include_router` contains a route matching the request path with a different HTTP method, `_IncludedRouter.matches(scope)` returns `(Match.PARTIAL, {})`. `getattr(route, "path")` then raises `AttributeError: '_IncludedRouter' object has no attribute 'path'`, turning a normal `405 Method Not Allowed` response into an unhandled `500 Internal Server Error` inside `OpenTelemetryMiddleware`.

Because every sub-router in this repository (`backend/routers/admin/__init__.py`, `backend/routers/services/__init__.py`, `backend/routers/share_admin/__init__.py`, `backend/routers/rum/__init__.py`, `backend/routers/session_scoring/__init__.py`) is included with the default `prefix=""`, defining `path` as `property(lambda self: "")` on `_IncludedRouter` prevents the `AttributeError` without altering route matching or span route templates.

### Removal condition

Remove this monkeypatch and `MONKEYPATCHES.md` once `opentelemetry-instrumentation-fastapi` guards `getattr(route, "path", None)` (or ignores `_IncludedRouter` sentinels) in `_get_route_details`.

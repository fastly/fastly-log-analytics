# Monkeypatches

## Active monkeypatches

### `fastapi.routing._IncludedRouter.path` (`backend/main.py:56-63`)

- **Patch site:** [`backend/main.py`](backend/main.py) (lines 56–63)
- **What it changes:** Defines a read-only `path = property(lambda self: "")` on `fastapi.routing._IncludedRouter` if that internal FastAPI helper class is present.
- **Why upstream cannot fix it immediately:** `opentelemetry-instrumentation-fastapi` walks `app.routes` and inspects `route.path` on every route entry; newer FastAPI versions can transiently include `_IncludedRouter` objects that do not define `.path`, raising `AttributeError` during instrumentation startup.
- **Criteria for removal:** Remove once `opentelemetry-instrumentation-fastapi` guards `getattr(route, "path", "")` across all supported FastAPI versions.

## How to add an entry

If you must patch a third-party class or function at import time:

1. Add the patch with an inline comment block explaining which incident or
   symptom justified it.
2. Add an entry here with the patch site, what it changes, why upstream cannot
   fix it, and the criteria for removing it.
3. Cross-link from the patch site to this file.

When you remove a monkeypatch, delete its entry here rather than leaving a
tombstone.

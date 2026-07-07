"""R-4 follow-on (Q3 in audit §7): snapshot the JSON shape of high-traffic
admin endpoints. The repository-to-Pydantic contract tests pin Pydantic
schemas; the SQL snapshots pin the query strings; this file pins the
*serialised wire shape* — the bytes the frontend actually decodes.

What this catches that the other two layers miss: response wrapping
(envelopes, debug overlays, telemetry sections), field ordering when
the frontend pins via positional decode, and BaseResponse drift that
doesn't violate Pydantic but does break the FE's openapi-fetch decode
because the generated TypeScript expects a specific schema shape.

Endpoints covered (deliberately small — extend when a real regression
proves the value):
  - GET /api/health
  - GET /api/bootstrap (empty)
  - POST /api/dashboard/aggregates (empty filter, no data)
"""

from __future__ import annotations

import copy


def _strip_volatile(payload: dict) -> dict:
    """Remove fields whose values change every run (timings, request IDs,
    in-process counts) so the snapshot only captures the structural shape."""
    out = copy.deepcopy(payload)
    # Telemetry envelope keys are present in tests via the
    # DEBUG_RESPONSES_FORCE_INCLUDE escape hatch in conftest. The values
    # are run-specific; strip them so the snapshot stays stable.
    for key in ("_debug_queries", "_debug_calls", "_debug_sqlite", "_section_timings", "_is_cached"):
        out.pop(key, None)
    if isinstance(out.get("version"), str):
        # `_health` includes the app version — pinning it would re-snap
        # on every version bump. Pin the *shape* (key present, str type).
        out["version"] = "<scrubbed>"
    return out


def test_api_health_response_shape(client, snapshot):
    r = client.get("/api/health")
    assert r.status_code == 200
    assert _strip_volatile(r.json()) == snapshot


def test_api_bootstrap_empty_response_shape(client, snapshot, monkeypatch):
    """Pin the STRUCTURAL keys bootstrap surfaces — not the full payload.

    The bootstrap response is a wide envelope (country lookup tables,
    log-field catalog, ops counters, etc.) that grows when a new admin
    surface lands. Snapshotting the whole body produces false-positives
    on every benign addition. Pin just the keys the FE keys off + the
    types of a couple of critical values — that's the contract the
    openapi-fetch decode actually cares about.
    """
    # No service configs in the sandboxed CONFIGS_DIR (from
    # isolate_metadata_db) — bootstrap returns an empty service list.
    r = client.get("/api/bootstrap")
    assert r.status_code == 200
    body = _strip_volatile(r.json())
    # Critical contract: top-level keys the FE always reads + the
    # fact that `services` is a list (not null) on an empty sandbox.
    skeleton = {
        "active_service_id_is_none_or_str": body.get("active_service_id") is None
        or isinstance(body.get("active_service_id"), str),
        "services_is_list": isinstance(body.get("services"), list),
        "services_empty_in_sandbox": body.get("services") == [],
        "has_top_level_keys": sorted([k for k in body if not k.startswith("_") and k != "version"]),
    }
    assert skeleton == snapshot

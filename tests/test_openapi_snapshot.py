"""OpenAPI drift snapshot test.

CI regenerates ``frontend/openapi.json`` on every run via
``npm run gen:types`` (see ``frontend/package.json`` and
``scripts/generate_openapi.py``), but nothing fails CI if a backend
change SHIFTS the spec without the frontend having been regenerated
locally. The result is a silently-drifted ``api.generated.ts`` whose
type signatures no longer match the live backend.

This test compares the live ``app.openapi()`` output against the
committed ``frontend/openapi.json`` snapshot and fails loudly with
regeneration instructions when they diverge.

Closes TESTING_PLAN_3 item 12.
"""

from __future__ import annotations

import json
import os

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SNAPSHOT_PATH = os.path.join(REPO_ROOT, "frontend", "openapi.json")


def test_openapi_snapshot_matches_live_app():
    """``frontend/openapi.json`` must match what the running app emits.

    When this fails, regenerate the snapshot locally:

        cd frontend && npm run gen:types

    Then commit ``frontend/openapi.json`` and ``types/api.generated.ts``.

    Failing here is a *signal*, not a flake — every backend change to a
    router, request/response model, or status-code annotation needs the
    frontend types regenerated so the openapi-fetch call sites stay
    type-correct against the real wire format.
    """
    from backend.main import app

    live = app.openapi()

    if not os.path.exists(SNAPSHOT_PATH):
        pytest.fail(f"Snapshot missing: {SNAPSHOT_PATH}\nRun: cd frontend && npm run gen:types")

    with open(SNAPSHOT_PATH) as f:
        snapshot = json.load(f)

    if live == snapshot:
        return

    # Helpful diff: name the top-level keys that drifted so the reader
    # can find the offending router without ploughing through 16k lines.
    live_paths = set(live.get("paths", {}).keys())
    snap_paths = set(snapshot.get("paths", {}).keys())
    added = sorted(live_paths - snap_paths)
    removed = sorted(snap_paths - live_paths)

    live_schemas = set(live.get("components", {}).get("schemas", {}).keys())
    snap_schemas = set(snapshot.get("components", {}).get("schemas", {}).keys())
    added_schemas = sorted(live_schemas - snap_schemas)
    removed_schemas = sorted(snap_schemas - live_schemas)

    # Find paths whose definitions changed (same key, different value)
    shared_paths = live_paths & snap_paths
    changed = sorted(p for p in shared_paths if live["paths"][p] != snapshot["paths"][p])

    msg = ["OpenAPI snapshot is stale. Regenerate with:\n"]
    msg.append("    cd frontend && npm run gen:types\n")
    msg.append("Then commit frontend/openapi.json and frontend/types/api.generated.ts.\n")
    if added:
        msg.append(f"\nRoutes added (in live, not in snapshot): {added}")
    if removed:
        msg.append(f"\nRoutes removed (in snapshot, not in live): {removed}")
    if changed:
        # Cap the list to avoid wall-of-text — first 10 is plenty to
        # point the reader at the offending router.
        shown = changed[:10]
        rest = len(changed) - len(shown)
        msg.append(f"\nRoutes whose schema drifted: {shown}")
        if rest > 0:
            msg.append(f" (+{rest} more)")
    if added_schemas:
        msg.append(f"\nModels added: {added_schemas}")
    if removed_schemas:
        msg.append(f"\nModels removed: {removed_schemas}")
    if not (added or removed or changed or added_schemas or removed_schemas):
        # Top-level diff (version bump, info change, etc.)
        live_keys = set(live.keys())
        snap_keys = set(snapshot.keys())
        msg.append(f"\nTop-level diff: live={sorted(live_keys)} snapshot={sorted(snap_keys)}")
        if "info" in live and "info" in snapshot and live["info"] != snapshot["info"]:
            msg.append(f"\ninfo block changed: live={live['info']} snapshot={snapshot['info']}")

    pytest.fail("\n".join(msg))

"""Regression coverage for cron-settings partial-update semantics.

Observed 2026-08-13: POSTing ``{"cron_sync": {"interval_seconds": 90}}`` returned
"Successfully applied changes" and changed nothing. Two independent reasons:

  1. ``CronSettingsPartial`` didn't model ``interval_seconds`` at all, and
     Pydantic silently drops unknown fields — so the value never reached the
     handler.
  2. Even once modelled, the scheduler prefers ``interval_mins`` over
     ``interval_seconds``. A persisted ``interval_mins`` therefore keeps winning,
     so the caller's value lands in the config and is then ignored.

The merge is additive by design (``model_dump(exclude_unset=True)`` plus an
``is not None`` filter, which protects persisted values from being clobbered
with nulls when Pydantic dumps absent sub-fields). That means null can never
mean "unset", so the handler resolves the conflict by dropping the losing key.
"""

from __future__ import annotations

import json
from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.main import app
from backend.models.services import CronSettingsPartial


def _post(body: dict, stored_cron_sync: dict) -> dict:
    cfg = {
        "service_id": "svc1",
        "name": "Test Service",
        "access_level": "read_write",
        "log_period": 60,
        "provisioning": {"cron_sync": dict(stored_cron_sync), "cron_compact": {"enabled": True}},
    }
    saved: dict = {}

    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.save_config", side_effect=lambda sid, c: saved.update(c)),
        patch("backend.provision._sync_crontab"),
        patch("backend.core.metadata.record_audit"),
    ):
        resp = TestClient(app).post("/api/services/svc1/cron-settings", json=body)
    assert resp.status_code == 200, resp.text
    # SSE stream must have reached "done", not errored.
    assert any('"done"' in line for line in resp.text.splitlines()), resp.text
    return saved["provisioning"]["cron_sync"]


def test_interval_seconds_is_modelled():
    """It must survive request parsing — Pydantic drops unknown fields."""
    assert "interval_seconds" in CronSettingsPartial.model_fields
    parsed = CronSettingsPartial(**json.loads('{"interval_seconds": 90}'))
    assert parsed.interval_seconds == 90


def test_interval_seconds_is_persisted():
    out = _post({"cron_sync": {"interval_seconds": 90}}, {"enabled": True, "interval_seconds": 30})
    assert out["interval_seconds"] == 90


def test_setting_interval_seconds_drops_stale_interval_mins():
    """THE REGRESSION: a persisted interval_mins would otherwise keep winning."""
    out = _post(
        {"cron_sync": {"interval_seconds": 45}},
        {"enabled": True, "interval_seconds": 30, "interval_mins": 2},
    )
    assert out["interval_seconds"] == 45
    assert "interval_mins" not in out, "stale interval_mins still wins in the scheduler"


def test_explicit_interval_mins_still_wins_when_both_sent():
    """Sending both is unambiguous — respect mins, don't second-guess."""
    out = _post(
        {"cron_sync": {"interval_mins": 5, "interval_seconds": 45}},
        {"enabled": True, "interval_seconds": 30},
    )
    assert out["interval_mins"] == 5
    assert out["interval_seconds"] == 45


def test_unrelated_keys_are_preserved():
    """Partial update must not clobber persisted siblings."""
    out = _post(
        {"cron_sync": {"interval_seconds": 90}},
        {"enabled": True, "interval_seconds": 30, "data_retention_days": 120, "delete_after": True},
    )
    assert out["data_retention_days"] == 120
    assert out["delete_after"] is True
    assert out["enabled"] is True


def test_expiry_tuning_fields_round_trip():
    out = _post(
        {"cron_sync": {"keep_snapshot_days": 3, "expire_interval_mins": 30}},
        {"enabled": True, "interval_seconds": 30},
    )
    assert out["keep_snapshot_days"] == 3
    assert out["expire_interval_mins"] == 30

"""Pin the runtime log-line size budget against Fastly's 16 KiB Deliver cap.

Background
----------
Fastly silently truncates emitted log lines past 16 KiB on Deliver services
(64 KiB on Compute). No error surfaces to the customer — corrupt JSON shows
up in DuckDB hours later. The template-size gate at
``provision.fastly_api.FASTLY_LOG_FORMAT_SAFE_MAX`` only protects the static
template string; it cannot see per-request value sizes.

``check_log_line_budget`` exists to give the customer a config-time signal
before that truncation starts costing data. It compares
``estimate_log_line_bytes`` (an average) against a safe-max set at ~60% of
the cap to leave headroom for URL- / UA-heavy requests that routinely
inflate real lines 2-3x past the average.

What this file pins
-------------------
1. A small config produces no warning.
2. A config that lands between safe_max and deliver_max returns
   ``LOG_LINE_APPROACHING_LIMIT`` (severity "warn").
3. A config that exceeds the deliver_max returns ``LOG_LINE_TOO_LARGE``
   (severity "error").
4. Warning payload shape (frontend reads these specific keys).
5. The two router endpoints that consume the estimate also surface the
   warning, so the dashboard can render it.

Why these specific cases: the warn band is the genuinely useful one (the
error band only triggers for pathological configs), so this file leans
on the warn-band assertions to lock in the actionable behavior.
"""

from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient

from backend import config
from backend.core import log_fields as lf
from backend.main import app


def _make_custom_fields(count: int, bytes_each: int) -> list[dict]:
    """Build saved-shape custom field records with a target bytes_estimate.

    Used to drive estimate_log_line_bytes into the warn or error band
    without depending on which built-in groups happen to be enabled.
    """
    return [
        {
            "name": f"cf{i:03d}",
            "label": f"cf{i:03d}",
            "description": "",
            "vcl_log_expression": "req.http.Host",
            "collection_stage": "edge",
            "duckdb_type": "VARCHAR",
            "value_type": "string",
            "bytes_estimate": bytes_each,
            "nullable": True,
            "enabled": True,
            "show_in_dashboard": False,
            "show_in_logs": True,
            "filterable": True,
        }
        for i in range(count)
    ]


def test_constants_pin_the_fastly_caps():
    """If anyone moves the constants we want a loud failure here.

    16 KiB is the Fastly Deliver cap straight from
    https://docs.fastly.com/products/network-services-resource-limits and
    must not drift. The safe-max is a tunable headroom (~60%); the test
    pins the ratio band rather than the exact integer so a deliberate
    tweak (e.g., to 55% or 65%) doesn't require touching this assertion.
    """
    assert lf.FASTLY_LOG_LINE_DELIVER_MAX == 16 * 1024
    # Safe max must leave material headroom for value-size variance but
    # not so much that even fat configs never warn.
    assert 0.40 < lf.FASTLY_LOG_LINE_SAFE_MAX / lf.FASTLY_LOG_LINE_DELIVER_MAX < 0.80


def test_small_config_no_warning():
    """A minimal config has tons of slack — no warning, no false alarm."""
    cfg = {"groups": ["A"], "field_overrides": {}}
    estimate = lf.estimate_log_line_bytes(cfg)
    assert estimate < lf.FASTLY_LOG_LINE_SAFE_MAX
    assert lf.check_log_line_budget(cfg) is None


def test_warn_band_returns_approaching_limit():
    """When estimate crosses safe_max but stays under deliver_max, the warn
    band fires. ~20 fat custom fields land around ~11.7 KB — squarely in
    the warn zone (9830 < x < 16384).
    """
    cfg = {
        "groups": ["A", "B", "C", "D"],
        "field_overrides": {},
        "custom_fields": _make_custom_fields(count=20, bytes_each=500),
    }
    estimate = lf.estimate_log_line_bytes(cfg)
    assert lf.FASTLY_LOG_LINE_SAFE_MAX <= estimate < lf.FASTLY_LOG_LINE_DELIVER_MAX, (
        f"test fixture drifted; estimate {estimate} is no longer in the warn band"
    )

    warning = lf.check_log_line_budget(cfg)
    assert warning is not None
    assert warning["code"] == "LOG_LINE_APPROACHING_LIMIT"
    assert warning["severity"] == "warn"
    assert warning["estimate_bytes"] == estimate
    assert warning["deliver_max_bytes"] == lf.FASTLY_LOG_LINE_DELIVER_MAX
    assert warning["safe_max_bytes"] == lf.FASTLY_LOG_LINE_SAFE_MAX
    # The message must surface the actual size and the cap so the user
    # knows how much to trim. CustomFieldDrawer reads `message` verbatim.
    assert str(estimate) in warning["message"]
    assert "16" in warning["message"]


def test_error_band_returns_too_large():
    """Past the hard cap the warning escalates to error severity. ~20
    1-KiB custom fields land at ~20.7 KB — well past the 16 KiB cap.
    """
    cfg = {
        "groups": [],
        "field_overrides": {},
        "custom_fields": _make_custom_fields(count=20, bytes_each=1024),
    }
    estimate = lf.estimate_log_line_bytes(cfg)
    assert estimate >= lf.FASTLY_LOG_LINE_DELIVER_MAX

    warning = lf.check_log_line_budget(cfg)
    assert warning is not None
    assert warning["code"] == "LOG_LINE_TOO_LARGE"
    assert warning["severity"] == "error"
    assert warning["estimate_bytes"] == estimate
    # The error message must mention "truncate" / "16" so users see the
    # operational consequence (data loss), not just the threshold.
    assert "truncate" in warning["message"].lower()
    assert "16" in warning["message"]


def test_get_log_fields_endpoint_surfaces_warning(tmp_path, monkeypatch):
    """The GET /api/services/{id}/log-fields response must include
    ``line_budget_warning`` so the frontend can render it. Without this
    field on the wire, the helper would be dead code.
    """
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    svc_id = "test_svc_log_budget_get"
    cfg = {
        "service_id": svc_id,
        "log_fields": {
            "schema_version": 2,
            "groups": ["A", "B", "C", "D"],
            "field_overrides": {},
            "custom_fields": _make_custom_fields(count=20, bytes_each=500),
        },
    }
    config.save_config(svc_id, cfg)

    client = TestClient(app)
    resp = client.get(f"/api/services/{svc_id}/log-fields")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "line_budget_warning" in body
    warning = body["line_budget_warning"]
    assert warning is not None
    assert warning["code"] == "LOG_LINE_APPROACHING_LIMIT"


def test_post_log_fields_endpoint_surfaces_warning(tmp_path, monkeypatch):
    """The POST endpoint must also surface the warning so the save flow
    can show the same inline banner as the load flow.

    Falco/state_sync are patched to keep the test offline.
    """
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    svc_id = "test_svc_log_budget_post"
    config.save_config(
        svc_id,
        {
            "service_id": svc_id,
            "log_fields": {"schema_version": 2, "groups": ["A"]},
        },
    )

    payload = {
        "log_fields": {
            "schema_version": 2,
            "groups": ["A", "B", "C", "D"],
            "custom_fields": _make_custom_fields(count=20, bytes_each=500),
        }
    }
    client = TestClient(app)
    # export_admin_state hits state_sync internals; we don't need its
    # side effects for this assertion.
    with patch("backend.state_sync.export_admin_state", return_value=None):
        resp = client.post(f"/api/services/{svc_id}/log-fields", json=payload)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "line_budget_warning" in body
    warning = body["line_budget_warning"]
    assert warning is not None
    assert warning["code"] == "LOG_LINE_APPROACHING_LIMIT"


def test_post_no_change_path_does_not_crash(tmp_path, monkeypatch):
    """When the POST handler short-circuits on a no-op change, it returns
    early WITHOUT the warning key. That branch must not crash and the
    response shape is allowed to omit ``line_budget_warning``.
    Locking this in prevents an over-eager refactor from changing the
    return shape and breaking the no-op contract.
    """
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    svc_id = "test_svc_log_budget_noop"
    initial = {
        "service_id": svc_id,
        "log_fields": {"schema_version": 2, "groups": ["A"]},
    }
    config.save_config(svc_id, initial)
    # Pre-compute hash so the next save is recognized as a no-op.
    cfg = config.load_config(svc_id)
    cfg["log_fields"]["format_hash"] = lf.format_hash(cfg["log_fields"])
    config.save_config(svc_id, cfg)

    client = TestClient(app)
    resp = client.post(
        f"/api/services/{svc_id}/log-fields",
        json={"log_fields": cfg["log_fields"]},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body.get("ok") is True

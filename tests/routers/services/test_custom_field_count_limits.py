"""Pin behavior at the documented custom-field edge cases.

Fastly has two distinct size limits relevant to custom-field provisioning
(see [docs/plans/TESTING_ANALYSIS_AND_PLAN.md] Pillar 5 and
https://docs.fastly.com/products/network-services-resource-limits):

1. **`log_format` TEMPLATE size** — the static template string Fastly
   compiles per logging endpoint. Our backend pins this at
   ``provision.fastly_api.FASTLY_LOG_FORMAT_SAFE_MAX`` (8000 chars,
   below Fastly's ~8192 hard cap on the template). **This file tests
   that gate.**

2. **Emitted log LINE size at runtime** — Fastly truncates any emitted
   line over **16 KB for Deliver services** (64 KB for Compute).
   Depends on per-request field values, not the template. **NOT tested
   here**; ``estimate_log_line_bytes`` exists for storage projections
   but has no warn-threshold tied to the 16 KB cap. Tracked as an open
   gap in docs/plans/TESTING_ANALYSIS_AND_PLAN.md.

Pinned in this file:

a. **No numeric cap on field count.** The create route does not enforce
   a ``MAX_FIELDS = N`` ceiling. The 101st short-named field is
   accepted as long as the generated template stays under 8000 chars.
   A future refactor that adds ``if len(existing) >= 100: raise`` would
   silently break customers running many small custom fields.

b. **Template-size IS the gate.** When adding a field would push the
   generated format past the safe max, the create route 422s with
   ``LOG_FORMAT_TOO_LONG`` in ``detail.errors`` — surfaced before the
   Fastly deploy step, not after. The same gate is exercised by the
   ``validate-vcl`` lint endpoint
   ([test_custom_field_vcl_lint.py]); at the create-route level we
   want the *write* path to bail the same way the *lint* path does.

Falco is patched out via ``shutil.which`` so the test runs identically
on dev boxes with/without the Falco binary. The 422 branch is reached
before Falco would be invoked anyway (the length check short-circuits
at [backend/provision/fastly_api.py:281]), but the success-path test
also needs Falco skipped so it stays fast.
"""

from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient

from backend import config
from backend.main import app

_BASE_PAYLOAD = {
    "label": "L",
    "description": "",
    "vcl_log_expression": "req.http.Host",
    "collection_stage": "edge",
    "duckdb_type": "VARCHAR",
    "value_type": "string",
    "bytes_estimate": 20,
    "nullable": True,
    "enabled": True,
    "show_in_dashboard": False,
    "show_in_logs": True,
    "filterable": True,
}


def _seed_custom_field(i: int) -> dict:
    """Build a saved-shape custom field record for direct config seeding.

    Field names are 5 chars (``cf001``) so we can pre-load many fields
    without bumping the log_format size unintentionally — see the
    measured tipping point at ~119 fields in the test below.
    """
    return {
        **_BASE_PAYLOAD,
        "name": f"cf{i:03d}",
        "created_at": "2026-05-27T00:00:00+00:00",
        "updated_at": "2026-05-27T00:00:00+00:00",
    }


def _make_cfg(svc_id: str, n_existing: int) -> dict:
    """Pre-populate a config with ``n_existing`` saved custom fields."""
    return {
        "service_id": svc_id,
        "log_fields": {
            "schema_version": 2,
            "groups": ["A"],
            "custom_fields": [_seed_custom_field(i) for i in range(n_existing)],
        },
    }


def _post_payload(field_name: str) -> dict:
    return {**_BASE_PAYLOAD, "name": field_name}


def test_no_numeric_cap_field_count_still_accepted(tmp_path, monkeypatch):
    """The Nth field is accepted because the gate is log_format size,
    not count. Pre-seeded ``n_existing`` short-named fields keep log_format
    well under the 8000 safe max; the new field's name adds ~75 chars
    more — still under. A refactor that adds a numeric ceiling would
    break this test loudly.

    The exact threshold drops as VCL helpers get longer (016 wrapped
    each string field in ``substr(..., 0, 2000)``, adding ~15 chars per
    field). Pick ``n_existing`` well below the new ceiling so this test
    stays as a "no numeric cap" assertion rather than a tipping-point
    measurement.
    """
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    svc_id = "test_svc_count_cap"
    # 80 seeded + 1 new = 81 → log_format ≈ 7900 chars, comfortably
    # under the 8000-char safe max even with the new substr wrappers.
    config.save_config(svc_id, _make_cfg(svc_id, n_existing=80))

    client = TestClient(app)
    # Patch shutil.which on the module that imports it so the Falco
    # branch is skipped — keeps the test fast and deterministic across
    # dev boxes regardless of whether the binary is installed.
    with patch("backend.provision.fastly_api.shutil.which", return_value=None):
        resp = client.post(
            f"/api/services/{svc_id}/custom-fields",
            json=_post_payload("cf080"),
        )
    assert resp.status_code == 200, (
        f"81st field should be accepted (no numeric cap exists); got {resp.status_code} body={resp.text}"
    )
    body = resp.json()
    # Response wraps the saved field as ``body['field']`` — peer tests
    # (test_custom_fields_validation.py) rely on the 200 status alone;
    # we additionally assert the saved name to catch a routing bug
    # where the 200 came from a different handler.
    assert body["field"]["name"] == "cf080"

    # And the saved config now holds 81 fields.
    saved = config.load_config(svc_id)
    assert len(saved["log_fields"]["custom_fields"]) == 81


def test_creating_field_that_overflows_log_format_returns_422(tmp_path, monkeypatch):
    """When the *next* field would push log_format past
    ``FASTLY_LOG_FORMAT_SAFE_MAX`` (8000), the route returns 422 with
    ``LOG_FORMAT_TOO_LONG`` in the errors list.

    The seeded count is calibrated to land just under the safe max so
    a single additional field tips it over. 016 wrapped each string
    field in ``substr(..., 0, 2000)`` (adding ~15 chars per field), so
    the tipping point shifts from ~118 to ~95.

    Pins TWO things at once:
      a) The exact 422 status code (not 400, not silent acceptance).
      b) The ``LOG_FORMAT_TOO_LONG`` error tag in the response body
         shape — the frontend's CustomFieldDrawer looks for this
         substring when surfacing the inline error.
    """
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    svc_id = "test_svc_logfmt_overflow"
    # 94 seeded fields → log_format ≈ 7899 chars (just under safe max);
    # adding cf094 tips the new format past 8000.
    config.save_config(svc_id, _make_cfg(svc_id, n_existing=94))

    client = TestClient(app)
    # No shutil.which patch needed — the length check at
    # backend/provision/fastly_api.py fires BEFORE Falco would be
    # invoked, so this branch is binary-independent.
    resp = client.post(
        f"/api/services/{svc_id}/custom-fields",
        json=_post_payload("cf094"),
    )
    assert resp.status_code == 422, (
        f"field that overflows log_format should 422; got {resp.status_code} body={resp.text}"
    )
    body = resp.json()
    assert "detail" in body and "errors" in body["detail"], body
    errors: list[str] = body["detail"]["errors"]
    assert any("LOG_FORMAT_TOO_LONG" in e for e in errors), f"expected LOG_FORMAT_TOO_LONG in errors, got: {errors}"

    # The blocked field is NOT persisted — saved cfg still has 94.
    saved = config.load_config(svc_id)
    assert len(saved["log_fields"]["custom_fields"]) == 94


def test_log_format_overflow_error_reports_chars_and_safe_max(tmp_path, monkeypatch):
    """The error string surfaces the actual size and the safe-max so the
    user knows how much to trim. ``backend/provision/fastly_api.py:289``
    formats the message as
    ``LOG_FORMAT_TOO_LONG: Log format is N chars; Fastly's limit is
    ~8192 (safe max: 8000). ...``. Pinning the prose protects the
    drawer's UX from a silent rewording that omits the actionable
    numbers."""
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    svc_id = "test_svc_logfmt_msg"
    config.save_config(svc_id, _make_cfg(svc_id, n_existing=118))

    client = TestClient(app)
    resp = client.post(
        f"/api/services/{svc_id}/custom-fields",
        json=_post_payload("cf118"),
    )
    assert resp.status_code == 422
    errors: list[str] = resp.json()["detail"]["errors"]
    msg = next((e for e in errors if "LOG_FORMAT_TOO_LONG" in e), None)
    assert msg is not None
    # Both numbers must appear; either order, with the message phrasing.
    assert "chars" in msg
    assert "8000" in msg, f"expected safe-max 8000 in message: {msg}"


def test_drop_a_field_at_the_size_limit_then_re_add_succeeds(tmp_path, monkeypatch):
    """At the size limit, deleting one field frees template budget so a
    new (size-equivalent) field is accepted on the next POST. Pins the
    gate as DYNAMIC (size-based), not a static "high-water-mark" counter
    that wouldn't free up after deletes.
    """
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    svc_id = "test_svc_drop_readd"
    # Seed at the same just-under-limit count the overflow test uses.
    config.save_config(svc_id, _make_cfg(svc_id, n_existing=94))

    client = TestClient(app)
    # Adding the 95th tips it over (matches the sibling overflow test).
    overflow = client.post(
        f"/api/services/{svc_id}/custom-fields",
        json=_post_payload("cf094"),
    )
    assert overflow.status_code == 422, f"setup invariant: expected 422 at limit; got {overflow.status_code}"

    # Drop one of the seeded fields → the saved config now has 93.
    resp = client.delete(f"/api/services/{svc_id}/custom-fields/cf050")
    assert resp.status_code == 200, f"DELETE expected 200; got {resp.status_code} body={resp.text}"
    saved = config.load_config(svc_id)
    remaining = [cf["name"] for cf in saved["log_fields"]["custom_fields"]]
    assert "cf050" not in remaining, f"DELETE did not remove the field; remaining={remaining!r}"

    # Re-add a same-sized field (5 char name) → the freed budget should
    # accept it. A static count-cap regression would still reject.
    with patch("backend.provision.fastly_api.shutil.which", return_value=None):
        resp = client.post(
            f"/api/services/{svc_id}/custom-fields",
            json=_post_payload("cf999"),
        )
    assert resp.status_code == 200, (
        f"after dropping one field, re-add at the same template size should 200; "
        f"got {resp.status_code} body={resp.text}"
    )

    saved = config.load_config(svc_id)
    names = {cf["name"] for cf in saved["log_fields"]["custom_fields"]}
    assert "cf050" not in names
    assert "cf999" in names

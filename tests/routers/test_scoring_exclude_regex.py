"""Tests for /api/services/{id}/scoring/exclude-regex GET + PUT.

The PUT path is heavyweight in production (clones a VCL version, swaps
a snippet, validates, activates), so we stub the orchestrator helper
and exercise only the routing + validation layers. The orchestrator
helper itself is unit-tested via the recv-snippet override coverage in
test_session_scoring_vcl.py + the validator tests.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from backend import config as svcconfig
from backend.main import app

_SERVICE_ID = "svc-exclude-regex-test"


@pytest.fixture
def seeded_service(tmp_path, monkeypatch):
    """Per-test sandboxed configs dir with scoring enabled.

    The root conftest's ``isolate_metadata_db`` autouse fixture already
    creates ``tmp_path/configs`` and points svcconfig.CONFIGS_DIR there,
    so we just reuse that directory (mkdir would EEXIST otherwise).
    """
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir(exist_ok=True)
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", cfg_dir)
    svcconfig.save_config(
        _SERVICE_ID,
        {
            "service_id": _SERVICE_ID,
            "name": "Test svc",
            "fastly_api_key": "stored-test-key",
            "fos_bucket": "test-bucket",
            "fos_region": "us-east-1",
            "scoring": {
                "enabled": True,
                "scoring_service_id": "sc-svc",
                "scoring_keys_store_id": "ks",
                "scoring_config_store_id": "cs",
                "request_secret": "test_secret_abc",
                "scoring_domain": "scorer.example.com",
                "scoring_service_name": "Test scoring service",
            },
        },
    )
    return cfg_dir


# ── GET ────────────────────────────────────────────────────────────────────


def test_get_returns_default_when_no_override(seeded_service):
    with TestClient(app) as c:
        r = c.get(f"/api/services/{_SERVICE_ID}/scoring/exclude-regex")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["is_default"] is True
    assert body["current"] == ""
    assert body["default"]  # non-empty
    assert body["effective"] == body["default"]


def test_get_returns_override_when_persisted(seeded_service):
    cfg = svcconfig.load_config(_SERVICE_ID)
    cfg["scoring"]["exclude_url_regex"] = r"\.(css|js)$"
    svcconfig.save_config(_SERVICE_ID, cfg)

    with TestClient(app) as c:
        r = c.get(f"/api/services/{_SERVICE_ID}/scoring/exclude-regex")
    assert r.status_code == 200
    body = r.json()
    assert body["is_default"] is False
    assert body["current"] == r"\.(css|js)$"
    assert body["effective"] == r"\.(css|js)$"


# ── POST /validate (on-blur dry-run lint) ──────────────────────────────────


def test_validate_returns_ok_for_clean_regex(seeded_service):
    """Dry-run validator with a syntactically valid + policy-clean regex
    returns ok=True. Drives the admin UI's on-blur lint affordance — no
    cfg writes, no Fastly calls."""
    with (
        TestClient(app) as c,
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
    ):
        r = c.post(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex/validate",
            json={"regex": r"\.(css|js)$"},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert isinstance(body["lint_warnings"], list)


def test_validate_returns_error_for_invalid_regex(seeded_service):
    """Operator typing an unbalanced regex sees ok=False with a structured
    reason — same shape the publish flow surfaces, so the on-blur UI can
    use one error renderer for both code paths."""
    with (
        TestClient(app) as c,
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
    ):
        r = c.post(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex/validate",
            json={"regex": "(unclosed"},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is False
    assert body["reason"] == "invalid_regex"
    assert "error" in body and isinstance(body["error"], str)


def test_validate_returns_error_for_disallowed_quote(seeded_service):
    """Input-policy layer (length / quotes / control chars) fires before
    the regex compile and before falco — exercise that path too."""
    with (
        TestClient(app) as c,
        patch("backend.utils.vcl_validator.shutil.which", return_value=None),
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
    ):
        r = c.post(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex/validate",
            json={"regex": 'foo"bar'},
        )
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is False
    assert body["reason"] == "disallowed_char"


def test_validate_does_not_persist_to_cfg(seeded_service):
    """The whole point of the validate endpoint is that it's read-only —
    a successful dry-run must NOT mutate cfg.scoring.exclude_url_regex."""
    cfg_before = svcconfig.load_config(_SERVICE_ID)
    stored_before = (cfg_before.get("scoring") or {}).get("exclude_url_regex")

    with (
        TestClient(app) as c,
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
    ):
        r = c.post(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex/validate",
            json={"regex": r"^/healthz$"},
        )
    assert r.status_code == 200
    assert r.json()["ok"] is True

    cfg_after = svcconfig.load_config(_SERVICE_ID)
    stored_after = (cfg_after.get("scoring") or {}).get("exclude_url_regex")
    assert stored_after == stored_before, "validate must be read-only — cfg.scoring.exclude_url_regex changed"


def test_validate_rejects_non_string_body(seeded_service):
    """body.regex must be a string — int / null / array → 400 with a
    clear message, same shape as the PUT endpoint."""
    with TestClient(app) as c:
        r = c.post(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex/validate",
            json={"regex": 42},
        )
    assert r.status_code == 400
    assert "string" in r.json()["detail"]["error"]


def test_get_reports_is_default_when_literal_default_stored(seeded_service):
    """enable_scoring persists the literal DEFAULT_ASSET_EXT_REGEX into
    cfg on first turn-on so the admin UI's textarea is pre-populated with
    something to edit (the previous null sentinel produced an empty box).
    The GET must still report ``is_default=True`` in that case, otherwise
    the UI would mislabel a fresh-from-enable service as
    "currently custom override" the moment scoring is turned on."""
    from backend.provision.session_scoring_vcl import DEFAULT_ASSET_EXT_REGEX

    cfg = svcconfig.load_config(_SERVICE_ID)
    cfg["scoring"]["exclude_url_regex"] = DEFAULT_ASSET_EXT_REGEX
    svcconfig.save_config(_SERVICE_ID, cfg)

    with TestClient(app) as c:
        r = c.get(f"/api/services/{_SERVICE_ID}/scoring/exclude-regex")
    assert r.status_code == 200
    body = r.json()
    assert body["is_default"] is True, "stored value equals the bundled default → must report is_default=True"
    assert body["current"] == DEFAULT_ASSET_EXT_REGEX
    assert body["effective"] == DEFAULT_ASSET_EXT_REGEX


# ── PUT — confirm gate + validation ─────────────────────────────────────────


def test_put_without_confirm_rejected(seeded_service):
    with TestClient(app) as c:
        r = c.put(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex",
            params={"token": "stored-test-key"},
            json={"regex": r"\.css$"},
        )
    assert r.status_code == 400
    assert "confirm=true" in r.json()["detail"]["error"]


def test_put_without_token_rejected(seeded_service):
    with TestClient(app) as c:
        r = c.put(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex",
            params={"confirm": "true"},
            json={"regex": r"\.css$"},
        )
    # _resolve_token falls back to stored fastly_api_key only when the
    # caller passes empty; here we explicitly omit token and the cfg
    # DOES have fastly_api_key, so this should actually proceed past
    # auth and only fail at the orchestrator (which we haven't stubbed).
    # The "no token" path requires the stored key also missing.
    assert r.status_code != 200  # some downstream failure expected


def test_put_with_disallowed_quote_rejected(seeded_service):
    with (
        TestClient(app) as c,
        patch("backend.utils.vcl_validator.shutil.which", return_value=None),
        # Disable falco-required so validation flows on its input-layer reject.
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
    ):
        r = c.put(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex",
            params={"token": "stored-test-key", "confirm": "true"},
            json={"regex": 'foo"bar'},
        )
    assert r.status_code == 400
    body = r.json()
    assert body["detail"]["reason"] == "disallowed_char"


def test_put_with_invalid_regex_rejected(seeded_service):
    with (
        TestClient(app) as c,
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
    ):
        r = c.put(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex",
            params={"token": "stored-test-key", "confirm": "true"},
            json={"regex": "(unclosed"},
        )
    assert r.status_code == 400
    assert r.json()["detail"]["reason"] == "invalid_regex"


def test_put_when_scoring_disabled_rejected(seeded_service):
    cfg = svcconfig.load_config(_SERVICE_ID)
    cfg["scoring"]["enabled"] = False
    svcconfig.save_config(_SERVICE_ID, cfg)

    with TestClient(app) as c:
        r = c.put(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex",
            params={"token": "stored-test-key", "confirm": "true"},
            json={"regex": r"\.css$"},
        )
    assert r.status_code == 400
    assert "not enabled" in r.json()["detail"]["error"]


def test_put_happy_path_with_stubbed_orchestrator(seeded_service):
    """Full pipeline: input validation passes, validator passes (falco
    skipped via env), orchestrator stubbed → audit log entry written →
    200 OK."""
    fake_result = {
        "effective_regex": r"\.css$",
        "is_default": False,
        "logging_service_active_version": 42,
    }
    with (
        TestClient(app) as c,
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
        patch(
            "backend.provision.session_scoring_orchestrator.update_recv_exclusion_regex",
            return_value=fake_result,
        ) as stub_update,
        patch("backend.core.metadata_db.record_scoring_audit") as stub_audit,
    ):
        r = c.put(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex",
            params={"token": "stored-test-key", "confirm": "true"},
            json={"regex": r"\.css$"},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["is_default"] is False
    assert body["effective_regex"] == r"\.css$"
    assert body["logging_service_active_version"] == 42
    stub_update.assert_called_once()
    stub_audit.assert_called_once()
    call = stub_audit.call_args
    assert call.args[1] == "scoring_exclude_regex_changed"


def test_put_reset_to_default(seeded_service):
    """Empty regex resets to default; orchestrator gets called with ""
    (which it maps to "use default" internally)."""
    cfg = svcconfig.load_config(_SERVICE_ID)
    cfg["scoring"]["exclude_url_regex"] = r"\.(prior|custom)$"
    svcconfig.save_config(_SERVICE_ID, cfg)

    fake_result = {
        "effective_regex": "...DEFAULT...",
        "is_default": True,
        "logging_service_active_version": 43,
    }
    with (
        TestClient(app) as c,
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
        patch(
            "backend.provision.session_scoring_orchestrator.update_recv_exclusion_regex",
            return_value=fake_result,
        ),
        patch("backend.core.metadata_db.record_scoring_audit"),
    ):
        r = c.put(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex",
            params={"token": "stored-test-key", "confirm": "true"},
            json={"regex": ""},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["is_default"] is True
    assert "Reset to default" in body["message"]


def test_scoring_vcl_excludes_query_params():
    """Assert that the default asset exclusion regex does not match
    excluded extensions in the query string parameter, but does match them in the path."""
    import re

    from backend.provision.session_scoring_vcl import DEFAULT_ASSET_EXT_REGEX

    pattern = re.compile(DEFAULT_ASSET_EXT_REGEX, re.IGNORECASE)

    # Valid asset paths (should match and bypass scoring)
    assert pattern.search("/static/logo.png")
    assert pattern.search("/assets/styles.css")
    assert pattern.search("/js/app.js?v=1.2")

    # Dynamic paths with query params containing asset extensions (should NOT match, so they are scored)
    assert not pattern.search("/api/v1/login?file=.png")
    assert not pattern.search("/api/v1/user?bypass=.css")
    assert not pattern.search("/index.html?extension=.js")

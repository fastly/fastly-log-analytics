"""Tests for the analyst-safe projection of GET /rum/status (F1 audit fix).

Before this fix, ``/rum/status`` was wholesale-blocked for analysts
(``_ANALYST_BLOCKED_RUM_SUFFIXES``), which 403'd the RUM page's status
fetch and left it permanently stuck rendering the admin-only
``RumStatusPanel``. The route now projects an analyst-safe body — mirrors
the ``/api/log-extents`` vs ``/api/sync-status`` sibling shape: an analyst
gets ``{enabled, enabled_at}``; ``deployed_vcl_sha`` / ``current_vcl_sha`` /
``vcl_drift`` (the operator's deployed edge-VCL fingerprint) stay
admin-only.

The route function is called directly with a stand-in ``request`` object
(only ``request.state.analyst_session`` is read) rather than through the
full RemoteAccessMiddleware + tunnel/invite flow — same lightweight idiom
``backend/routers/web_vitals.py`` uses to distinguish analyst vs admin
callers.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from backend.routers import rum as rum_router

SVC = "svc-rum-status-projection"


@pytest.fixture
def with_config(monkeypatch):
    container: dict = {}
    monkeypatch.setattr("backend.config.load_config", lambda service_id: container.get(service_id))
    return container


def _request(analyst_session: object | None) -> SimpleNamespace:
    return SimpleNamespace(state=SimpleNamespace(analyst_session=analyst_session))


def _analyst_session() -> SimpleNamespace:
    return SimpleNamespace(session_id="s-test", service_ids=[SVC], email="analyst@example.com")


@pytest.mark.security_regression
async def test_analyst_status_excludes_vcl_fingerprint_fields(with_config, monkeypatch):
    """No ``*_vcl_sha`` and no ``vcl_drift`` key must survive to an analyst,
    even when RUM is enabled and drifted."""
    with_config[SVC] = {
        "rum_enabled": True,
        "rum_enabled_at": "2026-01-01T00:00:00Z",
        "rum_vcl_sha": "deadbeef",
    }
    # rum_vcl_fingerprint reads the deployed VCL — must not even be called
    # for an analyst caller (skip the fingerprint work entirely).
    monkeypatch.setattr(
        rum_router,
        "rum_vcl_fingerprint",
        lambda service_id: pytest.fail("rum_vcl_fingerprint must not be called for an analyst caller"),
    )

    result = await rum_router.rum_status(_request(_analyst_session()), service_id=SVC)

    assert result == {"enabled": True, "enabled_at": "2026-01-01T00:00:00Z"}
    assert "deployed_vcl_sha" not in result
    assert "current_vcl_sha" not in result
    assert "vcl_drift" not in result


@pytest.mark.security_regression
async def test_analyst_can_read_enabled_field(with_config):
    """Analyst CAN read ``enabled`` — this is the field the RUM page gates
    its entire body on, and it is not operator-sensitive."""
    with_config[SVC] = {"rum_enabled": False}

    result = await rum_router.rum_status(_request(_analyst_session()), service_id=SVC)

    assert result["enabled"] is False
    assert "enabled_at" in result


def test_admin_status_still_sees_vcl_fingerprint_fields(with_config, monkeypatch):
    """Negative control: the admin (no analyst_session) branch is untouched —
    it still gets the full VCL-drift-detection payload."""
    with_config[SVC] = {
        "rum_enabled": True,
        "rum_enabled_at": "2026-01-01T00:00:00Z",
        "rum_vcl_sha": "deadbeef",
    }
    monkeypatch.setattr(rum_router, "rum_vcl_fingerprint", lambda service_id: "deadbeef")

    import asyncio

    result = asyncio.run(rum_router.rum_status(_request(None), service_id=SVC))

    assert result["enabled"] is True
    assert result["deployed_vcl_sha"] == "deadbeef"
    assert result["current_vcl_sha"] == "deadbeef"
    assert result["vcl_drift"] is False


def test_admin_status_defaults_deployed_sha_to_current_when_enabled_but_unstored(with_config, monkeypatch):
    """Enabled with no ``rum_vcl_sha`` recorded yet (e.g. enabled by an older
    code path that never wrote it) must NOT report drift against itself —
    ``deployed_sha`` falls back to ``current_sha`` rather than staying None."""
    with_config[SVC] = {"rum_enabled": True, "rum_enabled_at": "2026-01-01T00:00:00Z"}
    monkeypatch.setattr(rum_router, "rum_vcl_fingerprint", lambda service_id: "freshsha123")

    import asyncio

    result = asyncio.run(rum_router.rum_status(_request(None), service_id=SVC))

    assert result["deployed_vcl_sha"] == "freshsha123"
    assert result["current_vcl_sha"] == "freshsha123"
    assert result["vcl_drift"] is False


def test_admin_status_migrates_legacy_pre_f4_sha_instead_of_false_positive_drift(with_config, monkeypatch):
    """#2 audit finding: the F-4 fix made ``rum_vcl_fingerprint`` depend on
    ``faro_version``, but nothing migrated services whose ``rum_vcl_sha``
    was stored by the OLD algorithm (which never varied with
    ``faro_version`` at all). That stale value can never again match the
    new algorithm's output, so every pre-existing RUM service would report
    ``vcl_drift: true`` forever — even one that was just correctly
    reconciled.

    Pins the chosen fix (one-time migration on read): a stored sha that
    exactly matches what the OLD algorithm would have produced for this
    service is recognized as legacy-format (not real drift), cleared to
    ``vcl_drift: false`` for this response, AND persisted as the new
    algorithm's value so subsequent reads don't need the fallback again.
    """
    with_config[SVC] = {
        "rum_enabled": True,
        "rum_enabled_at": "2026-01-01T00:00:00Z",
        "rum_vcl_sha": "legacy-sha-abc",
    }
    monkeypatch.setattr(rum_router, "rum_vcl_fingerprint", lambda service_id: "current-sha-xyz")
    monkeypatch.setattr(rum_router, "legacy_rum_vcl_fingerprint", lambda service_id: "legacy-sha-abc")

    saved: dict = {}
    monkeypatch.setattr("backend.config.save_config", lambda service_id, cfg: saved.update({service_id: cfg}))

    import asyncio

    result = asyncio.run(rum_router.rum_status(_request(None), service_id=SVC))

    assert result["vcl_drift"] is False
    assert result["deployed_vcl_sha"] == "current-sha-xyz"
    assert result["current_vcl_sha"] == "current-sha-xyz"
    # Migration persisted so the next read skips the legacy fallback.
    assert saved[SVC]["rum_vcl_sha"] == "current-sha-xyz"


def test_admin_status_reports_genuine_drift_when_sha_matches_neither_algorithm(with_config, monkeypatch):
    """Negative control: a stored sha that matches NEITHER the legacy NOR
    the current algorithm's output is real drift (e.g. VCL changed
    upstream, or the service was reconciled against a different
    faro_version) and must still be reported — the legacy-migration
    fallback must not mask genuine drift."""
    with_config[SVC] = {
        "rum_enabled": True,
        "rum_enabled_at": "2026-01-01T00:00:00Z",
        "rum_vcl_sha": "genuinely-stale-sha",
    }
    monkeypatch.setattr(rum_router, "rum_vcl_fingerprint", lambda service_id: "current-sha-xyz")
    monkeypatch.setattr(rum_router, "legacy_rum_vcl_fingerprint", lambda service_id: "legacy-sha-abc")

    save_calls: list = []
    monkeypatch.setattr("backend.config.save_config", lambda service_id, cfg: save_calls.append((service_id, cfg)))

    import asyncio

    result = asyncio.run(rum_router.rum_status(_request(None), service_id=SVC))

    assert result["vcl_drift"] is True
    assert result["deployed_vcl_sha"] == "genuinely-stale-sha"
    assert result["current_vcl_sha"] == "current-sha-xyz"
    # No migration write for a sha that isn't recognized as legacy-format.
    assert save_calls == []

from __future__ import annotations

import os
import subprocess
import sys

import pytest


@pytest.mark.parametrize("flag", ["FASTLY_MOCK_MODE", "OAUTH_MOCK_IDP"])
def test_production_rejects_mock_modes_at_startup(flag: str) -> None:
    env = {**os.environ, "FLA_PRODUCTION": "1", "FASTLY_MOCK_MODE": "0", "OAUTH_MOCK_IDP": "0", flag: "1"}
    result = subprocess.run(
        [sys.executable, "-c", "import backend.main"],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0
    assert f"{flag} must not be enabled in production" in result.stderr


@pytest.mark.parametrize("flag", ["FASTLY_MOCK_MODE", "OAUTH_MOCK_IDP"])
@pytest.mark.parametrize("production", [False, True])
def test_mock_modes_are_test_only(monkeypatch: pytest.MonkeyPatch, flag: str, production: bool) -> None:
    from backend.core.fastly.client import is_mock_mode
    from backend.routers.share_admin.tunnel_routes import mock_idp_enabled

    monkeypatch.setenv("FLA_PRODUCTION", "1" if production else "0")
    monkeypatch.setenv(flag, "1")
    enabled = is_mock_mode if flag == "FASTLY_MOCK_MODE" else mock_idp_enabled
    if production:
        with pytest.raises(RuntimeError, match=f"{flag} must not be enabled in production"):
            enabled()
    else:
        assert enabled() is True


def test_production_with_mock_modes_disabled_is_valid(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.utils.runtime_modes import validate_mock_modes

    monkeypatch.setenv("FLA_PRODUCTION", "1")
    monkeypatch.delenv("FASTLY_MOCK_MODE", raising=False)
    monkeypatch.delenv("OAUTH_MOCK_IDP", raising=False)
    validate_mock_modes()

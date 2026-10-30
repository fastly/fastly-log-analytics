from __future__ import annotations

import os


def mock_mode_enabled(variable: str) -> bool:
    enabled = os.environ.get(variable) == "1"
    if enabled and os.environ.get("FLA_PRODUCTION") == "1":
        raise RuntimeError(f"{variable} must not be enabled in production")
    return enabled


def validate_mock_modes() -> None:
    for variable in ("FASTLY_MOCK_MODE", "OAUTH_MOCK_IDP"):
        mock_mode_enabled(variable)

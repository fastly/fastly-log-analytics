from __future__ import annotations

import os

_TRUTHY_ENV_VALUES = frozenset({"1", "true", "yes", "on"})


def _env_flag_enabled(variable: str) -> bool:
    return os.environ.get(variable, "").strip().lower() in _TRUTHY_ENV_VALUES


def mock_mode_enabled(variable: str) -> bool:
    enabled = _env_flag_enabled(variable)
    if enabled and _env_flag_enabled("FLA_PRODUCTION"):
        raise RuntimeError(f"{variable} must not be enabled in production")
    return enabled


def validate_mock_modes() -> None:
    for variable in ("FASTLY_MOCK_MODE", "FASTLY_API_MOCK", "OAUTH_MOCK_IDP"):
        mock_mode_enabled(variable)

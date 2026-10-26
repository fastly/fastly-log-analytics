import sys

from backend.core.fastly.client import fastly, parse_period

from . import cli as _cli_mod
from . import fastly_api as _fastly_api_mod
from . import orchestrator as _orchestrator_mod
from .fastly_api import (
    generate_capture_vcl,
    load_log_format,
    update_logging_endpoint,
    validate_log_format,
)
from .fos_setup import ensure_fos_access_key, find_fos_key
from .orchestrator import (
    _sync_crontab,
    analyst_path_a_supported,
    generate_analyst_invite,
    perform_teardown,
    provision,
    write_service_config,
)

# Register historical module aliases for callers importing merged submodules.
utils = _cli_mod
log_paths = _cli_mod
draft_version = _cli_mod
cmcd_fields = _orchestrator_mod
sharing_domain = _fastly_api_mod
sys.modules[f"{__name__}.utils"] = _cli_mod
sys.modules[f"{__name__}.log_paths"] = _cli_mod
sys.modules[f"{__name__}.draft_version"] = _cli_mod
sys.modules[f"{__name__}.cmcd_fields"] = _orchestrator_mod
sys.modules[f"{__name__}.sharing_domain"] = _fastly_api_mod

import backend as _backend_pkg  # noqa: E402

if hasattr(_backend_pkg, "_MODULE_ALIASES"):
    _backend_pkg._MODULE_ALIASES.update(
        {
            "backend.provision.utils": "backend.provision.cli",
            "backend.provision.log_paths": "backend.provision.cli",
            "backend.provision.draft_version": "backend.provision.cli",
            "backend.provision.cmcd_fields": "backend.provision.orchestrator",
            "backend.provision.declarative.iac_reconciler": "backend.provision.declarative.reconciler",
        }
    )

# Public API of the package — declared explicitly so ruff doesn't flag the
# re-exports above as "unused imports". Any name that callers import as
# `from backend.provision import X` should be listed here.
__all__ = [
    "fastly",
    "parse_period",
    # fastly_api
    "generate_capture_vcl",
    "load_log_format",
    "update_logging_endpoint",
    "validate_log_format",
    # fos_setup
    "ensure_fos_access_key",
    "find_fos_key",
    # orchestrator
    "_sync_crontab",
    "analyst_path_a_supported",
    "generate_analyst_invite",
    "perform_teardown",
    "provision",
    "write_service_config",
    # submodule aliases
    "utils",
    "log_paths",
    "draft_version",
    "cmcd_fields",
    "sharing_domain",
]

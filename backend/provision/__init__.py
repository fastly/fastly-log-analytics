import sys

import backend as _backend_pkg

if hasattr(_backend_pkg, "_MODULE_ALIASES"):
    _backend_pkg._MODULE_ALIASES.update(
        {
            "backend.provision.utils": "backend.provision.cli",
            "backend.provision.log_paths": "backend.provision.cli",
            "backend.provision.draft_version": "backend.provision.cli",
            "backend.provision.cmcd_fields": "backend.provision.orchestrator",
            "backend.provision.fos_setup": "backend.provision.orchestrator",
            "backend.provision.rum_orchestrator_v2": "backend.provision.orchestrator",
            "backend.provision.rum_assets": "backend.provision.orchestrator",
            "backend.utils.terraform_gen": "backend.provision.orchestrator",
            "backend.provision.declarative.iac_reconciler": "backend.provision.declarative.reconciler",
            "backend.provision.declarative.fastly_integration": "backend.provision.declarative.reconciler",
            "backend.provision.declarative.generators": "backend.provision.declarative.reconciler",
            "backend.provision.session_scoring_vcl": "backend.provision.session_scoring_orchestrator",
            "backend.provision.session_scoring_setup": "backend.provision.session_scoring_orchestrator",
            "backend.provision.declarative.state": "backend.provision.declarative.reconciler",
            "backend.provision.declarative.config_migration": "backend.provision.declarative.reconciler",
        }
    )

from backend.core.fastly.client import fastly, parse_period  # noqa: E402

from . import cli as _cli_mod  # noqa: E402
from . import fastly_api as _fastly_api_mod  # noqa: E402
from . import orchestrator as _orchestrator_mod  # noqa: E402
from . import session_scoring_orchestrator as _ss_orch_mod  # noqa: E402
from .fastly_api import (  # noqa: E402
    generate_capture_vcl,
    load_log_format,
    update_logging_endpoint,
    validate_log_format,
)
from .orchestrator import (  # noqa: E402
    _sync_crontab,
    ensure_fos_access_key,
    find_fos_key,
    perform_teardown,
    provision,
    write_service_config,
)

# Register historical module aliases for callers importing merged submodules.
utils = _cli_mod
log_paths = _cli_mod
draft_version = _cli_mod
cmcd_fields = _orchestrator_mod
fos_setup = _orchestrator_mod
sharing_domain = _fastly_api_mod
session_scoring_vcl = _ss_orch_mod
session_scoring_setup = _ss_orch_mod
sys.modules[f"{__name__}.utils"] = _cli_mod
sys.modules[f"{__name__}.log_paths"] = _cli_mod
sys.modules[f"{__name__}.draft_version"] = _cli_mod
sys.modules[f"{__name__}.cmcd_fields"] = _orchestrator_mod
sys.modules[f"{__name__}.fos_setup"] = _orchestrator_mod
sys.modules[f"{__name__}.sharing_domain"] = _fastly_api_mod
sys.modules[f"{__name__}.session_scoring_vcl"] = _ss_orch_mod
sys.modules[f"{__name__}.session_scoring_setup"] = _ss_orch_mod

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
    "perform_teardown",
    "provision",
    "write_service_config",
    # submodule aliases
    "utils",
    "log_paths",
    "draft_version",
    "cmcd_fields",
    "fos_setup",
    "sharing_domain",
    "session_scoring_vcl",
    "session_scoring_setup",
]

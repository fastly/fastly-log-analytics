import sys

import backend as _backend_pkg

if hasattr(_backend_pkg, "_MODULE_ALIASES"):
    for _unfolded in (
        "backend.provision.cmcd_fields",
        "backend.provision.fos_setup",
        "backend.provision.rum_orchestrator_v2",
        "backend.provision.rum_assets",
        "backend.utils.terraform_gen",
    ):
        _backend_pkg._MODULE_ALIASES.pop(_unfolded, None)
    _backend_pkg._MODULE_ALIASES.update(
        {
            "backend.provision.utils": "backend.provision.cli",
            "backend.provision.log_paths": "backend.provision.cli",
            "backend.provision.draft_version": "backend.provision.cli",
            "backend.provision.declarative.iac_reconciler": "backend.provision.declarative.reconciler",
            "backend.provision.declarative.fastly_integration": "backend.provision.declarative.reconciler",
            "backend.provision.declarative.generators": "backend.provision.declarative.reconciler",
            "backend.provision.session_scoring_vcl": "backend.provision.session_scoring_orchestrator",
            "backend.provision.session_scoring_setup": "backend.provision.session_scoring_orchestrator",
            "backend.provision.declarative.state": "backend.provision.declarative.reconciler",
            "backend.provision.declarative.config_migration": "backend.provision.declarative.reconciler",
        }
    )

for _mod_name in (
    "backend.provision.cmcd_fields",
    "backend.provision.fos_setup",
    "backend.provision.rum_orchestrator_v2",
    "backend.provision.rum_assets",
    "backend.utils.terraform_gen",
):
    _existing = sys.modules.get(_mod_name)
    if _existing is not None and not (getattr(_existing, "__file__", None) or "").endswith(
        f"{_mod_name.rsplit('.', 1)[-1]}.py"
    ):
        del sys.modules[_mod_name]

from backend.core.fastly.client import fastly, parse_period  # noqa: E402

from . import cli as _cli_mod  # noqa: E402
from . import cmcd_fields, fos_setup, rum_assets, rum_orchestrator_v2  # noqa: E402
from . import fastly_api as _fastly_api_mod  # noqa: E402
from . import session_scoring_orchestrator as _ss_orch_mod  # noqa: E402
from .fastly_api import (  # noqa: E402
    generate_capture_vcl,
    load_log_format,
    update_logging_endpoint,
    validate_log_format,
)
from .fos_setup import (  # noqa: E402
    ensure_fos_access_key,
    find_fos_key,
)
from .orchestrator import (  # noqa: E402
    _sync_crontab,
    perform_teardown,
    provision,
    write_service_config,
)

# Register historical module aliases for callers importing merged submodules.
utils = _cli_mod
log_paths = _cli_mod
draft_version = _cli_mod
sharing_domain = _fastly_api_mod
session_scoring_vcl = _ss_orch_mod
session_scoring_setup = _ss_orch_mod
sys.modules[f"{__name__}.utils"] = _cli_mod
sys.modules[f"{__name__}.log_paths"] = _cli_mod
sys.modules[f"{__name__}.draft_version"] = _cli_mod
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
    # submodules & aliases
    "utils",
    "log_paths",
    "draft_version",
    "cmcd_fields",
    "fos_setup",
    "rum_assets",
    "rum_orchestrator_v2",
    "sharing_domain",
    "session_scoring_vcl",
    "session_scoring_setup",
]

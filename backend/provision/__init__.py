import sys

from backend.core.fastly.client import fastly, parse_period  # noqa: E402

from . import cli, cmcd_fields, fos_setup, rum_assets, rum_orchestrator  # noqa: E402
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

if "backend.provision.cli" in getattr(sys, "orig_argv", ()):
    sys.modules.pop("backend.provision.cli", None)

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
    "cli",
    "cmcd_fields",
    "fos_setup",
    "rum_assets",
    "rum_orchestrator",
]

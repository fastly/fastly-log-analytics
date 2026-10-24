from backend.core.fastly.client import fastly
from backend.core.fastly.utils import parse_period

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
]

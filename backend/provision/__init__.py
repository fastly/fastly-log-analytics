from backend.core.fastly.client import fastly
from backend.core.fastly.utils import parse_period

from .cli import (
    handle_invite_analyst,
    handle_list_fields,
    handle_list_groups,
    handle_teardown,
    handle_update_cdn,
    handle_update_logs,
    wizard,
)
from .fastly_api import (
    delete_cdn_service,
    ensure_cdn_service,
    ensure_logging_endpoint,
    generate_capture_vcl,
    load_log_format,
    redeploy_cdn_vcl,
    remove_logging_endpoint,
    resolve_shield_secret,
    update_logging_endpoint,
    validate_log_format,
)
from .fos_setup import ensure_fos_access_key, find_fos_key
from .orchestrator import (
    _sync_crontab,
    cleanup_local_data,
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
    # cli
    "handle_invite_analyst",
    "handle_list_fields",
    "handle_list_groups",
    "handle_teardown",
    "handle_update_cdn",
    "handle_update_logs",
    "wizard",
    # fastly_api
    "delete_cdn_service",
    "ensure_cdn_service",
    "ensure_logging_endpoint",
    "generate_capture_vcl",
    "load_log_format",
    "redeploy_cdn_vcl",
    "remove_logging_endpoint",
    "resolve_shield_secret",
    "update_logging_endpoint",
    "validate_log_format",
    # fos_setup
    "ensure_fos_access_key",
    "find_fos_key",
    # orchestrator
    "_sync_crontab",
    "cleanup_local_data",
    "generate_analyst_invite",
    "perform_teardown",
    "provision",
    "write_service_config",
]

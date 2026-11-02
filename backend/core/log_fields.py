"""Log field catalog, typed registry, metric expressions, and VCL format/safety utilities.

Facade module re-exporting from:
- ``backend.core.catalog``
- ``backend.core.presets``
- ``backend.core.insight_definitions``
- ``backend.core.metric_expressions``
- ``backend.core.vcl_generator``
"""

from __future__ import annotations

# ruff: noqa: I001
import hashlib  # noqa: F401
import json  # noqa: F401
import os  # noqa: F401
import re  # noqa: F401
import shutil  # noqa: F401
import subprocess  # noqa: F401
import sys
import tempfile  # noqa: F401
import types
from typing import Any

from backend.core import catalog as _catalog_mod
from backend.core.catalog import (  # noqa: F401
    _BUILTIN_FIELD_NAMES,
    _CATALOG,
    _DEFAULT_LF_CONFIG,
    _DUCKDB_RESERVED,
    _DUCKDB_TYPE_VALUE_TYPE_COMPAT,
    _NUMERIC_FIELD_MAX_LEN,
    _SUBSTR_RE,
    _VARIABLE_FIELD_DEFAULTS,
    BY_CODE,
    BY_GROUP,
    CONN_SPEED_ENCODE,
    DEFAULT_OFF_FIELD_IDS,
    FASTLY_LOG_LINE_DELIVER_MAX,
    FASTLY_LOG_LINE_SAFE_MAX,
    GROUP_DEPENDENCIES,
    GROUP_INFO,
    LOG_FIELD_CATALOG,
    PROXY_DESC_ENCODE,
    PROXY_TYPE_ENCODE,
    REGISTRY,
    VALID_NAME_RE,
    DuckType,
    Group,
    LogField,
    _detect_substr_cap,
    _f,
    _field_from_dict,
    _filter_user_custom_fields,
    _g,
    _group_index,
    _is_system_field,
    _m,
    _opt_int,
    _opt_str,
    _v,
    check_log_line_budget,
    derived,
    duckdb_decode_case,
    estimate_log_line_bytes,
    get,
    get_catalog_for_api,
    get_custom_fields_catalog_entries,
    get_groups_for_api,
    get_lf_config,
    get_required_edge_headers,
    in_group,
    resolve_enabled_fields,
    try_get,
    validate_custom_field,
    vcl_encode_chain,
)
from backend.core.fastly.client import fastly  # noqa: F401
from backend.core import (
    insight_definitions as _insights_mod,
    metric_expressions as _metrics_mod,
    presets as _presets_mod,
    vcl_generator as _vcl_mod,
)
from backend.core.insight_definitions import INSIGHT_DEFINITIONS, _i  # noqa: F401
from backend.core.metric_expressions import (  # noqa: F401
    HIT_STATES,
    METRIC_DEFINITIONS,
    MetricType,
    coalesce,
    column_or_default,
    get_metric_sql,
    origin_latency_us,
    sum_or_zero,
)
from backend.core.presets import PRESETS, _p  # noqa: F401
from backend.core.vcl_generator import (  # noqa: F401
    _DESTRUCTIVE_SCOPE,
    _FORBIDDEN_ALLOW_BACKSLASH,
    _FORBIDDEN_STRICT,
    MAX_REGEX_BYTES,
    LintResult,
    RegexValidationError,
    VclSafetyError,
    _auth_err,
    _falco_binary,
    _parse_falco_output,
    _parse_scopes,
    _run_falco_lint,
    format_hash,
    generate_log_format,
    lint_document,
    lint_log_format,
    lint_vcl,
    log_format_to_vcl_log,
    logger,
    validate_destructive_token,
    validate_log_format_macro,
    validate_quoted_literal,
    validate_recv_exclusion_regex_with_lint,
    validate_url_exclusion_regex,
)

_SUBMODULES = (
    _catalog_mod,
    _presets_mod,
    _insights_mod,
    _metrics_mod,
    _vcl_mod,
)


class _LogFieldsShimModule(types.ModuleType):
    """Forwards attribute assignments/deletions on ``backend.core.log_fields`` to owning submodules."""

    def __setattr__(self, name: str, value: Any) -> None:
        super().__setattr__(name, value)
        for submod in _SUBMODULES:
            if hasattr(submod, name):
                setattr(submod, name, value)

    def __delattr__(self, name: str) -> None:
        super().__delattr__(name)


_self_mod = sys.modules.get(__name__)
if _self_mod is not None and not isinstance(_self_mod, _LogFieldsShimModule):
    _self_mod.__class__ = _LogFieldsShimModule

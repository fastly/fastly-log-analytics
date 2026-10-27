# ruff: noqa: T201
"""VCL & Multi-Service Declarative Reconciliation Control Loop (Steps 1-8 + IaC Engine)."""

from __future__ import annotations

import copy
import json
import logging
import os
import re
import sys
import time
import urllib.parse
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field, TypeAdapter

from backend.config import is_rum_enabled
from backend.core.fastly.client import fastly, find_service_by_name
from backend.core.fastly.client import find_service_by_name as _orig_find_service_by_name

_AUTO_INJECTED_NAMES: set[str] = {
    "rum_cid",
    "fastly_req_id",
    "rum_metric_name",
    "rum_metric_value",
    "rum_metric_rating",
    "rum_error_message",
    "rum_error_stack",
    "rum_trace_id",
    "rum_span_id",
    "rum_pathname",
    "rum_connection_speed",
    "rum_dns_ms",
    "rum_tcp_ms",
    "rum_tls_ms",
    "rum_ttfb_ms",
    "rum_raw_query",
    "rum_body",
    "edge_score",
    "edge_score_l1",
    "edge_score_l2",
    "edge_cookie_compliance",
    "edge_score_reason",
    "edge_sid",
    "edge_score_rtt_us",
    "edge_score_exec_us",
    "edge_matrix_version",
    "cmcd_sid",
    "cmcd_cid",
    "cmcd_br",
    "cmcd_bl",
    "cmcd_bs",
    "cmcd_d",
    "cmcd_dl",
    "cmcd_mtp",
    "cmcd_ot",
    "cmcd_sf",
    "cmcd_st",
    "cmcd_su",
    "cmcd_tb",
    "cmcd_rtp",
}


@dataclass(frozen=True)
class CmcdConfig:
    """Immutable CMCD feature configuration."""

    enabled: bool = False
    mode: Literal["query_string", "headers"] = "query_string"
    version: Literal[1, 2] = 1


@dataclass(frozen=True)
class ScoringConfig:
    """Immutable Scoring feature configuration."""

    enabled: bool = False
    domain: str = ""
    request_secret: str = ""
    exclude_url_regex: str = ""
    enforce_status_code: int = 429


@dataclass(frozen=True)
class LogFieldsConfig:
    """Immutable log field configuration."""

    groups: list[str] = field(default_factory=list)
    custom_fields: list[dict[str, Any]] = field(default_factory=list)
    field_overrides: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FeatureState:
    """Immutable representation of the desired configuration of a Fastly service."""

    service_id: str
    log_period: int
    sample_rate: int
    edge_only: bool
    custom_condition: str
    fos_prefix: str
    fos_endpoint: str
    logging_enabled: bool = True
    fos_region: str = "us-east-1"
    cdn_shield: str = ""
    fos_access_key_id: str = ""
    fos_secret_access_key: str = ""
    fos_bucket: str = ""
    logging_endpoint_name: str = "Fastly Log Analytics"
    rum_endpoint_name: str = "Fastly RUM Logs"
    rum_enabled: bool = False
    rum_log_period: int | None = None
    faro_version: str | None = None
    rum_custom_condition: str = ""
    cmcd: CmcdConfig = field(default_factory=CmcdConfig)
    scoring: ScoringConfig = field(default_factory=ScoringConfig)
    log_fields: LogFieldsConfig = field(default_factory=LogFieldsConfig)

    def __post_init__(self) -> None:
        """Validate cross-feature dependencies and field constraints."""
        if not (1 <= self.log_period <= 3600):
            raise ValueError(f"log_period must be in range [1, 3600], got {self.log_period}")
        if self.rum_log_period is not None and not (1 <= self.rum_log_period <= 3600):
            raise ValueError(f"rum_log_period must be in range [1, 3600], got {self.rum_log_period}")
        if not (1 <= self.sample_rate <= 100):
            raise ValueError(f"sample_rate must be in range [1, 100], got {self.sample_rate}")
        for field_dict in self.log_fields.custom_fields:
            field_name = field_dict.get("name", "")
            if not re.fullmatch(r"^[a-z_][a-z0-9_]*$", field_name):
                raise ValueError(f"Custom field name '{field_name}' must be lowercase alphanumeric + underscore")
        if self.cmcd.enabled and self.cmcd.mode not in ("query_string", "headers"):
            raise ValueError(f"cmcd.mode must be 'query_string' or 'headers', got {self.cmcd.mode!r}")
        if self.cmcd.enabled and self.cmcd.version not in (1, 2):
            raise ValueError(f"cmcd.version must be 1 or 2, got {self.cmcd.version}")
        if self.scoring.enabled and not self.scoring.domain:
            raise ValueError("scoring.enabled=True requires scoring.domain to be set")
        if self.scoring.enabled and not (400 <= self.scoring.enforce_status_code <= 599):
            raise ValueError(
                f"scoring.enforce_status_code must be in range [400, 599], got {self.scoring.enforce_status_code}"
            )

    @classmethod
    def from_config(cls, cfg: dict) -> FeatureState:
        """Parse raw config dict and construct FeatureState with automatic dependency injection."""
        if not (service_id := cfg.get("service_id")):
            raise KeyError("config missing 'service_id'")
        if (log_period_raw := cfg.get("log_period")) is None:
            raise KeyError("config missing 'log_period'")
        prov_cfg = cfg.get("provisioning", {})
        sample_rate_raw = prov_cfg.get("sample_rate") if "sample_rate" in prov_cfg else cfg.get("sample_rate")
        edge_only = bool(prov_cfg.get("edge_only") if "edge_only" in prov_cfg else cfg.get("edge_only", False))
        raw_cond = (
            prov_cfg.get("custom_condition") if "custom_condition" in prov_cfg else cfg.get("custom_condition", "")
        )
        rum_cfg = cfg.get("rum", {})
        rum_dict = rum_cfg if isinstance(rum_cfg, dict) else {}
        rum_lp_raw = rum_dict.get("log_period") if rum_dict.get("log_period") is not None else cfg.get("rum_log_period")
        rum_enabled = is_rum_enabled(cfg)
        cmcd_cfg: dict[str, Any] = cfg["cmcd"] if isinstance(cfg.get("cmcd"), dict) else {}
        cmcd_enabled = cfg.get("cmcd_enabled", cmcd_cfg.get("enabled", False))
        cmcd_config = CmcdConfig(
            enabled=cmcd_enabled,
            mode=cfg.get("cmcd_mode", cmcd_cfg.get("mode", "query_string")),
            version=cfg.get("cmcd_version", cmcd_cfg.get("version", 1)),
        )
        scoring_cfg: dict[str, Any] = cfg["scoring"] if isinstance(cfg.get("scoring"), dict) else {}
        scoring_enabled = cfg.get("scoring_enabled", scoring_cfg.get("enabled", False))
        scoring_config = ScoringConfig(
            enabled=scoring_enabled,
            domain=cfg.get("scoring_domain") or scoring_cfg.get("domain") or scoring_cfg.get("scoring_domain") or "",
            request_secret=cfg.get("scoring_request_secret", scoring_cfg.get("request_secret", "")),
            exclude_url_regex=cfg.get("scoring_exclude_url_regex", scoring_cfg.get("exclude_url_regex", "")),
            enforce_status_code=cfg.get("scoring_enforce_status_code", scoring_cfg.get("enforce_status_code", 429)),
        )
        lf_cfg: dict[str, Any] = cfg["log_fields"] if isinstance(cfg.get("log_fields"), dict) else {}
        log_groups = lf_cfg.get("groups") or cfg.get("log_groups") or cfg.get("groups") or []
        custom_fields = lf_cfg.get("custom_fields") or cfg.get("custom_fields") or []
        field_overrides = lf_cfg.get("field_overrides") or cfg.get("field_overrides") or {}
        injected_fields = list(custom_fields) if custom_fields else []
        for feat_on, mod_path, attr, as_copy in (
            (rum_enabled, "backend.provision.rum_orchestrator_v2", "_RUM_CUSTOM_FIELDS", False),
            (scoring_enabled, "backend.provision.session_scoring_orchestrator", "_SCORING_CUSTOM_FIELDS", True),
            (cmcd_enabled, "backend.provision.cmcd_fields", "_CMCD_CUSTOM_FIELDS", True),
        ):
            if feat_on:
                existing_names = {f.get("name") for f in injected_fields}
                for f in getattr(__import__(mod_path, fromlist=[attr]), attr):
                    if f["name"] not in existing_names:
                        injected_fields.append(dict(f) if as_copy else f)
        log_fields_config = TypeAdapter(LogFieldsConfig).validate_python(
            {"groups": log_groups, "custom_fields": injected_fields, "field_overrides": field_overrides}
        )
        return cls(
            service_id=service_id,
            log_period=int(log_period_raw),
            sample_rate=int(100 if sample_rate_raw is None else sample_rate_raw),
            edge_only=edge_only,
            custom_condition=(raw_cond or "").strip(),
            fos_prefix=cfg.get("fos_prefix", ""),
            fos_endpoint=cfg.get("fos_endpoint", "fos.example.com"),
            logging_enabled=cfg.get("logging_enabled", True),
            fos_region=cfg.get("fos_region", "us-east-1"),
            cdn_shield=cfg.get("cdn_shield", ""),
            fos_access_key_id=cfg.get("fos_access_key_id", ""),
            fos_secret_access_key=cfg.get("fos_secret_access_key", ""),
            fos_bucket=cfg.get("fos_bucket", "") or cfg.get("fos_bucket_name", ""),
            logging_endpoint_name=prov_cfg.get("endpoint_name", "") or cfg.get("endpoint_name", "Fastly Log Analytics"),
            rum_endpoint_name=rum_cfg.get("endpoint_name", "") or cfg.get("rum_endpoint_name", "Fastly RUM Logs"),
            rum_enabled=rum_enabled,
            rum_log_period=int(rum_lp_raw) if rum_lp_raw is not None else None,
            faro_version=rum_dict.get("faro_version"),
            rum_custom_condition=rum_dict.get("custom_condition", "").strip(),
            cmcd=cmcd_config,
            scoring=scoring_config,
            log_fields=log_fields_config,
        )

    def to_dict(self) -> dict:
        """Serialize to a dictionary suitable for JSON storage."""
        base = {k: v for k, v in asdict(self).items() if k not in ("cmcd", "scoring", "log_fields")}
        return {
            **base,
            "cmcd_enabled": self.cmcd.enabled,
            "cmcd_mode": self.cmcd.mode,
            "cmcd_version": self.cmcd.version,
            "scoring_enabled": self.scoring.enabled,
            "scoring_domain": self.scoring.domain,
            "scoring_request_secret": self.scoring.request_secret,
            "scoring_exclude_url_regex": self.scoring.exclude_url_regex,
            "scoring_enforce_status_code": self.scoring.enforce_status_code,
            "log_groups": self.log_fields.groups,
            "custom_fields": self.log_fields.custom_fields,
            "field_overrides": self.log_fields.field_overrides,
        }


_MIGRATE_SPECS: tuple[tuple[str, tuple[tuple[str, str, Any], ...]], ...] = (
    (
        "cmcd",
        (("cmcd_enabled", "enabled", False), ("cmcd_mode", "mode", "query_string"), ("cmcd_version", "version", 1)),
    ),
    (
        "scoring",
        (
            ("scoring_enabled", "enabled", False),
            ("scoring_domain", "domain", ""),
            ("scoring_request_secret", "request_secret", ""),
            ("scoring_exclude_url_regex", "exclude_url_regex", ""),
            ("scoring_enforce_status_code", "enforce_status_code", 429),
        ),
    ),
)


def migrate_config(cfg: dict[str, Any]) -> dict[str, Any]:
    """Migrate flat CMCD/Scoring fields to nested structure."""
    if not cfg:
        return cfg
    result = dict(cfg)
    for section, fields in _MIGRATE_SPECS:
        if any(flat_k in result for flat_k, _, _ in fields):
            nested = result[section] if isinstance(result.get(section), dict) else {}
            for flat_k, nested_k, default in fields:
                nested[nested_k] = result.pop(flat_k, nested.get(nested_k, default))
            result[section] = nested
    return result


def config_changed(before: dict[str, Any], after: dict[str, Any]) -> bool:
    """Detect if migration changed the config structure."""
    has_flat_keys = any(flat_key in before for _, fields in _MIGRATE_SPECS for flat_key, _, _ in fields)
    return has_flat_keys and before != after


fastly_integration = sys.modules[__name__]


def upload_rum_tracker_js(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from backend.provision import orchestrator as _orch_mod

    return _orch_mod.upload_rum_tracker_js(*args, **kwargs)


logger = logging.getLogger(__name__)


@dataclass(unsafe_hash=True)
class VCLSnippet:
    """A single VCL snippet (name, priority, body, subroutine)."""

    name: str
    priority: int
    body: str
    subroutine: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class LoggingEndpoint:
    """A single logging endpoint (S3, etc)."""

    name: str
    endpoint_type: str
    path: str
    period: int
    response_condition: str
    format_string: str
    placement: str | None
    response_object_name: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "type": self.endpoint_type,
            "path": self.path,
            "period": self.period,
            "response_condition": self.response_condition,
            "format": self.format_string,
            "placement": self.placement,
            "response_object_name": self.response_object_name,
        }

    def __hash__(self) -> int:
        return hash((self.name, self.endpoint_type, self.path, self.period))


@dataclass
class Backend:
    """A single backend definition."""

    name: str
    address: str
    port: int
    ssl_check_cert: bool = True
    ssl_hostname: str = ""
    connect_timeout: int = 1000
    first_byte_timeout: int = 15000
    between_bytes_timeout: int = 10000
    auto_loadbalance: bool = True
    use_ssl: bool = True
    override_host: str = ""
    shield: str = ""
    request_condition: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def __hash__(self) -> int:
        return hash((self.name, self.address, self.port))


@dataclass
class ServiceDictionary:
    """A single Fastly service dictionary (ConfigStore)."""

    name: str
    write_only: bool = field(default=False, compare=False)
    items: dict[str, str] = field(default_factory=dict)

    def __hash__(self) -> int:
        return hash((self.name, tuple(sorted(self.items.items()))))


_DIFF_KINDS = ("snippets", "endpoints", "backends", "dictionaries")


@dataclass
class DiffResult:
    """Computed diff between current and desired state."""

    snippets_to_add: list[VCLSnippet] = field(default_factory=list)
    snippets_to_update: list[VCLSnippet] = field(default_factory=list)
    snippets_to_remove: list[str] = field(default_factory=list)
    endpoints_to_add: list[LoggingEndpoint] = field(default_factory=list)
    endpoints_to_update: list[LoggingEndpoint] = field(default_factory=list)
    endpoints_to_remove: list[str] = field(default_factory=list)
    backends_to_add: list[Backend] = field(default_factory=list)
    backends_to_update: list[Backend] = field(default_factory=list)
    backends_to_remove: list[str] = field(default_factory=list)
    dictionaries_to_add: list[ServiceDictionary] = field(default_factory=list)
    dictionaries_to_update: list[ServiceDictionary] = field(default_factory=list)
    dictionaries_to_remove: list[str] = field(default_factory=list)

    def is_empty(self) -> bool:
        """True if no changes needed."""
        for kind in _DIFF_KINDS:
            for op in ("add", "update", "remove"):
                if getattr(self, f"{kind}_to_{op}"):
                    return False
        return True

    def summary(self) -> dict[str, Any]:
        """Return a summary of changes for logging."""
        return {
            f"{k}_{op}": len(getattr(self, f"{k}_to_{src}"))
            for k in _DIFF_KINDS
            for op, src in (("added", "add"), ("updated", "update"), ("removed", "remove"))
        }


def _diff_named_resources(
    current_items: list[Any], desired_items: list[Any], label: str
) -> tuple[list[Any], list[Any], list[str]]:
    cur = {item.name: item for item in current_items}
    des = {item.name: item for item in desired_items}
    to_add = [item for name, item in des.items() if name not in cur]
    to_update = [item for name, item in des.items() if name in cur and cur[name] != item]
    to_remove = [name for name in cur if name not in des]
    if overlap := ({i.name for i in to_add} & {i.name for i in to_update}):
        raise RuntimeError(f"{label} in both to_add and to_update: {overlap}")
    return to_add, to_update, to_remove


def compute_diff(
    current_snippets: list[VCLSnippet],
    desired_snippets: list[VCLSnippet],
    current_endpoints: list[LoggingEndpoint],
    desired_endpoints: list[LoggingEndpoint],
    current_backends: list[Backend],
    desired_backends: list[Backend],
    current_dictionaries: list[ServiceDictionary] | None = None,
    desired_dictionaries: list[ServiceDictionary] | None = None,
) -> DiffResult:
    """Compute diff between current and desired state."""
    diff = DiffResult()
    for attr, cur, des, label in (
        ("snippets", current_snippets, desired_snippets, "Snippets"),
        ("endpoints", current_endpoints, desired_endpoints, "Endpoints"),
        ("backends", current_backends, desired_backends, "Backends"),
        ("dictionaries", current_dictionaries or [], desired_dictionaries or [], "Dictionaries"),
    ):
        add, upd, rem = _diff_named_resources(cur, des, label)
        setattr(diff, f"{attr}_to_add", add)
        setattr(diff, f"{attr}_to_update", upd)
        setattr(diff, f"{attr}_to_remove", rem)
    return diff


MANAGED_BACKEND_NAMES = {"session_scorer", "fos_origin", "F_fos_origin", "F_F_fos_origin", "rum_collector"}
MANAGED_DICTIONARY_NAMES = {"fos_credentials"}


class VclValidationError(Exception):
    """Raised when Fastly VCL validation fails."""

    def __init__(self, service_id: str, draft_version: int, errors: str) -> None:
        self.service_id = service_id
        self.draft_version = draft_version
        self.errors = errors
        super().__init__(f"VCL validation failed for {service_id} v{draft_version}: {errors}")


class LockAcquisitionTimeout(Exception):
    """Raised when VCL lock cannot be acquired within timeout."""

    def __init__(self, service_id: str, elapsed_seconds: float) -> None:
        self.service_id = service_id
        self.elapsed_seconds = elapsed_seconds
        super().__init__(f"Failed to acquire VCL lock for {service_id} after {elapsed_seconds:.1f}s")


@dataclass
class ReconciliationResult:
    """Result of a reconciliation run."""

    service_id: str
    activated_version: int | None = None
    draft_version: int | None = None
    changes_applied: dict[str, Any] | None = None
    error: str | None = None
    duration_ms: int = 0
    audit_log_id: int | None = None

    def __post_init__(self) -> None:
        if self.changes_applied is None:
            self.changes_applied = {}


class FileLock:
    """Simple file-based lock for concurrency control."""

    def __init__(self, lock_path: Path) -> None:
        self.lock_path = lock_path

    def __enter__(self) -> FileLock:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.lock_path.exists():
            self.lock_path.unlink()


def acquire_vcl_lock(service_id: str, timeout_sec: int = 300) -> FileLock:
    """Acquire a file-based lock for reconciliation."""
    from contextlib import suppress

    from tenacity import Retrying, retry_if_exception_type, wait_exponential

    lock_path = Path(f"data/services/{service_id}.vcl.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    start = time.time()
    for attempt in Retrying(
        wait=wait_exponential(multiplier=0.1, exp_base=1.5, min=0.1, max=1.0),
        retry=retry_if_exception_type(FileExistsError),
        sleep=time.sleep,
        reraise=True,
    ):
        with attempt:
            try:
                fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
                os.write(fd, f"{os.getpid()} {int(time.time())}".encode())
                os.close(fd)
                return FileLock(lock_path)
            except FileExistsError:
                if (elapsed := time.time() - start) > timeout_sec:
                    raise LockAcquisitionTimeout(service_id, elapsed)
                with suppress(FileNotFoundError):
                    if time.time() - lock_path.stat().st_mtime > 300:
                        lock_path.unlink(missing_ok=True)
                raise
    raise LockAcquisitionTimeout(service_id, time.time() - start)


def _upload_rum_tracker_best_effort(service_id: str, token: str, status_cb: Callable[[str], None] | None) -> None:
    """Upload the RUM tracker JS, logging (not raising) on failure."""
    if status_cb:
        status_cb("⏳ Ensuring RUM tracker JS is uploaded to FOS...")
    try:
        upload_rum_tracker_js(service_id, token, status_cb=status_cb)
    except Exception as e:
        logger.warning(f"Failed to upload RUM tracker JS: {e}")
        if status_cb:
            status_cb(f"⚠️  RUM JS upload warning: {e}")


def _update_local_config(service_id: str, mutate: Callable[[dict[str, Any]], None]) -> None:
    """Best-effort locked edit of the local config cache."""
    from backend.config import mutate_config

    try:
        mutate_config(service_id, mutate)
    except OSError as e:
        logger.info("Skipped updating read-only config cache: %s", e)


_FORMAT_KEY_RE = re.compile(r'"([A-Za-z0-9_]+)"\s*:')


def _format_field_names(endpoints: list[LoggingEndpoint]) -> set[str]:
    fields: set[str] = set()
    for ep in endpoints:
        if ep.format_string:
            fields.update(_FORMAT_KEY_RE.findall(ep.format_string))
    return fields


def build_rum_log_condition(desired_state: FeatureState | None) -> str:
    """Build the VCL statement for the RUM beacon log-emission response condition."""
    statement = (
        'req.url.path == "/rum-beacon" && req.method == "POST" '
        "&& fastly.ff.visits_this_service == 0 && req.restarts == 0"
    )
    if (custom := getattr(desired_state, "rum_custom_condition", None)) and custom.strip():
        statement += f" && ({custom.strip()})"
    return statement


def _record_reconcile_audit(
    service_id: str,
    diff: DiffResult,
    current_endpoints: list[LoggingEndpoint],
    desired_endpoints: list[LoggingEndpoint],
    version: int,
) -> None:
    """Write a ``log_format_change`` audit row for an activated reconcile."""
    before = _format_field_names(current_endpoints)
    after = _format_field_names(desired_endpoints)
    details = {
        "source": "reconciler",
        "version": version,
        "groups_before": [],
        "groups_after": [],
        "fields_added": sorted(after - before),
        "fields_removed": sorted(before - after),
        "changes": diff.summary(),
    }
    try:
        from backend.core import metadata as metadata_db

        metadata_db.record_audit(service_id, "log_format_change", details, actor="reconciler")
    except Exception:
        logger.warning("Failed to record reconcile audit for %s", service_id, exc_info=True)


def _load_or_bootstrap_feature_state(service_id: str, token: str, *, persist_bootstrap: bool = False) -> FeatureState:
    from backend.config import config_lock, save_config
    from backend.config import config_path as _config_path

    config_path = _config_path(service_id)
    if not config_path.exists():
        state = _bootstrap_featurestate_from_fastly(service_id, token)
        if persist_bootstrap:
            try:
                with config_lock(service_id):
                    if not config_path.exists():
                        save_config(service_id, asdict(state))
            except OSError as e:
                logging.getLogger("backend.scheduler").info(f"Skipped updating read-only config cache: {e}")
        return state
    return FeatureState.from_config(json.loads(config_path.read_text()))


def reconcile_vcl_state(
    service_id: str,
    token: str,
    dry_run: bool = False,
    status_cb: Callable[[str], None] | None = None,
    activate: bool = True,
) -> ReconciliationResult:
    """Execute the complete reconciliation control loop (Steps 1-8)."""
    result = ReconciliationResult(service_id=service_id)
    start_time = time.time()
    try:
        with acquire_vcl_lock(service_id):
            desired_state = _load_or_bootstrap_feature_state(service_id, token, persist_bootstrap=True)
            if status_cb:
                status_cb("⏳ Fetching active configuration from Fastly...")
            current_snippets = _fetch_snippets(service_id, token)
            current_endpoints = _fetch_logging_endpoints(service_id, token)
            current_backends = [b for b in _fetch_backends(service_id, token) if b.name in MANAGED_BACKEND_NAMES]
            current_dictionaries = [
                d for d in _fetch_dictionaries(service_id, token) if d.name in MANAGED_DICTIONARY_NAMES
            ]
            if status_cb:
                status_cb("⏳ Computing VCL configuration difference...")
            desired_snippets_list = desired_snippets(desired_state)
            desired_endpoints_list = desired_logging_endpoints(desired_state)
            desired_backends_list = desired_backends(desired_state)
            desired_dictionaries_list = desired_dictionaries(desired_state)
            for ep in desired_endpoints_list:
                if ep.format_string and len(ep.format_string) > 12288:
                    raise VclValidationError(
                        service_id=service_id,
                        draft_version=0,
                        errors=(
                            f"LOG_FORMAT_TOO_LONG: Logging endpoint '{ep.name}' format string length is {len(ep.format_string)} characters, "
                            "which exceeds Fastly's maximum allowed limit of 12288 characters. "
                            "Please disable some unused log field groups or shorten custom field expressions to fit within the limit."
                        ),
                    )
            diff = compute_diff(
                current_snippets,
                desired_snippets_list,
                current_endpoints,
                desired_endpoints_list,
                current_backends,
                desired_backends_list,
                current_dictionaries,
                desired_dictionaries_list,
            )
            _detect_and_queue_legacy_cleanup(current_snippets, diff, status_cb)
            diff.snippets_to_remove = [n for n in diff.snippets_to_remove if _is_managed_snippet(n)]
            if diff.is_empty():
                if desired_state.rum_enabled:
                    _upload_rum_tracker_best_effort(service_id, token, status_cb)
                return _finalize_reconcile_early_exit(result, service_id, token, start_time)
            if dry_run:
                result.changes_applied = diff.summary()
                return _finalize_reconcile_early_exit(result, service_id, token, start_time)
            draft_version = None
            try:
                if status_cb:
                    status_cb("⏳ Cloning active version to draft...")
                draft_version = _clone_active_version(service_id, token, desired_state)
                if status_cb:
                    status_cb("⏳ Applying custom snippets and VCL log format...")
                _apply_diff(
                    service_id,
                    token,
                    draft_version,
                    diff,
                    desired_endpoints=desired_endpoints_list,
                    desired_state=desired_state,
                    status_cb=status_cb,
                    current_snippets=current_snippets,
                )
                if status_cb:
                    status_cb("⏳ Validating draft configuration...")
                if validation_errors := _validate_draft(service_id, token, draft_version):
                    raise VclValidationError(service_id, draft_version, validation_errors)
                result.draft_version = draft_version
                if activate:
                    if status_cb:
                        status_cb("⏳ Activating draft version...")
                    _activate_draft(service_id, token, draft_version)
                    result.activated_version = draft_version
                    _record_reconcile_audit(service_id, diff, current_endpoints, desired_endpoints_list, draft_version)
                    if desired_state.rum_enabled:
                        _upload_rum_tracker_best_effort(service_id, token, status_cb)
                    _purge_rum_surrogate_key(service_id, token)

                    def _set_last_activated_version(c: dict[str, Any]) -> None:
                        c["last_activated_version"] = draft_version

                    _update_local_config(service_id, _set_last_activated_version)
                elif status_cb:
                    status_cb(
                        f"✓ Configuration compiled and validated successfully in draft version {draft_version} (activation bypassed)."
                    )
                result.changes_applied = diff.summary()
            except Exception as rollback_err:
                if draft_version is not None:
                    if status_cb:
                        status_cb(
                            f"🧹 Reconcile failed: {rollback_err}. Broken draft version {draft_version} left as inactive draft (Fastly does not support deleting individual draft versions)."
                        )
                    logging.getLogger("backend.scheduler").warning(
                        f"Reconcile failed: {rollback_err}. Broken draft version {draft_version} left in Fastly configuration."
                    )
                raise rollback_err
    finally:
        result.duration_ms = int((time.time() - start_time) * 1000)
    return result


def _finalize_reconcile_early_exit(
    result: ReconciliationResult,
    service_id: str,
    token: str,
    start_time: float,
    *,
    active_ver: int | None = None,
    lookup_active: bool = True,
) -> ReconciliationResult:
    if lookup_active:
        try:
            result.activated_version = _fetch_active_version(service_id, token)
        except RuntimeError:
            result.activated_version = None
    else:
        result.activated_version = active_ver
    result.duration_ms = int((time.time() - start_time) * 1000)
    return result


def reconcile_cdn_service_state(
    logging_service_id: str,
    token: str,
    dry_run: bool = False,
    status_cb: Callable[[str], None] | None = None,
    activate: bool = True,
) -> ReconciliationResult:
    """Execute the complete reconciliation control loop for the FOS Proxy / CDN service (Steps 1-8)."""
    from backend.config import config_path as _config_path
    from backend.core.fastly.client import SHIELD_MAP, load_vcl, region_endpoint
    from backend.provision.fastly_api import _CDN_SNIPPETS

    cdn_managed_backend_names = {"fos_origin"}
    cdn_managed_dictionary_names = {"fos_credentials", "cdn_auth"}
    start_time = time.time()
    config_path = _config_path(logging_service_id)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file {config_path} not found.")
    cfg = json.loads(config_path.read_text())
    fos_proxy_cfg = cfg.get("fos_proxy", {})
    cdn_service_id = fos_proxy_cfg.get("service_id") or cfg.get("cdn_service_id", "")
    cdn_service_name = (
        fos_proxy_cfg.get("service_name")
        or cfg.get("cdn_service_name", "")
        or f"Log Analysis CDN Service for {logging_service_id}"
    )
    cdn_url = fos_proxy_cfg.get("domain") or cfg.get("cdn_url", "")
    cdn_shield = fos_proxy_cfg.get("shield") or cfg.get("cdn_shield", "")
    cdn_secret = fos_proxy_cfg.get("secret") or cfg.get("cdn_secret", "")
    rate_limiting_enabled = (
        fos_proxy_cfg["rate_limiting_enabled"]
        if "rate_limiting_enabled" in fos_proxy_cfg
        else cfg.get("rate_limiting", True)
    )
    fos_access_key = cfg.get("fos_access_key_id", "")
    fos_secret_key = cfg.get("fos_secret_access_key", "")
    fos_bucket = cfg.get("fos_bucket", "") or cfg.get("fos_bucket_name", "")
    fos_region = cfg.get("fos_region", "us-east-1")
    fos_host = region_endpoint(fos_region)
    if not cdn_url:
        raise ValueError("CDN URL is required to reconcile CDN service state.")
    domain = cdn_url.replace("https://", "").replace("http://", "").split("/")[0]
    if not cdn_service_id:
        if status_cb:
            status_cb(f"⏳ Finding existing CDN service named '{cdn_service_name}'...")
        from backend.core.fastly import client as _fastly_service

        _find_fn = (
            _fastly_service.find_service_by_name
            if _fastly_service.find_service_by_name is not _orig_find_service_by_name
            else find_service_by_name
        )
        existing = _find_fn(cdn_service_name, token)
        if existing and existing.get("id") == logging_service_id:
            existing = None
            if status_cb:
                status_cb(
                    f"⚠️ Ignoring service {logging_service_id} as CDN candidate; the analytics CDN must be a separate service."
                )
        if existing:
            cdn_service_id = existing["id"]
            if status_cb:
                status_cb(f"✓ Found existing CDN service: {cdn_service_id}")
            if not dry_run:
                _ensure_cdn_domain(cdn_service_id, domain, token, status_cb=status_cb)
        else:
            if dry_run:
                if status_cb:
                    status_cb(f"ℹ️ [Dry Run] Would create new CDN service '{cdn_service_name}'")
                return ReconciliationResult(service_id="dry-run-cdn")
            if status_cb:
                status_cb(f"⏳ Creating new Fastly CDN service '{cdn_service_name}'...")
            svc = fastly("POST", "/service", {"name": cdn_service_name, "type": "vcl"}, token=token)
            cdn_service_id = svc["id"]
            if status_cb:
                status_cb(f"✓ Created CDN service {cdn_service_id}")
            service_comment = (
                f"CDN fronting service for the Fastly Object Storage log bucket associated with "
                f"service {logging_service_id}. Provides authenticated read access to stored log "
                f"files for the Fastly Log Analysis tool."
            )
            fastly("PUT", f"/service/{cdn_service_id}", {"comment": service_comment}, token=token)
            if status_cb:
                status_cb(f"⏳ Adding domain '{domain}' to CDN service...")
            fastly(
                "POST",
                f"/service/{cdn_service_id}/version/1/domain",
                {"name": domain, "comment": "Log Analysis CDN"},
                token=token,
            )
            if status_cb:
                status_cb("✓ Domain added to CDN service")

        def _persist_cdn(c: dict[str, Any]) -> None:
            c.setdefault("fos_proxy", {}).update(
                {
                    "service_id": cdn_service_id,
                    "service_name": cdn_service_name,
                    "domain": cdn_url,
                    "shield": cdn_shield,
                    "secret": cdn_secret,
                }
            )
            c["cdn_service_id"] = cdn_service_id

        _update_local_config(logging_service_id, _persist_cdn)
    result = ReconciliationResult(service_id=cdn_service_id)
    try:
        with acquire_vcl_lock(cdn_service_id):
            if status_cb:
                status_cb("⏳ Fetching active CDN configuration from Fastly...")
            current_snippets = _fetch_snippets(cdn_service_id, token)
            current_backends = [
                b for b in _fetch_backends(cdn_service_id, token) if b.name in cdn_managed_backend_names
            ]
            current_dictionaries = [
                d for d in _fetch_dictionaries(cdn_service_id, token) if d.name in cdn_managed_dictionary_names
            ]
            active_ver = fastly_integration.fetch_active_version(cdn_service_id, token)
            current_vcl_content = ""
            if active_ver is not None:
                try:
                    vcl_resp = fastly("GET", f"/service/{cdn_service_id}/version/{active_ver}/vcl/main", token=token)
                    if isinstance(vcl_resp, dict):
                        current_vcl_content = vcl_resp.get("content", "")
                except Exception:
                    pass
            desired_vcl_content = load_vcl(rate_limiting=rate_limiting_enabled)
            desired_snippets_list = [
                VCLSnippet(name=name, subroutine=stype, body=content, priority=priority)
                for name, stype, content, priority in _CDN_SNIPPETS
            ]
            shield_pop = cdn_shield or SHIELD_MAP.get(fos_region, "iad-va-us")
            shield_val = shield_pop if shield_pop.lower() != "none" else ""
            desired_backends_list = [
                Backend(
                    name="fos_origin",
                    address=fos_host,
                    port=443,
                    ssl_check_cert=True,
                    ssl_hostname=fos_host,
                    use_ssl=True,
                    override_host=fos_host,
                    auto_loadbalance=False,
                    shield=shield_val,
                    connect_timeout=5000,
                    first_byte_timeout=60000,
                    between_bytes_timeout=30000,
                )
            ]
            fos_items = (
                {"access_key": fos_access_key, "secret_key": fos_secret_key, "bucket": fos_bucket, "region": fos_region}
                if fos_access_key
                else {}
            )
            desired_dictionaries_list = [
                ServiceDictionary(name="fos_credentials", write_only=True, items=fos_items),
                ServiceDictionary(name="cdn_auth", write_only=True, items={"secret": cdn_secret} if cdn_secret else {}),
            ]
            diff = compute_diff(
                current_snippets,
                desired_snippets_list,
                [],
                [],
                current_backends,
                desired_backends_list,
                current_dictionaries,
                desired_dictionaries_list,
            )
            vcl_needs_update = current_vcl_content != desired_vcl_content
            if diff.is_empty() and not vcl_needs_update:
                if status_cb:
                    status_cb("✓ CDN service configuration is up-to-date and matches desired state. No changes needed.")
                return _finalize_reconcile_early_exit(
                    result, cdn_service_id, token, start_time, active_ver=active_ver, lookup_active=False
                )
            if dry_run:
                result.changes_applied = {**diff.summary(), "main_vcl_updated": vcl_needs_update}
                return _finalize_reconcile_early_exit(
                    result, cdn_service_id, token, start_time, active_ver=active_ver, lookup_active=False
                )
            draft_version = None
            try:
                if status_cb:
                    status_cb("⏳ Cloning active version to draft...")
                draft_version = _clone_active_version(cdn_service_id, token)
                if status_cb:
                    status_cb("⏳ Applying custom snippets and dictionaries...")
                _apply_diff(
                    cdn_service_id,
                    token,
                    draft_version,
                    diff,
                    desired_endpoints=[],
                    desired_state=None,
                    status_cb=status_cb,
                    current_snippets=current_snippets,
                )
                if vcl_needs_update:
                    if status_cb:
                        status_cb("⏳ Updating main VCL content...")
                    try:
                        fastly(
                            "PUT",
                            f"/service/{cdn_service_id}/version/{draft_version}/vcl/main",
                            {"content": desired_vcl_content},
                            token=token,
                        )
                    except Exception:
                        fastly(
                            "POST",
                            f"/service/{cdn_service_id}/version/{draft_version}/vcl",
                            {"name": "main", "content": desired_vcl_content, "main": True},
                            token=token,
                        )
                if status_cb:
                    status_cb("⏳ Validating draft configuration...")
                if validation_errors := _validate_draft(cdn_service_id, token, draft_version):
                    raise VclValidationError(cdn_service_id, draft_version, validation_errors)
                result.draft_version = draft_version
                if activate:
                    if status_cb:
                        status_cb("⏳ Activating draft version...")
                    _activate_draft(cdn_service_id, token, draft_version)
                    result.activated_version = draft_version

                    def _persist_activation(c: dict[str, Any]) -> None:
                        c.setdefault("fos_proxy", {})["last_activated_version"] = draft_version
                        c["last_activated_version_cdn"] = draft_version

                    _update_local_config(logging_service_id, _persist_activation)
                elif status_cb:
                    status_cb(
                        f"✓ CDN configuration compiled and validated successfully in draft version {draft_version} (activation bypassed)."
                    )
                result.changes_applied = {**diff.summary(), "main_vcl_updated": vcl_needs_update}
            except Exception as rollback_err:
                if draft_version is not None and status_cb:
                    status_cb(
                        f"🧹 Reconcile failed: {rollback_err}. Broken draft version {draft_version} left as inactive draft."
                    )
                raise rollback_err
    finally:
        result.duration_ms = int((time.time() - start_time) * 1000)
    return result


from backend.core.fastly.client import (  # noqa: E402
    RUM_ASSET_FETCH_NAME,
    RUM_FARO_FETCH_NAME,
    generate_rum_asset_fetch_vcl,
    generate_rum_recv_parts,
    generate_rum_vcl,
)
from backend.provision import cli as log_paths  # noqa: E402
from backend.provision.cli import VclBuilder  # noqa: E402
from backend.provision.fastly_api import generate_capture_vcl  # noqa: E402
from backend.provision.session_scoring_orchestrator import (  # noqa: E402
    SCORING_BACKEND_API_NAME,
    generate_scoring_vcl,
)


def _state_log_fields_dict(state: FeatureState) -> dict[str, Any]:
    return {
        "groups": state.log_fields.groups,
        "custom_fields": state.log_fields.custom_fields,
        "field_overrides": state.log_fields.field_overrides,
    }


def logging_service_snippets(state: FeatureState) -> list[VCLSnippet]:
    """Return capture snippets for the logging service."""
    if not state.logging_enabled:
        return []
    capture_vcl_dict = generate_capture_vcl(
        _state_log_fields_dict(state),
        scoring_enabled=state.scoring.enabled,
        rum_enabled=state.rum_enabled,
        scoring_exclude_url_regex=state.scoring.exclude_url_regex,
        cmcd_enabled=state.cmcd.enabled,
        cmcd_mode=state.cmcd.mode,
        cmcd_version=state.cmcd.version,
    )
    subroutine_map = {
        "recv": ("Fastly Log Analysis Capture", 1),
        "recv_reset": ("Fastly Log Analysis Reset Client IP", -100),
        "miss": ("Fastly Log Analysis Miss", 10),
        "pass": ("Fastly Log Analysis Pass", 10),
        "fetch": ("Fastly Log Analysis Origin Fetch", 10),
        "deliver": ("Fastly Log Analysis Origin Deliver", 10),
        "error": ("Fastly Log Analysis Origin Error", 10),
    }
    return [
        VCLSnippet(
            name=snippet_name,
            priority=priority,
            body=capture_vcl_dict[subroutine_key],
            subroutine="vcl_recv" if subroutine_key == "recv_reset" else f"vcl_{subroutine_key}",
        )
        for subroutine_key, (snippet_name, priority) in subroutine_map.items()
        if subroutine_key in capture_vcl_dict
    ]


def desired_snippets(state: FeatureState) -> list[VCLSnippet]:
    """Return the 5 consolidated snippets (vcl_recv, vcl_miss, vcl_fetch, vcl_deliver, vcl_error)."""
    snippets: list[VCLSnippet] = []
    for subroutine in ("vcl_recv", "vcl_miss", "vcl_pass", "vcl_fetch", "vcl_deliver", "vcl_error"):
        body = generate_consolidated_snippet(state, subroutine)
        snippets.append(
            VCLSnippet(name=f"Fastly Log Analytics - {subroutine}", priority=10, body=body, subroutine=subroutine)
        )
        declared_vars = re.findall(r"^\s*declare\s+local\s+var\.(\w+)", body, re.MULTILINE)
        duplicates = [v for v in set(declared_vars) if declared_vars.count(v) > 1]
        if duplicates:
            raise RuntimeError(f"{subroutine}: duplicate var declarations {duplicates}")
    return snippets


def generate_consolidated_snippet(state: FeatureState, subroutine: str) -> str:
    """Generate consolidated VCL for a single subroutine."""
    if subroutine != "vcl_recv":
        return _legacy_generate_consolidated_snippet(state, subroutine)
    sections: list[str] = []
    edge_first_hop_statements: list[str] = []
    log_fields_dict = _state_log_fields_dict(state)
    if state.logging_enabled:
        from backend.provision.fastly_api import get_scrub_vcl_statements

        edge_first_hop_statements.extend(
            [
                "  # Section 6: Log Field Capture (vcl_recv)",
                "  # [security] strip client-supplied internal-routing headers",
                *(f"  {scrub}" for scrub in get_scrub_vcl_statements(log_fields_dict)),
            ]
        )
    if state.rum_enabled:
        edge_first_hop_statements.extend(
            [
                "  # Mint a per-request ID for RUM beacons on fresh requests",
                "  set req.http.x-rum-req-id = randomstr(12);",
            ]
        )
    if state.rum_enabled and state.logging_enabled:
        rum_extraction, _ = generate_rum_recv_parts()
        edge_first_hop_statements.extend(
            [
                "  # Section 5: RUM field extraction (vcl_recv)",
                '  if (req.url.path == "/rum-beacon") {',
                *(f"    {line}" for line in rum_extraction.splitlines()),
                "  }",
            ]
        )
    if state.logging_enabled:
        from backend.provision.fastly_api import get_capture_vcl_statements

        edge_first_hop_statements.append("")
        if state.cmcd.enabled:
            from backend.provision.cmcd_fields import generate_cmcd_vcl

            cmcd_body = next(iter(generate_cmcd_vcl(mode=state.cmcd.mode, version=state.cmcd.version).values()))
            edge_first_hop_statements.append("  # Section 3: CMCD Extraction (vcl_recv) — before field capture")
            edge_first_hop_statements.extend(f"  {line}" for line in cmcd_body.splitlines() if line.strip())
        edge_first_hop_statements.append("  # Capture edge data for logging before shielding or backend fetch")
        edge_first_hop_statements.extend(f"  {cap}" for cap in get_capture_vcl_statements(log_fields_dict))
    if state.rum_enabled:
        _, rum_response = generate_rum_recv_parts()
        edge_first_hop_statements.extend(
            [
                "  # Section 5: RUM (vcl_recv)",
                '  if (req.url.path == "/rum-beacon") {',
                *(f"    {line}" for line in rum_response.splitlines()),
                "  }",
            ]
        )
    if state.scoring.enabled:
        from backend.provision.session_scoring_orchestrator import SCORING_BACKEND_VCL_NAME, resolve_exclude_url_regex

        effective_regex = resolve_exclude_url_regex(state.scoring.exclude_url_regex)
        rum_asset_guard = (
            ' && req.url.path != "/js/rum.js" && req.url.path != "/js/faro-sdk.js"' if state.rum_enabled else ""
        )
        edge_first_hop_statements.extend(
            [
                "",
                "  # Section 4: Session Scoring (vcl_recv)",
                "  # Session Scoring: route the first-pass dynamic request to the scorer.",
                "  # Edge-only — fastly.ff.visits_this_service == 0 is true only at the true edge.",
                *(
                    ["  # RUM assets we serve ourselves are never scored — see rum_asset_guard in generators.py."]
                    if rum_asset_guard
                    else []
                ),
                f'  if (req.http.X-Edge-Scoring-Pass != "1" && !fastly.ddos_detected{rum_asset_guard}'
                f' && std.tolower(req.url) !~ "{effective_regex}") {{',
                f"    set req.backend = {SCORING_BACKEND_VCL_NAME};",
                "    # Skip NGWAF inspection on the scoring sub-fetch ONLY.",
                '    set req.http.x-sigsci-skip-inspection-once = "true";',
                '    set req.http.X-Edge-Scoring-Pass = "1";',
                "    # Stamp the round-trip start so deliver can compute latency.",
                "    set req.http.x-edge-score-t0 = time.elapsed.usec;",
                "    # PASS — skip cache for the scoring sub-fetch.",
                "    return(pass);",
                "  }",
            ]
        )
    if edge_first_hop_statements:
        body = VclBuilder.join_lines(edge_first_hop_statements)
        sections.append(f"if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {{\n{body}\n}}")
    if state.rum_enabled:
        from backend.core.fastly.client import SHIELD_MAP

        shield_pop = state.cdn_shield or SHIELD_MAP.get(state.fos_region, "iad-va-us")
        sections.append(generate_rum_asset_fetch_vcl(shield_pop, state.faro_version)[RUM_ASSET_FETCH_NAME])
    if state.scoring.enabled:
        sections.append(_generate_scoring_section_vcl(state, "vcl_recv"))
    return VclBuilder.join_sections(sections)


def _legacy_generate_consolidated_snippet(state: FeatureState, subroutine: str) -> str:
    """Generate consolidated VCL for other subroutines using legacy multi-section append logic."""
    sections: list[str] = []
    if state.logging_enabled:
        capture_snippets = generate_capture_vcl(
            _state_log_fields_dict(state),
            scoring_enabled=state.scoring.enabled,
            rum_enabled=state.rum_enabled,
            scoring_exclude_url_regex=state.scoring.exclude_url_regex,
            cmcd_enabled=state.cmcd.enabled,
            cmcd_mode=state.cmcd.mode,
            cmcd_version=state.cmcd.version,
        )
        capture_vcl = capture_snippets.get(subroutine.replace("vcl_", ""), "")
        if capture_vcl.strip():
            sections.append(f"# Section 6: Log Field Capture ({subroutine})\n{capture_vcl}")
    if state.scoring.enabled:
        sections.append(_generate_scoring_section_vcl(state, subroutine))
    if state.rum_enabled:
        sections.append(_generate_rum_section_vcl(state, subroutine))
    return VclBuilder.join_sections(sections)


def _generate_scoring_section_vcl(state: FeatureState, subroutine: str) -> str:
    """Section 4: Session Scoring (if enabled)."""
    if not state.scoring.enabled:
        return ""
    scoring_vcl_dict = generate_scoring_vcl(
        logging_service_id=state.service_id,
        request_secret=state.scoring.request_secret,
        exclude_url_regex=state.scoring.exclude_url_regex,
        enforce_status_code=state.scoring.enforce_status_code,
    )
    snippet_map = {
        "vcl_deliver": "Session Scoring - Deliver",
        "vcl_fetch": "Session Scoring - Fetch",
        "vcl_pass": "Session Scoring - Pass",
    }
    bodies = [scoring_vcl_dict[snippet_map[subroutine]]] if snippet_map.get(subroutine) in scoring_vcl_dict else []
    if subroutine == "vcl_recv":
        if "Session Scoring - Enforce" in scoring_vcl_dict:
            bodies.append(scoring_vcl_dict["Session Scoring - Enforce"])
        bodies.append(
            "# Session Scoring: post-scoring restart shielding & NGWAF restore.\n"
            "if (req.restarts == 1 && req.http.x-edge-score) {\n"
            "  set var.fastly_req_do_shield = true;\n"
            "}\n"
            "if (req.restarts == 1) {\n"
            "  unset req.http.x-sigsci-skip-inspection-once;\n"
            "}"
        )
    return f"# Section 4: Session Scoring ({subroutine})\n" + "\n\n".join(bodies) if bodies else ""


def _generate_rum_section_vcl(state: FeatureState, subroutine: str) -> str:
    """Section 5: RUM (if enabled, includes Phase 1 + Phase 3 asset-fetch)."""
    if not state.rum_enabled:
        return ""
    from backend.core.fastly.client import SHIELD_MAP

    rum_vcl_dict = generate_rum_vcl(state.service_id, state.faro_version)
    shield_pop = state.cdn_shield or SHIELD_MAP.get(state.fos_region, "iad-va-us")
    asset_fetch_dict = generate_rum_asset_fetch_vcl(shield_pop, state.faro_version)
    if subroutine == "vcl_recv":
        sections = ([rum_vcl_dict["RUM - Recv"]] if "RUM - Recv" in rum_vcl_dict else []) + [
            asset_fetch_dict["RUM - Asset fetch FOS"]
        ]
        return f"# Section 5: RUM ({subroutine})\n" + "\n\n".join(sections)
    if subroutine == "vcl_deliver":
        return f"# Section 5: RUM ({subroutine})\n{rum_vcl_dict['RUM - Set cookies']}" if state.scoring.enabled else ""
    if subroutine == "vcl_miss":
        return f"# Section 5: RUM ({subroutine})\n{asset_fetch_dict['RUM - Asset fetch SigV4 signing']}"
    if subroutine == "vcl_error":
        from backend.core.fastly.client import _generate_vcl_error_handler

        return f"# Section 5: RUM ({subroutine})\n{_generate_vcl_error_handler()}"
    if subroutine == "vcl_fetch":
        body = """# Cache RUM tracker JS at the edge; browsers get a short TTL so a
# Surrogate-Key purge of "rum-js" becomes visible to clients quickly.
if (req.url.path == "/js/rum.js") {
    if (beresp.status == 200) {
        set beresp.ttl = 86400s;
        set beresp.cacheable = true;
        set beresp.http.Surrogate-Key = "rum-js";
        set beresp.http.Surrogate-Control = "max-age=86400";
        set beresp.http.Cache-Control = "public, max-age=300";
    } else {
        set beresp.ttl = 0s;
        set beresp.cacheable = false;
    }
}"""
        if RUM_FARO_FETCH_NAME in asset_fetch_dict:
            body = body + "\n\n" + asset_fetch_dict[RUM_FARO_FETCH_NAME]
        return f"# Section 5: RUM ({subroutine})\n{body}"
    return ""


def generate_log_format(state: FeatureState) -> str:
    """Generate complete log format VCL expression."""
    from backend.provision.fastly_api import load_log_format
    from backend.provision.rum_orchestrator_v2 import _RUM_FIELD_NAMES

    custom_fields = [cf for cf in state.log_fields.custom_fields if cf.get("name") not in _RUM_FIELD_NAMES]
    return load_log_format(
        {
            "groups": state.log_fields.groups,
            "custom_fields": custom_fields,
            "field_overrides": state.log_fields.field_overrides,
        }
    )


def desired_logging_endpoints(state: FeatureState) -> list[LoggingEndpoint]:
    """Return desired logging endpoints (main S3 + secondary RUM if enabled)."""
    endpoints: list[LoggingEndpoint] = []
    if state.logging_enabled:
        endpoints.append(
            LoggingEndpoint(
                name=state.logging_endpoint_name,
                endpoint_type="s3",
                path=log_paths.analytics_log_path(),
                period=state.log_period,
                response_condition="log_analytics_condition",
                format_string=generate_log_format(state),
                placement=None,
                response_object_name="",
            )
        )
    if state.rum_enabled:
        from backend.provision.fastly_api import load_log_format
        from backend.provision.rum_orchestrator_v2 import _RUM_FIELD_NAMES

        rum_fields = [f for f in state.log_fields.custom_fields if f.get("name") in _RUM_FIELD_NAMES]
        endpoints.append(
            LoggingEndpoint(
                name=state.rum_endpoint_name,
                endpoint_type="s3",
                path=log_paths.rum_log_path(),
                period=state.rum_log_period if state.rum_log_period is not None else state.log_period,
                response_condition="rum_log_condition",
                format_string=load_log_format({"custom_fields": rum_fields}),
                placement=None,
                response_object_name="",
            )
        )
    return endpoints


def desired_backends(state: FeatureState) -> list[Backend]:
    """Return desired backends (session_scorer, fos_origin, rum_collector)."""
    backends: list[Backend] = []
    if state.scoring.enabled:
        backends.append(
            Backend(
                name=SCORING_BACKEND_API_NAME,
                address=state.scoring.domain,
                port=443,
                ssl_check_cert=False,
                ssl_hostname=state.scoring.domain,
                connect_timeout=100,
                first_byte_timeout=100,
                between_bytes_timeout=200,
                auto_loadbalance=False,
                request_condition="fastly_log_analytics_false",
            )
        )
    if state.rum_enabled:
        from backend.core.fastly.client import SHIELD_MAP

        shield_pop = state.cdn_shield or SHIELD_MAP.get(state.fos_region, "iad-va-us")
        backends.append(
            Backend(
                name="fos_origin",
                address=state.fos_endpoint,
                port=443,
                ssl_check_cert=True,
                ssl_hostname=state.fos_endpoint,
                use_ssl=True,
                override_host=state.fos_endpoint,
                auto_loadbalance=False,
                shield=shield_pop if shield_pop.lower() != "none" else "",
                request_condition="fastly_log_analytics_false",
            )
        )
    return backends


def desired_dictionaries(state: FeatureState) -> list[ServiceDictionary]:
    """Return desired Fastly service dictionaries (fos_credentials, etc)."""
    if not state.rum_enabled:
        return []
    items = (
        {
            "access_key": state.fos_access_key_id,
            "secret_key": state.fos_secret_access_key,
            "bucket": state.fos_bucket,
            "region": state.fos_region,
        }
        if state.fos_access_key_id
        else {}
    )
    return [ServiceDictionary(name="fos_credentials", write_only=True, items=items)]


def fetch_active_version(service_id: str, token: str) -> int | None:
    """Fetch the currently active version number."""
    try:
        active = fastly("GET", f"/service/{service_id}/version/active", token=token)
        if isinstance(active, dict) and active.get("active"):
            return int(active["number"])
    except RuntimeError:
        pass
    try:
        resp = fastly("GET", f"/service/{service_id}/version", token=token)
        for item in _unwrap_fastly_list(resp):
            if isinstance(item, dict) and item.get("active"):
                return int(item["number"])
        return None
    except Exception:
        return None


def _unwrap_fastly_list(resp: object) -> list[dict]:
    if isinstance(resp, dict) and "items" in resp:
        return list(resp["items"])
    return resp if isinstance(resp, list) else []


def _delete_version_resource(service_id: str, version: int, resource_path: str, name: str, token: str) -> None:
    encoded_name = urllib.parse.quote(name, safe="")
    fastly(
        "DELETE",
        f"/service/{service_id}/version/{version}/{resource_path}/{encoded_name}",
        token=token,
        expect_empty=True,
    )


def _upsert_version_resource(
    service_id: str, version: int, resource_path: str, name: str, body: dict, token: str
) -> dict:
    encoded_name = urllib.parse.quote(name, safe="")
    item_url = f"/service/{service_id}/version/{version}/{resource_path}/{encoded_name}"
    collection_url = f"/service/{service_id}/version/{version}/{resource_path}"
    try:
        fastly("GET", item_url, token=token)
        return fastly("PUT", item_url, body, token=token)
    except RuntimeError:
        return fastly("POST", collection_url, body, token=token)


def fetch_snippets(service_id: str, version: int, token: str) -> list[VCLSnippet]:
    """Fetch all VCL snippets for a service version."""
    try:
        resp = fastly("GET", f"/service/{service_id}/version/{version}/snippet", token=token)
        return [
            VCLSnippet(
                name=item.get("name", ""),
                priority=int(item.get("priority", 100)),
                body=item.get("snippet", ""),
                subroutine=item.get("type", "vcl_recv"),
            )
            for item in _unwrap_fastly_list(resp)
        ]
    except Exception:
        return []


def fetch_logging_endpoints(service_id: str, version: int, token: str) -> list[LoggingEndpoint]:
    """Fetch all S3 logging endpoints for a service version."""
    try:
        resp = fastly("GET", f"/service/{service_id}/version/{version}/logging/s3", token=token)
        return [
            LoggingEndpoint(
                name=item.get("name", ""),
                endpoint_type="s3",
                path=item.get("path", ""),
                period=int(item.get("period", 3600)),
                response_condition=item.get("response_condition", "true"),
                format_string=item.get("format", ""),
                placement=item.get("placement", "none"),
                response_object_name=item.get("response_object_name", ""),
            )
            for item in _unwrap_fastly_list(resp)
        ]
    except Exception:
        return []


def fetch_backends(service_id: str, version: int, token: str) -> list[Backend]:
    """Fetch all backends for a service version."""
    try:
        resp = fastly("GET", f"/service/{service_id}/version/{version}/backend", token=token)
        return [
            Backend(
                name=item.get("name", ""),
                address=item.get("address", ""),
                port=int(item.get("port", 443)),
                ssl_check_cert=item.get("ssl_check_cert", True),
                ssl_hostname=item.get("ssl_hostname", ""),
                connect_timeout=int(item.get("connect_timeout", 1000)),
                first_byte_timeout=int(item.get("first_byte_timeout", 15000)),
                between_bytes_timeout=int(item.get("between_bytes_timeout", 10000)),
                auto_loadbalance=item.get("auto_loadbalance", True),
                use_ssl=item.get("use_ssl", True),
                override_host=item.get("override_host", ""),
                shield=item.get("shield") or "",
                request_condition=item.get("request_condition") or "",
            )
            for item in _unwrap_fastly_list(resp)
        ]
    except Exception:
        return []


def fetch_dictionaries(service_id: str, version: int, token: str) -> list[ServiceDictionary]:
    """Fetch all service dictionaries (ConfigStore) for a service version."""
    dicts: list[ServiceDictionary] = []
    try:
        resp = fastly("GET", f"/service/{service_id}/version/{version}/dictionary", token=token)
        for item in _unwrap_fastly_list(resp):
            dict_id, entries_dict = item.get("id", ""), {}
            if dict_id:
                try:
                    items_resp = fastly("GET", f"/service/{service_id}/dictionary/{dict_id}/items", token=token)
                    items_entries = (
                        [items_resp]
                        if isinstance(items_resp, dict) and "items" not in items_resp
                        else _unwrap_fastly_list(items_resp)
                    )
                    for entry in items_entries:
                        if isinstance(entry, dict) and (key := entry.get("item_key", "") or entry.get("key", "")):
                            entries_dict[key] = entry.get("item_value", "") or entry.get("value", "")
                except Exception:
                    pass
            dicts.append(
                ServiceDictionary(
                    name=item.get("name", ""), write_only=item.get("write_only", False), items=entries_dict
                )
            )
    except Exception:
        pass
    return dicts


def clone_version(service_id: str, version: int, token: str, comment: str = "") -> int:
    """Clone an existing version to create a new draft."""
    new_version = int(fastly("PUT", f"/service/{service_id}/version/{version}/clone", token=token)["number"])
    if comment:
        fastly("PUT", f"/service/{service_id}/version/{new_version}", {"comment": comment}, token=token)
    return new_version


def delete_snippet(service_id: str, version: int, snippet_name: str, token: str) -> None:
    _delete_version_resource(service_id, version, "snippet", snippet_name, token)


def create_or_update_snippet(service_id: str, version: int, snippet: VCLSnippet, token: str) -> None:
    snippet_type = (
        snippet.subroutine.replace("vcl_", "") if snippet.subroutine.startswith("vcl_") else snippet.subroutine
    )
    body = {
        "name": snippet.name,
        "type": snippet_type,
        "priority": snippet.priority,
        "content": snippet.body,
        "dynamic": 0,
    }
    _upsert_version_resource(service_id, version, "snippet", snippet.name, body, token)


def delete_logging_endpoint(service_id: str, version: int, endpoint_name: str, token: str) -> None:
    _delete_version_resource(service_id, version, "logging/s3", endpoint_name, token)


def create_or_update_logging_endpoint(
    service_id: str,
    version: int,
    endpoint: LoggingEndpoint,
    token: str,
    bucket_name: str | None = None,
    domain: str | None = None,
    access_key: str | None = None,
    secret_key: str | None = None,
) -> None:
    """Create or update an S3 logging endpoint."""
    body: dict[str, Any] = {
        "name": endpoint.name,
        "path": endpoint.path,
        "period": endpoint.period,
        "format": endpoint.format_string,
        "placement": endpoint.placement,
        "response_object_name": endpoint.response_object_name,
        "format_version": 2,
        "gzip_level": 9,
    }
    for k, v in (
        ("response_condition", endpoint.response_condition),
        ("bucket_name", bucket_name),
        ("domain", domain),
        ("access_key", access_key),
        ("secret_key", secret_key),
    ):
        if v:
            body[k] = v
    _upsert_version_resource(service_id, version, "logging/s3", endpoint.name, body, token)


def delete_backend(service_id: str, version: int, backend_name: str, token: str) -> None:
    _delete_version_resource(service_id, version, "backend", backend_name, token)


def create_or_update_backend(service_id: str, version: int, backend: Backend, token: str) -> None:
    """Create or update a backend."""
    body: dict[str, Any] = {
        "name": backend.name,
        "address": backend.address,
        "port": backend.port,
        "ssl_check_cert": backend.ssl_check_cert,
        "ssl_hostname": backend.ssl_hostname,
        "connect_timeout": backend.connect_timeout,
        "first_byte_timeout": backend.first_byte_timeout,
        "between_bytes_timeout": backend.between_bytes_timeout,
        "auto_loadbalance": backend.auto_loadbalance,
        "use_ssl": backend.use_ssl,
        "request_condition": backend.request_condition or None,
        "shield": backend.shield if backend.shield else None,
    }
    if backend.override_host:
        body["override_host"] = backend.override_host
    _upsert_version_resource(service_id, version, "backend", backend.name, body, token)


def delete_dictionary(service_id: str, version: int, dict_name: str, token: str) -> None:
    _delete_version_resource(service_id, version, "dictionary", dict_name, token)


def create_or_update_dictionary(service_id: str, version: int, dictionary: ServiceDictionary, token: str) -> None:
    """Create or update a service dictionary and its items."""
    resp = _upsert_version_resource(
        service_id,
        version,
        "dictionary",
        dictionary.name,
        {"name": dictionary.name, "write_only": dictionary.write_only},
        token,
    )
    if not (dict_id := resp.get("id", "")):
        raise RuntimeError(f"Failed to get dictionary ID for {dictionary.name}")
    if dictionary.items:
        _upsert_dictionary_items(service_id, dict_id, dictionary.items, token)


def _upsert_dictionary_items(service_id: str, dict_id: str, items: dict[str, str], token: str) -> None:
    """Upsert items into a service dictionary."""
    for key, value in items.items():
        encoded_key = urllib.parse.quote(key, safe="")
        try:
            fastly(
                "PUT",
                f"/service/{service_id}/dictionary/{dict_id}/item/{encoded_key}",
                {"item_value": value},
                token=token,
            )
        except RuntimeError:
            fastly(
                "POST",
                f"/service/{service_id}/dictionary/{dict_id}/item",
                {"item_key": key, "item_value": value},
                token=token,
            )


def validate_version(service_id: str, version: int, token: str) -> bool:
    """Validate a service version."""
    try:
        return fastly("GET", f"/service/{service_id}/version/{version}/validate", token=token).get("status") == "ok"
    except RuntimeError as e:
        raise RuntimeError(f"VCL validation failed: {e}")


def activate_version(service_id: str, version: int, token: str) -> None:
    fastly("PUT", f"/service/{service_id}/version/{version}/activate", token=token)


def _bootstrap_featurestate_from_fastly(service_id: str, token: str) -> FeatureState:
    """Introspect active (or draft) Fastly version and reverse-engineer FeatureState (Gotcha 1)."""
    source_ver = fastly_integration.fetch_active_version(service_id, token) or _fetch_highest_version(service_id, token)
    if source_ver is None:
        return FeatureState(
            service_id=service_id,
            log_period=60,
            sample_rate=100,
            edge_only=False,
            custom_condition="",
            fos_prefix="",
            fos_endpoint="fos.example.com",
        )
    snippets = fastly_integration.fetch_snippets(service_id, source_ver, token)
    endpoints = fastly_integration.fetch_logging_endpoints(service_id, source_ver, token)
    snippet_names = {s.name for s in snippets}
    rum_enabled = any("RUM" in name for name in snippet_names)
    scoring_enabled = any("Session Scoring" in name for name in snippet_names)
    cmcd_enabled = any("CMCD" in name for name in snippet_names)
    if not rum_enabled:
        try:
            vcl_resp = fastly("GET", f"/service/{service_id}/version/{source_ver}/vcl", token=token)
            if isinstance(vcl_resp, dict) and "items" in vcl_resp:
                vcl_content = " ".join([v.get("content", "") for v in vcl_resp["items"]])
                if "fos_origin" in vcl_content or "F_fos_origin" in vcl_content or "rum" in vcl_content.lower():
                    rum_enabled = True
                if "session_scorer" in vcl_content or "Session Scoring" in vcl_content:
                    scoring_enabled = True
        except Exception:
            pass
    log_period = 60
    rum_log_period = None
    for endpoint in endpoints:
        if (
            "Fastly Log Analytics" in endpoint.name or "Fastly Object Storage Logs" in endpoint.name
        ) and "RUM" not in endpoint.name:
            log_period = endpoint.period
        elif "RUM" in endpoint.name:
            rum_log_period = endpoint.period
    return FeatureState(
        service_id=service_id,
        log_period=log_period,
        sample_rate=100,
        edge_only=False,
        custom_condition="",
        fos_prefix="raw",
        fos_endpoint="fos.example.com",
        rum_enabled=rum_enabled,
        rum_log_period=rum_log_period,
        cmcd=CmcdConfig(enabled=cmcd_enabled),
        scoring=ScoringConfig(enabled=scoring_enabled, domain="" if not scoring_enabled else "scorer.example.com"),
    )


def _fetch_highest_version(service_id: str, token: str, *, raise_on_error: bool = False) -> int | None:
    """Return the highest existing version number for ``service_id``, or ``None``."""
    try:
        resp = fastly("GET", f"/service/{service_id}/version", token=token)
    except Exception as e:
        if raise_on_error:
            raise RuntimeError(f"Cannot determine version to clone for {service_id}: {e}")
        return None
    if isinstance(resp, list):
        items = resp
    elif isinstance(resp, dict):
        items = resp.get("items", [])
    else:
        items = []
    versions = sorted([int(v.get("number", 0)) for v in items if isinstance(v, dict)])
    return versions[-1] if versions else None


def _fetch_from_active(service_id: str, token: str, fetcher: Callable[[str, int, str], list[Any]]) -> list[Any]:
    active_ver = fastly_integration.fetch_active_version(service_id, token)
    return fetcher(service_id, active_ver, token) if active_ver is not None else []


def _fetch_snippets(service_id: str, token: str) -> list[VCLSnippet]:
    return _fetch_from_active(service_id, token, fastly_integration.fetch_snippets)


def _fetch_logging_endpoints(service_id: str, token: str) -> list[LoggingEndpoint]:
    return _fetch_from_active(service_id, token, fastly_integration.fetch_logging_endpoints)


def _fetch_backends(service_id: str, token: str) -> list[Backend]:
    return _fetch_from_active(service_id, token, fastly_integration.fetch_backends)


def _fetch_dictionaries(service_id: str, token: str) -> list[ServiceDictionary]:
    return _fetch_from_active(service_id, token, fastly_integration.fetch_dictionaries)


def _ensure_cdn_domain(
    service_id: str, domain: str, token: str, status_cb: Callable[[str], None] | None = None
) -> None:
    """Add ``domain`` to a REUSED CDN service if it isn't already attached."""
    active_ver = fastly_integration.fetch_active_version(service_id, token)
    if active_ver is None:
        return
    existing_domains = fastly("GET", f"/service/{service_id}/version/{active_ver}/domain", token=token)
    names = {d.get("name", "").lower() for d in existing_domains} if isinstance(existing_domains, list) else set()
    if domain.lower() in names:
        return
    if status_cb:
        status_cb(f"⏳ Domain '{domain}' missing from existing CDN service; adding...")
    draft_version = fastly_integration.clone_version(service_id, active_ver, token, f"Add missing domain {domain}")
    fastly("POST", f"/service/{service_id}/version/{draft_version}/domain", {"name": domain}, token=token)
    if error := _validate_draft(service_id, token, draft_version):
        raise RuntimeError(f"Failed to validate CDN domain addition for {service_id}: {error}")
    _activate_draft(service_id, token, draft_version)
    if status_cb:
        status_cb(f"✓ Domain '{domain}' added to existing CDN service")


def _fetch_active_version(service_id: str, token: str) -> int:
    ver = fastly_integration.fetch_active_version(service_id, token)
    if ver is None:
        raise RuntimeError(f"No active version found for {service_id}")
    return ver


def _clone_active_version(service_id: str, token: str, desired_state: FeatureState | None = None) -> int:
    """Clone active version to a new draft version."""
    import datetime as dt

    ts = dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    comment = f"Fastly Log Analytics Reconciliation at {ts}"
    if desired_state is not None:
        enabled_features = [
            label
            for flag, label in (
                (desired_state.logging_enabled, "Request Logs"),
                (desired_state.rum_enabled, "RUM"),
                (bool(desired_state.scoring and desired_state.scoring.enabled), "Session Scoring"),
                (bool(desired_state.cmcd and desired_state.cmcd.enabled), "CMCD"),
            )
            if flag
        ]
        user_custom_fields = (
            [cf for cf in desired_state.log_fields.custom_fields if cf.get("name") not in _AUTO_INJECTED_NAMES]
            if desired_state.log_fields
            else []
        )
        if num_cf := len(user_custom_fields):
            enabled_features.append(f"{num_cf} Custom Field{'s' if num_cf > 1 else ''}")
        comment += (
            f" (Enabled: {', '.join(enabled_features)})" if enabled_features else " (All Features Disabled / Teardown)"
        )
    ver = fastly_integration.fetch_active_version(service_id, token) or _fetch_highest_version(
        service_id, token, raise_on_error=True
    )
    if ver is not None:
        return fastly_integration.clone_version(service_id, ver, token, comment)
    raise RuntimeError(f"Cannot determine version to clone for {service_id}")


def _apply_diff(
    service_id: str,
    token: str,
    draft_version: int,
    diff: DiffResult,
    desired_endpoints: list[LoggingEndpoint] | None = None,
    desired_state: FeatureState | None = None,
    status_cb: Callable[[str], None] | None = None,
    current_snippets: list[VCLSnippet] | None = None,
) -> None:
    """Apply diff mutations on the draft version (Step 6)."""
    from backend.config import config_path as _config_path
    from backend.core.fastly.client import ensure_condition, region_endpoint

    desired_endpoints = desired_endpoints or []
    unmanaged_snippets = [s for s in (current_snippets or []) if not _is_managed_snippet(s.name)]
    vars_to_deconflict = (
        "fosAccessKey",
        "fosSecretKey",
        "fosBucket",
        "fosRegion",
        "fosHost",
        "canonicalHeaders",
        "signedHeaders",
        "canonicalRequest",
        "canonicalQuery",
        "stringToSign",
        "dateStamp",
        "signature",
        "scope",
    )
    for snippet in diff.snippets_to_add + diff.snippets_to_update:
        body, subroutine = snippet.body, snippet.subroutine
        for var_name in vars_to_deconflict:
            if any(
                subroutine == s.subroutine and f"declare local var.{var_name}" in s.body for s in unmanaged_snippets
            ):
                if status_cb:
                    status_cb(
                        f"ℹ️  unmanaged snippet already declares 'var.{var_name}' in '{subroutine}'. Skipping duplicate declaration."
                    )
                body = body.replace(
                    f"declare local var.{var_name} STRING;",
                    f"# declare local var.{var_name} STRING; (omitted to prevent collision with unmanaged snippet)",
                )
        snippet.body = body
    bucket_name = domain = access_key = secret_key = None
    config_path = _config_path(service_id)
    if config_path.exists():
        try:
            cfg = json.loads(config_path.read_text())
            bucket_name = cfg.get("fos_bucket") or cfg.get("fos_bucket_name")
            domain = region_endpoint(cfg.get("fos_region") or "us-east-1")
            access_key = cfg.get("fos_access_key_id") or cfg.get("fos_access_key")
            secret_key = cfg.get("fos_secret_access_key") or cfg.get("fos_secret_key")
        except Exception:
            pass

    def _purge_outdated(
        names: list[str],
        label: str,
        deleter: Callable[[str, int, str, str], None],
        *,
        whitelist: set[str] | None = None,
        err_label: str = "",
        skip_fn: Callable[[str], bool] | None = None,
    ) -> None:
        for name in names:
            if skip_fn and skip_fn(name):
                continue
            if whitelist is not None and name not in whitelist:
                raise RuntimeError(
                    f"CRITICAL: Attempted to delete non-whitelisted {err_label} '{name}'. Whitelisted: {whitelist}"
                )
            if status_cb:
                status_cb(f"🧹 Removing outdated {label} '{name}'...")
            try:
                deleter(service_id, draft_version, name, token)
            except Exception:
                pass

    _purge_outdated(
        diff.snippets_to_remove,
        "VCL snippet",
        fastly_integration.delete_snippet,
        skip_fn=lambda n: not _is_managed_snippet(n),
    )
    _purge_outdated(diff.endpoints_to_remove, "logging endpoint", fastly_integration.delete_logging_endpoint)
    _purge_outdated(
        diff.backends_to_remove,
        "backend",
        fastly_integration.delete_backend,
        whitelist=MANAGED_BACKEND_NAMES,
        err_label="backend",
    )
    _purge_outdated(
        diff.dictionaries_to_remove,
        "edge dictionary",
        fastly_integration.delete_dictionary,
        whitelist=MANAGED_DICTIONARY_NAMES,
        err_label="dictionary",
    )

    def _ensure_cond_safe(name: str, statement: str, cond_type: str = "RESPONSE") -> None:
        try:
            ensure_condition(
                name=name,
                statement=statement,
                type=cond_type,
                service_id=service_id,
                version=draft_version,
                token=token,
            )
        except Exception as e:
            logging.warning(f"WARNING: Failed to ensure {name}: {e}")

    if any(ep.response_condition == "log_analytics_condition" for ep in desired_endpoints):
        if status_cb:
            status_cb("➕ Configuring log analytics condition 'log_analytics_condition'...")
        cond_parts = ["!segmented_caching.is_inner_req"]
        if desired_state and desired_state.rum_enabled:
            cond_parts.append('req.url.path != "/rum-beacon"')
        if desired_state:
            if desired_state.edge_only:
                from backend.provision.fastly_api import _log_sampling_edge_clause

                cond_parts.append(_log_sampling_edge_clause(desired_state.scoring.enabled))
            if desired_state.sample_rate < 100:
                cond_parts.append(f"randombool({desired_state.sample_rate}, 100)")
            if desired_state.custom_condition and desired_state.custom_condition.strip():
                cond_parts.append(f"({desired_state.custom_condition.strip()})")
        _ensure_cond_safe("log_analytics_condition", " && ".join(cond_parts))
    if any(ep.response_condition == "rum_log_condition" for ep in desired_endpoints):
        if status_cb:
            status_cb("➕ Configuring RUM routing condition 'rum_log_condition'...")
        _ensure_cond_safe("rum_log_condition", build_rum_log_condition(desired_state))
    desired_backends_list = desired_backends(desired_state) if desired_state else []
    if any(ep.response_condition == "fastly_log_analytics_false" for ep in desired_endpoints) or any(
        b.request_condition == "fastly_log_analytics_false" for b in desired_backends_list
    ):
        if status_cb:
            status_cb("➕ Configuring request condition 'fastly_log_analytics_false'...")
        _ensure_cond_safe("fastly_log_analytics_false", "false", cond_type="REQUEST")
    for backend in diff.backends_to_add + diff.backends_to_update:
        if status_cb:
            status_cb(f"➕ Installing backend '{backend.name}'...")
        try:
            fastly_integration.create_or_update_backend(service_id, draft_version, backend, token)
        except Exception as e:
            logging.warning(f"WARNING: Backend creation failed for {backend.name}: {e}")
            raise
    for dictionary in diff.dictionaries_to_add + diff.dictionaries_to_update:
        if status_cb:
            status_cb(f"➕ Installing edge dictionary '{dictionary.name}'...")
        try:
            fastly_integration.create_or_update_dictionary(service_id, draft_version, dictionary, token)
        except Exception as e:
            logger.warning("Failed to install/update edge dictionary '%s': %s", dictionary.name, e)
    for snippet in diff.snippets_to_add + diff.snippets_to_update:
        if status_cb:
            status_cb(f"➕ Installing VCL snippet '{snippet.name}'...")
        try:
            fastly_integration.create_or_update_snippet(service_id, draft_version, snippet, token)
        except Exception:
            pass
    for endpoint in diff.endpoints_to_add + diff.endpoints_to_update:
        if status_cb:
            status_cb(f"➕ Installing logging endpoint '{endpoint.name}'...")
        try:
            fastly_integration.create_or_update_logging_endpoint(
                service_id,
                draft_version,
                endpoint,
                token,
                bucket_name=bucket_name,
                domain=domain,
                access_key=access_key,
                secret_key=secret_key,
            )
        except Exception as e:
            logger.exception(f"Failed to install/update logging endpoint '{endpoint.name}': {e}")
            if status_cb:
                status_cb(f"❌ Failed to install logging endpoint '{endpoint.name}': {e}")


def _is_managed_snippet(name: str) -> bool:
    return name.startswith("Fastly Log Analytics - ") or _is_legacy_snippet(name)


def _is_legacy_snippet(name: str) -> bool:
    if name.startswith("Fastly Log Analytics - "):
        return False
    return any(
        name.startswith(p) for p in ("Fastly Log Analysis", "Fastly Log Analytics", "RUM -", "Session Scoring", "CMCD")
    )


def _detect_and_queue_legacy_cleanup(
    current_snippets: list[VCLSnippet], diff: DiffResult, status_cb: Callable[[str], None] | None = None
) -> None:
    if legacy := [s for s in current_snippets if _is_legacy_snippet(s.name)]:
        if status_cb:
            status_cb(
                f"⏳ Detected {len(legacy)} legacy VCL snippet(s) from prior deployment — will remove during reconciliation…"
            )
        for snippet in legacy:
            if snippet.name not in diff.snippets_to_remove:
                diff.snippets_to_remove.append(snippet.name)


def _validate_draft(service_id: str, token: str, draft_version: int) -> str:
    try:
        if not fastly_integration.validate_version(service_id, draft_version, token):
            return f"VCL validation failed: version {draft_version} did not validate (status not ok)"
        return ""
    except RuntimeError as e:
        return str(e)


def _activate_draft(service_id: str, token: str, draft_version: int) -> None:
    fastly_integration.activate_version(service_id, draft_version, token)


RUM_SURROGATE_KEY = "rum-js"


def _purge_rum_surrogate_key(service_id: str, token: str) -> None:
    """Purge the ``rum-js`` surrogate key so the edge drops every cached RUM script."""
    from backend.core.fastly.client import fastly as fastly_client

    sched_logger = logging.getLogger("backend.scheduler")
    try:
        sched_logger.info(f"Purging RUM surrogate key {RUM_SURROGATE_KEY} on service {service_id}")
        fastly_client("POST", f"/service/{service_id}/purge/{RUM_SURROGATE_KEY}", token=token, expect_empty=True)
    except Exception as e:
        sched_logger.warning(f"Failed to purge surrogate key {RUM_SURROGATE_KEY} for service {service_id}: {e}")


from backend.provision.session_scoring_orchestrator import (  # noqa: E402
    _SCORER_PACKAGE,
    _SCORING_FIELD_NAMES,
    MATRIX_STORE_NAME_TEMPLATE,
    _deploy_wasm_package,
    _find_kv_store,
    _remove_scoring_custom_fields,
    _write_matrix_to_kv,
    delete_scoring_service,
    ensure_scoring_service,
    scorer_vcl_fingerprint,
)


class DeclarativeKVStore(BaseModel):
    name: str
    keys: dict[str, str] = Field(default_factory=dict)
    link_to_services: list[str] = Field(default_factory=list)


class DeclarativeComputeService(BaseModel):
    name: str
    package_path: str
    domains: list[str] = Field(default_factory=list)
    linked_kv_stores: list[str] = Field(default_factory=list)


class DeclarativeVCLService(BaseModel):
    name: str
    custom_fields: list[dict[str, Any]] = Field(default_factory=list)
    logging_endpoints: list[dict[str, Any]] = Field(default_factory=list)
    backends: list[dict[str, Any]] = Field(default_factory=list)
    snippets: list[dict[str, Any]] = Field(default_factory=list)


class UnifiedDesiredState(BaseModel):
    service_id: str
    vcl_service: DeclarativeVCLService
    compute_scorer: DeclarativeComputeService | None = None
    kv_stores: dict[str, DeclarativeKVStore] = Field(default_factory=dict)


def build_desired_state(logging_service_id: str, token: str) -> UnifiedDesiredState:
    """Build the complete, unified desired infrastructure state from configuration."""
    state = _load_or_bootstrap_feature_state(logging_service_id, token, persist_bootstrap=False)
    vcl_service = DeclarativeVCLService(
        name=state.logging_endpoint_name,
        custom_fields=state.log_fields.custom_fields,
        logging_endpoints=[ep if isinstance(ep, dict) else ep.to_dict() for ep in desired_logging_endpoints(state)],
        backends=[b if isinstance(b, dict) else b.to_dict() for b in desired_backends(state)],
        snippets=[s if isinstance(s, dict) else s.to_dict() for s in desired_snippets(state)],
    )
    compute_scorer = None
    kv_stores: dict[str, DeclarativeKVStore] = {}
    if state.scoring.enabled:
        from backend.provision.session_scoring_orchestrator import _resolve_tenant_matrix_for_deploy
        from backend.scoring import serialize_kv

        store_name = MATRIX_STORE_NAME_TEMPLATE.format(sid=logging_service_id)
        compute_scorer = DeclarativeComputeService(
            name=f"Session Scoring Service for {logging_service_id}",
            package_path=str(_SCORER_PACKAGE),
            domains=[state.scoring.domain],
            linked_kv_stores=[store_name],
        )
        keys: dict[str, str] = {}
        if (matrix_path := _resolve_tenant_matrix_for_deploy(logging_service_id)) and matrix_path.exists():
            with matrix_path.open() as f:
                keys["matrix"] = serialize_kv(json.load(f)).hex()
        kv_stores[store_name] = DeclarativeKVStore(name=store_name, keys=keys, link_to_services=[compute_scorer.name])
    return UnifiedDesiredState(
        service_id=logging_service_id, vcl_service=vcl_service, compute_scorer=compute_scorer, kv_stores=kv_stores
    )


def discover_current_state(logging_service_id: str, token: str) -> dict[str, Any]:
    """Retrieve the active live infrastructure matching names in our scope."""
    store_name = MATRIX_STORE_NAME_TEMPLATE.format(sid=logging_service_id)
    store = _find_kv_store(store_name, token)
    return {
        "kv_stores": {store_name: store} if store else {},
        "compute_scorer": find_service_by_name(f"Session Scoring Service for {logging_service_id}", token),
    }


def verify_compute_scorer_readiness(
    compute_def: DeclarativeComputeService, token: str, status_cb: Callable[[str], None] | None = None
) -> None:
    """Probes the Compute Scorer domain to guarantee DNS and Edge Routing are active."""
    import httpx
    from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

    url = f"https://{compute_def.domains[0]}/health"
    max_retries = 10
    for attempt in Retrying(
        stop=stop_after_attempt(max_retries),
        wait=wait_exponential(multiplier=1.0, exp_base=1.5, min=1.0, max=5.0),
        retry=retry_if_exception_type(Exception),
        sleep=time.sleep,
        reraise=False,
    ):
        with attempt:
            idx = attempt.retry_state.attempt_number
            if status_cb:
                status_cb(f"🔍 Issuing HTTP probe to {url} (attempt {idx}/{max_retries})...")
            try:
                response = httpx.get(url, headers={"X-Edge-Scorer-Auth": "probe-sentinel"}, timeout=2.0)
            except Exception as e:
                if status_cb:
                    status_cb(f"⚠️ Probe {idx} failed: {type(e).__name__}. Retrying...")
                raise
            if response.status_code in (200, 401, 404):
                if status_cb:
                    status_cb(f"✅ Compute Scorer is hot (status {response.status_code}).")
                return
            raise RuntimeError(f"Unexpected status {response.status_code}")
    raise RuntimeError(f"Readiness probe failed: Scorer service at {url} did not respond within timeout window.")


def reconcile_infrastructure(
    logging_service_id: str, token: str, dry_run: bool = False, status_cb: Callable[[str], None] | None = None
) -> None:
    """The central unified IaC control loop managing all multi-service resource operations."""
    desired = build_desired_state(logging_service_id, token)
    if status_cb:
        status_cb("⏳ Discovering active edge resources on Fastly account...")
    current = discover_current_state(logging_service_id, token)
    if dry_run:
        return
    from backend.config import config_path as _config_path
    from backend.config import mutate_config, snapshot_config

    config_path = _config_path(logging_service_id)
    if (orig_config_content := snapshot_config(logging_service_id)) is None:
        raise RuntimeError(f"Configuration file {config_path} is required for live deployment.")
    cfg = json.loads(orig_config_content)
    orig_scoring = copy.deepcopy(cfg.get("scoring"))
    orig_scoring_fields = [
        copy.deepcopy(cf)
        for cf in (cfg.get("log_fields") or {}).get("custom_fields") or []
        if cf.get("name") in _SCORING_FIELD_NAMES
    ]
    persisted = False

    def _persist(apply: Callable[[dict[str, Any]], None]) -> None:
        nonlocal persisted
        persisted = True
        if mutate_config(logging_service_id, apply) is None:
            raise RuntimeError(f"Configuration file {config_path} disappeared during reconciliation.")

    def _rollback_owned_keys(disk_cfg: dict[str, Any]) -> None:
        if orig_scoring is None:
            disk_cfg.pop("scoring", None)
        else:
            disk_cfg["scoring"] = copy.deepcopy(orig_scoring)
        if orig_scoring_fields:
            _remove_scoring_custom_fields(disk_cfg)
            lf = disk_cfg.setdefault("log_fields", {})
            lf["custom_fields"] = [*(lf.get("custom_fields") or []), *copy.deepcopy(orig_scoring_fields)]

    try:
        apply_phase3: Callable[[dict[str, Any]], None] | None = None
        if desired.compute_scorer:
            if status_cb:
                status_cb("⏳ Phase 1: Reconciling edge KV Stores...")
                status_cb("⏳ Phase 2: Reconciling Compute Scorer Service and uploading WASM package...")
            scoring_meta = ensure_scoring_service(logging_service_id, token, status_cb=status_cb)
            scoring_service_id, matrix_store_id = (
                scoring_meta["scoring_service_id"],
                scoring_meta.get("scoring_matrix_store_id", ""),
            )
            _write_matrix_to_kv(matrix_store_id, logging_service_id, token, status_cb=status_cb)
            package_meta = _deploy_wasm_package(scoring_service_id, matrix_store_id, token, status_cb=status_cb)
            resolved_domain = scoring_meta.get("scoring_domain") or (
                desired.compute_scorer.domains[0] if desired.compute_scorer.domains else ""
            )
            if resolved_domain:
                desired.compute_scorer.domains = [resolved_domain]
            cfg_scoring = cfg.get("scoring") or {}
            new_scoring: dict[str, Any] = {
                "enabled": True,
                "scoring_service_id": scoring_service_id,
                **{
                    k: scoring_meta.get(k, "")
                    for k in (
                        "scoring_service_name",
                        "scoring_domain",
                        "scoring_keys_store_id",
                        "scoring_config_store_id",
                    )
                },
                "scoring_matrix_store_id": matrix_store_id,
                "request_secret": scoring_meta.get("request_secret", cfg_scoring.get("request_secret", "")),
                "aes_key_hex": scoring_meta.get("aes_key_hex", "") or cfg_scoring.get("aes_key_hex", ""),
                "deployed_package_sha": package_meta["sha"],
                "deployed_package_files_hash": package_meta["files_hash"],
                "deployed_vcl_sha": scorer_vcl_fingerprint(logging_service_id),
            }

            def _set_scoring(disk_cfg: dict[str, Any]) -> None:
                existing_scoring = disk_cfg.get("scoring")
                base_scoring = copy.deepcopy(existing_scoring) if isinstance(existing_scoring, dict) else {}
                base_scoring.update(copy.deepcopy(new_scoring))
                disk_cfg["scoring"] = base_scoring

            apply_phase3 = _set_scoring
            if status_cb:
                status_cb("⏳ Phase 2.5: Issuing HTTP readiness checks on deployed domain...")
            verify_compute_scorer_readiness(desired.compute_scorer, token, status_cb=status_cb)
        elif current["compute_scorer"]:
            if status_cb:
                status_cb("⏳ Phase 4: Executing safe reverse-topological teardown of disabled assets...")
                status_cb("⏳ Stripping VCL scoring backend and snippets first...")

            def _disable_scoring_keys(disk_cfg: dict[str, Any]) -> None:
                _remove_scoring_custom_fields(disk_cfg)
                if "scoring" in disk_cfg:
                    disk_cfg["scoring"]["enabled"] = False

            apply_phase3 = _disable_scoring_keys
        if status_cb:
            status_cb("⏳ Phase 3: Syncing VCL Edge Logging Configurations, Custom Fields, and Backends...")
        if apply_phase3 is not None:
            _persist(apply_phase3)
        reconcile_vcl_state(logging_service_id, token, dry_run=False, status_cb=status_cb)
        if not desired.compute_scorer and current["compute_scorer"]:
            if status_cb:
                status_cb("⏳ Sleeping/Draining edge requests for 5 seconds to ensure clean cutover...")
            time.sleep(5.0)
            if status_cb:
                status_cb("⏳ Deleting serverless Compute Scorer service and resources last...")
            old_matrix_store_id = (
                current["kv_stores"].get(MATRIX_STORE_NAME_TEMPLATE.format(sid=logging_service_id), {}).get("id", "")
            )
            prior_scoring = json.loads(orig_config_content).get("scoring") or {}
            delete_scoring_service(
                scoring_service_id=current["compute_scorer"].get("id"),
                scoring_keys_store_id=prior_scoring.get("scoring_keys_store_id", ""),
                scoring_config_store_id=prior_scoring.get("scoring_config_store_id", ""),
                scoring_matrix_store_id=old_matrix_store_id,
                token=token,
                status_cb=status_cb,
            )
            _persist(lambda disk_cfg: disk_cfg.pop("scoring", None))
        if status_cb:
            status_cb("✅ Infrastructure state reconciliation completed successfully.")
    except Exception as err:
        logger.error("IaC reconciliation failure, executing transactional configuration rollback", exc_info=True)
        if status_cb:
            status_cb(f"❌ Error during reconciliation: {err}. Rolling back to previous configuration...")
        if persisted:
            mutate_config(logging_service_id, _rollback_owned_keys)
        raise err

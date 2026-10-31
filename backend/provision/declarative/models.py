# ruff: noqa: T201
"""Declarative reconciliation models, feature state, and diff computation."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from pydantic import BaseModel, Field, TypeAdapter

from backend.config import is_rum_enabled

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
            (rum_enabled, "backend.provision.rum_orchestrator", "_RUM_CUSTOM_FIELDS", False),
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

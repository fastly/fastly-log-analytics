# ruff: noqa: T201
"""Declarative reconciliation models, feature state, and diff computation."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from pydantic import BaseModel, Field, TypeAdapter

from backend.config import is_rum_enabled
from backend.core.catalog import (
    validate_custom_field_entry,
    validate_vcl_custom_condition,
    validate_vcl_log_expression,
)
from backend.core.fastly.client import validate_timing_allow_origins

_AUTO_INJECTED_NAMES: set[str] = {
    "service_id",
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

_CANONICAL_SYSTEM_FIELDS_CACHE: dict[str, dict[str, Any]] | None = None


def _canonical_system_fields() -> dict[str, dict[str, Any]]:
    """Return canonical system custom-field definitions keyed by field name."""
    global _CANONICAL_SYSTEM_FIELDS_CACHE
    if _CANONICAL_SYSTEM_FIELDS_CACHE is None:
        from backend.provision.cmcd_fields import _CMCD_CUSTOM_FIELDS
        from backend.provision.rum_orchestrator import _RUM_CUSTOM_FIELDS
        from backend.provision.session_scoring_orchestrator import _SCORING_CUSTOM_FIELDS

        _CANONICAL_SYSTEM_FIELDS_CACHE = {
            f["name"]: dict(f) for f in (*_RUM_CUSTOM_FIELDS, *_SCORING_CUSTOM_FIELDS, *_CMCD_CUSTOM_FIELDS)
        }
    return _CANONICAL_SYSTEM_FIELDS_CACHE


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
    field_limits: dict[str, int] = field(default_factory=dict)
    preset: str | None = None


def _subdict(cfg: dict[str, Any], key: str) -> dict[str, Any]:
    val = cfg.get(key)
    if val is None:
        return {}
    if not isinstance(val, dict):
        raise ValueError(f"{key} must be a dict, got {type(val).__name__}")
    return val


def _cfg_str(val: Any, field_name: str, default: str = "") -> str:
    if val is None:
        return default
    if not isinstance(val, str):
        raise ValueError(f"{field_name} must be a string, got {type(val).__name__}")
    return val


def _cfg_int(val: Any, field_name: str) -> int:
    if isinstance(val, bool) or not isinstance(val, (int, str)):
        raise ValueError(f"{field_name} must be an integer, got {val!r}")
    try:
        return int(val.strip() if isinstance(val, str) else val)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an integer, got {val!r}") from exc


_SERVICE_HOSTNAME_RE = re.compile(r"^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$")
MAX_TIMING_ALLOW_ORIGINS = 20


def service_timing_origins(domains: list[str]) -> list[str]:
    """Page origins for the hostnames attached to the Fastly service, for ``Timing-Allow-Origin``.

    Wildcard and malformed entries are skipped; the list is sorted, de-duplicated and capped.
    """
    hosts = sorted({d.strip().lower() for d in domains if isinstance(d, str)})
    valid = [h for h in hosts if ".." not in h and _SERVICE_HOSTNAME_RE.fullmatch(h)]
    return [f"https://{host}" for host in valid[:MAX_TIMING_ALLOW_ORIGINS]]


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
    rum_timing_allow_origins: list[str] = field(default_factory=list)
    cmcd: CmcdConfig = field(default_factory=CmcdConfig)
    scoring: ScoringConfig = field(default_factory=ScoringConfig)
    log_fields: LogFieldsConfig = field(default_factory=LogFieldsConfig)

    def __post_init__(self) -> None:
        """Validate cross-feature dependencies and field constraints."""
        from backend.core.fastly.client import _assert_faro_version_safe, validate_rum_custom_condition
        from backend.core.vcl_generator import validate_quoted_literal, validate_url_exclusion_regex

        if not isinstance(self.service_id, str) or not self.service_id:
            raise ValueError(f"service_id must be a non-empty string, got {self.service_id!r}")
        if (
            not isinstance(self.log_period, int)
            or isinstance(self.log_period, bool)
            or not (1 <= self.log_period <= 3600)
        ):
            raise ValueError(f"log_period must be in range [1, 3600], got {self.log_period!r}")
        if self.rum_log_period is not None and (
            not isinstance(self.rum_log_period, int)
            or isinstance(self.rum_log_period, bool)
            or not (1 <= self.rum_log_period <= 3600)
        ):
            raise ValueError(f"rum_log_period must be in range [1, 3600], got {self.rum_log_period!r}")
        if (
            not isinstance(self.sample_rate, int)
            or isinstance(self.sample_rate, bool)
            or not (1 <= self.sample_rate <= 100)
        ):
            raise ValueError(f"sample_rate must be in range [1, 100], got {self.sample_rate!r}")
        if not isinstance(self.custom_condition, str):
            raise ValueError("'custom_condition' must be a string")
        if cond_err := validate_vcl_custom_condition(self.custom_condition, field_name="custom_condition"):
            raise ValueError(cond_err)
        if not isinstance(self.rum_custom_condition, str):
            raise ValueError("'rum_custom_condition' must be a string")
        if self.rum_enabled or self.rum_custom_condition:
            if rum_cond_err := validate_rum_custom_condition(self.rum_custom_condition):
                raise ValueError(rum_cond_err)
        if self.faro_version is not None:
            if not isinstance(self.faro_version, str):
                raise ValueError(f"faro_version must be a string, got {self.faro_version!r}")
            if self.faro_version == "":
                object.__setattr__(self, "faro_version", None)
            else:
                _assert_faro_version_safe(self.faro_version)
        if not isinstance(self.log_fields.custom_fields, list):
            raise ValueError("custom_fields must be a list")
        canonical_fields = _canonical_system_fields()
        seen_post_names: list[str] = []
        for field_dict in self.log_fields.custom_fields:
            if not isinstance(field_dict, dict):
                raise ValueError("Each custom field must be a dict")
            field_name = field_dict.get("name")
            if not isinstance(field_name, str) or not re.fullmatch(r"^[a-z_][a-z0-9_]*$", field_name):
                raise ValueError(f"Custom field name '{field_name}' must be lowercase alphanumeric + underscore")
            if "vcl_log_expression" in field_dict:
                raw_expr = field_dict.get("vcl_log_expression")
                if not isinstance(raw_expr, str):
                    raise ValueError(f"'custom_fields[{field_name}].vcl_log_expression' must be a non-empty string")
                expr_errs = validate_vcl_log_expression(
                    raw_expr,
                    field_name=f"custom_fields[{field_name}].vcl_log_expression",
                )
                if expr_errs:
                    raise ValueError("; ".join(expr_errs))
            raw_stage = field_dict.get("collection_stage", "edge")
            stage = "edge" if raw_stage is None else raw_stage
            if not isinstance(stage, str):
                raise ValueError(f"custom_fields[{field_name}].collection_stage must be a string, got {stage!r}")
            if field_name in _AUTO_INJECTED_NAMES and field_name in canonical_fields:
                canonical_def = canonical_fields[field_name]
                if "collection_stage" in field_dict and stage != canonical_def["collection_stage"]:
                    raise ValueError(
                        f"System custom field '{field_name}' must keep canonical collection_stage "
                        f"{canonical_def['collection_stage']!r}, got {stage!r}"
                    )
                if (
                    "vcl_log_expression" in field_dict
                    and field_dict["vcl_log_expression"] != canonical_def["vcl_log_expression"]
                ):
                    raise ValueError(f"System custom field '{field_name}' must keep its canonical vcl_log_expression")
                entry_errs = validate_custom_field_entry(field_dict, seen_post_names, system_names=_AUTO_INJECTED_NAMES)
                if entry_errs:
                    raise ValueError("; ".join(entry_errs))
            else:
                if stage not in ("edge", "origin"):
                    raise ValueError(
                        f"custom_fields[{field_name}].collection_stage must be 'edge' or 'origin', got {stage!r}"
                    )
                entry_errs = validate_custom_field_entry(field_dict, seen_post_names, system_names=_AUTO_INJECTED_NAMES)
                if entry_errs:
                    raise ValueError("; ".join(entry_errs))
                seen_post_names.append(field_name)
        if not isinstance(self.cmcd.mode, str) or (
            self.cmcd.enabled and self.cmcd.mode not in ("query_string", "headers")
        ):
            raise ValueError(f"cmcd.mode must be 'query_string' or 'headers', got {self.cmcd.mode!r}")
        if (
            not isinstance(self.cmcd.version, int)
            or isinstance(self.cmcd.version, bool)
            or (self.cmcd.enabled and self.cmcd.version not in (1, 2))
        ):
            raise ValueError(f"cmcd.version must be 1 or 2, got {self.cmcd.version!r}")
        if not isinstance(self.cdn_shield, str) or (
            self.cdn_shield and not re.fullmatch(r"^[A-Za-z0-9_-]{1,64}$", self.cdn_shield)
        ):
            raise ValueError(f"cdn_shield must match ^[A-Za-z0-9_-]{{1,64}}$, got {self.cdn_shield!r}")
        if not isinstance(self.fos_region, str) or (
            self.fos_region and not re.fullmatch(r"^[A-Za-z0-9_-]{1,64}$", self.fos_region)
        ):
            raise ValueError(f"fos_region must match ^[A-Za-z0-9_-]{{1,64}}$, got {self.fos_region!r}")
        if not isinstance(self.scoring.exclude_url_regex, str):
            raise ValueError("scoring.exclude_url_regex must be a string")
        if self.scoring.exclude_url_regex:
            validate_url_exclusion_regex(self.scoring.exclude_url_regex)
        if not isinstance(self.scoring.request_secret, str):
            raise ValueError("scoring.request_secret must be a string")
        if self.scoring.request_secret:
            validate_quoted_literal(self.scoring.request_secret, field="scoring.request_secret")
        if not isinstance(self.scoring.domain, str):
            raise ValueError("scoring.domain must be a string")
        if self.scoring.domain:
            validate_quoted_literal(self.scoring.domain, field="scoring.domain")
            if ".." in self.scoring.domain or not re.fullmatch(
                r"^[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$", self.scoring.domain
            ):
                raise ValueError(f"scoring.domain must be a valid hostname, got {self.scoring.domain!r}")
        if self.scoring.enabled and not self.scoring.domain:
            raise ValueError("scoring.enabled=True requires scoring.domain to be set")
        if self.scoring.enabled:
            validate_quoted_literal(self.service_id, field="service_id")
        if (
            not isinstance(self.scoring.enforce_status_code, int)
            or isinstance(self.scoring.enforce_status_code, bool)
            or not (400 <= self.scoring.enforce_status_code <= 599)
        ):
            raise ValueError(
                f"scoring.enforce_status_code must be in range [400, 599], got {self.scoring.enforce_status_code!r}"
            )

    @classmethod
    def from_config(cls, cfg: dict) -> FeatureState:
        """Parse raw config dict and construct FeatureState with automatic dependency injection."""
        from backend.core.fastly.client import _assert_faro_version_safe, validate_rum_custom_condition
        from backend.core.vcl_generator import validate_quoted_literal, validate_url_exclusion_regex

        raw_sid = cfg.get("service_id")
        if raw_sid is None or raw_sid == "":
            raise KeyError("config missing 'service_id'")
        service_id = _cfg_str(raw_sid, "service_id")
        if (log_period_raw := cfg.get("log_period")) is None:
            raise KeyError("config missing 'log_period'")
        log_period = _cfg_int(log_period_raw, "log_period")

        prov_cfg = _subdict(cfg, "provisioning")
        rum_dict = _subdict(cfg, "rum")
        cmcd_cfg = _subdict(cfg, "cmcd")
        scoring_cfg = _subdict(cfg, "scoring")
        fos_proxy = _subdict(cfg, "fos_proxy")
        lf_cfg = _subdict(cfg, "log_fields")

        prov_sr = (
            _cfg_int(prov_cfg["sample_rate"], "provisioning.sample_rate")
            if prov_cfg.get("sample_rate") is not None
            else None
        )
        top_sr = _cfg_int(cfg["sample_rate"], "sample_rate") if cfg.get("sample_rate") is not None else None
        sample_rate = prov_sr if prov_sr is not None else (top_sr if top_sr is not None else 100)
        edge_only = bool(prov_cfg.get("edge_only") if "edge_only" in prov_cfg else cfg.get("edge_only", False))

        prov_cond = _cfg_str(prov_cfg.get("custom_condition"), "provisioning.custom_condition").strip()
        top_cond = _cfg_str(cfg.get("custom_condition"), "custom_condition").strip()
        for cname, cval in (("provisioning.custom_condition", prov_cond), ("custom_condition", top_cond)):
            if cval and (c_err := validate_vcl_custom_condition(cval, field_name=cname)):
                raise ValueError(c_err)
        custom_condition = prov_cond if prov_cfg.get("custom_condition") is not None else top_cond

        nested_rum_lp = (
            _cfg_int(rum_dict["log_period"], "rum.log_period") if rum_dict.get("log_period") is not None else None
        )
        top_rum_lp = (
            _cfg_int(cfg["rum_log_period"], "rum_log_period") if cfg.get("rum_log_period") is not None else None
        )
        rum_log_period = nested_rum_lp if nested_rum_lp is not None else top_rum_lp
        rum_enabled = is_rum_enabled(cfg)

        top_rum_cond = _cfg_str(cfg.get("rum_custom_condition"), "rum_custom_condition").strip()
        nested_rum_cond = _cfg_str(rum_dict.get("custom_condition"), "rum.custom_condition").strip()
        rum_custom_condition = nested_rum_cond if rum_dict.get("custom_condition") is not None else top_rum_cond
        if rum_enabled or rum_custom_condition:
            for rval in (nested_rum_cond, top_rum_cond):
                if rval and (r_err := validate_rum_custom_condition(rval)):
                    raise ValueError(r_err)

        nested_faro = _cfg_str(rum_dict.get("faro_version"), "rum.faro_version")
        top_faro = _cfg_str(cfg.get("faro_version"), "faro_version")
        for fv in (nested_faro, top_faro):
            if fv:
                _assert_faro_version_safe(fv)
        faro_version = nested_faro or top_faro or None

        shield_aliases = (
            _cfg_str(cfg.get("cdn_shield"), "cdn_shield"),
            _cfg_str(prov_cfg.get("cdn_shield"), "provisioning.cdn_shield"),
            _cfg_str(fos_proxy.get("shield"), "fos_proxy.shield"),
        )
        for sh in shield_aliases:
            if sh and not re.fullmatch(r"^[A-Za-z0-9_-]{1,64}$", sh):
                raise ValueError(f"cdn_shield must match ^[A-Za-z0-9_-]{{1,64}}$, got {sh!r}")
        cdn_shield = shield_aliases[0] or shield_aliases[1] or shield_aliases[2] or ""

        cmcd_enabled = bool(
            cfg["cmcd_enabled"] if cfg.get("cmcd_enabled") is not None else cmcd_cfg.get("enabled", False)
        )
        cmcd_mode = _cfg_str(cfg.get("cmcd_mode"), "cmcd_mode") or _cfg_str(cmcd_cfg.get("mode"), "cmcd.mode")
        top_cv = _cfg_int(cfg["cmcd_version"], "cmcd_version") if cfg.get("cmcd_version") is not None else None
        nested_cv = _cfg_int(cmcd_cfg["version"], "cmcd.version") if cmcd_cfg.get("version") is not None else None
        cmcd_config = CmcdConfig(
            enabled=cmcd_enabled,
            mode=cmcd_mode or "query_string",  # type: ignore[arg-type]
            version=top_cv if top_cv is not None else (nested_cv if nested_cv is not None else 1),  # type: ignore[arg-type]
        )

        scoring_enabled = bool(
            cfg["scoring_enabled"] if cfg.get("scoring_enabled") is not None else scoring_cfg.get("enabled", False)
        )
        top_sec = _cfg_str(cfg.get("scoring_request_secret"), "scoring_request_secret")
        nested_sec = _cfg_str(scoring_cfg.get("request_secret"), "scoring.request_secret")
        for sv in (top_sec, nested_sec):
            if sv:
                validate_quoted_literal(sv, field="scoring.request_secret")
        req_sec = top_sec if cfg.get("scoring_request_secret") is not None else nested_sec

        top_excl = _cfg_str(cfg.get("scoring_exclude_url_regex"), "scoring_exclude_url_regex")
        nested_excl = _cfg_str(scoring_cfg.get("exclude_url_regex"), "scoring.exclude_url_regex")
        for ev in (top_excl, nested_excl):
            if ev:
                validate_url_exclusion_regex(ev)
        excl_re = top_excl if cfg.get("scoring_exclude_url_regex") is not None else nested_excl

        dom_aliases = (
            _cfg_str(cfg.get("scoring_domain"), "scoring_domain"),
            _cfg_str(scoring_cfg.get("domain"), "scoring.domain"),
            _cfg_str(scoring_cfg.get("scoring_domain"), "scoring.scoring_domain"),
        )
        for dv in dom_aliases:
            if dv:
                validate_quoted_literal(dv, field="scoring.domain")
                if ".." in dv or not re.fullmatch(r"^[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$", dv):
                    raise ValueError(f"scoring.domain must be a valid hostname, got {dv!r}")
        raw_enf_code = (
            cfg.get("scoring_enforce_status_code")
            if cfg.get("scoring_enforce_status_code") is not None
            else scoring_cfg.get("enforce_status_code")
        )
        if raw_enf_code is not None and (not isinstance(raw_enf_code, int) or isinstance(raw_enf_code, bool)):
            raise ValueError(f"scoring.enforce_status_code must be an integer, got {raw_enf_code!r}")
        scoring_config = ScoringConfig(
            enabled=scoring_enabled,
            domain=dom_aliases[0] or dom_aliases[1] or dom_aliases[2] or "",
            request_secret=req_sec,
            exclude_url_regex=excl_re,
            enforce_status_code=429 if raw_enf_code is None else raw_enf_code,
        )

        log_groups = (
            lf_cfg["groups"]
            if lf_cfg.get("groups") is not None
            else (
                cfg["log_groups"]
                if cfg.get("log_groups") is not None
                else (cfg["groups"] if cfg.get("groups") is not None else [])
            )
        )
        custom_fields = (
            lf_cfg["custom_fields"]
            if lf_cfg.get("custom_fields") is not None
            else (cfg["custom_fields"] if cfg.get("custom_fields") is not None else [])
        )
        field_overrides = (
            lf_cfg["field_overrides"]
            if lf_cfg.get("field_overrides") is not None
            else (cfg["field_overrides"] if cfg.get("field_overrides") is not None else {})
        )
        field_limits = (
            lf_cfg["field_limits"]
            if lf_cfg.get("field_limits") is not None
            else (cfg["field_limits"] if cfg.get("field_limits") is not None else {})
        )
        raw_preset = lf_cfg.get("preset") if lf_cfg.get("preset") is not None else cfg.get("preset")
        preset = _cfg_str(raw_preset, "log_fields.preset") or None if raw_preset is not None else None
        if not isinstance(custom_fields, list):
            raise ValueError("custom_fields must be a list")
        canonical_fields = _canonical_system_fields()
        injected_fields: list[dict[str, Any]] = []
        seen_user_names: list[str] = []
        for raw_f in custom_fields:
            if not isinstance(raw_f, dict):
                raise ValueError("Each custom field must be a dict")
            fname = raw_f.get("name")
            if not isinstance(fname, str) or not re.fullmatch(r"^[a-z_][a-z0-9_]*$", fname):
                raise ValueError(f"Custom field name '{fname}' must be lowercase alphanumeric + underscore")
            if "vcl_log_expression" in raw_f:
                raw_expr = raw_f.get("vcl_log_expression")
                if not isinstance(raw_expr, str):
                    raise ValueError(f"'custom_fields[{fname}].vcl_log_expression' must be a non-empty string")
                expr_errs = validate_vcl_log_expression(
                    raw_expr,
                    field_name=f"custom_fields[{fname}].vcl_log_expression",
                )
                if expr_errs:
                    raise ValueError("; ".join(expr_errs))
            if fname in _AUTO_INJECTED_NAMES and fname in canonical_fields:
                if "collection_stage" in raw_f and raw_f["collection_stage"] is not None:
                    if not isinstance(raw_f["collection_stage"], str):
                        raise ValueError(f"custom_fields[{fname}].collection_stage must be a string")
                canon_copy = dict(canonical_fields[fname])
                if "enabled" in raw_f and isinstance(raw_f["enabled"], bool):
                    canon_copy["enabled"] = raw_f["enabled"]
                injected_fields.append(canon_copy)
            else:
                entry_errs = validate_custom_field_entry(raw_f, seen_user_names, system_names=_AUTO_INJECTED_NAMES)
                if entry_errs:
                    raise ValueError("; ".join(entry_errs))
                seen_user_names.append(fname)
                cf_copy = dict(raw_f)
                if "description" in cf_copy and cf_copy["description"] is None:
                    cf_copy["description"] = ""
                injected_fields.append(cf_copy)
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
            {
                "groups": log_groups,
                "custom_fields": injected_fields,
                "field_overrides": field_overrides,
                "field_limits": field_limits,
                "preset": preset,
            }
        )
        return cls(
            service_id=service_id,
            log_period=log_period,
            sample_rate=sample_rate,
            edge_only=edge_only,
            custom_condition=custom_condition,
            fos_prefix=_cfg_str(cfg.get("fos_prefix"), "fos_prefix"),
            fos_endpoint=_cfg_str(cfg.get("fos_endpoint"), "fos_endpoint", default="fos.example.com"),
            logging_enabled=bool(cfg.get("logging_enabled", True)),
            fos_region=_cfg_str(cfg.get("fos_region"), "fos_region", default="us-east-1"),
            cdn_shield=cdn_shield,
            fos_access_key_id=_cfg_str(cfg.get("fos_access_key_id"), "fos_access_key_id"),
            fos_secret_access_key=_cfg_str(cfg.get("fos_secret_access_key"), "fos_secret_access_key"),
            fos_bucket=_cfg_str(cfg.get("fos_bucket"), "fos_bucket")
            or _cfg_str(cfg.get("fos_bucket_name"), "fos_bucket_name"),
            logging_endpoint_name=_cfg_str(prov_cfg.get("endpoint_name"), "provisioning.endpoint_name")
            or _cfg_str(cfg.get("endpoint_name"), "endpoint_name", default="Fastly Log Analytics")
            or "Fastly Log Analytics",
            rum_endpoint_name=_cfg_str(rum_dict.get("endpoint_name"), "rum.endpoint_name")
            or _cfg_str(cfg.get("rum_endpoint_name"), "rum_endpoint_name", default="Fastly RUM Logs")
            or "Fastly RUM Logs",
            rum_enabled=rum_enabled,
            rum_log_period=rum_log_period,
            faro_version=faro_version,
            rum_custom_condition=rum_custom_condition,
            rum_timing_allow_origins=validate_timing_allow_origins(rum_dict.get("timing_allow_origins")),
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
            "field_limits": self.log_fields.field_limits,
            **({"preset": self.log_fields.preset} if self.log_fields.preset is not None else {}),
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
    """A single native Fastly Object Storage logging endpoint ("fos")."""

    name: str
    endpoint_type: str
    path: str
    period: int
    response_condition: str
    format_string: str
    placement: str | None
    response_object_name: str
    compression_codec: str = "zstd"

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
            "compression_codec": self.compression_codec,
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
    ssl_cert_hostname: str = ""
    ssl_sni_hostname: str = ""

    def __post_init__(self) -> None:
        host = self.ssl_cert_hostname or self.ssl_sni_hostname or self.ssl_hostname
        if host:
            if not self.ssl_cert_hostname:
                self.ssl_cert_hostname = host
            if not self.ssl_sni_hostname:
                self.ssl_sni_hostname = host
            if not self.ssl_hostname:
                self.ssl_hostname = host

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
    created: bool = False

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

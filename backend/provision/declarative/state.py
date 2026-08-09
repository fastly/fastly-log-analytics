"""FeatureState: Immutable, validated representation of desired service configuration.

The FeatureState is the sole input to the VCL generation system. It's constructed
via from_config() which auto-injects mandatory custom fields when features are
enabled (rum_cid for RUM, edge_score for Scoring, cmcd_* for CMCD).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Literal


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
    """Immutable representation of the desired configuration of a Fastly service.

    Always construct via from_config() to ensure mandatory custom fields are
    auto-injected when features are enabled.
    """

    service_id: str

    # Core Logging Settings
    log_period: int  # Rotation period in seconds (e.g. 60)
    sample_rate: int  # Log sampling percentage (1 to 100)
    edge_only: bool  # Capture logs only at the CDN edge hop
    custom_condition: str  # Arbitrary operator-defined condition string
    fos_prefix: str  # Prefix for raw S3 logs
    fos_endpoint: str  # FOS endpoint hostname for RUM asset-fetch backend
    logging_enabled: bool = True  # Toggle standard CDN request logging
    fos_region: str = "us-east-1"
    cdn_shield: str = ""
    fos_access_key_id: str = ""
    fos_secret_access_key: str = ""
    fos_bucket: str = ""
    logging_endpoint_name: str = "Fastly Log Analytics"  # Main request logs endpoint name
    rum_endpoint_name: str = "Fastly RUM Logs"  # RUM beacon logs endpoint name

    # Feature Toggles & Parameter Blocks
    rum_enabled: bool = False

    cmcd: CmcdConfig = field(default_factory=CmcdConfig)
    scoring: ScoringConfig = field(default_factory=ScoringConfig)

    # Log Field Collections
    log_fields: LogFieldsConfig = field(default_factory=LogFieldsConfig)

    def __post_init__(self) -> None:
        """Validate cross-feature dependencies and field constraints.

        Raises:
            ValueError: if validation fails.
        """
        # Validate log_period range
        if not (30 <= self.log_period <= 3600):
            raise ValueError(f"log_period must be in range [30, 3600], got {self.log_period}")

        # Validate sample_rate range
        if not (1 <= self.sample_rate <= 100):
            raise ValueError(f"sample_rate must be in range [1, 100], got {self.sample_rate}")

        # Validate custom field names
        for field_dict in self.log_fields.custom_fields:
            field_name = field_dict.get("name", "")
            if not re.match(r"^[a-z_][a-z0-9_]*$", field_name):
                raise ValueError(f"Custom field name '{field_name}' must be lowercase alphanumeric + underscore")

        # Validate cmcd settings
        if self.cmcd.enabled and self.cmcd.mode not in ("query_string", "headers"):
            raise ValueError(f"cmcd.mode must be 'query_string' or 'headers', got {self.cmcd.mode!r}")
        if self.cmcd.enabled and self.cmcd.version not in (1, 2):
            raise ValueError(f"cmcd.version must be 1 or 2, got {self.cmcd.version}")

        # Validate scoring settings
        if self.scoring.enabled and not self.scoring.domain:
            raise ValueError("scoring.enabled=True requires scoring.domain to be set")
        if self.scoring.enabled:
            if not (400 <= self.scoring.enforce_status_code <= 599):
                raise ValueError(
                    f"scoring.enforce_status_code must be in range [400, 599], got {self.scoring.enforce_status_code}"
                )

    @classmethod
    def from_config(cls, cfg: dict) -> FeatureState:
        """Parse raw config dict and construct FeatureState with automatic dependency injection.

        Algorithm:
        1. Parse all core logging settings and feature toggles from cfg.
        2. Start with user-provided custom_fields list.
        3. If rum_enabled=True and no field named 'rum_cid' exists, inject it.
        4. If scoring_enabled=True and no field named 'edge_score' exists, inject it.
        5. If cmcd_enabled=True and no field named 'cmcd_*' exists, inject all CMCD fields.
        6. Validate: all required fields for enabled features are present.

        Args:
            cfg: Raw configuration dictionary.

        Returns:
            FeatureState instance with auto-injected mandatory fields.

        Raises:
            KeyError: if required core logging settings are missing.
            ValueError: if validation fails.
        """
        # Extract core logging settings
        service_id = cfg.get("service_id")
        if not service_id:
            raise KeyError("config missing 'service_id'")

        log_period = cfg.get("log_period")
        if log_period is None:
            raise KeyError("config missing 'log_period'")
        log_period = int(log_period)

        # Extract endpoint names and request log settings from provisioning config or top-level (for backwards compatibility)
        prov_cfg = cfg.get("provisioning", {})

        sample_rate = prov_cfg.get("sample_rate") if "sample_rate" in prov_cfg else cfg.get("sample_rate")
        if sample_rate is None:
            sample_rate = 100  # Default fallback for backward compat
        sample_rate = int(sample_rate)

        edge_only = prov_cfg.get("edge_only") if "edge_only" in prov_cfg else cfg.get("edge_only", False)
        edge_only = bool(edge_only)
        custom_condition = (
            prov_cfg.get("custom_condition") if "custom_condition" in prov_cfg else cfg.get("custom_condition", "")
        )
        custom_condition = (custom_condition or "").strip()
        fos_prefix = cfg.get("fos_prefix", "")
        fos_endpoint = cfg.get("fos_endpoint", "fos.example.com")
        logging_enabled = cfg.get("logging_enabled", True)
        fos_region = cfg.get("fos_region", "us-east-1")
        cdn_shield = cfg.get("cdn_shield", "")
        fos_access_key_id = cfg.get("fos_access_key_id", "")
        fos_secret_access_key = cfg.get("fos_secret_access_key", "")
        fos_bucket = cfg.get("fos_bucket", "") or cfg.get("fos_bucket_name", "")
        # Extract endpoint names from provisioning config or top-level (for backwards compatibility)
        logging_endpoint_name = prov_cfg.get("endpoint_name", "") or cfg.get("endpoint_name", "Fastly Log Analytics")
        rum_cfg = cfg.get("rum", {})
        rum_endpoint_name = rum_cfg.get("endpoint_name", "") or cfg.get("rum_endpoint_name", "Fastly RUM Logs")

        # Extract feature toggles
        rum_enabled = cfg.get("rum_enabled", False)
        if not rum_enabled:
            rum_cfg = cfg.get("rum", {})
            if isinstance(rum_cfg, dict):
                rum_enabled = bool(rum_cfg.get("enabled", False))

        # Extract and construct CMCD config
        cmcd_cfg = cfg.get("cmcd", {})
        if not isinstance(cmcd_cfg, dict):
            cmcd_cfg = {}
        # Support backwards compat: flat fields override nested, but normalize to nested
        cmcd_enabled = cfg.get("cmcd_enabled", cmcd_cfg.get("enabled", False))
        cmcd_mode = cfg.get("cmcd_mode", cmcd_cfg.get("mode", "query_string"))
        cmcd_version = cfg.get("cmcd_version", cmcd_cfg.get("version", 1))
        cmcd_config = CmcdConfig(enabled=cmcd_enabled, mode=cmcd_mode, version=cmcd_version)

        # Extract and construct Scoring config
        scoring_cfg = cfg.get("scoring", {})
        if not isinstance(scoring_cfg, dict):
            scoring_cfg = {}
        # Support backwards compat: flat fields override nested, but normalize to nested
        scoring_enabled = cfg.get("scoring_enabled", scoring_cfg.get("enabled", False))
        scoring_domain = (
            cfg.get("scoring_domain") or scoring_cfg.get("domain") or scoring_cfg.get("scoring_domain") or ""
        )
        scoring_request_secret = cfg.get("scoring_request_secret", scoring_cfg.get("request_secret", ""))
        scoring_exclude_url_regex = cfg.get("scoring_exclude_url_regex", scoring_cfg.get("exclude_url_regex", ""))
        scoring_enforce_status_code = cfg.get(
            "scoring_enforce_status_code", scoring_cfg.get("enforce_status_code", 429)
        )
        scoring_config = ScoringConfig(
            enabled=scoring_enabled,
            domain=scoring_domain,
            request_secret=scoring_request_secret,
            exclude_url_regex=scoring_exclude_url_regex,
            enforce_status_code=scoring_enforce_status_code,
        )

        # Extract log field collections (nested format only — Task 9.5 migration handles conversion)
        log_fields_cfg = cfg.get("log_fields", {})
        if not isinstance(log_fields_cfg, dict):
            log_fields_cfg = {}

        log_groups = log_fields_cfg.get("groups") or cfg.get("log_groups") or cfg.get("groups") or []
        custom_fields = log_fields_cfg.get("custom_fields") or cfg.get("custom_fields") or []
        field_overrides = log_fields_cfg.get("field_overrides") or cfg.get("field_overrides") or {}

        # Make a mutable copy of custom_fields for injection
        injected_fields = list(custom_fields) if custom_fields else []

        # Auto-inject mandatory RUM custom fields
        if rum_enabled:
            from backend.provision.rum_orchestrator_v2 import _RUM_CUSTOM_FIELDS

            existing_names = {f.get("name") for f in injected_fields}
            for field in _RUM_CUSTOM_FIELDS:
                if field["name"] not in existing_names:
                    injected_fields.append(field)

        # Auto-inject mandatory Scoring custom fields
        if scoring_enabled:
            mandatory_score_fields = [
                ("edge_score", "Edge Score", "req.http.x-edge-score:score"),
                ("edge_score_l1", "Edge Score (Layer 1)", "req.http.x-edge-score:l1"),
                ("edge_score_l2", "Edge Score (Layer 2)", "req.http.x-edge-score:l2"),
                ("edge_cookie_compliance", "Cookie Compliance", "req.http.x-edge-score:compliance"),
                ("edge_score_reason", "Score Reason", "req.http.x-edge-score:reason"),
                ("edge_sid", "Session ID", "req.http.x-edge-score:sid"),
                ("edge_score_rtt_us", "Scorer Round-Trip (µs)", "req.http.x-edge-score:rtt"),
                ("edge_score_exec_us", "Scorer Exec (µs)", "req.http.x-edge-score:exec"),
                ("edge_matrix_version", "Matrix Version", "req.http.x-edge-score:matrix"),
            ]

            existing_names = {f.get("name") for f in injected_fields}
            for field_name, label, vcl_expr in mandatory_score_fields:
                if field_name not in existing_names:
                    injected_fields.append(
                        {
                            "name": field_name,
                            "label": label,
                            "description": "Auto-injected by scoring feature.",
                            "vcl_log_expression": vcl_expr,
                            "collection_stage": "deliver",
                            "duckdb_type": "VARCHAR"
                            if "compliance" in field_name or "reason" in field_name or "version" in field_name
                            else "INTEGER",
                            "value_type": "string"
                            if "compliance" in field_name or "reason" in field_name or "version" in field_name
                            else "numeric",
                            "bytes_estimate": 60 if "reason" in field_name or "version" in field_name else 10,
                            "enabled": True,
                        }
                    )

        # Auto-inject mandatory CMCD custom fields
        if cmcd_enabled:
            cmcd_fields = [
                ("cmcd_sid", "CMCD Session ID", "req.http.x-cmcd:sid"),
                ("cmcd_cid", "CMCD Content ID", "req.http.x-cmcd:cid"),
                ("cmcd_br", "Encoded Bitrate (kbps)", "req.http.x-cmcd:br"),
                ("cmcd_bl", "Buffer Length (ms)", "req.http.x-cmcd:bl"),
                ("cmcd_bs", "Buffer Starvation", "req.http.x-cmcd:bs"),
                ("cmcd_d", "Object Duration (ms)", "req.http.x-cmcd:d"),
                ("cmcd_dl", "Deadline (ms)", "req.http.x-cmcd:dl"),
                ("cmcd_mtp", "Measured Throughput (kbps)", "req.http.x-cmcd:mtp"),
                ("cmcd_ot", "Object Type", "req.http.x-cmcd:ot"),
                ("cmcd_sf", "Streaming Format", "req.http.x-cmcd:sf"),
                ("cmcd_st", "Stream Type", "req.http.x-cmcd:st"),
                ("cmcd_su", "Startup", "req.http.x-cmcd:su"),
                ("cmcd_tb", "Top Bitrate (kbps)", "req.http.x-cmcd:tb"),
                ("cmcd_rtp", "Requested Max Throughput (kbps)", "req.http.x-cmcd:rtp"),
            ]

            existing_names = {f.get("name") for f in injected_fields}
            for field_name, label, vcl_expr in cmcd_fields:
                if field_name not in existing_names:
                    injected_fields.append(
                        {
                            "name": field_name,
                            "label": label,
                            "description": "Auto-injected by CMCD feature.",
                            "vcl_log_expression": vcl_expr,
                            "collection_stage": "edge",
                            "duckdb_type": "BOOLEAN"
                            if "bs" in field_name or "su" in field_name
                            else (
                                "INTEGER"
                                if any(x in field_name for x in ["br", "bl", "d", "dl", "mtp", "tb", "rtp"])
                                else "VARCHAR"
                            ),
                            "value_type": "boolean"
                            if "bs" in field_name or "su" in field_name
                            else (
                                "numeric"
                                if any(x in field_name for x in ["br", "bl", "d", "dl", "mtp", "tb", "rtp"])
                                else "string"
                            ),
                            "bytes_estimate": 1
                            if "bs" in field_name or "su" in field_name
                            else (
                                5 if any(x in field_name for x in ["br", "bl", "d", "dl", "mtp", "tb", "rtp"]) else 40
                            ),
                            "enabled": True,
                        }
                    )

        # Construct nested LogFieldsConfig
        log_fields_config = LogFieldsConfig(
            groups=log_groups,
            custom_fields=injected_fields,
            field_overrides=field_overrides,
        )

        # Construct FeatureState; __post_init__ will validate
        return cls(
            service_id=service_id,
            log_period=log_period,
            sample_rate=sample_rate,
            edge_only=edge_only,
            custom_condition=custom_condition,
            fos_prefix=fos_prefix,
            fos_endpoint=fos_endpoint,
            logging_enabled=logging_enabled,
            fos_region=fos_region,
            cdn_shield=cdn_shield,
            fos_access_key_id=fos_access_key_id,
            fos_secret_access_key=fos_secret_access_key,
            fos_bucket=fos_bucket,
            logging_endpoint_name=logging_endpoint_name,
            rum_endpoint_name=rum_endpoint_name,
            rum_enabled=rum_enabled,
            cmcd=cmcd_config,
            scoring=scoring_config,
            log_fields=log_fields_config,
        )

    def to_dict(self) -> dict:
        """Serialize to a dictionary suitable for JSON storage.

        Returns:
            Dict with all fields.
        """
        return {
            "service_id": self.service_id,
            "log_period": self.log_period,
            "sample_rate": self.sample_rate,
            "edge_only": self.edge_only,
            "custom_condition": self.custom_condition,
            "fos_prefix": self.fos_prefix,
            "fos_endpoint": self.fos_endpoint,
            "logging_enabled": self.logging_enabled,
            "fos_region": self.fos_region,
            "cdn_shield": self.cdn_shield,
            "fos_access_key_id": self.fos_access_key_id,
            "fos_secret_access_key": self.fos_secret_access_key,
            "fos_bucket": self.fos_bucket,
            "logging_endpoint_name": self.logging_endpoint_name,
            "rum_endpoint_name": self.rum_endpoint_name,
            "rum_enabled": self.rum_enabled,
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

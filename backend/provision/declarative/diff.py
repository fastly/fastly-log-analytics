"""Diff computation between desired and current Fastly state.

Computes the minimal set of snippets, endpoints, and backends to add/update/remove
to bring the current state into sync with the desired state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class VCLSnippet:
    """A single VCL snippet (name, priority, body, subroutine)."""

    name: str
    priority: int
    body: str
    subroutine: str

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "priority": self.priority,
            "body": self.body,
            "subroutine": self.subroutine,
        }

    def __hash__(self) -> int:
        return hash((self.name, self.priority, self.body, self.subroutine))

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, VCLSnippet):
            return NotImplemented
        return (
            self.name == other.name
            and self.priority == other.priority
            and self.body == other.body
            and self.subroutine == other.subroutine
        )


@dataclass
class LoggingEndpoint:
    """A single logging endpoint (S3, etc)."""

    name: str
    endpoint_type: str  # "s3"
    path: str
    period: int
    response_condition: str
    format_string: str
    placement: str | None  # "waf_log", "none", etc
    response_object_name: str

    def to_dict(self) -> dict:
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

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, LoggingEndpoint):
            return NotImplemented
        return (
            self.name == other.name
            and self.endpoint_type == other.endpoint_type
            and self.path == other.path
            and self.period == other.period
            and self.response_condition == other.response_condition
            and self.format_string == other.format_string
            and self.placement == other.placement
            and self.response_object_name == other.response_object_name
        )


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

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "address": self.address,
            "port": self.port,
            "ssl_check_cert": self.ssl_check_cert,
            "ssl_hostname": self.ssl_hostname,
            "connect_timeout": self.connect_timeout,
            "first_byte_timeout": self.first_byte_timeout,
            "between_bytes_timeout": self.between_bytes_timeout,
            "auto_loadbalance": self.auto_loadbalance,
            "use_ssl": self.use_ssl,
            "override_host": self.override_host,
            "shield": self.shield,
            "request_condition": self.request_condition,
        }

    def __hash__(self) -> int:
        return hash((self.name, self.address, self.port))

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Backend):
            return NotImplemented
        return (
            self.name == other.name
            and self.address == other.address
            and self.port == other.port
            and self.ssl_check_cert == other.ssl_check_cert
            and self.ssl_hostname == other.ssl_hostname
            and self.connect_timeout == other.connect_timeout
            and self.first_byte_timeout == other.first_byte_timeout
            and self.between_bytes_timeout == other.between_bytes_timeout
            and self.auto_loadbalance == other.auto_loadbalance
            and self.use_ssl == other.use_ssl
            and self.override_host == other.override_host
            and self.shield == other.shield
            and self.request_condition == other.request_condition
        )


@dataclass
class ServiceDictionary:
    """A single Fastly service dictionary (ConfigStore)."""

    name: str
    write_only: bool = False
    items: dict[str, str] = field(default_factory=dict)

    def __hash__(self) -> int:
        return hash((self.name, tuple(sorted(self.items.items()))))

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ServiceDictionary):
            return NotImplemented
        return self.name == other.name and self.items == other.items


@dataclass
class DiffResult:
    """Computed diff between current and desired state."""

    snippets_to_add: list[VCLSnippet] = field(default_factory=list)
    snippets_to_update: list[VCLSnippet] = field(default_factory=list)
    snippets_to_remove: list[str] = field(default_factory=list)  # names only

    endpoints_to_add: list[LoggingEndpoint] = field(default_factory=list)
    endpoints_to_update: list[LoggingEndpoint] = field(default_factory=list)
    endpoints_to_remove: list[str] = field(default_factory=list)  # names only

    backends_to_add: list[Backend] = field(default_factory=list)
    backends_to_update: list[Backend] = field(default_factory=list)
    backends_to_remove: list[str] = field(default_factory=list)  # names only

    dictionaries_to_add: list[ServiceDictionary] = field(default_factory=list)
    dictionaries_to_update: list[ServiceDictionary] = field(default_factory=list)
    dictionaries_to_remove: list[str] = field(default_factory=list)  # names only

    def is_empty(self) -> bool:
        """True if no changes needed."""
        return (
            not self.snippets_to_add
            and not self.snippets_to_update
            and not self.snippets_to_remove
            and not self.endpoints_to_add
            and not self.endpoints_to_update
            and not self.endpoints_to_remove
            and not self.backends_to_add
            and not self.backends_to_update
            and not self.backends_to_remove
            and not self.dictionaries_to_add
            and not self.dictionaries_to_update
            and not self.dictionaries_to_remove
        )

    def summary(self) -> dict:
        """Return a summary of changes for logging."""
        return {
            "snippets_added": len(self.snippets_to_add),
            "snippets_updated": len(self.snippets_to_update),
            "snippets_removed": len(self.snippets_to_remove),
            "endpoints_added": len(self.endpoints_to_add),
            "endpoints_updated": len(self.endpoints_to_update),
            "endpoints_removed": len(self.endpoints_to_remove),
            "backends_added": len(self.backends_to_add),
            "backends_updated": len(self.backends_to_update),
            "backends_removed": len(self.backends_to_remove),
            "dictionaries_added": len(self.dictionaries_to_add),
            "dictionaries_updated": len(self.dictionaries_to_update),
            "dictionaries_removed": len(self.dictionaries_to_remove),
        }


def _diff_named_resources(
    current_items: list[Any],
    desired_items: list[Any],
    label: str,
) -> tuple[list[Any], list[Any], list[str]]:
    current_by_name = {item.name: item for item in current_items}
    desired_by_name = {item.name: item for item in desired_items}

    to_add: list[Any] = []
    to_update: list[Any] = []
    to_remove: list[str] = []

    for name, desired_item in desired_by_name.items():
        if name not in current_by_name:
            to_add.append(desired_item)
        elif current_by_name[name] != desired_item:
            to_update.append(desired_item)

    for name in current_by_name:
        if name not in desired_by_name:
            to_remove.append(name)

    overlap = {item.name for item in to_add} & {item.name for item in to_update}
    if overlap:
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
    """Compute diff between current and desired state.

    Returns a DiffResult with lists of snippets/endpoints/backends/dictionaries to add/update/remove.

    Args:
        current_snippets: Currently deployed snippets from Fastly.
        desired_snippets: Snippets we want to deploy.
        current_endpoints: Currently deployed logging endpoints.
        desired_endpoints: Logging endpoints we want to deploy.
        current_backends: Currently deployed backends.
        desired_backends: Backends we want to deploy.
        current_dictionaries: Currently deployed dictionaries. Defaults to empty list.
        desired_dictionaries: Dictionaries we want to deploy. Defaults to empty list.

    Returns:
        DiffResult with computed diffs.

    Assertions:
        - No resource appears in both to_add and to_update (mutually exclusive).
        - Diff is deterministic (same input → same output).
    """
    diff = DiffResult()
    current_dictionaries = current_dictionaries or []
    desired_dictionaries = desired_dictionaries or []

    diff.snippets_to_add, diff.snippets_to_update, diff.snippets_to_remove = _diff_named_resources(
        current_snippets, desired_snippets, "Snippets"
    )
    diff.endpoints_to_add, diff.endpoints_to_update, diff.endpoints_to_remove = _diff_named_resources(
        current_endpoints, desired_endpoints, "Endpoints"
    )
    diff.backends_to_add, diff.backends_to_update, diff.backends_to_remove = _diff_named_resources(
        current_backends, desired_backends, "Backends"
    )
    diff.dictionaries_to_add, diff.dictionaries_to_update, diff.dictionaries_to_remove = _diff_named_resources(
        current_dictionaries, desired_dictionaries, "Dictionaries"
    )

    return diff

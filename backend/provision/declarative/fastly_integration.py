"""Fastly API integration for declarative reconciliation.

Thin wrappers around backend.core.fastly.client to support the reconciler's
control loop. These functions are called by reconcile_vcl_state() to fetch
current state and apply mutations.
"""

from __future__ import annotations

import urllib.parse

from backend.core.fastly.client import fastly
from backend.provision.declarative.diff import Backend, LoggingEndpoint, ServiceDictionary, VCLSnippet

# ============================================================================
# Fetch Current State (Read-Only)
# ============================================================================


def fetch_active_version(service_id: str, token: str) -> int | None:
    """Fetch the currently active version number.

    Returns:
        Active version number, or None if no active version exists (new service).
    """
    try:
        active = fastly("GET", f"/service/{service_id}/version/active", token=token)
        if isinstance(active, dict) and active.get("active"):
            return int(active["number"])
    except RuntimeError:
        pass

    try:
        resp = fastly("GET", f"/service/{service_id}/version", token=token)
        # Fastly returns a list or a dictionary; find the active one
        if isinstance(resp, list):
            for item in resp:
                if isinstance(item, dict) and item.get("active"):
                    return int(item["number"])
        elif isinstance(resp, dict) and "items" in resp:
            for item in resp["items"]:
                if item.get("active"):
                    return int(item["number"])
        return None
    except Exception:
        return None


def fetch_snippets(service_id: str, version: int, token: str) -> list[VCLSnippet]:
    """Fetch all VCL snippets for a service version.

    Args:
        service_id: Fastly service ID.
        version: Version number.
        token: API token.

    Returns:
        List of VCLSnippet objects.
    """
    snippets: list[VCLSnippet] = []
    try:
        resp = fastly(
            "GET",
            f"/service/{service_id}/version/{version}/snippet",
            token=token,
        )
        items = resp
        if isinstance(resp, dict) and "items" in resp:
            items = resp["items"]
        elif not isinstance(resp, list):
            items = []

        for item in items:
            snippet = VCLSnippet(
                name=item.get("name", ""),
                priority=int(item.get("priority", 100)),
                body=item.get("snippet", ""),
                subroutine=item.get("type", "vcl_recv"),
            )
            snippets.append(snippet)
    except Exception:
        pass
    return snippets


def fetch_logging_endpoints(service_id: str, version: int, token: str) -> list[LoggingEndpoint]:
    """Fetch all S3 logging endpoints for a service version.

    Args:
        service_id: Fastly service ID.
        version: Version number.
        token: API token.

    Returns:
        List of LoggingEndpoint objects.
    """
    endpoints: list[LoggingEndpoint] = []
    try:
        resp = fastly(
            "GET",
            f"/service/{service_id}/version/{version}/logging/s3",
            token=token,
        )
        items = resp
        if isinstance(resp, dict) and "items" in resp:
            items = resp["items"]
        elif not isinstance(resp, list):
            items = []

        for item in items:
            endpoint = LoggingEndpoint(
                name=item.get("name", ""),
                endpoint_type="s3",
                path=item.get("path", ""),
                period=int(item.get("period", 3600)),
                response_condition=item.get("response_condition", "true"),
                format_string=item.get("format", ""),
                placement=item.get("placement", "none"),
                response_object_name=item.get("response_object_name", ""),
            )
            endpoints.append(endpoint)
    except Exception:
        pass
    return endpoints


def fetch_backends(service_id: str, version: int, token: str) -> list[Backend]:
    """Fetch all backends for a service version.

    Args:
        service_id: Fastly service ID.
        version: Version number.
        token: API token.

    Returns:
        List of Backend objects.
    """
    backends: list[Backend] = []
    try:
        resp = fastly(
            "GET",
            f"/service/{service_id}/version/{version}/backend",
            token=token,
        )
        items = resp
        if isinstance(resp, dict) and "items" in resp:
            items = resp["items"]
        elif not isinstance(resp, list):
            items = []

        for item in items:
            backend = Backend(
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
            backends.append(backend)
    except Exception:
        pass
    return backends


def fetch_dictionaries(service_id: str, version: int, token: str) -> list[ServiceDictionary]:
    """Fetch all service dictionaries (ConfigStore) for a service version.

    Args:
        service_id: Fastly service ID.
        version: Version number.
        token: API token.

    Returns:
        List of ServiceDictionary objects.
    """
    dicts: list[ServiceDictionary] = []
    try:
        resp = fastly(
            "GET",
            f"/service/{service_id}/version/{version}/dictionary",
            token=token,
        )
        items = resp
        if isinstance(resp, dict) and "items" in resp:
            items = resp["items"]
        elif not isinstance(resp, list):
            items = []

        for item in items:
            dict_name = item.get("name", "")
            dict_id = item.get("id", "")
            write_only = item.get("write_only", False)

            entries_dict = {}
            if dict_id:
                try:
                    items_resp = fastly(
                        "GET",
                        f"/service/{service_id}/dictionary/{dict_id}/items",
                        token=token,
                    )
                    items_entries = items_resp
                    if isinstance(items_resp, dict) and "items" in items_resp:
                        items_entries = items_resp["items"]
                    elif isinstance(items_resp, dict):
                        items_entries = [items_resp]
                    elif not isinstance(items_resp, list):
                        items_entries = []

                    for entry in items_entries:
                        if isinstance(entry, dict):
                            key = entry.get("item_key", "") or entry.get("key", "")
                            val = entry.get("item_value", "") or entry.get("value", "")
                            if key:
                                entries_dict[key] = val
                except Exception:
                    pass

            dicts.append(
                ServiceDictionary(
                    name=dict_name,
                    write_only=write_only,
                    items=entries_dict,
                )
            )
    except Exception:
        pass
    return dicts


# ============================================================================
# Mutations (Write Operations)
# ============================================================================


def clone_version(service_id: str, version: int, token: str, comment: str = "") -> int:
    """Clone an existing version to create a new draft.

    Args:
        service_id: Fastly service ID.
        version: Version number to clone.
        token: API token.
        comment: Optional comment for the new version.

    Returns:
        New draft version number.

    Raises:
        RuntimeError: if clone fails.
    """
    resp = fastly(
        "PUT",
        f"/service/{service_id}/version/{version}/clone",
        token=token,
    )
    new_version = int(resp["number"])

    if comment:
        fastly(
            "PUT",
            f"/service/{service_id}/version/{new_version}",
            {"comment": comment},
            token=token,
        )

    return new_version


def delete_snippet(service_id: str, version: int, snippet_name: str, token: str) -> None:
    """Delete a VCL snippet.

    Args:
        service_id: Fastly service ID.
        version: Version number.
        snippet_name: Name of snippet to delete.
        token: API token.
    """
    encoded_name = urllib.parse.quote(snippet_name, safe="")
    fastly(
        "DELETE",
        f"/service/{service_id}/version/{version}/snippet/{encoded_name}",
        token=token,
        expect_empty=True,
    )


def create_or_update_snippet(
    service_id: str,
    version: int,
    snippet: VCLSnippet,
    token: str,
) -> None:
    """Create or update a VCL snippet.

    Args:
        service_id: Fastly service ID.
        version: Version number.
        snippet: VCLSnippet to create/update.
        token: API token.
    """
    # Strip "vcl_" prefix from subroutine name if present
    snippet_type = (
        snippet.subroutine.replace("vcl_", "") if snippet.subroutine.startswith("vcl_") else snippet.subroutine
    )

    # Try to get existing snippet
    encoded_name = urllib.parse.quote(snippet.name, safe="")
    try:
        fastly(
            "GET",
            f"/service/{service_id}/version/{version}/snippet/{encoded_name}",
            token=token,
        )
        # Exists, update it
        fastly(
            "PUT",
            f"/service/{service_id}/version/{version}/snippet/{encoded_name}",
            {
                "name": snippet.name,
                "type": snippet_type,
                "priority": snippet.priority,
                "content": snippet.body,
                "dynamic": 0,
            },
            token=token,
        )
    except RuntimeError:
        # Doesn't exist, create it
        fastly(
            "POST",
            f"/service/{service_id}/version/{version}/snippet",
            {
                "name": snippet.name,
                "type": snippet_type,
                "priority": snippet.priority,
                "content": snippet.body,
                "dynamic": 0,
            },
            token=token,
        )


def delete_logging_endpoint(service_id: str, version: int, endpoint_name: str, token: str) -> None:
    """Delete an S3 logging endpoint.

    Args:
        service_id: Fastly service ID.
        version: Version number.
        endpoint_name: Name of endpoint to delete.
        token: API token.
    """
    encoded_name = urllib.parse.quote(endpoint_name, safe="")
    fastly(
        "DELETE",
        f"/service/{service_id}/version/{version}/logging/s3/{encoded_name}",
        token=token,
        expect_empty=True,
    )


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
    """Create or update an S3 logging endpoint.

    Args:
        service_id: Fastly service ID.
        version: Version number.
        endpoint: LoggingEndpoint to create/update.
        token: API token.
        bucket_name: Optional S3 bucket name.
        domain: Optional S3 domain/endpoint.
        access_key: Optional S3 access key.
        secret_key: Optional S3 secret key.
    """
    # Try to get existing endpoint
    encoded_name = urllib.parse.quote(endpoint.name, safe="")
    body = {
        "name": endpoint.name,
        "path": endpoint.path,
        "period": endpoint.period,
        "format": endpoint.format_string,
        "placement": endpoint.placement,
        "response_object_name": endpoint.response_object_name,
        "format_version": 2,
        "gzip_level": 9,
    }
    if endpoint.response_condition:
        body["response_condition"] = endpoint.response_condition

    # Add S3-specific parameters if provided
    if bucket_name:
        body["bucket_name"] = bucket_name
    if domain:
        body["domain"] = domain
    if access_key:
        body["access_key"] = access_key
    if secret_key:
        body["secret_key"] = secret_key

    try:
        fastly(
            "GET",
            f"/service/{service_id}/version/{version}/logging/s3/{encoded_name}",
            token=token,
        )
        # Exists, update it
        fastly(
            "PUT",
            f"/service/{service_id}/version/{version}/logging/s3/{encoded_name}",
            body,
            token=token,
        )
    except RuntimeError:
        # Doesn't exist, create it
        fastly(
            "POST",
            f"/service/{service_id}/version/{version}/logging/s3",
            body,
            token=token,
        )


def delete_backend(service_id: str, version: int, backend_name: str, token: str) -> None:
    """Delete a backend.

    Args:
        service_id: Fastly service ID.
        version: Version number.
        backend_name: Name of backend to delete.
        token: API token.
    """
    encoded_name = urllib.parse.quote(backend_name, safe="")
    fastly(
        "DELETE",
        f"/service/{service_id}/version/{version}/backend/{encoded_name}",
        token=token,
        expect_empty=True,
    )


def create_or_update_backend(service_id: str, version: int, backend: Backend, token: str) -> None:
    """Create or update a backend.

    Args:
        service_id: Fastly service ID.
        version: Version number.
        backend: Backend to create/update.
        token: API token.
    """
    body = {
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
    }
    if backend.override_host:
        body["override_host"] = backend.override_host
    if backend.shield:
        body["shield"] = backend.shield
    else:
        body["shield"] = None

    # Try to get existing backend
    encoded_name = urllib.parse.quote(backend.name, safe="")
    try:
        fastly(
            "GET",
            f"/service/{service_id}/version/{version}/backend/{encoded_name}",
            token=token,
        )
        # Exists, update it
        fastly(
            "PUT",
            f"/service/{service_id}/version/{version}/backend/{encoded_name}",
            body,
            token=token,
        )
    except RuntimeError:
        # Doesn't exist, create it
        fastly(
            "POST",
            f"/service/{service_id}/version/{version}/backend",
            body,
            token=token,
        )


def delete_dictionary(service_id: str, version: int, dict_name: str, token: str) -> None:
    """Delete a service dictionary.

    Args:
        service_id: Fastly service ID.
        version: Version number.
        dict_name: Name of dictionary to delete.
        token: API token.
    """
    encoded_name = urllib.parse.quote(dict_name, safe="")
    fastly(
        "DELETE",
        f"/service/{service_id}/version/{version}/dictionary/{encoded_name}",
        token=token,
        expect_empty=True,
    )


def create_or_update_dictionary(
    service_id: str,
    version: int,
    dictionary: ServiceDictionary,
    token: str,
) -> None:
    """Create or update a service dictionary and its items.

    Args:
        service_id: Fastly service ID.
        version: Version number.
        dictionary: ServiceDictionary to create/update.
        token: API token.
    """
    encoded_name = urllib.parse.quote(dictionary.name, safe="")

    try:
        fastly(
            "GET",
            f"/service/{service_id}/version/{version}/dictionary/{encoded_name}",
            token=token,
        )
        dict_exists = True
    except RuntimeError:
        dict_exists = False

    if dict_exists:
        resp = fastly(
            "PUT",
            f"/service/{service_id}/version/{version}/dictionary/{encoded_name}",
            {
                "name": dictionary.name,
                "write_only": dictionary.write_only,
            },
            token=token,
        )
    else:
        resp = fastly(
            "POST",
            f"/service/{service_id}/version/{version}/dictionary",
            {
                "name": dictionary.name,
                "write_only": dictionary.write_only,
            },
            token=token,
        )

    dict_id = resp.get("id", "")
    if not dict_id:
        raise RuntimeError(f"Failed to get dictionary ID for {dictionary.name}")

    if dictionary.items:
        _upsert_dictionary_items(service_id, dict_id, dictionary.items, token)


def _upsert_dictionary_items(service_id: str, dict_id: str, items: dict[str, str], token: str) -> None:
    """Upsert items into a service dictionary.

    Args:
        service_id: Fastly service ID.
        dict_id: Dictionary ID.
        items: Dict of key-value pairs to upsert.
        token: API token.
    """
    for key, value in items.items():
        encoded_key = urllib.parse.quote(key, safe="")
        try:
            fastly(
                "PATCH",
                f"/service/{service_id}/dictionary/{dict_id}/item/{encoded_key}",
                {"item_value": value},
                token=token,
            )
        except RuntimeError:
            fastly(
                "POST",
                f"/service/{service_id}/dictionary/{dict_id}/items",
                {
                    "item_key": key,
                    "item_value": value,
                },
                token=token,
            )


def validate_version(service_id: str, version: int, token: str) -> bool:
    """Validate a service version.

    Args:
        service_id: Fastly service ID.
        version: Version number.
        token: API token.

    Returns:
        True if valid, False if invalid.

    Raises:
        RuntimeError: with validation error details if validation fails.
    """
    try:
        resp = fastly(
            "GET",
            f"/service/{service_id}/version/{version}/validate",
            token=token,
        )
        # Check if validation result is good
        return resp.get("status") == "ok"
    except RuntimeError as e:
        # Fastly returns a 400 on validation failure with error details
        raise RuntimeError(f"VCL validation failed: {e}")


def activate_version(service_id: str, version: int, token: str) -> None:
    """Activate a draft version.

    Args:
        service_id: Fastly service ID.
        version: Version number to activate.
        token: API token.
    """
    fastly(
        "PUT",
        f"/service/{service_id}/version/{version}/activate",
        token=token,
    )

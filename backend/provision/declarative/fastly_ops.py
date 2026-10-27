# ruff: noqa: T201
"""Fastly API operations, file locking, and draft version mutation helpers."""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.parse
from collections.abc import Callable
from pathlib import Path
from typing import Any

from backend.core.fastly.client import fastly
from backend.provision.declarative.models import (
    _AUTO_INJECTED_NAMES,
    MANAGED_BACKEND_NAMES,
    MANAGED_DICTIONARY_NAMES,
    Backend,
    DiffResult,
    FeatureState,
    LockAcquisitionTimeout,
    LoggingEndpoint,
    ServiceDictionary,
    VCLSnippet,
)
from backend.provision.declarative.vcl_builder import (
    _is_managed_snippet,
    build_rum_log_condition,
    desired_backends,
)

fastly_integration: Any = sys.modules[__name__]
logger = logging.getLogger(__name__)


def upload_rum_tracker_js(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from backend.provision import orchestrator as _orch_mod

    return _orch_mod.upload_rum_tracker_js(*args, **kwargs)


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

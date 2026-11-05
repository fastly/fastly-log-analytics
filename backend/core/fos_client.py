"""Fastly Object Storage (FOS) S3 client, Geo/DMA & ASN lookup, service source config, and metadata helpers."""

from __future__ import annotations

import concurrent.futures
import contextvars
import json
import logging
import os
import re
import secrets
import socket
import sys
import threading
import urllib.parse
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
from botocore import UNSIGNED
from botocore.config import Config

from backend import config as svcconfig
from backend.utils.bounded_cache import BoundedLRUCache
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.telemetry import parse_iso_utc

logger = logging.getLogger(__name__)

_TASK_TO_CRON_KEY = {"sync": "cron_sync", "local_compact": "cron_compact"}

_CDN_URL_ALLOWED_HOST_SUFFIXES = (
    ".fastly.net",
    ".fastlystorage.app",
)


def _safe_cdn_url(cdn_url: str | None) -> str | None:
    """Return ``cdn_url`` only if it's an https:// URL on an allowlisted
    Fastly hostname, else None.
    """
    if not cdn_url:
        return None
    try:
        parsed = urllib.parse.urlsplit(cdn_url)
    except ValueError:
        return None
    if parsed.scheme != "https":
        return None
    hostname = (parsed.hostname or "").lower()
    if not hostname:
        return None
    for suffix in _CDN_URL_ALLOWED_HOST_SUFFIXES:
        if hostname.endswith(suffix):
            return cdn_url
    return None


def _source_service_id(source: dict | None) -> str:
    return str(source.get("name") or source.get("service_id") or "") if source else ""


_dma_map_cache: dict[str, str] | None = None


def _get_dma_map() -> dict[str, str]:
    global _dma_map_cache
    if _dma_map_cache is not None:
        return _dma_map_cache
    _dma_map_cache = {}
    for fname in ("data/system/dma_geojson.json", "data/system/dma.json"):
        if not os.path.exists(fname):
            continue
        try:
            with open(fname) as f:
                data = json.load(f)
            for feature in data.get("features", []):
                props = feature.get("properties", {})
                code, name = props.get("dma_code"), props.get("name") or props.get("dma_name") or props.get("dma1")
                if code is not None and name:
                    _dma_map_cache[str(int(code))] = name
            break
        except Exception:
            logger.warning("Could not load %s", fname, exc_info=True)
    return _dma_map_cache


def _source_from_config(cfg: dict) -> dict:
    return svcconfig.config_to_source(cfg)


def _build_default_source() -> dict:
    cfgs = svcconfig.list_configs()
    if cfgs:
        return _source_from_config(cfgs[0])
    return {
        "name": "default",
        "endpoint": "",
        "access_key_id": "",
        "secret_access_key": "",
        "bucket": "",
        "prefix": "",
        "region": "us-east-1",
        "cdn_url": "",
        "cdn_secret": "",
        "cdn_service_id": "",
        "logging_service_id": "",
        "duckdb_path": "logs.duckdb",
        "storage_mode": "cloud",
    }


_DEFAULT_SOURCE = _build_default_source()


def get_source_for_service(service_id: str) -> dict | None:
    if (cfg := svcconfig.load_config(service_id)) is not None:
        return _source_from_config(cfg)
    return next(
        (_source_from_config(c) for c in svcconfig.list_configs() if c.get("cdn_service_id") == service_id), None
    )


def reload_default_source() -> None:
    _DEFAULT_SOURCE.update(_build_default_source())


def is_configured(source: dict | None = None) -> bool:
    s = source or _DEFAULT_SOURCE
    return all(s.get(k) for k in ("endpoint", "access_key_id", "secret_access_key", "bucket"))


def _safe_table_name(name: str) -> str:
    clean = re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_").lower()
    return f"logs_{clean}" if clean != "default" else "logs"


def _cache_dir(source: dict | None = None) -> str:
    src = source or _DEFAULT_SOURCE
    if "_cache_dir_override" in src:
        return src["_cache_dir_override"]
    return os.path.join("cache", (src.get("bucket") or src.get("fos_bucket") or "default").strip())


def current_rss_bytes() -> int | None:
    try:
        with open("/proc/self/statm") as f:
            return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    except (FileNotFoundError, OSError, ValueError, IndexError):
        try:
            import resource

            raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            return raw if sys.platform == "darwin" else raw * 1024
        except Exception:
            return None


def rum_duckdb_path(path: str) -> str:
    return path.removesuffix(".duckdb") + ".rum.duckdb"


def rum_source_for(source: dict) -> dict:
    rum_source = {**source, "name": f"{source.get('name') or source.get('service_id') or 'default'}::rum"}
    if source.get("duckdb_path"):
        rum_source["duckdb_path"] = rum_duckdb_path(source["duckdb_path"])
    return rum_source


_cid_salt_lock = threading.Lock()
_process_cid_salt: str | None = None


def ensure_cid_salt(rum_cfg: dict) -> str:
    if not (salt := rum_cfg.get("cid_salt")):
        rum_cfg["cid_salt"] = salt = secrets.token_hex(32)
    return salt


def get_or_generate_cid_salt(service_id: str) -> str:
    global _process_cid_salt
    from backend.config import load_config, mutate_config

    if cfg := load_config(service_id):
        if isinstance(rum_cfg := cfg.get("rum"), dict) and rum_cfg.get("cid_salt"):
            return rum_cfg["cid_salt"]
    else:
        with _cid_salt_lock:
            if _process_cid_salt is None:
                _process_cid_salt = secrets.token_hex(32)
            logger.warning(
                "cid_salt_no_config service_id=%s: using a non-persistent per-process salt; cids will not be stable",
                service_id,
            )
            return _process_cid_salt
    logger.warning("cid_salt_missing service_id=%s: generating and persisting under cross-process lock", service_id)

    def _set(c: dict) -> None:
        raw_rum = c.get("rum")
        rum: dict[str, Any] = raw_rum if isinstance(raw_rum, dict) else {}
        ensure_cid_salt(rum)
        c["rum"] = rum

    saved = mutate_config(service_id, _set)
    return get_or_generate_cid_salt(service_id) if saved is None else saved["rum"]["cid_salt"]


def rotate_cid_salt(service_id: str) -> str:
    """Rotate the pseudonymous RUM cid salt in config and purge ClickHouse RUM tables."""
    from backend.config import mutate_config
    from backend.core.reset import reset_service_rum

    new_salt = secrets.token_hex(32)

    def _rotate(c: dict) -> None:
        raw_rum = c.get("rum")
        rum: dict[str, Any] = raw_rum if isinstance(raw_rum, dict) else {}
        rum["cid_salt"] = new_salt
        c["rum"] = rum

    saved = mutate_config(service_id, _rotate, allow_cid_salt_change=True)
    if saved is None:
        raise ValueError(f"No service found with id {service_id!r}")
    for _ in reset_service_rum(service_id, delete_raw_logs=False, delete_fos=False):
        pass
    return saved["rum"]["cid_salt"]


_fos_client_cache: BoundedLRUCache[tuple[str, str, str, str], Any] = BoundedLRUCache(maxsize=128)
_fos_client_lock = threading.Lock()
_CacheRegistry.register("duckdb._fos_client_cache", _fos_client_cache)


class _ProxyPaginatorShim:
    def __init__(self, paginator, caller_hint: str | None):
        self._paginator, self._caller_hint = paginator, caller_hint

    def _set_caller_hint_context(self):
        from backend.utils.telemetry import _BOTO3_CALLER_HINT

        return _BOTO3_CALLER_HINT.set(self._caller_hint)

    def _reset_caller_hint_context(self, token) -> None:
        from backend.utils.telemetry import _BOTO3_CALLER_HINT

        _BOTO3_CALLER_HINT.reset(token)

    def paginate(self, *args, **kwargs):
        token = self._set_caller_hint_context()
        try:
            yield from self._paginator.paginate(*args, **kwargs)
        finally:
            self._reset_caller_hint_context(token)

    def __getattr__(self, name):
        return getattr(self._paginator, name)


_FOS_SPAN_METHODS = frozenset(
    {
        "list_objects_v2",
        "list_objects",
        "get_object",
        "head_object",
        "put_object",
        "copy_object",
        "delete_object",
        "delete_objects",
    }
)


class _ProxyClientShim:
    def __init__(self, obj):
        self._obj = obj

    def get_paginator(self, s3_method: str, caller_hint: str | None = None):
        return _ProxyPaginatorShim(self._obj.get_paginator(s3_method), caller_hint)

    def __getattr__(self, name):
        attr = getattr(self._obj, name)
        if name not in _FOS_SPAN_METHODS or not callable(attr):
            return attr

        def _span_wrapped(*args, **kwargs):
            try:
                from backend.core.request_telemetry import (
                    _end_db_span,
                    _start_db_span,
                )

                span = _start_db_span("fos", name, None)
            except Exception:
                span = None
            try:
                result = attr(*args, **kwargs)
            except BaseException as exc:
                _end_db_span(span, exc)
                raise
            _end_db_span(span, None)
            return result

        return _span_wrapped


def _get_fos_client(source: dict):
    source_key = (
        source.get("name", "default"),
        source.get("access_key_id", ""),
        source.get("cdn_url", ""),
        source.get("cdn_secret", ""),
    )
    with _fos_client_lock:
        if source_key in _fos_client_cache:
            return _fos_client_cache[source_key]
        from backend.utils import telemetry as telemetry_proxy

        telemetry_proxy.start_proxy_server()
        proxy_config = Config(
            retries={"max_attempts": 3, "mode": "adaptive"},
            connect_timeout=10,
            read_timeout=30,
            signature_version=UNSIGNED,
            s3={"addressing_style": "path"},
            max_pool_connections=128,
        )
        raw_client = boto3.client("s3", endpoint_url=telemetry_proxy.proxy_endpoint(), config=proxy_config)
        telemetry_proxy.install_boto3_proxy_hook(raw_client, source)
        client = _ProxyClientShim(raw_client)
        _fos_client_cache[source_key] = client
        return client


def clear_fos_client(source_or_name: Any = None) -> None:
    if source_or_name is None:
        with _fos_client_lock:
            _fos_client_cache.clear()
        return
    name = source_or_name.get("name", "default") if isinstance(source_or_name, dict) else source_or_name
    with _fos_client_lock:
        for k in [k for k in _fos_client_cache if (k[0] if isinstance(k, tuple) else k) == name]:
            _fos_client_cache.pop(k, None)


def fos_reachable(source: dict) -> dict:
    try:
        client = _get_fos_client(source)
        client.list_objects_v2(Bucket=source.get("bucket"), Prefix=(source.get("prefix") or "").strip("/"), MaxKeys=1)
        return {"reachable": True, "error": None}
    except Exception as e:  # noqa: BLE001
        return {"reachable": False, "error": str(e)[:200]}


def get_raw_tree_node(source: dict | None, prefix_filter: str = "", root: str = "raw") -> dict[str, Any]:
    src = source or _DEFAULT_SOURCE
    if not is_configured(src):
        return {"children": []}
    if prefix_filter and not prefix_filter.endswith("/"):
        prefix_filter += "/"
    prefix = src["prefix"].strip().rstrip("/")
    target_prefix = f"{prefix + '/' if prefix else ''}{root}/{prefix_filter}"
    children = []
    try:
        paginator = _get_fos_client(src).get_paginator("list_objects_v2")
        dirs_map: dict[str, dict[str, Any]] = {}
        files = []
        object_count = 0
        for page in paginator.paginate(Bucket=src["bucket"], Prefix=target_prefix):
            for obj in page.get("Contents", ()):
                object_count += 1
                key = obj["Key"]
                rel_path = key[len(target_prefix) :] if key != target_prefix else ""
                if not rel_path:
                    continue
                parts = rel_path.split("/")
                if len(parts) == 1:
                    mtime = obj["LastModified"].isoformat() if "LastModified" in obj else None
                    files.append({"name": parts[0], "type": "file", "size": obj["Size"], "mtime": mtime, "key": key})
                else:
                    dmeta = dirs_map.setdefault(
                        parts[0], {"size": 0, "count": 0, "prefix": f"{prefix_filter}{parts[0]}"}
                    )
                    dmeta["size"] += obj["Size"]
                    dmeta["count"] += 1
            if object_count >= 10000:
                break
        dirs = [
            {"name": dname, "type": "directory", "size": meta["size"], "prefix": meta["prefix"]}
            for dname, meta in sorted(dirs_map.items())
        ]
        files.sort(key=lambda x: x["name"])
        children = dirs + files
    except Exception:
        logger.error("Error in get_raw_tree_node", exc_info=True)
    return {"children": children}


def get_storage_tree_node(source: dict | None, prefix_filter: str = "") -> dict[str, Any]:
    """List the service's canonical Archive Parquet files in FOS under ``archive/``."""
    return get_raw_tree_node(source, prefix_filter=prefix_filter, root="archive")


def _is_permission_error(err: Any) -> bool:
    s = str(err or "")
    return "AccessDenied" in s or "UnauthorizedAccess" in s


def _delete_objects_robust_with_failures(fos_client, bucket: str, keys: list[str]) -> tuple[int, set[str]]:
    if not keys:
        return 0, set()
    failed_keys: set[str] = set()
    try:
        batch_size, total_deleted = 500, 0
        for i in range(0, len(keys), batch_size):
            batch = keys[i : i + batch_size]
            response = fos_client.delete_objects(
                Bucket=bucket, Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True}
            )
            errors = response.get("Errors", [])
            batch_failures = (
                set(batch)
                if any(err.get("Key") not in batch for err in errors)
                else {err.get("Key") for err in errors if err.get("Key") in batch}
            )
            if errors and _is_permission_error(errors[0].get("Code", "")):
                logger.warning(
                    "Bulk delete skipped due to missing permissions (%s). Disabling further delete attempts for this batch.",
                    errors[0].get("Code"),
                )
                return total_deleted + len(batch) - len(batch_failures), failed_keys | batch_failures | set(
                    keys[i + len(batch) :]
                )
            total_deleted += len(batch) - len(batch_failures)
            failed_keys.update(batch_failures)
        return total_deleted, failed_keys
    except Exception as e:
        err_str = str(e)
        if _is_permission_error(err_str):
            logger.warning("Delete failed due to missing permissions: %s", err_str.split(":", 1)[-1].strip() or err_str)
            return 0, set(keys)
        logger.warning("Bulk delete failed, falling back to individual", exc_info=True)
        deleted_count, individual_failures = 0, set()
        if len(keys) <= 4:
            for index, k in enumerate(keys):
                try:
                    fos_client.delete_object(Bucket=bucket, Key=k)
                    deleted_count += 1
                except Exception as individual_err:
                    ind_err_str = str(individual_err)
                    if _is_permission_error(ind_err_str):
                        logger.warning(
                            "Individual delete failed due to missing permissions: %s. Stopping further deletes.",
                            ind_err_str,
                        )
                        individual_failures.update(keys[index:])
                        break
                    logger.warning("Failed to delete object %s", k, exc_info=True)
                    individual_failures.add(k)
            return deleted_count, individual_failures
        access_denied_event = threading.Event()

        def _delete_single_key(k: str) -> tuple[str, bool, str | None]:
            if access_denied_event.is_set():
                return k, False, "AccessDenied_aborted"
            try:
                fos_client.delete_object(Bucket=bucket, Key=k)
                return k, True, None
            except Exception as individual_err:
                ind_err_str = str(individual_err)
                if _is_permission_error(ind_err_str):
                    access_denied_event.set()
                return k, False, ind_err_str

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(32, len(keys)), thread_name_prefix="fos_del"
        ) as del_ex:
            for fut in concurrent.futures.as_completed(
                {del_ex.submit(contextvars.copy_context().run, _delete_single_key, k): k for k in keys}
            ):
                k, ok, err_s = fut.result()
                if ok:
                    deleted_count += 1
                else:
                    msg = (
                        "Individual delete permission error on %s: %s"
                        if _is_permission_error(err_s)
                        else "Failed to delete object %s: %s"
                    )
                    logger.warning(msg, k, err_s)
                    individual_failures.add(k)
        return deleted_count, individual_failures


def _delete_objects_robust(fos_client, bucket: str, keys: list[str]) -> int:
    return _delete_objects_robust_with_failures(fos_client, bucket, keys)[0]


ASN_CACHE_TTL_DAYS = 30


def get_asn_names(service_id: str, asns: list) -> dict:
    asns_clean = [int(a) for a in (asns or ()) if a is not None]
    if not asns_clean or not service_id:
        return {}
    from backend.core import metadata as metadata_db

    try:
        cached = metadata_db.lookup_asn_names(service_id, asns_clean, max_age_days=ASN_CACHE_TTL_DAYS)
    except Exception:
        cached = {}
    need = [a for a in asns_clean if a not in cached]
    resolved: dict[int, str] = {}
    if need:
        try:
            import cymruwhois  # type: ignore

            c = cymruwhois.Client()
            if callable(orig_connect := getattr(c, "_connect", None)):

                def _safe_connect() -> None:
                    host, port = getattr(c, "host", None), getattr(c, "port", None)
                    if isinstance(host, str) and isinstance(port, int):
                        c.socket = sock = socket.create_connection((host, port), timeout=2.0)
                        c.file = sock.makefile("rw", encoding="utf-8", errors="replace")
                    else:
                        orig_connect()
                        sock_obj = getattr(c, "socket", None)
                        if sock_obj is not None and hasattr(sock_obj, "settimeout"):
                            sock_obj.settimeout(2.0)
                        if callable(reconf := getattr(getattr(c, "file", None), "reconfigure", None)):
                            reconf(encoding="utf-8", errors="replace")

                c._connect = _safe_connect
            for res in c.lookupmany([f"AS{asn}" for asn in need]):
                if res and res.asn:
                    asn_int = int(res.asn)
                    raw_owner = res.owner or f"AS{asn_int}"
                    resolved[asn_int] = raw_owner.split(" - ", 1)[1] if " - " in raw_owner else raw_owner
        except Exception as exc:
            logger.warning("ASN resolution failed: %s", type(exc).__name__)
        for asn in need:
            resolved.setdefault(asn, f"AS{asn}")
        if resolved:
            try:
                metadata_db.upsert_asn_names(service_id, resolved)
            except Exception:
                logger.warning("get_asn_names: metadata_db.upsert_asn_names failed (ignored)", exc_info=True)
    result = {**cached, **resolved}
    for asn in need:
        result.setdefault(asn, f"AS{asn}")
    return result


def format_asn_label(asn: int, name: str) -> str:
    return f"AS{asn}" if not name or (name.startswith("AS") and name[2:].isdigit()) else f"{name} ({asn})"


def enrich_asn_labels(values: list[dict], service_id: str) -> list[dict]:
    asn_list = [int(v["value"]) for v in values if str(v["value"]).isdigit()]
    if not asn_list:
        return values
    names_map = get_asn_names(service_id, asn_list)
    for v in values:
        if str(v["value"]).isdigit():
            v["label"] = format_asn_label(int(v["value"]), names_map.get(int(v["value"]), ""))
    return values


def refresh_config_status(service_id: str, include_top_values: bool = True) -> None:
    """No-op compatibility hook for ClickHouse-backed services."""
    if not svcconfig.load_config(service_id):
        return
    logger.debug("[refresh_status] %s: ClickHouse-backed service status managed by ingest controller", service_id)


def start_cron_run(source: dict, task: str) -> int | None:
    from backend.core import metadata as metadata_db

    return metadata_db.start_cron_run(source["name"], task)


def log_cron_run(
    source: dict,
    task: str,
    duration_s: float,
    status: str,
    error_message: str | None = None,
    files_downloaded: int = 0,
    files_deleted_fos: int = 0,
    rows_ingested: int = 0,
    corrupt_rows: int = 0,
    parquet_files_created: int = 0,
    parquet_files_optimized: int = 0,
    parquet_keys: list | None = None,
    summary: str | None = None,
    log_output: str | None = None,
    run_id: int | None = None,
    outcome_counters: dict[str, int] | None = None,
):
    from backend.core import metadata as metadata_db

    service_id = source["name"]
    prov = (svcconfig.load_config(service_id) or {}).get("provisioning", {})
    cron_key = _TASK_TO_CRON_KEY.get(task)
    log_enabled = prov.get(cron_key, {}).get("log_enabled", True) if cron_key else True
    if status == "success" and corrupt_rows and corrupt_rows > 0:
        status = "partial_success"
    if not log_enabled and status == "success":
        if run_id is not None:
            try:
                metadata_db.delete_cron_run(service_id, run_id)
            except Exception:
                logger.warning("log_cron_run: metadata_db.delete_cron_run failed (ignored)", exc_info=True)
        return
    try:
        metadata_db.log_cron_run(
            service_id,
            task,
            duration_s,
            status,
            error_message=error_message,
            files_downloaded=files_downloaded or 0,
            files_deleted_fos=files_deleted_fos or 0,
            rows_ingested=rows_ingested or 0,
            corrupt_rows=corrupt_rows or 0,
            parquet_files_created=parquet_files_created or 0,
            parquet_files_optimized=parquet_files_optimized or 0,
            parquet_keys=parquet_keys or [],
            summary=summary,
            log_output=log_output,
            run_id=run_id,
            outcome_counters=outcome_counters,
        )
    except Exception as e:
        logger.warning("[cron_log] failed to persist cron run for %s/%s: %s", service_id, task, e)


def finalize_cron_run_if_running(
    source: dict,
    task: str,
    run_id: int | None,
    *,
    duration_s: float = 0.0,
    summary: str | None = None,
    error_message: str | None = None,
) -> bool:
    if run_id is None:
        return False
    from backend.core import metadata as metadata_db

    try:
        return metadata_db.finalize_cron_run_if_running(
            source["name"], task, run_id, duration_s=duration_s, summary=summary, error_message=error_message
        )
    except Exception as e:
        logger.warning("[cron_log] finalize-backstop failed for %s/%s: %s", source.get("name"), task, e)
        return False


def update_cron_duration(source: dict, run_id: int, duration_s: float, log_output: str | None = None):
    from backend.core import metadata as metadata_db

    if service_id := _source_service_id(source):
        metadata_db.update_cron_duration(service_id, run_id, duration_s, log_output=log_output)


def get_ingested_files(con: Any = None, source: dict | None = None) -> list[dict]:
    from backend.core import metadata as metadata_db

    if isinstance(con, dict) and source is None:
        source = con
    return metadata_db.list_ingested_files((source or _DEFAULT_SOURCE)["name"])


def log_usage_calls(source: dict, calls: list[dict], process_context: str | None = None):
    if svcconfig.is_usage_logging_enabled() and (service_id := _source_service_id(source)):
        from backend.core import metadata as metadata_db

        metadata_db.log_usage_calls(service_id, calls, process_context=process_context)


def backfill_fastly_edge_writes(source: dict) -> int:
    if not svcconfig.is_usage_logging_enabled() or not (service_id := _source_service_id(source)):
        return 0
    try:
        from backend.core import metadata as metadata_db

        since = (datetime.now(UTC) - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        files = metadata_db.list_unbackfilled_fastly_edge_files(service_id, since=since)
        if not files:
            return 0
        calls = [
            {
                "method": "PUT_OBJECT",
                "path": f_name,
                "service": "FOS",
                "details": "Class A · synthesized from ingest",
                "bytes": f_size,
                "status": "OK",
                "caller": "fastly.edge",
                "time_ms": 0,
                "_timestamp_override": (m.group(1) + "Z")
                if (m := re.search(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})", f_name))
                else f_ingested,
            }
            for f_name, f_ingested, _row_count, f_size in files
            if f_name != "__seeding_attempted__"
        ]
        return metadata_db.log_synthetic_usage(service_id, calls)
    except Exception as e:
        logger.debug("[usage_log] Fastly-edge write backfill failed: %s", e)
        return 0


def reconcile_fastly_stats(source: dict, hours_back: int = 12) -> int:
    if not svcconfig.is_usage_logging_enabled():
        return 0
    service_id, logging_svc_id = _source_service_id(source), source.get("logging_service_id", "")
    if not service_id or not logging_svc_id or not (api_key := svcconfig.get_fastly_api_key(logging_svc_id)):
        return 0
    try:
        from backend.core import metadata as metadata_db
        from backend.core.fastly.client import StatsResponse, fastly

        now_dt = datetime.now(UTC)
        if latest_recon := metadata_db.get_latest_reconciliation_ts(service_id):
            latest_dt = parse_iso_utc(latest_recon) if isinstance(latest_recon, str) else None
            if latest_dt is not None and (now_dt - latest_dt) < timedelta(hours=1):
                return 0
        now = now_dt.replace(minute=0, second=0, microsecond=0)
        from_ts, to_ts = (
            int((now - timedelta(hours=hours_back)).timestamp()),
            int((now + timedelta(hours=1)).timestamp()),
        )
        payload = fastly(
            "GET", f"/stats/aggregate?by=hour&from={from_ts}&to={to_ts}", token=api_key, max_retries=0, timeout=30
        )
        hourly = [
            {
                "hour_iso": datetime.fromtimestamp(rec.start_time, tz=UTC).strftime("%Y-%m-%dT%H:00:00Z"),
                "class_a": rec.fos_ops()[0],
                "class_b": rec.fos_ops()[1],
            }
            for rec in StatsResponse.model_validate(payload).data
            if rec.start_time is not None
        ]
        return metadata_db.reconcile_fastly_stats(service_id, hourly)
    except Exception as e:
        logger.debug("[usage_log] Fastly stats reconciliation failed: %s", e)
        return 0


def purge_usage_log(source: dict):
    retention_days = int(svcconfig.load_usage_logging_config().get("retention_days", 30))
    if service_id := _source_service_id(source):
        from backend.core import metadata as metadata_db

        metadata_db.purge_usage_log(service_id, retention_days)


__all__ = [
    "ASN_CACHE_TTL_DAYS",
    "_DEFAULT_SOURCE",
    "_cache_dir",
    "_delete_objects_robust",
    "_delete_objects_robust_with_failures",
    "_get_dma_map",
    "_get_fos_client",
    "_is_permission_error",
    "_safe_cdn_url",
    "_safe_table_name",
    "_source_from_config",
    "_source_service_id",
    "backfill_fastly_edge_writes",
    "clear_fos_client",
    "current_rss_bytes",
    "enrich_asn_labels",
    "ensure_cid_salt",
    "finalize_cron_run_if_running",
    "format_asn_label",
    "fos_reachable",
    "get_asn_names",
    "get_ingested_files",
    "get_or_generate_cid_salt",
    "get_raw_tree_node",
    "get_source_for_service",
    "get_storage_tree_node",
    "is_configured",
    "log_cron_run",
    "log_usage_calls",
    "purge_usage_log",
    "reconcile_fastly_stats",
    "refresh_config_status",
    "reload_default_source",
    "rotate_cid_salt",
    "rum_duckdb_path",
    "rum_source_for",
    "start_cron_run",
    "update_cron_duration",
]

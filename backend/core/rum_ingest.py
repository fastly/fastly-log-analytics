"""RUM beacon ingest pipeline.

Parses raw_rum/ logs from FOS and streams them into local Parquet buffers
which are then committed to Apache Iceberg tables.
Uses the standard ingested_files table to ensure atomic, de-duplicated imports.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Generator
from typing import Any

import pyarrow as pa

from backend.core import iceberg
from backend.core import metadata as metadata_db
from backend.core.duckdb import _get_fos_client, get_source_for_service
from backend.core.metadata.cron_log import (
    finalize_cron_run_if_running,
    log_cron_run,
    start_cron_run,
)
from backend.core.rollups.rum import (  # noqa: F401
    backfill_missing_rum_aggregates,
    recompute_rum_aggregates,
)
from backend.utils.telemetry import log_once_per

logger = logging.getLogger(__name__)


def safe_int(val, default: int = 0) -> int:
    if val is None or val == "":
        return default
    try:
        return int(val)
    except Exception:
        return default


def safe_float(val, default: float | None = None) -> float | None:
    if val is None or val == "":
        return default
    try:
        return float(val)
    except Exception:
        return default


def extract_metrics_from_faro_payload(payload: dict, log_data: dict) -> list[dict]:
    """Extract all distinct metrics, timings, and exceptions from a raw Faro payload."""
    extracted = []

    # Extract common context
    meta = payload.get("meta") or {}
    browser_meta = meta.get("browser") or {}
    os_meta = meta.get("os") or {}
    page_meta = payload.get("page") or meta.get("page") or {}

    browser_name = browser_meta.get("name") or log_data.get("browser") or "Chrome"
    os_name = os_meta.get("name") or log_data.get("os") or "macOS"
    device_type = "Desktop"
    if browser_meta.get("mobile"):
        device_type = "Mobile"

    url_str = page_meta.get("url") or log_data.get("url") or ""
    from urllib.parse import urlparse

    pathname = "/"
    if url_str:
        try:
            pathname = urlparse(url_str).path
        except Exception:
            if log_once_per("extract_metrics_from_faro_payload: urlparse failed (ignored)", 60):
                logger.debug("extract_metrics_from_faro_payload: urlparse failed (ignored)", exc_info=True)

    cid = log_data.get("rum_cid") or log_data.get("cid") or ""

    def _entry(metric_name: str, metric_value: Any, metric_rating: str = "", **extra: Any) -> dict[str, Any]:
        return {
            "metric_name": metric_name,
            "metric_value": metric_value,
            "metric_rating": metric_rating,
            "pathname": pathname,
            "cid": cid,
            "browser": browser_name,
            "os": os_name,
            "device": device_type,
            **extra,
        }

    # 1. Web Vitals
    measurements = payload.get("measurements") or []
    if isinstance(measurements, list):
        for m in measurements:
            if not isinstance(m, dict):
                continue
            if m.get("type") == "web-vitals":
                values = m.get("values") or {}
                rating = m.get("context", {}).get("rating") or (m.get("meta") or {}).get("rating") or ""
                for k, v in values.items():
                    if k == "delta":
                        continue
                    extracted.append(_entry(k, v, rating))

    # 2. Performance Navigation Timing and Custom Events (Interactions)
    events = payload.get("events") or []
    if isinstance(events, list):
        for e in events:
            if not isinstance(e, dict):
                continue
            name = e.get("name")
            if not name:
                continue
            if name == "faro.performance.navigation":
                attrs = e.get("attributes") or e.get("values") or {}
                for k, v in attrs.items():
                    extracted.append(_entry(k, v))
            else:
                # Custom/interaction event (like user click)
                extracted.append(_entry(f"event_{name}", 1.0))

    # 3. Custom logs
    logs = payload.get("logs") or []
    if isinstance(logs, list):
        for log_item in logs:
            if not isinstance(log_item, dict):
                continue
            extracted.append(_entry("log", 1.0, log_item.get("level") or "info"))

    # 3. Exceptions
    exceptions = payload.get("exceptions") or []
    if isinstance(exceptions, list):
        for exc in exceptions:
            if not isinstance(exc, dict):
                continue
            err_msg = exc.get("value") or exc.get("message") or "Unknown error"
            err_file = "unknown.js"
            err_line = 0
            err_col = 0
            stack = exc.get("stacktrace") or {}
            frames = stack.get("frames") or []
            if isinstance(frames, list) and len(frames) > 0:
                frame = frames[0]
                err_file = frame.get("filename") or "unknown.js"
                err_line = frame.get("lineno") or 0
                err_col = frame.get("colno") or 0

            extracted.append(
                _entry(
                    "exception",
                    None,
                    error_message=err_msg,
                    error_file=err_file,
                    error_line=err_line,
                    error_col=err_col,
                )
            )

    if not extracted:
        # Fallback pageview entry if we got a payload but no specific vitals/events were extracted
        extracted.append(_entry("pageview", 1.0))

    return extracted


def ingest_rum_logs(
    service_id: str,
    max_seconds: int | None = None,
    run_id: int | None = None,
) -> Generator[tuple]:
    """Ingest RUM beacon logs from FOS raw_rum/ prefix into local Parquet buffers & DuckDB Iceberg views."""
    import tempfile

    from backend.config import load_config, resolve_raw_delete_after
    from backend.core.iceberg._core import (
        CLIENT_ERRORS_ARROW_SCHEMA,
        CLIENT_VITALS_ARROW_SCHEMA,
    )
    from backend.core.ingest import (
        _RECONCILE_LEDGER_EPOCH,
        _capture_corrupt_container,
        _corrupt_gzip_error,
        _delete_objects_robust_with_failures,
        _deterministic_buffer_name,
        _download_chunk_to_local,
        _new_object_outcome,
        _parse_rum_beacon_file,
        _quarantine_rum_corrupt_lines,
        _recover_in_flight,
        list_fos_files,
    )

    start_time = time.time()
    if run_id is None:
        run_id = start_cron_run(service_id, "rum_sync")
    outcome_counters = _new_object_outcome(processed=0)
    yield ("started", run_id)

    src = get_source_for_service(service_id)
    if not src or not src.get("bucket"):
        logger.info(f"RUM sync skipped: bucket not configured for {service_id}")
        yield ("done", 0)
        log_cron_run(
            service_id,
            "rum_sync",
            time.time() - start_time,
            "success",
            files_downloaded=0,
            rows_ingested=0,
            outcome_counters=outcome_counters,
            run_id=run_id,
        )
        return

    cfg = load_config(service_id)
    raw_delete_after = resolve_raw_delete_after(cfg)

    try:
        s3 = _get_fos_client(src)
        bucket = src["bucket"]

        # Run crash recovery for BOTH RUM tables
        _recover_in_flight(src, table_name="client_vitals")
        _recover_in_flight(src, table_name="client_errors")

        # Fetch already ingested files to avoid duplicate processing.
        # Capping the fetch to 50k rows shares the process-wide dedup cache key
        # maintained by insert_ingested_files instead of an unbounded full-table
        # scan on every incremental RUM sync tick.
        vitals_ingested = (
            metadata_db.get_ingested_filenames(service_id, limit=50_000, table_name="client_vitals") or set()
        )
        errors_ingested = (
            metadata_db.get_ingested_filenames(service_id, limit=50_000, table_name="client_errors") or set()
        )
        already_ingested_raw = vitals_ingested.union(errors_ingested)

        bucket_prefix = f"s3://{bucket}/"
        already_ingested = set()
        for p in already_ingested_raw:
            if p.startswith("s3://"):
                already_ingested.add(p)
            else:
                already_ingested.add(f"{bucket_prefix}{p}")

        # Use the shared list_fos_files helper to discover files in raw/rum/ prefix
        list_gen = list_fos_files(
            src=src,
            prefix_subpath="raw/rum/",
            already_ingested=already_ingested,
            incremental_only=True,
            delete_after=raw_delete_after,
            elapsed_fn=lambda: f"{time.time() - start_time:.1f}s",
            fos_client=s3,
        )
        list_error = None
        try:
            while True:
                evt = next(list_gen)
                if evt.get("type") == "status":
                    logger.debug(f"RUM sync: {evt['message']}")
                elif evt.get("type") == "error":
                    list_error = evt.get("message", "FOS list error")
                    logger.error(f"RUM sync: {list_error}")
                    yield ("error", "list", list_error)
        except StopIteration as e:
            list_res = e.value

        if list_error:
            log_cron_run(
                service_id,
                "rum_sync",
                time.time() - start_time,
                "error",
                files_downloaded=0,
                rows_ingested=0,
                error_message=list_error,
                run_id=run_id,
            )
            return

        new_files_s3 = list_res["new_files"]
        file_sizes = list_res["file_sizes"]
        stranded_already = list_res.get("stranded_already", [])

        if not new_files_s3:
            logger.info(f"RUM sync: no new RUM logs found in bucket for {service_id}")
            yield ("done", 0)
            log_cron_run(
                service_id,
                "rum_sync",
                time.time() - start_time,
                "success",
                files_downloaded=0,
                rows_ingested=0,
                run_id=run_id,
            )
            return

        total_vitals_rows = 0
        total_errors_rows = 0
        error_count = 0
        failed_paths: set[str] = set()
        capture_failed_paths: set[str] = set()
        partial_paths: set[str] = set()
        delete_failed_paths: set[str] = set()
        deleted = 0
        reclaimed = 0

        # Download and process in parallel chunks to unify request and RUM ingestion logic
        CHUNK_SIZE = 50
        chunks = [new_files_s3[i : i + CHUNK_SIZE] for i in range(0, len(new_files_s3), CHUNK_SIZE)]

        for chunk_idx, chunk in enumerate(chunks):
            # Time limit check — Trap #41: ALWAYS allow the first chunk (chunk_idx == 0)
            # to run even if discovery consumed the full budget.
            if chunk_idx > 0 and max_seconds and (time.time() - start_time) > max_seconds:
                logger.warning(
                    "RUM sync: Reached time limit (%ds) for %s, stopping early after %d/%d files",
                    max_seconds,
                    service_id,
                    chunk_idx * CHUNK_SIZE,
                    len(new_files_s3),
                )
                yield ("status", "budget", f"Time limit of {max_seconds}s reached. Stopping batch early.")
                break

            chunk_vitals_rows: list[dict[str, object]] = []
            chunk_errors_rows: list[dict[str, object]] = []
            vitals_batch_records = []
            errors_batch_records = []

            with tempfile.TemporaryDirectory() as tmpdir:
                s3_to_local, _ = _download_chunk_to_local(s3, chunk, tmpdir)

                for s3_path in chunk:
                    outcome_counters["objects_processed"] += 1
                    if s3_path not in s3_to_local:
                        error_count += 1
                        failed_paths.add(s3_path)
                        logger.error(f"RUM sync: Failed to download {s3_path}")
                        yield ("error", s3_path, "Download failed")
                        continue

                    local_path = s3_to_local[s3_path]
                    size = file_sizes.get(s3_path, 0)

                    try:
                        file_vitals, file_errors, corrupt_lines = _parse_rum_beacon_file(local_path, service_id)
                    except Exception as e:
                        error_count += 1
                        failed_paths.add(s3_path)
                        failed_gzip = _corrupt_gzip_error(local_path)
                        if failed_gzip is not None:
                            q_res = _capture_corrupt_container(
                                service_id,
                                "rum",
                                s3_path.removeprefix(f"s3://{bucket}/"),
                                local_path,
                                failed_gzip,
                            )
                            outcome_counters["corrupt_containers"] += q_res["corrupt_containers"]
                            outcome_counters["cap_evictions"] += q_res["cap_evictions"]
                            outcome_counters["quarantine_capture_failures"] += q_res["quarantine_capture_failures"]
                            if q_res["quarantine_capture_failures"] > 0:
                                capture_failed_paths.add(s3_path)
                        logger.error(f"RUM sync: Failed to ingest RUM log file {s3_path}: {e}")
                        yield ("error", s3_path, str(e))
                        continue

                    valid_for_file = len(file_vitals) + len(file_errors)
                    outcome_counters["valid_records"] += valid_for_file

                    if corrupt_lines:
                        logger.warning(
                            "RUM sync: %s has %d malformed line(s) quarantined",
                            s3_path,
                            len(corrupt_lines),
                        )
                        try:
                            quarantine_result = _quarantine_rum_corrupt_lines(
                                s3,
                                src,
                                s3_path.removeprefix(f"s3://{bucket}/"),
                                corrupt_lines,
                                valid_for_file,
                                local_file=local_path,
                            )
                        except Exception:
                            logger.exception("RUM sync: exact-byte quarantine failed for %s", s3_path)
                            quarantine_result = {
                                "malformed_records": len(corrupt_lines),
                                "cap_evictions": 0,
                                "quarantine_capture_failures": len(corrupt_lines),
                            }
                        outcome_counters["malformed_records"] += quarantine_result["malformed_records"]
                        outcome_counters["cap_evictions"] += quarantine_result["cap_evictions"]
                        outcome_counters["quarantine_capture_failures"] += quarantine_result[
                            "quarantine_capture_failures"
                        ]
                        if quarantine_result["quarantine_capture_failures"]:
                            capture_failed_paths.add(s3_path)
                        elif valid_for_file > 0:
                            partial_paths.add(s3_path)

                    chunk_vitals_rows.extend(file_vitals)
                    chunk_errors_rows.extend(file_errors)
                    total_vitals_rows += len(file_vitals)
                    total_errors_rows += len(file_errors)

                    # Bookkeeping: record durable row counts (0 for files that produced no rows)
                    vitals_batch_records.append((s3_path, len(file_vitals), size))
                    errors_batch_records.append((s3_path, len(file_errors), size))
                    yield ("file_done", s3_path.split("/")[-1], valid_for_file)

            # End of chunk: Write tables to buffers
            buf_filename = _deterministic_buffer_name(chunk)
            for table_name, batch_records, chunk_rows, schema in (
                ("client_vitals", vitals_batch_records, chunk_vitals_rows, CLIENT_VITALS_ARROW_SCHEMA),
                ("client_errors", errors_batch_records, chunk_errors_rows, CLIENT_ERRORS_ARROW_SCHEMA),
            ):
                if not batch_records:
                    continue
                if chunk_rows:
                    metadata_db.record_in_flight(service_id, buf_filename, batch_records, table_name=table_name)
                    tbl = pa.Table.from_pylist(chunk_rows, schema=schema)
                    iceberg.write_to_buffer(src, tbl, buf_filename, table_name=table_name)
                    metadata_db.insert_ingested_files(service_id, batch_records, table_name=table_name)
                    metadata_db.clear_in_flight(service_id, buf_filename, table_name=table_name)
                else:
                    metadata_db.insert_ingested_files(service_id, batch_records, table_name=table_name)

            # Inline deletion per chunk (Gap 2)
            if raw_delete_after:
                exclude_from_delete = failed_paths | capture_failed_paths
                chunk_keys = [
                    f.removeprefix(f"s3://{bucket}/")
                    for f in chunk
                    if f.startswith(f"s3://{bucket}/") and f not in exclude_from_delete
                ]
                if chunk_keys:
                    try:
                        chunk_deleted, chunk_failed_keys = _delete_objects_robust_with_failures(s3, bucket, chunk_keys)
                        deleted += chunk_deleted
                        delete_failed_paths.update(f"s3://{bucket}/{k}" for k in chunk_failed_keys)
                    except Exception as del_err:
                        delete_failed_paths.update(f"s3://{bucket}/{k}" for k in chunk_keys)
                        logger.error("RUM sync: failed to delete %d raw files: %s", len(chunk_keys), del_err)

        # Reclaim stranded already-ingested objects (Gap 2)
        if raw_delete_after and stranded_already:
            try:
                deletable = metadata_db.get_reclaimable_strand_filenames(
                    service_id, set(stranded_already), _RECONCILE_LEDGER_EPOCH
                )
            except Exception as classify_err:
                logger.warning(
                    "RUM sync: could not classify reclaimable strands for %s (%s) — skipping reclaim this run",
                    service_id,
                    classify_err,
                )
                deletable = set()
            stranded_keys = [
                f.removeprefix(f"s3://{bucket}/")
                for f in stranded_already
                if f in deletable and f.startswith(f"s3://{bucket}/")
            ]
            if stranded_keys:
                try:
                    reclaim_del, reclaim_failures = _delete_objects_robust_with_failures(s3, bucket, stranded_keys)
                    reclaimed += reclaim_del
                    deleted += reclaim_del
                    delete_failed_paths.update(f"s3://{bucket}/{k}" for k in reclaim_failures)
                except Exception as rec_err:
                    delete_failed_paths.update(f"s3://{bucket}/{k}" for k in stranded_keys)
                    logger.error("RUM sync: failed to reclaim stranded raw files: %s", rec_err)

        yield ("done", total_vitals_rows + total_errors_rows)
        duration_s = time.time() - start_time

        # Outcome counters calculation matching ingest()
        outcome_counters["source_delete_failures"] = len(delete_failed_paths)
        failed_objects = failed_paths | capture_failed_paths | delete_failed_paths
        partial_objects = partial_paths - failed_objects
        outcome_counters["objects_failed"] = len(failed_objects)
        outcome_counters["objects_partial"] = len(partial_objects)
        outcome_counters["objects_successful"] = max(
            0,
            outcome_counters["objects_processed"]
            - outcome_counters["objects_failed"]
            - outcome_counters["objects_partial"],
        )
        had_errors = any(
            (
                error_count,
                outcome_counters["malformed_records"],
                outcome_counters["corrupt_containers"],
                outcome_counters["quarantine_capture_failures"],
                outcome_counters["source_delete_failures"],
                outcome_counters["objects_failed"],
            )
        )
        run_status = "error" if had_errors else "success"
        summary_msg = (
            f"Ingested {total_vitals_rows + total_errors_rows} RUM rows ({error_count} file error(s))"
            if had_errors
            else f"Ingested {total_vitals_rows + total_errors_rows} RUM rows"
        )
        log_cron_run(
            service_id,
            "rum_sync",
            duration_s,
            run_status,
            files_downloaded=len(new_files_s3),
            rows_ingested=total_vitals_rows + total_errors_rows,
            run_id=run_id,
            summary=summary_msg,
            outcome_counters=outcome_counters,
        )
    except Exception as e:
        logger.error(f"RUM ingest failed: {e}", exc_info=True)
        duration_s = time.time() - start_time
        log_cron_run(
            service_id,
            "rum_sync",
            duration_s,
            "error",
            files_downloaded=len(new_files_s3) if "new_files_s3" in locals() else 0,
            error_message=str(e),
            outcome_counters=outcome_counters,
            run_id=run_id,
        )
        yield ("error", "sync", str(e))
    finally:
        finalize_cron_run_if_running(service_id, "rum_sync", run_id)


# ============================================================================
# Faro SDK Version Inventory & Drift Helpers (folded from faro_versions.py)
# ============================================================================

"""Faro Web SDK version discovery and bundle downloading.

Fetches available versions from the npm registry and downloads the IIFE
bundle by pulling the published npm **tarball** from the registry itself
(no third-party CDN involved) and extracting the one file we need.

npm's registry publishes ``dist.integrity`` (SRI hash) / ``dist.shasum`` for
each version, but those hashes are computed over the package **tarball**
(the full ``.tgz``, e.g. ~570 files for ``@grafana/faro-web-sdk``) — never
over any single file inside it. An earlier version of this module fetched
the standalone ``dist/bundle/faro-web-sdk.iife.js`` from unpkg.com and
compared *those* bytes against ``dist.integrity``; that comparison can never
succeed for any real release, because the hashes describe different byte
sequences (confirmed empirically for 2.9.0: the tarball hash matches the
tarball, and does not match the unpkg-served single file). Every download
was therefore silently rejected as a "failed integrity verification",
regardless of whether unpkg served the genuine file.

The fix: download the tarball (the only artifact the registry's hash
actually describes), verify the *tarball* bytes against
``dist.integrity``/``dist.shasum``, and only after that succeeds extract
``package/dist/bundle/faro-web-sdk.iife.js`` from the verified archive. This
is real supply-chain verification — it checks exactly what npm published —
and it removes the unpkg dependency entirely: one request, one third-party
host, one signature the registry actually vouches for.
"""


import asyncio
import base64
import hashlib
import hmac
import io
import tarfile
from urllib.parse import urlsplit

import httpx

REGISTRY_URL = "https://registry.npmjs.org/@grafana/faro-web-sdk"

# npm tarballs use a fixed leading "package/" path component (mandated by
# the packing tool, not per-package config), so the member path inside the
# archive is stable across versions.
_BUNDLE_MEMBER_NAME = "package/dist/bundle/faro-web-sdk.iife.js"

# Single source of truth for "the version we self-host when nothing else is
# pinned." The generated RUM tracker JS unconditionally loads the first-party
# /js/faro-sdk.js (no third-party CDN fallback), so a version must always be
# pinned once RUM is enabled. ``enable_rum`` applies this default when a
# caller omits an explicit faro_version, and the RUM sync cron
# (backend/cron/jobs/rum_sync.py) adopts it to self-heal a service that was
# enabled before this default existed and never got a version pinned.
# Chosen by the operator to match npm's dist-tags.latest as of this task;
# bump deliberately when a new version is vetted, not automatically.
DEFAULT_FARO_VERSION = "2.10.0"

_TIMEOUT = 10.0
# The tarball (~180 KB for 2.9.0, includes source maps, README, etc.) is
# larger than the single extracted file (~98 KB) and originates from the
# same registry host as the metadata call, so it gets a slightly longer
# timeout than the plain JSON lookups.
_TARBALL_TIMEOUT = 20.0

# Real-world tarball is ~180 KB. This ceiling is generous headroom against a
# future release bloating the package, while still bounding memory/CPU
# spent hashing and un-gzipping a response before it's been verified as the
# artifact the registry actually published.
_TARBALL_MAX_BYTES = 10 * 1024 * 1024
# Real-world extracted bundle is ~98 KB.
_BUNDLE_MAX_BYTES = 5 * 1024 * 1024

# SRI integrity strings are "<algo>-<base64 digest>". Only algorithms with a
# hashlib constructor of the same name are attempted; npm publishes sha512
# almost universally, sha256/sha384 are supported as a courtesy.
_SRI_ALGOS = ("sha512", "sha384", "sha256")

# The registry is the only origin this module trusts for both the integrity
# metadata AND the tarball bytes it describes — a ``dist.tarball`` URL
# pointing anywhere else is refused before it is ever fetched.
_TRUSTED_TARBALL_HOST = "registry.npmjs.org"
_ABBREVIATED_PACKUMENT_HEADERS = {"Accept": "application/vnd.npm.install-v1+json"}


def _version_sort_key(version: str) -> tuple[int, ...]:
    """Parse a semver string into a comparable tuple of its first 3 parts."""
    return tuple(int(part) for part in version.split(".")[:3])


def _is_stable_numeric(version: str) -> bool:
    """True for plain ``X.Y.Z`` releases — no pre-release or build metadata.

    The numeric check also guards the sort: a registry entry whose first
    three parts aren't all digits would make ``int()`` raise mid-sort.

    ``str.isdigit()`` is Unicode-aware (true for fullwidth "１" or Arabic-Indic
    "١", not just ASCII 0-9), so it's paired with ``isascii()`` here — a
    registry key using non-ASCII digit codepoints must not be treated as a
    valid version, since this return value can flow onward as a "version"
    string into VCL/object-path interpolation (see
    backend/core/fastly/rum_provisioning.py's _assert_faro_version_safe).
    """
    if "-" in version or "+" in version:
        return False
    parts = version.split(".")[:3]
    return len(parts) == 3 and all(part.isascii() and part.isdigit() for part in parts)


async def fetch_available_faro_versions() -> list[str]:
    """Fetch available Faro Web SDK versions from the npm registry.

    Filters out pre-releases (versions containing ``-`` or ``+``) and sorts
    descending by semver (newest first).

    Returns:
        List of version strings, newest first (e.g. ``["2.9.0", "2.8.2", ...]``).

    Raises:
        ValueError: On a non-200 response, a network error, malformed JSON,
            or a payload without a usable ``versions`` map.
    """
    try:
        from backend.utils.telemetry import tracked_call

        transport = httpx.AsyncHTTPTransport()
        async with httpx.AsyncClient(transport=transport) as client:
            with tracked_call("GET", REGISTRY_URL, service="NPM Registry"):
                response = await client.get(
                    REGISTRY_URL,
                    headers=_ABBREVIATED_PACKUMENT_HEADERS,
                    timeout=_TIMEOUT,
                )
            response.raise_for_status()
            data = response.json()
    except httpx.HTTPStatusError as exc:
        raise ValueError(f"Failed to fetch Faro versions: registry returned {exc.response.status_code}") from exc
    except httpx.RequestError as exc:
        raise ValueError(f"Failed to fetch Faro versions: {exc}") from exc
    except ValueError as exc:  # json() raises ValueError/JSONDecodeError
        raise ValueError(f"Failed to fetch Faro versions: malformed registry JSON: {exc}") from exc

    versions_dict = data.get("versions") if isinstance(data, dict) else None
    if not isinstance(versions_dict, dict) or not versions_dict:
        raise ValueError("Failed to fetch Faro versions: registry response has no 'versions' map")

    stable_versions = [v for v in versions_dict if _is_stable_numeric(v)]
    stable_versions.sort(key=_version_sort_key, reverse=True)
    return stable_versions


# The published Faro Web SDK version list changes rarely (a handful of npm
# releases a year), but ``fetch_available_faro_versions`` makes a live
# external call to registry.npmjs.org with a 10s timeout. The /rum/versions
# page-load endpoint called it on EVERY request: measured in the 2026-10-01
# deploy run, each call took 0.4-12s and, under verify-phase CPU/network
# saturation, escalated to a 21.5s timeout -> ValueError -> 503 that
# hard-failed the RUM dashboard on all four environments. This process-level
# TTL cache (with stale-on-error) makes the page-load path return instantly
# from memory and never 503 on a transient registry hiccup — the versions
# list is a non-critical "is an upgrade available?" affordance, not page-
# critical data. Callers that must distinguish "registry unreachable" from
# "no newer version" (provisioning, the explicit upgrade action) keep using
# the raising ``fetch_available_faro_versions`` directly.
_VERSIONS_CACHE_TTL = 3600.0
# After a failed refresh we keep serving the stale list but retry sooner than
# the full TTL so recovery is quick once the registry is reachable again.
_VERSIONS_ERROR_BACKOFF = 30.0
_versions_cache_lock = asyncio.Lock()
# (expiry_monotonic, versions) or None when never successfully fetched.
_versions_cache: tuple[float, list[str]] | None = None


def reset_versions_cache() -> None:
    """Clear the cached version list (test seam)."""
    global _versions_cache
    _versions_cache = None


class _VersionsCacheProxy:
    """Adapter exposing ``.clear()`` so ``_versions_cache`` participates in ``CacheRegistry``."""

    @staticmethod
    def clear() -> None:
        reset_versions_cache()


from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("faro_versions._versions_cache", _VersionsCacheProxy())


async def get_faro_versions_cached() -> list[str]:
    """Return the Faro Web SDK version list for page-load use, cached.

    Serves a cached list within the TTL, refreshes past it, and on a refresh
    failure returns the last known list (stale) rather than raising. Returns
    an empty list only when the registry has never been reached — so a
    transient npm hiccup degrades the "update available" badge instead of
    503-ing the whole RUM page. Never raises.
    """
    global _versions_cache

    cached = _versions_cache
    if cached is not None and time.monotonic() < cached[0]:
        return cached[1]

    async with _versions_cache_lock:
        # Re-check under the lock: a concurrent caller may have just refreshed.
        cached = _versions_cache
        if cached is not None and time.monotonic() < cached[0]:
            return cached[1]
        try:
            versions = await fetch_available_faro_versions()
        except ValueError:
            if cached is not None:
                _versions_cache = (time.monotonic() + _VERSIONS_ERROR_BACKOFF, cached[1])
                return cached[1]
            return []
        _versions_cache = (time.monotonic() + _VERSIONS_CACHE_TTL, versions)
        return versions


async def _fetch_version_dist(client: httpx.AsyncClient, version: str) -> dict[str, Any]:
    """Return the npm registry's ``dist`` metadata for ``version``.

    A bundle can't be verified without this, so any registry failure here
    (network, non-200, malformed JSON, unknown version, missing dist block)
    raises ``ValueError`` — the caller must fail the download closed rather
    than silently skip verification on a registry hiccup.
    """
    try:
        from backend.utils.telemetry import tracked_call

        with tracked_call("GET", REGISTRY_URL, service="NPM Registry"):
            response = await client.get(
                REGISTRY_URL,
                headers=_ABBREVIATED_PACKUMENT_HEADERS,
                timeout=_TIMEOUT,
            )
        response.raise_for_status()
        data = response.json()
    except httpx.HTTPStatusError as exc:
        raise ValueError(
            f"Failed to verify Faro bundle {version}: registry returned {exc.response.status_code}"
        ) from exc
    except httpx.RequestError as exc:
        raise ValueError(f"Failed to verify Faro bundle {version}: registry error: {exc}") from exc
    except ValueError as exc:  # json() raises ValueError/JSONDecodeError
        raise ValueError(f"Failed to verify Faro bundle {version}: malformed registry JSON: {exc}") from exc

    versions_dict = data.get("versions") if isinstance(data, dict) else None
    version_meta = versions_dict.get(version) if isinstance(versions_dict, dict) else None
    dist = (
        version_meta.get("dist")
        if isinstance(version_meta, dict)
        else (data.get("dist") if isinstance(data, dict) else None)
    )
    if not isinstance(dist, dict):
        raise ValueError(f"Failed to verify Faro bundle {version}: registry has no dist metadata for this version")
    return dist


def _tarball_url_from_dist(dist: dict[str, Any], version: str) -> str:
    """Return the registry-published tarball URL for ``version``, host-pinned.

    ``dist.tarball`` is the only URL this module trusts for the bytes that
    ``dist.integrity``/``dist.shasum`` actually describe. It is expected to
    live on the same origin as the metadata call itself; refusing anything
    else is cheap defense-in-depth against a malformed or tampered
    ``tarball`` field pointing the download at an untrusted host.
    """
    tarball = dist.get("tarball")
    if not isinstance(tarball, str) or not tarball:
        raise ValueError(f"Failed to verify Faro bundle {version}: registry dist has no tarball URL")

    parsed = urlsplit(tarball)
    if parsed.scheme != "https" or parsed.hostname != _TRUSTED_TARBALL_HOST:
        raise ValueError(
            f"Failed to verify Faro bundle {version}: refusing untrusted tarball URL host {parsed.hostname!r}"
        )
    return tarball


def _verify_artifact_integrity(artifact: bytes, dist: dict[str, Any]) -> bool:
    """Verify ``artifact`` bytes against the registry's published ``dist``.

    ``artifact`` must be the raw tarball bytes — ``dist.integrity`` /
    ``dist.shasum`` are computed by npm over the packed ``.tgz``, never over
    any single file extracted from it. Prefers the SRI ``integrity`` field
    (``"<algo>-<base64 digest>"``, npm publishes sha512 almost universally);
    falls back to the legacy ``shasum`` (sha1 hex digest) field when
    integrity is absent. Returns False — never raises — when neither field
    is present/usable so the caller can raise a single, consistent "failed
    verification" error.
    """
    integrity = dist.get("integrity")
    if isinstance(integrity, str) and integrity:
        algo, sep, expected_b64 = integrity.partition("-")
        if sep and algo in _SRI_ALGOS and expected_b64:
            digest = hashlib.new(algo, artifact).digest()
            actual_b64 = base64.b64encode(digest).decode()
            return hmac.compare_digest(actual_b64, expected_b64)

    shasum = dist.get("shasum")
    if isinstance(shasum, str) and shasum:
        # npm's legacy ``dist.shasum`` field is always sha1 — not our
        # choice of algorithm, just matching what the registry publishes.
        actual_hex = hashlib.sha1(artifact, usedforsecurity=False).hexdigest()  # noqa: S324
        return hmac.compare_digest(actual_hex, shasum.lower())

    return False


def _extract_bundle_from_tarball(tarball: bytes, version: str) -> bytes:
    """Extract the IIFE bundle from an *already-integrity-verified* tarball.

    Never call this before ``_verify_artifact_integrity`` has returned True
    for ``tarball`` — extracting from an unverified archive defeats the
    point of verifying it.

    Reads exactly one member, by its exact expected name — never
    ``extractall()``. There is deliberately no "extraction root" for a
    traversal to escape: a member whose name isn't byte-for-byte
    ``_BUNDLE_MEMBER_NAME`` (an absolute path, a ``..``-laden path, or any
    other name) simply never matches and is ignored, so a maliciously named
    entry elsewhere in the archive can't affect anything. The matched member
    is additionally required to be a plain file (rejects a symlink/hardlink
    masquerading under the expected name) and is read with a hard size cap
    rather than trusted at face value.
    """
    try:
        with tarfile.open(fileobj=io.BytesIO(tarball), mode="r:gz") as tar:
            member = None
            for candidate in tar.getmembers():
                name = candidate.name
                # Defense-in-depth: reject unsafe-looking paths outright,
                # even though the exact-match below already can't select
                # one (belt-and-suspenders against a future refactor that
                # loosens the match to e.g. a suffix comparison).
                if name.startswith("/") or any(part == ".." for part in name.split("/")):
                    continue
                if name == _BUNDLE_MEMBER_NAME:
                    member = candidate
                    break

            if member is None:
                raise ValueError(f"Faro tarball {version} is missing expected member {_BUNDLE_MEMBER_NAME!r}")
            if not member.isfile():
                raise ValueError(f"Faro tarball {version} member {_BUNDLE_MEMBER_NAME!r} is not a regular file")
            if member.size > _BUNDLE_MAX_BYTES:
                raise ValueError(f"Faro tarball {version} member {_BUNDLE_MEMBER_NAME!r} exceeds size ceiling")

            extracted = tar.extractfile(member)
            if extracted is None:
                raise ValueError(f"Faro tarball {version} member {_BUNDLE_MEMBER_NAME!r} could not be read")
            data = extracted.read(_BUNDLE_MAX_BYTES + 1)
    except tarfile.TarError as exc:
        raise ValueError(f"Faro tarball {version} is not a valid gzipped tar archive: {exc}") from exc

    if len(data) > _BUNDLE_MAX_BYTES:
        raise ValueError(f"Faro tarball {version} member {_BUNDLE_MEMBER_NAME!r} exceeds size ceiling")
    return data


async def fetch_faro_bundle(version: str) -> bytes:
    """Download the Faro Web SDK npm tarball, verify it against the
    registry's published integrity hash, and return the extracted IIFE
    bundle bytes.

    Verification happens against the tarball — the only artifact
    ``dist.integrity``/``dist.shasum`` actually describes — never against
    the extracted file. Extraction only ever runs after verification
    succeeds.

    Args:
        version: Version string (e.g. ``"2.9.0"``).

    Returns:
        Raw bytes of ``package/dist/bundle/faro-web-sdk.iife.js``, taken
        from an integrity-verified tarball.

    Raises:
        ValueError: On 404 (unknown version), any other non-200 status, a
            network error, a registry lookup failure, an untrusted tarball
            host, an integrity mismatch/absence between the downloaded
            tarball and the registry's published hash for this version, a
            tarball that exceeds the size ceiling, or a verified tarball
            that doesn't contain the expected bundle member.
    """
    try:
        from backend.utils.telemetry import tracked_call

        transport = httpx.AsyncHTTPTransport()
        async with httpx.AsyncClient(transport=transport) as client:
            dist = await _fetch_version_dist(client, version)
            tarball_url = _tarball_url_from_dist(dist, version)
            with tracked_call("GET", tarball_url, service="NPM Registry"):
                response = await client.get(tarball_url, timeout=_TARBALL_TIMEOUT)
            response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise ValueError(f"Faro version {version} not found (404)") from exc
        raise ValueError(f"Failed to download Faro tarball {version}: HTTP {exc.response.status_code}") from exc
    except httpx.RequestError as exc:
        raise ValueError(f"Failed to download Faro tarball {version}: {exc}") from exc

    tarball = response.content
    if len(tarball) > _TARBALL_MAX_BYTES:
        raise ValueError(f"Faro tarball {version} exceeds size ceiling ({len(tarball)} bytes)")

    if not _verify_artifact_integrity(tarball, dist):
        raise ValueError(
            f"Faro tarball {version} failed integrity verification against the npm registry's "
            "published hash for this version — refusing to extract from an unverified archive"
        )

    return _extract_bundle_from_tarball(tarball, version)


import sys as _sys

_self_mod = _sys.modules[__name__]
_sys.modules["backend.core.faro_versions"] = _self_mod
_core_pkg = _sys.modules.get("backend.core")
if _core_pkg is not None:
    setattr(_core_pkg, "faro_versions", _self_mod)
_backend_pkg = _sys.modules.get("backend")
if _backend_pkg is not None and hasattr(_backend_pkg, "_MODULE_ALIASES"):
    _backend_pkg._MODULE_ALIASES.setdefault("backend.core.faro_versions", __name__)

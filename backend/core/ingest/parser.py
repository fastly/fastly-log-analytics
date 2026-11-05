"""Pure-Python compressed log stream inspection, ingest field catalog helpers, and quarantine outcome counters."""

from __future__ import annotations

import contextlib
import gzip
import io
import logging
import re
from typing import Any

import orjson
import zstandard as zstd

from backend.core.log_fields import LOG_FIELD_CATALOG

logger = logging.getLogger("backend.core.ingest")

_ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
_GZIP_MAGIC = b"\x1f\x8b"
_COMPRESSED_STREAM_ERRORS = (gzip.BadGzipFile, zstd.ZstdError, EOFError, OSError)


def _detect_compression(path: str) -> str:
    try:
        with open(path, "rb") as fh:
            magic = fh.read(4)
    except OSError:
        magic = b""
    if magic == _ZSTD_MAGIC:
        return "zstd"
    if magic[:2] == _GZIP_MAGIC:
        return "gzip"
    if path.endswith((".zst", ".zstd")):
        return "zstd"
    if path.endswith(".gz"):
        return "gzip"
    return "none"


@contextlib.contextmanager
def _open_compressed_log(path: str):
    codec = _detect_compression(path)
    if codec == "zstd":
        with open(path, "rb") as raw_fh:
            if raw_fh.read(4) != _ZSTD_MAGIC:
                raise zstd.ZstdError("invalid zstd header")
            raw_fh.seek(0)
            with zstd.ZstdDecompressor().stream_reader(raw_fh) as reader:
                yield io.BufferedReader(reader)
    elif codec == "gzip":
        with gzip.open(path, "rb") as fh:
            yield fh
    else:
        with open(path, "rb") as fh:
            yield fh


_INGEST_OUTCOME_COUNTER_KEYS = (
    "valid_records",
    "malformed_records",
    "corrupt_containers",
    "quarantine_capture_failures",
    "source_delete_failures",
    "cap_evictions",
    "objects_processed",
    "objects_successful",
    "objects_partial",
    "objects_failed",
)
_INGEST_EXCLUDED_GROUPS = ("METRICS", "VIRTUAL", "INTERNAL")
_EMPTY_VALUE_RE = re.compile(r":(?=[,}])")


def _new_object_outcome(*, processed: int = 1) -> dict[str, int]:
    return {**dict.fromkeys(_INGEST_OUTCOME_COUNTER_KEYS, 0), "objects_processed": processed}


def _mark_object_failed(counters: dict[str, int]) -> dict[str, int]:
    counters.update(objects_failed=1, objects_successful=0, objects_partial=0)
    return counters


def _failed_object_outcome() -> dict[str, int]:
    return _mark_object_failed(_new_object_outcome())


def _finalize_object_outcome(outcome: dict[str, int]) -> dict[str, int]:
    if outcome["quarantine_capture_failures"]:
        _mark_object_failed(outcome)
    elif outcome["malformed_records"] or outcome["corrupt_containers"]:
        outcome["objects_partial"] = 1
    else:
        outcome["objects_successful"] = 1
    return outcome


def _sum_evidence_counts(evidence_results: list[dict[str, Any]], extra_failures: int = 0) -> dict[str, int]:
    return {
        "cap_evictions": sum(int(r.get("cap_evictions", 0)) for r in evidence_results),
        "quarantine_capture_failures": extra_failures
        + sum(int(r.get("quarantine_capture_failures", 0)) for r in evidence_results),
    }


def _capture_corrupt_container(
    service_id: str,
    source_type: str,
    object_key: str,
    local_file: str,
    error: BaseException,
    *,
    full_outcome: bool = True,
) -> dict[str, int]:
    from backend.core.metadata.ingest_log import capture_evidence

    try:
        with open(local_file, "rb") as raw:
            payload = raw.read()
        result = capture_evidence(
            service_id,
            source_type,
            object_key,
            payload,
            line_ordinal=None,
            byte_offset=None,
            error_category="corrupt_container",
            error_text=str(error)[:2_000],
        )
    except OSError:
        result = {"cap_evictions": 0, "quarantine_capture_failures": 1}
    base = {
        "corrupt_containers": 1,
        "cap_evictions": int(result.get("cap_evictions", 0)),
        "quarantine_capture_failures": int(result.get("quarantine_capture_failures", 0)),
    }
    return (
        {**_failed_object_outcome(), **base} if full_outcome else {"valid_records": 0, "malformed_records": 0, **base}
    )


def _corrupt_gzip_error(local_file: str) -> BaseException | None:
    try:
        with _open_compressed_log(local_file) as raw:
            while raw.read(64 * 1024):
                pass
    except _COMPRESSED_STREAM_ERRORS as exc:
        return exc
    return None


def get_ingest_type_hints(log_fields_config: dict | None = None) -> dict[str, str]:
    hints = {
        f["id"]: ("TIMESTAMPTZ" if f["id"] == "timestamp" else f["duckdb_type"])
        for f in LOG_FIELD_CATALOG
        if f.get("group") not in _INGEST_EXCLUDED_GROUPS and f.get("vcl") is not None
    }
    if log_fields_config:
        for cf in log_fields_config.get("custom_fields", []):
            if cf.get("enabled", True):
                hints[cf["name"]] = cf.get("duckdb_type", "VARCHAR")
    return hints


def get_catalog_field_ids(log_fields_config: dict | None = None) -> list[str]:
    ids = [
        f["id"] for f in LOG_FIELD_CATALOG if f.get("group") not in _INGEST_EXCLUDED_GROUPS and f.get("vcl") is not None
    ]
    if log_fields_config:
        for cf in sorted(log_fields_config.get("custom_fields", []), key=lambda x: x["name"]):
            if cf.get("enabled", True):
                ids.append(cf["name"])
    return ids


def _scan_invalid_gzip_lines(paths: list[str]) -> list[tuple[str, str]]:
    invalid_rows: list[tuple[str, str]] = []
    for path in paths:
        with _open_compressed_log(path) as compressed:
            for raw_line in compressed:
                if not raw_line.strip():
                    continue
                try:
                    row = orjson.loads(raw_line)
                    is_valid = isinstance(row, dict) and bool(row.get("timestamp"))
                except (orjson.JSONDecodeError, UnicodeDecodeError, ValueError):
                    is_valid = False
                if not is_valid:
                    invalid_rows.append((path, raw_line.decode("utf-8", errors="replace").rstrip("\r\n")))
    return invalid_rows


def _finalize_sync_outcome_counters(
    outcome_counters: dict[str, int],
    failed_paths: set[str],
    capture_failed_paths: set[str],
    delete_failed_paths: set[str],
    partial_paths: set[str],
    **extra_counters: int,
) -> None:
    if extra_counters:
        outcome_counters.update(extra_counters)
    outcome_counters["source_delete_failures"] = len(delete_failed_paths)
    failed_objects = failed_paths | capture_failed_paths | delete_failed_paths
    outcome_counters["objects_failed"] = len(failed_objects)
    outcome_counters["objects_partial"] = len(partial_paths - failed_objects)
    outcome_counters["objects_successful"] = max(
        0,
        outcome_counters["objects_processed"]
        - outcome_counters["objects_failed"]
        - outcome_counters["objects_partial"],
    )

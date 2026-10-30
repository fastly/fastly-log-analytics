import gzip
import logging
import re
from typing import Any

import orjson

from backend.core import log_fields as fc
from backend.core.duckdb import escape_sql_literal
from backend.core.log_fields import LOG_FIELD_CATALOG

logger = logging.getLogger("backend.core.ingest")

_INGEST_OUTCOME_COUNTER_KEYS = ( "valid_records", "malformed_records", "corrupt_containers", "quarantine_capture_failures",
    "source_delete_failures", "cap_evictions", "objects_processed", "objects_successful", "objects_partial", "objects_failed",
)  # fmt: skip
_INGEST_EXCLUDED_GROUPS = ("METRICS", "VIRTUAL", "INTERNAL")
_EMPTY_VALUE_RE = re.compile(r":(?=[,}])")

_DECODE_EXPRS: dict[str, str] = {
    "c_speed": fc.duckdb_decode_case("c_speed", fc.CONN_SPEED_ENCODE),
    "p_type": fc.duckdb_decode_case("p_type", fc.PROXY_TYPE_ENCODE),
    "p_desc": fc.duckdb_decode_case("p_desc", fc.PROXY_DESC_ENCODE),
}


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


def _read_json_auto_sql(paths_expr: str, columns_sql: str, *, filename: bool = False) -> str:
    return f"read_json_auto({paths_expr}, format='newline_delimited', records='auto'{', filename=true' if filename else ''}, columns={columns_sql}, ignore_errors=true)"


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
        result = capture_evidence( service_id, source_type, object_key, payload,
            line_ordinal=None, byte_offset=None, error_category="corrupt_container", error_text=str(error)[:2_000],
        )  # fmt: skip
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
        with gzip.open(local_file, "rb") as raw:
            while raw.read(64 * 1024):
                pass
    except (gzip.BadGzipFile, EOFError, OSError) as exc:
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


def get_ingest_columns_sql(log_fields_config: dict | None = None) -> str:
    hints = get_ingest_type_hints(log_fields_config)
    return "{" + ", ".join(f"'{escape_sql_literal(k)}': '{escape_sql_literal(v)}'" for k, v in hints.items()) + "}"


def _build_column_select_exprs(
    src: dict | None, log_fields_config: dict | None = None, *, source_file_expr: str | None = None
) -> list[str]:
    svc_id = (src or {}).get("logging_service_id") or (src or {}).get("name", "")
    backend_expr = (
        f"regexp_replace(\"backend\", '^{escape_sql_literal(svc_id)}--', '') AS \"backend\"" if svc_id else '"backend"'
    )
    field_selects: list[str] = []
    for fid in get_catalog_field_ids(log_fields_config):
        if fid not in ("backend", "_source_file"):
            safe_fid = fid.replace('"', '""')
            field_selects.append(f'{_DECODE_EXPRS[fid]} AS "{safe_fid}"' if fid in _DECODE_EXPRS else f'"{safe_fid}"')
    if source_file_expr is not None:
        field_selects.append(f"{source_file_expr} AS _source_file")
    field_selects.append(backend_expr)
    return field_selects


def _scan_invalid_gzip_lines(paths: list[str]) -> list[tuple[str, str]]:
    invalid_rows: list[tuple[str, str]] = []
    for path in paths:
        with gzip.open(path, "rb") as compressed:
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


def _quarantine_convert_corrupt_lines(
    duckdb_con,
    fos_client,
    src: dict,
    read_expr: str,
    local_file: str,
    object_key: str,
) -> dict[str, int]:
    from backend.core.metadata.ingest_log import capture_evidence

    valid_count, corrupt_count = duckdb_con.execute(
        f"SELECT count(*) FILTER (timestamp IS NOT NULL), count(*) FILTER (timestamp IS NULL) FROM {read_expr}"
    ).fetchone()
    if not corrupt_count:
        return {
            "valid_records": int(valid_count or 0),
            "malformed_records": 0,
            "cap_evictions": 0,
            "quarantine_capture_failures": 0,
        }
    service_id = src.get("service_id") or src.get("name", "")
    prefix_path = src.get("prefix", "").strip("/")
    raw_prefix = f"{prefix_path}/raw/request/" if prefix_path else "raw/request/"
    if not object_key.startswith(raw_prefix):
        logger.warning(
            "[ledger] %s: refusing request quarantine for key outside raw prefix: %s", service_id, object_key
        )
        return {
            "valid_records": int(valid_count or 0),
            "malformed_records": int(corrupt_count or 0),
            "cap_evictions": 0,
            "quarantine_capture_failures": int(corrupt_count or 0),
        }
    open_file = gzip.open if local_file.endswith(".gz") else open
    evidence_results = []
    offset = 0
    try:
        with open_file(local_file, "rb") as raw:
            for ordinal, line in enumerate(raw, start=1):
                current_offset = offset
                offset += len(line)
                stripped = line.strip()
                if not stripped:
                    continue
                category = None
                error_text = ""
                try:
                    parsed = orjson.loads(stripped)
                    if not isinstance(parsed, dict) or not parsed.get("timestamp"):
                        category, error_text = "missing_timestamp", "record has no timestamp"
                except (orjson.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
                    category, error_text = "invalid_json", str(exc)[:2000]
                if category is not None:
                    evidence_results.append(
                        capture_evidence(
                            service_id,
                            "request",
                            object_key,
                            line,
                            line_ordinal=ordinal,
                            byte_offset=current_offset,
                            error_category=category,
                            error_text=error_text,
                        )
                    )
    except (gzip.BadGzipFile, EOFError, OSError) as exc:
        logger.warning("[ledger] %s: malformed request gzip %s: %s", service_id, object_key, exc)
        return _capture_corrupt_container(service_id, "request", object_key, local_file, exc, full_outcome=False)
    if len(evidence_results) != int(corrupt_count or 0):
        logger.error(
            "[ledger] %s: DuckDB/raw-line corruption count mismatch for %s (%d vs %d)",
            service_id,
            object_key,
            corrupt_count,
            len(evidence_results),
        )
        return {
            "valid_records": int(valid_count or 0),
            "malformed_records": len(evidence_results),
            **_sum_evidence_counts(evidence_results, extra_failures=1),
        }
    for result in evidence_results:
        if int(result.get("quarantine_capture_failures", 0)):
            logger.error("[ledger] %s: request quarantine capture failed for %s", service_id, object_key)
    return {
        "valid_records": int(valid_count or 0),
        "malformed_records": len(evidence_results),
        **_sum_evidence_counts(evidence_results),
    }


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

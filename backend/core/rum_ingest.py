"""Pure-Python RUM beacon / Faro / query-param parsing and quarantine helpers."""

from __future__ import annotations

import logging
import sys as _sys
from typing import Any
from urllib.parse import parse_qs, urlparse

import orjson

from backend.core.faro_versions import DEFAULT_FARO_VERSION  # noqa: F401
from backend.core.fos_client import (
    finalize_cron_run_if_running,  # noqa: F401
    get_source_for_service,  # noqa: F401
    log_cron_run,  # noqa: F401
    start_cron_run,  # noqa: F401
)
from backend.core.ingest.parser import _sum_evidence_counts
from backend.utils.date_utils import parse_iso_utc
from backend.utils.telemetry import log_once_per

logger = logging.getLogger(__name__)


def _resolve_ingest_hook(attr_name: str, current: Any, orig: Any) -> Any:
    if current is not orig:
        return current
    ingest_mod = _sys.modules.get("backend.core.ingest")
    if ingest_mod is not None:
        other_val = ingest_mod.__dict__.get(attr_name, orig)
        if other_val is not orig:
            return other_val
    return current


def safe_int(val: Any, default: int = 0) -> int:
    if val is None or val == "":
        return default
    try:
        return int(val)
    except Exception:
        return default


def safe_float(val: Any, default: float | None = None) -> float | None:
    if val is None or val == "":
        return default
    try:
        return float(val)
    except Exception:
        return default


def extract_metrics_from_faro_payload(payload: dict, log_data: dict) -> list[dict]:
    """Extract all distinct metrics, timings, and exceptions from a raw Faro payload."""
    extracted: list[dict] = []
    meta = payload.get("meta") or {}
    browser_meta = meta.get("browser") or {}
    os_meta = meta.get("os") or {}
    page_meta = payload.get("page") or meta.get("page") or {}
    browser_name = browser_meta.get("name") or log_data.get("browser") or "Chrome"
    os_name = os_meta.get("name") or log_data.get("os") or "macOS"
    device_type = "Mobile" if browser_meta.get("mobile") else "Desktop"
    url_str = page_meta.get("url") or log_data.get("url") or ""
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

    measurements = payload.get("measurements") or []
    if isinstance(measurements, list):
        for m in measurements:
            if isinstance(m, dict) and m.get("type") == "web-vitals":
                rating = m.get("context", {}).get("rating") or (m.get("meta") or {}).get("rating") or ""
                for k, v in (m.get("values") or {}).items():
                    if k != "delta":
                        extracted.append(_entry(k, v, rating))

    events = payload.get("events") or []
    if isinstance(events, list):
        for e in events:
            if not isinstance(e, dict) or not (name := e.get("name")):
                continue
            if name == "faro.performance.navigation":
                for k, v in (e.get("attributes") or e.get("values") or {}).items():
                    extracted.append(_entry(k, v))
            else:
                extracted.append(_entry(f"event_{name}", 1.0))

    logs = payload.get("logs") or []
    if isinstance(logs, list):
        for log_item in logs:
            if isinstance(log_item, dict):
                extracted.append(_entry("log", 1.0, log_item.get("level") or "info"))

    exceptions = payload.get("exceptions") or []
    if isinstance(exceptions, list):
        for exc in exceptions:
            if not isinstance(exc, dict):
                continue
            err_msg = exc.get("value") or exc.get("message") or "Unknown error"
            frames = (exc.get("stacktrace") or {}).get("frames") or []
            frame = frames[0] if isinstance(frames, list) and frames else {}
            extracted.append(
                _entry(
                    "exception",
                    None,
                    error_message=err_msg,
                    error_file=frame.get("filename") or "unknown.js",
                    error_line=frame.get("lineno") or 0,
                    error_col=frame.get("colno") or 0,
                )
            )

    if not extracted:
        extracted.append(_entry("pageview", 1.0))
    return extracted


def extract_metrics_from_query_params(qparams: dict[str, list[str]], log_data: dict) -> list[dict]:
    """Extract batched metrics and exceptions from repeated URL query parameters.

    Returns an empty list when the query string carries at most one item so single-item
    beacons continue through the flat fallback path (preserving top-level field precedence).
    """
    raw_names = qparams.get("rum_metric_name") or []
    raw_values = qparams.get("rum_metric_value") or []
    raw_ratings = qparams.get("rum_metric_rating") or []
    raw_err_msgs = qparams.get("rum_error_message") or []
    raw_err_files = qparams.get("rum_error_file") or []
    raw_err_lines = qparams.get("rum_error_line") or []
    raw_err_cols = qparams.get("rum_error_col") or []

    non_empty_metrics = [m for m in raw_names if m]
    non_empty_errors = [e for e in raw_err_msgs if e]
    if len(non_empty_metrics) + len(non_empty_errors) <= 1:
        return []

    browser_name = log_data.get("browser") or "Chrome"
    os_name = log_data.get("os") or "macOS"
    device_type = log_data.get("device") or "Desktop"
    cid = (
        next((c for c in (qparams.get("cid") or []) if c), "")
        or next((c for c in (qparams.get("rum_cid") or []) if c), "")
        or log_data.get("rum_cid")
        or log_data.get("cid")
        or ""
    )
    pathname = next((p for p in (qparams.get("rum_pathname") or []) if p), None) or log_data.get("rum_pathname") or "/"

    extracted: list[dict] = []
    for idx, name in enumerate(raw_names):
        if not name:
            continue
        val = raw_values[idx] if idx < len(raw_values) else None
        rating = raw_ratings[idx] if idx < len(raw_ratings) else ""
        extracted.append(
            {
                "metric_name": name,
                "metric_value": val,
                "metric_rating": rating,
                "pathname": pathname,
                "cid": cid,
                "browser": browser_name,
                "os": os_name,
                "device": device_type,
            }
        )

    for idx, err_msg in enumerate(raw_err_msgs):
        if not err_msg:
            continue
        err_file = (raw_err_files[idx] if idx < len(raw_err_files) else "") or "unknown.js"
        err_line = safe_int(raw_err_lines[idx]) if idx < len(raw_err_lines) else 0
        err_col = safe_int(raw_err_cols[idx]) if idx < len(raw_err_cols) else 0
        extracted.append(
            {
                "metric_name": "exception",
                "metric_value": None,
                "metric_rating": "",
                "pathname": pathname,
                "cid": cid,
                "browser": browser_name,
                "os": os_name,
                "device": device_type,
                "error_message": err_msg,
                "error_file": err_file,
                "error_line": err_line,
                "error_col": err_col,
            }
        )

    return extracted


def _parse_rum_line(log_data: dict, service_id: str) -> tuple[list[dict], list[dict]] | None:
    log_service_id = log_data.get("service_id") or log_data.get("rum_service_id")
    if log_service_id and log_service_id != service_id:
        return None
    if (
        log_data.get("method") == "OPTIONS"
        or log_data.get("is_healthcheck")
        or log_data.get("path") in ("/health", "/ping", "/favicon.ico")
    ):
        return None
    received_at = log_data.get("timestamp")
    if not received_at:
        raise ValueError("missing_timestamp")
    dt = parse_iso_utc(received_at) if isinstance(received_at, str) else None
    if dt is None:
        raise ValueError(f"invalid_timestamp: {received_at!r}")
    from backend.utils.pop_utils import clean_edge_meta_str, resolve_pop_geo_fallback

    city_val = clean_edge_meta_str(log_data.get("city") or log_data.get("geo_city"))
    region_val = clean_edge_meta_str(log_data.get("region") or log_data.get("geo_region"))
    country_val = clean_edge_meta_str(log_data.get("country") or log_data.get("geo_country_code"))
    pop_val = clean_edge_meta_str(log_data.get("pop") or log_data.get("server_pop"))
    city_val, region_val, country_val = resolve_pop_geo_fallback(pop_val, city_val, region_val, country_val)
    tls_val = clean_edge_meta_str(log_data.get("tls") or log_data.get("tls_version"))
    ttfb_val = safe_float(log_data.get("ttfb") or log_data.get("time_to_first_byte") or log_data.get("rum_ttfb_ms"))
    raw_url = log_data.get("url") or log_data.get("rum_raw_query") or ""
    qparams: dict[str, list[str]] = {}
    if raw_url:
        try:
            qparams = parse_qs(urlparse(raw_url).query, keep_blank_values=True)
        except Exception:
            if log_once_per("_parse_rum_line: step failed (ignored)", 60):
                logger.debug("_parse_rum_line: step failed (ignored)", exc_info=True)
    req_id_val = (
        clean_edge_meta_str(log_data.get("fastly_req_id") or log_data.get("req_id"))
        or (qparams.get("req", [""])[0] if qparams else "")
        or ""
    )
    url_cid_fallback = (qparams.get("cid", [""])[0] or qparams.get("rum_cid", [""])[0]) if qparams else ""
    url_pathname_fallback = qparams.get("rum_pathname", [None])[0] if qparams else None

    def _common(pathname, browser, os_name, device, cid, req_id) -> dict:
        return {
            "pathname": pathname,
            "browser": browser,
            "os": os_name,
            "device": device,
            "cid": cid,
            "req_id": req_id,
            "city": city_val,
            "region": region_val,
            "country": country_val,
            "pop": pop_val,
            "tls": tls_val,
            "ttfb": ttfb_val,
        }

    vitals_rows: list[dict] = []
    errors_rows: list[dict] = []

    def _append_rum_row(
        *,
        is_error: bool,
        err_msg: Any,
        err_file: Any,
        err_line: Any,
        err_col: Any,
        metric_name: Any,
        metric_value: Any,
        metric_rating: Any,
        common: dict,
    ) -> None:
        if is_error:
            errors_rows.append(
                {
                    "timestamp": dt,
                    "error_message": err_msg or "Unknown error",
                    "error_file": err_file or "unknown.js",
                    "error_line": safe_int(err_line),
                    "error_col": safe_int(err_col),
                    **common,
                }
            )
        else:
            val = safe_float(metric_value)
            row_common = dict(common)
            if row_common.get("ttfb") is None and str(metric_name or "").upper() == "TTFB" and val is not None:
                row_common["ttfb"] = val
            vitals_rows.append(
                {
                    "timestamp": dt,
                    "metric_name": metric_name or "unknown",
                    "metric_value": val if val is not None else 0.0,
                    "metric_rating": metric_rating or "",
                    **row_common,
                }
            )

    raw_body = log_data.get("rum_body")
    extracted_metrics: list[dict] = []
    if raw_body:
        try:
            payload = orjson.loads(raw_body)
            if isinstance(payload, str):
                payload = orjson.loads(payload)
            if isinstance(payload, dict):
                extracted_metrics = extract_metrics_from_faro_payload(payload, log_data)
        except Exception:
            extracted_metrics = []
    if not extracted_metrics and qparams:
        extracted_metrics = extract_metrics_from_query_params(qparams, log_data)
    if extracted_metrics:
        if ttfb_val is None:
            for m in extracted_metrics:
                if str(m.get("metric_name") or "").upper() == "TTFB":
                    ttfb_val = safe_float(m.get("metric_value"))
                    if ttfb_val is not None:
                        break
        for metric in extracted_metrics:
            m_path = metric.get("pathname")
            if (not m_path or m_path in ("/", "/rum-beacon", "/_rum/v1/beacon")) and (
                log_data.get("rum_pathname") or url_pathname_fallback
            ):
                m_path = log_data.get("rum_pathname") or url_pathname_fallback
            common = _common(
                (m_path or "/").replace("//", "/"),
                metric.get("browser") or log_data.get("browser") or "Chrome",
                metric.get("os") or log_data.get("os") or "macOS",
                metric.get("device") or log_data.get("device") or "Desktop",
                metric.get("cid") or log_data.get("rum_cid") or log_data.get("cid") or url_cid_fallback or "",
                req_id_val,
            )
            _append_rum_row(
                is_error=bool((metric.get("metric_name") == "exception") or metric.get("error_message")),
                err_msg=metric.get("error_message"),
                err_file=metric.get("error_file"),
                err_line=metric.get("error_line"),
                err_col=metric.get("error_col"),
                metric_name=metric.get("metric_name"),
                metric_value=metric.get("metric_value"),
                metric_rating=metric.get("metric_rating"),
                common=common,
            )
    else:
        metric_name = log_data.get("rum_metric_name")
        metric_value = log_data.get("rum_metric_value")
        metric_rating = log_data.get("rum_metric_rating")
        err_msg = log_data.get("rum_error_message")
        err_file = log_data.get("rum_error_file")
        err_line = log_data.get("rum_error_line")
        err_col = log_data.get("rum_error_col")
        cid_val = log_data.get("rum_cid") or log_data.get("cid") or ""
        pathname_val = log_data.get("rum_pathname")
        if qparams:
            metric_name = metric_name or qparams.get("rum_metric_name", [metric_name])[0]
            if metric_value is None or metric_value == "":
                metric_value = qparams.get("rum_metric_value", [metric_value])[0]
            metric_rating = metric_rating or qparams.get("rum_metric_rating", [metric_rating])[0]
            err_msg = err_msg or qparams.get("rum_error_message", [err_msg])[0]
            err_file = err_file or qparams.get("rum_error_file", [err_file])[0]
            if err_line in (None, ""):
                err_line = qparams.get("rum_error_line", [err_line])[0]
            if err_col in (None, ""):
                err_col = qparams.get("rum_error_col", [err_col])[0]
            cid_val = cid_val or url_cid_fallback
            pathname_val = pathname_val or url_pathname_fallback
        if metric_value is not None and isinstance(metric_value, str):
            try:
                metric_value = float(metric_value) if "." in metric_value else int(metric_value)
            except ValueError:
                pass
        if not pathname_val and log_data.get("referer"):
            try:
                pathname_val = urlparse(log_data["referer"]).path
            except Exception:
                if log_once_per("_parse_rum_line: urlparse failed (ignored)", 60):
                    logger.debug("_parse_rum_line: urlparse failed (ignored)", exc_info=True)
        pathname_val = (pathname_val or "/").replace("//", "/")
        common = _common(
            pathname_val,
            log_data.get("browser") or "Chrome",
            log_data.get("os") or "macOS",
            log_data.get("device") or "Desktop",
            cid_val,
            req_id_val,
        )
        _append_rum_row(
            is_error=bool(err_msg),
            err_msg=err_msg,
            err_file=err_file,
            err_line=err_line,
            err_col=err_col,
            metric_name=metric_name,
            metric_value=metric_value,
            metric_rating=metric_rating,
            common=common,
        )
    return vitals_rows, errors_rows


_PRECOLLECTED_CORRUPT_RECORDS: dict[int, tuple[list[tuple[str, str]], str, list[tuple[int, int, bytes, str]]]] = {}


def _parse_rum_beacon_file(local_path: str, service_id: str) -> tuple[list[dict], list[dict], list[tuple[str, str]]]:
    from backend.core.ingest.parser import _open_compressed_log

    vitals_rows: list[dict] = []
    errors_rows: list[dict] = []
    corrupt_lines: list[tuple[str, str]] = []
    corrupt_records: list[tuple[int, int, bytes, str]] = []
    byte_offset = 0
    with _open_compressed_log(local_path) as f:
        for ordinal, raw_line in enumerate(f, start=1):
            current_offset = byte_offset
            byte_offset += len(raw_line)
            stripped_bytes = raw_line.strip()
            if not stripped_bytes:
                continue
            try:
                log_data = orjson.loads(stripped_bytes)
            except (orjson.JSONDecodeError, ValueError):
                stripped = stripped_bytes.decode("utf-8", errors="replace")
                corrupt_lines.append((stripped, "invalid_json"))
                corrupt_records.append((ordinal, current_offset, raw_line, "invalid_json"))
                continue
            try:
                parsed = _parse_rum_line(log_data, service_id)
            except Exception as e:
                stripped = stripped_bytes.decode("utf-8", errors="replace")
                msg = str(e)
                if msg == "missing_timestamp":
                    reason = "missing_timestamp"
                elif msg.startswith("invalid_timestamp"):
                    reason = f"invalid_timestamp: {msg.removeprefix('invalid_timestamp: ')}"[:200]
                else:
                    reason = f"parse_error: {e}"[:200]
                corrupt_lines.append((stripped, reason))
                corrupt_records.append((ordinal, current_offset, raw_line, reason))
                continue
            if parsed is None:
                continue
            v_rows, e_rows = parsed
            if not v_rows and not e_rows:
                stripped = stripped_bytes.decode("utf-8", errors="replace")
                corrupt_lines.append((stripped, "empty_beacon_payload"))
                corrupt_records.append((ordinal, current_offset, raw_line, "empty_beacon_payload"))
                continue
            vitals_rows.extend(v_rows)
            errors_rows.extend(e_rows)
    if corrupt_records:
        if len(_PRECOLLECTED_CORRUPT_RECORDS) >= 64:
            _PRECOLLECTED_CORRUPT_RECORDS.pop(next(iter(_PRECOLLECTED_CORRUPT_RECORDS)), None)
        _PRECOLLECTED_CORRUPT_RECORDS[id(corrupt_lines)] = (corrupt_lines, local_path, corrupt_records)
    return vitals_rows, errors_rows, corrupt_lines


_orig_parse_rum_beacon_file = _parse_rum_beacon_file


def _quarantine_rum_corrupt_lines(
    fos_client,
    src: dict,
    object_key: str,
    corrupt_lines: list[tuple[str, str]],
    valid_count: int,
    *,
    local_file: str,
    precollected_corrupt: list[tuple[int, int, bytes, str]] | None = None,
) -> dict[str, int]:
    from backend.core.metadata.ingest_log import capture_evidence

    pre_entry = _PRECOLLECTED_CORRUPT_RECORDS.pop(id(corrupt_lines), None)
    if (
        precollected_corrupt is None
        and pre_entry is not None
        and pre_entry[0] is corrupt_lines
        and pre_entry[1] == local_file
    ):
        precollected_corrupt = pre_entry[2]

    service_id = src.get("service_id") or src.get("name", "")
    prefix_path = src.get("prefix", "").strip("/")
    raw_prefix = f"{prefix_path}/raw/rum/" if prefix_path else "raw/rum/"
    if not object_key.startswith(raw_prefix):
        return {
            "valid_records": valid_count,
            "malformed_records": len(corrupt_lines),
            "cap_evictions": 0,
            "quarantine_capture_failures": len(corrupt_lines),
        }
    evidence_results = []
    if precollected_corrupt is not None:
        for ordinal, current_offset, raw_line, reason in precollected_corrupt:
            evidence_results.append(
                capture_evidence(
                    service_id,
                    "rum",
                    object_key,
                    raw_line,
                    line_ordinal=ordinal,
                    byte_offset=current_offset,
                    error_category=reason.partition(":")[0],
                    error_text=reason,
                )
            )
    else:
        from backend.core.ingest.parser import _open_compressed_log

        occurrences: dict[str, list[str]] = {}
        for line, reason in corrupt_lines:
            occurrences.setdefault(line, []).append(reason)
        offsets: dict[str, int] = {}
        byte_offset = 0
        with _open_compressed_log(local_file) as raw:
            for ordinal, raw_line in enumerate(raw, start=1):
                stripped = raw_line.strip().decode("utf-8", errors="replace")
                current_offset = byte_offset
                byte_offset += len(raw_line)
                if not occurrences.get(stripped):
                    continue
                reason = occurrences[stripped][offsets.get(stripped, 0)]
                offsets[stripped] = offsets.get(stripped, 0) + 1
                evidence_results.append(
                    capture_evidence(
                        service_id,
                        "rum",
                        object_key,
                        raw_line,
                        line_ordinal=ordinal,
                        byte_offset=current_offset,
                        error_category=reason.partition(":")[0],
                        error_text=reason,
                    )
                )
    counts = _sum_evidence_counts(evidence_results, extra_failures=len(corrupt_lines) - len(evidence_results))
    if counts["quarantine_capture_failures"]:
        logger.error(
            "[ledger] %s: RUM evidence capture failed for %s (%d line(s))",
            service_id,
            object_key,
            counts["quarantine_capture_failures"],
        )
    return {"valid_records": valid_count, "malformed_records": len(corrupt_lines), **counts}


_orig_quarantine_rum_corrupt_lines = _quarantine_rum_corrupt_lines

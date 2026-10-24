"""Shared RUM DuckDB test fixtures and beacon insertion helpers."""

from __future__ import annotations

import datetime
import uuid
from pathlib import Path
from typing import Any

import duckdb
import pytest

import backend.core.duckdb_pool as duckdb_pool


def _is_faro_payload(beacon: dict[str, Any]) -> bool:
    """Return True if *beacon* uses the nested Grafana Faro wire schema."""
    if "measurements" in beacon or "events" in beacon:
        return True
    meta = beacon.get("meta")
    if isinstance(meta, dict) and (isinstance(meta.get("browser"), dict) or isinstance(meta.get("page"), dict)):
        return True
    return False


@pytest.fixture
def setup_temp_rum_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Set up a temporary DuckDB database for RUM and mock the source resolver."""
    base_db_path = tmp_path / "test.duckdb"
    rum_db_path = tmp_path / "test.rum.duckdb"

    con = duckdb.connect(str(rum_db_path))
    try:
        con.execute(
            """
            CREATE TABLE client_vitals (
                timestamp TIMESTAMPTZ,
                metric_name VARCHAR,
                metric_value DOUBLE,
                metric_rating VARCHAR,
                pathname VARCHAR,
                browser VARCHAR,
                os VARCHAR,
                device VARCHAR,
                cid VARCHAR,
                req_id VARCHAR,
                city VARCHAR DEFAULT NULL,
                region VARCHAR DEFAULT NULL,
                country VARCHAR DEFAULT NULL,
                pop VARCHAR DEFAULT NULL,
                tls VARCHAR DEFAULT NULL,
                ttfb DOUBLE DEFAULT NULL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE client_errors (
                timestamp TIMESTAMPTZ,
                error_message VARCHAR,
                error_file VARCHAR,
                error_line INTEGER,
                error_col INTEGER,
                pathname VARCHAR,
                browser VARCHAR,
                os VARCHAR,
                device VARCHAR,
                cid VARCHAR,
                req_id VARCHAR,
                city VARCHAR DEFAULT NULL,
                region VARCHAR DEFAULT NULL,
                country VARCHAR DEFAULT NULL,
                pop VARCHAR DEFAULT NULL,
                tls VARCHAR DEFAULT NULL,
                ttfb DOUBLE DEFAULT NULL
            )
            """
        )
    finally:
        con.close()

    unique_id = f"test_service_{uuid.uuid4().hex[:8]}"
    mock_source = {
        "name": unique_id,
        "service_id": unique_id,
        "duckdb_path": str(base_db_path),
        "access_level": "read_write",
        "endpoint": "localhost",
        "access_key_id": "mock",
        "secret_access_key": "mock",
        "region": "mock",
    }

    duckdb_pool.reset_pool_for_service("test_service")
    duckdb_pool.reset_pool_for_service("test_service_rum")
    monkeypatch.setattr("backend.core.request_context._resolve_source", lambda service_id, read_only=False: mock_source)
    monkeypatch.setattr("backend.deps._resolve_source_or_400", lambda service_id, read_only=False: mock_source)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda service_id: mock_source)

    return rum_db_path


def insert_rum_beacons(temp_rum_db_path: Path | str, beacons_list: list[dict[str, Any]]) -> None:
    """Insert synthetic flat beacons or Faro-shaped payloads into ``temp_rum_db_path``."""
    from backend.core.rum_ingest import extract_metrics_from_faro_payload

    duckdb_pool.shutdown_all()

    vitals_rows: list[tuple[Any, ...]] = []
    errors_rows: list[tuple[Any, ...]] = []

    for i, b in enumerate(beacons_list):
        if _is_faro_payload(b):
            received_at_dt = datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=i + 5)
            req_id = f"req_{i}_{id(b)}"
            extracted = extract_metrics_from_faro_payload(b, {})
            for item in extracted:
                metric_name = item.get("metric_name")
                if metric_name == "exception":
                    errors_rows.append(
                        (
                            received_at_dt,
                            item.get("error_message"),
                            item.get("error_file"),
                            item.get("error_line"),
                            item.get("error_col"),
                            item.get("pathname"),
                            item.get("browser"),
                            item.get("os"),
                            item.get("device"),
                            item.get("cid"),
                            req_id,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                        )
                    )
                else:
                    val = item.get("metric_value")
                    if metric_name == "pageLoadTime" and val is not None:
                        try:
                            val = float(val)
                        except Exception:
                            pass
                    rating = item.get("metric_rating")
                    if not rating and val is not None:
                        try:
                            val_float = float(val)
                            if metric_name == "lcp":
                                rating = (
                                    "good"
                                    if val_float <= 2.5
                                    else ("needs_improvement" if val_float <= 4.0 else "poor")
                                )
                            elif metric_name == "cls":
                                rating = (
                                    "good"
                                    if val_float <= 0.1
                                    else ("needs_improvement" if val_float <= 0.25 else "poor")
                                )
                            elif metric_name == "inp":
                                rating = (
                                    "good"
                                    if val_float <= 200
                                    else ("needs_improvement" if val_float <= 500 else "poor")
                                )
                            else:
                                rating = "good"
                        except Exception:
                            rating = "good"
                    vitals_rows.append(
                        (
                            received_at_dt,
                            metric_name,
                            val,
                            rating,
                            item.get("pathname"),
                            item.get("browser"),
                            item.get("os"),
                            item.get("device"),
                            item.get("cid"),
                            req_id,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                        )
                    )
            continue

        received_at = b.get("received_at")
        if not received_at:
            received_at = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=i + 5)).isoformat()
        dt = datetime.datetime.fromisoformat(received_at.replace("Z", "+00:00"))

        meta = b.get("meta") or {}
        browser_val = meta.get("browser") or "Chrome"
        os_val = meta.get("os") or "macOS"
        device_val = meta.get("device") or "Desktop"
        pathname_val = b.get("pathname") or "/"
        cid_val = b.get("cid") or "test_cid_1"
        req_id_val = b.get("req_id") if "req_id" in b else f"req_{id(b)}"

        city_val = b.get("city") or "Austin"
        region_val = b.get("region") or "TX"
        country_val = b.get("country") or "US"
        pop_val = b.get("pop") or "SIN"
        tls_val = b.get("tls") or "1.3"
        ttfb_val = float(b["ttfb"]) if "ttfb" in b else 45.0

        if "load_time" in b:
            vitals_rows.append(
                (
                    dt,
                    "load_time",
                    float(b["load_time"]),
                    "good" if b["load_time"] <= 2.0 else "poor",
                    pathname_val,
                    browser_val,
                    os_val,
                    device_val,
                    cid_val,
                    req_id_val,
                    city_val,
                    region_val,
                    country_val,
                    pop_val,
                    tls_val,
                    ttfb_val,
                )
            )

        if "lcp" in b:
            val = float(b["lcp"])
            rating = "good" if val <= 2.5 else ("needs_improvement" if val <= 4.0 else "poor")
            vitals_rows.append(
                (
                    dt,
                    "lcp",
                    val,
                    rating,
                    pathname_val,
                    browser_val,
                    os_val,
                    device_val,
                    cid_val,
                    req_id_val,
                    city_val,
                    region_val,
                    country_val,
                    pop_val,
                    tls_val,
                    ttfb_val,
                )
            )

        if "cls" in b:
            val = float(b["cls"])
            rating = "good" if val <= 0.1 else ("needs_improvement" if val <= 0.25 else "poor")
            vitals_rows.append(
                (
                    dt,
                    "cls",
                    val,
                    rating,
                    pathname_val,
                    browser_val,
                    os_val,
                    device_val,
                    cid_val,
                    req_id_val,
                    city_val,
                    region_val,
                    country_val,
                    pop_val,
                    tls_val,
                    ttfb_val,
                )
            )

        if "inp" in b:
            val = float(b["inp"])
            rating = "good" if val <= 200 else ("needs_improvement" if val <= 500 else "poor")
            vitals_rows.append(
                (
                    dt,
                    "inp",
                    val,
                    rating,
                    pathname_val,
                    browser_val,
                    os_val,
                    device_val,
                    cid_val,
                    req_id_val,
                    city_val,
                    region_val,
                    country_val,
                    pop_val,
                    tls_val,
                    ttfb_val,
                )
            )

        exceptions = b.get("exceptions") or []
        for exc in exceptions:
            errors_rows.append(
                (
                    dt,
                    exc.get("value") or exc.get("message") or "TypeError",
                    exc.get("filename") or "main.js",
                    0,
                    0,
                    pathname_val,
                    browser_val,
                    os_val,
                    device_val,
                    cid_val,
                    req_id_val,
                    city_val,
                    region_val,
                    country_val,
                    pop_val,
                    tls_val,
                    ttfb_val,
                )
            )

    con = duckdb.connect(str(temp_rum_db_path), read_only=False)
    try:
        if vitals_rows:
            con.executemany(
                "INSERT INTO client_vitals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                vitals_rows,
            )
        if errors_rows:
            con.executemany(
                "INSERT INTO client_errors VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                errors_rows,
            )
    finally:
        con.close()

"""Cross-service scoping for custom log fields.

Audit finding: custom log fields are per-service tenant data. A field
declared on service A must not leak into service B's schema, log-fields
API response, or query results — and a field that happens to share a
name across both services must keep its values independent.

The single-service e2e (``test_integration_custom_fields.py``) and the
auth-side scope gates (``test_cross_tenant_scope.py``) cover their
respective halves. This file pins the third invariant: two configured
services with overlapping (or distinct) custom fields stay isolated at
the storage + view + API layers.

OBSERVED-vs-spec: the spec referenced ``GET /api/services/{id}/log-fields``
as the surface that reveals ``svc_a_field``. That route's
``LogFieldsResponse`` model only exposes ``groups``/``field_overrides``/
``field_limits`` and strips ``custom_fields`` from the wire — the route
that actually surfaces custom-field membership cross-service is
``GET /custom-fields``. We pin both endpoints below.
"""

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend import config as svcconfig
from backend.core import duckdb as _bk_duckdb
from backend.core.iceberg import _core as _iceberg_core
from backend.routers.services import core as services_core_router

pytestmark = pytest.mark.security_regression


@pytest.fixture
def app():
    """Mini FastAPI app — services-core router only, no auth middleware
    (mirrors ``tests/routers/test_cross_tenant_scope.py``). Admin scope is
    fine here; the per-service auth gate has separate coverage."""
    a = FastAPI()
    a.include_router(services_core_router.router)
    return a


def _save_service_config(service_id: str, custom_fields: list[dict] | None = None) -> dict:
    """Persist a minimal service config under the sandbox CONFIGS_DIR
    (already redirected by the autouse ``isolate_metadata_db`` fixture)."""
    cfg = {
        "service_id": service_id,
        "name": service_id,
        "log_fields": {
            "schema_version": 2,
            "groups": ["A"],
            "field_overrides": {},
            "custom_fields": list(custom_fields or []),
        },
    }
    svcconfig.save_config(service_id, cfg)
    return cfg


def _cf(name: str, duckdb_type: str = "VARCHAR", value_type: str = "string") -> dict:
    """A CustomField-shaped dict (matches the persisted on-disk form)."""
    return {
        "name": name,
        "label": name,
        "description": "",
        "vcl_log_expression": "req.http.X-Test",
        "collection_stage": "edge",
        "duckdb_type": duckdb_type,
        "value_type": value_type,
        "bytes_estimate": 20,
        "nullable": True,
        "enabled": True,
        "show_in_dashboard": False,
        "show_in_logs": True,
        "filterable": True,
    }


# ── Test 1 ──────────────────────────────────────────────────────────────


def test_custom_field_in_service_a_not_visible_in_service_b(app):
    """Field declared on A must not leak to B's schema, custom-fields
    listing, or DuckDB table identifier."""
    svc_a, svc_b = "svc-aaa", "svc-bbb"
    _save_service_config(svc_a)
    _save_service_config(svc_b)

    create_body = _cf("svc_a_field")

    with TestClient(app) as c:
        # Create on A only.
        r = c.post(f"/api/services/{svc_a}/custom-fields", json=create_body)
        assert r.status_code == 200, r.text
        assert r.json()["field"]["name"] == "svc_a_field"

        # /custom-fields is the route that actually surfaces membership.
        r_a = c.get(f"/api/services/{svc_a}/custom-fields")
        r_b = c.get(f"/api/services/{svc_b}/custom-fields")
        assert r_a.status_code == 200 and r_b.status_code == 200
        names_a = {f["name"] for f in r_a.json()["fields"]}
        names_b = {f["name"] for f in r_b.json()["fields"]}
        assert "svc_a_field" in names_a
        assert "svc_a_field" not in names_b, f"leak into B: {names_b}"

        # /log-fields strips custom_fields from the wire — pin both.
        r_a_lf = c.get(f"/api/services/{svc_a}/log-fields")
        r_b_lf = c.get(f"/api/services/{svc_b}/log-fields")
        assert r_a_lf.status_code == 200 and r_b_lf.status_code == 200
        assert "svc_a_field" not in json.dumps(r_a_lf.json()["log_fields"])
        assert "svc_a_field" not in json.dumps(r_b_lf.json()["log_fields"])

    # Schema layer — derive Arrow schema per service from persisted config.
    cfg_a = svcconfig.load_config(svc_a)
    cfg_b = svcconfig.load_config(svc_b)
    assert cfg_a is not None and cfg_b is not None
    names_schema_a = {f.name for f in _iceberg_core.get_arrow_schema(cfg_a["log_fields"])}
    names_schema_b = {f.name for f in _iceberg_core.get_arrow_schema(cfg_b["log_fields"])}
    assert "svc_a_field" in names_schema_a
    assert "svc_a_field" not in names_schema_b, f"leak into B schema: {names_schema_b}"

    # Identifier layer — distinct DuckDB table names per service.
    assert _bk_duckdb._safe_table_name(svc_a) != _bk_duckdb._safe_table_name(svc_b)


# ── Test 2 ──────────────────────────────────────────────────────────────


def test_same_field_name_in_both_services_has_independent_values(tmp_path):
    """A field of the same name in both services keeps its values
    independent. Skip the full ingest pipeline (covered by
    test_integration_custom_fields.py) and exercise the storage-layer
    invariant directly: write a parquet "buffer" per service into a
    distinct cache dir, then query each via per-service DuckDB views."""
    svc_a, svc_b = "svc-A-values", "svc-B-values"
    _save_service_config(svc_a, custom_fields=[_cf("my_field")])
    _save_service_config(svc_b, custom_fields=[_cf("my_field")])

    # Sanity: both Arrow schemas have the column.
    cfg_a = svcconfig.load_config(svc_a)
    cfg_b = svcconfig.load_config(svc_b)
    assert "my_field" in {f.name for f in _iceberg_core.get_arrow_schema(cfg_a["log_fields"])}
    assert "my_field" in {f.name for f in _iceberg_core.get_arrow_schema(cfg_b["log_fields"])}

    # Tiny parquet per service. Distinct values so any leak shows up.
    pq_a = tmp_path / "a" / "rows.parquet"
    pq_b = tmp_path / "b" / "rows.parquet"
    pq_a.parent.mkdir()
    pq_b.parent.mkdir()
    pq.write_table(pa.table({"my_field": ["alpha-from-A"]}), str(pq_a))
    pq.write_table(pa.table({"my_field": ["beta-from-B"]}), str(pq_b))

    # In-memory DuckDB: per-service view over per-service parquet
    # (mirrors the production ``logs_<svc>`` view pattern).
    import duckdb

    con = duckdb.connect(":memory:")
    try:
        table_a = _bk_duckdb._safe_table_name(svc_a)
        table_b = _bk_duckdb._safe_table_name(svc_b)
        assert table_a != table_b
        con.execute(f"CREATE OR REPLACE VIEW {table_a} AS SELECT * FROM read_parquet('{pq_a}')")
        con.execute(f"CREATE OR REPLACE VIEW {table_b} AS SELECT * FROM read_parquet('{pq_b}')")
        rows_a = con.execute(f"SELECT my_field FROM {table_a}").fetchall()
        rows_b = con.execute(f"SELECT my_field FROM {table_b}").fetchall()
    finally:
        con.close()

    # Independent values — no cross-leakage.
    assert rows_a == [("alpha-from-A",)], f"service A returned {rows_a}"
    assert rows_b == [("beta-from-B",)], f"service B returned {rows_b}"
    assert "beta-from-B" not in [r[0] for r in rows_a]
    assert "alpha-from-A" not in [r[0] for r in rows_b]

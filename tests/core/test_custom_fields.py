"""Regression: custom-field byte budgets, cross-service isolation, lifecycle, roundtrip, and type mismatches."""

from __future__ import annotations

import copy
import gzip
import io
import json
import os
import re
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from backend.provision.cmcd_fields import get_cmcd_fields
from fastapi import FastAPI
from fastapi.testclient import TestClient
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from backend import config as svcconfig
from backend.core import duckdb as _bk_duckdb
from backend.core import iceberg
from backend.core import ingest as _bk_ingest
from backend.core.iceberg import _core as _iceberg_core
from backend.core.ingest import get_catalog_field_ids, get_ingest_type_hints
from backend.core.log_fields import (
    _BUILTIN_FIELD_NAMES,
    _DUCKDB_RESERVED,
    check_log_line_budget,
    estimate_log_line_bytes,
    generate_log_format,
    get_catalog_for_api,
    validate_custom_field,
)
from backend.models.services import CustomFieldCreate, CustomFieldUpdate
from backend.provision.session_scoring_orchestrator import _SCORING_CUSTOM_FIELDS
from backend.repositories._base import _safe_table
from backend.routers import services
from backend.routers.services import core as services_core_router


@pytest.fixture
def custom_field_env(
    s3_mock: Any, fos_source: dict[str, Any], monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> dict[str, Any]:
    unique_id = f"cf{uuid.uuid4().hex[:10]}"
    fos_source["service_id"] = unique_id
    fos_source["name"] = unique_id
    fos_source["duckdb_path"] = str(tmp_path / "e2e.duckdb")
    cache_path = str(tmp_path / "cache")
    os.makedirs(cache_path, exist_ok=True)

    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: cache_path)
    monkeypatch.setattr("backend.core.duckdb._configure_fos", lambda *a, **kw: None)

    class _CallerHintShim:
        def __init__(self, client: Any) -> None:
            self._client = client

        def get_paginator(self, op: str, caller_hint: str | None = None) -> Any:
            return self._client.get_paginator(op)

        def __getattr__(self, name: str) -> Any:
            return getattr(self._client, name)

    shim = _CallerHintShim(s3_mock)
    monkeypatch.setattr("backend.core.ingest._get_fos_client", lambda _src: shim)

    if hasattr(iceberg, "_view_cache"):
        iceberg._view_cache.clear()

    def _set_custom_fields(custom_fields: list[dict[str, Any]]) -> None:
        cfg = {
            "service_id": fos_source["service_id"],
            "log_fields": {"schema_version": 2, "groups": ["A"], "custom_fields": custom_fields},
        }
        monkeypatch.setattr("backend.config.load_config", lambda _sid: cfg)

    def _seed_gz(key: str, rows: list[dict[str, Any]]) -> None:
        body = io.BytesIO()
        with gzip.GzipFile(fileobj=body, mode="wb") as gz:
            gz.write(("\n".join(json.dumps(r) for r in rows) + "\n").encode())
        s3_mock.put_object(Bucket=fos_source["bucket"], Key=key, Body=body.getvalue())

    return {
        "fos_source": fos_source,
        "cache": cache_path,
        "set_custom_fields": _set_custom_fields,
        "seed_gz": _seed_gz,
        "drain_ingest": lambda: list(_bk_ingest.ingest(source=fos_source)),
    }


# ── Custom-field byte budgets & validator ──
_CFG = {
    "groups": ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "L", "M"],
    "custom_fields": get_cmcd_fields(True) + [dict(c) for c in _SCORING_CUSTOM_FIELDS],
    "field_overrides": {},
}
_UUID_LEN = 36


def _caps(fmt: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for m in re.finditer(r'"(\w+)":"?%\{[^}]*?substr\([^,]+,\s*0,\s*(\d+)\)', fmt):
        out.setdefault(m.group(1), int(m.group(2)))
    return out


def test_cmcd_sid_fits_a_full_uuid():
    caps = _caps(generate_log_format(_CFG))
    assert "cmcd_sid" in caps and caps["cmcd_sid"] >= _UUID_LEN


def test_cmcd_cid_fits_a_realistic_content_id():
    assert _caps(generate_log_format(_CFG)).get("cmcd_cid", 0) >= _UUID_LEN


def test_no_custom_field_is_clamped_to_zero():
    fmt = generate_log_format(_CFG)
    zeros = [m.group(1) for m in re.finditer(r'"(\w+)":[^,]*?substr\([^,]+,\s*0,\s*0\)', fmt)]
    assert zeros == []


def test_raising_a_byte_limit_raises_the_char_cap():
    for limit, expected_cap in ((60, 10), (600, 100)):
        cfg = copy.deepcopy(_CFG)
        for cf in cfg["custom_fields"]:
            if cf["name"] == "cmcd_sid":
                cf["byte_limit"] = limit
        assert _caps(generate_log_format(cfg))["cmcd_sid"] == expected_cap


def test_widened_budgets_stay_inside_the_line_limit():
    assert check_log_line_budget(_CFG) is None
    assert estimate_log_line_bytes(_CFG) < 16384


def test_catalog_enrichment():
    by_id = {f["id"]: f for f in get_catalog_for_api()}
    assert by_id["resp_bytes"].get("formatter") == "bytes"
    assert by_id["country"].get("formatter") == "country"
    assert by_id["elapsed"].get("unit") == "µs"
    assert by_id["tls"].get("formatter") == "number" and by_id["tls"].get("precision") == 1


def _base_field(**overrides) -> dict:
    return {
        "name": "my_field",
        "label": "My Field",
        "vcl_log_expression": "req.url",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 20,
        **overrides,
    }


_NAME_REGEX = re.compile(r"^[a-z][a-z0-9_]{0,47}$")
_FORBIDDEN_VCL_CHARS = ("\n", ";", "//", "/*", "#")


@given(name=st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789_", min_size=1, max_size=48))
def test_regex_match_plus_clean_name_passes_validator(name: str):
    assume(_NAME_REGEX.match(name) and name not in _DUCKDB_RESERVED and name not in _BUILTIN_FIELD_NAMES)
    errors = validate_custom_field(_base_field(name=name), existing_names=[])
    assert not any("lowercase alphanumeric" in e for e in errors)


@given(name=st.text(min_size=1, max_size=80).filter(lambda s: not _NAME_REGEX.match(s)))
@settings(suppress_health_check=[HealthCheck.filter_too_much])
def test_regex_mismatch_always_errors(name: str):
    errors = validate_custom_field(_base_field(name=name), existing_names=[])
    assert any("lowercase alphanumeric" in e for e in errors)


@given(expr=st.text(min_size=1, max_size=400).filter(lambda s: s.strip() and any(c in s for c in _FORBIDDEN_VCL_CHARS)))
@settings(suppress_health_check=[HealthCheck.filter_too_much])
def test_vcl_expression_with_forbidden_char_always_errors(expr: str):
    errors = validate_custom_field(_base_field(vcl_log_expression=expr), existing_names=[])
    assert [e for e in errors if "newlines" in e or "semicolons" in e or "comments" in e]


@given(expr_body=st.text(alphabet=st.characters(blacklist_characters="\n;/#"), min_size=1, max_size=500))
def test_vcl_expression_clean_and_under_limit_passes(expr_body: str):
    assume(expr_body.strip())
    errors = validate_custom_field(_base_field(vcl_log_expression=expr_body), existing_names=[])
    bad = [e for e in errors if any(k in e for k in ("newlines", "semicolons", "comments", "≤ 512", "not be empty"))]
    assert not bad


@given(extra_len=st.integers(min_value=1, max_value=2000))
def test_vcl_expression_over_limit_always_errors(extra_len: int):
    errors = validate_custom_field(_base_field(vcl_log_expression="a" * (512 + extra_len)), existing_names=[])
    assert any("≤ 512" in e for e in errors)


@given(n=st.integers(min_value=-(2**31), max_value=2**31).filter(lambda x: x < 1 or x > 1024))
def test_bytes_estimate_outside_1_1024_always_errors(n: int):
    errors = validate_custom_field(_base_field(bytes_estimate=n), existing_names=[])
    assert any("bytes_estimate" in e for e in errors)


@given(n=st.integers(min_value=1, max_value=1024))
def test_bytes_estimate_within_range_passes(n: int):
    errors = validate_custom_field(_base_field(bytes_estimate=n), existing_names=[])
    assert not [e for e in errors if "bytes_estimate" in e and not e.startswith("WARN:")]


# ── Cross-service isolation ──
@pytest.fixture
def app():
    a = FastAPI()
    a.include_router(services_core_router.router)
    return a


def _save_service_config(service_id: str, custom_fields: list[dict] | None = None) -> dict:
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


@pytest.mark.security_regression
def test_custom_field_in_service_a_not_visible_in_service_b(app):
    svc_a, svc_b = "svc-aaa", "svc-bbb"
    _save_service_config(svc_a)
    _save_service_config(svc_b)

    with TestClient(app) as c:
        r = c.post(f"/api/services/{svc_a}/custom-fields", json=_cf("svc_a_field"))
        assert r.status_code == 200 and r.json()["field"]["name"] == "svc_a_field"

        r_a = c.get(f"/api/services/{svc_a}/custom-fields")
        r_b = c.get(f"/api/services/{svc_b}/custom-fields")
        assert r_a.status_code == 200 and r_b.status_code == 200
        names_a = {f["name"] for f in r_a.json()["fields"]}
        names_b = {f["name"] for f in r_b.json()["fields"]}
        assert "svc_a_field" in names_a and "svc_a_field" not in names_b

        r_a_lf = c.get(f"/api/services/{svc_a}/log-fields")
        r_b_lf = c.get(f"/api/services/{svc_b}/log-fields")
        assert {f["name"] for f in r_a_lf.json()["log_fields"]["custom_fields"]} == names_a
        assert {f["name"] for f in r_b_lf.json()["log_fields"]["custom_fields"]} == names_b

    cfg_a, cfg_b = svcconfig.load_config(svc_a), svcconfig.load_config(svc_b)
    assert cfg_a is not None and cfg_b is not None
    assert "svc_a_field" in {f.name for f in _iceberg_core.get_arrow_schema(cfg_a["log_fields"])}
    assert "svc_a_field" not in {f.name for f in _iceberg_core.get_arrow_schema(cfg_b["log_fields"])}
    assert _bk_duckdb._safe_table_name(svc_a) != _bk_duckdb._safe_table_name(svc_b)


@pytest.mark.security_regression
def test_same_field_name_in_both_services_has_independent_values(tmp_path):
    svc_a, svc_b = "svc-A-values", "svc-B-values"
    _save_service_config(svc_a, custom_fields=[_cf("my_field")])
    _save_service_config(svc_b, custom_fields=[_cf("my_field")])

    for svc in (svc_a, svc_b):
        assert "my_field" in {f.name for f in _iceberg_core.get_arrow_schema(svcconfig.load_config(svc)["log_fields"])}

    pq_a, pq_b = tmp_path / "a" / "rows.parquet", tmp_path / "b" / "rows.parquet"
    pq_a.parent.mkdir()
    pq_b.parent.mkdir()
    pq.write_table(pa.table({"my_field": ["alpha-from-A"]}), str(pq_a))
    pq.write_table(pa.table({"my_field": ["beta-from-B"]}), str(pq_b))

    con = duckdb.connect(":memory:")
    try:
        table_a, table_b = _bk_duckdb._safe_table_name(svc_a), _bk_duckdb._safe_table_name(svc_b)
        assert table_a != table_b
        con.execute(f"CREATE OR REPLACE VIEW {table_a} AS SELECT * FROM read_parquet('{pq_a}')")
        con.execute(f"CREATE OR REPLACE VIEW {table_b} AS SELECT * FROM read_parquet('{pq_b}')")
        rows_a = con.execute(f"SELECT my_field FROM {table_a}").fetchall()
        rows_b = con.execute(f"SELECT my_field FROM {table_b}").fetchall()
    finally:
        con.close()

    assert rows_a == [("alpha-from-A",)] and rows_b == [("beta-from-B",)]


# ── Hard delete & lifecycle helpers ──
def _row(ts: datetime, idx: int, **extra):
    return {
        "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S+0000"),
        "ip": f"10.0.0.{idx}",
        "status": 200,
        "url": f"/path/{idx}",
        "method": "GET",
        "cache": "HIT",
        "resp_bytes": 1024 + idx,
        "elapsed": 1500 + idx * 10,
        **extra,
    }


def _connect(env):
    con = _bk_duckdb.get_connection(env["fos_source"])
    iceberg.update_iceberg_view(con, env["fos_source"])
    return con, _safe_table(env["fos_source"]["name"])


def _query_all(env, columns: str) -> list:
    con, view_name = _connect(env)
    try:
        return con.execute(f"SELECT {columns} FROM {view_name} ORDER BY url").fetchall()
    finally:
        con.close()


def _ingest_and_commit(env, expected: int):
    done = next((e for e in env["drain_ingest"]() if e["type"] == "done"), None)
    assert done is not None and done["rows_inserted"] == expected, f"expected {expected} rows, got {done}"
    iceberg.commit_buffer(env["fos_source"])


def test_hard_delete_preserves_old_data_and_allows_new_ingest(custom_field_env):
    env = custom_field_env
    env["set_custom_fields"]([{"name": "doomed", "duckdb_type": "VARCHAR", "enabled": True, "vcl": '"doomed":""'}])
    iceberg.init_iceberg_table(env["fos_source"])

    base = datetime.now(UTC) - timedelta(hours=2)
    env["seed_gz"](
        "raw/request/year=2026/month=05/day=22/hour=10/minute=00/2026-05-22T10-00-00.svc.gz",
        [_row(base, 0, doomed="first"), _row(base + timedelta(seconds=1), 1, doomed="second")],
    )
    _ingest_and_commit(env, expected=2)

    env["set_custom_fields"]([])
    if hasattr(iceberg, "_view_cache"):
        iceberg._view_cache.clear()
    iceberg.init_iceberg_table(env["fos_source"])

    env["seed_gz"](
        "raw/request/year=2026/month=05/day=22/hour=11/minute=00/2026-05-22T11-00-00.svc.gz",
        [_row(base + timedelta(hours=1), 2)],
    )
    _ingest_and_commit(env, expected=1)

    by_url = dict(_query_all(env, "url, doomed"))
    assert by_url == {"/path/0": "first", "/path/1": "second", "/path/2": None}


def test_hard_delete_does_not_corrupt_remaining_field_values(custom_field_env):
    env = custom_field_env
    env["set_custom_fields"](
        [
            {"name": "alpha", "duckdb_type": "VARCHAR", "enabled": True, "vcl": '"alpha":""'},
            {"name": "beta", "duckdb_type": "VARCHAR", "enabled": True, "vcl": '"beta":""'},
        ]
    )
    iceberg.init_iceberg_table(env["fos_source"])

    base = datetime.now(UTC) - timedelta(hours=2)
    env["seed_gz"](
        "raw/request/year=2026/month=05/day=22/hour=10/minute=00/2026-05-22T10-00-00.svc.gz",
        [_row(base, 0, alpha="A0", beta="B0"), _row(base + timedelta(seconds=1), 1, alpha="A1", beta="B1")],
    )
    _ingest_and_commit(env, expected=2)

    env["set_custom_fields"]([{"name": "beta", "duckdb_type": "VARCHAR", "enabled": True, "vcl": '"beta":""'}])
    if hasattr(iceberg, "_view_cache"):
        iceberg._view_cache.clear()
    iceberg.init_iceberg_table(env["fos_source"])

    env["seed_gz"](
        "raw/request/year=2026/month=05/day=22/hour=11/minute=00/2026-05-22T11-00-00.svc.gz",
        [_row(base + timedelta(hours=1), 2, beta="B2")],
    )
    _ingest_and_commit(env, expected=1)

    assert dict(_query_all(env, "url, beta")) == {"/path/0": "B0", "/path/1": "B1", "/path/2": "B2"}


def _cfg(custom_fields: list[dict]) -> dict:
    return {"schema_version": 2, "groups": ["A"], "custom_fields": custom_fields}


def test_disable_field_removes_from_ingest_hints_and_catalog_ids():
    cfg = _cfg(
        [
            {"name": "field_a", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "field_b", "duckdb_type": "BIGINT", "enabled": False},
        ]
    )
    for fn in (get_ingest_type_hints, get_catalog_field_ids):
        res = fn(cfg)
        assert "field_a" in res and "field_b" not in res


def test_disable_then_reenable_restores_ingest_hints():
    disabled = _cfg([{"name": "round_trip", "duckdb_type": "VARCHAR", "enabled": False}])
    reenabled = _cfg([{"name": "round_trip", "duckdb_type": "VARCHAR", "enabled": True}])
    assert "round_trip" not in get_ingest_type_hints(disabled)
    assert get_ingest_type_hints(reenabled).get("round_trip") == "VARCHAR"


def test_disable_does_not_affect_other_enabled_fields():
    cfg = _cfg(
        [
            {"name": "keep_me", "duckdb_type": "DOUBLE", "enabled": True},
            {"name": "drop_me", "duckdb_type": "BIGINT", "enabled": False},
            {"name": "also_keep", "duckdb_type": "VARCHAR", "enabled": True},
        ]
    )
    hints = get_ingest_type_hints(cfg)
    assert hints["keep_me"] == "DOUBLE" and hints["also_keep"] == "VARCHAR" and "drop_me" not in hints


def test_new_field_added_while_old_is_disabled_does_not_collide_on_name():
    cfg = _cfg(
        [
            {"name": "old_disabled", "duckdb_type": "VARCHAR", "enabled": False},
            {"name": "new_enabled", "duckdb_type": "VARCHAR", "enabled": True},
        ]
    )
    names = {f.name for f in iceberg.get_arrow_schema(cfg)}
    assert "new_enabled" in names and "old_disabled" not in names


def test_aggregation_over_all_null_column(custom_field_env):
    env = custom_field_env
    env["set_custom_fields"]([{"name": "score", "duckdb_type": "INTEGER", "enabled": True, "vcl": '"score":0'}])
    iceberg.init_iceberg_table(env["fos_source"])

    base = datetime.now(UTC) - timedelta(hours=2)
    env["seed_gz"](
        "raw/request/year=2026/month=05/day=20/hour=10/minute=00/2026-05-20T10-00-00.svc.gz",
        [_row(base + timedelta(seconds=i), i, score=None) for i in range(3)],
    )
    _ingest_and_commit(env, expected=3)

    con, view = _connect(env)
    n_rows, n_score, avg_score = con.execute(f"SELECT COUNT(*), COUNT(score), AVG(score) FROM {view}").fetchone()
    assert n_rows == 3 and n_score == 0 and avg_score is None


# ── End-to-end roundtrip tests ──
def _build_source(tmp_path, svc_id):
    return {
        "name": svc_id,
        "duckdb_path": str(tmp_path / f"{svc_id}.db"),
        "bucket": "mock_bucket",
        "prefix": "",
        "endpoint": "mock_endpoint",
        "region": "mock_region",
    }


def _admin_request():
    return SimpleNamespace(state=SimpleNamespace())


def _install_pipeline_mocks(monkeypatch, tmp_path, source, log_dir):
    monkeypatch.setattr(_bk_duckdb, "DUCKDB_PATH", source["duckdb_path"])
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", tmp_path)
    monkeypatch.setattr(_bk_duckdb, "_cache_dir", lambda s: str(tmp_path / "cache" / s.get("name", "unknown")))
    monkeypatch.setattr(_bk_duckdb, "_configure_fos", lambda *a: None)
    monkeypatch.setattr(_bk_ingest, "_configure_fos", lambda *a: None)
    monkeypatch.setattr(_bk_duckdb, "get_source_for_service", lambda x: None)
    monkeypatch.setattr(os, "remove", lambda *a: None)

    bucket_prefix = f"s3://{source['bucket']}/"
    file_prefix = "file://" + str(log_dir.absolute()) + "/"
    _orig = _bk_duckdb.get_memory_connection

    class _Rewriter:
        def __init__(self, conn):
            self._conn = conn

        def execute(self, sql, *a, **k):
            if isinstance(sql, str) and bucket_prefix in sql:
                sql = sql.replace(bucket_prefix, file_prefix)
            return self._conn.execute(sql, *a, **k)

        def __getattr__(self, name):
            return getattr(self._conn, name)

    monkeypatch.setattr(_bk_duckdb, "get_memory_connection", lambda s: _Rewriter(_orig(s)))
    monkeypatch.setattr(_bk_ingest, "get_memory_connection", lambda s: _Rewriter(_orig(s)), raising=False)


def _ingest_once(monkeypatch, source, log_dir, key, payload, *, include_commit: bool = False):
    log_file = log_dir / key
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(log_file, "wt") as f:
        f.write(json.dumps(payload) + "\n")

    class _Fos:
        def get_paginator(self, *a, **k):
            return type("P", (), {"paginate": lambda self, **kw: [{"Contents": [{"Key": key, "Size": 100}]}]})()

        def get_object(self, Bucket, Key):
            with open(log_dir / Key, "rb") as fh:
                return {"Body": io.BytesIO(fh.read())}

        def delete_objects(self, **k):
            return {}

    monkeypatch.setattr(_bk_ingest, "_get_fos_client", lambda *a: _Fos())
    iceberg._view_cache.clear()
    _bk_duckdb._clear_schema_cache()
    _bk_duckdb._initialized_paths.clear()
    list(_bk_ingest.ingest(source=source))
    if include_commit:
        iceberg.commit_buffer(source)
    iceberg._view_cache.clear()
    _bk_duckdb._clear_schema_cache()
    _bk_duckdb._initialized_paths.clear()


def _open_view(source):
    con = _bk_duckdb.get_connection(source=source, skip_view_update=False)
    iceberg._view_cache.clear()
    iceberg.update_iceberg_view(con, source)
    return con


def _setup_roundtrip_svc(tmp_path, monkeypatch, svc_id: str, log_subdir: str, custom_fields: list[dict] | None = None):
    source = _build_source(tmp_path, svc_id)
    log_dir = tmp_path / log_subdir
    _install_pipeline_mocks(monkeypatch, tmp_path, source, log_dir)
    svcconfig.save_config(
        svc_id,
        {
            "service_id": svc_id,
            "status": {},
            "log_fields": {"schema_version": 2, "groups": ["A"], "custom_fields": custom_fields or []},
        },
    )
    _bk_duckdb._ensure_source_registered(source)
    return source, log_dir


def test_custom_field_full_roundtrip_end_to_end(tmp_path, monkeypatch):
    svc_id = "roundtrip_svc"
    source, log_dir = _setup_roundtrip_svc(tmp_path, monkeypatch, svc_id, "mock_logs_rt")

    cf = CustomFieldCreate(
        name="my_field",
        label="My Field",
        vcl_log_expression="%{req.http.X-My-Field}V",
        duckdb_type="VARCHAR",
        value_type="string",
        bytes_estimate=20,
    )
    services.api_create_custom_field(_admin_request(), svc_id, cf)

    custom = services_core_router.api_list_custom_fields(_admin_request(), svc_id).fields
    assert len(custom) == 1 and custom[0].name == "my_field" and custom[0].duckdb_type == "VARCHAR"

    _ingest_once(
        monkeypatch,
        source,
        log_dir,
        "raw/2026-05-10/12/2026-05-10T12-00-00.test.gz",
        {"timestamp": "2026-05-10T12:00:00Z", "ip": "1.2.3.4", "status": 200, "my_field": "hello"},
    )

    con = _open_view(source)
    table_name = _bk_duckdb._safe_table_name(svc_id)
    assert "my_field" in {c["name"] for c in _bk_duckdb.get_schema(con, source)}
    assert con.execute(f"SELECT my_field FROM {table_name}").fetchall() == [("hello",)]


def test_custom_field_full_roundtrip_after_disable_enable_cycle(tmp_path, monkeypatch):
    svc_id = "roundtrip_cycle_svc"
    source, log_dir = _setup_roundtrip_svc(tmp_path, monkeypatch, svc_id, "mock_logs_cycle")

    cf = CustomFieldCreate(
        name="cycle_field",
        label="Cycle Field",
        vcl_log_expression="%{req.http.X-Cycle-Field}V",
        duckdb_type="VARCHAR",
        value_type="string",
        bytes_estimate=20,
    )
    services.api_create_custom_field(_admin_request(), svc_id, cf)
    _ingest_once(
        monkeypatch,
        source,
        log_dir,
        "raw/2026-05-10/12/2026-05-10T12-00-00.v1.gz",
        {"timestamp": "2026-05-10T12:00:00Z", "ip": "1.2.3.4", "status": 200, "cycle_field": "v1"},
    )

    for enabled in (False, True):
        services_core_router.api_update_custom_field(
            _admin_request(), svc_id, "cycle_field", CustomFieldUpdate(enabled=enabled)
        )
        cfg_fields = svcconfig.load_config(svc_id)["log_fields"]["custom_fields"]
        assert next(c for c in cfg_fields if c["name"] == "cycle_field")["enabled"] is enabled

    _ingest_once(
        monkeypatch,
        source,
        log_dir,
        "raw/2026-05-10/13/2026-05-10T13-00-00.v2.gz",
        {"timestamp": "2026-05-10T13:00:00Z", "ip": "1.2.3.5", "status": 200, "cycle_field": "v2"},
    )

    con = _open_view(source)
    table_name = _bk_duckdb._safe_table_name(svc_id)
    values = [r[0] for r in con.execute(f"SELECT cycle_field FROM {table_name} ORDER BY timestamp").fetchall()]
    assert values == ["v1", "v2"]


def test_ingest_and_query_custom_fields(tmp_path, monkeypatch):
    svc_id = "custom_test_svc"
    source, log_dir = _setup_roundtrip_svc(
        tmp_path,
        monkeypatch,
        svc_id,
        "mock_logs_int",
        [
            {
                "name": "x_custom_id",
                "label": "Custom ID",
                "vcl_log_expression": "%{req.http.X-Custom-ID}V",
                "duckdb_type": "VARCHAR",
                "value_type": "string",
                "bytes_estimate": 36,
                "enabled": True,
            }
        ],
    )
    _ingest_once(
        monkeypatch,
        source,
        log_dir,
        "raw/2026-05-01/00/2026-05-01T00-00-00.000-test.log.gz",
        {
            "timestamp": "2026-05-01T00:00:00Z",
            "ip": "1.2.3.4",
            "status": 200,
            "url": "/test",
            "method": "GET",
            "cache": "HIT",
            "resp_bytes": 100,
            "elapsed": 5000,
            "x_custom_id": "abc-123-xyz",
        },
        include_commit=True,
    )

    con = _open_view(source)
    assert "x_custom_id" in {c["name"] for c in _bk_duckdb.get_schema(con, source)}
    assert con.execute(f"SELECT x_custom_id FROM {_bk_duckdb._safe_table_name(svc_id)}").fetchall() == [
        ("abc-123-xyz",)
    ]


def test_ingest_and_query_numeric_custom_fields(tmp_path, monkeypatch):
    svc_id = "numeric_custom_svc"
    source, log_dir = _setup_roundtrip_svc(
        tmp_path,
        monkeypatch,
        svc_id,
        "mock_logs_num",
        [
            {
                "name": "custom_int",
                "label": "Custom Int",
                "vcl_log_expression": "%{req.http.X-Int}V",
                "duckdb_type": "BIGINT",
                "value_type": "integer",
                "bytes_estimate": 8,
                "enabled": True,
            },
            {
                "name": "custom_float",
                "label": "Custom Float",
                "vcl_log_expression": "%{req.http.X-Float}V",
                "duckdb_type": "DOUBLE",
                "value_type": "float",
                "bytes_estimate": 10,
                "enabled": True,
            },
        ],
    )
    _ingest_once(
        monkeypatch,
        source,
        log_dir,
        "raw/2026-05-01/00/2026-05-01T00-00-00.000-num.log.gz",
        {
            "timestamp": "2026-05-01T00:00:00Z",
            "ip": "1.2.3.4",
            "status": 200,
            "url": "/test",
            "method": "GET",
            "cache": "HIT",
            "resp_bytes": 100,
            "elapsed": 5000,
            "custom_int": 42,
            "custom_float": 3.14159,
        },
        include_commit=True,
    )

    con = _open_view(source)
    schema_dict = {c["name"]: c["type"] for c in _bk_duckdb.get_schema(con, source)}
    assert schema_dict.get("custom_int") == "BIGINT" and schema_dict.get("custom_float") == "DOUBLE"
    table_name = _bk_duckdb._safe_table_name(svc_id)
    assert con.execute(f"SELECT custom_int * 2, ROUND(custom_float, 2) FROM {table_name}").fetchall() == [(84, 3.14)]


# ── Type mismatch resilience ──
def _run_type_mismatch_batch(env, field_def: dict, gz_key: str, values: list):
    env["set_custom_fields"]([field_def])
    iceberg.init_iceberg_table(env["fos_source"])
    base = datetime.now(UTC) - timedelta(hours=2)
    col = field_def["name"]
    env["seed_gz"](
        gz_key,
        [_row(base + timedelta(seconds=i), i, **{col: v}) for i, v in enumerate(values)],
    )
    _ingest_and_commit(env, expected=len(values))
    return dict(_query_all(env, f"url, {col}"))


def test_integer_field_with_string_value_nulls_only_that_cell(custom_field_env):
    by_url = _run_type_mismatch_batch(
        custom_field_env,
        {
            "name": "user_score",
            "duckdb_type": "INTEGER",
            "enabled": True,
            "vcl": '"user_score":%{req.http.X-User-Score}V',
        },
        "raw/request/year=2026/month=05/day=20/hour=10/minute=00/2026-05-20T10-00-00.svc.gz",
        [42, "NaN", 17],
    )
    assert by_url == {"/path/0": 42, "/path/1": None, "/path/2": 17}


def test_integer_field_with_string_float_truncates_not_nulls(custom_field_env):
    by_url = _run_type_mismatch_batch(
        custom_field_env,
        {"name": "score", "duckdb_type": "INTEGER", "enabled": True, "vcl": '"score":1'},
        "raw/request/year=2026/month=05/day=20/hour=10/minute=00/2026-05-20T10-00-00.svc.gz",
        [1, "3.14"],
    )
    assert by_url == {"/path/0": 1, "/path/1": 3}


def test_boolean_field_with_arbitrary_string_nulls_the_cell(custom_field_env):
    by_url = _run_type_mismatch_batch(
        custom_field_env,
        {"name": "is_bot", "duckdb_type": "BOOLEAN", "enabled": True, "vcl": '"is_bot":false'},
        "raw/request/year=2026/month=05/day=20/hour=10/minute=00/2026-05-20T10-00-00.svc.gz",
        [True, "maybe", False],
    )
    assert by_url == {"/path/0": True, "/path/1": None, "/path/2": False}


def test_batch_proceeds_when_one_file_has_only_type_mismatches(custom_field_env):
    env = custom_field_env
    env["set_custom_fields"]([{"name": "n", "duckdb_type": "INTEGER", "enabled": True, "vcl": '"n":1'}])
    iceberg.init_iceberg_table(env["fos_source"])

    base = datetime.now(UTC) - timedelta(hours=2)
    env["seed_gz"](
        "raw/request/year=2026/month=05/day=20/hour=10/minute=00/2026-05-20T10-00-00.bad.gz",
        [_row(base + timedelta(seconds=i), i, n="bad") for i in range(3)],
    )
    env["seed_gz"](
        "raw/request/year=2026/month=05/day=20/hour=10/minute=05/2026-05-20T10-05-00.good.gz",
        [_row(base + timedelta(minutes=5, seconds=i), 100 + i, n=50 + i) for i in range(3)],
    )
    _ingest_and_commit(env, expected=6)

    rows = _query_all(env, "url, n")
    assert len(rows) == 6
    assert sum(1 for _, v in rows if v is None) == 3
    assert sum(1 for _, v in rows if v is not None) == 3


@pytest.mark.parametrize(
    "ddb_type,bad_value,description",
    [
        ("BIGINT", "NaN", "BIGINT ← string 'NaN' → NULL"),
        ("BOOLEAN", "maybe", "BOOLEAN ← arbitrary string → NULL"),
        ("BIGINT", "not-a-number-at-all", "BIGINT ← clearly invalid string → NULL"),
        ("DOUBLE", "lol", "DOUBLE ← clearly invalid string → NULL"),
    ],
)
def test_typed_field_with_invalid_string_value_nulls_cell(custom_field_env, ddb_type, bad_value, description):
    by_url = _run_type_mismatch_batch(
        custom_field_env,
        {"name": "metric", "duckdb_type": ddb_type, "enabled": True, "vcl": '"metric":%{req.http.X-Metric}V'},
        "raw/request/year=2026/month=05/day=21/hour=10/minute=00/2026-05-21T10-00-00.matrix.gz",
        [1, bad_value, 2],
    )
    assert by_url["/path/1"] is None, f"contract violation ({description}): got {by_url['/path/1']!r}"

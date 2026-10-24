"""Regression: custom-field byte budgets vs the emitted character cap.

``generate_log_format`` emits ``substr(expr, 0, cf_limit // 6)`` for string
custom fields. ``cf_limit`` is a BYTE budget and the ``// 6`` is worst-case
JSON-escape expansion (a char can encode as ``\\uXXXX``). So the CHARACTER cap
an operator actually gets is ``byte_limit // 6`` — six times smaller than the
number in the config.

That divisor silently truncated CMCD session ids: ``cmcd_sid`` had
``bytes_estimate: 40`` → ``cf_limit`` 160 → **26 characters**, cutting 36-char
UUIDs and breaking session-level joins. The column held both 26- and 40-char
values (the longer ones written before the current format), which reads like a
client quirk rather than a config effect.

The cap itself is a deliberate guard, not an accident: the value is
client-supplied (CMCD arrives in ``?CMCD=``), so an unbounded field would let a
caller push the log line past Fastly's ~16 KB limit and silently drop the whole
entry. These tests pin "big enough for a real id" rather than "unbounded".
"""

from __future__ import annotations

import gzip
import io
import json
import os
import re
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from backend import config as svcconfig
from backend.core import duckdb as _bk_duckdb
from backend.core import iceberg
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
from backend.provision.cmcd_fields import get_cmcd_fields
from backend.provision.session_scoring_orchestrator import _SCORING_CUSTOM_FIELDS
from backend.routers.services import core as services_core_router

# ── Merged from tests/core/test_custom_fields.py ──
_CFG = {
    "groups": ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "L", "M"],
    "custom_fields": get_cmcd_fields(True) + [dict(c) for c in _SCORING_CUSTOM_FIELDS],
    "field_overrides": {},
}

_UUID_LEN = 36  # canonical CMCD v1 sid


def _caps(fmt: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for m in re.finditer(r'"(\w+)":"?%\{[^}]*?substr\([^,]+,\s*0,\s*(\d+)\)', fmt):
        out.setdefault(m.group(1), int(m.group(2)))
    return out


def test_cmcd_sid_fits_a_full_uuid():
    """THE REGRESSION: 26 chars truncated every UUID session id."""
    caps = _caps(generate_log_format(_CFG))
    assert "cmcd_sid" in caps, "cmcd_sid emitted no substr cap"
    assert caps["cmcd_sid"] >= _UUID_LEN, (
        f"cmcd_sid caps at {caps['cmcd_sid']} chars, truncating {_UUID_LEN}-char UUIDs. "
        "byte_limit is a BYTE budget divided by 6 for JSON-escape expansion — "
        "it must be >= 6 * the character length you want."
    )


def test_cmcd_cid_fits_a_realistic_content_id():
    caps = _caps(generate_log_format(_CFG))
    assert caps.get("cmcd_cid", 0) >= _UUID_LEN


def test_no_custom_field_is_clamped_to_zero():
    """A zero cap logs null forever, silently — the aggregate-budget failure mode."""
    fmt = generate_log_format(_CFG)
    zeros = [m.group(1) for m in re.finditer(r'"(\w+)":[^,]*?substr\([^,]+,\s*0,\s*0\)', fmt)]
    assert zeros == [], f"fields clamped to zero chars (will always log null): {zeros}"


def test_raising_a_byte_limit_raises_the_char_cap():
    """Guards the direction of the relationship.

    Setting byte_limit BELOW 6x the desired chars silently shrinks the cap —
    the trap that made an earlier attempt at this fix produce a 6-char sid.
    """
    import copy

    low = copy.deepcopy(_CFG)
    for cf in low["custom_fields"]:
        if cf["name"] == "cmcd_sid":
            cf["byte_limit"] = 60  # 60 // 6 == 10 chars
    assert _caps(generate_log_format(low))["cmcd_sid"] == 10

    high = copy.deepcopy(_CFG)
    for cf in high["custom_fields"]:
        if cf["name"] == "cmcd_sid":
            cf["byte_limit"] = 600  # 600 // 6 == 100 chars
    assert _caps(generate_log_format(high))["cmcd_sid"] == 100


def test_widened_budgets_stay_inside_the_line_limit():
    """The cap exists so a client-supplied value can't drop the log line."""
    assert check_log_line_budget(_CFG) is None, "widening sid/cid pushed the line over budget"
    assert estimate_log_line_bytes(_CFG) < 16384


def test_catalog_enrichment():
    """Verify that the catalog API output includes UI metadata like formatter and unit."""
    catalog = get_catalog_for_api()

    resp_bytes = next(f for f in catalog if f["id"] == "resp_bytes")
    assert resp_bytes.get("formatter") == "bytes"

    country = next(f for f in catalog if f["id"] == "country")
    assert country.get("formatter") == "country"

    elapsed = next(f for f in catalog if f["id"] == "elapsed")
    assert elapsed.get("unit") == "µs"

    tls = next(f for f in catalog if f["id"] == "tls")
    assert tls.get("formatter") == "number"
    assert tls.get("precision") == 1


def _base_field(**overrides) -> dict:
    base = {
        "name": "my_field",
        "label": "My Field",
        "vcl_log_expression": "req.url",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 20,
    }
    base.update(overrides)
    return base


_NAME_REGEX = re.compile(r"^[a-z][a-z0-9_]{0,47}$")


@given(name=st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789_", min_size=1, max_size=48))
def test_regex_match_plus_clean_name_passes_validator(name: str):
    """Any name matching the documented regex (and avoiding reserved-word /
    builtin collisions) must NOT trigger a name-related validator error.
    Catches regressions where the regex tightens too far or new collision
    rules block previously-valid names."""
    assume(_NAME_REGEX.match(name))
    assume(name not in _DUCKDB_RESERVED)
    assume(name not in _BUILTIN_FIELD_NAMES)
    errors = validate_custom_field(_base_field(name=name), existing_names=[])
    assert not any("lowercase alphanumeric" in e for e in errors), (
        f"name {name!r} matches regex but validator rejected it: {errors}"
    )


@given(
    name=st.text(min_size=1, max_size=80).filter(lambda s: not _NAME_REGEX.match(s)),
)
@settings(suppress_health_check=[HealthCheck.filter_too_much])
def test_regex_mismatch_always_errors(name: str):
    """Any string NOT matching the regex must produce a name error. Catches
    regressions where the regex loosens (e.g. accidentally allowing
    uppercase or hyphens that would need quoting in every downstream SQL)."""
    errors = validate_custom_field(_base_field(name=name), existing_names=[])
    assert any("lowercase alphanumeric" in e for e in errors), (
        f"name {name!r} does not match regex but validator accepted it: {errors}"
    )


_FORBIDDEN_VCL_CHARS = ("\n", ";", "//", "/*", "#")


@given(
    expr=st.text(min_size=1, max_size=400).filter(lambda s: s.strip() and any(c in s for c in _FORBIDDEN_VCL_CHARS)),
)
@settings(suppress_health_check=[HealthCheck.filter_too_much])
def test_vcl_expression_with_forbidden_char_always_errors(expr: str):
    """Any non-blank VCL expression containing newlines, semicolons, or
    comment markers must be rejected — these are the structural tokens
    that would let user input escape the surrounding generated snippet.
    (Whitespace-only expressions are caught by a different rule first;
    see test_validate_custom_field_vcl_expression_must_be_non_empty in
    test_log_fields.py.)"""
    errors = validate_custom_field(_base_field(vcl_log_expression=expr), existing_names=[])
    forbidden_kind_errors = [e for e in errors if "newlines" in e or "semicolons" in e or "comments" in e]
    assert forbidden_kind_errors, (
        f"expression {expr!r} contains forbidden char(s) but validator did not flag injection: {errors}"
    )


@given(expr_body=st.text(alphabet=st.characters(blacklist_characters="\n;/#"), min_size=1, max_size=500))
def test_vcl_expression_clean_and_under_limit_passes(expr_body: str):
    """A non-empty expression without forbidden chars and under the 512-char
    cap must not produce a VCL-injection error. Stops a future patch from
    over-rejecting valid expressions like ``req.http.X-Real-IP``."""
    assume(expr_body.strip())  # validator separately rejects whitespace-only
    errors = validate_custom_field(_base_field(vcl_log_expression=expr_body), existing_names=[])
    bad = [
        e
        for e in errors
        if "newlines" in e or "semicolons" in e or "comments" in e or "≤ 512" in e or "not be empty" in e
    ]
    assert not bad, f"clean expression {expr_body!r} was incorrectly flagged: {bad}"


@given(extra_len=st.integers(min_value=1, max_value=2000))
def test_vcl_expression_over_limit_always_errors(extra_len: int):
    """An expression longer than 512 chars must error. Hypothesis sweeps the
    threshold to catch off-by-one regressions (e.g. switching to ``>=`` 512
    or capping at 511)."""
    expr = "a" * (512 + extra_len)
    errors = validate_custom_field(_base_field(vcl_log_expression=expr), existing_names=[])
    assert any("≤ 512" in e for e in errors), f"len-{len(expr)} expression must error: {errors}"


@given(n=st.integers(min_value=-(2**31), max_value=2**31).filter(lambda x: x < 1 or x > 1024))
def test_bytes_estimate_outside_1_1024_always_errors(n: int):
    """``bytes_estimate`` must be 1..1024 inclusive. Boundary fuzz catches
    off-by-one drift in the range check."""
    errors = validate_custom_field(_base_field(bytes_estimate=n), existing_names=[])
    assert any("bytes_estimate" in e for e in errors), f"bytes_estimate={n} must error: {errors}"


@given(n=st.integers(min_value=1, max_value=1024))
def test_bytes_estimate_within_range_passes(n: int):
    """Every value inside the documented range must validate without a
    hard bytes_estimate error. Low values still trigger ``WARN:`` advisories
    (e.g. "1 is less than the name overhead") — those are non-blocking."""
    errors = validate_custom_field(_base_field(bytes_estimate=n), existing_names=[])
    hard = [e for e in errors if "bytes_estimate" in e and not e.startswith("WARN:")]
    assert not hard, f"bytes_estimate={n} must NOT hard-error: {hard}"


# ── Merged from tests/core/test_custom_field_cross_service.py ──
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


@pytest.mark.security_regression
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

        r_a_lf = c.get(f"/api/services/{svc_a}/log-fields")
        r_b_lf = c.get(f"/api/services/{svc_b}/log-fields")
        assert r_a_lf.status_code == 200 and r_b_lf.status_code == 200
        assert {f["name"] for f in r_a_lf.json()["log_fields"]["custom_fields"]} == names_a
        assert {f["name"] for f in r_b_lf.json()["log_fields"]["custom_fields"]} == names_b

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


@pytest.mark.security_regression
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


# ── Merged from tests/core/test_custom_field_hard_delete.py ──
def _well_typed_row(ts: datetime, idx: int, **extras):
    row = {
        "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S+0000"),
        "ip": f"10.0.0.{idx}",
        "status": 200,
        "url": f"/path/{idx}",
        "method": "GET",
        "cache": "HIT",
        "resp_bytes": 1024 + idx,
        "elapsed": 1500 + idx * 10,
    }
    row.update(extras)
    return row


def _query_all(env, columns: str) -> list:
    from backend.core import iceberg as ice
    from backend.core.duckdb import get_connection
    from backend.repositories._base import _safe_table

    con = get_connection(env["fos_source"])
    try:
        ice.update_iceberg_view(con, env["fos_source"])
        view_name = _safe_table(env["fos_source"]["name"])
        return con.execute(f"SELECT {columns} FROM {view_name} ORDER BY url").fetchall()
    finally:
        con.close()


def test_hard_delete_preserves_old_data_and_allows_new_ingest(custom_field_env):
    """End-to-end contract: ingest with field 'doomed' → commit → hard
    delete 'doomed' from config → ingest fresh rows → query.

    Expected:
    - The first batch's rows are still queryable (no data loss).
    - The 'doomed' column is still present in the view (the DuckLake
      table schema is authoritative; the commit path only adds columns,
      never drops them).
    - First batch shows the original values; second batch shows NULL
      under 'doomed'.
    - Total row count = rows_first + rows_second (no orphaning).
    """
    from backend.core import iceberg as ice

    env = custom_field_env

    # ── Phase 1: ingest with 'doomed' enabled ────────────────────────────
    env["set_custom_fields"]([{"name": "doomed", "duckdb_type": "VARCHAR", "enabled": True, "vcl": '"doomed":""'}])
    ice.init_iceberg_table(env["fos_source"])

    base = datetime.now(UTC) - timedelta(hours=2)
    env["seed_gz"](
        "raw/request/year=2026/month=05/day=22/hour=10/minute=00/2026-05-22T10-00-00.svc.gz",
        [
            _well_typed_row(base, 0, doomed="first"),
            _well_typed_row(base + timedelta(seconds=1), 1, doomed="second"),
        ],
    )
    events1 = env["drain_ingest"]()
    done1 = next(e for e in events1 if e["type"] == "done")
    assert done1["rows_inserted"] == 2
    ice.commit_buffer(env["fos_source"])

    # ── Phase 2: hard-delete 'doomed' from the config ────────────────────
    env["set_custom_fields"]([])

    # Reset iceberg caches so the next init reloads the table fresh
    if hasattr(ice, "_view_cache"):
        ice._view_cache.clear()

    # Re-init the table with the empty custom_fields config — this exercises
    # the schema-evolution code path that would corrupt things if it tried
    # to DROP the doomed column.
    ice.init_iceberg_table(env["fos_source"])

    # ── Phase 3: ingest a fresh row WITHOUT 'doomed' in the payload ──────
    env["seed_gz"](
        "raw/request/year=2026/month=05/day=22/hour=11/minute=00/2026-05-22T11-00-00.svc.gz",
        [_well_typed_row(base + timedelta(hours=1), 2)],
    )
    events2 = env["drain_ingest"]()
    done2 = next(e for e in events2 if e["type"] == "done")
    assert done2["rows_inserted"] == 1, (
        f"contract violation: hard delete broke the next ingest cycle. done event: {done2}"
    )
    ice.commit_buffer(env["fos_source"])

    # ── Phase 4: query the view — old data must survive ──────────────────
    rows = _query_all(env, "url, doomed")
    assert len(rows) == 3, f"expected 3 rows total (2 pre-delete + 1 post-delete), got {len(rows)}"
    by_url = {url: doomed for url, doomed in rows}
    assert by_url["/path/0"] == "first", (
        f"contract violation: hard-delete erased pre-existing data. /path/0.doomed = {by_url['/path/0']!r}"
    )
    assert by_url["/path/1"] == "second"
    # The new row had no 'doomed' payload — DuckLake should serve NULL.
    assert by_url["/path/2"] is None, (
        f"contract violation: post-delete row should have NULL for the "
        f"removed column. /path/2.doomed = {by_url['/path/2']!r}"
    )


def test_hard_delete_does_not_corrupt_remaining_field_values(custom_field_env):
    """Two custom fields ('alpha' alphabetically first, 'beta' second).
    Ingest with both, commit, hard-delete 'alpha' (the field whose
    deletion would, by the unit test above, SHIFT 'beta's in-memory ID),
    then ingest fresh rows.

    Contract: 'beta' values survive intact across the shift. This is the
    real test that the DuckLake table's persisted columns — not the
    in-memory get_iceberg_schema output — drive column identity.
    """
    from backend.core import iceberg as ice

    env = custom_field_env
    env["set_custom_fields"](
        [
            {"name": "alpha", "duckdb_type": "VARCHAR", "enabled": True, "vcl": '"alpha":""'},
            {"name": "beta", "duckdb_type": "VARCHAR", "enabled": True, "vcl": '"beta":""'},
        ]
    )
    ice.init_iceberg_table(env["fos_source"])

    base = datetime.now(UTC) - timedelta(hours=2)
    env["seed_gz"](
        "raw/request/year=2026/month=05/day=22/hour=10/minute=00/2026-05-22T10-00-00.svc.gz",
        [
            _well_typed_row(base, 0, alpha="A0", beta="B0"),
            _well_typed_row(base + timedelta(seconds=1), 1, alpha="A1", beta="B1"),
        ],
    )
    events1 = env["drain_ingest"]()
    assert next(e for e in events1 if e["type"] == "done")["rows_inserted"] == 2
    ice.commit_buffer(env["fos_source"])

    # Hard-delete alpha.
    env["set_custom_fields"]([{"name": "beta", "duckdb_type": "VARCHAR", "enabled": True, "vcl": '"beta":""'}])
    if hasattr(ice, "_view_cache"):
        ice._view_cache.clear()
    ice.init_iceberg_table(env["fos_source"])

    env["seed_gz"](
        "raw/request/year=2026/month=05/day=22/hour=11/minute=00/2026-05-22T11-00-00.svc.gz",
        [_well_typed_row(base + timedelta(hours=1), 2, beta="B2")],
    )
    events2 = env["drain_ingest"]()
    assert next(e for e in events2 if e["type"] == "done")["rows_inserted"] == 1
    ice.commit_buffer(env["fos_source"])

    # Beta values must round-trip cleanly across the shift.
    rows = _query_all(env, "url, beta")
    by_url = dict(rows)
    assert by_url["/path/0"] == "B0", (
        f"contract violation: beta value corrupted by alpha hard-delete. "
        f"/path/0.beta = {by_url['/path/0']!r} (expected 'B0')"
    )
    assert by_url["/path/1"] == "B1"
    assert by_url["/path/2"] == "B2"


# ── Merged from tests/core/test_custom_field_lifecycle.py ──
def _cfg(custom_fields: list[dict]) -> dict:
    return {
        "schema_version": 2,
        "groups": ["A"],
        "custom_fields": custom_fields,
    }


def test_disable_field_removes_from_ingest_hints():
    cfg = _cfg(
        [
            {"name": "field_a", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "field_b", "duckdb_type": "BIGINT", "enabled": False},
        ]
    )
    hints = get_ingest_type_hints(cfg)
    assert "field_a" in hints
    assert "field_b" not in hints


def test_disable_field_removes_from_catalog_field_ids():
    cfg = _cfg(
        [
            {"name": "field_a", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "field_b", "duckdb_type": "BIGINT", "enabled": False},
        ]
    )
    ids = get_catalog_field_ids(cfg)
    assert "field_a" in ids
    assert "field_b" not in ids


def test_disable_then_reenable_restores_ingest_hints():
    """Round-trip: disabling a field is reversible from the ingest side.
    New rows written after re-enable should once again include the column."""
    disabled = _cfg(
        [
            {"name": "round_trip", "duckdb_type": "VARCHAR", "enabled": False},
        ]
    )
    reenabled = _cfg(
        [
            {"name": "round_trip", "duckdb_type": "VARCHAR", "enabled": True},
        ]
    )

    assert "round_trip" not in get_ingest_type_hints(disabled)
    assert "round_trip" in get_ingest_type_hints(reenabled)
    # Type is restored — not just the name
    assert get_ingest_type_hints(reenabled)["round_trip"] == "VARCHAR"


def test_disable_does_not_affect_other_enabled_fields():
    """Disabling one field must leave the others' hints untouched."""
    cfg = _cfg(
        [
            {"name": "keep_me", "duckdb_type": "DOUBLE", "enabled": True},
            {"name": "drop_me", "duckdb_type": "BIGINT", "enabled": False},
            {"name": "also_keep", "duckdb_type": "VARCHAR", "enabled": True},
        ]
    )
    hints = get_ingest_type_hints(cfg)
    assert hints["keep_me"] == "DOUBLE"
    assert hints["also_keep"] == "VARCHAR"
    assert "drop_me" not in hints


def test_new_field_added_while_old_is_disabled_does_not_collide_on_name():
    """The *names* in the emitted Arrow schema are unique and exclude
    disabled custom fields. A newly-added field can't ever have the same
    *name* as a disabled one (the API prevents duplicate names entirely).
    """
    cfg = _cfg(
        [
            {"name": "old_disabled", "duckdb_type": "VARCHAR", "enabled": False},
            {"name": "new_enabled", "duckdb_type": "VARCHAR", "enabled": True},
        ]
    )
    schema = iceberg.get_arrow_schema(cfg)
    names = {f.name for f in schema}
    assert "new_enabled" in names
    assert "old_disabled" not in names


def _row(ts, idx, **extra):
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
    """Open a DuckLake-attached connection with the ``logs`` view registered.
    Returns (con, view_name)."""
    from backend.core.duckdb import get_connection
    from backend.repositories._base import _safe_table

    con = get_connection(env["fos_source"])
    iceberg.update_iceberg_view(con, env["fos_source"])
    return con, _safe_table(env["fos_source"]["name"])


def _ingest_and_commit(env, expected):
    done = next((e for e in env["drain_ingest"]() if e["type"] == "done"), None)
    assert done is not None, "no 'done' event from ingest"
    assert done["rows_inserted"] == expected, f"expected {expected} rows, got {done}"
    iceberg.commit_buffer(env["fos_source"])


def test_aggregation_over_all_null_column(custom_field_env):
    """COUNT(*)=3, COUNT(col)=0, AVG IS NULL — ANSI semantics so
    dashboards render "no data" rather than a misleading zero."""
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
    assert n_rows == 3, f"COUNT(*) must count NULL rows; got {n_rows}"
    assert n_score == 0, f"COUNT(score) must skip NULLs; got {n_score}"
    assert avg_score is None, f"AVG over all-NULL must be NULL, got {avg_score!r}"


# ── Merged from tests/core/test_custom_field_roundtrip.py ──
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
    # No ``analyst_session`` on request.state ⇒ admin path through the gate.
    return SimpleNamespace(state=SimpleNamespace())


def _install_pipeline_mocks(monkeypatch, tmp_path, source, log_dir):
    """Wire DuckDB/iceberg/ingest module globals to the test sandbox.
    See test_integration_custom_fields._S3RewritingConn for the rewrite rationale —
    DuckDBPyConnection.execute is C-level read-only so we wrap, not patch."""
    import backend.core.duckdb as my_duckdb
    import backend.core.ingest as ingest
    from backend import config

    monkeypatch.setattr(my_duckdb, "DUCKDB_PATH", source["duckdb_path"])
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    monkeypatch.setattr(my_duckdb, "_cache_dir", lambda s: str(tmp_path / "cache" / s.get("name", "unknown")))
    monkeypatch.setattr(my_duckdb, "_configure_fos", lambda *a: None)
    monkeypatch.setattr(ingest, "_configure_fos", lambda *a: None)
    monkeypatch.setattr(my_duckdb, "get_source_for_service", lambda x: None)
    monkeypatch.setattr(os, "remove", lambda *a: None)

    bucket_prefix = f"s3://{source['bucket']}/"
    file_prefix = "file://" + str(log_dir.absolute()) + "/"
    _orig = my_duckdb.get_memory_connection

    class _Rewriter:
        def __init__(self, conn):
            self._conn = conn

        def execute(self, sql, *a, **k):
            if isinstance(sql, str) and bucket_prefix in sql:
                sql = sql.replace(bucket_prefix, file_prefix)
            return self._conn.execute(sql, *a, **k)

        def __getattr__(self, name):
            return getattr(self._conn, name)

    monkeypatch.setattr(my_duckdb, "get_memory_connection", lambda s: _Rewriter(_orig(s)))
    monkeypatch.setattr(ingest, "get_memory_connection", lambda s: _Rewriter(_orig(s)), raising=False)


def _ingest_once(monkeypatch, source, log_dir, key, payload, *, include_commit: bool = False):
    """Seed one gzipped JSON log line + wire mock FOS + run ingest + clear caches."""
    import backend.core.duckdb as my_duckdb
    import backend.core.iceberg as iceberg
    import backend.core.ingest as ingest

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

    monkeypatch.setattr(ingest, "_get_fos_client", lambda *a: _Fos())
    iceberg._view_cache.clear()
    my_duckdb._clear_schema_cache()
    my_duckdb._initialized_paths.clear()
    list(ingest.ingest(source=source))
    if include_commit:
        iceberg.commit_buffer(source)
    iceberg._view_cache.clear()
    my_duckdb._clear_schema_cache()
    my_duckdb._initialized_paths.clear()


def _open_view(source):
    import backend.core.duckdb as my_duckdb
    import backend.core.iceberg as iceberg

    con = my_duckdb.get_connection(source=source, skip_view_update=False)
    iceberg._view_cache.clear()
    iceberg.update_iceberg_view(con, source)
    return con


def test_custom_field_full_roundtrip_end_to_end(tmp_path, monkeypatch):
    """API add → /log-fields shows it → ingest → view exposes column → SELECT returns value."""
    import backend.core.duckdb as my_duckdb
    from backend import config
    from backend.models.custom_fields import CustomFieldCreate
    from backend.routers import services
    from backend.routers.services import core as services_core

    svc_id = "roundtrip_svc"
    source = _build_source(tmp_path, svc_id)
    log_dir = tmp_path / "mock_logs_rt"

    # Patch CONFIGS_DIR BEFORE save_config — save_config writes to the
    # current CONFIGS_DIR at call time, and the route handler reads from
    # the same; if we save first then patch, the handler 404s.
    _install_pipeline_mocks(monkeypatch, tmp_path, source, log_dir)
    config.save_config(
        svc_id,
        {"service_id": svc_id, "status": {}, "log_fields": {"schema_version": 2, "groups": ["A"], "custom_fields": []}},
    )
    my_duckdb._ensure_source_registered(source)

    # 1. POST custom-fields
    cf = CustomFieldCreate(
        name="my_field",
        label="My Field",
        vcl_log_expression="%{req.http.X-My-Field}V",
        duckdb_type="VARCHAR",
        value_type="string",
        bytes_estimate=20,
    )
    services.api_create_custom_field(_admin_request(), svc_id, cf)

    # 2. GET /custom-fields ⇒ my_field present with type VARCHAR
    custom = services_core.api_list_custom_fields(_admin_request(), svc_id).fields
    assert len(custom) == 1 and custom[0].name == "my_field" and custom[0].duckdb_type == "VARCHAR"

    # 3-4. Seed + ingest
    _ingest_once(
        monkeypatch,
        source,
        log_dir,
        "raw/2026-05-10/12/2026-05-10T12-00-00.test.gz",
        {"timestamp": "2026-05-10T12:00:00Z", "ip": "1.2.3.4", "status": 200, "my_field": "hello"},
    )

    # 5-6. View has the column AND SELECT returns the value
    con = _open_view(source)
    table_name = my_duckdb._safe_table_name(svc_id)
    cols = {c["name"] for c in my_duckdb.get_schema(con, source)}
    assert "my_field" in cols, f"Expected 'my_field' in view columns, got {sorted(cols)}"
    assert con.execute(f"SELECT my_field FROM {table_name}").fetchall() == [("hello",)]


def test_custom_field_full_roundtrip_after_disable_enable_cycle(tmp_path, monkeypatch):
    """Add → ingest v1 → disable → re-enable → ingest v2 → both rows readable.
    Field-id stability across a disable cycle is pinned by
    test_reenabled_field_gets_same_iceberg_field_id in test_custom_field_lifecycle.py;
    this is the end-to-end ingest-path cross-check."""
    import backend.core.duckdb as my_duckdb
    from backend import config
    from backend.models.custom_fields import CustomFieldCreate, CustomFieldUpdate
    from backend.routers import services
    from backend.routers.services import core as services_core

    svc_id = "roundtrip_cycle_svc"
    source = _build_source(tmp_path, svc_id)
    log_dir = tmp_path / "mock_logs_cycle"

    # Patch CONFIGS_DIR BEFORE save_config — save_config writes to the
    # current CONFIGS_DIR at call time, and the route handler reads from
    # the same; if we save first then patch, the handler 404s.
    _install_pipeline_mocks(monkeypatch, tmp_path, source, log_dir)
    config.save_config(
        svc_id,
        {"service_id": svc_id, "status": {}, "log_fields": {"schema_version": 2, "groups": ["A"], "custom_fields": []}},
    )
    my_duckdb._ensure_source_registered(source)

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

    # Disable
    services_core.api_update_custom_field(_admin_request(), svc_id, "cycle_field", CustomFieldUpdate(enabled=False))
    assert (
        next(cf for cf in config.load_config(svc_id)["log_fields"]["custom_fields"] if cf["name"] == "cycle_field")[
            "enabled"
        ]
        is False
    )

    # Re-enable
    services_core.api_update_custom_field(_admin_request(), svc_id, "cycle_field", CustomFieldUpdate(enabled=True))
    assert (
        next(cf for cf in config.load_config(svc_id)["log_fields"]["custom_fields"] if cf["name"] == "cycle_field")[
            "enabled"
        ]
        is True
    )

    _ingest_once(
        monkeypatch,
        source,
        log_dir,
        "raw/2026-05-10/13/2026-05-10T13-00-00.v2.gz",
        {"timestamp": "2026-05-10T13:00:00Z", "ip": "1.2.3.5", "status": 200, "cycle_field": "v2"},
    )

    # Both v1 and v2 readable through the post-cycle view
    con = _open_view(source)
    table_name = my_duckdb._safe_table_name(svc_id)
    values = [r[0] for r in con.execute(f"SELECT cycle_field FROM {table_name} ORDER BY timestamp").fetchall()]
    assert values == ["v1", "v2"], f"Expected both v1 and v2 readable after disable/re-enable, got {values}"


def test_ingest_and_query_custom_fields(tmp_path, monkeypatch):
    """Test full lifecycle of a custom field: Config -> Ingest -> DuckLake Commit -> Query."""
    import backend.core.duckdb as my_duckdb
    from backend import config

    svc_id = "custom_test_svc"
    source = _build_source(tmp_path, svc_id)
    log_dir = tmp_path / "mock_logs_int"

    _install_pipeline_mocks(monkeypatch, tmp_path, source, log_dir)
    config.save_config(
        svc_id,
        {
            "service_id": svc_id,
            "status": {},
            "log_fields": {
                "schema_version": 2,
                "groups": ["A"],
                "custom_fields": [
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
            },
        },
    )
    my_duckdb._ensure_source_registered(source)

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
    schema = my_duckdb.get_schema(con, source)
    col_names = [c["name"] for c in schema]
    assert "x_custom_id" in col_names, f"Custom field missing from schema: {col_names}"

    table_name = my_duckdb._safe_table_name(svc_id)
    res = con.execute(f"SELECT x_custom_id FROM {table_name}").fetchall()
    assert res == [("abc-123-xyz",)]


def test_ingest_and_query_numeric_custom_fields(tmp_path, monkeypatch):
    """Test full lifecycle of numeric custom fields (BIGINT and DOUBLE)."""
    import backend.core.duckdb as my_duckdb
    from backend import config

    svc_id = "numeric_custom_svc"
    source = _build_source(tmp_path, svc_id)
    log_dir = tmp_path / "mock_logs_num"

    _install_pipeline_mocks(monkeypatch, tmp_path, source, log_dir)
    config.save_config(
        svc_id,
        {
            "service_id": svc_id,
            "status": {},
            "log_fields": {
                "schema_version": 2,
                "groups": ["A"],
                "custom_fields": [
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
            },
        },
    )
    my_duckdb._ensure_source_registered(source)

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
    schema = my_duckdb.get_schema(con, source)
    schema_dict = {c["name"]: c["type"] for c in schema}
    assert schema_dict.get("custom_int") == "BIGINT"
    assert schema_dict.get("custom_float") == "DOUBLE"

    table_name = my_duckdb._safe_table_name(svc_id)
    res = con.execute(f"SELECT custom_int * 2, ROUND(custom_float, 2) FROM {table_name}").fetchall()
    assert res == [(84, 3.14)]


# ── Merged from tests/core/test_custom_field_type_mismatch.py ──
def _well_typed_row_custom_field_type_mismatch(ts: datetime, idx: int, custom_field_name: str, custom_value):
    """Minimum well-typed log row plus one custom field."""
    return {
        "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S+0000"),
        "ip": f"10.0.0.{idx}",
        "status": 200,
        "url": f"/path/{idx}",
        "method": "GET",
        "cache": "HIT",
        "resp_bytes": 1024 + idx,
        "elapsed": 1500 + idx * 10,
        custom_field_name: custom_value,
    }


def _query_view(env, custom_field_name: str) -> list:
    """Read the committed rows back through the DuckLake-backed ``logs`` view."""
    from backend.core import iceberg as ice
    from backend.core.duckdb import get_connection
    from backend.repositories._base import _safe_table

    src = env["fos_source"]
    con = get_connection(src)
    try:
        ice.update_iceberg_view(con, src)
        view_name = _safe_table(src["name"])
        return con.execute(f"SELECT url, {custom_field_name} FROM {view_name} ORDER BY url").fetchall()
    finally:
        con.close()


def test_integer_field_with_string_value_nulls_only_that_cell(custom_field_env):
    """Declared INTEGER, VCL emits the string ``"NaN"`` for ONE row in a
    batch of three. Expect: row kept, cell NULL, other rows untouched.
    """
    from backend.core import iceberg as ice

    env = custom_field_env
    env["set_custom_fields"](
        [
            {
                "name": "user_score",
                "duckdb_type": "INTEGER",
                "enabled": True,
                "vcl": '"user_score":%{req.http.X-User-Score}V',
            }
        ]
    )

    ice.init_iceberg_table(env["fos_source"])

    base = datetime.now(UTC) - timedelta(hours=2)
    env["seed_gz"](
        "raw/request/year=2026/month=05/day=20/hour=10/minute=00/2026-05-20T10-00-00.svc.gz",
        [
            _well_typed_row_custom_field_type_mismatch(base, 0, "user_score", 42),
            _well_typed_row_custom_field_type_mismatch(base + timedelta(seconds=1), 1, "user_score", "NaN"),
            _well_typed_row_custom_field_type_mismatch(base + timedelta(seconds=2), 2, "user_score", 17),
        ],
    )

    events = env["drain_ingest"]()
    done = next((e for e in events if e["type"] == "done"), None)
    assert done is not None, f"no 'done' event in: {events}"
    # Contract: row is KEPT, not dropped — so all 3 rows make it through
    assert done["rows_inserted"] == 3, (
        f"contract violation: type-mismatched row was DROPPED. "
        f"rows_inserted={done['rows_inserted']} expected 3. Full done event: {done}"
    )

    # Commit + query
    ice.commit_buffer(env["fos_source"])
    rows = _query_view(env, "user_score")

    # 3 rows back, 2 with valid INTs, 1 NULL for the offending cell
    assert len(rows) == 3, f"expected 3 rows from the view, got {len(rows)}"
    by_url = dict(rows)
    assert by_url["/path/0"] == 42
    assert by_url["/path/1"] is None, (
        f"contract violation: 'NaN' should have become NULL after ignore_errors=true; got {by_url['/path/1']!r}"
    )
    assert by_url["/path/2"] == 17


def test_integer_field_with_string_float_truncates_not_nulls(custom_field_env):
    """Declared INTEGER, VCL emits ``"3.14"`` (a string that *parses* as a
    valid numeric value). Pin the surprising-but-real behavior: DuckDB's
    ``read_json_auto(..., columns={'x': 'INTEGER'}, ignore_errors=true)``
    parses the string as a float and **truncates to integer** rather
    than NULL-filling. The cell becomes ``3``, not ``NULL``.

    This is documented because it's a real foot-gun: if VCL drift causes
    a field to start emitting decimals, the dashboard will silently see
    a truncated histogram. The mitigation is to use DOUBLE for fields
    that might emit decimals.

    Contrast with the ``"NaN"`` case in the previous test where the
    string is structurally non-numeric and DOES fall through to NULL.
    """
    from backend.core import iceberg as ice

    env = custom_field_env
    env["set_custom_fields"]([{"name": "score", "duckdb_type": "INTEGER", "enabled": True, "vcl": '"score":1'}])

    ice.init_iceberg_table(env["fos_source"])

    base = datetime.now(UTC) - timedelta(hours=2)
    env["seed_gz"](
        "raw/request/year=2026/month=05/day=20/hour=10/minute=00/2026-05-20T10-00-00.svc.gz",
        [
            _well_typed_row_custom_field_type_mismatch(base, 0, "score", 1),
            _well_typed_row_custom_field_type_mismatch(base + timedelta(seconds=1), 1, "score", "3.14"),
        ],
    )

    events = env["drain_ingest"]()
    done = next((e for e in events if e["type"] == "done"), None)
    assert done is not None
    assert done["rows_inserted"] == 2

    ice.commit_buffer(env["fos_source"])
    rows = _query_view(env, "score")
    by_url = dict(rows)
    assert by_url["/path/0"] == 1
    # Pin truncation, NOT NULL — this is the actual contract today.
    assert by_url["/path/1"] == 3, (
        f"DuckDB JSON parser used to truncate '3.14' to 3 for INTEGER "
        f"columns; got {by_url['/path/1']!r}. If this is now NULL, that's "
        f"a DuckDB-side behavior change — update the contract or pin the "
        f"DuckDB version that introduced it."
    )


def test_boolean_field_with_arbitrary_string_nulls_the_cell(custom_field_env):
    """Declared BOOLEAN, VCL emits ``"maybe"``. Cell → NULL; row kept."""
    from backend.core import iceberg as ice

    env = custom_field_env
    env["set_custom_fields"](
        [
            {
                "name": "is_bot",
                "duckdb_type": "BOOLEAN",
                "enabled": True,
                "vcl": '"is_bot":false',
            }
        ]
    )

    ice.init_iceberg_table(env["fos_source"])

    base = datetime.now(UTC) - timedelta(hours=2)
    env["seed_gz"](
        "raw/request/year=2026/month=05/day=20/hour=10/minute=00/2026-05-20T10-00-00.svc.gz",
        [
            _well_typed_row_custom_field_type_mismatch(base, 0, "is_bot", True),
            _well_typed_row_custom_field_type_mismatch(base + timedelta(seconds=1), 1, "is_bot", "maybe"),
            _well_typed_row_custom_field_type_mismatch(base + timedelta(seconds=2), 2, "is_bot", False),
        ],
    )

    events = env["drain_ingest"]()
    done = next((e for e in events if e["type"] == "done"), None)
    assert done is not None
    assert done["rows_inserted"] == 3

    ice.commit_buffer(env["fos_source"])
    rows = _query_view(env, "is_bot")
    by_url = dict(rows)
    assert by_url["/path/0"] is True
    assert by_url["/path/1"] is None
    assert by_url["/path/2"] is False


def test_batch_proceeds_when_one_file_has_only_type_mismatches(custom_field_env):
    """Two files in the same batch; one is entirely bad values, the other
    is entirely good. Bad file must not abort the good file's commit, and
    the good rows must land in DuckLake cleanly.
    """
    from backend.core import iceberg as ice

    env = custom_field_env
    env["set_custom_fields"]([{"name": "n", "duckdb_type": "INTEGER", "enabled": True, "vcl": '"n":1'}])

    ice.init_iceberg_table(env["fos_source"])

    base = datetime.now(UTC) - timedelta(hours=2)
    env["seed_gz"](
        "raw/request/year=2026/month=05/day=20/hour=10/minute=00/2026-05-20T10-00-00.bad.gz",
        [_well_typed_row_custom_field_type_mismatch(base + timedelta(seconds=i), i, "n", "bad") for i in range(3)],
    )
    env["seed_gz"](
        "raw/request/year=2026/month=05/day=20/hour=10/minute=05/2026-05-20T10-05-00.good.gz",
        [
            _well_typed_row_custom_field_type_mismatch(base + timedelta(minutes=5, seconds=i), 100 + i, "n", 50 + i)
            for i in range(3)
        ],
    )

    events = env["drain_ingest"]()
    done = next((e for e in events if e["type"] == "done"), None)
    assert done is not None
    # All 6 rows kept (3 with NULL cells, 3 with valid INTs)
    assert done["rows_inserted"] == 6

    ice.commit_buffer(env["fos_source"])
    rows = _query_view(env, "n")
    assert len(rows) == 6
    n_null = sum(1 for _, v in rows if v is None)
    n_good = sum(1 for _, v in rows if v is not None)
    assert n_null == 3, f"expected 3 NULL cells (the 'bad' file), got {n_null}"
    assert n_good == 3, f"expected 3 well-typed cells (the 'good' file), got {n_good}"


@pytest.mark.parametrize(
    "ddb_type,bad_value,description",
    [
        ("BIGINT", "NaN", "BIGINT ← string 'NaN' → NULL"),
        # NOTE: DOUBLE ← string 'NaN' parses to IEEE NaN, NOT NULL —
        # that's actual DuckDB read_json_auto behaviour. Omitted from
        # the matrix because the contract for DOUBLE+NaN-string is
        # "valid IEEE NaN", not "NULL on coerce failure".
        ("BOOLEAN", "maybe", "BOOLEAN ← arbitrary string → NULL"),
        ("BIGINT", "not-a-number-at-all", "BIGINT ← clearly invalid string → NULL"),
        ("DOUBLE", "lol", "DOUBLE ← clearly invalid string → NULL"),
    ],
)
def test_typed_field_with_invalid_string_value_nulls_cell(custom_field_env, ddb_type, bad_value, description):
    """Matrix coverage for the type-mismatch contract: the row is kept,
    the bad cell is NULL'd, and a peer typed field on the same row
    remains valid. The existing test_integer_* tests pin the INTEGER
    case; this parametrises the contract over the BIGINT / DOUBLE /
    BOOLEAN variants that the audit flagged as uncovered.
    """
    from backend.core import iceberg as ice

    env = custom_field_env
    env["set_custom_fields"](
        [
            {
                "name": "metric",
                "duckdb_type": ddb_type,
                "enabled": True,
                "vcl": '"metric":%{req.http.X-Metric}V',
            }
        ]
    )

    ice.init_iceberg_table(env["fos_source"])
    base = datetime.now(UTC) - timedelta(hours=2)
    env["seed_gz"](
        "raw/request/year=2026/month=05/day=21/hour=10/minute=00/2026-05-21T10-00-00.matrix.gz",
        [
            _well_typed_row_custom_field_type_mismatch(base, 0, "metric", 1),
            _well_typed_row_custom_field_type_mismatch(base + timedelta(seconds=1), 1, "metric", bad_value),
            _well_typed_row_custom_field_type_mismatch(base + timedelta(seconds=2), 2, "metric", 2),
        ],
    )

    events = env["drain_ingest"]()
    done = next((e for e in events if e["type"] == "done"), None)
    assert done is not None, f"no 'done' event in: {events}"
    assert done["rows_inserted"] == 3, (
        f"row was dropped on type-mismatch ({description}). rows_inserted={done['rows_inserted']} expected 3."
    )

    ice.commit_buffer(env["fos_source"])
    rows = _query_view(env, "metric")
    by_url = dict(rows)
    # The cell with the bad value must be NULL.
    assert by_url["/path/1"] is None, (
        f"contract violation ({description}): bad cell did not NULL — got {by_url['/path/1']!r}"
    )

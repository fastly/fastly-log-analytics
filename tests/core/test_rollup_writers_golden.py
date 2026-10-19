"""Golden characterization of the rollup writers and day compactors.

Runs the real per-hour writers, the per-field/bundle pipeline and every
closed-day compactor against a deterministic synthetic log table, then pins:

* the exact set of files each phase leaves behind (relative names only),
* each parquet's schema, row count and a digest of its canonical rows,
* a digest of every SQL statement the phase sent to DuckDB (multiset, with
  path lists sorted so filesystem listing order cannot matter).

The snapshot in ``golden/rollup_writers.json`` was captured from the
pre-refactor writers, so a mechanical refactor of the COPY / weighted-average /
cleanup scaffolding must reproduce it byte-for-byte. Regenerate deliberately
with ``UPDATE_ROLLUP_GOLDEN=1`` and review the diff.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import patch

import duckdb
import pyarrow.parquet as pq
import pytest

from tests._helpers.locks import noop_lock as _noop_lock

pytestmark = pytest.mark.usefixtures("frozen_clock")

GOLDEN_PATH = Path(__file__).parent / "golden" / "rollup_writers.json"
SERVICE = "svc_golden"
TABLE = "logs_svc_golden"

_LOG_TABLE_SQL = f"""
CREATE TABLE {TABLE} AS
SELECT
  TIMESTAMPTZ '2026-06-13 00:00:00+00' + to_minutes(i::INTEGER) AS timestamp,
  '10.0.' || (i % 5) || '.' || (i % 7) AS ip,
  (['Mozilla/5.0', 'curl/7.68', 'Googlebot/2.1', 'python-requests'])[(i % 4) + 1] AS ua,
  '/p' || (i % 6) AS url,
  (['US', 'JP', 'DE'])[(i % 3) + 1] AS country,
  (['NYC', 'TYO', 'BER'])[(i % 3) + 1] AS city,
  (['NY', 'TK', 'BE'])[(i % 3) + 1] AS region,
  (['501', '392', '999'])[(i % 3) + 1] AS metro,
  40.0 + (i % 3) AS lat,
  -70.0 + (i % 3) AS lon,
  CAST(100 + (i % 3) AS BIGINT) AS asn,
  (['DEN', 'IAD', 'LAX'])[((i // 3) % 3) + 1] AS pop,
  (CASE i % 5 WHEN 0 THEN 200 WHEN 1 THEN 200 WHEN 2 THEN 404 WHEN 3 THEN 500 ELSE 304 END)::INTEGER AS status,
  (['HIT', 'MISS', 'PASS', 'HIT-STALE', 'MISS'])[(i % 5) + 1] AS cache,
  (['GET', 'POST'])[(i % 2) + 1] AS method,
  0.01 + (i % 10) * 0.01 AS ttfb,
  CAST(50000 + (i % 20) * 1000 AS DOUBLE) AS ottfb,
  CAST(80000 + (i % 25) * 1000 AS DOUBLE) AS ottlb,
  CAST(1000 + (i % 40) AS BIGINT) AS obytes,
  CAST(100 + (i % 30) AS DOUBLE) AS elapsed,
  CAST(10000 + (i % 50) * 100 AS BIGINT) AS tcp_rtt,
  CAST(5000 + (i % 7) * 100 AS BIGINT) AS rtt_min,
  CAST(100 + (i % 9) AS BIGINT) AS rtt_var,
  (i % 10) / 100.0 AS ploss,
  CAST(500 + (i % 100) AS BIGINT) AS req_bytes,
  CAST(1000 + (i % 100) AS BIGINT) AS resp_bytes,
  CAST(200 + (i % 30) AS BIGINT) AS req_header_bytes,
  CAST(1 + (i % 25) AS BIGINT) AS conn_requests,
  (['slow', 'med', 'fast'])[(i % 3) + 1] AS c_speed,
  (i % 4) <> 0 AS edge,
  '203.0.113.' || (i % 4) AS oip,
  (CASE i % 3 WHEN 0 THEN 200 WHEN 1 THEN 200 ELSE 500 END)::INTEGER AS ost,
  'ja4_' || (i % 5) AS ja4,
  CASE WHEN i % 7 = 0 THEN NULL ELSE 'sha' || (i % 3) END AS tls_ciphers_sha,
  CAST((i % 5) * 60 AS BIGINT) AS ttl,
  CASE WHEN i % 9 = 0 THEN 'VERIFIED-BOT.google,WAF.x' ELSE NULL END AS waf_sig,
  (i % 11 = 0) AS waf,
  (i % 13 = 0) AS waf_resp,
  (i % 2 = 0) AS is_shield,
  (i % 3 <> 0) AS is_ssl,
  (i % 8 = 0) AS ipv6,
  'HTTP/2' AS protocol,
  'sid' || (i % 12) AS edge_sid,
  'cmcd' || (i % 6) AS cmcd_sid,
  'req' || i AS waf_req_id
FROM range(0, 2880) AS t(i)
"""

_TMP_RE = re.compile(r"(\.tmp_[a-z0-9_]*?)[0-9a-f]{12}(\.parquet)")
_RANDOM_NAME_RE = re.compile(r"(compacted_|part-|data_)[0-9a-f]{12,32}")
_TMP_DIR_RE = re.compile(r"(rollups/tmp/[A-Za-z0-9_]+?)_[0-9a-f]{8}\b")
_PATHS_RE = re.compile(r"read_parquet\(\[([^\]]*)\]")


class _Recorder:
    """Forwards to a DuckDB connection while recording every statement."""

    def __init__(self, con: duckdb.DuckDBPyConnection, log: list[str], close: bool) -> None:
        self._con = con
        self._log = log
        self._close = close

    def execute(self, query: str, *args: Any, **kwargs: Any) -> Any:
        self._log.append(query)
        return self._con.execute(query, *args, **kwargs)

    def close(self) -> None:
        if self._close:
            self._con.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._con, name)


def _normalise_sql(q: str, root: str) -> str:
    q = q.replace(root, "<R>")
    q = _TMP_RE.sub(r"\1<id>\2", q)
    q = _RANDOM_NAME_RE.sub(r"\1<id>", q)
    q = _TMP_DIR_RE.sub(r"\1_<id>", q)

    def _sorted_paths(m: re.Match) -> str:
        parts = sorted(p.strip() for p in m.group(1).split(","))
        return "read_parquet([" + ", ".join(parts) + "]"

    return _PATHS_RE.sub(_sorted_paths, q)


_SHAPE_RE = re.compile(r"\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{2}:\d{2})?|-\d{2})?")


def _digest(text: str) -> str:
    return hashlib.sha1(text.encode()).hexdigest()[:12]


def _shape(q: str) -> str:
    """The statement with date/hour literals blanked, for readable diagnostics."""
    return _SHAPE_RE.sub("<date>", q)


SCHEMAS: dict[str, list[str]] = {}


def _canon(v: Any) -> Any:
    if isinstance(v, float):
        return round(v, 4)
    if isinstance(v, bytes):
        return v.hex()
    if isinstance(v, list):
        return [_canon(x) for x in v]
    return v


def _snapshot_tree(root: Path) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for p in sorted(root.rglob("*")):
        if not p.is_file():
            continue
        rel = _RANDOM_NAME_RE.sub(r"\1<id>", p.relative_to(root).as_posix())
        if rel.startswith("_scratch"):
            continue
        if p.suffix != ".parquet":
            out[rel] = {"bytes": p.stat().st_size}
            continue
        table = pq.ParquetFile(p).read()
        rows = sorted(json.dumps(_canon(list(r.values())), default=str) for r in table.to_pylist())
        schema = [f"{f.name}:{f.type}" for f in table.schema]
        SCHEMAS[_digest(json.dumps(schema))] = schema
        out[rel] = {
            "schema": _digest(json.dumps(schema)),
            "rows": len(rows),
            "sha": _digest("\n".join(rows)),
        }
    return out


def _writers() -> list[tuple[str, Callable[[str, dict, list[str]], int]]]:
    from backend.core.rollups import (
        network_health_geo,
        network_health_heatmap,
        network_quality,
        network_rtt,
        network_speed,
        network_summary,
        origin_dims,
        origin_latency_ts,
        origin_summary,
        overview,
        perf_dims,
        perf_latency,
        pop_health,
        security_dims,
        sessions,
        slow_urls,
        time_series,
        verified_bots_ts,
    )

    return [
        ("time_series", time_series.build_time_series_bundles),
        ("sessions", sessions.build_session_bundles),
        ("slow_urls", slow_urls.build_slow_urls_bundles),
        ("origin_summary", origin_summary.build_origin_summary_bundles),
        ("origin_dims", origin_dims.build_origin_dims_bundles),
        ("origin_latency_ts", origin_latency_ts.build_origin_latency_ts_bundles),
        ("network_rtt", network_rtt.build_network_rtt_bundles),
        ("network_speed", network_speed.build_network_speed_bundles),
        ("network_quality", network_quality.build_network_quality_bundles),
        ("network_summary", network_summary.build_network_summary_bundles),
        ("network_heatmap", network_health_heatmap.build_network_heatmap_bundles),
        ("network_geo", network_health_geo.build_network_geo_bundles),
        ("perf_dims", perf_dims.build_perf_dims_bundles),
        ("perf_latency", perf_latency.build_perf_latency_bundles),
        ("security_dims", security_dims.build_security_dims_bundles),
        ("overview", overview.build_overview_bundles),
        ("pop_health", pop_health.build_pop_health_bundles),
        ("verified_bots_ts", verified_bots_ts.build_verified_bots_ts_bundles),
    ]


def _compactors() -> list[tuple[str, Callable[..., int]]]:
    from backend.core.rollups import day_bundles as d

    return [
        ("origin_summary", d.compact_origin_summary_closed_days_to_daily),
        ("network_rtt", d.compact_network_rtt_closed_days_to_daily),
        ("network_speed", d.compact_network_speed_closed_days_to_daily),
        ("ngwaf_bots", d.compact_ngwaf_bots_closed_days_to_daily),
        ("verified_bots_ts", d.compact_verified_bots_ts_closed_days_to_daily),
        ("origin_latency_ts", d.compact_origin_latency_ts_closed_days_to_daily),
        ("perf_latency", d.compact_perf_latency_closed_days_to_daily),
        ("origin_dims", d.compact_origin_dims_closed_days_to_daily),
        ("security_dims", d.compact_security_dims_closed_days_to_daily),
        ("perf_dims", d.compact_perf_dims_closed_days_to_daily),
        ("overview", d.compact_overview_closed_days_to_daily),
        ("pop_health", d.compact_pop_health_closed_days_to_daily),
        ("network_quality", d.compact_network_quality_closed_days_to_daily),
    ]


_FIELDS = ["country", "url", "status", "pop", "ua", "cache"]


def _closed_hours() -> list[str]:
    return [f"2026-06-{d}-{h:02d}" for d in (13, 14) for h in range(24)]


def _run_pipeline(tmp_path: Path) -> tuple[dict[str, Any], dict[str, str]]:
    from backend.core import rollups
    from backend.core.rollups import day_bundles, hour_bundles

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    root = str(cache_root)
    log: list[str] = []

    shared = duckdb.connect(":memory:")
    shared.execute("PRAGMA threads=1")
    shared.execute(_LOG_TABLE_SQL)

    def _writer_con(*_a: Any, **_k: Any) -> _Recorder:
        return _Recorder(shared.cursor(), log, close=True)

    def _memory_con(*_a: Any, **_k: Any) -> _Recorder:
        con = duckdb.connect(":memory:")
        con.execute("PRAGMA threads=1")
        return _Recorder(con, log, close=True)

    source = {"name": SERVICE, "service_id": SERVICE}
    hours = _closed_hours()
    phases: dict[str, Any] = {}
    texts: dict[str, str] = {}
    seen: dict[str, dict] = {}

    def phase(name: str, fn: Callable[[], Any]) -> None:
        log.clear()
        returned = fn()
        tree = _snapshot_tree(cache_root)
        delta = {k: v for k, v in tree.items() if seen.get(k) != v}
        removed = sorted(k for k in seen if k not in tree)
        seen.clear()
        seen.update(tree)
        normalised = [_normalise_sql(q, root) for q in log]
        texts.update({_digest(q): _shape(q) for q in normalised})
        phases[name] = {
            "returned": returned,
            "changed": delta,
            "removed": removed,
            "sql": sorted(_digest(q) for q in normalised),
            "sql_count": len(log),
        }

    with (
        patch("backend.core.duckdb._cache_dir", return_value=root),
        patch("backend.core.duckdb.get_connection", side_effect=_writer_con),
        patch("backend.core.duckdb.get_memory_connection", side_effect=_memory_con),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch("backend.core.iceberg.execute_with_stale_view_retry", side_effect=lambda c, _s, fn: fn(c)),
    ):
        phase("per_field_copy", lambda: rollups.backfill_rollups(SERVICE, source, _FIELDS))
        phase("bundle_hours", lambda: hour_bundles.bundle_hours(SERVICE, source, hours))
        phase("compact_closed_days", lambda: rollups.compact_closed_days_to_daily(SERVICE, source))
        phase("bundle_days", lambda: day_bundles.bundle_days(SERVICE, source, ["2026-06-13", "2026-06-14"]))
        for name, writer in _writers():
            phase(f"hour_writer:{name}", lambda w=writer: w(SERVICE, source, hours))
        for name, compactor in _compactors():
            phase(f"day_compactor:{name}", lambda c=compactor: c(SERVICE, source))
    shared.close()
    return phases, texts


@pytest.mark.timeout(600)
def test_writers_match_golden(tmp_path: Path) -> None:
    phases, texts = _run_pipeline(tmp_path)
    got = json.loads(json.dumps(phases, sort_keys=True))

    if os.environ.get("UPDATE_ROLLUP_GOLDEN"):
        GOLDEN_PATH.parent.mkdir(parents=True, exist_ok=True)
        GOLDEN_PATH.write_text(
            json.dumps(
                {"phases": got, "schemas": SCHEMAS, "sql_shapes": sorted(set(texts.values()))}, indent=1, sort_keys=True
            )
            + "\n"
        )
        return

    golden = json.loads(GOLDEN_PATH.read_text())
    want = golden["phases"]
    assert got.keys() == want.keys()
    for name in want:
        if got[name] != want[name]:
            new = [h for h in got[name]["sql"] if h not in want[name]["sql"]]
            detail = "\n".join(f"new statement: {texts.get(h)}" for h in new[:3])
            raise AssertionError(f"phase {name!r} drifted from golden\n{detail}")

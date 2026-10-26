"""Consolidated tests for test_main."""

from __future__ import annotations

import asyncio
import contextvars
import json
import logging
import sqlite3
import threading
import time
from datetime import UTC, datetime, timedelta
from unittest.mock import ANY, MagicMock, patch

import duckdb
import httpx
import pytest
from backend.core.query_attribution import current_attribution
from backend.cron.decorators import global_job
from fastapi import FastAPI, HTTPException
from starlette.requests import Request

from backend import config as svcconfig
from backend import deps
from backend.core import duckdb_pool
from backend.core import rollups as rr
from backend.core.duckdb import DBBusyError
from backend.core.duckdb_pool import _Pool, _PoolBusy
from backend.core.query_instrumentation import InstrumentedDuckDBConnection
from backend.core.query_registry import _HISTORY_CAP, Attribution, QueryRegistry, query_registry
from backend.main import (
    _background_startup,
    _bounded_scheduler_shutdown,
    _ensure_persistent_view,
    _ensure_pop_cache,
    _ensure_scoring_matrix,
    _initialize_service,
    _maybe_instrument_asgi,
    health_check,
    telemetry_middleware,
)
from backend.repositories._base import QueryRunner
from backend.utils.remote_access import _new_request_id

# ── Shared Helpers ───────────────────────────────────────────────────────────


def _make_health_db(
    service_id: str = "svc",
    ingest_minutes_ago: int | None = 1,
    cron_rows: list[tuple] | None = None,
) -> sqlite3.Connection:
    now = datetime.now(UTC)
    con = sqlite3.connect(":memory:", check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("CREATE TABLE ingested_files (source_name TEXT, ingested_at TEXT)")
    con.execute(
        "CREATE TABLE cron_runs (id INTEGER PRIMARY KEY AUTOINCREMENT, task TEXT, "
        "status TEXT, started_at TEXT, error_message TEXT, "
        "files_downloaded INTEGER DEFAULT 0, rows_ingested INTEGER DEFAULT 0)"
    )
    if ingest_minutes_ago is not None:
        ts = (now - timedelta(minutes=ingest_minutes_ago)).strftime("%Y-%m-%d %H:%M:%S")
        con.execute("INSERT INTO ingested_files VALUES (?, ?)", (service_id, ts))
    for row in cron_rows or []:
        task, status, mins_ago, err = row[:4]
        ingest_count = row[4] if len(row) > 4 else 0
        started = (now - timedelta(minutes=mins_ago)).strftime("%Y-%m-%d %H:%M:%S")
        con.execute(
            "INSERT INTO cron_runs (task, status, started_at, error_message, rows_ingested) VALUES (?, ?, ?, ?, ?)",
            (task, status, started, err, ingest_count),
        )
    con.commit()
    return con


def _deep_health(
    cron_rows: list[tuple],
    *,
    ingest_minutes_ago: int = 1,
    service_id: str = "svc",
    is_remote: bool = False,
    durable: bool | None = None,
):
    """Drive health_check(deep=True) against an in-memory metadata.db."""
    con = _make_health_db(service_id=service_id, ingest_minutes_ago=ingest_minutes_ago, cron_rows=cron_rows)
    fake_request = MagicMock()
    with (
        patch("backend.config.list_service_ids", return_value=[service_id]),
        patch("backend.core.metadata.get_con", return_value=con),
        patch("backend.utils.remote_access.is_request_remote", return_value=is_remote),
    ):
        if durable is not None:
            with patch("backend.config.is_durable_serving_mode", return_value=durable):
                result = health_check(fake_request, deep=True)
        else:
            result = health_check(fake_request, deep=True)

    return (200, result) if isinstance(result, dict) else (result.status_code, json.loads(result.body))


def _call_get_service_id(service=None, sid=None, x_fastly_service_id=None, x_service_id=None) -> str | None:
    """Call get_service_id directly without going through FastAPI's DI."""
    request = MagicMock()
    request.path_params = {}
    return deps.get_service_id(
        request=request,
        service=service,
        sid=sid,
        x_fastly_service_id=x_fastly_service_id,
        x_service_id=x_service_id,
    )


def _run_init_service(
    cfg: dict,
    *,
    reap_ret: object = 0,
    src_ret: object = None,
):
    reap_kw = {"side_effect": reap_ret} if isinstance(reap_ret, Exception) else {"return_value": reap_ret}
    src_kw = {"side_effect": src_ret} if isinstance(src_ret, Exception) else {"return_value": src_ret}
    with (
        patch("backend.core.metadata.reap_running_cron_runs", **reap_kw) as mock_reap,
        patch("backend.core.duckdb.get_source_for_service", **src_kw) as mock_get_src,
        patch("backend.core.duckdb.refresh_config_status") as mock_refresh,
        patch("backend.main._ensure_persistent_view") as mock_ensure,
        patch("backend.routers.admin.compute_sync_status_cached"),
        patch("backend.core.metadata.usage_log_db.get_con"),
        patch("backend.main.logger.info") as mock_log_info,
    ):
        _initialize_service(cfg)
    return mock_reap, mock_get_src, mock_refresh, mock_ensure, mock_log_info


# ── _initialize_service ──────────────────────────────────────────────────────


def test_initialize_service_skips_when_no_service_id():
    mock_reap, mock_get_src, _, _, _ = _run_init_service({})
    mock_reap.assert_not_called()
    mock_get_src.assert_not_called()


def test_initialize_service_reaps_orphans_and_refreshes_status():
    fake_src = {"name": "svc-1", "service_id": "svc-1"}
    mock_reap, _, mock_refresh, mock_ensure, _ = _run_init_service(
        {"service_id": "svc-1"}, reap_ret=3, src_ret=fake_src
    )
    mock_reap.assert_called_once_with("svc-1")
    mock_refresh.assert_called_once_with("svc-1")
    mock_ensure.assert_called_once_with("svc-1", fake_src)


def test_initialize_service_logs_reaped_count_only_when_nonzero():
    _, _, _, _, mock_log_info = _run_init_service({"service_id": "svc-x"}, reap_ret=0, src_ret=None)
    assert [c for c in mock_log_info.call_args_list if "reaped" in str(c)] == []


def test_initialize_service_tolerates_reap_failure():
    _, _, mock_refresh, _, _ = _run_init_service(
        {"service_id": "svc-1"}, reap_ret=RuntimeError("locked"), src_ret={"name": "svc-1"}
    )
    mock_refresh.assert_called_once_with("svc-1")


def test_initialize_service_skips_status_refresh_when_no_source():
    _, _, mock_refresh, _, _ = _run_init_service({"service_id": "svc-1"}, reap_ret=0, src_ret=None)
    mock_refresh.assert_not_called()


def test_initialize_service_swallows_unexpected_outer_exception():
    _run_init_service({"service_id": "svc-1"}, src_ret=RuntimeError("unexpected"))


@pytest.mark.parametrize(
    ("service_id", "mode", "durable"),
    [
        ("svc-durable", "high_scale", True),
        ("svc-file", "standard", False),
    ],
)
def test_initialize_service_defers_or_skips_rollup_catchup(service_id: str, mode: str, durable: bool):
    rr.reset_rollup_coverage_ready()
    fake_src = {"service_id": service_id, "deployment_mode": mode}
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.core.duckdb.refresh_config_status"),
        patch("backend.main._ensure_persistent_view"),
        patch("backend.routers.admin.compute_sync_status_cached"),
        patch("backend.config.is_durable_serving_mode", return_value=durable),
        patch(
            "backend.core.rollups.recompute.backfill_missing_hour_bundles",
            side_effect=AssertionError("startup rollup catch-up must be deferred"),
        ) as mock_backfill,
    ):
        _initialize_service({"service_id": service_id})

    mock_backfill.assert_not_called()
    assert rr.rollup_coverage_ready(service_id) is False


# ── _ensure_persistent_view ──────────────────────────────────────────────────


def test_ensure_persistent_view_always_rebuilds_at_startup():
    fake_src = {"name": "svc-1"}
    writer_con = MagicMock()
    with (
        patch("backend.core.duckdb.get_connection", return_value=writer_con) as mock_get_conn,
        patch("backend.core.iceberg.update_iceberg_view") as mock_update,
    ):
        _ensure_persistent_view("svc-1", fake_src)

    assert mock_get_conn.call_count == 1
    assert mock_get_conn.call_args.kwargs.get("read_only") is False
    mock_update.assert_called_once_with(writer_con, fake_src)
    writer_con.close.assert_called_once()


def test_ensure_persistent_view_closes_writer_on_update_failure():
    writer_con = MagicMock()
    with (
        patch("backend.core.duckdb.get_connection", return_value=writer_con),
        patch("backend.core.iceberg.update_iceberg_view", side_effect=RuntimeError("catalog corrupt")),
    ):
        _ensure_persistent_view("svc-1", {"name": "svc-1"})
    writer_con.close.assert_called_once()


def test_ensure_persistent_view_swallows_outer_exceptions():
    with patch("backend.core.duckdb.get_connection", side_effect=RuntimeError("db locked")):
        _ensure_persistent_view("svc-1", {"name": "svc-1"})


# ── _ensure_pop_cache ────────────────────────────────────────────────────────


def test_ensure_pop_cache_skips_when_cache_already_exists(tmp_path):
    cache_file = tmp_path / "pop_cache.json"
    cache_file.write_text("[]")
    with (
        patch("backend.utils.pop_utils.CACHE_FILE", str(cache_file)),
        patch("backend.utils.pop_utils.fetch_pop_locations") as mock_fetch,
    ):
        _ensure_pop_cache()
    mock_fetch.assert_not_called()


@pytest.mark.parametrize(
    ("configs", "fetch_ret", "expected_key", "expect_warn"),
    [
        (
            [
                {"service_id": "a", "fastly_api_key": ""},
                {"service_id": "b", "fastly_api_key": "key-b"},
                {"service_id": "c", "fastly_api_key": "key-c"},
            ],
            True,
            "key-b",
            False,
        ),
        ([{"service_id": "a", "fastly_api_key": "key"}], False, "key", True),
        ([{"service_id": "a", "fastly_api_key": ""}], True, None, False),
    ],
)
def test_ensure_pop_cache_missing_cache_scenarios(
    tmp_path, configs: list[dict], fetch_ret: bool, expected_key: str | None, expect_warn: bool
):
    with (
        patch("backend.utils.pop_utils.CACHE_FILE", str(tmp_path / "missing.json")),
        patch("backend.config.list_configs", return_value=configs),
        patch("backend.utils.pop_utils.fetch_pop_locations", return_value=fetch_ret) as mock_fetch,
        patch("backend.main.logger.warning") as mock_warn,
    ):
        _ensure_pop_cache()

    if expected_key is None:
        mock_fetch.assert_not_called()
    else:
        mock_fetch.assert_called_once_with(expected_key)
    if expect_warn:
        assert any("failed" in str(c).lower() for c in mock_warn.call_args_list)


def test_ensure_pop_cache_swallows_unexpected_exceptions():
    with patch("backend.utils.pop_utils.CACHE_FILE", side_effect=RuntimeError("oops")):
        _ensure_pop_cache()


# ── _ensure_scoring_matrix ──────────────────────────────────────────────────


def _run_scoring_matrix(tmp_path, configs: list[dict], fetch_fn=None):
    fake_matrix_path = tmp_path / "matrix.json"
    fetch_kw = {"side_effect": fetch_fn} if fetch_fn is not None else {}
    with (
        patch("backend.provision.session_scoring_orchestrator._MATRIX_PATH", fake_matrix_path),
        patch("backend.config.list_configs", return_value=configs),
        patch("backend.state_sync.fetch_matrix_from_fos", **fetch_kw) as mock_fetch,
    ):
        _ensure_scoring_matrix()
    return fake_matrix_path, mock_fetch


def test_ensure_scoring_matrix_writes_tenant_scoped_paths_for_each_service(tmp_path):
    configs = [
        {"service_id": "svc-a", "scoring": {"enabled": True}},
        {"service_id": "svc-b", "scoring": {"enabled": True}},
        {"service_id": "svc-c", "scoring": {"enabled": False}},
    ]
    matrices = {"svc-a": {"version": "a-v1", "vocab_size": 10}, "svc-b": {"version": "b-v1", "vocab_size": 20}}
    fake_matrix_path, _ = _run_scoring_matrix(tmp_path, configs, fetch_fn=lambda sid: matrices.get(sid))

    assert (tmp_path / "matrix_svc-a.json").exists()
    assert (tmp_path / "matrix_svc-b.json").exists()
    assert not fake_matrix_path.exists(), "shared matrix.json must not be written"
    assert json.loads((tmp_path / "matrix_svc-a.json").read_text())["version"] == "a-v1"
    assert json.loads((tmp_path / "matrix_svc-b.json").read_text())["version"] == "b-v1"


def test_ensure_scoring_matrix_tolerates_per_service_failure(tmp_path):
    def fetch(sid):
        if sid == "svc-bad":
            raise RuntimeError("FOS unreachable")
        return {"version": "v1", "vocab_size": 5}

    configs = [
        {"service_id": "svc-bad", "scoring": {"enabled": True}},
        {"service_id": "svc-good", "scoring": {"enabled": True}},
    ]
    _run_scoring_matrix(tmp_path, configs, fetch_fn=fetch)
    assert (tmp_path / "matrix_svc-good.json").exists()
    assert not (tmp_path / "matrix_svc-bad.json").exists()


def test_ensure_scoring_matrix_skips_services_without_scoring_enabled(tmp_path):
    configs = [{"service_id": "svc-off", "scoring": {"enabled": False}}, {"service_id": "svc-none"}]
    _, mock_fetch = _run_scoring_matrix(tmp_path, configs)
    mock_fetch.assert_not_called()


# ── _background_startup ────────────────────────────────────────────────────


def test_background_startup_reloads_db_and_initialises_each_service():
    fake_scheduler = MagicMock()
    configs = [{"service_id": "a"}, {"service_id": "b"}]
    with (
        patch("backend.core.duckdb.reload_default_source"),
        patch("backend.main._ensure_pop_cache"),
        patch("backend.cron.scheduler.get_scheduler", return_value=fake_scheduler),
        patch("backend.config.list_configs", return_value=configs),
        patch("backend.main._initialize_service") as mock_init,
    ):
        _background_startup()

    fake_scheduler.start.assert_called_once()
    assert {call[0][0]["service_id"] for call in mock_init.call_args_list} == {"a", "b"}


def test_background_startup_tolerates_reload_and_scheduler_failures():
    fake_scheduler = MagicMock()
    with (
        patch("backend.core.duckdb.reload_default_source", side_effect=RuntimeError("S3 down")),
        patch("backend.main._ensure_pop_cache"),
        patch("backend.cron.scheduler.get_scheduler", return_value=fake_scheduler),
        patch("backend.config.list_configs", return_value=[]),
    ):
        _background_startup()
    fake_scheduler.start.assert_called_once()

    with (
        patch("backend.core.duckdb.reload_default_source"),
        patch("backend.main._ensure_pop_cache"),
        patch("backend.cron.scheduler.get_scheduler", side_effect=RuntimeError("scheduler oom")),
    ):
        _background_startup()


# ── /api/health ─────────────────────────────────────────────────────────────


def test_health_endpoint_returns_ok_status_without_service_id(client):
    resp = client.get("/api/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert "version" in body


def test_health_endpoint_deep_returns_shallow_for_remote_analyst():
    fake_request = MagicMock()
    with (
        patch("backend.config.list_service_ids", return_value=["secret-svc-1", "secret-svc-2"]),
        patch("backend.utils.remote_access.is_request_remote", return_value=True),
    ):
        result = health_check(fake_request, deep=True)

    assert isinstance(result, dict)
    assert result["status"] == "ok" and "version" in result
    assert "services" not in result, f"deep=1 from remote leaked services: {result.get('services')!r}"
    payload_str = str(result)
    assert "secret-svc-1" not in payload_str and "secret-svc-2" not in payload_str


def test_health_endpoint_deep_returns_full_payload_for_admin():
    code, result = _deep_health([], ingest_minutes_ago=0, service_id="admin-svc-1", is_remote=False)
    assert code == 200 and result["status"] == "ok"
    assert "services" in result, "admin caller must get the per-service report"
    assert any(s["service_id"] == "admin-svc-1" for s in result["services"])


def test_health_endpoint_deep(client):
    con = _make_health_db(service_id="test-svc-1", ingest_minutes_ago=None)
    with (
        patch("backend.config.list_service_ids", return_value=["test-svc-1"]),
        patch("backend.core.metadata.get_con", return_value=con),
    ):
        resp = client.get("/api/health?deep=1")
        assert resp.status_code == 200 and resp.json()["status"] == "ok"

        now_str = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
        con.execute("INSERT INTO ingested_files VALUES (?, ?)", ("test-svc-1", now_str))
        resp = client.get("/api/health?deep=1")
        assert resp.status_code == 200 and resp.json()["status"] == "ok"

        stale_str = (datetime.now(UTC) - timedelta(minutes=45)).strftime("%Y-%m-%d %H:%M:%S")
        con.execute("DELETE FROM ingested_files")
        con.execute("INSERT INTO ingested_files VALUES (?, ?)", ("test-svc-1", stale_str))
        resp = client.get("/api/health?deep=1")
        assert resp.status_code == 200 and resp.json()["status"] == "ok"
        assert resp.json()["services"][0]["status"] == "stale"


# ── telemetry middleware: cdn_service_id resolution ────────────────────────


def test_middleware_resolves_cdn_service_id_and_swallows_flush_exceptions(client):
    with (
        patch.object(svcconfig, "load_config", return_value=None),
        patch.object(svcconfig, "get_cdn_service_id_map", return_value={"cdn-svc-1": "log-svc-1"}),
        patch.object(svcconfig, "get_active_service_id", return_value=None),
        patch("backend.utils.telemetry.flush_usage_log") as mock_flush,
    ):
        client.get("/api/health", headers={"x-fastly-service-id": "cdn-svc-1"})
    if mock_flush.called:
        assert mock_flush.call_args[0][0] == "log-svc-1"

    with patch("backend.utils.telemetry.flush_usage_log", side_effect=RuntimeError("disk full")):
        assert client.get("/api/health").status_code == 200


# ── _bounded_scheduler_shutdown & _maybe_instrument_asgi ─────────────────────


def test_bounded_shutdown_fast_timeout_and_exception_paths():
    fake_scheduler = MagicMock()
    fake_scheduler.shutdown = MagicMock(return_value=None)
    t0 = time.monotonic()
    _bounded_scheduler_shutdown(fake_scheduler, timeout_secs=10.0)
    assert time.monotonic() - t0 < 0.5
    fake_scheduler.shutdown.assert_called_once_with(wait=True)

    never_returns = threading.Event()
    fake_scheduler.shutdown = lambda wait: never_returns.wait()  # noqa: ARG005
    t0 = time.monotonic()
    _bounded_scheduler_shutdown(fake_scheduler, timeout_secs=0.3)
    elapsed = time.monotonic() - t0
    assert 0.25 <= elapsed < 0.6, f"bounded wait released at {elapsed:.2f}s, expected ~0.3s"
    never_returns.set()

    fake_scheduler.shutdown = MagicMock(side_effect=RuntimeError("apscheduler internal"))
    _bounded_scheduler_shutdown(fake_scheduler, timeout_secs=5.0)


@pytest.mark.parametrize("enabled", [False, True])
def test_maybe_instrument_asgi(enabled: bool):
    fake_app = MagicMock()
    with (
        patch("backend.main.request_telemetry.asgi_instrumentation_enabled", return_value=enabled),
        patch("backend.main.request_telemetry.ensure_initialized") as mock_ensure_init,
        patch("backend.main.FastAPIInstrumentor") as mock_instrumentor,
    ):
        _maybe_instrument_asgi(fake_app)

    if enabled:
        mock_ensure_init.assert_called_once()
        mock_instrumentor.instrument_app.assert_called_once_with(fake_app, exclude_spans=["receive", "send"])
    else:
        mock_instrumentor.instrument_app.assert_not_called()


# ── Merged from test_deps.py ─────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({"sid": "abc"}, "abc"),
        (
            {
                "service": "from-query",
                "sid": "from-sid",
                "x_fastly_service_id": "from-fastly-header",
                "x_service_id": "from-svc-header",
            },
            "from-query",
        ),
        ({"x_fastly_service_id": "hdr"}, "hdr"),
        ({"x_service_id": "old"}, "old"),
    ],
)
def test_get_service_id_resolution_priority(kwargs: dict, expected: str):
    with patch("backend.deps.svcconfig.load_config", return_value={"service_id": expected}):
        assert _call_get_service_id(**kwargs) == expected


@pytest.mark.parametrize(
    ("sid", "cdn_map", "expected"),
    [
        ("cdn-svc-1", {"cdn-svc-1": "log-svc-1"}, "log-svc-1"),
        ("ghost", {}, "ghost"),
    ],
)
def test_get_service_id_cdn_fallback(sid: str, cdn_map: dict, expected: str):
    with (
        patch("backend.deps.svcconfig.load_config", return_value=None),
        patch("backend.deps.svcconfig.get_cdn_service_id_map", return_value=cdn_map),
    ):
        assert _call_get_service_id(sid=sid) == expected


@pytest.mark.parametrize("active_id", ["active-svc", None])
def test_get_service_id_falls_back_to_active_when_nothing_provided(active_id: str | None):
    with patch("backend.deps.svcconfig.get_active_service_id", return_value=active_id):
        assert _call_get_service_id() == active_id


# ── get_source: source lookup + 400 on miss ──────────────────────────────────


def test_get_source_returns_source_dict_when_found():
    fake_src = {"name": "logs_svc", "service_id": "svc"}
    with patch("backend.deps.db.get_source_for_service", return_value=fake_src):
        assert deps.get_source(service_id="svc") is fake_src


@pytest.mark.parametrize("service_id", [None, "ghost"])
def test_get_source_raises_400_when_missing(service_id: str | None):
    with patch("backend.deps.db.get_source_for_service", return_value=None):
        with pytest.raises(HTTPException) as exc:
            deps.get_source(service_id=service_id)
    assert exc.value.status_code == 400
    assert exc.value.detail["no_service"] is True


# ── _ConnectionHolder: enter / DBBusyError / exit ────────────────────────────


@pytest.fixture
def disable_pool(monkeypatch):
    monkeypatch.setenv("DUCKDB_CONNECTION_POOL", "0")
    yield


def test_connection_holder_opens_and_closes(disable_pool):
    fake_con = MagicMock()
    src = {"name": "x"}
    with patch("backend.deps.get_connection", return_value=fake_con) as mock_get:
        holder = deps._ConnectionHolder(src, read_only=True)
        assert holder.__enter__() is fake_con
        mock_get.assert_called_once_with(source=src, max_wait=ANY, skip_view_update=False, read_only=True)
        holder.__exit__(None, None, None)
        fake_con.close.assert_called_once()


def test_connection_holder_skip_view_update_forwarded():
    with patch("backend.deps.get_connection") as mock_get:
        deps._ConnectionHolder({"name": "x"}, skip_view_update=True).__enter__()
        assert mock_get.call_args.kwargs["skip_view_update"] is True


@pytest.mark.parametrize(("env_val", "expected_wait"), [(None, 10.0), ("9.5", 9.5)])
def test_connection_holder_max_wait(disable_pool, monkeypatch, env_val: str | None, expected_wait: float):
    if env_val is None:
        monkeypatch.delenv("DUCKDB_POOL_MAX_WAIT_S", raising=False)
    else:
        monkeypatch.setenv("DUCKDB_POOL_MAX_WAIT_S", env_val)
    with patch("backend.deps.get_connection", return_value=MagicMock()) as mock_get:
        deps._ConnectionHolder({"name": "x"}, read_only=True).__enter__()
        assert mock_get.call_args.kwargs["max_wait"] == expected_wait


def test_connection_holder_db_busy_and_exit_edge_cases(disable_pool):
    with patch("backend.deps.get_connection", side_effect=DBBusyError("locked")):
        with pytest.raises(HTTPException) as exc:
            deps._ConnectionHolder({"name": "x"}).__enter__()
    assert exc.value.status_code == 503 and exc.value.detail["busy"] is True

    fake_con = MagicMock()
    fake_con.close.side_effect = RuntimeError("already closed")
    with patch("backend.deps.get_connection", return_value=fake_con):
        holder = deps._ConnectionHolder({"name": "x"})
        holder.__enter__()
        holder.__exit__(None, None, None)

    deps._ConnectionHolder({"name": "x"}).__exit__(None, None, None)


# ── get_con & removed symbols ────────────────────────────────────────────────


def test_get_con_yields_connection_and_closes_after(disable_pool):
    fake_con = MagicMock()
    with patch("backend.deps.get_connection", return_value=fake_con) as mock_get:
        gen = deps.get_con(source={"name": "x"})
        assert next(gen) is fake_con
        assert mock_get.call_args.kwargs["read_only"] is True
        with pytest.raises(StopIteration):
            next(gen)
        fake_con.close.assert_called_once()


@pytest.mark.parametrize("symbol", ["get_meta_con", "AnalyticsDeps"])
def test_removed_deps_symbols(symbol: str):
    assert not hasattr(deps, symbol), f"{symbol} was removed at the v2.0 cut"


# ── _ConnectionHolder: pool path ─────────────────────────────────────────────


def test_connection_holder_pool_path_checkout_return_and_error():
    fake_con = MagicMock()
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=fake_con)
    cm.__exit__ = MagicMock(return_value=False)

    with patch.object(duckdb_pool, "checkout_connection", return_value=cm) as mock_checkout:
        holder = deps._ConnectionHolder({"name": "x"}, read_only=True)
        assert holder.__enter__() is fake_con
        mock_checkout.assert_called_once()
        holder.__exit__(None, None, None)
        cm.__exit__.assert_called_once()
        fake_con.close.assert_not_called()

        cm.reset_mock()
        holder2 = deps._ConnectionHolder({"name": "x"}, read_only=True)
        holder2.__enter__()
        holder2.__exit__(RuntimeError, RuntimeError("boom"), None)
        assert cm.__exit__.call_args[0][0] is RuntimeError


def test_connection_holder_pool_path_skipped_when_skip_view_update():
    with (
        patch.object(duckdb_pool, "checkout_connection") as mock_checkout,
        patch("backend.deps.get_connection", return_value=MagicMock()) as mock_get,
    ):
        deps._ConnectionHolder({"name": "x"}, skip_view_update=True).__enter__()
        mock_checkout.assert_not_called()
        mock_get.assert_called_once()


# ── Merged from test_sre_observability.py ────────────────────────────────────


def test_admin_access_log_carries_rid_and_duration(client):
    captured: list[str] = []
    h = logging.Handler()
    h.emit = lambda record: captured.append(record.getMessage())  # type: ignore[method-assign]
    lg = logging.getLogger("backend.access.admin")
    lg.addHandler(h)
    old = lg.level
    lg.setLevel(logging.INFO)
    try:
        r = client.get("/api/health")
    finally:
        lg.removeHandler(h)
        lg.setLevel(old)

    assert r.status_code == 200
    assert any("rid=" in m and "ms)" in m for m in captured), captured
    assert not any("rid=- " in m for m in captured), captured


def test_new_request_id_is_nonempty_hex():
    rid = _new_request_id()
    assert isinstance(rid, str) and len(rid) == 16
    int(rid, 16)
    assert _new_request_id() != rid


def test_global_job_runs_in_cron_attribution_scope(monkeypatch):
    import backend.cron.decorators as sj

    monkeypatch.setattr(sj, "record_job_run", lambda *a, **k: None)
    seen: dict = {}

    @global_job("share_audit_purge", color="35", tag="purge", label="Purge")
    def _job():
        attr = current_attribution.get()
        seen["kind"] = attr.kind if attr else None
        seen["job"] = attr.cron_job if attr else None
        return "ok"

    _job()
    assert seen == {"kind": "cron", "job": "share_audit_purge"}
    assert current_attribution.get() is None


def test_pool_stats_and_reject_counters():
    p = _Pool("svc-sre12", max_size=2)
    s = p.stats()
    assert s["saturated_rejects_total"] == 0 and s["drain_rejects_total"] == 0
    assert "last_warmed_at" in s and s["last_warmed_at"] is None

    p_sat = _Pool("svc-sat", max_size=1)
    p_sat._in_use = 1
    with pytest.raises(_PoolBusy):
        p_sat.acquire({"name": "svc-sat"}, max_wait=0)
    assert p_sat.stats()["saturated_rejects_total"] == 1 and p_sat.stats()["drain_rejects_total"] == 0

    p_drain = _Pool("svc-drain", max_size=2)
    p_drain.begin_drain()
    with pytest.raises(_PoolBusy):
        p_drain.acquire({"name": "svc-drain"}, max_wait=0)
    assert p_drain.stats()["drain_rejects_total"] == 1 and p_drain.stats()["saturated_rejects_total"] == 0


def test_history_cap_is_400():
    assert _HISTORY_CAP == 400
    assert QueryRegistry()._history.maxlen == 400


# ── SRE-04 / SRE-05 / SRE-22: deep /api/health ──────────────────────────────


@pytest.mark.parametrize(
    ("cron_rows", "expected_code", "expected_status", "reason_substr"),
    [
        ([("sync", "success", 2, None), ("commit", "success", 3, None)], 200, "ok", None),
        ([("sync", "success", 120, None), ("sync", "running", 20, None)], 503, "degraded", "stuck running"),
        ([("sync", "success", 30, None), ("sync", "running", 2, None)], 200, "ok", None),
        ([("sync", "success", 2, None), ("commit", "error", 1, "boom")], 503, "degraded", "commit cron errored"),
        ([("metadata_sync", "error", 1, "fos down")], 503, "degraded", "metadata_sync cron errored"),
    ],
)
def test_deep_health_cron_modes(
    cron_rows: list[tuple],
    expected_code: int,
    expected_status: str,
    reason_substr: str | None,
):
    code, body = _deep_health(cron_rows)
    assert code == expected_code
    if expected_code == 200:
        assert body["status"] == expected_status
    else:
        svc = body["services"][0]
        assert svc["status"] == expected_status
        if reason_substr:
            assert reason_substr in svc["reason"]


def test_deep_health_ignores_metadata_sync_error_in_durable_mode():
    code, result = _deep_health(
        [("metadata_sync", "error", 0, "old topology"), ("sync", "success", 0, None)],
        durable=True,
    )
    assert code == 200 and result["status"] == "ok"


@pytest.mark.parametrize(
    ("quiet_mins", "expected_status", "reason_substr"),
    [
        (35, "ok", None),
        (200, "stale", "no ingest since"),
    ],
)
def test_deep_health_adaptive_staleness(quiet_mins: int, expected_status: str, reason_substr: str | None):
    history = [("sync", "success", quiet_mins + i * 40, None, 5) for i in range(12)]
    code, body = _deep_health(history, ingest_minutes_ago=quiet_mins)
    assert code == 200
    svc = body["services"][0]
    assert svc["status"] == expected_status and svc["stale_minutes_used"] == 80
    if reason_substr:
        assert reason_substr in svc["reason"]


# ── SRE-03 / 06 / 11 / 13 / 20: health-snapshot additions ────────────────────


def test_health_snapshot_has_new_observability_fields(client, monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER", raising=False)
    monkeypatch.delenv("STRUCTLOG_FORMAT", raising=False)
    body = client.get("/api/admin/health-snapshot").json()
    for key in ("scheduler_last_tick_age_s", "recent_cron_failures", "observability", "config_backup"):
        assert key in body, f"{key} missing from health-snapshot"
    assert body["observability"] == {"log_format": "console", "otel_exporter": "none"}


def test_health_snapshot_scheduler_tick_age_surfaced(client):
    with patch("backend.core.metric_snapshots.last_snapshot_age_s", return_value=12.5):
        body = client.get("/api/admin/health-snapshot").json()
    assert body["scheduler_last_tick_age_s"] == 12.5


def test_health_snapshot_recent_cron_failures_aggregates_errors(client):
    latest = {
        "sync": {"status": "success", "started_at": "2026-06-20T10:00:00Z", "error_message": None},
        "commit": {"status": "error", "started_at": "2026-06-20T10:05:00Z", "error_message": "boom"},
    }
    with (
        patch("backend.config.list_service_ids", return_value=["svc-a"]),
        patch("backend.core.metadata.cron_log.latest_cron_per_task", return_value=latest),
    ):
        body = client.get("/api/admin/health-snapshot").json()

    fails = body["recent_cron_failures"]
    assert len(fails) == 1
    assert fails[0]["service_id"] == "svc-a" and fails[0]["task"] == "commit" and fails[0]["status"] == "error"


def test_health_snapshot_config_backup_marker_present_and_absent(client, tmp_path, monkeypatch):
    marker = tmp_path / "last_config_backup.json"
    at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    marker.write_text(json.dumps({"at": at, "remote": "gs://example/configs.tar.gz"}))
    monkeypatch.setenv("CONFIG_BACKUP_MARKER", str(marker))

    cb = client.get("/api/admin/health-snapshot").json()["config_backup"]
    assert cb is not None and cb["last_backup_at"] == at
    assert cb["age_s"] is not None and cb["age_s"] >= 0
    assert cb["source"] == "gs://example/configs.tar.gz"

    monkeypatch.setenv("CONFIG_BACKUP_MARKER", str(tmp_path / "does-not-exist.json"))
    assert client.get("/api/admin/health-snapshot").json()["config_backup"] is None


def test_health_snapshot_fos_probe_opt_in(client):
    assert client.get("/api/admin/health-snapshot").json().get("fos") is None
    with (
        patch("backend.config.list_configs", return_value=[{"service_id": "svc-a", "name": "svc-a"}]),
        patch("backend.config.config_to_source", side_effect=lambda c: c),
        patch("backend.core.duckdb.fos_reachable", return_value={"reachable": True, "error": None}),
    ):
        body = client.get("/api/admin/health-snapshot?probe_fos=1").json()
    assert body["fos"]["svc-a"]["reachable"] is True


# ── Merged from test_client_disconnect_cancels_queries.py ────────────────────


class _Conn:
    def __init__(self) -> None:
        self.interrupts = 0

    def interrupt(self) -> None:
        self.interrupts += 1


class _Resp:
    def __init__(self, status_code: int = 200) -> None:
        self.status_code = status_code


def _request(path: str = "/api/dashboard/bundle", receive=None) -> Request:
    scope = {
        "type": "http",
        "method": "POST",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "headers": [],
        "client": ("203.0.113.9", 4242),
        "server": ("testserver", 80),
        "scheme": "http",
        "state": {},
    }
    return Request(scope, receive=receive) if receive is not None else Request(scope)


@pytest.fixture(autouse=True)
def _clean_registry():
    before = dict(query_registry._queries)
    before_keys = list(query_registry._cancelled_keys)
    yield
    query_registry._queries.clear()
    query_registry._queries.update(before)
    query_registry._cancelled_keys.clear()
    query_registry._cancelled_keys.extend(before_keys)


def test_a_cancelled_request_interrupts_the_queries_it_started():
    conn = _Conn()

    async def call_next(request):
        query_registry.register("DuckDB", "SELECT slow()", service_id="svc", con=conn, pool_slot="svc#1")
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(telemetry_middleware(_request(), call_next))
    assert conn.interrupts == 1


def test_a_request_that_completes_normally_interrupts_nothing():
    conn = _Conn()

    async def call_next(request):
        query_registry.register("DuckDB", "SELECT 1", service_id="svc", con=conn, pool_slot="svc#1")
        return _Resp(200)

    asyncio.run(telemetry_middleware(_request(), call_next))
    assert conn.interrupts == 0


def test_a_cancelled_request_never_interrupts_another_requests_queries():
    mine, theirs = _Conn(), _Conn()
    other_registered: list[int] = []

    async def other_request_query():
        async def inner(request):
            qid = query_registry.register("DuckDB", "SELECT other()", service_id="svc", con=theirs, pool_slot="svc#2")
            other_registered.append(qid)
            return _Resp(200)

        await telemetry_middleware(_request("/api/network-health"), inner)

    async def scenario():
        await other_request_query()

        async def call_next(request):
            query_registry.register("DuckDB", "SELECT mine()", service_id="svc", con=mine, pool_slot="svc#1")
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await telemetry_middleware(_request(), call_next)

    asyncio.run(scenario())
    assert mine.interrupts == 1 and theirs.interrupts == 0


def test_a_real_duckdb_query_is_killed_when_its_request_is_abandoned():
    app = FastAPI()
    app.middleware("http")(telemetry_middleware)
    outcome: dict[str, object] = {}
    started = threading.Event()
    finished = threading.Event()

    @app.get("/slow")
    async def slow():
        raw = duckdb.connect(":memory:")
        con = InstrumentedDuckDBConnection(raw, service_id="svc")
        safety = threading.Timer(15, raw.interrupt)
        safety.daemon = True
        safety.start()

        def run() -> None:
            started.set()
            t0 = time.monotonic()
            try:
                con.execute("SELECT count(*) FROM range(100000000000) a, range(100000000000) b").fetchall()
                outcome["result"] = "finished"
            except Exception as exc:  # noqa: BLE001
                outcome["result"] = type(exc).__name__
            finally:
                outcome["seconds"] = time.monotonic() - t0
                safety.cancel()
                finished.set()

        asyncio.get_running_loop().run_in_executor(None, contextvars.copy_context().run, run)
        await asyncio.sleep(3600)

    async def scenario() -> None:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            request_task = asyncio.create_task(client.get("/slow"))
            deadline = time.monotonic() + 10
            while not started.is_set() and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
            await asyncio.sleep(0.3)
            request_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await request_task
            await asyncio.get_running_loop().run_in_executor(None, finished.wait, 12)

    asyncio.run(scenario())
    assert finished.is_set(), "the orphaned query must be interrupted, not left to run to completion"
    assert outcome["result"] == "InterruptException"
    assert outcome["seconds"] < 8  # type: ignore[operator]


def test_tcp_disconnect_via_asgi_receive_interrupts_sync_duckdb_query():
    conn = _Conn()
    disconnected = False

    async def receive():
        nonlocal disconnected
        if not disconnected:
            await asyncio.sleep(0.05)
        return {"type": "http.disconnect"}

    req = _request("/api/network-health", receive=receive)
    req._stream_consumed = True  # type: ignore[attr-defined]

    async def call_next(request):
        query_registry.register("DuckDB", "SELECT slow_post()", service_id="svc", con=conn, pool_slot="svc#1")
        nonlocal disconnected
        disconnected = True
        for _ in range(20):
            if conn.interrupts > 0:
                break
            await asyncio.sleep(0.05)
        return _Resp(499)

    asyncio.run(telemetry_middleware(req, call_next))
    assert conn.interrupts == 1


def test_cancelled_key_blocks_subsequent_queries_but_allows_drop_table_cleanup():
    raw = duckdb.connect(":memory:")
    con = InstrumentedDuckDBConnection(raw, service_id="svc")
    con.execute("CREATE TEMP TABLE tmp_cleanup AS SELECT 1 AS x")

    token = current_attribution.set(
        Attribution(
            kind="admin", admin_id="local", request_path="/api/security/aggregates", cancel_key="ck-multi-query"
        )
    )
    try:
        query_registry.cancel_for_key("ck-multi-query")
        with pytest.raises(duckdb.InterruptException):
            con.execute("SELECT * FROM tmp_cleanup").fetchall()
        con.execute('DROP TABLE IF EXISTS "tmp_cleanup"')
    finally:
        current_attribution.reset(token)
        raw.close()


def test_query_runner_lazy_con_factory_defers_connection_checkout():
    calls = 0
    raw = duckdb.connect(":memory:")

    def factory():
        nonlocal calls
        calls += 1
        return raw

    try:
        runner = QueryRunner(factory, {"name": "test_svc"})
        assert calls == 0, "con_factory must not be called on QueryRunner construction"
        res = runner.try_network_rtt_from_rollup(
            "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z", top_asns=[15169], has_filters=True
        )
        assert res is None and calls == 0
        assert runner.execute("SELECT 42").fetchone()[0] == 42 and calls == 1
        assert runner.execute("SELECT 43").fetchone()[0] == 43 and calls == 1
    finally:
        raw.close()

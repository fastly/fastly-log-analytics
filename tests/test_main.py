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
from fastapi import FastAPI, HTTPException
from starlette.requests import Request

import backend.core.request_context as deps
from backend import config as svcconfig
from backend.core import duckdb_pool
from backend.core import rollups as rr
from backend.core.duckdb import DBBusyError
from backend.core.duckdb_pool import _Pool, _PoolBusy
from backend.core.request_telemetry import (
    _HISTORY_CAP,
    Attribution,
    InstrumentedDuckDBConnection,
    QueryRegistry,
    current_attribution,
    query_registry,
)
from backend.cron.scheduler import global_job
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
        patch("backend.core.metadata.usage_log.get_con"),
        patch("backend.main.logger.info") as mock_log_info,
    ):
        _initialize_service(cfg)
    return mock_reap, mock_get_src, mock_refresh, mock_ensure, mock_log_info


# ── _initialize_service ──────────────────────────────────────────────────────


@pytest.mark.usefixtures("_clean_registry")
def test_initialize_service_skips_when_no_service_id():
    mock_reap, mock_get_src, _, _, _ = _run_init_service({})
    mock_reap.assert_not_called()
    mock_get_src.assert_not_called()


@pytest.mark.usefixtures("_clean_registry")
def test_initialize_service_reaps_orphans_and_refreshes_status():
    fake_src = {"name": "svc-1", "service_id": "svc-1"}
    mock_reap, _, mock_refresh, mock_ensure, _ = _run_init_service(
        {"service_id": "svc-1"}, reap_ret=3, src_ret=fake_src
    )
    mock_reap.assert_called_once_with("svc-1")
    mock_refresh.assert_called_once_with("svc-1")
    mock_ensure.assert_called_once_with("svc-1", fake_src)


@pytest.mark.usefixtures("_clean_registry")
def test_initialize_service_logs_reaped_count_only_when_nonzero():
    _, _, _, _, mock_log_info = _run_init_service({"service_id": "svc-x"}, reap_ret=0, src_ret=None)
    assert [c for c in mock_log_info.call_args_list if "reaped" in str(c)] == []


@pytest.mark.usefixtures("_clean_registry")
def test_initialize_service_tolerates_reap_failure():
    _, _, mock_refresh, _, _ = _run_init_service(
        {"service_id": "svc-1"}, reap_ret=RuntimeError("locked"), src_ret={"name": "svc-1"}
    )
    mock_refresh.assert_called_once_with("svc-1")


@pytest.mark.usefixtures("_clean_registry")
def test_initialize_service_skips_status_refresh_when_no_source():
    _, _, mock_refresh, _, _ = _run_init_service({"service_id": "svc-1"}, reap_ret=0, src_ret=None)
    mock_refresh.assert_not_called()


@pytest.mark.usefixtures("_clean_registry")
def test_initialize_service_swallows_unexpected_outer_exception():
    _run_init_service({"service_id": "svc-1"}, src_ret=RuntimeError("unexpected"))


@pytest.mark.usefixtures("_clean_registry")
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


@pytest.mark.usefixtures("_clean_registry")
def test_ensure_persistent_view_always_rebuilds_at_startup():
    fake_src = {"name": "svc-1"}
    writer_con = MagicMock()
    with (
        patch("backend.core.duckdb.get_connection", return_value=writer_con) as mock_get_conn,
        patch("backend.core.storage.update_storage_view") as mock_update,
    ):
        _ensure_persistent_view("svc-1", fake_src)

    assert mock_get_conn.call_count == 1
    assert mock_get_conn.call_args.kwargs.get("read_only") is False
    mock_update.assert_called_once_with(writer_con, fake_src)
    writer_con.close.assert_called_once()


@pytest.mark.usefixtures("_clean_registry")
def test_ensure_persistent_view_closes_writer_on_update_failure():
    writer_con = MagicMock()
    with (
        patch("backend.core.duckdb.get_connection", return_value=writer_con),
        patch("backend.core.storage.update_storage_view", side_effect=RuntimeError("catalog corrupt")),
    ):
        _ensure_persistent_view("svc-1", {"name": "svc-1"})
    writer_con.close.assert_called_once()


@pytest.mark.usefixtures("_clean_registry")
def test_ensure_persistent_view_swallows_outer_exceptions():
    with patch("backend.core.duckdb.get_connection", side_effect=RuntimeError("db locked")):
        _ensure_persistent_view("svc-1", {"name": "svc-1"})


# ── _ensure_pop_cache ────────────────────────────────────────────────────────


@pytest.mark.usefixtures("_clean_registry")
def test_ensure_pop_cache_skips_when_cache_already_exists(tmp_path):
    cache_file = tmp_path / "pop_cache.json"
    cache_file.write_text("[]")
    with (
        patch("backend.utils.pop_utils.CACHE_FILE", str(cache_file)),
        patch("backend.utils.pop_utils.fetch_pop_locations") as mock_fetch,
    ):
        _ensure_pop_cache()
    mock_fetch.assert_not_called()


@pytest.mark.usefixtures("_clean_registry")
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


@pytest.mark.usefixtures("_clean_registry")
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
        patch("backend.core.metadata.base.fetch_matrix_from_fos", **fetch_kw) as mock_fetch,
    ):
        _ensure_scoring_matrix()
    return fake_matrix_path, mock_fetch


@pytest.mark.usefixtures("_clean_registry")
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


@pytest.mark.usefixtures("_clean_registry")
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


@pytest.mark.usefixtures("_clean_registry")
def test_ensure_scoring_matrix_skips_services_without_scoring_enabled(tmp_path):
    configs = [{"service_id": "svc-off", "scoring": {"enabled": False}}, {"service_id": "svc-none"}]
    _, mock_fetch = _run_scoring_matrix(tmp_path, configs)
    mock_fetch.assert_not_called()


# ── _background_startup ────────────────────────────────────────────────────


@pytest.mark.usefixtures("_clean_registry")
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


@pytest.mark.usefixtures("_clean_registry")
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


@pytest.mark.usefixtures("_clean_registry")
def test_health_endpoint_returns_ok_status_without_service_id(client):
    resp = client.get("/api/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert "version" in body


@pytest.mark.usefixtures("_clean_registry")
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


@pytest.mark.usefixtures("_clean_registry")
def test_health_endpoint_deep_returns_full_payload_for_admin():
    code, result = _deep_health([], ingest_minutes_ago=0, service_id="admin-svc-1", is_remote=False)
    assert code == 200 and result["status"] == "ok"
    assert "services" in result, "admin caller must get the per-service report"
    assert any(s["service_id"] == "admin-svc-1" for s in result["services"])


@pytest.mark.usefixtures("_clean_registry")
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


@pytest.mark.usefixtures("_clean_registry")
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


@pytest.mark.usefixtures("_clean_registry")
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


@pytest.mark.usefixtures("_clean_registry")
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


@pytest.mark.usefixtures("_clean_registry")
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
    with patch("backend.core.request_context.svcconfig.load_config", return_value={"service_id": expected}):
        assert _call_get_service_id(**kwargs) == expected


@pytest.mark.usefixtures("_clean_registry")
@pytest.mark.parametrize(
    ("sid", "cdn_map", "expected"),
    [
        ("cdn-svc-1", {"cdn-svc-1": "log-svc-1"}, "log-svc-1"),
        ("ghost", {}, "ghost"),
    ],
)
def test_get_service_id_cdn_fallback(sid: str, cdn_map: dict, expected: str):
    with (
        patch("backend.core.request_context.svcconfig.load_config", return_value=None),
        patch("backend.core.request_context.svcconfig.get_cdn_service_id_map", return_value=cdn_map),
    ):
        assert _call_get_service_id(sid=sid) == expected


@pytest.mark.usefixtures("_clean_registry")
@pytest.mark.parametrize("active_id", ["active-svc", None])
def test_get_service_id_falls_back_to_active_when_nothing_provided(active_id: str | None):
    with patch("backend.core.request_context.svcconfig.get_active_service_id", return_value=active_id):
        assert _call_get_service_id() == active_id


# ── get_source: source lookup + 400 on miss ──────────────────────────────────


@pytest.mark.usefixtures("_clean_registry")
def test_get_source_returns_source_dict_when_found():
    fake_src = {"name": "logs_svc", "service_id": "svc"}
    with patch("backend.core.request_context.db.get_source_for_service", return_value=fake_src):
        assert deps.get_source(service_id="svc") is fake_src


@pytest.mark.usefixtures("_clean_registry")
@pytest.mark.parametrize("service_id", [None, "ghost"])
def test_get_source_raises_400_when_missing(service_id: str | None):
    with patch("backend.core.request_context.db.get_source_for_service", return_value=None):
        with pytest.raises(HTTPException) as exc:
            deps.get_source(service_id=service_id)
    assert exc.value.status_code == 400
    assert exc.value.detail["no_service"] is True


# ── _ConnectionHolder: enter / DBBusyError / exit ────────────────────────────


@pytest.fixture
def disable_pool(monkeypatch):
    monkeypatch.setenv("DUCKDB_CONNECTION_POOL", "0")
    yield


@pytest.mark.usefixtures("_clean_registry")
def test_connection_holder_opens_and_closes(disable_pool):
    fake_con = MagicMock()
    src = {"name": "x"}
    with patch("backend.core.request_context.get_connection", return_value=fake_con) as mock_get:
        holder = deps._ConnectionHolder(src, read_only=True)
        assert holder.__enter__() is fake_con
        mock_get.assert_called_once_with(source=src, max_wait=ANY, skip_view_update=False, read_only=True)
        holder.__exit__(None, None, None)
        fake_con.close.assert_called_once()


@pytest.mark.usefixtures("_clean_registry")
def test_connection_holder_skip_view_update_forwarded():
    with patch("backend.core.request_context.get_connection") as mock_get:
        deps._ConnectionHolder({"name": "x"}, skip_view_update=True).__enter__()
        assert mock_get.call_args.kwargs["skip_view_update"] is True


@pytest.mark.usefixtures("_clean_registry")
@pytest.mark.parametrize(("env_val", "expected_wait"), [(None, 10.0), ("9.5", 9.5)])
def test_connection_holder_max_wait(disable_pool, monkeypatch, env_val: str | None, expected_wait: float):
    if env_val is None:
        monkeypatch.delenv("DUCKDB_POOL_MAX_WAIT_S", raising=False)
    else:
        monkeypatch.setenv("DUCKDB_POOL_MAX_WAIT_S", env_val)
    with patch("backend.core.request_context.get_connection", return_value=MagicMock()) as mock_get:
        deps._ConnectionHolder({"name": "x"}, read_only=True).__enter__()
        assert mock_get.call_args.kwargs["max_wait"] == expected_wait


@pytest.mark.usefixtures("_clean_registry")
def test_connection_holder_db_busy_and_exit_edge_cases(disable_pool):
    with patch("backend.core.request_context.get_connection", side_effect=DBBusyError("locked")):
        with pytest.raises(HTTPException) as exc:
            deps._ConnectionHolder({"name": "x"}).__enter__()
    assert exc.value.status_code == 503 and exc.value.detail["busy"] is True

    fake_con = MagicMock()
    fake_con.close.side_effect = RuntimeError("already closed")
    with patch("backend.core.request_context.get_connection", return_value=fake_con):
        holder = deps._ConnectionHolder({"name": "x"})
        holder.__enter__()
        holder.__exit__(None, None, None)

    deps._ConnectionHolder({"name": "x"}).__exit__(None, None, None)


# ── get_con & removed symbols ────────────────────────────────────────────────


@pytest.mark.usefixtures("_clean_registry")
def test_get_con_yields_connection_and_closes_after(disable_pool):
    fake_con = MagicMock()
    with patch("backend.core.request_context.get_connection", return_value=fake_con) as mock_get:
        gen = deps.get_con(source={"name": "x"})
        assert next(gen) is fake_con
        assert mock_get.call_args.kwargs["read_only"] is True
        with pytest.raises(StopIteration):
            next(gen)
        fake_con.close.assert_called_once()


@pytest.mark.usefixtures("_clean_registry")
@pytest.mark.parametrize("symbol", ["get_meta_con", "AnalyticsDeps"])
def test_removed_deps_symbols(symbol: str):
    assert not hasattr(deps, symbol), f"{symbol} was removed at the v2.0 cut"


# ── _ConnectionHolder: pool path ─────────────────────────────────────────────


@pytest.mark.usefixtures("_clean_registry")
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


@pytest.mark.usefixtures("_clean_registry")
def test_connection_holder_pool_path_skipped_when_skip_view_update():
    with (
        patch.object(duckdb_pool, "checkout_connection") as mock_checkout,
        patch("backend.core.request_context.get_connection", return_value=MagicMock()) as mock_get,
    ):
        deps._ConnectionHolder({"name": "x"}, skip_view_update=True).__enter__()
        mock_checkout.assert_not_called()
        mock_get.assert_called_once()


# ── Merged from test_sre_observability.py ────────────────────────────────────


@pytest.mark.usefixtures("_clean_registry")
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


@pytest.mark.usefixtures("_clean_registry")
def test_new_request_id_is_nonempty_hex():
    rid = _new_request_id()
    assert isinstance(rid, str) and len(rid) == 16
    int(rid, 16)
    assert _new_request_id() != rid


@pytest.mark.usefixtures("_clean_registry")
def test_global_job_runs_in_cron_attribution_scope(monkeypatch):
    import backend.cron.scheduler as sj

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


@pytest.mark.usefixtures("_clean_registry")
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


@pytest.mark.usefixtures("_clean_registry")
def test_history_cap_is_400():
    assert _HISTORY_CAP == 400
    assert QueryRegistry()._history.maxlen == 400


# ── SRE-04 / SRE-05 / SRE-22: deep /api/health ──────────────────────────────


@pytest.mark.usefixtures("_clean_registry")
@pytest.mark.parametrize(
    ("cron_rows", "expected_code", "expected_status", "reason_substr"),
    [
        ([("log_discovery", "success", 2, None), ("commit", "success", 3, None)], 200, "ok", None),
        (
            [("log_discovery", "success", 120, None), ("log_discovery", "running", 20, None)],
            503,
            "degraded",
            "stuck running",
        ),
        ([("log_discovery", "success", 30, None), ("log_discovery", "running", 2, None)], 200, "ok", None),
        (
            [("log_discovery", "success", 2, None), ("commit", "error", 1, "boom")],
            503,
            "degraded",
            "commit cron errored",
        ),
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


@pytest.mark.usefixtures("_clean_registry")
@pytest.mark.parametrize(
    ("quiet_mins", "expected_status", "reason_substr"),
    [
        (35, "ok", None),
        (200, "stale", "no ingest since"),
    ],
)
def test_deep_health_adaptive_staleness(quiet_mins: int, expected_status: str, reason_substr: str | None):
    history = [("log_discovery", "success", quiet_mins + i * 40, None, 5) for i in range(12)]
    code, body = _deep_health(history, ingest_minutes_ago=quiet_mins)
    assert code == 200
    svc = body["services"][0]
    assert svc["status"] == expected_status and svc["stale_minutes_used"] == 80
    if reason_substr:
        assert reason_substr in svc["reason"]


# ── SRE-03 / 06 / 11 / 13 / 20: health-snapshot additions ────────────────────


@pytest.mark.usefixtures("_clean_registry")
def test_health_snapshot_has_new_observability_fields(client, monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER", raising=False)
    monkeypatch.delenv("STRUCTLOG_FORMAT", raising=False)
    body = client.get("/api/admin/health-snapshot").json()
    for key in ("scheduler_last_tick_age_s", "recent_cron_failures", "observability", "config_backup"):
        assert key in body, f"{key} missing from health-snapshot"
    assert body["observability"] == {"log_format": "console", "otel_exporter": "none"}


@pytest.mark.usefixtures("_clean_registry")
def test_health_snapshot_scheduler_tick_age_surfaced(client):
    with patch("backend.cron.jobs.metric_snapshots.last_snapshot_age_s", return_value=12.5):
        body = client.get("/api/admin/health-snapshot").json()
    assert body["scheduler_last_tick_age_s"] == 12.5


@pytest.mark.usefixtures("_clean_registry")
def test_health_snapshot_recent_cron_failures_aggregates_errors(client):
    latest = {
        "sync": {"status": "success", "started_at": "2026-06-20T10:00:00Z", "error_message": None},
        "commit": {"status": "error", "started_at": "2026-06-20T10:05:00Z", "error_message": "boom"},
    }
    with (
        patch("backend.config.list_service_ids", return_value=["svc-a"]),
        patch("backend.core.metadata.usage_log.latest_cron_per_task", return_value=latest),
    ):
        body = client.get("/api/admin/health-snapshot").json()

    fails = body["recent_cron_failures"]
    assert len(fails) == 1
    assert fails[0]["service_id"] == "svc-a" and fails[0]["task"] == "commit" and fails[0]["status"] == "error"


@pytest.mark.usefixtures("_clean_registry")
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


@pytest.mark.usefixtures("_clean_registry")
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


@pytest.fixture()
def _clean_registry():
    before = dict(query_registry._queries)
    before_keys = list(query_registry._cancelled_keys)
    yield
    query_registry._queries.clear()
    query_registry._queries.update(before)
    query_registry._cancelled_keys.clear()
    query_registry._cancelled_keys.extend(before_keys)


@pytest.mark.usefixtures("_clean_registry")
def test_a_cancelled_request_interrupts_the_queries_it_started():
    conn = _Conn()

    async def call_next(request):
        query_registry.register("DuckDB", "SELECT slow()", service_id="svc", con=conn, pool_slot="svc#1")
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(telemetry_middleware(_request(), call_next))
    assert conn.interrupts == 1


@pytest.mark.usefixtures("_clean_registry")
def test_a_request_that_completes_normally_interrupts_nothing():
    conn = _Conn()

    async def call_next(request):
        query_registry.register("DuckDB", "SELECT 1", service_id="svc", con=conn, pool_slot="svc#1")
        return _Resp(200)

    asyncio.run(telemetry_middleware(_request(), call_next))
    assert conn.interrupts == 0


@pytest.mark.usefixtures("_clean_registry")
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


@pytest.mark.usefixtures("_clean_registry")
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


@pytest.mark.usefixtures("_clean_registry")
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


@pytest.mark.usefixtures("_clean_registry")
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


@pytest.mark.usefixtures("_clean_registry")
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


# ── Merged from tests/test_e2e_pipeline.py ──

import copy
import glob
import gzip
import importlib
import io
import os
import shutil
import tempfile
import uuid
from contextlib import asynccontextmanager
from functools import partial

import pyarrow as pa
import pytest
from fastapi.testclient import TestClient
from scripts.check_environment_health import (
    ENVIRONMENTS,
    check_backend_health,
    check_rbac,
    check_tenancy,
)

import backend.core.metadata as metadata_db
import backend.core.request_telemetry as module
import backend.core.storage._core as _dl
import backend.core.storage._core as buffer_mod
import backend.cron.scheduler as cron_progress
from backend.celery_app import SyncStatusPublisher
from backend.core import ingest as ing
from backend.core import storage as ice
from backend.core.duckdb import _cache_dir
from backend.cron.scheduler import CronRunsPublisher, compute_sync_status_cached
from backend.high_scale.archive import ServingWatermark
from backend.high_scale.ingest_controller import HighScaleService
from backend.main import app
from backend.repositories._base import _safe_table
from backend.repositories.dashboard import _dashboard_cache, get_aggregates
from backend.repositories.insights import _insights_cache, get_insights
from tests._helpers.sources import make_source
from tests._helpers.wire import await_until
from tests.conftest import MOCK_SERVICE_ID


@pytest.fixture
def pipeline_env(monkeypatch):
    """Stand up a full pipeline environment with a local-FS DuckLake warehouse."""
    tmpdir = tempfile.mkdtemp(prefix="e2e_pipeline_")
    warehouse_path = os.path.join(tmpdir, "warehouse")
    cache_path = os.path.join(tmpdir, "cache")
    for p in (warehouse_path, cache_path, os.path.join(tmpdir, "ducklake_data")):
        os.makedirs(p, exist_ok=True)

    sfx = uuid.uuid4().hex[:6]
    source = {
        "name": f"e2e_svc_{sfx}",
        "service_id": f"e2e-svc-{sfx}",
        "service_name": f"E2E Test {sfx}",
        "bucket": "e2e-bucket",
        "prefix": "logs",
        "region": "us-east-1",
        "endpoint": "us-east-1.object.fastlystorage.app",
        "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
        "access_key_id": "test-key",
        "secret_access_key": "test-secret",
        "storage_mode": "cloud",
        "duckdb_path": os.path.join(tmpdir, f"e2e_{sfx}.duckdb"),
    }

    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: cache_path)
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})
    monkeypatch.setattr("backend.config.DUCKLAKE_CATALOG", os.path.join(tmpdir, "ducklake"))
    monkeypatch.setattr("backend.config.DUCKLAKE_DATA_PATH", os.path.join(tmpdir, "ducklake_data/"))

    if hasattr(ice, "_view_cache"):
        ice._view_cache.clear()
    yield {"src": source, "tmpdir": tmpdir, "warehouse": warehouse_path, "cache": cache_path}
    shutil.rmtree(tmpdir, ignore_errors=True)
    if hasattr(ice, "_view_cache"):
        ice._view_cache.clear()


def _simulate_sync(src, warehouse_path, cache_path):
    """Copy parquet files from the warehouse into the local cache/data/ dir."""
    data_dir = os.path.join(cache_path, "data")
    os.makedirs(data_dir, exist_ok=True)
    for src_path in glob.glob(os.path.join(warehouse_path, "**", "*.parquet"), recursive=True):
        dst = os.path.join(data_dir, os.path.basename(src_path))
        if not os.path.exists(dst):
            shutil.copy2(src_path, dst)


def _make_log_batch(n: int = 20, *, ip_octet: int = 0, path_prefix: str | None = None) -> pa.Table:
    """Build a PyArrow table shaped like a buffer batch from real ingest."""
    base = datetime.now(UTC) - timedelta(hours=1)
    urls = (
        [f"/{path_prefix}/{i}" for i in range(n)] if path_prefix is not None else [f"/path/{i % 10}" for i in range(n)]
    )
    countries = ["US"] * n if path_prefix is not None else ["US" if i % 3 == 0 else "GB" for i in range(n)]
    pops = ["LAX"] * n if path_prefix is not None else [["LAX", "JFK", "LHR"][i % 3] for i in range(n)]
    return pa.table(
        {
            "timestamp": pa.array(
                [base + timedelta(minutes=i * 2) for i in range(n)], type=pa.timestamp("us", tz="UTC")
            ),
            "ip": pa.array([f"10.{ip_octet}.0.{i}" for i in range(n)]),
            "status": pa.array([200 if i % 5 else 500 for i in range(n)], type=pa.uint16()),
            "url": pa.array(urls),
            "country": pa.array(countries),
            "method": pa.array(["GET"] * n),
            "ua": pa.array(["Mozilla/5.0"] * n),
            "pop": pa.array(pops),
        }
    )


def _seed_and_commit(src: dict, n: int, fname: str) -> dict:
    ice.init_storage_table(src)
    ice.write_to_buffer(src, _make_log_batch(n=n), fname)
    return ice.commit_buffer(src)


def test_e2e_buffer_to_commit_produces_queryable_snapshot(pipeline_env):
    """Full lifecycle: write buffer parquet -> commit -> update DuckDB view -> query."""
    src = pipeline_env["src"]
    assert ice.init_storage_table(src) is not None
    ice.write_to_buffer(src, _make_log_batch(n=20), "batch_test_0.parquet")
    assert len(ice.buffer_files(src)) == 1

    result = ice.commit_buffer(src)
    assert result["files_committed"] >= 1
    assert result["rows_committed"] == 20
    assert result["snapshot_id"] is not None
    assert ice.buffer_files(src) == []

    _simulate_sync(src, pipeline_env["warehouse"], pipeline_env["cache"])
    con = duckdb.connect(":memory:")
    ice.update_storage_view(con, src)
    row = con.execute(f"SELECT COUNT(*) FROM {_safe_table(src['name'])}").fetchone()
    assert row[0] == 20


def test_e2e_aggregates_via_full_pipeline(pipeline_env):
    """Pipeline -> get_aggregates returns the ingested row count."""
    src = pipeline_env["src"]
    _seed_and_commit(src, 15, "batch_agg_0.parquet")
    _simulate_sync(src, pipeline_env["warehouse"], pipeline_env["cache"])
    con = duckdb.connect(":memory:")
    ice.update_storage_view(con, src)

    _dashboard_cache.clear()
    out = get_aggregates(
        con=con, src=src, start_time=None, end_time=None, filters={}, chart_interval="1 minute", chart_metric="requests"
    )
    assert out["total_rows"] == 15
    assert out["total_rows_total"] >= 15


def test_e2e_insights_via_full_pipeline(pipeline_env):
    """Pipeline -> get_insights runs all registered insights without error."""
    src = pipeline_env["src"]
    _seed_and_commit(src, 30, "batch_insights_0.parquet")
    _simulate_sync(src, pipeline_env["warehouse"], pipeline_env["cache"])
    con = duckdb.connect(":memory:")
    ice.update_storage_view(con, src)

    _insights_cache.clear()
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value={"LAX": (33.94, -118.4)}):
        result = get_insights(con, src, window_hours=1, baseline_hours=1)

    errors = [i for i in result["insights"] if i.get("severity") == "error"]
    assert not errors, "Insights errored:\n" + "\n".join(f"  {i['id']}: {i.get('summary', '')[:120]}" for i in errors)


def test_commit_buffer_does_no_redundant_catalog_work_per_call(pipeline_env, monkeypatch):
    """Single-chunk commit attaches DuckLake RW once and does not re-enter catalog init."""
    src = pipeline_env["src"]
    ice.init_storage_table(src)
    ice.write_to_buffer(src, _make_log_batch(n=5), "batch_load_pin_0.parquet")

    counts = {"public": 0, "locked": 0, "write_attach": 0}
    real_public, real_locked, real_write_conn = (
        ice.init_storage_table,
        ice._init_storage_table_locked,
        buffer_mod._ducklake_write_connection,
    )

    def counting_public(source, create=True, table_name="logs"):
        counts["public"] += 1
        return real_public(source, create=create, table_name=table_name)

    def counting_locked(source, create=True, table_name="logs"):
        counts["locked"] += 1
        return real_locked(source, create=create, table_name=table_name)

    def counting_write_conn(source):
        counts["write_attach"] += 1
        return real_write_conn(source)

    monkeypatch.setattr(ice, "init_storage_table", counting_public)
    monkeypatch.setattr(ice, "_init_storage_table_locked", counting_locked)
    monkeypatch.setattr(buffer_mod, "_ducklake_write_connection", counting_write_conn)

    result = ice.commit_buffer(src)
    assert result["rows_committed"] == 5
    assert counts == {"public": 0, "locked": 0, "write_attach": 1}


def test_e2e_empty_buffer_commit_is_noop(pipeline_env):
    """commit_buffer with no buffer files is a clean no-op."""
    src = pipeline_env["src"]
    ice.init_storage_table(src)
    result = ice.commit_buffer(src)
    assert (result["files_committed"], result["rows_committed"], result["snapshot_id"]) == (0, 0, None)


def test_e2e_two_commits_append_rather_than_overwrite(pipeline_env):
    """Two separate commit cycles append rather than overwrite."""
    src = pipeline_env["src"]
    _seed_and_commit(src, 10, "batch_a.parquet")
    ice.write_to_buffer(src, _make_log_batch(n=12), "batch_b.parquet")
    ice.commit_buffer(src)

    _simulate_sync(src, pipeline_env["warehouse"], pipeline_env["cache"])
    con = duckdb.connect(":memory:")
    ice.update_storage_view(con, src)
    row = con.execute(f"SELECT COUNT(*) FROM {_safe_table(src['name'])}").fetchone()
    assert row[0] == 22


def test_full_pipeline_including_raw_gzip_ingest(s3_mock, fos_source, monkeypatch, tmp_path, pipeline_env):
    """End-to-end through the RAW .gz ingest path: upload to moto S3 -> ingest -> commit -> query."""
    cache_path, warehouse_path = str(tmp_path / "cache"), str(tmp_path / "warehouse")
    os.makedirs(cache_path, exist_ok=True)
    os.makedirs(warehouse_path, exist_ok=True)

    fos_source["duckdb_path"] = os.path.join(tmp_path, "e2e.duckdb")
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: cache_path)
    monkeypatch.setattr("backend.core.duckdb._configure_fos", lambda *a, **kw: None)

    class _CallerHintShim:
        def __init__(self, client):
            self._client = client

        def get_paginator(self, op, caller_hint=None):
            return self._client.get_paginator(op)

        def __getattr__(self, name):
            return getattr(self._client, name)

    monkeypatch.setattr("backend.core.ingest._get_fos_client", lambda _src: _CallerHintShim(s3_mock))
    if hasattr(ice, "_view_cache"):
        ice._view_cache.clear()

    unique_id = uuid.uuid4().hex[:8]
    unique_prefix = f"pipe_{unique_id}"
    fos_source = {
        **fos_source,
        "prefix": unique_prefix,
        "service_id": f"svc_{unique_id}",
        "name": f"tbl_{unique_id}",
    }

    base = datetime.now(UTC) - timedelta(hours=2)
    rows_per_file = 5
    files = [
        (f"{unique_prefix}/raw/request/year=2026/month=05/day=20/hour=10/minute=00/2026-05-20T10-00-00.svc.gz", base),
        (
            f"{unique_prefix}/raw/request/year=2026/month=05/day=20/hour=10/minute=05/2026-05-20T10-05-00.svc.gz",
            base + timedelta(minutes=5),
        ),
    ]
    for key, ts in files:
        rows = [
            {
                "timestamp": (ts + timedelta(seconds=i)).strftime("%Y-%m-%dT%H:%M:%S+0000"),
                "ip": f"10.0.0.{i}",
                "status": 200 if i % 2 == 0 else 404,
                "url": f"/path/{i}",
                "method": "GET",
                "cache": "HIT",
                "resp_bytes": 1024 + i,
                "elapsed": 1500 + i * 10,
            }
            for i in range(rows_per_file)
        ]
        body = io.BytesIO()
        with gzip.GzipFile(fileobj=body, mode="wb") as gz:
            gz.write(("\n".join(json.dumps(r) for r in rows) + "\n").encode())
        s3_mock.put_object(Bucket=fos_source["bucket"], Key=key, Body=body.getvalue())

    ice.init_storage_table(fos_source)
    events = list(ing.ingest(source=fos_source))
    done = next((e for e in events if e["type"] == "done"), None)
    assert done is not None
    assert done["new_files"] == len(files)
    assert done["rows_inserted"] == rows_per_file * len(files)
    assert len(ice.buffer_files(fos_source)) >= 1

    result = ice.commit_buffer(fos_source)
    assert result["rows_committed"] == rows_per_file * len(files)
    assert result["snapshot_id"] is not None
    assert ice.buffer_files(fos_source) == []

    _simulate_sync(fos_source, warehouse_path, cache_path)
    con = duckdb.connect(":memory:")
    ice.update_storage_view(con, fos_source)

    view_name = _safe_table(fos_source["name"])
    (count,) = con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()
    assert count == rows_per_file * len(files)

    sample = con.execute(
        f"SELECT status, url, method, cache, resp_bytes FROM {view_name} ORDER BY url LIMIT 1"
    ).fetchone()
    assert sample is not None
    status, url, method, cache, resp_bytes = sample
    assert isinstance(status, int) and status in (200, 404)
    assert url.startswith("/path/") and method == "GET" and cache == "HIT"
    assert isinstance(resp_bytes, int) and resp_bytes >= 1024

    service_name = fos_source["name"]
    ingested_names = {row["file_name"] for row in metadata_db.list_ingested_files(service_name)}
    for key, _ in files:
        assert any(name.endswith(key) for name in ingested_names)
    assert metadata_db.list_in_flight(service_name) == []


def test_e2e_readonly_dashboard_query_path(pipeline_env):
    """Writer commits buffer -> builds persistent view; separate RO connection hits fast-path cache."""
    src = pipeline_env["src"]
    db_path = os.path.join(pipeline_env["tmpdir"], "e2e.duckdb")

    _seed_and_commit(src, 8, "batch_ro_e2e.parquet")
    _simulate_sync(src, pipeline_env["warehouse"], pipeline_env["cache"])

    rw = duckdb.connect(db_path)
    ice.update_storage_view(rw, src)
    view_name = _safe_table(src["name"])
    assert rw.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0] == 8
    rw.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT NULL::INTEGER AS x WHERE false")
    rw.close()

    ro = duckdb.connect(db_path, read_only=True)
    try:
        ice.update_storage_view(ro, src)
        assert ro.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0] == 8
        _dashboard_cache.clear()
        out = get_aggregates(
            con=ro,
            src=src,
            start_time=None,
            end_time=None,
            filters={},
            chart_interval="1 hour",
            chart_metric="requests",
        )
        assert out["total_rows"] == 8
    finally:
        ro.close()


def test_e2e_readonly_dashboard_cold_cache_path(pipeline_env):
    """Fresh RO connection + evicted caches rebuilds TEMP view via slow path."""
    src = pipeline_env["src"]
    db_path = os.path.join(pipeline_env["tmpdir"], "e2e_cold.duckdb")

    _seed_and_commit(src, 8, "batch_ro_cold.parquet")
    _simulate_sync(src, pipeline_env["warehouse"], pipeline_env["cache"])

    rw = duckdb.connect(db_path)
    ice.update_storage_view(rw, src)
    view_name = _safe_table(src["name"])
    assert rw.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0] == 8
    rw.close()

    ice._view_cache.pop(src["name"], None)

    ro = duckdb.connect(db_path, read_only=True)
    try:
        ice.update_storage_view(ro, src)
        assert ro.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0] == 8
        _dashboard_cache.clear()
        out = get_aggregates(
            con=ro,
            src=src,
            start_time=None,
            end_time=None,
            filters={},
            chart_interval="1 hour",
            chart_metric="requests",
        )
        assert out["total_rows"] == 8
    finally:
        ro.close()


# ── Merged from test_multi_service_e2e.py ──────────────────────────────────────────────

_make_source = partial(
    make_source,
    kind="cloud",
    prefix="logs",
    fos_native_endpoint="us-east-1.object.fastlystorage.app",
    access_key_id="test-key",
    secret_access_key="test-secret",
)


@pytest.fixture
def two_service_env(monkeypatch):
    """Two local warehouses, one per service."""
    tmpdir = tempfile.mkdtemp(prefix="multi_svc_e2e_")
    warehouses = {
        "svc_a": os.path.join(tmpdir, "warehouse_a"),
        "svc_b": os.path.join(tmpdir, "warehouse_b"),
    }
    for p in warehouses.values():
        os.makedirs(p, exist_ok=True)
    src_a = _make_source(
        name="svc_a",
        service_id="svc-a-id",
        bucket="svc-a-bucket",
        service_name="Multi-Svc svc_a",
        duckdb_path=svcconfig.duckdb_path("svc-a-id"),
    )
    src_b = _make_source(
        name="svc_b",
        service_id="svc-b-id",
        bucket="svc-b-bucket",
        service_name="Multi-Svc svc_b",
        duckdb_path=svcconfig.duckdb_path("svc-b-id"),
    )

    monkeypatch.setattr(_dl, "_default_data_path", lambda s: warehouses[s["name"]])
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})
    if hasattr(ice, "_view_cache"):
        ice._view_cache.clear()

    yield {"src_a": src_a, "src_b": src_b, "warehouses": warehouses}
    shutil.rmtree(tmpdir, ignore_errors=True)


def _simulate_sync_one(source: dict, warehouse_path: str) -> None:
    data_dir = os.path.join(_cache_dir(source), "data")
    os.makedirs(data_dir, exist_ok=True)
    for sp in glob.glob(os.path.join(warehouse_path, "**", "*.parquet"), recursive=True):
        dst = os.path.join(data_dir, os.path.basename(sp))
        if not os.path.exists(dst):
            shutil.copy2(sp, dst)


def test_concurrent_storage_commits_two_services_no_cross_contamination(two_service_env):
    """Two services run buffer-write + commit concurrently without cross-tenant contamination."""
    src_a, src_b = two_service_env["src_a"], two_service_env["src_b"]
    ice.init_storage_table(src_a)
    ice.init_storage_table(src_b)
    ice.write_to_buffer(src_a, _make_log_batch(n=12, ip_octet=1, path_prefix="alpha"), "svc_a.parquet")
    ice.write_to_buffer(src_b, _make_log_batch(n=18, ip_octet=2, path_prefix="bravo"), "svc_b.parquet")

    barrier = threading.Barrier(2)
    results: dict[str, dict] = {}
    rlock = threading.Lock()

    def commit_worker(label: str, src: dict) -> None:
        outcome: dict = {"label": label}
        try:
            barrier.wait()
            res = ice.commit_buffer(src)
            outcome["rows_committed"] = res.get("rows_committed")
            outcome["snapshot_id"] = res.get("snapshot_id")
        except Exception as exc:
            outcome["error"] = repr(exc)
        with rlock:
            results[label] = outcome

    threads = [
        threading.Thread(target=commit_worker, args=("a", src_a)),
        threading.Thread(target=commit_worker, args=("b", src_b)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
        assert not t.is_alive()

    for label, expected in (("a", 12), ("b", 18)):
        r = results[label]
        assert "error" not in r, f"svc_{label} commit raised: {r['error']}"
        assert r["rows_committed"] == expected
        assert r["snapshot_id"] is not None
    assert ice.buffer_files(src_a) == [] and ice.buffer_files(src_b) == []

    _simulate_sync_one(src_a, two_service_env["warehouses"]["svc_a"])
    _simulate_sync_one(src_b, two_service_env["warehouses"]["svc_b"])
    con_a, con_b = duckdb.connect(":memory:"), duckdb.connect(":memory:")
    ice.update_storage_view(con_a, src_a)
    ice.update_storage_view(con_b, src_b)

    view_a, view_b = _safe_table(src_a["name"]), _safe_table(src_b["name"])
    assert con_a.execute(f"SELECT COUNT(*) FROM {view_a}").fetchone()[0] == 12
    assert con_b.execute(f"SELECT COUNT(*) FROM {view_b}").fetchone()[0] == 18
    assert (
        con_b.execute(f"SELECT COUNT(*) FROM {view_b} WHERE ip LIKE '10.1.%' OR url LIKE '/alpha/%'").fetchone()[0] == 0
    )
    assert (
        con_a.execute(f"SELECT COUNT(*) FROM {view_a} WHERE ip LIKE '10.2.%' OR url LIKE '/bravo/%'").fetchone()[0] == 0
    )


def test_concurrent_metadata_db_writes_two_services_no_cross_contamination():
    """Two services run independent metadata_db mutations on separate threads without cross-contamination."""
    svc_a, svc_b = "svc-md-a", "svc-md-b"
    seed_key, new_key = "raw/2026-06-10/00/seed.gz", "raw/2026-06-10/01/svc_b_only.gz"

    metadata_db.insert_ingested_files(svc_a, [(seed_key, 100, 1024)])
    metadata_db.insert_ingested_files(svc_b, [(seed_key, 100, 1024)])
    assert {r["file_name"] for r in metadata_db.list_ingested_files(svc_a)} == {seed_key}
    assert {r["file_name"] for r in metadata_db.list_ingested_files(svc_b)} == {seed_key}

    barrier = threading.Barrier(2)
    errors: list[str] = []
    elock = threading.Lock()

    def deleter_svc_a() -> None:
        try:
            barrier.wait()
            con = metadata_db.get_con(svc_a)
            con.execute("DELETE FROM ingested_files WHERE source_name = ? AND file_name = ?", (svc_a, seed_key))
            con.commit()
        except Exception as exc:
            with elock:
                errors.append(f"svc_a deleter: {exc!r}")

    def inserter_svc_b() -> None:
        try:
            barrier.wait()
            metadata_db.insert_ingested_files(svc_b, [(new_key, 250, 2048)])
        except Exception as exc:
            with elock:
                errors.append(f"svc_b inserter: {exc!r}")

    threads = [threading.Thread(target=deleter_svc_a), threading.Thread(target=inserter_svc_b)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)
        assert not t.is_alive()
    assert not errors

    assert {r["file_name"] for r in metadata_db.list_ingested_files(svc_a)} == set()
    assert {r["file_name"] for r in metadata_db.list_ingested_files(svc_b)} == {seed_key, new_key}


# ── Merged from test_smoke_end_to_end.py ──────────────────────────────────────────────


@pytest.fixture
def seeded_service(in_memory_duckdb, test_service_source):
    """Seed the test DuckDB with 50 log rows spanning the last 2 hours."""
    table = _safe_table(test_service_source["name"])
    catalog_fields = [
        ("timestamp", "TIMESTAMPTZ"),
        ("dt", "VARCHAR"),
        ("timestamp_hour", "VARCHAR"),
        ("status", "INTEGER"),
        ("country", "VARCHAR"),
        ("url", "VARCHAR"),
        ("ip", "VARCHAR"),
        ("method", "VARCHAR"),
        ("ua", "VARCHAR"),
        ("pop", "VARCHAR"),
        ("asn", "INTEGER"),
        ("city", "VARCHAR"),
        ("region", "VARCHAR"),
        ("elapsed", "INTEGER"),
        ("cache", "VARCHAR"),
        ("ottfb", "DOUBLE"),
        ("ttfb", "DOUBLE"),
        ("waf_sig", "VARCHAR"),
        ("edge", "BOOLEAN"),
        ("lat", "DOUBLE"),
        ("lon", "DOUBLE"),
        ("resp_bytes", "INTEGER"),
        ("oip", "VARCHAR"),
        ("ost", "INTEGER"),
    ]
    cols_def = ", ".join(f'"{n}" {t}' for n, t in catalog_fields)
    in_memory_duckdb.execute(f"CREATE TABLE {table} ({cols_def})")

    base = datetime.now(UTC) - timedelta(hours=2)
    statuses = [200, 200, 200, 404, 500] * 10
    pops = ["LAX", "JFK", "LHR"] * 17
    countries = ["US", "US", "GB", "DE", "JP"] * 10
    rows = [
        (
            (ts := base + timedelta(minutes=i * 2)),
            ts.strftime("%Y-%m-%d"),
            ts.strftime("%Y-%m-%d-%H"),
            statuses[i],
            countries[i],
            f"/path-{i % 10}",
            f"10.0.{i // 10}.{i % 10}",
            "GET",
            "Mozilla/5.0",
            pops[i % 3],
            15169 + (i % 5),
            "San Francisco",
            "CA",
            50 + i * 5,
            "HIT" if i % 3 == 0 else "MISS",
            50000.0 + i * 100,
            0.05 + i * 0.001,
            "",
            True,
            37.7749,
            -122.4194,
            500 + i * 50,
            "origin-1.example.com",
            statuses[i],
        )
        for i in range(50)
    ]
    placeholders = ", ".join(["?"] * len(catalog_fields))
    in_memory_duckdb.executemany(
        f"INSERT INTO {table} ({', '.join(f'''\"{n}\"''' for n, _ in catalog_fields)}) VALUES ({placeholders})",
        rows,
    )
    return test_service_source


def test_smoke_dashboard_aggregates_returns_real_counts(client, seeded_service):
    resp = client.post(
        "/api/dashboard/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"start_time": None, "end_time": None, "filters": {}},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_rows"] > 0
    assert body["total_rows_total"] >= body["total_rows"]
    for key in ("data", "time_series", "interval", "metric"):
        assert key in body
    assert any(entry.get("top") for entry in body["data"].values())


def test_smoke_dashboard_aggregates_with_filter_narrows_results(client, seeded_service):
    unfiltered = client.post(
        "/api/dashboard/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"start_time": None, "end_time": None, "filters": {}},
    ).json()
    gb_only = client.post(
        "/api/dashboard/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"start_time": None, "end_time": None, "filters": {"country": {"mode": "include", "values": ["GB"]}}},
    ).json()
    assert 0 < gb_only["total_rows"] < unfiltered["total_rows"]


def test_smoke_dashboard_aggregates_with_numeric_filter_on_string_column_does_not_crash(client, seeded_service):
    resp = client.post(
        "/api/dashboard/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"start_time": None, "end_time": None, "filters": {"country": {"mode": "include", "values": [0]}}},
    )
    assert resp.status_code == 200
    assert resp.json()["total_rows"] == 0


def test_smoke_insights_endpoint_returns_insight_cards_with_distinct_ids(client, seeded_service):
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value={"LAX": (33.94, -118.4)}):
        resp = client.post(
            "/api/insights",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"window_size_hrs": 1, "baseline_hours": 1, "filters": {}},
        )
    assert resp.status_code == 200
    insights = resp.json()["insights"]
    assert len(insights) >= 15
    for card in insights:
        for key in ("id", "title", "severity", "summary", "items"):
            assert key in card
    ids = [c["id"] for c in insights]
    assert len(ids) == len(set(ids))


def test_smoke_origin_timeseries_with_sub_minute_bucket(client, seeded_service):
    resp = client.post(
        "/api/origin/timeseries",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "start_time": None,
            "end_time": None,
            "filters": {},
            "bucket_minutes": 1.0 / 60.0,
            "split_by_leg": False,
            "metric": "ttfb",
            "percentile": "p95",
        },
    )
    assert resp.status_code == 200
    assert "has_data" in resp.json()


def test_query_errors_decorator_returns_documented_envelope_for_malformed_sql(client, seeded_service):
    resp = client.post(
        "/api/dashboard/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"start_time": None, "end_time": None, "filters": {"status": {"mode": "", "values": [200]}}},
    )
    if resp.status_code != 200:
        assert "detail" in resp.json()


def test_query_errors_envelope_for_filter_type_mismatch(client, seeded_service):
    resp = client.post(
        "/api/dashboard/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"start_time": None, "end_time": None, "filters": {"country": {"mode": "include", "values": [0]}}},
    )
    if resp.status_code != 200:
        assert "detail" in resp.json()


# ── Merged from test_performance_smoke.py ──────────────────────────────────────────────


@pytest.fixture
def one_million_row_service(in_memory_duckdb, test_service_source):
    """Seed 1,000,000 rows into DuckDB using native generation for speed."""
    table = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(f"""
        CREATE TABLE {table} (
            timestamp TIMESTAMPTZ, status INTEGER, country VARCHAR, pop VARCHAR,
            url VARCHAR, method VARCHAR, ip VARCHAR, elapsed INTEGER,
            cache VARCHAR, ttfb DOUBLE, edge BOOLEAN, resp_bytes INTEGER, backend VARCHAR
        )
    """)
    in_memory_duckdb.execute(f"""
        INSERT INTO {table}
        SELECT
            now() - interval (random() * 24) hour as timestamp,
            (case when random() > 0.95 then 500 when random() > 0.90 then 404 else 200 end) as status,
            (case when random() > 0.5 then 'US' when random() > 0.3 then 'GB' else 'DE' end) as country,
            (case when random() > 0.7 then 'LAX' when random() > 0.4 then 'JFK' else 'LHR' end) as pop,
            '/path/' || (random() * 100)::int as url,
            'GET' as method,
            '10.0.0.1' as ip,
            (random() * 1000)::int as elapsed,
            (case when random() > 0.3 then 'HIT' else 'MISS' end) as cache,
            random() as ttfb,
            true as edge,
            (random() * 10000)::int as resp_bytes,
            'origin-1' as backend
        FROM range(1000000)
    """)
    return test_service_source


def test_performance_dashboard_aggregates_1m_rows(in_memory_duckdb, one_million_row_service):
    start = time.time()
    res = get_aggregates(
        con=in_memory_duckdb,
        src=one_million_row_service,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 hour",
        chart_metric="requests",
    )
    duration = time.time() - start
    assert res["total_rows"] == 1000000
    assert duration < 5.0


def test_performance_insights_1m_rows(in_memory_duckdb, one_million_row_service):
    start = time.time()
    with patch(
        "backend.utils.pop_utils.get_pop_lat_lon_map",
        return_value={"LAX": (33.9, -118.4), "JFK": (40.6, -73.7), "LHR": (51.4, -0.4)},
    ):
        res = get_insights(con=in_memory_duckdb, src=one_million_row_service, window_hours=1, baseline_hours=24)
    duration = time.time() - start
    assert len(res["insights"]) > 0
    assert duration < 10.0


# ── Merged from test_environment_connectivity.py ──────────────────────────────────────────────


@pytest.mark.skipif(
    bool(os.getenv("CI") or os.getenv("GITHUB_ACTIONS")),
    reason="Environment connectivity tests require local/remote port-forwards not present in CI",
)
@pytest.mark.skipif(
    any(not target.fastly_service_id for target in ENVIRONMENTS.values()),
    reason="Environment connectivity tests require all three service ID environment variables",
)
class TestEnvironmentConnectivity:
    @pytest.mark.parametrize("env_name", list(ENVIRONMENTS.keys()))
    def test_environment_backend_health(self, env_name: str) -> None:
        target = ENVIRONMENTS[env_name]
        result = check_backend_health(target, timeout_s=3.0)
        assert result.backend_ok, f"{env_name} failed health check: {result.error}"
        assert result.status_code == 200
        assert result.version == "3.0.0-rc1"

    @pytest.mark.parametrize("env_name", list(ENVIRONMENTS.keys()))
    def test_environment_service_and_tenancy(self, env_name: str) -> None:
        target = ENVIRONMENTS[env_name]
        result = check_tenancy(target, timeout_s=5.0)
        assert result.service_match, f"{env_name} service mismatch: {result.error}"
        assert result.rejection_ok, f"{env_name} tenancy breach: {result.error}"

    @pytest.mark.parametrize("env_name", list(ENVIRONMENTS.keys()))
    def test_environment_rbac(self, env_name: str) -> None:
        target = ENVIRONMENTS[env_name]
        result = check_rbac(target, timeout_s=5.0)
        assert result.auth_config_ok, f"{env_name} auth config failed: {result.error}"
        assert result.unauth_analyst_blocked, f"{env_name} unauth analyst blocking failed: {result.error}"


# ── Merged from test_helpers_sources.py ──────────────────────────────────────────────


def test_lake_kind_creates_cache_dirs_and_duckdb_path(tmp_path):
    src = make_source(tmp_path, "svc", kind="lake", buffer_dir=True)
    assert src == {
        "name": "svc",
        "service_id": "svc",
        "fos_local_warehouse": True,
        "_cache_dir_override": str(tmp_path / "cache_svc"),
        "duckdb_path": str(tmp_path / "svc.duckdb"),
    }
    assert (tmp_path / "cache_svc" / "buffer").is_dir()


def test_lake_kind_without_buffer_dir_only_makes_the_cache_dir(tmp_path):
    make_source(tmp_path, "svc", kind="lake")
    assert (tmp_path / "cache_svc").is_dir()
    assert not (tmp_path / "cache_svc" / "buffer").exists()


def test_lake_kind_applies_overrides_last(tmp_path):
    src = make_source(tmp_path, "svc", kind="lake", fos_local_warehouse=False, extra=1)
    assert src["fos_local_warehouse"] is False and src["extra"] == 1


def test_lake_kind_requires_root_and_name():
    with pytest.raises(TypeError):
        make_source(kind="lake")


def test_cache_kind_defaults_and_custom_subdirs(tmp_path):
    assert make_source(str(tmp_path)) == {
        "name": "test_service",
        "service_id": "test-service-id",
        "_cache_dir_override": str(tmp_path),
    }
    root = tmp_path / "cache" / "x"
    src = make_source(root, "x", cache_key="_test_cache_root", mkdirs=("data",))
    assert src == {"name": "x", "service_id": "x", "_test_cache_root": str(root)}
    assert (root / "data").is_dir()


def test_cloud_kind_defaults_overrides_and_errors():
    src = make_source(kind="cloud", bucket="b2", access_key_id="k")
    assert src["name"] == src["service_id"] == "test"
    assert src["bucket"] == "b2" and src["access_key_id"] == "k"
    assert src["storage_mode"] == "cloud" and "access_level" not in src

    preset = partial(make_source, kind="cloud", provisioning={})
    preset()["provisioning"]["mutated"] = True
    assert preset()["provisioning"] == {}

    with pytest.raises(ValueError, match="unknown source kind"):
        make_source(kind="nope")


# ── Merged from tests/test_sync_status_and_cron_sse.py ──


@pytest.fixture
def snapshot_state(monkeypatch):
    state = {
        "latest_log_at": "2026-09-05T01:00:00Z",
        "local_rows": 7,
        "request": {"latest_log_at": "2026-09-07T19:31:22Z", "total_rows": 7},
        "rum": {"latest_log_at": "2026-09-08T01:00:00Z", "total_rows": 3},
    }
    summary = {"latest_file_name": "logs_2026-09-06T17-19-00.gz", "total_rows": 99}
    ledger = MagicMock()
    ledger.execute.return_value.fetchone.return_value = None
    monkeypatch.setattr("backend.config.get_status", lambda _: copy.deepcopy(state))
    monkeypatch.setattr("backend.config.load_config", lambda _: {})
    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service", lambda _: {"name": "snapshot-svc", "duckdb_path": __file__}
    )
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _: ".")
    monkeypatch.setattr("backend.cron.scheduler._get_dir_size", lambda _: 0)
    monkeypatch.setattr("backend.core.metadata.get_ingested_files_status_summary", lambda _: summary)
    monkeypatch.setattr("backend.core.metadata.base.get_con", lambda _: ledger)
    monkeypatch.setattr("backend.cron.scheduler.get_latest_progress_for_service", lambda _: None)
    monkeypatch.setattr(
        "backend.core.duckdb.get_connection", lambda *a, **kw: pytest.fail("cached status must not acquire DuckDB")
    )
    return state, summary, ledger


@pytest.mark.parametrize("request_latest", ["2026-09-07T19:31:22Z", "2026-09-04T01:00:00Z", None])
@pytest.mark.parametrize("use_ledger", [False, True])
def test_file_overlay_preserves_current_request_extent(snapshot_state, request_latest, use_ledger):
    state, summary, ledger = snapshot_state
    state["request"]["latest_log_at"] = request_latest
    if request_latest is None:
        state["request"]["total_rows"] = state["local_rows"] = 0
    if use_ledger:
        ledger.execute.return_value.fetchone.return_value = (summary["latest_file_name"],)
        summary["latest_file_name"] = None

    snap = compute_sync_status_cached("snapshot-svc")
    assert snap["latest_log_at"] == snap["request"]["latest_log_at"] == request_latest
    assert snap["latest_ingested_file_at"] == snap["latest_available_file_at"] == "2026-09-06 17:19:00"
    assert snap["local_rows"] == state["local_rows"] and snap["rum"] == state["rum"]


@pytest.mark.parametrize("flat_latest", ["2026-09-07T19:31:22Z", "2026-09-04T01:00:00Z", None])
def test_legacy_flat_extent_is_not_a_filename_fallback(snapshot_state, flat_latest):
    state, _, _ = snapshot_state
    del state["request"]
    state["latest_log_at"] = flat_latest
    state["local_rows"] = 0
    snap = compute_sync_status_cached("snapshot-svc")
    assert snap["latest_log_at"] == flat_latest and snap["local_rows"] == 0


def _bind_high_scale_service(monkeypatch, *, include_rum: bool) -> None:
    class Client:
        def execute(self, sql, params=None):
            domain = (params or {}).get("domain")
            if domain == "request":
                return [
                    {
                        "total_rows": 12,
                        "earliest_log_at": datetime(2026, 9, 15, 18, 0, tzinfo=UTC),
                        "latest_log_at": datetime(2026, 9, 15, 19, 0, tzinfo=UTC),
                    }
                ]
            if not include_rum:
                return [{"total_rows": 0, "earliest_log_at": None, "latest_log_at": None}]
            rows = 8 if domain == "rum_vitals" else 2
            minute = 1 if domain == "rum_vitals" else 2
            return [
                {
                    "total_rows": rows,
                    "earliest_log_at": datetime(2026, 9, 15, 18, 0, tzinfo=UTC),
                    "latest_log_at": datetime(2026, 9, 15, 19, minute, tzinfo=UTC),
                }
            ]

    wm = ServingWatermark(
        service_id="high-scale-svc",
        domain="request",
        owner_epoch=1,
        coverage_start=datetime(2026, 9, 15, tzinfo=UTC),
        coverage_end=datetime(2026, 9, 16, tzinfo=UTC),
        last_accepted_cursor=None,
        last_archived_event_id=None,
        last_visible_event_id=None,
        exact=True,
    )
    service = HighScaleService(
        service_id="high-scale-svc", client=Client(), cursor_secret=b"secret", request_watermark=wm
    )
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda _: {"name": "high-scale-svc"})
    monkeypatch.setattr(
        "backend.high_scale.ingest_controller.get_high_scale_service_registry",
        lambda: type("Registry", (), {"resolve": lambda _, sid: service})(),
    )
    monkeypatch.setattr(
        "backend.config.get_status", lambda _: pytest.fail("high-scale must not use stale config status")
    )


def test_high_scale_snapshot_and_log_extents_use_clickhouse_metrics(monkeypatch):
    _bind_high_scale_service(monkeypatch, include_rum=True)
    snapshot = compute_sync_status_cached("high-scale-svc")
    assert (snapshot["request"]["total_rows"], snapshot["rum"]["total_rows"], snapshot["local_rows"]) == (12, 10, 22)
    assert snapshot["earliest_log_at"] == "2026-09-15T18:00:00+00:00"
    assert snapshot["latest_log_at"] == "2026-09-15T19:00:00+00:00"

    _bind_high_scale_service(monkeypatch, include_rum=False)
    router = importlib.import_module("backend.routers.admin.sync_status")
    extents = router.log_extents("high-scale-svc")
    assert extents.configured is True
    assert extents.earliest_log_at == "2026-09-15T18:00:00+00:00"
    assert extents.latest_log_at == "2026-09-15T19:00:00+00:00"


@pytest.mark.parametrize("request_latest", ["2026-09-07T19:31:22Z", "2026-09-04T01:00:00Z", None])
def test_cached_rest_and_extents_use_request_snapshot(snapshot_state, request_latest):
    state, _, _ = snapshot_state
    state["request"]["latest_log_at"] = request_latest
    if request_latest is None:
        state["local_rows"] = state["request"]["total_rows"] = 0
    router = importlib.import_module("backend.routers.admin.sync_status")
    payload = router.sync_status("snapshot-svc", skip_fos=True, force=False).model_dump()
    assert payload["latest_log_at"] == payload["request"]["latest_log_at"] == request_latest
    assert payload["local_rows"] == state["local_rows"]
    assert router.log_extents("snapshot-svc").latest_log_at == request_latest


@pytest.mark.asyncio
@pytest.mark.parametrize("admin", [False, True])
@pytest.mark.parametrize("request_latest", ["2026-09-07T19:31:22Z", None])
async def test_initial_sse_uses_request_extent(snapshot_state, admin, request_latest):
    state, _, _ = snapshot_state
    state["request"]["latest_log_at"] = request_latest

    class Request:
        async def is_disconnected(self):
            return False

    if admin:
        from backend.routers.admin.events import admin_events_stream

        response = await admin_events_stream(Request(), channels="sync-status", service_id="snapshot-svc")
    else:
        from backend.routers.admin.sync_status import log_extents_stream

        response = await log_extents_stream(Request(), service_id="snapshot-svc")
    iterator = response.body_iterator
    try:
        payload = json.loads(await asyncio.wait_for(anext(iterator), timeout=2))
        if admin:
            payload = payload["data"]
        assert payload["latest_log_at"] == payload["request"]["latest_log_at"] == request_latest
        if not admin:
            assert "latest_ingested_file_at" not in payload
    finally:
        await iterator.aclose()


# ── SSE Publishers (SyncStatusPublisher & CronRunsPublisher) ────────────────


def _parse_sse_data_events(text: str) -> list[dict]:
    out: list[dict] = []
    for chunk in text.replace("\r\n", "\n").split("\n\n"):
        data_parts = [line[5:].strip() for line in chunk.strip().split("\n") if line.startswith("data:")]
        if data_parts:
            try:
                out.append(json.loads("".join(data_parts)))
            except json.JSONDecodeError:
                pass
    return out


@pytest.mark.parametrize("pub_cls", [SyncStatusPublisher, CronRunsPublisher])
class TestSsePublishers:
    @pytest.mark.asyncio
    async def test_publish_from_worker_thread_and_noop_without_subscribers(self, pub_cls):
        pub = pub_cls()
        pub.publish("svc", {"x": 1})  # before loop bound: silent no-op
        pub.bind_loop(asyncio.get_running_loop())
        pub.publish("svc-nobody", {"x": 1})
        assert pub.subscriber_count("svc-nobody") == 0

        async def consume_one():
            async for payload in pub.subscribe("svc-1"):
                return payload
            return None

        consumer = asyncio.create_task(consume_one())
        await await_until(lambda: pub.subscriber_count("svc-1") >= 1, timeout=2.0, message="subscriber not registered")
        msg = {"event": "test", "run_id": 42, "local_rows": 42}
        threading.Thread(target=lambda: pub.publish("svc-1", msg)).start()
        assert await asyncio.wait_for(consumer, timeout=2.0) == msg

    @pytest.mark.asyncio
    async def test_subscribe_isolates_by_service_id_and_cleans_up_on_aclose(self, pub_cls):
        pub = pub_cls()
        pub.bind_loop(asyncio.get_running_loop())
        received_a: list[dict] = []
        received_b: list[dict] = []

        async def reader(svc, sink):
            async for p in pub.subscribe(svc):
                sink.append(p)
                return

        task_a = asyncio.create_task(reader("svc-A", received_a))
        task_b = asyncio.create_task(reader("svc-B", received_b))
        await await_until(
            lambda: pub.subscriber_count("svc-A") >= 1 and pub.subscriber_count("svc-B") >= 1, timeout=2.0
        )

        pub.publish("svc-A", {"who": "A", "run_id": 1})
        await asyncio.wait_for(task_a, timeout=2.0)
        assert received_b == [] and received_a == [{"who": "A", "run_id": 1}]
        task_b.cancel()
        try:
            await task_b
        except asyncio.CancelledError:
            pass

        gen = pub.subscribe("svc-clean")
        task = asyncio.create_task(gen.__anext__())
        await await_until(lambda: pub.subscriber_count("svc-clean") == 1, timeout=2.0)
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, StopAsyncIteration):
            pass
        await gen.aclose()
        assert pub.subscriber_count("svc-clean") == 0

    @pytest.mark.asyncio
    async def test_overflow_drops_oldest_for_last_write_wins(self, pub_cls):
        pub = pub_cls()
        pub.bind_loop(asyncio.get_running_loop())
        received: list[int] = []

        async def consume_4():
            async for payload in pub.subscribe("svc"):
                received.append(payload["n"])
                if len(received) >= 4:
                    return

        consumer = asyncio.create_task(consume_4())
        await await_until(lambda: pub.subscriber_count("svc") >= 1, timeout=2.0)
        for n in range(10):
            pub.publish("svc", {"n": n})

        await asyncio.wait_for(consumer, timeout=2.0)
        assert len(received) == 4 and received[-1] == 9 and received[0] > 0


def test_log_extents_stream_requires_service_id_and_projects_safe_fields():
    with TestClient(app) as client:
        resp = client.get("/api/log-extents/stream")
        assert resp.status_code == 400 and resp.json()["detail"] == {"error": "service_id_required"}

    full_snapshot = {
        "configured": True,
        "latest_log_at": "2026-06-15T22:46:46+00:00",
        "local_rows": 6_659_858,
        "ngwaf_workspace_id": "example_corp.test",
        "active_run": {"type": "status", "message": "0.6s Processing 12 files..."},
        "cdn_service_id": "ExampleCdnId0000000001",
        "logging_service_id": "ExampleSvcId0000000001",
        "schema": [{"name": "ip", "type": "VARCHAR"}],
        "duckdb_size_bytes": 451_651_568,
        "storage_files": 1238,
    }
    pushed_full = {**full_snapshot, "latest_log_at": "2026-06-15T22:48:00+00:00", "local_rows": 6_660_000}

    from backend.routers.admin import sync_status as router_module

    pushed_event = asyncio.Event()

    async def fake_subscribe(_svc_id):
        for _ in range(20):
            if pushed_event.is_set():
                yield pushed_full
                return
            await asyncio.sleep(0.1)

    def trigger_push():
        loop = router_module.sync_status_publisher._loop
        if loop:
            loop.call_soon_threadsafe(pushed_event.set)

    threading.Timer(0.3, trigger_push).start()
    with (
        patch.object(router_module, "compute_sync_status_cached", return_value=full_snapshot),
        patch.object(router_module.sync_status_publisher, "subscribe", fake_subscribe),
        TestClient(app) as client,
        client.stream("GET", "/api/log-extents/stream", headers={"x-service-id": "svc-analyst"}) as resp,
    ):
        assert resp.status_code == 200
        body = "".join(resp.iter_text())

    events = _parse_sse_data_events(body)
    assert len(events) >= 2
    assert events[0] == {
        "latest_log_at": "2026-06-15T22:46:46+00:00",
        "local_rows": 6_659_858,
        "request": None,
        "rum": None,
    }
    assert {
        "latest_log_at": "2026-06-15T22:48:00+00:00",
        "local_rows": 6_660_000,
        "request": None,
        "rum": None,
    } in events
    for ev in events:
        assert set(ev.keys()) == {"latest_log_at", "local_rows", "request", "rum"}


# ── cron_progress ────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _reset_progress_state():
    for store in (
        cron_progress._progress,
        cron_progress._last_update,
        cron_progress._run_metadata,
        cron_progress._terminal_run_ids,
    ):
        store.clear()
    yield
    for store in (
        cron_progress._progress,
        cron_progress._last_update,
        cron_progress._run_metadata,
        cron_progress._terminal_run_ids,
    ):
        store.clear()


def test_start_add_and_get_progress_lifecycle(monkeypatch):
    cron_progress.start_progress(1, service_id="svc-a", task="sync")
    assert cron_progress._progress[1] == []
    meta = cron_progress._run_metadata[1]
    assert (meta["service_id"], meta["task"]) == ("svc-a", "sync") and isinstance(meta["started_at"], float)

    # Idempotent start_progress preserves existing events and metadata
    cron_progress.add_progress(1, {"type": "step", "i": 1})
    cron_progress.start_progress(1, service_id="other", task="other")
    assert len(cron_progress._progress[1]) == 1 and cron_progress._run_metadata[1]["service_id"] == "svc-a"

    # Unknown run is silently ignored by add_progress and returns None on get_progress
    cron_progress.add_progress(999, {"type": "step"})
    assert 999 not in cron_progress._progress and cron_progress.get_progress(999) is None

    # Slice and copy semantics on get_progress
    for i in range(2, 5):
        cron_progress.add_progress(1, {"type": "step", "i": i})
    snap = cron_progress.get_progress(1)
    assert len(snap) == 4
    assert cron_progress.get_progress(1, start_idx=2) == [{"type": "step", "i": 3}, {"type": "step", "i": 4}]
    cron_progress.add_progress(1, {"type": "step", "i": 5})
    assert len(snap) == 4  # copy not mutated

    # Monotonic timestamp advance on add_progress
    ticks = iter([1_700_000_000.0, 1_700_000_001.0])
    monkeypatch.setattr(cron_progress.time, "time", lambda: next(ticks))
    cron_progress.start_progress(2)
    initial_ts = cron_progress._last_update[2]
    cron_progress.add_progress(2, {"type": "step"})
    assert cron_progress._last_update[2] > initial_ts


def test_get_latest_progress_for_service():
    assert cron_progress.get_latest_progress_for_service("svc-nothing") is None

    # No events yet -> returns task placeholder
    cron_progress.start_progress(1, service_id="svc-a", task="sync_logs")
    assert cron_progress.get_latest_progress_for_service("svc-a") == {"task": "sync_logs"}

    # Active step event includes task
    cron_progress.add_progress(1, {"type": "step", "msg": "downloading"})
    assert cron_progress.get_latest_progress_for_service("svc-a") == {
        "type": "step",
        "msg": "downloading",
        "task": "sync_logs",
    }

    # Newer run_id wins when multiple active; other service runs are ignored
    cron_progress.start_progress(2, service_id="svc-a", task="sync")
    cron_progress.add_progress(2, {"type": "step", "i": "new"})
    cron_progress.start_progress(3, service_id="svc-b", task="sync")
    cron_progress.add_progress(3, {"type": "step", "i": "other"})
    assert cron_progress.get_latest_progress_for_service("svc-a")["i"] == "new"

    # Terminal events (done / error) are skipped
    cron_progress.add_progress(1, {"type": "done", "rows": 100})
    cron_progress.add_progress(2, {"type": "error", "msg": "S3 down"})
    assert cron_progress.get_latest_progress_for_service("svc-a") is None


@pytest.mark.parametrize(
    ("last_event", "final_event", "expected_last_type", "expected_done_count"),
    [
        ({"type": "step"}, {"type": "done", "rows": 42}, "done", 1),
        ({"type": "status", "message": "view refresh: 12ms"}, None, "done", 1),
        ({"type": "done", "rows": 100}, None, "done", 1),
        ({"type": "error", "message": "S3 timeout"}, None, "error", 0),
        ({"type": "status", "message": "x"}, {"type": "done", "rows": 42}, "done", 1),
    ],
)
def test_end_progress_terminal_event_semantics(last_event, final_event, expected_last_type, expected_done_count):
    cron_progress.start_progress(1)
    cron_progress.add_progress(1, last_event)
    if final_event is None:
        cron_progress.end_progress(1)
    else:
        cron_progress.end_progress(1, final_event=final_event)

    events = cron_progress._progress[1]
    assert events[-1].get("type") == expected_last_type
    assert sum(1 for e in events if e.get("type") == "done") == expected_done_count
    assert cron_progress.list_active_runs() == []

    cron_progress.end_progress(999, {"type": "done"})
    assert 999 not in cron_progress._progress


def test_cleanup_progress_and_concurrent_writers():
    cron_progress.cleanup_progress()  # safe on empty state
    cron_progress.start_progress(1)
    cron_progress.start_progress(2)
    cron_progress._last_update[1] = time.time() - 3700
    cron_progress._last_update[2] = time.time() - 3500
    cron_progress.cleanup_progress()
    assert 1 not in cron_progress._progress and 1 not in cron_progress._run_metadata
    assert 2 in cron_progress._progress

    cron_progress.start_progress(3)
    t1 = threading.Thread(target=lambda: [cron_progress.add_progress(3, {"t": "A", "i": i}) for i in range(100)])
    t2 = threading.Thread(target=lambda: [cron_progress.add_progress(3, {"t": "B", "i": i}) for i in range(100)])
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    assert len(cron_progress._progress[3]) == 200


def test_list_active_runs_and_reap_zombies():
    assert cron_progress.list_active_runs() == []

    cron_progress.start_progress(1, service_id="svc-a", task="sync")
    cron_progress.add_progress(1, {"type": "step", "i": 1})
    cron_progress.start_progress(2, service_id="svc-a", task="sync")
    cron_progress.add_progress(2, {"type": "done", "rows": 100})
    cron_progress.start_progress(3, service_id="svc-b", task="sync")
    cron_progress.add_progress(3, {"type": "error", "msg": "S3 timeout"})
    cron_progress.start_progress(4, service_id="svc-b", task="commit")

    out_by_id = {entry["run_id"]: entry for entry in cron_progress.list_active_runs()}
    assert set(out_by_id) == {1, 4}
    assert (out_by_id[1]["service_id"], out_by_id[1]["task"]) == ("svc-a", "sync")
    assert isinstance(out_by_id[1]["started_at"], float)

    # DB cross-check filters terminal runs but keeps None / running
    for db_status, expected_ids in ((None, {1, 4}), ("running", {1, 4}), ("success", set())):
        cron_progress._terminal_run_ids.clear()
        with patch("backend.core.metadata.get_cron_run_status", return_value=db_status):
            assert {e["run_id"] for e in cron_progress.list_active_runs()} == expected_ids

    # Zombie filtering (>5 min stale) and reap_zombie_runs
    cron_progress._terminal_run_ids.clear()
    cron_progress._last_update[1] = time.time() - 400
    assert {e["run_id"] for e in cron_progress.list_active_runs()} == {4}
    assert cron_progress.reap_zombie_runs() == 1
    assert 1 not in cron_progress._run_metadata and 4 in cron_progress._run_metadata
    assert (
        cron_progress._progress[1][-1]["type"] == "error"
        and "zombie" in cron_progress._progress[1][-1]["message"].lower()
    )


# ── RequestMetricsObserver ───────────────────────────────────────────────────


@pytest.fixture
def observer_state(monkeypatch):
    monkeypatch.setattr(module.config, "is_durable_serving_mode", lambda source=None: True)
    monkeypatch.setattr(module.config, "list_configs", lambda: [{"name": "test-service"}])
    monkeypatch.setattr(module.config, "config_to_source", lambda cfg: cfg)
    state = {"request": {"total_rows": 1, "latest_log_at": "2026-09-07T00:00:00Z", "last_sync_at": None}}
    persisted: dict = {}
    calls: list[str] = []

    def refresh(source, **kwargs):
        persisted.clear()
        persisted.update(copy.deepcopy(state))
        calls.append("persist")
        return copy.deepcopy(state)

    def publish(sid, snapshot):
        assert snapshot == persisted
        calls.append("publish")

    monkeypatch.setattr(module, "refresh_durable_request_metrics", refresh)
    monkeypatch.setattr(module, "compute_sync_status_cached", lambda sid: copy.deepcopy(persisted))
    monkeypatch.setattr(module.publisher, "publish", publish)
    return module.RequestMetricsObserver(), state, persisted, calls


def test_observer_persistence_dedup_failure_backoff_and_high_scale_skip(observer_state, monkeypatch, caplog):
    observer, state, persisted, calls = observer_state
    observer.reconcile()
    observer.reconcile()
    assert calls == ["persist", "publish", "persist"]
    state["request"]["last_sync_at"] = "2026-09-08T18:00:00Z"
    observer.reconcile()
    assert calls[-2:] == ["persist", "publish"]
    state["request"].update(latest_log_at=None, total_rows=0)
    observer.reconcile()
    assert calls[-2:] == ["persist", "publish"]

    # Failed observation preserves last success and backs off
    before = copy.deepcopy(persisted)
    calls.clear()
    refresh_fail = MagicMock(side_effect=RuntimeError("unavailable"))
    monkeypatch.setattr(module, "refresh_durable_request_metrics", refresh_fail)
    monkeypatch.setattr(module, "FAILURE_BACKOFF_BASE_S", 10.0)
    observer.reconcile()
    observer.reconcile()
    assert persisted == before and calls == [] and "request_metrics.refresh_failed" in caplog.text
    assert refresh_fail.call_count == 1 and observer._failure_backoff_s["test-service"] == 20.0

    # High-scale registered service is skipped
    refresh_hs = MagicMock()
    monkeypatch.setattr(module, "refresh_durable_request_metrics", refresh_hs)
    monkeypatch.setattr(
        module,
        "get_high_scale_service_registry",
        lambda: type("Registry", (), {"resolve": lambda self, sid: object()})(),
    )
    module.RequestMetricsObserver().reconcile()
    refresh_hs.assert_not_called()


def test_concurrent_passes_coalesce_without_overlapping(observer_state, monkeypatch):
    observer, _, _, calls = observer_state
    entered, release = threading.Event(), threading.Event()
    refresh = module.refresh_durable_request_metrics

    def slow(*args, **kwargs):
        entered.set()
        assert release.wait(2)
        return refresh(*args, **kwargs)

    monkeypatch.setattr(module, "refresh_durable_request_metrics", slow)
    thread = threading.Thread(target=observer.reconcile)
    thread.start()
    try:
        assert entered.wait(2)
        observer.reconcile()
        assert calls == []
    finally:
        release.set()
        thread.join(2)
    assert not thread.is_alive() and calls == ["persist", "publish"]


def test_observer_lifecycle_modes_and_bounded_stop(observer_state, monkeypatch, caplog):
    # Disabled in file mode
    monkeypatch.setattr(module.config, "is_durable_serving_mode", lambda source=None: False)
    file_obs = module.RequestMetricsObserver()
    file_obs.start()
    assert file_obs._thread is None
    file_obs.stop()

    # Periodic reconciliation continues and stops cleanly even with FLA_DEV_NO_CRONS=1
    monkeypatch.setattr(module.config, "is_durable_serving_mode", lambda source=None: True)
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    observer, _, _, calls = observer_state
    monkeypatch.setattr(module, "RECONCILE_INTERVAL_S", 0.01)
    twice = threading.Event()
    orig_reconcile = observer.reconcile

    def observed_pass():
        orig_reconcile()
        if calls.count("persist") >= 2:
            twice.set()

    monkeypatch.setattr(observer, "reconcile", observed_pass)
    observer.start()
    first_thread = observer._thread
    try:
        assert twice.wait(2)
        observer.start()
        assert observer._thread is first_thread
    finally:
        observer.stop()
    assert not first_thread.is_alive() and observer._thread is None and calls.count("publish") == 1

    # Bounded stop timeout prevents overlapping restart
    entered, release = threading.Event(), threading.Event()
    monkeypatch.setattr(module, "STOP_TIMEOUT_S", 0.01)
    monkeypatch.setattr(observer, "reconcile", lambda: (entered.set(), release.wait(2)))
    observer.start()
    blocked_thread = observer._thread
    try:
        assert entered.wait(2)
        observer.stop()
        assert blocked_thread.is_alive()
        observer.start()
        assert observer._thread is blocked_thread and "request_metrics.stop_timeout" in caplog.text
    finally:
        release.set()
        blocked_thread.join(2)
        observer.stop()
    assert observer._thread is None


@pytest.mark.asyncio
async def test_application_lifespan_owns_observer_order(monkeypatch):
    from backend import main

    calls = []

    @asynccontextmanager
    async def application(app):
        calls.append("application_start")
        yield
        calls.append("application_stop")

    monkeypatch.setattr(main, "_application_lifespan", application)
    monkeypatch.setattr(module.observer, "start", lambda: calls.append("observer_start"))
    monkeypatch.setattr(module.observer, "stop", lambda: calls.append("observer_stop"))
    monkeypatch.setattr(main.request_telemetry, "force_flush", lambda: None)
    monkeypatch.setattr(main.clickhouse_observer, "stop_clickhouse_observer", lambda: None)
    monkeypatch.setattr(main.clickhouse_client, "close_clickhouse_client", lambda: None)
    async with main.lifespan(main.app):
        calls.append("serving")
    assert calls == ["application_start", "observer_start", "serving", "observer_stop", "application_stop"]

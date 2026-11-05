"""Consolidated tests for test_main."""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import threading
import time
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException
from starlette.requests import Request

import backend.core.request_context as deps
from backend import config as svcconfig
from backend.core.request_telemetry import (
    _HISTORY_CAP,
    QueryRegistry,
    current_attribution,
    query_registry,
)
from backend.cron.scheduler import global_job
from backend.main import (
    _background_startup,
    _bounded_scheduler_shutdown,
    _ensure_pop_cache,
    _ensure_scoring_matrix,
    _initialize_service,
    _maybe_instrument_asgi,
    health_check,
    telemetry_middleware,
)
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
):
    """Drive health_check(deep=True) against an in-memory metadata.db."""
    con = _make_health_db(service_id=service_id, ingest_minutes_ago=ingest_minutes_ago, cron_rows=cron_rows)
    fake_request = MagicMock()
    with (
        patch("backend.config.list_service_ids", return_value=[service_id]),
        patch("backend.core.metadata.get_con", return_value=con),
        patch("backend.utils.remote_access.is_request_remote", return_value=is_remote),
    ):
        result = health_check(fake_request, deep=True)

    return (200, result) if isinstance(result, dict) else (result.status_code, json.loads(result.body))


def test_default_sse_backplane_resolution(monkeypatch):
    monkeypatch.delenv("SSE_BACKPLANE", raising=False)
    monkeypatch.setattr(svcconfig, "CELERY_BROKER_URL", "")
    assert svcconfig._default_sse_backplane() == "local"

    monkeypatch.setattr(svcconfig, "CELERY_BROKER_URL", "redis://localhost:6379/0")
    assert svcconfig._default_sse_backplane() == "valkey"

    monkeypatch.setenv("SSE_BACKPLANE", "local")
    assert svcconfig._default_sse_backplane() == "local"


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
        patch("backend.core.fos_client.get_source_for_service", **src_kw) as mock_get_src,
        patch("backend.core.fos_client.refresh_config_status") as mock_refresh,
        patch("backend.routers.admin.compute_sync_status_cached") as mock_sync_status,
        patch("backend.core.metadata.usage_log.get_con"),
        patch("backend.main.logger.info") as mock_log_info,
    ):
        _initialize_service(cfg)
    return mock_reap, mock_get_src, mock_refresh, mock_sync_status, mock_log_info


# ── _initialize_service ──────────────────────────────────────────────────────


@pytest.mark.usefixtures("_clean_registry")
def test_initialize_service_skips_when_no_service_id():
    mock_reap, mock_get_src, _, _, _ = _run_init_service({})
    mock_reap.assert_not_called()
    mock_get_src.assert_not_called()


@pytest.mark.usefixtures("_clean_registry")
def test_initialize_service_reaps_orphans_and_refreshes_status():
    fake_src = {"name": "svc-1", "service_id": "svc-1"}
    mock_reap, _, mock_refresh, mock_sync_status, _ = _run_init_service(
        {"service_id": "svc-1"}, reap_ret=3, src_ret=fake_src
    )
    mock_reap.assert_called_once_with("svc-1")
    mock_refresh.assert_called_once_with("svc-1")
    mock_sync_status.assert_called_once_with("svc-1")


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
        patch("backend.core.fos_client.reload_default_source"),
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
        patch("backend.core.fos_client.reload_default_source", side_effect=RuntimeError("S3 down")),
        patch("backend.main._ensure_pop_cache"),
        patch("backend.cron.scheduler.get_scheduler", return_value=fake_scheduler),
        patch("backend.config.list_configs", return_value=[]),
    ):
        _background_startup()
    fake_scheduler.start.assert_called_once()

    with (
        patch("backend.core.fos_client.reload_default_source"),
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


# ── removed symbols ──────────────────────────────────────────────────────────


@pytest.mark.usefixtures("_clean_registry")
@pytest.mark.parametrize("symbol", ["get_meta_con", "AnalyticsDeps", "_ConnectionHolder", "get_con"])
def test_removed_deps_symbols(symbol: str):
    assert not hasattr(deps, symbol), f"{symbol} was removed"


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
def test_history_cap_is_400():
    assert _HISTORY_CAP == 400
    assert QueryRegistry()._history.maxlen == 400


# ── SRE-04 / SRE-05 / SRE-22: deep /api/health ──────────────────────────────


@pytest.mark.usefixtures("_clean_registry")
@pytest.mark.parametrize(
    ("cron_rows", "expected_code", "expected_status", "reason_substr"),
    [
        ([("log_discovery", "success", 2, None), ("ledger_sweep", "success", 3, None)], 200, "ok", None),
        (
            [("log_discovery", "success", 120, None), ("log_discovery", "running", 20, None)],
            503,
            "degraded",
            "stuck running",
        ),
        ([("log_discovery", "success", 30, None), ("log_discovery", "running", 2, None)], 200, "ok", None),
        (
            [("log_discovery", "success", 2, None), ("ledger_sweep", "error", 1, "boom")],
            503,
            "degraded",
            "ledger_sweep cron errored",
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
    for key in ("recent_cron_failures", "observability", "config_backup"):
        assert key in body, f"{key} missing from health-snapshot"
    assert body["observability"] == {"log_format": "console", "otel_exporter": "none"}


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
        patch("backend.core.fos_client.fos_reachable", return_value={"reachable": True, "error": None}),
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
        query_registry.register("ClickHouse", "SELECT slow()", service_id="svc", con=conn, pool_slot="svc#1")
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(telemetry_middleware(_request(), call_next))
    assert conn.interrupts == 1


@pytest.mark.usefixtures("_clean_registry")
def test_a_request_that_completes_normally_interrupts_nothing():
    conn = _Conn()

    async def call_next(request):
        query_registry.register("ClickHouse", "SELECT 1", service_id="svc", con=conn, pool_slot="svc#1")
        return _Resp(200)

    asyncio.run(telemetry_middleware(_request(), call_next))
    assert conn.interrupts == 0


@pytest.mark.usefixtures("_clean_registry")
def test_a_cancelled_request_never_interrupts_another_requests_queries():
    mine, theirs = _Conn(), _Conn()
    other_registered: list[int] = []

    async def other_request_query():
        async def inner(request):
            qid = query_registry.register(
                "ClickHouse", "SELECT other()", service_id="svc", con=theirs, pool_slot="svc#2"
            )
            other_registered.append(qid)
            return _Resp(200)

        await telemetry_middleware(_request("/api/network-health"), inner)

    async def scenario():
        await other_request_query()

        async def call_next(request):
            query_registry.register("ClickHouse", "SELECT mine()", service_id="svc", con=mine, pool_slot="svc#1")
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await telemetry_middleware(_request(), call_next)

    asyncio.run(scenario())
    assert mine.interrupts == 1 and theirs.interrupts == 0


@pytest.mark.usefixtures("_clean_registry")
def test_tcp_disconnect_via_asgi_receive_interrupts_sync_clickhouse_query():
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
        query_registry.register("ClickHouse", "SELECT slow_post()", service_id="svc", con=conn, pool_slot="svc#1")
        nonlocal disconnected
        disconnected = True
        for _ in range(20):
            if conn.interrupts > 0:
                break
            await asyncio.sleep(0.05)
        return _Resp(499)

    asyncio.run(telemetry_middleware(req, call_next))
    assert conn.interrupts == 1


# ── Metadata concurrency & environment connectivity ──

import copy
import importlib
import os
from functools import partial

from fastapi.testclient import TestClient
from scripts.check_environment_health import (
    ENVIRONMENTS,
    check_backend_health,
    check_rbac,
    check_tenancy,
)

import backend.core.metadata as metadata_db
import backend.cron.scheduler as cron_progress
from backend.celery_app import SyncStatusPublisher
from backend.cron.scheduler import CronRunsPublisher, compute_sync_status_cached
from backend.high_scale.archive import ServingWatermark
from backend.high_scale.ingest_controller import HighScaleService
from backend.main import app
from tests._helpers.sources import make_source
from tests._helpers.wire import await_until


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


def test_lake_kind_creates_cache_dirs(tmp_path):
    src = make_source(tmp_path, "svc", kind="lake", buffer_dir=True)
    assert src == {
        "name": "svc",
        "service_id": "svc",
        "fos_local_warehouse": True,
        "_cache_dir_override": str(tmp_path / "cache_svc"),
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
    monkeypatch.setattr("backend.core.fos_client.get_source_for_service", lambda _: {"name": "snapshot-svc"})
    monkeypatch.setattr(
        "backend.high_scale.ingest_controller.get_high_scale_service_registry",
        lambda: type("Registry", (), {"resolve": lambda _, sid: None})(),
    )
    monkeypatch.setattr("backend.cron.scheduler._get_dir_size", lambda _: 0)
    monkeypatch.setattr("backend.core.metadata.get_ingested_files_status_summary", lambda _: summary)
    monkeypatch.setattr("backend.core.metadata.base.get_con", lambda _: ledger)
    monkeypatch.setattr("backend.cron.scheduler.get_latest_progress_for_service", lambda _: None)
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
    monkeypatch.setattr("backend.core.fos_client.get_source_for_service", lambda _: {"name": "high-scale-svc"})
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

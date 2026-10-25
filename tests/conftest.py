import os as _os

# Force OTEL_EXPORTER=none in test environments to avoid blocking/timeouts on shutdown
_os.environ["OTEL_EXPORTER"] = "none"

# 038: enable the telemetry overlay in API responses for the test
# environment so existing assertions on ``_debug_queries`` /
# ``_debug_calls`` keep passing. Production reads the env at
# backend.models.common import time and defaults to "excluded".
# Set it BEFORE the backend imports below so the module-level read
# in backend.models.common picks it up.
_os.environ.setdefault("DEBUG_RESPONSES", "true")
# Force the telemetry-response middleware to inject _debug_queries /
# _debug_calls / _is_cached on every test response. Production gates the
# envelope on an explicit ``x-debug-responses: 1`` header (perf-driven —
# the envelope is 5-40 KB per response and the admin UI only consumes it
# when the Debug Panel toggle is on); tests assert on those keys directly
# and would otherwise have to thread the header through every TestClient
# call. Single env-var escape hatch keeps the prod opt-in load-bearing
# while letting the suite stay terse.
_os.environ.setdefault("DEBUG_RESPONSES_FORCE_INCLUDE", "1")


# Silence the "--- Logging error --- ValueError: I/O operation on closed file"
# tracebacks that pytest's capture plugin provokes. The iceberg
# ``_write_table_summary_async`` background daemon thread (backend/core/
# iceberg/_core.py) keeps running its metadata scan + ``logger.info(...)``
# after the test that spawned it has finished; by then pytest has already
# closed that test item's captured stderr, so the StreamHandler's
# ``stream.write`` raises and ``logging.Handler.handleError`` prints a
# traceback to the real stderr. The daemon thread is correct in production
# (the process lives forever) — this is purely a test-shutdown timing
# artefact, the same class of test-only teardown hygiene as the telemetry-
# proxy close and the sqlite drain below. ``raiseExceptions`` defaults back
# to True in production (we only flip it here), so a genuine handler
# misconfiguration still surfaces outside the suite.
import logging as _logging

_logging.raiseExceptions = False


# NOTE: the historical ``backend.scheduler`` compat shim (and the
# session-start pre-import that defended its binding order under
# pytest-randomly) was retired 2026-07-06 — every caller and test patch
# now targets the real homes under ``backend.cron.*`` directly, so the
# shim-binding pollution class no longer exists.


def _cassette_recorded_at(path) -> float | None:
    """Parse an optional ``recorded_at: <ISO8601>`` line from a VCR cassette.

    Cassettes recorded by ``vcrpy`` don't include this field by default; this
    is an opt-in extension consulted only on platforms where
    ``st_birthtime`` isn't available. Returns POSIX seconds or None.
    """
    import datetime as _dt
    import re

    try:
        with open(path, encoding="utf-8") as f:
            head = f.read()
    except OSError:
        return None
    m = re.search(r"^recorded_at:\s*['\"]?([0-9T:\-+.Z]+)['\"]?$", head, re.MULTILINE)
    if not m:
        return None
    raw = m.group(1).replace("Z", "+00:00")
    try:
        return _dt.datetime.fromisoformat(raw).timestamp()
    except ValueError:
        return None


# ── Hang forensics ───────────────────────────────────────────────────────────
import pytest as _pytest_for_hooks  # noqa: E402 - hook decorators below; `pytest` itself is imported later

# pytest-timeout's "thread" method kills the whole xdist worker (os._exit) when a test passes the
# 60s ceiling, and the stack dump it writes dies with the worker — CI then shows only
# "worker crashed". Dump every thread's stack to a per-worker file shortly BEFORE that, so the next
# CI failure names the blocked frame (the CI "Worker-crash diagnostics" step prints these files).
_HANG_DUMP_AFTER_S = float(_os.environ.get("FLA_HANG_DUMP_AFTER_S", "45"))
_hang_log = None


def _hang_log_file():
    global _hang_log
    if _hang_log is None:
        import tempfile

        path = _os.path.join(tempfile.gettempdir(), f"fla-hang-{_os.getpid()}.log")
        _hang_log = open(path, "a", buffering=1)  # noqa: SIM115 - lives for the worker's lifetime
    return _hang_log


@_pytest_for_hooks.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    import faulthandler

    log = _hang_log_file()
    log.write(f"--- {item.nodeid}\n")
    faulthandler.dump_traceback_later(_HANG_DUMP_AFTER_S, repeat=False, file=log)


@_pytest_for_hooks.hookimpl(trylast=True)
def pytest_runtest_teardown(item, nextitem):  # noqa: ARG001 — pytest hook signature
    import faulthandler

    faulthandler.cancel_dump_traceback_later()


def pytest_sessionstart(session):  # noqa: ARG001 — pytest hook signature
    """R-11: warn at 60 days, fail loudly at 90, on any VCR cassette age.

    Cassettes silently going stale (their upstream API moved on) is the
    failure mode this guards. The 90-day hard gate gives ops room to
    refresh in batches; the 60-day soft warn surfaces upcoming drift on
    the next test run rather than waiting for the hard gate to fire
    mid-CI. To re-record, run the relevant suite with ``--vcr-record=all``:

        pytest tests/core/test_fastly_client_vcr.py --vcr-record=all
        pytest tests/utils/test_ngwaf_vcr.py --vcr-record=all
    """
    import os
    import pathlib
    import sys
    import time

    repo_root = pathlib.Path(__file__).resolve().parent.parent
    cassette_dir = repo_root / "tests" / "cassettes"
    if not cassette_dir.exists():
        return

    threshold_seconds = 90 * 24 * 3600
    warn_seconds = 60 * 24 * 3600
    now = time.time()
    stale: list[tuple[str, int]] = []
    nearing: list[tuple[str, int]] = []
    for path in cassette_dir.rglob("*.yaml"):
        # Freshness reference, in precedence order:
        #   1. An explicit ``recorded_at: <ISO8601>`` line in the cassette
        #      body. Hand-authored fixtures bump this line when the
        #      operator has verified the recording is still accurate.
        #   2. ``st_birthtime`` (macOS, BSD) — set when the file was
        #      created in the working tree.
        #   3. ``st_mtime`` — last-resort fallback for ext4 (< 2.6.39)
        #      and a small number of CI runners. NOTE: this is what the
        #      audit identified as the bug. ``git checkout`` resets
        #      mtime, so it always reports "fresh" right after a branch
        #      switch — defeating the gate. mtime is here only because
        #      something has to be returned; the explicit fields above
        #      are the load-bearing signal.
        stat = os.stat(path)
        ref = _cassette_recorded_at(path)
        if ref is None:
            ref = getattr(stat, "st_birthtime", None)
        if ref is None or ref == 0:
            ref = stat.st_mtime
        age = now - ref
        rel_path = str(path.relative_to(repo_root))
        if age > threshold_seconds:
            stale.append((rel_path, int(age // 86400)))
        elif age > warn_seconds:
            nearing.append((rel_path, int(age // 86400)))

    if nearing and not stale:
        # Surface upcoming drift on the next test run so the hard gate
        # doesn't fire unexpectedly mid-CI. Print to stderr so it shows
        # up in CI logs without being mistaken for a test failure.
        details = "\n".join(f"  {p} ({age_days}d old)" for p, age_days in nearing)
        print(
            "\n⚠️  VCR cassettes are >60 days old (hard gate fires at 90d):\n"
            f"{details}\n"
            "Schedule a re-record session before they hit the 90-day gate.\n",
            file=sys.stderr,
        )

    if stale:
        details = "\n".join(f"  {p} ({age_days}d old)" for p, age_days in stale)
        raise RuntimeError(
            "VCR cassettes are >90 days old — re-record before they drift "
            "from the upstream API:\n"
            f"{details}\n"
            "To refresh:\n"
            "  uv run pytest tests/core/test_fastly_client_vcr.py --vcr-record=all\n"
            "  uv run pytest tests/utils/test_ngwaf_vcr.py --vcr-record=all"
        )


import duckdb
import pytest

from backend.deps import get_con


@pytest.fixture(scope="session", autouse=True)
def _seed_random():
    """Seed Python's stdlib RNG so unseeded callers (notably
    ``tests/_helpers/duckdb_fixtures.py``) are reproducible.

    Per-test code that explicitly seeds its own ``random.Random()`` (hypothesis,
    fuzz suites) is unaffected — those create their own Random instances.
    """
    import random

    random.seed(42)


@pytest.fixture(scope="session", autouse=True)
def _mock_disk_usage():
    """Mock shutil.disk_usage globally so tests are robust against host disk pressure."""
    import shutil
    from collections import namedtuple

    usage_tuple = namedtuple("usage", ["total", "used", "free"])
    # Mock a large disk: 500 GB total, 450 GB free (90% free space)
    mocked_usage = usage_tuple(500 * 1024 * 1024 * 1024, 50 * 1024 * 1024 * 1024, 450 * 1024 * 1024 * 1024)

    original_disk_usage = shutil.disk_usage
    shutil.disk_usage = lambda path: mocked_usage
    yield
    shutil.disk_usage = original_disk_usage


@pytest.fixture(scope="session", autouse=True)
def _close_telemetry_proxy_at_session_end():
    """Close the local telemetry proxy's aiohttp session once per worker at
    session teardown.

    The proxy is a process-lifetime singleton: ``start_proxy_server`` is lazy
    and idempotent (FastAPI lifespan, DuckDB httpfs, and PyIceberg all call it),
    so a given xdist worker starts at most one ``aiohttp.ClientSession`` and
    never stops it — in production that's fine, the process just exits. Under
    pytest the worker's interpreter shuts down *after* pytest has torn down its
    log-capture stream, so aiohttp's ``__del__`` "Unclosed client session" /
    "Unclosed connector" warnings fire into a closed stream and the logging
    module prints a secondary "--- Logging error --- ValueError: I/O operation
    on closed file". Closing the session here (loop still running, capture still
    live) removes both. Same class of test-only teardown hygiene as the
    sqlite drain in ``isolate_metadata_db`` and the ResourceWarning filters in
    pyproject.toml.
    """
    yield
    try:
        from backend.utils import telemetry_proxy

        telemetry_proxy.stop_proxy_server()
    except Exception:
        pass


@pytest.fixture(autouse=True)
def reset_telemetry():
    """Reset the global telemetry ContextVars before every test."""
    from backend.utils.telemetry import start_call_tracking

    start_call_tracking()


_PG_SLOT_LOCK_BASE = 0x464C4100


@pytest.fixture(scope="session")
def _pg_worker_schema():
    """Give this pytest process its own Postgres database for metadata storage.

    The process claims an exclusive database slot (see below), resets it,
    points ``METADATA_DSN`` / ``DUCKLAKE_CATALOG`` at it, then runs the real
    schema bootstrap
    (:func:`backend.core.metadata.pg_schema.ensure_pg_schema`) against it.
    Two workers — or two concurrent pytest runs — therefore never see each
    other's rows, and share nothing but the underlying server.

    Runs once per worker process, before any test in that worker touches
    metadata — the pool in ``pg_connection`` is a lazy module-level
    singleton created on first ``get_pg_pool()`` call, so ``METADATA_DSN``
    must be set before that first call happens anywhere in this process.
    """
    import psycopg

    from backend.core.metadata import pg_connection, pg_schema

    base_dsn = _os.environ.get(
        "METADATA_DSN",
        "postgresql://fla:fla_test_password@localhost:5432/ducklake_test",
    )
    from urllib.parse import urlparse, urlunparse

    parsed = urlparse(base_dsn)
    maint_dsn = urlunparse(parsed._replace(path="/postgres"))

    import time

    admin_conn = psycopg.connect(maint_dsn, autocommit=True)
    # Claim a database slot with a session-level advisory lock (released when the
    # connection closes, even if the process dies). Naming the database after the
    # xdist worker id alone made two concurrent pytest runs (parallel agents,
    # ``make ci`` beside a quick loop) share ``ducklake_test_gw0`` and wipe each
    # other's live connections; slots are exclusive per live process instead.
    # Slots are reset in place, never dropped: DROP DATABASE waits on a forced
    # checkpoint, which stalls for minutes on a busy server.
    slot = 0
    while not admin_conn.execute("SELECT pg_try_advisory_lock(%s)", (_PG_SLOT_LOCK_BASE + slot,)).fetchone()[0]:
        slot += 1
    worker_db = f"ducklake_test_slot{slot}"
    if not admin_conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (worker_db,)).fetchone():
        # CREATE DATABASE clones template1 and fails with ObjectInUse while another
        # session is cloning it; wait that out.
        deadline = time.monotonic() + 60
        while True:
            try:
                admin_conn.execute(f'CREATE DATABASE "{worker_db}"')
                break
            except psycopg.errors.ObjectInUse:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.25)

    worker_dsn = urlunparse(parsed._replace(path=f"/{worker_db}"))
    with psycopg.connect(worker_dsn, autocommit=True) as reset_conn:
        for (schema,) in reset_conn.execute(
            "SELECT nspname FROM pg_namespace WHERE nspname NOT LIKE 'pg\\_%' AND nspname <> 'information_schema'"
        ).fetchall():
            reset_conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        reset_conn.execute("CREATE SCHEMA public")

    _os.environ["METADATA_DSN"] = worker_dsn
    _os.environ["DUCKLAKE_CATALOG"] = worker_dsn

    pg_schema.ensure_pg_schema(force=True)

    yield worker_db, admin_conn

    pg_connection.close_all_pg_connections()
    pg_connection.reset_pg_pool_for_tests()
    admin_conn.close()


_PER_TEST_RESET_TABLES = (
    "ingest_ledger",
    "ingested_files",
    "ingested_files_summary",
    "quarantined_files",
    "quarantine_evidence",
    "job_runs",
    "cron_runs",
    "audit_logs",
    "views",
    "alerts",
    "scoring_labels",
    "scoring_audit",
    "asn_names",
    "usage_log",
    "usage_log_hourly_summary",
    "metric_snapshots",
    "slow_queries",
    "ingest_in_flight",
    "ngwaf_bots",
    "ngwaf_sync_state",
    "rdns",
    "sources",
    "remote_sessions",
    "remote_invite_claim_tokens",
    "invite_services",
    "remote_invites",
    "remote_share_audit_logs",
    "share_settings",
)


@pytest.fixture(autouse=True)
def isolate_metadata_db(tmp_path, monkeypatch, _pg_worker_schema):
    """Point metadata storage at this worker's isolated Postgres schema, and
    service-config/DuckDB data dirs at a per-test sandbox.

    Operational metadata (alerts, views, audit, cron, sources, ingested_files,
    asn_names, usage_log, share_db) lives in Postgres, scoped to this xdist
    worker's own schema by :func:`_pg_worker_schema` — replacing the historical
    per-service SQLite file at ``data/services/{id}.metadata.db`` (see ADR-15,
    reversed by the SQLite -> Postgres metadata migration this fixture is
    part of).

    The analytical DuckDB file lives at ``data/services/{id}.duckdb`` and is
    located via :func:`backend.config.duckdb_path` (which reads
    ``backend.config.SERVICES_DATA_DIR``). Without redirecting that too,
    tests that exercise ``svcconfig.duckdb_path`` or any code path that
    opens a per-service DuckDB connection by service ID will write into the
    real ``data/services/`` directory and leave behind ``svc1.duckdb``,
    ``test-service-id.duckdb``, etc. Patch the four config-level DATA paths
    in parallel so the whole tree is sandboxed.
    """
    import os
    from pathlib import Path

    from backend import config as svcconfig
    from backend.core import metadata as metadata_db

    sandbox_data = Path(tmp_path) / "data"
    sandbox_services = sandbox_data / "services"
    sandbox_configs = Path(tmp_path) / "configs"
    sandbox_ngwaf = sandbox_data / "ngwaf"
    sandbox_cache = sandbox_data / "cache"
    sandbox_system = sandbox_data / "system"

    # Pre-create the sandbox tree. ``backend.config._ensure_dirs`` calls
    # ``mkdir(exist_ok=True)`` (no ``parents=True``) so nested dirs trip
    # FileNotFoundError when only ``tmp_path`` exists. Some call sites
    # (e.g. routers that touch usage_logging) write into SERVICES_DATA_DIR
    # without going through _ensure_dirs at all, so we pre-create here.
    for d in (sandbox_data, sandbox_services, sandbox_configs, sandbox_ngwaf, sandbox_cache, sandbox_system):
        d.mkdir(parents=True, exist_ok=True)

    try:
        from backend.core.metadata import pg_connection

        _pg_con = pg_connection.get_pg_thread_connection()
        # Truncate only the tables that hold rows: a TRUNCATE recreates every listed relation, and
        # doing all of them per test across N xdist workers made each reset take seconds.
        _dirty = [
            r[0]
            for r in _pg_con.execute(
                " UNION ALL ".join(f"SELECT '{t}' WHERE EXISTS (SELECT 1 FROM {t})" for t in _PER_TEST_RESET_TABLES)
            ).fetchall()
        ]
        if _dirty:
            _pg_con.execute(f"TRUNCATE TABLE {', '.join(_dirty)} CASCADE")
        _pg_con.commit()
    except Exception:
        try:
            _pg_con.rollback()
        except Exception:
            pass

    metadata_db._clear_ingested_filenames_cache()

    # ``usage_log_db.db_path`` still derives from its own ``_DATA_DIR`` binding.
    from backend.core.metadata import usage_log_db as _usage_log_db
    from backend.core.share_db import connection as _share_db_connection

    monkeypatch.setattr(_usage_log_db, "_DATA_DIR", str(sandbox_services))

    from backend.core import metric_snapshots as _metric_snapshots

    monkeypatch.setattr(svcconfig, "DATA_DIR", sandbox_data)
    monkeypatch.setattr(svcconfig, "SERVICES_DATA_DIR", sandbox_services)
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", sandbox_configs)
    monkeypatch.setattr(svcconfig, "NGWAF_DATA_DIR", sandbox_ngwaf)
    monkeypatch.setattr(svcconfig, "CACHE_DATA_DIR", sandbox_cache)
    monkeypatch.setattr(svcconfig, "DUCKLAKE_CATALOG", os.environ["DUCKLAKE_CATALOG"])
    monkeypatch.setattr(svcconfig, "SYSTEM_DATA_DIR", sandbox_system)
    # ``_ensured_dirs`` is a per-path memo for the mkdir storm — must be
    # cleared so _ensure_dirs() actually creates the new sandbox dirs.
    monkeypatch.setattr(svcconfig, "_ensured_dirs", set())

    # ``backend.core.duckdb._cache_dir`` builds a *relative* ``cache/<bucket>``
    # path (deliberate, for Mac↔Docker portability of DuckDB views). Without
    # redirection, tests that exercise the cache path drop ``mock-bucket``,
    # ``my-bucket``, ``test-bucket`` directories into the repo root.
    # Redirect to the per-test sandbox while preserving the
    # ``_cache_dir_override`` escape hatch and the ``bucket`` shape.
    from backend.core import duckdb as _bk_duckdb

    sandbox_cache_root = sandbox_data / "cache"

    def _sandboxed_cache_dir(source: dict) -> str:
        if "_cache_dir_override" in source:
            return source["_cache_dir_override"]
        bucket = (source.get("bucket") or source.get("fos_bucket") or "default").strip()
        return os.path.join(str(sandbox_cache_root), bucket)

    monkeypatch.setattr(_bk_duckdb, "_cache_dir", _sandboxed_cache_dir)

    yield
    # Close every connection across every thread — FastAPI's TestClient
    # spawns worker threads that open their own thread-local connections,
    # invisible to this fixture's ``_local``. Without this drain, those
    # connections live until GC and emit ResourceWarning at process exit.
    # Postgres-aware: ``metadata_db.close_all_connections()`` (and the
    # share_db one) dispatch to ``pg_connection.close_all_pg_connections()``
    # under ``METADATA_DSN`` — same call, now returns pool connections
    # instead of closing SQLite file handles.
    metadata_db.close_all_connections()
    _metric_snapshots.close_all_connections()
    _usage_log_db.close_all_connections()
    _share_db_connection.close_all_connections()


@pytest.fixture(autouse=True)
def _reset_module_caches():
    """Reset module-level caches between tests.

    Several modules cache state at import scope (schema metadata, FOS client,
    iceberg views/snapshots, source registration). Across tests using the
    same source name with different in-memory DuckDB connections, that cache
    leaks and produces order-dependent failures — surfaced by
    ``pytest-randomly`` (Milestone A, task 0.3).

    Some individual tests (e.g. ``test_integration_custom_fields.py``,
    ``test_dashboard.py``) used to clear these inline. Clearing once here
    means they no longer have to.
    """
    # A-3: drain every cache registered via
    # backend.utils.bounded_cache.CacheRegistry. The iceberg, duckdb,
    # dashboard, and network modules all register their caches at
    # module load — this single call replaces the long enumeration
    # the R-1 work expanded. New module-level caches that ship with a
    # register() call are picked up automatically.
    from backend.utils.bounded_cache import CacheRegistry

    CacheRegistry.clear_all()

    # All previously hand-cleared module caches (_logging_settings_cache,
    # _enforce_threshold_cache, active_requests._active_count via the
    # _ActiveRequestsResetAdapter) now register with CacheRegistry from
    # their owning modules — see R-1 in the audit. CacheRegistry.clear_all()
    # above drains them. New module-level caches that ship with a
    # register() call are picked up automatically.
    yield
    from backend.main import app

    app.dependency_overrides.clear()


@pytest.fixture
def in_memory_duckdb():
    """In-memory DuckDB for tests that exercise the analytical engine.

    No operational tables are created here — those live in per-service SQLite
    via the ``isolate_metadata_db`` fixture (autouse).
    """
    con = duckdb.connect(":memory:")
    yield con
    con.close()


def override_request_context(*, source, con=None, _con_override=None, session=None, path="/test", time_bounds=None):
    if _con_override is not None:
        con = _con_override
    """Return a ``build_request_context`` override that yields a RequestContext
    wired to ``source``/``con`` (read-only). Mirrors what the ``client`` fixture
    installs; router tests use it to inject a custom source or an analyst
    session without re-declaring the generator inline.

    ``time_bounds`` is the analyst clamp window that production resolves via
    ``get_analyst_time_bounds(request)`` inside ``build_request_context``;
    handlers read it through ``ctx.clamp``. Pass it here to exercise the
    analyst-clamp path (defaults to an open window when omitted).
    """
    from backend.core.request_context import RequestContext
    from backend.core.request_telemetry import RequestTelemetry

    def _override():
        yield RequestContext(
            service_id=source["service_id"],
            source=source,
            _con_override=con,
            telemetry=RequestTelemetry(request_method="POST", request_path=path),
            analyst_session=session,
            read_only=True,
            time_bounds=time_bounds,
        )

    return _override


def make_test_request_context(
    con=None,
    *,
    service_id: str = "test-service-id",
    source: dict | None = None,
    session=None,
    read_only: bool = True,
    path: str = "/test",
    time_bounds=None,
    holder=None,
):
    """Construct a RequestContext for direct repository/router handler unit tests."""
    from backend.core.request_context import RequestContext
    from backend.core.request_telemetry import RequestTelemetry

    resolved_source = source or {"name": "test_service", "service_id": service_id}
    return RequestContext(
        service_id=resolved_source.get("service_id", service_id),
        source=resolved_source,
        _con_override=con,
        _holder=holder,
        telemetry=RequestTelemetry(request_method="POST", request_path=path),
        analyst_session=session,
        read_only=read_only,
        time_bounds=time_bounds,
    )


@pytest.fixture
def client(in_memory_duckdb, test_service_source):
    from fastapi.testclient import TestClient

    from backend.core.request_context import build_request_context
    from backend.deps import get_service_id, get_source
    from backend.main import app

    app.dependency_overrides[get_con] = lambda: in_memory_duckdb
    app.dependency_overrides[get_source] = lambda: test_service_source
    # ``get_service_id`` resolves from query/header/active-config. Under the
    # sandbox ``CONFIGS_DIR`` (isolate_metadata_db) there's no active config,
    # so without this override every ``Depends(get_service_id)`` route returns
    # ``configured=False`` before the test's patches get a chance to run.
    app.dependency_overrides[get_service_id] = lambda: test_service_source["service_id"]

    # Routers migrated to ``RequestContext`` (Phase 8 v2.0 cut) get their
    # connection + source via ``build_request_context``, which inlines its
    # own source resolution + opens its own connection — it does NOT honour
    # the ``get_source``/``get_con`` overrides above. Provide an equivalent
    # override that returns a RequestContext wired to the same in-memory
    # fixtures so dashboard / query / security / etc. tests keep working.
    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source, con=in_memory_duckdb
    )

    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def test_service_source():
    """Minimal source dict for tests.

    ``name`` is the SQL-safe table identifier; ``service_id`` is the Fastly
    logging service ID (also keys the per-service metadata SQLite file).
    """
    return {"name": "test_service", "service_id": "test-service-id"}


MOCK_SERVICE_ID = "test-service-id"


# ── moto-backed S3 fixtures ───────────────────────────────────────────────────
#
# A single shared S3 mock replaces the four ad-hoc boto3-mocking idioms that
# accumulated across the suite. Tests that need a "real" S3 endpoint to
# exercise ingest/iceberg/state_sync paths should depend on `s3_mock` for the
# bucket and `fos_source` for a source dict shaped like the real config.


@pytest.fixture
def s3_mock(monkeypatch):
    """Stand up a moto S3 endpoint with one empty bucket named ``test-bucket``.

    Yields the boto3 client so individual tests can seed keys.

    ``_get_fos_client`` is patched to return a moto-bound client directly so
    tests don't need to stand up the local telemetry proxy. Production code
    routes boto3 through the proxy (which would forward to the real FOS host
    moto can't intercept here), so we short-circuit at the factory.
    """
    # Deferred imports — boto3+moto pull ~1.2s of cold start each. With xdist
    # spawning 10 workers, that's ~12s of CPU paid up front even on test runs
    # that never touch S3. Most of the suite doesn't use this fixture.
    import boto3
    from moto import mock_aws

    monkeypatch.setenv("MOTO_S3_CUSTOM_ENDPOINTS", "https://us-east-1.object.fastlystorage.app")
    with mock_aws():
        client = boto3.client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
        )
        client.create_bucket(Bucket="test-bucket")
        monkeypatch.setattr("backend.core.duckdb._get_fos_client", lambda _src: client)
        yield client


@pytest.fixture
def fos_source(s3_mock):
    """A source dict shaped like a real Fastly Object Storage source config.

    Pairs with ``s3_mock``: the access keys / region / bucket / endpoint all
    point at moto's in-memory S3. Tests that need extra keys (e.g. CDN
    routing, custom prefix) should ``{**fos_source, "cdn_url": "..."}``.
    """
    return {
        "name": "test_service",
        "service_id": "test-service-id",
        "service_name": "Test Service",
        "bucket": "test-bucket",
        "prefix": "",
        "region": "us-east-1",
        "endpoint": "us-east-1.object.fastlystorage.app",
        "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
        "access_key_id": "test-key",
        "secret_access_key": "test-secret",
        "access_level": "read_write",
        "storage_mode": "cloud",
    }


# Deterministic "now" for the active-hour seam tests: hour 12 (so "today" has a
# closed hour to merge — kills the UTC-hour-00 skip) and minute 10 (so a 20-min
# active window ends at :30, comfortably inside the hour — kills the latent
# hour-rollover flake). The date is arbitrary; the in-memory harness has no
# real-time dependency (bundles live in tmp; queries use seeded timestamps).
_FROZEN_NOW = "2026-06-15 12:10:00+00:00"


@pytest.fixture
def frozen_clock():
    """Freeze ``datetime.now`` (incl. the reader's function-local import) at
    :data:`_FROZEN_NOW` so the closed/active-hour seam is deterministic."""
    import time_machine

    with time_machine.travel(_FROZEN_NOW, tick=False) as traveller:
        yield traveller


@pytest.fixture
def isolated_configs_dir(tmp_path, monkeypatch):
    """Redirect ``backend.config.CONFIGS_DIR`` and ``DATA_DIR`` to ``tmp_path``."""
    from backend import config as svcconfig

    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir(exist_ok=True)
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", cfg_dir)
    data_dir = tmp_path / "data"
    data_dir.mkdir(exist_ok=True)
    monkeypatch.setattr(svcconfig, "DATA_DIR", data_dir)
    return cfg_dir


@pytest.fixture
def override_source():
    """Override ``backend.deps.get_source`` with a default read-write test source."""
    from backend.deps import get_source
    from backend.main import app

    src = {
        "service_id": "service123",
        "name": "service123",
        "bucket": "test-bucket",
        "prefix": "raw_prefix",
        "access_level": "read_write",
        "cdn_url": "",
    }
    app.dependency_overrides[get_source] = lambda: src
    yield src
    app.dependency_overrides.clear()


@pytest.fixture(autouse=True)
def _reset_oauth_caches(request: pytest.FixtureRequest):
    """Reset OIDC discovery/JWKS and provider registry caches around remote_access tests."""
    if "remote_access" not in request.node.nodeid:
        yield
        return
    from backend.core.oauth import client as oidc
    from backend.core.oauth import registry

    registry.reset_cache_for_tests()
    oidc.reset_caches_for_tests()
    yield
    oidc.set_test_transport(None)
    registry.reset_cache_for_tests()
    oidc.reset_caches_for_tests()


from tests._helpers.config_fakes import with_config  # noqa: E402, F401

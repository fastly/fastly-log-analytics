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
# tracebacks that pytest's capture plugin provokes. Background startup
# daemon threads may keep running a metadata scan + ``logger.info(...)``
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


import pytest


@pytest.fixture(scope="session", autouse=True)
def _seed_random():
    """Seed Python's stdlib RNG so unseeded callers are reproducible.

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
    """
    yield
    try:
        import backend.utils.telemetry as telemetry_proxy

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
    points ``METADATA_DSN`` at it, then runs the real schema bootstrap
    (:func:`backend.core.metadata.base.ensure_pg_schema`) against it.
    Two workers — or two concurrent pytest runs — therefore never see each
    other's rows, and share nothing but the underlying server.
    """
    import psycopg

    import backend.core.metadata.base as pg_connection
    import backend.core.metadata.base as pg_schema

    base_dsn = _os.environ.get(
        "METADATA_DSN",
        "postgresql://fla:fla_test_password@localhost:5432/fla_test",
    )
    from urllib.parse import urlparse, urlunparse

    parsed = urlparse(base_dsn)
    maint_dsn = urlunparse(parsed._replace(path="/postgres"))

    import time

    admin_conn = psycopg.connect(maint_dsn, autocommit=True)
    slot = 0
    while not admin_conn.execute("SELECT pg_try_advisory_lock(%s)", (_PG_SLOT_LOCK_BASE + slot,)).fetchone()[0]:
        slot += 1
    worker_db = f"fla_test_slot{slot}"
    for db_to_ensure in ("fla_test", worker_db):
        if not admin_conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_to_ensure,)).fetchone():
            deadline = time.monotonic() + 60
            while True:
                try:
                    admin_conn.execute(f'CREATE DATABASE "{db_to_ensure}"')
                    break
                except (psycopg.errors.ObjectInUse, psycopg.errors.DuplicateDatabase):
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
    "cloud_copy_deletions",
    "metric_snapshots",
    "slow_queries",
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
    service-config/data dirs at a per-test sandbox.
    """
    import os
    from pathlib import Path

    import backend.core.metadata as metadata_db
    from backend import config as svcconfig

    sandbox_data = Path(tmp_path) / "data"
    sandbox_services = sandbox_data / "services"
    sandbox_configs = Path(tmp_path) / "configs"
    sandbox_ngwaf = sandbox_data / "ngwaf"
    sandbox_cache = sandbox_data / "cache"
    sandbox_system = sandbox_data / "system"

    for d in (sandbox_data, sandbox_services, sandbox_configs, sandbox_ngwaf, sandbox_cache, sandbox_system):
        d.mkdir(parents=True, exist_ok=True)

    try:
        import backend.core.metadata.base as pg_connection

        _pg_con = pg_connection.get_pg_thread_connection()
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

    import backend.core.metadata.usage_log as _usage_log_db
    import backend.core.share_db as _share_db_connection

    monkeypatch.setattr(svcconfig, "DATA_DIR", sandbox_data)
    monkeypatch.setattr(svcconfig, "SERVICES_DATA_DIR", sandbox_services)
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", sandbox_configs)
    monkeypatch.setattr(svcconfig, "NGWAF_DATA_DIR", sandbox_ngwaf)
    monkeypatch.setattr(svcconfig, "CACHE_DATA_DIR", sandbox_cache)
    monkeypatch.setattr(svcconfig, "SYSTEM_DATA_DIR", sandbox_system)
    monkeypatch.setattr(svcconfig, "_ensured_dirs", set())

    from backend.core import fos_client as _bk_fos_client

    sandbox_cache_root = sandbox_data / "cache"

    def _sandboxed_cache_dir(source: dict | None = None) -> str:
        src = source or _bk_fos_client._DEFAULT_SOURCE
        if "_cache_dir_override" in src:
            return src["_cache_dir_override"]
        bucket = (src.get("bucket") or src.get("fos_bucket") or "default").strip()
        return os.path.join(str(sandbox_cache_root), bucket)

    monkeypatch.setattr(_bk_fos_client, "_cache_dir", _sandboxed_cache_dir)

    yield
    metadata_db.close_all_connections()
    _usage_log_db.close_all_connections()
    _share_db_connection.close_all_connections()


@pytest.fixture(autouse=True)
def _reset_module_caches():
    """Reset module-level caches between tests."""
    from backend.utils.bounded_cache import CacheRegistry

    CacheRegistry.clear_all()
    yield
    from backend.main import app

    app.dependency_overrides.clear()


def override_request_context(*, source, con=None, _con_override=None, session=None, path="/test", time_bounds=None):
    """Return a ``build_request_context`` override that yields a RequestContext
    wired to ``source`` (read-only). Mirrors what the ``client`` fixture
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
        telemetry=RequestTelemetry(request_method="POST", request_path=path),
        analyst_session=session,
        read_only=read_only,
        time_bounds=time_bounds,
    )


@pytest.fixture
def client(test_service_source):
    from fastapi.testclient import TestClient

    from backend.core.request_context import build_request_context, get_service_id, get_source
    from backend.main import app

    app.dependency_overrides[get_source] = lambda: test_service_source
    app.dependency_overrides[get_service_id] = lambda: test_service_source["service_id"]
    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source,
    )

    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def test_service_source():
    """Minimal source dict for tests.

    ``name`` is the SQL-safe table identifier; ``service_id`` is the Fastly
    logging service ID.
    """
    return {"name": "test_service", "service_id": "test-service-id"}


@pytest.fixture
def service_source(test_service_source):
    """Alias for ``test_service_source``."""
    return test_service_source


MOCK_SERVICE_ID = "test-service-id"


# ── moto-backed S3 fixtures ───────────────────────────────────────────────────
#
# A single shared S3 mock replaces the four ad-hoc boto3-mocking idioms that
# accumulated across the suite. Tests that need a "real" S3 endpoint to
# exercise ingest/storage/state_sync paths should depend on `s3_mock` for the
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
        monkeypatch.setattr("backend.core.fos_client._get_fos_client", lambda _src: client)
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
    """Override ``backend.core.request_context.get_source`` with a default read-write test source."""
    from backend.core.request_context import get_source
    from backend.main import app

    src = {
        "service_id": "service123",
        "name": "service123",
        "bucket": "test-bucket",
        "prefix": "raw_prefix",
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
    import backend.core.oauth as oidc
    import backend.core.oauth as registry

    registry.reset_cache_for_tests()
    oidc.reset_caches_for_tests()
    yield
    oidc.set_test_transport(None)
    registry.reset_cache_for_tests()
    oidc.reset_caches_for_tests()


from tests._helpers.config_fakes import with_config  # noqa: E402, F401

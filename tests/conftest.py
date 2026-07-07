import os as _os

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


# Same daemon-thread root cause as the logging block above: the
# ``_write_table_summary_async`` thread opens a pyiceberg SqlCatalog, whose
# sqlite3 connection GCs *unclosed* after the spawning test finishes (the pool's
# close_all() deliberately leaves live foreign-thread handles alone — closing a
# sqlite handle from another live thread segfaults; see
# backend/core/sqlite_pool.py::ThreadLocalPool.close_all). pyproject's
# ``filterwarnings`` already ignores this ResourceWarning, but that filter is
# only active INSIDE a test's warning-filter context; pytest's
# ``unraisableexception`` plugin runs ``gc.collect()`` BETWEEN tests, outside
# that context, so the "unclosed database" warning leaks to the console. Install
# the same ignore at the process level so it also covers the inter-test gc path.
# Benign: production holds its connections for the whole process lifetime.
import warnings as _warnings

_warnings.filterwarnings("ignore", message="unclosed database", category=ResourceWarning)


# NOTE: the historical ``backend.scheduler`` compat shim (and the
# session-start pre-import that defended its binding order under
# pytest-randomly) was retired 2026-07-06 — every caller and test patch
# now targets the real homes under ``backend.cron.*`` directly, so the
# shim-binding pollution class no longer exists.


def pytest_configure(config):
    """Stop pytest 9's ``unraisableexception`` plugin from FORCE-running
    ``gc.collect()`` at session end (per xdist worker).

    That forced collection is the *only* thing that surfaces the benign
    "unclosed database" ResourceWarning from pyiceberg's cached ``SqlCatalog``:
    a catalog evicted from ``_catalog_cache`` becomes cyclic garbage, and the
    plugin's ``gc_collect_harder`` reaps it from inside a
    ``catch_warnings(record=True)`` context whose ``simplefilter("always")``
    overrides Python's *default* ``ignore::ResourceWarning`` — at a point where
    the pyproject / line-53 "unclosed database" filters are no longer applied.
    Every surfaced warning is sourced to ``unraisableexception.py:33
    gc.collect()`` precisely because of this forced pass.

    Left to natural collection (a later auto-gc inside a test's filter context,
    or interpreter shutdown), those connections are reaped under filters where
    ResourceWarning is ignored — silent. ``sys.unraisablehook`` stays installed,
    so genuine unraisable exceptions are still caught when they occur; we only
    skip the *extra forced* gc passes. ``gc_collect_iterations`` is a documented,
    overridable stash key (pytest marks it "not a simple constant ... to allow
    pytester to override it"). Guarded so a future pytest that renames/removes
    the key degrades to the line-53 filter rather than failing collection.
    """
    try:
        from _pytest.unraisableexception import gc_collect_iterations_key

        config.stash[gc_collect_iterations_key] = 0
    except Exception:
        pass


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
            head = f.read(2048)
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
    ``tests/utils/mock_data.py``) are reproducible.

    Per-test code that explicitly seeds its own ``random.Random()`` (hypothesis,
    fuzz suites) is unaffected — those create their own Random instances.
    """
    import random

    random.seed(42)


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


@pytest.fixture(autouse=True)
def isolate_metadata_db(tmp_path, monkeypatch):
    """Point metadata_db AND service-config data dirs at a per-test sandbox.

    Operational metadata (alerts, views, audit, cron, sources, ingested_files,
    asn_names, usage_log) all live in per-service SQLite at
    ``data/services/{id}.metadata.db``.

    The analytical DuckDB file lives at ``data/services/{id}.duckdb`` and is
    located via :func:`backend.config.duckdb_path` (which reads
    ``backend.config.SERVICES_DATA_DIR``). Without redirecting that too,
    tests that exercise ``svcconfig.duckdb_path`` or any code path that
    opens a per-service DuckDB connection by service ID will write into the
    real ``data/services/`` directory and leave behind ``svc1.duckdb``,
    ``test-service-id.duckdb``, etc. Patch the four config-level DATA paths
    in parallel so the whole tree is sandboxed.
    """
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

    monkeypatch.setattr(metadata_db, "_DATA_DIR", str(sandbox_services))
    monkeypatch.setattr(metadata_db, "_initialized", set())
    monkeypatch.setattr(metadata_db, "_local", __import__("threading").local())
    metadata_db._clear_ingested_filenames_cache()

    # Per-service usage_log lives in its own SQLite file post-2026-06-12;
    # it shares ``_DATA_DIR`` with metadata.db but uses its own thread-
    # local pool + initialised-paths set, so isolate those too. Without
    # this a test would either (a) collide on a real-disk file because
    # _DATA_DIR was already cached, or (b) leak thread-local connections
    # across test runs and emit ResourceWarning on shutdown.
    from backend.core.metadata import usage_log_db as _usage_log_db

    monkeypatch.setattr(_usage_log_db, "_DATA_DIR", str(sandbox_services))
    monkeypatch.setattr(_usage_log_db, "_initialized", set())
    monkeypatch.setattr(_usage_log_db, "_local", __import__("threading").local())

    # Sandbox share_db connection pool and directory
    from backend.core.share_db import connection as _share_db_connection

    monkeypatch.setattr(_share_db_connection, "_DATA_DIR", str(sandbox_system))
    monkeypatch.setattr(_share_db_connection, "_initialized", set())
    monkeypatch.setattr(_share_db_connection, "_local", __import__("threading").local())

    # system_metrics.db (operational-vital snapshots) lives in
    # ``data/system/system_metrics.db``. Redirect to the sandbox so tests
    # can't leak rows into the real-disk file across runs.
    from backend.core import metric_snapshots as _metric_snapshots

    monkeypatch.setattr(_metric_snapshots, "_DATA_DIR", str(sandbox_system))
    monkeypatch.setattr(_metric_snapshots, "_initialized", False)
    monkeypatch.setattr(_metric_snapshots, "_local", __import__("threading").local())

    # rdns_cache hardcodes a RELATIVE _DB_PATH (data/cache/rdns_cache.db)
    # decoupled from svcconfig.CACHE_DATA_DIR, so redirect it explicitly to
    # the per-test sandbox. Otherwise every xdist worker process shares the
    # one real-disk file → cross-process WAL contention ("database is
    # locked") and leaked rows in the repo tree.
    from backend.utils import rdns_cache as _rdns_cache

    monkeypatch.setattr(_rdns_cache, "_DB_PATH", Path(sandbox_cache) / "rdns_cache.db")

    monkeypatch.setattr(svcconfig, "DATA_DIR", sandbox_data)
    monkeypatch.setattr(svcconfig, "SERVICES_DATA_DIR", sandbox_services)
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", sandbox_configs)
    monkeypatch.setattr(svcconfig, "NGWAF_DATA_DIR", sandbox_ngwaf)
    monkeypatch.setattr(svcconfig, "CACHE_DATA_DIR", sandbox_cache)
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
    import os as _os

    from backend.core import duckdb as _bk_duckdb

    sandbox_cache_root = sandbox_data / "cache"

    def _sandboxed_cache_dir(source: dict) -> str:
        if "_cache_dir_override" in source:
            return source["_cache_dir_override"]
        bucket = (source.get("bucket") or source.get("fos_bucket") or "default").strip()
        return _os.path.join(str(sandbox_cache_root), bucket)

    monkeypatch.setattr(_bk_duckdb, "_cache_dir", _sandboxed_cache_dir)

    yield
    # Close every connection across every thread — FastAPI's TestClient
    # spawns worker threads that open their own thread-local connections,
    # invisible to this fixture's ``_local``. Without this drain, those
    # connections live until GC and emit ResourceWarning at process exit.
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
    from backend.core import iceberg as _ic

    # A-3: drain every cache registered via
    # backend.utils.cache_registry.CacheRegistry. The iceberg, duckdb,
    # dashboard, and network modules all register their caches at
    # module load — this single call replaces the long enumeration
    # the R-1 work expanded. New module-level caches that ship with a
    # register() call are picked up automatically.
    from backend.utils.cache_registry import CacheRegistry

    CacheRegistry.clear_all()

    # Non-cache module globals the registry can't help with: a counter
    # and a class-reference that need explicit reset between tests.
    _ic._sql_load_table_real_calls["n"] = 0
    _ic._FOS_CATALOG_CLASS = None
    # All previously hand-cleared module caches (_logging_settings_cache,
    # _enforce_threshold_cache, active_requests._active_count via the
    # _ActiveRequestsResetAdapter) now register with CacheRegistry from
    # their owning modules — see R-1 in the audit. CacheRegistry.clear_all()
    # above drains them. New module-level caches that ship with a
    # register() call are picked up automatically.
    yield


@pytest.fixture
def in_memory_duckdb():
    """In-memory DuckDB for tests that exercise the analytical engine.

    No operational tables are created here — those live in per-service SQLite
    via the ``isolate_metadata_db`` fixture (autouse).
    """
    con = duckdb.connect(":memory:")
    yield con
    con.close()


def override_request_context(*, source, con, session=None, path="/test", time_bounds=None):
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
            con=con,
            telemetry=RequestTelemetry(request_method="POST", request_path=path),
            analyst_session=session,
            read_only=True,
            time_bounds=time_bounds,
        )

    return _override


@pytest.fixture
def client(in_memory_duckdb, test_service_source):
    from fastapi.testclient import TestClient

    from backend.core.request_context import build_request_context
    from backend.deps import get_service_id, get_source
    from backend.main import app

    app.dependency_overrides[get_con] = lambda: in_memory_duckdb
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

import os as _os

# 038: enable the telemetry overlay in API responses for the test
# environment so existing assertions on ``_debug_queries`` /
# ``_debug_calls`` keep passing. Production reads the env at
# backend.models.common import time and defaults to "excluded".
# Set it BEFORE the backend imports below so the module-level read
# in backend.models.common picks it up.
_os.environ.setdefault("DEBUG_RESPONSES", "true")

import duckdb
import pytest

from backend.deps import get_con


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
    from backend.core import metadata_db

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
    from backend.core import duckdb as _db
    from backend.core import iceberg as _ic
    from backend.repositories import dashboard as _dash

    _db._clear_schema_cache()
    _db._fos_client_cache.clear()
    _db._initialized_paths.clear()
    _ic._view_cache.clear()
    _ic._snapshot_files_cache.clear()
    _ic._catalog_cache.clear()
    _ic._table_object_cache.clear()
    _ic._sql_load_table_real_calls["n"] = 0
    _ic._FOS_CATALOG_CLASS = None
    _dash._dashboard_cache.clear()
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


@pytest.fixture
def client(in_memory_duckdb, test_service_source):
    from fastapi.testclient import TestClient

    from backend.deps import get_con, get_service_id, get_source
    from backend.main import app

    app.dependency_overrides[get_con] = lambda: in_memory_duckdb
    app.dependency_overrides[get_con] = lambda: in_memory_duckdb
    app.dependency_overrides[get_source] = lambda: test_service_source
    # ``get_service_id`` resolves from query/header/active-config. Under the
    # sandbox ``CONFIGS_DIR`` (isolate_metadata_db) there's no active config,
    # so without this override every ``Depends(get_service_id)`` route returns
    # ``configured=False`` before the test's patches get a chance to run.
    app.dependency_overrides[get_service_id] = lambda: test_service_source["service_id"]
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

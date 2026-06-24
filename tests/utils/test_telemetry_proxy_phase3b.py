"""Phase 3b tests for the telemetry proxy — PyIceberg routed through the proxy.

Design spec: docs/superpowers/specs/2026-05-19-telemetry-proxy-design.md (§Phase 3b)
Plan: docs/superpowers/plans/2026-05-19-telemetry-proxy-phase3b.md

These tests live in their own file (matching the Phase 2 / Phase 3a split) so
the Phase 3b fixtures don't pollute earlier files. Shared infrastructure with
Phase 1/2/3a is duplicated on purpose — moving it to conftest.py would silently
change discovery elsewhere, which we don't want.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import aiohttp
import boto3
import botocore
import pytest
from botocore.config import Config
from moto.server import ThreadedMotoServer

from backend.utils import telemetry_proxy


@pytest.fixture(autouse=True)
def _clear_s3fs_instance_cache():
    """fsspec caches S3FileSystem instances by kwargs hash; without clearing,
    a second S3FileSystem(...) call in the same process reuses the first
    instance and bypasses __init__ — so flag-on tests would silently leak
    into flag-off tests. Also reset the per-context proxy source so one test's
    source can't bleed into another's S3FileSystem construction."""
    from s3fs import S3FileSystem

    from backend.core import iceberg as _ic

    S3FileSystem.clear_instance_cache()
    _ic._PENDING_FS_SOURCE.set(None)
    yield
    S3FileSystem.clear_instance_cache()
    _ic._PENDING_FS_SOURCE.set(None)


@pytest.fixture
async def proxy_server():
    telemetry_proxy._reset_for_tests()
    telemetry_proxy.start_proxy_server()
    deadline = asyncio.get_event_loop().time() + 5.0
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(f"{telemetry_proxy.proxy_endpoint()}/healthz") as r:
                    if r.status == 200:
                        break
        except (aiohttp.ClientError, RuntimeError):
            pass
        if asyncio.get_event_loop().time() > deadline:
            telemetry_proxy.stop_proxy_server()
            raise RuntimeError("proxy did not become healthy in 5s")
        await asyncio.sleep(0.02)
    try:
        yield telemetry_proxy
    finally:
        telemetry_proxy.stop_proxy_server()
        telemetry_proxy._reset_for_tests()


@pytest.fixture
def moto_s3_server():
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    endpoint_url = f"http://{host}:{port}"
    seed_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    seed_client.create_bucket(Bucket="test-bucket")
    try:
        yield endpoint_url, f"{host}:{port}", seed_client
    finally:
        server.stop()


# ── Task 1: s3fs monkey-patch flag-on overrides endpoint and uses UNSIGNED ──


def test_s3fs_init_flag_on_routes_endpoint_through_proxy_and_unsigned(proxy_server):
    """Flag-on must rewrite the s3fs ``S3FileSystem.client_kwargs['endpoint_url']``
    to the local proxy and force ``signature_version=botocore.UNSIGNED`` so the
    proxy is the sole SigV4 signer (double-signing causes upstream
    SignatureDoesNotMatch). Path-style addressing is also forced because Fastly
    Object Storage requires it. The original FOS endpoint must be stashed on
    the instance for the deferred ``before-send.s3.*`` handler to read."""
    # Import inside the test so the monkey-patch in backend.core.iceberg has
    # taken effect by the time we touch S3FileSystem.
    from s3fs import S3FileSystem

    # Pass the source through via the side-channel that the patched __init__
    # reads (Task 3 wires this in production via a ContextVar from
    # _get_catalog; for Task 1 we just need to know the patched __init__
    # consumed and stashed it).
    source = {
        "name": "phase3b-task1",
        "service_id": "phase3b-task1",
        "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
        "endpoint": "us-east-1.object.fastlystorage.app",
        "access_key_id": "AKIA-phase3b",
        "secret_access_key": "secret-phase3b",
        "region": "us-east-1",
    }
    # The patched __init__ reads source from a module-level ContextVar
    # (_PENDING_FS_SOURCE). Setting it here mirrors what _get_catalog does.
    from backend.core import iceberg as _ic

    token = _ic._PENDING_FS_SOURCE.set(source)
    try:
        fs = S3FileSystem(
            client_kwargs={
                "endpoint_url": "https://us-east-1.object.fastlystorage.app",
                "region_name": "us-east-1",
            },
            config_kwargs={},
        )
    finally:
        _ic._PENDING_FS_SOURCE.reset(token)

    proxy_ep = proxy_server.proxy_endpoint()
    assert fs.client_kwargs["endpoint_url"] == proxy_ep, (
        f"proxy must rewrite endpoint_url to {proxy_ep}; got {fs.client_kwargs['endpoint_url']!r}"
    )
    assert fs.config_kwargs.get("signature_version") is botocore.UNSIGNED, (
        f"flag-on must set signature_version=UNSIGNED to avoid double-sign; got {fs.config_kwargs.get('signature_version')!r}"
    )
    assert fs.config_kwargs.get("s3", {}).get("addressing_style") == "path", (
        f"flag-on must force path-style addressing; got {fs.config_kwargs.get('s3')!r}"
    )
    assert getattr(fs, "_fos_proxy_target", None) == "us-east-1.object.fastlystorage.app", (
        f"flag-on must stash original FOS endpoint on the instance; got {getattr(fs, '_fos_proxy_target', None)!r}"
    )
    assert getattr(fs, "_fos_proxy_source", None) == source, (
        "flag-on must stash source dict on the instance so the before-send hook can read service_id / cdn config"
    )


# ── Task 2: before-send.s3.* handler injects telemetry headers per-request ──


def test_s3fs_through_proxy_records_telemetry_with_caller_pyiceberg_s3fs(proxy_server, moto_s3_server):
    """End-to-end: an s3fs ``ls`` against a moto bucket must produce a
    captured ``_usage_log`` row tagged ``caller='pyiceberg.s3fs'`` and the
    test's service_id. Without the deferred ``before-send.s3.*`` handler the
    proxy returns 400 ("Missing X-Fos-Target") and no row is captured."""
    from s3fs import S3FileSystem

    from backend.core import iceberg as _ic

    moto_endpoint, moto_host_port, _ = moto_s3_server
    source = {
        "name": "svc-3b-task2",
        "service_id": "svc-3b-task2",
        # X-Fos-Target must include the http:// scheme because moto is HTTP.
        "fos_native_endpoint": moto_endpoint,
        "endpoint": moto_host_port,
        "access_key_id": "testing",
        "secret_access_key": "testing",
        "region": "us-east-1",
    }

    captured_rows: list[dict] = []

    def _capture(service_id, rows, process_context=None):
        for r in rows:
            r["_service_id"] = service_id
            r["_process_context"] = process_context
            captured_rows.append(r)

    token = _ic._PENDING_FS_SOURCE.set(source)
    try:
        with (
            patch("backend.core.metadata.log_usage_calls", side_effect=_capture),
            patch(
                "backend.config.load_config",
                return_value={
                    "fos_access_key_id": "testing",
                    "fos_secret_access_key": "testing",
                    "fos_region": "us-east-1",
                },
            ),
        ):
            proxy_server._bust_config_cache()
            fs = S3FileSystem(
                client_kwargs={"endpoint_url": moto_endpoint, "region_name": "us-east-1"},
                config_kwargs={},
            )
            listing = fs.ls("test-bucket/")
            assert listing == [] or isinstance(listing, list)
            proxy_server._flush_log_writes_for_tests()
    finally:
        _ic._PENDING_FS_SOURCE.reset(token)

    assert captured_rows, "expected at least 1 _usage_log row from s3fs through proxy"
    callers = {r.get("caller") for r in captured_rows}
    service_ids = {r.get("_service_id") for r in captured_rows}
    assert "pyiceberg.s3fs" in callers, f"expected caller='pyiceberg.s3fs' in captured rows; got callers={callers}"
    assert "svc-3b-task2" in service_ids, f"expected service_id='svc-3b-task2'; got service_ids={service_ids}"


def test_s3fs_through_proxy_carries_per_call_process_context(proxy_server, moto_s3_server):
    """The before-send.s3.* handler must read ``get_process_context()`` at
    REQUEST TIME, not at fs construction time. This is the Phase 3b win over
    Phase 3a: per-call context propagation rather than connection-scoped.

    Setting two different process contexts between two s3fs ops must produce
    two captured rows with two different process_context values."""
    from s3fs import S3FileSystem

    from backend.core import iceberg as _ic
    from backend.utils.telemetry import _set_process_context_for_tests

    moto_endpoint, moto_host_port, _ = moto_s3_server
    source = {
        "name": "svc-3b-ctx",
        "service_id": "svc-3b-ctx",
        "fos_native_endpoint": moto_endpoint,
        "endpoint": moto_host_port,
        "access_key_id": "testing",
        "secret_access_key": "testing",
        "region": "us-east-1",
    }

    captured_rows: list[dict] = []

    def _capture(service_id, rows, process_context=None):
        for r in rows:
            r["_process_context"] = process_context
            captured_rows.append(r)

    token = _ic._PENDING_FS_SOURCE.set(source)
    try:
        with (
            patch("backend.core.metadata.log_usage_calls", side_effect=_capture),
            patch(
                "backend.config.load_config",
                return_value={
                    "fos_access_key_id": "testing",
                    "fos_secret_access_key": "testing",
                    "fos_region": "us-east-1",
                },
            ),
        ):
            proxy_server._bust_config_cache()
            fs = S3FileSystem(
                client_kwargs={"endpoint_url": moto_endpoint, "region_name": "us-east-1"},
                config_kwargs={},
            )
            # refresh=True is load-bearing — fsspec.DirCache short-circuits
            # the second ls without an S3 call, so the second context never
            # gets a chance to ride a request through the proxy.
            _set_process_context_for_tests("ctx-A")
            fs.ls("test-bucket/", refresh=True)
            _set_process_context_for_tests("ctx-B")
            fs.ls("test-bucket/", refresh=True)
            proxy_server._flush_log_writes_for_tests()
    finally:
        _ic._PENDING_FS_SOURCE.reset(token)

    contexts = {r.get("_process_context") for r in captured_rows}
    assert {"ctx-A", "ctx-B"}.issubset(contexts), f"expected per-call process_context propagation; got {contexts}"


# ── Task 2b: per-method target routing — writes MUST NOT route via CDN ───────


def test_register_proxy_event_hook_routes_writes_to_fos_native_not_cdn():
    """Fastly's CDN VCL only authorizes object GET/HEAD. Writes (PUT/POST/
    DELETE) routed through the CDN return ``HTTP 503 Service Unavailable``
    every time. The commit cron silently failed for 2+ hours on 2026-05-19
    after Phase 4 (proxy default ON) because every pyiceberg PUT — new
    parquet data files, manifest lists, metadata.json — was being sent to
    the customer's ``cdn_url`` target and rejected.

    The before-send hook MUST inspect ``request.method`` and route:
      - GET/HEAD → CDN host (so cached reads still hit the edge); attach
        ``x-fastly-key`` for CDN auth.
      - PUT/POST/DELETE/PATCH (etc.) → FOS native endpoint (the proxy
        SigV4-signs and forwards direct to FOS); MUST NOT attach
        ``x-fastly-key`` (CDN-only auth header).
    """
    from unittest.mock import MagicMock

    from backend.core import iceberg as _ic

    source = {
        "service_id": "svc-method-routing",
        "name": "svc-method-routing",
        "cdn_url": "https://cdn.example.com",
        "cdn_secret": "shared-cdn-secret",
        "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
    }

    # Capture the callback that the hook registers on the client's event system.
    registered: list = []
    fake_client = MagicMock()
    fake_client.meta.events.register = lambda event, cb: registered.append(cb)

    _ic._register_proxy_event_hook(
        fake_client,
        cdn_target="cdn.example.com",
        fos_native_target="us-east-1.object.fastlystorage.app",
        source=source,
    )

    assert registered, "_register_proxy_event_hook must register a before-send callback"
    inject = registered[0]

    def _req(method: str, url: str = "https://host/bucket/object.parquet"):
        r = MagicMock()
        r.method = method
        r.url = url
        r.headers = {}
        return r

    # Reads route to CDN with cdn_secret attached.
    for m in ("GET", "HEAD"):
        r = _req(m)
        inject(r)
        assert r.headers["X-Fos-Target"] == "cdn.example.com", (
            f"{m} must route to CDN host when cdn_url configured; got {r.headers['X-Fos-Target']!r}"
        )
        assert r.headers.get("x-fastly-key") == "shared-cdn-secret", f"{m} must attach x-fastly-key for CDN auth"

    # Writes route to FOS native; cdn_secret MUST NOT be attached (the proxy
    # signs SigV4 to FOS, and CDN auth headers leaking to FOS confuse logs).
    for m in ("PUT", "POST", "DELETE", "PATCH"):
        r = _req(m)
        inject(r)
        assert r.headers["X-Fos-Target"] == "us-east-1.object.fastlystorage.app", (
            f"{m} MUST route to FOS native (CDN rejects writes with 503); got {r.headers['X-Fos-Target']!r}"
        )
        assert "x-fastly-key" not in r.headers, f"{m} must NOT carry x-fastly-key — that's a CDN-only auth header"

    # Standard telemetry headers attached on every request.
    r = _req("GET")
    inject(r)
    assert r.headers["X-Telemetry-Service-Id"] == "svc-method-routing"
    assert r.headers["X-Telemetry-Caller"] == "pyiceberg.s3fs"


def test_register_proxy_event_hook_routes_bucket_level_get_to_fos_native_not_cdn():
    """Fastly's CDN VCL authorizes object-level GET/HEAD ONLY — bucket-level
    API operations (LIST = ``?list-type=2``, multi-delete = ``?delete``, etc.)
    are signed against the bucket path and the CDN rejects them with HTTP 403
    ``SignatureDoesNotMatch``.

    The commit cron failed for 2+ hours on 2026-05-19 because pyiceberg's
    ``exists()`` falls back to a LIST when HEAD returns 404, and that LIST
    was being routed to the CDN host (every GET went to CDN, regardless of
    whether it carried a query string).

    The discriminator is the URL's query string: an object read like
    ``GET /bucket/file.parquet`` has no query, while a bucket-level operation
    like ``GET /bucket?list-type=2&prefix=...`` does. The hook MUST route
    any GET/HEAD that carries a query string to FOS native, even when a CDN
    is configured. ``x-fastly-key`` MUST NOT be attached on that route.
    """
    from unittest.mock import MagicMock

    from backend.core import iceberg as _ic

    source = {
        "service_id": "svc-bucket-get",
        "name": "svc-bucket-get",
        "cdn_url": "https://cdn.example.com",
        "cdn_secret": "shared-cdn-secret",
        "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
    }

    registered: list = []
    fake_client = MagicMock()
    fake_client.meta.events.register = lambda event, cb: registered.append(cb)

    _ic._register_proxy_event_hook(
        fake_client,
        cdn_target="cdn.example.com",
        fos_native_target="us-east-1.object.fastlystorage.app",
        source=source,
    )
    inject = registered[0]

    # The real failing call from the 2026-05-19 commit-cron outage:
    # pyiceberg's exists() fallback LIST on a metadata.json key.
    bucket_list_url = (
        "https://us-east-1.object.fastlystorage.app/fos-bucket-logs"
        "?list-type=2&prefix=iceberg%2Fdefault%2Flogs%2Fmetadata%2F"
        "01005-a4470b9b-84fa-4131-aea4-01838585a9e8.metadata.json%2F"
        "&delimiter=%2F&max-keys=1&encoding-type=url"
    )
    for m in ("GET", "HEAD"):
        r = MagicMock()
        r.method = m
        r.url = bucket_list_url
        r.headers = {}
        inject(r)
        assert r.headers["X-Fos-Target"] == "us-east-1.object.fastlystorage.app", (
            f"{m} with query string is a bucket-level API call and MUST route "
            f"to FOS native (CDN rejects with 403 SignatureDoesNotMatch); "
            f"got {r.headers['X-Fos-Target']!r}"
        )
        assert "x-fastly-key" not in r.headers, (
            f"{m} routed to FOS native MUST NOT carry x-fastly-key (CDN-only header)"
        )

    # Object reads (no query string) still route to CDN — caching is the win.
    for m in ("GET", "HEAD"):
        r = MagicMock()
        r.method = m
        r.url = "https://cdn.example.com/fos-bucket-logs/iceberg/default/logs/metadata/v1.metadata.json"
        r.headers = {}
        inject(r)
        assert r.headers["X-Fos-Target"] == "cdn.example.com", (
            f"{m} without query string is an object read and must still route to CDN"
        )
        assert r.headers.get("x-fastly-key") == "shared-cdn-secret"


def test_register_proxy_event_hook_no_cdn_routes_everything_to_fos_native():
    """When the source has no ``cdn_url``, both reads and writes route to the
    FOS native target. No x-fastly-key is ever attached."""
    from unittest.mock import MagicMock

    from backend.core import iceberg as _ic

    source = {
        "service_id": "svc-no-cdn",
        "name": "svc-no-cdn",
        "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
    }

    registered: list = []
    fake_client = MagicMock()
    fake_client.meta.events.register = lambda event, cb: registered.append(cb)

    _ic._register_proxy_event_hook(
        fake_client,
        cdn_target=None,
        fos_native_target="us-east-1.object.fastlystorage.app",
        source=source,
    )
    inject = registered[0]

    for m in ("GET", "HEAD", "PUT", "POST", "DELETE"):
        r = MagicMock()
        r.method = m
        r.headers = {}
        inject(r)
        assert r.headers["X-Fos-Target"] == "us-east-1.object.fastlystorage.app", (
            f"{m} must route to FOS native when no CDN configured"
        )
        assert "x-fastly-key" not in r.headers


# ── Task 3: _get_catalog flag-on swaps py-io-impl + passes source via ContextVar


def test_get_catalog_flag_on_uses_stock_fsspec_file_io_and_seeds_source(tmp_path, proxy_server):
    """When the proxy is on, _get_catalog must:
      - Set ``py-io-impl=pyiceberg.io.fsspec.FsspecFileIO`` (stock) — the
        TrackedFsspec* wrappers would double-count every PyIceberg call
        because the proxy already logs the underlying S3 request.
      - Seed _PENDING_FS_SOURCE so the patched S3FileSystem.__init__ stashes
        the source on the fs instance for the before-send hook.

    We inspect the catalog's resolved IO config via its ``properties`` dict;
    a unit-level assertion that bypasses an actual PyIceberg S3 call (those
    are covered E2E in Task 4)."""
    from backend.core import iceberg as _ic

    source = {
        "name": "phase3b-task3",
        "service_id": "phase3b-task3",
        "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
        "endpoint": "us-east-1.object.fastlystorage.app",
        "access_key_id": "AKIA-phase3b",
        "secret_access_key": "secret-phase3b",
        "region": "us-east-1",
        "bucket": "test-bucket",
        # _cache_dir() resolves under config-cache-dir; redirect there.
        "cache_dir": str(tmp_path),
    }

    # _get_catalog memoises by source['name']; ensure a clean slate.
    _ic._catalog_cache.pop(source["name"], None)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("backend.core.iceberg._warehouse_uri", return_value=f"s3://{source['bucket']}/iceberg/"),
    ):
        catalog = _ic._get_catalog(source)

        try:
            # Stock FsspecFileIO — proxy is the sole telemetry sink.
            py_io = catalog.properties.get("py-io-impl")
            assert py_io == "pyiceberg.io.fsspec.FsspecFileIO", (
                f"_get_catalog must use stock FsspecFileIO; got {py_io!r}"
            )
            # Constructing an fs in the same scope should pick up the source
            # seeded by _get_catalog via _PENDING_FS_SOURCE.
            from s3fs import S3FileSystem

            fs = S3FileSystem(
                client_kwargs={"endpoint_url": "https://us-east-1.object.fastlystorage.app"},
                config_kwargs={},
            )
            assert fs._fos_proxy_source.get("service_id") == "phase3b-task3", (
                "_get_catalog must seed _PENDING_FS_SOURCE so the patched s3fs __init__ stashes it"
            )
        finally:
            _ic._catalog_cache.pop(source["name"], None)


# ── Task 4: E2E PyIceberg commit + read through proxy logs telemetry ────────


def test_pyiceberg_through_proxy_logs_telemetry_end_to_end(tmp_path, proxy_server, moto_s3_server):
    """End-to-end: drive a real PyIceberg create-table → append → scan flow
    through the proxy against moto. Assert ≥1 PUT and ≥1 GET row are captured
    with ``caller='pyiceberg.s3fs'``, AND that the legacy TrackedFsspec*
    record_call codepath did NOT fire (proxy is the sole sink under flag-on).
    """
    import pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField, StringType

    from backend.core import iceberg as _ic

    moto_endpoint, moto_host_port, seed_client = moto_s3_server
    source = {
        "name": "phase3b-task4",
        "service_id": "phase3b-task4",
        "fos_native_endpoint": moto_endpoint,
        "endpoint": moto_host_port,
        "access_key_id": "testing",
        "secret_access_key": "testing",
        "region": "us-east-1",
        "bucket": "test-bucket",
    }

    captured_proxy_rows: list[dict] = []
    captured_tracked_calls: list[tuple] = []

    def _capture_proxy(service_id, rows, process_context=None):
        for r in rows:
            r["_service_id"] = service_id
            captured_proxy_rows.append(r)

    def _capture_tracked(*args, **kwargs):
        captured_tracked_calls.append((args, kwargs))

    _ic._catalog_cache.pop(source["name"], None)
    try:
        with (
            patch("backend.core.metadata.log_usage_calls", side_effect=_capture_proxy),
            patch("backend.utils.telemetry.record_call", side_effect=_capture_tracked),
            patch(
                "backend.config.load_config",
                return_value={
                    "fos_access_key_id": "testing",
                    "fos_secret_access_key": "testing",
                    "fos_region": "us-east-1",
                },
            ),
        ):
            proxy_server._bust_config_cache()

            # Build the SqlCatalog directly so we can point at a tmp SQLite
            # and proxy-route via the same _PENDING_FS_SOURCE seed that
            # _get_catalog populates in production. The patched
            # ThreadPoolExecutor.submit (see iceberg.py) copies the
            # current context into worker threads so PyIceberg's
            # parquet-write workers also see this source.
            _ic._PENDING_FS_SOURCE.set(source)
            db_path = str(tmp_path / "phase3b_task4.db")
            catalog = SqlCatalog(
                "fos",
                **{
                    "uri": f"sqlite:///{db_path}",
                    "warehouse": f"s3://{source['bucket']}/iceberg/",
                    "s3.endpoint": f"http://{moto_host_port}",
                    "s3.access-key-id": "testing",
                    "s3.secret-access-key": "testing",
                    "s3.path-style-access": "true",
                    "s3.region": "us-east-1",
                    "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
                },
            )

            catalog.create_namespace("default")
            schema = Schema(
                NestedField(1, "id", LongType(), required=False),
                NestedField(2, "name", StringType(), required=False),
            )
            table = catalog.create_table("default.t1", schema=schema)

            arrow_tbl = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
            table.append(arrow_tbl)

            scan_result = catalog.load_table("default.t1").scan().to_arrow()
            assert scan_result.num_rows == 3, f"round-trip failed; got {scan_result.num_rows} rows"

            proxy_server._flush_log_writes_for_tests(timeout=5.0)
    finally:
        _ic._catalog_cache.pop(source["name"], None)

    callers = {r.get("caller") for r in captured_proxy_rows}
    methods = {r.get("method") for r in captured_proxy_rows}
    assert "pyiceberg.s3fs" in callers, f"expected caller='pyiceberg.s3fs' in captured rows; got callers={callers}"
    # Mix of GET and PUT — commit writes manifest/metadata, scan reads them.
    assert "PUT" in methods, f"expected at least one PUT (commit/metadata); got methods={methods}"
    assert "GET" in methods, f"expected at least one GET (scan/metadata); got methods={methods}"
    # The legacy TrackedFsspec* path must NOT have fired — proxy is the sole
    # sink for PyIceberg under flag-on.
    assert captured_tracked_calls == [], (
        f"flag-on must bypass TrackedFsspec* wrappers (proxy is sole sink); "
        f"got record_call invocations: {captured_tracked_calls!r}"
    )

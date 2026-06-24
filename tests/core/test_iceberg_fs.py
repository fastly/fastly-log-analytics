"""Unit tests for ``backend.core.iceberg.fs`` helpers.

Audit finding addressed:
    The s3fs/ThreadPoolExecutor monkey-patches in ``backend/core/iceberg/fs.py``
    are load-bearing for FOS proxy routing — if ``_patched_submit`` stops
    propagating ContextVars to worker threads, every pyiceberg parquet-write
    worker sees ``_PENDING_FS_SOURCE.get() == None`` and the proxy 400s with
    "Missing X-Fos-Target" (the failure mode that produced 68 silent 400s in
    6 minutes on 2026-06-09). Similarly, ``_proxy_targets_from_endpoint`` is
    the URL-parsing seam that determines whether GET/HEAD reads go through
    the customer's CDN host: scheme/path-strip + lowercase bugs there would
    silently mis-route reads (e.g. include a path in the ``X-Fos-Target``
    header → 403 SignatureDoesNotMatch).

Scope: this file covers ONLY the parts NOT already covered by
``tests/utils/test_telemetry_proxy_phase3b.py`` (which tests
``_register_proxy_event_hook``'s per-method routing end-to-end through a
moto-backed proxy server). Here we pin the small pure helpers + the
ThreadPoolExecutor ContextVar propagation.
"""

from __future__ import annotations

import concurrent.futures as _futures
import contextvars as _contextvars

# Import the module under test. Side-effect: installs the s3fs and
# ThreadPoolExecutor monkey-patches. The patches are global and idempotent
# (re-importing is safe — the module guards on top-level try/except ImportError).
from backend.core.iceberg import fs as _fs

# ── _patched_submit: ContextVar propagation into executor workers ───────────


def test_patched_submit_propagates_contextvar_to_worker():
    """The patched ``ThreadPoolExecutor.submit`` must wrap ``fn`` in
    ``copy_context().run`` so a ContextVar set in the submitter is visible
    to the worker. Without this propagation, pyiceberg's parquet-write
    pool workers see ``_PENDING_FS_SOURCE.get() == None`` and the proxy
    fires 400s for every PUT (MONKEYPATCHES.md §6, 2026-06-06 audit).

    We exercise this on the real ContextVar the patch was added to
    propagate — ``_PENDING_FS_SOURCE`` — so a regression in
    ``_patched_submit`` would fail here for the same reason production
    would break.
    """
    sentinel = {"service_id": "ctxvar-propagation-test"}
    token = _fs._PENDING_FS_SOURCE.set(sentinel)
    try:
        with _futures.ThreadPoolExecutor(max_workers=1) as ex:
            future = ex.submit(_fs._PENDING_FS_SOURCE.get)
            assert future.result(timeout=5) is sentinel, (
                "patched submit must copy the calling context into the worker — "
                "without this, the worker sees ContextVar default (None)"
            )
    finally:
        _fs._PENDING_FS_SOURCE.reset(token)


def test_patched_submit_uses_separate_context_per_call():
    """A second submit() after the parent ContextVar changes must see the
    NEW value, not the value captured by the first submit. ``copy_context``
    snapshots at submit time, so each submit gets its own isolated snapshot.
    """
    var: _contextvars.ContextVar[str] = _contextvars.ContextVar("test_var", default="default")

    with _futures.ThreadPoolExecutor(max_workers=1) as ex:
        var.set("first")
        f1 = ex.submit(var.get)
        var.set("second")
        f2 = ex.submit(var.get)
        assert f1.result(timeout=5) == "first"
        assert f2.result(timeout=5) == "second"


# ── _proxy_targets_from_endpoint: pure URL parsing ──────────────────────────


def test_proxy_targets_strips_https_scheme_path_and_lowercases():
    """``cdn_url`` may arrive as a full URL with scheme + path + uppercase.
    The proxy's ``X-Fos-Target`` header is a hostname, NOT a URL — a stray
    path or scheme leaks into the upstream Host header and FOS rejects
    with 403 SignatureDoesNotMatch."""
    source = {
        "cdn_url": "https://Fastly.Example.COM/some/path",
        "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
    }
    cdn_target, fos_native_target = _fs._proxy_targets_from_endpoint(
        "https://us-east-1.object.fastlystorage.app",
        source,
    )
    assert cdn_target == "fastly.example.com", f"cdn_target must be host-only + lowercase; got {cdn_target!r}"
    assert fos_native_target == "us-east-1.object.fastlystorage.app"


def test_proxy_targets_strips_http_scheme():
    """``http://`` scheme is stripped just like ``https://`` — some
    customer-configured cdn_urls are bare HTTP in dev / staging."""
    source = {"cdn_url": "http://cdn.test.local"}
    cdn_target, _ = _fs._proxy_targets_from_endpoint("http://endpoint", source)
    assert cdn_target == "cdn.test.local"


def test_proxy_targets_missing_fos_native_endpoint_falls_back_to_endpoint_url():
    """When the source has no ``fos_native_endpoint``, writes still need
    SOMEWHERE to go — the caller's endpoint_url is the right fallback
    (it's what reads target, so writes go to the same origin)."""
    source = {"cdn_url": "https://cdn.example.com"}  # no fos_native_endpoint
    _, fos_native_target = _fs._proxy_targets_from_endpoint(
        "https://fallback.endpoint.example",
        source,
    )
    assert fos_native_target == "https://fallback.endpoint.example", (
        "missing fos_native_endpoint must fall back to caller's endpoint_url"
    )


def test_proxy_targets_empty_cdn_url_returns_none_cdn_target():
    """Empty-string ``cdn_url`` means "no CDN configured" — the per-method
    router in ``_register_proxy_event_hook`` keys off ``cdn_target is None``
    to route every request (reads AND writes) to FOS native."""
    source = {"cdn_url": "", "fos_native_endpoint": "fos.example.com"}
    cdn_target, fos_native_target = _fs._proxy_targets_from_endpoint("https://anywhere", source)
    assert cdn_target is None
    assert fos_native_target == "fos.example.com"


def test_proxy_targets_missing_cdn_url_returns_none_cdn_target():
    """Same contract as empty-string: a source dict that simply lacks the
    ``cdn_url`` key must return ``None`` for cdn_target — no KeyError, no
    fall-through to a bogus value."""
    source = {"fos_native_endpoint": "fos.example.com"}
    cdn_target, fos_native_target = _fs._proxy_targets_from_endpoint("https://anywhere", source)
    assert cdn_target is None
    assert fos_native_target == "fos.example.com"


def test_proxy_targets_none_source_returns_defaults():
    """When the patched s3fs init sees ``_PENDING_FS_SOURCE.get() is None
    and _LAST_FS_SOURCE is None`` (very first call before any service has
    been seeded), source defaults to ``{}`` and routing must degrade
    gracefully: no CDN target, fos_native_target == endpoint_url."""
    cdn_target, fos_native_target = _fs._proxy_targets_from_endpoint(
        "https://endpoint.example",
        None,
    )
    assert cdn_target is None
    assert fos_native_target == "https://endpoint.example"


def test_proxy_targets_whitespace_only_cdn_url_treated_as_empty():
    """``cdn_url`` with only whitespace must strip to empty and yield
    ``cdn_target is None``. The helper uses ``.strip()`` for exactly this —
    misconfigured admin UI submissions ("  ") would otherwise produce
    cdn_target=='' and the proxy would inject an empty X-Fos-Target."""
    source = {"cdn_url": "   ", "fos_native_endpoint": "fos.example.com"}
    cdn_target, _ = _fs._proxy_targets_from_endpoint("https://anywhere", source)
    assert cdn_target is None


def test_proxy_targets_malformed_url_no_scheme_returns_host_lowercased():
    """A ``cdn_url`` without a scheme (e.g. admin-pasted bare hostname)
    must still degrade to a sane host — no crash, no URL parse error.
    The function's ``.replace().split()`` chain is tolerant by design."""
    source = {"cdn_url": "CDN.EXAMPLE.COM/path"}
    cdn_target, _ = _fs._proxy_targets_from_endpoint("https://anywhere", source)
    # No scheme to strip; path is stripped by split('/', 1); lowercased.
    assert cdn_target == "cdn.example.com"


def test_proxy_targets_does_not_mutate_source_dict():
    """The helper is read-only on its source argument. Mutating the dict
    would leak transformed values back into the calling code (``_get_catalog``
    keeps the same source dict in ``_catalog_cache`` for re-use)."""
    source = {
        "cdn_url": "https://Fastly.Example.COM/path",
        "fos_native_endpoint": "fos.example.com",
    }
    before = dict(source)
    _fs._proxy_targets_from_endpoint("https://endpoint", source)
    assert source == before, "source dict must not be mutated by _proxy_targets_from_endpoint"


# ── Patches are actually INSTALLED on S3FileSystem (silent-no-op backstop) ───
#
# The cache-patch tests above (and in tests/core/test_iceberg.py) exercise the
# patch *logic* via fakes/shims — they save+replace ``_orig_cat_file`` or build a
# FakeS3FS, so they pass even if the wrapper was never wired onto the real class.
# That leaves a gap: if a future s3fs bump (or an accidental edit) dropped the
# ``S3FileSystem._cat_file = _patched_cat_file`` install lines (fs.py:497-503),
# the manifest cache would silently vanish and CI would stay green — re-running
# the exact 2026-05-20 regression (1,104 manifests re-read ~470×, 2.4 GB CDN
# egress) in prod. The contract guard (fs.py:175-182) only catches a slot being
# RENAMED/removed (hasattr), not the install being dropped or the upstream
# signature drifting. These two tests close that gap.


def _s3fs_or_skip():
    """The patches live behind ``try: from s3fs import S3FileSystem`` in fs.py.
    In the project venv s3fs is always present (pyiceberg[s3fs] extra), so a
    skip here would itself be a signal worth noticing."""
    import pytest

    try:
        from s3fs import S3FileSystem
    except ImportError:  # pragma: no cover - s3fs is a hard transitive dep
        pytest.skip("s3fs not importable; FOS patches inactive")
    return S3FileSystem


def test_s3fs_cache_and_proxy_patches_are_installed_on_the_class():
    """Every s3fs seam the proxy + manifest cache depend on must be the patched
    function on the live class. Catches a dropped install line or a rename that
    slips the wrapper onto the wrong slot — neither of which the logic-shim
    tests notice (they never touch ``S3FileSystem.<slot>`` identity)."""
    S3FileSystem = _s3fs_or_skip()
    assert S3FileSystem._cat_file is _fs._patched_cat_file, "manifest LRU cat_file patch not installed"
    assert S3FileSystem._info is _fs._patched_info, "info()-from-cache patch not installed"
    assert S3FileSystem._open is _fs._patched_open, "open()-bridges-iothread patch not installed"
    assert S3FileSystem.__init__ is _fs._patched_s3fs_init, "proxy-routing __init__ patch not installed"
    assert S3FileSystem.set_session is _fs._patched_s3fs_set_session, "event-hook set_session patch not installed"
    # _connect is an alias of set_session in s3fs (core.py:678 `_connect = set_session`);
    # both must point at the same wrapper so a session refresh re-registers the hook.
    assert S3FileSystem._connect is _fs._patched_s3fs_set_session, "_connect alias patch not installed"


def test_orig_s3fs_signatures_still_accept_the_kwargs_the_patches_pass():
    """The contract guard only checks the slots EXIST (hasattr), not that their
    signatures still accept the kwargs our wrappers forward. Pin the params the
    patches actually depend on so a signature drift on bump fails here at test
    time instead of at the first manifest fetch in prod:
      - _get_or_fetch_immutable_async (fs.py:344) -> _orig_cat_file(..., version_id=, max_concurrency=1)
      - _patched_info (fs.py:393)               -> _orig_info(self, path, bucket=, key=, refresh=, version_id=)
    """
    import inspect

    _s3fs_or_skip()
    cat_params = inspect.signature(_fs._orig_cat_file).parameters
    for name in ("version_id", "max_concurrency"):
        assert name in cat_params, (
            f"s3fs _cat_file no longer accepts {name!r}; the manifest-cache helper passes it "
            "(fs.py:344). max_concurrency=1 is load-bearing — it skips the probe-GET that doubled "
            "FOS billing (2026-05-21 2.00x ratio)."
        )

    info_params = inspect.signature(_fs._orig_info).parameters
    for name in ("path", "bucket", "key", "refresh", "version_id"):
        assert name in info_params, f"s3fs _info no longer accepts {name!r}; _patched_info forwards it (fs.py:393)."

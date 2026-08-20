"""Provision-orchestrator survives transient Fastly failures.

Audit finding: the existing orchestrator tests
([test_provision_orchestrator.py](test_provision_orchestrator.py)) pin the
8-step happy path and the step-3 (early bucket create) rollback, but
leave two real-world failure modes uncovered:

  1. Transient Fastly 429 / 503 mid-flow → the tenacity-backed retry in
     ``backend.core.fastly.client.fastly`` must absorb these without
     aborting the multi-step provision. The two existing VCR cassettes
     pin the wire behaviour at the client layer; this file pins that the
     *orchestrator* benefits from the same retry — i.e. nobody wrapped
     the call in a non-retrying helper or shortened the retry budget.
  2. Mid-flow failure AFTER step 7's logging endpoint was created →
     ``perform_teardown`` must see ``activated_logging_version`` in state
     and tear it down. The step-3 rollback test in the sibling file only
     covers a failure BEFORE any Fastly-side state was created.

Both surfaces have been silent-regression class incidents before.
"""

from __future__ import annotations

import contextlib
import os
from unittest.mock import patch

import vcr

from backend.provision import orchestrator

CASSETTE_DIR = os.path.join(os.path.dirname(__file__), "cassettes")

_my_vcr = vcr.VCR(
    cassette_library_dir=CASSETTE_DIR,
    record_mode="none",
    filter_headers=[("Fastly-Key", "REDACTED"), ("Authorization", "REDACTED")],
    match_on=["method", "scheme", "host", "port", "path", "query"],
)


def _provision_cfg(**overrides):
    base = {
        "admin_token": "test-token-not-real",
        "logging_service_id": "SU3xxxxxxxxxxxxxx0000",
        "service_name": "Test Service",
        "fos_region": "us-east-1",
        "fos_bucket_name": "test-bucket",
        "fos_prefix": "",
        "endpoint_name": "Test Endpoint",
        "sample_rate": 100,
        "edge_only": True,
        "log_period": 60,
        "cdn_service_name": "Test CDN",
        "cdn_url": "https://test.example",
        "cdn_secret": "secret",
        "log_fields": {"groups": ["A"]},
    }
    base.update(overrides)
    return base


def _consume(gen):
    events = []
    try:
        for e in gen:
            events.append(e)
    except Exception as exc:
        return events, exc
    return events, None


def _fos_key(suffix: str = "X"):
    return {"access_key": f"AK_{suffix}", "secret_key": f"SK_{suffix}", "id": f"ID_{suffix}"}


@contextlib.contextmanager
def _mocked_provision_steps(bucket_side_effect=None, logging_return=42, write_side_effect=None):
    """Stack the standard provision-step mocks so the three tests stay terse.

    Every step except the ones the caller wants live is mocked to a no-op or
    a benign return value. ``bucket_side_effect`` is hooked onto step 3 so
    the 429/503 tests can drive a real wire call through it. ``write_side_
    effect`` is hooked onto step 8 for the late-failure test."""
    with (
        patch("backend.provision.orchestrator.validate_log_format", return_value=[]),
        patch(
            "backend.provision.orchestrator.ensure_fos_access_key",
            side_effect=[_fos_key("T"), _fos_key("P")],
        ),
        patch("backend.provision.orchestrator.ensure_fos_bucket", side_effect=bucket_side_effect),
        patch("backend.provision.orchestrator.delete_fos_access_key"),
        patch("backend.provision.orchestrator.ensure_cdn_service", return_value={"id": "cdn-svc-id"}),
        patch("backend.provision.orchestrator.ensure_logging_via_reconciler", return_value=logging_return),
        patch(
            "backend.provision.orchestrator.write_service_config",
            side_effect=write_side_effect,
        ),
        patch("backend.core.duckdb.get_source_for_service", return_value=None),
    ):
        yield


# ── 1) Provision survives a transient Fastly 429 mid-flow ───────────────────


def test_provision_survives_fastly_429_with_retry(tmp_path, monkeypatch):
    """A Fastly call inside the provision flow that returns 429-then-200
    must be absorbed by the client's tenacity retry. Provision completes
    with no exception bubbling up.

    We exercise the cassette by making one real ``fastly()`` call inside
    an otherwise-fully-mocked orchestrator — the other provision helpers
    would issue arbitrary numbers of Fastly calls that the cassette
    doesn't cover. Tenacity nap is patched so the test doesn't sleep
    through the backoff."""
    monkeypatch.chdir(tmp_path)
    from backend.core.fastly.client import fastly as real_fastly

    real_call_results = []

    def fake_bucket(*args, **kwargs):
        real_call_results.append(real_fastly("GET", "/service/SU3xxxxxxxxxxxxxx0000", token="test-token-not-real"))

    with (
        patch("tenacity.nap.time.sleep"),
        _my_vcr.use_cassette("fastly_429_then_success.yaml"),
        _mocked_provision_steps(bucket_side_effect=fake_bucket),
    ):
        events, exc = _consume(orchestrator.provision(_provision_cfg()))

    assert exc is None, f"429 should be retried transparently; instead saw {exc!r}"
    assert real_call_results == [{"id": "SU3xxxxxxxxxxxxxx0000", "name": "Test CDN", "active_version": {"number": 42}}]
    assert events[-1]["type"] == "done"


# ── 2) Provision survives a transient Fastly 503 mid-flow ───────────────────


def test_provision_survives_fastly_503_with_retry(tmp_path, monkeypatch):
    """Same shape as the 429 test against the 503 cassette. 503 is in
    ``_RETRYABLE_HTTP_CODES`` for the same reason as 429 — a transient
    upstream blip must not abort a partially-completed provision."""
    monkeypatch.chdir(tmp_path)
    from backend.core.fastly.client import fastly as real_fastly

    real_call_results = []

    def fake_bucket(*args, **kwargs):
        # /account is what the 503-then-success cassette covers.
        real_call_results.append(real_fastly("GET", "/account", token="test-token-not-real"))

    with (
        patch("tenacity.nap.time.sleep"),
        _my_vcr.use_cassette("fastly_503_then_success.yaml"),
        _mocked_provision_steps(bucket_side_effect=fake_bucket),
    ):
        events, exc = _consume(orchestrator.provision(_provision_cfg()))

    assert exc is None, f"503 should be retried transparently; instead saw {exc!r}"
    assert real_call_results == [{"id": "acct-1", "name": "Test Account"}]
    assert events[-1]["type"] == "done"


# ── 3) Late-step failure triggers rollback that sees the logging version ────


def test_provision_partial_failure_at_step_7_triggers_rollback(tmp_path, monkeypatch):
    """Realistic production failure: step 7 (``ensure_logging_endpoint``)
    succeeds — Fastly now has a new active version with the logging
    endpoint on it — and then step 8 (``write_service_config``) raises.
    The orchestrator must invoke ``perform_teardown`` with state that
    carries ``activated_logging_version`` + ``endpoint_name`` so the
    teardown can remove what was just created. The existing step-3 test
    only covers a failure BEFORE any Fastly-side state was created.

    Observed-behaviour note: the orchestrator wraps all 8 steps in one
    try/except, so any post-step-7 raise (write_service_config or
    install_capture_snippets bubbling up out of ensure_logging_endpoint)
    triggers the same rollback branch. We simulate at write_service_
    config because state has the freshly-set activated_logging_version
    by the time the raise fires — same teardown shape either way."""
    monkeypatch.chdir(tmp_path)
    teardown_calls = []

    def fake_teardown(state, token, opts=None):
        teardown_calls.append({"state": dict(state), "token": token, "opts": opts})
        yield {"type": "status", "message": "rollback step"}

    call_count = 0

    def raising_write(state):
        nonlocal call_count
        call_count += 1
        if call_count > 2:
            raise RuntimeError("config write failed mid-flow")

    with (
        _mocked_provision_steps(
            logging_return=99,
            write_side_effect=raising_write,
        ),
        patch("backend.provision.orchestrator.perform_teardown", side_effect=fake_teardown),
        patch("backend.config.config_path", return_value=str(tmp_path / "no.json")),
    ):
        events, exc = _consume(orchestrator.provision(_provision_cfg()))

    # Original exception is re-raised after teardown
    assert isinstance(exc, RuntimeError)
    assert "config write failed mid-flow" in str(exc)

    # perform_teardown was called exactly once with the right rollback state
    assert len(teardown_calls) == 1
    ts = teardown_calls[0]["state"]
    assert ts["activated_logging_version"] == 99
    assert ts.get("endpoint_name", "Test Endpoint") == "Test Endpoint"
    assert ts["logging_service_id"] == "SU3xxxxxxxxxxxxxx0000"
    assert ts["cdn_service_id"] == "cdn-svc-id"
    assert ts["fos_key_id"] == "ID_P"  # permanent FOS key — teardown revokes it

    # SSE stream surfaces an error event before re-raise
    error_events = [e for e in events if e["type"] == "error"]
    assert any("config write failed mid-flow" in e["message"] for e in error_events)


# ── 4) Config is persisted early, before the fallible CDN/logging steps ──────


def test_provision_persists_config_before_cdn_and_logging_steps(tmp_path, monkeypatch):
    """Regression: the service config must be written as soon as permanent
    FOS creds + bucket exist (after step 4), BEFORE the fallible CDN/logging
    steps and the streaming-gated finalize write at step 8.

    Previously the ONLY config write was step 8, and it runs *after* a
    ``yield`` in this SSE generator — so a client disconnect (or a backend
    teardown mid-provision) any time after the bucket was created closed the
    generator at that yield and the config never landed. That orphaned the
    FOS bucket with no config the backend could read: the telemetry-proxy
    logs ``no config for service_id`` and forwards the FOS request unsigned
    → 403 → empty / "internally inconsistent" dashboard. Asserting the
    config is persisted before the CDN step pins the early-write fix and
    that it survives the consumer dropping mid-stream."""
    from unittest.mock import MagicMock

    monkeypatch.chdir(tmp_path)
    write_mock = MagicMock()
    calls_before_cdn = None

    with (
        patch("backend.provision.orchestrator.validate_log_format", return_value=[]),
        patch(
            "backend.provision.orchestrator.ensure_fos_access_key",
            side_effect=[_fos_key("T"), _fos_key("P")],
        ),
        patch("backend.provision.orchestrator.ensure_fos_bucket"),
        patch("backend.provision.orchestrator.delete_fos_access_key"),
        patch("backend.provision.orchestrator.ensure_cdn_service", return_value={"id": "cdn-svc-id"}),
        patch("backend.provision.orchestrator.ensure_logging_via_reconciler", return_value=42),
        patch("backend.provision.orchestrator.write_service_config", write_mock),
        patch("backend.core.duckdb.get_source_for_service", return_value=None),
    ):
        gen = orchestrator.provision(_provision_cfg())
        try:
            for e in gen:
                # Step 6/8 is the first fallible Fastly-CDN step.
                if "Step 6/8" in e.get("message", ""):
                    calls_before_cdn = write_mock.call_count
                    break
        finally:
            gen.close()  # simulate the SSE consumer dropping mid-stream

    assert calls_before_cdn is not None, "never reached the CDN-creation step"
    assert calls_before_cdn >= 1, (
        "service config was not persisted before the fallible CDN/logging steps — "
        "a stream interruption after this point would orphan the FOS bucket with no config"
    )

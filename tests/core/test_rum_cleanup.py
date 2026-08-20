"""Tests for backend.core.rum_ingest.cleanup_old_rum_logs.

Deletes RUM beacon logs from FOS older than the configured retention
window. Uses the moto-backed ``s3_mock`` fixture (real S3 list/delete
semantics) rather than a hand-rolled paginator stub, so the
retention-cutoff and delete-count logic runs against real object
LastModified timestamps.

``rum_ingest.py`` imports ``_get_fos_client`` at module scope
(``from backend.core.duckdb import _get_fos_client``), so the shared
``s3_mock`` fixture's ``monkeypatch.setattr("backend.core.duckdb._get_fos_client", ...)``
does NOT reach it — that only rebinds the attribute on the ``duckdb``
module, not the name already bound into ``rum_ingest``'s namespace at
import time. Every test here additionally patches
``backend.core.rum_ingest._get_fos_client`` directly so calls actually
reach moto instead of silently falling through to the real (unmocked)
telemetry-proxy-routed client and erroring out — which would make these
tests pass vacuously via the outer except-and-return-(0, 0) branch
regardless of whether the retention logic is correct.

moto stamps ``LastModified`` as wall-clock "now" on ``put_object`` with no
way to backdate it, so to exercise the "object IS older than the cutoff"
branch, the module's notion of "now" is pushed into the future via a
``datetime`` subclass — the object's real LastModified then falls behind
the (fast-forwarded) cutoff, exactly like a genuinely stale object would.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import patch

from backend.core.rum_ingest import cleanup_old_rum_logs


class _FastForwardedDateTime(datetime):
    """``datetime`` subclass whose ``.now()`` is offset into the future."""

    _offset = timedelta(days=0)

    @classmethod
    def now(cls, tz=None):
        return datetime.now(tz) + cls._offset


def _fast_forwarded(days: int):
    return type("_FF", (_FastForwardedDateTime,), {"_offset": timedelta(days=days)})


def _put(s3_mock, key: str, body: bytes = b"x") -> None:
    s3_mock.put_object(Bucket="test-bucket", Key=key, Body=body)


def _patched_client(s3_mock):
    """Patch rum_ingest's own bound ``_get_fos_client`` name to hand back
    the moto client, bypassing the real telemetry-proxy-routed one."""
    return patch("backend.core.rum_ingest._get_fos_client", lambda _src: s3_mock)


@patch("backend.core.rum_ingest.get_source_for_service")
@patch("backend.config.load_config")
def test_delete_after_disabled_is_a_noop(mock_load_config, mock_get_source, fos_source):
    mock_load_config.return_value = {"rum": {"delete_after": False}}
    mock_get_source.return_value = fos_source

    files, freed = cleanup_old_rum_logs("svc1")

    assert (files, freed) == (0, 0)
    mock_get_source.assert_not_called()  # short-circuits before even resolving the source


@patch("backend.core.rum_ingest.get_source_for_service")
@patch("backend.config.load_config")
def test_no_source_bucket_configured_returns_zero(mock_load_config, mock_get_source, fos_source):
    mock_load_config.return_value = {"rum": {"delete_after": 30}}
    mock_get_source.return_value = {**fos_source, "bucket": ""}

    files, freed = cleanup_old_rum_logs("svc1")

    assert (files, freed) == (0, 0)


@patch("backend.core.rum_ingest.get_source_for_service")
@patch("backend.config.load_config")
def test_deletes_only_objects_older_than_retention_window(mock_load_config, mock_get_source, s3_mock, fos_source):
    source = {**fos_source, "prefix": "test-prefix"}
    mock_load_config.return_value = {"rum": {"delete_after": 7}}
    mock_get_source.return_value = source

    _put(s3_mock, "test-prefix/rum/raw/old-file.log.gz", body=b"y" * 500)
    _put(s3_mock, "test-prefix/not-rum/other.log.gz", body=b"z" * 999)  # outside the rum/ prefix — must survive

    # Fast-forward "now" by 30 days so the (real-timestamped) object falls
    # behind the 7-day retention cutoff, i.e. simulate a genuinely stale file.
    with _patched_client(s3_mock), patch("backend.core.rum_ingest.datetime", _fast_forwarded(30)):
        files, freed = cleanup_old_rum_logs("svc1")

    assert files == 1
    assert freed == 500
    remaining = s3_mock.list_objects_v2(Bucket="test-bucket", Prefix="test-prefix/")
    remaining_keys = [o["Key"] for o in remaining.get("Contents", [])]
    assert "test-prefix/not-rum/other.log.gz" in remaining_keys
    assert "test-prefix/rum/raw/old-file.log.gz" not in remaining_keys


@patch("backend.core.rum_ingest.get_source_for_service")
@patch("backend.config.load_config")
def test_objects_within_retention_window_are_kept(mock_load_config, mock_get_source, s3_mock, fos_source):
    mock_load_config.return_value = {"rum": {"delete_after": 90}}
    mock_get_source.return_value = fos_source

    _put(s3_mock, "rum/raw/fresh-file.log.gz")

    with _patched_client(s3_mock):
        files, freed = cleanup_old_rum_logs("svc1")

    assert (files, freed) == (0, 0)
    remaining = s3_mock.list_objects_v2(Bucket="test-bucket", Prefix="rum/raw/")
    assert len(remaining.get("Contents", [])) == 1


@patch("backend.core.rum_ingest.get_source_for_service")
@patch("backend.config.load_config")
def test_directory_marker_keys_are_skipped(mock_load_config, mock_get_source, s3_mock, fos_source):
    mock_load_config.return_value = {"rum": {"delete_after": 7}}
    mock_get_source.return_value = fos_source

    _put(s3_mock, "rum/raw/")  # a directory-marker-style key (trailing slash)

    with _patched_client(s3_mock), patch("backend.core.rum_ingest.datetime", _fast_forwarded(30)):
        files, freed = cleanup_old_rum_logs("svc1")

    assert (files, freed) == (0, 0)


@patch("backend.core.rum_ingest.get_source_for_service")
@patch("backend.config.load_config")
def test_fos_listing_failure_is_caught_and_returns_zero(mock_load_config, mock_get_source, fos_source):
    mock_load_config.return_value = {"rum": {"delete_after": 30}}
    mock_get_source.return_value = fos_source

    with patch("backend.core.rum_ingest._get_fos_client") as mock_client:
        mock_client.return_value.get_paginator.side_effect = RuntimeError("network unreachable")
        files, freed = cleanup_old_rum_logs("svc1")

    assert (files, freed) == (0, 0)


@patch("backend.core.rum_ingest.get_source_for_service")
@patch("backend.config.load_config")
def test_per_object_delete_failure_is_swallowed_and_others_still_deleted(
    mock_load_config, mock_get_source, s3_mock, fos_source
):
    mock_load_config.return_value = {"rum": {"delete_after": 7}}
    mock_get_source.return_value = fos_source

    _put(s3_mock, "rum/raw/a.log.gz", body=b"a" * 100)
    _put(s3_mock, "rum/raw/b.log.gz", body=b"b" * 200)

    real_delete = s3_mock.delete_object

    def _flaky_delete(Bucket, Key):
        if Key.endswith("a.log.gz"):
            raise RuntimeError("simulated delete failure")
        return real_delete(Bucket=Bucket, Key=Key)

    with (
        _patched_client(s3_mock),
        patch.object(s3_mock, "delete_object", side_effect=_flaky_delete),
        patch("backend.core.rum_ingest.datetime", _fast_forwarded(30)),
    ):
        files, freed = cleanup_old_rum_logs("svc1")

    # "a" failed to delete (swallowed, non-fatal); "b" succeeded.
    assert files == 1
    assert freed == 200

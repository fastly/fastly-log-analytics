"""Tests for ``backend.core.lake_info`` — Iceberg lake metadata fetch.

``fetch_lake_info`` is the single source of truth for the time range
and snapshot calendar the frontend's date pickers and Iceberg view
use. It has a two-tier strategy:

1. **Fast path** — read a pre-computed ``table_summary.json`` from S3
   (or via the CDN, if the source has ``cdn_url``).
2. **Iceberg fallback** — open the Iceberg table directly and derive
   info from its metadata.

Both paths need pinning because every chart on the dashboard renders
the wrong window if the timestamp range is off.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

# ── Helpers ──────────────────────────────────────────────────────────────────


@pytest.fixture
def fos_src() -> dict:
    return {
        "name": "logs_lake",
        "service_id": "svc-lake",
        "bucket": "test-bucket",
        "prefix": "",
        "region": "us-east-1",
        "endpoint": "us-east-1.object.fastlystorage.app",
        "access_key_id": "k",
        "secret_access_key": "s",
    }


def _summary_payload() -> dict:
    return {
        "info": {"min_timestamp": "2026-01-01T00:00:00Z", "max_timestamp": "2026-01-02T00:00:00Z"},
        "calendar": [{"day": "2026-01-01"}, {"day": "2026-01-02"}],
        "range": {"start": "2026-01-01T00:00:00Z", "end": "2026-01-02T00:00:00Z"},
    }


# ── Fast path: S3 ────────────────────────────────────────────────────────────


def test_fast_path_returns_payload_from_s3(fos_src):
    """Happy path: ``table_summary.json`` exists in S3 → fetch_lake_info
    returns its parsed contents without touching Iceberg."""
    payload = _summary_payload()

    fake_resp = {"Body": MagicMock()}
    fake_resp["Body"].read.return_value = json.dumps(payload).encode("utf-8")
    fake_s3 = MagicMock()
    fake_s3.get_object.return_value = fake_resp

    with (
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
        patch(
            "backend.core.iceberg._table_identifier",
            return_value=("ns", "tbl"),
        ),
    ):
        from backend.models.lake import fetch_lake_info

        out = fetch_lake_info(fos_src)

    assert out["ok"] is True
    assert out["table_exists"] is True
    assert out["info"] == payload["info"]
    assert out["calendar"] == payload["calendar"]
    assert out["range"]["start"] == "2026-01-01T00:00:00Z"
    # The S3 key must include the iceberg/{ns}/{tbl}/ path — pinned to
    # catch a refactor that flips the prefix layout.
    assert "iceberg/ns/tbl/table_summary.json" in fake_s3.get_object.call_args.kwargs["Key"]


def test_fast_path_handles_non_empty_source_prefix(fos_src):
    """``prefix='services/svc1'`` → S3 key is ``services/svc1/iceberg/...``,
    not ``/iceberg/...``. Pinned because a missing prefix would cause
    the lookup to land on the wrong customer's data."""
    src = {**fos_src, "prefix": "services/svc1"}

    fake_s3 = MagicMock()
    fake_s3.get_object.return_value = {"Body": MagicMock(read=lambda: json.dumps(_summary_payload()).encode("utf-8"))}

    with (
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
        patch("backend.core.iceberg._table_identifier", return_value=("ns", "tbl")),
    ):
        from backend.models.lake import fetch_lake_info

        fetch_lake_info(src)

    key = fake_s3.get_object.call_args.kwargs["Key"]
    assert key == "services/svc1/iceberg/ns/tbl/table_summary.json"


# ── Fast path: CDN ───────────────────────────────────────────────────────────


def test_fast_path_uses_cdn_when_cdn_url_is_set(fos_src):
    """When the source has ``cdn_url``, the helper goes through the CDN
    (cheaper than S3 reads) instead of the boto3 client. The secret
    query-param appends with URL encoding."""
    src = {**fos_src, "cdn_url": "https://cdn.example.com/", "cdn_secret": "shh secret"}
    payload_bytes = json.dumps(_summary_payload()).encode("utf-8")

    fake_resp = MagicMock()
    fake_resp.read.return_value = payload_bytes
    fake_resp.headers = {}
    cm = MagicMock()
    cm.__enter__.return_value = fake_resp
    cm.__exit__.return_value = False

    with (
        patch("urllib.request.urlopen", return_value=cm) as mock_open,
        patch("backend.utils.telemetry.record_cdn_call"),
        patch("backend.core.iceberg._table_identifier", return_value=("ns", "tbl")),
    ):
        from backend.models.lake import fetch_lake_info

        out = fetch_lake_info(src)

    assert out["table_exists"] is True
    # The CDN URL must include the secret AND URL-encode the space in it
    req = mock_open.call_args[0][0]
    full_url = req.full_url
    assert "cdn.example.com" in full_url
    assert "shh%20secret" in full_url
    # Don't double-slash the CDN base (trailing-slash safety)
    assert "//iceberg" not in full_url.split("cdn.example.com", 1)[1]


def test_fast_path_records_cdn_call_telemetry(fos_src):
    """Every CDN fetch must call ``record_cdn_call`` so the usage page
    can attribute bytes/latency. Pinned because skipping it would
    under-report customer egress and miscompute their bill."""
    src = {**fos_src, "cdn_url": "https://cdn.example.com"}
    payload_bytes = json.dumps(_summary_payload()).encode("utf-8")

    fake_resp = MagicMock()
    fake_resp.read.return_value = payload_bytes
    fake_resp.headers = {"x-cache": "HIT"}
    cm = MagicMock()
    cm.__enter__.return_value = fake_resp
    cm.__exit__.return_value = False

    with (
        patch("urllib.request.urlopen", return_value=cm),
        patch("backend.utils.telemetry.record_cdn_call") as mock_record,
        patch("backend.core.iceberg._table_identifier", return_value=("ns", "tbl")),
    ):
        from backend.models.lake import fetch_lake_info

        fetch_lake_info(src)

    mock_record.assert_called_once()
    kwargs = mock_record.call_args.kwargs
    assert kwargs["bytes_count"] == len(payload_bytes)
    assert kwargs["caller"] == "fetch_lake_info"


# ── Fast path: payload missing required fields → fall through to fallback ───


def test_fast_path_missing_info_falls_through_to_iceberg(fos_src):
    """If ``table_summary.json`` exists but lacks ``info`` / ``calendar``
    keys (legacy summary, partial write), the helper must fall through
    to the Iceberg discovery path, not return a malformed result."""
    fake_s3 = MagicMock()
    fake_s3.get_object.return_value = {"Body": MagicMock(read=lambda: json.dumps({"unrelated": True}).encode("utf-8"))}

    fake_table = object()  # init_iceberg_table is mocked, identity doesn't matter

    with (
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
        patch("backend.core.iceberg._table_identifier", return_value=("ns", "tbl")),
        patch("backend.core.iceberg.init_iceberg_table", return_value=fake_table),
        patch(
            "backend.core.iceberg.get_table_info",
            return_value={"min_timestamp": "2026-01-01T00:00:00Z", "max_timestamp": "2026-01-02T00:00:00Z"},
        ),
        patch("backend.core.iceberg.get_snapshot_calendar", return_value=[]),
    ):
        from backend.models.lake import fetch_lake_info

        out = fetch_lake_info(fos_src)

    assert out["table_exists"] is True
    assert out["info"]["min_timestamp"] == "2026-01-01T00:00:00Z"


# ── Iceberg fallback: direct ─────────────────────────────────────────────────


def test_iceberg_fallback_returns_table_info(fos_src):
    """When the fast path fails (e.g. ``get_object`` raises NoSuchKey),
    the Iceberg fallback opens the table and derives info/calendar
    directly."""
    fake_table = object()

    with (
        patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("no s3")),
        patch("backend.core.iceberg._table_identifier", return_value=("ns", "tbl")),
        patch("backend.core.iceberg.init_iceberg_table", return_value=fake_table),
        patch(
            "backend.core.iceberg.get_table_info",
            return_value={
                "min_timestamp": "2026-02-01T00:00:00Z",
                "max_timestamp": "2026-02-02T00:00:00Z",
                "row_count": 100,
            },
        ),
        patch(
            "backend.core.iceberg.get_snapshot_calendar",
            return_value=[{"day": "2026-02-01"}],
        ),
    ):
        from backend.models.lake import fetch_lake_info

        out = fetch_lake_info(fos_src)

    assert out["table_exists"] is True
    assert out["info"]["row_count"] == 100
    assert out["range"]["start"] == "2026-02-01T00:00:00Z"


def test_iceberg_fallback_returns_table_does_not_exist_when_none(fos_src):
    """``init_iceberg_table`` returning None means the table file isn't
    in the bucket yet (pre-first-ingest). The helper must surface this
    as ``table_exists: False`` so the UI shows the empty state instead
    of an error."""
    with (
        patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("no s3")),
        patch("backend.core.iceberg._table_identifier", return_value=("ns", "tbl")),
        patch("backend.core.iceberg.init_iceberg_table", return_value=None),
    ):
        from backend.models.lake import fetch_lake_info

        out = fetch_lake_info(fos_src)

    assert out["ok"] is True
    assert out["table_exists"] is False
    assert "not found" in out["message"].lower()


def test_iceberg_fallback_treats_not_found_error_as_empty_lake(fos_src):
    """``init_iceberg_table`` may raise with a message like "NoSuchTable"
    or "metadata.json does not exist" — both must be coerced to the
    same ``table_exists: False`` shape, not surfaced as `ok: False`."""
    with (
        patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("no s3")),
        patch("backend.core.iceberg._table_identifier", return_value=("ns", "tbl")),
        patch("backend.core.iceberg.init_iceberg_table", side_effect=Exception("NoSuchTable: missing")),
    ):
        from backend.models.lake import fetch_lake_info

        out = fetch_lake_info(fos_src)

    assert out == {"ok": True, "table_exists": False, "message": "Iceberg table not found in bucket."}


def test_iceberg_fallback_surfaces_unexpected_errors(fos_src):
    """A real error (S3 perms, decode error) — NOT a missing-table —
    surfaces as ``ok: False`` with the error string. The frontend
    distinguishes this from the empty-lake case to render a different
    UI affordance."""
    with (
        patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("no s3")),
        patch("backend.core.iceberg._table_identifier", return_value=("ns", "tbl")),
        patch("backend.core.iceberg.init_iceberg_table", side_effect=Exception("403 AccessDenied")),
    ):
        from backend.models.lake import fetch_lake_info

        out = fetch_lake_info(fos_src)

    assert out["ok"] is False
    assert "403" in out["error"]


# ── Iceberg fallback: temp cache (use_temp_cache=True) ───────────────────────


def test_temp_cache_path_clears_source_caches_on_exit(fos_src):
    """``use_temp_cache=True`` is used during PROVISIONING (the service
    isn't registered yet). After deriving info, it MUST call
    ``clear_source_caches`` — otherwise the in-memory catalog cache
    grows by one entry per provisioning attempt that gets aborted."""
    fake_table = object()

    with (
        patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("no s3")),
        patch("backend.core.iceberg._table_identifier", return_value=("ns", "tbl")),
        patch("backend.core.iceberg.init_iceberg_table", return_value=fake_table),
        patch(
            "backend.core.iceberg.get_table_info",
            return_value={"min_timestamp": "x", "max_timestamp": "y"},
        ),
        patch("backend.core.iceberg.get_snapshot_calendar", return_value=[]),
        patch("backend.core.iceberg.clear_source_caches") as mock_clear,
    ):
        from backend.models.lake import fetch_lake_info

        fetch_lake_info(fos_src, use_temp_cache=True)

    mock_clear.assert_called_once_with(fos_src["name"])


def test_temp_cache_path_returns_empty_lake_when_table_missing(fos_src):
    """Same empty-table coercion as the direct path, but routed through
    the temp_cache branch — pinned because both branches must agree
    on the shape they return to the provision wizard."""
    with (
        patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("no s3")),
        patch("backend.core.iceberg._table_identifier", return_value=("ns", "tbl")),
        patch("backend.core.iceberg.init_iceberg_table", return_value=None),
        patch("backend.core.iceberg.clear_source_caches"),
    ):
        from backend.models.lake import fetch_lake_info

        out = fetch_lake_info(fos_src, use_temp_cache=True)

    assert out == {"ok": True, "table_exists": False, "message": "Iceberg table not found in bucket."}


def test_temp_cache_path_surfaces_unexpected_errors(fos_src):
    """An unexpected error inside the temp_cache branch must surface
    via the outer except as ``ok: False``."""
    with (
        patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("no s3")),
        patch("backend.core.iceberg._table_identifier", return_value=("ns", "tbl")),
        patch("backend.core.iceberg.init_iceberg_table", side_effect=Exception("403 boom")),
        patch("backend.core.iceberg.clear_source_caches"),
    ):
        from backend.models.lake import fetch_lake_info

        out = fetch_lake_info(fos_src, use_temp_cache=True)

    assert out["ok"] is False
    assert "403" in out["error"]

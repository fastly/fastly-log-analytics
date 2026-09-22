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

import io
import json
import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from tests.core.test_ducklake_layer import _make_source, _write_buffer


def _bytes_response(data: bytes, headers: dict | None = None):
    """Build a context-manager mock whose ``read([size])`` drains a BytesIO,
    matching the production behaviour (read returns ``b""`` once exhausted)
    so size/deadline-bounded readers terminate."""
    buf = io.BytesIO(data)
    resp = MagicMock()
    resp.read.side_effect = buf.read
    resp.headers = headers or {}
    cm = MagicMock()
    cm.__enter__.return_value = resp
    cm.__exit__.return_value = False
    return cm


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

    body = MagicMock()
    body.read.side_effect = io.BytesIO(json.dumps(payload).encode("utf-8")).read
    fake_resp = {"Body": body}
    fake_s3 = MagicMock()
    fake_s3.get_object.return_value = fake_resp

    with (
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
        patch(
            "backend.core.iceberg._table_identifier",
            return_value=("ns", "tbl"),
        ),
    ):
        from backend.core.iceberg.lake_info import fetch_lake_info

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

    body = MagicMock()
    body.read.side_effect = io.BytesIO(json.dumps(_summary_payload()).encode("utf-8")).read
    fake_s3 = MagicMock()
    fake_s3.get_object.return_value = {"Body": body}

    with (
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
        patch("backend.core.iceberg._table_identifier", return_value=("ns", "tbl")),
    ):
        from backend.core.iceberg.lake_info import fetch_lake_info

        fetch_lake_info(src)

    key = fake_s3.get_object.call_args.kwargs["Key"]
    assert key == "services/svc1/iceberg/ns/tbl/table_summary.json"


# ── Fast path: CDN ───────────────────────────────────────────────────────────


def test_fast_path_uses_cdn_when_cdn_url_is_set(fos_src):
    """When the source has ``cdn_url``, the helper goes through the CDN
    (cheaper than S3 reads) instead of the boto3 client. The secret
    query-param appends with URL encoding.

    The hostname must be an https Fastly hostname — fetch_lake_info's
    SSRF guard rejects anything else and falls through to the S3 SDK.
    """
    src = {**fos_src, "cdn_url": "https://cdn-test.fastly.net/", "cdn_secret": "shh secret"}
    payload_bytes = json.dumps(_summary_payload()).encode("utf-8")
    cm = _bytes_response(payload_bytes)

    with (
        patch("urllib.request.urlopen", return_value=cm) as mock_open,
        patch("backend.utils.telemetry.record_cdn_call"),
        patch("backend.core.iceberg._table_identifier", return_value=("ns", "tbl")),
    ):
        from backend.core.iceberg.lake_info import fetch_lake_info

        out = fetch_lake_info(src)

    assert out["table_exists"] is True
    # The CDN URL must include the secret AND URL-encode the space in it
    req = mock_open.call_args[0][0]
    full_url = req.full_url
    assert "cdn-test.fastly.net" in full_url
    assert "shh%20secret" in full_url
    # Don't double-slash the CDN base (trailing-slash safety)
    assert "//iceberg" not in full_url.split("cdn-test.fastly.net", 1)[1]


def test_fast_path_records_cdn_call_telemetry(fos_src):
    """Every CDN fetch must call ``record_cdn_call`` so the usage page
    can attribute bytes/latency. Pinned because skipping it would
    under-report customer egress and miscompute their bill."""
    src = {**fos_src, "cdn_url": "https://cdn-test.fastly.net"}
    payload_bytes = json.dumps(_summary_payload()).encode("utf-8")
    cm = _bytes_response(payload_bytes, headers={"x-cache": "HIT"})

    with (
        patch("urllib.request.urlopen", return_value=cm),
        patch("backend.utils.telemetry.record_cdn_call") as mock_record,
        patch("backend.core.iceberg._table_identifier", return_value=("ns", "tbl")),
    ):
        from backend.core.iceberg.lake_info import fetch_lake_info

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
    body = MagicMock()
    body.read.side_effect = io.BytesIO(json.dumps({"unrelated": True}).encode("utf-8")).read
    fake_s3 = MagicMock()
    fake_s3.get_object.return_value = {"Body": body}

    with (
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
        patch("backend.core.iceberg._table_identifier", return_value=("ns", "tbl")),
        patch("backend.core.iceberg.ducklake_table_exists", return_value=True),
        patch(
            "backend.core.iceberg.get_table_info",
            return_value={"min_timestamp": "2026-01-01T00:00:00Z", "max_timestamp": "2026-01-02T00:00:00Z"},
        ),
        patch("backend.core.iceberg.get_snapshot_calendar", return_value=[]),
    ):
        from backend.core.iceberg.lake_info import fetch_lake_info

        out = fetch_lake_info(fos_src)

    assert out["table_exists"] is True
    assert out["info"]["min_timestamp"] == "2026-01-01T00:00:00Z"


# ── DuckLake fallback: direct (v3 write-path cutover) ────────────────────────
#
# The pyiceberg catalog these used to mock (`init_iceberg_table`) is
# permanently frozen post-DuckLake-cutover — see `backend/core/iceberg/
# _ducklake.py` module docstring. `_fetch_direct` now reads real DuckLake
# state, so these are integration tests against a real file-backed DuckLake
# catalog (via `_make_source`/`_write_buffer`/`_commit_buffer_impl` from
# `test_ducklake_layer.py`), not mocks of the pyiceberg Table API.


def test_iceberg_fallback_returns_table_info(tmp_path):
    """When the fast path fails (e.g. ``get_object`` raises NoSuchKey),
    the DuckLake fallback reads real committed lake state directly."""
    src = _make_source(tmp_path, f"lk{uuid.uuid4().hex[:8]}")
    ts = datetime(2026, 2, 1, tzinfo=UTC)
    _write_buffer(src, "batch.parquet", ts=ts, source_file="s3://b/raw/a.gz", n=2)
    from backend.core.iceberg.buffer import _commit_buffer_impl

    assert _commit_buffer_impl(src)["rows_committed"] == 2

    with patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("no s3")):
        from backend.core.iceberg.lake_info import fetch_lake_info

        out = fetch_lake_info(src)

    assert out["ok"] is True
    assert out["table_exists"] is True
    assert out["info"]["min_timestamp"] == "2026-02-01T00:00:00+00:00"
    assert out["info"]["data_files"] == 2
    assert out["calendar"] == {"2026-02-01": {"data_files": 2, "size_bytes": 0}}
    assert out["range"]["start"] == "2026-02-01T00:00:00+00:00"


def test_iceberg_fallback_returns_table_does_not_exist_when_none(tmp_path):
    """A DuckLake catalog with no committed table yet (pre-first-ingest)
    must surface as ``table_exists: False`` so the UI shows the empty
    state instead of an error."""
    src = _make_source(tmp_path, f"lk{uuid.uuid4().hex[:8]}")

    with patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("no s3")):
        from backend.core.iceberg.lake_info import fetch_lake_info

        out = fetch_lake_info(src)

    assert out == {"ok": True, "table_exists": False, "message": "DuckLake table not found."}


def test_iceberg_fallback_surfaces_unexpected_errors(tmp_path):
    """A real error (bad DuckLake attach) — NOT a missing-table — surfaces
    as ``ok: False`` with the error string. The frontend distinguishes
    this from the empty-lake case to render a different UI affordance."""
    src = _make_source(tmp_path, f"lk{uuid.uuid4().hex[:8]}")

    def _boom(con, source, read_only=False):
        raise RuntimeError("403 AccessDenied")

    with (
        patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("no s3")),
        patch("backend.core.iceberg._ducklake._ducklake_attach", side_effect=_boom),
    ):
        from backend.core.iceberg.lake_info import fetch_lake_info

        out = fetch_lake_info(src)

    assert out["ok"] is False
    assert "403" in out["error"]


# ── DuckLake fallback: temp cache (use_temp_cache=True) ──────────────────────


def test_temp_cache_path_clears_source_caches_on_exit(tmp_path):
    """``use_temp_cache=True`` is used during PROVISIONING (the service
    isn't registered yet). After deriving info, it MUST call
    ``clear_source_caches`` — otherwise the in-memory catalog cache
    grows by one entry per provisioning attempt that gets aborted."""
    src = _make_source(tmp_path, f"lk{uuid.uuid4().hex[:8]}")
    ts = datetime(2026, 1, 1, tzinfo=UTC)
    _write_buffer(src, "batch.parquet", ts=ts, source_file="s3://b/raw/a.gz", n=1)
    from backend.core.iceberg.buffer import _commit_buffer_impl

    assert _commit_buffer_impl(src)["rows_committed"] == 1

    with (
        patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("no s3")),
        patch("backend.core.iceberg.clear_source_caches") as mock_clear,
    ):
        from backend.core.iceberg.lake_info import fetch_lake_info

        out = fetch_lake_info(src, use_temp_cache=True)

    assert out["table_exists"] is True
    mock_clear.assert_called_once_with(src["name"])


def test_temp_cache_path_returns_empty_lake_when_table_missing(tmp_path):
    """Same empty-table coercion as the direct path, but routed through
    the temp_cache branch — pinned because both branches must agree
    on the shape they return to the provision wizard."""
    src = _make_source(tmp_path, f"lk{uuid.uuid4().hex[:8]}")

    with (
        patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("no s3")),
        patch("backend.core.iceberg.clear_source_caches"),
    ):
        from backend.core.iceberg.lake_info import fetch_lake_info

        out = fetch_lake_info(src, use_temp_cache=True)

    assert out == {"ok": True, "table_exists": False, "message": "DuckLake table not found."}


def test_temp_cache_path_surfaces_unexpected_errors(tmp_path):
    """An unexpected error inside the temp_cache branch must surface
    via the outer except as ``ok: False``."""
    src = _make_source(tmp_path, f"lk{uuid.uuid4().hex[:8]}")

    def _boom(con, source, read_only=False):
        raise RuntimeError("403 boom")

    with (
        patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("no s3")),
        patch("backend.core.iceberg._ducklake._ducklake_attach", side_effect=_boom),
        patch("backend.core.iceberg.clear_source_caches"),
    ):
        from backend.core.iceberg.lake_info import fetch_lake_info

        out = fetch_lake_info(src, use_temp_cache=True)

    assert out["ok"] is False
    assert "403" in out["error"]

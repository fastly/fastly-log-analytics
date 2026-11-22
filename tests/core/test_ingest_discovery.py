"""Unit tests for ``backend.core.ingest`` and ``backend.core.ingest.parser``."""

from __future__ import annotations

import gzip
from pathlib import Path
from unittest.mock import patch

import pytest
import zstandard as zstd

import backend.core.ingest as ingest_mod
from backend.core.ingest import parser as ingest_parser


def test_detect_compression_magic_and_extension(tmp_path: Path):
    gz_file = tmp_path / "sample.log.gz"
    with gzip.open(gz_file, "wb") as fh:
        fh.write(b'{"timestamp":"2026-01-01T00:00:00Z"}\n')
    assert ingest_parser._detect_compression(str(gz_file)) == "gzip"

    zst_file = tmp_path / "sample.log.zst"
    cctx = zstd.ZstdCompressor()
    zst_file.write_bytes(cctx.compress(b'{"timestamp":"2026-01-01T00:00:00Z"}\n'))
    assert ingest_parser._detect_compression(str(zst_file)) == "zstd"

    plain_file = tmp_path / "sample.log"
    plain_file.write_bytes(b'{"timestamp":"2026-01-01T00:00:00Z"}\n')
    assert ingest_parser._detect_compression(str(plain_file)) == "none"

    missing_gz = tmp_path / "missing.gz"
    assert ingest_parser._detect_compression(str(missing_gz)) == "gzip"

    missing_zst = tmp_path / "missing.zst"
    assert ingest_parser._detect_compression(str(missing_zst)) == "zstd"


def test_open_compressed_log_and_corrupt_detection(tmp_path: Path):
    gz_file = tmp_path / "valid.gz"
    with gzip.open(gz_file, "wb") as fh:
        fh.write(b'{"timestamp":"2026-01-01T00:00:00Z"}\n')
    assert ingest_parser._corrupt_gzip_error(str(gz_file)) is None

    bad_gz = tmp_path / "corrupt.gz"
    bad_gz.write_bytes(b"\x1f\x8b\xff\x00not-a-valid-gzip-stream")
    err = ingest_parser._corrupt_gzip_error(str(bad_gz))
    assert err is not None

    bad_zst = tmp_path / "corrupt.zst"
    bad_zst.write_bytes(b"not-zstd-magic")
    err_zst = ingest_parser._corrupt_gzip_error(str(bad_zst))
    assert isinstance(err_zst, zstd.ZstdError)


def test_scan_invalid_gzip_lines(tmp_path: Path):
    gz_file = tmp_path / "mixed.gz"
    payload = b'{"timestamp":"2026-01-01T00:00:00Z","status":200}\n\n{"status":200}\nnot-json-at-all\n'
    with gzip.open(gz_file, "wb") as fh:
        fh.write(payload)

    invalid = ingest_parser._scan_invalid_gzip_lines([str(gz_file)])
    assert len(invalid) == 2
    assert invalid[0] == (str(gz_file), '{"status":200}')
    assert invalid[1] == (str(gz_file), "not-json-at-all")


def test_outcome_counter_lifecycle():
    outcome = ingest_parser._new_object_outcome()
    assert outcome["objects_processed"] == 1
    assert outcome["objects_successful"] == 0

    finalized_ok = ingest_parser._finalize_object_outcome(dict(outcome))
    assert finalized_ok["objects_successful"] == 1
    assert finalized_ok["objects_partial"] == 0
    assert finalized_ok["objects_failed"] == 0

    partial = dict(outcome, malformed_records=2)
    finalized_partial = ingest_parser._finalize_object_outcome(partial)
    assert finalized_partial["objects_partial"] == 1
    assert finalized_partial["objects_successful"] == 0

    failed_capture = dict(outcome, quarantine_capture_failures=1)
    finalized_failed = ingest_parser._finalize_object_outcome(failed_capture)
    assert finalized_failed["objects_failed"] == 1
    assert finalized_failed["objects_successful"] == 0

    failed = ingest_parser._failed_object_outcome()
    assert failed["objects_failed"] == 1
    assert failed["objects_processed"] == 1

    summed = ingest_parser._sum_evidence_counts(
        [
            {"cap_evictions": 2, "quarantine_capture_failures": 1},
            {"cap_evictions": 3, "quarantine_capture_failures": 0},
        ],
        extra_failures=1,
    )
    assert summed == {"cap_evictions": 5, "quarantine_capture_failures": 2}


def test_finalize_sync_outcome_counters():
    counters = ingest_parser._new_object_outcome(processed=4)
    ingest_parser._finalize_sync_outcome_counters(
        counters,
        failed_paths={"a.gz"},
        capture_failed_paths=set(),
        delete_failed_paths={"b.gz"},
        partial_paths={"b.gz", "c.gz"},
        valid_records=100,
    )
    assert counters["valid_records"] == 100
    assert counters["source_delete_failures"] == 1
    assert counters["objects_failed"] == 2
    assert counters["objects_partial"] == 1
    assert counters["objects_successful"] == 1


def test_capture_corrupt_container_records_quarantine(tmp_path: Path):
    corrupt_path = tmp_path / "broken.gz"
    corrupt_path.write_bytes(b"corrupt-bytes")

    with patch(
        "backend.core.metadata.ingest_log.capture_evidence",
        return_value={"cap_evictions": 1, "quarantine_capture_failures": 0},
    ) as mock_capture:
        full = ingest_parser._capture_corrupt_container(
            "svc-test",
            "request",
            "raw/broken.gz",
            str(corrupt_path),
            OSError("bad header"),
            full_outcome=True,
        )
        assert full["corrupt_containers"] == 1
        assert full["cap_evictions"] == 1
        assert full["objects_failed"] == 1
        mock_capture.assert_called_once()

    missing_outcome = ingest_parser._capture_corrupt_container(
        "svc-test",
        "request",
        "raw/missing.gz",
        str(tmp_path / "does-not-exist.gz"),
        OSError("missing"),
        full_outcome=False,
    )
    assert missing_outcome["corrupt_containers"] == 1
    assert missing_outcome["quarantine_capture_failures"] == 1
    assert missing_outcome["valid_records"] == 0


def test_catalog_field_ids_and_type_hints():
    cfg = {
        "custom_fields": [
            {"name": "cf_enabled", "sql_type": "BIGINT", "enabled": True},
            {"name": "cf_disabled", "sql_type": "VARCHAR", "enabled": False},
        ]
    }
    hints = ingest_mod.get_ingest_type_hints(cfg)
    assert hints["timestamp"] == "TIMESTAMPTZ"
    assert hints["cf_enabled"] == "BIGINT"
    assert "cf_disabled" not in hints

    field_ids = ingest_mod.get_catalog_field_ids(cfg)
    assert "timestamp" in field_ids
    assert "cf_enabled" in field_ids
    assert "cf_disabled" not in field_ids


@pytest.mark.parametrize("attr", ["safe_float", "safe_int", "DEFAULT_FARO_VERSION"])
def test_delegated_ingest_attrs_resolve(attr: str):
    assert getattr(ingest_mod, attr) is not None

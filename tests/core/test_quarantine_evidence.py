from __future__ import annotations

import stat
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from backend.core import quarantine
from backend.core.metadata.quarantine import (
    delete_quarantine_evidence,
    get_quarantine_evidence,
    get_quarantine_evidence_summary,
    list_quarantine_evidence,
)


def test_capture_evidence_preserves_exact_bytes_and_metadata():
    payload = b'{"bad":"\x00\xff\xf0\x9f\x8e\x89"}\n'

    result = quarantine.capture_evidence(
        "evidence-service",
        "request",
        "raw/request/part.log.gz",
        payload,
        line_ordinal=7,
        byte_offset=128,
        error_category="invalid_json",
        error_text="invalid record",
    )

    assert result["quarantine_capture_failures"] == 0
    assert result["cap_evictions"] == 0
    item, evidence = quarantine.read_evidence("evidence-service", int(result["id"]))
    try:
        assert evidence.read() == payload
    finally:
        evidence.close()

    assert item["source_type"] == "request"
    assert item["original_key"] == "raw/request/part.log.gz"
    assert item["line_ordinal"] == 7
    assert item["byte_offset"] == 128
    assert item["byte_length"] == len(payload)
    assert item["error_category"] == "invalid_json"
    assert item["sha256"] == "7aa0ef0bdccc1d2cb18dac4f2af559c2a259024bae2653330536f95cbeb84fe8"
    evidence_path = quarantine._evidence_path("evidence-service", int(result["id"]))
    assert stat.S_IMODE(evidence_path.stat().st_mode) == 0o600
    assert stat.S_IMODE(evidence_path.parent.stat().st_mode) == 0o700
    assert quarantine.QUARANTINE_ITEM_CAP == 1_000


def test_capture_evidence_enforces_shared_fifo_cap(monkeypatch):
    monkeypatch.setattr(quarantine, "QUARANTINE_ITEM_CAP", 2)
    payloads = [b"first", b"second", b"third"]

    results = [
        quarantine.capture_evidence(
            "fifo-service",
            source_type,
            f"raw/{source_type}/{index}.gz",
            payload,
            line_ordinal=None,
            byte_offset=None,
            error_category="corrupt_container",
            error_text="invalid gzip",
        )
        for index, (source_type, payload) in enumerate(zip(("request", "rum", "request"), payloads, strict=True))
    ]

    assert [result["cap_evictions"] for result in results] == [0, 0, 1]
    rows = list_quarantine_evidence("fifo-service", oldest_first=True)
    assert [row["original_key"] for row in rows] == ["raw/rum/1.gz", "raw/request/2.gz"]
    assert quarantine.read_evidence("fifo-service", int(results[0]["id"])) is None


def test_evidence_summary_counts_sources_categories_and_bytes():
    for source_type, category, payload in (
        ("request", "invalid_json", b"bad"),
        ("rum", "invalid_json", b"worse"),
        ("request", "corrupt_container", b"gzip"),
    ):
        result = quarantine.capture_evidence(
            "summary-service",
            source_type,
            f"raw/{source_type}/{category}.gz",
            payload,
            line_ordinal=None,
            byte_offset=None,
            error_category=category,
            error_text="invalid evidence",
        )
        assert result["quarantine_capture_failures"] == 0

    summary = get_quarantine_evidence_summary("summary-service")
    assert summary["total_items"] == 3
    assert summary["total_bytes"] == 12
    assert summary["request_items"] == 2
    assert summary["rum_items"] == 1
    assert summary["category_counts"] == {"invalid_json": 2, "corrupt_container": 1}
    assert summary["oldest_at"] is not None
    assert summary["newest_at"] is not None


def test_concurrent_capture_keeps_all_items_and_purge_removes_bytes():
    payload = b"corrupt gzip bytes"

    def capture(index: int):
        return quarantine.capture_evidence(
            "concurrent-service",
            "rum" if index % 2 else "request",
            f"raw/{index}.gz",
            payload + str(index).encode(),
            line_ordinal=None,
            byte_offset=None,
            error_category="corrupt_container",
            error_text="invalid gzip",
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(capture, range(16)))

    assert all(result["quarantine_capture_failures"] == 0 for result in results)
    rows = list_quarantine_evidence("concurrent-service")
    assert len(rows) == 16

    metadata, evidence = quarantine.read_evidence("concurrent-service", int(results[0]["id"]))
    path = quarantine._evidence_path("concurrent-service", int(metadata["id"]))
    evidence.close()

    assert quarantine.purge_evidence("concurrent-service", int(metadata["id"])) == {"purged": 1}
    assert not path.exists()
    assert quarantine.purge_evidence("concurrent-service") == {"purged": 15}
    assert list_quarantine_evidence("concurrent-service") == []


def test_capture_failure_is_reported_to_the_ingest_caller(monkeypatch):
    def fail_storage_path(_service_id: str) -> Path:
        raise OSError("storage unavailable")

    monkeypatch.setattr(quarantine, "_evidence_dir", fail_storage_path)
    result = quarantine.capture_evidence(
        "unavailable-service",
        "request",
        "raw/unavailable.gz",
        b"bad line",
        line_ordinal=1,
        byte_offset=0,
        error_category="invalid_json",
        error_text="invalid record",
    )

    assert result == {"id": 0, "cap_evictions": 0, "quarantine_capture_failures": 1}


def test_capture_lock_failure_is_reported_to_the_ingest_caller(monkeypatch):
    class FailedServiceLock:
        def __enter__(self):
            raise TimeoutError("metadata lock unavailable")

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(quarantine, "_service_lock", lambda _service_id: FailedServiceLock())
    result = quarantine.capture_evidence(
        "unavailable-service",
        "request",
        "raw/unavailable.gz",
        b"bad line",
        line_ordinal=1,
        byte_offset=0,
        error_category="invalid_json",
        error_text="invalid record",
    )

    assert result == {"id": 0, "cap_evictions": 0, "quarantine_capture_failures": 1}


def test_fifo_eviction_retains_existing_evidence_when_file_unlink_fails(monkeypatch):
    monkeypatch.setattr(quarantine, "QUARANTINE_ITEM_CAP", 1)
    first = quarantine.capture_evidence(
        "unlink-service",
        "request",
        "raw/first.gz",
        b"first",
        line_ordinal=1,
        byte_offset=0,
        error_category="invalid_json",
        error_text="invalid record",
    )
    first_id = int(first["id"])
    first_path = quarantine._evidence_path("unlink-service", first_id)
    real_unlink = Path.unlink

    def fail_first_item(path: Path, *args, **kwargs):
        if path == first_path:
            raise PermissionError("file is busy")
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_first_item)
    second = quarantine.capture_evidence(
        "unlink-service",
        "rum",
        "raw/second.gz",
        b"second",
        line_ordinal=None,
        byte_offset=None,
        error_category="corrupt_container",
        error_text="invalid gzip",
    )

    assert second == {"id": 0, "cap_evictions": 0, "quarantine_capture_failures": 1}
    assert [item["original_key"] for item in list_quarantine_evidence("unlink-service")] == ["raw/first.gz"]
    assert first_path.exists()
    assert list(quarantine._evidence_dir("unlink-service").glob("*.dat")) == [first_path]


def test_purge_preserves_metadata_when_file_unlink_fails(monkeypatch):
    item = quarantine.capture_evidence(
        "failed-purge-service",
        "request",
        "raw/request/purge.gz",
        b"bad line",
        line_ordinal=1,
        byte_offset=0,
        error_category="invalid_json",
        error_text="invalid record",
    )
    item_id = int(item["id"])
    path = quarantine._evidence_path("failed-purge-service", item_id)
    real_unlink = Path.unlink

    def fail_item(path_to_unlink: Path, *args, **kwargs):
        if path_to_unlink == path:
            raise PermissionError("file is busy")
        return real_unlink(path_to_unlink, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_item)

    with pytest.raises(OSError, match="could not delete quarantine evidence"):
        quarantine.purge_evidence("failed-purge-service", item_id)

    assert get_quarantine_evidence("failed-purge-service", item_id) is not None
    assert path.exists()


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        ({"source_type": "other"}, ValueError),
        ({"payload": "not-bytes"}, TypeError),
        ({"line_ordinal": 0}, ValueError),
        ({"byte_offset": -1}, ValueError),
    ],
)
def test_capture_evidence_rejects_invalid_metadata(overrides, error):
    args = {
        "service_id": "invalid-evidence-service",
        "source_type": "request",
        "original_key": "raw/request/invalid.gz",
        "payload": b"bad",
        "line_ordinal": None,
        "byte_offset": None,
        "error_category": "invalid_json",
        "error_text": "invalid record",
    }
    args.update(overrides)

    with pytest.raises(error):
        quarantine.capture_evidence(**args)


def test_evidence_paths_reject_service_traversal():
    with pytest.raises(ValueError, match="outside the services data directory"):
        quarantine._evidence_dir("../../outside")


def test_read_evidence_rejects_metadata_paths_outside_the_evidence_directory(monkeypatch):
    monkeypatch.setattr(
        quarantine,
        "get_quarantine_evidence",
        lambda _service_id, _item_id: {"id": 17, "file_path": "../outside.dat"},
    )

    with pytest.raises(ValueError, match="invalid evidence path"):
        quarantine.read_evidence("invalid-path-service", 17)


def test_read_evidence_propagates_missing_local_file():
    result = quarantine.capture_evidence(
        "missing-file-service",
        "request",
        "raw/request/missing.gz",
        b"bad",
        line_ordinal=1,
        byte_offset=0,
        error_category="invalid_json",
        error_text="invalid record",
    )
    item_id = int(result["id"])
    quarantine._evidence_path("missing-file-service", item_id).unlink()

    with pytest.raises(FileNotFoundError):
        quarantine.read_evidence("missing-file-service", item_id)


def test_capture_failure_after_metadata_insert_cleans_metadata_and_temp_file(monkeypatch):
    def fail_replace(*_args):
        raise OSError("rename failed")

    monkeypatch.setattr(quarantine.os, "replace", fail_replace)

    result = quarantine.capture_evidence(
        "rename-failure-service",
        "rum",
        "raw/rum/rename-failure.gz",
        b"bad",
        line_ordinal=1,
        byte_offset=0,
        error_category="invalid_json",
        error_text="invalid record",
    )

    assert result == {"id": 0, "cap_evictions": 0, "quarantine_capture_failures": 1}
    assert list_quarantine_evidence("rename-failure-service") == []
    assert list(quarantine._evidence_dir("rename-failure-service").iterdir()) == []


def test_purge_missing_item_is_idempotent():
    assert quarantine.purge_evidence("empty-purge-service", 987654) == {"purged": 0}
    assert get_quarantine_evidence("empty-purge-service", 987654) is None


def test_empty_evidence_id_delete_is_a_noop():
    assert delete_quarantine_evidence("empty-delete-service", []) == 0
    assert list_quarantine_evidence("empty-delete-service") == []

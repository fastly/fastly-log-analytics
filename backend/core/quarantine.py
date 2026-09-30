"""Exact-byte local evidence storage for malformed request and RUM records."""

from __future__ import annotations

import hashlib
import logging
import os
import tempfile
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO

from backend import config
from backend.core.metadata import pg_connection
from backend.core.metadata.quarantine import (
    delete_quarantine_evidence,
    get_quarantine_evidence,
    insert_quarantine_evidence,
    list_quarantine_evidence,
)

logger = logging.getLogger(__name__)

QUARANTINE_ITEM_CAP = 1_000
_MAX_ERROR_TEXT_LENGTH = 2_000
_service_locks: dict[str, threading.RLock] = {}
_service_locks_guard = threading.Lock()


def _evidence_dir(service_id: str) -> Path:
    services_root = config.SERVICES_DATA_DIR.resolve()
    service_root = (services_root / service_id).resolve()
    if not service_root.is_relative_to(services_root):
        raise ValueError("service_id resolves outside the services data directory")
    evidence_dir = (service_root / "quarantine").resolve()
    if not evidence_dir.is_relative_to(service_root):
        raise ValueError("quarantine directory resolves outside the service data directory")
    evidence_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(evidence_dir, 0o700)
    return evidence_dir


def _evidence_path(service_id: str, item_id: int) -> Path:
    if item_id <= 0:
        raise ValueError("item_id must be positive")
    return _evidence_dir(service_id) / f"{item_id}.dat"


def _advisory_key(service_id: str) -> int:
    value = int.from_bytes(hashlib.sha256(b"fla-quarantine:" + service_id.encode()).digest()[:8], "big")
    return value - (1 << 64) if value >= (1 << 63) else value


@contextmanager
def _service_lock(service_id: str) -> Iterator[None]:
    with _service_locks_guard:
        lock = _service_locks.setdefault(service_id, threading.RLock())
    with lock:
        if not pg_connection.is_postgres():
            yield
            return
        with pg_connection.get_pg_pool().connection() as con:
            with con.transaction():
                con.execute("SELECT pg_advisory_xact_lock(%s)", (_advisory_key(service_id),))
                yield


def _unlink_evidence(service_id: str, item: dict) -> bool:
    item_id = int(item["id"])
    expected_name = f"{item_id}.dat"
    if item["file_path"] != expected_name:
        logger.error(
            "quarantine.evidence.invalid_path",
            extra={"service_id": service_id, "item_id": item_id},
        )
        return False
    path = _evidence_path(service_id, item_id)
    try:
        path.unlink(missing_ok=True)
        return True
    except OSError:
        logger.exception(
            "quarantine.evidence.unlink_failed",
            extra={"service_id": service_id, "item_id": item_id},
        )
        return False


def capture_evidence(
    service_id: str,
    source_type: str,
    original_key: str,
    payload: bytes,
    *,
    line_ordinal: int | None,
    byte_offset: int | None,
    error_category: str,
    error_text: str,
) -> dict[str, int | str]:
    if source_type not in {"request", "rum"}:
        raise ValueError("source_type must be 'request' or 'rum'")
    if not isinstance(payload, bytes):
        raise TypeError("payload must be bytes")
    if line_ordinal is not None and line_ordinal < 1:
        raise ValueError("line_ordinal must be positive when provided")
    if byte_offset is not None and byte_offset < 0:
        raise ValueError("byte_offset must be non-negative when provided")

    try:
        with _service_lock(service_id):
            return _capture_evidence_locked(
                service_id,
                source_type,
                original_key,
                payload,
                line_ordinal=line_ordinal,
                byte_offset=byte_offset,
                error_category=error_category,
                error_text=error_text,
            )
    except Exception:
        logger.exception(
            "quarantine.evidence.lock_failed",
            extra={"service_id": service_id, "source_type": source_type, "original_key": original_key},
        )
        return {"id": 0, "cap_evictions": 0, "quarantine_capture_failures": 1}


def _capture_evidence_locked(
    service_id: str,
    source_type: str,
    original_key: str,
    payload: bytes,
    *,
    line_ordinal: int | None,
    byte_offset: int | None,
    error_category: str,
    error_text: str,
) -> dict[str, int | str]:
    temp_path: Path | None = None
    item_id: int | None = None
    final_path: Path | None = None
    try:
        evidence_dir = _evidence_dir(service_id)
        with tempfile.NamedTemporaryFile(dir=evidence_dir, prefix=".capture-", delete=False) as evidence_file:
            temp_path = Path(evidence_file.name)
            evidence_file.write(payload)
            evidence_file.flush()
            os.fsync(evidence_file.fileno())

        item_id = insert_quarantine_evidence(
            service_id,
            source_type,
            original_key,
            line_ordinal=line_ordinal,
            byte_offset=byte_offset,
            byte_length=len(payload),
            error_category=error_category,
            error_text=error_text[:_MAX_ERROR_TEXT_LENGTH],
            sha256=hashlib.sha256(payload).hexdigest(),
        )
        final_path = evidence_dir / f"{item_id}.dat"
        os.replace(temp_path, final_path)
        temp_path = None

        rows = list_quarantine_evidence(service_id, limit=QUARANTINE_ITEM_CAP + 1, oldest_first=True)
        evicted = rows[: max(0, len(rows) - QUARANTINE_ITEM_CAP)]
        for item in evicted:
            if not _unlink_evidence(service_id, item):
                raise OSError(f"could not delete quarantine evidence {item['id']}")
        evicted_count = delete_quarantine_evidence(service_id, [int(item["id"]) for item in evicted])
        return {
            "id": item_id,
            "cap_evictions": evicted_count,
            "quarantine_capture_failures": 0,
        }
    except Exception:
        logger.exception(
            "quarantine.evidence.capture_failed",
            extra={"service_id": service_id, "source_type": source_type, "original_key": original_key},
        )
        if temp_path is not None:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                logger.exception("quarantine.evidence.temp_cleanup_failed", extra={"service_id": service_id})
        if item_id is not None:
            try:
                delete_quarantine_evidence(service_id, [item_id])
            except Exception:
                logger.exception(
                    "quarantine.evidence.metadata_cleanup_failed",
                    extra={"service_id": service_id, "item_id": item_id},
                )
        if final_path is not None:
            try:
                final_path.unlink(missing_ok=True)
            except OSError:
                logger.exception(
                    "quarantine.evidence.file_cleanup_failed",
                    extra={"service_id": service_id, "item_id": item_id},
                )
        return {"id": 0, "cap_evictions": 0, "quarantine_capture_failures": 1}


def read_evidence(service_id: str, item_id: int) -> tuple[dict, BinaryIO] | None:
    item = get_quarantine_evidence(service_id, item_id)
    if item is None:
        return None
    path = _evidence_path(service_id, item_id)
    if item["file_path"] != path.name:
        raise ValueError(f"invalid evidence path for item {item_id}")
    try:
        return item, path.open("rb")
    except OSError:
        logger.exception("quarantine.evidence.read_failed", extra={"service_id": service_id, "item_id": item_id})
        raise


def purge_evidence(service_id: str, item_id: int | None = None) -> dict[str, int]:
    with _service_lock(service_id):
        if item_id is None:
            items = list_quarantine_evidence(service_id, limit=2**31 - 1)
        else:
            item = get_quarantine_evidence(service_id, item_id)
            items = [item] if item is not None else []
        for item in items:
            if not _unlink_evidence(service_id, item):
                raise OSError(f"could not delete quarantine evidence {item['id']}")
        purged = delete_quarantine_evidence(service_id, [int(item["id"]) for item in items])
        return {"purged": purged}

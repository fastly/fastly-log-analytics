"""High-scale ClickHouse batch publication and visibility fencing."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol


class PublicationStatus(StrEnum):
    PENDING = "pending"
    VISIBLE = "visible"


class LostAcknowledgement(RuntimeError):
    """The insert may have committed, but its acknowledgement was lost."""


class PartialInsert(RuntimeError):
    def __init__(self, rows_inserted: int) -> None:
        super().__init__(f"partial ClickHouse insert: {rows_inserted} rows")
        self.rows_inserted = rows_inserted


@dataclass(frozen=True)
class HighScaleBatch:
    batch_id: str
    service_id: str
    domain: str
    generation: str
    rows: tuple[dict[str, Any], ...]

    @property
    def digest(self) -> str:
        payload = "\n".join(
            json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False) for row in self.rows
        )
        return f"sha256:{hashlib.sha256(payload.encode()).hexdigest()}"


@dataclass(frozen=True)
class BatchManifest:
    batch_id: str
    service_id: str
    domain: str
    generation: str
    digest: str
    expected_rows: int
    status: PublicationStatus
    visible_rows: int = 0


@dataclass(frozen=True)
class InsertReceipt:
    batch_id: str
    rows_inserted: int
    digest: str


@dataclass(frozen=True)
class PublicationResult:
    batch_id: str
    status: PublicationStatus
    rows_visible: int
    duplicate: bool


class BatchManifestStore(Protocol):
    def get(self, batch_id: str) -> BatchManifest | None: ...

    def put(self, manifest: BatchManifest) -> None: ...


class ClickHouseClient(Protocol):
    """Client contract: retries with one batch_id must not duplicate rows."""

    def insert(self, batch: HighScaleBatch) -> InsertReceipt: ...


class InMemoryBatchManifestStore:
    def __init__(self) -> None:
        self._manifests: dict[str, BatchManifest] = {}

    def get(self, batch_id: str) -> BatchManifest | None:
        return self._manifests.get(batch_id)

    def put(self, manifest: BatchManifest) -> None:
        self._manifests[manifest.batch_id] = manifest


class ClickHousePublication:
    def __init__(self, manifests: BatchManifestStore, client: ClickHouseClient) -> None:
        self._manifests = manifests
        self._client = client

    def publish(self, batch: HighScaleBatch) -> PublicationResult:
        manifest = self._manifests.get(batch.batch_id)
        if manifest is None:
            manifest = BatchManifest(
                batch_id=batch.batch_id,
                service_id=batch.service_id,
                domain=batch.domain,
                generation=batch.generation,
                digest=batch.digest,
                expected_rows=len(batch.rows),
                status=PublicationStatus.PENDING,
            )
            self._manifests.put(manifest)
        else:
            self._validate_existing(manifest, batch)
            if manifest.status is PublicationStatus.VISIBLE:
                return PublicationResult(batch.batch_id, manifest.status, manifest.visible_rows, True)

        receipt = self._client.insert(batch)
        if receipt.batch_id != batch.batch_id:
            raise ValueError("ClickHouse receipt batch id mismatch")
        if receipt.digest != batch.digest:
            raise ValueError("ClickHouse receipt digest mismatch")
        if receipt.rows_inserted != len(batch.rows):
            raise PartialInsert(receipt.rows_inserted)

        self._manifests.put(
            BatchManifest(
                batch_id=manifest.batch_id,
                service_id=manifest.service_id,
                domain=manifest.domain,
                generation=manifest.generation,
                digest=manifest.digest,
                expected_rows=manifest.expected_rows,
                status=PublicationStatus.VISIBLE,
                visible_rows=receipt.rows_inserted,
            )
        )
        return PublicationResult(batch.batch_id, PublicationStatus.VISIBLE, receipt.rows_inserted, False)

    def is_visible(self, batch_id: str) -> bool:
        manifest = self._manifests.get(batch_id)
        return manifest is not None and manifest.status is PublicationStatus.VISIBLE

    @staticmethod
    def _validate_existing(manifest: BatchManifest, batch: HighScaleBatch) -> None:
        if (
            manifest.service_id != batch.service_id
            or manifest.domain != batch.domain
            or manifest.generation != batch.generation
        ):
            raise ValueError("batch identity does not match existing manifest")
        if manifest.digest != batch.digest:
            raise ValueError("batch digest does not match existing manifest")

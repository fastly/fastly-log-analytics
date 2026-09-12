"""High-scale ClickHouse batch publication and visibility fencing."""

from __future__ import annotations

import hashlib
import json
import math
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Any, Protocol
from uuid import UUID, uuid5

from backend.core.clickhouse_client import (
    CLICKHOUSE_HIGH_SCALE_TABLES,
)
from backend.core.clickhouse_client import (
    ClickHouseClient as HttpClickHouseClient,
)
from backend.high_scale.schema import build_cmcd_projection_key

_BATCH_NAMESPACE = UUID("f3e6bd14-9df9-5f8a-9d92-7a4f1fd36d9a")
_EVENT_NAMESPACE = UUID("8e5d1e37-4e66-5b66-9f4c-73d7c4a3f0d8")
_PUBLICATION_TABLE = "high_scale_batch_publications"
_PUBLICATION_COLUMNS = (
    "batch_id",
    "service_id",
    "domain",
    "generation",
    "batch_digest",
    "expected_rows",
    "visible_rows",
    "quorum_acked",
    "publication_state",
    "manifest_version",
    "updated_at",
)
_DOMAIN_TABLES = {
    "request": "request_facts",
    "rum_vitals": "rum_vitals_facts",
    "rum_errors": "rum_error_facts",
    "cmcd": "cmcd_projection_facts",
}
_DOMAIN_COLUMNS = {
    "request": (
        "service_id",
        "event_id",
        "event_timestamp",
        "ingest_timestamp",
        "source_object_key",
        "source_object_version",
        "line_ordinal",
        "transform_version",
        "batch_id",
        "publication_state",
        "country",
        "client_ip",
        "url",
        "custom_fields",
        "cmcd",
    ),
    "rum_vitals": (
        "service_id",
        "event_id",
        "event_timestamp",
        "ingest_timestamp",
        "source_object_key",
        "source_object_version",
        "line_ordinal",
        "transform_version",
        "batch_id",
        "publication_state",
        "client_id",
        "request_event_id",
        "metric_name",
        "metric_value",
        "metric_rating",
        "pathname",
        "country",
    ),
    "rum_errors": (
        "service_id",
        "event_id",
        "event_timestamp",
        "ingest_timestamp",
        "source_object_key",
        "source_object_version",
        "line_ordinal",
        "transform_version",
        "batch_id",
        "publication_state",
        "client_id",
        "request_event_id",
        "error_message",
        "error_file",
        "pathname",
        "country",
    ),
    "cmcd": (
        "service_id",
        "projection_key",
        "request_event_id",
        "event_timestamp",
        "batch_id",
        "publication_state",
        "cmcd",
    ),
}


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


class ClickHouseBatchAdapter:
    """Adapt high-scale batches to the allowlisted ClickHouse HTTP client.

    Fact rows are written with a visible row state only behind a pending
    publication row. Readers must join the latest publication row and require
    ``publication_state = 'visible'``; this fence hides partial inserts and
    makes retries after a lost HTTP acknowledgement safe.
    """

    def __init__(self, client: HttpClickHouseClient) -> None:
        self._client = client
        self._batch_locks: dict[str, threading.Lock] = {}
        self._batch_locks_guard = threading.Lock()

    def insert(self, batch: HighScaleBatch) -> InsertReceipt:
        batch_uuid = _batch_uuid(batch)
        with self._batch_lock(batch_uuid):
            return self._insert_locked(batch, batch_uuid)

    def _insert_locked(self, batch: HighScaleBatch, batch_uuid: str) -> InsertReceipt:
        table = _DOMAIN_TABLES.get(batch.domain)
        if table is None or table not in CLICKHOUSE_HIGH_SCALE_TABLES:
            raise ValueError(f"unsupported high-scale domain: {batch.domain}")
        if not batch.rows:
            raise ValueError("high-scale batch must contain at least one row")
        mapped_rows = _rows_for_domain(batch, batch_uuid)
        existing = self._existing_publication(batch, batch_uuid)
        if existing is not None:
            self._validate_publication(existing, batch)
            if existing["publication_state"] == "visible":
                return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)
            version = int(existing["manifest_version"])
        else:
            version = 0
            self._insert_publication(batch, batch_uuid, version=1, state="pending", visible_rows=0)
            version = 1

        expected = len(batch.rows)
        existing_rows = self._count_rows(table, batch, batch_uuid)
        if existing_rows not in (0, expected):
            raise PartialInsert(existing_rows)
        if existing_rows == 0:
            self._client.insert_rows(table, list(_DOMAIN_COLUMNS[batch.domain]), mapped_rows)
            existing_rows = self._count_rows(table, batch, batch_uuid)
        if existing_rows != expected:
            raise PartialInsert(existing_rows)

        self._insert_publication(
            batch,
            batch_uuid,
            version=version + 1,
            state="visible",
            visible_rows=expected,
        )
        return InsertReceipt(batch.batch_id, expected, batch.digest)

    def _batch_lock(self, batch_uuid: str):
        with self._batch_locks_guard:
            lock = self._batch_locks.setdefault(batch_uuid, threading.Lock())
        return lock

    def _existing_publication(self, batch: HighScaleBatch, batch_uuid: str) -> dict[str, Any] | None:
        rows = self._client.execute(
            "SELECT batch_id, service_id, domain, generation, batch_digest, expected_rows, "
            "visible_rows, publication_state, manifest_version "
            "FROM high_scale_batch_publications FINAL "
            "WHERE service_id={service_id:String} AND domain={domain:String} "
            "AND generation={generation:String} AND batch_id={batch_id:String}",
            {
                "service_id": batch.service_id,
                "domain": batch.domain,
                "generation": batch.generation,
                "batch_id": batch_uuid,
            },
        )
        if not rows:
            return None
        if len(rows) != 1:
            raise ValueError("ClickHouse publication receipt is ambiguous")
        return rows[0]

    def _count_rows(self, table: str, batch: HighScaleBatch, batch_uuid: str) -> int:
        rows = self._client.execute(
            f"SELECT count() AS row_count FROM `{table}` "
            "WHERE service_id={service_id:String} AND batch_id={batch_id:UUID}",
            {"service_id": batch.service_id, "batch_id": batch_uuid},
        )
        if len(rows) != 1 or not isinstance(rows[0].get("row_count"), int):
            raise ValueError("ClickHouse insert receipt is invalid")
        return rows[0]["row_count"]

    def _insert_publication(
        self,
        batch: HighScaleBatch,
        batch_uuid: str,
        *,
        version: int,
        state: str,
        visible_rows: int,
    ) -> None:
        self._client.insert_rows(
            _PUBLICATION_TABLE,
            list(_PUBLICATION_COLUMNS),
            [
                (
                    batch_uuid,
                    batch.service_id,
                    batch.domain,
                    batch.generation,
                    batch.digest,
                    len(batch.rows),
                    visible_rows,
                    1 if state == "visible" else 0,
                    state,
                    version,
                    datetime.now(UTC),
                )
            ],
        )

    @staticmethod
    def _validate_publication(existing: dict[str, Any], batch: HighScaleBatch) -> None:
        if (
            existing.get("service_id") != batch.service_id
            or existing.get("domain") != batch.domain
            or existing.get("generation") != batch.generation
            or existing.get("batch_digest") != batch.digest
            or existing.get("expected_rows") != len(batch.rows)
        ):
            raise ValueError("ClickHouse publication identity or digest mismatch")
        if existing.get("publication_state") not in {"pending", "visible"}:
            raise ValueError("ClickHouse publication state is invalid")


def _batch_uuid(batch: HighScaleBatch) -> str:
    identity = "\0".join((batch.service_id, batch.domain, batch.generation, batch.batch_id))
    return str(uuid5(_BATCH_NAMESPACE, identity))


def _event_uuid(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")
    return str(uuid5(_EVENT_NAMESPACE, f"{field}:{value}"))


def _required_string(row: Mapping[str, Any], field: str, *, default: str | None = None) -> str:
    value = row.get(field, default)
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    return value


def _required_nonnegative_int(row: Mapping[str, Any], field: str) -> int:
    value = row.get(field)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _timestamp(row: Mapping[str, Any], field: str) -> datetime:
    value = row.get(field)
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, datetime.min.time())
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(f"{field} must be an ISO timestamp") from None
    else:
        raise ValueError(f"{field} must be an ISO timestamp")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _string_map(row: Mapping[str, Any], field: str) -> dict[str, str]:
    value = row.get(field) or {}
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be a mapping")
    result: dict[str, str] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not isinstance(item, str):
            raise ValueError(f"{field} keys and values must be strings")
        result[key] = item
    return result


def _common_values(batch: HighScaleBatch, row: Mapping[str, Any], batch_uuid: str) -> tuple[Any, ...]:
    event_id = row.get("event_id")
    if not isinstance(event_id, str) or not event_id:
        raise ValueError("event_id must be a non-empty string")
    timestamp = _timestamp(row, "timestamp")
    return (
        batch.service_id,
        _event_uuid(event_id, field="event_id"),
        timestamp,
        _timestamp(row, "ingest_timestamp") if "ingest_timestamp" in row else timestamp,
        _required_string(row, "source_object_key"),
        _required_string(row, "source_object_version"),
        _required_nonnegative_int(row, "line_ordinal"),
        _required_string(row, "transform_version"),
        batch_uuid,
        "visible",
    )


def _rows_for_domain(batch: HighScaleBatch, batch_uuid: str) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for row in batch.rows:
        if not isinstance(row, Mapping):
            raise ValueError("high-scale batch rows must be mappings")
        if batch.domain == "request":
            common = _common_values(batch, row, batch_uuid)
            rows.append(
                common
                + (
                    _required_string(row, "country", default=""),
                    _required_string(row, "client_ip", default=""),
                    _required_string(row, "url", default=""),
                    _string_map(row, "custom_fields"),
                    _string_map(row, "cmcd"),
                )
            )
        elif batch.domain == "rum_vitals":
            common = _common_values(batch, row, batch_uuid)
            metric_value = row.get("metric_value")
            if (
                not isinstance(metric_value, (int, float))
                or isinstance(metric_value, bool)
                or not math.isfinite(metric_value)
            ):
                raise ValueError("metric_value must be finite")
            request_event_id = row.get("request_event_id")
            rows.append(
                common
                + (
                    _required_string(row, "client_id", default=""),
                    None if request_event_id is None else _event_uuid(request_event_id, field="request_event_id"),
                    _required_string(row, "metric_name"),
                    float(metric_value),
                    _required_string(row, "metric_rating", default=""),
                    _required_string(row, "pathname", default=""),
                    _required_string(row, "country", default=""),
                )
            )
        elif batch.domain == "rum_errors":
            common = _common_values(batch, row, batch_uuid)
            request_event_id = row.get("request_event_id")
            rows.append(
                common
                + (
                    _required_string(row, "client_id", default=""),
                    None if request_event_id is None else _event_uuid(request_event_id, field="request_event_id"),
                    _required_string(row, "error_message", default=""),
                    _required_string(row, "error_file", default=""),
                    _required_string(row, "pathname", default=""),
                    _required_string(row, "country", default=""),
                )
            )
        elif batch.domain == "cmcd":
            request_event_id = row.get("request_event_id")
            if not isinstance(request_event_id, str) or not request_event_id:
                raise ValueError("request_event_id must be a non-empty string")
            projection_key = row.get("projection_key")
            if projection_key is None:
                projection_key = build_cmcd_projection_key({"request_event_id": request_event_id})
            rows.append(
                (
                    batch.service_id,
                    _event_uuid(projection_key, field="projection_key"),
                    _event_uuid(request_event_id, field="request_event_id"),
                    _timestamp(row, "timestamp"),
                    batch_uuid,
                    "visible",
                    _string_map({"cmcd": row.get("fields", row.get("cmcd"))}, "cmcd"),
                )
            )
    return rows


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

"""ClickHouse writer for pre-aggregated dimension-count batches.

Mirrors :class:`~backend.high_scale.publication.ClickHouseBatchAdapter`'s
idempotent insert-behind-a-publication-row pattern (same
``high_scale_batch_publications`` bookkeeping table, since its schema is
already domain-agnostic), but for the flat (dimension, value, bucket_start,
count) shape every aggregate table shares — no per-domain row mapping is
needed the way facts require. Aggregate batch domains are suffixed
"_aggregate" (e.g. "request_aggregate") so they never collide with a facts
batch for the same logical source object in that shared bookkeeping table.
"""

from __future__ import annotations

import threading
from datetime import datetime
from typing import Any

from backend.core.clickhouse_client import (
    ClickHouseClient as HttpClickHouseClient,
)
from backend.high_scale.publication import (
    HighScaleBatch,
    InsertReceipt,
    PartialInsert,
    _batch_uuid,
)

_AGGREGATE_TABLES: dict[str, tuple[str, str]] = {
    "request_aggregate": ("request_aggregates", "request_count"),
    "rum_vitals_aggregate": ("rum_vitals_aggregates", "event_count"),
    "rum_errors_aggregate": ("rum_error_aggregates", "error_count"),
    "cmcd_aggregate": ("cmcd_aggregates", "event_count"),
}


def _bucket_start(value: Any) -> datetime:
    """Batch rows carry bucket_start as an ISO string (HighScaleBatch.digest
    requires JSON-serializable row values) — parse back for the insert."""
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


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


class AggregateBatchAdapter:
    def __init__(self, client: HttpClickHouseClient) -> None:
        self._client = client
        self._batch_locks: dict[str, threading.Lock] = {}
        self._batch_locks_guard = threading.Lock()

    def insert(self, batch: HighScaleBatch) -> InsertReceipt:
        if batch.domain not in _AGGREGATE_TABLES:
            raise ValueError(f"unsupported aggregate domain: {batch.domain}")
        if not batch.rows:
            raise ValueError("aggregate batch must contain at least one row")
        batch_uuid = _batch_uuid(batch)
        with self._batch_lock(batch_uuid):
            return self._insert_locked(batch, batch_uuid)

    def delete_batch_rows(self, table: str, batch_id: str) -> None:
        self._client.delete_batch_rows(table, batch_id)

    def _insert_locked(self, batch: HighScaleBatch, batch_uuid: str) -> InsertReceipt:
        table, metric_column = _AGGREGATE_TABLES[batch.domain]
        columns = ["service_id", "bucket_start", "dimension", "value", metric_column, "batch_id", "publication_state"]
        mapped_rows = [
            (
                batch.service_id,
                _bucket_start(row["bucket_start"]),
                str(row["dimension"]),
                str(row["value"]),
                int(row["count"]),
                batch_uuid,
                "visible",
            )
            for row in batch.rows
        ]

        existing = self._existing_publication(batch, batch_uuid)
        if existing is not None:
            if existing["publication_state"] == "visible":
                return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)
            version = int(existing["manifest_version"])
        else:
            self._insert_publication(batch, batch_uuid, version=1, state="pending", visible_rows=0)
            version = 1

        expected = len(batch.rows)
        existing_rows = self._count_rows(table, batch, batch_uuid)
        if existing_rows not in (0, expected):
            self._client.delete_batch_rows(table, batch_uuid)
            raise PartialInsert(existing_rows)
        if existing_rows == 0:
            self._client.insert_rows(table, columns, mapped_rows)
            existing_rows = self._count_rows(table, batch, batch_uuid)
        if existing_rows != expected:
            self._client.delete_batch_rows(table, batch_uuid)
            raise PartialInsert(existing_rows)

        self._insert_publication(batch, batch_uuid, version=version + 1, state="visible", visible_rows=expected)
        return InsertReceipt(batch.batch_id, expected, batch.digest)

    def _batch_lock(self, batch_uuid: str) -> threading.Lock:
        with self._batch_locks_guard:
            return self._batch_locks.setdefault(batch_uuid, threading.Lock())

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
                    1,
                    state,
                    version,
                    None,
                )
            ],
        )

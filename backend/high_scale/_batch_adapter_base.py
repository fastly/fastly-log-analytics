"""Shared ClickHouse batch-publication adapter base class."""

from __future__ import annotations

import threading
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from backend.core.clickhouse_client import ClickHouseClient as HttpClickHouseClient

if TYPE_CHECKING:
    from backend.high_scale.publication import HighScaleBatch, InsertReceipt

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


class BaseClickHouseBatchAdapter:
    """Shared idempotent insert-behind-a-publication-row mechanics."""

    def __init__(self, client: HttpClickHouseClient) -> None:
        self._client = client
        self._batch_locks: dict[str, threading.Lock] = {}
        self._batch_locks_guard = threading.Lock()

    def delete_batch_rows(self, table: str, batch_id: str) -> None:
        self._client.delete_batch_rows(table, batch_id)

    def _batch_lock(self, batch_uuid: str) -> threading.Lock:
        with self._batch_locks_guard:
            return self._batch_locks.setdefault(batch_uuid, threading.Lock())

    def _publish_table_rows(
        self,
        *,
        table: str,
        columns: list[str],
        mapped_rows: Sequence[tuple[Any, ...]],
        batch: HighScaleBatch,
        batch_uuid: str,
    ) -> InsertReceipt:
        from backend.high_scale.publication import InsertReceipt, PartialInsert

        existing = self._existing_publication(batch, batch_uuid)
        if existing is not None:
            self._validate_publication(existing, batch)
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
            self._client.insert_rows(table, columns, list(mapped_rows))
            existing_rows = self._count_rows(table, batch, batch_uuid)
        if existing_rows != expected:
            self._client.delete_batch_rows(table, batch_uuid)
            raise PartialInsert(existing_rows)

        self._insert_publication(
            batch,
            batch_uuid,
            version=version + 1,
            state="visible",
            visible_rows=expected,
        )
        return InsertReceipt(batch.batch_id, expected, batch.digest)

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

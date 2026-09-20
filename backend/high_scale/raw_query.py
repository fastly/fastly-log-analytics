"""Bounded raw-row production for cold-tier historical query plans.

Reads immutable archive artifacts covering a requested ``[start, end)``
window and yields their rows in coverage order. This module only produces
rows; it has no opinion about job lifecycle, output limits, or cancellation
— :class:`~backend.high_scale.historical_jobs.HistoricalJobManager` owns
those. It never touches ClickHouse or the live ingest ledger, so a cold
query can never race or interfere with continuous ingest.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping
from datetime import UTC, datetime
from typing import Any, Protocol

import pyarrow as pa
import pyarrow.parquet as pq

from backend.high_scale.archive_models import ArchiveManifest
from backend.high_scale.archive_publication import ArchivePublication


class ArchiveManifestCatalog(Protocol):
    def manifests_covering(
        self, service_id: str, domain: str, start: datetime, end: datetime
    ) -> tuple[ArchiveManifest, ...]: ...


def raw_query_producer(
    catalog: ArchiveManifestCatalog,
    archive: ArchivePublication,
    *,
    service_id: str,
    domain: str,
    start: datetime,
    end: datetime,
) -> Callable[[], Iterable[Mapping[str, Any]]]:
    """Build a producer scanning archived rows in ``[start, end)``.

    Suitable as the ``producer`` argument to
    :meth:`HistoricalJobManager.submit` — a fresh call re-reads the catalog
    and every covering artifact, so the returned callable may be invoked at
    most once per job.
    """

    def _produce() -> Iterator[dict[str, Any]]:
        manifests = sorted(
            catalog.manifests_covering(service_id, domain, start, end),
            key=lambda manifest: manifest.coverage_start,
        )
        for manifest in manifests:
            payload = archive.read_artifact(manifest)
            table = pq.read_table(pa.BufferReader(payload))
            for row in table.to_pylist():
                timestamp = _row_timestamp(row)
                if timestamp is not None and (timestamp < start or timestamp >= end):
                    continue
                yield row

    return _produce


def _row_timestamp(row: Mapping[str, Any]) -> datetime | None:
    value = row.get("timestamp")
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    return None

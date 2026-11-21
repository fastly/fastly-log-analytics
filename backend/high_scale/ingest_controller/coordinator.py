"""Fenced source-object controller, aggregate batch adapter, assets, and service registry for high-scale."""

from __future__ import annotations

import base64
import hashlib
import inspect
import json
import logging
import os
import tempfile
import time
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from backend.high_scale.cmcd import project_cmcd
    from backend.high_scale.network import build_network_projection_rows
    from backend.high_scale.origin import build_origin_projection_rows
    from backend.high_scale.performance import build_performance_projection_rows
    from backend.high_scale.query_service import DecodeResult, decode_source_object
    from backend.high_scale.security import build_security_projection_rows

from backend.high_scale._common import (  # noqa: F401
    _BOOLEAN,
    _COLUMNS,
    _DOMAIN_IDENT_RE,
    _LOG_GAMMA,
    _ROUNDED,
    _TIMESERIES_METRICS,
    _UNSUPPORTED,
    GAMMA,
    MAX_PAGE_SIZE,
    RELATIVE_ERROR,
    VISIBLE_PUBLICATION_FENCE_SQL,
    ArchiveManifestCatalog,
    KeysetCursor,
    _b64,
    _bucket,
    _bucket_case,
    _centroid,
    _column,
    _row_timestamp,
    _unb64,
    build_clickhouse_filters,
    build_published_facts_where,
    build_published_projection_where,
    finite_float,
    format_iso_utc,
    format_iso_utc_or_empty,
    inclusive_minute_end,
    optional_int,
    parse_time_window,
    percentiles,
    range_value,
    raw_query_producer,
    request_filter_clauses,
    resolve_asn_display_labels,
    sketch,
    text,
    visible_fact_clauses,
    visible_publication_fence_sql,
)
from backend.high_scale.archive import (
    ArchiveManifest,
    ArchivePublication,
    ArchiveSourceObject,
    ArchiveState,
    ClickHousePublication,
    HighScaleBatch,
    PublicationResult,
    PublicationStatus,
    _read_manifest_rows,
    build_batch_id_from_manifest_id,
    event_digest,
    row_matches_serving_domain,
    write_batch_archive_checkpoint,
)

logger = logging.getLogger("backend.high_scale.ingest_controller")

from backend.high_scale.ingest_controller.service import compute_dimension_counts
from backend.high_scale.query_service import DecodeResult, decode_source_object  # noqa: E402

# ── Ingest Controller ─────────────────────────────────────────────────────────
HIGH_SCALE_PAGE_MAX_BYTES = 64 * 1024 * 1024  # 64 MB in-flight byte guard
HIGH_SCALE_DEFAULT_MAX_BATCH_OBJECTS = 50
HIGH_SCALE_DEFAULT_MAX_BATCH_BYTES = 6 * 1024 * 1024  # 6 MB compressed sub-batch target
HIGH_SCALE_SNAPSHOT_DEFAULT_MAX_OBJECTS = 500
HIGH_SCALE_SNAPSHOT_DEFAULT_MAX_BYTES = 32 * 1024 * 1024  # 32 MB consolidated snapshot target
HIGH_SCALE_SNAPSHOT_CACHE_DEFAULT_MAX_BYTES = 16 * 1024 * 1024  # 16 MB bounded in-memory payload cache
HIGH_SCALE_MAX_DEAD_LETTER_RATIO = 0.05  # 5% dead-letter limit
_RUM_SIBLING_DOMAIN = {"rum_vitals": "rum_errors", "rum_errors": "rum_vitals"}


@dataclass(frozen=True)
class BatchIngestResult:
    manifest: ArchiveManifest | None
    publication: PublicationResult
    total_events: int
    quarantined_events: int
    sources_count: int
    derived_publication_complete: bool = True


@dataclass(frozen=True)
class SnapshotFlushResult:
    service_id: str
    domain: str
    manifest: ArchiveManifest | None
    sources_flushed: int
    rows_archived: int
    skipped_missing: int = 0


def _build_ingest_publication_id(
    service_id: str,
    domain: str,
    sources_payloads: Sequence[tuple[SourceObjectDescriptor, bytes]],
    schema_version: str,
    transform_version: str,
) -> str:
    """Deterministic publication ID for decoupled fast ClickHouse ingest."""
    hasher = hashlib.sha256()
    hasher.update(f"{service_id}\x00{domain}\x00{schema_version}\x00{transform_version}\x00".encode())
    for desc, _ in sources_payloads:
        hasher.update(f"{desc.object_key}\x00{desc.checksum}\x00".encode())
    return f"ingest-{hasher.hexdigest()[:40]}"


@dataclass(frozen=True)
class ArchiveRetryResult:
    acknowledged: int = 0
    failed: int = 0
    released_to_raw: int = 0


_NGWAF_ENRICHMENT_KEY = "_ngwaf_enrichment"
_PUBLICATION_RETRY_BACKOFF_SECONDS = 30.0
_PUBLICATION_RETRY_MAX_BACKOFF_SECONDS = 3600.0


class HighScaleControlPlane(Protocol):
    def owner(self, service_id: str) -> Any: ...
    def discover_source(
        self,
        service_id: str,
        domain: str,
        object_key: str,
        checksum: str,
        *,
        size_bytes: int,
        version: str | None,
        expected_owner: str,
        expected_owner_epoch: int,
    ) -> Any: ...
    def claim_source(
        self,
        service_id: str,
        object_key: str,
        worker_id: str,
        *,
        expected_owner: str,
        expected_owner_epoch: int,
        lease_seconds: float,
        now: datetime | None,
    ) -> Any: ...
    def record_source_counts(
        self, service_id: str, object_key: str, *, accepted_rows: int, malformed_rows: int, expected_owner_epoch: int
    ) -> None: ...
    def register_archive_manifest(self, manifest: ArchiveManifest, *, owner_epoch: int) -> Any: ...
    def archive_manifest(self, manifest_id: str) -> Any: ...
    def transition_archive_manifest(
        self, manifest_id: str, *, expected_state: ArchiveState, next_state: ArchiveState, expected_owner_epoch: int
    ) -> Any: ...
    def claim_sources_batch(
        self,
        service_id: str,
        object_keys: Sequence[str],
        worker_id: str,
        lease_seconds: float = 300.0,
        expected_owner_epoch: int = 1,
        *,
        expected_owner: str | None = None,
        now: datetime | None = None,
    ) -> tuple[Any, ...]: ...
    def acknowledge_sources_batch(
        self,
        service_id: str,
        object_keys: Sequence[str],
        *,
        manifest_id: str,
        expected_owner_epoch: int,
        claim_tokens: Sequence[Any],
        next_cursor: str | None = None,
        domain: str = "request",
        partition_id: str = "default",
    ) -> None: ...
    def mark_source_missing(
        self,
        service_id: str,
        object_key: str,
        *,
        expected_owner: str,
        expected_owner_epoch: int,
        error: str,
        now: datetime | None = None,
    ) -> Any: ...
    def mark_sources_appended_batch(
        self,
        service_id: str,
        object_keys: Sequence[str],
        *,
        expected_owner_epoch: int,
        claim_tokens: Sequence[Any],
        next_cursor: str | None = None,
        domain: str = "request",
        partition_id: str = "default",
    ) -> None: ...
    def unarchived_appended_summary(self, service_id: str, domain: str) -> tuple[int, int, datetime | None]: ...
    def claim_unarchived_appended_sources(
        self,
        service_id: str,
        domain: str,
        worker_id: str,
        *,
        expected_owner_epoch: int,
        lease_seconds: float = 300.0,
        limit: int = 500,
        max_bytes: int | None = None,
        now: datetime | None = None,
    ) -> tuple[Any, ...]: ...
    def release_unarchived_appended_sources(
        self,
        service_id: str,
        object_keys: Sequence[str],
        *,
        worker_id: str,
        error: str | None = None,
    ) -> None: ...
    def attach_archive_manifest_to_sources(
        self,
        service_id: str,
        object_keys: Sequence[str],
        *,
        manifest_id: str,
        expected_owner_epoch: int,
        worker_id: str | None = None,
    ) -> None: ...
    def fail_sources_batch(
        self,
        service_id: str,
        object_keys: Sequence[str],
        *,
        expected_owner_epoch: int,
        error: str,
        claim_tokens: Sequence[Any],
    ) -> None: ...


class HighScaleIngestController:
    """Coordinates idempotent, owner-fenced source-object batch ingest."""

    batch_capable: bool = True

    def __init__(
        self,
        *,
        ownership: Any = None,
        ledger: Any = None,
        control_plane: HighScaleControlPlane | None = None,
        archive: ArchivePublication,
        serving: ClickHousePublication,
        worker_id: str,
        aggregates: Any | None = None,
        transform_version: str = "normalize.v1",
        schema_version: str = "archive.v1",
        archive_epoch: int = 0,
        retention_seconds: int = 0,
        deletion_grace_seconds: int = 900,
        lease_seconds: float = 300.0,
        defer_archive_flush: bool = False,
        snapshot_cache_max_bytes: int | None = None,
    ) -> None:
        if not worker_id:
            raise ValueError("worker identity is required")
        if lease_seconds <= 0:
            raise ValueError("lease duration must be positive")
        if control_plane is None and (ownership is None or ledger is None):
            raise ValueError("control_plane or both ownership and ledger are required")
        self._ownership: Any = ownership if ownership is not None else control_plane
        self._ledger: Any = ledger if ledger is not None else control_plane
        self._control = control_plane
        self._archive = archive
        self._serving = serving
        self._aggregates = aggregates
        self._worker_id = worker_id
        self._transform_version = transform_version
        self._schema_version = schema_version
        self._archive_epoch = archive_epoch
        self._retention_seconds = retention_seconds
        self._deletion_grace_seconds = deletion_grace_seconds
        self._lease_seconds = lease_seconds
        self._defer_archive_flush = bool(defer_archive_flush)
        self._snapshot_cache_max_bytes = (
            max(0, int(snapshot_cache_max_bytes))
            if snapshot_cache_max_bytes is not None
            else max(
                0,
                int(
                    os.getenv(
                        "HIGH_SCALE_SNAPSHOT_CACHE_MAX_BYTES",
                        str(HIGH_SCALE_SNAPSHOT_CACHE_DEFAULT_MAX_BYTES),
                    )
                ),
            )
        )
        self._pending_payload_cache: OrderedDict[tuple[str, str, str], bytes] = OrderedDict()
        self._pending_payload_cache_bytes = 0
        self._publication_retry_backoff: dict[str, tuple[float, int]] = {}

    def ingest_batch(
        self,
        *,
        service_id: str,
        domain: str,
        sources_payloads: Sequence[tuple[SourceObjectDescriptor, bytes]],
        next_cursor: str | None = None,
        partition_id: str = "default",
        claim_tokens: Sequence[Any] = (),
        now: datetime | None = None,
        max_dead_letter_ratio: float = HIGH_SCALE_MAX_DEAD_LETTER_RATIO,
    ) -> BatchIngestResult:
        if not sources_payloads:
            pub = PublicationResult(f"{service_id}:{domain}:empty", PublicationStatus.VISIBLE, 0, False)
            return BatchIngestResult(None, pub, 0, 0, 0)  # type: ignore[arg-type]
        total_bytes = sum(len(payload) for _, payload in sources_payloads)
        if total_bytes > HIGH_SCALE_PAGE_MAX_BYTES:
            raise ValueError(f"high-scale page size {total_bytes} exceeds maximum {HIGH_SCALE_PAGE_MAX_BYTES}")
        owner = self._control.owner(service_id) if self._control is not None else self._ownership.get(service_id)
        if owner.current_owner != "high_scale":
            raise RuntimeError(f"high-scale ingest rejected for {service_id}: owner is {owner.current_owner}")
        tokens_by_key = {token.object_key: token for token in claim_tokens}
        if self._control is not None and not tokens_by_key and hasattr(self._control, "source"):
            for descriptor, _ in sources_payloads:
                source = self._control.source(service_id, descriptor.object_key)
                tokens_by_key[descriptor.object_key] = source.claim_token
        source_batches: list[tuple[ArchiveSourceObject, list[dict[str, Any]]]] = []
        all_events: list[dict[str, Any]] = []
        sibling_events: list[dict[str, Any]] = []
        total_accepted = total_quarantined = sibling_quarantined = 0
        use_batch_counts = self._control is not None and (
            getattr(type(self._control), "record_source_counts_batch", None) is not None
            or "record_source_counts_batch" in vars(self._control)
        )
        batch_counts: list[tuple[str, int, int]] = []
        per_source_decoded: list[
            tuple[
                SourceObjectDescriptor,
                bytes,
                ArchiveSourceObject,
                DecodeResult,
                list[dict[str, Any]],
                list[dict[str, Any]],
                int,
            ]
        ] = []
        for descriptor, payload in sources_payloads:
            if self._control is None:
                src_obj = self._ledger.discover(
                    service_id,
                    domain,
                    descriptor.object_key,
                    descriptor.checksum,
                    size_bytes=len(payload),
                    version=descriptor.version,
                )
                if len(sources_payloads) == 1:
                    domain = src_obj.domain
                if src_obj.status == "discovered":
                    claim = self._ledger.claim(
                        service_id,
                        descriptor.object_key,
                        self._worker_id,
                        lease_seconds=int(self._lease_seconds),
                        now=now,
                    )
                    if not claim.claimed:
                        raise RuntimeError(f"source object {descriptor.object_key} is already claimed")
            archive_source = ArchiveSourceObject(
                service_id=service_id,
                domain=domain,
                object_key=descriptor.object_key,
                checksum=descriptor.checksum,
                size_bytes=len(payload),
                version=descriptor.version,
            )
            decoded: DecodeResult = decode_source_object(
                archive_source, payload, transform_version=self._transform_version, domain=domain
            )
            total_accepted += decoded.accepted_rows
            total_quarantined += decoded.quarantined_rows
            if use_batch_counts:
                batch_counts.append((descriptor.object_key, decoded.accepted_rows, decoded.quarantined_rows))
            elif self._control is not None:
                self._control.record_source_counts(
                    service_id,
                    descriptor.object_key,
                    accepted_rows=decoded.accepted_rows,
                    malformed_rows=decoded.quarantined_rows,
                    expected_owner_epoch=owner.owner_epoch,
                )
            else:
                self._ledger.record_counts(
                    service_id,
                    descriptor.object_key,
                    accepted_rows=decoded.accepted_rows,
                    malformed_rows=decoded.quarantined_rows,
                )
            archive_rows = [dict(event, _record_kind="event") for event in decoded.events]
            archive_rows.extend(
                {
                    "_record_kind": "dead_letter",
                    "source_object_key": item.source_object_key,
                    "line_ordinal": item.line_ordinal,
                    "raw_line_base64": item.raw_line_base64,
                    "reason": item.reason,
                }
                for item in decoded.dead_letters
            )
            source_batches.append((archive_source, archive_rows))
            all_events.extend(decoded.events)
            sibling = _RUM_SIBLING_DOMAIN.get(domain)
            src_sibling_events: list[dict[str, Any]] = []
            src_sibling_quarantined = 0
            if sibling is not None:
                sibling_decoded = decode_source_object(
                    replace(archive_source, domain=sibling),
                    payload,
                    transform_version=self._transform_version,
                    domain=sibling,
                )
                src_sibling_quarantined = sibling_decoded.quarantined_rows
                src_sibling_events = [e for e in sibling_decoded.events if row_matches_serving_domain(sibling, e)]
                sibling_quarantined += src_sibling_quarantined
                sibling_events.extend(src_sibling_events)
            per_source_decoded.append(
                (
                    descriptor,
                    payload,
                    archive_source,
                    decoded,
                    archive_rows,
                    src_sibling_events,
                    src_sibling_quarantined,
                )
            )
        if use_batch_counts and batch_counts and self._control is not None:
            getattr(self._control, "record_source_counts_batch")(
                service_id, batch_counts, expected_owner_epoch=owner.owner_epoch
            )
        total_rows = total_accepted + total_quarantined
        if total_rows > 0 and (total_quarantined / total_rows) > max_dead_letter_ratio:
            isolated_healthy = False
            if len(per_source_decoded) > 1:
                healthy_items = []
                poison_items = []
                for item in per_source_decoded:
                    src_dec = item[3]
                    src_total = src_dec.accepted_rows + src_dec.quarantined_rows
                    if src_total > 0 and (src_dec.quarantined_rows / src_total) > max_dead_letter_ratio:
                        poison_items.append(item)
                    else:
                        healthy_items.append(item)
                if healthy_items:
                    h_acc = sum(it[3].accepted_rows for it in healthy_items)
                    h_quar = sum(it[3].quarantined_rows for it in healthy_items)
                    h_tot = h_acc + h_quar
                    if h_tot == 0 or (h_quar / h_tot) <= max_dead_letter_ratio:
                        isolated_healthy = True
                        poison_keys = tuple(it[0].object_key for it in poison_items)
                        poison_err = (
                            f"high-scale poison-pill source isolated: dead letter ratio exceeds {max_dead_letter_ratio}"
                        )
                        if self._control is not None and hasattr(self._control, "fail_sources_batch"):
                            self._control.fail_sources_batch(
                                service_id,
                                poison_keys,
                                expected_owner_epoch=owner.owner_epoch,
                                error=poison_err,
                                claim_tokens=tuple(tokens_by_key[key] for key in poison_keys if key in tokens_by_key),
                            )
                        elif self._control is None and hasattr(self._ledger, "mark_failed"):
                            for p_key in poison_keys:
                                cur_src = self._ledger.source(service_id, p_key)
                                if cur_src is not None and cur_src.status == "claimed":
                                    self._ledger.mark_failed(service_id, p_key, cur_src.lease_generation, poison_err)
                        sources_payloads = [(it[0], it[1]) for it in healthy_items]
                        source_batches = [(it[2], it[4]) for it in healthy_items]
                        all_events = [e for it in healthy_items for e in it[3].events]
                        sibling_events = [e for it in healthy_items for e in it[5]]
                        sibling_quarantined = sum(it[6] for it in healthy_items)
                        total_accepted = h_acc
                        total_quarantined = h_quar
            if not isolated_healthy:
                raise ValueError(
                    f"high-scale dead letter ratio {total_quarantined}/{total_rows} exceeds maximum {max_dead_letter_ratio}"
                )
        if sibling_quarantined:
            logger.warning(
                "high-scale RUM sibling decode rejected retained source records",
                extra={"domain": _RUM_SIBLING_DOMAIN[domain], "quarantined_rows": sibling_quarantined},
            )
        if domain in _RUM_SIBLING_DOMAIN:
            source_batches = [(replace(src, domain=domain), rows) for src, rows in source_batches]
        elif len(source_batches) == 1 and source_batches[0][0].domain != domain:
            source_batches = [(replace(source_batches[0][0], domain=domain), source_batches[0][1])]
        ngwaf_bots = _lookup_high_scale_ngwaf_bots(all_events) if domain == "request" else {}
        if domain == "request":
            for _, rows in source_batches:
                _mark_ngwaf_enrichment(rows, ngwaf_bots)
        observed = (now or datetime.now(UTC)).astimezone(UTC)
        object_keys = tuple(desc.object_key for desc, _ in sources_payloads)

        if self._defer_archive_flush:
            source_batches.clear()
            publication_id = _build_ingest_publication_id(
                service_id, domain, sources_payloads, self._schema_version, self._transform_version
            )
            current_owner = (
                self._control.owner(service_id) if self._control is not None else self._ownership.get(service_id)
            )
            if current_owner.current_owner != "high_scale" or current_owner.owner_epoch != owner.owner_epoch:
                raise RuntimeError(f"high-scale owner epoch changed during ingest for {service_id}")
            if domain == "request":
                _apply_high_scale_ngwaf_bots(all_events, ngwaf_bots)
            try:
                publication, derived_ok, derived_err = self._publish_manifest_publications(
                    service_id, domain, publication_id, owner.owner_epoch, all_events, sibling_events
                )
            except Exception as exc:
                self._fail_deferred_sources(service_id, object_keys, owner.owner_epoch, str(exc), tokens_by_key)
                raise
            if not derived_ok:
                self._fail_deferred_sources(service_id, object_keys, owner.owner_epoch, derived_err, tokens_by_key)
                return BatchIngestResult(
                    None,
                    publication,
                    total_accepted,
                    total_quarantined,
                    len(sources_payloads),
                    derived_publication_complete=False,
                )
            if self._control is not None:
                self._control.mark_sources_appended_batch(
                    service_id,
                    object_keys,
                    expected_owner_epoch=owner.owner_epoch,
                    claim_tokens=tuple(tokens_by_key[key] for key in object_keys if key in tokens_by_key),
                    next_cursor=next_cursor,
                    domain=domain,
                    partition_id=partition_id,
                )
            else:
                for desc, _ in sources_payloads:
                    cur_src = self._ledger.source(service_id, desc.object_key)
                    gen = cur_src.lease_generation if cur_src is not None else 1
                    if cur_src is None or cur_src.status not in {"appended", "acknowledged"}:
                        self._ledger.mark_appended(service_id, desc.object_key, gen)
            self._cache_pending_payloads(service_id, domain, sources_payloads)
            return BatchIngestResult(
                None,
                publication,
                total_accepted,
                total_quarantined,
                len(sources_payloads),
                derived_publication_complete=True,
            )

        with tempfile.TemporaryDirectory(prefix="high-scale-archive-") as root:
            local_manifest = write_batch_archive_checkpoint(
                Path(root),
                service_id=service_id,
                domain=domain,
                source_batches=source_batches,
                archive_epoch=self._archive_epoch,
                schema_version=self._schema_version,
                transform_version=self._transform_version,
                coverage_start=observed,
                coverage_end=observed,
                retention_seconds=self._retention_seconds,
                deletion_grace_seconds=self._deletion_grace_seconds,
            )
            source_batches.clear()
            artifact = Path(local_manifest.artifact.uri.removeprefix("file://")).read_bytes()
            manifest = replace(
                local_manifest,
                artifact=replace(local_manifest.artifact, uri=f"s3://archive/{local_manifest.manifest_id}.parquet"),
            )
            if self._control is not None:
                try:
                    manifest = self._control.archive_manifest(manifest.manifest_id).manifest
                except KeyError:
                    pass
            self._archive.publish(manifest, artifact)
            del artifact
        if self._control is not None:
            self._publish_control_manifest(manifest, owner.owner_epoch)
        current_owner = (
            self._control.owner(service_id) if self._control is not None else self._ownership.get(service_id)
        )
        if current_owner.current_owner != "high_scale" or current_owner.owner_epoch != owner.owner_epoch:
            raise RuntimeError(f"high-scale owner epoch changed during ingest for {service_id}")
        if domain == "request":
            _apply_high_scale_ngwaf_bots(all_events, ngwaf_bots)
        try:
            publication, derived_ok, derived_err = self._publish_manifest_publications(
                service_id, domain, manifest.manifest_id, owner.owner_epoch, all_events, sibling_events
            )
        except Exception as exc:
            if self._control is not None:
                self._hold_sources_on_archive(
                    service_id,
                    object_keys,
                    manifest.manifest_id,
                    owner.owner_epoch,
                    str(exc),
                    tokens_by_key,
                )
            raise
        if self._control is not None:
            if derived_ok:
                self._control.acknowledge_sources_batch(
                    service_id,
                    object_keys,
                    manifest_id=manifest.manifest_id,
                    expected_owner_epoch=owner.owner_epoch,
                    claim_tokens=tuple(tokens_by_key[key] for key in object_keys if key in tokens_by_key),
                    next_cursor=next_cursor,
                    domain=domain,
                    partition_id=partition_id,
                )
            else:
                self._hold_sources_on_archive(
                    service_id,
                    object_keys,
                    manifest.manifest_id,
                    owner.owner_epoch,
                    derived_err,
                    tokens_by_key,
                )
        else:
            for desc, _ in sources_payloads:
                cur_src = self._ledger.source(service_id, desc.object_key)
                gen = cur_src.lease_generation if cur_src is not None else 1
                if cur_src is None or cur_src.status != "acknowledged":
                    if derived_ok:
                        self._ledger.mark_appended(service_id, desc.object_key, gen)
                        self._ledger.mark_archived(
                            service_id, desc.object_key, gen, manifest.manifest_id, owner.owner_epoch
                        )
                        self._ledger.acknowledge(service_id, desc.object_key, manifest.manifest_id)
                    elif hasattr(self._ledger, "mark_failed") and cur_src is not None and cur_src.status == "claimed":
                        self._ledger.mark_failed(service_id, desc.object_key, gen, derived_err)
        return BatchIngestResult(
            manifest,
            publication,
            total_accepted,
            total_quarantined,
            len(sources_payloads),
            derived_publication_complete=derived_ok,
        )

    def _fail_deferred_sources(
        self,
        service_id: str,
        object_keys: Sequence[str],
        owner_epoch: int,
        error: str,
        tokens_by_key: Mapping[str, Any],
    ) -> None:
        if self._control is not None:
            self._control.fail_sources_batch(
                service_id,
                object_keys,
                expected_owner_epoch=owner_epoch,
                error=error,
                claim_tokens=tuple(tokens_by_key[key] for key in object_keys if key in tokens_by_key),
            )
            return
        for key in object_keys:
            cur_src = self._ledger.source(service_id, key)
            if cur_src is not None and cur_src.status == "claimed" and hasattr(self._ledger, "mark_failed"):
                self._ledger.mark_failed(service_id, key, cur_src.lease_generation, error)

    def _cache_pending_payloads(
        self,
        service_id: str,
        domain: str,
        sources_payloads: Sequence[tuple[SourceObjectDescriptor, bytes]],
    ) -> None:
        max_bytes = self._snapshot_cache_max_bytes
        if max_bytes <= 0:
            return
        for desc, payload in sources_payloads:
            cache_key = (service_id, domain, desc.object_key)
            if cache_key in self._pending_payload_cache:
                self._pending_payload_cache.move_to_end(cache_key)
                continue
            size = len(payload)
            if size > max_bytes:
                continue
            while self._pending_payload_cache and (self._pending_payload_cache_bytes + size > max_bytes):
                _, evicted = self._pending_payload_cache.popitem(last=False)
                self._pending_payload_cache_bytes = max(0, self._pending_payload_cache_bytes - len(evicted))
            self._pending_payload_cache[cache_key] = payload
            self._pending_payload_cache_bytes += size

    def _pop_cached_payload(self, service_id: str, domain: str, object_key: str) -> bytes | None:
        payload = self._pending_payload_cache.pop((service_id, domain, object_key), None)
        if payload is not None:
            self._pending_payload_cache_bytes = max(0, self._pending_payload_cache_bytes - len(payload))
        return payload

    def flush_archive_snapshot(
        self,
        *,
        service_id: str,
        domain: str,
        reader: SourceObjectReader,
        snapshot_interval_minutes: int = 5,
        force: bool = False,
        now: datetime | None = None,
        max_objects: int | None = None,
        max_bytes: int | None = None,
    ) -> SnapshotFlushResult:
        """Flush accumulated 'appended' sources for *(service_id, domain)* into one consolidated archive."""
        owner = self._control.owner(service_id) if self._control is not None else self._ownership.get(service_id)
        if owner.current_owner != "high_scale":
            return SnapshotFlushResult(service_id, domain, None, 0, 0, 0)

        observed = (now or datetime.now(UTC)).astimezone(UTC)
        resolved_max_objects = (
            max(1, int(max_objects))
            if max_objects is not None
            else max(
                1,
                int(os.getenv("HIGH_SCALE_SNAPSHOT_MAX_OBJECTS", str(HIGH_SCALE_SNAPSHOT_DEFAULT_MAX_OBJECTS))),
            )
        )
        resolved_max_bytes = min(
            HIGH_SCALE_PAGE_MAX_BYTES,
            max(1, int(max_bytes))
            if max_bytes is not None
            else max(
                1,
                int(os.getenv("HIGH_SCALE_SNAPSHOT_MAX_BYTES", str(HIGH_SCALE_SNAPSHOT_DEFAULT_MAX_BYTES))),
            ),
        )

        source_store = self._control if self._control is not None else self._ledger
        count, total_bytes, oldest_at = source_store.unarchived_appended_summary(service_id, domain)
        if count <= 0:
            return SnapshotFlushResult(service_id, domain, None, 0, 0, 0)

        if not force and count < resolved_max_objects and total_bytes < resolved_max_bytes:
            if oldest_at is not None:
                oldest_utc = (oldest_at if oldest_at.tzinfo else oldest_at.replace(tzinfo=UTC)).astimezone(UTC)
                elapsed = (observed - oldest_utc).total_seconds()
                if elapsed < max(1, int(snapshot_interval_minutes)) * 60:
                    return SnapshotFlushResult(service_id, domain, None, 0, 0, 0)

        if self._control is not None:
            claimed = self._control.claim_unarchived_appended_sources(
                service_id,
                domain,
                self._worker_id,
                expected_owner_epoch=owner.owner_epoch,
                lease_seconds=self._lease_seconds,
                limit=resolved_max_objects,
                max_bytes=resolved_max_bytes,
                now=observed,
            )
        else:
            claimed = self._ledger.unarchived_appended_sources(
                service_id,
                domain,
                limit=resolved_max_objects,
                max_bytes=resolved_max_bytes,
            )
        if not claimed:
            return SnapshotFlushResult(service_id, domain, None, 0, 0, 0)

        claimed_keys = tuple(rec.object_key for rec in claimed)
        skipped_missing = 0
        try:
            source_batches: list[tuple[ArchiveSourceObject, list[dict[str, Any]]]] = []
            valid_recs: list[Any] = []
            all_events: list[dict[str, Any]] = []
            total_archive_rows = 0

            for rec in claimed:
                payload = self._pop_cached_payload(service_id, domain, rec.object_key)
                if payload is None:
                    try:
                        payload = reader.read_source_object(service_id, domain, rec.object_key)
                    except SourceObjectMissingError as exc:
                        skipped_missing += 1
                        if self._control is not None:
                            self._control.mark_source_missing(
                                service_id,
                                rec.object_key,
                                expected_owner="high_scale",
                                expected_owner_epoch=owner.owner_epoch,
                                error=str(exc),
                            )
                        elif hasattr(self._ledger, "mark_source_missing"):
                            self._ledger.mark_source_missing(service_id, rec.object_key, error=str(exc))
                        logger.error(
                            "high-scale snapshot flush skipped missing source",
                            extra={"service_id": service_id, "domain": domain, "object_key": rec.object_key},
                        )
                        continue

                archive_src = ArchiveSourceObject(
                    service_id=service_id,
                    domain=domain,
                    object_key=rec.object_key,
                    checksum=rec.checksum,
                    size_bytes=len(payload),
                    version=rec.version,
                )
                decoded = decode_source_object(
                    archive_src, payload, transform_version=self._transform_version, domain=domain
                )
                archive_rows: list[dict[str, Any]] = [dict(row, _record_kind="event") for row in decoded.events]
                archive_rows.extend(
                    {
                        "_record_kind": "dead_letter",
                        "source_object_key": dl.source_object_key,
                        "line_ordinal": dl.line_ordinal,
                        "raw_line_base64": dl.raw_line_base64,
                        "reason": dl.reason,
                    }
                    for dl in decoded.dead_letters
                )
                total_archive_rows += len(archive_rows)
                all_events.extend(decoded.events)
                source_batches.append((archive_src, archive_rows))
                valid_recs.append(rec)

            if total_archive_rows == 0:
                for rec in valid_recs:
                    skipped_missing += 1
                    if self._control is not None:
                        self._control.mark_source_missing(
                            service_id,
                            rec.object_key,
                            expected_owner="high_scale",
                            expected_owner_epoch=owner.owner_epoch,
                            error="empty source object (0 rows)",
                        )
                    elif hasattr(self._ledger, "mark_source_missing"):
                        self._ledger.mark_source_missing(
                            service_id, rec.object_key, error="empty source object (0 rows)"
                        )
                return SnapshotFlushResult(service_id, domain, None, 0, 0, skipped_missing)

            if domain == "request":
                ngwaf_bots = _lookup_high_scale_ngwaf_bots(all_events)
                for _, rows in source_batches:
                    _mark_ngwaf_enrichment(rows, ngwaf_bots)

            discovered_times = [
                (dt if dt.tzinfo else dt.replace(tzinfo=UTC)).astimezone(UTC)
                for rec in valid_recs
                if (dt := getattr(rec, "discovered_at", None)) is not None
            ]
            coverage_start = min(discovered_times, default=observed)
            if coverage_start > observed:
                coverage_start = observed

            with tempfile.TemporaryDirectory(prefix="high-scale-snapshot-") as root:
                local_manifest = write_batch_archive_checkpoint(
                    Path(root),
                    service_id=service_id,
                    domain=domain,
                    source_batches=source_batches,
                    archive_epoch=self._archive_epoch,
                    schema_version=self._schema_version,
                    transform_version=self._transform_version,
                    coverage_start=coverage_start,
                    coverage_end=observed,
                    retention_seconds=self._retention_seconds,
                    deletion_grace_seconds=self._deletion_grace_seconds,
                )
                source_batches.clear()
                artifact = Path(local_manifest.artifact.uri.removeprefix("file://")).read_bytes()
                manifest = replace(
                    local_manifest,
                    artifact=replace(
                        local_manifest.artifact,
                        uri=f"s3://archive/{local_manifest.manifest_id}.parquet",
                    ),
                )
                if self._control is not None:
                    try:
                        manifest = self._control.archive_manifest(manifest.manifest_id).manifest
                    except KeyError:
                        pass
                self._archive.publish(manifest, artifact)
                del artifact

            if self._control is not None:
                self._publish_control_manifest(manifest, owner.owner_epoch)
                self._control.attach_archive_manifest_to_sources(
                    service_id,
                    tuple(rec.object_key for rec in valid_recs),
                    manifest_id=manifest.manifest_id,
                    expected_owner_epoch=owner.owner_epoch,
                    worker_id=self._worker_id,
                )
            else:
                for rec in valid_recs:
                    self._ledger.mark_archived(
                        service_id,
                        rec.object_key,
                        rec.lease_generation,
                        manifest.manifest_id,
                        owner.owner_epoch,
                    )
                    self._ledger.acknowledge(service_id, rec.object_key, manifest.manifest_id)

            return SnapshotFlushResult(
                service_id=service_id,
                domain=domain,
                manifest=manifest,
                sources_flushed=len(valid_recs),
                rows_archived=manifest.artifact.row_count,
                skipped_missing=skipped_missing,
            )
        except Exception as exc:
            if self._control is not None:
                try:
                    self._control.release_unarchived_appended_sources(
                        service_id,
                        claimed_keys,
                        worker_id=self._worker_id,
                        error=str(exc),
                    )
                except Exception:
                    logger.warning(
                        "high-scale snapshot flush failed to release claimed sources",
                        extra={"service_id": service_id, "domain": domain},
                        exc_info=True,
                    )
            raise

    def _publish_manifest_publications(
        self,
        service_id: str,
        domain: str,
        manifest_id: str,
        owner_epoch: int,
        events: Sequence[dict[str, Any]],
        sibling_events: Sequence[dict[str, Any]],
    ) -> tuple[PublicationResult, bool, str]:
        """Publish a committed manifest's facts and derived projections under batch ids derived from the manifest.

        A fact publication failure raises; derived failures are reported so the caller can hold the sources.
        """
        serving_rows = tuple(e for e in events if row_matches_serving_domain(domain, e))
        batch_id = build_batch_id_from_manifest_id(manifest_id)
        if serving_rows:
            publication = self._serving.publish(
                HighScaleBatch(batch_id, service_id, domain, str(owner_epoch), serving_rows)
            )
        else:
            publication = PublicationResult(batch_id, PublicationStatus.VISIBLE, 0, False)
        agg_ok, agg_err = self._publish_aggregates_batch(service_id, domain, manifest_id, owner_epoch, events)
        cmcd_ok, cmcd_err = True, None
        if domain == "request":
            cmcd_ok, cmcd_err = self._publish_cmcd_projection(
                service_id, build_batch_id_from_manifest_id(f"{manifest_id}:cmcd"), owner_epoch, events
            )
        sibling = _RUM_SIBLING_DOMAIN.get(domain)
        sibling_ok, sibling_err = True, None
        if sibling is not None and sibling_events:
            sibling_batch_id = build_batch_id_from_manifest_id(f"{manifest_id}:{sibling}")
            try:
                self._serving.publish(
                    HighScaleBatch(sibling_batch_id, service_id, sibling, str(owner_epoch), tuple(sibling_events))
                )
            except Exception as exc:
                logger.exception(
                    "high-scale RUM sibling fact publication failed",
                    extra={"service_id": service_id, "domain": sibling, "manifest_id": manifest_id},
                )
                sibling_ok, sibling_err = False, str(exc)
            else:
                sibling_ok, sibling_err = self._publish_aggregates_batch(
                    service_id, sibling, manifest_id, owner_epoch, sibling_events
                )
        derived_ok = agg_ok and cmcd_ok and sibling_ok
        return publication, derived_ok, agg_err or cmcd_err or sibling_err or "derived publication failed"

    def _hold_sources_on_archive(
        self,
        service_id: str,
        object_keys: Sequence[str],
        manifest_id: str,
        owner_epoch: int,
        error: str,
        tokens_by_key: Mapping[str, Any],
    ) -> None:
        control: Any = self._control
        if hasattr(control, "bind_sources_to_archive_manifest"):
            control.bind_sources_to_archive_manifest(
                service_id,
                object_keys,
                manifest_id=manifest_id,
                expected_owner_epoch=owner_epoch,
                error=error,
                claim_tokens=tuple(tokens_by_key[key] for key in object_keys if key in tokens_by_key),
            )
        elif hasattr(control, "fail_sources_batch"):
            control.fail_sources_batch(
                service_id,
                object_keys,
                expected_owner_epoch=owner_epoch,
                error=error,
                claim_tokens=tuple(tokens_by_key[key] for key in object_keys if key in tokens_by_key),
            )

    def retry_archived_publications(
        self, service_id: str, *, limit: int = 4, min_idle_seconds: float = 30.0
    ) -> ArchiveRetryResult:
        """Republish committed manifests whose publication failed, from the archive artifact, under their original ids."""
        control: Any = self._control
        if control is None or not hasattr(control, "claim_archived_publication_retries"):
            return ArchiveRetryResult()
        owner = control.owner(service_id)
        if owner.current_owner != "high_scale":
            return ArchiveRetryResult()
        clock = time.monotonic()
        for manifest_id, (not_before, _) in tuple(self._publication_retry_backoff.items()):
            if not_before + _PUBLICATION_RETRY_MAX_BACKOFF_SECONDS < clock:
                del self._publication_retry_backoff[manifest_id]
        claims = control.claim_archived_publication_retries(
            service_id,
            self._worker_id,
            expected_owner_epoch=owner.owner_epoch,
            lease_seconds=self._lease_seconds,
            min_idle_seconds=min_idle_seconds,
            limit=limit,
            exclude_manifest_ids=tuple(
                mid for mid, (not_before, _) in self._publication_retry_backoff.items() if not_before > clock
            ),
        )
        acknowledged = failed = released = 0
        for claim in claims:
            outcome = self._retry_archived_manifest(service_id, claim, owner.owner_epoch)
            acknowledged += outcome == "acknowledged"
            failed += outcome == "failed"
            released += outcome == "released"
        return ArchiveRetryResult(acknowledged, failed, released)

    def _retry_archived_manifest(self, service_id: str, claim: Any, owner_epoch: int) -> str:
        control: Any = self._control
        manifest_id = claim.manifest_id
        log_extra = {"service_id": service_id, "manifest_id": manifest_id, "sources": len(claim.object_keys)}
        manifest = control.archive_manifest(manifest_id).manifest
        try:
            events, sibling_events = self._rebuild_archived_events(manifest)
        except (ValueError, KeyError, TypeError) as exc:
            logger.error(
                "high-scale archive publication retry cannot read archive; returning sources to raw ingest",
                extra={**log_extra, "error": str(exc)},
            )
            control.unbind_archived_sources(
                service_id,
                manifest_id,
                self._worker_id,
                expected_owner_epoch=owner_epoch,
                error=f"archive manifest {manifest_id} is not replayable: {exc}",
            )
            self._publication_retry_backoff.pop(manifest_id, None)
            return "released"
        except Exception as exc:
            logger.exception("high-scale archive publication retry could not read archive", extra=log_extra)
            failure = str(exc)
        else:
            try:
                _, derived_ok, derived_err = self._publish_manifest_publications(
                    service_id, manifest.domain, manifest_id, claim.publication_epoch, events, sibling_events
                )
            except Exception as exc:
                logger.exception("high-scale archive publication retry failed", extra=log_extra)
                failure = str(exc)
            else:
                if derived_ok:
                    control.acknowledge_sources_batch(
                        service_id,
                        claim.object_keys,
                        manifest_id=manifest_id,
                        expected_owner_epoch=owner_epoch,
                        claim_tokens=claim.claim_tokens,
                        domain=manifest.domain,
                    )
                    self._publication_retry_backoff.pop(manifest_id, None)
                    logger.info("high-scale archive publication retry acknowledged sources", extra=log_extra)
                    return "acknowledged"
                failure = derived_err
        control.release_archived_publication_retry(
            service_id, manifest_id, self._worker_id, expected_owner_epoch=owner_epoch, error=failure
        )
        _, failures = self._publication_retry_backoff.get(manifest_id, (0.0, 0))
        delay = min(_PUBLICATION_RETRY_BACKOFF_SECONDS * 2**failures, _PUBLICATION_RETRY_MAX_BACKOFF_SECONDS)
        self._publication_retry_backoff[manifest_id] = (time.monotonic() + delay, failures + 1)
        return "failed"

    def _rebuild_archived_events(self, manifest: ArchiveManifest) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Re-decode the archived source lines so events match what the original ingest published.

        The rebuilt archive rows must reproduce the manifest's canonical digest, which proves the events are the
        ones the pending publications were built from; NGWAF enrichment is replayed from the archived marker.
        """
        rows = _read_manifest_rows(manifest, self._archive)
        domain = manifest.domain
        sibling = _RUM_SIBLING_DOMAIN.get(domain)
        transform_version = manifest.artifact.transform_version
        rebuilt_rows: list[dict[str, Any]] = []
        events: list[dict[str, Any]] = []
        sibling_events: list[dict[str, Any]] = []
        markers: dict[tuple[str, int], str] = {}
        for entry in manifest.sources:
            lines: dict[int, bytes] = {}
            for row in rows[entry.row_start : entry.row_start + entry.row_count]:
                ordinal = int(row["line_ordinal"])
                if row.get("_record_kind") == "dead_letter":
                    lines[ordinal] = base64.b64decode(row["raw_line_base64"])
                    continue
                lines[ordinal] = str(row["raw_json"]).encode("utf-8")
                if row.get(_NGWAF_ENRICHMENT_KEY) is not None:
                    markers[(entry.key, ordinal)] = str(row[_NGWAF_ENRICHMENT_KEY])
            payload = b"\n".join(lines.get(i, b"") for i in range(max(lines) + 1)) if lines else b""
            source = ArchiveSourceObject(
                manifest.service_id, domain, entry.key, entry.checksum, entry.size_bytes, entry.version
            )
            decoded = decode_source_object(source, payload, transform_version=transform_version, domain=domain)
            for event in decoded.events:
                row = dict(event, _record_kind="event")
                marker = markers.get((entry.key, int(event["line_ordinal"])))
                if marker is not None:
                    row[_NGWAF_ENRICHMENT_KEY] = marker
                rebuilt_rows.append(row)
            rebuilt_rows.extend(
                {
                    "_record_kind": "dead_letter",
                    "source_object_key": item.source_object_key,
                    "line_ordinal": item.line_ordinal,
                    "raw_line_base64": item.raw_line_base64,
                    "reason": item.reason,
                }
                for item in decoded.dead_letters
            )
            events.extend(decoded.events)
            if sibling is not None:
                sibling_decoded = decode_source_object(
                    replace(source, domain=sibling), payload, transform_version=transform_version, domain=sibling
                )
                sibling_events.extend(e for e in sibling_decoded.events if row_matches_serving_domain(sibling, e))
        if event_digest(rebuilt_rows) != manifest.artifact.canonical_digest:
            raise ValueError("re-decoded archive rows do not match the committed manifest digest")
        if domain == "request":
            unmarked: list[dict[str, Any]] = []
            for event in events:
                if not event.get("waf_req_id"):
                    continue
                marker = markers.get((str(event["source_object_key"]), int(event["line_ordinal"])))
                if marker is None:
                    unmarked.append(event)
                elif bot := json.loads(marker):
                    _apply_ngwaf_bot(event, *bot)
            if unmarked:
                _enrich_high_scale_ngwaf_bots(tuple(unmarked))
        return events, sibling_events

    def _publish_aggregates_batch(
        self, service_id: str, domain: str, manifest_id: str, owner_epoch: int, events: Sequence[dict[str, Any]]
    ) -> tuple[bool, str | None]:
        if self._aggregates is None or not events:
            return True, None
        try:
            event_tuple: tuple[dict[str, Any], ...] = tuple(events)
            batches_to_publish: list[HighScaleBatch] = []
            epoch_str = str(owner_epoch)
            if domain == "request" and len(event_tuple) >= 2000:
                with ThreadPoolExecutor(max_workers=5) as pool:
                    fut_counts = pool.submit(compute_dimension_counts, domain, event_tuple)
                    fut_origin = pool.submit(build_origin_projection_rows, event_tuple)
                    fut_perf = pool.submit(build_performance_projection_rows, event_tuple)
                    fut_sec = pool.submit(build_security_projection_rows, event_tuple)
                    fut_net = pool.submit(build_network_projection_rows, event_tuple)
                    counts = fut_counts.result()
                    origin_proj = fut_origin.result()
                    perf_proj = fut_perf.result()
                    security_proj = fut_sec.result()
                    network_proj = fut_net.result()
                if counts:
                    agg_id = build_batch_id_from_manifest_id(f"{manifest_id}:{domain}_aggregate")
                    batches_to_publish.append(
                        HighScaleBatch(agg_id, service_id, f"{domain}_aggregate", epoch_str, counts)
                    )
                for sub_domain, rows in (
                    ("origin_summary", origin_proj.summary_rows),
                    ("origin_dimensions", origin_proj.dimension_rows),
                    ("performance_dimensions", perf_proj.dimension_rows),
                    ("performance_summary", perf_proj.summary_rows),
                    ("performance_scatter", perf_proj.scatter_rows),
                    ("security_dimensions", security_proj.dimension_rows),
                    ("network_dimensions", network_proj.dimension_rows),
                ):
                    if rows:
                        sub_id = build_batch_id_from_manifest_id(f"{manifest_id}:{sub_domain}")
                        batches_to_publish.append(HighScaleBatch(sub_id, service_id, sub_domain, epoch_str, rows))
                del origin_proj, perf_proj, security_proj, network_proj
            else:
                counts = compute_dimension_counts(domain, event_tuple)
                if counts:
                    agg_id = build_batch_id_from_manifest_id(f"{manifest_id}:{domain}_aggregate")
                    batches_to_publish.append(
                        HighScaleBatch(agg_id, service_id, f"{domain}_aggregate", epoch_str, counts)
                    )
                if domain == "request":
                    origin_proj = build_origin_projection_rows(event_tuple)
                    for sub_domain, rows in (
                        ("origin_summary", origin_proj.summary_rows),
                        ("origin_dimensions", origin_proj.dimension_rows),
                    ):
                        if rows:
                            sub_id = build_batch_id_from_manifest_id(f"{manifest_id}:{sub_domain}")
                            batches_to_publish.append(HighScaleBatch(sub_id, service_id, sub_domain, epoch_str, rows))
                    del origin_proj

                    perf_proj = build_performance_projection_rows(event_tuple)
                    for sub_domain, rows in (
                        ("performance_dimensions", perf_proj.dimension_rows),
                        ("performance_summary", perf_proj.summary_rows),
                        ("performance_scatter", perf_proj.scatter_rows),
                    ):
                        if rows:
                            sub_id = build_batch_id_from_manifest_id(f"{manifest_id}:{sub_domain}")
                            batches_to_publish.append(HighScaleBatch(sub_id, service_id, sub_domain, epoch_str, rows))
                    del perf_proj

                    security_proj = build_security_projection_rows(event_tuple)
                    if security_proj.dimension_rows:
                        sub_id = build_batch_id_from_manifest_id(f"{manifest_id}:security_dimensions")
                        batches_to_publish.append(
                            HighScaleBatch(
                                sub_id, service_id, "security_dimensions", epoch_str, security_proj.dimension_rows
                            )
                        )
                    del security_proj

                    network_proj = build_network_projection_rows(event_tuple)
                    if network_proj.dimension_rows:
                        sub_id = build_batch_id_from_manifest_id(f"{manifest_id}:network_dimensions")
                        batches_to_publish.append(
                            HighScaleBatch(
                                sub_id, service_id, "network_dimensions", epoch_str, network_proj.dimension_rows
                            )
                        )
                    del network_proj
            can_parallel_publish = (
                len(batches_to_publish) > 1
                and len(event_tuple) >= 2000
                and "InMemory" not in type(getattr(self._aggregates, "_manifests", None)).__name__
            )
            if can_parallel_publish:
                with ThreadPoolExecutor(max_workers=min(4, len(batches_to_publish))) as pub_pool:
                    futs = [pub_pool.submit(self._aggregates.publish, b) for b in batches_to_publish]
                    for fut in futs:
                        fut.result()
            else:
                for b in batches_to_publish:
                    self._aggregates.publish(b)
            return True, None
        except Exception as exc:
            logger.exception(
                "high-scale batch aggregate publish failed",
                extra={"service_id": service_id, "domain": domain, "manifest_id": manifest_id},
            )
            return False, str(exc)

    def _publish_cmcd_projection(
        self, service_id: str, batch_id: str, owner_epoch: int, events: Sequence[dict[str, Any]]
    ) -> tuple[bool, str | None]:
        """Publish CMCD projection rows for request events carrying CMCD fields."""
        try:
            rows: list[dict[str, Any]] = []
            skipped = 0
            for event in events:
                if not event.get("cmcd"):
                    continue
                try:
                    projection = project_cmcd(event)
                except ValueError:
                    skipped += 1
                    continue
                if projection is not None:
                    rows.append(projection)
            if skipped:
                logger.warning(
                    "high-scale cmcd projection skipped malformed events",
                    extra={"service_id": service_id, "skipped": skipped},
                )
            if rows:
                self._serving.publish(HighScaleBatch(batch_id, service_id, "cmcd", str(owner_epoch), tuple(rows)))
            return True, None
        except Exception as exc:
            logger.exception(
                "high-scale cmcd projection publish failed", extra={"service_id": service_id, "batch_id": batch_id}
            )
            return False, str(exc)

    def _publish_control_manifest(self, manifest: ArchiveManifest, owner_epoch: int) -> None:
        if self._control is None:
            raise RuntimeError("self._control is unexpectedly None")
        accepts_state = False
        try:
            accepts_state = "state" in inspect.signature(self._control.register_archive_manifest).parameters
        except (TypeError, ValueError):
            accepts_state = False
        if accepts_state:
            reg_fn: Any = self._control.register_archive_manifest
            record = reg_fn(manifest, owner_epoch=owner_epoch, state=ArchiveState.MANIFEST_COMMITTED)
        else:
            record = self._control.register_archive_manifest(manifest, owner_epoch=owner_epoch)
        states = (
            ArchiveState.ARTIFACT_UPLOADING,
            ArchiveState.ARTIFACT_VERIFIED,
            ArchiveState.MANIFEST_PREPARED,
            ArchiveState.MANIFEST_COMMITTED,
        )
        state_index = states.index(record.state)
        for next_state in states[state_index + 1 :]:
            self._control.transition_archive_manifest(
                manifest.manifest_id,
                expected_state=states[state_index],
                next_state=next_state,
                expected_owner_epoch=owner_epoch,
            )
            state_index += 1


def _lookup_high_scale_ngwaf_bots(events: Sequence[dict[str, Any]]) -> dict[str, tuple[Any, Any, Any]]:
    waf_req_ids = {str(e.get("waf_req_id")) for e in events if e.get("waf_req_id")}
    if not waf_req_ids:
        return {}
    try:
        from backend.core.metadata import base as pg_connection

        pconn = pg_connection.get_pg_readonly_connection()
        try:
            placeholders = ",".join("%s" for _ in waf_req_ids)
            cur = pconn.execute(
                f"SELECT waf_req_id, bot_name, category, wellknown_bot_name FROM ngwaf_bots WHERE waf_req_id IN ({placeholders})",
                list(waf_req_ids),
            )
            rows = cur.fetchall()
        finally:
            pconn.close()
        return {
            (r["waf_req_id"] if isinstance(r, dict) else r[0]): (
                r["bot_name"] if isinstance(r, dict) else r[1],
                r["category"] if isinstance(r, dict) else r[2],
                r["wellknown_bot_name"] if isinstance(r, dict) else r[3],
            )
            for r in rows
        }
    except Exception:
        logger.warning("high-scale NGWAF bot enrichment failed", exc_info=True)
        return {}


def _apply_ngwaf_bot(event: dict[str, Any], name: Any, cat: Any, wk: Any) -> None:
    event["_ngwaf_bot_name"] = event["ngwaf_bot"] = name
    event["_ngwaf_bot_category"] = cat
    event["_ngwaf_wellknown_bot_name"] = wk
    event["ngwaf_bot_verification_state"] = "verified"
    if cat:
        event["ngwaf_bot_category"] = cat
    if wk:
        event["ngwaf_wellknown_bot"] = wk


def _apply_high_scale_ngwaf_bots(events: Sequence[dict[str, Any]], bot_map: dict[str, tuple[Any, Any, Any]]) -> None:
    if not bot_map:
        return
    for e in events:
        wid = str(e.get("waf_req_id")) if e.get("waf_req_id") else None
        if wid and wid in bot_map:
            _apply_ngwaf_bot(e, *bot_map[wid])


def _mark_ngwaf_enrichment(archive_rows: Sequence[dict[str, Any]], bot_map: dict[str, tuple[Any, Any, Any]]) -> None:
    """Record the bot lookup result in the archive so a publication retry reproduces the same enriched rows."""
    for row in archive_rows:
        if row.get("_record_kind") == "event" and row.get("waf_req_id"):
            bot = bot_map.get(str(row["waf_req_id"]))
            row[_NGWAF_ENRICHMENT_KEY] = json.dumps(list(bot) if bot is not None else [], separators=(",", ":"))


def _enrich_high_scale_ngwaf_bots(events: tuple[dict[str, Any], ...]) -> None:
    _apply_high_scale_ngwaf_bots(events, _lookup_high_scale_ngwaf_bots(events))


# ── Worker & Orchestration (folded from worker.py & orchestration.py) ─────────
MAX_SOURCE_PAGE_SIZE = 500
INITIAL_SOURCE_CURSOR = "__initial__"
TERMINAL_SOURCE_CURSOR_PREFIX = "__terminal__:"
RUM_REINSPECTION_INTERVAL = 10
RUM_SOURCE_DOMAINS = frozenset({"rum_vitals", "rum_errors"})


class SourceObjectMissingError(RuntimeError):
    """Raised when a claimed source object is confirmed gone from storage."""


@dataclass(frozen=True)
class SourceObjectDescriptor:
    object_key: str
    checksum: str
    size_bytes: int = 0
    version: str | None = None


@dataclass(frozen=True)
class SourceObjectPage:
    objects: Sequence[SourceObjectDescriptor]
    next_cursor: str | None
    next_reinspection_cursor: str | None = None
    forward_pages_since_reinspection: int | None = None
    reinspection_page: bool = False


class SourceObjectLister(Protocol):
    def list_source_objects(
        self,
        service_id: str,
        domain: str,
        *,
        page_size: int,
        cursor: str | None,
        reinspection_cursor: str | None = None,
        reinspect: bool = False,
        forward_pages_since_reinspection: int = 0,
        partition_prefix: str = "",
    ) -> SourceObjectPage: ...


class S3SourceObjectLister:
    """Paginated S3-compatible source listing for the high-scale worker plane."""

    _PREFIXES = {"request": "raw/request/", "rum_vitals": "raw/rum/", "rum_errors": "raw/rum/"}

    def __init__(self, client: Any, *, bucket: str, prefix: str = "") -> None:
        if not bucket:
            raise ValueError("bucket is required")
        self._client = client
        self._bucket = bucket
        self._prefix = prefix.strip("/")
        self._minute_backscans: dict[str, tuple[int, float]] = {}

    def _should_backscan_minute(self, start_after: str) -> bool:
        now_mono = time.monotonic()
        count, last_mono = self._minute_backscans.get(start_after, (0, 0.0))
        if count >= 3 or (count > 0 and now_mono - last_mono < 5.0):
            return False
        self._minute_backscans[start_after] = (count + 1, now_mono)
        if len(self._minute_backscans) > 256:
            oldest_key = next(iter(self._minute_backscans))
            self._minute_backscans.pop(oldest_key, None)
        return True

    def list_source_objects(
        self,
        service_id: str,
        domain: str,
        *,
        page_size: int,
        cursor: str | None,
        reinspection_cursor: str | None = None,
        reinspect: bool = False,
        forward_pages_since_reinspection: int = 0,
        partition_prefix: str = "",
    ) -> SourceObjectPage:
        del service_id
        if page_size <= 0 or page_size > MAX_SOURCE_PAGE_SIZE:
            raise ValueError(f"page_size must be between 1 and {MAX_SOURCE_PAGE_SIZE}")
        domain_prefix = self._PREFIXES.get(domain)
        if domain_prefix is None:
            raise ValueError(f"unsupported source domain: {domain}")
        if reinspect and domain not in RUM_SOURCE_DOMAINS:
            raise ValueError("only RUM source domains support reinspection")
        if forward_pages_since_reinspection < 0:
            raise ValueError("forward_pages_since_reinspection cannot be negative")
        sub_prefix = partition_prefix.lstrip("/")
        combined_prefix = f"{domain_prefix.rstrip('/')}/{sub_prefix}" if sub_prefix else domain_prefix
        resolved_prefix = self._key(combined_prefix)
        listing_cursor = (reinspection_cursor or INITIAL_SOURCE_CURSOR) if reinspect else cursor
        request: dict[str, Any] = {
            "Bucket": self._bucket,
            "Prefix": resolved_prefix,
            "PaginationConfig": {"PageSize": page_size},
        }
        if listing_cursor and listing_cursor != INITIAL_SOURCE_CURSOR:
            if listing_cursor.startswith(TERMINAL_SOURCE_CURSOR_PREFIX):
                start_after = listing_cursor.removeprefix(TERMINAL_SOURCE_CURSOR_PREFIX)
                if start_after.startswith(resolved_prefix):
                    request["StartAfter"] = start_after
            else:
                request["PaginationConfig"]["StartingToken"] = listing_cursor
        page = next(iter(self._client.get_paginator("list_objects_v2").paginate(**request)))
        objects = tuple(
            SourceObjectDescriptor(
                object_key=item["Key"],
                checksum=_source_checksum(item),
                size_bytes=int(item.get("Size", 0)),
                version=item.get("VersionId") or item.get("ETag"),
            )
            for item in page.get("Contents", [])
            if isinstance(item.get("Key"), str)
        )
        next_cursor = page.get("NextToken")
        if (
            not objects
            and next_cursor is None
            and not reinspect
            and listing_cursor
            and listing_cursor.startswith(TERMINAL_SOURCE_CURSOR_PREFIX)
        ):
            start_after = listing_cursor.removeprefix(TERMINAL_SOURCE_CURSOR_PREFIX)
            if "/minute=" in start_after and start_after.startswith(resolved_prefix):
                minute_prefix = start_after.rsplit("/", 1)[0] + "/"
                if self._should_backscan_minute(start_after):
                    backscan_req: dict[str, Any] = {
                        "Bucket": self._bucket,
                        "Prefix": resolved_prefix,
                        "StartAfter": minute_prefix,
                        "PaginationConfig": {"PageSize": page_size},
                    }
                    backscan_page = next(iter(self._client.get_paginator("list_objects_v2").paginate(**backscan_req)))
                    objects = tuple(
                        SourceObjectDescriptor(
                            object_key=item["Key"],
                            checksum=_source_checksum(item),
                            size_bytes=int(item.get("Size", 0)),
                            version=item.get("VersionId") or item.get("ETag"),
                        )
                        for item in backscan_page.get("Contents", [])
                        if isinstance(item.get("Key"), str)
                    )
                    next_cursor = backscan_page.get("NextToken")
        if domain not in RUM_SOURCE_DOMAINS:
            return SourceObjectPage(objects, next_cursor)
        if reinspect:
            has_more = bool(page.get("NextContinuationToken"))
            next_reinspection = (
                f"{TERMINAL_SOURCE_CURSOR_PREFIX}{objects[-1].object_key}"
                if has_more and objects
                else INITIAL_SOURCE_CURSOR
            )
            return SourceObjectPage(
                objects,
                cursor or INITIAL_SOURCE_CURSOR,
                next_reinspection_cursor=next_reinspection,
                forward_pages_since_reinspection=0,
                reinspection_page=True,
            )
        return SourceObjectPage(
            objects,
            next_cursor,
            next_reinspection_cursor=reinspection_cursor or INITIAL_SOURCE_CURSOR,
            forward_pages_since_reinspection=forward_pages_since_reinspection + 1,
        )

    def _key(self, key: str) -> str:
        normalized = key.lstrip("/")
        return f"{self._prefix}/{normalized}" if self._prefix else normalized


class S3SourceObjectReader:
    """Reads immutable source objects from an S3-compatible bucket."""

    def __init__(self, client: Any, *, bucket: str) -> None:
        if not bucket:
            raise ValueError("bucket is required")
        self._client = client
        self._bucket = bucket

    def read_source_object(self, service_id: str, domain: str, object_key: str) -> bytes:
        del service_id, domain
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=object_key)
        except Exception as exc:
            if _client_error_code(exc) in {"NoSuchKey", "404"}:
                raise SourceObjectMissingError(f"source object is missing from storage: {object_key}") from exc
            raise
        return response["Body"].read()


def _client_error_code(exc: Exception) -> str | None:
    """Extract the S3/boto3 error code without importing botocore directly."""
    response = getattr(exc, "response", None)
    error = response.get("Error") if isinstance(response, dict) else None
    code = error.get("Code") if isinstance(error, dict) else None
    return code if isinstance(code, str) else None


def _source_checksum(item: dict[str, object]) -> str:
    checksum = item.get("ChecksumSHA256") or item.get("ChecksumSHA1")
    if isinstance(checksum, str) and checksum:
        return checksum if ":" in checksum else f"checksum:{checksum}"
    etag = item.get("ETag")
    if isinstance(etag, str) and etag:
        return f"etag:{etag.strip(chr(34))}"
    raise ValueError("source object listing item has no checksum or ETag")


class SourceObjectReader(Protocol):
    def read_source_object(self, service_id: str, domain: str, object_key: str) -> bytes: ...


class SourceControlPlane(Protocol):
    def owner(self, service_id: str) -> Any: ...
    def source(self, service_id: str, object_key: str) -> Any: ...
    def source_cursor_for(self, service_id: str, domain: str, *, partition_id: str = "default") -> str: ...
    def source_reinspection_state_for(self, service_id: str, domain: str) -> tuple[str, int]: ...
    def advance_reinspection_state(
        self,
        service_id: str,
        domain: str,
        reinspection_cursor: str,
        forward_pages_since_reinspection: int,
        *,
        expected_owner: str,
        expected_owner_epoch: int,
    ) -> None: ...
    def discover_source(
        self,
        service_id: str,
        domain: str,
        object_key: str,
        checksum: str,
        *,
        size_bytes: int,
        version: str | None,
        expected_owner: str,
        expected_owner_epoch: int,
    ) -> Any: ...
    def claim_source(
        self,
        service_id: str,
        object_key: str,
        worker_id: str,
        *,
        expected_owner: str,
        expected_owner_epoch: int,
        lease_seconds: float,
        now: datetime | None = None,
    ) -> Any: ...


@dataclass(frozen=True)
class PageRun:
    cursor: str | None
    next_cursor: str | None
    discovered: int
    processed: int
    duplicates: int
    failed: int
    missing: int = 0


def _owner_fields(owner: Any) -> tuple[str, int, str | None]:
    return owner.current_owner, int(owner.owner_epoch), getattr(owner, "source_cursor", None)


class HighScaleWorkerCoordinator:
    """Runs bounded discovery and processing under an owner-epoch fence."""

    def __init__(
        self,
        *,
        lister: SourceObjectLister,
        reader: SourceObjectReader,
        controller: HighScaleIngestController,
        ownership: Any,
        ledger: Any,
        worker_id: str,
        control_plane: SourceControlPlane | None = None,
        page_size: int = 100,
        lease_seconds: float = 300.0,
        object_concurrency: int = 1,
    ) -> None:
        if not worker_id:
            raise ValueError("worker identity is required")
        if page_size <= 0 or page_size > MAX_SOURCE_PAGE_SIZE:
            raise ValueError(f"page_size must be between 1 and {MAX_SOURCE_PAGE_SIZE}")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        if object_concurrency <= 0:
            raise ValueError("object_concurrency must be positive")
        self._lister = lister
        self._reader = reader
        self._controller = controller
        self._ownership = ownership
        self._ledger = ledger
        self._worker_id = worker_id
        self._control = control_plane
        self._page_size = page_size
        self._lease_seconds = lease_seconds
        self._object_concurrency = object_concurrency

    def retry_archived_publications(self, *, service_id: str, limit: int = 4) -> ArchiveRetryResult:
        return self._controller.retry_archived_publications(service_id, limit=limit)

    def run_page(
        self,
        *,
        service_id: str,
        domain: str,
        cursor: str | None = None,
        partition_id: str = "default",
        partition_prefix: str = "",
        now: datetime | None = None,
    ) -> PageRun:
        current_owner, epoch, persisted_cursor = _owner_fields(self._owner(service_id))
        self._require_high_scale(current_owner, service_id)
        if cursor is None:
            cursor_reader = self._control if self._control is not None else self._ownership
            if hasattr(cursor_reader, "source_cursor_for"):
                if partition_id != "default":
                    try:
                        persisted_cursor = cursor_reader.source_cursor_for(
                            service_id, domain, partition_id=partition_id
                        )
                    except TypeError:
                        persisted_cursor = cursor_reader.source_cursor_for(service_id, domain)
                else:
                    persisted_cursor = cursor_reader.source_cursor_for(service_id, domain)
        listing_cursor = persisted_cursor if cursor is None else cursor
        has_reinspection = self._control is not None and domain in RUM_SOURCE_DOMAINS
        listing_options: dict[str, Any] = {}
        if has_reinspection and self._control is not None:
            reinspection_cursor, fwd_pages = self._control.source_reinspection_state_for(service_id, domain)
            listing_options = {
                "reinspection_cursor": reinspection_cursor,
                "reinspect": fwd_pages >= RUM_REINSPECTION_INTERVAL,
                "forward_pages_since_reinspection": fwd_pages,
            }
        if partition_prefix:
            listing_options["partition_prefix"] = partition_prefix
        page = self._lister.list_source_objects(
            service_id, domain, page_size=self._page_size, cursor=listing_cursor, **listing_options
        )
        objects = tuple(page.objects)
        if len(objects) > self._page_size:
            raise ValueError("source object listing exceeded page_size")

        expired_sources = self._expired_claim_descriptors(service_id, domain)
        if self._control is not None and (
            getattr(type(self._control), "discover_sources_batch", None) is not None
            or "discover_sources_batch" in vars(self._control)
        ):
            discovered_by_key = getattr(self._control, "discover_sources_batch")(
                service_id, domain, objects, expected_owner=current_owner, expected_owner_epoch=epoch
            )
        else:
            discovered_by_key = {
                s.object_key: self._discover(service_id, domain, s, current_owner, epoch) for s in objects
            }
        next_cursor = page.next_cursor
        if next_cursor is None:
            if objects:
                candidate_cursor = f"{TERMINAL_SOURCE_CURSOR_PREFIX}{objects[-1].object_key}"
                if (
                    isinstance(listing_cursor, str)
                    and listing_cursor.startswith(TERMINAL_SOURCE_CURSOR_PREFIX)
                    and listing_cursor > candidate_cursor
                ):
                    next_cursor = listing_cursor
                else:
                    next_cursor = candidate_cursor
            elif isinstance(listing_cursor, str) and listing_cursor.startswith(TERMINAL_SOURCE_CURSOR_PREFIX):
                next_cursor = listing_cursor

        processed = duplicates = failed = missing = 0
        active_sources: list[SourceObjectDescriptor] = list(expired_sources)
        active_keys = {s.object_key for s in active_sources}
        for s in objects:
            if s.object_key in active_keys:
                continue
            rec_status = getattr(discovered_by_key.get(s.object_key), "status", None)
            is_terminal = (
                rec_status in _TERMINAL_SOURCE_STATES
                if isinstance(rec_status, str)
                else self._already_terminal(service_id, s.object_key)
            )
            if is_terminal:
                duplicates += 1
            else:
                active_sources.append(s)
                active_keys.add(s.object_key)

        if active_sources:
            source_chunks = self._chunk_active_sources(active_sources)
            for chunk_idx, source_chunk in enumerate(source_chunks):
                is_final_chunk = chunk_idx == len(source_chunks) - 1
                unclaimable_keys: set[str] = set()
                claim_tokens_by_key: dict[str, Any] = {}
                if self._control is not None and (
                    getattr(type(self._control), "claim_sources_batch", None) is not None
                    or "claim_sources_batch" in vars(self._control)
                ):
                    claims = getattr(self._control, "claim_sources_batch")(
                        service_id,
                        tuple(s.object_key for s in source_chunk),
                        self._worker_id,
                        expected_owner=current_owner,
                        expected_owner_epoch=epoch,
                        lease_seconds=self._lease_seconds,
                        now=now,
                    )
                    claimed_keys = {c.object_key for c in claims}
                    claim_tokens_by_key = {c.object_key: c.claim_token for c in claims}
                elif self._control is not None:
                    claimed_keys = set()
                    for s in source_chunk:
                        try:
                            c = self._control.claim_source(
                                service_id,
                                s.object_key,
                                self._worker_id,
                                expected_owner=current_owner,
                                expected_owner_epoch=epoch,
                                lease_seconds=self._lease_seconds,
                                now=now,
                            )
                            if c.claimed:
                                claimed_keys.add(s.object_key)
                                if hasattr(self._control, "source"):
                                    source = self._control.source(service_id, s.object_key)
                                    claim_tokens_by_key[s.object_key] = source.claim_token
                        except ValueError:
                            if not self._already_terminal(service_id, s.object_key):
                                failed += 1
                                unclaimable_keys.add(s.object_key)
                else:
                    claimed_keys = {
                        s.object_key
                        for s in source_chunk
                        if self._ledger.claim(service_id, s.object_key, self._worker_id).claimed
                    }

                to_read: list[SourceObjectDescriptor] = []
                for s in source_chunk:
                    if s.object_key in claimed_keys:
                        to_read.append(s)
                    elif s.object_key not in unclaimable_keys:
                        duplicates += 1

                if to_read:

                    def read_one(
                        src: SourceObjectDescriptor,
                    ) -> tuple[SourceObjectDescriptor, bytes | None, str | None, bool]:
                        try:
                            return (
                                src,
                                self._reader.read_source_object(service_id, domain, src.object_key),
                                None,
                                False,
                            )
                        except SourceObjectMissingError as exc:
                            return (src, None, str(exc), True)
                        except Exception as exc:
                            return (src, None, str(exc), False)

                    workers = min(self._object_concurrency, len(to_read))
                    if workers <= 1:
                        read_results = [read_one(s) for s in to_read]
                    else:
                        with ThreadPoolExecutor(
                            max_workers=workers, thread_name_prefix="high-scale-batch-reader"
                        ) as pool:
                            read_results = list(pool.map(read_one, to_read))

                    valid_payloads: list[tuple[SourceObjectDescriptor, bytes]] = []
                    for src, data, err, is_missing in read_results:
                        if is_missing:
                            self._mark_missing(
                                service_id,
                                object_key=src.object_key,
                                expected_owner=current_owner,
                                expected_epoch=epoch,
                                error=err or "missing",
                                now=now,
                            )
                            missing += 1
                        elif err is not None:
                            logger.exception(
                                "high-scale source read failed",
                                extra={
                                    "service_id": service_id,
                                    "domain": domain,
                                    "object_key": src.object_key,
                                    "error": err,
                                },
                            )
                            failed += 1
                        elif data is not None:
                            valid_payloads.append((src, data))
                    del read_results

                    if valid_payloads:
                        try:
                            ingest_kwargs: dict[str, Any] = {}
                            if partition_id != "default":
                                ingest_kwargs["partition_id"] = partition_id
                            batch_res = self._controller.ingest_batch(
                                service_id=service_id,
                                domain=domain,
                                sources_payloads=valid_payloads,
                                next_cursor=(
                                    next_cursor
                                    if is_final_chunk and failed == 0 and not page.reinspection_page
                                    else None
                                ),
                                claim_tokens=(
                                    tuple(
                                        claim_tokens_by_key[src.object_key]
                                        for src, _ in valid_payloads
                                        if src.object_key in claim_tokens_by_key
                                    )
                                    if self._control is not None
                                    else ()
                                ),
                                now=now,
                                **ingest_kwargs,
                            )
                            if getattr(batch_res, "derived_publication_complete", True):
                                processed += batch_res.sources_count
                            else:
                                failed += len(valid_payloads)
                        except Exception as exc:
                            logger.exception(
                                "high-scale batch ingest failed",
                                extra={"service_id": service_id, "domain": domain, "count": len(valid_payloads)},
                            )
                            if self._control is not None and hasattr(self._control, "fail_sources_batch"):
                                try:
                                    self._control.fail_sources_batch(
                                        service_id,
                                        tuple(src.object_key for src, _ in valid_payloads),
                                        expected_owner_epoch=epoch,
                                        error=str(exc),
                                        claim_tokens=tuple(
                                            claim_tokens_by_key[src.object_key]
                                            for src, _ in valid_payloads
                                            if src.object_key in claim_tokens_by_key
                                        ),
                                    )
                                except Exception:
                                    logger.debug("fail_sources_batch after ingest_batch error failed", exc_info=True)
                            elif self._control is None and hasattr(self._ledger, "mark_failed"):
                                for src, _ in valid_payloads:
                                    cur = self._ledger.source(service_id, src.object_key)
                                    if cur is not None:
                                        self._ledger.mark_failed(
                                            service_id, src.object_key, cur.lease_generation, str(exc)
                                        )
                            failed += len(valid_payloads)

        if failed == 0:
            if page.reinspection_page and has_reinspection and self._control is not None:
                self._control.advance_reinspection_state(
                    service_id,
                    domain,
                    page.next_reinspection_cursor or INITIAL_SOURCE_CURSOR,
                    page.forward_pages_since_reinspection or 0,
                    expected_owner=current_owner,
                    expected_owner_epoch=epoch,
                )
            else:
                self._advance_cursor(
                    service_id,
                    domain,
                    next_cursor or (INITIAL_SOURCE_CURSOR if listing_cursor == INITIAL_SOURCE_CURSOR else None),
                    current_owner,
                    epoch,
                    partition_id=partition_id,
                    reinspection_cursor=page.next_reinspection_cursor if has_reinspection else None,
                    forward_pages_since_reinspection=(
                        page.forward_pages_since_reinspection if has_reinspection else None
                    ),
                )
        return PageRun(listing_cursor, next_cursor, len(objects), processed, duplicates, failed, missing)

    def flush_archive_snapshot(
        self,
        *,
        service_id: str,
        domain: str,
        snapshot_interval_minutes: int = 5,
        force: bool = False,
        now: datetime | None = None,
        max_objects: int | None = None,
        max_bytes: int | None = None,
    ) -> SnapshotFlushResult:
        """Delegate consolidated archive snapshot flushing to the ingest controller."""
        return self._controller.flush_archive_snapshot(
            service_id=service_id,
            domain=domain,
            reader=self._reader,
            snapshot_interval_minutes=snapshot_interval_minutes,
            force=force,
            now=now,
            max_objects=max_objects,
            max_bytes=max_bytes,
        )

    def _chunk_active_sources(
        self, active_sources: Sequence[SourceObjectDescriptor]
    ) -> list[list[SourceObjectDescriptor]]:
        max_batch_objects = max(
            1, int(os.getenv("HIGH_SCALE_MAX_BATCH_OBJECTS", str(HIGH_SCALE_DEFAULT_MAX_BATCH_OBJECTS)))
        )
        max_batch_bytes = min(
            HIGH_SCALE_PAGE_MAX_BYTES,
            max(1, int(os.getenv("HIGH_SCALE_MAX_BATCH_BYTES", str(HIGH_SCALE_DEFAULT_MAX_BATCH_BYTES)))),
        )
        chunks: list[list[SourceObjectDescriptor]] = []
        current: list[SourceObjectDescriptor] = []
        current_bytes = 0
        for src in active_sources:
            src_bytes = max(0, int(src.size_bytes or 0))
            if current and (len(current) >= max_batch_objects or (current_bytes + src_bytes) > max_batch_bytes):
                chunks.append(current)
                current = []
                current_bytes = 0
            current.append(src)
            current_bytes += src_bytes
        if current:
            chunks.append(current)
        return chunks

    def _expired_claim_descriptors(self, service_id: str, domain: str) -> list[SourceObjectDescriptor]:
        expired_claims = getattr(self._control if self._control is not None else self._ledger, "expired_claims", None)
        if expired_claims is None:
            return []
        return [
            SourceObjectDescriptor(s.object_key, s.checksum, s.size_bytes, s.version)
            for s in expired_claims(service_id, domain, limit=self._page_size)
        ]

    def _mark_missing(
        self,
        service_id: str,
        *,
        object_key: str,
        expected_owner: str,
        expected_epoch: int,
        error: str,
        now: datetime | None,
    ) -> None:
        if self._control is not None and hasattr(self._control, "mark_source_missing"):
            self._control.mark_source_missing(
                service_id,
                object_key,
                expected_owner=expected_owner,
                expected_owner_epoch=expected_epoch,
                error=error,
                now=now,
            )
        elif hasattr(self._ledger, "mark_source_missing"):
            self._ledger.mark_source_missing(service_id, object_key, error=error)

    def _owner(self, service_id: str) -> Any:
        return self._control.owner(service_id) if self._control is not None else self._ownership.get(service_id)

    def _discover(
        self, service_id: str, domain: str, source: SourceObjectDescriptor, expected_owner: str, epoch: int
    ) -> Any:
        if self._control is not None:
            return self._control.discover_source(
                service_id,
                domain,
                source.object_key,
                source.checksum,
                size_bytes=source.size_bytes,
                version=source.version,
                expected_owner=expected_owner,
                expected_owner_epoch=epoch,
            )
        return self._ledger.discover(
            service_id,
            domain,
            source.object_key,
            source.checksum,
            size_bytes=source.size_bytes,
            version=source.version,
        )

    def _advance_cursor(
        self,
        service_id: str,
        domain: str,
        cursor: str | None,
        owner: str,
        epoch: int,
        *,
        partition_id: str = "default",
        reinspection_cursor: str | None = None,
        forward_pages_since_reinspection: int | None = None,
    ) -> None:
        if cursor is None:
            return
        if self._control is not None and hasattr(self._control, "advance_source_cursor"):
            state_kwargs: dict[str, Any] = (
                {
                    "reinspection_cursor": reinspection_cursor,
                    "forward_pages_since_reinspection": forward_pages_since_reinspection,
                }
                if reinspection_cursor is not None and forward_pages_since_reinspection is not None
                else {}
            )
            if partition_id != "default":
                state_kwargs["partition_id"] = partition_id
            self._control.advance_source_cursor(
                service_id, cursor, expected_owner=owner, expected_owner_epoch=epoch, domain=domain, **state_kwargs
            )
        elif hasattr(self._ownership, "advance_cursor"):
            self._ownership.advance_cursor(
                service_id, cursor, expected_owner=owner, expected_epoch=epoch, domain=domain
            )

    def _already_terminal(self, service_id: str, object_key: str) -> bool:
        source = None
        if self._control is not None and hasattr(self._control, "source"):
            try:
                source = self._control.source(service_id, object_key)
            except KeyError:
                source = None
        elif hasattr(self._ledger, "source"):
            source = self._ledger.source(service_id, object_key)
        elif hasattr(self._ledger, "get_source"):
            source = self._ledger.get_source(service_id, object_key)
        return getattr(source, "status", None) in _TERMINAL_SOURCE_STATES

    @staticmethod
    def _require_high_scale(owner: str, service_id: str) -> None:
        if owner != "high_scale":
            raise RuntimeError(f"high-scale worker is not the owner for {service_id}")


_TERMINAL_SOURCE_STATES = frozenset({"appended", "archived", "acknowledged", "source_deleted", "source_missing"})

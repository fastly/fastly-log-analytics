"""Fenced source-object controller for the high-scale ingestion boundary."""

from __future__ import annotations

import inspect
import logging
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from backend.core.high_scale_contracts import ArchiveState
from backend.high_scale.aggregate_writer import compute_dimension_counts
from backend.high_scale.archive_models import ArchiveManifest, ArchiveSourceObject
from backend.high_scale.archive_publication import ArchivePublication
from backend.high_scale.archive_writer import write_batch_archive_checkpoint
from backend.high_scale.decoder import DecodeResult, decode_source_object
from backend.high_scale.network_projection import build_network_projection_rows
from backend.high_scale.origin_projection import build_origin_projection_rows
from backend.high_scale.performance_projection import build_performance_projection_rows
from backend.high_scale.projections import project_cmcd
from backend.high_scale.publication import (
    ClickHousePublication,
    HighScaleBatch,
    PublicationResult,
    PublicationStatus,
    row_matches_serving_domain,
)
from backend.high_scale.schema import build_batch_id_from_manifest_id
from backend.high_scale.security_projection import build_security_projection_rows
from backend.high_scale.source_discovery import SourceObjectDescriptor

logger = logging.getLogger(__name__)

HIGH_SCALE_PAGE_MAX_BYTES = 64 * 1024 * 1024  # 64 MB in-flight byte guard
HIGH_SCALE_MAX_DEAD_LETTER_RATIO = 0.05  # 5% dead-letter limit


@dataclass(frozen=True)
class BatchIngestResult:
    manifest: ArchiveManifest
    publication: PublicationResult
    total_events: int
    quarantined_events: int
    sources_count: int


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
        self,
        service_id: str,
        object_key: str,
        *,
        accepted_rows: int,
        malformed_rows: int,
        expected_owner_epoch: int,
    ) -> None: ...

    def register_archive_manifest(self, manifest: ArchiveManifest, *, owner_epoch: int) -> Any: ...

    def archive_manifest(self, manifest_id: str) -> Any: ...

    def transition_archive_manifest(
        self,
        manifest_id: str,
        *,
        expected_state: ArchiveState,
        next_state: ArchiveState,
        expected_owner_epoch: int,
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
        next_cursor: str | None = None,
        domain: str = "request",
        partition_id: str = "default",
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

    def ingest_batch(
        self,
        *,
        service_id: str,
        domain: str,
        sources_payloads: Sequence[tuple[SourceObjectDescriptor, bytes]],
        next_cursor: str | None = None,
        partition_id: str = "default",
        now: datetime | None = None,
        max_dead_letter_ratio: float = HIGH_SCALE_MAX_DEAD_LETTER_RATIO,
    ) -> BatchIngestResult:
        if not sources_payloads:
            return BatchIngestResult(
                manifest=None,  # type: ignore[arg-type]
                publication=PublicationResult(
                    batch_id=f"{service_id}:{domain}:empty",
                    status=PublicationStatus.VISIBLE,
                    rows_visible=0,
                    duplicate=False,
                ),
                total_events=0,
                quarantined_events=0,
                sources_count=0,
            )

        total_bytes = sum(len(payload) for _, payload in sources_payloads)
        if total_bytes > HIGH_SCALE_PAGE_MAX_BYTES:
            raise ValueError(f"high-scale page size {total_bytes} exceeds maximum {HIGH_SCALE_PAGE_MAX_BYTES}")

        owner = self._control.owner(service_id) if self._control is not None else self._ownership.get(service_id)
        if owner.current_owner != "high_scale":
            raise RuntimeError(f"high-scale ingest rejected for {service_id}: owner is {owner.current_owner}")

        decoded_sources: list[tuple[SourceObjectDescriptor, DecodeResult, list[dict[str, Any]]]] = []
        all_events: list[dict[str, Any]] = []
        total_accepted = 0
        total_quarantined = 0
        counts_batch: list[tuple[str, int, int]] = []
        has_batch_counts = (
            self._control is not None and getattr(type(self._control), "record_source_counts_batch", None) is not None
        )

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
            decoded = decode_source_object(
                archive_source,
                payload,
                transform_version=self._transform_version,
                domain=domain,
            )
            total_accepted += decoded.accepted_rows
            total_quarantined += decoded.quarantined_rows

            if self._control is not None:
                if has_batch_counts:
                    counts_batch.append((descriptor.object_key, decoded.accepted_rows, decoded.quarantined_rows))
                else:
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

            decoded_sources.append((descriptor, decoded, archive_rows))
            all_events.extend(decoded.events)

        if self._control is not None and counts_batch:
            batch_counts_fn = getattr(self._control, "record_source_counts_batch", None)
            if (
                callable(batch_counts_fn)
                and getattr(type(self._control), "record_source_counts_batch", None) is not None
            ):
                try:
                    batch_counts_fn(
                        service_id,
                        counts_batch,
                        expected_owner_epoch=owner.owner_epoch,
                    )
                except TypeError:
                    for object_key, accepted_rows, malformed_rows in counts_batch:
                        self._control.record_source_counts(
                            service_id,
                            object_key,
                            accepted_rows=accepted_rows,
                            malformed_rows=malformed_rows,
                            expected_owner_epoch=owner.owner_epoch,
                        )
            else:
                for object_key, accepted_rows, malformed_rows in counts_batch:
                    self._control.record_source_counts(
                        service_id,
                        object_key,
                        accepted_rows=accepted_rows,
                        malformed_rows=malformed_rows,
                        expected_owner_epoch=owner.owner_epoch,
                    )

        total_rows = total_accepted + total_quarantined
        if total_rows > 0 and (total_quarantined / total_rows) > max_dead_letter_ratio:
            raise ValueError(
                f"high-scale dead letter ratio {total_quarantined}/{total_rows} exceeds maximum {max_dead_letter_ratio}"
            )

        observed = (now or datetime.now(UTC)).astimezone(UTC)
        source_batches = [
            (
                ArchiveSourceObject(
                    service_id=service_id,
                    domain=domain,
                    object_key=desc.object_key,
                    checksum=desc.checksum,
                    size_bytes=len(payload),
                    version=desc.version,
                ),
                rows,
            )
            for (desc, _, rows), (_, payload) in zip(decoded_sources, sources_payloads)
        ]

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
            artifact_path = Path(local_manifest.artifact.uri.removeprefix("file://"))
            artifact = artifact_path.read_bytes()
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

        if self._control is not None:
            self._publish_control_manifest(manifest, owner.owner_epoch)

        current_owner = (
            self._control.owner(service_id) if self._control is not None else self._ownership.get(service_id)
        )
        if current_owner.current_owner != "high_scale" or current_owner.owner_epoch != owner.owner_epoch:
            raise RuntimeError(f"high-scale owner epoch changed during ingest for {service_id}")

        if domain == "request":
            _enrich_high_scale_ngwaf_bots(tuple(all_events))
        serving_rows = tuple(e for e in all_events if row_matches_serving_domain(domain, e))
        batch_id = build_batch_id_from_manifest_id(manifest.manifest_id)
        if serving_rows:
            batch = HighScaleBatch(
                batch_id=batch_id,
                service_id=service_id,
                domain=domain,
                generation=str(owner.owner_epoch),
                rows=serving_rows,
            )
            publication = self._serving.publish(batch)
        else:
            publication = PublicationResult(
                batch_id=batch_id,
                status=PublicationStatus.VISIBLE,
                rows_visible=0,
                duplicate=False,
            )

        self._publish_aggregates_batch(service_id, domain, manifest.manifest_id, owner.owner_epoch, all_events)
        if domain == "request":
            self._publish_cmcd_projection(
                service_id,
                build_batch_id_from_manifest_id(f"{manifest.manifest_id}:cmcd"),
                owner.owner_epoch,
                all_events,
            )

        object_keys = tuple(desc.object_key for desc, _ in sources_payloads)
        if self._control is not None:
            self._control.acknowledge_sources_batch(
                service_id,
                object_keys,
                manifest_id=manifest.manifest_id,
                expected_owner_epoch=owner.owner_epoch,
                next_cursor=next_cursor,
                domain=domain,
                partition_id=partition_id,
            )
        else:
            for desc, _ in sources_payloads:
                cur_src = self._ledger.source(service_id, desc.object_key)
                gen = cur_src.lease_generation if cur_src is not None else 1
                if cur_src is None or cur_src.status != "acknowledged":
                    self._ledger.mark_appended(service_id, desc.object_key, gen)
                    self._ledger.mark_archived(
                        service_id,
                        desc.object_key,
                        gen,
                        manifest.manifest_id,
                        owner.owner_epoch,
                    )
                    self._ledger.acknowledge(service_id, desc.object_key, manifest.manifest_id)

        return BatchIngestResult(
            manifest=manifest,
            publication=publication,
            total_events=total_accepted,
            quarantined_events=total_quarantined,
            sources_count=len(sources_payloads),
        )

    def _publish_aggregates_batch(
        self,
        service_id: str,
        domain: str,
        manifest_id: str,
        owner_epoch: int,
        events: Sequence[dict[str, Any]],
    ) -> None:
        if self._aggregates is None or not events:
            return
        try:
            event_tuple: tuple[dict[str, Any], ...] = tuple(events)
            counts = compute_dimension_counts(domain, event_tuple)
            if counts:
                batch = HighScaleBatch(
                    batch_id=build_batch_id_from_manifest_id(f"{manifest_id}:{domain}_aggregate"),
                    service_id=service_id,
                    domain=f"{domain}_aggregate",
                    generation=str(owner_epoch),
                    rows=counts,
                )
                self._aggregates.publish(batch)
            if domain == "request":
                origin_proj = build_origin_projection_rows(event_tuple)
                perf_proj = build_performance_projection_rows(event_tuple)
                security_proj = build_security_projection_rows(event_tuple)
                network_proj = build_network_projection_rows(event_tuple)

                for sub_domain, rows in (
                    ("origin_summary", origin_proj.summary_rows),
                    ("origin_dimensions", origin_proj.dimension_rows),
                    ("performance_dimensions", perf_proj.dimension_rows),
                    ("performance_summary", perf_proj.summary_rows),
                    ("performance_scatter", perf_proj.scatter_rows),
                    ("security_dimensions", security_proj.dimension_rows),
                    ("network_dimensions", network_proj.dimension_rows),
                ):
                    if not rows:
                        continue
                    self._aggregates.publish(
                        HighScaleBatch(
                            batch_id=build_batch_id_from_manifest_id(f"{manifest_id}:{sub_domain}"),
                            service_id=service_id,
                            domain=sub_domain,
                            generation=str(owner_epoch),
                            rows=rows,
                        )
                    )
        except Exception:
            logger.exception(
                "high-scale batch aggregate publish failed",
                extra={"service_id": service_id, "domain": domain, "manifest_id": manifest_id},
            )

    def _publish_cmcd_projection(
        self,
        service_id: str,
        batch_id: str,
        owner_epoch: int,
        events: Sequence[dict[str, Any]],
    ) -> None:
        """Best-effort CMCD projection of request events carrying CMCD fields.

        Layered on the facts path like the aggregates: a failure here must
        never fail, retry, or duplicate the request ingest. The batch id is
        derived from the request manifest/object, so a replay is idempotent,
        and the archive remains the recovery source for any projection gap.
        """
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
            if not rows:
                return
            self._serving.publish(
                HighScaleBatch(
                    batch_id=batch_id,
                    service_id=service_id,
                    domain="cmcd",
                    generation=str(owner_epoch),
                    rows=tuple(rows),
                )
            )
        except Exception:
            logger.exception(
                "high-scale cmcd projection publish failed",
                extra={"service_id": service_id, "batch_id": batch_id},
            )

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
            record = reg_fn(
                manifest,
                owner_epoch=owner_epoch,
                state=ArchiveState.MANIFEST_COMMITTED,
            )
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


def _enrich_high_scale_ngwaf_bots(events: tuple[dict[str, Any], ...]) -> None:
    waf_req_ids = {str(e.get("waf_req_id")) for e in events if e.get("waf_req_id")}
    if not waf_req_ids:
        return

    try:
        from backend.core.metadata import pg_connection

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

        bot_map = {
            (r["waf_req_id"] if isinstance(r, dict) else r[0]): {
                "name": r["bot_name"] if isinstance(r, dict) else r[1],
                "cat": r["category"] if isinstance(r, dict) else r[2],
                "wk": r["wellknown_bot_name"] if isinstance(r, dict) else r[3],
            }
            for r in rows
        }
        for e in events:
            wid = str(e.get("waf_req_id")) if e.get("waf_req_id") else None
            if wid and wid in bot_map:
                bot = bot_map[wid]
                e["_ngwaf_bot_name"] = bot["name"]
                e["_ngwaf_bot_category"] = bot["cat"]
                e["_ngwaf_wellknown_bot_name"] = bot["wk"]
                e["ngwaf_bot_verification_state"] = "verified"
                # Persisted on the fact row (no underscore) so filtered Security
                # queries can group by bot without the waf_req_id join.
                e["ngwaf_bot"] = bot["name"]
                if bot["cat"]:
                    e["ngwaf_bot_category"] = bot["cat"]
                if bot["wk"]:
                    e["ngwaf_wellknown_bot"] = bot["wk"]
    except Exception:
        logger.warning("high-scale NGWAF bot enrichment failed", exc_info=True)

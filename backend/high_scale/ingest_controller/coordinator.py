"""Fenced source-object controller, aggregate batch adapter, assets, and service registry for high-scale."""

from __future__ import annotations

import inspect
import logging
import tempfile
from collections.abc import Sequence
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
    build_batch_id_from_manifest_id,
    row_matches_serving_domain,
    write_batch_archive_checkpoint,
)

logger = logging.getLogger("backend.high_scale.ingest_controller")

from backend.high_scale.ingest_controller.service import compute_dimension_counts
from backend.high_scale.query_service import DecodeResult, decode_source_object  # noqa: E402

# ── Ingest Controller ─────────────────────────────────────────────────────────
HIGH_SCALE_PAGE_MAX_BYTES = 64 * 1024 * 1024  # 64 MB in-flight byte guard
HIGH_SCALE_MAX_DEAD_LETTER_RATIO = 0.05  # 5% dead-letter limit
_RUM_SIBLING_DOMAIN = {"rum_vitals": "rum_errors", "rum_errors": "rum_vitals"}


@dataclass(frozen=True)
class BatchIngestResult:
    manifest: ArchiveManifest
    publication: PublicationResult
    total_events: int
    quarantined_events: int
    sources_count: int
    derived_publication_complete: bool = True


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
            pub = PublicationResult(f"{service_id}:{domain}:empty", PublicationStatus.VISIBLE, 0, False)
            return BatchIngestResult(None, pub, 0, 0, 0)  # type: ignore[arg-type]
        total_bytes = sum(len(payload) for _, payload in sources_payloads)
        if total_bytes > HIGH_SCALE_PAGE_MAX_BYTES:
            raise ValueError(f"high-scale page size {total_bytes} exceeds maximum {HIGH_SCALE_PAGE_MAX_BYTES}")
        owner = self._control.owner(service_id) if self._control is not None else self._ownership.get(service_id)
        if owner.current_owner != "high_scale":
            raise RuntimeError(f"high-scale ingest rejected for {service_id}: owner is {owner.current_owner}")
        source_batches: list[tuple[ArchiveSourceObject, list[dict[str, Any]]]] = []
        all_events: list[dict[str, Any]] = []
        sibling_events: list[dict[str, Any]] = []
        total_accepted = total_quarantined = sibling_quarantined = 0
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
            if self._control is not None:
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
            if sibling is not None:
                sibling_decoded = decode_source_object(
                    replace(archive_source, domain=sibling),
                    payload,
                    transform_version=self._transform_version,
                    domain=sibling,
                )
                sibling_quarantined += sibling_decoded.quarantined_rows
                sibling_events.extend(e for e in sibling_decoded.events if row_matches_serving_domain(sibling, e))
        if sibling_quarantined:
            logger.warning(
                "high-scale RUM sibling decode rejected retained source records",
                extra={"domain": _RUM_SIBLING_DOMAIN[domain], "quarantined_rows": sibling_quarantined},
            )
        if domain in _RUM_SIBLING_DOMAIN:
            source_batches = [(replace(src, domain=domain), rows) for src, rows in source_batches]
        elif len(source_batches) == 1 and source_batches[0][0].domain != domain:
            source_batches = [(replace(source_batches[0][0], domain=domain), source_batches[0][1])]
        total_rows = total_accepted + total_quarantined
        if total_rows > 0 and (total_quarantined / total_rows) > max_dead_letter_ratio:
            raise ValueError(
                f"high-scale dead letter ratio {total_quarantined}/{total_rows} exceeds maximum {max_dead_letter_ratio}"
            )
        observed = (now or datetime.now(UTC)).astimezone(UTC)
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
            publication = self._serving.publish(
                HighScaleBatch(batch_id, service_id, domain, str(owner.owner_epoch), serving_rows)
            )
        else:
            publication = PublicationResult(batch_id, PublicationStatus.VISIBLE, 0, False)
        agg_ok, agg_err = self._publish_aggregates_batch(
            service_id, domain, manifest.manifest_id, owner.owner_epoch, all_events
        )
        cmcd_ok, cmcd_err = True, None
        if domain == "request":
            cmcd_ok, cmcd_err = self._publish_cmcd_projection(
                service_id,
                build_batch_id_from_manifest_id(f"{manifest.manifest_id}:cmcd"),
                owner.owner_epoch,
                all_events,
            )
        sibling = _RUM_SIBLING_DOMAIN.get(domain)
        sibling_ok, sibling_err = True, None
        if sibling is not None and sibling_events:
            sibling_batch_id = build_batch_id_from_manifest_id(f"{manifest.manifest_id}:{sibling}")
            self._serving.publish(
                HighScaleBatch(sibling_batch_id, service_id, sibling, str(owner.owner_epoch), tuple(sibling_events))
            )
            sibling_ok, sibling_err = self._publish_aggregates_batch(
                service_id, sibling, manifest.manifest_id, owner.owner_epoch, sibling_events
            )
        derived_ok = agg_ok and cmcd_ok and sibling_ok
        derived_err = agg_err or cmcd_err or sibling_err or "derived publication failed"
        object_keys = tuple(desc.object_key for desc, _ in sources_payloads)
        if self._control is not None:
            if derived_ok:
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

    def _publish_aggregates_batch(
        self, service_id: str, domain: str, manifest_id: str, owner_epoch: int, events: Sequence[dict[str, Any]]
    ) -> tuple[bool, str | None]:
        if self._aggregates is None or not events:
            return True, None
        try:
            event_tuple: tuple[dict[str, Any], ...] = tuple(events)
            counts = compute_dimension_counts(domain, event_tuple)
            if counts:
                agg_id = build_batch_id_from_manifest_id(f"{manifest_id}:{domain}_aggregate")
                self._aggregates.publish(
                    HighScaleBatch(agg_id, service_id, f"{domain}_aggregate", str(owner_epoch), counts)
                )
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
                    if rows:
                        sub_id = build_batch_id_from_manifest_id(f"{manifest_id}:{sub_domain}")
                        self._aggregates.publish(HighScaleBatch(sub_id, service_id, sub_domain, str(owner_epoch), rows))
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


def _enrich_high_scale_ngwaf_bots(events: tuple[dict[str, Any], ...]) -> None:
    waf_req_ids = {str(e.get("waf_req_id")) for e in events if e.get("waf_req_id")}
    if not waf_req_ids:
        return
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
        bot_map = {
            (r["waf_req_id"] if isinstance(r, dict) else r[0]): (
                r["bot_name"] if isinstance(r, dict) else r[1],
                r["category"] if isinstance(r, dict) else r[2],
                r["wellknown_bot_name"] if isinstance(r, dict) else r[3],
            )
            for r in rows
        }
        for e in events:
            wid = str(e.get("waf_req_id")) if e.get("waf_req_id") else None
            if wid and wid in bot_map:
                name, cat, wk = bot_map[wid]
                e["_ngwaf_bot_name"] = e["ngwaf_bot"] = name
                e["_ngwaf_bot_category"] = cat
                e["_ngwaf_wellknown_bot_name"] = wk
                e["ngwaf_bot_verification_state"] = "verified"
                if cat:
                    e["ngwaf_bot_category"] = cat
                if wk:
                    e["ngwaf_wellknown_bot"] = wk
    except Exception:
        logger.warning("high-scale NGWAF bot enrichment failed", exc_info=True)


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
        resolved_prefix = self._key(domain_prefix)
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
    def source_cursor_for(self, service_id: str, domain: str) -> str: ...
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

    def run_page(
        self, *, service_id: str, domain: str, cursor: str | None = None, now: datetime | None = None
    ) -> PageRun:
        current_owner, epoch, persisted_cursor = _owner_fields(self._owner(service_id))
        self._require_high_scale(current_owner, service_id)
        if cursor is None:
            cursor_reader = self._control if self._control is not None else self._ownership
            if hasattr(cursor_reader, "source_cursor_for"):
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
        page = self._lister.list_source_objects(
            service_id, domain, page_size=self._page_size, cursor=listing_cursor, **listing_options
        )
        objects = tuple(page.objects)
        if len(objects) > self._page_size:
            raise ValueError("source object listing exceeded page_size")

        expired_sources = self._expired_claim_descriptors(service_id, domain)
        discovered_by_key = {s.object_key: self._discover(service_id, domain, s, current_owner, epoch) for s in objects}
        next_cursor = page.next_cursor
        if next_cursor is None:
            if objects:
                next_cursor = f"{TERMINAL_SOURCE_CURSOR_PREFIX}{objects[-1].object_key}"
            elif listing_cursor and listing_cursor.startswith(TERMINAL_SOURCE_CURSOR_PREFIX):
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
            unclaimable_keys: set[str] = set()
            if self._control is not None and (
                getattr(type(self._control), "claim_sources_batch", None) is not None
                or "claim_sources_batch" in vars(self._control)
            ):
                claims = getattr(self._control, "claim_sources_batch")(
                    service_id,
                    tuple(s.object_key for s in active_sources),
                    self._worker_id,
                    expected_owner=current_owner,
                    expected_owner_epoch=epoch,
                    lease_seconds=self._lease_seconds,
                    now=now,
                )
                claimed_keys = {c.object_key for c in claims}
            elif self._control is not None:
                claimed_keys = set()
                for s in active_sources:
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
                    except ValueError:
                        if not self._already_terminal(service_id, s.object_key):
                            failed += 1
                            unclaimable_keys.add(s.object_key)
            else:
                claimed_keys = {
                    s.object_key
                    for s in active_sources
                    if self._ledger.claim(service_id, s.object_key, self._worker_id).claimed
                }

            to_read: list[SourceObjectDescriptor] = []
            for s in active_sources:
                if s.object_key in claimed_keys:
                    to_read.append(s)
                elif s.object_key not in unclaimable_keys:
                    duplicates += 1

            if to_read:

                def read_one(
                    src: SourceObjectDescriptor,
                ) -> tuple[SourceObjectDescriptor, bytes | None, str | None, bool]:
                    try:
                        return (src, self._reader.read_source_object(service_id, domain, src.object_key), None, False)
                    except SourceObjectMissingError as exc:
                        return (src, None, str(exc), True)
                    except Exception as exc:
                        return (src, None, str(exc), False)

                workers = min(self._object_concurrency, len(to_read))
                if workers <= 1:
                    read_results = [read_one(s) for s in to_read]
                else:
                    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="high-scale-batch-reader") as pool:
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

                if valid_payloads:
                    try:
                        batch_res = self._controller.ingest_batch(
                            service_id=service_id,
                            domain=domain,
                            sources_payloads=valid_payloads,
                            next_cursor=None if failed > 0 or page.reinspection_page else next_cursor,
                            now=now,
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
                        if self._control is None and hasattr(self._ledger, "mark_failed"):
                            for src, _ in valid_payloads:
                                cur = self._ledger.source(service_id, src.object_key)
                                if cur is not None:
                                    self._ledger.mark_failed(service_id, src.object_key, cur.lease_generation, str(exc))
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
                    next_cursor,
                    current_owner,
                    epoch,
                    reinspection_cursor=page.next_reinspection_cursor if has_reinspection else None,
                    forward_pages_since_reinspection=(
                        page.forward_pages_since_reinspection if has_reinspection else None
                    ),
                )
        return PageRun(listing_cursor, next_cursor, len(objects), processed, duplicates, failed, missing)

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
        reinspection_cursor: str | None = None,
        forward_pages_since_reinspection: int | None = None,
    ) -> None:
        if cursor is None:
            return
        if self._control is not None and hasattr(self._control, "advance_source_cursor"):
            state_kwargs = (
                {
                    "reinspection_cursor": reinspection_cursor,
                    "forward_pages_since_reinspection": forward_pages_since_reinspection,
                }
                if reinspection_cursor is not None and forward_pages_since_reinspection is not None
                else {}
            )
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

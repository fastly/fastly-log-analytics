"""Cold-start disaster recovery from Fastly Object Storage (FOS) for High-Scale Elevation.

Recovers a brand-new Elevation installation with empty Postgres and empty ClickHouse
using only the Fastly API key + bucket/service (with explicit FOS keys as a fallback):

1. Mints/rotates a fresh bucket-scoped FOS access key via ``ensure_fos_access_key``
   (or uses explicit FOS credentials when both are provided).
2. Reads ``meta/dr_state.json`` directly from FOS via SigV4 boto3 S3 (never through
   the local telemetry proxy before ``configs/{service_id}.json`` exists), reconstructs
   and persists ``configs/{service_id}.json``, and idempotently restores Postgres
   metadata tables (``views``, ``alerts``, ``scoring_labels``, ``audit_logs``).
3. Initializes the High-Scale Postgres control plane, scans committed archive manifests
   under ``archive/manifests/*.commit``, registers them with grace-window-aware states,
   and records their source objects in ``high_scale_source_objects`` to prevent
   double-ingestion when normal ingest resumes.
4. Bootstraps ClickHouse tables and runs ``rebuild_serving_state_from_fos()`` across
   all three serving domains (``request``, ``rum_vitals``, ``rum_errors``).
"""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import re
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote, quote_plus

import boto3
from botocore.config import Config

from backend import config as svcconfig
from backend import scoring
from backend.core.clickhouse import ClickHouseClient, get_clickhouse_client
from backend.core.fastly.client import region_endpoint
from backend.core.fos_client import clear_fos_client
from backend.core.metadata import get_con
from backend.core.metadata.pg_schema import ensure_pg_schema
from backend.core.metadata.state_sync import read_dr_state
from backend.core.service_defaults import DEFAULT_LOG_PERIOD_SECONDS
from backend.high_scale.archive import (
    ArchiveManifest,
    ArchivePublication,
    ClickHouseBatchAdapter,
    ClickHousePublication,
    PostgresBatchManifestStore,
    S3ObjectStore,
    install_clickhouse_schema,
)
from backend.high_scale.archive.clickhouse_batch import _SCHEMA_FILES
from backend.high_scale.ingest_controller.service import AggregateBatchAdapter
from backend.high_scale.postgres_control import (
    ArchiveState,
    PostgresControlPlane,
)
from backend.high_scale.recovery import RecoveryReport, rebuild_serving_state_from_fos
from backend.provision.fos_setup import ensure_fos_access_key

logger = logging.getLogger(__name__)

_SERVICE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_REGION_RE = re.compile(r"^[a-z0-9-]{1,64}$")
_RESTORE_DOMAINS: tuple[str, ...] = ("request", "rum_vitals", "rum_errors")
_RECENT_WINDOW = timedelta(hours=24)
_DEFAULT_WARM_DAYS = 30
_GRACE_SAFETY_BUFFER = timedelta(hours=1)
_VALID_RESTORED_SOURCE_STATUSES = frozenset({"acknowledged", "source_deleted"})
_SOURCE_INSERT_CHUNK_SIZE = 1000


@dataclass(frozen=True)
class ColdStartRestoreResult:
    service_id: str
    dr_state_restored: bool
    views_restored: int
    alerts_restored: int
    scoring_labels_restored: int
    audit_logs_restored: int
    manifests_registered: int
    domains_rebuilt: dict[str, dict[str, Any]]


def scrub_secrets(text: str, secrets_to_hide: Iterable[str | None]) -> str:
    """Redact all non-empty secret values (and their URL-encoded forms) from ``text``."""
    if not text:
        return text
    candidates: set[str] = set()
    for raw in secrets_to_hide:
        if not raw:
            continue
        val = str(raw).strip()
        if not val:
            continue
        candidates.add(val)
        encoded = quote(val, safe="")
        if encoded != val:
            candidates.add(encoded)
        encoded_plus = quote_plus(val)
        if encoded_plus != val:
            candidates.add(encoded_plus)
    scrubbed = text
    for secret in sorted(candidates, key=len, reverse=True):
        scrubbed = scrubbed.replace(secret, "[REDACTED]")
    return scrubbed


def _build_direct_fos_s3_client(*, access_key: str, secret_key: str, region: str) -> Any:
    """Construct a direct SigV4 boto3 S3 client for FOS (bypassing the telemetry proxy)."""
    endpoint = region_endpoint(region)
    return boto3.client(
        "s3",
        endpoint_url=f"https://{endpoint}",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    )


def _source_object_id(service_id: str, domain: str, object_key: str, checksum: str) -> str:
    """Match ``PostgresControlPlane``'s deterministic ``object_id`` format."""
    return hashlib.sha256("\0".join((service_id, domain, object_key, checksum)).encode()).hexdigest()


def reconstruct_service_config(
    *,
    service_id: str,
    bucket: str,
    region: str,
    prefix: str,
    fos_access_key: str,
    fos_secret_key: str,
    fastly_api_key: str | None = None,
    dr_service_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a complete, valid ``configs/{service_id}.json`` payload from DR state + inputs."""
    cfg: dict[str, Any] = copy.deepcopy(dr_service_config) if isinstance(dr_service_config, dict) else {}
    clean_prefix = prefix.strip("/")
    cfg["service_id"] = service_id
    if not cfg.get("name"):
        fetched = None
        if fastly_api_key:
            try:
                fetched = svcconfig.fetch_service_name(service_id, fastly_api_key)
            except Exception:
                fetched = None
        cfg["name"] = fetched or service_id

    cfg["fos_endpoint"] = region_endpoint(region)
    cfg["fos_bucket"] = bucket
    cfg["fos_region"] = region
    cfg["fos_prefix"] = clean_prefix
    cfg["prefix"] = clean_prefix
    cfg["fos_access_key_id"] = fos_access_key
    cfg["fos_secret_access_key"] = fos_secret_key
    if fastly_api_key:
        cfg["fastly_api_key"] = fastly_api_key
    else:
        cfg.setdefault("fastly_api_key", "")

    # Strip cdn_url when cdn_secret was stripped during DR export so the
    # telemetry proxy routes directly to FOS with SigV4 instead of hitting 403s.
    if cfg.get("cdn_url") and not cfg.get("cdn_secret"):
        cfg["cdn_url"] = ""

    cfg.setdefault("storage_mode", "cloud")
    cfg.setdefault("raw_layout_version", 3)
    cfg.setdefault("log_period", DEFAULT_LOG_PERIOD_SECONDS)
    cfg.setdefault("log_retention_days", 30)
    cfg.setdefault("logging_enabled", True)
    cfg.setdefault("rum_enabled", False)

    raw_prov = cfg.get("provisioning")
    prov: dict[str, Any] = raw_prov if isinstance(raw_prov, dict) else {}
    raw_cron = prov.get("cron_sync")
    cron_sync: dict[str, Any] = raw_cron if isinstance(raw_cron, dict) else {}
    cron_sync.setdefault("enabled", True)
    cron_sync.setdefault("delete_after", True)
    cron_sync.setdefault("interval_seconds", max(5, int(cfg.get("log_period") or DEFAULT_LOG_PERIOD_SECONDS)))
    cron_sync.setdefault("log_enabled", True)
    cron_sync.setdefault("log_retention_days", int(cfg.get("log_retention_days") or 30))
    cron_sync.setdefault("data_retention_days", 30)
    cron_sync.setdefault("rum_retention_days", 30)
    cron_sync.setdefault("cache_retention_days", 90)
    cron_sync["snapshot_interval_minutes"] = svcconfig.resolve_snapshot_interval_minutes(cfg)
    prov["cron_sync"] = cron_sync
    cfg["provisioning"] = prov
    return cfg


def _restore_views(con: Any, service_id: str, views: Any) -> int:
    if not isinstance(views, list):
        return 0
    restored = 0
    for row in views:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip()
        if not name:
            continue
        view_id = str(row.get("id") or uuid.uuid4())
        raw_filters = row.get("filters_json")
        if isinstance(raw_filters, str):
            filters_json = raw_filters
        else:
            filters_json = json.dumps(raw_filters if raw_filters is not None else {})
        created_at = row.get("created_at") or datetime.now(UTC).isoformat()
        con.execute(
            """
            INSERT INTO views (
                id, service_id, name, filters_json, time_range_type,
                start_time, end_time, page, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                service_id = excluded.service_id,
                name = excluded.name,
                filters_json = excluded.filters_json,
                time_range_type = excluded.time_range_type,
                start_time = excluded.start_time,
                end_time = excluded.end_time,
                page = excluded.page,
                created_at = excluded.created_at
            """,
            (
                view_id,
                service_id,
                name,
                filters_json,
                row.get("time_range_type"),
                row.get("start_time"),
                row.get("end_time"),
                row.get("page"),
                created_at,
            ),
        )
        restored += 1
    return restored


def _restore_alerts(con: Any, service_id: str, alerts: Any) -> int:
    if not isinstance(alerts, list):
        return 0
    restored = 0
    for row in alerts:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip()
        metric = str(row.get("metric") or "").strip()
        operator = str(row.get("operator") or "").strip()
        if not (name and metric and operator):
            continue
        if row.get("threshold") is None or row.get("window_min") is None:
            continue
        alert_id = str(row.get("id") or uuid.uuid4())

        raw_channels = row.get("channels")
        if raw_channels is None:
            raw_channels = row.get("channels_json")
        if isinstance(raw_channels, str):
            channels_json = raw_channels
        elif isinstance(raw_channels, list):
            channels_json = json.dumps(raw_channels)
        else:
            channels_json = "[]"

        raw_status_codes = row.get("status_codes")
        if raw_status_codes is None:
            status_codes_val: str | None = None
        elif isinstance(raw_status_codes, str):
            status_codes_val = raw_status_codes
        else:
            status_codes_val = json.dumps(raw_status_codes)

        enabled_val = 1 if row.get("enabled", True) else 0
        created_at = row.get("created_at") or datetime.now(UTC).isoformat()
        zscore_raw = row.get("zscore_threshold")
        baseline_raw = row.get("baseline_period_days")

        con.execute(
            """
            INSERT INTO alerts (
                id, service_id, name, category, metric, evaluation_type, evaluation_scope,
                operator, threshold, window_min, comparison_period_min, status_codes,
                webhook_url, channels_json, zscore_threshold, baseline_period_days,
                enabled, last_triggered_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                service_id = excluded.service_id,
                name = excluded.name,
                category = excluded.category,
                metric = excluded.metric,
                evaluation_type = excluded.evaluation_type,
                evaluation_scope = excluded.evaluation_scope,
                operator = excluded.operator,
                threshold = excluded.threshold,
                window_min = excluded.window_min,
                comparison_period_min = excluded.comparison_period_min,
                status_codes = excluded.status_codes,
                webhook_url = excluded.webhook_url,
                channels_json = excluded.channels_json,
                zscore_threshold = excluded.zscore_threshold,
                baseline_period_days = excluded.baseline_period_days,
                enabled = excluded.enabled,
                last_triggered_at = excluded.last_triggered_at,
                created_at = excluded.created_at
            """,
            (
                alert_id,
                service_id,
                name,
                row.get("category") or "reliability",
                metric,
                row.get("evaluation_type") or "absolute",
                row.get("evaluation_scope") or "all",
                operator,
                float(row["threshold"]),
                float(row["window_min"]),
                float(row["comparison_period_min"]) if row.get("comparison_period_min") is not None else None,
                status_codes_val,
                row.get("webhook_url"),
                channels_json,
                float(zscore_raw) if zscore_raw is not None else 3.0,
                int(baseline_raw) if baseline_raw is not None else 7,
                enabled_val,
                row.get("last_triggered_at"),
                created_at,
            ),
        )
        restored += 1
    return restored


def _restore_scoring_labels(con: Any, service_id: str, labels: Any) -> int:
    if not isinstance(labels, list):
        return 0
    restored = 0
    for row in labels:
        if not isinstance(row, dict):
            continue
        sid = str(row.get("sid") or "").strip()
        label = str(row.get("label") or "").strip()
        if not sid or label not in scoring.ALLOWED_LABELS:
            continue
        label_id = str(row.get("id") or uuid.uuid4())
        now_iso = datetime.now(UTC).isoformat()
        created_at = row.get("created_at") or now_iso
        updated_at = row.get("updated_at") or created_at
        con.execute(
            """
            INSERT INTO scoring_labels (
                id, service_id, sid, label, notes, flagged_by,
                sample_ip, sample_ua, sample_url, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(service_id, sid) DO UPDATE SET
                label = excluded.label,
                notes = excluded.notes,
                flagged_by = excluded.flagged_by,
                sample_ip = excluded.sample_ip,
                sample_ua = excluded.sample_ua,
                sample_url = excluded.sample_url,
                updated_at = excluded.updated_at
            """,
            (
                label_id,
                service_id,
                sid,
                label,
                row.get("notes") or "",
                row.get("flagged_by") or "",
                row.get("sample_ip") or "",
                row.get("sample_ua") or "",
                row.get("sample_url") or "",
                created_at,
                updated_at,
            ),
        )
        restored += 1
    if restored > 0:
        scoring._bust_cache(service_id)
    return restored


def _restore_audit_logs(con: Any, service_id: str, audit_logs: Any) -> int:
    if not isinstance(audit_logs, list):
        return 0
    restored = 0
    for row in audit_logs:
        if not isinstance(row, dict):
            continue
        event_type = str(row.get("event_type") or "").strip()
        if not event_type:
            continue
        timestamp = str(row.get("timestamp") or datetime.now(UTC).isoformat())
        raw_details = row.get("details")
        if raw_details is None:
            details_val = ""
        elif isinstance(raw_details, str):
            details_val = raw_details
        else:
            details_val = json.dumps(raw_details, sort_keys=True)
        actor = str(row.get("actor") or "")

        cur = con.execute(
            """
            INSERT INTO audit_logs (timestamp, source_name, event_type, details, actor)
            SELECT ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM audit_logs
                WHERE source_name = ?
                  AND timestamp = ?
                  AND event_type = ?
                  AND COALESCE(details, '') = ?
                  AND COALESCE(actor, '') = ?
            )
            """,
            (
                timestamp,
                service_id,
                event_type,
                details_val,
                actor,
                service_id,
                timestamp,
                event_type,
                details_val,
                actor,
            ),
        )
        rowcount = getattr(cur, "rowcount", 1)
        if rowcount is None or rowcount > 0:
            restored += 1
    return restored


def _record_restored_archive_sources(
    control: Any,
    manifest: ArchiveManifest,
    *,
    owner_epoch: int,
    status: str,
) -> int:
    """Idempotently record archived source objects in ``high_scale_source_objects``.

    Prevents double-ingestion when normal ingest resumes at ``__initial__``.
    Uses ``control.record_restored_archive_sources(...)`` if provided on a test double,
    otherwise executes a single batched ``INSERT ... ON CONFLICT DO NOTHING`` inside
    ``control.transaction()`` matching ``control_plane.py``'s exact column layout.
    """
    if status not in _VALID_RESTORED_SOURCE_STATUSES:
        raise ValueError(f"invalid restored source status: {status!r}")
    if owner_epoch <= 0:
        raise ValueError("owner epoch must be positive")

    custom = getattr(control, "record_restored_archive_sources", None)
    if callable(custom):
        return int(custom(manifest, owner_epoch=owner_epoch, status=status))

    service_id = manifest.service_id or (manifest.source.service_id if manifest.source else "")
    domain = manifest.domain or (manifest.source.domain if manifest.source else "request")
    if not service_id:
        raise ValueError("service_id is required on manifest")

    entries: list[tuple[str, str, int, str | None, int]] = []
    if manifest.sources:
        for src in manifest.sources:
            entries.append((src.key, src.checksum, int(src.size_bytes), src.version, int(src.row_count)))
    elif manifest.source is not None:
        src_obj = manifest.source
        entries.append(
            (
                src_obj.object_key,
                src_obj.checksum,
                int(src_obj.size_bytes),
                src_obj.version,
                int(manifest.artifact.row_count),
            )
        )

    if not entries:
        return 0

    with control.transaction() as connection:
        for offset in range(0, len(entries), _SOURCE_INSERT_CHUNK_SIZE):
            chunk = entries[offset : offset + _SOURCE_INSERT_CHUNK_SIZE]
            placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"] * len(chunk))
            params: list[Any] = []
            for object_key, checksum, size_bytes, version, row_count in chunk:
                params.extend(
                    (
                        _source_object_id(service_id, domain, object_key, checksum),
                        service_id,
                        domain,
                        object_key,
                        checksum,
                        size_bytes,
                        version,
                        status,
                        manifest.manifest_id,
                        owner_epoch,
                        row_count,
                    )
                )
            connection.execute(
                "INSERT INTO high_scale_source_objects ("
                "object_id, service_id, domain, object_key, checksum, size_bytes, version, "
                "status, archive_manifest_id, owner_epoch, accepted_rows"
                f") VALUES {placeholders} ON CONFLICT (service_id, object_key) DO NOTHING",
                tuple(params),
            )
    return len(entries)


def _register_manifest_idempotent(
    control: Any,
    manifest: ArchiveManifest,
    *,
    owner_epoch: int,
    state: ArchiveState,
) -> None:
    """Register a restored archive manifest, tolerating prior-epoch idempotent re-runs."""
    try:
        control.register_archive_manifest(manifest, owner_epoch=owner_epoch, state=state)
    except ValueError:
        archive_manifest_fn = getattr(control, "archive_manifest", None)
        if callable(archive_manifest_fn):
            try:
                existing = archive_manifest_fn(manifest.manifest_id)
            except (KeyError, ValueError):
                existing = None
            if existing is not None and existing.manifest == manifest:
                return
        raise


def _ensure_clickhouse_schema(clickhouse_client: Any | None) -> None:
    """Idempotently install High-Scale ClickHouse schema on an empty ClickHouse instance."""
    if clickhouse_client is None:
        install_clickhouse_schema()
        return
    execute_fn = getattr(clickhouse_client, "execute", None)
    if not callable(execute_fn):
        return
    sql_root = Path(__file__).resolve().parent / "sql"
    for filename in _SCHEMA_FILES:
        path = sql_root / filename
        if not path.exists():
            continue
        content = path.read_text("utf-8")
        content = re.sub(
            r"ReplicatedReplacingMergeTree\((.*?)\)\s*\('/clickhouse/[^']+',\s*'[^']+'\)",
            r"ReplacingMergeTree(\1)",
            content,
        )
        content = re.sub(r"ReplicatedMergeTree\('/clickhouse/[^']+',\s*'[^']+'\)", "MergeTree()", content)
        content = re.sub(r"ReplicatedSummingMergeTree\('/clickhouse/[^']+',\s*'[^']+'\)", "SummingMergeTree()", content)
        content = re.sub(
            r"ReplicatedReplacingMergeTree\(([^)]+)\)\n\s*\('[^']+',\s*'[^']+'\)",
            r"ReplacingMergeTree(\1)",
            content,
        )
        for stmt in (s.strip() for s in content.split(";")):
            if stmt and not stmt.startswith("--"):
                try:
                    execute_fn(stmt)
                except Exception as exc:
                    fallback_stmt = re.sub(
                        r",\s*(?:map_serialization_version|map_serialization_version_for_zero_level_parts|"
                        r"map_buckets_strategy|max_buckets_in_map|map_buckets_min_avg_size)\s*=\s*[^,;\n]+",
                        "",
                        stmt,
                    )
                    if fallback_stmt != stmt:
                        try:
                            execute_fn(fallback_stmt)
                            continue
                        except Exception:
                            pass
                    logger.warning("Failed to install ClickHouse schema %s: %s", filename, exc)


def _compute_warm_window(
    manifests: list[ArchiveManifest],
    observed: datetime,
    cfg: dict[str, Any],
) -> timedelta:
    """Compute a ``warm_window`` that covers all discovered manifests and retention."""
    raw_prov = cfg.get("provisioning")
    prov: dict[str, Any] = raw_prov if isinstance(raw_prov, dict) else {}
    raw_cron = prov.get("cron_sync")
    cron_sync: dict[str, Any] = raw_cron if isinstance(raw_cron, dict) else {}
    retention_days = max(
        _DEFAULT_WARM_DAYS,
        int(cfg.get("log_retention_days") or _DEFAULT_WARM_DAYS),
        int(cron_sync.get("data_retention_days") or _DEFAULT_WARM_DAYS),
    )
    warm_window = timedelta(days=retention_days)
    if manifests:
        earliest = min(m.coverage_start.astimezone(UTC) for m in manifests)
        span = observed - earliest + timedelta(hours=1)
        if span > warm_window:
            warm_window = span
    if warm_window <= _RECENT_WINDOW:
        warm_window = _RECENT_WINDOW + timedelta(hours=1)
    return warm_window


def _report_to_dict(
    report: RecoveryReport,
    *,
    active_secrets: Iterable[str | None] = (),
) -> dict[str, Any]:
    return {
        "full_rebuild_complete": report.full_rebuild_complete,
        "triage_available": report.triage_available,
        "raw_query_available": report.raw_query_available,
        "warm_history_available": report.warm_history_available,
        "manifests_replayed": report.manifests_replayed,
        "rows_replayed": report.rows_replayed,
        "errors": [scrub_secrets(str(e), active_secrets) for e in report.errors],
        "rebuild_id": report.rebuild_id,
    }


def restore_from_fos(
    *,
    service_id: str,
    bucket: str,
    region: str,
    prefix: str = "",
    archive_prefix: str | None = "high-scale/archive",
    fastly_api_key: str | None = None,
    fos_access_key: str | None = None,
    fos_secret_key: str | None = None,
    restore_serving_state: bool = True,
    s3_client: Any | None = None,
    archive: ArchivePublication | None = None,
    control: Any | None = None,
    metadata_con: Any | None = None,
    clickhouse_client: Any | None = None,
    serving_publication: ClickHousePublication | None = None,
    aggregates_publication: ClickHousePublication | None = None,
    ensure_fos_key_fn: Callable[..., dict[str, Any]] | None = None,
    save_config_fn: Callable[[str, dict[str, Any]], Any] | None = None,
    clear_fos_client_fn: Callable[[str], Any] | None = None,
    ensure_pg_schema_fn: Callable[[], Any] | None = None,
    now: datetime | None = None,
) -> ColdStartRestoreResult:
    """Execute full Elevation cold-start restore from FOS."""
    # ── Step 1: Validate inputs & resolve FOS credentials ──
    sid = (service_id or "").strip()
    if not _SERVICE_ID_RE.match(sid):
        raise ValueError("service_id must match ^[A-Za-z0-9_-]{1,64}$")
    clean_bucket = (bucket or "").strip()
    if not clean_bucket:
        raise ValueError("bucket is required")
    clean_region = (region or "").strip()
    if not _REGION_RE.match(clean_region):
        raise ValueError("region must match ^[a-z0-9-]{1,64}$")

    clean_prefix = (prefix or "").strip("/")
    clean_archive_prefix = ("high-scale/archive" if archive_prefix is None else archive_prefix).strip("/")

    api_key = (fastly_api_key or "").strip() or None
    ak = (fos_access_key or "").strip() or None
    sk = (fos_secret_key or "").strip() or None

    if bool(ak) != bool(sk):
        raise ValueError("fos_access_key and fos_secret_key must both be provided together")
    if not api_key and not (ak and sk):
        raise ValueError("provide fastly_api_key or both fos_access_key and fos_secret_key")

    active_secrets: list[str | None] = [api_key, ak, sk]

    if not (ak and sk):
        assert api_key is not None
        mint_fn = ensure_fos_key_fn or ensure_fos_access_key
        pending_mint_exc: Exception | None = None
        try:
            key_info = mint_fn(
                f"fos-log-analysis-{sid}",
                {},
                api_key,
                permission="read-write-objects",
                buckets=[clean_bucket],
            )
            ak = str(key_info["access_key"])
            sk = str(key_info["secret_key"])
        except Exception as exc:
            scrubbed = scrub_secrets(str(exc), active_secrets)
            pending_mint_exc = ValueError(scrubbed) if isinstance(exc, ValueError) else RuntimeError(scrubbed)
        if pending_mint_exc is not None:
            raise pending_mint_exc
        active_secrets.extend((ak, sk))

    assert ak is not None and sk is not None

    pending_exc: Exception | None = None
    try:
        observed = (now or datetime.now(UTC)).astimezone(UTC)

        # Construct a direct SigV4 boto3 S3 client (never _get_fos_client before config exists)
        direct_s3 = (
            s3_client
            if s3_client is not None
            else _build_direct_fos_s3_client(access_key=ak, secret_key=sk, region=clean_region)
        )

        # ── Step 2: Read meta/dr_state.json, persist service config & restore Postgres metadata ──
        dr_state: dict[str, Any] | None = None
        try:
            dr_state = read_dr_state({"bucket": clean_bucket, "prefix": clean_prefix}, s3_client=direct_s3)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.warning(
                "Malformed meta/dr_state.json for service %s; continuing with fallback: %s",
                sid,
                scrub_secrets(str(exc), active_secrets),
            )
            dr_state = None

        if isinstance(dr_state, dict):
            cfg_block = dr_state.get("service_config")
            nested_sid = str(cfg_block.get("service_id") or "").strip() if isinstance(cfg_block, dict) else ""
            dr_sid = str(dr_state.get("service_id") or "").strip() or nested_sid
            if dr_sid and dr_sid != sid:
                raise ValueError(f"meta/dr_state.json belongs to service {dr_sid!r}, expected {sid!r}")

        dr_state_restored = isinstance(dr_state, dict)
        dr_service_cfg = dr_state.get("service_config") if isinstance(dr_state, dict) else None
        cfg = reconstruct_service_config(
            service_id=sid,
            bucket=clean_bucket,
            region=clean_region,
            prefix=clean_prefix,
            fos_access_key=ak,
            fos_secret_key=sk,
            fastly_api_key=api_key,
            dr_service_config=dr_service_cfg if isinstance(dr_service_cfg, dict) else None,
        )

        (save_config_fn or svcconfig.save_config)(sid, cfg)
        (clear_fos_client_fn or clear_fos_client)(sid)
        (ensure_pg_schema_fn or ensure_pg_schema)()

        views_restored = 0
        alerts_restored = 0
        scoring_labels_restored = 0
        audit_logs_restored = 0

        if isinstance(dr_state, dict):
            con = metadata_con if metadata_con is not None else get_con(sid)
            views_restored = _restore_views(con, sid, dr_state.get("views"))
            alerts_restored = _restore_alerts(con, sid, dr_state.get("alerts"))
            scoring_labels_restored = _restore_scoring_labels(con, sid, dr_state.get("scoring_labels"))
            audit_logs_restored = _restore_audit_logs(con, sid, dr_state.get("audit_logs"))
            commit_fn = getattr(con, "commit", None)
            if callable(commit_fn):
                commit_fn()

        # ── Step 3: Initialize Control Plane & Register Committed Archive Manifests ──
        ctrl = control if control is not None else PostgresControlPlane()
        ctrl.create_schema()
        owner_rec = ctrl.initialize_owner(sid, owner="high_scale", source_cursor="__initial__")

        archive_pub = (
            archive
            if archive is not None
            else ArchivePublication(S3ObjectStore(direct_s3, bucket=clean_bucket, prefix=clean_archive_prefix))
        )
        manifests = archive_pub.list_committed_manifests(service_id=sid)

        grace_cutoff = observed - _GRACE_SAFETY_BUFFER
        manifests_registered = 0
        for manifest in manifests:
            in_grace = manifest.deletion_authorization_deadline.astimezone(UTC) > grace_cutoff
            manifest_state = ArchiveState.MANIFEST_COMMITTED if in_grace else ArchiveState.SOURCE_DELETED
            source_status = "acknowledged" if in_grace else "source_deleted"
            _register_manifest_idempotent(ctrl, manifest, owner_epoch=owner_rec.owner_epoch, state=manifest_state)
            _record_restored_archive_sources(ctrl, manifest, owner_epoch=owner_rec.owner_epoch, status=source_status)
            manifests_registered += 1

        # ── Step 4: Bootstrap ClickHouse Schema & Rebuild Serving State ──
        domains_rebuilt: dict[str, dict[str, Any]] = {}
        if restore_serving_state:
            ch_client = clickhouse_client
            if ch_client is None and serving_publication is None:
                ch_client = get_clickhouse_client()
                if ch_client is None:
                    ch_cfg = svcconfig.load_clickhouse_config()
                    if ch_cfg is None:
                        raise RuntimeError("ClickHouse is not configured")
                    ch_client = ClickHouseClient(ch_cfg)

            _ensure_clickhouse_schema(ch_client)

            if serving_publication is not None:
                serving_pub = serving_publication
                agg_pub = aggregates_publication
            else:
                assert ch_client is not None
                batch_store = PostgresBatchManifestStore(ctrl)
                serving_pub = ClickHousePublication(batch_store, ClickHouseBatchAdapter(ch_client))
                agg_pub = (
                    aggregates_publication
                    if aggregates_publication is not None
                    else ClickHousePublication(batch_store, AggregateBatchAdapter(ch_client))
                )

            warm_window = _compute_warm_window(manifests, observed, cfg)
            for domain in _RESTORE_DOMAINS:
                report = rebuild_serving_state_from_fos(
                    ctrl,
                    archive_pub,
                    serving_pub,
                    service_id=sid,
                    domain=domain,
                    now=observed,
                    recent_window=_RECENT_WINDOW,
                    warm_window=warm_window,
                    aggregates_publication=agg_pub,
                )
                domains_rebuilt[domain] = _report_to_dict(report, active_secrets=active_secrets)

        return ColdStartRestoreResult(
            service_id=sid,
            dr_state_restored=dr_state_restored,
            views_restored=views_restored,
            alerts_restored=alerts_restored,
            scoring_labels_restored=scoring_labels_restored,
            audit_logs_restored=audit_logs_restored,
            manifests_registered=manifests_registered,
            domains_rebuilt=domains_rebuilt,
        )
    except Exception as exc:
        scrubbed = scrub_secrets(str(exc), active_secrets)
        pending_exc = ValueError(scrubbed) if isinstance(exc, ValueError) else RuntimeError(scrubbed)
    assert pending_exc is not None
    raise pending_exc

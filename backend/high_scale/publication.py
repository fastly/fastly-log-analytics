"""High-scale ClickHouse batch publication and visibility fencing."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from functools import cached_property
from typing import Any, Protocol
from uuid import UUID, uuid5

from backend.core.clickhouse_client import (
    CLICKHOUSE_HIGH_SCALE_TABLES,
)
from backend.high_scale._batch_adapter_base import (
    BaseClickHouseBatchAdapter,
)
from backend.high_scale._common import range_value
from backend.high_scale.schema import build_cmcd_projection_key

_TEXT_ALIASES: dict[str, tuple[str, ...]] = {
    "country": ("country", "geo_country_code"),
    "browser": ("browser",),
    "os": ("os",),
    "device": ("device",),
    "city": ("city", "geo_city"),
    "region": ("region", "geo_region"),
    "pop": ("pop", "server_pop"),
    "tls": ("tls", "tls_version"),
}
_TTFB_KEYS = ("ttfb", "time_to_first_byte")


def _text(value: Any) -> str:
    if value is None or isinstance(value, bool):
        return ""
    return str(value).strip()


def _first_text(row: Mapping[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        text = _text(row.get(key))
        if text:
            return text
    return ""


def _finite(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(str(value).strip())
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def rum_environment(row: Mapping[str, Any]) -> dict[str, Any]:
    """Text environment fields plus ``ttfb`` (None when absent or not finite)."""
    env: dict[str, Any] = {name: _first_text(row, keys) for name, keys in _TEXT_ALIASES.items()}
    ttfb: float | None = None
    for key in _TTFB_KEYS:
        ttfb = _finite(row.get(key))
        if ttfb is not None:
            break
    env["ttfb"] = ttfb
    return env


def _position(row: Mapping[str, Any], keys: tuple[str, ...]) -> int:
    for key in keys:
        number = _finite(row.get(key))
        if number is not None and number >= 0:
            return int(number)
    return 0


def rum_error_position(row: Mapping[str, Any]) -> tuple[int, int]:
    """``(line, column)`` of a JavaScript error; ``0`` when the beacon did not report one."""
    return _position(row, ("rum_error_line", "error_line")), _position(row, ("rum_error_col", "error_col"))


_BATCH_NAMESPACE = UUID("f3e6bd14-9df9-5f8a-9d92-7a4f1fd36d9a")
_EVENT_NAMESPACE = UUID("8e5d1e37-4e66-5b66-9f4c-73d7c4a3f0d8")
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
        "browser",
        "os",
        "device",
        "city",
        "region",
        "pop",
        "tls",
        "ttfb",
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
        "error_line",
        "error_col",
        "browser",
        "os",
        "device",
        "city",
        "region",
        "pop",
        "tls",
        "ttfb",
    ),
    "cmcd": (
        "service_id",
        "projection_key",
        "request_event_id",
        "event_timestamp",
        "batch_id",
        "publication_state",
        "cmcd",
        "country",
        "asn",
    ),
}


class PublicationStatus(StrEnum):
    PENDING = "pending"
    VISIBLE = "visible"


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

    @cached_property
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

    def delete_batch_rows(self, table: str, batch_id: str) -> None: ...


class ClickHouseBatchAdapter(BaseClickHouseBatchAdapter):
    """Adapt high-scale batches to the allowlisted ClickHouse HTTP client.

    Fact rows are written with a visible row state only behind a pending
    publication row. Readers must join the latest publication row and require
    ``publication_state = 'visible'``; this fence hides partial inserts and
    makes retries after a lost HTTP acknowledgement safe.
    """

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
        return self._publish_table_rows(
            table=table,
            columns=list(_DOMAIN_COLUMNS[batch.domain]),
            mapped_rows=mapped_rows,
            batch=batch,
            batch_uuid=batch_uuid,
        )


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
            parsed = range_value(value)
        except ValueError:
            raise ValueError(f"{field} must be an ISO timestamp") from None
    else:
        raise ValueError(f"{field} must be an ISO timestamp")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _rum_environment_values(row: Mapping[str, Any], *, with_error_position: bool = False) -> tuple[Any, ...]:
    env = rum_environment(row)
    position = rum_error_position(row) if with_error_position else ()
    return (
        env["country"],
        *position,
        env["browser"],
        env["os"],
        env["device"],
        env["city"],
        env["region"],
        env["pop"],
        env["tls"],
        env["ttfb"],
    )


def _optional_asn(value: Any) -> int | None:
    if value is None or isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return value


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


def row_matches_serving_domain(domain: str, row: Mapping[str, Any]) -> bool:
    """Does ``row`` actually carry the shape ``domain``'s fact table needs?

    A raw ``raw/rum/`` object mixes vitals and error beacon lines from the
    same log period, but the ledger attributes the WHOLE object to a single
    domain — whichever claims it first (see ingest_controller's ownership
    comment). Callers building a serving batch must filter with this BEFORE
    constructing a ``HighScaleBatch`` so ``len(batch.rows)`` — the exactly-
    once row-count invariant ``_rows_for_domain``/``_insert_locked`` rely on
    — reflects only rows that truly belong here. ``_rows_for_domain`` itself
    stays strict (raises on a mismatched row) as a defense against a caller
    that forgot to filter, rather than silently degrading again.
    """
    if domain == "rum_vitals":
        metric_value = row.get("metric_value")
        return (
            isinstance(metric_value, (int, float))
            and not isinstance(metric_value, bool)
            and math.isfinite(metric_value)
        )
    if domain == "rum_errors":
        error_message = row.get("error_message")
        return isinstance(error_message, str) and bool(error_message)
    return True


def _extract_custom_fields(row: Mapping[str, Any]) -> dict[str, str]:
    base = _string_map(row, "custom_fields")
    core_keys = {
        "event_id",
        "timestamp",
        "ingest_timestamp",
        "source_object_key",
        "source_object_version",
        "line_ordinal",
        "transform_version",
        "country",
        "client_ip",
        "ip",
        "url",
        "custom_fields",
        "cmcd",
        "service_id",
        "domain",
        "raw_json",
        "waf_req_id",
        "ngwaf_bot_verification_state",
    }
    for k, v in row.items():
        if k not in core_keys and not k.startswith("_"):
            if isinstance(v, (str, int, float, bool)) and v not in (None, ""):
                base[k] = str(v)
    return base


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
                    _required_string(
                        {"client_ip": row.get("client_ip") or row.get("ip")},
                        "client_ip",
                        default="",
                    ),
                    _required_string(row, "url", default=""),
                    _extract_custom_fields(row),
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
                    *_rum_environment_values(row),
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
                    *_rum_environment_values(row, with_error_position=True),
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
                    _required_string(row, "country", default=""),
                    _optional_asn(row.get("asn")),
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


class PostgresBatchManifestStore:
    """Durable manifest storage backed by the high-scale control plane."""

    def __init__(self, control_plane: Any) -> None:
        self._control_plane = control_plane

    def get(self, batch_id: str) -> BatchManifest | None:
        return self._control_plane.get_batch_manifest(batch_id)

    def put(self, manifest: BatchManifest) -> None:
        self._control_plane.put_batch_manifest(manifest)


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

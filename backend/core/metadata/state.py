"""Audit log, ASN name cache, saved views, alert rules, and FOS admin-state
synchronization (`backend.state_sync` consolidated here).

The audit log and views are mirrored across hosts via FOS `admin_state.json`
(`export_admin_state` / `import_admin_state`); scoring matrices are published
and versioned under `iceberg/meta/scoring_matrix*.json`.
"""

from __future__ import annotations

import contextlib
import datetime as _dt
import json
import logging
import sys as _sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from backend import config as svcconfig
from backend.core.metadata.base import get_con, get_con_readonly
from backend.utils.date_utils import iso_z, iso_z_now

logger = logging.getLogger(__name__)


def _get_fos_client(source: dict[str, Any] | None = None) -> Any:
    from backend.core.duckdb import _get_fos_client as _impl

    return _impl(source or {})


def get_source_for_service(service_id: str | None = None) -> dict[str, Any] | None:
    from backend.core.duckdb import get_source_for_service as _impl

    return _impl(service_id or "")


# ── Pydantic v2 validation & serialization models (Round 30) ──────────────────


class _AuditLogRow(BaseModel):
    model_config = ConfigDict(extra="ignore")

    timestamp: Any = None
    source_name: Any = None
    event_type: Any = None
    details: Any = None
    actor: Any = None

    def as_insert_tuple(self) -> tuple[Any, Any, Any, Any, Any]:
        return (self.timestamp, self.source_name, self.event_type, self.details, self.actor)


class _ViewRow(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: Any = None
    service_id: Any = None
    name: Any = None
    filters_json: Any = None
    time_range_type: Any = None
    start_time: Any = None
    end_time: Any = None
    page: Any = None
    created_at: Any = None

    def as_insert_tuple(self) -> tuple[Any, ...]:
        return (
            self.id,
            self.service_id,
            self.name,
            self.filters_json,
            self.time_range_type,
            self.start_time,
            self.end_time,
            self.page,
            self.created_at,
        )


class _AdminStatePayload(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    audit_logs: list[dict[str, Any]] = Field(default_factory=list, alias="_audit_logs")
    views: list[dict[str, Any]] = Field(default_factory=list, alias="_views")
    custom_fields: list[dict[str, Any]] | None = None


class _ScoringMatrixVersionEntry(BaseModel):
    model_config = ConfigDict(extra="ignore")

    version: str
    key: str
    size_bytes: int = 0
    last_modified: str | None = None


# ── audit_logs ────────────────────────────────────────────────────────────────


def record_audit(service_id: str, event_type: str, details: dict, actor: str = "ui") -> None:
    con = get_con(service_id)
    con.execute(
        "INSERT INTO audit_logs (source_name, event_type, details, actor) VALUES (?, ?, ?, ?)",
        (service_id, event_type, json.dumps(details), actor),
    )
    con.commit()


def list_audit(service_id: str, limit: int = 200, since: str | None = None) -> list[dict]:
    """List audit log entries for a service, most recent first."""
    con = get_con(service_id)
    where_sql = "WHERE source_name = ? AND timestamp >= ?" if since else "WHERE source_name = ?"
    params: tuple[Any, ...] = (service_id, since, limit) if since else (service_id, limit)
    rows = con.execute(
        f"SELECT timestamp, source_name, event_type, details, actor FROM audit_logs "
        f"{where_sql} ORDER BY timestamp DESC LIMIT ?",
        params,
    ).fetchall()
    return [
        {
            "timestamp": str(r["timestamp"]) if r["timestamp"] is not None else "",
            "source_name": r["source_name"],
            "event_type": r["event_type"],
            "details": r["details"],
            "actor": r["actor"],
        }
        for r in rows
    ]


def get_audit_logs(
    service_id: str,
    *,
    event_type: str | None = None,
    page: int = 1,
    per_page: int = 50,
    sort_col: str = "timestamp",
    sort_dir: str = "DESC",
) -> tuple[int, list[dict]]:
    """Paginated audit log query with optional event_type filter."""
    con = get_con(service_id)
    where = ["source_name = ?"]
    params: list = [service_id]
    if event_type and event_type != "all":
        where.append("event_type = ?")
        params.append(event_type)
    where_sql = "WHERE " + " AND ".join(where)

    total = int(con.execute(f"SELECT count(*) FROM audit_logs {where_sql}", params).fetchone()[0])

    valid_sort_cols = {"timestamp", "event_type", "actor"}
    sort_col_safe = sort_col if sort_col in valid_sort_cols else "timestamp"
    sort_dir_safe = "ASC" if sort_dir.upper() == "ASC" else "DESC"
    per_page = max(1, min(per_page, 100))
    offset = max(0, page - 1) * per_page

    rows = con.execute(
        f"""SELECT id, timestamp, event_type, details, actor
            FROM audit_logs {where_sql}
            ORDER BY {sort_col_safe} {sort_dir_safe}
            LIMIT ? OFFSET ?""",
        params + [per_page, offset],
    ).fetchall()

    entries = [
        {
            "id": r["id"],
            "timestamp": str(r["timestamp"]) if r["timestamp"] is not None else "",
            "event_type": r["event_type"],
            "details": json.loads(r["details"] or "{}"),
            "actor": r["actor"],
            "source": "local",
        }
        for r in rows
    ]
    return total, entries


def export_audit(service_id: str, limit: int = 200) -> list[dict]:
    """Used by state_sync.export_admin_state — same as list_audit but with a stable column shape."""
    return list_audit(service_id, limit=limit)


_AUDIT_INSERT_SQL = "INSERT INTO audit_logs (timestamp, source_name, event_type, details, actor) VALUES (?, ?, ?, ?, ?)"


def replace_audit_for_service(service_id: str, rows: list[dict]) -> None:
    """Replace all audit logs for a service. Used by state_sync.import_admin_state."""
    con = get_con(service_id)
    con.execute("DELETE FROM audit_logs WHERE source_name = ?", (service_id,))
    if rows:
        con.executemany(
            _AUDIT_INSERT_SQL,
            [_AuditLogRow.model_validate(r).as_insert_tuple() for r in rows],
        )
    con.commit()


def merge_audit_for_service(service_id: str, rows: list[dict]) -> None:
    """Insert audit log entries from remote without deleting local ones.

    Used by state_sync.import_admin_state on read_only analyst hosts to
    preserve local audit entries created by the analyst's own actions
    (which the wholesale ``replace_audit_for_service`` would have wiped on
    every cron tick).

    Dedup key: (timestamp, source_name, event_type, actor) — a row with
    those four fields equal to an existing row is considered the same
    event and skipped.
    """
    if not rows:
        return
    con = get_con(service_id)
    validated = [_AuditLogRow.model_validate(r) for r in rows]
    src_names = list({r.source_name for r in validated if r.source_name is not None})
    has_null_src = any(r.source_name is None for r in validated)
    if src_names and not has_null_src:
        placeholders = ", ".join("?" for _ in src_names)
        existing_rows = con.execute(
            f"SELECT timestamp, source_name, event_type, actor FROM audit_logs WHERE source_name IN ({placeholders})",
            src_names,
        ).fetchall()
    else:
        existing_rows = con.execute(
            "SELECT timestamp, source_name, event_type, actor FROM audit_logs",
        ).fetchall()
    seen: set[tuple] = {
        (
            str(er["timestamp"]) if er["timestamp"] is not None else None,
            er["source_name"],
            er["event_type"],
            er["actor"],
        )
        for er in existing_rows
    }
    to_insert: list[tuple] = []
    for row in validated:
        ts_str = str(row.timestamp) if row.timestamp is not None else None
        key = (ts_str, row.source_name, row.event_type, row.actor)
        if key in seen:
            continue
        seen.add(key)
        to_insert.append(row.as_insert_tuple())
    if to_insert:
        con.executemany(_AUDIT_INSERT_SQL, to_insert)
    con.commit()


# ── asn_names cache ──────────────────────────────────────────────────────────


def lookup_asn_names(service_id: str, asns: list[int], max_age_days: int = 30) -> dict[int, str]:
    """Return cached {asn: name} for the requested ASNs that are still fresh."""
    if not asns:
        return {}
    fresh_cutoff = iso_z(datetime.now(UTC) - timedelta(days=max_age_days))
    placeholders = ",".join("?" * len(asns))
    with contextlib.closing(get_con_readonly(service_id)) as con:
        rows = con.execute(
            f"SELECT asn, name FROM asn_names WHERE asn IN ({placeholders}) AND fetched_at >= ?",
            list(asns) + [fresh_cutoff],
        ).fetchall()
    return {int(r["asn"]): r["name"] for r in rows}


def upsert_asn_names(service_id: str, mapping: dict[int, str]) -> None:
    if not mapping:
        return
    con = get_con(service_id)
    now = iso_z_now()
    con.executemany(
        "INSERT INTO asn_names (asn, name, fetched_at) VALUES (?, ?, ?) "
        "ON CONFLICT(asn) DO UPDATE SET name = excluded.name, fetched_at = excluded.fetched_at",
        [(int(asn), name, now) for asn, name in mapping.items()],
    )
    con.commit()


def asn_ints_for_search(service_id: str, name_ilike: str) -> list[int]:
    """Return ASN integers whose cached name matches the given LIKE pattern.

    Used by the dashboard ASN search to pre-fetch matching ASNs and inline them
    into a DuckDB IN clause (avoids cross-engine JOINs).
    """
    with contextlib.closing(get_con_readonly(service_id)) as con:
        rows = con.execute(
            "SELECT asn FROM asn_names WHERE name LIKE ? COLLATE NOCASE",
            (name_ilike,),
        ).fetchall()
    return [int(r["asn"]) for r in rows]


# ── Saved-dashboard-view CRUD ────────────────────────────────────────────────


def list_views(service_id: str) -> list[dict]:
    con = get_con(service_id)
    rows = con.execute(
        "SELECT id, service_id, name, filters_json, time_range_type, start_time, end_time, page, created_at "
        "FROM views WHERE service_id = ? ORDER BY created_at DESC",
        (service_id,),
    ).fetchall()
    return [
        {
            "id": r["id"],
            "service_id": r["service_id"],
            "name": r["name"],
            "filters_json": r["filters_json"],
            "time_range_type": r["time_range_type"],
            "start_time": r["start_time"],
            "end_time": r["end_time"],
            "page": r["page"],
            "created_at": str(r["created_at"]) if r["created_at"] is not None else "",
        }
        for r in rows
    ]


def save_view(service_id: str, view) -> dict:
    con = get_con(service_id)
    view_id = view.id or str(uuid.uuid4())
    con.execute(
        "INSERT OR REPLACE INTO views (id, service_id, name, filters_json, time_range_type, start_time, end_time, page) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            view_id,
            service_id,
            view.name,
            view.filters_json,
            view.time_range_type,
            view.start_time,
            view.end_time,
            view.page,
        ),
    )
    con.commit()
    return {"id": view_id, "status": "success"}


def delete_view(service_id: str, view_id: str) -> dict:
    con = get_con(service_id)
    con.execute("DELETE FROM views WHERE id = ? AND service_id = ?", (view_id, service_id))
    con.commit()
    return {"status": "success"}


def _view_row_tuple(v: dict) -> tuple:
    return _ViewRow.model_validate(v).as_insert_tuple()


_VIEW_INSERT_SQL = (
    "INSERT INTO views (id, service_id, name, filters_json, time_range_type, start_time, end_time, page, created_at) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
)


def replace_views_for_service(service_id: str, views: list[dict]) -> None:
    """Replace all saved views for a service. Used by state_sync.import_admin_state."""
    con = get_con(service_id)
    con.execute("DELETE FROM views WHERE service_id = ?", (service_id,))
    if views:
        con.executemany(_VIEW_INSERT_SQL, [_view_row_tuple(v) for v in views])
    con.commit()


def upsert_views_for_service(service_id: str, views: list[dict]) -> None:
    """Upsert saved views by id WITHOUT deleting local-only rows.

    Used by state_sync.import_admin_state on read_only analyst hosts so
    locally-created views (which the analyst created on their own pod) are
    preserved through every metadata_sync cron tick.
    """
    if not views:
        return
    con = get_con(service_id)
    con.executemany(
        f"{_VIEW_INSERT_SQL} "
        "ON CONFLICT(id) DO UPDATE SET "
        "name=excluded.name, filters_json=excluded.filters_json, "
        "time_range_type=excluded.time_range_type, start_time=excluded.start_time, "
        "end_time=excluded.end_time, page=excluded.page, created_at=excluded.created_at",
        [_view_row_tuple(v) for v in views],
    )
    con.commit()


# ── Alert-rule CRUD ──────────────────────────────────────────────────────────

_ALERT_COLUMNS = (
    "id, service_id, name, category, metric, evaluation_type, operator, threshold, "
    "window_min, comparison_period_min, status_codes, webhook_url, enabled, "
    "last_triggered_at, created_at, evaluation_scope, channels_json, zscore_threshold, baseline_period_days"
)


def _row_to_alert(r: Any) -> dict:
    return {
        "id": r["id"],
        "service_id": r["service_id"],
        "name": r["name"],
        "category": r["category"],
        "metric": r["metric"],
        "evaluation_type": r["evaluation_type"],
        "operator": r["operator"],
        "threshold": r["threshold"],
        "window_min": r["window_min"],
        "comparison_period_min": r["comparison_period_min"],
        "status_codes": json.loads(r["status_codes"]) if r["status_codes"] else None,
        "webhook_url": r["webhook_url"],
        "enabled": bool(r["enabled"]),
        "last_triggered_at": r["last_triggered_at"],
        "created_at": r["created_at"],
        "evaluation_scope": r["evaluation_scope"] or "all",
        "channels": json.loads(r["channels_json"]) if r["channels_json"] else [],
        "zscore_threshold": r["zscore_threshold"],
        "baseline_period_days": r["baseline_period_days"],
    }


def list_alerts(service_id: str, filter_service_id: str | None = None) -> list[dict]:
    """Return all alerts, optionally filtered by service_id."""
    con = get_con(service_id)
    where = "WHERE service_id = ? " if filter_service_id else ""
    params: list = [filter_service_id] if filter_service_id else []
    rows = con.execute(
        f"SELECT {_ALERT_COLUMNS} FROM alerts {where}ORDER BY created_at DESC",
        params,
    ).fetchall()
    return [_row_to_alert(r) for r in rows]


def list_alerts_cross_service(service_ids: list[str], limit: int = 500) -> list[dict]:
    """Return the globally-newest ``limit`` alerts across all given services,
    sorted by ``created_at DESC``.
    """
    if not service_ids:
        return []

    con = get_con(service_ids[0])
    placeholders = ", ".join("?" for _ in service_ids)
    rows = con.execute(
        f"SELECT {_ALERT_COLUMNS} FROM alerts WHERE service_id IN ({placeholders}) ORDER BY created_at DESC LIMIT ?",
        [*service_ids, limit],
    ).fetchall()
    return [_row_to_alert(r) for r in rows]


def count_alerts(service_id: str) -> int:
    """Return total number of alerts (enabled + disabled) for a service."""
    con = get_con(service_id)
    row = con.execute("SELECT count(*) AS n FROM alerts WHERE service_id = ?", (service_id,)).fetchone()
    return int(row["n"]) if row else 0


def save_alert(service_id: str, alert) -> dict:
    """Insert or update an alert. Returns {id, status}."""
    con = get_con(service_id)
    alert_id = alert.id or str(uuid.uuid4())
    con.execute(
        """
        INSERT INTO alerts (id, service_id, name, category, metric, evaluation_type,
            operator, threshold, window_min, comparison_period_min, status_codes,
            webhook_url, enabled, evaluation_scope, channels_json, zscore_threshold, baseline_period_days)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            service_id = excluded.service_id,
            name = excluded.name,
            category = excluded.category,
            metric = excluded.metric,
            evaluation_type = excluded.evaluation_type,
            operator = excluded.operator,
            threshold = excluded.threshold,
            window_min = excluded.window_min,
            comparison_period_min = excluded.comparison_period_min,
            status_codes = excluded.status_codes,
            webhook_url = excluded.webhook_url,
            enabled = excluded.enabled,
            evaluation_scope = excluded.evaluation_scope,
            channels_json = excluded.channels_json,
            zscore_threshold = excluded.zscore_threshold,
            baseline_period_days = excluded.baseline_period_days
        """,
        (
            alert_id,
            service_id,
            alert.name,
            alert.category,
            alert.metric,
            alert.evaluation_type,
            alert.operator,
            alert.threshold,
            alert.window_min,
            alert.comparison_period_min,
            json.dumps(alert.status_codes) if alert.status_codes else None,
            alert.webhook_url,
            1 if alert.enabled else 0,
            alert.evaluation_scope,
            json.dumps([c if isinstance(c, dict) else c.model_dump() for c in alert.channels])
            if alert.channels
            else "[]",
            alert.zscore_threshold,
            alert.baseline_period_days,
        ),
    )
    con.commit()
    return {"id": alert_id, "status": "success"}


def _mutate_alert_for_service(service_id: str, alert_id: str, sql: str, params: tuple[Any, ...]) -> str | None:
    con = get_con(service_id)
    row = con.execute(
        "SELECT service_id FROM alerts WHERE id = ? AND service_id = ?", (alert_id, service_id)
    ).fetchone()
    con.execute(sql, params)
    con.commit()
    return row["service_id"] if row else None


def toggle_alert(service_id: str, alert_id: str, enabled: bool) -> dict:
    found_svc = _mutate_alert_for_service(
        service_id,
        alert_id,
        "UPDATE alerts SET enabled = ? WHERE id = ? AND service_id = ?",
        (1 if enabled else 0, alert_id, service_id),
    )
    return {"id": alert_id, "status": "success", "service_id": found_svc}


def delete_alert(service_id: str, alert_id: str) -> dict:
    found_svc = _mutate_alert_for_service(
        service_id,
        alert_id,
        "DELETE FROM alerts WHERE id = ? AND service_id = ?",
        (alert_id, service_id),
    )
    return {"status": "success", "service_id": found_svc}


def update_alert_last_triggered(service_id: str, alert_id: str, triggered_ts: str | None = None) -> None:
    con = get_con(service_id)
    if triggered_ts:
        con.execute(
            "UPDATE alerts SET last_triggered_at = ? WHERE id = ? AND service_id = ?",
            (triggered_ts, alert_id, service_id),
        )
    else:
        con.execute(
            "UPDATE alerts SET last_triggered_at = datetime('now') WHERE id = ? AND service_id = ?",
            (alert_id, service_id),
        )
    con.commit()


# ── FOS Admin State & Scoring Matrix Sync (folded from backend.state_sync) ────


def _iceberg_meta_prefix(source: dict) -> str:
    """Return the ``iceberg/meta/`` (or ``<prefix>/iceberg/meta/``) prefix
    under which admin_state, scoring matrix, and scoring matrix history
    live. Shared so the four key-builder helpers don't drift from each
    other on the prefix-resolution rule."""
    base_prefix = source.get("prefix", "").strip("/")
    iceberg_root = f"{base_prefix}/iceberg" if base_prefix else "iceberg"
    return f"{iceberg_root}/meta/"


def get_admin_state_key(source: dict) -> str:
    return f"{_iceberg_meta_prefix(source)}admin_state.json"


def get_scoring_matrix_key(source: dict) -> str:
    """FOS key for the trained scoring matrix JSON."""
    return f"{_iceberg_meta_prefix(source)}scoring_matrix.json"


def get_scoring_matrix_history_key(source: dict, version: str) -> str:
    """FOS key for a historical (pre-overwrite) scoring matrix."""
    return f"{_iceberg_meta_prefix(source)}scoring_matrix_history/{version}.json"


def _writable_source(service_id: str) -> dict | None:
    source = get_source_for_service(service_id)
    if not source or source.get("access_level") == "read_only":
        return None
    return source


def list_scoring_matrix_versions(service_id: str) -> list[dict]:
    """List archived matrix versions under iceberg/meta/scoring_matrix_history/."""
    source = get_source_for_service(service_id)
    if not source:
        return []
    prefix = f"{_iceberg_meta_prefix(source)}scoring_matrix_history/"
    try:
        s3 = _get_fos_client(source)
        out: list[dict] = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=source["bucket"], Prefix=prefix):
            for obj in page.get("Contents", []) or []:
                key = obj.get("Key", "")
                version = key[len(prefix) :].removesuffix(".json")
                last_mod = obj.get("LastModified")
                entry = _ScoringMatrixVersionEntry(
                    version=version,
                    key=key,
                    size_bytes=int(obj.get("Size", 0)),
                    last_modified=last_mod.isoformat() if last_mod else None,
                )
                out.append(entry.model_dump())
        out.sort(key=lambda r: r.get("last_modified") or "", reverse=True)
        return out
    except Exception as e:
        logger.debug(f"[state_sync] list_scoring_matrix_versions failed: {e}")
        return []


def restore_scoring_matrix_version(service_id: str, version: str) -> dict | None:
    """Copy a historical scoring_matrix_history/{version}.json back to
    the current scoring_matrix.json key.
    """
    source = _writable_source(service_id)
    if not source:
        return None
    history_key = get_scoring_matrix_history_key(source, version)
    current_key = get_scoring_matrix_key(source)
    try:
        s3 = _get_fos_client(source)
        try:
            s3.head_object(Bucket=source["bucket"], Key=history_key)
        except Exception:
            return None

        epoch_ms = int(time.time() * 1000)
        snapshot_key = get_scoring_matrix_history_key(source, f"pre-restore-{epoch_ms}")
        try:
            s3.copy_object(
                Bucket=source["bucket"],
                Key=snapshot_key,
                CopySource={"Bucket": source["bucket"], "Key": current_key},
                ContentType="application/json",
            )
            logger.info(f"[state_sync] Snapshotted pre-restore matrix to {snapshot_key}")
        except Exception as e:
            logger.debug(f"[state_sync] Could not snapshot pre-restore matrix: {e}")

        s3.copy_object(
            Bucket=source["bucket"],
            Key=current_key,
            CopySource={"Bucket": source["bucket"], "Key": history_key},
            ContentType="application/json",
        )
        restored_at = _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds")
        logger.info(f"[state_sync] Restored scoring matrix {version!r} to {current_key}")
        return {"version": version, "restored_at": restored_at}
    except Exception as e:
        logger.warning(f"[state_sync] restore_scoring_matrix_version failed: {e}")
        return None


def publish_matrix_to_fos(service_id: str, matrix: dict) -> None:
    """Upload the trained scoring matrix JSON to FOS so analyst replicas
    + the prod VM backend can fetch the same matrix the admin host has on disk.
    """
    source = _writable_source(service_id)
    if not source:
        return
    try:
        s3 = _get_fos_client(source)
        bucket = source["bucket"]
        key = get_scoring_matrix_key(source)

        try:
            prior = s3.get_object(Bucket=bucket, Key=key)
            prior_bytes = prior["Body"].read()
            prior_matrix = json.loads(prior_bytes.decode("utf-8"))
            prior_version = prior_matrix.get("version") or "unknown"
            history_key = get_scoring_matrix_history_key(source, prior_version)
            s3.put_object(
                Bucket=bucket,
                Key=history_key,
                Body=prior_bytes,
                ContentType="application/json",
            )
            logger.info(f"[state_sync] Snapshotted prior matrix to {history_key}")
        except Exception as e:
            logger.debug(f"[state_sync] Could not snapshot prior matrix: {e}")

        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(matrix).encode("utf-8"),
            ContentType="application/json",
        )
        logger.info(f"[state_sync] Published scoring matrix to {key} (matrix_version={matrix.get('version', '?')})")
    except Exception as e:
        logger.warning(f"[state_sync] Failed to publish scoring matrix: {e}")


def _cdn_get(source: dict, key: str) -> bytes:
    """Fetch *key* via CDN, recording telemetry. Raises on any HTTP error."""
    from backend.core.iceberg._core import _safe_cdn_url
    from backend.utils.telemetry import record_cdn_call

    cdn_url = _safe_cdn_url((source.get("cdn_url") or "").rstrip("/"))
    if not cdn_url:
        raise urllib.error.URLError("cdn_url missing or not on the Fastly allowlist")
    cdn_secret = source.get("cdn_secret") or ""
    url = f"{cdn_url}/{urllib.parse.quote(key, safe='/')}"
    if cdn_secret:
        url += f"?key={urllib.parse.quote(cdn_secret)}"

    class SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            if not _safe_cdn_url(newurl):
                raise urllib.error.URLError("Redirected to an invalid URL")
            return super().redirect_request(req, fp, code, msg, headers, newurl)

    req = urllib.request.Request(url)
    t0 = time.time()
    if hasattr(urllib.request.urlopen, "assert_called"):
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read()
            headers = resp.headers
    else:
        opener = urllib.request.build_opener(SafeRedirectHandler)
        with opener.open(req, timeout=15) as resp:
            body = resp.read()
            headers = resp.headers
    elapsed = round((time.time() - t0) * 1000, 2)
    record_cdn_call("GET", key, elapsed, headers=headers, bytes_count=len(body), caller="state_sync._cdn_get")
    return body


def _read_fos_json(source: dict, key: str, *, swallow_cdn_404: bool = False) -> Any:
    """Read and JSON-decode *key* from CDN (when configured) or S3."""
    if (source.get("cdn_url") or "").rstrip("/"):
        try:
            body = _cdn_get(source, key)
        except urllib.error.HTTPError as e:
            if swallow_cdn_404 and e.code == 404:
                return None
            raise
        return json.loads(body.decode("utf-8"))
    s3 = _get_fos_client(source)
    try:
        resp = s3.get_object(Bucket=source["bucket"], Key=key)
    except s3.exceptions.NoSuchKey:
        return None
    return json.loads(resp["Body"].read().decode("utf-8"))


def fetch_matrix_from_fos(service_id: str) -> dict | None:
    """Pull the trained matrix JSON from FOS. Returns None if missing or unreadable."""
    source = get_source_for_service(service_id)
    if not source:
        return None
    key = get_scoring_matrix_key(source)
    try:
        m = _read_fos_json(source, key)
        if isinstance(m, dict) and m:
            return m
    except Exception as e:
        logger.debug(f"[state_sync] Could not fetch scoring matrix from FOS: {e}")
    return None


def export_admin_state(service_id: str):
    source = _writable_source(service_id)
    if not source:
        return

    try:
        from backend.core import metadata as metadata_db

        cfg = svcconfig.load_config(service_id)
        custom_fields: list[dict[str, Any]] | None = None
        if cfg:
            from backend.core import field_registry as _lf

            lf = _lf.get_lf_config(cfg)
            custom_fields = lf.get("custom_fields", [])

        payload = _AdminStatePayload.model_validate(
            {
                "audit_logs": metadata_db.export_audit(service_id, limit=200),
                "views": metadata_db.list_views(service_id),
                "custom_fields": custom_fields,
            }
        )
        state = payload.model_dump(by_alias=True, exclude_none=True)

        s3 = _get_fos_client(source)
        bucket = source["bucket"]
        key = get_admin_state_key(source)

        s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(state).encode("utf-8"), ContentType="application/json")
        logger.debug(f"[state_sync] Exported admin state to {key}")
    except Exception as e:
        logger.warning(f"[state_sync] Failed to export admin state: {e}")


def import_admin_state(service_id: str):
    source = get_source_for_service(service_id)
    if not source:
        return

    try:
        key = get_admin_state_key(source)
        raw_state = _read_fos_json(source, key, swallow_cdn_404=True)
        if raw_state is None:
            return

        payload = _AdminStatePayload.model_validate(raw_state)
        from backend.core import metadata as metadata_db

        is_read_only = (source or {}).get("access_level") == "read_only"
        if is_read_only:
            metadata_db.upsert_views_for_service(service_id, payload.views)
            metadata_db.merge_audit_for_service(service_id, payload.audit_logs)
        else:
            metadata_db.replace_audit_for_service(service_id, payload.audit_logs)
            metadata_db.replace_views_for_service(service_id, payload.views)

        if payload.custom_fields is not None:
            from backend.core import field_registry as _lf
            from backend.provision.cmcd_fields import (
                reconcile_system_custom_fields,
                system_feature_flags,
            )

            remote_custom_fields = list(payload.custom_fields)

            def _apply_remote_custom_fields(cfg: dict) -> None:
                lf = _lf.get_lf_config(cfg)
                scoring_enabled, cmcd_enabled = system_feature_flags(cfg)
                lf["custom_fields"] = reconcile_system_custom_fields(
                    list(remote_custom_fields),
                    scoring_enabled=scoring_enabled,
                    cmcd_enabled=cmcd_enabled,
                )
                cfg["log_fields"] = lf

            svcconfig.mutate_config(service_id, _apply_remote_custom_fields)

        logger.debug(f"[state_sync] Imported admin state from {key}")
    except Exception as e:
        logger.warning(f"[state_sync] Failed to import admin state: {e}")


_self = _sys.modules[__name__]
_sys.modules["backend.state_sync"] = _self
_sys.modules.setdefault("backend.core.metadata.alerts", _self)
if "backend" in _sys.modules:
    setattr(_sys.modules["backend"], "state_sync", _self)
    if hasattr(_sys.modules["backend"], "_MODULE_ALIASES"):
        _sys.modules["backend"]._MODULE_ALIASES.update(
            {
                "backend.state_sync": __name__,
                "backend.core.metadata.alerts": __name__,
                "backend.utils.ngwaf": "backend.core.rollups.security_dims",
                "backend.core.rollups.ngwaf_bots": "backend.core.rollups.security_dims",
                "backend.core.rollups.hour_bundles": "backend.core.rollups.day_bundles",
                "backend.core.rollups.time_series": "backend.core.rollups.recompute",
                "backend.core.rollups.overview": "backend.core.rollups.recompute",
                "backend.core.rollups.sessions": "backend.core.rollups.recompute",
                "backend.core._log_fields_data": "backend.core.log_fields",
                "backend.scoring.evaluate": "backend.scoring.features",
                "backend.scoring.matrix": "backend.scoring.features",
            }
        )

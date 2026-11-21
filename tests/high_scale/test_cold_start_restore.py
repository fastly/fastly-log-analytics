"""Unit tests for High-Scale Elevation cold-start restore from FOS (Feature F6 / M2)."""

from __future__ import annotations

import io
import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from urllib.parse import quote, quote_plus

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from backend.core.metadata import pg_schema
from backend.core.request_context import require_admin
from backend.high_scale import cold_start_restore
from backend.high_scale.archive import (
    ArchiveManifest,
    ArchivePublication,
    ClickHousePublication,
    InMemoryBatchManifestStore,
    InMemoryObjectStore,
    S3ObjectStore,
)
from backend.high_scale.cold_start_restore import (
    ColdStartRestoreResult,
    _compute_warm_window,
    _record_restored_archive_sources,
    _register_manifest_idempotent,
    reconstruct_service_config,
    restore_from_fos,
    scrub_secrets,
)
from backend.high_scale.postgres_control import (
    ArchiveManifestRecord,
    ArchiveState,
    OwnerEpochRecord,
    PostgresControlPlane,
)
from backend.high_scale.recovery import RecoveryReport
from backend.routers.admin import high_scale as high_scale_router_mod
from scripts import restore_from_fos as restore_cli
from tests.high_scale.conftest import ReplayClickHouseClient, make_test_archive_manifest

NOW = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)


# ── Local in-memory doubles ──────────────────────────────────────────────────


class _FakeNoSuchKeyError(Exception):
    def __init__(self, key: str) -> None:
        super().__init__(f"NoSuchKey: {key}")
        self.response = {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}}


class _FakeS3Exceptions:
    NoSuchKey = _FakeNoSuchKeyError


class _FakeS3Client:
    """In-memory S3 client supporting get_object, put_object, head_object, delete_object, and pagination."""

    exceptions = _FakeS3Exceptions()

    def __init__(self, *, use_paginator: bool = True, page_size: int = 2) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.list_prefixes: list[str] = []
        self._use_paginator = use_paginator
        self._page_size = page_size
        if not use_paginator:
            # Remove get_paginator attribute so S3ObjectStore exercises the ContinuationToken fallback.
            self.get_paginator = None  # type: ignore[assignment]

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, **_kwargs: Any) -> dict[str, Any]:
        self.objects[(Bucket, Key)] = bytes(Body)
        return {}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        if (Bucket, Key) not in self.objects:
            raise _FakeNoSuchKeyError(Key)
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        if (Bucket, Key) not in self.objects:
            raise _FakeNoSuchKeyError(Key)
        return {"ContentLength": len(self.objects[(Bucket, Key)])}

    def delete_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        self.objects.pop((Bucket, Key), None)
        return {}

    def _matching_keys(self, bucket: str, prefix: str) -> list[str]:
        return sorted(k for (b, k) in self.objects if b == bucket and k.startswith(prefix))

    def list_objects_v2(
        self,
        *,
        Bucket: str,
        Prefix: str = "",
        ContinuationToken: str | None = None,
    ) -> dict[str, Any]:
        self.list_prefixes.append(Prefix)
        keys = self._matching_keys(Bucket, Prefix)
        start = int(ContinuationToken) if ContinuationToken is not None else 0
        chunk = keys[start : start + self._page_size]
        next_idx = start + len(chunk)
        truncated = next_idx < len(keys)
        page: dict[str, Any] = {"Contents": [{"Key": k} for k in chunk], "IsTruncated": truncated}
        if truncated:
            page["NextContinuationToken"] = str(next_idx)
        return page

    def get_paginator(self, operation_name: str) -> Any:
        assert operation_name == "list_objects_v2"
        client = self

        class _Paginator:
            def paginate(self, *, Bucket: str, Prefix: str = "") -> list[dict[str, Any]]:
                client.list_prefixes.append(Prefix)
                keys = client._matching_keys(Bucket, Prefix)
                pages: list[dict[str, Any]] = []
                for idx in range(0, len(keys), client._page_size):
                    pages.append({"Contents": [{"Key": k} for k in keys[idx : idx + client._page_size]]})
                if not pages:
                    pages.append({})
                return pages

        return _Paginator()


class _InMemoryControlPlane:
    """In-memory High-Scale control plane double for cold-start restore tests."""

    def __init__(self, *, owner_epoch: int = 1) -> None:
        self.schema_created = False
        self._owner_epoch = owner_epoch
        self.owners: dict[str, OwnerEpochRecord] = {}
        self.manifests: dict[str, ArchiveManifestRecord] = {}
        self.restored_sources: dict[tuple[str, str], dict[str, Any]] = {}

    def create_schema(self) -> None:
        self.schema_created = True

    def initialize_owner(self, service_id: str, *, owner: str, source_cursor: str) -> OwnerEpochRecord:
        if service_id not in self.owners:
            self.owners[service_id] = OwnerEpochRecord(
                service_id=service_id,
                owner_epoch=self._owner_epoch,
                current_owner=owner,
                previous_owner=None,
                source_cursor=source_cursor,
                drain_complete=False,
                cutover_committed=False,
                rollback_allowed=True,
            )
        return self.owners[service_id]

    def register_archive_manifest(
        self,
        manifest: ArchiveManifest,
        *,
        owner_epoch: int,
        state: ArchiveState = ArchiveState.ARTIFACT_UPLOADING,
    ) -> ArchiveManifestRecord:
        existing = self.manifests.get(manifest.manifest_id)
        if existing is not None:
            if existing.manifest != manifest or existing.owner_epoch != owner_epoch:
                raise ValueError("archive manifest identity changed")
            return existing
        rec = ArchiveManifestRecord(manifest=manifest, state=state, owner_epoch=owner_epoch)
        self.manifests[manifest.manifest_id] = rec
        return rec

    def archive_manifest(self, manifest_id: str) -> ArchiveManifestRecord | None:
        return self.manifests.get(manifest_id)

    def record_restored_archive_sources(
        self,
        manifest: ArchiveManifest,
        *,
        owner_epoch: int,
        status: str = "acknowledged",
    ) -> int:
        count = 0
        for src in manifest.sources:
            key = (manifest.service_id, src.key)
            if key not in self.restored_sources:
                self.restored_sources[key] = {
                    "service_id": manifest.service_id,
                    "domain": manifest.domain,
                    "object_key": src.key,
                    "checksum": src.checksum,
                    "size_bytes": src.size_bytes,
                    "version": src.version,
                    "status": status,
                    "archive_manifest_id": manifest.manifest_id,
                    "owner_epoch": owner_epoch,
                    "accepted_rows": src.row_count,
                }
                count += 1
        return count

    def manifests_covering(
        self,
        service_id: str,
        domain: str,
        start: datetime,
        end: datetime,
    ) -> tuple[ArchiveManifest, ...]:
        return tuple(
            rec.manifest
            for rec in self.manifests.values()
            if rec.manifest.service_id == service_id
            and rec.manifest.domain == domain
            and rec.manifest.coverage_end > start
            and rec.manifest.coverage_start < end
        )


def _make_sqlite_metadata_db() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    for stmt in pg_schema._SCHEMA:
        con.execute(stmt)
    con.commit()
    return con


def _sample_dr_state(service_id: str = "svc_prod") -> dict[str, Any]:
    return {
        "schema_version": 1,
        "exported_at": "2026-11-20T11:55:00+00:00",
        "service_id": service_id,
        "service_config": {
            "service_id": service_id,
            "name": "Production Site",
            "log_period": 10,
            "log_retention_days": 30,
            "rum_enabled": True,
            "cdn_url": "https://cdn.example.invalid",
            "provisioning": {
                "cron_sync": {
                    "snapshot_interval_minutes": 15,
                    "data_retention_days": 45,
                }
            },
        },
        "views": [
            {
                "id": "view-1",
                "service_id": "other_svc_should_be_overridden",
                "name": "5xx Errors",
                "filters_json": {"status": "5xx"},
                "time_range_type": "relative",
                "start_time": None,
                "end_time": None,
                "page": "overview",
                "created_at": "2026-11-19T10:00:00+00:00",
            }
        ],
        "alerts": [
            {
                "id": "alert-1",
                "service_id": service_id,
                "name": "High 5xx",
                "category": "reliability",
                "metric": "error_rate",
                "evaluation_type": "absolute",
                "evaluation_scope": "all",
                "operator": ">",
                "threshold": 5.0,
                "window_min": 5.0,
                "comparison_period_min": None,
                "status_codes": [500, 502, 503],
                "webhook_url": "https://hooks.example.invalid/alert",
                "channels": [{"type": "slack", "target": "#ops"}],
                "zscore_threshold": 3.0,
                "baseline_period_days": 7,
                "enabled": False,
                "last_triggered_at": "2026-11-20T09:00:00+00:00",
                "created_at": "2026-11-10T08:00:00+00:00",
            }
        ],
        "scoring_labels": [
            {
                "id": "lbl-1",
                "service_id": service_id,
                "sid": "session-abc",
                "label": "bad",
                "notes": "credential stuffing",
                "flagged_by": "admin",
                "sample_ip": "198.51.100.1",
                "sample_ua": "BadBot/1.0",
                "sample_url": "/login",
                "created_at": "2026-11-18T00:00:00+00:00",
                "updated_at": "2026-11-19T00:00:00+00:00",
            },
            {
                "id": "lbl-invalid",
                "service_id": service_id,
                "sid": "session-invalid",
                "label": "not_allowed_label",
            },
        ],
        "audit_logs": [
            {
                "timestamp": "2026-11-20T11:00:00+00:00",
                "source_name": service_id,
                "event_type": "config_updated",
                "details": {"snapshot_interval_minutes": 15},
                "actor": "admin",
            }
        ],
    }


# ── 1. ObjectStore.list_keys & ArchivePublication.list_committed_manifests ───


@pytest.mark.no_postgres
def test_in_memory_object_store_list_keys() -> None:
    store = InMemoryObjectStore()
    store.put("archive/manifests/b.commit", b"1")
    store.put("archive/manifests/a.commit", b"2")
    store.put("archive/manifests_other/c.commit", b"3")

    assert store.list_keys("archive/manifests/") == [
        "archive/manifests/a.commit",
        "archive/manifests/b.commit",
    ]
    assert store.list_keys("/archive/manifests/") == [
        "archive/manifests/a.commit",
        "archive/manifests/b.commit",
    ]
    assert store.list_keys() == [
        "archive/manifests/a.commit",
        "archive/manifests/b.commit",
        "archive/manifests_other/c.commit",
    ]


@pytest.mark.no_postgres
@pytest.mark.parametrize("use_paginator", [True, False])
def test_s3_object_store_list_keys_pagination_and_prefix_stripping(use_paginator: bool) -> None:
    s3 = _FakeS3Client(use_paginator=use_paginator, page_size=2)
    store = S3ObjectStore(s3, bucket="my-bucket", prefix="/high-scale/archive/")
    store.put("archive/manifests/m3.commit", b"3")
    store.put("archive/manifests/m1.commit", b"1")
    store.put("archive/manifests/m2.commit", b"2")
    # Sibling prefix that must NOT match "archive/manifests/"
    s3.put_object(Bucket="my-bucket", Key="high-scale/archive/archive/manifests_other/x.commit", Body=b"x")
    # Unrelated bucket prefix
    s3.put_object(Bucket="my-bucket", Key="other/archive/manifests/y.commit", Body=b"y")

    keys = store.list_keys("archive/manifests/")
    assert keys == [
        "archive/manifests/m1.commit",
        "archive/manifests/m2.commit",
        "archive/manifests/m3.commit",
    ]
    assert s3.list_prefixes[0] == "high-scale/archive/archive/manifests/"
    # Verify returned keys round-trip cleanly with get() and exists()
    for key in keys:
        assert store.exists(key)
        assert store.get(key) in {b"1", b"2", b"3"}


@pytest.mark.no_postgres
def test_list_committed_manifests_filters_uncommitted_corrupted_and_service_id() -> None:
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)

    m1, b1 = make_test_archive_manifest(
        "m-older",
        NOW - timedelta(hours=3),
        NOW - timedelta(hours=2),
        ({"event_id": "1"},),
        service_id="svc_a",
        retention_deadline=NOW + timedelta(days=1),
        deletion_deadline=NOW + timedelta(days=2),
    )
    m2, b2 = make_test_archive_manifest(
        "m-newer",
        NOW - timedelta(hours=1),
        NOW - timedelta(minutes=30),
        ({"event_id": "2"},),
        service_id="svc_a",
        retention_deadline=NOW + timedelta(days=1),
        deletion_deadline=NOW + timedelta(days=2),
    )
    m_other, b_other = make_test_archive_manifest(
        "m-other-svc",
        NOW - timedelta(hours=2),
        NOW - timedelta(hours=1),
        ({"event_id": "3"},),
        service_id="svc_b",
        retention_deadline=NOW + timedelta(days=1),
        deletion_deadline=NOW + timedelta(days=2),
    )
    archive.publish(m2, b2)
    archive.publish(m1, b1)
    archive.publish(m_other, b_other)

    # Uncommitted .json without .commit must be ignored
    store.put("archive/manifests/uncommitted.json", b'{"manifest_id":"uncommitted"}')
    # Corrupted .commit (bad JSON) must be skipped without aborting
    store.put("archive/manifests/corrupt-json.json", b'{"manifest_id":"corrupt-json"}')
    store.put("archive/manifests/corrupt-json.commit", b"{not-valid-json")
    # Checksum mismatch .commit must be skipped without aborting
    store.put("archive/manifests/bad-hash.json", store.get("archive/manifests/m-older.json"))
    store.put(
        "archive/manifests/bad-hash.commit",
        json.dumps(
            {
                "manifest_id": "bad-hash",
                "manifest_checksum": "sha256:wrong",
                "artifact_checksum": "sha256:wrong",
            }
        ).encode(),
    )

    svc_a_manifests = archive.list_committed_manifests(service_id="svc_a")
    assert [m.manifest_id for m in svc_a_manifests] == ["m-older", "m-newer"]

    all_manifests = archive.list_committed_manifests()
    assert [m.manifest_id for m in all_manifests] == ["m-older", "m-other-svc", "m-newer"]


# ── 2. Full End-to-End Cold-Start Restore Across All 3 Domains ───────────────


@pytest.mark.no_postgres
def test_full_end_to_end_cold_start_restore_all_three_domains() -> None:
    s3 = _FakeS3Client()
    dr_state = _sample_dr_state("svc_prod")
    s3.put_object(
        Bucket="prod-fos-bucket",
        Key="meta/dr_state.json",
        Body=json.dumps(dr_state).encode("utf-8"),
    )

    archive_store = InMemoryObjectStore()
    archive = ArchivePublication(archive_store)

    m_req, b_req = make_test_archive_manifest(
        "req-1",
        NOW - timedelta(hours=2),
        NOW - timedelta(hours=1),
        (
            {
                "event_id": "r1",
                "timestamp": "2026-11-20T10:30:00Z",
                "status": 200,
                "url": "/home",
                "country": "US",
            },
            {
                "event_id": "r2",
                "timestamp": "2026-11-20T10:35:00Z",
                "status": 502,
                "url": "/api/items",
                "country": "GB",
            },
        ),
        service_id="svc_prod",
        domain="request",
        retention_deadline=NOW + timedelta(days=30),
        deletion_deadline=NOW + timedelta(days=30, minutes=15),
    )
    m_rum_v, b_rum_v = make_test_archive_manifest(
        "rum-v-1",
        NOW - timedelta(hours=2),
        NOW - timedelta(hours=1),
        (
            {
                "event_id": "v1",
                "timestamp": "2026-11-20T10:30:00Z",
                "metric_name": "LCP",
                "metric_value": 1.25,
                "metric_rating": "good",
                "url": "/home",
                "country": "US",
                "device": "desktop",
            },
        ),
        service_id="svc_prod",
        domain="rum_vitals",
        retention_deadline=NOW + timedelta(days=30),
        deletion_deadline=NOW + timedelta(days=30, minutes=15),
    )
    m_rum_e, b_rum_e = make_test_archive_manifest(
        "rum-e-1",
        NOW - timedelta(hours=2),
        NOW - timedelta(hours=1),
        (
            {
                "event_id": "e1",
                "timestamp": "2026-11-20T10:31:00Z",
                "error_type": "TypeError",
                "error_message": "Cannot read properties of undefined",
                "url": "/checkout",
                "country": "US",
                "device": "mobile",
            },
        ),
        service_id="svc_prod",
        domain="rum_errors",
        retention_deadline=NOW + timedelta(days=30),
        deletion_deadline=NOW + timedelta(days=30, minutes=15),
    )
    for m, b in ((m_req, b_req), (m_rum_v, b_rum_v), (m_rum_e, b_rum_e)):
        archive.publish(m, b)

    con = _make_sqlite_metadata_db()
    control = _InMemoryControlPlane()
    replay_ch = ReplayClickHouseClient()
    serving_pub = ClickHousePublication(InMemoryBatchManifestStore(), replay_ch)
    agg_pub = ClickHousePublication(InMemoryBatchManifestStore(), ReplayClickHouseClient())

    saved_configs: dict[str, dict[str, Any]] = {}
    cleared_clients: list[str] = []
    mint_calls: list[dict[str, Any]] = []
    pg_schema_ensured = False

    def _fake_ensure_key(desc: str, state: dict, token: str, *, permission: str, buckets: list[str]) -> dict[str, str]:
        mint_calls.append({"desc": desc, "token": token, "permission": permission, "buckets": buckets})
        return {"id": "minted-ak", "access_key": "minted-ak", "secret_key": "minted-sk"}

    def _fake_ensure_pg() -> bool:
        nonlocal pg_schema_ensured
        pg_schema_ensured = True
        return True

    result = restore_from_fos(
        service_id="svc_prod",
        bucket="prod-fos-bucket",
        region="us-east-1",
        fastly_api_key="fastly-token-xyz",
        s3_client=s3,
        archive=archive,
        control=control,
        metadata_con=con,
        serving_publication=serving_pub,
        aggregates_publication=agg_pub,
        ensure_fos_key_fn=_fake_ensure_key,
        save_config_fn=lambda sid, cfg: saved_configs.__setitem__(sid, cfg),
        clear_fos_client_fn=cleared_clients.append,
        ensure_pg_schema_fn=_fake_ensure_pg,
        now=NOW,
    )

    assert result.service_id == "svc_prod"
    assert result.dr_state_restored is True
    assert result.views_restored == 1
    assert result.alerts_restored == 1
    assert result.scoring_labels_restored == 1  # invalid label filtered out
    assert result.audit_logs_restored == 1
    assert result.manifests_registered == 3
    assert set(result.domains_rebuilt.keys()) == {"request", "rum_vitals", "rum_errors"}
    assert result.domains_rebuilt["request"]["full_rebuild_complete"] is True
    assert result.domains_rebuilt["request"]["rows_replayed"] == 2
    assert result.domains_rebuilt["rum_vitals"]["rows_replayed"] == 1
    assert result.domains_rebuilt["rum_errors"]["rows_replayed"] == 1

    # Verify Fastly API key minted a bucket-scoped FOS key
    assert mint_calls == [
        {
            "desc": "fos-log-analysis-svc_prod",
            "token": "fastly-token-xyz",
            "permission": "read-write-objects",
            "buckets": ["prod-fos-bucket"],
        }
    ]
    assert pg_schema_ensured is True
    assert control.schema_created is True
    assert cleared_clients == ["svc_prod"]

    # Verify reconstructed config
    cfg = saved_configs["svc_prod"]
    assert cfg["fos_access_key_id"] == "minted-ak"
    assert cfg["fos_secret_access_key"] == "minted-sk"
    assert cfg["fastly_api_key"] == "fastly-token-xyz"
    assert cfg["fos_prefix"] == ""
    assert cfg["prefix"] == ""
    assert cfg["cdn_url"] == ""  # cleared because cdn_secret was absent
    assert cfg["provisioning"]["cron_sync"]["snapshot_interval_minutes"] == 15

    # Verify Postgres metadata rows
    view_row = con.execute("SELECT * FROM views WHERE id = 'view-1'").fetchone()
    assert view_row is not None and view_row["service_id"] == "svc_prod"
    assert json.loads(view_row["filters_json"]) == {"status": "5xx"}

    alert_row = con.execute("SELECT * FROM alerts WHERE id = 'alert-1'").fetchone()
    assert alert_row is not None
    assert json.loads(alert_row["channels_json"]) == [{"type": "slack", "target": "#ops"}]
    assert json.loads(alert_row["status_codes"]) == [500, 502, 503]
    assert alert_row["enabled"] == 0

    label_rows = con.execute("SELECT * FROM scoring_labels").fetchall()
    assert len(label_rows) == 1 and label_rows[0]["sid"] == "session-abc"

    audit_rows = con.execute("SELECT * FROM audit_logs").fetchall()
    assert len(audit_rows) == 1 and audit_rows[0]["source_name"] == "svc_prod"


# ── 3. Explicit FOS Keys Fallback vs Partial/Missing Credentials ─────────────


@pytest.mark.no_postgres
def test_explicit_fos_keys_skips_fastly_key_minting() -> None:
    s3 = _FakeS3Client()
    archive = ArchivePublication(InMemoryObjectStore())
    control = _InMemoryControlPlane()
    mint_called = False

    def _should_not_be_called(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        nonlocal mint_called
        mint_called = True
        return {}

    saved: dict[str, dict[str, Any]] = {}
    result = restore_from_fos(
        service_id="svc_explicit",
        bucket="bkt",
        region="eu-central-1",
        prefix="/logs/v3/",
        fos_access_key="explicit-ak",
        fos_secret_key="explicit-sk",
        restore_serving_state=False,
        s3_client=s3,
        archive=archive,
        control=control,
        ensure_fos_key_fn=_should_not_be_called,
        save_config_fn=lambda sid, cfg: saved.__setitem__(sid, cfg),
        clear_fos_client_fn=lambda _sid: None,
        ensure_pg_schema_fn=lambda: True,
        now=NOW,
    )

    assert mint_called is False
    assert result.dr_state_restored is False
    assert result.domains_rebuilt == {}
    assert saved["svc_explicit"]["fos_access_key_id"] == "explicit-ak"
    assert saved["svc_explicit"]["fos_secret_access_key"] == "explicit-sk"
    assert saved["svc_explicit"]["fastly_api_key"] == ""
    assert saved["svc_explicit"]["fos_prefix"] == "logs/v3"
    assert saved["svc_explicit"]["prefix"] == "logs/v3"


@pytest.mark.no_postgres
@pytest.mark.parametrize(
    ("kwargs", "expected_msg"),
    [
        ({"fos_access_key": "only-ak"}, "must both be provided together"),
        ({"fos_secret_key": "only-sk"}, "must both be provided together"),
        ({}, "provide fastly_api_key or both"),
    ],
)
def test_restore_rejects_missing_or_partial_credentials(kwargs: dict[str, Any], expected_msg: str) -> None:
    with pytest.raises(ValueError, match=expected_msg):
        restore_from_fos(service_id="svc1", bucket="bkt", region="us-east-1", **kwargs)


# ── 4. Missing & Malformed meta/dr_state.json + cdn_url Clearing ─────────────


@pytest.mark.no_postgres
def test_malformed_dr_state_falls_back_gracefully() -> None:
    s3 = _FakeS3Client()
    s3.put_object(Bucket="bkt", Key="meta/dr_state.json", Body=b"{corrupt-json!!")
    saved: dict[str, dict[str, Any]] = {}

    result = restore_from_fos(
        service_id="svc_corrupt",
        bucket="bkt",
        region="us-east-1",
        fos_access_key="ak",
        fos_secret_key="sk",
        restore_serving_state=False,
        s3_client=s3,
        archive=ArchivePublication(InMemoryObjectStore()),
        control=_InMemoryControlPlane(),
        save_config_fn=lambda sid, cfg: saved.__setitem__(sid, cfg),
        clear_fos_client_fn=lambda _sid: None,
        ensure_pg_schema_fn=lambda: True,
        now=NOW,
    )

    assert result.dr_state_restored is False
    assert result.views_restored == 0
    assert saved["svc_corrupt"]["name"] == "svc_corrupt"
    assert saved["svc_corrupt"]["provisioning"]["cron_sync"]["snapshot_interval_minutes"] == 5


@pytest.mark.no_postgres
def test_reconstruct_service_config_clears_cdn_url_without_cdn_secret() -> None:
    cfg = reconstruct_service_config(
        service_id="svc_cdn",
        bucket="bkt",
        region="us-east-1",
        prefix="p1/",
        fos_access_key="ak",
        fos_secret_key="sk",
        dr_service_config={"cdn_url": "https://cdn.example.invalid", "name": "My Site"},
    )
    assert cfg["cdn_url"] == ""
    assert cfg["fos_prefix"] == "p1"
    assert cfg["prefix"] == "p1"


# ── 5. Grace Classification, Anti-Double-Ingest & Idempotent Re-Run ──────────


@pytest.mark.no_postgres
def test_grace_classification_and_idempotent_rerun_zero_duplicates() -> None:
    s3 = _FakeS3Client()
    s3.put_object(Bucket="bkt", Key="meta/dr_state.json", Body=json.dumps(_sample_dr_state("svc_idem")).encode())

    archive = ArchivePublication(InMemoryObjectStore())
    # In-grace manifest: deletion deadline is in the future (> NOW - 1h)
    m_recent, b_recent = make_test_archive_manifest(
        "m-in-grace",
        NOW - timedelta(minutes=20),
        NOW - timedelta(minutes=10),
        ({"event_id": "recent-1"},),
        service_id="svc_idem",
        domain="request",
        retention_deadline=NOW + timedelta(minutes=5),
        deletion_deadline=NOW + timedelta(minutes=20),
    )
    # Past-grace historical manifest: deletion deadline was 2 days ago (< NOW - 1h)
    m_old, b_old = make_test_archive_manifest(
        "m-past-grace",
        NOW - timedelta(days=5),
        NOW - timedelta(days=5) + timedelta(minutes=10),
        ({"event_id": "old-1"},),
        service_id="svc_idem",
        domain="request",
        retention_deadline=NOW - timedelta(days=3),
        deletion_deadline=NOW - timedelta(days=2),
    )
    archive.publish(m_recent, b_recent)
    archive.publish(m_old, b_old)

    con = _make_sqlite_metadata_db()
    control = _InMemoryControlPlane(owner_epoch=1)

    common_kwargs: dict[str, Any] = dict(
        service_id="svc_idem",
        bucket="bkt",
        region="us-east-1",
        fos_access_key="ak",
        fos_secret_key="sk",
        restore_serving_state=False,
        s3_client=s3,
        archive=archive,
        control=control,
        metadata_con=con,
        save_config_fn=lambda _sid, _cfg: None,
        clear_fos_client_fn=lambda _sid: None,
        ensure_pg_schema_fn=lambda: True,
        now=NOW,
    )

    first = restore_from_fos(**common_kwargs)
    assert first.manifests_registered == 2
    assert control.manifests["m-in-grace"].state is ArchiveState.MANIFEST_COMMITTED
    assert control.manifests["m-past-grace"].state is ArchiveState.SOURCE_DELETED
    assert control.restored_sources[("svc_idem", m_recent.sources[0].key)]["status"] == "acknowledged"
    assert control.restored_sources[("svc_idem", m_old.sources[0].key)]["status"] == "source_deleted"

    # Simulate an owner epoch bump before a second restore run to verify prior-epoch tolerance
    control.owners["svc_idem"] = OwnerEpochRecord(
        service_id="svc_idem",
        owner_epoch=2,
        current_owner="high_scale",
        previous_owner="high_scale",
        source_cursor="__initial__",
        drain_complete=False,
        cutover_committed=False,
        rollback_allowed=True,
    )

    second = restore_from_fos(**common_kwargs)
    assert second.manifests_registered == 2
    assert second.audit_logs_restored == 0  # WHERE NOT EXISTS skipped duplicate

    # Zero duplicate rows across all 4 metadata tables and control plane
    assert con.execute("SELECT COUNT(*) FROM views").fetchone()[0] == 1
    assert con.execute("SELECT COUNT(*) FROM alerts").fetchone()[0] == 1
    assert con.execute("SELECT COUNT(*) FROM scoring_labels").fetchone()[0] == 1
    assert con.execute("SELECT COUNT(*) FROM audit_logs").fetchone()[0] == 1
    assert len(control.manifests) == 2
    assert len(control.restored_sources) == 2


# ── 6. Real PostgresControlPlane.transaction() SQL Test ──────────────────────


@pytest.mark.no_postgres
def test_record_restored_archive_sources_with_real_postgres_control_plane() -> None:
    executed: list[tuple[str, tuple[Any, ...]]] = []

    class _FakeConn:
        def execute(self, sql: str, params: tuple[Any, ...] = ()) -> Any:
            executed.append((sql, params))
            return SimpleNamespace(fetchall=lambda: [], fetchone=lambda: None)

        @contextmanager
        def transaction(self):
            yield self

    class _FakePool:
        @contextmanager
        def connection(self):
            yield _FakeConn()

    control = PostgresControlPlane(pool=_FakePool())
    manifest, _ = make_test_archive_manifest(
        "m-pg-sql",
        NOW - timedelta(hours=1),
        NOW - timedelta(minutes=30),
        ({"event_id": "1"}, {"event_id": "2"}),
        service_id="svc_pg",
        domain="request",
        retention_deadline=NOW + timedelta(days=1),
        deletion_deadline=NOW + timedelta(days=2),
    )

    count = _record_restored_archive_sources(control, manifest, owner_epoch=3, status="source_deleted")
    assert count == 1
    assert len(executed) == 1
    sql, params = executed[0]
    assert "INSERT INTO high_scale_source_objects" in sql
    assert "ON CONFLICT (service_id, object_key) DO NOTHING" in sql
    assert params[1] == "svc_pg"
    assert params[2] == "request"
    assert params[3] == manifest.sources[0].key
    assert params[7] == "source_deleted"
    assert params[8] == "m-pg-sql"
    assert params[9] == 3
    assert params[10] == 2


# ── 7. Admin API Tests ───────────────────────────────────────────────────────


def _build_isolated_high_scale_app(*, allow_admin: bool = True) -> FastAPI:
    app = FastAPI()
    app.include_router(high_scale_router_mod.router)
    if allow_admin:
        app.dependency_overrides[require_admin] = lambda: None
    else:

        def _deny_admin() -> None:
            raise HTTPException(
                status_code=403,
                detail={"error": "admin_only", "message": "This operation requires admin access."},
            )

        app.dependency_overrides[require_admin] = _deny_admin
    return app


@pytest.mark.no_postgres
def test_admin_api_restore_from_fos_200_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def _fake_restore(**kwargs: Any) -> ColdStartRestoreResult:
        captured.update(kwargs)
        return ColdStartRestoreResult(
            service_id=kwargs["service_id"],
            dr_state_restored=True,
            views_restored=2,
            alerts_restored=1,
            scoring_labels_restored=3,
            audit_logs_restored=4,
            manifests_registered=5,
            domains_rebuilt={
                "request": {"full_rebuild_complete": True, "manifests_replayed": 5, "rows_replayed": 100, "errors": []},
                "rum_vitals": {
                    "full_rebuild_complete": True,
                    "manifests_replayed": 0,
                    "rows_replayed": 0,
                    "errors": [],
                },
                "rum_errors": {
                    "full_rebuild_complete": True,
                    "manifests_replayed": 0,
                    "rows_replayed": 0,
                    "errors": [],
                },
            },
        )

    monkeypatch.setattr(cold_start_restore, "restore_from_fos", _fake_restore)
    client = TestClient(_build_isolated_high_scale_app(allow_admin=True))

    resp = client.post(
        "/api/high-scale/restore-from-fos",
        json={
            "service_id": "svc_api",
            "bucket": "my-fos-bucket",
            "region": "us-east-1",
            "fastly_api_key": "api-key-123",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["service_id"] == "svc_api"
    assert body["dr_state_restored"] is True
    assert body["views_restored"] == 2
    assert body["alerts_restored"] == 1
    assert body["scoring_labels_restored"] == 3
    assert body["audit_logs_restored"] == 4
    assert body["manifests_registered"] == 5
    assert set(body["domains_rebuilt"].keys()) == {"request", "rum_vitals", "rum_errors"}
    assert captured["archive_prefix"] == "high-scale/archive"
    assert captured["restore_serving_state"] is True


@pytest.mark.no_postgres
def test_admin_api_restore_from_fos_403_for_non_admin() -> None:
    client = TestClient(_build_isolated_high_scale_app(allow_admin=False))
    resp = client.post(
        "/api/high-scale/restore-from-fos",
        json={
            "service_id": "svc_api",
            "bucket": "my-fos-bucket",
            "region": "us-east-1",
            "fastly_api_key": "api-key-123",
        },
    )
    assert resp.status_code == 403
    assert resp.json()["detail"]["error"] == "admin_only"


@pytest.mark.no_postgres
@pytest.mark.parametrize(
    "payload",
    [
        {"service_id": "svc_api", "bucket": "bkt", "region": "us-east-1"},  # missing credentials
        {"service_id": "svc_api", "bucket": "bkt", "region": "us-east-1", "fos_access_key": "only-ak"},  # partial
        {"service_id": "../bad", "bucket": "bkt", "region": "us-east-1", "fastly_api_key": "tok"},  # bad service_id
        {"service_id": "svc_api", "bucket": "   ", "region": "us-east-1", "fastly_api_key": "tok"},  # blank bucket
        {
            "service_id": "svc_api",
            "bucket": "bkt",
            "region": "us-east-1",
            "fastly_api_key": "tok",
            "unexpected_field": 1,
        },  # extra="forbid"
    ],
)
def test_admin_api_restore_from_fos_422_validation(payload: dict[str, Any]) -> None:
    client = TestClient(_build_isolated_high_scale_app(allow_admin=True))
    resp = client.post("/api/high-scale/restore-from-fos", json=payload)
    assert resp.status_code == 422


@pytest.mark.no_postgres
@pytest.mark.security_regression
def test_admin_api_scrubs_sentinel_secrets_from_errors_and_logs(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    sentinel_api = "SENTINEL_FASTLY_API_KEY_9f8e7d6c"
    sentinel_ak = "SENTINEL_FOS_AK_1a2b3c4d"
    sentinel_sk = "SENTINEL_FOS_SK_5e6f7a8b+slash/eq="

    def _boom(**_kwargs: Any) -> ColdStartRestoreResult:
        raise RuntimeError(f"upstream failed with {sentinel_api} / {sentinel_ak} / {sentinel_sk}")

    monkeypatch.setattr(cold_start_restore, "restore_from_fos", _boom)
    client = TestClient(_build_isolated_high_scale_app(allow_admin=True))

    with caplog.at_level(logging.WARNING):
        resp = client.post(
            "/api/high-scale/restore-from-fos",
            json={
                "service_id": "svc_sec",
                "bucket": "bkt",
                "region": "us-east-1",
                "fastly_api_key": sentinel_api,
                "fos_access_key": sentinel_ak,
                "fos_secret_key": sentinel_sk,
            },
        )

    assert resp.status_code == 500
    response_text = resp.text
    log_text = "\n".join(rec.getMessage() for rec in caplog.records)
    for sentinel in (sentinel_api, sentinel_ak, sentinel_sk):
        assert sentinel not in response_text
        assert sentinel not in log_text
    assert "[REDACTED]" in response_text


@pytest.mark.no_postgres
@pytest.mark.security_regression
def test_scrub_secrets_redacts_raw_and_url_encoded_values() -> None:
    secret = "s3cr3t+value/with=chars"
    raw = f"error key={secret} qs=s3cr3t%2Bvalue%2Fwith%3Dchars"
    scrubbed = scrub_secrets(raw, [secret])
    assert secret not in scrubbed
    assert "s3cr3t%2Bvalue%2Fwith%3Dchars" not in scrubbed
    assert scrubbed.count("[REDACTED]") == 2


# ── 8. CLI Tests (scripts/restore_from_fos.py) ───────────────────────────────


@pytest.mark.no_postgres
def test_cli_json_output_and_env_fallbacks(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, Any] = {}

    def _fake_restore(**kwargs: Any) -> ColdStartRestoreResult:
        captured.update(kwargs)
        return ColdStartRestoreResult(
            service_id=kwargs["service_id"],
            dr_state_restored=True,
            views_restored=1,
            alerts_restored=2,
            scoring_labels_restored=0,
            audit_logs_restored=3,
            manifests_registered=4,
            domains_rebuilt={
                "request": {"full_rebuild_complete": True, "manifests_replayed": 4, "rows_replayed": 50, "errors": []},
            },
        )

    monkeypatch.setattr(restore_cli, "restore_from_fos", _fake_restore)
    monkeypatch.setenv("FASTLY_API_KEY", "env-fastly-token")
    monkeypatch.delenv("FOS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("FOS_ACCESS_KEY", raising=False)
    monkeypatch.delenv("FOS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.delenv("FOS_SECRET_KEY", raising=False)

    code = restore_cli.main(["--service-id", "svc_cli", "--bucket", "bkt", "--region", "us-east-1", "--json"])
    assert code == 0
    out = json.loads(capsys.readouterr().out)
    assert out["service_id"] == "svc_cli"
    assert out["manifests_registered"] == 4
    assert captured["fastly_api_key"] == "env-fastly-token"
    assert captured["restore_serving_state"] is True


@pytest.mark.no_postgres
def test_cli_skip_serving_rebuild_and_fos_env_vars(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, Any] = {}

    def _fake_restore(**kwargs: Any) -> ColdStartRestoreResult:
        captured.update(kwargs)
        return ColdStartRestoreResult(
            service_id=kwargs["service_id"],
            dr_state_restored=False,
            views_restored=0,
            alerts_restored=0,
            scoring_labels_restored=0,
            audit_logs_restored=0,
            manifests_registered=0,
            domains_rebuilt={},
        )

    monkeypatch.setattr(restore_cli, "restore_from_fos", _fake_restore)
    monkeypatch.delenv("FASTLY_API_KEY", raising=False)
    monkeypatch.setenv("FOS_ACCESS_KEY_ID", "env-ak")
    monkeypatch.setenv("FOS_SECRET_ACCESS_KEY", "env-sk")

    code = restore_cli.main(
        [
            "--service-id",
            "svc_cli2",
            "--bucket",
            "bkt",
            "--region",
            "us-east-1",
            "--skip-serving-rebuild",
        ]
    )
    assert code == 0
    assert captured["restore_serving_state"] is False
    assert captured["fos_access_key"] == "env-ak"
    assert captured["fos_secret_key"] == "env-sk"
    assert "Restored service svc_cli2" in capsys.readouterr().out


@pytest.mark.no_postgres
def test_cli_exit_2_on_missing_or_partial_credentials(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    for var in ("FASTLY_API_KEY", "FOS_ACCESS_KEY_ID", "FOS_ACCESS_KEY", "FOS_SECRET_ACCESS_KEY", "FOS_SECRET_KEY"):
        monkeypatch.delenv(var, raising=False)

    assert restore_cli.main(["--service-id", "svc", "--bucket", "bkt", "--region", "us-east-1"]) == 2
    assert "provide --fastly-api-key" in capsys.readouterr().err

    assert (
        restore_cli.main(
            ["--service-id", "svc", "--bucket", "bkt", "--region", "us-east-1", "--fos-access-key", "only-ak"]
        )
        == 2
    )
    assert "must both be provided together" in capsys.readouterr().err


@pytest.mark.no_postgres
def test_cli_exit_1_on_incomplete_domain_rebuild(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def _incomplete(**kwargs: Any) -> ColdStartRestoreResult:
        return ColdStartRestoreResult(
            service_id=kwargs["service_id"],
            dr_state_restored=True,
            views_restored=0,
            alerts_restored=0,
            scoring_labels_restored=0,
            audit_logs_restored=0,
            manifests_registered=1,
            domains_rebuilt={
                "request": {
                    "full_rebuild_complete": False,
                    "manifests_replayed": 0,
                    "rows_replayed": 0,
                    "errors": ["boom"],
                },
            },
        )

    monkeypatch.setattr(restore_cli, "restore_from_fos", _incomplete)
    code = restore_cli.main(
        ["--service-id", "svc", "--bucket", "bkt", "--region", "us-east-1", "--fastly-api-key", "tok"]
    )
    assert code == 1
    assert "incomplete" in capsys.readouterr().err


@pytest.mark.no_postgres
@pytest.mark.security_regression
def test_cli_scrubs_sentinel_secrets_from_stderr(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    sentinel_api = "CLI_SENTINEL_API_KEY_xyz987"
    sentinel_sk = "CLI_SENTINEL_FOS_SECRET_abc123"

    def _boom(**_kwargs: Any) -> ColdStartRestoreResult:
        raise RuntimeError(f"failed calling FOS with {sentinel_api} and {sentinel_sk}")

    monkeypatch.setattr(restore_cli, "restore_from_fos", _boom)
    code = restore_cli.main(
        [
            "--service-id",
            "svc",
            "--bucket",
            "bkt",
            "--region",
            "us-east-1",
            "--fastly-api-key",
            sentinel_api,
            "--fos-access-key",
            "CLI_AK",
            "--fos-secret-key",
            sentinel_sk,
        ]
    )
    assert code == 1
    captured = capsys.readouterr()
    assert sentinel_api not in captured.err and sentinel_api not in captured.out
    assert sentinel_sk not in captured.err and sentinel_sk not in captured.out
    assert "[REDACTED]" in captured.err


# ── 9. Iteration 2 Security, Integrity & Edge-Case Regression Tests ──────────


@pytest.mark.no_postgres
@pytest.mark.security_regression
def test_admin_api_scrubs_minted_fos_secrets_on_post_mint_exception(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    sentinel_api = "fl_api+key/with=special&chars"
    minted_ak = "AKIA+MINTED/KEY=1&2"
    minted_sk = "sk+MINTED/secret=value&tail"

    def _fake_mint(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        return {"id": minted_ak, "access_key": minted_ak, "secret_key": minted_sk}

    def _boom_s3(*, access_key: str, secret_key: str, region: str) -> Any:
        raise RuntimeError(
            f"S3 SigV4 auth failed for ak={access_key} sk={secret_key} "
            f"enc_sk={quote(secret_key, safe='')} plus_sk={quote_plus(secret_key)} "
            f"enc_ak={quote(access_key, safe='')} plus_ak={quote_plus(access_key)}"
        )

    monkeypatch.setattr(cold_start_restore, "ensure_fos_access_key", _fake_mint)
    monkeypatch.setattr(cold_start_restore, "_build_direct_fos_s3_client", _boom_s3)

    client = TestClient(_build_isolated_high_scale_app(allow_admin=True))
    with caplog.at_level(logging.WARNING):
        resp = client.post(
            "/api/high-scale/restore-from-fos",
            json={
                "service_id": "svc_mint_api",
                "bucket": "bkt",
                "region": "us-east-1",
                "fastly_api_key": sentinel_api,
            },
        )

    assert resp.status_code == 500
    response_text = resp.text
    log_text = "\n".join(rec.getMessage() for rec in caplog.records)
    for raw in (sentinel_api, minted_ak, minted_sk):
        for variant in (raw, quote(raw, safe=""), quote_plus(raw)):
            assert variant not in response_text
            assert variant not in log_text
    assert "[REDACTED]" in response_text


@pytest.mark.no_postgres
@pytest.mark.security_regression
def test_cli_scrubs_minted_fos_secrets_on_post_mint_exception(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    sentinel_api = "fl_api+key/with=special&chars"
    minted_ak = "AKIA+MINTED/KEY=1&2"
    minted_sk = "sk+MINTED/secret=value&tail"

    def _fake_mint(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        return {"id": minted_ak, "access_key": minted_ak, "secret_key": minted_sk}

    def _boom_s3(*, access_key: str, secret_key: str, region: str) -> Any:
        raise RuntimeError(
            f"S3 SigV4 auth failed for ak={access_key} sk={secret_key} "
            f"enc_sk={quote(secret_key, safe='')} plus_sk={quote_plus(secret_key)}"
        )

    monkeypatch.setattr(cold_start_restore, "ensure_fos_access_key", _fake_mint)
    monkeypatch.setattr(cold_start_restore, "_build_direct_fos_s3_client", _boom_s3)
    for var in ("FASTLY_API_KEY", "FOS_ACCESS_KEY_ID", "FOS_ACCESS_KEY", "FOS_SECRET_ACCESS_KEY", "FOS_SECRET_KEY"):
        monkeypatch.delenv(var, raising=False)

    code = restore_cli.main(
        [
            "--service-id",
            "svc_mint_cli",
            "--bucket",
            "bkt",
            "--region",
            "us-east-1",
            "--fastly-api-key",
            sentinel_api,
        ]
    )
    assert code == 1
    captured = capsys.readouterr()
    combined = captured.out + captured.err
    for raw in (sentinel_api, minted_ak, minted_sk):
        for variant in (raw, quote(raw, safe=""), quote_plus(raw)):
            assert variant not in combined
    assert "[REDACTED]" in captured.err


@pytest.mark.no_postgres
@pytest.mark.security_regression
def test_restore_scrubs_minted_fos_secrets_from_domains_rebuilt_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel_api = "fl_api+key/with=special&chars"
    minted_ak = "AKIA+MINTED/KEY=1&2"
    minted_sk = "sk+MINTED/secret=value&tail"

    def _fake_mint(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        return {"id": minted_ak, "access_key": minted_ak, "secret_key": minted_sk}

    def _fake_rebuild(*_args: Any, domain: str, **_kwargs: Any) -> RecoveryReport:
        return RecoveryReport(
            ingest_accepted=True,
            triage_available=True,
            raw_query_available=True,
            warm_history_available=False,
            full_rebuild_complete=False,
            manifests_replayed=0,
            rows_replayed=0,
            errors=(
                f"replay error ak={minted_ak} sk={minted_sk} "
                f"enc={quote(minted_sk, safe='')} plus={quote_plus(minted_sk)} api={sentinel_api}",
            ),
            rebuild_id=f"rb-{domain}",
        )

    monkeypatch.setattr(cold_start_restore, "rebuild_serving_state_from_fos", _fake_rebuild)

    result = restore_from_fos(
        service_id="svc_err",
        bucket="bkt",
        region="us-east-1",
        fastly_api_key=sentinel_api,
        s3_client=_FakeS3Client(),
        archive=ArchivePublication(InMemoryObjectStore()),
        control=_InMemoryControlPlane(),
        serving_publication=ClickHousePublication(InMemoryBatchManifestStore(), ReplayClickHouseClient()),
        ensure_fos_key_fn=_fake_mint,
        save_config_fn=lambda _sid, _cfg: None,
        clear_fos_client_fn=lambda _sid: None,
        ensure_pg_schema_fn=lambda: True,
        now=NOW,
    )

    for domain_info in result.domains_rebuilt.values():
        err_text = " ".join(domain_info["errors"])
        for raw in (sentinel_api, minted_ak, minted_sk):
            for variant in (raw, quote(raw, safe=""), quote_plus(raw)):
                assert variant not in err_text
        assert "[REDACTED]" in err_text


@pytest.mark.no_postgres
def test_mismatched_dr_state_service_id_rejected_without_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    s3 = _FakeS3Client()
    foreign_dr = _sample_dr_state("svc_OTHER_TENANT")
    s3.put_object(Bucket="bkt", Key="meta/dr_state.json", Body=json.dumps(foreign_dr).encode("utf-8"))

    con = _make_sqlite_metadata_db()
    saved: dict[str, dict[str, Any]] = {}

    with pytest.raises(ValueError, match="belongs to service 'svc_OTHER_TENANT', expected 'svc_TARGET'"):
        restore_from_fos(
            service_id="svc_TARGET",
            bucket="bkt",
            region="us-east-1",
            fos_access_key="ak",
            fos_secret_key="sk",
            restore_serving_state=False,
            s3_client=s3,
            archive=ArchivePublication(InMemoryObjectStore()),
            control=_InMemoryControlPlane(),
            metadata_con=con,
            save_config_fn=lambda sid, cfg: saved.__setitem__(sid, cfg),
            clear_fos_client_fn=lambda _sid: None,
            ensure_pg_schema_fn=lambda: True,
            now=NOW,
        )

    assert saved == {}
    assert con.execute("SELECT COUNT(*) FROM views").fetchone()[0] == 0
    assert con.execute("SELECT COUNT(*) FROM alerts").fetchone()[0] == 0
    assert con.execute("SELECT COUNT(*) FROM scoring_labels").fetchone()[0] == 0
    assert con.execute("SELECT COUNT(*) FROM audit_logs").fetchone()[0] == 0

    # Verify Admin API maps mismatched service_id ValueError to HTTP 400
    monkeypatch.setattr(cold_start_restore, "_build_direct_fos_s3_client", lambda **_kwargs: s3)
    client = TestClient(_build_isolated_high_scale_app(allow_admin=True))
    resp = client.post(
        "/api/high-scale/restore-from-fos",
        json={
            "service_id": "svc_TARGET",
            "bucket": "bkt",
            "region": "us-east-1",
            "fos_access_key": "ak",
            "fos_secret_key": "sk",
            "restore_serving_state": False,
        },
    )
    assert resp.status_code == 400
    assert resp.json()["detail"]["error"] == "restore_from_fos_invalid"


@pytest.mark.no_postgres
@pytest.mark.parametrize(
    "bad_region",
    [
        "evil.com/#",
        "us-east-1@attacker.invalid",
        "US-EAST-1",
        "us_east_1",
        "us-east-1/path",
        "   ",
    ],
)
def test_region_validation_rejects_ssrf_and_invalid_values(bad_region: str) -> None:
    with pytest.raises(ValueError, match="region must match"):
        restore_from_fos(
            service_id="svc1",
            bucket="bkt",
            region=bad_region,
            fastly_api_key="tok",
        )

    client = TestClient(_build_isolated_high_scale_app(allow_admin=True))
    resp = client.post(
        "/api/high-scale/restore-from-fos",
        json={
            "service_id": "svc1",
            "bucket": "bkt",
            "region": bad_region,
            "fastly_api_key": "tok",
        },
    )
    assert resp.status_code == 422


@pytest.mark.no_postgres
def test_list_committed_manifests_skips_non_dict_json_payloads() -> None:
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)

    valid_m, valid_b = make_test_archive_manifest(
        "m-valid",
        NOW - timedelta(hours=2),
        NOW - timedelta(hours=1),
        ({"event_id": "1"},),
        service_id="svc_a",
        retention_deadline=NOW + timedelta(days=1),
        deletion_deadline=NOW + timedelta(days=2),
    )
    archive.publish(valid_m, valid_b)

    for idx, bad_payload in enumerate((b"[]", b"null", b"123", b'"str"')):
        mid = f"bad-nondict-{idx}"
        store.put(f"archive/manifests/{mid}.json", bad_payload)
        store.put(
            f"archive/manifests/{mid}.commit",
            json.dumps(
                {
                    "manifest_id": mid,
                    "manifest_checksum": "sha256:any",
                    "artifact_checksum": "sha256:any",
                }
            ).encode(),
        )

    manifests = archive.list_committed_manifests(service_id="svc_a")
    assert [m.manifest_id for m in manifests] == ["m-valid"]


@pytest.mark.no_postgres
def test_read_committed_manifest_rejects_mismatched_internal_manifest_id() -> None:
    import hashlib

    store = InMemoryObjectStore()
    archive = ArchivePublication(store)

    inner_m, inner_b = make_test_archive_manifest(
        "inner_id",
        NOW - timedelta(hours=2),
        NOW - timedelta(hours=1),
        ({"event_id": "1"},),
        service_id="svc_a",
        retention_deadline=NOW + timedelta(days=1),
        deletion_deadline=NOW + timedelta(days=2),
    )
    archive.publish(inner_m, inner_b)

    inner_json = store.get("archive/manifests/inner_id.json")
    manifest_hash = f"sha256:{hashlib.sha256(inner_json).hexdigest()}"
    store.put("archive/manifests/outer_id.json", inner_json)
    store.put(
        "archive/manifests/outer_id.commit",
        json.dumps(
            {
                "manifest_id": "outer_id",
                "manifest_checksum": manifest_hash,
                "artifact_checksum": inner_m.artifact.checksum,
            }
        ).encode(),
    )

    assert archive.read_committed_manifest("outer_id") is None
    assert archive.is_replayable("outer_id") is False
    assert archive.read_committed_manifest("inner_id") is not None


@pytest.mark.no_postgres
def test_register_manifest_idempotent_preserves_value_error_when_archive_manifest_raises_key_error() -> None:
    class _FencingControl:
        def register_archive_manifest(self, *_args: Any, **_kwargs: Any) -> Any:
            raise ValueError("owner epoch fenced")

        def archive_manifest(self, manifest_id: str) -> Any:
            raise KeyError(manifest_id)

    manifest, _ = make_test_archive_manifest(
        "m-fence",
        NOW - timedelta(hours=1),
        NOW - timedelta(minutes=30),
        ({"event_id": "1"},),
        service_id="svc_a",
        retention_deadline=NOW + timedelta(days=1),
        deletion_deadline=NOW + timedelta(days=2),
    )
    with pytest.raises(ValueError, match="owner epoch fenced"):
        _register_manifest_idempotent(
            _FencingControl(),
            manifest,
            owner_epoch=1,
            state=ArchiveState.MANIFEST_COMMITTED,
        )


@pytest.mark.no_postgres
@pytest.mark.parametrize("use_paginator", [True, False])
def test_s3_object_store_list_keys_ignores_directory_marker_object(use_paginator: bool) -> None:
    s3 = _FakeS3Client(use_paginator=use_paginator, page_size=2)
    store = S3ObjectStore(s3, bucket="my-bucket", prefix="high-scale/archive")
    # Zero-byte directory placeholder equal to f"{self._prefix}/"
    s3.put_object(Bucket="my-bucket", Key="high-scale/archive/", Body=b"")
    store.put("archive/manifests/m1.commit", b"1")

    keys = store.list_keys("")
    assert "" not in keys
    assert keys == ["archive/manifests/m1.commit"]


@pytest.mark.no_postgres
def test_restore_raises_clear_runtime_error_when_clickhouse_unconfigured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cold_start_restore, "get_clickhouse_client", lambda: None)
    monkeypatch.setattr(cold_start_restore.svcconfig, "load_clickhouse_config", lambda: None)

    with pytest.raises(RuntimeError, match="ClickHouse is not configured"):
        restore_from_fos(
            service_id="svc_noch",
            bucket="bkt",
            region="us-east-1",
            fos_access_key="ak",
            fos_secret_key="sk",
            restore_serving_state=True,
            s3_client=_FakeS3Client(),
            archive=ArchivePublication(InMemoryObjectStore()),
            control=_InMemoryControlPlane(),
            save_config_fn=lambda _sid, _cfg: None,
            clear_fos_client_fn=lambda _sid: None,
            ensure_pg_schema_fn=lambda: True,
            now=NOW,
        )


@pytest.mark.no_postgres
def test_record_restored_archive_sources_chunks_large_inserts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from backend.high_scale.archive.manifests import ArchiveSourceEntry

    executed_counts: list[int] = []

    class _FakeConn:
        def execute(self, _sql: str, params: tuple[Any, ...] = ()) -> Any:
            executed_counts.append(len(params) // 11)
            return SimpleNamespace(fetchall=lambda: [], fetchone=lambda: None)

        @contextmanager
        def transaction(self):
            yield self

    class _FakePool:
        @contextmanager
        def connection(self):
            yield _FakeConn()

    monkeypatch.setattr(cold_start_restore, "_SOURCE_INSERT_CHUNK_SIZE", 2)
    base_manifest, _ = make_test_archive_manifest(
        "m-chunk",
        NOW - timedelta(hours=1),
        NOW - timedelta(minutes=30),
        ({"event_id": "1"},),
        service_id="svc_chunk",
        domain="request",
        retention_deadline=NOW + timedelta(days=1),
        deletion_deadline=NOW + timedelta(days=2),
    )
    sources = tuple(
        ArchiveSourceEntry(
            key=f"raw/obj-{i}.json.gz",
            checksum=f"sha256:{i:064x}",
            size_bytes=100,
            row_start=i,
            row_count=1,
            deletion_deadline=NOW + timedelta(days=2),
            version="v1",
        )
        for i in range(5)
    )
    manifest = ArchiveManifest(
        manifest_id=base_manifest.manifest_id,
        source=None,
        artifact=base_manifest.artifact,
        coverage_start=base_manifest.coverage_start,
        coverage_end=base_manifest.coverage_end,
        retention_deadline=base_manifest.retention_deadline,
        deletion_authorization_deadline=base_manifest.deletion_authorization_deadline,
        archive_epoch=base_manifest.archive_epoch,
        service_id=base_manifest.service_id,
        domain=base_manifest.domain,
        sources=sources,
    )

    control = PostgresControlPlane(pool=_FakePool())
    count = _record_restored_archive_sources(control, manifest, owner_epoch=1, status="acknowledged")
    assert count == 5
    assert executed_counts == [2, 2, 1]


@pytest.mark.no_postgres
def test_restore_allows_empty_archive_prefix_for_bucket_root_archives() -> None:
    s3 = _FakeS3Client()
    root_store = S3ObjectStore(s3, bucket="bkt", prefix="")
    root_archive = ArchivePublication(root_store)
    m, b = make_test_archive_manifest(
        "m-root",
        NOW - timedelta(hours=2),
        NOW - timedelta(hours=1),
        ({"event_id": "1"},),
        service_id="svc_root",
        domain="request",
        retention_deadline=NOW + timedelta(days=1),
        deletion_deadline=NOW + timedelta(days=2),
    )
    root_archive.publish(m, b)
    assert ("bkt", "archive/manifests/m-root.commit") in s3.objects

    control = _InMemoryControlPlane()
    result = restore_from_fos(
        service_id="svc_root",
        bucket="bkt",
        region="us-east-1",
        archive_prefix="",
        fos_access_key="ak",
        fos_secret_key="sk",
        restore_serving_state=False,
        s3_client=s3,
        control=control,
        save_config_fn=lambda _sid, _cfg: None,
        clear_fos_client_fn=lambda _sid: None,
        ensure_pg_schema_fn=lambda: True,
        now=NOW,
    )
    assert result.manifests_registered == 1
    assert "m-root" in control.manifests


@pytest.mark.no_postgres
def test_compute_warm_window_and_replay_with_90_day_old_manifest() -> None:
    archive = ArchivePublication(InMemoryObjectStore())
    m_old, b_old = make_test_archive_manifest(
        "m-90d",
        NOW - timedelta(days=90),
        NOW - timedelta(days=90) + timedelta(minutes=15),
        (
            {
                "event_id": "old-90d",
                "timestamp": (NOW - timedelta(days=90) + timedelta(minutes=5)).isoformat(),
                "status": 200,
                "url": "/legacy",
                "country": "US",
            },
        ),
        service_id="svc_old",
        domain="request",
        retention_deadline=NOW - timedelta(days=60),
        deletion_deadline=NOW - timedelta(days=59),
    )
    archive.publish(m_old, b_old)

    window = _compute_warm_window([m_old], NOW, {"log_retention_days": 30})
    assert window >= timedelta(days=90, hours=1)

    control = _InMemoryControlPlane()
    replay_ch = ReplayClickHouseClient()
    serving_pub = ClickHousePublication(InMemoryBatchManifestStore(), replay_ch)
    agg_pub = ClickHousePublication(InMemoryBatchManifestStore(), ReplayClickHouseClient())

    result = restore_from_fos(
        service_id="svc_old",
        bucket="bkt",
        region="us-east-1",
        fos_access_key="ak",
        fos_secret_key="sk",
        restore_serving_state=True,
        s3_client=_FakeS3Client(),
        archive=archive,
        control=control,
        serving_publication=serving_pub,
        aggregates_publication=agg_pub,
        save_config_fn=lambda _sid, _cfg: None,
        clear_fos_client_fn=lambda _sid: None,
        ensure_pg_schema_fn=lambda: True,
        now=NOW,
    )
    assert result.manifests_registered == 1
    assert control.manifests["m-90d"].state is ArchiveState.SOURCE_DELETED
    assert result.domains_rebuilt["request"]["full_rebuild_complete"] is True
    assert result.domains_rebuilt["request"]["manifests_replayed"] == 1
    assert result.domains_rebuilt["request"]["rows_replayed"] == 1


# ── 10. Iteration 3 Exception Chain & Nested DR State Regression Tests ───────


@pytest.mark.no_postgres
@pytest.mark.security_regression
@pytest.mark.parametrize("phase", ["pre_mint", "post_mint"])
@pytest.mark.parametrize("exc_cls", [RuntimeError, ValueError])
def test_restore_from_fos_scrubbed_exceptions_have_no_cause_or_context(
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    exc_cls: type[Exception],
) -> None:
    sentinel_api = "API+SENTINEL/TOKEN=789&c=3"
    minted_ak = "AK+SENTINEL/KEY=123&a=1"
    minted_sk = "SK+SENTINEL/SECRET=456&b=2"

    def _leak_payload(secrets: tuple[str, ...]) -> str:
        parts: list[str] = []
        for idx, secret in enumerate(secrets):
            parts.append(f"s{idx}={secret} q{idx}={quote(secret, safe='')} qp{idx}={quote_plus(secret)}")
        return "boom " + " ".join(parts)

    if phase == "pre_mint":
        active_secrets = (sentinel_api,)

        def _failing_mint(*_args: Any, **_kwargs: Any) -> dict[str, str]:
            raise exc_cls(_leak_payload(active_secrets))

        mint_fn = _failing_mint
    else:
        active_secrets = (sentinel_api, minted_ak, minted_sk)

        def _ok_mint(*_args: Any, **_kwargs: Any) -> dict[str, str]:
            return {"id": minted_ak, "access_key": minted_ak, "secret_key": minted_sk}

        def _failing_s3(*, access_key: str, secret_key: str, region: str) -> Any:
            raise exc_cls(_leak_payload((sentinel_api, access_key, secret_key)))

        mint_fn = _ok_mint
        monkeypatch.setattr(cold_start_restore, "_build_direct_fos_s3_client", _failing_s3)

    with pytest.raises(exc_cls) as exc_info:
        restore_from_fos(
            service_id="svc_ctx",
            bucket="bkt",
            region="us-east-1",
            fastly_api_key=sentinel_api,
            ensure_fos_key_fn=mint_fn,
            now=NOW,
        )

    caught = exc_info.value
    assert caught.__cause__ is None
    assert caught.__context__ is None

    rendered = f"{caught!s}\n{caught!r}"
    for secret in active_secrets:
        for variant in (secret, quote(secret, safe=""), quote_plus(secret)):
            assert variant not in rendered
    assert "[REDACTED]" in str(caught)


@pytest.mark.no_postgres
@pytest.mark.security_regression
@pytest.mark.parametrize(("exc_cls", "expected_status"), [(ValueError, 400), (RuntimeError, 500)])
def test_admin_api_endpoint_http_exception_has_no_cause_or_context(
    monkeypatch: pytest.MonkeyPatch,
    exc_cls: type[Exception],
    expected_status: int,
) -> None:
    from backend.models.admin.high_scale import HighScaleRestoreFromFosRequest

    sentinel_api = "API+SENTINEL/TOKEN=789&c=3"
    sentinel_sk = "SK+SENTINEL/SECRET=456&b=2"

    def _boom(**_kwargs: Any) -> ColdStartRestoreResult:
        raise exc_cls(f"api={sentinel_api} sk={sentinel_sk} q={quote(sentinel_sk, safe='')}")

    monkeypatch.setattr(cold_start_restore, "restore_from_fos", _boom)
    req = HighScaleRestoreFromFosRequest(
        service_id="svc_http_ctx",
        bucket="bkt",
        region="us-east-1",
        fastly_api_key=sentinel_api,
        fos_access_key="ak",
        fos_secret_key=sentinel_sk,
    )

    with pytest.raises(HTTPException) as exc_info:
        high_scale_router_mod.restore_from_fos_endpoint(req)

    caught = exc_info.value
    assert caught.status_code == expected_status
    assert caught.__cause__ is None
    assert caught.__context__ is None

    rendered = f"{caught!s}\n{caught!r}"
    for secret in (sentinel_api, sentinel_sk):
        for variant in (secret, quote(secret, safe=""), quote_plus(secret)):
            assert variant not in rendered


@pytest.mark.no_postgres
def test_mismatched_nested_service_config_service_id_rejected_without_writes() -> None:
    s3 = _FakeS3Client()
    dr_state = {"service_id": "", "service_config": {"service_id": "svc_OTHER"}}
    s3.put_object(Bucket="bkt", Key="meta/dr_state.json", Body=json.dumps(dr_state).encode("utf-8"))

    con = _make_sqlite_metadata_db()
    saved: dict[str, dict[str, Any]] = {}
    cleared: list[str] = []
    pg_ensured = False

    def _fake_ensure_pg() -> bool:
        nonlocal pg_ensured
        pg_ensured = True
        return True

    with pytest.raises(ValueError, match="belongs to service 'svc_OTHER', expected 'svc_TARGET'"):
        restore_from_fos(
            service_id="svc_TARGET",
            bucket="bkt",
            region="us-east-1",
            fos_access_key="ak",
            fos_secret_key="sk",
            restore_serving_state=False,
            s3_client=s3,
            archive=ArchivePublication(InMemoryObjectStore()),
            control=_InMemoryControlPlane(),
            metadata_con=con,
            save_config_fn=lambda sid, cfg: saved.__setitem__(sid, cfg),
            clear_fos_client_fn=cleared.append,
            ensure_pg_schema_fn=_fake_ensure_pg,
            now=NOW,
        )

    assert saved == {}
    assert cleared == []
    assert pg_ensured is False
    assert con.execute("SELECT COUNT(*) FROM views").fetchone()[0] == 0
    assert con.execute("SELECT COUNT(*) FROM alerts").fetchone()[0] == 0
    assert con.execute("SELECT COUNT(*) FROM scoring_labels").fetchone()[0] == 0
    assert con.execute("SELECT COUNT(*) FROM audit_logs").fetchone()[0] == 0

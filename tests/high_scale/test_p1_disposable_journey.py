"""Disposable real-store P1 provisioning, custom-field ingest, masking, and revocation journey."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit
from uuid import uuid4

import httpx
import psycopg
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient
from testcontainers.postgres import PostgresContainer

import backend.core.oauth as oidc
from backend import config as svcconfig
from backend.config import ClickHouseConfig
from backend.core import share_db
from backend.core.clickhouse import ClickHouseClient
from backend.core.metadata import base as pg_meta
from backend.high_scale.archive import (
    ArchivePublication,
    ClickHouseBatchAdapter,
    ClickHousePublication,
    InMemoryObjectStore,
    PostgresBatchManifestStore,
    install_clickhouse_schema,
)
from backend.high_scale.ingest_controller import (
    AggregateBatchAdapter,
    HighScaleIngestController,
    HighScaleService,
    HighScaleServiceRegistration,
    SourceObjectDescriptor,
    get_high_scale_service_registry,
    register_high_scale_services,
)
from backend.high_scale.postgres_control import PostgresControlPlane
from backend.main import app
from backend.utils import tunnel
from tests.e2e_security.cloud_harness import CloudTestServer
from tests.remote_access import conftest as oauth_fixtures

pytestmark = pytest.mark.no_postgres

_CLICKHOUSE_IMAGE = os.environ.get("RUM_CLICKHOUSE_TEST_IMAGE", "clickhouse/clickhouse-server:25.8.4.13")
_FIELD_NAME = "journey_marker"
_RAW_IP = "198.51.100.42"


@contextmanager
def _colima_docker_env() -> Iterator[None]:
    previous = os.environ.get("DOCKER_HOST"), os.environ.get("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE")
    colima_socket = Path.home() / ".colima/default/docker.sock"
    if previous[0] is None and colima_socket.exists():
        os.environ["DOCKER_HOST"] = f"unix://{colima_socket}"
    if str(colima_socket) in os.environ.get("DOCKER_HOST", "") and previous[1] is None:
        os.environ["TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE"] = "/var/run/docker.sock"
    try:
        yield
    finally:
        for name, value in zip(("DOCKER_HOST", "TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE"), previous, strict=True):
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


@contextmanager
def _isolated_postgres_dsn() -> Iterator[str]:
    external = (
        os.environ.get("DISPOSABLE_POSTGRES_DSN")
        or os.environ.get("TEST_METADATA_DSN")
        or os.environ.get("METADATA_DSN")
    )
    if external:
        parsed = urlsplit(external)
        maint_dsn = urlunsplit(parsed._replace(path="/postgres"))
        db_name = f"fla_p1_journey_{uuid4().hex[:8]}"
        with psycopg.connect(maint_dsn, autocommit=True) as conn:
            conn.execute(f'CREATE DATABASE "{db_name}"')
        isolated = urlunsplit(parsed._replace(path=f"/{db_name}"))
        try:
            yield isolated
        finally:
            pg_meta.close_all_pg_connections()
            pg_meta.reset_pg_pool_for_tests()
            with psycopg.connect(maint_dsn, autocommit=True) as conn:
                conn.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()",
                    (db_name,),
                )
                conn.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
        return

    with _colima_docker_env():
        with PostgresContainer("postgres:16-alpine", driver="psycopg") as postgres:
            yield postgres.get_connection_url().replace("postgresql+psycopg://", "postgresql://", 1)


@contextmanager
def _isolated_clickhouse_client() -> Iterator[ClickHouseClient]:
    external = os.environ.get("DISPOSABLE_CLICKHOUSE_HTTP_URL")
    db_name = f"fla_p1_jch_{uuid4().hex[:8]}"
    container_name: str | None = None
    if external:
        parsed = urlsplit(external)
        base = f"{parsed.scheme}://{parsed.hostname}:{parsed.port or (8443 if parsed.scheme == 'https' else 8123)}/"
        auth = (parsed.username or "admin", parsed.password or "") if (parsed.username or parsed.password) else None
    else:
        import socket

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            port = int(s.getsockname()[1])
        container_name = f"fla-p1-journey-ch-{uuid4().hex[:8]}"
        with _colima_docker_env():
            subprocess.run(
                [
                    "docker",
                    "run",
                    "-d",
                    "--rm",
                    "--name",
                    container_name,
                    "-p",
                    f"127.0.0.1:{port}:8123",
                    "-e",
                    "CLICKHOUSE_USER=admin",
                    "-e",
                    "CLICKHOUSE_PASSWORD=adminpw",
                    "-e",
                    "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
                    _CLICKHOUSE_IMAGE,
                ],
                check=True,
                capture_output=True,
            )
        base = f"http://127.0.0.1:{port}/"
        auth = ("admin", "adminpw")
    try:
        deadline = datetime.now(UTC) + timedelta(seconds=45)
        while datetime.now(UTC) < deadline:
            try:
                if httpx.get(f"{base}ping", timeout=2.0).text.strip() == "Ok.":
                    break
            except httpx.HTTPError:
                pass
            import time

            time.sleep(0.5)
        else:
            raise RuntimeError("ClickHouse container did not become ready")
        httpx.post(base, content=f"CREATE DATABASE IF NOT EXISTS {db_name}", auth=auth, timeout=15.0).raise_for_status()
        cfg_url = external if external else base.replace("http://", "http://admin:adminpw@", 1)
        parsed = urlsplit(cfg_url)
        client = ClickHouseClient(
            ClickHouseConfig(
                host=parsed.hostname or "127.0.0.1",
                port=parsed.port or 8123,
                database=db_name,
                user=parsed.username or "admin",
                password=parsed.password or "",
                secure=parsed.scheme == "https",
            )
        )
        client._enable_cf_rewrite_in_tests = True  # type: ignore[attr-defined]
        try:
            install_clickhouse_schema(client)
            yield client
        finally:
            try:
                httpx.post(base, content=f"DROP DATABASE IF EXISTS {db_name}", auth=auth, timeout=15.0)
            finally:
                client.close()
    finally:
        if container_name is not None:
            with _colima_docker_env():
                subprocess.run(["docker", "rm", "-f", container_name], check=False, capture_output=True)


def _cloud_custom_field(label: str, expression: str) -> dict[str, Any]:
    return {
        "name": _FIELD_NAME,
        "label": label,
        "vcl_log_expression": expression,
        "collection_stage": "edge",
        "value_type": "string",
        "enabled": True,
        "bytes_estimate": 36,
        "duckdb_type": "VARCHAR",
        "byte_limit": 192,
        "show_in_logs": False,
    }


def _hs_custom_field(label: str, expression: str) -> dict[str, Any]:
    return {
        "name": _FIELD_NAME,
        "label": label,
        "vcl_log_expression": expression,
        "collection_stage": "edge",
        "value_type": "string",
        "sql_type": "VARCHAR",
        "enabled": True,
        "bytes_estimate": 36,
    }


def _response_cookie(response: Any, name: str) -> str:
    prefix = f"{name}="
    for line in response.headers.get_list("set-cookie"):
        first = line.split(";", 1)[0]
        if first.startswith(prefix):
            return first[len(prefix) :]
    raise AssertionError(f"missing {name} cookie")


def _run_cloud_disposable_journey() -> None:
    server = CloudTestServer()
    server.start()
    try:
        admin = server.register_admin(display_name="P1 Disposable Admin")
        sid = server.provision_fixture_service(admin, fos_prefix="p1-disp")
        svc_resp = server.request("GET", f"/api/admin/services/{sid}", session_cookie=admin.session_cookie)
        assert svc_resp.status_code == 200, svc_resp.text
        svc_info = svc_resp.json()["service"]
        bucket = str(svc_info["fos_bucket"])
        prefix = str(svc_info["fos_prefix"]).strip("/")
        key_prefix = f"{prefix}/" if prefix else ""

        def patch_fields(fields: list[dict[str, Any]], session_cookie: str | None = None) -> httpx.Response:
            gen = server.service_generation(admin, sid)
            req_kwargs: dict[str, Any] = {}
            if session_cookie:
                req_kwargs["client_ip"] = "198.51.100.77"
            return server.request(
                "PATCH",
                "/api/admin/log-fields/config",
                session_cookie=session_cookie or admin.session_cookie,
                json_body={
                    "generation": gen,
                    "service_id": sid,
                    "log_fields": {
                        "groups": ["A", "C"],
                        "custom_fields": fields,
                        "field_overrides": {},
                        "field_limits": {},
                    },
                },
                **req_kwargs,
            )

        def seed_and_ingest(ts_dt: datetime, rows: list[dict[str, Any]]) -> None:
            ndjson = ("\n".join(json.dumps(r) for r in rows) + "\n").encode("utf-8")
            raw_key = (
                f"{key_prefix}raw/request/"
                f"year={ts_dt.year:04d}/month={ts_dt.month:02d}/day={ts_dt.day:02d}/"
                f"hour={ts_dt.hour:02d}/minute={ts_dt.minute:02d}/b{ts_dt.minute:02d}.zst"
            )
            server.put_raw_log_object(bucket, raw_key, ndjson, zstd_compress=True)
            win_from = ts_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            win_to = (ts_dt + timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
            base_clock = int((ts_dt + timedelta(minutes=2, seconds=5)).timestamp())
            converted = False
            last_listed: Any = None
            for step in range(6):
                server.set_clock(base_clock + step * 6)
                server.run_ingest_cycle()
                listed = server.request(
                    "GET",
                    f"/api/files?dataset=request&from={win_from}&to={win_to}",
                    session_cookie=admin.session_cookie,
                )
                assert listed.status_code == 200, listed.text
                last_listed = listed.json()
                if last_listed.get("files"):
                    converted = True
                    server.set_clock(base_clock + (step + 1) * 6)
                    server.run_ingest_cycle()
                    break
            assert converted, f"raw log {raw_key} was not converted within 6 ticks: {last_listed}"

        base_dt = datetime.now(UTC).replace(second=0, microsecond=0) + timedelta(minutes=2)
        list_from = (base_dt - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        list_to = (base_dt + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")

        def read_all_parquet_rows() -> list[dict[str, Any]]:
            listed = server.request(
                "GET",
                f"/api/files?dataset=request&from={list_from}&to={list_to}",
                session_cookie=admin.session_cookie,
            )
            assert listed.status_code == 200, listed.text
            files = listed.json()["files"]
            assert files, f"expected ingested Parquet files, got {listed.json()}"
            out: list[dict[str, Any]] = []
            for entry in files:
                file_resp = server.request(
                    "GET",
                    f"/api/file?key={quote(entry['key'], safe='')}",
                    session_cookie=admin.session_cookie,
                )
                assert file_resp.status_code == 200, file_resp.text
                out.extend(pq.read_table(io.BytesIO(file_resp.content)).to_pylist())
            return out

        # Stage 1: Uncommitted custom field create/change/remove + committed create & real ingest -> Parquet.
        r_uncommitted = patch_fields([_cloud_custom_field("Temp marker", "req.http.X-Temp")])
        assert r_uncommitted.status_code == 200, r_uncommitted.text
        assert patch_fields([]).status_code == 200

        r_create = patch_fields([_cloud_custom_field("Journey marker", "req.http.X-Journey-One")])
        assert r_create.status_code == 200, r_create.text
        t0 = base_dt
        seed_and_ingest(
            t0,
            [
                {
                    "timestamp": (t0 + timedelta(seconds=10)).isoformat().replace("+00:00", "Z"),
                    "ip": _RAW_IP,
                    "status": 200,
                    "url": "/one",
                    _FIELD_NAME: "alpha-1",
                }
            ],
        )
        rows1 = read_all_parquet_rows()
        assert any(r.get(_FIELD_NAME) == "alpha-1" and r.get("ip") == _RAW_IP for r in rows1), rows1

        # Stage 2: Post-ingest change preserving `committed: true` + second real ingest -> Parquet.
        committed_field = next(
            f
            for f in server.request("GET", f"/api/admin/services/{sid}", session_cookie=admin.session_cookie).json()[
                "service"
            ]["log_fields"]["custom_fields"]
            if f["name"] == _FIELD_NAME
        )
        assert committed_field.get("committed") is True
        r_change = patch_fields(
            [{**committed_field, "label": "Journey marker v2", "vcl_log_expression": "req.http.X-Journey-Two"}]
        )
        assert r_change.status_code == 200, r_change.text
        t1 = base_dt + timedelta(minutes=3)
        seed_and_ingest(
            t1,
            [
                {
                    "timestamp": (t1 + timedelta(seconds=10)).isoformat().replace("+00:00", "Z"),
                    "ip": _RAW_IP,
                    "status": 200,
                    "url": "/two",
                    _FIELD_NAME: "beta-2",
                }
            ],
        )
        rows2 = read_all_parquet_rows()
        assert any(r.get(_FIELD_NAME) == "beta-2" for r in rows2), rows2

        # Stage 3: Deleting a committed field is locked (409); disabling keeps schema slot null.
        assert patch_fields([]).status_code == 409
        updated_field = next(
            f
            for f in server.request("GET", f"/api/admin/services/{sid}", session_cookie=admin.session_cookie).json()[
                "service"
            ]["log_fields"]["custom_fields"]
            if f["name"] == _FIELD_NAME
        )
        r_disable = patch_fields([{**updated_field, "enabled": False}])
        assert r_disable.status_code == 200, r_disable.text
        svc_after = server.request("GET", f"/api/admin/services/{sid}", session_cookie=admin.session_cookie).json()
        disabled = next(f for f in svc_after["service"]["log_fields"]["custom_fields"] if f["name"] == _FIELD_NAME)
        assert disabled["enabled"] is False
        t2 = base_dt + timedelta(minutes=6)
        seed_and_ingest(
            t2,
            [
                {
                    "timestamp": (t2 + timedelta(seconds=10)).isoformat().replace("+00:00", "Z"),
                    "ip": _RAW_IP,
                    "status": 200,
                    "url": "/three",
                }
            ],
        )
        rows3 = read_all_parquet_rows()
        row3 = next(r for r in rows3 if r.get("url") == "/three")
        assert row3.get(_FIELD_NAME) in (None, ""), row3

        # Stage 4: Saved view + masked analyst enforcement.
        create_view = server.request(
            "POST",
            "/api/views",
            session_cookie=admin.session_cookie,
            json_body={
                "service_id": sid,
                "name": "P1 Disposable Raw IP View",
                "filters_json": json.dumps({"client_ip": {"mode": "include", "values": [_RAW_IP]}}),
            },
        )
        assert create_view.status_code == 201, create_view.text

        analyst = server.invite_and_register(
            admin, role="analyst-no-pii", display_name="P1 Disposable Cloud Analyst", client_ip="198.51.100.77"
        )
        server.login_passkey(analyst, client_ip="198.51.100.77")

        assert patch_fields([], session_cookie=analyst.session_cookie).status_code == 403
        bootstrap = server.request(
            "GET", f"/api/bootstrap?service_id={sid}", session_cookie=analyst.session_cookie, client_ip="198.51.100.77"
        )
        assert bootstrap.status_code == 200 and bootstrap.json()["settings"]["mask_ips"] is True
        views = server.request(
            "GET", f"/api/views/{sid}", session_cookie=analyst.session_cookie, client_ip="198.51.100.77"
        )
        assert views.status_code == 200
        assert _RAW_IP not in views.text and "P1 Disposable Raw IP View" not in views.text

        first_key = server.request(
            "GET",
            f"/api/files?dataset=request&from={list_from}&to={list_to}",
            session_cookie=admin.session_cookie,
        ).json()["files"][0]["key"]
        assert (
            server.request(
                "GET",
                f"/api/file?key={quote(first_key, safe='')}",
                session_cookie=analyst.session_cookie,
                client_ip="198.51.100.77",
            ).status_code
            == 403
        )

        # Stage 5: Revocation invalidates analyst session immediately.
        users = server.request("GET", "/api/admin/users", session_cookie=admin.session_cookie).json()["users"]
        uid = next(u["id"] for u in users if u["display_name"] == "P1 Disposable Cloud Analyst")
        assert (
            server.request(
                "POST", f"/api/admin/users/{uid}/revoke", session_cookie=admin.session_cookie, json_body={}
            ).status_code
            == 200
        )
        revoked_boot = server.request(
            "GET", "/api/bootstrap", session_cookie=analyst.session_cookie, client_ip="198.51.100.77"
        )
        assert revoked_boot.json()["settings"]["needs_login"] is True
        assert (
            server.request(
                "GET",
                f"/api/files?dataset=request&from={list_from}&to={list_to}",
                session_cookie=analyst.session_cookie,
                client_ip="198.51.100.77",
            ).status_code
            == 401
        )
    finally:
        server.stop()


def _run_high_scale_disposable_journey(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with _isolated_postgres_dsn() as pg_dsn, _isolated_clickhouse_client() as ch_client:
        monkeypatch.setenv("METADATA_DSN", pg_dsn)
        monkeypatch.setenv("REMOTE_SHARE_DB_DIR", str(tmp_path / "system"))
        pg_meta.close_all_pg_connections()
        pg_meta.reset_pg_pool_for_tests()
        pg_meta.ensure_pg_schema(force=True)
        svcconfig.invalidate_config_cache()
        tunnel.reset_for_tests()

        sid = f"svc-p1-hs-{uuid4().hex[:8]}"
        objects = InMemoryObjectStore()
        archive = ArchivePublication(objects)
        control = PostgresControlPlane(pool=pg_meta.get_pg_pool(), pool_timeout=30)
        control.create_schema()
        manifest_store = PostgresBatchManifestStore(control)
        controller = HighScaleIngestController(
            control_plane=control,
            archive=archive,
            serving=ClickHousePublication(manifest_store, ClickHouseBatchAdapter(ch_client)),
            aggregates=ClickHousePublication(manifest_store, AggregateBatchAdapter(ch_client)),
            worker_id="worker-p1-journey",
            deletion_grace_seconds=60,
        )

        registry = get_high_scale_service_registry()
        previous_service = registry.resolve(sid)
        original_register = registry.register

        def register_with_real_ch(service: HighScaleService | HighScaleServiceRegistration) -> None:
            if isinstance(service, HighScaleServiceRegistration):
                register_high_scale_services(
                    registry,
                    client=ch_client,
                    service_ids=(service.service_id,),
                    cursor_secret=b"p1-disposable-hs-cursor-32bytes!",
                    manifest_catalog=control,
                    archive_for=lambda _sid: archive,
                )
            else:
                original_register(service)

        def fake_fastly(method: str, path: str, **_kwargs: Any) -> dict[str, Any]:
            assert method == "GET"
            if path == "/tokens/self":
                return {"id": "mock-token", "scope": "global", "customer_id": "mock-customer"}
            return {"id": sid, "customer_id": "mock-customer"}

        monkeypatch.setattr("backend.core.vcl_generator.fastly", fake_fastly)
        monkeypatch.setattr("backend.utils.pop_utils.fetch_pop_locations", lambda _tok: [])
        monkeypatch.setattr("backend.provision._sync_crontab", lambda: None)
        monkeypatch.setattr("backend.provision.validate_log_format", lambda *_a, **_kw: [])
        monkeypatch.setattr("backend.core.fos_client.get_source_for_service", lambda _sid: {"name": sid, "bucket": "b"})
        monkeypatch.setattr("backend.routers.services.core._committed_field_types", lambda *_a, **_kw: {})
        monkeypatch.setattr(registry, "register", register_with_real_ch)

        try:
            admin = TestClient(app, client=("127.0.0.1", 50000))
            prov = admin.post(
                "/api/provision/ingest",
                json={
                    "token": "test-sentinel-fastly-token-p1-hs",
                    "service_id": sid,
                    "service_name": "P1 HighScale Disposable",
                    "fos_bucket_name": "bucket-a",
                    "fos_region": "us-east-1",
                    "fos_access_key": "test-sentinel-fos-ak-p1-hs",  # gitleaks:allow
                    "fos_secret_key": "test-sentinel-fos-sk-p1-hs",  # gitleaks:allow
                    "log_fields": {"groups": ["A", "C"], "custom_fields": []},
                },
            )
            assert prov.status_code == 200, prov.text
            control.initialize_owner(sid, owner="high_scale", source_cursor="initial")

            def ingest_real_batch(idx: int, rows: list[dict[str, Any]]) -> None:
                payload = gzip.compress(("\n".join(json.dumps(r) for r in rows) + "\n").encode("utf-8"))
                key = f"raw/request/2027/01/15/12/batch-{idx}.ndjson.gz"
                desc = SourceObjectDescriptor(
                    key, f"sha256:{hashlib.sha256(payload).hexdigest()}", len(payload), f"v{idx}"
                )
                control.discover_source(
                    sid,
                    "request",
                    desc.object_key,
                    desc.checksum,
                    size_bytes=desc.size_bytes,
                    version=desc.version,
                    expected_owner="high_scale",
                    expected_owner_epoch=1,
                )
                claimed = control.claim_sources_batch(
                    sid, [desc.object_key], "worker-p1-journey", expected_owner="high_scale", expected_owner_epoch=1
                )
                res = controller.ingest_batch(
                    service_id=sid,
                    domain="request",
                    sources_payloads=[(desc, payload)],
                    claim_tokens=[claimed[0].claim_token],
                    now=datetime(2027, 1, 15, 12, 30, tzinfo=UTC),
                    max_dead_letter_ratio=0.0,
                )
                assert res.quarantined_events == 0

            # Stage 1: Custom field create + ingest -> query real ClickHouse.
            c_res = admin.post(
                f"/api/services/{sid}/custom-fields",
                json=_hs_custom_field("Journey marker", "req.http.X-Journey-One"),
            )
            assert c_res.status_code == 200, c_res.text
            ingest_real_batch(
                1,
                [
                    {
                        "timestamp": "2027-01-15T12:05:00Z",
                        "client_ip": _RAW_IP,
                        "status": 200,
                        "url": "/one",
                        "pop": "IAD",
                        "country": "US",
                        _FIELD_NAME: "alpha-1",
                    }
                ],
            )
            q1 = admin.post(
                f"/api/high-scale/services/{sid}/request-facts",
                json={"start_time": "2027-01-15T12:00:00Z", "end_time": "2027-01-15T13:00:00Z", "limit": 20},
            )
            assert q1.status_code == 200, q1.text
            assert any(
                r["custom_fields"].get(_FIELD_NAME) == "alpha-1" and r["client_ip"] == _RAW_IP
                for r in q1.json()["rows"]
            ), q1.json()

            # Stage 2: Custom field change + second real ingest.
            u_res = admin.patch(
                f"/api/services/{sid}/custom-fields/{_FIELD_NAME}",
                json={"label": "Journey marker v2", "vcl_log_expression": "req.http.X-Journey-Two"},
            )
            assert u_res.status_code == 200, u_res.text
            listed_after_update = admin.get(f"/api/services/{sid}/custom-fields")
            assert listed_after_update.status_code == 200
            assert (
                next(f for f in listed_after_update.json()["fields"] if f["name"] == _FIELD_NAME)["vcl_log_expression"]
                == "req.http.X-Journey-Two"
            )
            ingest_real_batch(
                2,
                [
                    {
                        "timestamp": "2027-01-15T12:10:00Z",
                        "client_ip": _RAW_IP,
                        "status": 200,
                        "url": "/two",
                        "pop": "IAD",
                        "country": "US",
                        _FIELD_NAME: "beta-2",
                    }
                ],
            )
            q2 = admin.post(
                f"/api/high-scale/services/{sid}/request-facts",
                json={"start_time": "2027-01-15T12:00:00Z", "end_time": "2027-01-15T13:00:00Z", "limit": 20},
            )
            assert q2.status_code == 200
            assert any(r["custom_fields"].get(_FIELD_NAME) == "beta-2" for r in q2.json()["rows"]), q2.json()

            # Stage 3: Custom field remove + third real ingest.
            d_res = admin.delete(f"/api/services/{sid}/custom-fields/{_FIELD_NAME}")
            assert d_res.status_code == 200, d_res.text
            listed_after_delete = admin.get(f"/api/services/{sid}/custom-fields")
            assert all(f["name"] != _FIELD_NAME for f in listed_after_delete.json()["fields"])
            ingest_real_batch(
                3,
                [
                    {
                        "timestamp": "2027-01-15T12:15:00Z",
                        "client_ip": _RAW_IP,
                        "status": 200,
                        "url": "/three",
                        "pop": "IAD",
                        "country": "US",
                    }
                ],
            )
            q3 = admin.post(
                f"/api/high-scale/services/{sid}/request-facts",
                json={"start_time": "2027-01-15T12:00:00Z", "end_time": "2027-01-15T13:00:00Z", "limit": 20},
            )
            assert any(r["url"] == "/three" for r in q3.json()["rows"]), q3.json()

            # Stage 4: Saved view + masked analyst OAuth session backed by real Postgres share_db.
            view_res = admin.post(
                "/api/views/",
                json={
                    "service_id": sid,
                    "name": "P1 Disposable HS Raw IP View",
                    "filters_json": json.dumps({"client_ip": {"mode": "include", "values": [_RAW_IP]}}),
                },
            )
            assert view_res.status_code == 201, view_res.text

            oauth_fixtures.configure_registry(monkeypatch, tmp_path)
            tunnel.get_tunnel_manager().start_sharing(public_endpoint=oauth_fixtures.PUBLIC_ENDPOINT)
            invite = admin.post(
                "/api/admin/share/invites",
                json={
                    "name": "P1 Disposable HS Analyst",
                    "email": "analyst@corp.com",
                    "auth_method": "oauth",
                    "oauth_provider": "google",
                    "service_ids": [sid],
                    "pii_policy": {"mask_ips": True},
                },
            )
            assert invite.status_code == 200, invite.text
            invite_id = invite.json()["id"]
            tos = share_db.get_latest_tos()
            if tos is not None:
                share_db.mark_tos_accepted(invite_id, str(tos["version"]))

            sealed = oidc.seal_flow_state(
                {
                    "provider": "google",
                    "state": "p1-disp-state",
                    "nonce": "p1-disp-nonce",
                    "verifier": "p1-disp-verifier",
                    "redirect_uri": "https://testserver/api/share/oauth/callback",
                    "return": "/dashboard",
                }
            )
            tok = oauth_fixtures.mint_id_token(nonce="p1-disp-nonce", email="analyst@corp.com")
            oidc.set_test_transport(oauth_fixtures.make_transport(token_body=oauth_fixtures.token_response(tok)))
            remote_headers = {
                "Host": oauth_fixtures.PUBLIC_ENDPOINT.removeprefix("https://"),
                "Origin": oauth_fixtures.PUBLIC_ENDPOINT,
                "X-Proxied-By-Caddy": "true",
                "Fastly-Client-IP": "203.0.113.77",
                "X-Fastly-Service-Id": sid,
            }
            with TestClient(app, client=("203.0.113.77", 50000), follow_redirects=False) as cb_client:
                cb = cb_client.get(
                    "/api/share/oauth/callback",
                    params={"code": "mock-code", "state": "p1-disp-state"},
                    headers={**oauth_fixtures.REMOTE_HEADERS, "Cookie": f"oauth_flow_state={sealed}"},
                )
                assert cb.status_code == 302, cb.text
                analyst_cookie = _response_cookie(cb, "analyst_session_id")

            analyst = TestClient(app, client=("203.0.113.77", 50000), cookies={"analyst_session_id": analyst_cookie})
            assert analyst.post(
                f"/api/services/{sid}/custom-fields",
                headers=remote_headers,
                json=_hs_custom_field("forbidden", "req.http.X-No"),
            ).status_code in (401, 403)
            analyst_views = analyst.get(f"/api/views/{sid}", headers=remote_headers)
            assert analyst_views.status_code == 200
            assert _RAW_IP not in analyst_views.text

            analyst_facts = analyst.post(
                f"/api/high-scale/services/{sid}/request-facts",
                headers=remote_headers,
                json={"start_time": "2027-01-15T12:00:00Z", "end_time": "2027-01-15T13:00:00Z", "limit": 20},
            )
            assert analyst_facts.status_code == 200, analyst_facts.text
            assert _RAW_IP not in analyst_facts.text
            assert all(r["client_ip"] != _RAW_IP for r in analyst_facts.json()["rows"])

            # Stage 5: Revocation rejects subsequent bootstrap & data queries.
            assert admin.post(f"/api/admin/share/invites/{invite_id}/revoke").status_code == 200
            revoked_boot = analyst.get("/api/bootstrap", headers=remote_headers)
            assert revoked_boot.json()["settings"]["needs_login"] is True
            assert analyst.post(
                f"/api/high-scale/services/{sid}/request-facts",
                headers=remote_headers,
                json={"start_time": "2027-01-15T12:00:00Z", "end_time": "2027-01-15T13:00:00Z", "limit": 20},
            ).status_code in (401, 403)
        finally:
            registry._services.pop(sid, None) if hasattr(registry, "_services") else None  # type: ignore[attr-defined]
            if previous_service is not None:
                original_register(previous_service)
            share_db.close_all_connections()
            tunnel.reset_for_tests()
            pg_meta.close_all_pg_connections()
            pg_meta.reset_pg_pool_for_tests()


@pytest.mark.parametrize("edition", ["high_scale", "cloud"])
def test_p1_disposable_real_store_journey(edition: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Disposable real-store P1 journey across High-Scale (PG+CH) and Cloud (native Parquet)."""
    if os.environ.get("RUN_DISPOSABLE_P1_JOURNEY") != "1":
        pytest.skip("set RUN_DISPOSABLE_P1_JOURNEY=1 to run the disposable real-store P1 journey")
    if edition == "cloud":
        _run_cloud_disposable_journey()
        return
    if edition == "high_scale":
        _run_high_scale_disposable_journey(tmp_path, monkeypatch)
        return
    raise AssertionError(f"unexpected edition: {edition}")

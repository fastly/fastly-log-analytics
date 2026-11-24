"""Offline Cloud/High-Scale adapters for the P1 customer journey contract."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from fastapi.testclient import TestClient

import backend.core.oauth as oidc
import backend.high_scale.ingest_controller as high_scale_ingest
from backend import config as svcconfig
from backend.core import share_db
from backend.high_scale.archive import ServingWatermark
from backend.high_scale.ingest_controller import (
    HighScaleService,
    HighScaleServiceRegistration,
)
from backend.main import app
from tests.e2e_security.cloud_harness import CloudTestServer, PasskeyIdentity
from tests.e2e_security.conftest import RecordingClickHouseClient, make_client
from tests.remote_access import conftest as oauth_fixtures

REPO_ROOT = Path(__file__).resolve().parents[2]
CANONICAL_FIXTURE = REPO_ROOT / "tests" / "fixtures" / "canonical_ingest.ndjson"
FIELD_NAME = "journey_marker"


@dataclass(frozen=True)
class AttachmentResult:
    service_id: str
    initially_absent: bool
    persisted_registration_count: int
    activation_visible: bool
    attached: bool


@dataclass(frozen=True)
class AnalystCredential:
    mask_ips: bool
    session_cookie: str
    invite_id: str
    subject_id: str


@dataclass(frozen=True)
class MaskingResult:
    policy_enabled: bool
    raw_ip_absent: bool
    raw_file_denied: bool
    raw_file_positive_control: bool


@dataclass(frozen=True)
class CustomFieldResult:
    read_back_expression: str
    removed: bool


@dataclass(frozen=True)
class RevocationResult:
    old_session_rejected: bool


class P1JourneyAdapter(Protocol):
    kind: str

    def setup_and_attach(self) -> AttachmentResult: ...

    def invite_and_sign_in(self) -> AnalystCredential: ...

    def observe_masking(self, analyst: AnalystCredential) -> MaskingResult: ...

    def change_and_remove_custom_field(self) -> CustomFieldResult: ...

    def revoke(self, analyst: AnalystCredential) -> RevocationResult: ...


def _response_cookie(response: Any, name: str) -> str:
    prefix = f"{name}="
    for line in response.headers.get_list("set-cookie"):
        first = line.split(";", 1)[0]
        if first.startswith(prefix):
            return first[len(prefix) :]
    raise AssertionError(f"missing {name} cookie in response")


def _canonical_request_row() -> dict[str, Any]:
    rows = []
    for line in CANONICAL_FIXTURE.read_text(encoding="utf-8").splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            rows.append(value)
    stable = next(row for row in rows if row.get("rid") == "canonical-stable")
    return {
        **stable,
        "event_id": stable["rid"],
        "event_timestamp": stable["timestamp"],
        "ingested_at": stable["timestamp"],
        "owner_epoch": 1,
        "client_ip": "203.0.113.55",
        "method": "GET",
        "host": "shop.example.test",
        "pop": "IAD",
        "country": "US",
        "cache_status": "HIT",
        "custom_fields": {},
    }


def _field(*, label: str, expression: str, cloud: bool) -> dict[str, Any]:
    field = {
        "name": FIELD_NAME,
        "label": label,
        "vcl_log_expression": expression,
        "collection_stage": "edge",
        "value_type": "string",
        "enabled": True,
        "bytes_estimate": 36,
    }
    field["duckdb_type" if cloud else "sql_type"] = "VARCHAR"
    if cloud:
        field |= {"byte_limit": 192, "show_in_logs": False}
    return field


class HighScaleP1JourneyAdapter:
    kind = "high_scale"

    def __init__(
        self,
        *,
        monkeypatch: Any,
        tmp_path: Path,
        clickhouse: RecordingClickHouseClient,
    ) -> None:
        self._monkeypatch = monkeypatch
        self._tmp_path = tmp_path
        self._clickhouse = clickhouse
        self._service_id = "svc-p1-journey"

    def setup_and_attach(self) -> AttachmentResult:
        initial_services = svcconfig.list_service_ids()
        registry = high_scale_ingest.get_high_scale_service_registry()
        initially_absent = self._service_id not in initial_services and registry.resolve(self._service_id) is None
        registration_calls: list[str] = []
        original_register = registry.register

        def register(service: HighScaleService | HighScaleServiceRegistration) -> None:
            registration_calls.append(service.service_id)
            if isinstance(service, HighScaleServiceRegistration):
                original_register(
                    HighScaleService(
                        service_id=service.service_id,
                        client=self._clickhouse,
                        cursor_secret=b"test-sentinel-cursor-secret-p1-32b",
                        request_watermark=lambda: ServingWatermark(
                            service.service_id,
                            "request",
                            1,
                            datetime(2027, 1, 15, 0, 0, tzinfo=UTC),
                            datetime(2027, 1, 15, 23, 59, tzinfo=UTC),
                            "cursor",
                            "event",
                            "event",
                            True,
                        ),
                    )
                )
                return
            original_register(service)

        def fastly(method: str, path: str, **kwargs: Any) -> dict[str, Any]:
            assert method == "GET"
            if path == "/tokens/self":
                return {"id": "mock-token", "scope": "global", "customer_id": "mock-customer"}
            assert path == f"/service/{self._service_id}"
            return {"id": self._service_id, "customer_id": "mock-customer"}

        self._monkeypatch.setattr("backend.core.vcl_generator.fastly", fastly)
        self._monkeypatch.setattr("backend.utils.pop_utils.fetch_pop_locations", lambda token: [])
        self._monkeypatch.setattr("backend.provision._sync_crontab", lambda: None)
        self._monkeypatch.setattr(registry, "register", register)
        response = make_client("127.0.0.1").post(
            "/api/provision/ingest",
            json={
                "token": "test-sentinel-fastly-token-p1-0001",
                "service_id": self._service_id,
                "service_name": "P1 Journey Fixture",
                "fos_bucket_name": "bucket-a",
                "fos_region": "us-east-1",
                "fos_access_key": "test-sentinel-fos-access-p1",
                "fos_secret_key": "test-sentinel-fos-secret-p1",
                "log_fields": {"groups": ["A", "C"], "custom_fields": []},
            },
        )
        assert response.status_code == 200, response.text
        persisted = [service_id for service_id in svcconfig.list_service_ids() if service_id == self._service_id]
        bootstrap = make_client("127.0.0.1").get("/api/bootstrap")
        assert bootstrap.status_code == 200, bootstrap.text
        activated = [
            service for service in bootstrap.json()["services"] if service.get("service_id") == self._service_id
        ]
        return AttachmentResult(
            response.json()["service_id"],
            initially_absent,
            len(persisted) if registration_calls == [self._service_id] else 0,
            len(activated) == 1,
            response.json()["ok"] is True and registry.resolve(self._service_id) is not None,
        )

    def invite_and_sign_in(self) -> AnalystCredential:
        oauth_fixtures.configure_registry(self._monkeypatch, self._tmp_path)
        from backend.utils import tunnel

        tunnel.get_tunnel_manager().start_sharing(public_endpoint=oauth_fixtures.PUBLIC_ENDPOINT)
        admin = make_client("127.0.0.1")
        invite_response = admin.post(
            "/api/admin/share/invites",
            json={
                "name": "P1 Journey Analyst",
                "email": "analyst@corp.com",
                "auth_method": "oauth",
                "oauth_provider": "google",
                "service_ids": [self._service_id],
                "pii_policy": {"mask_ips": True},
            },
        )
        assert invite_response.status_code == 200, invite_response.text
        invite = invite_response.json()
        tos = share_db.get_latest_tos()
        if tos is not None:
            share_db.mark_tos_accepted(invite["id"], str(tos["version"]))

        state = "p1-state"
        nonce = "p1-nonce"
        sealed = oidc.seal_flow_state(
            {
                "provider": "google",
                "state": state,
                "nonce": nonce,
                "verifier": "p1-verifier",
                "redirect_uri": "https://testserver/api/share/oauth/callback",
                "return": "/dashboard",
            }
        )
        token = oauth_fixtures.mint_id_token(nonce=nonce, email="analyst@corp.com")
        oidc.set_test_transport(oauth_fixtures.make_transport(token_body=oauth_fixtures.token_response(token)))

        with TestClient(app, client=("203.0.113.77", 50000), follow_redirects=False) as analyst_client:
            callback = analyst_client.get(
                "/api/share/oauth/callback",
                params={"code": "mock-code", "state": state},
                headers={**oauth_fixtures.REMOTE_HEADERS, "Cookie": f"oauth_flow_state={sealed}"},
            )
            assert callback.status_code == 302, callback.text
            assert callback.headers["location"] == "/dashboard"
            session_cookie = _response_cookie(callback, "analyst_session_id")

        return AnalystCredential(
            mask_ips=True,
            session_cookie=session_cookie,
            invite_id=invite["id"],
            subject_id="google-sub-1",
        )

    def observe_masking(self, analyst: AnalystCredential) -> MaskingResult:
        row = _canonical_request_row()
        self._clickhouse.set_handler(lambda sql, params, settings: [dict(row)])
        client = make_client(
            "203.0.113.77",
            cookies={"analyst_session_id": analyst.session_cookie},
        )
        response = client.post(
            f"/api/high-scale/services/{self._service_id}/request-facts",
            headers={
                "Host": oauth_fixtures.PUBLIC_ENDPOINT.removeprefix("https://"),
                "Origin": oauth_fixtures.PUBLIC_ENDPOINT,
                "X-Proxied-By-Caddy": "true",
                "Fastly-Client-IP": "203.0.113.77",
                "X-Fastly-Service-Id": self._service_id,
            },
            json={
                "start_time": "2027-01-15T00:00:00Z",
                "end_time": "2027-01-15T09:00:00Z",
                "limit": 1,
            },
        )
        assert response.status_code == 200, response.text
        observed_ip = response.json()["rows"][0]["client_ip"]
        return MaskingResult(
            policy_enabled=observed_ip != row["client_ip"],
            raw_ip_absent=row["client_ip"] not in response.text,
            raw_file_denied=False,
            raw_file_positive_control=False,
        )

    def change_and_remove_custom_field(self) -> CustomFieldResult:
        client = make_client("127.0.0.1")
        create = client.post(
            f"/api/services/{self._service_id}/custom-fields",
            json=_field(label="Journey marker", expression="req.http.X-Journey-One", cloud=False),
        )
        assert create.status_code == 200, create.text
        update = client.patch(
            f"/api/services/{self._service_id}/custom-fields/{FIELD_NAME}",
            json={"label": "Journey marker changed", "vcl_log_expression": "req.http.X-Journey-Two"},
        )
        assert update.status_code == 200, update.text
        listed_after_update = client.get(f"/api/services/{self._service_id}/custom-fields")
        assert listed_after_update.status_code == 200, listed_after_update.text
        persisted = next(field for field in listed_after_update.json()["fields"] if field["name"] == FIELD_NAME)
        remove = client.delete(f"/api/services/{self._service_id}/custom-fields/{FIELD_NAME}")
        assert remove.status_code == 200, remove.text
        listed = client.get(f"/api/services/{self._service_id}/custom-fields")
        assert listed.status_code == 200, listed.text
        return CustomFieldResult(
            read_back_expression=persisted["vcl_log_expression"],
            removed=all(field["name"] != FIELD_NAME for field in listed.json()["fields"]),
        )

    def revoke(self, analyst: AnalystCredential) -> RevocationResult:
        response = make_client("127.0.0.1").post(f"/api/admin/share/invites/{analyst.invite_id}/revoke")
        assert response.status_code == 200, response.text
        old_session = make_client(
            "203.0.113.77",
            cookies={"analyst_session_id": analyst.session_cookie},
        ).get(
            "/api/bootstrap",
            headers={
                "Host": oauth_fixtures.PUBLIC_ENDPOINT.removeprefix("https://"),
                "Origin": oauth_fixtures.PUBLIC_ENDPOINT,
                "X-Proxied-By-Caddy": "true",
                "Fastly-Client-IP": "203.0.113.77",
            },
        )
        return RevocationResult(old_session.json()["settings"]["needs_login"] is True)


class CloudP1JourneyAdapter:
    kind = "cloud"

    def __init__(self, *, server: CloudTestServer) -> None:
        self._server = server
        self._admin: PasskeyIdentity | None = None
        self._service_id = "fixtureService"
        self._raw_file_key: str | None = None

    def setup_and_attach(self) -> AttachmentResult:
        self._admin = self._server.register_admin(display_name="P1 Journey Admin")
        before = self._server.request(
            "GET",
            "/api/admin/services",
            session_cookie=self._admin_identity().session_cookie,
        )
        assert before.status_code == 200, before.text
        initially_absent = all(service.get("service_id") != self._service_id for service in before.json()["services"])
        service_id = self._server.provision_fixture_service(self._admin, fos_prefix="p1-journey")
        after = self._server.request(
            "GET",
            "/api/admin/services",
            session_cookie=self._admin_identity().session_cookie,
        )
        assert after.status_code == 200, after.text
        persisted = [service for service in after.json()["services"] if service.get("service_id") == self._service_id]
        assert len(persisted) == 1
        prefix = str(persisted[0]["fos_prefix"]).strip("/")
        self._raw_file_key = (
            f"{prefix + '/' if prefix else ''}processed/request/year=2027/month=01/day=15/journey.parquet"
        )
        return AttachmentResult(
            service_id,
            initially_absent,
            len(persisted),
            len(persisted) == 1,
            service_id == self._service_id,
        )

    def _admin_identity(self) -> PasskeyIdentity:
        assert self._admin is not None, "setup_and_attach must run before analyst operations"
        return self._admin

    def invite_and_sign_in(self) -> AnalystCredential:
        identity = self._server.invite_and_register(
            self._admin_identity(),
            role="analyst-no-pii",
            display_name="P1 Cloud Analyst",
            client_ip="198.51.100.77",
        )
        self._server.login_passkey(identity, client_ip="198.51.100.77")
        users = self._server.request(
            "GET",
            "/api/admin/users",
            session_cookie=self._admin_identity().session_cookie,
        )
        assert users.status_code == 200, users.text
        user = next(item for item in users.json()["users"] if item["display_name"] == "P1 Cloud Analyst")
        invites = self._server.request(
            "GET",
            "/api/admin/invites",
            session_cookie=self._admin_identity().session_cookie,
        )
        assert invites.status_code == 200, invites.text
        invite = next(item for item in invites.json()["invites"] if item["user_id"] == user["id"])
        return AnalystCredential(
            mask_ips=True,
            session_cookie=identity.session_cookie,
            invite_id=invite["id"],
            subject_id=user["id"],
        )

    def observe_masking(self, analyst: AnalystCredential) -> MaskingResult:
        raw_ip = "203.0.113.91"
        create_view = self._server.request(
            "POST",
            "/api/views",
            session_cookie=self._admin_identity().session_cookie,
            json_body={
                "service_id": self._service_id,
                "name": "P1 Raw IP Investigation",
                "filters_json": json.dumps({"client_ip": {"mode": "include", "values": [raw_ip]}}),
            },
        )
        assert create_view.status_code == 201, create_view.text

        bootstrap = self._server.request(
            "GET",
            f"/api/bootstrap?service_id={self._service_id}",
            session_cookie=analyst.session_cookie,
            client_ip="198.51.100.77",
        )
        assert bootstrap.status_code == 200, bootstrap.text
        views = self._server.request(
            "GET",
            f"/api/views/{self._service_id}",
            session_cookie=analyst.session_cookie,
            client_ip="198.51.100.77",
        )
        assert views.status_code == 200, views.text
        assert self._raw_file_key is not None
        key = self._raw_file_key
        denied = self._server.request(
            "GET",
            f"/api/file?key={key}",
            session_cookie=analyst.session_cookie,
            client_ip="198.51.100.77",
        )
        positive = self._server.request(
            "GET",
            f"/api/file?key={key}",
            session_cookie=self._admin_identity().session_cookie,
        )
        assert positive.status_code == 200, positive.text
        return MaskingResult(
            policy_enabled=bootstrap.json()["settings"]["mask_ips"] is True,
            raw_ip_absent=raw_ip not in views.text and "P1 Raw IP Investigation" not in views.text,
            raw_file_denied=denied.status_code == 403,
            raw_file_positive_control=positive.status_code == 200,
        )

    def _service_custom_fields(self) -> list[dict[str, Any]]:
        response = self._server.request(
            "GET",
            f"/api/admin/services/{self._service_id}",
            session_cookie=self._admin_identity().session_cookie,
        )
        assert response.status_code == 200, response.text
        return list(response.json()["service"]["log_fields"]["custom_fields"])

    def _patch_custom_fields(self, fields: list[dict[str, Any]]) -> None:
        generation = self._server.service_generation(self._admin_identity(), self._service_id)
        response = self._server.request(
            "PATCH",
            "/api/admin/log-fields/config",
            session_cookie=self._admin_identity().session_cookie,
            json_body={
                "generation": generation,
                "service_id": self._service_id,
                "log_fields": {
                    "groups": ["A", "C"],
                    "custom_fields": fields,
                    "field_overrides": {},
                    "field_limits": {},
                },
            },
        )
        assert response.status_code == 200, response.text

    def change_and_remove_custom_field(self) -> CustomFieldResult:
        self._patch_custom_fields([_field(label="Journey marker", expression="req.http.X-Journey-One", cloud=True)])
        self._patch_custom_fields(
            [_field(label="Journey marker changed", expression="req.http.X-Journey-Two", cloud=True)]
        )
        persisted = next(field for field in self._service_custom_fields() if field["name"] == FIELD_NAME)
        self._patch_custom_fields([])
        return CustomFieldResult(
            read_back_expression=persisted["vcl_log_expression"],
            removed=all(field["name"] != FIELD_NAME for field in self._service_custom_fields()),
        )

    def revoke(self, analyst: AnalystCredential) -> RevocationResult:
        response = self._server.request(
            "POST",
            f"/api/admin/users/{analyst.subject_id}/revoke",
            session_cookie=self._admin_identity().session_cookie,
            json_body={},
        )
        assert response.status_code == 200, response.text
        old_session = self._server.request(
            "GET",
            "/api/bootstrap",
            session_cookie=analyst.session_cookie,
            client_ip="198.51.100.77",
        )
        return RevocationResult(old_session.json()["settings"]["needs_login"] is True)


def make_p1_journey_adapter(
    adapter: str,
    *,
    monkeypatch: Any,
    tmp_path: Path,
    clickhouse: RecordingClickHouseClient | None = None,
    cloud: CloudTestServer | None = None,
) -> P1JourneyAdapter:
    if adapter == "high_scale":
        assert clickhouse is not None
        return HighScaleP1JourneyAdapter(monkeypatch=monkeypatch, tmp_path=tmp_path, clickhouse=clickhouse)
    if adapter == "cloud":
        assert cloud is not None
        return CloudP1JourneyAdapter(server=cloud)
    raise AssertionError(f"unknown P1 journey adapter: {adapter}")

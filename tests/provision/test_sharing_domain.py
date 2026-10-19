from unittest.mock import patch

import pytest

from backend.provision.sharing_domain import delete_remote_frontend, deploy_remote_frontend


def test_deploy_remote_frontend_success():
    """deploy_remote_frontend issues the correct sequential calls via the shared fastly() client."""
    service_name = "test-service"
    domain_name = "test-domain.global.ssl.fastly.net"
    origin_host = "1.2.3.4"
    origin_port = 80
    use_ssl = False
    token = "fake-token"

    # Sequential parsed-JSON responses returned by the shared client.
    responses = [
        {"id": "service123", "name": service_name},  # Create Service
        {"number": 1, "service_id": "service123"},  # Verify Draft Version
        {"name": domain_name},  # Attach Domain
        {"name": "gce_vm_origin"},  # Attach Backend Origin
        {"number": 1, "active": True},  # Activate Version
    ]
    calls: list[tuple] = []

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        assert token == "fake-token"
        calls.append((method, path, body))
        return responses.pop(0)

    with patch("backend.provision.sharing_domain.fastly", side_effect=fake_fastly):
        res = deploy_remote_frontend(
            service_name=service_name,
            domain_name=domain_name,
            origin_host=origin_host,
            origin_port=origin_port,
            use_ssl=use_ssl,
            token=token,
        )

    assert res == {
        "service_id": "service123",
        "version": 1,
        "domain_name": domain_name,
        "origin_host": origin_host,
    }
    assert calls == [
        ("POST", "/service", {"name": service_name, "type": "vcl"}),
        ("POST", "/service/service123/version", None),
        ("POST", "/service/service123/version/1/domain", {"name": domain_name}),
        (
            "POST",
            "/service/service123/version/1/backend",
            {
                "name": "gce_vm_origin",
                "address": origin_host,
                "port": origin_port,
                "use_ssl": use_ssl,
                "ssl_check_cert": False,
            },
        ),
        ("PUT", "/service/service123/version/1/activate", None),
    ]


@pytest.mark.parametrize(
    "step_fail,client_err,expected_step",
    [
        (0, "HTTP 400 POST /service\n    Name already taken", "Create Service"),
        (1, "HTTP 400 POST /service/service123/version\n    Failed to create version", "Verify Draft Version"),
        (2, "HTTP 422 POST /service/service123/version/1/domain\n    Invalid domain name", "Attach Domain"),
        (3, "HTTP 400 POST /service/service123/version/1/backend\n    Invalid backend config", "Attach Backend Origin"),
        (4, "HTTP 500 PUT /service/service123/version/1/activate\n    Could not activate", "Activate Version"),
    ],
)
def test_deploy_remote_frontend_failures(step_fail, client_err, expected_step):
    """Any failed Fastly call is re-raised with the human-readable step name preserved."""
    token = "fake-token"

    ok_responses = [
        {"id": "service123"},
        {"number": 1},
        {},
        {},
        {"active": True},
    ]
    state = {"i": 0}

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        idx = state["i"]
        state["i"] += 1
        if idx == step_fail:
            raise RuntimeError(client_err)
        return ok_responses[idx]

    with patch("backend.provision.sharing_domain.fastly", side_effect=fake_fastly):
        with pytest.raises(RuntimeError) as exc_info:
            deploy_remote_frontend(
                service_name="test-service",
                domain_name="test-domain.global.ssl.fastly.net",
                origin_host="1.2.3.4",
                origin_port=80,
                use_ssl=False,
                token=token,
            )

    msg = str(exc_info.value)
    assert f"Fastly API error during {expected_step}:" in msg
    # The underlying client error (HTTP code + body) is preserved.
    assert client_err in msg


def test_deploy_remote_frontend_missing_keys_in_json():
    """Error raised when service_id is missing from the create-service response."""
    with patch("backend.provision.sharing_domain.fastly", return_value={"not_id": "service123"}):
        with pytest.raises(RuntimeError) as exc_info:
            deploy_remote_frontend(
                service_name="test-service",
                domain_name="test-domain.global.ssl.fastly.net",
                origin_host="1.2.3.4",
                origin_port=80,
                use_ssl=False,
                token="fake-token",
            )
    assert "Failed to retrieve service ID" in str(exc_info.value)


def test_deploy_remote_frontend_missing_version_number():
    """Error raised when the draft-version response has no number."""
    responses = [{"id": "service123"}, {"service_id": "service123"}]

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        return responses.pop(0)

    with patch("backend.provision.sharing_domain.fastly", side_effect=fake_fastly):
        with pytest.raises(RuntimeError) as exc_info:
            deploy_remote_frontend(
                service_name="test-service",
                domain_name="test-domain.global.ssl.fastly.net",
                origin_host="1.2.3.4",
                origin_port=80,
                use_ssl=False,
                token="fake-token",
            )
    assert "Failed to retrieve version number" in str(exc_info.value)


def test_deploy_remote_frontend_with_override_host_success():
    """override_host adds override_host; SNI/cert fields only when use_ssl is True."""
    service_name = "test-service"
    domain_name = "test-domain.global.ssl.fastly.net"
    origin_host = "1.2.3.4"
    origin_port = 443
    use_ssl = True
    token = "fake-token"
    override_host = "my-override.example.com"

    responses = [
        {"id": "service123", "name": service_name},
        {"number": 1},
        {"name": domain_name},
        {"name": "gce_vm_origin"},
        {"active": True},
    ]
    calls: list[tuple] = []

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        calls.append((method, path, body))
        return responses.pop(0)

    with patch("backend.provision.sharing_domain.fastly", side_effect=fake_fastly):
        res = deploy_remote_frontend(
            service_name=service_name,
            domain_name=domain_name,
            origin_host=origin_host,
            origin_port=origin_port,
            use_ssl=use_ssl,
            token=token,
            override_host=override_host,
        )

    assert res["service_id"] == "service123"
    backend_call = next(c for c in calls if c[1] == "/service/service123/version/1/backend")
    backend_payload = backend_call[2]
    assert backend_payload["override_host"] == override_host
    assert backend_payload["ssl_sni_hostname"] == override_host
    assert backend_payload["ssl_cert_hostname"] == override_host


def test_delete_remote_frontend_deactivates_then_deletes():
    """delete_remote_frontend deactivates active versions, then deletes the service."""
    token = "fake-token"
    remote_service_id = "remote123"
    responses = [
        [{"number": 2, "active": True}, {"number": 1, "active": False}],  # Get Versions
        {},  # Deactivate Version 2
        {"status": "ok"},  # Delete Service
    ]
    calls: list[tuple] = []

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        calls.append((method, path))
        return responses.pop(0)

    with patch("backend.provision.sharing_domain.fastly", side_effect=fake_fastly):
        delete_remote_frontend(remote_service_id=remote_service_id, token=token)

    assert calls == [
        ("GET", f"/service/{remote_service_id}/version"),
        ("PUT", f"/service/{remote_service_id}/version/2/deactivate"),
        ("DELETE", f"/service/{remote_service_id}"),
    ]


def test_delete_remote_frontend_no_active_versions():
    """No deactivation call is issued when there are no active versions."""
    token = "fake-token"
    remote_service_id = "remote123"
    responses = [
        [{"number": 1, "active": False}],  # Get Versions
        {"status": "ok"},  # Delete Service
    ]
    calls: list[tuple] = []

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        calls.append((method, path))
        return responses.pop(0)

    with patch("backend.provision.sharing_domain.fastly", side_effect=fake_fastly):
        delete_remote_frontend(remote_service_id=remote_service_id, token=token)

    assert calls == [
        ("GET", f"/service/{remote_service_id}/version"),
        ("DELETE", f"/service/{remote_service_id}"),
    ]


def test_delete_remote_frontend_tolerates_404():
    """A 404 during version lookup/delete is swallowed (service already gone)."""

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        raise RuntimeError("Fastly API error during Get Versions: HTTP 404 GET /service/x/version\n    not found")

    with patch("backend.provision.sharing_domain.fastly", side_effect=fake_fastly):
        # Should not raise.
        delete_remote_frontend(remote_service_id="remote123", token="fake-token")


def test_delete_remote_frontend_reraises_non_404():
    """A non-404 error during deletion propagates."""
    responses = [[], RuntimeError("HTTP 500 DELETE /service/remote123\n    boom")]

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        r = responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r

    with patch("backend.provision.sharing_domain.fastly", side_effect=fake_fastly):
        with pytest.raises(RuntimeError) as exc_info:
            delete_remote_frontend(remote_service_id="remote123", token="fake-token")
    assert "500" in str(exc_info.value)

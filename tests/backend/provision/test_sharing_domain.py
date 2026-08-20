from unittest.mock import MagicMock, patch

import pytest
import requests

from backend.provision.sharing_domain import deploy_remote_frontend


def test_deploy_remote_frontend_success():
    """Verify that deploy_remote_frontend executes the correct sequential calls with proper payloads."""
    service_name = "test-service"
    domain_name = "test-domain.global.ssl.fastly.net"
    origin_host = "1.2.3.4"
    origin_port = 80
    use_ssl = False
    token = "fake-token"

    # Define mock responses
    mock_service_resp = MagicMock(spec=requests.Response)
    mock_service_resp.status_code = 201
    mock_service_resp.json.return_value = {"id": "service123", "name": service_name}
    mock_service_resp.raise_for_status.return_value = None

    mock_version_resp = MagicMock(spec=requests.Response)
    mock_version_resp.status_code = 200
    mock_version_resp.json.return_value = {"number": 1, "service_id": "service123"}
    mock_version_resp.raise_for_status.return_value = None

    mock_domain_resp = MagicMock(spec=requests.Response)
    mock_domain_resp.status_code = 200
    mock_domain_resp.json.return_value = {"name": domain_name}
    mock_domain_resp.raise_for_status.return_value = None

    mock_backend_resp = MagicMock(spec=requests.Response)
    mock_backend_resp.status_code = 200
    mock_backend_resp.json.return_value = {"name": "gce_vm_origin"}
    mock_backend_resp.raise_for_status.return_value = None

    mock_activate_resp = MagicMock(spec=requests.Response)
    mock_activate_resp.status_code = 200
    mock_activate_resp.json.return_value = {"number": 1, "active": True}
    mock_activate_resp.raise_for_status.return_value = None

    # Track sequential post requests
    post_responses = [
        mock_service_resp,
        mock_version_resp,
        mock_domain_resp,
        mock_backend_resp,
    ]

    def mock_post(url, headers, json=None, **kwargs):
        assert headers == {"Fastly-Key": token, "Accept": "application/json"}
        resp = post_responses.pop(0)
        # Match expected URL and JSON payload
        if len(post_responses) == 3:  # Create Service call was popped
            assert url == "https://api.fastly.com/service"
            assert json == {"name": service_name, "type": "vcl"}
        elif len(post_responses) == 2:  # Verify Draft Version call was popped
            assert url == "https://api.fastly.com/service/service123/version"
            assert json is None
        elif len(post_responses) == 1:  # Attach Domain call was popped
            assert url == "https://api.fastly.com/service/service123/version/1/domain"
            assert json == {"name": domain_name}
        elif len(post_responses) == 0:  # Attach Backend call was popped
            assert url == "https://api.fastly.com/service/service123/version/1/backend"
            assert json == {
                "name": "gce_vm_origin",
                "address": origin_host,
                "port": origin_port,
                "use_ssl": use_ssl,
                "ssl_check_cert": False,
            }
        return resp

    def mock_put(url, headers, **kwargs):
        assert url == "https://api.fastly.com/service/service123/version/1/activate"
        assert headers == {"Fastly-Key": token, "Accept": "application/json"}
        return mock_activate_resp

    with (
        patch("backend.provision.sharing_domain.requests.post", side_effect=mock_post) as mock_p,
        patch("backend.provision.sharing_domain.requests.put", side_effect=mock_put) as mock_u,
    ):
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
        assert mock_p.call_count == 4
        assert mock_u.call_count == 1


@pytest.mark.parametrize(
    "step_fail,json_err,text_err,expected_msg",
    [
        (0, {"msg": "Name already taken"}, "", "Fastly API error during Create Service: HTTP 400 - Name already taken"),
        (
            1,
            {"detail": "Failed to create version"},
            "",
            "Fastly API error during Verify Draft Version: HTTP 400 - Failed to create version",
        ),
        (
            2,
            {"message": "Invalid domain name"},
            "",
            "Fastly API error during Attach Domain: HTTP 422 - Invalid domain name",
        ),
        (
            3,
            {},
            "Invalid backend config",
            "Fastly API error during Attach Backend Origin: HTTP 400 - Invalid backend config",
        ),
        (4, None, "Could not activate", "Fastly API error during Activate Version: HTTP 500 - Could not activate"),
    ],
)
def test_deploy_remote_frontend_failures(step_fail, json_err, text_err, expected_msg):
    """Verify that any failed Fastly API call triggers a descriptive exception."""
    service_name = "test-service"
    domain_name = "test-domain.global.ssl.fastly.net"
    origin_host = "1.2.3.4"
    origin_port = 80
    use_ssl = False
    token = "fake-token"

    responses = []
    # Mocking first step
    if step_fail == 0:
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 400
        resp.json.return_value = json_err or {}
        resp.text = text_err
        resp.raise_for_status.side_effect = requests.HTTPError(response=resp)
        responses.append(resp)
    else:
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 201
        resp.json.return_value = {"id": "service123"}
        resp.raise_for_status.return_value = None
        responses.append(resp)

    # Mocking second step
    if step_fail == 1:
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 400
        resp.json.return_value = json_err or {}
        resp.text = text_err
        resp.raise_for_status.side_effect = requests.HTTPError(response=resp)
        responses.append(resp)
    elif step_fail > 1:
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 200
        resp.json.return_value = {"number": 1}
        resp.raise_for_status.return_value = None
        responses.append(resp)

    # Mocking third step
    if step_fail == 2:
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 422
        resp.json.return_value = json_err or {}
        resp.text = text_err
        resp.raise_for_status.side_effect = requests.HTTPError(response=resp)
        responses.append(resp)
    elif step_fail > 2:
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 200
        resp.json.return_value = {}
        resp.raise_for_status.return_value = None
        responses.append(resp)

    # Mocking fourth step
    if step_fail == 3:
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 400
        if json_err is not None:
            resp.json.return_value = json_err
        else:
            resp.json.side_effect = ValueError()
        resp.text = text_err
        resp.raise_for_status.side_effect = requests.HTTPError(response=resp)
        responses.append(resp)
    elif step_fail > 3:
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 200
        resp.json.return_value = {}
        resp.raise_for_status.return_value = None
        responses.append(resp)

    # Mocking fifth step (put)
    mock_activate_resp = MagicMock(spec=requests.Response)
    if step_fail == 4:
        mock_activate_resp.status_code = 500
        if json_err is not None:
            mock_activate_resp.json.return_value = json_err
        else:
            mock_activate_resp.json.side_effect = ValueError()
        mock_activate_resp.text = text_err
        mock_activate_resp.raise_for_status.side_effect = requests.HTTPError(response=mock_activate_resp)
    else:
        mock_activate_resp.status_code = 200
        mock_activate_resp.raise_for_status.return_value = None

    def mock_post(url, headers, json=None, **kwargs):
        return responses.pop(0)

    def mock_put(url, headers, **kwargs):
        return mock_activate_resp

    with (
        patch("backend.provision.sharing_domain.requests.post", side_effect=mock_post),
        patch("backend.provision.sharing_domain.requests.put", side_effect=mock_put),
    ):
        with pytest.raises(RuntimeError) as exc_info:
            deploy_remote_frontend(
                service_name=service_name,
                domain_name=domain_name,
                origin_host=origin_host,
                origin_port=origin_port,
                use_ssl=use_ssl,
                token=token,
            )
        assert expected_msg in str(exc_info.value)


def test_deploy_remote_frontend_missing_keys_in_json():
    """Verify error raised when service_id or version_number is missing in response."""
    service_name = "test-service"
    domain_name = "test-domain.global.ssl.fastly.net"
    origin_host = "1.2.3.4"
    origin_port = 80
    use_ssl = False
    token = "fake-token"

    # Define mock response missing 'id'
    mock_service_resp = MagicMock(spec=requests.Response)
    mock_service_resp.status_code = 201
    mock_service_resp.json.return_value = {"not_id": "service123"}
    mock_service_resp.raise_for_status.return_value = None

    with patch("backend.provision.sharing_domain.requests.post", return_value=mock_service_resp):
        with pytest.raises(RuntimeError) as exc_info:
            deploy_remote_frontend(
                service_name=service_name,
                domain_name=domain_name,
                origin_host=origin_host,
                origin_port=origin_port,
                use_ssl=use_ssl,
                token=token,
            )
        assert "Failed to retrieve service ID" in str(exc_info.value)


def test_deploy_remote_frontend_with_override_host_success():
    """Verify that deploy_remote_frontend executes correct calls with override_host."""
    service_name = "test-service"
    domain_name = "test-domain.global.ssl.fastly.net"
    origin_host = "1.2.3.4"
    origin_port = 80
    use_ssl = False
    token = "fake-token"
    override_host = "my-override.example.com"

    # Define mock responses
    mock_service_resp = MagicMock(spec=requests.Response)
    mock_service_resp.status_code = 201
    mock_service_resp.json.return_value = {"id": "service123", "name": service_name}
    mock_service_resp.raise_for_status.return_value = None

    mock_version_resp = MagicMock(spec=requests.Response)
    mock_version_resp.status_code = 200
    mock_version_resp.json.return_value = {"number": 1, "service_id": "service123"}
    mock_version_resp.raise_for_status.return_value = None

    mock_domain_resp = MagicMock(spec=requests.Response)
    mock_domain_resp.status_code = 200
    mock_domain_resp.json.return_value = {"name": domain_name}
    mock_domain_resp.raise_for_status.return_value = None

    mock_backend_resp = MagicMock(spec=requests.Response)
    mock_backend_resp.status_code = 200
    mock_backend_resp.json.return_value = {"name": "gce_vm_origin"}
    mock_backend_resp.raise_for_status.return_value = None

    mock_activate_resp = MagicMock(spec=requests.Response)
    mock_activate_resp.status_code = 200
    mock_activate_resp.json.return_value = {"number": 1, "active": True}
    mock_activate_resp.raise_for_status.return_value = None

    # Track sequential post requests
    post_responses = [
        mock_service_resp,
        mock_version_resp,
        mock_domain_resp,
        mock_backend_resp,
    ]

    def mock_post(url, headers, json=None, **kwargs):
        assert headers == {"Fastly-Key": token, "Accept": "application/json"}
        resp = post_responses.pop(0)
        # Match expected URL and JSON payload
        if len(post_responses) == 3:  # Create Service call was popped
            assert url == "https://api.fastly.com/service"
            assert json == {"name": service_name, "type": "vcl"}
        elif len(post_responses) == 2:  # Verify Draft Version call was popped
            assert url == "https://api.fastly.com/service/service123/version"
            assert json is None
        elif len(post_responses) == 1:  # Attach Domain call was popped
            assert url == "https://api.fastly.com/service/service123/version/1/domain"
            assert json == {"name": domain_name}
        elif len(post_responses) == 0:  # Attach Backend call was popped
            assert url == "https://api.fastly.com/service/service123/version/1/backend"
            assert json == {
                "name": "gce_vm_origin",
                "address": origin_host,
                "port": origin_port,
                "use_ssl": use_ssl,
                "ssl_check_cert": False,
                "override_host": override_host,
            }
        return resp

    def mock_put(url, headers, **kwargs):
        assert url == "https://api.fastly.com/service/service123/version/1/activate"
        assert headers == {"Fastly-Key": token, "Accept": "application/json"}
        return mock_activate_resp

    with (
        patch("backend.provision.sharing_domain.requests.post", side_effect=mock_post) as mock_p,
        patch("backend.provision.sharing_domain.requests.put", side_effect=mock_put) as mock_u,
    ):
        res = deploy_remote_frontend(
            service_name=service_name,
            domain_name=domain_name,
            origin_host=origin_host,
            origin_port=origin_port,
            use_ssl=use_ssl,
            token=token,
            override_host=override_host,
        )

        assert res == {
            "service_id": "service123",
            "version": 1,
            "domain_name": domain_name,
            "origin_host": origin_host,
        }
        assert mock_p.call_count == 4
        assert mock_u.call_count == 1


def test_delete_remote_frontend_success():
    """Verify delete_remote_frontend deactivates any active version and deletes the service."""
    remote_service_id = "service123"
    token = "fake-token"

    # GET versions response
    mock_get_resp = MagicMock(spec=requests.Response)
    mock_get_resp.status_code = 200
    mock_get_resp.json.return_value = [
        {"number": 1, "active": False},
        {"number": 2, "active": True},
    ]
    mock_get_resp.raise_for_status.return_value = None

    # PUT deactivate response
    mock_put_resp = MagicMock(spec=requests.Response)
    mock_put_resp.status_code = 200
    mock_put_resp.raise_for_status.return_value = None

    # DELETE response
    mock_delete_resp = MagicMock(spec=requests.Response)
    mock_delete_resp.status_code = 200
    mock_delete_resp.raise_for_status.return_value = None

    from backend.provision.sharing_domain import delete_remote_frontend

    def mock_request(method, url, headers, json=None, **kwargs):
        assert headers == {"Fastly-Key": token, "Accept": "application/json"}
        if method.upper() == "GET":
            assert url == "https://api.fastly.com/service/service123/version"
            return mock_get_resp
        elif method.upper() == "DELETE":
            assert url == "https://api.fastly.com/service/service123"
            return mock_delete_resp
        raise ValueError(f"Unexpected call: {method} {url}")

    with (
        patch("backend.provision.sharing_domain.requests.request", side_effect=mock_request) as mock_req,
        patch("backend.provision.sharing_domain.requests.put", return_value=mock_put_resp) as mock_put,
    ):
        delete_remote_frontend(remote_service_id, token)
        assert mock_req.call_count == 2
        mock_put.assert_called_once()
        args, kwargs = mock_put.call_args
        assert args[0] == "https://api.fastly.com/service/service123/version/2/deactivate"
        assert kwargs["headers"] == {"Fastly-Key": token, "Accept": "application/json"}


def test_delete_remote_frontend_no_active_versions():
    """Verify delete_remote_frontend skips deactivation if no active versions are found."""
    remote_service_id = "service123"
    token = "fake-token"

    mock_get_resp = MagicMock(spec=requests.Response)
    mock_get_resp.status_code = 200
    mock_get_resp.json.return_value = [
        {"number": 1, "active": False},
    ]
    mock_get_resp.raise_for_status.return_value = None

    mock_delete_resp = MagicMock(spec=requests.Response)
    mock_delete_resp.status_code = 200
    mock_delete_resp.raise_for_status.return_value = None

    from backend.provision.sharing_domain import delete_remote_frontend

    def mock_request(method, url, headers, json=None, **kwargs):
        if method.upper() == "GET":
            return mock_get_resp
        elif method.upper() == "DELETE":
            return mock_delete_resp
        raise ValueError(f"Unexpected call: {method} {url}")

    with patch("backend.provision.sharing_domain.requests.request", side_effect=mock_request) as mock_req:
        delete_remote_frontend(remote_service_id, token)
        assert mock_req.call_count == 2


def test_delete_remote_frontend_graceful_404():
    """Verify delete_remote_frontend is tolerant to 404 Not Found errors during lookup/deletion."""
    remote_service_id = "service123"
    token = "fake-token"

    # GET returns 404
    mock_get_resp = MagicMock(spec=requests.Response)
    mock_get_resp.status_code = 404
    mock_get_resp.json.side_effect = ValueError()
    mock_get_resp.text = "Not Found"
    mock_get_resp.raise_for_status.side_effect = requests.HTTPError(response=mock_get_resp)

    # DELETE also returns 404
    mock_delete_resp = MagicMock(spec=requests.Response)
    mock_delete_resp.status_code = 404
    mock_delete_resp.json.side_effect = ValueError()
    mock_delete_resp.text = "Not Found"
    mock_delete_resp.raise_for_status.side_effect = requests.HTTPError(response=mock_delete_resp)

    from backend.provision.sharing_domain import delete_remote_frontend

    def mock_request(method, url, headers, json=None, **kwargs):
        if method.upper() == "GET":
            return mock_get_resp
        elif method.upper() == "DELETE":
            return mock_delete_resp
        raise ValueError(f"Unexpected call: {method} {url}")

    with patch("backend.provision.sharing_domain.requests.request", side_effect=mock_request) as mock_req:
        delete_remote_frontend(remote_service_id, token)
        assert mock_req.call_count == 2


def test_delete_remote_frontend_other_error_raises():
    """Verify delete_remote_frontend propagates any non-404 API exceptions."""
    remote_service_id = "service123"
    token = "fake-token"

    # GET returns 500
    mock_get_resp = MagicMock(spec=requests.Response)
    mock_get_resp.status_code = 500
    mock_get_resp.json.side_effect = ValueError()
    mock_get_resp.text = "Server Error"
    mock_get_resp.raise_for_status.side_effect = requests.HTTPError(response=mock_get_resp)

    from backend.provision.sharing_domain import delete_remote_frontend

    with (
        patch("backend.provision.sharing_domain.requests.request", return_value=mock_get_resp),
        pytest.raises(RuntimeError) as exc_info,
    ):
        delete_remote_frontend(remote_service_id, token)
    assert "HTTP 500" in str(exc_info.value)

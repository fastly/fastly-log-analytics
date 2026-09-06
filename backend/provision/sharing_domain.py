from typing import Any

import requests


def _fastly_api_request(
    method: str,
    url: str,
    headers: dict[str, str],
    json_data: Any = None,
    step: str = "",
) -> requests.Response:
    """Perform a request to the Fastly API with uniform error handling."""
    try:
        if method.upper() == "POST":
            resp = requests.post(url, headers=headers, json=json_data)
        elif method.upper() == "PUT":
            resp = requests.put(url, headers=headers, json=json_data)
        else:
            resp = requests.request(method, url, headers=headers, json=json_data)
        resp.raise_for_status()
        return resp
    except requests.HTTPError as exc:
        err_msg = ""
        try:
            err_data = resp.json()
            err_msg = err_data.get("msg") or err_data.get("detail") or err_data.get("message") or ""
        except Exception:
            pass
        if not err_msg:
            err_msg = resp.text
        raise RuntimeError(f"Fastly API error during {step}: HTTP {resp.status_code} - {err_msg}") from exc
    except Exception as exc:
        raise RuntimeError(f"Unexpected error during {step}: {exc}") from exc


def deploy_remote_frontend(
    service_name: str,
    domain_name: str,
    origin_host: str,
    origin_port: int,
    use_ssl: bool,
    token: str,
    override_host: str | None = None,
) -> dict[str, Any]:
    """Deploy a remote frontend service to Fastly.

    Coordinates with the Fastly REST API to register a new proxy/reverse-proxy
    service pointing to our GCE VM origin.

    Steps executed sequentially:
    1. Create Service: POST https://api.fastly.com/service
       with JSON {"name": service_name, "type": "vcl"}
    2. Verify Draft Version: POST https://api.fastly.com/service/{service_id}/version
       to generate active draft Version 1. Retrieve the version `number`.
    3. Attach Domain: POST https://api.fastly.com/service/{service_id}/version/{version}/domain
       with JSON {"name": domain_name}
    4. Attach Backend Origin: POST https://api.fastly.com/service/{service_id}/version/{version}/backend
       with JSON payload {"name": "gce_vm_origin", "address": origin_host, "port": origin_port, "use_ssl": use_ssl, "ssl_check_cert": False}
    5. Activate Version: PUT https://api.fastly.com/service/{service_id}/version/{version}/activate

    Args:
        service_name: Name of the service to create.
        domain_name: Domain name to attach.
        origin_host: Backend host address.
        origin_port: Backend port.
        use_ssl: Whether to use SSL for the backend origin.
        token: Fastly API token.
        override_host: Optional host header override for the backend origin.

    Returns:
        dict: A dictionary containing:
            - service_id: The ID of the created service.
            - version: The version number.
            - domain_name: The domain name.
            - origin_host: The backend origin host.

    Raises:
        RuntimeError: If any of the Fastly API calls fail.
    """
    headers = {
        "Fastly-Key": token,
        "Accept": "application/json",
    }

    # Step 1: Create Service
    step = "Create Service"
    url = "https://api.fastly.com/service"
    service_payload = {"name": service_name, "type": "vcl"}
    resp = _fastly_api_request("POST", url, headers, json_data=service_payload, step=step)
    service_data = resp.json()
    service_id = service_data.get("id")
    if not service_id:
        raise RuntimeError(f"Failed to retrieve service ID from response during {step}")

    # Step 2: Verify Draft Version
    step = "Verify Draft Version"
    url = f"https://api.fastly.com/service/{service_id}/version"
    resp = _fastly_api_request("POST", url, headers, step=step)
    version_data = resp.json()
    version = version_data.get("number")
    if version is None:
        raise RuntimeError(f"Failed to retrieve version number from response during {step}")

    # Step 3: Attach Domain
    step = "Attach Domain"
    url = f"https://api.fastly.com/service/{service_id}/version/{version}/domain"
    domain_payload = {"name": domain_name}
    _fastly_api_request("POST", url, headers, json_data=domain_payload, step=step)

    # Step 4: Attach Backend Origin
    step = "Attach Backend Origin"
    url = f"https://api.fastly.com/service/{service_id}/version/{version}/backend"
    backend_payload: dict[str, Any] = {
        "name": "gce_vm_origin",
        "address": origin_host,
        "port": origin_port,
        "use_ssl": use_ssl,
        "ssl_check_cert": False,
    }
    if override_host:
        backend_payload["override_host"] = override_host
    _fastly_api_request("POST", url, headers, json_data=backend_payload, step=step)

    # Step 5: Activate Version
    step = "Activate Version"
    url = f"https://api.fastly.com/service/{service_id}/version/{version}/activate"
    _fastly_api_request("PUT", url, headers, step=step)

    return {
        "service_id": service_id,
        "version": version,
        "domain_name": domain_name,
        "origin_host": origin_host,
    }


def delete_remote_frontend(remote_service_id: str, token: str) -> None:
    """Deactivate and delete the remote frontend service on Fastly."""
    headers = {
        "Fastly-Key": token,
        "Accept": "application/json",
    }

    # Step 1: Get versions of the service to deactivate any active ones
    url = f"https://api.fastly.com/service/{remote_service_id}/version"
    try:
        resp = _fastly_api_request("GET", url, headers=headers, step="Get Versions")
        versions = resp.json()
        for v in versions:
            if v.get("active"):
                ver_num = v["number"]
                deactivate_url = f"https://api.fastly.com/service/{remote_service_id}/version/{ver_num}/deactivate"
                _fastly_api_request("PUT", deactivate_url, headers=headers, step=f"Deactivate Version {ver_num}")
    except Exception as exc:
        if "404" not in str(exc):
            raise exc

    # Step 2: Delete the service
    url = f"https://api.fastly.com/service/{remote_service_id}"
    try:
        _fastly_api_request("DELETE", url, headers=headers, step="Delete Service")
    except Exception as exc:
        if "404" not in str(exc):
            raise exc

from typing import Any

from backend.core.fastly.client import fastly


def _fastly_api_request(
    method: str,
    path: str,
    *,
    token: str,
    json_data: Any = None,
    step: str = "",
) -> Any:
    """Perform a request to the Fastly API via the shared client.

    Delegates transport to ``backend.core.fastly.client.fastly`` so this
    module inherits its retry/backoff, OTel telemetry, and mock-mode support
    instead of hand-rolling a second ``requests``-based client. ``path`` is a
    Fastly API path (e.g. ``/service``); the client pins the ``api.fastly.com``
    base and ``Fastly-Key`` auth. Returns the parsed JSON body (``dict`` or
    ``list``). Errors are re-raised with the human-readable ``step`` for
    provisioning diagnostics.
    """
    try:
        return fastly(method, path, body=json_data, token=token)
    except RuntimeError as exc:
        raise RuntimeError(f"Fastly API error during {step}: {exc}") from exc


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
    # Step 1: Create Service
    step = "Create Service"
    service_payload = {"name": service_name, "type": "vcl"}
    service_data = _fastly_api_request("POST", "/service", token=token, json_data=service_payload, step=step)
    service_id = service_data.get("id")
    if not service_id:
        raise RuntimeError(f"Failed to retrieve service ID from response during {step}")

    # Step 2: Verify Draft Version
    step = "Verify Draft Version"
    version_data = _fastly_api_request("POST", f"/service/{service_id}/version", token=token, step=step)
    version = version_data.get("number")
    if version is None:
        raise RuntimeError(f"Failed to retrieve version number from response during {step}")

    # Step 3: Attach Domain
    step = "Attach Domain"
    domain_payload = {"name": domain_name}
    _fastly_api_request(
        "POST",
        f"/service/{service_id}/version/{version}/domain",
        token=token,
        json_data=domain_payload,
        step=step,
    )

    # Step 4: Attach Backend Origin
    step = "Attach Backend Origin"
    backend_payload: dict[str, Any] = {
        "name": "gce_vm_origin",
        "address": origin_host,
        "port": origin_port,
        "use_ssl": use_ssl,
        "ssl_check_cert": False,
    }
    if override_host:
        backend_payload["override_host"] = override_host
    if use_ssl:
        sni_host = override_host or origin_host
        backend_payload["ssl_sni_hostname"] = sni_host
        backend_payload["ssl_cert_hostname"] = sni_host
    _fastly_api_request(
        "POST",
        f"/service/{service_id}/version/{version}/backend",
        token=token,
        json_data=backend_payload,
        step=step,
    )

    # Step 5: Activate Version
    step = "Activate Version"
    _fastly_api_request("PUT", f"/service/{service_id}/version/{version}/activate", token=token, step=step)

    return {
        "service_id": service_id,
        "version": version,
        "domain_name": domain_name,
        "origin_host": origin_host,
    }


def delete_remote_frontend(remote_service_id: str, token: str) -> None:
    """Deactivate and delete the remote frontend service on Fastly."""
    # Step 1: Get versions of the service to deactivate any active ones
    try:
        versions = _fastly_api_request("GET", f"/service/{remote_service_id}/version", token=token, step="Get Versions")
        for v in versions:
            if v.get("active"):
                ver_num = v["number"]
                _fastly_api_request(
                    "PUT",
                    f"/service/{remote_service_id}/version/{ver_num}/deactivate",
                    token=token,
                    step=f"Deactivate Version {ver_num}",
                )
    except Exception as exc:
        if "404" not in str(exc):
            raise exc

    # Step 2: Delete the service
    try:
        _fastly_api_request("DELETE", f"/service/{remote_service_id}", token=token, step="Delete Service")
    except Exception as exc:
        if "404" not in str(exc):
            raise exc

import urllib.parse

from backend.core.fastly.client import fastly


def get_active_version(service_id: str, token: str) -> int | None:
    try:
        active = fastly("GET", f"/service/{service_id}/version/active", token=token)
        if isinstance(active, dict) and active.get("active"):
            return int(active["number"])
    except RuntimeError:
        pass

    # Fallback for backwards-compatibility / mock testing
    try:
        versions = fastly("GET", f"/service/{service_id}/version", token=token)
        if isinstance(versions, list):
            for v in versions:
                if v.get("active"):
                    return int(v["number"])
    except RuntimeError:
        pass
    return None


def get_active_version_info(service_id: str, token: str, *, timeout: int = 8, max_retries: int = 1) -> dict | None:
    """Return the active version's number + timestamps for a service.

    Like :func:`get_active_version` but keeps the activation timestamp so
    callers can show *when* the live version went up (Fastly has no dedicated
    "activated_at"; ``updated_at`` on the active version is the activation/last
    -change time, ``created_at`` is when the version was cloned). Snappy
    defaults (1 retry / 8s) because this is called from a polled status path.
    Returns ``None`` on any error or if no active version exists.
    """
    try:
        v = fastly(
            "GET", f"/service/{service_id}/version/active", token=token, timeout=timeout, max_retries=max_retries
        )
        if isinstance(v, dict) and v.get("active"):
            return {
                "number": int(v["number"]),
                "updated_at": v.get("updated_at"),
                "created_at": v.get("created_at"),
            }
    except RuntimeError:
        pass

    # Fallback for backwards-compatibility / mock testing
    try:
        versions = fastly(
            "GET", f"/service/{service_id}/version", token=token, timeout=timeout, max_retries=max_retries
        )
        if isinstance(versions, list):
            for v in versions:
                if v.get("active"):
                    return {
                        "number": int(v["number"]),
                        "updated_at": v.get("updated_at"),
                        "created_at": v.get("created_at"),
                    }
    except RuntimeError:
        pass
    return None


def get_generated_vcl(service_id: str, version: int, token: str) -> str | None:
    """Return the fully-compiled (generated) VCL for a service version, or None.

    Fastly's ``GET /service/{id}/version/{n}/generated_vcl`` returns JSON
    ``{"content": "<vcl>", ...}`` — the COMPILED VCL, which carries the
    account-level pragmas Fastly injects (e.g.
    ``pragma optional_param ratelimit_opt_in true;``) that never appear in the
    source VCL we upload. Returns ``None`` for Compute/wasm services (which have
    no generated VCL) and on any API error, mirroring the RuntimeError→None
    sentinel style of :func:`get_active_version`.
    """
    try:
        result = fastly("GET", f"/service/{service_id}/version/{version}/generated_vcl", token=token)
    except RuntimeError:
        return None
    content = result.get("content") if isinstance(result, dict) else None
    return content or None


def find_service_by_name(name: str, token: str) -> dict | None:
    try:
        services = fastly("GET", "/service", token=token)
        for s in services:
            if s.get("name") == name:
                return s
    except RuntimeError:
        pass
    return None


def find_condition(name: str, service_id: str, version: int, token: str) -> dict | None:
    try:
        conditions = fastly("GET", f"/service/{service_id}/version/{version}/condition", token=token)
        for c in conditions:
            if c.get("name") == name:
                return c
    except RuntimeError:
        pass
    return None


def ensure_condition(name: str, statement: str, type: str, service_id: str, version: int, token: str) -> dict:
    existing = find_condition(name, service_id, version, token)
    if existing:
        if existing.get("statement") == statement and existing.get("type") == type:
            return existing
        else:
            # Update existing condition
            payload = {"name": name, "statement": statement, "type": type}
            encoded_name = urllib.parse.quote(name, safe="")
            return fastly(
                "PUT", f"/service/{service_id}/version/{version}/condition/{encoded_name}", payload, token=token
            )

    payload = {"name": name, "statement": statement, "type": type}
    return fastly("POST", f"/service/{service_id}/version/{version}/condition", payload, token=token)


def list_s3_endpoints(service_id: str, version: int, token: str) -> list[str]:
    try:
        res = fastly("GET", f"/service/{service_id}/version/{version}/logging/s3", token=token)
        return [r["name"] for r in res]
    except RuntimeError:
        return []


def list_vcl_snippets(service_id: str, version: int, token: str) -> list[str]:
    try:
        res = fastly("GET", f"/service/{service_id}/version/{version}/snippet", token=token)
        return [r["name"] for r in res]
    except RuntimeError:
        return []


def ensure_vcl_snippet(
    name: str, type: str, content: str, priority: int, service_id: str, version: int, token: str
) -> dict:
    try:
        snippets = fastly("GET", f"/service/{service_id}/version/{version}/snippet", token=token)
        for s in snippets:
            if s.get("name") == name:
                if s.get("type") == type and s.get("content") == content and int(s.get("priority", 100)) == priority:
                    return s
                else:
                    # Update existing snippet
                    payload = {"name": name, "type": type, "content": content, "priority": priority, "dynamic": 0}
                    encoded_name = urllib.parse.quote(name, safe="")
                    return fastly(
                        "PUT", f"/service/{service_id}/version/{version}/snippet/{encoded_name}", payload, token=token
                    )
    except RuntimeError:
        pass

    payload = {"name": name, "type": type, "content": content, "priority": priority, "dynamic": 0}
    return fastly("POST", f"/service/{service_id}/version/{version}/snippet", payload, token=token)

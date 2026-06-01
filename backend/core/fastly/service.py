import urllib.parse

from backend.core.fastly.client import fastly


def get_active_version(service_id: str, token: str) -> int | None:
    try:
        versions = fastly("GET", f"/service/{service_id}/version", token=token)
        for v in versions:
            if v.get("active"):
                return int(v["number"])
    except RuntimeError:
        pass
    return None


def find_service_by_name(name: str, token: str) -> dict | None:
    try:
        services = fastly("GET", "/service", token=token)
        for s in services:
            if s.get("name") == name:
                return s
    except RuntimeError:
        pass
    return None


def find_dictionary_by_name(service_id: str, version: int, name: str, token: str) -> dict | None:
    try:
        dicts = fastly("GET", f"/service/{service_id}/version/{version}/dictionary", token=token)
        for d in dicts:
            if d.get("name") == name:
                return d
    except RuntimeError:
        pass
    return None


def upsert_dictionary_items(service_id: str, dictionary_id: str, items: dict[str, str], token: str):
    payload = {"items": [{"item_key": k, "item_value": v} for k, v in items.items()]}
    return fastly("PATCH", f"/service/{service_id}/dictionary/{dictionary_id}/items", payload, token=token)


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

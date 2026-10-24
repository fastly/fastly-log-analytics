def service_id_from_source(
    source: dict,
    service_id: str | None = None,
    *,
    prefer_name: bool = False,
) -> str:
    """Resolve ``service_id`` from an explicit override or the ``source`` dict."""
    if service_id:
        return service_id
    if prefer_name:
        return source.get("name") or source.get("service_id") or ""
    return source.get("service_id") or source.get("name") or ""

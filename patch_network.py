def _patch():
    with open("backend/repositories/network.py") as f:
        content = f.read()

    target = "    _t = _time.perf_counter()\n    from backend.core._duckdb_status import _SCHEMA_CACHE_TTL"
    replacement = """
    _t = _time.perf_counter()
    rolled = get_runner().try_network_quality_from_rollup(
        start_time, end_time, has_filters=bool(filters), region_country=region_country
    )
    if rolled is not None:
        timer.mark("network_quality_rollup", _t)
        return {
            **rolled,
            **get_runner().telemetry(),
        }

    from backend.core._duckdb_status import _SCHEMA_CACHE_TTL"""

    content = content.replace(target, replacement)
    with open("backend/repositories/network.py", "w") as f:
        f.write(content)


_patch()

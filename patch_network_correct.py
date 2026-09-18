def _patch():
    with open("backend/repositories/network.py") as f:
        content = f.read()

    # Find the specific part inside get_quality
    idx = content.find("def get_quality(")
    assert idx != -1

    target = "    _t = _time.perf_counter()\n    from backend.core._duckdb_status import _SCHEMA_CACHE_TTL"
    idx_target = content.find(target, idx)
    assert idx_target != -1

    replacement = """    _t = _time.perf_counter()
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

    content = content[:idx_target] + replacement + content[idx_target + len(target) :]
    with open("backend/repositories/network.py", "w") as f:
        f.write(content)


_patch()

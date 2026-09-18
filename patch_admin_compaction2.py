def _patch():
    with open("backend/routers/admin/compaction.py") as f:
        content = f.read()

    backfill_call = "n_ns = backfill_network_speed_bundles(sid, source)"
    content = content.replace(
        backfill_call, backfill_call + "\n    n_nq = backfill_network_quality_bundles(sid, source)"
    )

    compact_call = "n_ns_day = compact_network_speed_closed_days_to_daily(sid, source)"
    content = content.replace(
        compact_call, compact_call + "\n    n_nq_day = compact_network_quality_closed_days_to_daily(sid, source)"
    )

    return_stmt = '        "network_speed_days": n_ns_day,'
    content = content.replace(
        return_stmt, return_stmt + '\n        "network_quality": n_nq,\n        "network_quality_days": n_nq_day,'
    )

    with open("backend/routers/admin/compaction.py", "w") as f:
        f.write(content)


_patch()

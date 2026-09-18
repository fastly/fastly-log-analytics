def _patch():
    with open("backend/routers/admin/compaction.py") as f:
        content = f.read()

    content = content.replace(
        "backfill_network_speed_bundles,", "backfill_network_speed_bundles,\n    backfill_network_quality_bundles,"
    )
    content = content.replace(
        "compact_network_speed_closed_days_to_daily,",
        "compact_network_speed_closed_days_to_daily,\n    compact_network_quality_closed_days_to_daily,",
    )

    backfill_call = """    try:
        n_network_speed = backfill_network_speed_bundles(service_id, source)
    except Exception as e:
        logger.warning("[%s] network_speed backfill failed: %s", service_id, e)
        n_network_speed = 0"""
    content = content.replace(
        backfill_call,
        backfill_call
        + """
    try:
        n_network_quality = backfill_network_quality_bundles(service_id, source)
    except Exception as e:
        logger.warning("[%s] network_quality backfill failed: %s", service_id, e)
        n_network_quality = 0""",
    )

    compact_call = """    try:
        n_net_speed_day = compact_network_speed_closed_days_to_daily(service_id, source)
    except Exception as e:
        logger.warning("[%s] network_speed day compact failed: %s", service_id, e)
        n_net_speed_day = 0"""
    content = content.replace(
        compact_call,
        compact_call
        + """
    try:
        n_net_quality_day = compact_network_quality_closed_days_to_daily(service_id, source)
    except Exception as e:
        logger.warning("[%s] network_quality day compact failed: %s", service_id, e)
        n_net_quality_day = 0""",
    )

    return_stmt = """        "network_speed": n_network_speed,
        "network_speed_day": n_net_speed_day,"""
    content = content.replace(
        return_stmt,
        return_stmt
        + """
        "network_quality": n_network_quality,
        "network_quality_day": n_net_quality_day,""",
    )

    with open("backend/routers/admin/compaction.py", "w") as f:
        f.write(content)


_patch()

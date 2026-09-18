def _patch():
    with open("backend/cron/jobs/compaction.py") as f:
        content = f.read()

    content = content.replace(
        "compact_network_speed_closed_days_to_daily,",
        "compact_network_speed_closed_days_to_daily,\n        compact_network_quality_closed_days_to_daily,",
    )

    call_target = """        try:
            network_speed_compacted = compact_network_speed_closed_days_to_daily(service_id, src)
        except Exception as e:
            logger.warning(
                "[rollup-compact] %s: network_speed day-compact failed (per-hour still serves): %s",
                _display,
                e,
            )
            network_speed_compacted = 0"""

    replacement = (
        call_target
        + """

        try:
            network_quality_compacted = compact_network_quality_closed_days_to_daily(service_id, src)
        except Exception as e:
            logger.warning(
                "[rollup-compact] %s: network_quality day-compact failed (per-hour still serves): %s",
                _display,
                e,
            )
            network_quality_compacted = 0"""
    )

    content = content.replace(call_target, replacement)

    with open("backend/cron/jobs/compaction.py", "w") as f:
        f.write(content)


_patch()

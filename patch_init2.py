def _patch():
    with open("backend/core/rollups/__init__.py") as f:
        content = f.read()

    content = content.replace(
        "compact_network_rtt_closed_days_to_daily,",
        "compact_network_rtt_closed_days_to_daily,\n    compact_network_quality_closed_days_to_daily,",
    )
    content = content.replace(
        '"compact_network_rtt_closed_days_to_daily",',
        '"compact_network_rtt_closed_days_to_daily",\n    "compact_network_quality_closed_days_to_daily",',
    )
    with open("backend/core/rollups/__init__.py", "w") as f:
        f.write(content)


_patch()

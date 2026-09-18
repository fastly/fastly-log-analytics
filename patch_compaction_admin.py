with open("backend/routers/admin/compaction.py") as f:
    content = f.read()

content = content.replace(
    "        backfill_overview_bundles,", "        backfill_overview_bundles,\n        backfill_pop_health_bundles,"
)
content = content.replace(
    "        compact_perf_latency_closed_days_to_daily,",
    "        compact_perf_latency_closed_days_to_daily,\n        compact_pop_health_closed_days_to_daily,",
)
content = content.replace(
    "    n_perf = backfill_perf_latency_bundles(sid, source)",
    "    n_perf = backfill_perf_latency_bundles(sid, source)\n    n_ph = backfill_pop_health_bundles(sid, source)",
)
content = content.replace(
    "    n_perf_day = compact_perf_latency_closed_days_to_daily(sid, source)",
    "    n_perf_day = compact_perf_latency_closed_days_to_daily(sid, source)\n    n_ph_day = compact_pop_health_closed_days_to_daily(sid, source)",
)
content = content.replace(
    '        "perf_latency": n_perf,', '        "perf_latency": n_perf,\n        "pop_health": n_ph,'
)
content = content.replace(
    '        "perf_latency_day": n_perf_day,',
    '        "perf_latency_day": n_perf_day,\n        "pop_health_day": n_ph_day,',
)

with open("backend/routers/admin/compaction.py", "w") as f:
    f.write(content)

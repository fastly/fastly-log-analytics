export const CRON_EXPLANATIONS: Record<string, string> = {
  sync: 'Downloads raw logs from Fastly Object Storage, parses them, and saves them to a local Parquet buffer.',
  full_sync: 'Daily catch-net: full LIST over the raw/ prefix to pick up late-arriving files that fall outside the regular sync’s 4h lookback window.',
  gap_heal: 'Reconciles Fastly’s authoritative log-line emission counts against ingested rows every 30 min. On sustained loss (≥2 consecutive hourly buckets ≥5% gap), triggers a full_sweep — throttled to one heal per 4h.',
  alerts: 'Evaluates recent logs against configured alert thresholds.',
  commit: 'Aggregates local buffer files and commits them as a single snapshot to the remote Iceberg table.',
  optimize: 'Compacts small Iceberg data files into larger ones (writes back to FOS — incurs 30-day-minimum cost on rewritten files).',
  local_compact: 'Merges small parquet files in the LOCAL CACHE every 10 min. Free vs FOS — speeds up dashboard scans without touching the cloud manifest.',
  expire: 'Removes old snapshots and orphaned files to reclaim storage.',
  metadata_sync: 'Downloads the latest Iceberg metadata to sync with the remote data source.',
  ngwaf_sync: 'Fetches verified bot records from Fastly NGWAF and caches them locally for enriched bot detection.',
  metadata_cleanup: 'Daily 03:15 UTC. Trims usage_log + ingested_files + cron_runs in the per-service metadata.db per the retention policy (defaults 1d/1d/7d). VACUUMs the file only when something was actually deleted.',
}

export const CRON_EXPLANATIONS: Record<string, string> = {
  log_discovery: 'Lists recent raw/ minute prefixes in Fastly Object Storage to discover new files and enqueues one convert task per file — background Celery workers download, parse, and commit each file into the data lake.',
  rum_sync: 'Downloads RUM beacon logs from Fastly Object Storage, parses them, and ingests them into the local RUM beacon table. Cleans up old beacons according to retention policy.',
  full_sync: 'Daily catch-net: full LIST over the raw/ prefix to pick up late-arriving files that fall outside the regular sync\'s 4h lookback window.',
  rum_commit: 'Aggregates RUM beacon buffer records and commits them as a snapshot to the local RUM beacons table for analytics.',
  gap_heal: 'Reconciles Fastly\'s authoritative log-line emission counts against ingested rows every 30 min. On sustained loss (>=2 consecutive hourly buckets >=5% gap), triggers a full_sweep — throttled to one heal per 4h.',
  alerts: 'Evaluates recent logs against configured alert thresholds.',
  log_ingest: 'Finalizes ingested data: compacts small lake files, reports how many files the ingest workers landed since the last tick, and deletes raw .gz files whose rows are durably committed (when delete_after is on).',
  optimize: 'Compacts small Iceberg data files into larger ones (writes back to FOS — incurs 30-day-minimum cost on rewritten files).',
  local_compact: 'Merges small parquet files in the LOCAL CACHE every 2 min. Free vs FOS — speeds up dashboard scans without touching the cloud manifest.',
  expire_snapshots: 'Removes old snapshots and orphaned files to reclaim storage.',
  metadata_sync: 'Downloads the latest Iceberg metadata to sync with the remote data source.',
  ngwaf_sync: 'Fetches verified bot records from Fastly NGWAF and caches them locally for enriched bot detection.',
  metadata_cleanup: 'Daily 03:15 UTC. Trims usage_log + ingested_files + cron_runs in the per-service metadata.db per the retention policy (defaults 1d/1d/7d). VACUUMs the file only when something was actually deleted.',
  insights_prewarmer: 'Every 4 min. Pre-runs the default-selection insights query and warms the response cache so /insights returns warm (~80-130 ms) instead of cold-scanning the parquet (~3 s).',
  rollup_compact_daily: 'Daily 02:00 UTC. Consolidates closed-day per-hour rollup parquet into per-day files (local writes only).',
  rollup_hour_heal: 'Hourly at :05. Rebuilds rollup hour-bundles for closed hours the per-sync recompute missed.',
  ledger_sweep: 'Every 15 min (Celery mode). Crash net for the ingest ledger: reclaims stale worker claims, re-dispatches stuck files, and diffs a 4h lookback LIST against the ledger.',
}

export const CRON_DISPLAY_NAMES: Record<string, string> = {
  log_discovery: 'Log Discovery',
  log_ingest: 'Ingest Logs',
  full_sync: 'Full Discovery Sweep',
  gap_heal: 'Gap Heal',
  local_compact: 'Local Compact',
  optimize: 'Cloud Compact',
  expire_snapshots: 'Expire Snapshots',
  rollup_compact_daily: 'Rollup Compact',
  rollup_hour_heal: 'Rollup Heal',
  ledger_sweep: 'Ledger Sweep',
  insights_prewarmer: 'Insights Prewarmer',
  metadata_cleanup: 'Metadata Cleanup',
  ngwaf_sync: 'NGWAF Sync',
  metadata_sync: 'Metadata Sync',
  alerts: 'Alerts Evaluation',
  rum_sync: 'RUM Discovery',
  rum_commit: 'Ingest RUM',
}

export const CRON_GROUPS = [
  {
    title: 'Ingestion Pipeline',
    tasks: ['log_discovery', 'log_ingest', 'full_sync', 'gap_heal'],
  },
  {
    title: 'Storage Maintenance',
    tasks: ['local_compact', 'optimize', 'expire_snapshots'],
  },
  {
    title: 'Rollups & Caching',
    tasks: ['rollup_compact_daily', 'rollup_hour_heal', 'insights_prewarmer'],
  },
  {
    title: 'Metadata & Enrichments',
    tasks: ['metadata_cleanup', 'ngwaf_sync', 'metadata_sync', 'alerts'],
  },
  {
    title: 'Real User Monitoring',
    tasks: ['rum_sync', 'rum_commit'],
  }
]

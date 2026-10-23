/**
 * Targeted cache-bust after an admin-initiated cron action (Import Logs,
 * Commit Buffer, Sync from Cloud, etc.).
 *
 * The naive prior pattern was:
 *   queryClient.invalidateQueries({ queryKey: ['admin'] })
 *   queryClient.invalidateQueries({ queryKey: ['dashboard'] })
 *
 * React Query treats those as prefix matches and busts every key starting
 * with the same first segment — including ``['admin', 'query-monitor', ...]``
 * (the Live Query Monitor's poll), ``['admin', 'metadata-storage']``, and
 * a half-dozen other admin surfaces that have nothing to do with the
 * cron action just kicked off. Same story on dashboard. The refetch storm
 * is wasteful, not wrong — but it shows up as a network spike on every
 * cron-action click.
 *
 * This helper enumerates only the keys whose data actually changes when a
 * cron task lands new logs / commits the buffer / refreshes sync state.
 * The list mirrors what ``useAdminEventStream (cron-runs channel)`` already targets (cron-logs,
 * cron-logs-recent, last-sync, iceberg) plus the dashboard/filter-bar
 * surfaces that depend on the new data (dashboard aggregates, log-extents,
 * sync-status, schema).
 *
 * Per-service scoping is best-effort: when ``serviceId`` is passed, keys
 * that include the sid get an exact-prefix invalidation; the rest stay
 * at their domain prefix so cross-service queries still drop.
 */

import type { QueryClient } from '@tanstack/react-query'

import { queryKeys } from '@/lib/query-keys'

export function cronCacheBust(queryClient: QueryClient, _serviceId?: string | null): void {
  // Cron history surfaces that ``useAdminEventStream (cron-runs channel)`` already targets.
  // These are the canonical destinations for "a cron run just changed".
  // Note: React Query's invalidateQueries uses prefix matching by default,
  // so domain-level prefixes cover all per-service keys under them.
  queryClient.invalidateQueries({ queryKey: queryKeys.admin.cronLogsAll() })
  queryClient.invalidateQueries({ queryKey: queryKeys.admin.cronLogsRecentAll() })
  queryClient.invalidateQueries({ queryKey: queryKeys.admin.icebergAll() })
  queryClient.invalidateQueries({ queryKey: queryKeys.admin.schemaAll() })

  // Header badge + FilterBar bounds + sync-status badge.
  queryClient.invalidateQueries({ queryKey: queryKeys.lastSyncAll() })
  queryClient.invalidateQueries({ queryKey: queryKeys.logExtentsAll() })
  queryClient.invalidateQueries({ queryKey: queryKeys.syncStatusAll() })

  // Dashboard surfaces that consume the new log data. These are the
  // legitimate "user pressed Import Logs, expects fresh charts" targets.
  queryClient.invalidateQueries({ queryKey: ['dashboard', 'aggregates'] })
  queryClient.invalidateQueries({ queryKey: ['dashboard', 'top-bots'] })
  queryClient.invalidateQueries({ queryKey: ['dashboard', 'raw'] })
  queryClient.invalidateQueries({ queryKey: ['dashboard', 'bundle'] })

  // Bootstrap carries seeded versions of several of the above; refresh
  // it so the next page navigation finds fresh seeds rather than stale
  // ones from the pre-cron snapshot.
  queryClient.invalidateQueries({ queryKey: queryKeys.bootstrap() })
}

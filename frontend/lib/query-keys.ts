import type { components, LogFieldsConfig } from '@/types/api'
import type { FiltersPayload } from '@/types/api'

/**
 * Centralised React Query key factory.
 *
 * Every page that queries the backend should build its keys here so the shape
 * is consistent, typo-safe, and easy to invalidate by prefix.
 *
 * Usage:
 *   queryKey: queryKeys.filtered('dashboard', 'aggregates', serviceId, start, end, filters, metric, interval)
 *   queryKey: queryKeys.bootstrap()
 */
export const queryKeys = {
  bootstrap: () => ['bootstrap'] as const,

  shareBanner: () => ['share-banner'] as const,

  services: () => ['services'] as const,

  systemJobs: () => ['system-jobs'] as const,

  views: (serviceId: string | null | undefined) => ['views', serviceId] as const,

  logExtents: (serviceId: string | null | undefined) => ['log-extents', serviceId] as const,
  logExtentsAll: () => ['log-extents'] as const,

  scoringLabels: (serviceId: string | null | undefined) => ['scoring-labels', serviceId] as const,
  scoringLabelsAll: () => ['scoring-labels'] as const,

  rumStatus: (serviceId: string | null | undefined) => ['rum-status', serviceId] as const,
  rumVersions: (serviceId: string | null | undefined) => ['rum-versions', serviceId] as const,

  syncStatus: (serviceId: string | null | undefined) => ['sync-status', serviceId] as const,
  /** Prefix key: matches every service's sync-status entry. */
  syncStatusAll: () => ['sync-status'] as const,

  lastSync: (serviceId: string | null | undefined) => ['last-sync', serviceId] as const,
  /** Prefix key: matches every service's last-sync entry. */
  lastSyncAll: () => ['last-sync'] as const,

  logFieldsCatalog: (serviceId?: string) => (serviceId ? ['log-fields-catalog', serviceId] as const : ['log-fields-catalog'] as const),

  /**
   * Admin-surface keys. Shapes are load-bearing: SSE appliers
   * (lib/admin-stream-apply.ts), cron cache-bust and the seeders write and
   * invalidate by these exact tuples, and `*All` / `*For` variants are the
   * prefixes that match the longer keys.
   */
  admin: {
    shareStatus: () => ['admin', 'share', 'status'] as const,
    shareLive: () => ['admin', 'share', 'live'] as const,
    metricHistoryBatch: (window: string) => ['admin', 'metric-history-batch', window] as const,
    celeryStatus: () => ['admin', 'celery-status'] as const,
    celeryLedger: (statusFilter: string, serviceId: string | null | undefined, offset: number) =>
      ['admin', 'celery-ledger', statusFilter, serviceId ?? null, offset] as const,
    celeryLedgerAll: () => ['admin', 'celery-ledger'] as const,
    healthSnapshot: () => ['admin', 'health-snapshot'] as const,
    metadataStorage: () => ['admin', 'metadata-storage'] as const,
    debugSettings: () => ['admin', 'debug-settings'] as const,
    status: () => ['admin', 'status'] as const,

    /** Prefix: matches every overview* entry. */
    overviewAll: () => ['admin', 'overview'] as const,
    overviewQueriesSummary: () => ['admin', 'overview', 'queries-summary'] as const,
    overviewLogAccounting: (serviceId: string | null | undefined) => ['admin', 'overview', 'log-accounting', serviceId] as const,
    overviewSlowQueriesCount: (serviceId: string | null | undefined) =>
      ['admin', 'overview', 'slow-queries-count', serviceId] as const,
    overviewCeleryStatus: () => ['admin', 'overview', 'celery-status'] as const,

    queryMonitorConfig: () => ['admin', 'query-monitor', 'config'] as const,
    queryMonitorSnapshot: (includeCompleted: boolean) => ['admin', 'query-monitor', 'snapshot', includeCompleted] as const,
    /** Prefix: matches every queryMonitorSnapshot entry. */
    queryMonitorSnapshotAll: () => ['admin', 'query-monitor', 'snapshot'] as const,
    queryMonitorSlowHistory: (mode: string, thresholdMs: number) =>
      ['admin', 'query-monitor', 'slow-history', mode, thresholdMs] as const,
    queryMonitorDetail: (queryId: number | null) => ['admin', 'query-monitor', 'detail', queryId] as const,

    quarantineSummary: (serviceId: string | null | undefined) => ['admin', 'quarantine', 'summary', serviceId] as const,
    quarantineList: (serviceId: string | null | undefined, errorCategory: string, offset: number) =>
      ['admin', 'quarantine', 'list', serviceId, errorCategory, offset] as const,
    /** Prefix: every quarantine query. */
    quarantineAll: () => ['admin', 'quarantine'] as const,

    cronLogs: (serviceId: string | null | undefined, taskFilter: string, statusFilter: string) =>
      ['admin', 'cron-logs', serviceId, taskFilter, statusFilter] as const,
    /** Prefix: one service's cron-logs entries (any filter). */
    cronLogsFor: (serviceId: string | null | undefined) => ['admin', 'cron-logs', serviceId] as const,
    /** Prefix: every cron-logs entry. */
    cronLogsAll: () => ['admin', 'cron-logs'] as const,
    cronLogsRecent: (serviceId: string | null | undefined) => ['admin', 'cron-logs-recent', serviceId] as const,
    cronLogsRecentAll: () => ['admin', 'cron-logs-recent'] as const,
    cronSchedule: (serviceId: string | null | undefined) => ['admin', 'cron-schedule', serviceId] as const,

    auditLogs: (serviceId: string | null | undefined, eventFilter: string) =>
      ['admin', 'audit-logs', serviceId, eventFilter] as const,
    /** Prefix: one service's audit-logs entries (any filter). */
    auditLogsFor: (serviceId: string | null | undefined) => ['admin', 'audit-logs', serviceId] as const,
    ingestedFiles: (serviceId: string | null | undefined) => ['admin', 'ingested-files', serviceId] as const,
    schema: (serviceId: string | null | undefined) => ['admin', 'schema', serviceId] as const,
    schemaAll: () => ['admin', 'schema'] as const,

    iceberg: (serviceId: string | null | undefined) => ['admin', 'iceberg', serviceId] as const,
    icebergAll: () => ['admin', 'iceberg'] as const,
    icebergCalendar: (serviceId: string | null | undefined) => ['admin', 'iceberg', 'calendar', serviceId] as const,
    icebergInfo: (serviceId: string | null | undefined) => ['admin', 'iceberg-info', serviceId] as const,
    icebergTree: (serviceId: string | null | undefined) => ['admin', 'iceberg-tree', serviceId] as const,
    rawTree: (serviceId: string | null | undefined) => ['admin', 'raw-tree', serviceId] as const,
    /** FileBrowser listing for one prefix; `fileTreeFor` is the prefix-less invalidation key. */
    fileTree: (type: 'iceberg' | 'raw', serviceId: string | null | undefined, prefix: string) =>
      ['admin', type === 'iceberg' ? 'iceberg-tree' : 'raw-tree', serviceId, prefix] as const,
    fileTreeFor: (type: 'iceberg' | 'raw', serviceId: string | null | undefined) =>
      ['admin', type === 'iceberg' ? 'iceberg-tree' : 'raw-tree', serviceId] as const,
    clickhouseStatus: (serviceId: string | null | undefined) =>
      ['admin', 'clickhouse', 'status', serviceId] as const,
  },

  filtered: (
    domain: string,
    endpoint: string,
    serviceId: string | null,
    startTime: string,
    endTime: string,
    filters: FiltersPayload,
    ...extras: unknown[]
  ) => [domain, endpoint, serviceId, startTime, endTime, filters, ...extras] as const,
}

interface BootstrapCacheSeedSource {
  active_service_id?: string | null
  views?: unknown
  log_fields_catalog?: unknown
  sync_status?: unknown
  log_extents?: unknown
  schema?: unknown
  table_name?: unknown
  cron_runs_first_page?: unknown
  last_sync?: unknown
  scoring_labels?: unknown
  services?: unknown
  share_banner?: {
    sharing_active?: boolean | null
    public_url?: string | null
  } | null
}

/**
 * Shared cache-seeding helper used by both SSR (`app/layout.tsx`) and client
 * (`hooks/useBootstrap.ts`) so dependent query caches are populated identically
 * through `queryKeys` helpers without drift.
 */
export function seedBootstrapDependentCaches(
  queryClient: { setQueryData: (queryKey: readonly unknown[], updater: unknown) => unknown },
  data: BootstrapCacheSeedSource | null | undefined,
): void {
  if (!data) return
  if (data.active_service_id) {
    const sid = data.active_service_id
    if (Array.isArray(data.views)) {
      queryClient.setQueryData(queryKeys.views(sid), data.views)
    }
    if (data.log_fields_catalog) {
      queryClient.setQueryData(queryKeys.logFieldsCatalog(sid), data.log_fields_catalog)
    }
    queryClient.setQueryData(queryKeys.syncStatus(sid), data.sync_status ?? null)
    if (data.log_extents) {
      queryClient.setQueryData(queryKeys.logExtents(sid), data.log_extents)
    }
    const seededSchemaList = data.schema
    const seededTableName = data.table_name
    if (
      Array.isArray(seededSchemaList) &&
      seededSchemaList.length > 0 &&
      typeof seededTableName === 'string'
    ) {
      queryClient.setQueryData(queryKeys.admin.schema(sid), {
        schema: seededSchemaList,
        table_name: seededTableName,
      })
    }
    if (data.cron_runs_first_page) {
      queryClient.setQueryData(queryKeys.admin.cronLogsRecent(sid), data.cron_runs_first_page)
    }
    queryClient.setQueryData(queryKeys.lastSync(sid), data.last_sync ?? null)
    if (data.scoring_labels) {
      queryClient.setQueryData(queryKeys.scoringLabels(sid), data.scoring_labels)
    }
  }
  if (Array.isArray(data.services)) {
    queryClient.setQueryData(queryKeys.services(), {
      services: data.services,
      _section_timings: [],
    })
  }
  const shareBanner = data.share_banner
  if (shareBanner && typeof shareBanner === 'object') {
    queryClient.setQueryData(queryKeys.shareBanner(), {
      sharing_active: !!shareBanner.sharing_active,
      public_url: typeof shareBanner.public_url === 'string' ? shareBanner.public_url : null,
    })
  }
}

// ── Log Field Config Operations ───────────────────────────────────────────────

type LogFieldsCatalogLike =
  | components["schemas"]["LogFieldsCatalogResponse"]
  | null
  | undefined;

function applyGroupDependencies<T extends string | null>(
  groups: Set<T>,
  catalog: LogFieldsCatalogLike,
): void {
  const catalogGroups = catalog?.groups || [];
  let changed = true;
  while (changed) {
    changed = false;
    catalogGroups.forEach((g) => {
      if (
        g.id != null &&
        groups.has(g.id as T) &&
        g.requires &&
        !groups.has(g.requires as T)
      ) {
        groups.add(g.requires as T);
        changed = true;
      }
    });
  }
}

export function resolveEnabledGroupsWithDeps(
  groups: string[] | undefined,
  catalog: LogFieldsCatalogLike,
): Set<string | null> {
  const enabledGroups = new Set<string | null>([null, ...(groups || [])]);
  applyGroupDependencies(enabledGroups, catalog);
  return enabledGroups;
}

export function toggleLogFieldGroup(
  prev: LogFieldsConfig,
  catalog: LogFieldsCatalogLike,
  groupId: string,
  checked: boolean,
): LogFieldsConfig {
  const nextGroups = new Set<string>(prev.groups || []);
  if (checked) {
    nextGroups.add(groupId);
    applyGroupDependencies(nextGroups, catalog);
  } else {
    nextGroups.delete(groupId);
  }
  return { ...prev, groups: Array.from(nextGroups) };
}

export function toggleLogFieldOverride(
  prev: LogFieldsConfig,
  fieldId: string,
  checked: boolean,
  defaultEnabledByGroup: boolean,
): LogFieldsConfig {
  const overrides = { ...(prev.field_overrides || {}) };
  if (checked === defaultEnabledByGroup) {
    delete overrides[fieldId];
  } else {
    overrides[fieldId] = checked;
  }
  return { ...prev, field_overrides: overrides };
}

export function updateLogFieldLimit(
  prev: LogFieldsConfig,
  fieldId: string,
  limit?: number,
): LogFieldsConfig {
  const field_limits = { ...(prev.field_limits || {}) };
  if (limit === undefined) {
    delete field_limits[fieldId];
  } else {
    field_limits[fieldId] = limit;
  }
  return { ...prev, field_limits };
}

export function isLogFieldPresetActive(
  config: LogFieldsConfig | undefined,
  groups: string[],
): boolean {
  if (!groups.length) return false;
  const currentGroups = new Set(config?.groups || []);
  return groups.every((g) => currentGroups.has(g));
}

export function toggleLogFieldPreset(
  prev: LogFieldsConfig,
  catalog: LogFieldsCatalogLike,
  presetGroups: string[],
  isPresetActiveFn?: (groups: string[]) => boolean,
): LogFieldsConfig {
  const currentGroups = new Set<string>(prev.groups || []);
  const allActive = presetGroups.every((g) => currentGroups.has(g));
  const nextGroups = new Set<string>(prev.groups || []);
  const checkActive =
    isPresetActiveFn ?? ((groups: string[]) => isLogFieldPresetActive(prev, groups));

  if (allActive) {
    const otherActivePresetsGroups = new Set<string>();
    if (catalog?.presets) {
      Object.entries(catalog.presets).forEach(([_key, preset]) => {
        if (
          preset.groups.length !== presetGroups.length ||
          !preset.groups.every((g: string) => presetGroups.includes(g))
        ) {
          if (checkActive(preset.groups)) {
            preset.groups.forEach((g: string) => otherActivePresetsGroups.add(g));
          }
        }
      });
    }

    presetGroups.forEach((g) => {
      if (!otherActivePresetsGroups.has(g)) {
        nextGroups.delete(g);
        catalog?.groups?.forEach((cg) => {
          if (
            cg.id != null &&
            cg.requires === g &&
            !otherActivePresetsGroups.has(cg.id)
          ) {
            nextGroups.delete(cg.id);
          }
        });
      }
    });
  } else {
    presetGroups.forEach((g) => nextGroups.add(g));
    applyGroupDependencies(nextGroups, catalog);
  }

  return { ...prev, groups: Array.from(nextGroups) };
}

export function estimateLogFieldBytesPerLine(
  config: LogFieldsConfig | undefined,
  catalog: LogFieldsCatalogLike,
): number {
  if (!catalog?.fields) return 0;
  let total = 0;
  const enabledGroups = new Set(config?.groups || []);
  const overrides = config?.field_overrides || {};
  for (const field of catalog.fields) {
    const inGroup = field.group === null || enabledGroups.has(field.group);
    const override = overrides[field.id];
    if (override === true) {
      total += field.typical_bytes || 0;
      continue;
    }
    if (override === false) continue;
    if (inGroup) total += field.typical_bytes || 0;
  }
  return total;
}

export function evaluateLogFieldPresetSummary(
  config: LogFieldsConfig | undefined,
  catalog: LogFieldsCatalogLike,
): {
  enabledGroupsSet: Set<string>;
  bestPresetName: string | null;
  disabledCount: number;
} {
  const enabledGroupsSet = new Set(config?.groups || []);
  const overrides = config?.field_overrides || {};
  const hasOverrides = Object.keys(overrides).length > 0;
  let bestPresetName: string | null = null;

  if (catalog?.presets && !hasOverrides) {
    for (const [key, preset] of Object.entries(catalog.presets)) {
      const presetGroups = preset.groups;
      if (
        presetGroups.length === enabledGroupsSet.size &&
        presetGroups.every((g) => enabledGroupsSet.has(g))
      ) {
        bestPresetName = preset.label || key;
        break;
      }
    }
  }

  const disabledCount = (catalog?.groups || []).filter(
    (g) => !(g.locked || (g.id != null && enabledGroupsSet.has(g.id))),
  ).length;

  return { enabledGroupsSet, bestPresetName, disabledCount };
}

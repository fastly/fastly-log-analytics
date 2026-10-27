import type { FiltersPayload } from "@/types/api";

// ── React Query Key Factory & Cache Seeding ───────────────────────────────────

export const queryKeys = {
  bootstrap: () => ["bootstrap"] as const,

  shareBanner: () => ["share-banner"] as const,

  services: () => ["services"] as const,

  systemJobs: () => ["system-jobs"] as const,

  views: (serviceId: string | null | undefined) => ["views", serviceId] as const,

  logExtents: (serviceId: string | null | undefined) => ["log-extents", serviceId] as const,
  logExtentsAll: () => ["log-extents"] as const,

  scoringLabels: (serviceId: string | null | undefined) => ["scoring-labels", serviceId] as const,
  scoringLabelsAll: () => ["scoring-labels"] as const,

  rumStatus: (serviceId: string | null | undefined) => ["rum-status", serviceId] as const,
  rumVersions: (serviceId: string | null | undefined) => ["rum-versions", serviceId] as const,

  syncStatus: (serviceId: string | null | undefined) => ["sync-status", serviceId] as const,
  syncStatusAll: () => ["sync-status"] as const,

  lastSync: (serviceId: string | null | undefined) => ["last-sync", serviceId] as const,
  lastSyncAll: () => ["last-sync"] as const,

  logFieldsCatalog: (serviceId?: string) =>
    serviceId ? (["log-fields-catalog", serviceId] as const) : (["log-fields-catalog"] as const),

  admin: {
    shareStatus: () => ["admin", "share", "status"] as const,
    shareLive: () => ["admin", "share", "live"] as const,
    metricHistoryBatch: (window: string) => ["admin", "metric-history-batch", window] as const,
    celeryStatus: () => ["admin", "celery-status"] as const,
    celeryLedger: (statusFilter: string, serviceId: string | null | undefined, offset: number) =>
      ["admin", "celery-ledger", statusFilter, serviceId ?? null, offset] as const,
    celeryLedgerAll: () => ["admin", "celery-ledger"] as const,
    healthSnapshot: () => ["admin", "health-snapshot"] as const,
    metadataStorage: () => ["admin", "metadata-storage"] as const,
    debugSettings: () => ["admin", "debug-settings"] as const,
    status: () => ["admin", "status"] as const,

    overviewAll: () => ["admin", "overview"] as const,
    overviewQueriesSummary: () => ["admin", "overview", "queries-summary"] as const,
    overviewLogAccounting: (serviceId: string | null | undefined) =>
      ["admin", "overview", "log-accounting", serviceId] as const,
    overviewSlowQueriesCount: (serviceId: string | null | undefined) =>
      ["admin", "overview", "slow-queries-count", serviceId] as const,
    overviewCeleryStatus: () => ["admin", "overview", "celery-status"] as const,

    queryMonitorConfig: () => ["admin", "query-monitor", "config"] as const,
    queryMonitorSnapshot: (includeCompleted: boolean) =>
      ["admin", "query-monitor", "snapshot", includeCompleted] as const,
    queryMonitorSnapshotAll: () => ["admin", "query-monitor", "snapshot"] as const,
    queryMonitorSlowHistory: (mode: string, thresholdMs: number) =>
      ["admin", "query-monitor", "slow-history", mode, thresholdMs] as const,
    queryMonitorDetail: (queryId: number | null) => ["admin", "query-monitor", "detail", queryId] as const,

    quarantineSummary: (serviceId: string | null | undefined) =>
      ["admin", "quarantine", "summary", serviceId] as const,
    quarantineList: (serviceId: string | null | undefined, errorCategory: string, offset: number) =>
      ["admin", "quarantine", "list", serviceId, errorCategory, offset] as const,
    quarantineAll: () => ["admin", "quarantine"] as const,

    cronLogs: (serviceId: string | null | undefined, taskFilter: string, statusFilter: string) =>
      ["admin", "cron-logs", serviceId, taskFilter, statusFilter] as const,
    cronLogsFor: (serviceId: string | null | undefined) => ["admin", "cron-logs", serviceId] as const,
    cronLogsAll: () => ["admin", "cron-logs"] as const,
    cronLogsRecent: (serviceId: string | null | undefined) => ["admin", "cron-logs-recent", serviceId] as const,
    cronLogsRecentAll: () => ["admin", "cron-logs-recent"] as const,
    cronSchedule: (serviceId: string | null | undefined) => ["admin", "cron-schedule", serviceId] as const,

    auditLogs: (serviceId: string | null | undefined, eventFilter: string) =>
      ["admin", "audit-logs", serviceId, eventFilter] as const,
    auditLogsFor: (serviceId: string | null | undefined) => ["admin", "audit-logs", serviceId] as const,
    ingestedFiles: (serviceId: string | null | undefined) => ["admin", "ingested-files", serviceId] as const,
    schema: (serviceId: string | null | undefined) => ["admin", "schema", serviceId] as const,
    schemaAll: () => ["admin", "schema"] as const,

    storage: (serviceId: string | null | undefined) => ["admin", "storage", serviceId] as const,
    storageAll: () => ["admin", "storage"] as const,
    storageCalendar: (serviceId: string | null | undefined) => ["admin", "storage", "calendar", serviceId] as const,
    storageInfo: (serviceId: string | null | undefined) => ["admin", "storage-info", serviceId] as const,
    storageTree: (serviceId: string | null | undefined) => ["admin", "storage-tree", serviceId] as const,
    rawTree: (serviceId: string | null | undefined) => ["admin", "raw-tree", serviceId] as const,
    fileTree: (type: "storage" | "raw", serviceId: string | null | undefined, prefix: string) =>
      ["admin", type === "storage" ? "storage-tree" : "raw-tree", serviceId, prefix] as const,
    fileTreeFor: (type: "storage" | "raw", serviceId: string | null | undefined) =>
      ["admin", type === "storage" ? "storage-tree" : "raw-tree", serviceId] as const,
    clickhouseStatus: (serviceId: string | null | undefined) =>
      ["admin", "clickhouse", "status", serviceId] as const,
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
};

export interface BootstrapCacheSeedSource {
  active_service_id?: string | null;
  views?: unknown;
  log_fields_catalog?: unknown;
  sync_status?: unknown;
  log_extents?: unknown;
  schema?: unknown;
  table_name?: unknown;
  cron_runs_first_page?: unknown;
  last_sync?: unknown;
  scoring_labels?: unknown;
  services?: unknown;
  share_banner?: {
    sharing_active?: boolean | null;
    public_url?: string | null;
  } | null;
}

export function seedBootstrapDependentCaches(
  queryClient: { setQueryData: (queryKey: readonly unknown[], updater: unknown) => unknown },
  data: BootstrapCacheSeedSource | null | undefined,
): void {
  if (!data) return;
  if (data.active_service_id) {
    const sid = data.active_service_id;
    if (Array.isArray(data.views)) {
      queryClient.setQueryData(queryKeys.views(sid), data.views);
    }
    if (data.log_fields_catalog) {
      queryClient.setQueryData(queryKeys.logFieldsCatalog(sid), data.log_fields_catalog);
    }
    queryClient.setQueryData(queryKeys.syncStatus(sid), data.sync_status ?? null);
    if (data.log_extents) {
      queryClient.setQueryData(queryKeys.logExtents(sid), data.log_extents);
    }
    const seededSchemaList = data.schema;
    const seededTableName = data.table_name;
    if (
      Array.isArray(seededSchemaList) &&
      seededSchemaList.length > 0 &&
      typeof seededTableName === "string"
    ) {
      queryClient.setQueryData(queryKeys.admin.schema(sid), {
        schema: seededSchemaList,
        table_name: seededTableName,
      });
    }
    if (data.cron_runs_first_page) {
      queryClient.setQueryData(queryKeys.admin.cronLogsRecent(sid), data.cron_runs_first_page);
    }
    queryClient.setQueryData(queryKeys.lastSync(sid), data.last_sync ?? null);
    if (data.scoring_labels) {
      queryClient.setQueryData(queryKeys.scoringLabels(sid), data.scoring_labels);
    }
  }
  if (Array.isArray(data.services)) {
    queryClient.setQueryData(queryKeys.services(), {
      services: data.services,
      _section_timings: [],
    });
  }
  const shareBanner = data.share_banner;
  if (shareBanner && typeof shareBanner === "object") {
    queryClient.setQueryData(queryKeys.shareBanner(), {
      sharing_active: !!shareBanner.sharing_active,
      public_url: typeof shareBanner.public_url === "string" ? shareBanner.public_url : null,
    });
  }
}

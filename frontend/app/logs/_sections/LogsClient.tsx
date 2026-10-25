'use client'

import React, { useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import dynamic from 'next/dynamic'
import { client, extractApiError, unwrapClient } from '@/lib/api'
import { showToast, cn } from '@/lib/utils'
import { cronCacheBust } from '@/lib/admin-stream-apply'
import { queryKeys } from '@/lib/query-keys'
import { useServiceStore } from '@/stores/filterStore'
import { useDateFormat, type useSSE } from '@/hooks/useSSE'
import { Badge } from '@/components/ui/badge'
import { Button } from "@/components/ui/button"
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  Database,
  HardDrive,
  History,
  FileCode,
  Archive,
  ClipboardList,
  RefreshCw,
  Download,
  Bot,
  Terminal,
  ChevronRight,
  ChevronDown,
  Folder,
  Cloud,
  FileJson,
  Loader2,
  Maximize2,
  Minimize2,
} from 'lucide-react'
import { NoServiceSelected } from '@/components/ReportLayout'
import { useBootstrapResolved } from '@/hooks/useBootstrap'
import { ScrollArea } from '@/components/ui/scroll-area'
import { SyncFromCloudModal } from '@/components/SyncStatusBadge/SyncStatusBadge'
import { DataTableReadonly, ColumnVisibilityDropdown, ingestedFilesColumns } from '@/components/DataTable'
import { PageHeader } from '@/components/ui/confirm-dialog'
import { Input } from '@/components/ui/input'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { formatCountMaybe, formatBytes } from '@/lib/format'
import type { ColumnDef } from '@tanstack/react-table'
import type { components } from '@/types/api.generated'
import type { SyncStatus } from '@/hooks/useEventStream'

import { useLogsPageState, type CronRun } from '../_state'
import Loading from '../loading'
import { useCronColumns } from './CronColumns'
import { useAuditColumns } from './AuditColumns'
import { FloatingOperationsDock } from './FloatingOperationsDock'
import { CronTab } from './CronTab'
import { ControlledSSEModal as SSEModal } from '@/components/SSEModal'

type CronRunsResponse = components['schemas']['CronRunsResponse']
type SchemaColumn = components['schemas']['SchemaResponse']['schema'][number]
type IngestedFile = components['schemas']['IngestedFile']

const hasStats = (col: SchemaColumn) => col.null_percentage !== undefined
const populatedPct = (col: SchemaColumn) =>
  col.null_percentage != null ? Math.max(0, 100 - col.null_percentage) : null

const SCHEMA_COLUMNS: ColumnDef<SchemaColumn>[] = [
  {
    id: 'name',
    header: 'Column Name',
    accessorFn: (c) => c.name,
    size: 220,
    cell: ({ row }) => <span className="font-mono text-xs font-bold">{row.original.name}</span>,
  },
  {
    id: 'type',
    header: 'DuckDB Type',
    accessorFn: (c) => c.type,
    size: 160,
    cell: ({ row }) => <span className="font-mono text-xs text-muted-foreground">{row.original.type}</span>,
  },
  {
    id: 'populated',
    header: 'Populated %',
    accessorFn: (c) => populatedPct(c) ?? -1,
    size: 130,
    cell: ({ row }) => {
      const pct = populatedPct(row.original)
      const text = pct != null ? pct.toFixed(1) : '—'
      return (
        <span className="block font-mono text-xs text-right tabular-nums">
          {hasStats(row.original) ? (
            <span className={text === '0.0' ? 'text-muted-foreground/30' : ''}>{text}%</span>
          ) : '—'}
        </span>
      )
    },
  },
  {
    id: 'approx_unique',
    header: 'Approx Unique',
    accessorFn: (c) => c.approx_unique ?? -1,
    size: 140,
    cell: ({ row }) => (
      <span className="block font-mono text-xs text-right text-muted-foreground tabular-nums">
        {hasStats(row.original) ? formatCountMaybe(row.original.approx_unique) ?? '—' : '—'}
      </span>
    ),
  },
  {
    id: 'min',
    header: 'Min Value',
    accessorFn: (c) => c.min ?? '',
    size: 200,
    cell: ({ row }) => (
      <span className="font-mono text-xs text-muted-foreground" title={row.original.min ?? undefined}>
        {hasStats(row.original) ? row.original.min ?? '—' : '—'}
      </span>
    ),
  },
  {
    id: 'max',
    header: 'Max Value',
    accessorFn: (c) => c.max ?? '',
    size: 200,
    cell: ({ row }) => (
      <span className="font-mono text-xs text-muted-foreground" title={row.original.max ?? undefined}>
        {hasStats(row.original) ? row.original.max ?? '—' : '—'}
      </span>
    ),
  },
]

export function SchemaTab({
  schemaData,
  isLoadingSchema,
}: {
  schemaData: components['schemas']['SchemaResponse'] | undefined
  isLoadingSchema: boolean
}) {
  return (
    <>
      <div className="p-4 border-b flex justify-between items-center">
        <div>
          <h3 className="text-sm font-medium">DuckDB Table Schema & Statistics</h3>
          <p className="text-xs text-muted-foreground mt-1">Based on a fast statistical sample of your logs.</p>
        </div>
      </div>
      <DataTableReadonly
        columns={SCHEMA_COLUMNS}
        data={schemaData?.schema ?? []}
        isLoading={isLoadingSchema}
        hideToolbar
        showPagination={false}
        tableCaption="DuckDB Table Schema and Statistics"
      />
    </>
  )
}

export function IngestionTab({
  ingestedColumns,
  ingestedFiles,
  isLoadingIngested,
}: {
  ingestedColumns: ColumnDef<IngestedFile>[]
  ingestedFiles: components['schemas']['IngestedFilesResponse'] | undefined
  isLoadingIngested: boolean
}) {
  return (
    <DataTableReadonly
      columns={ingestedColumns}
      data={ingestedFiles?.files || []}
      isLoading={isLoadingIngested}
      searchKey="file_name"
      initialSorting={[{ id: 'ingested_at', desc: true }]}
      renderToolbar={(table) => (
        <div className="p-4 border-b flex flex-wrap items-center justify-between gap-4">
          <h3 className="text-sm font-medium">Log Ingestion History</h3>
          <div className="flex items-center gap-2 ml-auto">
            <Input
              placeholder="Filter by filename..."
              value={(table.getColumn('file_name')?.getFilterValue() as string) ?? ''}
              onChange={(event) => table.getColumn('file_name')?.setFilterValue(event.target.value)}
              className="max-w-sm h-8"
            />
            <ColumnVisibilityDropdown table={table} compact />
          </div>
        </div>
      )}
    />
  )
}

export function ServiceHistoryTab({
  auditColumns,
  auditLogs,
  isLoadingAudit,
  isFetchingAudit,
  eventFilter,
  setEventFilter,
  activeServiceId,
}: {
  auditColumns: ColumnDef<components['schemas']['AuditLogEntry']>[]
  auditLogs: components['schemas']['AuditLogsResponse'] | undefined
  isLoadingAudit: boolean
  isFetchingAudit: boolean
  eventFilter: string
  setEventFilter: (v: string) => void
  activeServiceId: string | null | undefined
}) {
  const queryClient = useQueryClient()

  return (
    <div className="p-0">
      <DataTableReadonly
        columns={auditColumns}
        data={auditLogs?.entries || []}
        isLoading={isLoadingAudit}
        initialSorting={[{ id: 'timestamp', desc: true }]}
        renderToolbar={(table) => (
          <div className="flex flex-col sm:flex-row sm:items-center justify-between p-4 border-b gap-4">
            <div className="flex items-center gap-4">
              <h3 className="text-sm font-medium whitespace-nowrap">Service History</h3>
              <div className="flex items-center gap-2">
                <Select value={eventFilter} onValueChange={(v) => setEventFilter(v || 'all')}>
                  <SelectTrigger className="h-8 w-[200px] text-xs">
                    <SelectValue placeholder="All events" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All events</SelectItem>
                    <SelectItem value="provision">Provision</SelectItem>
                    <SelectItem value="fastly_activation">Fastly Activation</SelectItem>
                    <SelectItem value="cron_settings_update">Cron Settings Update</SelectItem>
                    <SelectItem value="logging_settings_update">Log Settings Updated</SelectItem>
                    <SelectItem value="log_format_update">Log Format Update</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
            <div className="flex items-center gap-2">
              <ColumnVisibilityDropdown table={table} />
              <Button
                variant="outline"
                size="sm"
                onClick={() => queryClient.invalidateQueries({ queryKey: queryKeys.admin.auditLogsFor(activeServiceId) })}
                disabled={isFetchingAudit}
                className="h-8 text-xs"
              >
                <RefreshCw className={`h-3 w-3 mr-1.5 ${isFetchingAudit ? 'animate-spin' : ''}`} />
                Refresh
              </Button>
            </div>
          </div>
        )}
      />
    </div>
  )
}

export function QuickActionsBar({
  isAnalyst,
  status,
  activeServiceId,
  recentCrons,
  cronLogs,
  setSseTitle,
  setSseDescription,
  setIsSSEModalOpen,
  setIsSyncModalOpen,
  reset,
  start,
  setDisplayedJobs,
  setSelectedConsoleJobId,
  setConsoleOpen,
}: {
  isAnalyst: boolean
  status: Partial<SyncStatus> | undefined
  activeServiceId: string | null | undefined
  recentCrons: Partial<CronRunsResponse> | undefined
  cronLogs: Partial<CronRunsResponse> | undefined
  setSseTitle: (s: string) => void
  setSseDescription: (s: string) => void
  setIsSSEModalOpen: (open: boolean) => void
  setIsSyncModalOpen: (open: boolean) => void
  reset: () => void
  start: ReturnType<typeof useSSE>['start']
  setDisplayedJobs: React.Dispatch<React.SetStateAction<CronRun[]>>
  setSelectedConsoleJobId: (id: number | string | null) => void
  setConsoleOpen: (open: boolean) => void
}) {
  const queryClient = useQueryClient()

  return (
    <div className="flex flex-wrap items-center gap-2 bg-muted/30 p-2 rounded-lg border">
      <div className="text-xs font-bold text-muted-foreground uppercase tracking-wider mx-2">Quick Actions</div>
      {!isAnalyst ? (
        <>
          <Button
            size="sm"
            variant="default"
            className="h-8 text-xs bg-primary text-primary-foreground hover:bg-primary/90"
            disabled={status?.access_level === 'read_only'}
            onClick={async () => {
              try {
                const data = await unwrapClient(client.POST("/api/admin/ingest-logs", {}))
                // 200 body is an opaque dict; the run_id keys the SSE stream.
                const runId = data?.run_id
                if (!runId) { showToast('No run id returned'); return }
                setSseTitle('Importing Logs')
                setSseDescription('Downloading new raw logs from Fastly Object Storage and processing them...')
                setIsSSEModalOpen(true)
                reset()
                start(`/api/cron-runs/${runId}/stream`)
                cronCacheBust(queryClient, activeServiceId)
              } catch (e) {
                showToast(extractApiError(e))
              }
            }}
          >
            <RefreshCw className="h-3 w-3 mr-1.5" /> Import Logs
          </Button>
          <Button
            size="sm"
            variant="outline"
            className="h-8 text-xs bg-background"
            disabled={status?.access_level === 'read_only'}
            onClick={() => setIsSyncModalOpen(true)}
            title="Open the range-picker to backfill a specific historical window (e.g., to close an Ingest Gap)"
          >
            <Download className="h-3 w-3 mr-1.5" /> Sync from Cloud
          </Button>
          <Button
            size="sm"
            variant="outline"
            className="h-8 text-xs bg-background"
            disabled={status?.access_level === 'read_only'}
            onClick={async () => {
              try {
                const data = await unwrapClient(client.POST("/api/admin/commit-iceberg", {}))
                const runId = data?.run_id
                if (!runId) { showToast('No run id returned'); return }
                setSseTitle('Committing Buffer')
                setSseDescription('Flushing local Parquet buffer to the shared Iceberg table in Object Storage...')
                setIsSSEModalOpen(true)
                reset()
                start(`/api/cron-runs/${runId}/stream`)
                cronCacheBust(queryClient, activeServiceId)
              } catch (e) {
                showToast(extractApiError(e))
              }
            }}
          >
            <Archive className="h-3 w-3 mr-1.5" /> Commit Buffer
          </Button>
        </>
      ) : (
        <Button
          size="sm"
          variant="default"
          className="h-8 text-xs bg-primary/90 hover:bg-primary"
          onClick={() => setIsSyncModalOpen(true)}
        >
          <Download className="h-3 w-3 mr-1.5" /> Sync from Cloud
        </Button>
      )}
      {!isAnalyst && status?.ngwaf_workspace_id && (
        <Button
          size="sm"
          variant="outline"
          className="h-8 text-xs bg-background"
          onClick={() => {
            setSseTitle('NGWAF Bot Sync')
            setSseDescription('Fetching verified bot records from Fastly NGWAF and caching them locally. Progress is saved after each page — run again if the time budget is reached.')
            setIsSSEModalOpen(true)
            reset()
            start(`/api/services/${activeServiceId}/ngwaf-sync`, {})
            queryClient.invalidateQueries({ queryKey: queryKeys.admin.cronLogsFor(activeServiceId) })
          }}
        >
          <Bot className="h-3 w-3 mr-1.5" /> NGWAF Bot Sync
        </Button>
      )}
      <Button
        size="sm"
        variant="outline"
        className="h-8 text-xs bg-background"
        onClick={() => {
          const latestSync = recentCrons?.entries?.find((e) => e.task === 'log_discovery') ||
                             cronLogs?.entries?.find((e) => e.task === 'log_discovery')
          if (latestSync) {
            setDisplayedJobs(prev => {
              if (prev.some((j) => j.id === latestSync.id)) return prev
              return [...prev, { ...latestSync, status: latestSync.status }]
            })
            setSelectedConsoleJobId(latestSync.id)
            setConsoleOpen(true)
          } else {
            window.alert("No recent sync run was found for this service.")
          }
        }}
      >
        <Terminal className="h-3 w-3 mr-1.5" /> View Recent Logs
      </Button>
    </div>
  )
}

// Lazy-mount the heavy tab content. Cron + ServiceHistory + Ingestion
// are the default landing tabs; the four below only matter when the
// operator clicks into them. ``ssr: false`` skips the SSR pass — none
// of them render server-side anyway (they hit DuckDB and S3 client-
// side via React Query hooks).
type TreeNode = components['schemas']['TreeNode']

interface TreeProps {
  prefix: string
  level: number
  type: 'iceberg' | 'raw'
  forceExpand?: boolean
}

function Node({ node, prefix, level, type, forceExpand }: { node: TreeNode, prefix: string, level: number, type: 'iceberg' | 'raw', forceExpand?: boolean }) {
  const [isOpen, setIsOpen] = useState(false)
  const { full, abbr, relative } = useDateFormat()
  const activeServiceId = useServiceStore(s => s.activeServiceId)

  // A defined `forceExpand` overrides the open state whenever it changes (adjusted during render).
  const [seenForceExpand, setSeenForceExpand] = useState<boolean | undefined>(undefined)
  if (forceExpand !== seenForceExpand) {
    setSeenForceExpand(forceExpand)
    if (forceExpand !== undefined) setIsOpen(forceExpand)
  }

  if (node.type === 'directory') {
    return (
      <div>
        <div
          className={cn(
            "flex items-center justify-between gap-2 py-1.5 hover:bg-muted/50 rounded-md transition-colors group",
            level === 0 ? "font-medium" : "text-sm text-muted-foreground"
          )}
          style={{ paddingLeft: `${level * 16 + 8}px`, paddingRight: '8px' }}
        >
          <button
            type="button"
            aria-expanded={isOpen}
            onClick={() => setIsOpen(!isOpen)}
            className="flex items-center gap-2 min-w-0 flex-1 bg-transparent border-0 p-0 text-left cursor-pointer"
          >
            {isOpen ? <ChevronDown className="h-4 w-4 shrink-0 text-muted-foreground" /> : <ChevronRight className="h-4 w-4 shrink-0 text-muted-foreground" />}
            <Folder className="h-4 w-4 shrink-0 text-blue-500/70 dark:text-blue-400/70" />
            <span className="truncate group-hover:text-foreground transition-colors">{node.name}</span>
          </button>
          <div className="flex items-center gap-4 shrink-0">
            {node.size != null && node.size > 0 && (
              <span className="text-xs font-mono text-muted-foreground tabular-nums w-20 text-right">
                {formatBytes(node.size)}
              </span>
            )}
            <div className="w-20 flex items-center justify-end gap-2">
              <Button
                variant="ghost"
                size="icon"
                aria-label="Download folder as ZIP"
                className="h-6 w-6 text-muted-foreground transition-opacity"
                onClick={(e) => {
                  e.stopPropagation()
                  window.open(`/api/download-folder?service=${activeServiceId}&root=${type}&prefix=${encodeURIComponent(node.prefix || (prefix + node.name))}`, '_blank')
                }}
                title="Download folder as ZIP"
              >
                <Download className="h-3.5 w-3.5" />
              </Button>
            </div>
          </div>
        </div>
        {isOpen && (
          <Tree prefix={`${prefix}${node.name}/`} level={level + 1} type={type} forceExpand={forceExpand} />
        )}
      </div>
    )
  }

  return (
    <div
      className="flex items-center justify-between gap-4 py-1 pr-2 hover:bg-muted/50 group rounded-md transition-colors"
      style={{ paddingLeft: `${level * 16 + 32}px` }}
    >
      <div className="flex items-center gap-2 min-w-0 flex-1">
        <FileJson className="h-4 w-4 shrink-0 text-muted-foreground opacity-70" />
        <span className="text-sm font-mono truncate text-muted-foreground group-hover:text-foreground transition-colors">
          {node.name}
        </span>
      </div>
      <div className="flex items-center gap-4 shrink-0">
        {node.size != null && (
          <span className="text-xs font-mono text-muted-foreground tabular-nums w-20 text-right">
            {formatBytes(node.size)}
          </span>
        )}
        {node.mtime && (
          <TooltipProvider>
            <Tooltip>
              {/* A-8 (a11y, WCAG 2.1.1): tabIndex + role="button" so
                  keyboard users can focus the relative-time label and
                  reveal the full timestamp tooltip. */}
              <TooltipTrigger render={
                <span
                  tabIndex={0}
                  role="button"
                  aria-label="Show full modified timestamp"
                  className="hidden sm:inline-block text-xs text-muted-foreground w-[120px] text-right truncate rounded-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                >
                  {relative(node.mtime)}
                </span>
              } />
              <TooltipContent className="text-xs">
                {full(node.mtime)} {abbr()}
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        )}
        <div className="w-20 flex items-center justify-end gap-2">
          {node.sync_status === 'synced' && <Badge variant="outline" className="px-1.5 py-0 h-5 text-[10px] bg-green-500/10 text-green-600 border-green-500/20 shadow-none"><Cloud className="w-3 h-3 mr-1"/> Synced</Badge>}
          {node.sync_status === 'local' && <Badge variant="outline" className="px-1.5 py-0 h-5 text-[10px] bg-blue-500/10 text-blue-600 border-blue-500/20 shadow-none"><HardDrive className="w-3 h-3 mr-1"/> Local</Badge>}
          {node.sync_status === 'cloud' && <Badge variant="outline" className="px-1.5 py-0 h-5 text-[10px] bg-purple-500/10 text-purple-600 border-purple-500/20 shadow-none"><Cloud className="w-3 h-3 mr-1"/> Cloud</Badge>}
          {!node.sync_status && node.is_cloud && <Badge variant="outline" className="px-1.5 py-0 h-5 text-[10px] bg-purple-500/10 text-purple-600 border-purple-500/20 shadow-none"><Cloud className="w-3 h-3 mr-1"/> Cloud</Badge>}

          {node.key && (
            <Button
              variant="ghost"
              size="icon"
              aria-label={`Download ${node.name}`}
              className="h-6 w-6 text-muted-foreground transition-opacity"
              onClick={(e) => {
                e.stopPropagation()
                window.open(`/api/download?service=${activeServiceId}&key=${encodeURIComponent(node.key as string)}`, '_blank')
              }}
              title="Download file"
            >
              <Download className="h-3.5 w-3.5" />
            </Button>
          )}
        </div>
      </div>
    </div>
  )
}

function Tree({ prefix, level, type, forceExpand }: TreeProps) {
  const activeServiceId = useServiceStore(s => s.activeServiceId)

  const { data, isLoading, error } = useQuery({
    queryKey: queryKeys.admin.fileTree(type, activeServiceId, prefix),
    queryFn: async () => {
      const init = { params: { query: { prefix: prefix || undefined } } }
      return type === 'iceberg'
        ? unwrapClient(client.GET("/api/admin/iceberg-tree", init))
        : unwrapClient(client.GET("/api/admin/raw-tree", init))
    },
    enabled: !!activeServiceId,
  })

  if (isLoading) {
    return (
      <div className="flex items-center gap-2 py-2 text-sm text-muted-foreground animate-pulse" style={{ paddingLeft: `${level * 16 + 28}px` }}>
        <Loader2 className="h-3 w-3 animate-spin" /> Loading...
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="py-2 text-sm text-destructive" style={{ paddingLeft: `${level * 16 + 28}px` }}>
        Failed to load directory.
      </div>
    )
  }

  const nodes = data.nodes

  if (nodes.length === 0) {
    return (
      <div className="py-2 text-xs text-muted-foreground italic" style={{ paddingLeft: `${level * 16 + 32}px` }}>
        Empty directory
      </div>
    )
  }

  return (
    <div className="flex flex-col">
      {nodes.map((node: TreeNode) => (
        <Node key={node.name} node={node} prefix={prefix} level={level} type={type} forceExpand={forceExpand} />
      ))}
    </div>
  )
}

export function FileBrowser({ type }: { type: 'iceberg' | 'raw' }) {
  const [forceExpand, setForceExpand] = useState<boolean | undefined>(undefined)
  const queryClient = useQueryClient()
  const [isRefreshing, setIsRefreshing] = useState(false)
  const activeServiceId = useServiceStore(s => s.activeServiceId)

  const handleRefresh = async () => {
    setIsRefreshing(true)
    await queryClient.invalidateQueries({ queryKey: queryKeys.admin.fileTreeFor(type, activeServiceId) })
    setTimeout(() => setIsRefreshing(false), 500)
  }

  return (
    <div className="flex flex-col font-sans h-full relative">
      <div className="flex items-center justify-between px-4 py-2 border-b bg-muted/5 sticky top-0 z-10 backdrop-blur-sm">
        <span className="text-[10px] font-bold text-muted-foreground uppercase tracking-widest">{type === 'iceberg' ? 'Iceberg Data Lake' : 'Raw Archives'}</span>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            className="h-7 w-7 p-0 font-semibold shadow-none border-muted/60 bg-background"
            onClick={handleRefresh}
            title="Refresh file list"
          >
            <RefreshCw className={cn("h-3.5 w-3.5", isRefreshing && "animate-spin")} />
          </Button>
          <Button
            variant="outline"
            size="sm"
            className="h-7 text-[10px] px-2 font-semibold shadow-none border-muted/60 bg-background"
            onClick={() => setForceExpand(prev => prev === undefined ? true : !prev)}
          >
            {forceExpand ? <Minimize2 className="h-3 w-3 mr-1" /> : <Maximize2 className="h-3 w-3 mr-1" />}
            {forceExpand ? 'Collapse All' : 'Expand All'}
          </Button>
        </div>
      </div>
      <div className="p-2 overflow-x-auto min-w-[500px]">
        <Tree prefix="" level={0} type={type} forceExpand={forceExpand} />
      </div>
    </div>
  )
}
const IcebergStatus = dynamic(
  () => import('@/components/IcebergStatus/IcebergStatus').then(m => m.IcebergStatus),
  { ssr: false },
)
const IcebergCalendar = dynamic(
  () => import('@/components/IcebergStatus/IcebergStatus').then(m => m.IcebergCalendar),
  { ssr: false },
)
const MetadataStorageCard = dynamic(
  () => import('@/components/MetadataStorageCard').then(m => m.MetadataStorageCard),
  { ssr: false },
)

export default function LogsClient({ initialTab }: { initialTab?: string } = {}) {
  const s = useLogsPageState(initialTab)
  const queryClient = useQueryClient()
  const bootstrapResolved = useBootstrapResolved()

  const auditColumns = useAuditColumns(s.catalogMaps)
  const cronColumns = useCronColumns(s.isAnalyst)

  // Suppress the NoServiceSelected fallback until bootstrap has
  // resolved — otherwise a cold load with empty localStorage flashes
  // the fallback for one render tick before HydrationBoundary or the
  // client-side useBootstrap fetch lands the active_service_id into
  // the React Query cache that useEffectiveServiceId (now used inside
  // useLogsPageState) reads from.
  if (!s.activeServiceId && bootstrapResolved) {
    return <NoServiceSelected icon={Database} message="Please select a service from the header to access admin controls." />
  }
  if (!s.activeServiceId) {
    // Bootstrap in flight — render the height-matched route skeleton rather
    // than collapsing the main container to 0px and flashing the footer up.
    return <Loading />
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title="Data Management"
        description="Monitor and manage log ingestion history and active data syncs."
      />

      <QuickActionsBar
        isAnalyst={s.isAnalyst}
        status={s.status}
        activeServiceId={s.activeServiceId}
        recentCrons={s.recentCrons}
        cronLogs={s.cronLogs}
        setSseTitle={s.setSseTitle}
        setSseDescription={s.setSseDescription}
        setIsSSEModalOpen={s.setIsSSEModalOpen}
        setIsSyncModalOpen={s.setIsSyncModalOpen}
        reset={s.reset}
        start={s.start}
        setDisplayedJobs={s.setDisplayedJobs}
        setSelectedConsoleJobId={s.setSelectedConsoleJobId}
        setConsoleOpen={s.setConsoleOpen}
      />

      <Tabs value={s.activeTab} onValueChange={s.handleTabChange} className="w-full">
        <ScrollArea className="w-full max-w-full overflow-hidden">
          <TabsList className="w-full flex">
            <TabsTrigger value="cron" className="flex-1 flex items-center justify-center gap-2 text-xs">
              <History className="h-4 w-4" /> Cron Runs
            </TabsTrigger>
            <TabsTrigger value="service_history" className="flex-1 flex items-center justify-center gap-2 text-xs">
              <ClipboardList className="h-4 w-4" /> Service History
            </TabsTrigger>
            {!s.isAnalyst && (
              <TabsTrigger value="ingestion" className="flex-1 flex items-center justify-center gap-2 text-xs">
                <Database className="h-4 w-4" /> Ingestion History
              </TabsTrigger>
            )}
            <TabsTrigger value="iceberg" className="flex-1 flex items-center justify-center gap-2 text-xs">
              <Archive className="h-4 w-4" /> DuckLake Storage
            </TabsTrigger>
            {!s.isAnalyst && (
              <TabsTrigger value="metadata_storage" className="flex-1 flex items-center justify-center gap-2 text-xs">
                <HardDrive className="h-4 w-4" /> Metadata Storage
              </TabsTrigger>
            )}
            {!s.isAnalyst && (
              <TabsTrigger value="raw" className="flex-1 flex items-center justify-center gap-2 text-xs">
                <FileCode className="h-4 w-4" /> Available Logs
              </TabsTrigger>
            )}
            <TabsTrigger value="schema" className="flex-1 flex items-center justify-center gap-2 text-xs">
              <FileCode className="h-4 w-4" /> DuckDB Schema
            </TabsTrigger>
          </TabsList>
        </ScrollArea>

        <TabsContent value="cron" className="mt-4 border rounded-lg overflow-hidden bg-card">
          <CronTab
            cronColumns={cronColumns}
            cronLogs={s.cronLogs}
            isLoadingCron={s.isLoadingCron}
            isFetchingCron={s.isFetchingCron}
            orderedSchedules={s.orderedSchedules}
            isLoadingSchedule={s.isLoadingSchedule}
            taskFilter={s.taskFilter}
            setTaskFilter={s.setTaskFilter}
            statusFilter={s.statusFilter}
            setStatusFilter={s.setStatusFilter}
            isAnalyst={s.isAnalyst}
            status={s.status}
            activeServiceId={s.activeServiceId}
            setDisplayedJobs={s.setDisplayedJobs}
            setSelectedConsoleJobId={s.setSelectedConsoleJobId}
            setConsoleOpen={s.setConsoleOpen}
            isPurgeOpen={s.isPurgeOpen}
            setIsPurgeOpen={s.setIsPurgeOpen}
            purgeMutation={s.purgeMutation}
          />
        </TabsContent>

        <TabsContent value="service_history" className="mt-4 border rounded-lg overflow-hidden bg-card">
          <ServiceHistoryTab
            auditColumns={auditColumns}
            auditLogs={s.auditLogs}
            isLoadingAudit={s.isLoadingAudit}
            isFetchingAudit={s.isFetchingAudit}
            eventFilter={s.eventFilter}
            setEventFilter={s.setEventFilter}
            activeServiceId={s.activeServiceId}
          />
        </TabsContent>

        <TabsContent value="ingestion" className="mt-4 border rounded-lg overflow-hidden bg-card">
          <IngestionTab
            ingestedColumns={ingestedFilesColumns}
            ingestedFiles={s.ingestedFiles}
            isLoadingIngested={s.isLoadingIngested}
          />
        </TabsContent>

        {/* Gate the dynamic-imported tabs on activeTab so the bundled
         * chunks defer until the user actually clicks in. Radix' default
         * is render-all (CSS-hide non-active), which would mount every
         * panel on the cron landing — defeating the dynamic() split. */}
        <TabsContent value="iceberg" className="mt-4 space-y-4">
          {s.activeTab === 'iceberg' && (
            <>
              <IcebergStatus accessLevel={s.status?.access_level ?? undefined} />
              <IcebergCalendar />

              <div className="border rounded-lg overflow-hidden bg-card">
                <div className="p-4 border-b">
                  <h3 className="text-sm font-medium">Iceberg Data Lake Layout</h3>
                  <p className="text-xs text-muted-foreground mt-1">Direct view into the Iceberg metadata and data files in Object Storage.</p>
                </div>
                <div className="p-0">
                  <FileBrowser type="iceberg" />
                </div>
              </div>
            </>
          )}
        </TabsContent>

        <TabsContent value="metadata_storage" className="mt-4 space-y-4">
          {s.activeTab === 'metadata_storage' && <MetadataStorageCard />}
        </TabsContent>

        <TabsContent value="raw" className="mt-4 border rounded-lg overflow-hidden bg-card">
          {s.activeTab === 'raw' && (
            <>
              <div className="p-4 border-b">
                <h3 className="text-sm font-medium">Available Logs</h3>
                <p className="text-xs text-muted-foreground mt-1">Raw .gz files delivered by Fastly waiting to be processed.</p>
              </div>
              <div className="p-0">
                <FileBrowser type="raw" />
              </div>
            </>
          )}
        </TabsContent>

        <TabsContent value="schema" className="mt-4 border rounded-lg overflow-hidden bg-card">
          {s.activeTab === 'schema' && <SchemaTab schemaData={s.schemaData} isLoadingSchema={s.isLoadingSchema} />}
        </TabsContent>
      </Tabs>

      <SyncFromCloudModal
        open={s.isSyncModalOpen}
        onOpenChange={s.setIsSyncModalOpen}
        onStartSync={async (range) => {
          const apiRange = range ? { start_time: range.start, end_time: range.end } : {}
          try {
            const data = await unwrapClient(client.POST("/api/admin/ingest-logs", {
              params: { query: apiRange }
            }))
            s.setSseTitle('Syncing from Cloud')
            s.setSseDescription('Fetching latest snapshots and downloading new data files from the cloud...')
            s.setIsSSEModalOpen(true)
            s.reset()
            s.start(`/api/cron-runs/${data?.run_id}/stream`)
            cronCacheBust(queryClient, s.activeServiceId)
          } catch (e) {
            console.error(e)
          }
        }}
      />

      <SSEModal
        isSSEModalOpen={s.isSSEModalOpen}
        setIsSSEModalOpen={s.setIsSSEModalOpen}
        sseStatus={s.sseStatus}
        sseTitle={s.sseTitle}
        sseError={s.sseError}
        sseDescription={s.sseDescription}
        lines={s.lines}
        stop={s.stop}
      />

      <FloatingOperationsDock
        displayedJobs={s.displayedJobs}
        setDisplayedJobs={s.setDisplayedJobs}
        isOpen={s.consoleOpen}
        setIsOpen={s.setConsoleOpen}
        selectedJobId={s.selectedConsoleJobId}
        setSelectedJobId={s.setSelectedConsoleJobId}
        onDismiss={s.removeDisplayedJob}
      />
    </div>
  )
}

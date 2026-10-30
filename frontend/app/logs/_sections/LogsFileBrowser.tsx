'use client'

import React, { useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import {
  HardDrive,
  Archive,
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
import { client, extractApiError, unwrapClient, queryKeys } from '@/lib/api'
import { showToast, cn, formatCountMaybe, formatBytes } from '@/lib/utils'
import { cronCacheBust, useSSE, useDateFormat, type SyncStatus } from '@/hooks/useSSE'
import { useServiceStore } from '@/stores/filterStore'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { DataTableReadonly, ColumnVisibilityDropdown } from '@/components/DataTable'
import { Input } from '@/components/ui/input'
import type { ColumnDef } from '@tanstack/react-table'
import { useVirtualizer } from '@tanstack/react-virtual'
import type { components } from '@/types/api.generated'
import type { CronRun } from './useLogsPageState'

type CronRunsResponse = components['schemas']['CronRunsResponse']
type SchemaColumn = components['schemas']['SchemaResponse']['schema'][number]
type IngestedFile = components['schemas']['IngestedFile']
type TreeNode = components['schemas']['TreeNode']

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
          ) : (
            '—'
          )}
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
  ...(['min', 'max'] as const).map(
    (field): ColumnDef<SchemaColumn> => ({
      id: field,
      header: field === 'min' ? 'Min Value' : 'Max Value',
      accessorFn: (c) => c[field] ?? '',
      size: 200,
      cell: ({ row }) => (
        <span className="font-mono text-xs text-muted-foreground" title={row.original[field] ?? undefined}>
          {hasStats(row.original) ? row.original[field] ?? '—' : '—'}
        </span>
      ),
    }),
  ),
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
              className="max-w-sm h-8"
              value={(table.getColumn('file_name')?.getFilterValue() as string) ?? ''}
              onChange={(event) => table.getColumn('file_name')?.setFilterValue(event.target.value)}
            />
            <ColumnVisibilityDropdown table={table} compact />
          </div>
        </div>
      )}
    />
  )
}

export function QuickActionsBar({
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
  const triggerCronAction = async (
    endpoint: '/api/admin/ingest-logs' | '/api/admin/commit-storage',
    title: string,
    description: string,
  ) => {
    try {
      const data = (await unwrapClient(client.POST(endpoint, {}))) as { run_id?: number | string } | undefined
      if (!data?.run_id) {
        showToast('No run id returned')
        return
      }
      setSseTitle(title)
      setSseDescription(description)
      setIsSSEModalOpen(true)
      reset()
      start(`/api/cron-runs/${data.run_id}/stream`)
      cronCacheBust(queryClient, activeServiceId)
    } catch (e) {
      showToast(extractApiError(e))
    }
  }

  return (
    <div className="flex flex-wrap items-center gap-2 bg-muted/30 p-2 rounded-lg border">
      <div className="text-xs font-bold text-muted-foreground uppercase tracking-wider mx-2">Quick Actions</div>
      <Button
        size="sm"
        variant="default"
        className="h-8 text-xs bg-primary text-primary-foreground hover:bg-primary/90"
        onClick={() =>
          triggerCronAction(
            '/api/admin/ingest-logs',
            'Importing Logs',
            'Downloading new raw logs from Fastly Object Storage and processing them...',
          )
        }
      >
        <RefreshCw className="h-3 w-3 mr-1.5" /> Import Logs
      </Button>
      <Button
        size="sm"
        variant="outline"
        className="h-8 text-xs bg-background"
        onClick={() => setIsSyncModalOpen(true)}
        title="Open the range-picker to backfill a specific historical window (e.g., to close an Ingest Gap)"
      >
        <Download className="h-3 w-3 mr-1.5" /> Backfill Logs
      </Button>
      <Button
        size="sm"
        variant="outline"
        className="h-8 text-xs bg-background"
        onClick={() =>
          triggerCronAction(
            '/api/admin/commit-storage',
            'Committing Buffer',
            'Committing the local Parquet buffer to DuckLake and pushing it to cloud storage...',
          )
        }
      >
        <Archive className="h-3 w-3 mr-1.5" /> Commit Buffer
      </Button>
      <Button
        size="sm"
        variant="outline"
        className="h-8 text-xs bg-background"
        onClick={() => {
          const latestSync =
            recentCrons?.entries?.find((e) => e.task === 'log_discovery') ||
            cronLogs?.entries?.find((e) => e.task === 'log_discovery')
          if (latestSync) {
            setDisplayedJobs((prev) =>
              prev.some((j) => j.id === latestSync.id) ? prev : [...prev, { ...latestSync, status: latestSync.status }],
            )
            setSelectedConsoleJobId(latestSync.id)
            setConsoleOpen(true)
          } else {
            window.alert('No recent sync run was found for this service.')
          }
        }}
      >
        <Terminal className="h-3 w-3 mr-1.5" /> View Recent Logs
      </Button>
      {status?.ngwaf_workspace_id && (
        <Button
          size="sm"
          variant="outline"
          className="h-8 text-xs bg-background"
          onClick={() => {
            setSseTitle('NGWAF Bot Sync')
            setSseDescription(
              'Fetching verified bot records from Fastly NGWAF and caching them locally. Progress is saved after each page — run again if the time budget is reached.',
            )
            setIsSSEModalOpen(true)
            reset()
            start(`/api/services/${activeServiceId}/ngwaf-sync`, {})
            queryClient.invalidateQueries({ queryKey: queryKeys.admin.cronLogsFor(activeServiceId) })
          }}
        >
          <Bot className="h-3 w-3 mr-1.5" /> NGWAF Bot Sync
        </Button>
      )}
    </div>
  )
}

interface TreeProps {
  prefix: string
  level: number
  type: 'storage' | 'raw'
  forceExpand?: boolean
}

function Node({ node, prefix, level, type, forceExpand }: TreeProps & { node: TreeNode }) {
  const [isOpen, setIsOpen] = useState(false)
  const { full, abbr, relative } = useDateFormat()
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
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
            'flex items-center justify-between gap-2 py-1.5 hover:bg-muted/50 rounded-md transition-colors group',
            level === 0 ? 'font-medium' : 'text-sm text-muted-foreground',
          )}
          style={{ paddingLeft: `${level * 16 + 8}px`, paddingRight: '8px' }}
        >
          <button
            type="button"
            aria-expanded={isOpen}
            onClick={() => setIsOpen(!isOpen)}
            className="flex items-center gap-2 min-w-0 flex-1 bg-transparent border-0 p-0 text-left cursor-pointer"
          >
            {isOpen ? (
              <ChevronDown className="h-4 w-4 shrink-0 text-muted-foreground" />
            ) : (
              <ChevronRight className="h-4 w-4 shrink-0 text-muted-foreground" />
            )}
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
                title="Download folder as ZIP"
                className="h-6 w-6 text-muted-foreground transition-opacity"
                onClick={(e) => {
                  e.stopPropagation()
                  window.open(
                    `/api/download-folder?service=${activeServiceId}&root=${type}&prefix=${encodeURIComponent(node.prefix || prefix + node.name)}`,
                    '_blank',
                  )
                }}
              >
                <Download className="h-3.5 w-3.5" />
              </Button>
            </div>
          </div>
        </div>
        {isOpen && <Tree prefix={`${prefix}${node.name}/`} level={level + 1} type={type} forceExpand={forceExpand} />}
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
          <Tooltip>
            <TooltipTrigger
              render={
                <span
                  tabIndex={0}
                  role="button"
                  aria-label="Show full modified timestamp"
                  className="hidden sm:inline-block text-xs text-muted-foreground w-[120px] text-right truncate rounded-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                >
                  {relative(node.mtime)}
                </span>
              }
            />
            <TooltipContent className="text-xs">
              {full(node.mtime)} {abbr()}
            </TooltipContent>
          </Tooltip>
        )}
        <div className="w-20 flex items-center justify-end gap-2">
          {node.sync_status === 'synced' && (
            <Badge
              variant="outline"
              className="px-1.5 py-0 h-5 text-[10px] bg-green-500/10 text-green-600 border-green-500/20 shadow-none"
            >
              <Cloud className="w-3 h-3 mr-1" /> Synced
            </Badge>
          )}
          {node.sync_status === 'local' && (
            <Badge
              variant="outline"
              className="px-1.5 py-0 h-5 text-[10px] bg-blue-500/10 text-blue-600 border-blue-500/20 shadow-none"
            >
              <HardDrive className="w-3 h-3 mr-1" /> Local
            </Badge>
          )}
          {(node.sync_status === 'cloud' || (!node.sync_status && node.is_cloud)) && (
            <Badge
              variant="outline"
              className="px-1.5 py-0 h-5 text-[10px] bg-purple-500/10 text-purple-600 border-purple-500/20 shadow-none"
            >
              <Cloud className="w-3 h-3 mr-1" /> Cloud
            </Badge>
          )}
          {node.key && (
            <Button
              variant="ghost"
              size="icon"
              aria-label={`Download ${node.name}`}
              title="Download file"
              className="h-6 w-6 text-muted-foreground transition-opacity"
              onClick={(e) => {
                e.stopPropagation()
                window.open(`/api/download?service=${activeServiceId}&key=${encodeURIComponent(node.key as string)}`, '_blank')
              }}
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
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const { data, isLoading, error } = useQuery({
    queryKey: queryKeys.admin.fileTree(type, activeServiceId, prefix),
    queryFn: () => {
      const init = { params: { query: { prefix: prefix || undefined } } }
      return type === 'storage'
        ? unwrapClient(client.GET('/api/admin/storage-tree', init))
        : unwrapClient(client.GET('/api/admin/raw-tree', init))
    },
    enabled: !!activeServiceId,
  })
  const nodes = data?.nodes ?? []
  const parentRef = React.useRef<HTMLDivElement>(null)
  const shouldVirtualize = nodes.length > 50 && forceExpand !== true
  const rowVirtualizer = useVirtualizer({
    count: shouldVirtualize ? nodes.length : 0,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 36,
    overscan: 15,
  })

  if (isLoading) {
    return (
      <div
        className="flex items-center gap-2 py-2 text-sm text-muted-foreground animate-pulse"
        style={{ paddingLeft: `${level * 16 + 28}px` }}
      >
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
  if (nodes.length === 0) {
    return (
      <div className="py-2 text-xs text-muted-foreground italic" style={{ paddingLeft: `${level * 16 + 32}px` }}>
        Empty directory
      </div>
    )
  }
  const virtualItems = shouldVirtualize ? rowVirtualizer.getVirtualItems() : []
  if (shouldVirtualize && virtualItems.length > 0) {
    return (
      <div ref={parentRef} className="max-h-[420px] overflow-y-auto">
        <div style={{ height: `${rowVirtualizer.getTotalSize()}px`, position: 'relative' }}>
          {virtualItems.map((vItem) => {
            const node = nodes[vItem.index]
            return (
              <div
                key={node.name}
                style={{
                  position: 'absolute',
                  top: 0,
                  left: 0,
                  width: '100%',
                  transform: `translateY(${vItem.start}px)`,
                }}
              >
                <Node node={node} prefix={prefix} level={level} type={type} forceExpand={forceExpand} />
              </div>
            )
          })}
        </div>
      </div>
    )
  }
  return (
    <div ref={parentRef} className="flex flex-col">
      {nodes.map((node: TreeNode) => (
        <Node key={node.name} node={node} prefix={prefix} level={level} type={type} forceExpand={forceExpand} />
      ))}
    </div>
  )
}

export function FileBrowser({ type }: { type: 'storage' | 'raw' }) {
  const [forceExpand, setForceExpand] = useState<boolean | undefined>(undefined)
  const queryClient = useQueryClient()
  const [isRefreshing, setIsRefreshing] = useState(false)
  const activeServiceId = useServiceStore((s) => s.activeServiceId)

  const handleRefresh = async () => {
    setIsRefreshing(true)
    await queryClient.invalidateQueries({ queryKey: queryKeys.admin.fileTreeFor(type, activeServiceId) })
    setTimeout(() => setIsRefreshing(false), 500)
  }

  return (
    <div className="flex flex-col font-sans h-full relative">
      <div className="flex items-center justify-between px-4 py-2 border-b bg-muted/5 sticky top-0 z-10 backdrop-blur-sm">
        <span className="text-[10px] font-bold text-muted-foreground uppercase tracking-widest">
          {type === 'storage' ? 'DuckLake Data' : 'Raw Archives'}
        </span>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            className="h-7 w-7 p-0 font-semibold shadow-none border-muted/60 bg-background"
            onClick={handleRefresh}
            title="Refresh file list"
          >
            <RefreshCw className={cn('h-3.5 w-3.5', isRefreshing && 'animate-spin')} />
          </Button>
          <Button
            variant="outline"
            size="sm"
            className="h-7 text-[10px] px-2 font-semibold shadow-none border-muted/60 bg-background"
            onClick={() => setForceExpand((prev) => (prev === undefined ? true : !prev))}
          >
            {forceExpand ? <Minimize2 className="h-3 w-3 mr-1" /> : <Maximize2 className="h-3 w-3 mr-1" />}
            {forceExpand ? 'Collapse All' : 'Expand All'}
          </Button>
        </div>
      </div>
      <div className="p-2 overflow-x-auto min-w-[500px]">
        <TooltipProvider>
          <Tree prefix="" level={0} type={type} forceExpand={forceExpand} />
        </TooltipProvider>
      </div>
    </div>
  )
}

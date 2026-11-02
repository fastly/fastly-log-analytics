'use client'

import * as React from 'react'
import { usePathname } from 'next/navigation'
import type { ColumnDef, Table as ReactTable, VisibilityState } from '@tanstack/react-table'
import { ChevronDown, Copy, ExternalLink, Filter, Settings2 } from 'lucide-react'
import { formatBytes, formatCount, formatCountMaybe, cn, copyToClipboard, isIpFamilyField } from '@/lib/utils'
import {
  DropdownMenu, DropdownMenuCheckboxItem, DropdownMenuContent,
  DropdownMenuItem, DropdownMenuShortcut, DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { Button } from '@/components/ui/button'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { useDateFormat } from '@/hooks/useSSE'
import { useMaskIps } from '@/hooks/useBootstrap'
import { TimeAgo } from '@/components/SyncStatusBadge/SyncStatusBadge'
import type { components } from '@/types/api.generated'
import { useFilterStore, usePopGeoStore } from '@/stores/filterStore'

// ─── PopLabel & PopGeo ───────────────────────────────────────────────────────

export type PopGeo = components['schemas']['BootstrapPopGeo']
const COUNTRY_ABBR: Record<string, string> = { us: 'USA', uk: 'UK', gb: 'UK' }

export function formatPopGeo(geo?: PopGeo | null): string {
  if (!geo) return ''
  const city = (geo.city ?? '').replace(' (Metro)', '').trim()
  if (!city) return ''
  const region = (geo.region ?? '').toUpperCase()
  const cc = (geo.country ?? '').toLowerCase()
  const country = COUNTRY_ABBR[cc] ?? cc.toUpperCase()
  const norm = (s: string) => s.replace(/[^a-z]/gi, '').toUpperCase()
  const locality = region && norm(region) !== norm(city) ? `${city}, ${region}` : city
  return country ? `${locality} - ${country}` : locality
}

export function usePopGeo(code?: string | null): string {
  const map = usePopGeoStore((s) => s.map)
  return code && map ? formatPopGeo(map[String(code).toUpperCase()]) : ''
}

export function PopLabel({ code, className }: { code?: string | null; className?: string }) {
  const raw = (code ?? '').toString()
  const geo = usePopGeo(raw)
  if (!raw) return null
  return (
    <span className={className}>
      {/^[a-z]{2,4}$/i.test(raw) ? raw.toUpperCase() : raw}
      {geo && <span className="ml-1 text-[10px] font-normal text-muted-foreground">({geo})</span>}
    </span>
  )
}

// ─── FilterValueCell ─────────────────────────────────────────────────────────

interface FilterValueCellFilter { column: string; value: string }
interface FilterValueCellProps {
  filters: FilterValueCellFilter[]; display?: React.ReactNode; className?: string; containerClassName?: string
}

const PAGE_LABELS: Record<string, string> = {
  '/dashboard': 'dashboard', '/origin': 'origin', '/performance': 'performance', '/security': 'security',
  '/network': 'network', '/sessions': 'sessions', '/charts': 'charts', '/usage': 'usage', '/query': 'query',
}

function pageLabelFor(pathname: string | null): string {
  if (!pathname) return 'this page'
  for (const [prefix, label] of Object.entries(PAGE_LABELS)) {
    if (pathname === prefix || pathname.startsWith(prefix + '/')) return label
  }
  return 'this page'
}

export function buildDashboardFilterUrl(filters: FilterValueCellFilter[]): string {
  return `/dashboard?${filters.map((f) => `filter_${f.column}=${encodeURIComponent(f.value)}`).join('&')}`
}

export function FilterValueCell({ filters, display, className, containerClassName }: FilterValueCellProps) {
  const pathname = usePathname()
  const addFilter = useFilterStore((state) => state.addFilter)
  const maskIps = useMaskIps()
  const ipFilterBlocked = maskIps && filters.some((f) => isIpFamilyField(f.column))
  const onDashboard = pathname === '/dashboard'
  const pageLabel = pageLabelFor(pathname)
  const shownValue = display ?? filters[0]?.value ?? ''
  const [open, setOpen] = React.useState(false)

  const handleFilterHere = React.useCallback(() => {
    for (const f of filters) addFilter(f.column, f.value, 'include')
  }, [filters, addFilter])

  const handleOpenInDashboard = React.useCallback(() => {
    window.open(buildDashboardFilterUrl(filters), '_blank', 'noopener,noreferrer')
  }, [filters])

  const handleCopy = React.useCallback(() => {
    const v = filters[0]?.value
    if (v) void copyToClipboard(v)
  }, [filters])

  const skipNextOpenRef = React.useRef(false)
  const handleMouseDown = React.useCallback((e: React.MouseEvent) => {
    if (e.metaKey || e.ctrlKey) {
      e.preventDefault()
      e.stopPropagation()
      skipNextOpenRef.current = true
      handleFilterHere()
    }
  }, [handleFilterHere])

  const handleOpenChange = React.useCallback((next: boolean) => {
    if (next && skipNextOpenRef.current) {
      skipNextOpenRef.current = false
      return
    }
    setOpen(next)
  }, [])

  if (filters.length === 0 || shownValue === '' || shownValue == null || ipFilterBlocked) {
    return (
      <div className={cn('flex items-center gap-2', containerClassName)}>
        <span className={cn('truncate block', className)}>{shownValue}</span>
      </div>
    )
  }

  return (
    <DropdownMenu open={open} onOpenChange={handleOpenChange}>
      <DropdownMenuTrigger
        render={
          <button
            type="button"
            aria-label={`Filter actions for ${filters[0].value}`}
            title="Click for actions · ⌘/Ctrl-click to filter this page"
            onMouseDown={handleMouseDown}
            className={cn(
              'group flex items-center gap-2 text-left rounded-sm -mx-1 px-1 py-0.5 hover:bg-accent/60 data-[popup-open]:bg-accent/60 transition-colors w-full min-w-0',
              containerClassName,
            )}
          />
        }
      >
        <span className={cn('truncate block flex-1', className)}>{shownValue}</span>
        <ChevronDown className="h-3 w-3 opacity-0 group-hover:opacity-70 data-[popup-open]:opacity-100 shrink-0 text-muted-foreground transition-opacity" aria-hidden="true" />
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start" sideOffset={2} className="min-w-[200px]">
        <DropdownMenuItem onClick={handleFilterHere}>
          <Filter className="h-3.5 w-3.5" aria-hidden="true" />
          <span>Filter {pageLabel} page</span>
          <DropdownMenuShortcut>⌘+Click</DropdownMenuShortcut>
        </DropdownMenuItem>
        {!onDashboard && (
          <DropdownMenuItem onClick={handleOpenInDashboard}>
            <ExternalLink className="h-3.5 w-3.5" aria-hidden="true" />
            <span>Open in dashboard</span>
          </DropdownMenuItem>
        )}
        <DropdownMenuItem onClick={handleCopy}>
          <Copy className="h-3.5 w-3.5" aria-hidden="true" />
          <span>Copy value</span>
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

// ─── ColumnVisibilityDropdown ────────────────────────────────────────────────

interface BaseColumnVisibilityDropdownProps {
  className?: string; align?: 'start' | 'end'; size?: 'sm' | 'default' | 'icon'; compact?: boolean
}

type ColumnVisibilityDropdownProps<TData = unknown> = BaseColumnVisibilityDropdownProps &
  (
    | { columns: { id: string; label: string }[]; visibility: VisibilityState; onChange: (id: string, visible: boolean) => void; table?: never }
    | { table: ReactTable<TData>; columns?: never; visibility?: never; onChange?: never }
  )

export function ColumnVisibilityDropdown<TData = unknown>(props: ColumnVisibilityDropdownProps<TData>) {
  const { className, align = 'end', compact = false } = props
  const size = props.size ?? (compact ? 'sm' : 'sm')

  const items = props.table
    ? props.table.getAllColumns().filter((c) => c.getCanHide()).map((c) => ({
        id: c.id,
        label: c.columnDef.meta?.label ?? (typeof c.columnDef.header === 'string' ? c.columnDef.header : c.id.replace(/_/g, ' ')),
        visible: c.getIsVisible(),
        onToggle: (visible: boolean) => c.toggleVisibility(visible),
      }))
    : props.columns.map((c) => ({
        id: c.id, label: c.label, visible: props.visibility[c.id] !== false,
        onToggle: (visible: boolean) => props.onChange(c.id, visible),
      }))

  return (
    <DropdownMenu>
      <DropdownMenuTrigger render={<Button variant="outline" size={size} className={cn('h-8 gap-2 px-2 text-xs font-normal', className)} />}>
        {size === 'icon' ? <Settings2 className="h-4 w-4" /> : <>Columns<ChevronDown className="h-3 w-3 opacity-50" /></>}
      </DropdownMenuTrigger>
      <DropdownMenuContent align={align} className="w-auto min-w-[180px]">
        {items.map((item) => (
          <DropdownMenuCheckboxItem key={item.id} className="capitalize whitespace-nowrap" checked={item.visible} onCheckedChange={(v) => item.onToggle(!!v)}>
            {item.label}
          </DropdownMenuCheckboxItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

// ─── DateTimeCell ────────────────────────────────────────────────────────────

interface DateTimeCellProps { iso: string | null | undefined; className?: string; emptyFallback?: React.ReactNode }

export function DateTimeCell({ iso, className, emptyFallback }: DateTimeCellProps) {
  const { full, abbr } = useDateFormat()
  if (!iso) return <>{emptyFallback ?? <span className="text-muted-foreground/40">—</span>}</>
  return (
    <Tooltip>
      <TooltipTrigger render={<span className={className ?? 'text-muted-foreground whitespace-nowrap border-b border-dotted border-muted-foreground/30'} />}>
        <TimeAgo timestamp={iso} />
      </TooltipTrigger>
      <TooltipContent className="text-xs">{full(iso)} {abbr()}</TooltipContent>
    </Tooltip>
  )
}

// ─── Shared Table Column Builders ────────────────────────────────────────────

export const ingestedFilesColumns: ColumnDef<components['schemas']['IngestedFile']>[] = [
  { accessorKey: 'file_name', id: 'file_name', meta: { label: 'File Name' }, header: 'File Name', cell: ({ row }) => <span className="font-mono text-xs">{row.original.file_name}</span> },
  { accessorKey: 'ingested_at', id: 'ingested_at', meta: { label: 'Ingested At' }, header: 'Ingested At', cell: ({ row }) => <DateTimeCell iso={row.original.ingested_at} /> },
  {
    accessorKey: 'row_count', id: 'row_count', meta: { label: 'Rows' }, header: 'Rows',
    cell: ({ row }) => <span className="font-mono text-muted-foreground tabular-nums text-xs">{formatCount(row.original.row_count || 0)}</span>,
  },
  {
    accessorKey: 'file_size_bytes', id: 'file_size_bytes', meta: { label: 'Size' }, header: 'Size',
    cell: ({ row }) => {
      const bytes = row.original.file_size_bytes
      return <span className="font-mono text-muted-foreground tabular-nums text-xs">{bytes ? formatBytes(bytes) : '—'}</span>
    },
  },
]

export const columnHeader = (label: string) =>
  function ColumnHeaderCell() {
    return <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">{label}</span>
  }

export interface LatencyRow {
  requests?: number | null; avg?: number | null; p50?: number | null; p95?: number | null; p99?: number | null
}

export const textValue = (v: unknown): string => (typeof v === 'string' ? v : '')
export const numberValue = (v: unknown): number | null | undefined => (typeof v === 'number' ? v : null)
export const msCell = (value: unknown) => <span>{numberValue(value)?.toFixed(1)}ms</span>
const ms2Cell = (info: { getValue: () => unknown }) => numberValue(info.getValue())?.toFixed(2) ?? '0.00'

export const makeLatencyColumns = <TRow extends LatencyRow>(
  labelField: keyof TRow & string,
  labelName: string,
  filterField: keyof TRow & string,
): ColumnDef<TRow>[] => [
  {
    accessorKey: labelField,
    header: columnHeader(labelName),
    cell: (info) => {
      const val: unknown = info.row.original[filterField]
      const rawDisplay = info.getValue()
      const displayVal = typeof rawDisplay === 'string' || typeof rawDisplay === 'number' ? rawDisplay : null
      if (val == null) return <span className="font-mono text-xs truncate block max-w-[300px]">{displayVal}</span>
      return <FilterValueCell filters={[{ column: filterField, value: String(val) }]} display={displayVal} className="font-mono text-xs" containerClassName="max-w-[300px]" />
    },
  },
  { accessorKey: 'requests', header: columnHeader('Reqs'), cell: (info) => formatCountMaybe(numberValue(info.getValue())) ?? '0' },
  { accessorKey: 'avg', header: columnHeader('Avg (ms)'), cell: ms2Cell },
  { accessorKey: 'p50', header: columnHeader('P50'), cell: ms2Cell },
  { accessorKey: 'p95', header: columnHeader('P95'), cell: ms2Cell },
  { accessorKey: 'p99', header: columnHeader('P99'), cell: ms2Cell },
]

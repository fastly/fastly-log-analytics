'use client'

import * as React from 'react'
import { usePathname } from 'next/navigation'
import { flexRender, type Header, type ColumnDef, type Row, type Table as ReactTable, type VisibilityState } from '@tanstack/react-table'
import type { Virtualizer } from '@tanstack/react-virtual'
import { ArrowDown, ArrowUp, ArrowUpDown, ChevronDown, Copy, ExternalLink, Filter, GripVertical, Inbox, Settings2 } from 'lucide-react'
import {
  DndContext,
  KeyboardSensor,
  MouseSensor,
  TouchSensor,
  closestCenter,
  useSensor,
  useSensors,
  DragEndEvent,
} from '@dnd-kit/core'
import { restrictToHorizontalAxis } from '@dnd-kit/modifiers'
import {
  arrayMove,
  SortableContext,
  horizontalListSortingStrategy,
  useSortable,
} from '@dnd-kit/sortable'
import { CSS } from '@dnd-kit/utilities'
import { formatBytes, formatCount, formatCountMaybe } from '@/lib/format'

import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuShortcut,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { useDateFormat } from '@/hooks/useSSE'
import { useMaskIps } from '@/hooks/useBootstrap'
import { TimeAgo } from '@/components/SyncStatusBadge/SyncStatusBadge'
import { useDataTableState, type DataTableBaseProps } from './useDataTableState'
import { getApiBase } from '@/lib/api'
import type { Schema } from '@/types/api'
import type { components } from '@/types/api.generated'
import { cn, copyToClipboard, isIpFamilyField } from '@/lib/utils'
import { useFilterStore } from '@/stores/filterStore'
import { usePopGeoStore } from '@/stores/popGeoStore'

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
  const showRegion = region && norm(region) !== norm(city)
  const locality = showRegion ? `${city}, ${region}` : city
  return country ? `${locality} - ${country}` : locality
}

function usePopGeoMap(): Record<string, PopGeo> {
  return usePopGeoStore((s) => s.map)
}

export function usePopGeo(code?: string | null): string {
  const map = usePopGeoMap()
  if (!code || !map) return ''
  return formatPopGeo(map[String(code).toUpperCase()])
}

export function PopLabel({ code, className }: { code?: string | null; className?: string }) {
  const raw = (code ?? '').toString()
  const geo = usePopGeo(raw)
  if (!raw) return null
  const display = /^[a-z]{2,4}$/i.test(raw) ? raw.toUpperCase() : raw
  return (
    <span className={className}>
      {display}
      {geo && <span className="ml-1 text-[10px] font-normal text-muted-foreground">({geo})</span>}
    </span>
  )
}

// ─── FilterValueCell ─────────────────────────────────────────────────────────

interface FilterValueCellFilter {
  column: string
  value: string
}

interface FilterValueCellProps {
  filters: FilterValueCellFilter[]
  display?: React.ReactNode
  className?: string
  containerClassName?: string
}

const PAGE_LABELS: Record<string, string> = {
  '/dashboard': 'dashboard',
  '/origin': 'origin',
  '/performance': 'performance',
  '/security': 'security',
  '/network': 'network',
  '/sessions': 'sessions',
  '/charts': 'charts',
  '/usage': 'usage',
  '/query': 'query',
}

function pageLabelFor(pathname: string | null): string {
  if (!pathname) return 'this page'
  for (const [prefix, label] of Object.entries(PAGE_LABELS)) {
    if (pathname === prefix || pathname.startsWith(prefix + '/')) return label
  }
  return 'this page'
}

export function buildDashboardFilterUrl(filters: FilterValueCellFilter[]): string {
  const qs = filters
    .map(f => `filter_${f.column}=${encodeURIComponent(f.value)}`)
    .join('&')
  return `/dashboard?${qs}`
}

export function FilterValueCell({
  filters,
  display,
  className,
  containerClassName,
}: FilterValueCellProps) {
  const pathname = usePathname()
  const addFilter = useFilterStore(state => state.addFilter)
  const maskIps = useMaskIps()
  const ipFilterBlocked = maskIps && filters.some(f => isIpFamilyField(f.column))
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
  const handleMouseDown = React.useCallback(
    (e: React.MouseEvent) => {
      if (e.metaKey || e.ctrlKey) {
        e.preventDefault()
        e.stopPropagation()
        skipNextOpenRef.current = true
        handleFilterHere()
      }
    },
    [handleFilterHere],
  )
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
        <ChevronDown
          className="h-3 w-3 opacity-0 group-hover:opacity-70 data-[popup-open]:opacity-100 shrink-0 text-muted-foreground transition-opacity"
          aria-hidden="true"
        />
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
  className?: string
  align?: 'start' | 'end'
  size?: 'sm' | 'default' | 'icon'
  compact?: boolean
}

type ColumnVisibilityDropdownProps<TData = unknown> = BaseColumnVisibilityDropdownProps &
  (
    | {
        columns: { id: string; label: string }[]
        visibility: VisibilityState
        onChange: (id: string, visible: boolean) => void
        table?: never
      }
    | {
        table: ReactTable<TData>
        columns?: never
        visibility?: never
        onChange?: never
      }
  )

export function ColumnVisibilityDropdown<TData = unknown>(
  props: ColumnVisibilityDropdownProps<TData>,
) {
  const { className, align = 'end', compact = false } = props
  const size = props.size ?? (compact ? 'sm' : 'sm')

  const items = props.table
    ? props.table
        .getAllColumns()
        .filter((column) => column.getCanHide())
        .map((column) => ({
          id: column.id,
          label:
            column.columnDef.meta?.label ??
            (typeof column.columnDef.header === 'string'
              ? column.columnDef.header
              : column.id.replace(/_/g, ' ')),
          visible: column.getIsVisible(),
          onToggle: (visible: boolean) => column.toggleVisibility(visible),
        }))
    : props.columns.map((column) => ({
        id: column.id,
        label: column.label,
        visible: props.visibility[column.id] !== false,
        onToggle: (visible: boolean) => props.onChange(column.id, visible),
      }))

  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        render={
          <Button
            variant="outline"
            size={size}
            className={cn('h-8 gap-2 px-2 text-xs font-normal', className)}
          />
        }
      >
        {size === 'icon' ? (
          <Settings2 className="h-4 w-4" />
        ) : (
          <>
            Columns
            <ChevronDown className="h-3 w-3 opacity-50" />
          </>
        )}
      </DropdownMenuTrigger>
      <DropdownMenuContent align={align} className="w-auto min-w-[180px]">
        {items.map((item) => (
          <DropdownMenuCheckboxItem
            key={item.id}
            className="capitalize whitespace-nowrap"
            checked={item.visible}
            onCheckedChange={(value) => item.onToggle(!!value)}
          >
            {item.label}
          </DropdownMenuCheckboxItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

// ─── DataTableBody / MemoizedTableRow ────────────────────────────────────────

interface TableRowProps<TData> {
  row: Row<TData>
  onRowClick?: (row: TData) => void
  rowClassName?: string
  columnVisibility: VisibilityState
  columnSizing?: Record<string, number>
}

function TableRowImpl<TData>({ row, onRowClick, rowClassName }: TableRowProps<TData>) {
  return (
    <TableRow
      data-state={row.getIsSelected() && 'selected'}
      onClick={() => onRowClick?.(row.original)}
      tabIndex={onRowClick ? 0 : undefined}
      onKeyDown={
        onRowClick
          ? (e) => {
              if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault()
                onRowClick(row.original)
              }
            }
          : undefined
      }
      className={cn(
        onRowClick &&
          'cursor-pointer hover:bg-muted/50 focus-visible:outline-none focus-visible:bg-muted/50 focus-visible:ring-1 focus-visible:ring-inset focus-visible:ring-ring',
        rowClassName,
      )}
    >
      {row.getVisibleCells().map((cell) => {
        const meta = cell.column.columnDef.meta as { cellClassName?: string } | undefined
        return (
          <TableCell
            key={cell.id}
            style={{ width: cell.column.getSize(), maxWidth: cell.column.getSize() }}
            className={cn('truncate', meta?.cellClassName)}
          >
            {flexRender(cell.column.columnDef.cell, cell.getContext())}
          </TableCell>
        )
      })}
    </TableRow>
  )
}

export const MemoizedTableRow = React.memo(TableRowImpl) as typeof TableRowImpl

interface DataTableBodyProps<TData, TValue> {
  table: ReactTable<TData>
  rows: Row<TData>[]
  rowVirtualizer: Virtualizer<HTMLDivElement, Element>
  columns: ColumnDef<TData, TValue>[]
  columnVisibility: VisibilityState
  columnSizing?: Record<string, number>
  isLoading?: boolean
  onRowClick?: (row: TData) => void
  getRowClassName?: (row: TData) => string
  emptyMessage?: string
  emptyHint?: React.ReactNode
  onClearFilter?: () => void
}

export function DataTableBody<TData, TValue>({
  table,
  rows,
  rowVirtualizer,
  columns,
  columnVisibility,
  columnSizing,
  isLoading,
  onRowClick,
  getRowClassName,
  emptyMessage = 'No results.',
  emptyHint,
  onClearFilter,
}: DataTableBodyProps<TData, TValue>) {
  return (
    <TableBody>
      {isLoading ? (
        <TableRow>
          <TableCell
            colSpan={columns.length}
            className="h-24 text-center text-muted-foreground"
          >
            Loading...
          </TableCell>
        </TableRow>
      ) : table.getRowModel().rows?.length ? (
        <>
          {rowVirtualizer.getVirtualItems().length > 0 && rowVirtualizer.getVirtualItems()[0].start > 0 && (
            <TableRow role="presentation" aria-hidden="true">
              <TableCell role="none" colSpan={columns.length} style={{ height: rowVirtualizer.getVirtualItems()[0].start, padding: 0, border: 0 }} />
            </TableRow>
          )}
          {rowVirtualizer.getVirtualItems().map((virtualRow) => {
            const row = rows[virtualRow.index]
            return (
              <MemoizedTableRow
                key={row.id}
                row={row}
                onRowClick={onRowClick}
                rowClassName={getRowClassName ? getRowClassName(row.original) : undefined}
                columnVisibility={columnVisibility}
                columnSizing={columnSizing}
              />
            )
          })}
          {rowVirtualizer.getVirtualItems().length > 0 && rowVirtualizer.getVirtualItems()[rowVirtualizer.getVirtualItems().length - 1].end < rowVirtualizer.getTotalSize() && (
            <TableRow role="presentation" aria-hidden="true">
              <TableCell role="none" colSpan={columns.length} style={{ height: rowVirtualizer.getTotalSize() - rowVirtualizer.getVirtualItems()[rowVirtualizer.getVirtualItems().length - 1].end, padding: 0, border: 0 }} />
            </TableRow>
          )}
        </>
      ) : (
        <TableRow>
          <TableCell
            colSpan={columns.length}
            className="h-32 text-center text-muted-foreground"
          >
            <div className="flex flex-col items-center justify-center gap-2 py-4">
              <Inbox className="h-7 w-7 opacity-30" aria-hidden="true" />
              <p className="text-sm font-medium">{emptyMessage}</p>
              {emptyHint && (
                <p className="text-xs text-muted-foreground">{emptyHint}</p>
              )}
              {onClearFilter && (
                <Button variant="outline" size="sm" className="mt-1" onClick={onClearFilter}>
                  Clear filter
                </Button>
              )}
            </div>
          </TableCell>
        </TableRow>
      )}
    </TableBody>
  )
}

// ─── StaticHeader / DraggableTableHeader ─────────────────────────────────────

export function getHeaderAriaSort<TData>(
  header: Header<TData, unknown>,
): 'ascending' | 'descending' | 'none' | undefined {
  if (!header.column.getCanSort()) return undefined
  const sorted = header.column.getIsSorted()
  return sorted === 'asc' ? 'ascending' : sorted === 'desc' ? 'descending' : 'none'
}

export function HeaderCellContent<TData>({
  header,
  dragHandle,
}: {
  header: Header<TData, unknown>
  dragHandle?: React.ReactNode
}) {
  return (
    <div className="flex items-center justify-between gap-1 w-full h-full pl-3 pr-2 overflow-hidden">
      {header.column.getCanSort() && !header.isPlaceholder ? (
        <button
          type="button"
          className="flex-1 flex items-center hover:text-foreground transition-colors overflow-hidden cursor-pointer focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring rounded-sm text-left bg-transparent border-0 p-0 font-inherit"
          onClick={header.column.getToggleSortingHandler()}
        >
          <span className="truncate">
            {flexRender(header.column.columnDef.header, header.getContext())}
          </span>
          <span className="ml-2 flex items-center shrink-0">
            {{
              asc: <ArrowUp className="w-3.5 h-3.5" />,
              desc: <ArrowDown className="w-3.5 h-3.5" />,
            }[header.column.getIsSorted() as string] ?? (
              <ArrowUpDown className="w-3.5 h-3.5 opacity-0 group-hover:opacity-50 transition-opacity" />
            )}
          </span>
        </button>
      ) : (
        <div className="flex-1 flex items-center overflow-hidden">
          <span className="truncate">
            {header.isPlaceholder
              ? null
              : flexRender(header.column.columnDef.header, header.getContext())}
          </span>
        </div>
      )}
      {dragHandle}
      <div
        role="presentation"
        onMouseDown={(e) => {
          try { header.getResizeHandler()(e) } catch { /* stale header */ }
        }}
        onTouchStart={(e) => {
          try { header.getResizeHandler()(e) } catch { /* stale header */ }
        }}
        className={cn(
          'absolute right-0 top-0 h-full w-2 cursor-col-resize hover:bg-primary/30 transition-colors z-10 touch-none',
          header.column.getIsResizing() ? 'bg-primary opacity-100' : 'opacity-0 group-hover:opacity-100',
        )}
      />
    </div>
  )
}

export const StaticTableHeader = <TData,>({ header }: { header: Header<TData, unknown> }) => (
  <TableHead
    style={{ width: header.column.getSize(), whiteSpace: 'nowrap' }}
    aria-sort={getHeaderAriaSort(header)}
    className="relative group select-none border-r last:border-r-0 px-0"
  >
    <HeaderCellContent header={header} />
  </TableHead>
)

export const DraggableTableHeader = <TData,>({ header }: { header: Header<TData, unknown> }) => {
  const { attributes, isDragging, listeners, setNodeRef, transform, transition } = useSortable({
    id: header.column.id,
  })

  const style: React.CSSProperties = {
    opacity: isDragging ? 0.8 : 1,
    position: 'relative',
    transform: CSS.Translate.toString(transform),
    transition,
    whiteSpace: 'nowrap',
    width: header.column.getSize(),
    zIndex: isDragging ? 1 : 0,
  }

  return (
    <TableHead
      ref={setNodeRef}
      style={style}
      aria-sort={getHeaderAriaSort(header)}
      className={cn(
        'relative group select-none border-r last:border-r-0 px-0',
        isDragging && 'bg-muted shadow-md z-10',
      )}
    >
      <HeaderCellContent
        header={header}
        dragHandle={
          <div
            {...attributes}
            {...listeners}
            className="opacity-0 group-hover:opacity-50 hover:!opacity-100 cursor-grab active:cursor-grabbing p-1 rounded"
            title="Drag to reorder"
          >
            <GripVertical className="w-3.5 h-3.5" />
          </div>
        }
      />
    </TableHead>
  )
}

// ─── DateTimeCell ────────────────────────────────────────────────────────────

interface DateTimeCellProps {
  iso: string | null | undefined
  className?: string
  emptyFallback?: React.ReactNode
}

export function DateTimeCell({ iso, className, emptyFallback }: DateTimeCellProps) {
  // Absolute time (tooltip content) is static; the relative "X ago" trigger
  // ticks every second via the shared <TimeAgo> so table cells stop looking
  // frozen between query refetches. Only the inner text node re-renders.
  const { full, abbr } = useDateFormat()
  if (!iso) return <>{emptyFallback ?? <span className="text-muted-foreground/40">—</span>}</>
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <span
            className={
              className ??
              'text-muted-foreground whitespace-nowrap border-b border-dotted border-muted-foreground/30'
            }
          />
        }
      >
        <TimeAgo timestamp={iso} />
      </TooltipTrigger>
      <TooltipContent className="text-xs">
        {full(iso)} {abbr()}
      </TooltipContent>
    </Tooltip>
  )
}

// ─── DataTable ───────────────────────────────────────────────────────────────

interface DataTableToolbarProps<TData> {
  table: ReactTable<TData>
  title?: React.ReactNode
  searchKey?: string
  compactToolbar?: boolean
  extraToolbarContent?: React.ReactNode
}

export const DataTableToolbar = <TData,>({
  table,
  title,
  searchKey,
  compactToolbar = false,
  extraToolbarContent,
}: DataTableToolbarProps<TData>) => {
  return (
    <div className={cn("flex items-center gap-4", compactToolbar ? "mb-2" : "py-4 px-4")}>
      {title && (
        <div className="flex-1">{title}</div>
      )}
      {searchKey && (
        <Input
          placeholder={`Filter ${searchKey}...`}
          value={(table.getColumn(searchKey)?.getFilterValue() as string) ?? ""}
          onChange={(event) =>
            table.getColumn(searchKey)?.setFilterValue(event.target.value)
          }
          className="max-w-sm h-8"
        />
      )}
      {extraToolbarContent && (
        <div className="flex items-center gap-2">
          {extraToolbarContent}
        </div>
      )}
      <div className="ml-auto flex items-center gap-2">
        <ColumnVisibilityDropdown table={table} compact={compactToolbar} />
      </div>
    </div>
  )
}

/**
 * Pagination footer shared by DataTable + DataTableReadonly.
 * The `>= 19` row-count gate and the [10, 20, 50, 100, 500] page
 * sizes are load-bearing — kept byte-identical to both former inline copies.
 */
export function DataTablePagination<TData>({ table }: { table: ReactTable<TData> }) {
  if (table.getFilteredRowModel().rows.length < 19) return null
  return (
    <div className="flex items-center justify-end px-4 py-4 border-t">
      <div className="flex items-center space-x-6 lg:space-x-8">
        <div className="flex items-center space-x-2">
          <p className="text-sm font-medium">Rows per page</p>
          <Select
            value={`${table.getState().pagination.pageSize}`}
            onValueChange={(value) => {
              table.setPageSize(Number(value))
            }}
          >
            <SelectTrigger className="h-8 w-[70px]">
              <SelectValue placeholder={table.getState().pagination.pageSize} />
            </SelectTrigger>
            <SelectContent side="top">
              {[10, 20, 50, 100, 500].map((pageSize) => (
                <SelectItem key={pageSize} value={`${pageSize}`}>
                  {pageSize}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="flex w-[100px] items-center justify-center text-sm font-medium">
          Page {table.getState().pagination.pageIndex + 1} of{' '}
          {table.getPageCount() || 1}
        </div>
        <div className="flex items-center space-x-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => table.previousPage()}
            disabled={!table.getCanPreviousPage()}
          >
            Previous
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={() => table.nextPage()}
            disabled={!table.getCanNextPage()}
          >
            Next
          </Button>
        </div>
      </div>
    </div>
  )
}

export function columnIdOf<TData, TValue>(c: ColumnDef<TData, TValue>): string {
  return (
    c.id ||
    (typeof (c as { accessorKey?: string }).accessorKey === 'string'
      ? (c as { accessorKey: string }).accessorKey
      : '')
  )
}

type UxEvent = Schema<'UxEventPayload'>

export function reportUxEvent(evt: UxEvent): void {
  if (typeof window === 'undefined') return
  try {
    const pathname = evt.pathname ?? window.location.pathname
    const body = JSON.stringify({
      event: evt.event,
      pathname,
      component_id: evt.component_id,
      details: evt.details ?? {},
    })
    const url = `${getApiBase()}/api/ux-events`

    if (typeof navigator !== 'undefined' && 'sendBeacon' in navigator) {
      const blob = new Blob([body], { type: 'application/json' })
      if (navigator.sendBeacon(url, blob)) return
    }
    void fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
      keepalive: true,
    })
  } catch {
    // Swallow — telemetry never breaks the page.
  }
}

export interface DataTableProps<TData, TValue> extends DataTableBaseProps<TData, TValue> {
  tableContainerClassName?: string
  tableClassName?: string
  headerRowClassName?: string
  initialColumnOrder?: string[]
  enableColumnReorder?: boolean
}

function DataTableImpl<TData, TValue>({
  tableContainerClassName,
  tableClassName,
  headerRowClassName,
  columns,
  data,
  searchKey,
  isLoading,
  showPagination = true,
  sorting: controlledSorting,
  onSortingChange,
  initialSorting = [],
  initialVisibility = {},
  initialColumnOrder,
  enableColumnReorder = true,
  title,
  compactToolbar = false,
  extraToolbarContent,
  renderToolbar,
  hideToolbar = false,
  columnVisibility: controlledVisibility,
  onColumnVisibilityChange,
  emptyMessage = "No results.",
  emptyHint,
  onClearFilter,
  onRowClick,
  getRowClassName,
  tableCaption
}: DataTableProps<TData, TValue>) {
  const {
    table,
    rows,
    rowVirtualizer,
    tableContainerRef,
    columnVisibility,
    columnSizing,
    columnOrder,
    setColumnOrder,
  } = useDataTableState({
    columns,
    data,
    showPagination,
    controlledSorting,
    onSortingChange,
    initialSorting,
    controlledVisibility,
    onColumnVisibilityChange,
    initialVisibility,
    initialColumnOrder,
    enableColumnOrder: enableColumnReorder,
  })

  const sensors = useSensors(
    useSensor(MouseSensor, { activationConstraint: { distance: 5 } }),
    useSensor(TouchSensor, { activationConstraint: { delay: 250, tolerance: 5 } }),
    useSensor(KeyboardSensor)
  )

  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event
    if (active && over && active.id !== over.id) {
      const oldIndex = columnOrder.indexOf(active.id as string)
      const newIndex = columnOrder.indexOf(over.id as string)
      if (oldIndex < 0 || newIndex < 0) return
      setColumnOrder(arrayMove(columnOrder, oldIndex, newIndex))
      reportUxEvent({
        event: 'column_reordered',
        component_id:
          tableCaption ?? (typeof title === 'string' ? title : 'unnamed'),
        details: {
          column_id: String(active.id),
          from_index: oldIndex,
          to_index: newIndex,
          column_count: columnOrder.length,
        },
      })
    }
  }

  const tableContainer = (
    <div
      ref={tableContainerRef}
      tabIndex={enableColumnReorder ? undefined : 0}
      className={cn(
        "rounded-md border overflow-auto w-full",
        !enableColumnReorder && "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
        tableContainerClassName || "max-h-[min(600px,calc(100dvh-200px))]",
      )}
    >
      <Table
        scrollFocusable={false}
        className={tableClassName}
        style={{ tableLayout: 'fixed', width: table.getTotalSize(), minWidth: '100%' }}
      >
        <caption className="sr-only">
          {tableCaption || (typeof title === 'string' ? title : 'Data Table')}
        </caption>
        <TableHeader>
          {table.getHeaderGroups().map((headerGroup) => (
            <TableRow key={headerGroup.id} className={headerRowClassName}>
              {enableColumnReorder ? (
                <SortableContext
                  items={columnOrder}
                  strategy={horizontalListSortingStrategy}
                >
                  {headerGroup.headers.map((header) => (
                    <DraggableTableHeader key={header.id} header={header} />
                  ))}
                </SortableContext>
              ) : (
                headerGroup.headers.map((header) => (
                  <StaticTableHeader key={header.id} header={header} />
                ))
              )}
            </TableRow>
          ))}
        </TableHeader>
        <DataTableBody
          table={table}
          rows={rows}
          rowVirtualizer={rowVirtualizer}
          columns={columns}
          columnVisibility={columnVisibility}
          columnSizing={columnSizing}
          isLoading={isLoading}
          onRowClick={onRowClick}
          getRowClassName={getRowClassName}
          emptyMessage={emptyMessage}
          emptyHint={emptyHint}
          onClearFilter={onClearFilter}
        />
      </Table>
    </div>
  )

  return (
    <div className="w-full">
      {!hideToolbar && (renderToolbar ? (
        renderToolbar(table)
      ) : (
        <DataTableToolbar
          table={table}
          title={title}
          searchKey={searchKey}
          compactToolbar={compactToolbar}
          extraToolbarContent={extraToolbarContent}
        />
      ))}

      {enableColumnReorder ? (
        <DndContext
          id="data-table-dnd"
          collisionDetection={closestCenter}
          modifiers={[restrictToHorizontalAxis]}
          onDragEnd={handleDragEnd}
          sensors={sensors}
        >
          {tableContainer}
        </DndContext>
      ) : (
        tableContainer
      )}

      {showPagination && <DataTablePagination table={table} />}
    </div>
  )
}

export const DataTable = React.memo(DataTableImpl) as typeof DataTableImpl

export type DataTableReadonlyProps<TData, TValue> = DataTableBaseProps<TData, TValue>

function DataTableReadonlyImpl<TData, TValue>(props: DataTableReadonlyProps<TData, TValue>) {
  return <DataTableImpl {...props} enableColumnReorder={false} />
}

export const DataTableReadonly = React.memo(DataTableReadonlyImpl) as typeof DataTableReadonlyImpl

// ─── Shared Table Column Builders (folded from lib/table-utils.tsx) ──────────

export const ingestedFilesColumns: ColumnDef<components['schemas']['IngestedFile']>[] = [
  {
    accessorKey: 'file_name',
    id: 'file_name',
    meta: { label: 'File Name' },
    header: 'File Name',
    cell: ({ row }) => <span className="font-mono text-xs">{row.original.file_name}</span>,
  },
  {
    accessorKey: 'ingested_at',
    id: 'ingested_at',
    meta: { label: 'Ingested At' },
    header: 'Ingested At',
    cell: ({ row }) => <DateTimeCell iso={row.original.ingested_at} />,
  },
  {
    accessorKey: 'row_count',
    id: 'row_count',
    meta: { label: 'Rows' },
    header: 'Rows',
    cell: ({ row }) => (
      <span className="font-mono text-muted-foreground tabular-nums text-xs">
        {formatCount(row.original.row_count || 0)}
      </span>
    ),
  },
  {
    accessorKey: 'file_size_bytes',
    id: 'file_size_bytes',
    meta: { label: 'Size' },
    header: 'Size',
    cell: ({ row }) => {
      const bytes = row.original.file_size_bytes
      return (
        <span className="font-mono text-muted-foreground tabular-nums text-xs">
          {bytes ? formatBytes(bytes) : '—'}
        </span>
      )
    },
  },
]

export const columnHeader = (label: string) =>
  function ColumnHeaderCell() {
    return <span className="text-[11px] font-bold uppercase tracking-tight text-muted-foreground">{label}</span>
  }

/** The numeric columns a latency table reads off each row. */
export interface LatencyRow {
  requests?: number | null
  avg?: number | null
  p50?: number | null
  p95?: number | null
  p99?: number | null
}

export const textValue = (v: unknown): string => (typeof v === 'string' ? v : '')
export const numberValue = (v: unknown): number | null | undefined => (typeof v === 'number' ? v : null)
export const msCell = (value: unknown) => <span>{numberValue(value)?.toFixed(1)}ms</span>

/**
 * Creates standard performance/latency columns with a filter-value drill-down
 * cell (filter the current page or open the dashboard in a new tab).
 */
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
      if (val == null) {
        return <span className="font-mono text-xs truncate block max-w-[300px]">{displayVal}</span>
      }
      return (
        <FilterValueCell
          filters={[{ column: filterField, value: String(val) }]}
          display={displayVal}
          className="font-mono text-xs"
          containerClassName="max-w-[300px]"
        />
      )
    }
  },
  { accessorKey: 'requests', header: columnHeader('Reqs'), cell: (info) => formatCountMaybe(numberValue(info.getValue())) ?? '0' },
  { accessorKey: 'avg', header: columnHeader('Avg (ms)'), cell: (info) => numberValue(info.getValue())?.toFixed(2) ?? '0.00' },
  { accessorKey: 'p50', header: columnHeader('P50'), cell: (info) => numberValue(info.getValue())?.toFixed(2) ?? '0.00' },
  { accessorKey: 'p95', header: columnHeader('P95'), cell: (info) => numberValue(info.getValue())?.toFixed(2) ?? '0.00' },
  { accessorKey: 'p99', header: columnHeader('P99'), cell: (info) => numberValue(info.getValue())?.toFixed(2) ?? '0.00' },
]

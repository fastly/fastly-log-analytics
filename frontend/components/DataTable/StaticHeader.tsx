'use client'

import * as React from 'react'
import { flexRender, type Header } from '@tanstack/react-table'
import { ArrowDown, ArrowUp, ArrowUpDown, GripVertical } from 'lucide-react'
import { useSortable } from '@dnd-kit/sortable'
import { CSS } from '@dnd-kit/utilities'

import { TableHead } from '@/components/ui/table'
import { cn } from '@/lib/utils'

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

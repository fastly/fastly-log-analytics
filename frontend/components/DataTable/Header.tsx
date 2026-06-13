'use client'

import * as React from 'react'
import { flexRender } from '@tanstack/react-table'
import { GripHorizontal, ArrowDown, ArrowUp, ArrowUpDown } from 'lucide-react'
import { cn } from '@/lib/utils'

import { TableHead } from '@/components/ui/table'

import { useSortable } from '@dnd-kit/sortable'
import { CSS } from '@dnd-kit/utilities'

// Draggable Header Component
export const DraggableTableHeader = ({ header }: { header: any }) => {
  const {
    attributes,
    isDragging,
    listeners,
    setNodeRef,
    transform,
    transition,
  } = useSortable({
    id: header.column.id,
  })

  const style: React.CSSProperties = {
    opacity: isDragging ? 1 : 1,
    transform: CSS.Translate.toString(transform),
    transition,
    whiteSpace: 'nowrap',
    width: header.column.getSize(),
    zIndex: isDragging ? 10 : 0,
    position: 'relative',
  }

  return (
    <TableHead
      ref={setNodeRef}
      style={style}
      className={`relative z-0 group select-none border-r last:border-r-0 px-0 ${isDragging ? 'bg-accent shadow-md rounded-md ring-1 ring-border' : 'bg-transparent'}`}
    >
      <div className="flex items-center justify-between gap-1 w-full h-full pl-3 pr-2 overflow-hidden">
        <div
          className={cn("flex-1 flex items-center hover:text-foreground transition-colors overflow-hidden", header.column.getCanSort() ? "cursor-pointer" : "")}
          onClick={header.column.getToggleSortingHandler()}
        >
          <span className="truncate">
            {header.isPlaceholder
              ? null
              : flexRender(
                  header.column.columnDef.header,
                  header.getContext()
                )}
          </span>
          {header.column.getCanSort() && !header.isPlaceholder && (
            <span className="ml-2 flex items-center shrink-0">
              {{
                asc: <ArrowUp className="w-3.5 h-3.5" />,
                desc: <ArrowDown className="w-3.5 h-3.5" />,
              }[header.column.getIsSorted() as string] ?? (
                <ArrowUpDown className="w-3.5 h-3.5 opacity-0 group-hover:opacity-50 transition-opacity" />
              )}
            </span>
          )}
        </div>
        <div
          {...attributes}
          {...listeners}
          className="cursor-grab text-muted-foreground/30 hover:text-foreground active:cursor-grabbing p-1 rounded hover:bg-muted opacity-40 group-hover:opacity-100 transition-opacity shrink-0"
          title="Drag to reorder"
        >
          <GripHorizontal className="w-3.5 h-3.5" />
        </div>
        <div
          // Guard `getResizeHandler` against the column being removed
          // mid-render: when a column toggles off, the DOM header lingers
          // for one frame and tanstack-table's resize handler throws
          // "Column with id '<id>' does not exist" if the user happens to
          // touch the resize handle in that window. Lazy-call the handler
          // and swallow the lookup error.
          onMouseDown={(e) => {
            try { header.getResizeHandler()(e) } catch { /* stale header */ }
          }}
          onTouchStart={(e) => {
            try { header.getResizeHandler()(e) } catch { /* stale header */ }
          }}
          className={cn(
            "absolute right-0 top-0 h-full w-2 cursor-col-resize hover:bg-primary/30 transition-colors z-10 touch-none",
            header.column.getIsResizing() ? "bg-primary opacity-100" : "opacity-0 group-hover:opacity-100"
          )}
        />
      </div>
    </TableHead>  )
}

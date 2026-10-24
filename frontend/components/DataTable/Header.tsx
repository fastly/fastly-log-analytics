'use client'

import * as React from 'react'
import type { Header } from '@tanstack/react-table'
import { GripHorizontal } from 'lucide-react'
import { TableHead } from '@/components/ui/table'
import { useSortable } from '@dnd-kit/sortable'
import { CSS } from '@dnd-kit/utilities'
import { getHeaderAriaSort, HeaderCellContent } from './StaticHeader'

export const DraggableTableHeader = <TData,>({ header }: { header: Header<TData, unknown> }) => {
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
    opacity: 1,
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
      aria-sort={getHeaderAriaSort(header)}
      className={`relative z-0 group select-none border-r last:border-r-0 px-0 ${isDragging ? 'bg-accent shadow-md rounded-md ring-1 ring-border' : 'bg-transparent'}`}
    >
      <HeaderCellContent
        header={header}
        dragHandle={
          <button
            type="button"
            {...attributes}
            {...listeners}
            aria-label={`Reorder column ${header.column.id}`}
            className="cursor-grab text-muted-foreground/30 hover:text-foreground active:cursor-grabbing p-1 rounded hover:bg-muted opacity-40 group-hover:opacity-100 focus-visible:opacity-100 focus-visible:ring-2 focus-visible:ring-ring transition-opacity shrink-0 bg-transparent border-0"
            title="Drag to reorder"
          >
            <GripHorizontal className="w-3.5 h-3.5" />
          </button>
        }
      />
    </TableHead>
  )
}

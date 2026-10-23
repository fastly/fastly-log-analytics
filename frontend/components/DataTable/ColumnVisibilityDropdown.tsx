'use client'

import React from 'react'
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { Button } from '@/components/ui/button'
import { ChevronDown, Settings2 } from 'lucide-react'
import type { Table, VisibilityState } from '@tanstack/react-table'
import { cn } from '@/lib/utils'

interface BaseColumnVisibilityDropdownProps {
  className?: string
  align?: 'start' | 'end'
  size?: 'sm' | 'default' | 'icon'
  compact?: boolean
}

export type ColumnVisibilityDropdownProps<TData = unknown> = BaseColumnVisibilityDropdownProps &
  (
    | {
        columns: { id: string; label: string }[]
        visibility: VisibilityState
        onChange: (id: string, visible: boolean) => void
        table?: never
      }
    | {
        table: Table<TData>
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

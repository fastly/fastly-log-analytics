'use client'

import * as React from 'react'
import type { Table } from '@tanstack/react-table'
import { cn } from '@/lib/utils'

import { Input } from '@/components/ui/input'
import { ColumnVisibilityDropdown } from './ColumnVisibilityDropdown'

interface DataTableToolbarProps<TData> {
  table: Table<TData>
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

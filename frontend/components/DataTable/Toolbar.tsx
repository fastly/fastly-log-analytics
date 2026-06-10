'use client'

import * as React from 'react'
import { cn } from '@/lib/utils'

import { Input } from '@/components/ui/input'
import { ColumnPicker } from './ColumnPicker'

interface DataTableToolbarProps {
  table: any
  title?: React.ReactNode
  searchKey?: string
  compactToolbar?: boolean
  extraToolbarContent?: React.ReactNode
}

export const DataTableToolbar = ({
  table,
  title,
  searchKey,
  compactToolbar = false,
  extraToolbarContent,
}: DataTableToolbarProps) => {
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
        <ColumnPicker table={table} compact={compactToolbar} />
      </div>
    </div>
  )
}

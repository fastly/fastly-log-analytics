'use client'

import * as React from 'react'
import { ChevronDown } from 'lucide-react'
import { cn } from '@/lib/utils'

import { buttonVariants } from '@/components/ui/button'
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'

interface ColumnPickerProps {
  table: any
  compact?: boolean
}

export const ColumnPicker = ({ table, compact = false }: ColumnPickerProps) => {
  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        className={buttonVariants({ variant: "outline", size: compact ? "sm" : "default", className: "h-8" })}
      >
        <span className={cn("flex items-center", compact && "text-xs")}>
          Columns <ChevronDown className="ml-2 h-4 w-4" />
        </span>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-auto min-w-[200px]">
        {table
          .getAllColumns()
          .filter((column: any) => column.getCanHide())
          .map((column: any) => {
            return (
              <DropdownMenuCheckboxItem
                key={column.id}
                className="whitespace-nowrap"
                checked={column.getIsVisible()}
                onCheckedChange={(value) =>
                  column.toggleVisibility(!!value)
                }
              >
                {(column.columnDef.meta as any)?.label ?? (typeof column.columnDef.header === 'string' ? column.columnDef.header : column.id)}
              </DropdownMenuCheckboxItem>
            )
          })}
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

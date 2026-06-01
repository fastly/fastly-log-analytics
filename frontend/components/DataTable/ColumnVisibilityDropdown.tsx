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
import { VisibilityState } from '@tanstack/react-table'
import { cn } from '@/lib/utils'

interface ColumnVisibilityDropdownProps {
  columns: { id: string; label: string }[]
  visibility: VisibilityState
  onChange: (id: string, visible: boolean) => void
  className?: string
  align?: 'start' | 'end'
  size?: 'sm' | 'default' | 'icon'
}

export function ColumnVisibilityDropdown({
  columns,
  visibility,
  onChange,
  className,
  align = 'end',
  size = 'sm'
}: ColumnVisibilityDropdownProps) {
  return (
    <DropdownMenu>
      <DropdownMenuTrigger render={
        <Button 
          variant="outline" 
          size={size} 
          className={cn("h-8 gap-2 px-2 text-xs font-normal", className)}
        />
      }>
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
        {columns.map((column) => (
          <DropdownMenuCheckboxItem
            key={column.id}
            className="capitalize"
            checked={visibility[column.id] !== false}
            onCheckedChange={(value) => onChange(column.id, !!value)}
          >
            {column.label}
          </DropdownMenuCheckboxItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

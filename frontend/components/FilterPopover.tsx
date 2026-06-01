'use client'

import React from 'react'
import { PlusCircle, FilterX } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'

interface FilterPopoverProps {
  col: string
  value: string
  onInclude: () => void
  onExclude: () => void
  triggerClassName?: string
  triggerLabel: React.ReactNode
  header?: React.ReactNode
  contentClassName?: string
}

export function FilterPopover({
  col,
  value,
  onInclude,
  onExclude,
  triggerClassName,
  triggerLabel,
  header,
  contentClassName = 'w-52 p-2',
}: FilterPopoverProps) {
  const [isOpen, setIsOpen] = React.useState(false)

  if (!isOpen) {
    return (
      <span 
        className={triggerClassName} 
        onClick={(e) => { 
          e.stopPropagation()
          e.preventDefault()
          setIsOpen(true) 
        }}
      >
        {triggerLabel}
      </span>
    )
  }

  return (
    <Popover open={isOpen} onOpenChange={setIsOpen}>
      <PopoverTrigger className={triggerClassName}>
        {triggerLabel}
      </PopoverTrigger>
      <PopoverContent className={contentClassName}>
        {header ?? (
          <>
            <p className="text-xs text-muted-foreground mb-1 font-mono truncate">{col}</p>
            <p className="text-xs font-mono font-medium break-all mb-2">{value}</p>
          </>
        )}
        <div className="flex gap-1">
          <Button size="sm" className="flex-1 h-7 text-xs gap-1" onClick={(e) => { e.stopPropagation(); onInclude(); setIsOpen(false); }}>
            <PlusCircle className="h-3 w-3" /> Include
          </Button>
          <Button size="sm" variant="outline" className="flex-1 h-7 text-xs gap-1" onClick={(e) => { e.stopPropagation(); onExclude(); setIsOpen(false); }}>
            <FilterX className="h-3 w-3" /> Exclude
          </Button>
        </div>
      </PopoverContent>
    </Popover>
  )
}

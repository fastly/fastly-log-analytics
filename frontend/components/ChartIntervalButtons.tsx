'use client'

import React from 'react'
import { Button } from '@/components/ui/button'
import { ButtonGroup } from '@/components/ui/button-group'
import { cn } from '@/lib/utils'
import { INTERVALS, type ChartInterval } from '@/lib/constants'

interface ChartIntervalButtonsProps {
  effectiveInterval: string
  validIntervals: Set<string>
  onIntervalChange: (val: ChartInterval) => void
}

export function ChartIntervalButtons({
  effectiveInterval,
  validIntervals,
  onIntervalChange,
}: ChartIntervalButtonsProps) {
  return (
    <ButtonGroup>
      {INTERVALS.map(i => (
        <Button
          key={i.value}
          variant={effectiveInterval === i.value ? 'default' : 'ghost'}
          size="sm"
          onClick={() => React.startTransition(() => onIntervalChange(i.value))}
          disabled={!validIntervals.has(i.value)}
          aria-pressed={effectiveInterval === i.value}
          // Distinct accessible name: the visible label ("1h", "1d", …) is
          // identical to the main time-range buttons, so screen readers and
          // automation see duplicate "1h"/"1d" controls. Scope the name to
          // the chart-bucket role.
          aria-label={`Chart bucket size: ${i.label}`}
          className={cn(
            'h-9 text-xs px-2 shadow-none transition-colors disabled:opacity-30 sm:h-7 sm:text-[11px]',
            effectiveInterval === i.value
              ? 'bg-primary text-primary-foreground hover:bg-primary/90'
              : 'hover:text-primary hover:bg-muted'
          )}
        >
          {i.label}
        </Button>
      ))}
    </ButtonGroup>
  )
}

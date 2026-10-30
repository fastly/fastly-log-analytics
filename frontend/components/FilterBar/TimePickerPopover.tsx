'use client'

import * as React from 'react'
import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'

// First-element of every React Query key whose results depend on the
// global start/end time range. Used by the active-pill loading dot's
// predicate so background polls (admin metrics, log-extents, etc.)
// don't blink the pill while their refetches are in flight.
export const PRESET_WINDOWS: ReadonlyArray<readonly [string, number]> = [
  ['1h', 1],
  ['3h', 3],
  ['6h', 6],
  ['12h', 12],
  ['24h', 24],
  ['3d', 72],
  ['7d', 168],
  ['30d', 720],
]

export const TIME_BOUND_QUERY_KEYS = new Set([
  'dashboard',
  'sessions',
  'usage',
  'insights',
  'usage-log',
])

export interface TimePickerPopoverProps {
  mounted: boolean
  pathname: string | null
  activePreset: string | null
  inFlightCount: number
  quickPresets: { label: string; value: () => void }[]
  localStart: string
  setLocalStart: (v: string) => void
  localEnd: string
  setLocalEnd: (v: string) => void
  localCompareStart: string
  setLocalCompareStart: (v: string) => void
  localCompareEnd: string
  setLocalCompareEnd: (v: string) => void
  edgeOnly: boolean
  compareMode: boolean
  onToggleEdgeOnly: (checked: boolean) => void
  onToggleCompareMode: (checked: boolean) => void
  onApplyComparePreset: (days: number) => void
  onApplyCustomDate: () => void
  onReset: () => void
}

export function TimePickerPopover({
  mounted,
  pathname,
  activePreset,
  inFlightCount,
  quickPresets,
  localStart,
  setLocalStart,
  localEnd,
  setLocalEnd,
  localCompareStart,
  setLocalCompareStart,
  localCompareEnd,
  setLocalCompareEnd,
  edgeOnly,
  compareMode,
  onToggleEdgeOnly,
  onToggleCompareMode,
  onApplyComparePreset,
  onApplyCustomDate,
  onReset,
}: TimePickerPopoverProps) {
  return (
    <>
      <div className="flex items-center gap-0.5 border rounded-md p-0.5 bg-muted/20 h-10 sm:h-8">
        {quickPresets.map((preset) => {
          // `mounted` gate: activePreset derives from `new Date()` (non-
          // deterministic server vs client), so highlight only post-hydration.
          // Active state is className-only — no layout impact either way.
          const isActive = mounted && activePreset === preset.label
          const isActiveLoading = isActive && inFlightCount > 0
          return (
            <Button
              key={preset.label}
              variant={isActive ? 'default' : 'ghost'}
              size="sm"
              onClick={preset.value}
              aria-pressed={isActive}
              aria-busy={isActiveLoading || undefined}
              className={cn(
                'relative h-9 sm:h-6.5 px-2.5 sm:px-2 text-xs sm:text-[11px]',
                isActive ? 'shadow-sm' : ''
              )}
            >
              {preset.label}
              {isActiveLoading && (
                <span
                  aria-hidden="true"
                  className="pointer-events-none absolute top-1 right-1 h-1 w-1 rounded-full bg-primary-foreground animate-pulse"
                />
              )}
            </Button>
          )
        })}
      </div>

      <div className="flex flex-col gap-0.5 w-full sm:w-auto">
        <div className="flex items-center gap-1.5 border rounded-md p-0.5 bg-muted/10 h-10 sm:h-8 w-full sm:w-auto">
          <Input
            type="datetime-local"
            aria-label="Range start"
            value={localStart}
            onChange={(e) => setLocalStart(e.target.value)}
            className="h-9 sm:h-7 text-xs sm:text-[11px] border-0 bg-transparent shadow-none focus-visible:ring-0 w-full sm:w-[160px] flex-1 sm:flex-none px-1"
          />
          <span className="text-muted-foreground text-[10px]">-</span>
          <Input
            type="datetime-local"
            aria-label="Range end"
            value={localEnd}
            onChange={(e) => setLocalEnd(e.target.value)}
            className="h-9 sm:h-7 text-xs sm:text-[11px] border-0 bg-transparent shadow-none focus-visible:ring-0 w-full sm:w-[160px] flex-1 sm:flex-none px-1"
          />
        </div>
        {pathname?.startsWith('/dashboard') && mounted && compareMode && (
          <div className="flex items-center gap-1.5 border rounded-md p-0.5 bg-orange-500/10 border-orange-500/20 h-10 sm:h-8 w-full sm:w-auto">
            <Input
              type="datetime-local"
              aria-label="Compare range start"
              value={localCompareStart}
              onChange={(e) => setLocalCompareStart(e.target.value)}
              className="h-9 sm:h-7 text-xs sm:text-[11px] border-0 bg-transparent shadow-none focus-visible:ring-0 w-full sm:w-[160px] flex-1 sm:flex-none px-1 text-orange-600 dark:text-orange-400"
            />
            <span className="text-muted-foreground text-[10px]">-</span>
            <Input
              type="datetime-local"
              aria-label="Compare range end"
              value={localCompareEnd}
              onChange={(e) => setLocalCompareEnd(e.target.value)}
              className="h-9 sm:h-7 text-xs sm:text-[11px] border-0 bg-transparent shadow-none focus-visible:ring-0 w-full sm:w-[160px] flex-1 sm:flex-none px-1 text-orange-600 dark:text-orange-400"
            />
            <div
              className="hidden sm:flex items-center gap-0.5 ml-1 pl-1 border-l border-orange-500/20"
              role="group"
              aria-label="Compare range presets"
            >
              {(
                [
                  ['1w', 7, 'Same window, 7 days earlier'],
                  ['4w', 28, 'Same window, 28 days earlier'],
                  ['1y', 365, 'Same window, 1 year earlier'],
                ] as const
              ).map(([label, days, title]) => (
                <Button
                  key={label}
                  variant="ghost"
                  size="sm"
                  onClick={() => onApplyComparePreset(days)}
                  title={title}
                  className="h-7 sm:h-6 px-1.5 text-[10px] font-medium text-orange-700 dark:text-orange-300 hover:bg-orange-500/20"
                >
                  {label}
                </Button>
              ))}
            </div>
          </div>
        )}
      </div>

      <div className="flex flex-col border-l pl-3 ml-1 gap-1 sm:gap-0 sm:-space-y-0.5">
        <Tooltip>
          <TooltipTrigger
            render={
              <div className="flex items-center space-x-2 h-7 sm:h-5">
                <Switch
                  id="edge-only"
                  size="sm"
                  checked={mounted && edgeOnly}
                  onCheckedChange={onToggleEdgeOnly}
                />
                <Label
                  htmlFor="edge-only"
                  className="text-xs sm:text-[11px] font-medium leading-none cursor-pointer"
                >
                  Edge only
                </Label>
              </div>
            }
          />
          <TooltipContent side="bottom" className="max-w-[260px] text-xs">
            Restrict results to edge (CDN) requests only — excludes origin and shield hops so each
            request counts once.
          </TooltipContent>
        </Tooltip>
        {pathname?.startsWith('/dashboard') && (
          <Tooltip>
            <TooltipTrigger
              render={
                <div className="flex items-center space-x-2 h-7 sm:h-5">
                  <Switch
                    id="compare-mode"
                    size="sm"
                    checked={mounted && compareMode}
                    onCheckedChange={onToggleCompareMode}
                  />
                  <Label
                    htmlFor="compare-mode"
                    className="text-xs sm:text-[11px] font-medium leading-none cursor-pointer"
                  >
                    Compare
                  </Label>
                </div>
              }
            />
            <TooltipContent side="bottom" className="max-w-[260px] text-xs">
              Overlay a second time range to compare deltas. The compare range appears below the
              primary range.
            </TooltipContent>
          </Tooltip>
        )}
      </div>

      <div className="flex items-center gap-1.5">
        <Button
          size="sm"
          onClick={onApplyCustomDate}
          className="h-9 sm:h-7 px-4 sm:px-3 text-xs sm:text-[11px]"
        >
          Apply
        </Button>
        <Button
          variant="secondary"
          size="sm"
          onClick={onReset}
          className="h-9 sm:h-7 px-4 sm:px-3 text-xs sm:text-[11px]"
        >
          Reset
        </Button>
      </div>
    </>
  )
}

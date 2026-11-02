'use client'

import * as React from 'react'
import { Bot, Check, Info, X } from 'lucide-react'
import { useShallow } from 'zustand/react/shallow'
import { useFilterStore } from '@/stores/filterStore'
import type { FilterPill } from '@/types/api'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'

export type FilterOperatorMode = 'include' | 'exclude'

export interface OperatorSelectorProps {
  mode: FilterOperatorMode
  onModeChange: (nextMode: FilterOperatorMode) => void
}

export function OperatorSelector({ mode, onModeChange }: OperatorSelectorProps) {
  return (
    <div className="grid gap-2">
      <Label className="text-[10px] font-bold text-muted-foreground uppercase tracking-widest">
        Filter Type
      </Label>
      <Tabs
        value={mode}
        onValueChange={(v) => {
          if (v === 'include' || v === 'exclude') onModeChange(v)
        }}
        className="w-full"
      >
        <TabsList className="grid w-full grid-cols-2 h-9">
          <TabsTrigger value="include" className="text-xs gap-1.5">
            <Check className="h-3 w-3 text-green-500" /> Include
          </TabsTrigger>
          <TabsTrigger value="exclude" className="text-xs gap-1.5">
            <X className="h-3 w-3 text-red-500" /> Exclude
          </TabsTrigger>
        </TabsList>
      </Tabs>
    </div>
  )
}

export function fmtBotId(id: string): string {
  return id.replace(/-/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())
}

export interface FilterChipProps {
  filter: FilterPill
  onToggleMode: (id: string) => void
  onRemove: (id: string) => void
}

export function FilterChip({ filter, onToggleMode, onRemove }: FilterChipProps) {
  return (
    <Badge
      variant={filter.mode === 'include' ? 'default' : 'destructive'}
      className="gap-1 pr-1 pl-1 h-8 sm:h-7 text-xs sm:text-[11px]"
    >
      <Button
        variant="ghost"
        size="icon"
        aria-label={`${filter.mode === 'include' ? 'Including' : 'Excluding'} ${filter.column}=${filter.value}. Activate to toggle.`}
        aria-pressed={filter.mode === 'include'}
        className="h-6 w-6 sm:h-5 sm:w-5 p-0 hover:bg-transparent mr-1"
        onClick={() => React.startTransition(() => onToggleMode(filter.id))}
        title="Toggle Include/Exclude"
      >
        <span className="font-bold text-[10px]" aria-hidden="true">
          {filter.mode === 'include' ? '+' : '-'}
        </span>
      </Button>
      {filter.column === '_bot_name' ? (
        <>
          <Bot className="h-3 w-3 opacity-70 mr-0.5" />
          <span className="opacity-70">Bot:</span>
          <span className="max-w-[180px] truncate">{fmtBotId(filter.value)}</span>
        </>
      ) : filter.column === '_ngwaf_bot_name' ? (
        <>
          <Bot className="h-3 w-3 opacity-70 mr-0.5" />
          <span className="opacity-70">Fastly Verified Bot:</span>
          <span className="max-w-[180px] truncate">{filter.value}</span>
        </>
      ) : (
        <>
          <span className="opacity-70">{filter.column}:</span>
          <span className="max-w-[180px] truncate">{filter.value}</span>
        </>
      )}
      <Button
        variant="ghost"
        size="icon"
        aria-label={`Remove filter ${filter.column}: ${filter.value}`}
        title="Remove filter"
        className="h-6 w-6 sm:h-5 sm:w-5 p-0 hover:bg-transparent ml-1"
        onClick={() => React.startTransition(() => onRemove(filter.id))}
      >
        <X className="h-3 w-3" aria-hidden="true" />
      </Button>
    </Badge>
  )
}

/**
 * Slim read-only chip strip for pages where the global FilterBar is hidden
 * (/insights, /alerts, /admin, /share-login). Surfaces any filters or the
 * "Edge only" toggle that the user previously set on /dashboard etc. so
 * they aren't invisibly carried forward to a page that doesn't apply them.
 *
 * Renders nothing when no filters are set AND edgeOnly is off.
 *
 * The pills are READ-ONLY here — no toggle, no individual remove. The
 * single "Clear all" affordance clears the entire filter state at once.
 * Per-pill editing belongs on the FilterBar; surfacing it here would
 * suggest those filters apply to the current page, which they don't.
 */
export function ActiveFiltersBanner() {
  const { filters, edgeOnly, clearFilters, toggleEdgeOnly } = useFilterStore(
    useShallow((s) => ({
      filters: s.filters,
      edgeOnly: s.edgeOnly,
      clearFilters: s.clearFilters,
      toggleEdgeOnly: s.toggleEdgeOnly,
    }))
  )
  const hasFilters = filters.length > 0
  if (!hasFilters && !edgeOnly) return null

  return (
    <div
      role="region"
      aria-label="Active filters from other pages"
      className="border-b bg-muted/40 px-3 py-1.5 sm:px-4"
    >
      <div className="flex items-center gap-2 flex-wrap text-xs">
        <span className="flex items-center gap-1 text-muted-foreground shrink-0">
          <Info className="h-3 w-3" aria-hidden="true" />
          Filters set elsewhere
          <span className="hidden sm:inline opacity-70">— don&apos;t affect this page</span>
        </span>
        {filters.map((filter) => (
          <Badge
            key={filter.id}
            variant={filter.mode === 'include' ? 'secondary' : 'outline'}
            className="gap-1 h-6 text-[11px] font-normal opacity-80"
            title={
              filter.mode === 'include'
                ? `Including ${filter.column} = ${filter.value}`
                : `Excluding ${filter.column} = ${filter.value}`
            }
          >
            <span className="font-bold" aria-hidden="true">
              {filter.mode === 'include' ? '+' : '−'}
            </span>
            <span className="opacity-70">{filter.column}:</span>
            <span className="max-w-[160px] truncate">{filter.value}</span>
          </Badge>
        ))}
        {edgeOnly && (
          <Badge
            variant="secondary"
            className="h-6 text-[11px] font-normal opacity-80"
            title="Edge-only toggle is on for dashboard / query pages"
          >
            Edge only
          </Badge>
        )}
        <Button
          variant="ghost"
          size="sm"
          className="h-6 px-2 text-[11px] text-muted-foreground hover:text-foreground ml-auto"
          onClick={() => {
            if (hasFilters) clearFilters()
            if (edgeOnly) toggleEdgeOnly()
          }}
        >
          Clear all
        </Button>
      </div>
    </div>
  )
}

'use client'

import * as React from 'react'
import { useShallow } from 'zustand/react/shallow'
import { useFilterStore } from '@/stores/filterStore'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Info } from 'lucide-react'

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
    })),
  )

  const hasFilters = filters.length > 0
  const hasAny = hasFilters || edgeOnly
  if (!hasAny) return null

  const onClearAll = () => {
    if (hasFilters) clearFilters()
    if (edgeOnly) toggleEdgeOnly()
  }

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
          onClick={onClearAll}
        >
          Clear all
        </Button>
      </div>
    </div>
  )
}

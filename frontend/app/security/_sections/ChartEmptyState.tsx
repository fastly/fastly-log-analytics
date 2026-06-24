import React from 'react'

/** Shared "No data available — Requires {X}" empty state for the security
 *  chart cards. The wrapping `data.length === 0 && !isLoading` guard stays
 *  at each call site; this only owns the inner markup. */
export function ChartEmptyState({ requires }: { requires: string }) {
  return (
    <div className="flex flex-col items-center justify-center h-full text-muted-foreground text-center px-4">
      <span className="text-sm font-medium mb-1">No data available</span>
      <span className="text-[10px] opacity-70">Requires {requires}</span>
    </div>
  )
}

import React from 'react'

/** Shared empty state for the security chart cards. The wrapping
 *  `data.length === 0 && !isLoading` guard stays at each call site; this only
 *  owns the inner markup.
 *
 *  `requires` is OPTIONAL: pass it only when the underlying field group is
 *  genuinely NOT enabled (callers decide via `useActiveLogFields`). When the
 *  group IS enabled but there's simply no data in the window yet, omit it and
 *  this renders a neutral "no data" state instead of a misleading
 *  "Requires Group X to be enabled" message. */
export function ChartEmptyState({ requires }: { requires?: string }) {
  return (
    <div className="flex flex-col items-center justify-center h-full text-muted-foreground text-center px-4">
      <span className="text-sm font-medium mb-1">
        {requires ? 'No data available' : 'No data in this time range yet'}
      </span>
      {requires && <span className="text-[10px] opacity-70">Requires {requires}</span>}
    </div>
  )
}

'use client'

import React from 'react'

interface StructuredModeProps {
  structuredSql: string
}

/**
 * Structured mode: show the generated SQL read-only so users can
 * see exactly what they're about to run. CodeEditor isn't wired
 * for read-only display, so we render a styled <pre> instead.
 */
export function StructuredMode({ structuredSql }: StructuredModeProps) {
  return (
    <div className="p-4 bg-muted/10">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground mb-2 font-semibold">
        Generated SQL (sync'd with filter bar)
      </div>
      <pre className="text-xs font-mono whitespace-pre-wrap break-words text-foreground/90 bg-background border rounded p-3 overflow-x-auto">
        {structuredSql}
      </pre>
      <div className="text-[10px] text-muted-foreground mt-2">
        Edit the date range or filters in the header bar above to refine.
        Click column headers below to change sort order — the query re-runs server-side.
      </div>
    </div>
  )
}

'use client'

import React from 'react'

import { useMounted } from '@/hooks/useMounted'

interface StructuredModeProps {
  structuredSql: string
}

/**
 * Structured mode: show the generated SQL read-only so users can
 * see exactly what they're about to run. CodeEditor isn't wired
 * for read-only display, so we render a styled <pre> instead.
 */
export function StructuredMode({ structuredSql }: StructuredModeProps) {
  // The generated SQL embeds the filter-bar's default time range, which the
  // filterStore computes from `new Date()` at module load — a different
  // instant on the server vs the client, so the SQL text diverges between the
  // SSR HTML and the first client render → React #418. Defer the SQL until
  // mounted so both render the same placeholder first.
  const mounted = useMounted()
  return (
    <div className="p-4 bg-muted/10">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground mb-2 font-semibold">
        Generated SQL (sync'd with filter bar)
      </div>
      <pre className="text-xs font-mono whitespace-pre-wrap break-words text-foreground/90 bg-background border rounded p-3 overflow-x-auto">
        {mounted ? structuredSql : '-- building query preview…'}
      </pre>
      <div className="text-[10px] text-muted-foreground mt-2">
        Edit the date range or filters in the header bar above to refine.
        Click column headers below to change sort order — the query re-runs server-side.
      </div>
    </div>
  )
}

'use client'

import React from 'react'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'

/**
 * Small "Approximate" pill shown on origin panels whose wide-window
 * (>= 48h, unfiltered) values are served from per-hour rollups. Across
 * hours the percentiles are request-weighted averages of per-hour
 * percentiles rather than an exact cross-window MEDIAN/APPROX_QUANTILE,
 * so the backend sets ``_approx: true`` on those section payloads. Counts
 * (request volume, error rate) stay exact. Markup mirrors the original
 * inline badge on the summary card so every origin panel reads the same.
 */
export function ApproxBadge({ message }: { message: string }) {
  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger render={<span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full bg-muted text-muted-foreground text-[10px] font-bold uppercase tracking-wider cursor-help" />}>
          <span className="w-1.5 h-1.5 rounded-full bg-muted-foreground/60" />
          Approximate
        </TooltipTrigger>
        <TooltipContent side="left" className="max-w-[260px] text-xs">
          {message}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}

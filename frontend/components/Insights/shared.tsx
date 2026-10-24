'use client'

import React from 'react'
import { cn } from '@/lib/utils'

interface InsightStatCardProps {
  icon: React.ReactNode
  label: string
  value: string
  valueClass?: string
  hint?: string
}

export function InsightStatCard({
  icon,
  label,
  value,
  valueClass,
  hint,
}: InsightStatCardProps) {
  return (
    <div className="p-3 rounded-lg bg-muted/40 border border-border flex flex-col justify-between">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold flex items-center gap-1">
        {icon} {label}
      </div>
      <div className={cn('mt-1.5 font-mono text-xl font-bold', valueClass ?? 'text-foreground')}>
        {value}
      </div>
      {hint && <div className="text-[10px] text-muted-foreground mt-0.5">{hint}</div>}
    </div>
  )
}

export function buildInvestigateHref(
  investigateUrl: string | null | undefined,
  activeServiceId: string | null | undefined,
): string | null {
  if (!investigateUrl) return null
  const url = new URL(investigateUrl, 'http://localhost')
  if (!url.searchParams.has('service') && activeServiceId) {
    url.searchParams.set('service', activeServiceId)
  }
  return url.pathname + url.search
}

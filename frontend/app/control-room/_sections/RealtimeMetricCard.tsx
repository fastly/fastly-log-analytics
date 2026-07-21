'use client'

import { useState } from 'react'
import { HelpCircle } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { HelpDialog } from '@/components/ui/help-dialog'
import { cn } from '@/lib/utils'

interface RateEntry {
  label: string
  value: number
}

interface RealtimeMetricCardProps {
  title: string
  value: number
  suffix?: string
  rates?: RateEntry[]
  thresholds?: { warn: number; critical: number; direction: 'above' | 'below' }
  dimmed?: boolean
  helpText?: string
}

function getThresholdLevel(
  value: number,
  thresholds: RealtimeMetricCardProps['thresholds']
): 'ok' | 'warn' | 'critical' {
  if (!thresholds) return 'ok'
  const { warn, critical, direction } = thresholds
  if (direction === 'above') {
    if (value > critical) return 'critical'
    if (value > warn) return 'warn'
  } else {
    if (value < critical) return 'critical'
    if (value < warn) return 'warn'
  }
  return 'ok'
}

function formatRate(value: number, suffix?: string): string {
  if (suffix?.includes('%')) return `${value.toFixed(1)}%`
  if (suffix?.includes('Mbps')) return `${value.toFixed(1)}`
  if (value >= 1000) return `${(value / 1000).toFixed(1)}k`
  return value < 10 ? value.toFixed(1) : Math.round(value).toString()
}

const levelClasses = {
  ok: '',
  warn: 'bg-amber-50 dark:bg-amber-950/20',
  critical: 'bg-red-50 dark:bg-red-950/20',
} as const

export function RealtimeMetricCard({
  title,
  value,
  suffix,
  rates,
  thresholds,
  dimmed,
  helpText,
}: RealtimeMetricCardProps) {
  const [helpOpen, setHelpOpen] = useState(false)
  const level = getThresholdLevel(value, thresholds)

  return (
    <Card
      className={cn(
        'relative overflow-hidden transition-colors',
        levelClasses[level],
        dimmed && 'opacity-50'
      )}
    >
      <CardHeader className="relative px-4 pt-4 pb-0">
        <div className="flex items-center justify-between">
          <CardTitle className="text-xs font-medium tracking-wide text-muted-foreground uppercase">
            {title}
          </CardTitle>
          {helpText && (
            <Button
              variant="ghost"
              size="icon"
              aria-label={`About ${title}`}
              className="h-6 w-6 text-muted-foreground hover:text-foreground"
              onClick={() => setHelpOpen(true)}
            >
              <HelpCircle className="h-3.5 w-3.5" />
            </Button>
          )}
        </div>
      </CardHeader>
      <CardContent className="relative px-4 pb-4 pt-0">
        <div className="flex items-baseline">
          <span className="text-3xl font-bold tabular-nums">
            {value}
          </span>
          {suffix && <span className="text-lg font-normal text-muted-foreground ml-1">{suffix.trim()}</span>}
        </div>
        {rates && rates.length > 0 && (
          <div className="mt-1 text-xs text-muted-foreground tabular-nums">
            {rates.map((r, i) => (
              <span key={r.label}>
                {i > 0 && <span className="mx-1">&middot;</span>}
                {r.label}: {formatRate(r.value, suffix)}
              </span>
            ))}
          </div>
        )}
      </CardContent>
      {helpText && (
        <HelpDialog open={helpOpen} onOpenChange={setHelpOpen} title={title}>
          <p>{helpText}</p>
        </HelpDialog>
      )}
    </Card>
  )
}

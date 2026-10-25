'use client'

import { useMemo, useState } from 'react'
import { HelpCircle } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { HelpDialog } from '@/components/ui/help-dialog'
import { PlotlyChart } from '@/components/PlotlyChart'
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
        dimmed && 'border-dashed border-muted-foreground/40 bg-muted/5'
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

interface RealtimeChartProps {
  title: string
  traces: Array<{
    y: number[]
    name: string
    color?: string
    yaxis?: 'y' | 'y2'
  }>
  height?: number
  className?: string
  stacked?: boolean
  dualYAxis?: boolean
  helpText?: string
  yAxisSuffix?: string
  y2AxisSuffix?: string
}

const WINDOW = 60
const STATIC_CONFIG = { displayModeBar: false, responsive: true }

function makeTimeLabels(count: number): string[] {
  const now = new Date()
  return Array.from({ length: count }, (_, i) => {
    const d = new Date(now.getTime() - (count - 1 - i) * 1000)
    const h = String(d.getHours()).padStart(2, '0')
    const m = String(d.getMinutes()).padStart(2, '0')
    const s = String(d.getSeconds()).padStart(2, '0')
    return `${h}:${m}:${s}`
  })
}

export function RealtimeChart({
  title,
  traces,
  height = 180,
  className,
  stacked,
  dualYAxis,
  helpText,
  yAxisSuffix,
  y2AxisSuffix,
}: RealtimeChartProps) {
  const [helpOpen, setHelpOpen] = useState(false)
  const pointCount = traces[0]?.y.length ?? WINDOW
  const labels = useMemo(() => makeTimeLabels(pointCount), [traces, pointCount])

  const data = useMemo(() => traces.map((trace) => ({
    type: 'bar' as const,
    x: labels,
    y: trace.y,
    name: trace.name,
    marker: { color: trace.color },
    yaxis: trace.yaxis || 'y',
    hovertemplate: `%{x} · %{y:,.4~g}<extra>${trace.name}</extra>`,
  })), [traces, labels])

  const hasLegend = traces.length > 1
  const layout = useMemo(() => {
    const l: Record<string, unknown> = {
      xaxis: {
        type: 'category',
        tickangle: -45,
        nticks: 6,
        tickfont: { size: 9 },
        range: [-0.5, WINDOW - 0.5],
      },
      yaxis: {
        showgrid: true,
        gridcolor: 'rgba(128,128,128,0.15)',
        zeroline: false,
        autorange: true,
        rangemode: 'tozero',
        exponentformat: 'SI',
        ...(yAxisSuffix ? { ticksuffix: yAxisSuffix } : {}),
      },
      barmode: stacked ? 'stack' : 'group',
      bargap: 0.02,
      margin: { l: yAxisSuffix ? 55 : 42, r: dualYAxis ? (y2AxisSuffix ? 55 : 42) : 15, t: hasLegend ? 32 : 8, b: 32 },
      plot_bgcolor: 'transparent',
      paper_bgcolor: 'transparent',
      showlegend: hasLegend,
      legend: {
        orientation: 'h',
        y: 1.05,
        x: 0.5,
        xanchor: 'center',
        yanchor: 'bottom',
        font: { size: 9.5 },
        bgcolor: 'transparent',
      },
      uirevision: 'stable',
      font: { size: 11 },
      hovermode: 'x unified',
    }

    if (dualYAxis) {
      l.yaxis2 = {
        side: 'right',
        overlaying: 'y',
        showgrid: false,
        zeroline: false,
        autorange: true,
        rangemode: 'tozero',
        exponentformat: 'SI',
        ...(y2AxisSuffix ? { ticksuffix: y2AxisSuffix } : {}),
      }
    }
    return l
  }, [stacked, dualYAxis, hasLegend, yAxisSuffix, y2AxisSuffix])

  return (
    <Card className={className}>
      <CardHeader className="px-4 pt-4 pb-0">
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
      <CardContent className="px-4 pb-4 pt-0">
        <PlotlyChart data={data} layout={layout} config={STATIC_CONFIG} height={height} a11yTitle={title} />
      </CardContent>
      {helpText && (
        <HelpDialog open={helpOpen} onOpenChange={setHelpOpen} title={title}>
          <p>{helpText}</p>
        </HelpDialog>
      )}
    </Card>
  )
}

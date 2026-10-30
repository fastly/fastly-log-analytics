'use client'

import React from 'react'
import { Bell, Loader2 } from 'lucide-react'
import { useLogFieldsCatalog } from '@/hooks/useBootstrap'
import { useTimezoneStore } from '@/stores/filterStore'
import { Button } from '@/components/ui/button'
import { ButtonGroup } from '@/components/ui/button-group'
import { Label } from '@/components/ui/label'
import { PlotlyChart } from '@/components/PlotlyChart'
import { CHART_COLORS, useTimeLayout, formatDate } from '@/lib/utils'
import type { components } from '@/types/api.generated'

type AlertPreviewData = components['schemas']['AlertPreviewData']

export interface AlertPreviewProps {
  previewData: AlertPreviewData | null
  isPreviewLoading: boolean
  lookbackHours: number
  setLookbackHours: (h: number) => void
  metric: string
  evalType: string
  threshold: string
}

export function AlertPreview({
  previewData,
  isPreviewLoading,
  lookbackHours,
  setLookbackHours,
  metric,
  evalType,
  threshold,
}: AlertPreviewProps) {
  const { data: catalog } = useLogFieldsCatalog()
  const metricField = React.useMemo(
    () => catalog?.fields?.find((f) => f.id === metric),
    [catalog, metric],
  )
  const { timezone } = useTimezoneStore()
  const startTime = React.useMemo(() => previewData?.times?.[0], [previewData])
  const endTime = React.useMemo(
    () => previewData?.times?.[previewData?.times?.length - 1],
    [previewData],
  )
  const timeLayout = useTimeLayout(startTime, endTime, timezone)
  const x = React.useMemo(
    () =>
      (previewData?.times || []).map((t: string) =>
        formatDate(t, timezone, 'yyyy-MM-dd HH:mm:ss'),
      ),
    [previewData, timezone],
  )

  const getHoverTemplate = React.useCallback(
    (m: string, label?: string) => {
      const pre = label ? `${label}: ` : ''
      const field = m === metric ? metricField : catalog?.fields?.find((f) => f.id === m)
      const unit = field?.unit || ''
      const precision = field?.precision ?? (m === 'requests' ? 0 : 1)
      const format = precision > 0 ? `.${precision}f` : ','
      return `${pre}%{y:${format}}${unit}<extra></extra>`
    },
    [catalog, metric, metricField],
  )

  const isBarMetric =
    metric === 'requests' ||
    metric === '5xx' ||
    metric === '4xx' ||
    metric === 'specific_status'

  const lineTrace = (
    name: string,
    y: number[],
    color: string,
    dash: string,
    hover?: string,
    xs = x,
  ) => ({
    x: xs,
    y,
    type: 'scatter' as const,
    mode: 'lines' as const,
    name,
    line: { color, width: 2, dash },
    ...(hover ? { hovertemplate: hover } : { hoverinfo: 'none' as const }),
  })

  return (
    <div className="flex flex-col min-h-[300px]" data-testid="alert-preview">
      <div className="flex items-center justify-between mb-2">
        <Label>Live Preview</Label>
        <ButtonGroup>
          {[1, 3, 6, 12, 24].map((h) => (
            <Button
              key={h}
              type="button"
              variant={lookbackHours === h ? 'default' : 'ghost'}
              size="sm"
              onClick={() => setLookbackHours(h)}
              className={`h-6 text-[10px] px-2 shadow-none transition-colors ${
                lookbackHours === h
                  ? 'bg-primary text-primary-foreground hover:bg-primary/90'
                  : 'hover:text-primary hover:bg-muted'
              }`}
            >
              {h}h
            </Button>
          ))}
        </ButtonGroup>
      </div>
      <div className="flex-1 border border-border/50 rounded-md p-4 bg-muted/10 relative flex flex-col">
        {isPreviewLoading && (
          <div className="absolute inset-0 z-10 flex items-center justify-center bg-background/50 rounded-md">
            <Loader2 className="w-6 h-6 animate-spin text-muted-foreground" />
          </div>
        )}
        {previewData && previewData.times && previewData.times.length > 0 ? (
          <div className="flex-1 w-full relative">
            <PlotlyChart
              data={[
                {
                  x,
                  y: previewData.values,
                  type: isBarMetric ? 'bar' : 'scatter',
                  mode: isBarMetric ? undefined : 'lines+markers',
                  name: 'Current',
                  marker: { color: CHART_COLORS.info },
                  line: { color: CHART_COLORS.info, width: 2 },
                  hovertemplate: getHoverTemplate(metric, 'Current'),
                },
                ...(previewData.type === 'relative' && previewData.hist_values
                  ? [
                      lineTrace(
                        'Baseline',
                        previewData.hist_values,
                        '#a1a1aa',
                        'dot',
                        getHoverTemplate(metric, 'Baseline'),
                      ),
                    ]
                  : []),
                ...(previewData.type === 'anomaly_zscore' && previewData.hist_values
                  ? [
                      lineTrace(
                        'Baseline Mean',
                        previewData.hist_values,
                        '#a1a1aa',
                        'dot',
                        getHoverTemplate(metric, 'Baseline Mean'),
                      ),
                    ]
                  : []),
                ...(previewData.type === 'anomaly_zscore' && previewData.threshold_values
                  ? [
                      lineTrace(
                        `Threshold (${threshold}σ)`,
                        previewData.threshold_values,
                        'hsl(var(--destructive))',
                        'dash',
                        getHoverTemplate(metric, 'Threshold'),
                      ),
                    ]
                  : []),
                ...(previewData.type === 'absolute' && parseFloat(threshold)
                  ? [
                      lineTrace(
                        'Threshold',
                        [parseFloat(threshold), parseFloat(threshold)],
                        'hsl(var(--destructive))',
                        'dash',
                        undefined,
                        [x[0], x[x.length - 1]],
                      ),
                    ]
                  : []),
                ...(previewData.type === 'relative' &&
                previewData.hist_values &&
                parseFloat(threshold)
                  ? [
                      lineTrace(
                        'Threshold',
                        previewData.hist_values.map((v: number) => {
                          const t = parseFloat(threshold)
                          return evalType === 'relative_increase'
                            ? v * (1 + t / 100)
                            : v * (1 - t / 100)
                        }),
                        'hsl(var(--destructive))',
                        'dash',
                      ),
                    ]
                  : []),
              ]}
              layout={{
                ...timeLayout,
                margin: { t: 10, r: 10, l: 40, b: 30 },
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                xaxis: {
                  ...timeLayout.xaxis,
                  showgrid: false,
                  zeroline: false,
                  fixedrange: true,
                },
                yaxis: {
                  title: metricField?.unit || (metric === 'requests' ? 'reqs' : ''),
                  ticksuffix: metricField?.unit || '',
                  separatethousands: true,
                  exponentformat: 'none',
                  showgrid: true,
                  gridcolor: 'hsl(var(--border))',
                  zeroline: false,
                },
                dragmode: false,
              }}
              config={{ displayModeBar: false }}
            />
          </div>
        ) : (
          <div className="flex-1 flex flex-col items-center justify-center text-sm text-muted-foreground">
            <Bell className="w-8 h-8 mb-2 opacity-20" />
            <p>No data available for preview.</p>
            <p className="text-xs opacity-60 mt-1">Adjust metric or window to see data.</p>
          </div>
        )}
      </div>
    </div>
  )
}

'use client'

import React from 'react'
import { Bell, Loader2 } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { ButtonGroup } from '@/components/ui/button-group'
import { Label } from '@/components/ui/label'
import { PlotlyChart } from '@/components/PlotlyChart'
import { useLogFieldsCatalog } from '@/hooks/useLogFieldsCatalog'
import { useTimeLayout } from '@/lib/chart-helpers'
import { formatDate } from '@/lib/date'
import { useTimezoneStore } from '@/stores/timezoneStore'

interface AlertPreviewProps {
  previewData: any
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
  const metricField = React.useMemo(() => catalog?.fields?.find(f => f.id === metric), [catalog, metric])

  const { timezone } = useTimezoneStore()
  const startTime = React.useMemo(() => previewData?.times?.[0], [previewData])
  const endTime = React.useMemo(() => previewData?.times?.[previewData?.times?.length - 1], [previewData])
  const timeLayout = useTimeLayout(startTime, endTime, timezone)

  // Convert the server's UTC timestamps into the user's selected timezone
  // before handing them to Plotly. makeTimeXAxis (inside useTimeLayout) sets
  // the x-axis RANGE from the same timestamps run through formatDate, so the
  // plotted points must be converted the same way — otherwise the data sits
  // at UTC wall-clock while the axis range is in local time, and any point
  // past the local "end" (e.g. a recent traffic spike) falls outside the
  // visible range and gets clipped. Mirrors the dashboard traffic chart
  // (app/dashboard/_sections/chartHelpers.ts).
  const x = React.useMemo(
    () => (previewData?.times || []).map((t: string) => formatDate(t, timezone, 'yyyy-MM-dd HH:mm:ss')),
    [previewData, timezone]
  )

  const getHoverTemplate = React.useCallback((m: string, label?: string) => {
    const pre = label ? `${label}: ` : ''
    const field = m === metric ? metricField : catalog?.fields?.find(f => f.id === m)
    const unit = field?.unit || ''
    const precision = field?.precision ?? (m === 'requests' ? 0 : 1)
    const format = precision > 0 ? `.${precision}f` : ','
    return `${pre}%{y:${format}}${unit}<extra></extra>`
  }, [catalog, metric, metricField])

  return (
    <div className="flex flex-col min-h-[300px]">
      <div className="flex items-center justify-between mb-2">
        <Label>Live Preview</Label>
        <ButtonGroup>
          {[1, 3, 6, 12, 24].map(h => (
            <Button
              key={h}
              type="button"
              variant={lookbackHours === h ? 'default' : 'ghost'}
              size="sm"
              onClick={() => setLookbackHours(h)}
              className={`h-6 text-[10px] px-2 shadow-none transition-colors ${lookbackHours === h ? 'bg-primary text-primary-foreground hover:bg-primary/90' : 'hover:text-primary hover:bg-muted'}`}
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
                     x: x,
                     y: previewData.values,
                     type: (metric === 'requests' || metric === '5xx' || metric === '4xx' || metric === 'specific_status') ? 'bar' : 'scatter',
                     mode: (metric === 'requests' || metric === '5xx' || metric === '4xx' || metric === 'specific_status') ? undefined : 'lines+markers',
                     name: 'Current',
                     marker: { color: '#3b82f6' },
                     line: { color: '#3b82f6', width: 2 },
                     hovertemplate: getHoverTemplate(metric, 'Current')
                  },
                  ...(previewData.type === 'relative' && previewData.hist_values ? [{
                     x: x,
                     y: previewData.hist_values,
                     type: 'scatter',
                     mode: 'lines',
                     name: 'Baseline',
                     line: { color: '#a1a1aa', width: 2, dash: 'dot' },
                     hovertemplate: getHoverTemplate(metric, 'Baseline')
                  }] : []),
                  // If anomaly_zscore, overlay baseline mean and threshold envelope line
                  ...(previewData.type === 'anomaly_zscore' && previewData.hist_values ? [{
                     x: x,
                     y: previewData.hist_values,
                     type: 'scatter',
                     mode: 'lines',
                     name: 'Baseline Mean',
                     line: { color: '#a1a1aa', width: 2, dash: 'dot' },
                     hovertemplate: getHoverTemplate(metric, 'Baseline Mean')
                  }] : []),
                  ...(previewData.type === 'anomaly_zscore' && previewData.threshold_values ? [{
                     x: x,
                     y: previewData.threshold_values,
                     type: 'scatter',
                     mode: 'lines',
                     name: `Threshold (${threshold}σ)`,
                     line: { color: 'hsl(var(--destructive))', width: 2, dash: 'dash' },
                     hovertemplate: getHoverTemplate(metric, 'Threshold')
                  }] : []),
                  // If absolute, overlay the threshold as a horizontal line
                  ...(previewData.type === 'absolute' && parseFloat(threshold) ? [{
                     x: [x[0], x[x.length - 1]],
                     y: [parseFloat(threshold), parseFloat(threshold)],
                     type: 'scatter',
                     mode: 'lines',
                     name: 'Threshold',
                     line: { color: 'hsl(var(--destructive))', width: 2, dash: 'dash' },
                     hoverinfo: 'none'
                  }] : []),
                  // If relative, overlay the calculated threshold line
                  ...(previewData.type === 'relative' && previewData.hist_values && parseFloat(threshold) ? [{
                    x: x,
                    y: previewData.hist_values.map((v: number) => {
                      const t = parseFloat(threshold)
                      return evalType === 'relative_increase' ? v * (1 + t/100) : v * (1 - t/100)
                    }),
                    type: 'scatter',
                    mode: 'lines',
                    name: 'Threshold',
                    line: { color: 'hsl(var(--destructive))', width: 2, dash: 'dash' },
                    hoverinfo: 'none'
                 }] : [])
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
                     // No zoom/pan in the preview — locking the x-axis (the
                     // y-axis is already fixed by PlotlyChart's default) also
                     // removes the on-hover zoom/drag affordances. Zooming here
                     // did nothing useful, so don't advertise it.
                     fixedrange: true
                  },
                  yaxis: {
                     title: metricField?.unit || (metric === 'requests' ? 'reqs' : ''),
                     ticksuffix: metricField?.unit || '',
                     separatethousands: true,
                     exponentformat: 'none',
                     showgrid: true,
                     gridcolor: 'hsl(var(--border))',
                     zeroline: false
                  },
                  dragmode: false
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

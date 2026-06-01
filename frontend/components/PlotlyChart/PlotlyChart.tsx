'use client'

import React, { useRef, useEffect } from 'react'
import dynamic from 'next/dynamic'
import { useTheme } from 'next-themes'

const Plot = dynamic(() => import('react-plotly.js'), { ssr: false })

interface PlotlyChartProps {
  data: any[]
  layout?: any
  config?: any
  className?: string
  height?: number | string
  onRelayout?: (event: any) => void
  onSelected?: (event: any) => void
  onUpdate?: (event: any) => void
}

export const PlotlyChart = React.memo(function PlotlyChart({
  data,
  layout,
  config,
  className,
  height = 300,
  onRelayout,
  onSelected,
  onUpdate
}: PlotlyChartProps) {
  const { theme } = useTheme()
  const isDark = theme === 'dark'
  // Keep a ref to the latest callbacks so the initialized handler is always current
  const onRelayoutRef = useRef(onRelayout)
  const onSelectedRef = useRef(onSelected)
  useEffect(() => { onRelayoutRef.current = onRelayout }, [onRelayout])
  useEffect(() => { onSelectedRef.current = onSelected }, [onSelected])

  // Store the graphDiv so we can re-attach listeners if needed
  const graphDivRef = useRef<any>(null)

  const handleInitialized = useRef((_figure: any, graphDiv: any) => {
    graphDivRef.current = graphDiv
    
    // react-plotly.js has known issues with the onRelayout prop dropping events
    // during zoom interactions, so we attach it directly to the graphDiv here.
    graphDiv.on('plotly_relayout', (event: any) => {
      onRelayoutRef.current?.(event)
    })
  }).current

  // Stable callbacks for Plotly to prevent it from detaching listeners on render
  const handleRelayout = React.useCallback((e: any) => onRelayoutRef.current?.(e), [])
  const handleSelected = React.useCallback((e: any) => onSelectedRef.current?.(e), [])

  const defaultLayout = {
    autosize: true,
    height: typeof height === 'number' ? height : undefined,
    margin: { l: 40, r: 20, t: 20 },
    paper_bgcolor: 'transparent',
    plot_bgcolor: 'transparent',
    font: {
      color: isDark ? '#a1a1aa' : '#3f3f46',
      family: 'Inter, sans-serif'
    },
    hovermode: layout?.hovermode !== undefined ? layout.hovermode : 'x unified',
    hoverlabel: {
      bgcolor: isDark ? '#1e293b' : '#ffffff',
      font: { color: isDark ? '#ffffff' : '#0f172a', size: 12 },
      bordercolor: isDark ? '#1e293b' : '#e2e8f0',
      namelength: -1
    },
    ...layout,
    xaxis: {
      gridcolor: isDark ? '#27272a' : '#e4e4e7',
      zerolinecolor: isDark ? '#27272a' : '#e4e4e7',
      showspikes: false,
      automargin: true,
      ...(layout?.xaxis || {})
    },
    yaxis: {
      gridcolor: isDark ? '#27272a' : '#e4e4e7',
      zerolinecolor: isDark ? '#27272a' : '#e4e4e7',
      showspikes: false,
      fixedrange: true,
      automargin: true,
      ...(layout?.yaxis || {})
    },
    shapes: [...(layout?.shapes || [])],
    annotations: [...(layout?.annotations || [])]
  }
  const defaultConfig = {
    responsive: true,
    displayModeBar: false,
    ...config
  }

  return (
    <div className={className} style={{ height }}>
      <Plot
        data={data}
        layout={defaultLayout}
        config={defaultConfig}
        style={{ width: '100%', height: '100%' }}
        useResizeHandler
        onInitialized={handleInitialized}
        onUpdate={onUpdate}
        onSelected={handleSelected}
      />
    </div>
  )
})

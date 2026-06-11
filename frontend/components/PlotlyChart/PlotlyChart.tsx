'use client'

import React, { useRef, useEffect, useState } from 'react'
import dynamic from 'next/dynamic'
import { useTheme } from 'next-themes'

// Use the cartesian-only Plotly distribution via react-plotly.js's factory
// API. The default `import 'react-plotly.js'` pulls full plotly.js (~4.7 MB
// minified) — we only render scatter / line / bar / pie / heatmap (see the
// dashboard repository), all of which are covered by the cartesian build
// (~1.4 MB minified, ~3.4× smaller). Initial dashboard render felt the
// difference: full-Plotly chunk fetch + parse on every fresh dashboard
// hit was visibly delaying the time-series chart's first paint behind the
// rest of the page. The factory pattern lets us load the leaner Plotly
// without touching every PlotlyChart caller.
const Plot = dynamic(
  async () => {
    const [{ default: createPlotlyComponent }, plotlyModule] = await Promise.all([
      import('react-plotly.js/factory'),
      // No types ship with the cartesian-dist-min package — the runtime
      // shape (`{Plot, plot, react, ...}` plus trace-type registrations)
      // matches the full plotly.js for everything the factory needs.
      import('plotly.js-cartesian-dist-min' as any) as any,
    ])
    return createPlotlyComponent(plotlyModule.default || plotlyModule)
  },
  { ssr: false },
)

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

  // Container ref + narrow-viewport flag. Declared above defaultLayout
  // so the layout block can read `narrow` for the responsive legend
  // reflow below. The IntersectionObserver `visible` gate stays here
  // for the same reason — both observers attach to containerRef in
  // effects further down.
  const containerRef = useRef<HTMLDivElement>(null)
  const [visible, setVisible] = useState(false)
  // Plotly's `responsive: true` resizes the plot but does NOT reflow
  // the legend orientation. A many-series chart (e.g. POP breakdown
  // across 20+ regions on /performance) overflows the chart area on
  // narrow screens. Switch legend to horizontal-below when the
  // container itself drops below 720 px (NOT viewport — a chart in a
  // 6-of-12-column grid on a 1280 px screen sits in ~640 px and
  // should use the narrow layout).
  const [narrow, setNarrow] = useState(false)

  // Narrow-viewport legend defaults: horizontal orientation pinned to
  // the bottom of the plot so labels stack horizontally instead of
  // crowding the right side. Caller's `layout.legend` (if any) gets
  // merged on top so explicit overrides still win.
  const narrowLegendDefaults = narrow
    ? { orientation: 'h' as const, x: 0, y: -0.2, yanchor: 'top' as const, xanchor: 'left' as const }
    : {}

  const defaultLayout = {
    autosize: true,
    height: typeof height === 'number' ? height : undefined,
    // Make room for the bottom legend on narrow viewports — Plotly's
    // default bottom margin is too tight for an h-orientation legend.
    margin: { l: 40, r: 20, t: 20, b: narrow ? 60 : undefined },
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
    // After the ...layout spread so callers can override individual
    // legend fields without losing the narrow-viewport defaults
    // entirely. Caller's full legend overrides take precedence here.
    legend: { ...narrowLegendDefaults, ...(layout?.legend || {}) },
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

  // Viewport gate: don't trigger the dynamic-import of the 1.4MB
  // plotly.js-cartesian-dist chunk until this chart is within 600px of
  // the viewport. `dynamic(...)` only starts fetching when <Plot/> is
  // actually rendered, so withholding the render = withholding the
  // chunk fetch.
  //
  // Initial state MUST be ``false`` on both server and client to avoid
  // a hydration mismatch. Earlier this used ``useState(() => typeof
  // IntersectionObserver === 'undefined')`` so SSR rendered with
  // visible=true; once PlotlyChart started being rendered at the
  // AppLayout level (PlotlyPrewarm), that produced a React 418
  // hydration error on every page load — server emitted ``<div>
  // <Plot/></div>``, client emitted ``<div></div>``. Now the effect
  // below promotes to true on mount when no IntersectionObserver
  // exists, which is the same effective behaviour without the SSR
  // divergence.

  useEffect(() => {
    if (visible || !containerRef.current) return
    if (typeof IntersectionObserver === 'undefined') {
      setVisible(true)
      return
    }
    const node = containerRef.current
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setVisible(true)
          observer.disconnect()
        }
      },
      { rootMargin: '600px' },
    )
    observer.observe(node)
    return () => observer.disconnect()
  }, [visible])

  // ResizeObserver tracks the container's actual rendered width — not
  // viewport. A chart placed in a 6-of-12-column grid on a 1280px
  // screen sits in ~640 px of real estate and SHOULD use the narrow
  // legend layout even though the viewport is wide. 720 px matches
  // the breakpoint shadcn/Tailwind treats as the small/medium hinge.
  useEffect(() => {
    if (!containerRef.current || typeof ResizeObserver === 'undefined') return
    const node = containerRef.current
    const ro = new ResizeObserver(([entry]) => {
      const w = entry.contentRect?.width ?? 0
      setNarrow((prev) => (w > 0 && w < 720 ? true : w >= 720 ? false : prev))
    })
    ro.observe(node)
    return () => ro.disconnect()
  }, [])

  return (
    <div ref={containerRef} className={className} style={{ height }}>
      {visible ? (
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
      ) : null}
    </div>
  )
})

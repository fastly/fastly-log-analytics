'use client'

import React from 'react'
import { PlotlyChart, preloadPlotlyChunk } from './PlotlyChart'

/**
 * Renders an invisible 1-point Plotly chart on app mount to force the
 * dynamic-import resolution + Plotly's first-plot draw cost to happen
 * during initial page load, BEFORE the user's real chart needs to render.
 *
 * Why this helps: the plotly.js-cartesian-dist-min chunk is already
 * preloaded via the modulepreload pattern (~442KB compressed), so the
 * NETWORK fetch is done early. What ISN'T done early is the JS
 * parse/compile (~200-500ms) and Plotly's internal `newPlot` init
 * (~500-1000ms) — those only run when a <Plot> component actually
 * mounts with non-empty data.
 *
 * The real dashboard chart mounts with `data=[]` while aggregates
 * loads, so Plotly's heavy first-draw path runs when REAL data
 * arrives. That's the ~1.7s gap users perceive between "data loaded"
 * and "chart appeared."
 *
 * Pre-warming with a 1-point chart on app mount runs that heavy path
 * during page load (when the user is already waiting for content),
 * so when the real chart re-renders with arriving data, it hits
 * Plotly's much-faster react()-update path instead of the cold init
 * path. Estimated saving: ~300-500ms on the data-to-chart gap.
 *
 * The prewarm chart is rendered off-screen (absolute positioned far
 * negative left + aria-hidden) so it's invisible to users and
 * screen-readers. Height/width are tiny so the chunk-fetch cost is
 * the only real work it does.
 */
function PlotlyPrewarmImpl() {
  // Kick the dynamic chunk fetch on mount, in parallel with React's
  // render of the invisible prewarm chart below. Without this, the
  // chunk fetch waits for the inner PlotlyChart's IntersectionObserver
  // to fire — even though the prewarm is in-flow, the IO callback
  // costs an extra idle frame vs. starting the import() immediately.
  // On chart-bearing route mount this shaves ~100-300ms off the cold
  // path from page-mount to first chart draw.
  React.useEffect(() => {
    void preloadPlotlyChunk()
  }, [])

  // Render once on mount; then never re-render (memoized + stable refs).
  // Wrapping in React.memo with no props is belt-and-suspenders so any
  // parent re-render does not re-trigger the prewarm.
  const data = React.useRef([
    {
      x: [0],
      y: [0],
      type: 'scatter' as const,
      mode: 'lines' as const,
    },
  ]).current

  // Layout/config trivial — we only care about forcing init.
  const layout = React.useRef({
    margin: { l: 0, r: 0, t: 0, b: 0 },
    showlegend: false,
    paper_bgcolor: 'transparent',
    plot_bgcolor: 'transparent',
    xaxis: { visible: false },
    yaxis: { visible: false },
  }).current

  return (
    // IMPORTANT: PlotlyChart gates <Plot> rendering on an
    // IntersectionObserver (rootMargin: '600px' from viewport). Off-
    // screen positioning would never trigger isIntersecting=true, so
    // the prewarm wouldn't actually run. Instead we keep the prewarm
    // IN the layout flow but visually hidden via opacity:0 +
    // pointer-events:none + tiny height. The IntersectionObserver
    // sees the element as visible and fires the dynamic import +
    // Plotly init — exactly the warming we want.
    <div
      aria-hidden="true"
      style={{
        opacity: 0,
        height: '1px',
        width: '1px',
        overflow: 'hidden',
        pointerEvents: 'none',
      }}
    >
      <PlotlyChart data={data} layout={layout} height={1} />
    </div>
  )
}

export const PlotlyPrewarm = React.memo(PlotlyPrewarmImpl)

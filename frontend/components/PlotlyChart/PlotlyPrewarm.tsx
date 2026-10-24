'use client'

import React from 'react'
import dynamic from 'next/dynamic'
import type { Map as MapLibreMap } from 'maplibre-gl'
import { PlotlyChart, preloadPlotlyChunk } from './PlotlyChart'
import type { PlotlyLayout, PlotlyTrace } from '@/types/plotly'

// One invisible point; module-level so the chart's props keep a stable identity across renders.
const PREWARM_DATA: PlotlyTrace[] = [
  {
    x: [0],
    y: [0],
    type: 'scatter',
    mode: 'lines',
  },
]

// Layout/config trivial — we only care about forcing init.
const PREWARM_LAYOUT: PlotlyLayout = {
  margin: { l: 0, r: 0, t: 0, b: 0 },
  showlegend: false,
  paper_bgcolor: 'transparent',
  plot_bgcolor: 'transparent',
  xaxis: { visible: false },
  yaxis: { visible: false },
}

function PlotlyPrewarmImpl() {
  React.useEffect(() => {
    void preloadPlotlyChunk()
  }, [])

  return (
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
      <PlotlyChart data={PREWARM_DATA} layout={PREWARM_LAYOUT} height={1} />
    </div>
  )
}

export const PlotlyPrewarm = React.memo(PlotlyPrewarmImpl)

// ─── MapPrewarm ──────────────────────────────────────────────────────────────

const PrewarmMap = dynamic(
  async () => {
    const maplibre = await import('maplibre-gl')
    maplibre.setWorkerUrl('/maplibre-gl-worker.mjs')
    const MaplibreMap: typeof MapLibreMap | undefined =
      maplibre.Map || (maplibre as { default?: { Map?: typeof MapLibreMap } }).default?.Map

    function PrewarmInner() {
      const ref = React.useRef<HTMLDivElement>(null)
      React.useEffect(() => {
        if (!ref.current || !MaplibreMap) return
        let map: MapLibreMap | null = null
        try {
          map = new MaplibreMap({
            container: ref.current,
            style: { version: 8, sources: {}, layers: [] },
            interactive: false,
            attributionControl: false,
          })
        } catch {
          // WebGL unavailable (test env / headless / locked-down browser).
        }
        return () => {
          try {
            map?.remove()
          } catch {}
        }
      }, [])
      return <div ref={ref} style={{ width: 1, height: 1 }} />
    }
    return PrewarmInner
  },
  { ssr: false },
)

function MapPrewarmImpl() {
  return (
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
      <PrewarmMap />
    </div>
  )
}

export const MapPrewarm = React.memo(MapPrewarmImpl)

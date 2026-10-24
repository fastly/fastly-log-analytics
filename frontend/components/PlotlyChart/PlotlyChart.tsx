'use client'

import React, { useRef, useEffect, useState } from 'react'
import dynamic from 'next/dynamic'
import { useTheme } from 'next-themes'

import { keyByOccurrence } from '@/lib/utils'
import type {
  PlotlyConfig,
  PlotlyFigure,
  PlotlyGraphDiv,
  PlotlyLayout,
  PlotlyRelayoutEvent,
  PlotlyTrace,
} from '@/types/plotly'

// ─── tracesToTable ──────────────────────────────────────────────────────────

export interface TableShape {
  /** Optional caption text. Falls back to "Chart data". */
  title: string
  /** Column headers, length = each row's length. */
  columns: string[]
  /** Each row is a flat list of strings/numbers, length === columns.length. */
  rows: (string | number)[][]
  /** True when the helper couldn't extract a meaningful table from the
   * input (unsupported trace mix, empty data, etc.). Caller may choose
   * to render a "{N} data points" fallback. */
  empty: boolean
  /** When the row list was capped for size, the original (pre-cap) row
   * count; undefined when the table is complete. The renderer surfaces
   * this so a screen-reader user knows the table is partial. */
  truncatedFrom?: number
}

// Backstop cap on the screen-reader companion table. A flat table beyond
// this many rows is effectively unnavigable by assistive tech and just
// bloats the (hidden) DOM; ordinary time-series charts sit far below it.
// Dense 2-D traces (heatmaps) are skipped entirely before this applies.
const MAX_A11Y_ROWS = 500

function capRows(rows: (string | number)[][]): {
  rows: (string | number)[][]
  truncatedFrom?: number
} {
  if (rows.length > MAX_A11Y_ROWS) {
    return { rows: rows.slice(0, MAX_A11Y_ROWS), truncatedFrom: rows.length }
  }
  return { rows }
}

/**
 * Convert Plotly traces to a flat table.
 *
 * Supported shapes:
 *   - line / bar / scatter: each trace has `{x, y, name?}`. Output is
 *     a wide table — first column is the shared x value, one column
 *     per trace named after `trace.name`.
 *   - pie / donut: single trace has `{labels, values}`. Output is
 *     two columns: label + value.
 *   - heatmap / surface / 3d: not supported — returns empty.
 *
 * If multiple traces are line/bar/scatter but have DIFFERENT x arrays
 * (e.g., x-ranges don't line up), we union the x values and put nulls
 * where a trace didn't have a point.
 */
export function tracesToTable(data: unknown, fallbackTitle = 'Chart data'): TableShape {
  if (!Array.isArray(data) || data.length === 0) {
    return { title: fallbackTitle, columns: [], rows: [], empty: true }
  }

  const traces = data as PlotlyTrace[]

  // ── Heatmap / surface / contour / any z-matrix trace ───────────
  if (
    traces.some(
      (t) =>
        t.type === 'heatmap' ||
        t.type === 'surface' ||
        t.type === 'contour' ||
        Array.isArray(t.z),
    )
  ) {
    return { title: fallbackTitle, columns: [], rows: [], empty: true }
  }

  // ── Pie / donut ────────────────────────────────────────────────
  if (
    traces.length === 1 &&
    (traces[0].type === 'pie' || traces[0].type === 'donut') &&
    Array.isArray(traces[0].labels) &&
    Array.isArray(traces[0].values)
  ) {
    const labels = traces[0].labels as Array<string | number>
    const values = traces[0].values as Array<string | number>
    const rows: (string | number)[][] = []
    const n = Math.min(labels.length, values.length)
    for (let i = 0; i < n; i++) {
      rows.push([labels[i], values[i]])
    }
    const capped = capRows(rows)
    return {
      title: traces[0].name || fallbackTitle,
      columns: ['Label', 'Value'],
      rows: capped.rows,
      empty: rows.length === 0,
      truncatedFrom: capped.truncatedFrom,
    }
  }

  // ── Line / bar / scatter (the common case) ─────────────────────
  const xyTraces = traces.filter(
    (t) => Array.isArray(t.x) && Array.isArray(t.y),
  )
  if (xyTraces.length === 0) {
    return { title: fallbackTitle, columns: [], rows: [], empty: true }
  }

  // Union of all x values, preserving first-seen order.
  const xKeys: (string | number)[] = []
  const xSeen = new Set<string>()
  for (const trace of xyTraces) {
    for (const xv of trace.x as Array<string | number>) {
      const k = String(xv)
      if (!xSeen.has(k)) {
        xSeen.add(k)
        xKeys.push(xv)
      }
    }
  }

  const columns: string[] = ['x']
  for (let i = 0; i < xyTraces.length; i++) {
    columns.push(String(xyTraces[i].name ?? `Series ${i + 1}`))
  }

  const xIndex: Map<string, number>[] = xyTraces.map((trace) => {
    const m = new Map<string, number>()
    const xs = trace.x as Array<string | number>
    for (let i = 0; i < xs.length; i++) m.set(String(xs[i]), i)
    return m
  })

  const rows: (string | number)[][] = []
  for (const xv of xKeys) {
    const key = String(xv)
    const row: (string | number)[] = [xv]
    for (let t = 0; t < xyTraces.length; t++) {
      const idx = xIndex[t].get(key)
      const ys = xyTraces[t].y as Array<string | number | null | undefined>
      const yv = idx !== undefined ? ys[idx] : undefined
      row.push(yv === undefined || yv === null ? '' : yv)
    }
    rows.push(row)
  }

  const capped = capRows(rows)
  return {
    title: fallbackTitle,
    columns,
    rows: capped.rows,
    empty: rows.length === 0,
    truncatedFrom: capped.truncatedFrom,
  }
}

// ─── ChartA11yTable ─────────────────────────────────────────────────────────

interface ChartA11yTableProps {
  shape: TableShape
}

export const ChartA11yTable = React.memo(function ChartA11yTable({ shape }: ChartA11yTableProps) {
  if (shape.empty) {
    return (
      <div className="sr-only" aria-hidden="false">
        <p>{shape.title}: no readable data points available.</p>
      </div>
    )
  }

  return (
    <table className="sr-only" aria-hidden="false">
      <caption>
        {shape.title}
        {shape.truncatedFrom != null &&
          ` — showing first ${shape.rows.length} of ${shape.truncatedFrom} rows`}
      </caption>
      <thead>
        <tr>
          {keyByOccurrence(shape.columns, String).map(({ key, item: col }) => (
            <th key={key} scope="col">
              {col}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {keyByOccurrence(shape.rows, (row) => String(row[0])).map(({ key, item: row }) => (
          <tr key={key}>
            {keyByOccurrence(row, String).map(({ key: cellKey, item: cell }) => (
              <td key={cellKey}>{String(cell)}</td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  )
})

// ─── PlotlyChart ────────────────────────────────────────────────────────────

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
      // No types ship with the cartesian-dist-min package (see
      // types/plotly-cartesian.d.ts) — the runtime shape (`{Plot, plot, react, ...}`
      // plus trace-type registrations) matches the full plotly.js for everything
      // the factory needs.
      import('plotly.js-cartesian-dist-min'),
    ])
    return createPlotlyComponent(plotlyModule.default || plotlyModule)
  },
  { ssr: false },
)

// Kick off the Plotly chunk fetch on demand. PlotlyPrewarm calls this in
// a mount effect so the 1.4MB cartesian-dist chunk starts downloading in
// parallel with React's render of the invisible prewarm chart, rather
// than after the prewarm's inner IntersectionObserver gate flips. Safe
// to call multiple times — the dynamic import cache de-dupes.
let plotlyPreloadPromise: Promise<unknown> | null = null
export function preloadPlotlyChunk(): Promise<unknown> {
  if (plotlyPreloadPromise) return plotlyPreloadPromise
  plotlyPreloadPromise = Promise.all([
    import('react-plotly.js/factory'),
    import('plotly.js-cartesian-dist-min'),
  ])
  return plotlyPreloadPromise
}

interface PlotlyChartProps {
  data: PlotlyTrace[]
  layout?: PlotlyLayout
  config?: PlotlyConfig
  className?: string
  height?: number | string
  onRelayout?: (event: PlotlyRelayoutEvent) => void
  onUpdate?: (figure: PlotlyFigure, graphDiv: HTMLElement) => void
  /**
   * Optional caption for the screen-reader-only data table emitted
   * alongside the chart. Defaults to "Chart data". Pass something
   * descriptive (e.g., "Requests over the last 24 hours") so assistive
   * tech announces useful context before reading the data.
   */
  a11yTitle?: string
}

function findScrollContainer(node: HTMLElement): HTMLElement | null {
  let parent = node.parentElement
  while (parent) {
    const style = window.getComputedStyle(parent)
    if (/(auto|scroll|overlay)/.test(`${style.overflow}${style.overflowX}${style.overflowY}`)) {
      return parent
    }
    parent = parent.parentElement
  }
  return null
}

export const PlotlyChart = React.memo(function PlotlyChart({
  data,
  layout,
  config,
  className,
  height = 300,
  onRelayout,
  onUpdate,
  a11yTitle = 'Chart data'
}: PlotlyChartProps) {
  const { theme } = useTheme()
  const isDark = theme === 'dark'
  // Keep refs to the latest callbacks so the initialized handler is always current
  const onRelayoutRef = useRef(onRelayout)
  useEffect(() => {
    onRelayoutRef.current = onRelayout
  }, [onRelayout])

  // Store the graphDiv so we can re-attach listeners if needed
  const graphDivRef = useRef<PlotlyGraphDiv | null>(null)

  const handleInitialized = useRef((_figure: PlotlyFigure, element: HTMLElement) => {
    const graphDiv: PlotlyGraphDiv = element
    graphDivRef.current = graphDiv

    // react-plotly.js has known issues with the onRelayout prop dropping events
    // during zoom interactions, so we attach it directly to the graphDiv here.
    graphDiv.on?.('plotly_relayout', (event) => {
      onRelayoutRef.current?.(event)
    })
  }).current

  const containerRef = useRef<HTMLDivElement>(null)
  const [visible, setVisible] = useState(false)
  const [narrow, setNarrow] = useState(false)

  const narrowLegendDefaults = narrow
    ? { orientation: 'h' as const, x: 0, y: -0.2, yanchor: 'top' as const, xanchor: 'left' as const }
    : {}

  const effectiveHeight =
    typeof height === 'number' && narrow ? Math.round(height * 1.2) : height

  const hasLegend = layout?.showlegend !== false && ((data && data.length > 1) || layout?.showlegend === true)
  const defaultTopMargin = hasLegend && !narrow ? 32 : 8
  const defaultBottomMargin = narrow ? 60 : 32

  const defaultLayout = {
    autosize: true,
    height: typeof effectiveHeight === 'number' ? effectiveHeight : undefined,
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
    margin: {
      l: 40,
      r: 20,
      t: defaultTopMargin,
      b: defaultBottomMargin,
      ...(layout?.margin || {})
    },
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
      { root: findScrollContainer(node), rootMargin: '600px' },
    )
    observer.observe(node)
    return () => observer.disconnect()
  }, [visible])

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

  const a11yShape = React.useMemo(() => tracesToTable(data, a11yTitle), [data, a11yTitle])

  return (
    <div ref={containerRef} className={className} style={{ height: effectiveHeight, position: 'relative', overflow: 'hidden' }}>
      {visible ? (
        <Plot
          data={data}
          layout={defaultLayout}
          config={defaultConfig}
          style={{ width: '100%', height: '100%' }}
          useResizeHandler
          onInitialized={handleInitialized}
          onUpdate={onUpdate}
        />
      ) : null}
      <ChartA11yTable shape={a11yShape} />
    </div>
  )
})

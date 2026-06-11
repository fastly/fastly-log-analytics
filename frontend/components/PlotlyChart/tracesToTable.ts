// Pure helper: Plotly trace array → generic table shape for a
// screen-reader-only <table> companion. Lives next to PlotlyChart
// so the contract between the helper and the renderer stays in
// one place.
//
// Plotly traces are loosely typed in upstream's d.ts — we accept
// `any` here and inspect at runtime rather than chase the union of
// every plotly trace type.

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

  const traces = data as Array<Record<string, any>>

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
    return {
      title: traces[0].name || fallbackTitle,
      columns: ['Label', 'Value'],
      rows,
      empty: rows.length === 0,
    }
  }

  // ── Line / bar / scatter (the common case) ─────────────────────
  const xyTraces = traces.filter(
    (t) => Array.isArray(t.x) && Array.isArray(t.y),
  )
  if (xyTraces.length === 0) {
    return { title: fallbackTitle, columns: [], rows: [], empty: true }
  }

  // Union of all x values, preserving first-seen order. We don't sort
  // because some charts plot a categorical x axis where order matters
  // (e.g., bucket labels '1', '2-5', '6-20', '21+').
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

  // For each x value, look up the corresponding y in each trace by
  // index in that trace's x array.
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
      // Replace null/undefined with '' so the table cell renders empty
      // rather than the literal string "undefined".
      row.push(yv === undefined || yv === null ? '' : yv)
    }
    rows.push(row)
  }

  return { title: fallbackTitle, columns, rows, empty: rows.length === 0 }
}

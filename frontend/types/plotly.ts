// Minimal local Plotly shapes.
//
// react-plotly.js types `data` / `layout` / `config` as `unknown` and ships no plotly.js
// typings; `plotly.js-cartesian-dist-min` has none either. These interfaces name the keys
// this app actually reads or sets and leave every other Plotly attribute open through the
// `unknown` index signature, so callers keep full attribute freedom without `any`.

export type PlotlyDatum = string | number | boolean | Date | null | undefined

export interface PlotlyFont {
  color?: string
  family?: string
  size?: number
  [key: string]: unknown
}

export interface PlotlyAxis {
  title?: string | { text?: string; [key: string]: unknown }
  type?: string
  range?: readonly PlotlyDatum[]
  nticks?: number
  tickangle?: number
  automargin?: boolean
  fixedrange?: boolean
  showgrid?: boolean
  showspikes?: boolean
  gridcolor?: string
  zerolinecolor?: string
  separatethousands?: boolean
  exponentformat?: string
  ticksuffix?: string
  tickformat?: string
  autorange?: boolean | string
  [key: string]: unknown
}

export interface PlotlyLegend {
  orientation?: 'h' | 'v'
  x?: number
  y?: number
  xanchor?: 'auto' | 'left' | 'center' | 'right'
  yanchor?: 'auto' | 'top' | 'middle' | 'bottom'
  font?: PlotlyFont
  bgcolor?: string
  [key: string]: unknown
}

export interface PlotlyMargin {
  l?: number
  r?: number
  t?: number
  b?: number
  pad?: number
}

export interface PlotlyLayout {
  autosize?: boolean
  height?: number
  width?: number
  title?: string | { text?: string; [key: string]: unknown }
  barmode?: 'stack' | 'group' | 'overlay' | 'relative'
  showlegend?: boolean
  hovermode?: string | false
  paper_bgcolor?: string
  plot_bgcolor?: string
  font?: PlotlyFont
  margin?: PlotlyMargin
  legend?: PlotlyLegend
  xaxis?: PlotlyAxis
  yaxis?: PlotlyAxis
  yaxis2?: PlotlyAxis
  shapes?: readonly Record<string, unknown>[]
  annotations?: readonly Record<string, unknown>[]
  [key: string]: unknown
}

export interface PlotlyConfig {
  responsive?: boolean
  displayModeBar?: boolean
  [key: string]: unknown
}

export interface PlotlyTrace {
  type?: string
  mode?: string
  name?: string
  x?: readonly PlotlyDatum[]
  y?: readonly PlotlyDatum[]
  z?: readonly (readonly PlotlyDatum[])[]
  labels?: readonly PlotlyDatum[]
  values?: readonly PlotlyDatum[]
  text?: readonly string[] | string
  fill?: string
  stackgroup?: string
  hovertemplate?: string
  width?: number | readonly number[]
  marker?: Record<string, unknown>
  line?: Record<string, unknown>
  [key: string]: unknown
}

/** `plotly_relayout` payload: flat dotted keys (`xaxis.range[0]`) or a nested `xaxis.range`. */
export type PlotlyRelayoutEvent = Record<string, unknown>

/** Event payload for callbacks whose shape this app never inspects. */
export type PlotlyEvent = unknown

export interface PlotlyFigure {
  data: readonly unknown[]
  layout: unknown
  frames: readonly unknown[] | null
}

/** The `graphDiv` handed to `onInitialized`: an element carrying Plotly's `on` emitter. */
export interface PlotlyGraphDiv extends HTMLElement {
  on?: (event: string, handler: (payload: PlotlyRelayoutEvent) => void) => void
}

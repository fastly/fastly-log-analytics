/**
 * Semantic hex colours for Plotly traces, maps and other canvas/SVG surfaces
 * that cannot read CSS variables. Tailwind 500-series values; keep in sync with
 * the status/severity intent, not with individual charts.
 */
export const CHART_COLORS = {
  success: '#10b981',
  danger: '#ef4444',
  warning: '#f59e0b',
  info: '#3b82f6',
  neutral: '#94a3b8',
} as const

/** HTTP status code to chart colour: 2xx success, 3xx info, 4xx warning, 5xx danger. */
export function statusHex(code: number): string {
  if (code >= 500) return CHART_COLORS.danger
  if (code >= 400) return CHART_COLORS.warning
  if (code >= 300) return CHART_COLORS.info
  return CHART_COLORS.success
}

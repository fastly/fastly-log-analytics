import { useCallback } from 'react'
import { parseFromInput } from '@/lib/date'
import type { PlotlyRelayoutEvent } from '@/types/plotly'

export function usePlotlyRangeZoom(
  timezone: string,
  setRange: (start: string, end: string) => void,
) {
  return useCallback(
    (event: PlotlyRelayoutEvent) => {
      if (event['xaxis.autorange'] === true || event['xaxis.showspikes'] !== undefined) return

      const nested = Array.isArray(event['xaxis.range']) ? event['xaxis.range'] : []
      const bound = (flat: unknown, fallback: unknown): string | number | undefined => {
        const v = flat ?? fallback
        return typeof v === 'string' || typeof v === 'number' ? v : undefined
      }
      const x0 = bound(event['xaxis.range[0]'], nested[0])
      const x1 = bound(event['xaxis.range[1]'], nested[1])

      if (x0 === undefined || x1 === undefined) return

      try {
        const toLocalStr = (val: string | number) => {
          if (typeof val === 'number') {
            const d = new Date(val)
            const pad = (n: number) => n.toString().padStart(2, '0')
            return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
          }
          return val.replace(' ', 'T')
        }
        const parsedStart = parseFromInput(toLocalStr(x0), timezone)
        const parsedEnd = parseFromInput(toLocalStr(x1), timezone)
        if (parsedStart && parsedEnd) {
          setRange(parsedStart, parsedEnd)
        }
      } catch {
        // Ignore unparseable zoom events
      }
    },
    [setRange, timezone],
  )
}

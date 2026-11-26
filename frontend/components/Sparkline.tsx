'use client'

import React from 'react'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { useDateFormat } from '@/hooks/useSSE'

type SparklinePoint = { ts: string; value: number }

export type SparklineProps = {
  points: SparklinePoint[]
  yDomain?: [number | 'auto', number | 'auto']
  height?: number
  stroke?: string
  formatValue?: (value: number) => string
  hoverThreshold?: number
  interactive?: boolean
  label?: string
}

/** Compact SVG trend line with optional pointer-hover sample inspection tooltip. */
export function Sparkline({
  points,
  yDomain = ['auto', 'auto'],
  height = 28,
  stroke = 'currentColor',
  formatValue,
  hoverThreshold = 48,
  interactive,
  label = 'trend',
}: SparklineProps) {
  if (!points || points.length < 2) return null

  const showHover = interactive ?? height >= hoverThreshold
  const width = 100
  let lo = Infinity
  let hi = -Infinity
  for (const p of points) {
    if (p.value < lo) lo = p.value
    if (p.value > hi) hi = p.value
  }
  const [minRaw, maxRaw] = yDomain
  const min = minRaw === 'auto' ? lo : minRaw
  const max = maxRaw === 'auto' ? hi : maxRaw
  const range = max - min || 1
  const yMin = min - (maxRaw === 'auto' ? range * 0.05 : 0)
  const yMax = max + (minRaw === 'auto' ? range * 0.05 : 0)
  const ySpan = yMax - yMin || 1
  const stepX = width / (points.length - 1)
  const pts = points.map((p, i) => ({ x: i * stepX, y: height - ((p.value - yMin) / ySpan) * height, raw: p }))
  const polylinePoints = pts.map((c) => `${c.x.toFixed(2)},${c.y.toFixed(2)}`).join(' ')

  if (!showHover) {
    return (
      <svg viewBox={`0 0 ${width} ${height}`} width="100%" height={height} preserveAspectRatio="none" aria-hidden="true" style={{ display: 'block' }}>
        <polyline points={polylinePoints} fill="none" stroke={stroke} strokeWidth={1.25} strokeOpacity={0.55} vectorEffect="non-scaling-stroke" />
      </svg>
    )
  }

  return <InteractiveSparkline width={width} height={height} stroke={stroke} pts={pts} polylinePoints={polylinePoints} formatValue={formatValue} label={label} />
}

function InteractiveSparkline({
  width,
  height,
  stroke,
  pts,
  polylinePoints,
  formatValue,
  label,
}: {
  width: number
  height: number
  stroke: string
  pts: { x: number; y: number; raw: SparklinePoint }[]
  polylinePoints: string
  formatValue?: (value: number) => string
  label: string
}) {
  const svgRef = React.useRef<SVGSVGElement>(null)
  const cachedRectRef = React.useRef<DOMRect | null>(null)
  const fmt = useDateFormat()

  const hoverKey = `${pts.length}|${pts[0]?.raw.ts}|${pts[pts.length - 1]?.raw.ts}`
  const [hover, setHover] = React.useState<{ key: string; idx: number | null }>({ key: hoverKey, idx: null })
  const hoverIdx = hover.key === hoverKey ? hover.idx : null
  const setHoverIdx = React.useCallback(
    (update: number | null | ((prev: number | null) => number | null)) =>
      setHover((h) => {
        const prev = h.key === hoverKey ? h.idx : null
        return { key: hoverKey, idx: typeof update === 'function' ? update(prev) : update }
      }),
    [hoverKey],
  )

  React.useEffect(() => {
    const invalidate = () => { cachedRectRef.current = null }
    window.addEventListener('resize', invalidate)
    return () => window.removeEventListener('resize', invalidate)
  }, [])

  const updateHover = React.useCallback((clientX: number) => {
    const el = svgRef.current
    if (!el) return
    const rect = cachedRectRef.current ?? (cachedRectRef.current = el.getBoundingClientRect())
    if (rect.width <= 0) return
    const idx = Math.max(0, Math.min(pts.length - 1, Math.round(((clientX - rect.left) / rect.width) * (pts.length - 1))))
    setHoverIdx((prev) => (prev === idx ? prev : idx))
  }, [pts.length, setHoverIdx])

  const onPointerEnter = React.useCallback((e: React.PointerEvent<SVGSVGElement>) => { cachedRectRef.current = e.currentTarget.getBoundingClientRect() }, [])
  const onPointerMove = React.useCallback((e: React.PointerEvent<SVGSVGElement>) => updateHover(e.clientX), [updateHover])
  const onPointerDown = React.useCallback((e: React.PointerEvent<SVGSVGElement>) => {
    cachedRectRef.current = e.currentTarget.getBoundingClientRect()
    updateHover(e.clientX)
  }, [updateHover])
  const onPointerLeave = React.useCallback((e: React.PointerEvent<SVGSVGElement>) => {
    if (e.pointerType !== 'touch' && e.pointerType !== 'pen') setHoverIdx(null)
  }, [setHoverIdx])
  const onPointerCancel = React.useCallback(() => setHoverIdx(null), [setHoverIdx])

  React.useEffect(() => {
    if (hoverIdx === null) return
    const onDocPointerDown = (e: PointerEvent) => {
      if (e.pointerType !== 'touch' && e.pointerType !== 'pen') return
      if (svgRef.current && e.target instanceof Node && svgRef.current.contains(e.target)) return
      setHoverIdx(null)
    }
    document.addEventListener('pointerdown', onDocPointerDown)
    return () => document.removeEventListener('pointerdown', onDocPointerDown)
  }, [hoverIdx, setHoverIdx])

  const hovered = hoverIdx !== null ? pts[hoverIdx] : null
  const fmtVal = (v: number) => (formatValue ? formatValue(v) : v.toFixed(2))
  const latestText = fmtVal(pts[pts.length - 1].raw.value)
  const tsPattern = React.useMemo(
    () => (!hovered || fmt.format(hovered.raw.ts, 'yyyy-MM-dd') === fmt.format(new Date(), 'yyyy-MM-dd') ? 'HH:mm' : 'MMM d, HH:mm'),
    [hovered, fmt],
  )
  const hoverLeft = hovered ? `${(hovered.x / width) * 100}%` : 0
  const hoverTop = hovered ? `${(hovered.y / height) * 100}%` : 0

  return (
    <div className="relative" style={{ width: '100%', height }}>
      <svg
        ref={svgRef}
        viewBox={`0 0 ${width} ${height}`}
        width="100%"
        height={height}
        preserveAspectRatio="none"
        role="img"
        aria-label={`${label} sparkline, latest ${latestText}, ${pts.length} samples`}
        style={{ display: 'block', touchAction: 'manipulation' }}
        onPointerEnter={onPointerEnter}
        onPointerMove={onPointerMove}
        onPointerDown={onPointerDown}
        onPointerLeave={onPointerLeave}
        onPointerCancel={onPointerCancel}
      >
        <polyline points={polylinePoints} fill="none" stroke={stroke} strokeWidth={1.25} strokeOpacity={0.55} vectorEffect="non-scaling-stroke" />
        {hovered && <line x1={hovered.x} x2={hovered.x} y1={0} y2={height} stroke="hsl(var(--muted-foreground))" strokeOpacity={0.55} strokeWidth={0.5} vectorEffect="non-scaling-stroke" />}
        <rect x={0} y={0} width={width} height={height} fill="transparent" />
      </svg>
      {hovered && <span aria-hidden style={{ position: 'absolute', left: hoverLeft, top: hoverTop, width: 6, height: 6, marginLeft: -3, marginTop: -3, borderRadius: '50%', background: stroke, pointerEvents: 'none' }} />}
      <Tooltip open={hovered !== null} onOpenChange={(open) => { if (!open) setHoverIdx(null) }}>
        <TooltipTrigger render={<span aria-hidden style={{ position: 'absolute', left: hoverLeft, top: hoverTop, width: 1, height: 1, pointerEvents: 'none' }} />} />
        {hovered && (
          <TooltipContent side="top" sideOffset={8} className="text-xs max-w-[200px] p-2 space-y-1">
            <div className="font-mono tabular-nums">{fmtVal(hovered.raw.value)}</div>
            <div className="text-muted-foreground">{fmt.format(hovered.raw.ts, tsPattern)}</div>
          </TooltipContent>
        )}
      </Tooltip>
    </div>
  )
}

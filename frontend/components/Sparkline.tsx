'use client'

import * as React from 'react'

import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { useDateFormat } from '@/hooks/useDateFormat'

type Point = { ts: string; value: number }

type Props = {
  points: Point[]
  /** Optional fixed y-domain. Useful for percentages (0-100) where the
   *  auto-domain would otherwise compress flat-line series to invisibility. */
  yDomain?: [number | 'auto', number | 'auto']
  height?: number
  /** Override stroke colour. Defaults to ``currentColor`` so the line
   *  inherits the parent's text colour (matches tone-based stat colours). */
  stroke?: string
  /** Format the hovered value (e.g. ``v => `${v.toFixed(1)}%```). When
   *  absent the tooltip shows ``value.toFixed(2)``. Only used when the
   *  hover overlay is active (see ``hoverThreshold``). */
  formatValue?: (value: number) => string
  /** Minimum SVG height (px) at which the hover overlay activates.
   *  Below this, Sparkline renders the decorative SVG unchanged
   *  (aria-hidden, no pointer events) to preserve the existing
   *  SystemHealthCard 28px form factor. Default 48. */
  hoverThreshold?: number
  /** Force-enable or force-disable the hover overlay regardless of height.
   *  Default ``undefined`` → derived from ``height >= hoverThreshold``. */
  interactive?: boolean
  /** Short label used for the interactive SVG's aria-label and tooltip
   *  context. Defaults to "trend". */
  label?: string
}

/** Tiny pure-SVG line chart for in-card trend rendering. Two modes:
 *
 *  - **Decorative** (default at height <48): pure ``<polyline>``,
 *    ``aria-hidden``, no pointer handlers — same as before, used by
 *    SystemHealthCard's 28px stat cards.
 *  - **Interactive** (height >=48 or ``interactive`` explicit): adds a
 *    hover guide line + point marker + Base UI tooltip showing the value
 *    and timestamp at the hovered point. Wraps the SVG in a ``relative``
 *    div for the moving-anchor positioning, so consumers should expect a
 *    ``div`` wrapper in interactive mode (vs a bare ``svg`` in
 *    decorative mode).
 *
 *  Renders nothing when there are fewer than 2 points so the card stays
 *  clean during the first few minutes after a fresh boot. */
export function Sparkline({
  points,
  yDomain = ['auto', 'auto'],
  height = 28,
  stroke = 'currentColor',
  formatValue,
  hoverThreshold = 48,
  interactive,
  label = 'trend',
}: Props) {
  if (!points || points.length < 2) return null

  const showHover = interactive ?? height >= hoverThreshold

  const width = 100 // viewBox units; the SVG scales to its container width

  // Single-pass min/max — defends against `Math.min(...arr)` spread limits
  // on a 7d window (~10k samples) and is cheaper at every size.
  let lo = Infinity
  let hi = -Infinity
  for (const p of points) {
    if (p.value < lo) lo = p.value
    if (p.value > hi) hi = p.value
  }
  const [minRaw, maxRaw] = yDomain
  const min = minRaw === 'auto' ? lo : minRaw
  const max = maxRaw === 'auto' ? hi : maxRaw
  // Add a 5% buffer when both bounds are auto so the line doesn't kiss
  // the top/bottom of the box.
  const range = max - min || 1
  const padTop = minRaw === 'auto' ? range * 0.05 : 0
  const padBot = maxRaw === 'auto' ? range * 0.05 : 0
  const yMin = min - padBot
  const yMax = max + padTop
  const ySpan = yMax - yMin || 1

  const stepX = width / (points.length - 1)
  const pts = points.map((p, i) => ({
    x: i * stepX,
    // Invert Y because SVG origin is top-left.
    y: height - ((p.value - yMin) / ySpan) * height,
    raw: p,
  }))
  const polylinePoints = pts.map((c) => `${c.x.toFixed(2)},${c.y.toFixed(2)}`).join(' ')

  if (!showHover) {
    return (
      <svg
        viewBox={`0 0 ${width} ${height}`}
        width="100%"
        height={height}
        preserveAspectRatio="none"
        aria-hidden="true"
        style={{ display: 'block' }}
      >
        <polyline
          points={polylinePoints}
          fill="none"
          stroke={stroke}
          strokeWidth={1.25}
          strokeOpacity={0.55}
          vectorEffect="non-scaling-stroke"
        />
      </svg>
    )
  }

  return (
    <InteractiveSparkline
      width={width}
      height={height}
      stroke={stroke}
      pts={pts}
      polylinePoints={polylinePoints}
      formatValue={formatValue}
      label={label}
    />
  )
}

type InteractiveProps = {
  width: number
  height: number
  stroke: string
  pts: { x: number; y: number; raw: Point }[]
  polylinePoints: string
  formatValue?: (value: number) => string
  label: string
}

function InteractiveSparkline({ width, height, stroke, pts, polylinePoints, formatValue, label }: InteractiveProps) {
  const svgRef = React.useRef<SVGSVGElement>(null)
  const cachedRectRef = React.useRef<DOMRect | null>(null)
  const [hoverIdx, setHoverIdx] = React.useState<number | null>(null)
  const fmt = useDateFormat()

  // Reset hover when the upstream data changes. Polling on /admin/trends
  // returns same-length rolling windows (60s cadence) — checking length
  // alone misses the case where the underlying ts/value tuples shift by
  // one. Bracket the array by first + last ts so any cross-tick refetch
  // clears the stale index without churn on identical refetches.
  const firstTs = pts[0]?.raw.ts
  const lastTs = pts[pts.length - 1]?.raw.ts
  React.useEffect(() => {
    setHoverIdx(null)
  }, [pts.length, firstTs, lastTs])

  // Invalidate the cached SVG bounding box on window resize so a card
  // that's resized (e.g. the user opens DevTools, rotates a tablet, or
  // the grid reflows) starts hit-testing against the new geometry.
  React.useEffect(() => {
    const invalidate = () => {
      cachedRectRef.current = null
    }
    window.addEventListener('resize', invalidate)
    return () => window.removeEventListener('resize', invalidate)
  }, [])

  const updateHover = React.useCallback(
    (clientX: number) => {
      const el = svgRef.current
      if (!el) return
      let rect = cachedRectRef.current
      if (!rect) {
        rect = el.getBoundingClientRect()
        cachedRectRef.current = rect
      }
      if (rect.width <= 0) return
      const ratio = (clientX - rect.left) / rect.width
      const idx = Math.max(0, Math.min(pts.length - 1, Math.round(ratio * (pts.length - 1))))
      // Only fire setState when crossing a point boundary — pointermove
      // fires hundreds of times per second on a fast move.
      setHoverIdx((prev) => (prev === idx ? prev : idx))
    },
    [pts.length],
  )

  const onPointerEnter = React.useCallback((e: React.PointerEvent<SVGSVGElement>) => {
    // Refresh the cached rect each enter — the chart may have shifted
    // since the last enter (scroll, tab show, etc).
    cachedRectRef.current = e.currentTarget.getBoundingClientRect()
  }, [])

  const onPointerMove = React.useCallback((e: React.PointerEvent<SVGSVGElement>) => updateHover(e.clientX), [updateHover])

  const onPointerDown = React.useCallback(
    (e: React.PointerEvent<SVGSVGElement>) => {
      // Refresh the cached rect for touch (the first event may be a tap
      // with no preceding enter) before mapping x to a point index.
      cachedRectRef.current = e.currentTarget.getBoundingClientRect()
      updateHover(e.clientX)
    },
    [updateHover],
  )

  const onPointerLeave = React.useCallback((e: React.PointerEvent<SVGSVGElement>) => {
    // Suppress leave for touch / pen — those don't have a true "hover"
    // and `pointerleave` fires immediately when the finger lifts. Touch
    // dismissal happens via the document-level handler below.
    if (e.pointerType === 'touch' || e.pointerType === 'pen') return
    setHoverIdx(null)
  }, [])

  const onPointerCancel = React.useCallback(() => setHoverIdx(null), [])

  // For touch users: once a tap pins the tooltip, dismiss it on the
  // next pointerdown anywhere outside this SVG.
  React.useEffect(() => {
    if (hoverIdx === null) return
    const onDocPointerDown = (e: PointerEvent) => {
      if (e.pointerType !== 'touch' && e.pointerType !== 'pen') return
      const el = svgRef.current
      if (el && e.target instanceof Node && el.contains(e.target)) return
      setHoverIdx(null)
    }
    document.addEventListener('pointerdown', onDocPointerDown)
    return () => document.removeEventListener('pointerdown', onDocPointerDown)
  }, [hoverIdx])

  const hovered = hoverIdx !== null ? pts[hoverIdx] : null
  const latest = pts[pts.length - 1]
  const latestText = formatValue ? formatValue(latest.raw.value) : latest.raw.value.toFixed(2)

  // Memoise the today/not-today format pattern — re-run only when the
  // hovered ts changes (not on every pointermove that crosses a sample).
  const tsPattern = React.useMemo(() => {
    if (!hovered) return 'HH:mm'
    const today = fmt.format(new Date(), 'yyyy-MM-dd')
    return fmt.format(hovered.raw.ts, 'yyyy-MM-dd') === today ? 'HH:mm' : 'MMM d, HH:mm'
  }, [hovered, fmt])

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
        <polyline
          points={polylinePoints}
          fill="none"
          stroke={stroke}
          strokeWidth={1.25}
          strokeOpacity={0.55}
          vectorEffect="non-scaling-stroke"
        />
        {hovered && (
          // Theme-aware guide line via the muted-foreground design
          // token. currentColor would inherit the parent stat-card's
          // tone class (red-500/amber-500/etc) which can render
          // invisible against a tone-coloured background.
          <line
            x1={hovered.x}
            x2={hovered.x}
            y1={0}
            y2={height}
            stroke="hsl(var(--muted-foreground))"
            strokeOpacity={0.55}
            strokeWidth={0.5}
            vectorEffect="non-scaling-stroke"
          />
        )}
        {/* Transparent capture surface so pointer events fire across the
            whole chart, not only on the 1.25px stroke. */}
        <rect x={0} y={0} width={width} height={height} fill="transparent" />
      </svg>

      {/* HTML overlay marker — keeps a true circle independent of the
          SVG's non-uniform scaling (`preserveAspectRatio="none"` would
          stretch an SVG <circle> into a wide ellipse on a wide card). */}
      {hovered && (
        <span
          aria-hidden
          style={{
            position: 'absolute',
            left: `${(hovered.x / width) * 100}%`,
            top: `${(hovered.y / height) * 100}%`,
            width: 6,
            height: 6,
            marginLeft: -3,
            marginTop: -3,
            borderRadius: '50%',
            background: stroke,
            pointerEvents: 'none',
          }}
        />
      )}

      {/* Moving anchor for the tooltip — invisible, repositioned to the
          hovered data point. Base UI's Positioner places the popup
          relative to this element and handles flip/shift at viewport
          edges automatically via Portal. */}
      <Tooltip
        open={hovered !== null}
        onOpenChange={(open) => {
          if (!open) setHoverIdx(null)
        }}
      >
        <TooltipTrigger
          render={
            <span
              aria-hidden
              style={{
                position: 'absolute',
                left: hovered ? `${(hovered.x / width) * 100}%` : 0,
                top: hovered ? `${(hovered.y / height) * 100}%` : 0,
                width: 1,
                height: 1,
                pointerEvents: 'none',
              }}
            />
          }
        />
        {hovered && (
          <TooltipContent side="top" sideOffset={8} className="text-xs max-w-[200px] p-2 space-y-1">
            <div className="font-mono tabular-nums">
              {formatValue ? formatValue(hovered.raw.value) : hovered.raw.value.toFixed(2)}
            </div>
            <div className="text-muted-foreground">{fmt.format(hovered.raw.ts, tsPattern)}</div>
          </TooltipContent>
        )}
      </Tooltip>
    </div>
  )
}

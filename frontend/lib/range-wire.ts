// Single source of truth for the time-range wire contract shared by the
// SSR-seedable analytics pages (dashboard, origin, performance, security,
// network).
//
// There are two ways the FE expresses the scan window to the backend, and they
// MUST agree with what the chart x-axis displays (makeTimeXAxis hard-clamps the
// x-axis to startTime/endTime — if the scanned window is wider, the y-axis
// auto-scales to off-screen data and squashes the visible bars):
//
//   1. Token mode — a server-reproducible relative token ('24h'/'7d'/'30d').
//      Used for an explicit quick-preset pill (filterStore.relativeRange) AND
//      for the cold-load / reset default (isAutoRange), where the displayed
//      window is the rolling 24h store default. The backend resolves the scan
//      window from (range_token, anchor); the token + quantized anchor are
//      server-reproducible, which is what lets the SSR seed key byte-match the
//      client's first-paint key (lib/ssr/*).
//
//   2. Absolute mode — explicit start/end bounds. Used when the user picked a
//      custom range (date-picker Apply, chart zoom, saved view): relativeRange
//      is null AND isAutoRange is false. The backend's _clamp_window falls back
//      to the FE-supplied start/end whenever range_token is absent, so the scan
//      matches exactly what the chart displays. (SSR never hits this branch — it
//      only ever seeds the cold-load default, which is token mode.)
//
// isAutoRange is the discriminator: it's true only in the default/auto/reset
// state and false the moment the user makes an explicit selection (preset OR
// custom). So: a preset → its label; auto/default → '24h'; custom absolute → no
// token (send the bounds).

/** The cold-load / reset default token. Matches the 24h store-default display
 *  window (filterStore startTime/endTime = [now-24h, now]). */
export const DEFAULT_RANGE_TOKEN = '24h'

export interface RangeWireArgs {
  /** filterStore.relativeRange — the active quick-preset label, or null. */
  relativeRange: string | null
  /** filterStore.isAutoRange — true in the default/auto/reset state. */
  isAutoRange: boolean
  /** Display window start (drives the absolute-mode bounds + cache key). */
  startTime: string | null
  /** Display window end. */
  endTime: string | null
  /** Quantized mount anchor (token mode only; ignored in absolute mode). */
  anchor: string
}

export interface RangeWire {
  /** The wire token, or null when sending absolute bounds. */
  rangeToken: string | null
  /** Value for the query key's range slot. The server-reproducible token in
   *  token mode; a stable "abs:<start>|<end>" identity in absolute mode so two
   *  distinct custom ranges never collide on one cache entry. */
  rangeKey: string
  /** Body fragment spread into the POST. Token mode → {range_token, anchor};
   *  absolute mode → {start_time, end_time} (no token → backend uses these). */
  rangeBody:
    | { range_token: string; anchor: string }
    | { start_time: string | null; end_time: string | null }
}

/**
 * Resolve the time-range wire contract from the filter-store state + display
 * window. Pure — same inputs give the same output; safe to call in render.
 */
export function resolveRangeWire({
  relativeRange,
  isAutoRange,
  startTime,
  endTime,
  anchor,
}: RangeWireArgs): RangeWire {
  const rangeToken = relativeRange ?? (isAutoRange ? DEFAULT_RANGE_TOKEN : null)
  if (rangeToken !== null) {
    return { rangeToken, rangeKey: rangeToken, rangeBody: { range_token: rangeToken, anchor } }
  }
  return {
    rangeToken: null,
    rangeKey: `abs:${startTime ?? ''}|${endTime ?? ''}`,
    rangeBody: { start_time: startTime, end_time: endTime },
  }
}

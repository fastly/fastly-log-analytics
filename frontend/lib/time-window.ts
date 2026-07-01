// Client-side `(rangeToken, anchor) -> (start, end)` window resolver.
//
// This is the FRONTEND port of `backend/utils/time_window.py`. It MUST stay a
// byte-for-byte mirror of that module's `quantize_anchor` / `_pick_auto_token`
// / `resolve_window` so the SSR seed key and the client first-paint key land on
// the identical `(range_token, quantized_anchor)` pair — and so the absolute
// bounds the FE sends as a legacy fallback match what the backend would resolve.
//
// WHY THIS EXISTS (the network 30d analyst cliff): the network response memo is
// anchor-faithful — it keyed on the rolling minute-bucketed RESOLVED bounds, so
// an analyst loading across rolling minutes got a fresh key every minute and
// recomputed the full ~26s 30d pipeline. A relative token + a QUANTIZED anchor
// is server-reproducible and STABLE within the quantum, so the key holds still
// long enough for the memo to actually serve.
//
// PARITY CONTRACT (see __tests__/lib/time-window.parity.test.ts): a fixed table
// of (token, anchor, earliest) → (start, end) is asserted against BOTH this
// resolver and the hard-coded values the Python `resolve_window` produces. Any
// drift in the epoch floor, the auto bands, or the ISO-Z formatting fails it.
//
// SECURITY: this module ONLY computes a window from a token + anchor. It does
// NOT read the analyst clamp and does NOT widen anything — the backend re-runs
// the resolved bounds through `ctx.clamp` so the invite ceiling is enforced
// regardless of which token an analyst supplies. An analyst can never widen past
// their invite by choosing "30d".

import { historyHoursFromExtents } from '@/lib/insights-defaults'

// Anchor quantization granularity (seconds). 60s aligns with the 30s
// response-memo TTL — an anchor stable for 60s keeps a memo entry reachable
// across the rolling-minute reloads that caused the cliff. Mirrors
// `ANCHOR_QUANTUM_SECONDS` in time_window.py; change in lockstep.
export const ANCHOR_QUANTUM_SECONDS = 60

// Fixed relative tokens → lookback delta in milliseconds. Mirrors
// `_FIXED_TOKEN_DELTAS` (timedelta) in time_window.py. "auto" is NOT here — it
// is resolved adaptively from log extents (see `pickAutoToken`).
export const FIXED_TOKEN_DELTAS: Record<string, number> = {
  '24h': 24 * 60 * 60 * 1000,
  '7d': 7 * 24 * 60 * 60 * 1000,
  '30d': 30 * 24 * 60 * 60 * 1000,
}

// The full accepted token vocabulary (fixed deltas + the adaptive sentinel).
// Mirrors `VALID_RANGE_TOKENS` in time_window.py.
export const VALID_RANGE_TOKENS: ReadonlySet<string> = new Set([
  ...Object.keys(FIXED_TOKEN_DELTAS),
  'auto',
])

/** True when `token` is a recognized relative-range token. Mirrors
 *  `is_valid_range_token`: a None/empty/unknown token means "no keyed path"
 *  (caller falls back to the legacy absolute-bounds branch) — never throws. */
export function isValidRangeToken(token: string | null | undefined): boolean {
  return token != null && VALID_RANGE_TOKENS.has(token)
}

// ISO-Z formatter matching backend `iso_z`: `%Y-%m-%dT%H:%M:%SZ` — UTC, NO
// milliseconds (Date.prototype.toISOString emits ".sssZ", which would NOT
// byte-match the Python output). The anchor is always quantized to whole
// seconds before formatting, so dropping the millis fraction is loss-free here.
function isoZ(ms: number): string {
  return new Date(ms).toISOString().replace(/\.\d{3}Z$/, 'Z')
}

/**
 * Floor an anchor instant to the `ANCHOR_QUANTUM_SECONDS` grid → ISO-Z.
 *
 * Mirrors `quantize_anchor` in time_window.py:
 *   - a missing / unparseable anchor falls back to `now` (so the keyed path is
 *     reachable even if the client omits the anchor),
 *   - the floor is `epoch - (epoch % quantum)` on whole SECONDS (matching
 *     Python's `int(dt.timestamp())`, which truncates toward zero), so two
 *     calls within the same 60s window produce the identical quantized anchor
 *     (and thus the identical cache key).
 *
 * @param iso  Anchor instant (ISO-8601 or YYYY-MM-DD). Optional.
 * @param now  Fallback / parse base. Defaults to `new Date()`.
 */
export function quantizeAnchor(iso?: string | null, now: Date = new Date()): string {
  let ms: number
  if (iso) {
    // Mirror parse_iso_utc: a date-only extent ("2026-06-15") is UTC
    // start-of-day. Date.parse handles both ISO-8601 and YYYY-MM-DD (the
    // latter as UTC midnight per the spec), matching the Python widening.
    const parsed = Date.parse(iso)
    ms = Number.isFinite(parsed) ? parsed : now.getTime()
  } else {
    ms = now.getTime()
  }
  // Truncate to whole seconds (Python int(timestamp) truncates toward zero;
  // Math.floor matches for the non-negative epoch values we deal with), then
  // floor to the quantum grid.
  const epoch = Math.floor(ms / 1000)
  const floored = epoch - (epoch % ANCHOR_QUANTUM_SECONDS)
  return isoZ(floored * 1000)
}

/**
 * Resolve "auto" to a concrete fixed token from the service's history.
 *
 * Mirrors `_pick_auto_token` in time_window.py — half-open bands, higher bucket
 * on the boundary:
 *   <7d  history  → "24h"   (new service: small, fast window)
 *   <30d history  → "7d"    (a week or two of data: week view)
 *   >=30d history → "30d"   (mature service: full month)
 * No usable extent → "7d" (the "we don't know yet" middle default; never pays
 * the 30d cost on a cold/unknown service).
 *
 * Uses `historyHoursFromExtents` (lib/insights-defaults.ts) — the SAME helper
 * the Python `_history_hours_from_extents` is a port of — so both sides compute
 * the identical history span over the same `earliest_log_at`.
 */
export function pickAutoToken(
  earliestLogAt: string | null | undefined,
  now: Date = new Date(),
): string {
  const historyHours = historyHoursFromExtents(earliestLogAt, now.getTime())
  if (historyHours == null) return '7d'
  if (historyHours < 7 * 24) return '24h'
  if (historyHours < 30 * 24) return '7d'
  return '30d'
}

export interface ResolveWindowExtents {
  /** Service `earliest_log_at` — drives the "auto" adaptive default. */
  earliestLogAt?: string | null
  /** Resolution base for the quantized anchor + the auto bands. */
  now?: Date
}

/**
 * Resolve `(rangeToken, anchor) -> { start, end }` deterministically.
 *
 * Mirrors `resolve_window` in time_window.py:
 *   - fixed tokens ("24h"/"7d"/"30d") → `[quantizedAnchor - delta, quantizedAnchor]`,
 *   - "auto" → `pickAutoToken(earliestLogAt)`, then the same shape.
 * The anchor used for the math is the QUANTIZED anchor (floored to the quantum)
 * so the bounds are stable within the quantum — the property that stabilizes the
 * cache key. Both returned strings are ISO-Z (no millis).
 *
 * Throws for an unrecognized token (callers gate on `isValidRangeToken` first),
 * matching the Python `ValueError` contract — a clear programming-error signal
 * rather than a silent wrong window.
 *
 * SECURITY: the returned bounds are the SCAN INTENT before the analyst clamp.
 * The backend MUST still pass them through `ctx.clamp`; this function never sees
 * the invite ceiling and must not be trusted to enforce it.
 */
export function resolveWindow(
  rangeToken: string | null | undefined,
  anchor: string | null | undefined,
  { earliestLogAt = null, now = new Date() }: ResolveWindowExtents = {},
): { start: string; end: string } {
  if (!isValidRangeToken(rangeToken)) {
    throw new Error(`unrecognized range_token: ${String(rangeToken)}`)
  }

  const quantized = quantizeAnchor(anchor, now)
  const anchorMs = Date.parse(quantized)

  const token =
    rangeToken === 'auto' ? pickAutoToken(earliestLogAt, now) : (rangeToken as string)
  const delta = FIXED_TOKEN_DELTAS[token]

  return { start: isoZ(anchorMs - delta), end: isoZ(anchorMs) }
}

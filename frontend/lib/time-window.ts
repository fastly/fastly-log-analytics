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

// Anchor quantization granularity (seconds). 60s aligns with the 30s
// response-memo TTL — an anchor stable for 60s keeps a memo entry reachable
// across the rolling-minute reloads that caused the cliff. Mirrors
// `ANCHOR_QUANTUM_SECONDS` in time_window.py; change in lockstep.
export const ANCHOR_QUANTUM_SECONDS = 60

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

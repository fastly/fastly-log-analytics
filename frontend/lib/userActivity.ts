'use client'

/**
 * Single source of truth for "did the human genuinely interact recently".
 *
 * Drives the ``X-User-Active`` request header (lib/api.ts + lib/analystFetch.ts).
 * The backend resets the analyst idle timeout only on genuine user activity:
 * automated react-query refetches on a FOREGROUND tab (e.g. the ~12s
 * /api/dashboard/bundle the badge stream invalidates as logs ingest) are
 * indistinguishable from a user click on the wire, so the client stamps
 * ``X-User-Active: 0`` when no real gesture happened within ACTIVE_WINDOW_MS.
 * That lets the 2h idle cap fire on a visible-but-idle tab. Absent or "1"
 * still bumps the timer (back-compat), so an old bundle behaves as before.
 *
 * Gesture set mirrors what the heartbeat historically tracked (mousemove +
 * keydown) plus pointer/touch/wheel so taps and wheel-scroll reading also
 * count. mousemove/wheel are throttled — they fire constantly and we only
 * need ~second resolution.
 */

export const ACTIVE_WINDOW_MS = 120_000
const MOUSE_THROTTLE_MS = 1_000

// Initial value: a fresh page load is genuine activity (the user just
// navigated here), so count as active for the first window. On the server
// there is no gesture concept — see isUserActive's SSR branch.
let lastInteractionAt = typeof window !== 'undefined' ? Date.now() : 0
let lastThrottledBump = 0
let listenersAttached = false

function bump() {
  lastInteractionAt = Date.now()
}

function bumpThrottled() {
  const now = Date.now()
  if (now - lastThrottledBump < MOUSE_THROTTLE_MS) return
  lastThrottledBump = now
  lastInteractionAt = now
}

function ensureListeners() {
  if (listenersAttached || typeof window === 'undefined') return
  listenersAttached = true
  window.addEventListener('mousemove', bumpThrottled, { passive: true })
  window.addEventListener('wheel', bumpThrottled, { passive: true })
  window.addEventListener('keydown', bump)
  window.addEventListener('pointerdown', bump, { passive: true })
  window.addEventListener('touchstart', bump, { passive: true })
}

// Attach on import in the browser so the tracker is live before the first
// request fires (the openapi-fetch middleware reads it synchronously).
ensureListeners()

export function msSinceLastInteraction(): number {
  if (typeof window === 'undefined') return 0
  ensureListeners()
  return Date.now() - lastInteractionAt
}

/**
 * True if a genuine user gesture happened within ``windowMs``. SSR / non-browser
 * returns true: the X-User-Active header then reads "1" (absent-or-1 → backend
 * touches), preserving today's behavior; SSR fetches are admin-classified
 * server-side and never touch a session anyway.
 */
export function isUserActive(windowMs: number = ACTIVE_WINDOW_MS): boolean {
  if (typeof window === 'undefined') return true
  ensureListeners()
  return Date.now() - lastInteractionAt < windowMs
}

/** Test seam — module state is global, reset it between tests. */
export function __resetForTests(at: number = 0): void {
  lastInteractionAt = at
  lastThrottledBump = 0
}

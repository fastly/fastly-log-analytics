'use client'

import { useSyncExternalStore } from 'react'

/**
 * Single global "now" tick, updated every 1000 ms.
 *
 * Subscribers re-render once per second when consuming this hook; the
 * setInterval itself runs ONCE per process (registered on first
 * subscribe, torn down on last unsubscribe). Drop-in replacement for
 * the per-component pattern:
 *
 *     const [, setTick] = useState(0)
 *     useEffect(() => {
 *       const id = setInterval(() => setTick(t => t + 1), 1000)
 *       return () => clearInterval(id)
 *     }, [])
 *
 * Why it matters: SystemJobBox + SyncStatusBadge + similar countdown
 * widgets each used to register their own setInterval. On the admin
 * page that meant 10+ independent timers all firing on the same
 * 1-second boundary, each triggering a setState + re-render. The main
 * thread was busy enough that clicks queued behind the cascade,
 * making the page feel "stuck for 2 seconds" before navigation.
 *
 * useSyncExternalStore is the right primitive here: a single source
 * of truth, consistent across the tree, no re-render storms.
 */

let _now = Date.now()
const _listeners = new Set<() => void>()
let _interval: ReturnType<typeof setInterval> | null = null

function _subscribe(cb: () => void): () => void {
  _listeners.add(cb)
  if (_interval === null) {
    _interval = setInterval(() => {
      _now = Date.now()
      for (const l of _listeners) l()
    }, 1000)
  }
  return () => {
    _listeners.delete(cb)
    if (_listeners.size === 0 && _interval !== null) {
      clearInterval(_interval)
      _interval = null
    }
  }
}

function _getSnapshot(): number {
  return _now
}

// Server-side snapshot for SSR safety. Always returns the same value
// per request so React's hydration doesn't see a mismatch between
// server and client first paint.
function _getServerSnapshot(): number {
  return 0
}

/**
 * Returns Date.now()-equivalent value that updates once per second.
 * All consumers share a single underlying timer.
 */
export function useNowMs(): number {
  return useSyncExternalStore(_subscribe, _getSnapshot, _getServerSnapshot)
}

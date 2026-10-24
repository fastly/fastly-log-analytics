'use client'

import * as React from 'react'
import { useRouter } from 'next/navigation'
import { analystFetch } from '@/lib/analystFetch'
import { msSinceLastInteraction } from '@/lib/userActivity'

interface Options {
  enabled: boolean
  idleAfterMs?: number
  intervalMs?: number
  failuresBeforeOverlay?: number
}

const DEFAULT_IDLE = 15_000
const DEFAULT_INTERVAL = 30_000
const DEFAULT_FAILURES = 2

export function useAnalystHeartbeat({
  enabled,
  idleAfterMs = DEFAULT_IDLE,
  intervalMs = DEFAULT_INTERVAL,
  failuresBeforeOverlay = DEFAULT_FAILURES,
}: Options) {
  const router = useRouter()
  const [isTerminated, setIsTerminated] = React.useState(false)
  const [disconnected, setDisconnected] = React.useState(false)

  React.useEffect(() => {
    if (!enabled || isTerminated) return
    if (typeof window === 'undefined') return

    let timer: ReturnType<typeof setInterval> | null = null
    let cancelled = false
    let consecutiveFailures = 0

    const tick = async () => {
      if (cancelled || isTerminated) return
      if (document.hidden) return
      if (msSinceLastInteraction() < idleAfterMs) return
      try {
        // Raw fetch with a relative URL: in remote-analyst mode the request
        // flows through the Next.js proxy that the tunnel exposes. The typed
        // client routes direct to 127.0.0.1:8000, which is unreachable from
        // the analyst's browser.
        // 10s timeout — heartbeats are frequent and must not back up if
        // the network is wedged. A stuck fetch would still leave the
        // setInterval queueing more, so a tight bound is load-bearing.
        const res = await analystFetch('/api/share/heartbeat', {}, 10_000)
        if (res.status === 401 || res.status === 403) {
          cancelled = true
          if (timer) clearInterval(timer)
          setIsTerminated(true)
          router.replace('/share-login')
          return
        }
        if (!res.ok) {
          consecutiveFailures++
        } else {
          consecutiveFailures = 0
          setDisconnected(false)
        }
      } catch {
        consecutiveFailures++
      }
      if (consecutiveFailures >= failuresBeforeOverlay) {
        setDisconnected(true)
      }
    }

    timer = setInterval(tick, intervalMs)

    return () => {
      cancelled = true
      if (timer) clearInterval(timer)
    }
  }, [enabled, isTerminated, idleAfterMs, intervalMs, failuresBeforeOverlay, router])

  return { disconnected }
}

/**
 * Self-service logout for remote analysts.
 *
 * POSTs ``/api/share/logout`` which evicts the analyst's server-side session
 * and clears the ``analyst_session_id`` / ``analyst_pending_session_id``
 * cookies, then hard-navigates to ``/share-login``.
 */
export function useAnalystLogout(): { logout: () => Promise<void>; isLoggingOut: boolean } {
  const [isLoggingOut, setIsLoggingOut] = React.useState(false)

  const logout = React.useCallback(async () => {
    if (isLoggingOut) return
    setIsLoggingOut(true)
    try {
      await analystFetch('/api/share/logout', { method: 'POST' }, 10_000)
    } catch {
      // Swallow — redirect anyway (see docstring).
    } finally {
      if (typeof window !== 'undefined') {
        window.location.assign('/share-login')
      } else {
        setIsLoggingOut(false)
      }
    }
  }, [isLoggingOut])

  return { logout, isLoggingOut }
}

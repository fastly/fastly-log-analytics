'use client'

import * as React from 'react'
import { useRouter } from 'next/navigation'
import { getPageLoadId } from '@/lib/api-client'
import { isUserActive, msSinceLastInteraction } from '@/lib/userActivity'

/**
 * fetch() wrapper that aborts after `timeoutMs` (default 30s).
 *
 * Why: browser `fetch()` has no implicit timeout — a hung network or
 * slow upstream will keep the request pending until the user closes the
 * tab. The share-login flow + heartbeat hooks need a UX bound so a
 * stuck request surfaces as an error instead of an infinite spinner.
 *
 * The default 30s sits below Caddy's 120s `response_header_timeout`
 * (the upper bound on the wire); shrink per call for snappier UIs
 * (e.g. an autocomplete fetch can use 5s safely).
 */
export async function fetchWithTimeout(
  url: string,
  init: RequestInit = {},
  timeoutMs = 30_000,
): Promise<Response> {
  const ctrl = new AbortController()
  const timer = setTimeout(() => ctrl.abort(), timeoutMs)
  const callerSignal = init.signal
  if (callerSignal) {
    if (callerSignal.aborted) {
      ctrl.abort()
    } else {
      callerSignal.addEventListener('abort', () => ctrl.abort(), { once: true })
    }
  }
  try {
    return await fetch(url, { ...init, signal: ctrl.signal })
  } finally {
    clearTimeout(timer)
  }
}

/**
 * Shared transport for remote-analyst requests (heartbeat, logout,
 * share-login, TOS acknowledge).
 *
 * Every analyst-facing call hits a RELATIVE url so the request flows through
 * the Next.js proxy the tunnel exposes — the typed `client` routes direct to
 * 127.0.0.1:8000, unreachable from the analyst's browser — and carries
 * `credentials: 'include'` plus the `X-Remote-Analyst` hint header. This wraps
 * that envelope so it lives in one place; callers supply only what differs
 * (method, body, extra headers like Content-Type).
 *
 * Defaults to `fetchWithTimeout`'s 30s bound; the heartbeat/logout hooks pass
 * an explicit 10s since those fire frequently and must not back up. A caller's
 * own `headers` are merged on top (none currently override `X-Remote-Analyst`).
 */
export function analystFetch(path: string, init: RequestInit = {}, timeoutMs?: number): Promise<Response> {
  const loadId = getPageLoadId()
  return fetchWithTimeout(
    path,
    {
      ...init,
      credentials: 'include',
      // X-User-Active: idle-timeout activity signal (see lib/userActivity).
      // "0" = no genuine gesture recently → backend must not reset the idle
      // clock. Caller headers win on collision.
      headers: {
        'X-Remote-Analyst': '1',
        'X-User-Active': isUserActive() ? '1' : '0',
        ...(loadId ? { 'X-Page-Load-ID': loadId } : {}),
        ...init.headers,
      },
    },
    timeoutMs,
  )
}

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

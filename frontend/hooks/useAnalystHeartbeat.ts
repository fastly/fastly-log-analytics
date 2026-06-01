'use client'

import * as React from 'react'
import { useRouter } from 'next/navigation'

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
  const [disconnected, setDisconnected] = React.useState(false)

  React.useEffect(() => {
    if (!enabled) return
    if (typeof window === 'undefined') return

    let lastActivity = Date.now()
    let timer: ReturnType<typeof setInterval> | null = null
    let cancelled = false
    let consecutiveFailures = 0

    const bump = () => {
      lastActivity = Date.now()
    }

    const tick = async () => {
      if (cancelled) return
      if (document.hidden) return
      if (Date.now() - lastActivity < idleAfterMs) return
      try {
        // Raw fetch with a relative URL: in remote-analyst mode the request
        // flows through the Next.js proxy that the tunnel exposes. The typed
        // client routes direct to 127.0.0.1:8000, which is unreachable from
        // the analyst's browser.
        const res = await fetch('/api/share/heartbeat', {
          credentials: 'include',
          headers: { 'X-Remote-Analyst': '1' },
        })
        if (res.status === 401 || res.status === 403) {
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
    window.addEventListener('mousemove', bump, { passive: true })
    window.addEventListener('keydown', bump)

    return () => {
      cancelled = true
      if (timer) clearInterval(timer)
      window.removeEventListener('mousemove', bump)
      window.removeEventListener('keydown', bump)
    }
  }, [enabled, idleAfterMs, intervalMs, failuresBeforeOverlay, router])

  return { disconnected }
}

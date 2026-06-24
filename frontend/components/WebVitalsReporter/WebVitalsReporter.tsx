'use client'

import * as React from 'react'
import { useReportWebVitals } from 'next/web-vitals'
import { getApiBase } from '@/lib/api'
import { usePathname } from 'next/navigation'
import { useBootstrap } from '@/hooks/useBootstrap'

/**
 * Wires Next.js's web-vitals callback to POST /api/web-vitals.
 *
 * Each metric (LCP / INP / CLS / FCP / TTFB) fires once per page load
 * (CLS / INP keep updating mid-life; the SDK delivers deltas under the
 * same id). We send via navigator.sendBeacon when available so the
 * request survives a fast unload; the fetch fallback covers Safari
 * and older browsers that throttle sendBeacon during bfcache restore.
 *
 * Backend logs each event via structlog — no SQLite write, no schema
 * migration. Slice by name / rating / pathname / cohort in log
 * aggregation (or wire a dashboard panel later).
 */
export function WebVitalsReporter() {
  const pathname = usePathname()
  const { data: bootstrap } = useBootstrap()

  // Collection is opt-in on the backend (WEB_VITALS_COLLECT), mirrored into
  // bootstrap.settings.web_vitals_enabled. When off we skip the POST entirely
  // so a disabled deployment sees zero web-vitals traffic — no beacons, no
  // access-log lines. Read through a ref so the captured useReportWebVitals
  // callback always sees the latest value: bootstrap resolves a tick after
  // first paint, and the callback is registered once on mount.
  const enabledRef = React.useRef(false)
  enabledRef.current = Boolean(bootstrap?.settings?.web_vitals_enabled)

  const disabledRef = React.useRef(false)
  useReportWebVitals((metric) => {
    if (disabledRef.current) return
    if (!enabledRef.current) return
    const body = {
      id: metric.id,
      name: metric.name,
      value: metric.value,
      rating: metric.rating,
      pathname,
      navigation_type: metric.navigationType,
      delta: metric.delta,
    }
    const json = JSON.stringify(body)
    const url = `${getApiBase()}/api/web-vitals`

    try {
      // sendBeacon is fire-and-forget and survives the unload that
      // hides the page right after the final metric fires.
      if (typeof navigator !== 'undefined' && 'sendBeacon' in navigator) {
        const blob = new Blob([json], { type: 'application/json' })
        const ok = navigator.sendBeacon(url, blob)
        if (ok) return
      }
      // Fallback for browsers that returned false from sendBeacon
      // (quota / unsupported content-type / strict-mode bfcache).
      void fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: json,
        // Keep alive across unload — chrome / firefox honor this; safari
        // best-effort.
        keepalive: true,
      }).then((res) => {
        if (res.status === 401 || res.status === 403) {
          disabledRef.current = true
        }
      })
    } catch {
      // Telemetry is fire-and-forget; never let a failed report break
      // the page that triggered it.
    }
  })

  return null
}

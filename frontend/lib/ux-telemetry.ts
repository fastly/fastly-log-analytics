import { getApiBase } from '@/lib/api'

// Fire-and-forget UX-event emitter — POSTs to /api/ux-events via
// sendBeacon when available (so the request survives a page nav that
// fires immediately after the event), falling back to keepalive fetch.
// Telemetry must never break the page that produced it; every failure
// path is swallowed.
//
// The companion of WebVitalsReporter's beacon pattern; sibling endpoint
// shape is documented in backend/routers/ux_events.py.

export interface UxEvent {
  event: string
  pathname?: string
  /** Disambiguates multiple instances of the same component on a page
   *  (e.g. multiple DataTables on /sessions). Pass the table caption /
   *  title where available; the column-reorder lens uses it to slice
   *  per-table reorder counts. */
  component_id?: string
  details?: Record<string, unknown>
}

export function reportUxEvent(evt: UxEvent): void {
  if (typeof window === 'undefined') return
  try {
    const pathname = evt.pathname ?? window.location.pathname
    const body = JSON.stringify({
      event: evt.event,
      pathname,
      component_id: evt.component_id,
      details: evt.details ?? {},
    })
    const url = `${getApiBase()}/api/ux-events`

    if (typeof navigator !== 'undefined' && 'sendBeacon' in navigator) {
      const blob = new Blob([body], { type: 'application/json' })
      if (navigator.sendBeacon(url, blob)) return
    }
    void fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
      keepalive: true,
    })
  } catch {
    // Swallow — telemetry never breaks the page.
  }
}

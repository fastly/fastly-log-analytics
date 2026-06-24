'use client'

import * as React from 'react'

import { analystFetch } from '@/lib/analystFetch'

/**
 * Self-service logout for remote analysts.
 *
 * POSTs ``/api/share/logout`` which evicts the analyst's server-side session
 * and clears the ``analyst_session_id`` / ``analyst_pending_session_id``
 * cookies, then hard-navigates to ``/share-login``.
 *
 * Transport mirrors ``useAnalystHeartbeat`` / ``ShareLoginForm``: a raw
 * ``fetchWithTimeout`` with a RELATIVE url (so the request flows through the
 * Next.js proxy the tunnel exposes — the typed ``client`` routes direct to
 * 127.0.0.1:8000, unreachable from the analyst's browser) plus
 * ``credentials: 'include'`` and the ``X-Remote-Analyst`` hint header.
 *
 * The redirect runs regardless of the request outcome: the cookies are the
 * server's to clear, but a failed/timed-out call must still land the user on
 * the sign-in page rather than leave them on a half-authed view. We use a hard
 * ``window.location.assign`` (not ``router.replace``) so all in-memory React
 * Query caches holding analyst data are dropped on the way out.
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

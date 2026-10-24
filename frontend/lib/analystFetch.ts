import { getPageLoadId } from '@/lib/api-client'
import { isUserActive } from '@/lib/userActivity'

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

import { fetchWithTimeout } from '@/lib/fetchWithTimeout'
import { isUserActive } from '@/lib/userActivity'

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
  return fetchWithTimeout(
    path,
    {
      ...init,
      credentials: 'include',
      // X-User-Active: idle-timeout activity signal (see lib/userActivity).
      // "0" = no genuine gesture recently → backend must not reset the idle
      // clock. Caller headers win on collision.
      headers: { 'X-Remote-Analyst': '1', 'X-User-Active': isUserActive() ? '1' : '0', ...init.headers },
    },
    timeoutMs,
  )
}

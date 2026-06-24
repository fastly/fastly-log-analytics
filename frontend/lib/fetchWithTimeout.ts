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
  // Compose the caller's signal (if any) with our timeout-driven abort
  // so an externally-cancelled request still terminates cleanly.
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

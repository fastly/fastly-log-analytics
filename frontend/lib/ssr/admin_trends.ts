// Server-only SSR fetcher for the /admin/trends page. Pre-fetches the
// default 1-hour metric-history batch so the 7 sparkline cards render
// without paying a client round-trip on cold load.
//
// This is the cleanest data-prefetch on the site: the client's window
// selector defaults to useState('1h') with no searchParams, so the cold
// queryKey is ALWAYS ['admin','metric-history-batch','1h'] and the seed is
// consumed 100% of the time. The endpoint is a tiny indexed SQLite read
// (system_metrics.db, ~10KB, p95 ~10-16ms) — none of the DuckDB / CPU /
// materialize TTFB caveats that block SSR on the analytics pages apply.
//
// Trust topology + Caddy-marker fail-closed defense live in ./_transport.ts
// (shared with alerts/logs/usage_log/tos/bootstrap). /api/admin/* is in the
// analyst-blocked prefix list, so without the Caddy marker the shared helper
// refuses; the analyst branch is sent X-Remote-Analyst:1 with NO admin token
// and gets 401/403 → null → client fetch. injectAdminToken attaches the
// shared secret only on the loopback admin branch.
//
// Failure path: returns null on timeout / non-2xx / malformed body so the
// client useQuery takes over unchanged — never a broken render.

import { parseSsrJson, ssrUpstreamGet } from './_transport'

export async function fetchMetricHistoryBatchServerSide(): Promise<unknown | null> {
  return parseSsrJson(
    await ssrUpstreamGet({
      path: '/api/admin/metric-history/batch?since=1h',
      logPrefix: 'ssr/admin-trends',
      injectAdminToken: true,
    }),
    'ssr/admin-trends',
  )
}

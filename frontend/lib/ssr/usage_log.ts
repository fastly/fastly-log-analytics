// Server-only SSR fetcher for the /admin/usage-log page. Returns null
// on any failure so the calling RSC falls back to the client-side
// useQuery — never let SSR errors propagate into a broken page.
//
// Trust topology + Caddy-marker fail-closed defense live in ./_transport.ts
// (shared with alerts/logs/tos/bootstrap). usage-log is admin-only on the
// wire (`/api/admin/*` is in the analyst-blocked prefix list), so without
// the Caddy marker the shared helper refuses and the page client-fetches.

import { parseSsrJson, ssrUpstreamGet } from './_transport'

export interface UsageLogSsrArgs {
  serviceId: string | undefined
  start: string
  end: string
  pageSize: number
  usageType?: string
  processContext?: string
  operationType?: string
}

export async function fetchUsageLogServerSide(args: UsageLogSsrArgs): Promise<unknown | null> {
  const query = new URLSearchParams({
    service_id: args.serviceId || '',
    start: args.start,
    end: args.end,
    page_size: String(args.pageSize),
  })
  if (args.usageType) query.set('usage_type', args.usageType)
  if (args.processContext) query.set('process_context', args.processContext)
  if (args.operationType) query.set('operation_type', args.operationType)

  return parseSsrJson(
    await ssrUpstreamGet({
      path: `/api/admin/usage-log?${query.toString()}`,
      logPrefix: 'ssr/usage_log',
      injectAdminToken: true,
    }),
    'ssr/usage_log',
  )
}

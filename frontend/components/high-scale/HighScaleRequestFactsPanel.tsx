'use client'

import { AlertCircle, ArrowLeft, ArrowRight, Database } from 'lucide-react'
import { useEffectiveServiceId, useBootstrapResolved } from '@/hooks/useIsDataReady'
import { useTimeRange } from '@/hooks/useTimeRange'
import { useHighScaleRequestFacts } from '@/hooks/useHighScaleRequestFacts'
import { extractApiError } from '@/lib/api'
import { NoServiceSelected } from '@/components/NoServiceSelected'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'

function valueAsString(value: unknown): string {
  if (value === null || value === undefined || value === '') return '—'
  return String(value)
}

function formatTimestamp(value: unknown): string {
  if (typeof value !== 'string') return valueAsString(value)
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? value : parsed.toISOString()
}

function formatFreshness(seconds: number): string {
  if (seconds < 60) return `${Math.round(seconds)}s`
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`
  return `${(seconds / 3600).toFixed(1)}h`
}

function Metadata({ metadata }: { metadata: NonNullable<ReturnType<typeof useHighScaleRequestFacts>['data']>['metadata'] }) {
  return (
    <div className="grid gap-3 sm:grid-cols-3" data-testid="high-scale-metadata">
      <div className="rounded-lg border bg-muted/30 p-3">
        <div className="text-xs uppercase tracking-wide text-muted-foreground">Status</div>
        <div className="mt-1 flex items-center gap-2 font-medium">
          <Badge variant={metadata.status === 'complete' ? 'default' : 'secondary'}>
            {metadata.status}
          </Badge>
          {metadata.exact ? 'Exact' : 'Approximate'}
        </div>
      </div>
      <div className="rounded-lg border bg-muted/30 p-3">
        <div className="text-xs uppercase tracking-wide text-muted-foreground">Freshness</div>
        <div className="mt-1 font-medium">{formatFreshness(metadata.freshness_lag_seconds)} lag</div>
      </div>
      <div className="rounded-lg border bg-muted/30 p-3">
        <div className="text-xs uppercase tracking-wide text-muted-foreground">Coverage</div>
        <div className="mt-1 font-medium">{Math.round(metadata.coverage * 100)}%</div>
      </div>
    </div>
  )
}

export function HighScaleRequestFactsPanel() {
  const serviceId = useEffectiveServiceId()
  const bootstrapResolved = useBootstrapResolved()
  const { startTime, endTime } = useTimeRange()
  const facts = useHighScaleRequestFacts({ serviceId, startTime, endTime })

  if (!bootstrapResolved) {
    return <Skeleton className="h-64 w-full" />
  }

  if (!serviceId) {
    return <NoServiceSelected icon={Database} message="Select a service to query high-scale request facts." />
  }

  if (facts.isLoading) {
    return (
      <Card>
        <CardHeader><CardTitle>Loading request facts…</CardTitle></CardHeader>
        <CardContent className="space-y-3">
          <Skeleton className="h-20 w-full" />
          <Skeleton className="h-48 w-full" />
        </CardContent>
      </Card>
    )
  }

  if (facts.isError) {
    return (
      <Alert variant="destructive" role="alert">
        <AlertCircle className="h-4 w-4" />
        <AlertTitle>High-scale request facts unavailable</AlertTitle>
        <AlertDescription>{extractApiError(facts.error)}</AlertDescription>
      </Alert>
    )
  }

  const response = facts.data
  if (!response) return null

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center justify-between gap-3">
          <span>Request facts</span>
          <span className="text-sm font-normal text-muted-foreground">
            Page {facts.pageIndex + 1}
          </span>
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <Metadata metadata={response.metadata} />
        <div className="overflow-x-auto rounded-lg border">
          <table className="w-full text-sm">
            <caption className="sr-only">High-scale request facts</caption>
            <thead className="bg-muted/50 text-left">
              <tr>
                <th className="px-3 py-2 font-medium">Timestamp</th>
                <th className="px-3 py-2 font-medium">Country</th>
                <th className="px-3 py-2 font-medium">Client IP</th>
                <th className="px-3 py-2 font-medium">URL</th>
              </tr>
            </thead>
            <tbody>
              {response.rows.length === 0 ? (
                <tr>
                  <td colSpan={4} className="px-3 py-8 text-center text-muted-foreground">
                    No request facts in this time range.
                  </td>
                </tr>
              ) : (
                response.rows.map((row) => (
                  <tr key={valueAsString(row.event_id)} className="border-t">
                    <td className="whitespace-nowrap px-3 py-2">{formatTimestamp(row.timestamp)}</td>
                    <td className="px-3 py-2">{valueAsString(row.country)}</td>
                    <td className="px-3 py-2">{valueAsString(row.client_ip)}</td>
                    <td className="max-w-xl truncate px-3 py-2" title={valueAsString(row.url)}>
                      {valueAsString(row.url)}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
        <div className="flex items-center justify-between gap-3">
          <span className="text-xs text-muted-foreground">
            Server-side cursor pagination; access controls and analyst masking are applied by the API.
          </span>
          <div className="flex gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={facts.previousPage}
              disabled={!facts.canPreviousPage || facts.isFetching}
            >
              <ArrowLeft className="mr-1 h-4 w-4" />
              Previous
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={facts.nextPage}
              disabled={!facts.canNextPage || facts.isFetching}
            >
              Next
              <ArrowRight className="ml-1 h-4 w-4" />
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}

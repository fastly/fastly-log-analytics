'use client'

import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'
import { DataTableReadonly } from '@/components/DataTable'
import { AlertCircle, ArrowLeft, ArrowRight, Database, Filter, X } from 'lucide-react'
import { useEffectiveServiceId, useBootstrapResolved } from '@/hooks/useIsDataReady'
import { useTimeRange } from '@/hooks/useReportRangeAnchor'
import { client, extractApiError } from '@/lib/api'
import type { components } from '@/types/api.generated'
import { ELAPSED_FRESHNESS, formatElapsed } from '@/lib/format'
import { NoServiceSelected } from '@/components/NoServiceSelected'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Skeleton } from '@/components/ui/skeleton'

const REQUEST_FACTS_PATH = '/api/high-scale/services/{service_id}/request-facts' as const
const DEFAULT_PAGE_SIZE = 100

type RequestFactsResponse = components['schemas']['HighScaleRequestFactResponse']
type RequestFactsRequest = components['schemas']['HighScaleRequestFactRequest']

interface HighScaleRequestFactsOptions {
  serviceId: string | null | undefined
  startTime: string | null | undefined
  endTime: string | null | undefined
  pageSize?: number
  status?: string | number | null
  domain?: string | null
  pop?: string | null
  country?: string | null
  batchId?: string | null
  emulateStandard?: boolean
}

export function useHighScaleRequestFacts({
  serviceId,
  startTime,
  endTime,
  pageSize = DEFAULT_PAGE_SIZE,
  status,
  domain,
  pop,
  country,
  batchId,
  emulateStandard,
}: HighScaleRequestFactsOptions) {
  const filterKey = `${status ?? ''}|${domain ?? ''}|${pop ?? ''}|${country ?? ''}|${batchId ?? ''}|${emulateStandard ?? ''}`
  const rangeKey = `${serviceId ?? ''}|${startTime ?? ''}|${endTime ?? ''}|${pageSize}|${filterKey}`
  const [pagination, setPagination] = useState<{
    key: string
    pageIndex: number
    cursors: Array<string | null>
  }>({ key: rangeKey, pageIndex: 0, cursors: [null] })
  const activePagination = pagination.key === rangeKey
    ? pagination
    : { key: rangeKey, pageIndex: 0, cursors: [null] }
  const { pageIndex, cursors } = activePagination
  const cursor = cursors[pageIndex] ?? null

  const queryKey = filterKey === '|||||'
    ? ['high-scale', 'request-facts', serviceId, startTime, endTime, pageSize, cursor]
    : ['high-scale', 'request-facts', serviceId, startTime, endTime, pageSize, cursor, filterKey]

  const query = useQuery<RequestFactsResponse, Error>({
    queryKey,
    enabled: Boolean(serviceId && startTime && endTime),
    queryFn: async ({ signal }) => {
      if (!serviceId || !startTime || !endTime) {
        throw new Error('A service and time range are required')
      }

      const body: RequestFactsRequest = {
        start_time: startTime,
        end_time: endTime,
        limit: pageSize,
        cursor,
        emulate_standard: Boolean(emulateStandard),
      }
      if (status !== undefined && status !== null && String(status).trim() !== '') {
        body.status = status
      }
      if (domain !== undefined && domain !== null && domain.trim() !== '') {
        body.domain = domain.trim()
      }
      if (pop !== undefined && pop !== null && pop.trim() !== '') {
        body.pop = pop.trim()
      }
      if (country !== undefined && country !== null && country.trim() !== '') {
        body.country = country.trim()
      }
      if (batchId !== undefined && batchId !== null && batchId.trim() !== '') {
        body.batch_id = batchId.trim()
      }
      const { data, error } = await client.POST(REQUEST_FACTS_PATH, {
        params: {
          path: { service_id: serviceId },
        },
        body,
        signal,
      })
      if (error || !data) {
        throw new Error(extractApiError(error))
      }
      return data
    },
    retry: false,
  })

  const nextPage = () => {
    const nextCursor = query.data?.next_cursor
    if (!nextCursor) return
    setPagination((current) => {
      const currentPage = current.key === rangeKey ? current.pageIndex : 0
      const currentCursors = current.key === rangeKey ? current.cursors : [null]
      if (currentCursors[currentPage + 1] === nextCursor) return current
      const nextCursors = currentCursors.slice(0, currentPage + 1)
      nextCursors.push(nextCursor)
      return { key: rangeKey, pageIndex: currentPage + 1, cursors: nextCursors }
    })
  }

  const previousPage = () => {
    setPagination((current) => ({
      key: rangeKey,
      pageIndex: Math.max(0, current.key === rangeKey ? current.pageIndex - 1 : 0),
      cursors: current.key === rangeKey ? current.cursors : [null],
    }))
  }

  return {
    ...query,
    pageIndex,
    nextPage,
    previousPage,
    canNextPage: Boolean(query.data?.next_cursor),
    canPreviousPage: pageIndex > 0,
  }
}

export const highScaleRequestFactsHooks = {
  useHighScaleRequestFacts,
}

function valueAsString(value: unknown): string {
  if (value === null || value === undefined || value === '') return '—'
  return String(value)
}

function formatTimestamp(value: unknown): string {
  if (typeof value !== 'string') return valueAsString(value)
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? value : parsed.toISOString()
}

type FactRow = NonNullable<ReturnType<typeof useHighScaleRequestFacts>['data']>['rows'][number]

const textColumn = (id: keyof FactRow & string, header: string, size: number): ColumnDef<FactRow> => ({
  id,
  header,
  accessorFn: (row) => valueAsString(row[id]),
  size,
  enableSorting: false,
  cell: ({ row }) => <span className="text-sm">{valueAsString(row.original[id])}</span>,
})

const FACT_COLUMNS: ColumnDef<FactRow>[] = [
  {
    id: 'timestamp',
    header: 'Timestamp',
    accessorFn: (row) => formatTimestamp(row.timestamp),
    size: 210,
    enableSorting: false,
    cell: ({ row }) => <span className="whitespace-nowrap text-sm">{formatTimestamp(row.original.timestamp)}</span>,
  },
  textColumn('status', 'Status', 80),
  textColumn('host', 'Host', 180),
  textColumn('pop', 'POP', 80),
  textColumn('country', 'Country', 90),
  textColumn('client_ip', 'Client IP', 150),
  {
    id: 'url',
    header: 'URL',
    accessorFn: (row) => valueAsString(row.url),
    size: 420,
    enableSorting: false,
    cell: ({ row }) => (
      <span className="text-sm" title={valueAsString(row.original.url)}>
        {valueAsString(row.original.url)}
      </span>
    ),
  },
]

function Metadata({ metadata }: { metadata: NonNullable<ReturnType<typeof useHighScaleRequestFacts>['data']>['metadata'] }) {
  return (
    <div
      className="grid gap-3 sm:grid-cols-3 [contain-intrinsic-size:auto_88px] [content-visibility:auto]"
      data-testid="high-scale-metadata"
    >
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
        <div className="mt-1 font-medium">{formatElapsed(metadata.freshness_lag_seconds, ELAPSED_FRESHNESS)} lag</div>
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

  const [statusFilter, setStatusFilter] = useState('')
  const [domainFilter, setDomainFilter] = useState('')
  const [popFilter, setPopFilter] = useState('')
  const [countryFilter, setCountryFilter] = useState('')
  const [batchIdFilter, setBatchIdFilter] = useState('')

  const parsedStatus = statusFilter.trim() !== '' && !Number.isNaN(Number(statusFilter.trim()))
    ? Number(statusFilter.trim())
    : null

  const facts = highScaleRequestFactsHooks.useHighScaleRequestFacts({
    serviceId,
    startTime,
    endTime,
    status: parsedStatus,
    domain: domainFilter.trim() || null,
    pop: popFilter.trim() || null,
    country: countryFilter.trim() || null,
    batchId: batchIdFilter.trim() || null,
    emulateStandard: true,
  })

  const hasFilters = Boolean(
    statusFilter.trim() ||
    domainFilter.trim() ||
    popFilter.trim() ||
    countryFilter.trim() ||
    batchIdFilter.trim()
  )

  const clearFilters = () => {
    setStatusFilter('')
    setDomainFilter('')
    setPopFilter('')
    setCountryFilter('')
    setBatchIdFilter('')
  }

  if (!bootstrapResolved) {
    return (
      <div className="min-h-[600px] [contain-intrinsic-size:auto_600px] [content-visibility:auto]" data-testid="high-scale-bootstrap-skeleton">
        <Skeleton className="h-64 w-full" />
      </div>
    )
  }

  if (!serviceId) {
    return <NoServiceSelected icon={Database} message="Select a service to query high-scale request facts." />
  }

  if (facts.isLoading) {
    return (
      <Card
        className="min-h-[600px] [contain-intrinsic-size:auto_600px] [content-visibility:auto]"
        data-testid="high-scale-loading-card"
      >
        <CardHeader>
          <CardTitle>Loading request facts…</CardTitle>
          <p className="text-sm text-muted-foreground">Querying distributed cluster shards...</p>
        </CardHeader>
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

  const isEmulated = (response.metadata as { mode?: string }).mode === 'duckdb_emulation'

  return (
    <Card
      className="min-h-[600px] [contain-intrinsic-size:auto_600px] [content-visibility:auto]"
      data-testid="high-scale-facts-card"
    >
      <CardHeader>
        <CardTitle className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <span>Request facts</span>
            <Badge
              variant={isEmulated ? 'secondary' : 'outline'}
              data-testid="high-scale-mode-badge"
            >
              {isEmulated ? 'DuckDB Standard Mode Emulation' : 'ClickHouse Distributed Cluster'}
            </Badge>
          </div>
          <span className="text-sm font-normal text-muted-foreground">
            Page {facts.pageIndex + 1}
          </span>
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <Metadata metadata={response.metadata} />

        <div
          className="grid gap-2 sm:grid-cols-6 [contain-intrinsic-size:auto_48px] [content-visibility:auto]"
          data-testid="high-scale-filters"
        >
          <div className="flex items-center gap-1.5 sm:col-span-1">
            <Filter className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
            <Input
              aria-label="Filter by HTTP status"
              data-testid="filter-status"
              placeholder="Status (e.g. 200)"
              value={statusFilter}
              onChange={(e) => setStatusFilter(e.target.value)}
              className="h-8 text-xs"
            />
          </div>
          <Input
            aria-label="Filter by domain or host"
            data-testid="filter-domain"
            placeholder="Host / Domain"
            value={domainFilter}
            onChange={(e) => setDomainFilter(e.target.value)}
            className="h-8 text-xs sm:col-span-1"
          />
          <Input
            aria-label="Filter by edge POP"
            data-testid="filter-pop"
            placeholder="POP (e.g. IAD)"
            value={popFilter}
            onChange={(e) => setPopFilter(e.target.value)}
            className="h-8 text-xs sm:col-span-1"
          />
          <Input
            aria-label="Filter by country"
            data-testid="filter-country"
            placeholder="Country (e.g. US)"
            value={countryFilter}
            onChange={(e) => setCountryFilter(e.target.value)}
            className="h-8 text-xs sm:col-span-1"
          />
          <Input
            aria-label="Filter by batch ID"
            data-testid="filter-batch-id"
            placeholder="Batch ID"
            value={batchIdFilter}
            onChange={(e) => setBatchIdFilter(e.target.value)}
            className="h-8 text-xs sm:col-span-1"
          />
          <div className="flex items-center justify-end sm:col-span-1">
            {hasFilters && (
              <Button
                variant="ghost"
                size="sm"
                onClick={clearFilters}
                data-testid="filter-clear"
                className="h-8 text-xs"
              >
                <X className="mr-1 h-3.5 w-3.5" />
                Clear filters
              </Button>
            )}
          </div>
        </div>

        <div
          className={`overflow-x-auto rounded-lg border transition-opacity ${
            facts.isFetching && !facts.isLoading ? 'opacity-40 pointer-events-none' : ''
          }`}
          data-testid="high-scale-table-wrapper"
        >
          <DataTableReadonly
            columns={FACT_COLUMNS}
            data={response.rows}
            hideToolbar
            showPagination={false}
            tableCaption="High-scale request facts"
            emptyMessage="No request facts in this time range."
          />
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

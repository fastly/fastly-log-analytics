'use client'

import React from 'react'
import { parseAsString, useQueryState } from 'nuqs'
import { Activity, Check, ChevronsUpDown, Globe, Link2, Server, X } from 'lucide-react'
import { client } from '@/lib/api'
import { useServiceQuery } from '@/hooks/useServiceQuery'
import { resolveRangeWire } from '@/lib/range-wire'
import { formatDate } from '@/lib/date'
import { formatBytes, resolveCountryName } from '@/lib/format'
import { cn } from '@/lib/utils'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { ChartEmptyState } from '@/components/ChartEmptyState'
import { TimeSeriesChart } from '@/components/charts/TimeSeriesChart'
import { Button } from '@/components/ui/button'
import { Command, CommandEmpty, CommandGroup, CommandInput, CommandItem, CommandList } from '@/components/ui/command'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import type { components } from '@/types/api.generated'
import type { StreamingTabProps } from './QualityTab'

type ContentSecurityData = components['schemas']['ContentSecurityResponse']
type TopRow = components['schemas']['ContentSecurityTopRow']
type ContentIdOption = components['schemas']['ContentSecurityContentId']

interface SecurityTabProps extends StreamingTabProps {
  bucketSeconds: number
}

// ?content=<cmcd cid> narrows the whole tab to one piece of content.
const contentParser = parseAsString.withOptions({ history: 'replace', shallow: true })

const TOP_N = 10

const BANDWIDTH_LAYOUT = {
  yaxis: { title: { text: 'Bandwidth' }, tickformat: '.3s', ticksuffix: 'bps', rangemode: 'tozero' as const },
}

function TopTable({
  rows,
  valueHeader,
  formatValue = (v) => v,
}: {
  rows: TopRow[]
  valueHeader: string
  formatValue?: (value: string) => string
}) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>{valueHeader}</TableHead>
          <TableHead className="text-right">Requests</TableHead>
          <TableHead className="text-right">Bandwidth</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {rows.map((row) => (
          <TableRow key={row.value}>
            <TableCell className="max-w-[16rem] truncate" title={row.value}>
              {formatValue(row.value)}
            </TableCell>
            <TableCell className="text-right tabular-nums">{row.requests.toLocaleString()}</TableCell>
            <TableCell className="text-right tabular-nums">{row.bytes == null ? '—' : formatBytes(row.bytes)}</TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  )
}

const formatCountry = (code: string) => `${resolveCountryName(code)} (${code})`

function ContentPicker({
  value,
  options,
  disabled,
  onChange,
}: {
  value: string | null
  options: ContentIdOption[]
  disabled: boolean
  onChange: (contentId: string | null) => void
}) {
  const [open, setOpen] = React.useState(false)
  const select = (contentId: string | null) => {
    onChange(contentId)
    setOpen(false)
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      {/* aria-label on PopoverTrigger, not the Button render-prop — see ViewSelector. */}
      <PopoverTrigger
        aria-label="Content ID"
        disabled={disabled}
        render={
          <Button
            variant="outline"
            role="combobox"
            aria-expanded={open}
            className="h-8 min-w-[220px] max-w-[360px] justify-between text-xs"
          >
            <span className="truncate">{value ?? 'All content'}</span>
            <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
          </Button>
        }
      />
      <PopoverContent className="w-[360px] p-0" align="start">
        <Command>
          <CommandInput placeholder="Search content IDs..." />
          <CommandEmpty>No content found.</CommandEmpty>
          <CommandList>
            <CommandGroup>
              <CommandItem value="__all__" onSelect={() => select(null)}>
                <Check className={cn('h-4 w-4', value === null ? 'opacity-100' : 'opacity-0')} />
                All content
              </CommandItem>
              {options.map((opt) => (
                <CommandItem key={opt.content_id} value={opt.content_id} onSelect={() => select(opt.content_id)}>
                  <Check className={cn('h-4 w-4', value === opt.content_id ? 'opacity-100' : 'opacity-0')} />
                  <span className="flex-1 truncate" title={opt.content_id}>{opt.content_id}</span>
                  <span className="text-muted-foreground tabular-nums">{opt.requests.toLocaleString()}</span>
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  )
}

export default function SecurityTab({
  activeServiceId,
  filterPayload,
  startTime,
  endTime,
  relativeRange,
  isAutoRange,
  anchor,
  timezone,
  bucketSeconds,
}: SecurityTabProps) {
  const [contentId, setContentId] = useQueryState('content', contentParser)
  const { rangeKey, rangeBody } = resolveRangeWire({ relativeRange, isAutoRange, startTime, endTime, anchor })

  const query = useServiceQuery<ContentSecurityData | undefined>(
    ['cmcd', 'content-security', activeServiceId, rangeKey, anchor, filterPayload, contentId, bucketSeconds],
    async ({ signal }) => {
      const { data, error } = await client.POST('/api/cmcd/content-security', {
        signal,
        body: {
          filters: filterPayload,
          content_id: contentId,
          bucket_seconds: bucketSeconds,
          top_n: TOP_N,
          ...rangeBody,
        },
      })
      if (error) throw error
      return data
    },
    { refetchInterval: 30_000 },
  )

  const data = query.data
  const fields = data?.fields ?? {}
  // Only claim a field is missing once the backend has told us so.
  const missing = (field: string) => data != null && fields[field] === false

  const bandwidthData = React.useMemo(() => {
    const ts = data?.bandwidth_ts
    if (!ts?.length) return []
    const x = ts.map((d) => formatDate(d.bucket, timezone, 'yyyy-MM-dd HH:mm:ss'))
    const toBps = (bytes: number) => (bytes * 8) / bucketSeconds
    const traces: Record<string, unknown>[] = [
      {
        x,
        y: ts.map((d) => toBps(d.edge_bytes)),
        name: 'Edge (to viewers)',
        type: 'scatter' as const,
        line: { color: '#6366f1' },
        hovertemplate: '%{y:.3s}bps<extra>Edge</extra>',
      },
    ]
    if (data?.has_shield_split) {
      traces.push({
        x,
        y: ts.map((d) => toBps(d.shield_bytes ?? 0)),
        name: 'Shield (to edge)',
        type: 'scatter' as const,
        line: { color: '#f59e0b', dash: 'dot' },
        hovertemplate: '%{y:.3s}bps<extra>Shield</extra>',
      })
    }
    return traces
  }, [data?.bandwidth_ts, data?.has_shield_split, timezone, bucketSeconds])

  if (data && !data.available) {
    return (
      <div className="py-16 text-center text-sm text-muted-foreground">
        {data.reason ?? 'Content security metrics are not available for this service.'}
      </div>
    )
  }

  const cardState = {
    isLoading: query.isLoading,
    isFetching: query.isFetching,
    error: query.error as AnalyticsCardError | null,
  }

  const topCard = (
    title: string,
    icon: React.ReactNode,
    rows: TopRow[] | undefined,
    valueHeader: string,
    formatValue: ((value: string) => string) | undefined,
    field: string,
    requires: string,
  ) => (
    <AnalyticsCard
      title={
        <>
          {title} <span className="ml-1 text-xs font-normal text-muted-foreground">(Top {TOP_N})</span>
        </>
      }
      icon={icon}
      {...cardState}
      className="min-h-[200px]"
    >
      {rows?.length ? (
        <TopTable rows={rows} valueHeader={valueHeader} formatValue={formatValue} />
      ) : (
        <ChartEmptyState requires={missing(field) ? requires : undefined} />
      )}
    </AnalyticsCard>
  )

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center gap-3">
        <span className="text-sm font-medium">Content</span>
        <ContentPicker
          value={contentId}
          options={data?.content_ids ?? []}
          disabled={missing('cmcd_cid')}
          onChange={(id) => void setContentId(id)}
        />
        {contentId && (
          <Button variant="ghost" size="sm" className="h-8 text-xs" onClick={() => void setContentId(null)}>
            <X className="h-3.5 w-3.5" />
            Clear
          </Button>
        )}
        {missing('cmcd_cid') && (
          <span className="text-xs text-muted-foreground">
            Requires CMCD with the content ID (cid) field enabled.
          </span>
        )}
      </div>

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
        {topCard('Countries', <Globe className="h-4 w-4" />, data?.top_countries, 'Country', formatCountry, 'country', 'Geolocation Basic (Group D) fields to be enabled in Fastly logging.')}
        {topCard('Referers', <Link2 className="h-4 w-4" />, data?.top_referers, 'Referer', undefined, 'referer', 'the Referer field (Group A) to be enabled in Fastly logging.')}
        {topCard('Hosts', <Server className="h-4 w-4" />, data?.top_hosts, 'Host', undefined, 'host', 'the Host field (Group A) to be enabled in Fastly logging.')}
      </div>

      <AnalyticsCard
        title="Bandwidth: Edge vs Shield"
        icon={<Activity className="h-4 w-4" />}
        {...cardState}
        className="h-[340px]"
        contentClassName="p-2"
      >
        {bandwidthData.length ? (
          <TimeSeriesChart
            data={bandwidthData}
            layout={BANDWIDTH_LAYOUT}
            startTime={startTime}
            endTime={endTime}
            timezone={timezone}
            height="100%"
          />
        ) : (
          <ChartEmptyState />
        )}
      </AnalyticsCard>
    </div>
  )
}

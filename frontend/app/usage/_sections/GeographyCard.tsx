'use client'

import React, { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Globe2 } from 'lucide-react'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import { Button } from '@/components/ui/button'
import { ButtonGroup } from '@/components/ui/button-group'
import { client, unwrapClient } from '@/lib/api'
import { cn, formatBytes, formatCount } from '@/lib/utils'
import type { components } from '@/types/api.generated'
import type { PlotlyTrace } from '@/types/api'

export type GeographyGroup = 'region' | 'datacenter'
type GeographyResponse = components['schemas']['PlatformGeographyResponse']
type GeographyPoint = components['schemas']['PlatformGeographyPoint']
type GeographyMeasure = 'requests' | 'bandwidth'

interface GeographyCardProps {
  activeServiceId: string | null
  startTime: string | null
  endTime: string | null
  enabled?: boolean
  fetchGeography?: (groupBy: GeographyGroup) => Promise<GeographyResponse>
}

function share(value: number) {
  return `${(value * 100).toFixed(1)}%`
}

export function GeographyCard({
  activeServiceId,
  startTime,
  endTime,
  enabled = true,
  fetchGeography,
}: GeographyCardProps) {
  const [groupBy, setGroupBy] = useState<GeographyGroup>('region')
  const [measure, setMeasure] = useState<GeographyMeasure>('requests')

  const geography = useQuery({
    queryKey: ['usage', 'geography', activeServiceId, startTime, endTime, groupBy],
    queryFn: async ({ signal }): Promise<GeographyResponse> => {
      if (fetchGeography) return fetchGeography(groupBy)
      return unwrapClient(
        client.GET('/api/usage/geography', {
          signal,
          params: {
            query: {
              start: startTime ?? undefined,
              end: endTime ?? undefined,
              group_by: groupBy,
            },
          },
        }),
      )
    },
    enabled: enabled && activeServiceId != null,
    staleTime: 60_000,
  })

  const rows = useMemo(() => {
    const data = geography.data?.source === 'fastly' ? geography.data.data : []
    const field = measure === 'requests' ? 'requests' : 'bandwidth_bytes'
    return [...data].sort(
      (a, b) => b[field] - a[field] || a.location.localeCompare(b.location),
    )
  }, [geography.data, measure])
  const chartRows = rows.slice(0, 10)
  const chartData: PlotlyTrace[] = chartRows.length
    ? [{
        type: 'bar',
        orientation: 'h',
        x: chartRows.map((row) => measure === 'requests' ? row.requests : row.bandwidth_bytes),
        y: chartRows.map((row) => row.location),
        marker: { color: '#0ea5e9' },
        hovertemplate: measure === 'requests'
          ? '%{y}: %{x:,} requests<extra></extra>'
          : '%{y}: %{x:,.0f} bytes<extra></extra>',
        showlegend: false,
      }]
    : []
  const unavailable = geography.data?.source === 'unavailable'
  const totalRequests = geography.data?.requests ?? 0
  const totalBandwidth = geography.data?.bandwidth_bytes ?? 0

  const groupButtons = (
    <ButtonGroup>
      {([
        ['Region', 'region'],
        ['Datacenter', 'datacenter'],
      ] as const).map(([label, value]) => (
        <Button
          key={value}
          type="button"
          size="sm"
          variant={groupBy === value ? 'default' : 'ghost'}
          aria-pressed={groupBy === value}
          onClick={() => setGroupBy(value)}
          className="h-7 text-[11px]"
        >
          {label}
        </Button>
      ))}
    </ButtonGroup>
  )

  return (
    <AnalyticsCard
      title="Traffic Geography"
      icon={<Globe2 className="h-4 w-4" />}
      description="Informational Platform attribution; not used in billing totals."
      headerAction={groupButtons}
      isLoading={geography.isLoading}
      isFetching={geography.isFetching}
      loadingMessage="Loading traffic geography..."
      error={geography.error as AnalyticsCardError | null}
      isEmpty={!unavailable && !geography.isLoading && !geography.error && rows.length === 0}
      className="mb-4"
      contentClassName="space-y-4"
    >
      {unavailable ? (
        <div className="flex min-h-64 items-center justify-center text-sm text-muted-foreground">
          Geography unavailable
        </div>
      ) : (
        <>
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="text-sm font-medium">
              {measure === 'requests'
                ? `${formatCount(totalRequests)} total requests`
                : `${formatBytes(totalBandwidth)} total bandwidth`}
            </div>
            <ButtonGroup>
              {([
                ['Requests', 'requests'],
                ['Bandwidth', 'bandwidth'],
              ] as const).map(([label, value]) => (
                <Button
                  key={value}
                  type="button"
                  size="sm"
                  variant={measure === value ? 'default' : 'ghost'}
                  aria-pressed={measure === value}
                  onClick={() => setMeasure(value)}
                  className="h-7 text-[11px]"
                >
                  {label}
                </Button>
              ))}
            </ButtonGroup>
          </div>

          <PlotlyChart
            data={chartData}
            layout={{
              autosize: true,
              height: Math.max(240, chartRows.length * 34),
              margin: { l: 110, r: 24, t: 8, b: 42 },
              barmode: 'overlay',
              showlegend: false,
              xaxis: {
                title: { text: measure === 'requests' ? 'Requests' : 'Bandwidth' },
                tickformat: measure === 'requests' ? ',~s' : '~s',
              },
              yaxis: { autorange: 'reversed', automargin: true },
            }}
            config={{ responsive: true, displayModeBar: false }}
            a11yTitle={`Top ${groupBy === 'region' ? 'regions' : 'datacenters'} by ${measure}`}
          />

          <div className="overflow-x-auto">
            <table className="w-full text-sm" aria-label="Traffic geography details">
              <thead>
                <tr className="border-b text-left text-muted-foreground">
                  <th className="py-2 pr-3 font-medium">{groupBy === 'region' ? 'Region' : 'Datacenter'}</th>
                  <th className="py-2 px-3 text-right font-medium">Requests</th>
                  <th className="py-2 px-3 text-right font-medium">Share</th>
                  <th className="py-2 px-3 text-right font-medium">Bandwidth</th>
                  <th className="py-2 pl-3 text-right font-medium">Share</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row: GeographyPoint) => (
                  <tr key={row.location} className="border-b last:border-0">
                    <th scope="row" className="py-2 pr-3 text-left font-medium">{row.location}</th>
                    <td className="py-2 px-3 text-right tabular-nums">{formatCount(row.requests)}</td>
                    <td className="py-2 px-3 text-right tabular-nums text-muted-foreground">
                      {share(row.request_share)}
                    </td>
                    <td className="py-2 px-3 text-right tabular-nums">{formatBytes(row.bandwidth_bytes)}</td>
                    <td className="py-2 pl-3 text-right tabular-nums text-muted-foreground">
                      {share(row.bandwidth_share)}
                    </td>
                  </tr>
                ))}
              </tbody>
              <tfoot>
                <tr className={cn('border-t font-semibold')}>
                  <th scope="row" className="pt-3 pr-3 text-left">Total</th>
                  <td className="pt-3 px-3 text-right tabular-nums">{formatCount(totalRequests)}</td>
                  <td className="pt-3 px-3 text-right tabular-nums">{totalRequests ? '100.0%' : '0.0%'}</td>
                  <td className="pt-3 px-3 text-right tabular-nums">{formatBytes(totalBandwidth)}</td>
                  <td className="pt-3 pl-3 text-right tabular-nums">{totalBandwidth ? '100.0%' : '0.0%'}</td>
                </tr>
              </tfoot>
            </table>
          </div>
        </>
      )}
    </AnalyticsCard>
  )
}

export const UsageGeographyCard = GeographyCard

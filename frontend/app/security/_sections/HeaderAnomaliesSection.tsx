import React from 'react'
import { Scale, Shield } from 'lucide-react'
import type { Dispatch, SetStateAction } from 'react'
import type { VisibilityState } from '@tanstack/react-table'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { DataTable, ColumnVisibilityDropdown } from '@/components/DataTable'
import { PlotlyChart } from '@/components/PlotlyChart'
import { FilterValueCell } from '@/components/FilterValueCell'
import { SECURITY_INFO, TOP_IP_COLUMN_IDS } from './securityInfo'
import type { components } from '@/types/api.generated'

type SecurityData = components['schemas']['SecurityAggregatesResponse']

type Props = {
  data: SecurityData | undefined
  isLoading: boolean
  isFetching: boolean
  getFieldLabel: (id: string) => string
  topIpVisibility: VisibilityState
  setTopIpVisibility: Dispatch<SetStateAction<VisibilityState>>
  onTopIpVisChange: (id: string, vis: boolean) => void
}

export function HeaderAnomaliesSection({
  data,
  isLoading,
  isFetching,
  getFieldLabel,
  topIpVisibility,
  setTopIpVisibility,
  onTopIpVisChange,
}: Props) {
  const headerSizeData = React.useMemo(() => {
    const req_size_dist = data?.req_size_dist
    if (!req_size_dist?.length) return []
    return [{
      x: req_size_dist.map((d: any) => d.bucket),
      y: req_size_dist.map((d: any) => d.count),
      type: 'bar',
      marker: { color: '#ec4899' }
    }]
  }, [data])

  const topIpHeaderColumns = [
    {
      accessorKey: 'ip',
      header: 'IP Address',
      cell: (info: any) => (
        <FilterValueCell
          filters={[{ column: 'client_ip', value: info.getValue() }]}
          className="font-mono text-xs"
        />
      )
    },
    { accessorKey: 'max_header', header: 'Max Header (Bytes)', cell: (info: any) => info.getValue().toLocaleString() },
  ]

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
      <AnalyticsCard
        title="Request Header Size Distribution"
        icon={<Scale className="h-4 w-4" />}
        isLoading={isLoading}
        isFetching={isFetching}
        className="h-[360px]"
        contentClassName="p-2"
        helpTitle={SECURITY_INFO.req_size.title}
        helpContent={SECURITY_INFO.req_size.body}
      >
        {headerSizeData.length === 0 && !isLoading ? (
          <div className="flex flex-col items-center justify-center h-full text-muted-foreground text-center px-4">
            <span className="text-sm font-medium mb-1">No data available</span>
            <span className="text-[10px] opacity-70">
              Requires Request Identity (Group A) fields to be enabled in Fastly logging.
            </span>
          </div>
        ) : (
          <PlotlyChart
            data={headerSizeData as any[]}
            layout={{ yaxis: { title: 'Count' } }}
            height="100%"
          />
        )}
      </AnalyticsCard>

      <AnalyticsCard
        title="Oversized Request Headers (by IP)"
        icon={<Shield className="h-4 w-4" />}
        headerAction={
          <ColumnVisibilityDropdown
            columns={TOP_IP_COLUMN_IDS.map(id => ({ id, label: getFieldLabel(id) }))}
            visibility={topIpVisibility}
            onChange={onTopIpVisChange}
          />
        }
        isLoading={isLoading}
        isFetching={isFetching}
        className="min-h-[300px]"
        contentClassName="p-0"
        helpTitle={SECURITY_INFO.top_ips_header.title}
        helpContent={SECURITY_INFO.top_ips_header.body}
      >
        <DataTable
          columns={topIpHeaderColumns}
          data={data?.top_ips_header || []}
          emptyMessage={isLoading ? "" : "Requires Request Identity (Group A) log fields to be enabled in Fastly logging."}
          hideToolbar
          columnVisibility={topIpVisibility}
          onColumnVisibilityChange={setTopIpVisibility}
        />
      </AnalyticsCard>
    </div>
  )
}

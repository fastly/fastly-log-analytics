import React from 'react'
import { Bot, Fingerprint, CheckCircle2, AlertTriangle, Clock, HelpCircle } from 'lucide-react'
import type { Dispatch, SetStateAction } from 'react'
import type { VisibilityState } from '@tanstack/react-table'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { DataTable, ColumnVisibilityDropdown } from '@/components/DataTable'
import { PlotlyChart } from '@/components/PlotlyChart'
import { DashboardLinkCell } from '@/components/DashboardLinkCell'
import { formatDate } from '@/lib/date'
import {
  SECURITY_INFO,
  NGWAF_BOT_COLUMN_IDS,
  BOT_COLUMN_IDS,
  FINGERPRINT_COLUMN_IDS,
  NgwafVerifiedBot,
} from './securityInfo'

type Props = {
  data: any
  isLoading: boolean
  isFetching: boolean
  intervalButtons: React.ReactNode
  bucketSeconds: number
  timezone: string
  commonTimeLayout: any
  getFieldLabel: (id: string) => string
  ngwafBotVisibility: VisibilityState
  setNgwafBotVisibility: Dispatch<SetStateAction<VisibilityState>>
  onNgwafBotVisChange: (id: string, vis: boolean) => void
  botVisibility: VisibilityState
  setBotVisibility: Dispatch<SetStateAction<VisibilityState>>
  onBotVisChange: (id: string, vis: boolean) => void
  fingerprintVisibility: VisibilityState
  setFingerprintVisibility: Dispatch<SetStateAction<VisibilityState>>
  onFingerprintVisChange: (id: string, vis: boolean) => void
}

export function BotsSection({
  data,
  isLoading,
  isFetching,
  intervalButtons,
  bucketSeconds,
  timezone,
  commonTimeLayout,
  getFieldLabel,
  ngwafBotVisibility,
  setNgwafBotVisibility,
  onNgwafBotVisChange,
  botVisibility,
  setBotVisibility,
  onBotVisChange,
  fingerprintVisibility,
  setFingerprintVisibility,
  onFingerprintVisChange,
}: Props) {
  const ngwafBotsData = React.useMemo(() => {
    const timeseries = (data as any)?.ngwaf_verified_bots_ts
    if (!timeseries?.length) return []
    const byName: Record<string, { x: string[], y: number[] }> = {}

    const allTimesSet = new Set<string>()
    timeseries.forEach((d: any) => {
      allTimesSet.add(formatDate(d.time, timezone, "yyyy-MM-dd HH:mm:ss"))
    })
    const allTimes = Array.from(allTimesSet).sort()

    const names = Array.from(new Set(timeseries.map((d: any) => d.bot_name)))
    for (const n of names) {
      byName[n as string] = { x: [...allTimes], y: new Array(allTimes.length).fill(0) }
    }

    for (const d of timeseries) {
      const t = formatDate(d.time, timezone, "yyyy-MM-dd HH:mm:ss")
      const idx = allTimes.indexOf(t)
      if (idx !== -1) {
        byName[d.bot_name].y[idx] = d.count
      }
    }

    return Object.entries(byName).map(([name, d], i) => ({
      x: d.x,
      y: d.y.map(val => val === 0 ? null : val),
      type: 'bar',
      name,
      width: bucketSeconds * 1000,
      hovertemplate: `${name}: %{y:,}<extra></extra>`,
      marker: { color: `hsl(${(i * 47 + 210) % 360}, 70%, 50%)` }
    }))
  }, [data, timezone, bucketSeconds])

  const botColumns = [
    {
      accessorKey: 'name',
      header: 'Bot',
      cell: (info: any) => {
        const row = info.row.original
        return (
          <DashboardLinkCell
            value={row.name}
            href={`/dashboard?filter__wellknown_bot_id=${encodeURIComponent(row.id)}`}
            className="font-medium"
            containerClassName="max-w-[200px]"
          />
        )
      }
    },
    {
      accessorKey: 'category',
      header: 'Category',
      cell: (info: any) => <span className="capitalize">{info.getValue()?.replace(/-/g, ' ')}</span>
    },
    { accessorKey: 'request_count', header: 'Requests', cell: (info: any) => info.getValue().toLocaleString() },
    {
      accessorKey: 'verified_count',
      header: 'Verified',
      cell: (info: any) => info.getValue() > 0 ? (
        <span className="flex items-center gap-1 text-green-500">
          <CheckCircle2 className="h-3 w-3" />{info.getValue().toLocaleString()}
        </span>
      ) : '—'
    },
    {
      accessorKey: 'impersonator_count',
      header: 'Spoofed',
      cell: (info: any) => info.getValue() > 0 ? (
        <span className="flex items-center gap-1 text-red-500">
          <AlertTriangle className="h-3 w-3" />{info.getValue().toLocaleString()}
        </span>
      ) : '—'
    },
    {
      accessorKey: 'unverified_count',
      header: 'Unverified',
      cell: (info: any) => info.getValue() > 0 ? (
        <span className="flex items-center gap-1 text-muted-foreground" title="Unverifiable (no IPs/domains in source)">
          <HelpCircle className="h-3 w-3" />{info.getValue().toLocaleString()}
        </span>
      ) : '—'
    },
    {
      accessorKey: 'pending_count',
      header: 'Pending',
      cell: (info: any) => info.getValue() > 0 ? (
        <span className="flex items-center gap-1 text-yellow-500" title="Pending rDNS lookup">
          <Clock className="h-3 w-3" />{info.getValue().toLocaleString()}
        </span>
      ) : '—'
    }
  ]

  const ngwafBotColumns = [
    {
      accessorKey: 'bot_name',
      header: 'Bot Name',
      cell: (info: any) => {
        const row = info.row.original as NgwafVerifiedBot
        return (
          <DashboardLinkCell
            value={row.bot_name}
            href={`/dashboard?filter__ngwaf_bot_name=${encodeURIComponent(row.bot_name || '')}`}
            className="font-medium"
            containerClassName="max-w-[200px]"
          />
        )
      }
    },
    {
      accessorKey: 'category',
      header: 'Category',
      cell: (info: any) => info.getValue()
        ? <span className="capitalize">{info.getValue().replace(/-/g, ' ')}</span>
        : <span className="text-muted-foreground">—</span>
    },
    { accessorKey: 'request_count', header: 'Requests', cell: (info: any) => info.getValue().toLocaleString() },
  ]

  const fingerprintColumns = [
    {
      accessorKey: 'fingerprint',
      header: 'Cipher Fingerprint (SHA)',
      cell: (info: any) => (
        <DashboardLinkCell
          value={info.getValue()}
          href={`/dashboard?filter_tls_client_hello=${encodeURIComponent(info.getValue())}`}
          className="font-mono text-[10px]"
          containerClassName="max-w-[200px]"
        />
      )
    },
    { accessorKey: 'ip_count', header: 'Unique IPs', cell: (info: any) => info.getValue().toLocaleString() },
    { accessorKey: 'request_count', header: 'Requests', cell: (info: any) => info.getValue().toLocaleString() },
  ]

  return (
    <>
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        <AnalyticsCard
          title="Verified Bots (NGWAF)"
          icon={<Bot className="h-4 w-4" />}
          headerAction={intervalButtons}
          isLoading={isLoading}
          isFetching={isFetching}
          className="h-[360px]"
          contentClassName="p-2"
          helpTitle={SECURITY_INFO.ngwaf_bots.title}
          helpContent={SECURITY_INFO.ngwaf_bots.body}
        >
          {ngwafBotsData.length === 0 && !isLoading ? (
            <div className="flex items-center justify-center h-full text-muted-foreground text-sm text-center px-4">
              {(data as any)?.ngwaf_configured
                ? "No NGWAF bot detections in this time window."
                : <>Set <code className="mx-1 text-xs bg-muted px-1 rounded">ngwaf_workspace_id</code> in service settings to enable NGWAF bot tracking.</>}
            </div>
          ) : (
            <PlotlyChart
              data={ngwafBotsData as any[]}
              layout={{
                ...commonTimeLayout,
                barmode: 'stack',
                showlegend: true,
                yaxis: { title: 'Requests', separatethousands: true, exponentformat: 'none' }
              }}
              height="100%"
            />
          )}
        </AnalyticsCard>

        <AnalyticsCard
          title="Verified Bot Names (NGWAF)"
          icon={<Bot className="h-4 w-4" />}
          headerAction={
            <ColumnVisibilityDropdown columns={NGWAF_BOT_COLUMN_IDS.map(id => ({ id, label: getFieldLabel(id) }))} visibility={ngwafBotVisibility} onChange={onNgwafBotVisChange} />
          }
          isLoading={isLoading}
          isFetching={isFetching}
          className="min-h-[360px]"
          contentClassName="p-0"
          helpTitle={SECURITY_INFO.ngwaf_bots.title}
          helpContent={SECURITY_INFO.ngwaf_bots.body}
        >
          <DataTable
            columns={ngwafBotColumns}
            data={(data as any)?.ngwaf_verified_bots || []}
            emptyMessage={isLoading ? "" : ((data as any)?.ngwaf_configured ? "No NGWAF bot detections in this time window." : "Set ngwaf_workspace_id in service settings to enable NGWAF bot tracking.")}
            hideToolbar
            columnVisibility={ngwafBotVisibility}
            onColumnVisibilityChange={setNgwafBotVisibility}
          />
        </AnalyticsCard>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        <AnalyticsCard
          title="Well-Known Bots"
          icon={<Bot className="h-4 w-4" />}
          headerAction={
            <ColumnVisibilityDropdown columns={BOT_COLUMN_IDS.map(id => ({ id, label: getFieldLabel(id) }))} visibility={botVisibility} onChange={onBotVisChange} />
          }
          isLoading={isLoading}
          isFetching={isFetching}
          className="min-h-[360px]"
          contentClassName="p-0"
          helpTitle={SECURITY_INFO.wellknown_bots.title}
          helpContent={SECURITY_INFO.wellknown_bots.body}
        >
          <DataTable
            columns={botColumns}
            data={(data as any)?.wellknown_bots || []}
            emptyMessage={isLoading ? "" : "No known bots detected. Ensure bot sources are cached in Admin settings."}
            hideToolbar
            columnVisibility={botVisibility}
            onColumnVisibilityChange={setBotVisibility}
          />
        </AnalyticsCard>

        <AnalyticsCard
          title="Top TLS Fingerprints"
          icon={<Fingerprint className="h-4 w-4" />}
          headerAction={
            <ColumnVisibilityDropdown columns={FINGERPRINT_COLUMN_IDS.map(id => ({ id, label: getFieldLabel(id) }))} visibility={fingerprintVisibility} onChange={onFingerprintVisChange} />
          }
          isLoading={isLoading}
          isFetching={isFetching}
          className="min-h-[300px]"
          contentClassName="p-0"
          helpTitle={SECURITY_INFO.fingerprints.title}
          helpContent={SECURITY_INFO.fingerprints.body}
        >
          <DataTable
            columns={fingerprintColumns}
            data={(data as any)?.tls_fingerprints || []}
            emptyMessage={isLoading ? "" : "Requires Security: TLS Fingerprinting (Group H) fields to be enabled in Fastly logging."}
            hideToolbar
            columnVisibility={fingerprintVisibility}
            onColumnVisibilityChange={setFingerprintVisibility}
          />
        </AnalyticsCard>
      </div>
    </>
  )
}

'use client'

import React from 'react'
import type { Dispatch, SetStateAction } from 'react'
import type { VisibilityState } from '@tanstack/react-table'
import { useActiveLogFields } from '@/hooks/useBootstrap'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { DataTable, ColumnVisibilityDropdown } from '@/components/DataTable'
import { PlotlyChart } from '@/components/PlotlyChart'
import { Bot, Fingerprint, Scale } from 'lucide-react'
import type { PlotlyLayout } from '@/types/api'
import {
  BOT_COLUMN_IDS,
  BOT_EFFICIENCY_COLUMN_IDS,
  botColumns,
  botEfficiencyColumns,
  FINGERPRINT_COLUMN_IDS,
  fingerprintColumns,
  FingerprintCoverageHint,
  NGWAF_BOT_COLUMN_IDS,
  ngwafBotColumns,
  SECURITY_INFO,
  type SecurityData,
  useBotEfficiencyChartData,
  useNgwafBotsChartData,
} from './botsSectionConfig'

export { SECURITY_INFO, type SecurityData }

export type BotsSectionProps = {
  data: SecurityData | undefined
  isLoading: boolean
  isFetching: boolean
  error: Error | null
  intervalButtons: React.ReactNode
  bucketSeconds: number
  timezone: string
  commonTimeLayout: PlotlyLayout
  getFieldLabel: (id: string) => string
  ngwafBotVisibility: VisibilityState
  setNgwafBotVisibility: Dispatch<SetStateAction<VisibilityState>>
  onNgwafBotVisChange: (id: string, vis: boolean) => void
  botVisibility: VisibilityState
  setBotVisibility: Dispatch<SetStateAction<VisibilityState>>
  onBotVisChange: (id: string, vis: boolean) => void
  botEfficiencyVisibility?: VisibilityState
  setBotEfficiencyVisibility?: Dispatch<SetStateAction<VisibilityState>>
  onBotEfficiencyVisChange?: (id: string, vis: boolean) => void
  fingerprintVisibility: VisibilityState
  setFingerprintVisibility: Dispatch<SetStateAction<VisibilityState>>
  onFingerprintVisChange: (id: string, vis: boolean) => void
}

export function BotsSection({
  data,
  isLoading,
  isFetching,
  error,
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
  botEfficiencyVisibility = {},
  setBotEfficiencyVisibility,
  onBotEfficiencyVisChange,
  fingerprintVisibility,
  setFingerprintVisibility,
  onFingerprintVisChange,
}: BotsSectionProps) {
  const { isFieldActive } = useActiveLogFields()
  const labeledColumns = React.useCallback(
    (ids: readonly string[]) => ids.map((id) => ({ id, label: getFieldLabel(id) })),
    [getFieldLabel],
  )

  const ngwafBotsData = useNgwafBotsChartData(data, timezone, bucketSeconds)
  const botEfficiencyChartData = useBotEfficiencyChartData(data)

  const cardErr = error as AnalyticsCardError | null
  const uaEmptyMsg = isFieldActive('ua')
    ? 'No bot or crawler traffic detected in this time window.'
    : 'Requires User-Agent (ua) field to be enabled in Fastly logging.'

  return (
    <>
      {data?.ngwaf_configured !== false && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
          <AnalyticsCard
            title="Verified Bots (NGWAF)"
            icon={<Bot className="h-4 w-4" />}
            headerAction={intervalButtons}
            isLoading={isLoading}
            isFetching={isFetching}
            loadingMessage="Loading NGWAF verified bots..."
            error={cardErr}
            className="h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
            contentClassName="p-2"
            helpTitle={SECURITY_INFO.ngwaf_bots.title}
            helpContent={SECURITY_INFO.ngwaf_bots.body}
          >
            {ngwafBotsData.length === 0 && !isLoading ? (
              <div className="flex items-center justify-center h-full text-muted-foreground text-sm text-center px-4">
                {data?.ngwaf_configured ? (
                  'No NGWAF bot detections in this time window.'
                ) : (
                  <>
                    Set <code className="mx-1 text-xs bg-muted px-1 rounded">ngwaf_workspace_id</code> in service
                    settings to enable NGWAF bot tracking.
                  </>
                )}
              </div>
            ) : (
              <PlotlyChart
                data={ngwafBotsData}
                layout={{
                  ...commonTimeLayout,
                  barmode: 'stack',
                  showlegend: true,
                  yaxis: { title: 'Requests', separatethousands: true, exponentformat: 'none' },
                }}
                height="100%"
              />
            )}
          </AnalyticsCard>

          <AnalyticsCard
            title="Verified Bot Names (NGWAF)"
            icon={<Bot className="h-4 w-4" />}
            headerAction={
              <ColumnVisibilityDropdown
                columns={labeledColumns(NGWAF_BOT_COLUMN_IDS)}
                visibility={ngwafBotVisibility}
                onChange={onNgwafBotVisChange}
              />
            }
            isLoading={isLoading}
            isFetching={isFetching}
            loadingMessage="Loading NGWAF bot names..."
            error={cardErr}
            className="min-h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
            contentClassName="p-0"
            helpTitle={SECURITY_INFO.ngwaf_bots.title}
            helpContent={SECURITY_INFO.ngwaf_bots.body}
          >
            <DataTable
              columns={ngwafBotColumns}
              data={data?.ngwaf_verified_bots || []}
              emptyMessage={
                isLoading
                  ? ''
                  : data?.ngwaf_configured
                    ? 'No NGWAF bot detections in this time window.'
                    : 'Set ngwaf_workspace_id in service settings to enable NGWAF bot tracking.'
              }
              hideToolbar
              columnVisibility={ngwafBotVisibility}
              onColumnVisibilityChange={setNgwafBotVisibility}
            />
          </AnalyticsCard>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        <AnalyticsCard
          title="Well-Known Bots"
          icon={<Bot className="h-4 w-4" />}
          headerAction={
            <ColumnVisibilityDropdown
              columns={labeledColumns(BOT_COLUMN_IDS)}
              visibility={botVisibility}
              onChange={onBotVisChange}
            />
          }
          isLoading={isLoading}
          isFetching={isFetching}
          loadingMessage="Loading well-known bots..."
          error={cardErr}
          className="min-h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
          contentClassName="p-0"
          helpTitle={SECURITY_INFO.wellknown_bots.title}
          helpContent={SECURITY_INFO.wellknown_bots.body}
        >
          <DataTable
            columns={botColumns}
            data={data?.wellknown_bots || []}
            emptyMessage={isLoading ? '' : 'No known bots detected. Ensure bot sources are cached in Admin settings.'}
            hideToolbar
            tableContainerClassName="max-h-[280px]"
            columnVisibility={botVisibility}
            onColumnVisibilityChange={setBotVisibility}
          />
        </AnalyticsCard>

        <AnalyticsCard
          title="Top TLS Fingerprints"
          icon={<Fingerprint className="h-4 w-4" />}
          headerAction={
            <ColumnVisibilityDropdown
              columns={labeledColumns(FINGERPRINT_COLUMN_IDS)}
              visibility={fingerprintVisibility}
              onChange={onFingerprintVisChange}
            />
          }
          isLoading={isLoading}
          isFetching={isFetching}
          loadingMessage="Loading TLS fingerprints..."
          error={cardErr}
          className="min-h-[300px] [contain-intrinsic-size:300px] [content-visibility:auto]"
          contentClassName="p-0"
          helpTitle={SECURITY_INFO.fingerprints.title}
          helpContent={SECURITY_INFO.fingerprints.body}
        >
          <FingerprintCoverageHint coverage={data?.fingerprint_coverage?.tls_ciphers_sha} field="tls_ciphers_sha" />
          <DataTable
            columns={fingerprintColumns}
            data={data?.tls_fingerprints || []}
            emptyMessage={
              isLoading
                ? ''
                : isFieldActive('ja3') || isFieldActive('ja4')
                  ? 'No TLS fingerprints in this time range.'
                  : 'Requires Security: TLS Fingerprinting (Group H) fields to be enabled in Fastly logging.'
            }
            hideToolbar
            tableContainerClassName="max-h-[280px]"
            columnVisibility={fingerprintVisibility}
            onColumnVisibilityChange={setFingerprintVisibility}
          />
        </AnalyticsCard>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        <AnalyticsCard
          title="Bot & Crawler Cost vs. Referrals"
          icon={<Scale className="h-4 w-4" />}
          isLoading={isLoading}
          isFetching={isFetching}
          loadingMessage="Loading bot cost vs. referrals chart..."
          error={cardErr}
          className="h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]"
          contentClassName="p-2"
          helpTitle={SECURITY_INFO.bot_efficiency.title}
          helpContent={SECURITY_INFO.bot_efficiency.body}
        >
          {botEfficiencyChartData.length === 0 && !isLoading ? (
            <div className="flex items-center justify-center h-full text-muted-foreground text-sm text-center px-4">
              {uaEmptyMsg}
            </div>
          ) : (
            <PlotlyChart
              data={botEfficiencyChartData}
              layout={{
                barmode: 'group',
                showlegend: true,
                yaxis: { title: 'Count', separatethousands: true, exponentformat: 'none' },
              }}
              height="100%"
            />
          )}
        </AnalyticsCard>

        <AnalyticsCard
          title="Bot & Crawler Value vs. Cost (Bandwidth, Requests & Referrals)"
          icon={<Scale className="h-4 w-4" />}
          headerAction={
            onBotEfficiencyVisChange ? (
              <ColumnVisibilityDropdown
                columns={labeledColumns(BOT_EFFICIENCY_COLUMN_IDS)}
                visibility={botEfficiencyVisibility}
                onChange={onBotEfficiencyVisChange}
              />
            ) : undefined
          }
          isLoading={isLoading}
          isFetching={isFetching}
          loadingMessage="Loading bot bandwidth & referral efficiency..."
          error={cardErr}
          className="min-h-[300px] [contain-intrinsic-size:300px] [content-visibility:auto]"
          contentClassName="p-0"
          helpTitle={SECURITY_INFO.bot_efficiency.title}
          helpContent={SECURITY_INFO.bot_efficiency.body}
        >
          <DataTable
            columns={botEfficiencyColumns}
            data={data?.bot_efficiency || []}
            emptyMessage={isLoading ? '' : uaEmptyMsg}
            hideToolbar
            tableContainerClassName="max-h-[280px]"
            columnVisibility={botEfficiencyVisibility}
            onColumnVisibilityChange={setBotEfficiencyVisibility}
          />
        </AnalyticsCard>
      </div>
    </>
  )
}

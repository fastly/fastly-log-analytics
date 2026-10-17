'use client'

import { type AnalyticsCardError } from '@/components/AnalyticsCard'
import { ComplianceHelp } from '@/components/SessionScoring/help-content'

import { StackedHourlyBarChart } from './StackedHourlyBarChart'
import { useScoringQuery } from './useScoringQuery'
import type { components } from '@/types/api.generated'
import { client } from '@/lib/api'
import { CHART_COLORS } from '@/lib/chart-colors'

interface ComplianceChartProps {
  serviceId: string
  sinceHours?: number
}

type CompRow = components['schemas']['ScoringComplianceRow']

const COMPLIANCE_COLORS: Record<string, string> = {
  ok: CHART_COLORS.success,
  missing: CHART_COLORS.neutral,
  tampered: '#e11d48',
  expired: CHART_COLORS.warning,
  unknown: '#7c3aed',
}

export function ComplianceChart({ serviceId, sinceHours = 24 }: ComplianceChartProps) {
  const { data, isLoading, isFetching, isError, error } = useScoringQuery(['scoring-compliance', serviceId, sinceHours], () =>
    client.GET('/api/services/{service_id}/scoring/compliance-breakdown', {
      params: { path: { service_id: serviceId }, query: { since_hours: sinceHours } },
    }),
  )

  return (
    <StackedHourlyBarChart<CompRow>
      title={`Cookie compliance — last ${sinceHours}h`}
      description="Breakdown of edge_cookie_compliance per hour. 'missing' is the canonical bot signal (no cookie at all); 'tampered' is the post-cookie threat (someone modified the payload)."
      helpContent={<ComplianceHelp />}
      helpTitle="About Cookie Compliance"
      isLoading={isLoading}
      isFetching={isFetching}
      error={isError ? (error as AnalyticsCardError) : null}
      rows={data?.rows ?? []}
      categoryKey="compliance"
      colors={COMPLIANCE_COLORS}
    />
  )
}

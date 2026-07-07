'use client'

import { type AnalyticsCardError } from '@/components/AnalyticsCard'
import { ComplianceHelp } from '@/components/SessionScoring/help-content'

import { StackedHourlyBarChart } from './StackedHourlyBarChart'
import { useScoringQuery } from './useScoringQuery'
import type { Expect, WireParity } from '@/types/api'
import type { components } from '@/types/api.generated'

interface ComplianceChartProps {
  serviceId: string
  sinceHours?: number
}

// Deliberate narrowing of the all-optional generated ScoringComplianceRow:
// the compliance-breakdown SQL always emits all three columns per row.
// Key/type drift against the wire schema fails on the parity guard below.
interface CompRow {
  hour: string
  compliance: string
  count: number
}
export type _CompRowParity = Expect<WireParity<CompRow, components['schemas']['ScoringComplianceRow']>>

const COMPLIANCE_COLORS: Record<string, string> = {
  ok: '#10b981',
  missing: '#94a3b8',
  tampered: '#e11d48',
  expired: '#f59e0b',
  unknown: '#7c3aed',
}

export function ComplianceChart({ serviceId, sinceHours = 24 }: ComplianceChartProps) {
  const { data, isLoading, isFetching, isError, error } = useScoringQuery<{ rows: CompRow[] }>(
    ['scoring-compliance', serviceId, sinceHours],
    serviceId,
    'compliance-breakdown',
    { since_hours: sinceHours },
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

'use client'

import { useQuery } from '@tanstack/react-query'

import { client } from '@/lib/api'
import { ComplianceHelp } from '@/components/SessionScoring/help-content'

import { StackedHourlyBarChart } from './StackedHourlyBarChart'

interface ComplianceChartProps {
  serviceId: string
  sinceHours?: number
}

interface CompRow {
  hour: string
  compliance: string
  count: number
}

const COMPLIANCE_COLORS: Record<string, string> = {
  ok: '#10b981',
  missing: '#94a3b8',
  tampered: '#e11d48',
  expired: '#f59e0b',
  unknown: '#7c3aed',
}

export function ComplianceChart({ serviceId, sinceHours = 24 }: ComplianceChartProps) {
  const { data, isLoading, isFetching } = useQuery({
    queryKey: ['scoring-compliance', serviceId, sinceHours],
    queryFn: async () => {
      const { data, response } = await client.GET(
        '/api/services/{service_id}/scoring/compliance-breakdown' as any,
        {
          params: {
            path: { service_id: serviceId },
            query: { since_hours: sinceHours },
          },
        } as any,
      )
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data as { rows: CompRow[] }
    },
  })

  return (
    <StackedHourlyBarChart<CompRow>
      title={`Cookie compliance — last ${sinceHours}h`}
      description="Breakdown of edge_cookie_compliance per hour. 'missing' is the canonical bot signal (no cookie at all); 'tampered' is the post-cookie threat (someone modified the payload)."
      helpContent={<ComplianceHelp />}
      helpTitle="About Cookie Compliance"
      isLoading={isLoading}
      isFetching={isFetching}
      rows={data?.rows ?? []}
      categoryKey="compliance"
      colors={COMPLIANCE_COLORS}
    />
  )
}

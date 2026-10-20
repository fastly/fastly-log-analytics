import { TrendingUp } from 'lucide-react'

import { AnalyticsCard } from '@/components/AnalyticsCard'
import { BackToAdminLink } from '@/components/BackToAdminLink'
import { Button } from '@/components/ui/button'
import { PageHeader } from '@/components/ui/page-header'
const WINDOWS = [
  { label: 'Last hour', value: '1h' },
  { label: 'Last 6h', value: '6h' },
  { label: 'Last 24h', value: '24h' },
  { label: 'Last 7 days', value: '7d' },
] as const

const SERIES_LAYOUT = [
  { title: 'CPU load (1m avg)', description: 'Linux load average over the trailing minute.', metric: 'cpu_load_1m' },
  { title: 'Memory used', description: 'Physical memory consumption.', metric: 'mem_used_pct' },
  { title: 'Data disk used', description: 'Disk consumption on the data mount.', metric: 'disk_used_pct' },
  { title: 'Boot disk used', description: 'Disk consumption on the root mount.', metric: 'disk_used_pct_root' },
  { title: 'Active DuckDB queries', description: 'Concurrent in-flight query count from the registry.', metric: 'active_query_count' },
  { title: 'Pool wait p95', description: 'Worst-case DuckDB pool checkout wait (ms) — max across services per timestamp.', metric: 'pool_wait_p95_ms' },
  { title: 'Ingest lag', description: 'Seconds since the most recent ingest per service — max across services per timestamp.', metric: 'ingest_lag_s' },
] as const

export default function Loading() {
  return (
    <div data-testid="admin-trends-loading" className="container mx-auto p-4 space-y-4">
      <PageHeader
        title="Operational Trends"
        description="Sampled every 60 s. Retained 30 days. Backed by PostgreSQL metric_snapshots."
        icon={TrendingUp}
      >
        <div className="flex items-center gap-2">
          <Button type="button" variant="outline" size="sm" disabled>
            Refresh
          </Button>
          <BackToAdminLink variant="ghost" prefetch={false} />
        </div>
      </PageHeader>

      <div
        data-testid="admin-trends-toolbar"
        className="flex flex-wrap items-center gap-2 min-h-[36px] [contain-intrinsic-size:auto_36px] [content-visibility:auto]"
      >
        {WINDOWS.map((w) => (
          <Button
            key={w.value}
            type="button"
            variant={w.value === '1h' ? 'default' : 'outline'}
            size="sm"
            disabled
          >
            {w.label}
          </Button>
        ))}
      </div>

      <div
        data-testid="admin-trends-grid-reservation"
        className="grid grid-cols-1 lg:grid-cols-2 gap-4 min-h-[680px] [contain-intrinsic-size:auto_680px] [content-visibility:auto]"
      >
        {SERIES_LAYOUT.map((cfg) => (
          <div key={cfg.metric} data-testid={`admin-trends-card-${cfg.metric}`}>
            <AnalyticsCard
              title={cfg.title}
              description={cfg.description}
              isLoading={true}
              loadingMessage="Loading system vital trends..."
            >
              <div className="h-[152px]" />
            </AnalyticsCard>
          </div>
        ))}
      </div>
    </div>
  )
}

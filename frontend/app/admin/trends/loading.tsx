import { TrendingUp } from 'lucide-react'

import { AnalyticsCard } from '@/components/AnalyticsCard'
import { BackToAdminLink } from '@/components/BackToAdminLink'
import { Button } from '@/components/ui/button'
import { PageHeader } from '@/components/ui/page-header'
import { SERIES_LAYOUT, WINDOWS } from './_sections/trends-config'

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

import { PageHeader } from '@/components/ui/confirm-dialog'
import { Card, CardContent, CardHeader } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'

const TREND_KEYS = ['queue-depth', 'active-tasks'] as const
const SECTION_KEYS = ['depths', 'workers', 'ledger', 'schedules'] as const

/**
 * Above-the-fold zero-CLS skeleton matching page.tsx (Queue & Workers) so the
 * skeleton -> hydrated swap does not reflow.
 */
export default function Loading() {
  return (
    <div className="space-y-6" data-testid="admin-queue-loading">
      <PageHeader
        title="Queue & Workers"
        description="Monitor Celery queue depths, active workers, and scheduled tasks."
      >
        <Skeleton className="h-7 w-28" />
      </PageHeader>

      <div
        data-testid="admin-queue-toolbar-skeleton"
        className="flex items-center justify-between gap-3 min-h-[36px] [contain-intrinsic-size:auto_36px] [content-visibility:auto]"
      >
        <div className="flex items-center gap-3">
          <Skeleton className="h-5 w-28 rounded-full" />
          <span className="text-xs text-muted-foreground">
            Inspecting Celery task broker...
          </span>
        </div>
        <Skeleton className="h-8 w-24" />
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 min-h-[204px] [contain-intrinsic-size:auto_204px] [content-visibility:auto]">
        {TREND_KEYS.map((key) => (
          <Card key={key} className="min-h-[204px]">
            <CardHeader className="pb-3 flex flex-row items-center justify-between space-y-0">
              <Skeleton className="h-5 w-44" />
              <span className="text-xs text-muted-foreground">
                Loading queue telemetry...
              </span>
            </CardHeader>
            <CardContent>
              <Skeleton className="h-8 w-16 mb-3" />
              <Skeleton className="h-20 w-full" />
            </CardContent>
          </Card>
        ))}
      </div>

      {SECTION_KEYS.map((sectionKey) => (
        <div
          key={sectionKey}
          className="min-h-[200px] [contain-intrinsic-size:auto_200px] [content-visibility:auto]"
        >
          <Card className="min-h-[200px]">
            <CardHeader className="pb-3 flex flex-row items-center justify-between space-y-0">
              <Skeleton className="h-5 w-48" />
              <span className="text-xs text-muted-foreground">
                {sectionKey === 'depths' || sectionKey === 'workers'
                  ? 'Inspecting Celery task broker...'
                  : 'Loading queue telemetry...'}
              </span>
            </CardHeader>
            <CardContent>
              <Skeleton className="h-24 w-full" />
            </CardContent>
          </Card>
        </div>
      ))}
    </div>
  )
}

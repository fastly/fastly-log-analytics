import { BackToAdminLink } from '@/components/BackToAdminLink'
import { PageHeader } from '@/components/ui/page-header'
import { Card, CardContent, CardHeader } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'

export function ClickHousePanelFallback() {
  return (
    <div className="space-y-6">
      {/* Toolbar reservation */}
      <div
        data-testid="admin-clickhouse-toolbar"
        className="flex flex-wrap items-center justify-between gap-3 rounded-lg border bg-card px-4 py-3 min-h-[56px] [contain-intrinsic-size:auto_56px] [content-visibility:auto]"
      >
        <div className="flex items-center gap-3">
          <Skeleton className="h-5 w-36 rounded-full" />
          <Skeleton className="h-5 w-24 rounded-full" />
        </div>
        <div className="flex items-center gap-2">
          <Skeleton className="h-8 w-24" />
        </div>
      </div>

      {/* Cluster & Publication Vitals reservation */}
      <Card
        data-testid="admin-clickhouse-vitals-reservation"
        className="min-h-[220px] [contain-intrinsic-size:auto_220px] [content-visibility:auto]"
      >
        <CardHeader className="pb-2">
          <Skeleton className="h-5 w-52" />
        </CardHeader>
        <CardContent className="space-y-4">
          <p className="text-xs text-muted-foreground">
            Inspecting ClickHouse cluster and manifest publication telemetry...
          </p>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <Skeleton className="h-20 w-full" />
            <Skeleton className="h-20 w-full" />
            <Skeleton className="h-20 w-full" />
            <Skeleton className="h-20 w-full" />
          </div>
        </CardContent>
      </Card>

      {/* Active Generation Card reservation */}
      <Card
        data-testid="admin-clickhouse-generation-reservation"
        className="min-h-[200px] [contain-intrinsic-size:auto_200px] [content-visibility:auto]"
      >
        <CardHeader className="pb-2">
          <Skeleton className="h-5 w-44" />
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <Skeleton className="h-16 w-full" />
            <Skeleton className="h-16 w-full" />
            <Skeleton className="h-16 w-full" />
            <Skeleton className="h-16 w-full" />
          </div>
        </CardContent>
      </Card>

      {/* Bounded Replay Control Panel reservation */}
      <Card
        data-testid="admin-clickhouse-replay-reservation"
        className="min-h-[300px] [contain-intrinsic-size:auto_300px] [content-visibility:auto]"
      >
        <CardHeader className="pb-2">
          <Skeleton className="h-5 w-56" />
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <Skeleton className="h-10 w-full" />
            <Skeleton className="h-10 w-full" />
            <Skeleton className="h-10 w-full" />
          </div>
          <Skeleton className="h-10 w-40" />
        </CardContent>
      </Card>
    </div>
  )
}

/**
 * Route-level Next.js loading skeleton for `/admin/clickhouse`.
 * Mirrors the exact layout of `frontend/app/admin/clickhouse/page.tsx` +
 * `ClickHouseAdminClient` with pre-allocated intrinsic heights so hydration
 * produces zero layout shift (CLS <= 0.05).
 */
export default function Loading() {
  return (
    <div className="space-y-6" data-testid="admin-clickhouse-loading">
      <PageHeader
        title="ClickHouse Replay & Cluster Operations"
        description="Monitor High-Scale ClickHouse cluster status, publication manifest health, and execute bounded artifact replays."
      >
        <BackToAdminLink />
      </PageHeader>

      <ClickHousePanelFallback />
    </div>
  )
}

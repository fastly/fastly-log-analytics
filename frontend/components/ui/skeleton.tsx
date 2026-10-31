import * as React from 'react'
import { Loader2 } from 'lucide-react'
import { cn } from '@/lib/utils'
import { Card, CardContent, CardHeader } from '@/components/ui/card'

function Skeleton({ className, ...props }: React.ComponentProps<'div'>) {
  return (
    <div
      data-slot="skeleton"
      className={cn('animate-pulse rounded-md bg-muted', className)}
      {...props}
    />
  )
}

interface SkeletonGridProps {
  count: number
  height?: string
  className?: string
}

function SkeletonGrid({ count, height = '120px', className }: SkeletonGridProps) {
  return (
    <>
      {Array.from({ length: count }, (_, i) => (
        <Skeleton key={`skel-grid-${i}`} className={cn('w-full rounded-xl', className)} style={{ height }} />
      ))}
    </>
  )
}

interface PageSkeletonHeaderProps {
  title?: string
  description?: string
  children?: React.ReactNode
}

function SkeletonHeader({ title, description, children }: PageSkeletonHeaderProps) {
  return (
    <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 mb-6">
      <div className="space-y-1">
        <h1 className="text-2xl font-bold tracking-tight">{title}</h1>
        {description && <p className="text-sm text-muted-foreground max-w-2xl">{description}</p>}
      </div>
      {children && <div className="flex items-center gap-2 shrink-0">{children}</div>}
    </div>
  )
}

export function DashboardSkeleton({ title, description }: PageSkeletonHeaderProps = {}) {
  return (
    <div className="space-y-4" data-testid="skeleton">
      {title ? <SkeletonHeader title={title} description={description} /> : <Skeleton className="h-7 w-48" />}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <SkeletonGrid count={4} height="5rem" className="rounded-md" />
      </div>
      <Skeleton className="h-72 w-full" />
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
        <Skeleton className="h-56 w-full" />
        <Skeleton className="h-56 w-full" />
      </div>
    </div>
  )
}

export function TableSkeleton({ rows = 12, title, description }: PageSkeletonHeaderProps & { rows?: number } = {}) {
  return (
    <div className="space-y-4">
      {title ? (
        <SkeletonHeader title={title} description={description} />
      ) : (
        <div className="flex items-center justify-between">
          <Skeleton className="h-7 w-48" />
          <Skeleton className="h-9 w-32" />
        </div>
      )}
      <div className="border rounded-md overflow-hidden">
        <Skeleton className="h-10 w-full rounded-none" />
        <div className="divide-y">
          <SkeletonGrid count={rows} height="3rem" className="rounded-none" />
        </div>
      </div>
    </div>
  )
}

export function FormSkeleton() {
  return (
    <div className="space-y-4">
      <Skeleton className="h-7 w-56" />
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="space-y-3"><Skeleton className="h-32 w-full" /><Skeleton className="h-32 w-full" /></div>
        <div className="space-y-3"><Skeleton className="h-32 w-full" /><Skeleton className="h-32 w-full" /></div>
      </div>
    </div>
  )
}

export function EditorSplitSkeleton() {
  return (
    <div className="space-y-4">
      <Skeleton className="h-7 w-40" />
      <div className="grid grid-cols-1 lg:grid-cols-[1fr_2fr] gap-3 min-h-[60vh]">
        <Skeleton className="w-full h-full min-h-72" />
        <Skeleton className="w-full h-full min-h-72" />
      </div>
    </div>
  )
}

export function AuthCardSkeleton({
  maxWidth = 'md',
  verticalCenter = false,
  topPad = false,
}: {
  maxWidth?: 'md' | 'xl'
  verticalCenter?: boolean
  topPad?: boolean
}) {
  const container = ['min-h-screen flex', verticalCenter ? 'items-center' : null, 'justify-center bg-muted/40 p-6', topPad ? 'pt-20' : null]
    .filter(Boolean)
    .join(' ')
  return (
    <div className={container} data-testid="auth-card-skeleton">
      <Card className={`w-full ${maxWidth === 'xl' ? 'max-w-xl' : 'max-w-md'} min-h-[380px] [contain-intrinsic-size:auto_380px] [content-visibility:auto]`}>
        <CardHeader className="space-y-2">
          <div className="flex items-center gap-2">
            <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" aria-hidden="true" />
            <span className="text-sm text-muted-foreground" role="status">
              Verifying share session &amp; security policies...
            </span>
          </div>
        </CardHeader>
        <CardContent className="min-h-[240px] [contain-intrinsic-size:auto_240px] [content-visibility:auto]">
          <div className="h-32 space-y-3" aria-hidden="true">
            <Skeleton className="h-4 w-24" />
            <Skeleton className="h-9 w-full" />
            <Skeleton className="h-4 w-28" />
            <Skeleton className="h-9 w-full" />
            <Skeleton className="h-9 w-full" />
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

export function ClickHousePanelFallback() {
  return (
    <div className="space-y-6">
      <div
        data-testid="admin-clickhouse-toolbar"
        className="flex flex-wrap items-center justify-between gap-3 rounded-lg border bg-card px-4 py-3 min-h-[56px]"
      >
        <div className="flex items-center gap-3">
          <Skeleton className="h-5 w-36 rounded-full" />
          <Skeleton className="h-5 w-24 rounded-full" />
        </div>
        <Skeleton className="h-8 w-24" />
      </div>
      {([
        ['admin-clickhouse-vitals-reservation', 'min-h-[300px]', 4, '5rem'],
        ['admin-clickhouse-generation-reservation', 'min-h-[212px]', 4, '4rem'],
        ['admin-clickhouse-replay-reservation', 'min-h-[320px]', 3, '2.5rem'],
      ] as const).map(([tid, cls, cols, h]) => (
        <Card key={tid} data-testid={tid} className={cls}>
          <CardHeader className="pb-2"><Skeleton className="h-5 w-48" /></CardHeader>
          <CardContent className="space-y-3">
            <div className={cn('grid gap-3', cols === 4 ? 'grid-cols-2 md:grid-cols-4' : 'grid-cols-1 md:grid-cols-3')}>
              <SkeletonGrid count={cols} height={h} />
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  )
}

export function RumPanelFallback() {
  return (
    <div className="space-y-4">
      <div
        className="flex items-center justify-between gap-3 rounded-lg border bg-card px-4 py-3 h-[56px] min-h-[56px]"
        data-testid="admin-rum-toolbar-skeleton"
      >
        <div className="flex items-center gap-3">
          <Skeleton className="h-5 w-36 rounded-full" />
          <Skeleton className="h-4 w-40" />
        </div>
        <div className="flex items-center justify-end gap-2 shrink-0 min-w-[428px]">
          <Skeleton className="h-8 w-28" />
          <Skeleton className="h-8 w-24" />
        </div>
      </div>
      <Card
        className="min-h-[260px]"
        data-testid="admin-rum-status-skeleton-card"
      >
        <CardHeader className="pb-2"><Skeleton className="h-5 w-32" /></CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2 min-h-[112px]">
            <p className="text-xs text-muted-foreground">Inspecting RUM beacon pipeline...</p>
            <Skeleton className="h-4 w-48" />
            <Skeleton className="h-4 w-64" />
            <Skeleton className="h-4 w-40" />
          </div>
          <div className="flex gap-2">
            <Skeleton className="h-9 w-28" />
            <Skeleton className="h-9 w-28" />
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

export type RouteSkeletonVariant =
  | 'admin-overview'
  | 'admin-clickhouse'
  | 'admin-queries'
  | 'admin-queue'
  | 'admin-rum'
  | 'admin-session-scoring'
  | 'admin-share'
  | 'admin-trends'
  | 'admin-usage-log'
  | 'high-scale-request-facts'
  | 'logs'

export function RouteSkeleton({ variant }: { variant: RouteSkeletonVariant }) {
  switch (variant) {
    case 'admin-overview':
      return (
        <div className="space-y-6" data-testid="admin-overview-loading" role="status" aria-live="polite">
          <div className="flex items-center justify-between text-xs text-muted-foreground">
            <span>Inspecting background scheduler state...</span>
            <span>Loading service configs...</span>
          </div>
          <div className="min-h-[108px] [contain-intrinsic-size:auto_108px] [content-visibility:auto]">
            <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
              {['queries', 'ingest-gap', 'slow-queries', 'workers'].map((slot) => (
                <div key={slot} className="h-[108px] rounded-xl border bg-card/50 animate-pulse" />
              ))}
            </div>
          </div>
          <div className="min-h-[296px]">
            <FormSkeleton />
          </div>
        </div>
      )
    case 'admin-clickhouse':
      return (
        <div className="space-y-6 min-h-[820px]" data-testid="admin-clickhouse-loading">
          <SkeletonHeader
            title="ClickHouse Replay & Cluster Operations"
            description="Monitor High-Scale ClickHouse cluster status, publication manifest health, and execute bounded artifact replays."
          />
          <ClickHousePanelFallback />
        </div>
      )
    case 'admin-queries':
      return (
        <div className="space-y-6" data-testid="admin-queries-loading">
          <SkeletonHeader
            title="Live Query Monitor"
            description="Real-time view of every executing DuckDB and Postgres query. Click a row to see the full SQL."
          >
            <Skeleton className="h-7 w-28" />
          </SkeletonHeader>
          <div
            data-testid="admin-queries-summary-skeleton"
            className="flex items-center justify-between gap-3 min-h-[36px] [contain-intrinsic-size:auto_36px]"
          >
            <div className="flex items-center gap-3">
              <Skeleton className="h-5 w-20 rounded-full" />
              <Skeleton className="h-5 w-16 rounded-full" />
              <span className="text-xs text-muted-foreground">Inspecting DuckDB connection pool...</span>
            </div>
            <Skeleton className="h-8 w-8" />
          </div>
          <Skeleton className="h-9 w-64" />
          {(['active', 'slow', 'completed'] as const).map((cardKey) => {
            const isActive = cardKey === 'active'
            return (
              <div
                key={cardKey}
                className={
                  isActive
                    ? 'min-h-[600px]'
                    : 'min-h-[200px]'
                }
              >
                <Card className={isActive ? 'min-h-[600px]' : 'min-h-[200px]'}>
                  <CardHeader className="pb-3 flex flex-row items-center justify-between space-y-0">
                    <Skeleton className="h-6 w-56" />
                    <span className="text-xs text-muted-foreground">
                      {isActive ? 'Inspecting DuckDB connection pool...' : 'Loading query telemetry...'}
                    </span>
                  </CardHeader>
                  <CardContent className="p-0">
                    <Skeleton className="h-10 w-full rounded-none" />
                    <Skeleton className={cn(isActive ? 'h-[520px]' : 'h-24', 'w-full rounded-none')} />
                  </CardContent>
                </Card>
              </div>
            )
          })}
        </div>
      )
    case 'admin-queue':
      return (
        <div className="space-y-6" data-testid="admin-queue-loading">
          <SkeletonHeader title="Queue & Workers" description="Monitor Celery queue depths, active workers, and scheduled tasks.">
            <Skeleton className="h-7 w-28" />
          </SkeletonHeader>
          <div
            data-testid="admin-queue-toolbar-skeleton"
            className="flex items-center justify-between gap-3 min-h-[36px]"
          >
            <div className="flex items-center gap-3">
              <Skeleton className="h-5 w-28 rounded-full" />
              <span className="text-xs text-muted-foreground">Inspecting Celery task broker...</span>
            </div>
            <Skeleton className="h-8 w-24" />
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 min-h-[204px]">
            {(['queue-depth', 'active-tasks'] as const).map((key) => (
              <Card key={key} className="min-h-[204px]">
                <CardHeader className="pb-3 flex flex-row items-center justify-between space-y-0">
                  <Skeleton className="h-5 w-44" />
                  <span className="text-xs text-muted-foreground">Loading queue telemetry...</span>
                </CardHeader>
                <CardContent><Skeleton className="h-8 w-16 mb-3" /><Skeleton className="h-20 w-full" /></CardContent>
              </Card>
            ))}
          </div>
          {(['depths', 'workers', 'ledger', 'schedules'] as const).map((sectionKey) => (
            <div key={sectionKey} className={sectionKey === 'depths' ? 'min-h-[248px]' : sectionKey === 'ledger' ? 'min-h-[240px]' : 'min-h-[200px]'}>
              <Card className={sectionKey === 'depths' ? 'min-h-[248px]' : sectionKey === 'ledger' ? 'min-h-[240px]' : 'min-h-[200px]'}>
                <CardHeader className="pb-3 flex flex-row items-center justify-between space-y-0">
                  <Skeleton className="h-5 w-48" />
                  <span className="text-xs text-muted-foreground">
                    {sectionKey === 'depths' || sectionKey === 'workers'
                      ? 'Inspecting Celery task broker...'
                      : 'Loading queue telemetry...'}
                  </span>
                </CardHeader>
                <CardContent><Skeleton className="h-24 w-full" /></CardContent>
              </Card>
            </div>
          ))}
        </div>
      )
    case 'admin-rum':
      return (
        <div className="space-y-6 min-h-[820px]" data-testid="admin-rum-loading">
          <SkeletonHeader title="Real User Monitoring" description="Configure and monitor RUM collection for your service" />
          <RumPanelFallback />
        </div>
      )
    case 'admin-session-scoring':
      return (
        <div className="space-y-6" data-testid="admin-session-scoring-loading">
          <SkeletonHeader
            title="Session Scoring"
            description="Real-time edge scoring of every request via the scorer Compute service. Toggle scoring on/off, watch the score distribution, and label sessions to evaluate matrix quality (ROC-AUC)."
          />
          <div className="space-y-6" data-testid="admin-session-scoring-fallback">
            <div className="min-h-[236px]" data-testid="admin-session-scoring-status-reservation">
              <Skeleton className="h-[236px] w-full rounded-xl" />
            </div>
            <div className="space-y-6 min-h-[900px]" data-testid="admin-session-scoring-overview-reservation">
              <Skeleton className="h-[240px] w-full rounded-xl" />
              <Skeleton className="h-80 w-full rounded-xl" />
            </div>
          </div>
        </div>
      )
    case 'admin-share':
      return (
        <div className="space-y-6" data-testid="admin-share-loading">
          <SkeletonHeader
            title="Remote Dashboard Sharing"
            description="Start the share tunnel, manage analyst invitations, monitor live sessions, and review the audit trail."
          />
          <div className="space-y-6" data-testid="admin-share-fallback">
            <div className="min-h-[220px]" data-testid="admin-share-control-reservation">
              <Skeleton className="h-[220px] w-full rounded-xl" />
            </div>
            <div className="min-h-[360px]" data-testid="admin-share-panel-reservation">
              <Skeleton className="h-[360px] w-full rounded-xl" />
            </div>
          </div>
        </div>
      )
    case 'admin-trends':
      return (
        <div data-testid="admin-trends-loading" className="container mx-auto p-4 space-y-4">
          <SkeletonHeader
            title="Operational Trends"
            description="Sampled every 60 s. Retained 30 days. Backed by PostgreSQL metric_snapshots."
          />
          <div
            data-testid="admin-trends-toolbar"
            className="flex flex-wrap items-center gap-2 min-h-[36px] [contain-intrinsic-size:auto_36px] [content-visibility:visible]"
          >
            <Skeleton className="h-8 w-48" />
          </div>
          <div
            data-testid="admin-trends-grid-reservation"
            className="grid grid-cols-1 lg:grid-cols-2 gap-4 min-h-[680px] [contain-intrinsic-size:auto_680px] [content-visibility:visible]"
          >
            <SkeletonGrid count={4} height="160px" />
          </div>
        </div>
      )
    case 'admin-usage-log':
      return (
        <div data-testid="admin-usage-log-loading" className="space-y-6">
          <SkeletonHeader
            title="FOS Usage Log"
            description="Fastly Object Storage and CDN operations captured for cost analysis."
          >
            <div
              data-testid="admin-usage-log-toolbar"
              className="flex flex-wrap items-center justify-end gap-2 min-h-[36px] min-w-[440px] [contain-intrinsic-size:auto_36px] [content-visibility:auto]"
            >
              <Skeleton className="h-8 w-24" />
            </div>
          </SkeletonHeader>
          <Skeleton className="h-11 w-full rounded-lg" />
          <div
            data-testid="admin-usage-log-kpi-reservation"
            className="grid grid-cols-2 md:grid-cols-4 gap-3 min-h-[216px] [contain-intrinsic-size:auto_164px] [content-visibility:auto]"
          >
            <SkeletonGrid count={4} height="216px" />
          </div>
          <div
            data-testid="admin-usage-log-chart-reservation"
            className="min-h-[380px] [contain-intrinsic-size:auto_380px] [content-visibility:auto]"
          >
            <Skeleton className="h-[380px] w-full rounded-lg" />
          </div>
          <div
            data-testid="admin-usage-log-table-reservation"
            className="rounded-lg border bg-card overflow-hidden min-h-[520px] [contain-intrinsic-size:auto_520px] [content-visibility:auto]"
          >
            <div className="px-4 py-3 border-b flex items-center justify-between">
              <span className="text-xs text-muted-foreground italic">Loading FOS usage ledger...</span>
              <Skeleton className="h-7 w-48" />
            </div>
            <div className="divide-y">
              <SkeletonGrid count={10} height="3rem" className="rounded-none" />
            </div>
          </div>
        </div>
      )
    case 'high-scale-request-facts':
      return (
        <div
          data-testid="high-scale-request-facts-route-loading"
          className="min-h-[720px] [content-visibility:auto] [contain-intrinsic-size:auto_720px]"
        >
          <TableSkeleton
            title="High-scale request facts"
            description="Explicit opt-in access to the isolated high-scale request-facts API."
          />
        </div>
      )
    case 'logs':
      return (
        <div className="space-y-6">
          <SkeletonHeader
            title="Data Management"
            description="Monitor and manage log ingestion history and active data syncs."
          />
          <Skeleton className="h-12 w-full rounded-lg" />
          <Skeleton className="h-10 w-full rounded-md" />
          <div className="border rounded-md overflow-hidden">
            <Skeleton className="h-10 w-full rounded-none" />
            <div className="divide-y">
              <SkeletonGrid count={10} height="3rem" className="rounded-none" />
            </div>
          </div>
        </div>
      )
  }
}

export { Skeleton, SkeletonGrid }

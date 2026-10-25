import { BackToAdminLink } from '@/components/ReportLayout';
import { PageHeader } from '@/components/ui/page-header';
import { Card, CardContent, CardHeader } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';

export function RumPanelFallback() {
  return (
    <div className="space-y-4">
      <div
        className="flex items-center justify-between gap-3 rounded-lg border bg-card px-4 py-3 h-[56px] min-h-[56px] [contain-intrinsic-size:auto_56px] [content-visibility:auto]"
        data-testid="admin-rum-toolbar-skeleton"
      >
        <div className="flex items-center gap-3">
          <Skeleton className="h-5 w-36 rounded-full" />
          <Skeleton className="h-4 w-40" />
        </div>
        <div className="flex items-center gap-2">
          <Skeleton className="h-8 w-28" />
          <Skeleton className="h-8 w-24" />
        </div>
      </div>

      <Card
        className="min-h-[260px] [contain-intrinsic-size:auto_260px] [content-visibility:auto]"
        data-testid="admin-rum-status-skeleton-card"
      >
        <CardHeader className="pb-2">
          <Skeleton className="h-5 w-32" />
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2 min-h-[112px]">
            <p className="text-xs text-muted-foreground">
              Inspecting RUM beacon pipeline...
            </p>
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
  );
}

/**
 * Route-level Next.js loading skeleton for `/admin/rum`.
 * Mirrors the exact two-section layout of `frontend/app/admin/rum/page.tsx` +
 * `RumStatusPanel` (toolbar + RUM Status card) with pre-allocated intrinsic
 * heights so hydration produces zero layout shift (CLS <= 0.05).
 */
export default function Loading() {
  return (
    <div className="space-y-6" data-testid="admin-rum-loading">
      <PageHeader
        title="Real User Monitoring"
        description="Configure and monitor RUM collection for your service"
      >
        <BackToAdminLink />
      </PageHeader>

      <RumPanelFallback />
    </div>
  );
}

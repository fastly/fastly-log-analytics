import { AlertTriangle, ShieldAlert } from 'lucide-react'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { BackToAdminLink } from '@/components/BackToAdminLink'
import { PageHeader } from '@/components/ui/page-header'
import { Skeleton } from '@/components/ui/skeleton'

export function ShareDashboardFallback() {
  return (
    <div className="space-y-6" data-testid="admin-share-fallback">
      <div
        className="min-h-[220px] [contain-intrinsic-size:auto_220px] [content-visibility:auto]"
        data-testid="admin-share-control-reservation"
      >
        <Skeleton className="h-[220px] w-full rounded-xl" />
      </div>

      <div className="w-full space-y-4">
        <div className="inline-flex h-9 items-center justify-center rounded-lg bg-muted p-1">
          <Skeleton className="h-7 w-24 rounded-md" />
          <Skeleton className="h-7 w-20 rounded-md ml-1" />
          <Skeleton className="h-7 w-16 rounded-md ml-1" />
        </div>

        <div
          className="min-h-[360px] [contain-intrinsic-size:auto_360px] [content-visibility:auto]"
          data-testid="admin-share-panel-reservation"
        >
          <Skeleton className="h-[360px] w-full rounded-xl" />
        </div>
      </div>
    </div>
  )
}

/**
 * Route-level Next.js loading skeleton for `/admin/share`.
 * Mirrors the exact PageHeader + security warning Alert + SharingControlPanel reservation +
 * Tabs/panel reservation geometry of `./page.tsx` so SSR/hydration produces zero layout shift.
 */
export default function Loading() {
  return (
    <div className="space-y-6" data-testid="admin-share-loading">
      <PageHeader
        title="Remote Dashboard Sharing"
        description="Start the share tunnel, manage analyst invitations, monitor live sessions, and review the audit trail."
        icon={ShieldAlert}
      >
        <Skeleton className="h-8 w-36 rounded-md" />
        <Skeleton className="h-8 w-24 rounded-md" />
        <BackToAdminLink />
      </PageHeader>

      <Alert className="border-amber-300 bg-amber-50 text-amber-900 [&>svg]:text-amber-900">
        <AlertTriangle className="h-4 w-4" />
        <AlertDescription>
          <span className="font-semibold">Secure your server before sharing.</span>{' '}
          Remote sharing exposes this dashboard to invited analysts over the public internet.
          Before enabling the tunnel, confirm the host has an up-to-date OS, firewall rules
          restricting inbound access to the share port, and (recommended) only allows the
          tunnel through a reverse proxy you control. Each invited analyst gets read-only
          access scoped to the services you grant — but the underlying server is yours to
          harden.
        </AlertDescription>
      </Alert>

      <ShareDashboardFallback />
    </div>
  )
}

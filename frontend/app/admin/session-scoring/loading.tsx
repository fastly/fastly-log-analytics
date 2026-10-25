'use client'

import { ShieldCheck } from 'lucide-react'
import { BackToAdminLink } from '@/components/ReportLayout'
import { PageHeader } from '@/components/ui/confirm-dialog'
import { Skeleton } from '@/components/ui/skeleton'

function SessionScoringFallback() {
  return (
    <div className="space-y-6" data-testid="admin-session-scoring-fallback">
      <div
        className="min-h-[220px] [contain-intrinsic-size:auto_220px] [content-visibility:auto]"
        data-testid="admin-session-scoring-status-reservation"
      >
        <Skeleton className="h-[220px] w-full rounded-xl" />
      </div>

      <div className="w-full space-y-4">
        <div className="inline-flex h-9 items-center justify-center rounded-lg bg-muted p-1">
          <Skeleton className="h-7 w-20 rounded-md" />
          <Skeleton className="h-7 w-16 rounded-md ml-1" />
          <Skeleton className="h-7 w-28 rounded-md ml-1" />
          <Skeleton className="h-7 w-16 rounded-md ml-1" />
        </div>

        <div
          className="space-y-6 min-h-[900px] [contain-intrinsic-size:auto_900px] [content-visibility:auto]"
          data-testid="admin-session-scoring-overview-reservation"
        >
          <Skeleton className="h-[240px] w-full rounded-xl" />
          <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
            <Skeleton className="h-80 w-full rounded-xl" />
            <Skeleton className="h-80 w-full rounded-xl" />
          </div>
          <Skeleton className="h-48 w-full rounded-xl" />
          <Skeleton className="h-64 w-full rounded-xl" />
          <Skeleton className="h-72 w-full rounded-xl" />
          <Skeleton className="h-64 w-full rounded-xl" />
          <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
            <Skeleton className="h-80 w-full rounded-xl" />
            <Skeleton className="h-80 w-full rounded-xl" />
          </div>
        </div>
      </div>
    </div>
  )
}

/**
 * Route-level Next.js loading skeleton for `/admin/session-scoring`.
 * Mirrors the exact PageHeader + StatusPanel reservation + Tabs/Overview
 * reservation geometry of `./page.tsx` so SSR/hydration produces zero layout shift.
 */
export default function Loading() {
  return (
    <div className="space-y-6" data-testid="admin-session-scoring-loading">
      <PageHeader
        title="Session Scoring"
        description="Real-time edge scoring of every request via the scorer Compute service. Toggle scoring on/off, watch the score distribution, and label sessions to evaluate matrix quality (ROC-AUC)."
        icon={ShieldCheck}
      >
        <Skeleton className="h-8 w-28 rounded-md" />
        <Skeleton className="h-8 w-28 rounded-md" />
        <Skeleton className="h-8 w-24 rounded-md" />
        <Skeleton className="h-8 w-24 rounded-md" />
        <BackToAdminLink />
      </PageHeader>

      <SessionScoringFallback />
    </div>
  )
}
